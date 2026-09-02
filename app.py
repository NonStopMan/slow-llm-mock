"""
Mock slow-provider LLM service for cbllmgateway QA performance testing.

Purpose
-------
cbllmgateway is suspected of blocking/piling up requests on a pod whenever an
upstream LLM provider is slow (3-4+ minutes), eventually starving the pod of
capacity for unrelated outbound calls. This service lets QA reproduce
provider-side latency on demand, deterministically, without depending on a
real (and uncontrollable) third-party provider being slow at the right time.

It exposes an OpenAI-compatible `/v1/chat/completions` endpoint (adjust the
schema in `to_openai_response` / request parsing below if the QA provider
config for cbllmgateway expects a different contract, e.g. Azure OpenAI,
Bedrock, or Anthropic's native format).

Three simulated "models" (providers) are exposed from the same host/URL so a
single deployment can act as both the "healthy" canary provider and the
"incident" provider in the same test:

  - mock-fast : always responds quickly (~150-300ms). Use this as the canary
                lane to detect whether unrelated traffic gets starved while
                mock-slow is saturated.
  - mock-slow : latency is controlled at runtime via the /control/latency
                endpoint (fixed delay, jitter range, or "hang"). Defaults to
                fast behavior until a test scenario dials it up.
  - mock-hang : always sleeps far beyond any sane client/provider timeout
                (default 10 minutes) to test the extreme "provider never
                responds" case and whatever timeout/circuit-breaker behavior
                cbllmgateway has (or doesn't have).

Completion text is always static/canned (an "echo" of the prompt) - this
service is only testing latency and pileup behavior, not generation
quality, so there's no model to load, no GPU/CPU inference cost, and no
risk of the mock itself becoming a bottleneck (or OOMing) under concurrent
load. See git history for an earlier version that used a real small model.

Security
--------
The /control/* endpoints let a caller change the induced-latency behavior at
runtime. They require a shared-secret header (X-Control-Token) matching the
CONTROL_TOKEN environment variable. If CONTROL_TOKEN is not set, the control
endpoints are disabled (return 503) rather than defaulting to an open,
unauthenticated control plane. Do not hardcode a token in this file or in
version control — set it via environment/secret manager at deploy time.

/v1/chat/completions requires `Authorization: Bearer <CONTROL_TOKEN>` too -
the same shared secret, reusing the standard OpenAI-client auth convention so
cbllmgateway needs no code change: its Cassandra subscription's `api_key`
field already gets decrypted and sent as this exact header by the OpenAI SDK,
so setting that field to CONTROL_TOKEN's value is the only wiring needed.
Required once CONTROL_TOKEN is set, since this service is meant to be
reachable beyond a local Docker network (e.g. deployed for shared team use) -
without it, anyone with the URL could invoke completions or induce latency
that disrupts someone else's test run.

This service is for QA use only. Do not point production traffic at it.
"""

import asyncio
import logging
import os
import random
import threading
import time
import uuid
from typing import Literal, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("mock-slow-llm")

app = FastAPI(title="mock-slow-llm", version="1.0.0")

CONTROL_TOKEN = os.environ.get("CONTROL_TOKEN")  # unset => control endpoints disabled
HANG_SECONDS_DEFAULT = float(os.environ.get("MOCK_HANG_SECONDS", "600"))  # 10 min


def _generate_text(prompt: str) -> str:
    return f"[mock-slow-llm canned response] echo: {prompt[:200]}"


# ---------------------------------------------------------------------------
# Runtime-controllable latency state (protects mock-slow only)
# ---------------------------------------------------------------------------
class LatencyState:
    def __init__(self):
        self.lock = threading.Lock()
        self.mode: Literal["off", "fixed", "jitter", "hang"] = "off"
        self.fixed_ms = 0
        self.jitter_min_ms = 0
        self.jitter_max_ms = 0

    def snapshot(self):
        with self.lock:
            return {
                "mode": self.mode,
                "fixed_ms": self.fixed_ms,
                "jitter_min_ms": self.jitter_min_ms,
                "jitter_max_ms": self.jitter_max_ms,
            }

    def delay_seconds(self) -> Optional[float]:
        """Returns seconds to sleep, or None to mean 'hang indefinitely'."""
        with self.lock:
            mode, fixed_ms = self.mode, self.fixed_ms
            jmin, jmax = self.jitter_min_ms, self.jitter_max_ms
        if mode == "off":
            return 0.0
        if mode == "fixed":
            return fixed_ms / 1000.0
        if mode == "jitter":
            lo, hi = sorted((jmin, jmax))
            return random.uniform(lo, hi) / 1000.0
        if mode == "hang":
            return None
        return 0.0


latency_state = LatencyState()

MODEL_PROFILES = {"mock-fast", "mock-slow", "mock-hang"}


# ---------------------------------------------------------------------------
# OpenAI-compatible chat completions endpoint
# ---------------------------------------------------------------------------
class ChatMessage(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str = Field(..., description="mock-fast | mock-slow | mock-hang")
    messages: list[ChatMessage]
    max_tokens: Optional[int] = 64
    temperature: Optional[float] = 0.7
    # Optional per-request override so a load test can force a specific delay
    # on an individual request without touching the global /control state.
    # Not part of the real OpenAI schema - safe to ignore/strip upstream if
    # cbllmgateway's provider config validates schema strictly.
    delay_ms_override: Optional[int] = None


async def _apply_latency(model: str, override_ms: Optional[int]):
    # Uses asyncio.sleep (not time.sleep) so a slow/hung request only "occupies"
    # its own coroutine and does not block the event loop from serving other
    # concurrent requests (e.g. the mock-fast canary lane) within this process.
    # This keeps the mock service itself from becoming a confound in the test -
    # any pileup you observe should come from cbllmgateway, not from this mock.
    if override_ms is not None:
        await asyncio.sleep(max(override_ms, 0) / 1000.0)
        return

    if model == "mock-fast":
        await asyncio.sleep(random.uniform(0.15, 0.3))
        return

    if model == "mock-hang":
        await asyncio.sleep(HANG_SECONDS_DEFAULT)
        return

    if model == "mock-slow":
        secs = latency_state.delay_seconds()
        if secs is None:
            await asyncio.sleep(HANG_SECONDS_DEFAULT)
        else:
            await asyncio.sleep(secs)
        return

    # Unknown model name: behave like mock-fast rather than failing the test run.
    await asyncio.sleep(random.uniform(0.15, 0.3))


def _require_bearer_token(authorization: Optional[str]) -> None:
    # No CONTROL_TOKEN configured => leave this endpoint open, matching prior
    # behavior for a purely local Docker-network deployment. Once a token is
    # set (expected for anything reachable outside localhost), require it.
    if not CONTROL_TOKEN:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or malformed Authorization header.")
    if authorization[len("Bearer "):] != CONTROL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid API key.")


@app.post("/v1/chat/completions")
async def chat_completions(
    req: ChatCompletionRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    _require_bearer_token(authorization)
    if req.model not in MODEL_PROFILES:
        logger.info("Unrecognized model '%s' - treating as mock-fast.", req.model)

    request_id = f"chatcmpl-{uuid.uuid4().hex[:24]}"
    started = time.monotonic()

    await _apply_latency(req.model, req.delay_ms_override)

    prompt = req.messages[-1].content if req.messages else ""
    text = _generate_text(prompt)

    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info("model=%s elapsed_ms=%d request_id=%s", req.model, elapsed_ms, request_id)

    return {
        "id": request_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": req.model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": len(prompt.split()),
            "completion_tokens": len(text.split()),
            "total_tokens": len(prompt.split()) + len(text.split()),
        },
        "x_mock_elapsed_ms": elapsed_ms,
    }


# ---------------------------------------------------------------------------
# Control plane
# ---------------------------------------------------------------------------
class LatencyUpdate(BaseModel):
    mode: Literal["off", "fixed", "jitter", "hang"]
    fixed_ms: Optional[int] = 0
    jitter_min_ms: Optional[int] = 0
    jitter_max_ms: Optional[int] = 0


def _require_control_token(x_control_token: Optional[str]):
    if not CONTROL_TOKEN:
        raise HTTPException(status_code=503, detail="Control endpoint disabled: CONTROL_TOKEN not set on server.")
    if not x_control_token or x_control_token != CONTROL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Control-Token.")


@app.get("/control/status")
def control_status(x_control_token: Optional[str] = Header(default=None)):
    _require_control_token(x_control_token)
    return {
        "mock_slow_latency": latency_state.snapshot(),
        "hang_seconds_default": HANG_SECONDS_DEFAULT,
    }


@app.post("/control/latency")
def control_latency(update: LatencyUpdate, x_control_token: Optional[str] = Header(default=None)):
    _require_control_token(x_control_token)
    with latency_state.lock:
        latency_state.mode = update.mode
        latency_state.fixed_ms = update.fixed_ms or 0
        latency_state.jitter_min_ms = update.jitter_min_ms or 0
        latency_state.jitter_max_ms = update.jitter_max_ms or 0
    logger.info("mock-slow latency updated: %s", latency_state.snapshot())
    return {"ok": True, "mock_slow_latency": latency_state.snapshot()}


class LatencyRangeUpdate(BaseModel):
    # Seconds, not ms - readability for QA runs that dial in minute-scale delays
    # (e.g. reproducing the 3-4+ minute provider-slowness incident) without
    # doing ms arithmetic by hand. Thin wrapper over the existing jitter mode.
    min_seconds: float = Field(..., ge=0, description="Lower bound of the random delay, in seconds")
    max_seconds: float = Field(..., ge=0, description="Upper bound of the random delay, in seconds")


@app.post("/control/latency-range")
def control_latency_range(
    update: LatencyRangeUpdate, x_control_token: Optional[str] = Header(default=None)
):
    _require_control_token(x_control_token)
    if update.max_seconds < update.min_seconds:
        raise HTTPException(
            status_code=422, detail="max_seconds must be >= min_seconds."
        )
    with latency_state.lock:
        latency_state.mode = "jitter"
        latency_state.jitter_min_ms = int(update.min_seconds * 1000)
        latency_state.jitter_max_ms = int(update.max_seconds * 1000)
    logger.info("mock-slow latency range updated: %s", latency_state.snapshot())
    return {"ok": True, "mock_slow_latency": latency_state.snapshot()}


@app.get("/healthz")
def healthz():
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
