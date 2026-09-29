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

Gemini (Google Vertex AI) support
---------------------------------
The same latency/error controls also drive a Vertex `generateContent` REST
endpoint, so Gemini gateways (`ChatVertexAI` / `VertexAI` with
api_transport="rest") can be pointed at this service by setting `api_endpoint`
through the `llmgateway.additionalLlmConfig` LaunchDarkly flag. The model name
is taken from the URL path (mock-fast / mock-slow / mock-normal / mock-hang).

  - POST /v1beta1/projects/{p}/locations/{l}/publishers/google/models/{m}:generateContent
    (also served under /v1/...). Errors use Google's error envelope.
  - Auth: with service-account credentials the Google SDK does NOT call a token
    endpoint; it sends a self-signed JWT (`iss` = the service account's
    client_email) as the bearer. Once CONTROL_TOKEN is set, the Vertex endpoint
    therefore requires either that JWT with `iss` == MOCK_GOOGLE_CLIENT_EMAIL, or
    the access token issued by /token. The JWT signature is NOT verified - this is
    a convenience filter against stray callers, not real authentication (the
    endpoint only returns canned text; the /control/* endpoints stay protected by
    CONTROL_TOKEN). Set MOCK_GOOGLE_CLIENT_EMAIL to the dummy service account's
    client_email.
  - POST /token: safety net standing in for Google's OAuth token endpoint. Set the
    dummy service account's `token_uri` to it so that, if any code path ever does
    an OAuth exchange, it hits this mock and never real Google.
  - The Vertex endpoint polls for client disconnects while it holds a request, so
    a test can confirm the gateway actually closed the socket at its timeout.
    MOCK_DISCONNECT_POLL_SECONDS (default 1.0) sets the polling interval.
  - Per-model request/response/disconnect counters: GET /control/status,
    POST /control/stats/reset.

This service is for QA use only. Do not point production traffic at it.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import random
import threading
import time
import uuid
from typing import Any, Literal, Optional
from urllib.parse import parse_qs

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("mock-slow-llm")

app = FastAPI(title="mock-slow-llm", version="1.0.0")

CONTROL_TOKEN = os.environ.get("CONTROL_TOKEN")  # unset => control endpoints disabled
HANG_SECONDS_DEFAULT = float(os.environ.get("MOCK_HANG_SECONDS", "600"))  # 10 min
DISCONNECT_POLL_SECONDS = float(os.environ.get("MOCK_DISCONNECT_POLL_SECONDS", "1.0"))
MOCK_GOOGLE_CLIENT_EMAIL = os.environ.get("MOCK_GOOGLE_CLIENT_EMAIL")  # optional gate


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

MODEL_PROFILES = {"mock-fast", "mock-slow", "mock-normal", "mock-hang"}


# ---------------------------------------------------------------------------
# Runtime-controllable error injection (applies to every model, independent
# of latency) - lets a test simulate a provider returning an error status
# (e.g. 429 rate-limit, 503 unavailable) instead of a slow/hung response, to
# exercise the same code paths a real provider outage would: adaptive
# fallback's error-rate tracking, the concurrency guard's own error handling,
# and openai_error_handler.py's status-code parsing.
# ---------------------------------------------------------------------------
class ErrorInjectionState:
    def __init__(self):
        self.lock = threading.Lock()
        self.mode: Literal["off", "always", "rate"] = "off"
        self.status_code: int = 429
        self.probability: float = 1.0  # used when mode == "rate"
        self.error_type: str = "rate_limit_error"
        self.message: str = "Mock injected provider error (rate limit)"

    def snapshot(self):
        with self.lock:
            return {
                "mode": self.mode,
                "status_code": self.status_code,
                "probability": self.probability,
                "error_type": self.error_type,
                "message": self.message,
            }

    def maybe_status_code(self) -> Optional[int]:
        """Returns the status code to return instead of a real response, or
        None to mean 'respond normally'."""
        with self.lock:
            mode, prob, code = self.mode, self.probability, self.status_code
        if mode == "off":
            return None
        if mode == "always":
            return code
        if mode == "rate":
            return code if random.random() < prob else None
        return None


error_state = ErrorInjectionState()


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


def _resolve_delay_seconds(model: str, override_ms: Optional[int]) -> float:
    if override_ms is not None:
        return max(override_ms, 0) / 1000.0

    if model == "mock-fast":
        return random.uniform(0.15, 0.3)

    if model == "mock-hang":
        return HANG_SECONDS_DEFAULT

    # mock-normal is an alias of mock-slow's runtime-controlled latency, used
    # when a separate mock deployment is dedicated to a "normal latency"
    # lane (e.g. a concurrent dual-lane test comparing a slow-provider lane
    # against a healthy one) - it reads the same /control/latency-range
    # state as mock-slow, just under a different model name so cbllmgateway
    # can route it via its own LD/subscription entry.
    if model in ("mock-slow", "mock-normal"):
        secs = latency_state.delay_seconds()
        return HANG_SECONDS_DEFAULT if secs is None else secs

    # Unknown model name: behave like mock-fast rather than failing the test run.
    return random.uniform(0.15, 0.3)


async def _apply_latency(model: str, override_ms: Optional[int]):
    # Uses asyncio.sleep (not time.sleep) so a slow/hung request only "occupies"
    # its own coroutine and does not block the event loop from serving other
    # concurrent requests (e.g. the mock-fast canary lane) within this process.
    # This keeps the mock service itself from becoming a confound in the test -
    # any pileup you observe should come from cbllmgateway, not from this mock.
    await asyncio.sleep(_resolve_delay_seconds(model, override_ms))


def _require_bearer_token(authorization: Optional[str]) -> None:
    # No CONTROL_TOKEN configured => leave this endpoint open, matching prior
    # behavior for a purely local Docker-network deployment. Once a token is
    # set (expected for anything reachable outside localhost), require it.
    if not CONTROL_TOKEN:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401, detail="Missing or malformed Authorization header."
        )
    if authorization[len("Bearer ") :] != CONTROL_TOKEN:
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

    inject_status = error_state.maybe_status_code()
    if inject_status is not None:
        message, error_type = error_state.message, error_state.error_type
        logger.info(
            "model=%s injected_error_status=%d request_id=%s",
            req.model,
            inject_status,
            request_id,
        )
        return JSONResponse(
            status_code=inject_status,
            content={
                "error": {
                    "message": message,
                    "type": error_type,
                    "param": None,
                    "code": error_type,
                }
            },
        )

    prompt = req.messages[-1].content if req.messages else ""
    text = _generate_text(prompt)

    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "model=%s elapsed_ms=%d request_id=%s", req.model, elapsed_ms, request_id
    )

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
# Google Vertex AI (Gemini) endpoints
# ---------------------------------------------------------------------------
_GOOGLE_STATUS_BY_CODE = {
    400: "INVALID_ARGUMENT",
    401: "UNAUTHENTICATED",
    403: "PERMISSION_DENIED",
    404: "NOT_FOUND",
    429: "RESOURCE_EXHAUSTED",
    500: "INTERNAL",
    503: "UNAVAILABLE",
    504: "DEADLINE_EXCEEDED",
}

# Nginx-style "client closed request". The response is never delivered (the
# client is already gone); the code only shows up in the mock's own logs/stats.
_CLIENT_CLOSED_REQUEST = 499


class VertexStats:
    """Counters that let a test assert what the mock actually saw, e.g. how many
    provider calls one gateway request turned into (SDK retries) and whether the
    gateway closed the socket at its timeout (client disconnects)."""

    _MAX_DISCONNECT_SAMPLES = 50

    def __init__(self):
        self.lock = threading.Lock()
        self._clear()

    def _clear(self):
        self.received: dict[str, int] = {}
        self.responses: dict[str, dict[str, int]] = {}
        self.disconnects: dict[str, dict[str, Any]] = {}
        self.in_flight = 0
        self.peak_in_flight = 0
        self.token_requests = 0
        self.token_rejected = 0

    def reset(self):
        with self.lock:
            self._clear()

    def begin(self, model: str):
        with self.lock:
            self.received[model] = self.received.get(model, 0) + 1
            self.in_flight += 1
            self.peak_in_flight = max(self.peak_in_flight, self.in_flight)

    def end(self):
        with self.lock:
            self.in_flight -= 1

    def record_response(self, model: str, status_code: int):
        with self.lock:
            by_status = self.responses.setdefault(model, {})
            by_status[str(status_code)] = by_status.get(str(status_code), 0) + 1

    def record_disconnect(self, model: str, elapsed_seconds: float):
        with self.lock:
            entry = self.disconnects.setdefault(
                model, {"count": 0, "elapsed_seconds_last": []}
            )
            entry["count"] += 1
            samples = entry["elapsed_seconds_last"]
            samples.append(round(elapsed_seconds, 2))
            del samples[: -self._MAX_DISCONNECT_SAMPLES]

    def record_token_request(self, rejected: bool):
        with self.lock:
            self.token_requests += 1
            if rejected:
                self.token_rejected += 1

    def snapshot(self):
        with self.lock:
            return {
                "received_by_model": dict(self.received),
                "responses_by_model_and_status": {
                    m: dict(s) for m, s in self.responses.items()
                },
                "client_disconnects_by_model": {
                    m: {
                        "count": d["count"],
                        "elapsed_seconds_last": list(d["elapsed_seconds_last"]),
                    }
                    for m, d in self.disconnects.items()
                },
                "in_flight": self.in_flight,
                "peak_in_flight": self.peak_in_flight,
                "token_requests": self.token_requests,
                "token_rejected": self.token_rejected,
            }


vertex_stats = VertexStats()



def _google_error_response(status_code: int, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": status_code,
                "message": message,
                "status": _GOOGLE_STATUS_BY_CODE.get(status_code, "UNKNOWN"),
            }
        },
    )


def _mock_google_access_token() -> str:
    # Derived from CONTROL_TOKEN rather than randomly generated so it is
    # stateless (still valid after a mock restart) and never exposes the control
    # token itself.
    digest = hmac.new(
        (CONTROL_TOKEN or "").encode(), b"mock-google-access-token", hashlib.sha256
    ).hexdigest()
    return f"mock-{digest[:40]}"


def _is_authorized_google_bearer(token: str) -> bool:
    if hmac.compare_digest(token, _mock_google_access_token()):
        return True
    return bool(MOCK_GOOGLE_CLIENT_EMAIL) and (
        _jwt_issuer(token) == MOCK_GOOGLE_CLIENT_EMAIL
    )


def _google_auth_error(authorization: Optional[str]) -> Optional[JSONResponse]:
    # Same open-when-unconfigured rule as _require_bearer_token: no CONTROL_TOKEN
    # means a purely local deployment, so leave the endpoint open.
    if not CONTROL_TOKEN:
        return None
    if not authorization or not authorization.startswith("Bearer "):
        return _google_error_response(
            401, "Request had invalid authentication credentials."
        )
    if not _is_authorized_google_bearer(authorization[len("Bearer ") :]):
        return _google_error_response(
            401, "Request had invalid authentication credentials."
        )
    return None


def _jwt_issuer(assertion: str) -> Optional[str]:
    # Unverified decode of the JWT payload - only used to gate which
    # service-account email is accepted, not as real authentication.
    try:
        payload_b64 = assertion.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        return json.loads(base64.urlsafe_b64decode(payload_b64)).get("iss")
    except Exception:
        return None


def _extract_vertex_prompt(body: dict) -> str:
    contents = body.get("contents") or []
    if not contents or not isinstance(contents[-1], dict):
        return ""
    parts = contents[-1].get("parts") or []
    return " ".join(
        p.get("text", "") for p in parts if isinstance(p, dict) and p.get("text")
    )


async def _wait_or_disconnect(request: Request, seconds: float) -> bool:
    """Waits up to `seconds`. Returns True if the client disconnected first."""
    deadline = time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        if await request.is_disconnected():
            return True
        await asyncio.sleep(min(DISCONNECT_POLL_SECONDS, remaining))


@app.post(
    "/v1beta1/projects/{project}/locations/{location}/publishers/google/models/{model}:generateContent"
)
@app.post(
    "/v1/projects/{project}/locations/{location}/publishers/google/models/{model}:generateContent"
)
async def vertex_generate_content(
    project: str,
    location: str,
    model: str,
    request: Request,
    authorization: Optional[str] = Header(default=None),
):
    auth_error = _google_auth_error(authorization)
    if auth_error is not None:
        return auth_error

    started = time.monotonic()
    vertex_stats.begin(model)
    try:
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}
        if model not in MODEL_PROFILES:
            logger.info("Unrecognized model '%s' - treating as mock-fast.", model)

        disconnected = await _wait_or_disconnect(
            request, _resolve_delay_seconds(model, None)
        )
        elapsed = time.monotonic() - started
        if disconnected:
            vertex_stats.record_disconnect(model, elapsed)
            logger.info(
                "vertex model=%s client_disconnected elapsed_s=%.2f", model, elapsed
            )
            return Response(status_code=_CLIENT_CLOSED_REQUEST)

        inject_status = error_state.maybe_status_code()
        if inject_status is not None:
            vertex_stats.record_response(model, inject_status)
            logger.info(
                "vertex model=%s injected_error_status=%d elapsed_s=%.2f",
                model,
                inject_status,
                elapsed,
            )
            return _google_error_response(inject_status, error_state.message)

        prompt = _extract_vertex_prompt(body)
        text = _generate_text(prompt)
        vertex_stats.record_response(model, 200)
        logger.info("vertex model=%s elapsed_s=%.2f", model, elapsed)
        prompt_tokens = len(prompt.split())
        completion_tokens = len(text.split())
        return {
            "candidates": [
                {
                    "content": {"role": "model", "parts": [{"text": text}]},
                    "finishReason": "STOP",
                    "index": 0,
                }
            ],
            "usageMetadata": {
                "promptTokenCount": prompt_tokens,
                "candidatesTokenCount": completion_tokens,
                "totalTokenCount": prompt_tokens + completion_tokens,
            },
            "modelVersion": model,
        }
    finally:
        vertex_stats.end()


@app.post("/token")
async def google_oauth_token(request: Request):
    # Stand-in for https://oauth2.googleapis.com/token, which the Google auth
    # library calls (JWT-bearer grant, form-encoded) to exchange a service
    # account's signed assertion for an access token.
    form = parse_qs((await request.body()).decode("utf-8", "replace"))
    assertion = (form.get("assertion") or [""])[0]

    if MOCK_GOOGLE_CLIENT_EMAIL and _jwt_issuer(assertion) != MOCK_GOOGLE_CLIENT_EMAIL:
        vertex_stats.record_token_request(rejected=True)
        return JSONResponse(
            status_code=400,
            content={
                "error": "invalid_grant",
                "error_description": "Unexpected JWT issuer.",
            },
        )

    vertex_stats.record_token_request(rejected=False)
    return {
        "access_token": _mock_google_access_token(),
        "expires_in": 3600,
        "token_type": "Bearer",
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
        raise HTTPException(
            status_code=503,
            detail="Control endpoint disabled: CONTROL_TOKEN not set on server.",
        )
    if not x_control_token or x_control_token != CONTROL_TOKEN:
        raise HTTPException(
            status_code=401, detail="Invalid or missing X-Control-Token."
        )


@app.get("/control/status")
def control_status(x_control_token: Optional[str] = Header(default=None)):
    _require_control_token(x_control_token)
    return {
        "mock_slow_latency": latency_state.snapshot(),
        "hang_seconds_default": HANG_SECONDS_DEFAULT,
        "error_injection": error_state.snapshot(),
        "vertex_stats": vertex_stats.snapshot(),
    }


@app.post("/control/stats/reset")
def control_stats_reset(x_control_token: Optional[str] = Header(default=None)):
    _require_control_token(x_control_token)
    vertex_stats.reset()
    logger.info("vertex stats reset")
    return {"ok": True, "vertex_stats": vertex_stats.snapshot()}



@app.post("/control/latency")
def control_latency(
    update: LatencyUpdate, x_control_token: Optional[str] = Header(default=None)
):
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
    min_seconds: float = Field(
        ..., ge=0, description="Lower bound of the random delay, in seconds"
    )
    max_seconds: float = Field(
        ..., ge=0, description="Upper bound of the random delay, in seconds"
    )


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


class ErrorInjectionUpdate(BaseModel):
    mode: Literal["off", "always", "rate"]
    status_code: int = Field(429, ge=400, le=599)
    probability: float = Field(1.0, ge=0.0, le=1.0)
    error_type: str = "rate_limit_error"
    message: str = "Mock injected provider error"


@app.post("/control/error")
def control_error(
    update: ErrorInjectionUpdate, x_control_token: Optional[str] = Header(default=None)
):
    _require_control_token(x_control_token)
    with error_state.lock:
        error_state.mode = update.mode
        error_state.status_code = update.status_code
        error_state.probability = update.probability
        error_state.error_type = update.error_type
        error_state.message = update.message
    logger.info("error injection updated: %s", error_state.snapshot())
    return {"ok": True, "error_injection": error_state.snapshot()}


@app.get("/healthz")
def healthz():
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
