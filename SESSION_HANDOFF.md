# cbllmgateway slow-provider QA test — session handoff

Context for a fresh Claude Code session to pick up and execute. Written after a Cowork
investigation session that read the actual `lp-cbllmgateway-app` source (not just docs)
to verify feasibility before building anything.

## Problem being investigated

Production `cbllmgateway`: when an upstream LLM provider is slow (3-4+ minutes), in-flight
requests pile up on the pod that received them, and the pod eventually loses the capacity
to make *any* outbound request — including calls unrelated to the slow provider. Can't be
reproduced in QA today because QA doesn't control third-party provider latency. Goal: a
controllable "slow provider" double, wired into cbllmgateway the same way a real provider
would be, so the pileup behavior can be triggered on demand and observed.

Provider allowlist for this test (organizational constraint): **Amazon, Anthropic, Cohere,
Google, IBM, Microsoft, OpenAI** only — no third-party mock-LLM hosting services (Groq,
OpenRouter, HF Inference Providers were considered and rejected for this reason).

## Key architecture findings (cite file:line when picking this back up)

- Flask + gevent (greenlets). Every provider call funnels through one shared helper:
  `src/error_handlers/openai_error_handler.py:15` `process_langchain_with_error_handling`,
  which wraps the SDK call in `gevent.Timeout(timeout_value)`.
- **That wrapper's comment documents this exact bug already happened**, for Google
  specifically: "observed with google-genai's requests-based transport hanging
  indefinitely under gevent, with no exception ever raised... starves this request's slot
  ... forever, since abstract_llm_gateway's finally block never runs." Cohere has its own
  duplicate hand-rolled copy of the same backstop (`cohere_llm_gateway_chats.py:51`),
  suggesting it hit something similar. **This makes Google the provider with direct
  historical evidence of the exact failure mode being investigated** — more valuable to
  target than a generic "any slow provider" test.
- `COMPLETIONS_REQUEST_TIMEOUT`: `.env.sample` sets it to `40000`, while
  `tests/config/test_config.env` uses `"10"`. That 4000x gap looks like a possible
  ms-vs-seconds unit mismatch (gevent.Timeout and every SDK `timeout=` kwarg here expect
  seconds). **Not yet confirmed against the real deployed QA value** — worth checking
  before assuming the timeout backstop is even set to something sane. If it's actually
  ~40000 seconds (~11 hours) instead of 40 seconds in QA, that alone could explain
  multi-minute pileups regardless of which provider is slow.
- Per-account provider config lives in Cassandra (`LlmGatewayProviderConfigModelNew`,
  `dal/cassandra/models/llm_provider_data_model.py`), keyed by
  `(account_id, provider_name, subscription_name)`, value is a JSON blob
  (`subscription_data`) decrypted/read per request via
  `gateway/llm/config/provider_config_manager.py:get_subscription_data`.
- A second, request-time override mechanism: `request_config["additional_config"]`, which
  most provider `preprocess()` methods merge straight into the LangChain client
  constructor kwargs. It is **not** client-supplied — it's injected server-side from the
  LaunchDarkly flag `llmgateway.additionalLlmConfig` (`constants/ld_features.py:43`),
  resolved by `llm_gateway_controller.py:681` `_get_additional_config_from_ld`, keyed
  `account_id` → `provider_name` (lowercased) → `model_name` (lowercased) →
  `client_type` (falls back to `"default"`).

## Per-provider feasibility (redirect to a mock/proxy, no cbllmgateway code changes)

| Provider | Mechanism | Status |
|---|---|---|
| **OpenAI** | `additional_config.base_url` → merged into `ChatOpenAI(...)` kwargs (`openai_chats_llm_gateway.py`, inherits `preprocess()` from `openai_completions_llm_gateway.py:89-96`). Confirmed in installed SDK: `langchain_openai/chat_models/base.py:664` — `openai_api_base: str \| None = Field(default=None, alias="base_url")`, docstring explicitly says "Only specify if using a proxy or service emulator." | **Ready now.** Mock already speaks this exact schema. |
| **Google** | Same `additional_config.base_url` mechanism (`google_completions_llm_gateway.py:150-168`, inherited by chats). Confirmed in installed SDK: `langchain_google_genai/chat_models.py:2558-2591` — `base_url` is honored even when `vertexai=True` (built into `HttpOptions(base_url=...)` passed to the genai `Client` regardless of backend). | **Feasible, mock needs a new route.** cbllmgateway's Google path speaks Vertex/Gemini-native REST (`generateContent`/`streamGenerateContent`, `contents`/`generationConfig` request shape, `candidates[].content.parts[].text` response shape) — **not** OpenAI's schema. The mock's OpenAI-shaped `/v1/chat/completions` route does not cover this; a Gemini-shaped route needs to be added (reuse the same latency-control mechanism). Avoid setting subscription `location` to exactly `"us"`/`"eu"`/`"global"` — those trigger an auto-set `api_endpoint` key in `_resolve_vertex_api_endpoint()` (`google_completions_llm_gateway.py:32`) that isn't a conflict but is an extra moving part; a normal regional value like `"us-central1"` avoids it. |
| **Microsoft** | `endpoint` read straight from the Cassandra subscription row (`microsoft_azure_chats_llm_gateway.py:109`, `AzureAIChatCompletionsModel(endpoint=self.llm_subscription_data["endpoint"], ...)`). Does **not** use the `additional_config` mechanism at all. | Feasible via subscription row alone — no LD flag needed. Not pursued this session (OpenAI/Google chosen instead), but available as a fallback if either stalls. |
| **IBM watsonx** | Same pattern: `url` from subscription row (`ibm_watson_x_chats_llm_gateway.py:50`). | Feasible via subscription row alone. Not pursued. |
| **Amazon Bedrock** | boto3 client built in `get_boto_client()` (`amazon_llm_gateway.py:56-67`) reads only `service_name`/`aws_access_key_id`/`aws_secret_access_key`/`region_name` — **no endpoint override anywhere**, and `additional_config` here flows into `BedrockLLM`'s `model_kwargs` (model params), not the boto3 client. | **Not feasible without a code change.** SigV4 request signing also rules out a naive host-swapping proxy. Would need `get_boto_client()` extended to accept an `endpoint_url` — a small, low-risk change if the dev team wants to add it, but out of scope for a QA-only/no-code-change test. |
| **Anthropic / Cohere** | Both merge `additional_config` into their LangChain client kwargs (`anthropic_chats_llm_gateway.py`, `cohere_llm_gateway_completions.py`). Whether `ChatAnthropic`/`ChatCohere` accept a `base_url`-style override **was not verified against the installed SDK** the way OpenAI/Google were. | Likely feasible, unconfirmed. If OpenAI/Google both stall, check `langchain_anthropic`/`langchain_cohere` in `.venv/lib/python3.11/site-packages/` the same way (grep for `base_url`, `alias=`). |

## Decision made this session

Pursue **OpenAI first** (zero mock changes, ready to run), with **Google as a second target**
specifically because it's the one with documented evidence of the actual transport-hang bug.
Amazon ruled out without a code change; Microsoft/IBM kept as fallback options if needed.

## Deliverables already created

All in `/Users/mohamedthabet/Documents/LivePerson/code/clearn-repos/cbllmgateway-slow-provider-test/`:

- `mock_slow_llm_service/app.py` — FastAPI mock. OpenAI-compatible `/v1/chat/completions`.
  Three model profiles from one deployment: `mock-fast` (~150-300ms, canary),
  `mock-slow` (latency controlled at runtime via `POST /control/latency` — fixed delay,
  jitter, or hang), `mock-hang` (always ~10min, tests the extreme case). Control endpoints
  require `X-Control-Token` matched against `CONTROL_TOKEN` env var (disabled if unset —
  no default open control plane). Uses `asyncio.sleep` + thread-pool generation so the mock
  itself can hold many concurrent slow/hung requests without becoming the bottleneck.
  Backed by TinyLlama-1.1B-Chat (Apache-2.0) with an automatic canned-response fallback if
  the model can't load.
- `mock_slow_llm_service/requirements.txt`, `Dockerfile` — pins fastapi/uvicorn/
  transformers/torch; Dockerfile pre-downloads model weights at build time (falls back
  gracefully if no internet at build time).
- `cbllmgateway_slow_provider_test.js` — k6 script. Two concurrent lanes against
  cbllmgateway itself (not the mock directly): constant-rate `fast_canary`
  (`model=mock-fast`) as a control/baseline, and ramping-VUs `slow_incident`
  (`model=mock-slow`), staggered 30s later. `setup()`/`teardown()` arm/disarm the mock's
  induced latency via its control endpoint. Thresholds on the fast lane's p95/p99 —
  if those degrade once the slow lane saturates, that's the pileup hypothesis confirmed.
  Requires env vars `GATEWAY_CHAT_URL`, `GATEWAY_AUTH_HEADER`, `MOCK_CONTROL_URL`,
  `MOCK_CONTROL_TOKEN` (no secrets hardcoded in the script).
- `cbllmgateway_slow_provider_test_plan.md` — full test plan: hypothesis, scenarios
  (baseline → ramp-up → saturation hold → recovery → optional hang case), metrics to
  capture (k6 + pod-level + app-level + k8s + mock's own `x_mock_elapsed_ms`),
  success/failure criteria, risks/rollback.
  **A `.docx` version of the same content was generated but could not be copied into
  this folder** — Claude's file-write tools can only write text into this connected
  folder, and a `.docx` is a binary zip archive, not text, so it can't be placed here
  the same way. It was shared earlier in the chat session as a downloadable file card;
  if you still need the `.docx` specifically, either re-request it from that chat
  session or ask the next session to regenerate it from this `.md` (the `docx` skill
  can do this) and hand it back the same way.
- `qa_setup_openai.md` — exact steps to wire OpenAI to the mock:
  1. Deploy the mock, confirm `/healthz`.
  2. Create the QA Cassandra subscription **via the real admin API**, not a raw DB
     insert — the `api_key` field is encrypted server-side by cbllmgateway's own
     encryption sidecar (`admin_controller.py:_encrypt_keys_in_subscription`), which
     can't be replicated outside the running app. Exact call:
     `POST /api/v2/accounts/<QA_ACCOUNT_ID>/llm-providers/openai/llm-subscriptions`
     (route: `admin_route_v2.py:31`, registered under `url_prefix="/api/v2/accounts/<account_id>"`
     in `server.py:124-127`), bearer token scoped to that account
     (`auth_decorators.py:93-94` checks the URL's `account_id` against the token's
     `accountId` claim), body `{"provider_name": "openai", "subscription_name": "...",
     "subscription_data": {"api_key": "<any placeholder — mock doesn't validate it>"}}`.
     Schema: `subscription_name` must match `^[a-zA-Z0-9\-_]+$`
     (`llm_provider_management_schemas.py:16`); `api_key` has no format validation for
     OpenAI specifically.
  3. Add the LaunchDarkly flag `llmgateway.additionalLlmConfig` (targeted at the QA
     account_id) with value
     `{"openai": {"<model_name>": {"default": {"base_url": "https://<mock-host>/v1"}}}}`.
     **Include the `/v1` suffix** — the OpenAI SDK appends `/chat/completions` on top.
  4. **Open item, not yet resolved:** the LD override only injects `base_url`, not the
     `model` value cbllmgateway sends — but the mock's routing (`mock-fast`/`mock-slow`/
     `mock-hang`) keys off exactly that `model` field. Simplest fix: name the test model
     literally `mock-slow` in `request_config.model_name`. Alternative if that's not
     acceptable: adjust the mock to route on something else (e.g. always use
     `mock-slow` behavior for a dedicated `base_url`/host, since a real deployment would
     only ever be pointed at by this one test model anyway) — flagged for the next
     session to decide.
- `docker-compose.override.yaml` — **functional copy** lives in
  `/Users/mohamedthabet/Documents/LivePerson/code/lp-cbllmgateway-app/` (docker compose
  only auto-loads an override file from the same directory as `docker-compose.yaml`, so
  it has to be there to actually take effect). A **reference-only copy** is also kept in
  *this* folder (`cbllmgateway-slow-provider-test/docker-compose.override.yaml`) purely so
  everything related to this test is visible in one place — the two are not symlinked, so
  if either is edited, copy the change to the other by hand.
  Adds a `mock-slow-llm` service to the existing `docker-compose.yaml` (which defines
  `llmgateway` + `cblptools` sidecar + `llmgw-lp-dpop-api`, all on Docker's default bridge
  network via `links:`), built from this folder's `mock_slow_llm_service/` (absolute path
  in `build:`, not duplicated into the `lp-cbllmgateway-app` repo). Requires
  `export MOCK_CONTROL_TOKEN=<generate one>` before `docker compose up` — not hardcoded.
  Reachable from inside `llmgateway` as `http://mock-slow-llm:8080/v1`; from the host Mac
  as `http://localhost:8090/v1`.
  **Not yet validated** — run `docker compose config` from `lp-cbllmgateway-app/` to
  sanity-check the merge before `up` (this session's sandbox couldn't get shell access
  into either connected repo folder to verify it directly — both reported as
  unmountable). Note: the functional copy was written once, reported as saved, but was
  actually missing on disk when checked in a later turn of this same session and had to
  be rewritten (confirmed present the second time via a follow-up directory listing) —
  worth confirming with `ls` / `git status` that both copies are really there before
  relying on them, since the same silent-failure could recur.
- `cbllmgateway_slow_provider_test_plan.docx` — **could not be added to this folder.**
  It's a binary zip file, and Claude's file-write tools in that session could only write
  text into this connected folder, so any attempt to copy it in here would silently fail
  the same way the override file initially did. It was shared in the chat as a downloadable
  file card instead; if it's still needed in this folder, either save it from that card
  manually (drag in Finder / Save As) or regenerate it from `cbllmgateway_slow_provider_test_plan.md`
  using the `docx` skill and hand it back as a file card again — a fresh Claude Code session
  will hit the identical binary-write limitation if it tries to write the `.docx` directly
  into this folder.

## Important context for local runs

Running cbllmgateway "locally" (either bare `flask run` or `docker-compose up`) does
**not** isolate it from real infrastructure: neither `.env.sample` nor
`docker-compose.yaml` define local Cassandra/Kafka/Redis/LaunchDarkly — the app points at
the same shared QA backends either way. So the subscription + LD flag setup above applies
identically whether cbllmgateway itself runs on a laptop or in the QA cluster.

**Flag for whoever owns this repo, unrelated to this test:** `.env.sample` contains what
looks like a live Cassandra password and real internal QA IPs checked into a *sample*
file. Not touched or repeated here — recommend rotating/redacting independent of this work.

## Suggested next steps (in order)

1. Confirm the actual deployed `COMPLETIONS_REQUEST_TIMEOUT` value/unit in QA — resolve
   the ms-vs-seconds question before running anything, since it changes what "3-4 minutes"
   even means relative to the existing gevent.Timeout backstop.
2. Resolve the model-name-vs-base_url mismatch (see "Open item" above) — decide the
   convention before wiring up the LD flag.
3. `docker compose config` to validate `docker-compose.override.yaml`'s merge, then
   `docker compose up --build` (needs real internet access for the TinyLlama download —
   confirm the machine running this isn't sandboxed).
4. Get a QA account_id + LP bearer token scoped to it; call the admin API in
   `qa_setup_openai.md` step 2 to create the subscription.
5. Set the LaunchDarkly flag per `qa_setup_openai.md` step 3.
6. Smoke-test directly against cbllmgateway's real `/chats` endpoint with
   `model_name` set to match step 2's convention — confirm it actually reaches the mock
   (check the mock's logs / `x_mock_elapsed_ms` in the response) before running the full
   k6 script.
7. Run `cbllmgateway_slow_provider_test.js`, capture results against the plan's success
   criteria (test plan doc, section 8).
8. If pursuing Google too: build the Gemini-shaped mock route (see feasibility table),
   then repeat steps 4-7 with `provider_name="google"`.
9. If Amazon is wanted later: raise the `get_boto_client()` `endpoint_url` change with the
   dev team as a separate, small PR — not something to route around in QA.
