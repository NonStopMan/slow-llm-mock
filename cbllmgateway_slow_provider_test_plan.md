# cbllmgateway Slow-Provider Performance Test Plan

*QA verification of pod-level request pileup under upstream LLM provider latency*
Prepared for: LivePerson Engineering | Author: mthabet@liveperson.com | Date: 2026-08-28

## 1. Background and Problem Statement

cbllmgateway is currently exhibiting the following behavior in production: when an upstream LLM provider becomes slow (observed delays of 3-4+ minutes), the requests waiting on that provider are not bounded or shed - they remain in flight on the pod that received them. As more slow requests pile up, the affected pod eventually loses the capacity to make any new outbound request at all, or those requests become very slow, even when the destination has nothing to do with the original slow provider.

This cannot currently be reproduced in QA because QA does not control third-party provider latency, and the real incident is intermittent and provider-side. The approach below removes that dependency by substituting a small, self-hosted LLM service whose response latency we can dial up on demand, so the same pileup behavior can be triggered deterministically and repeatedly.

## 2. Objective and Hypothesis

**Objective:** confirm, in QA, whether sustained upstream latency on a subset of requests causes cbllmgateway to exhaust pod-level resources (connection pool, thread pool, event loop, file descriptors, or similar) such that unrelated, otherwise-healthy requests on the same pod also degrade or stall.

**Hypothesis:** once the number of concurrent in-flight requests waiting on a slow provider crosses some threshold, previously-healthy request latency on the same pod will increase sharply (queueing/timeouts), and the pod will not recover until the slow requests drain or are forcibly cleared.

## 3. Test Approach Overview

- Stand up a small, self-hosted mock LLM service outside the QA cluster, reachable by URL, that exposes an OpenAI-compatible chat-completions endpoint and can hold responses for a controllable, adjustable delay (including an indefinite "hang" mode).
- Point a QA-only provider configuration in cbllmgateway at this mock service, using two logical model names so one service can act as both the "healthy" canary provider (mock-fast) and the "incident" provider (mock-slow) at the same time.
- Use k6 to drive two concurrent traffic lanes against cbllmgateway itself: a low, constant-rate fast lane (canary) and a ramping-concurrency slow lane. If the fast lane's latency/error rate degrades once the slow lane saturates, that confirms cross-request resource exhaustion on the pod.
- Capture pod- and application-level metrics throughout, not just client-observed latency, so the root cause (connection pool, thread pool, event loop, etc.) can be identified rather than just the symptom.

## 4. Mock Service Design

Delivered as `mock_slow_llm_service/` (app.py, requirements.txt, Dockerfile). Key properties:

- Built on FastAPI, backed by a small real open-weights model (default: TinyLlama-1.1B-Chat, Apache-2.0 licensed) so response bodies and connection behavior resemble a real provider; falls back automatically to canned text if the model can't be loaded, so latency-injection testing is unaffected either way.
- Exposes `/v1/chat/completions` (OpenAI-style). If cbllmgateway's QA provider config expects a different upstream schema (Azure OpenAI, Bedrock, Anthropic native, etc.), the request/response mapping in app.py should be adjusted accordingly - confirm the expected contract with the cbllmgateway team before deployment.
- Three model profiles from one deployment: `mock-fast` (always ~150-300ms, canary lane), `mock-slow` (delay controlled at runtime via `POST /control/latency` - fixed delay, jitter range, or hang), and `mock-hang` (always sleeps far past any sane timeout, default 10 minutes, for testing the extreme case).
- The control endpoints require a shared-secret header (`X-Control-Token`) matched against a `CONTROL_TOKEN` environment variable set at deploy time; if that variable isn't set, the control endpoints are disabled rather than left open. No token is hardcoded anywhere in the code.
- Latency is applied with `asyncio.sleep` (not a blocking sleep) and generation runs in a thread pool, so the mock service itself can hold many concurrent slow/hung requests without becoming the bottleneck - any pileup observed should be attributable to cbllmgateway, not to the mock.

## 5. Environment Setup

### 5.1 Deploy the mock service

- Build and run the container from `mock_slow_llm_service/` on a small host reachable from the QA cluster by URL (per team decision: external host rather than in-cluster). Provide `CONTROL_TOKEN` as a generated secret via environment/secret manager, never hardcoded.
- Confirm reachability and TLS from the QA cluster's egress path; restrict inbound access to QA network ranges only (this is a test double, not something that should be internet-reachable).
- Smoke-test directly against the mock (bypassing cbllmgateway) with curl for mock-fast, mock-slow (after arming latency), and /healthz before wiring up cbllmgateway.

### 5.2 Configure cbllmgateway (QA only)

- Add a QA-scoped provider/model configuration pointing model names `mock-fast` and `mock-slow` (and optionally `mock-hang`) at the mock service's URL and `/v1/chat/completions` path.
- Use a dedicated QA tenant/account for this test so it cannot be confused with or affect other QA activity or real provider configurations.

### 5.3 Prepare the load test

- Install k6 on the machine/CI runner that will drive the test.
- Set the required environment variables documented at the top of `cbllmgateway_slow_provider_test.js`: `GATEWAY_CHAT_URL`, `GATEWAY_AUTH_HEADER`, `MOCK_CONTROL_URL`, `MOCK_CONTROL_TOKEN` (all supplied at run time, never committed).

## 6. Test Scenarios

| Phase | What happens | Configuration | What we're watching for |
|---|---|---|---|
| 1. Baseline | Fast-lane canary only, no slow traffic. Confirms normal gateway latency and establishes a clean reference. | mock-slow disarmed (mode=off). Fast lane at low constant rate (e.g. 2 req/s). | Fast lane p95/p99 latency and error rate under normal conditions. |
| 2. Ramp-up | Slow lane starts and ramps concurrency (0 -> 25% -> 60% of target) against mock-slow, held at a fixed induced delay matching the real incident (~3-4 min). Fast lane keeps running throughout. | mock-slow armed via `/control/latency` (mode=fixed, fixed_ms ~200000-240000). Slow lane ramping VUs. | Whether fast-lane latency/error rate starts moving as slow-lane concurrency increases - the earliest signal of resource pileup. |
| 3. Saturation hold | Slow lane held at peak concurrency for several minutes so in-flight slow requests accumulate on the pod. | Slow lane VUs held at target (e.g. 40) for 3+ min. | Pod-level signals: outbound connection pool/thread pool utilization, event-loop lag or worker saturation, memory/FD growth, and whether fast lane requests start timing out or queueing. |
| 4. Recovery | Slow lane ramps back down to 0 and mock-slow is disarmed. | Slow lane VUs -> 0. `/control/latency` mode=off. | Whether the pod recovers on its own once slow requests drain, or whether it stays degraded (stuck connections, leaked resources) after the incident ends. |
| 5. Hang case (optional) | A small number of requests hit model=mock-hang, which never returns within any sane timeout. | 1-2 VUs sending to mock-hang with a long client-side timeout. | Whether cbllmgateway has (or lacks) a request timeout/circuit breaker for a provider that never responds at all, and whether such requests ever get cleaned up. |

The provided k6 script (`cbllmgateway_slow_provider_test.js`) implements phases 1-4 as a single run: a constant-arrival-rate "fast_canary" scenario runs for the full duration, while a "slow_incident" ramping-VUs scenario staggers in 30 seconds later. `setup()` arms mock-slow's induced delay before load starts; `teardown()` disarms it afterward. Phase 5 (hang case) is intentionally left as a separate, manual/small-scale run against `model=mock-hang`, since it is exploratory and could keep connections open for a long time.

## 7. Metrics and Observability

| Layer | Signals to capture |
|---|---|
| Load test (k6) | Per-lane request duration (p50/p95/p99), error/timeout rate, throughput. Exported as JSON/CSV for the test report; `fast_lane_duration` and `slow_lane_duration` trends are emitted by the provided script. |
| cbllmgateway pod | CPU, memory, open file descriptors, outbound HTTP connection pool usage, thread pool/event-loop queue depth (whatever the runtime exposes - JVM thread dumps, Node event-loop lag, etc.), GC pauses if JVM-based. |
| cbllmgateway application | Request queue depth/in-flight request count per pod, per-provider timeout configuration, retry counts, circuit-breaker state if one exists. |
| Kubernetes | Pod readiness/liveness probe results (does the pod get marked unhealthy and restarted, masking the issue?), restarts, OOMKilled events, HPA scaling activity. |
| Mock service | `x_mock_elapsed_ms` in each response body (confirms the mock actually held the connection for the intended duration) and `/control/status`. |

Recommend collecting these from cbllmgateway's existing QA dashboards/APM rather than building new tooling - the specific metric names/dashboards should be filled in by whoever owns cbllmgateway's runtime observability, since that wasn't available to this plan.

## 8. Success / Failure Criteria

- **Hypothesis confirmed if:** fast-lane p95/p99 latency or error rate increases materially (e.g. p95 crosses the 2s threshold defined in the k6 script, or errors/timeouts appear) once the slow lane reaches a certain concurrency, and recovers only after the slow lane drains.
- **Hypothesis not confirmed (for this mechanism) if:** fast-lane latency and error rate stay flat throughout ramp-up and saturation, regardless of slow-lane concurrency - in that case the pileup reported in production likely has a different or additional root cause (e.g. per-tenant limits, a specific code path, or a downstream dependency not exercised here).
- Either outcome, record the concurrency threshold (if any) at which degradation begins, whether the pod self-recovers after the slow lane drains, and which pod-level resource (connection pool, thread pool, event loop, FD limit) is exhausted first.

## 9. Risks, Safety, and Rollback

- Run only against QA, using a dedicated QA tenant/provider config - never point this mock at any production or shared provider configuration.
- This test intentionally tries to saturate a pod; coordinate the run window with whoever else uses the QA cbllmgateway environment so it doesn't disrupt unrelated QA activity.
- Keep the mock-hang scenario small-scale and time-boxed; if cbllmgateway has no timeout for a provider that never responds, that scenario can hold connections open indefinitely and should be stopped manually once the observation is made.
- Rollback: disable/remove the QA provider config pointing at the mock, tear down the mock host/container, and confirm no lingering QA traffic is still configured to reach it.

## 10. Deliverables

- `mock_slow_llm_service/app.py`, `requirements.txt`, `Dockerfile` - the mock provider service.
- `cbllmgateway_slow_provider_test.js` - the k6 load test script (fast canary lane + ramping slow lane, with setup/teardown to arm/disarm induced latency).
- This document - test plan, scenarios, metrics, and criteria.

## 11. Open Items for the cbllmgateway Team

- Confirm the exact upstream request/response schema cbllmgateway expects from a provider (OpenAI-style is assumed) so app.py's endpoint can be adjusted if needed.
- Identify the specific QA dashboards/metrics (APM, k8s) to watch for pod-level connection/thread pool saturation, so this plan's section 7 can be filled in with concrete dashboard links.
- Confirm acceptable test window and peak concurrency (`SLOW_LANE_MAX_VUS`) so the test doesn't disrupt other QA usage.
