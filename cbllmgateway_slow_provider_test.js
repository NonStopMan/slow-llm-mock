/*
 * k6 load test: cbllmgateway behavior under slow-provider latency.
 *
 * Hypothesis under test
 * ----------------------
 * When an upstream LLM provider is slow (3-4+ minutes), in-flight requests
 * pile up on the cbllmgateway pod handling them, and this eventually starves
 * the pod's capacity to make ANY outbound request - including calls that have
 * nothing to do with the slow provider. If true, a "fast lane" of requests
 * hitting a healthy provider will start degrading once the "slow lane" of
 * requests hitting the induced-latency mock provider crosses some
 * concurrency threshold.
 *
 * This script drives two concurrent lanes against cbllmgateway itself (not
 * against the mock directly):
 *   - fast lane: low, constant request rate, model="mock-fast" (canary)
 *   - slow lane: ramping concurrency, model="mock-slow"
 * and asks the mock service (via its /control/latency endpoint) to hold
 * mock-slow at a fixed induced delay for the duration of the incident window.
 *
 * IMPORTANT: cbllmgateway must be configured (in QA) with a provider entry
 * pointing at the mock service's URL for both "mock-fast" and "mock-slow"
 * model names before running this script. This script does not configure
 * cbllmgateway itself.
 *
 * Required environment variables (no secrets are hardcoded in this file):
 *   GATEWAY_CHAT_URL     - full URL of cbllmgateway's chat-completions
 *                          endpoint in QA, e.g. https://cbllmgateway.qa.internal/v1/chat/completions
 *   GATEWAY_AUTH_HEADER  - full Authorization header value, e.g. "Bearer <token>"
 *   MOCK_CONTROL_URL     - base URL of the mock service, e.g. https://mock-slow-llm.qa.internal
 *   MOCK_CONTROL_TOKEN   - control-plane token configured on the mock service (CONTROL_TOKEN)
 *
 * Optional environment variables:
 *   INDUCED_DELAY_MS     - fixed delay to induce on mock-slow (default 200000 = ~3m20s)
 *   FAST_LANE_RATE       - requests/sec for the canary lane (default 2)
 *   SLOW_LANE_MAX_VUS    - peak concurrent slow-lane requests (default 40)
 *   TEST_DURATION        - total scenario duration, e.g. "10m" (default "10m")
 *
 * Run:
 *   k6 run \
 *     -e GATEWAY_CHAT_URL=https://cbllmgateway.qa.internal/v1/chat/completions \
 *     -e GATEWAY_AUTH_HEADER="Bearer $QA_TOKEN" \
 *     -e MOCK_CONTROL_URL=https://mock-slow-llm.qa.internal \
 *     -e MOCK_CONTROL_TOKEN="$MOCK_TOKEN" \
 *     cbllmgateway_slow_provider_test.js
 */

import http from "k6/http";
import { check } from "k6";
import { Trend, Rate } from "k6/metrics";

const GATEWAY_CHAT_URL = __ENV.GATEWAY_CHAT_URL;
const GATEWAY_AUTH_HEADER = __ENV.GATEWAY_AUTH_HEADER;
const MOCK_CONTROL_URL = __ENV.MOCK_CONTROL_URL;
const MOCK_CONTROL_TOKEN = __ENV.MOCK_CONTROL_TOKEN;

const INDUCED_DELAY_MS = parseInt(__ENV.INDUCED_DELAY_MS || "200000", 10);
const FAST_LANE_RATE = parseInt(__ENV.FAST_LANE_RATE || "2", 10);
const SLOW_LANE_MAX_VUS = parseInt(__ENV.SLOW_LANE_MAX_VUS || "40", 10);
const TEST_DURATION = __ENV.TEST_DURATION || "10m";

if (!GATEWAY_CHAT_URL || !MOCK_CONTROL_URL || !MOCK_CONTROL_TOKEN) {
  throw new Error(
    "GATEWAY_CHAT_URL, MOCK_CONTROL_URL and MOCK_CONTROL_TOKEN must be set. See file header for usage."
  );
}

// Custom metrics, split by lane, so the two lanes can be compared directly
// in the k6 summary / exported results.
const fastLaneDuration = new Trend("fast_lane_duration", true);
const slowLaneDuration = new Trend("slow_lane_duration", true);
const fastLaneFailureRate = new Rate("fast_lane_failure_rate");
const slowLaneFailureRate = new Rate("slow_lane_failure_rate");

export const options = {
  scenarios: {
    fast_canary: {
      executor: "constant-arrival-rate",
      rate: FAST_LANE_RATE,
      timeUnit: "1s",
      duration: TEST_DURATION,
      preAllocatedVUs: Math.max(FAST_LANE_RATE * 2, 5),
      maxVUs: Math.max(FAST_LANE_RATE * 5, 20),
      exec: "fastLaneRequest",
      tags: { lane: "fast" },
    },
    slow_incident: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "1m", target: Math.round(SLOW_LANE_MAX_VUS * 0.25) },
        { duration: "2m", target: Math.round(SLOW_LANE_MAX_VUS * 0.6) },
        { duration: "2m", target: SLOW_LANE_MAX_VUS },
        { duration: "3m", target: SLOW_LANE_MAX_VUS }, // hold at saturation
        { duration: "2m", target: 0 }, // ramp down, confirm recovery
      ],
      exec: "slowLaneRequest",
      tags: { lane: "slow" },
      startTime: "30s", // let the fast-lane baseline establish first
    },
  },
  thresholds: {
    // If the fast/canary lane blows past these once the slow lane ramps up,
    // that is the signal that unrelated traffic is being starved by the
    // slow-provider pileup - i.e. the hypothesis is confirmed.
    "fast_lane_duration": ["p(95)<2000", "p(99)<5000"],
    "fast_lane_failure_rate": ["rate<0.01"],
  },
};

function chatPayload(model, tagValue) {
  return JSON.stringify({
    model: model,
    messages: [
      { role: "user", content: `QA perf test (${tagValue}) - please respond.` },
    ],
    max_tokens: 32,
  });
}

function headers(extraTimeoutHeader) {
  const h = { "Content-Type": "application/json" };
  if (GATEWAY_AUTH_HEADER) h["Authorization"] = GATEWAY_AUTH_HEADER;
  return h;
}

export function fastLaneRequest() {
  const res = http.post(GATEWAY_CHAT_URL, chatPayload("mock-fast", "fast"), {
    headers: headers(),
    timeout: "10s",
    tags: { lane: "fast" },
  });
  fastLaneDuration.add(res.timings.duration);
  const ok = check(res, { "fast lane status 200": (r) => r.status === 200 });
  fastLaneFailureRate.add(!ok);
}

export function slowLaneRequest() {
  // Long timeout: this request is *expected* to take close to INDUCED_DELAY_MS.
  // The point of the test is to observe cbllmgateway/pod behavior while these
  // are in flight, not to make this request fast.
  const res = http.post(GATEWAY_CHAT_URL, chatPayload("mock-slow", "slow"), {
    headers: headers(),
    timeout: "360s",
    tags: { lane: "slow" },
  });
  slowLaneDuration.add(res.timings.duration);
  const ok = check(res, { "slow lane status 200": (r) => r.status === 200 });
  slowLaneFailureRate.add(!ok);
}

export function setup() {
  // Arm the mock: mock-slow will now add a fixed delay to every request
  // until teardown() turns it back off.
  const res = http.post(
    `${MOCK_CONTROL_URL}/control/latency`,
    JSON.stringify({ mode: "fixed", fixed_ms: INDUCED_DELAY_MS }),
    {
      headers: {
        "Content-Type": "application/json",
        "X-Control-Token": MOCK_CONTROL_TOKEN,
      },
    }
  );
  if (res.status !== 200) {
    throw new Error(
      `Failed to arm mock-slow latency (status ${res.status}): ${res.body}`
    );
  }
  console.log(`Armed mock-slow with fixed_ms=${INDUCED_DELAY_MS}`);
}

export function teardown() {
  const res = http.post(
    `${MOCK_CONTROL_URL}/control/latency`,
    JSON.stringify({ mode: "off" }),
    {
      headers: {
        "Content-Type": "application/json",
        "X-Control-Token": MOCK_CONTROL_TOKEN,
      },
    }
  );
  console.log(`Disarmed mock-slow latency, status=${res.status}`);
}
