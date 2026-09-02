# QA setup: redirect the "openai" provider to the mock service

Three things needed, in order. The mock service itself (`mock_slow_llm_service/`) needs **no changes** — it already speaks OpenAI's real `/v1/chat/completions` schema.

## 1. Deploy the mock

Build/run `mock_slow_llm_service/` (Dockerfile already in this folder) somewhere reachable by URL from the QA cluster, with `CONTROL_TOKEN` set to a generated secret (never hardcode it). Confirm it's up:

```bash
curl https://<your-mock-host>/healthz
```

## 2. Create the QA subscription (via the admin API, not raw Cassandra)

The `api_key` field is encrypted server-side by cbllmgateway's own encryption sidecar when you call this endpoint (`admin_controller.register_subscription` -> `_encrypt_keys_in_subscription`) — there's no way to hand-craft a valid encrypted value outside the app, so this has to go through the real API, not a direct DB insert.

```bash
curl -X POST \
  "https://<cbllmgateway-qa-host>/api/v2/accounts/<QA_ACCOUNT_ID>/llm-providers/openai/llm-subscriptions" \
  -H "Authorization: Bearer <LP_BEARER_TOKEN_FOR_QA_ACCOUNT>" \
  -H "Content-Type: application/json" \
  -d '{
        "provider_name": "openai",
        "subscription_name": "<pick-a-name e.g. qa-slow-provider-test>",
        "subscription_data": {
          "api_key": "<any placeholder value — the mock never validates it>"
        }
      }'
```

Notes:
- `subscription_name` must match `^[a-zA-Z0-9\-_]+$` (`llm_provider_management_schemas.py:16`).
- `api_key` has no format validation for the OpenAI provider (`LlmSubscriptionData.api_key = fields.String()`), so a dummy value is fine — the mock doesn't check it.
- `account_id` in the URL must match the `accountId` claim on the bearer token you use (`auth_decorators.py:93-94`), so this has to be called with a token scoped to your QA account.

## 3. Add the LaunchDarkly override so requests go to the mock

Flag key: `llmgateway.additionalLlmConfig` (`ld_features.py:43`), targeted at your QA account_id. Set its value to:

```json
{
  "openai": {
    "<model_name-you-will-send-in-request_config.model_name>": {
      "default": { "base_url": "https://<your-mock-host>/v1" }
    }
  }
}
```

- The model name here must exactly match whatever `model_name` your test requests send in `request_config` — it doesn't need to be a real OpenAI model name since it's only used as a lookup key and then forwarded to the mock, which ignores it for OpenAI (the mock reads the `model` field it receives at the HTTP layer to pick mock-fast/mock-slow/mock-hang — see next point).
- **Important mismatch to resolve:** the LD `additional_config` mechanism only injects `base_url` — it does not change the `model` field cbllmgateway sends in the OpenAI request body, which will be your real/dummy model name, not `"mock-slow"`. The mock's current routing (`mock-fast` / `mock-slow` / `mock-hang` in `app.py`) keys off that `model` field. Two options: (a) rename your test model to literally `mock-slow` (simplest — no mock changes), or (b) tell me and I'll add a second override (e.g. a `delay_ms_override` default, or match on something else) so an arbitrary model name still hits the slow path.
- `base_url` must include the `/v1` suffix — the OpenAI SDK appends `/chat/completions` on top of it, matching the mock's existing route.

## 4. Run the test

Point your k6 script's `GATEWAY_CHAT_URL` at cbllmgateway's real `/chats` endpoint (not the mock directly), with `request_config.model_name` set to whatever you chose in step 3, and cbllmgateway will resolve provider "openai" → your QA subscription → LD override → the mock.
