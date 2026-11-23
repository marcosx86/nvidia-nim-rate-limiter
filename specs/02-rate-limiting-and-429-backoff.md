# Spec 02: Rate Limiting, TPM Budgeting & Adaptive `429` Backoff

## 1. Status

- **Implemented:** Global `TokenBucket` (RPM / window / burst), global `asyncio.Semaphore` (`MAX_CONCURRENT`, default `1`)
- **Planned:** Per-model buckets, TPM limiter, upstream `429` / `Retry-After` cooldown, `--max-wait-seconds`, daily/monthly budget caps

---

## 2. Motivation (Hermes Design + NIM TOS)

- **Hermes / `iron-proxy` workload:** Agents can fire rapid parallel or sequential loops. Keeping `MAX_CONCURRENT=1` prevents multiple slow LLM requests from saturating NIM quotas simultaneously, and a token bucket prevents fixed-window boundary double-dipping (`12:00:59` + `12:01:00`).
- **NIM TOS & Dynamic Quotas:** `integrate.api.nvidia.com` enforces unpublished, model-dependent RPM and **Tokens-Per-Minute (TPM)** limits that tighten under platform load. Circumventing limits via account rotation is prohibited; the proxy MUST respect upstream `429` signals cleanly.

---

## 3. Requirements

### 3.1 Per-Model Rate & Concurrency Overrides (Planned)

- Support optional `rate_limit.models.<model_id>` overrides in `config.yaml` (e.g., `requests_per_minute`, `burst_capacity`, `tokens_per_minute`, `concurrency_limit`).
- Request acquisition acquires the **per-model** bucket/semaphore (if configured) and the **global** bucket/semaphore.

### 3.2 Tokens-Per-Minute (TPM) Limiter (Planned)

- Optional `rate_limit.tokens_per_minute` (`--tpm` / `TPM`, `0` = disabled).
- Estimate pre-flight token reservation from `max_tokens` (plus approximate prompt size) and reconcile against actual `usage.total_tokens` returned by `extract_tokens()` after streaming completes.

### 3.3 Adaptive Upstream `429` & `Retry-After` Cooldown (Planned)

- When upstream returns HTTP `429` (or `503` overload):
  - Parse `Retry-After` (seconds or HTTP-date) and `x-ratelimit-reset*` headers.
  - Place the target `model` (or global upstream if model is unknown) into a temporary cooldown window with bounded exponential backoff + jitter.
  - Subsequent requests for that model wait for the cooldown (subject to `--max-wait-seconds`) instead of immediately hitting upstream again.
  - Record `throttled_upstream` / `cooldown_until` metrics for `/status` and `/admin/metrics`.

### 3.4 Queue Wait Timeout & Quota Caps (Planned)

- `--max-wait-seconds` (`MAX_WAIT_SECONDS`, default `0` = wait indefinitely): If projected wait exceeds `max_wait_seconds`, return local HTTP `429` JSON (`{"error": "rate_limit_wait_exceeded", "retry_after": ...}`) with `Retry-After` header and log `allowed=0`.
- Optional `--daily-request-cap` / `--daily-token-cap`: Soft-warn or hard-trip local `429` circuit breaker when daily usage reaches threshold.

---

## 4. Acceptance Criteria

1. Global `TokenBucket` enforces smooth refill without boundary bursts, and `MAX_CONCURRENT` serializes active upstream requests.
2. Upstream `429` with `Retry-After: 5` pauses subsequent requests to that model for ~5s without dropping client error transparency.
3. Requests exceeding `--max-wait-seconds` fail fast with local `429` + `Retry-After` and increment `throttled_requests` in SQLite.
4. Pre-flight + post-stream TPM accounting tracks actual `extract_tokens()` usage accurately.
