# Spec 02: Rate Limiting, TPM Budgeting & Adaptive `429` Backoff

## 1. Status

- **Implemented:** `rate_limiter.py` (`TokenBucket`, `TPMBucket`, `RateLimitManager`, `RateLimitExceeded`, `parse_retry_after`, `estimate_request_tokens`), `database.py` (`get_daily_usage`), `settings.py`, `app.py`

---

## 2. Motivation (Hermes Design + NIM TOS)

- **Hermes / `iron-proxy` workload:** Agents can fire rapid parallel or sequential loops. Keeping `MAX_CONCURRENT=1` prevents multiple slow LLM requests from saturating NIM quotas simultaneously, and a token bucket prevents fixed-window boundary double-dipping (`12:00:59` + `12:01:00`).
- **NIM TOS & Dynamic Quotas:** `integrate.api.nvidia.com` enforces unpublished, model-dependent RPM and **Tokens-Per-Minute (TPM)** limits that tighten under platform load. Circumventing limits via account rotation is prohibited; the proxy MUST respect upstream `429` signals cleanly.

---

## 3. Requirements

### 3.1 Per-Model Rate & Concurrency Overrides

- Support optional `rate_limit.models.<model_id>` overrides in `config.yaml` and `POST /admin/config` with keys: `requests_per_minute`, `window_seconds`, `burst_capacity`, `tokens_per_minute`, and `concurrency_limit`.
- Request acquisition acquires both the **per-model** bucket/semaphore (when configured for `body.model`) and the **global** bucket/semaphore.

### 3.2 Tokens-Per-Minute (TPM) Limiter

- Configurable `--tpm` / `TPM` / `rate_limit.tokens_per_minute` (default `0` = disabled), plus optional per-model `tokens_per_minute`.
- Pre-flight reservation estimates token cost from prompt payload length (`len(body) // 4`) plus `max_tokens` / `max_completion_tokens` (default `256` for JSON payloads, capped at bucket capacity).
- Post-stream reconciliation adjusts the TPM bucket against actual `usage.total_tokens` returned by `extract_tokens()` (or refunds the reservation if upstream returned an error without usage).

### 3.3 Adaptive Upstream `429` / `503` & `Retry-After` Cooldown

- When upstream returns HTTP `429` or `503`:
  - Parse `Retry-After` (numeric seconds or HTTP-date) or `x-ratelimit-reset*` headers (`s`, `ms`, `m`, or Unix epoch).
  - If no reset header is present, apply bounded exponential backoff (`min(60.0, 2.0 * 2**(streak - 1))`) with jitter.
  - Place the target `model` (or global key `"__global__"` if model is unknown) into cooldown; subsequent requests for that key wait for the cooldown (subject to `--max-wait-seconds`).
  - Reset consecutive backoff streak on any `2xx`/`3xx` response for that key.
  - Expose active cooldowns and `upstream_throttled_count` in `/status` and `/admin/metrics`.

### 3.4 Queue Wait Timeout & Daily Quota Caps

- `--max-wait-seconds` / `MAX_WAIT_SECONDS` / `rate_limit.max_wait_seconds` (default `0.0` = wait indefinitely):
  - If projected cooldown, RPM, or TPM wait exceeds `max_wait_seconds`, fail fast without consuming tokens, return HTTP `429` JSON `{"error": "rate_limit_wait_exceeded", "retry_after": ...}` with `Retry-After` header, and log `allowed=False` (`status_code=429`).
- `--daily-request-cap` / `DAILY_REQUEST_CAP` / `rate_limit.daily_request_cap` (default `0` = disabled) and `--daily-token-cap` / `DAILY_TOKEN_CAP` / `rate_limit.daily_token_cap` (default `0` = disabled):
  - Check UTC-day usage via `database.get_daily_usage(db_path)` before acquiring tokens.
  - If either cap is reached, return HTTP `429` JSON `{"error": "daily_quota_exceeded", "quota_type": "requests"|"tokens", "limit": ..., "used": ..., "retry_after": ...}` with `Retry-After` header (seconds until UTC midnight) and log `allowed=False` (`status_code=429`).

---

## 4. Acceptance Criteria

1. Global and per-model `TokenBucket` / concurrency semaphores enforce smooth refill and serialization independently and together.
2. Upstream `429` with `Retry-After: 5` pauses subsequent requests to that model for ~5s while returning the upstream `429` transparently to the caller.
3. Requests whose projected wait exceeds `--max-wait-seconds` fail fast with local `429` + `Retry-After` without consuming tokens, and increment `throttled_requests` in SQLite.
4. Pre-flight + post-stream TPM accounting reconciles against actual `extract_tokens()` usage and refunds on upstream errors.
5. Daily request/token caps return local `429` (`daily_quota_exceeded`) once UTC-day usage reaches the configured limit.
