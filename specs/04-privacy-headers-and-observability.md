# Spec 04: Privacy Guardrails, Header Hygiene & Observability

## 1. Status

- **Implemented:** SQLite `request_logs` (metadata only, no prompt/response bodies), idempotent `init_db` migrations, `/admin/metrics`, `/admin/logs`, `/admin/` dashboard with filters
- **Planned:** Outbound secret/PII guardrail, strict header allowlist & size cap, SQLite log retention auto-purge, optional local client/admin auth

---

## 2. Motivation (NVIDIA Build TOS & Gateway Constraints)

- **Confidential / Personal Data Warning:** Trial endpoints may log inputs/outputs to operate and improve NVIDIA services; users are warned not to send confidential data or PII.
- **Gateway Header Limits:** Upstream API gateways reject oversized headers (e.g., >8,192 chars).
- **Zero-Content Local Logs:** Keeping request/response bodies out of SQLite minimizes local data footprint, and automatic retention pruning prevents unbounded DB growth.

---

## 3. Requirements

### 3.1 Outbound Secret & PII Guardrail (Planned)

- Configurable `--pii-guardrail-mode` (`off` | `warn` | `redact` | `block`, defaulting to `warn` in `trial` mode).
- Scan outbound JSON payloads (`messages`, `prompt`, `input`) for high-confidence secrets (`nvapi-*`, `sk-*`, `ghp_*`, `AKIA*`, PEM private keys) and optional PII patterns (emails, SSNs, credit cards).
  - `warn`: Log a warning flag in `request_logs` and increment `privacy_warnings` in `/admin/metrics`.
  - `redact`: Replace matched spans with `[REDACTED:<type>]` before forwarding upstream.
  - `block`: Reject with HTTP `400` locally before consuming rate-limit tokens.

### 3.2 Outbound Header Hygiene (Planned)

- Strip hop-by-hop and client `Authorization` headers (already implemented) and enforce a maximum single-header value length (e.g., `<= 8192` bytes) to prevent upstream gateway faults.

### 3.3 SQLite Log Retention & Local Auth (Planned)

- `--log-retention-days` (`LOG_RETENTION_DAYS`, default `30`, `0` = keep forever): Prune `request_logs` rows older than N days on startup and periodically in background.
- Optional `--client-auth-token` / `--admin-auth-token`: Require `Bearer <token>` from `iron-proxy`/clients or `/admin/*` callers when set (especially if bound to a non-loopback interface).

---

## 4. Acceptance Criteria

1. `pii_guardrail_mode=block` rejects payloads containing `nvapi-...` or PEM keys with HTTP `400` without forwarding upstream or consuming a token.
2. `pii_guardrail_mode=redact` masks matched secrets before upstream `http_client.send()`.
3. Oversized headers (>8,192 chars) are stripped or rejected before hitting upstream.
4. `prune_old_logs(db_path, retention_days)` deletes expired rows while preserving recent metrics.
