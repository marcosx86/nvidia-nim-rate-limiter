# Spec 03: Trial-Mode Guardrails, Model Governance & Upstream Graduation

## 1. Status

- **Implemented:** `routing.py` (`detect_deployment_mode`, `ModelRouter`, `ModelCatalogCache`, `UpstreamTarget`), `settings.py`, `app.py`, `wwwroot/index.html`

---

## 2. Motivation (NVIDIA Build TOS)

- Free-tier access on `integrate.api.nvidia.com` is strictly for **prototyping, evaluation, and personal development**—commercial/production traffic is prohibited.
- Scaling past free-tier limits requires graduating models to **Self-Hosted NIM containers** or **Partner Endpoints**.
- Trial models have individual licenses and no continuity guarantee (models may be deprecated or removed without notice).

---

## 3. Requirements

### 3.1 Trial-Mode Detection & Surface Indicators

- `--deployment-mode` / `DEPLOYMENT_MODE` / `proxy.deployment_mode` (`auto` | `trial` | `self_hosted` | `partner`, default `"auto"`).
- In `"auto"` mode, `detect_deployment_mode()` classifies:
  - `upstream_url` whose hostname contains `integrate.api.nvidia.com` as `"trial"`,
  - loopback, RFC1918 private IPs, `localhost`, `.local`, or single-label container service names (no dots) as `"self_hosted"`,
  - all other public endpoints as `"partner"`.
- Expose `deployment_mode` in `/status` and `/admin/metrics`.
- Display a non-intrusive **"Trial / Prototyping Only — Not for Production (NVIDIA Build API)"** banner (`#trialBanner`) in `wwwroot/index.html` when `deployment_mode === "trial"`.

### 3.2 Per-Model Upstream Graduation Routing

- Support `proxy.model_routes` mapping in `config.yaml` and `POST /admin/config`:
  - `model_id -> {"upstream_url": "<url>", "api_key_env": "<ENV_VAR_NAME>", "strip_auth": <bool>}`.
  - **Credential Isolation:** Raw secrets (`api_key` or `nvapi-*` literals) MUST NOT appear in `proxy.model_routes`; validation raises `ConfigError` / HTTP `400`. Only `api_key_env` (resolved from `os.environ` > `.env`) is permitted, or `strip_auth: true` / omitted `api_key_env` for keyless local NIM containers.
- Unmatched models fall back to the default `proxy.upstream_url` + `NVIDIA_API_KEY`.
- Clients and `iron-proxy` continue calling `http://127.0.0.1:8000/v1/chat/completions` unchanged while individual models graduate off the trial tier.

### 3.3 Model Allowlist / Denylist & Catalog Health

- Optional `proxy.allowed_models` and `proxy.blocked_models` lists in `config.yaml` and `POST /admin/config`:
  - Checked before daily quota checks and before acquiring rate-limit tokens (`blocked_models` takes precedence over `allowed_models`).
  - Disallowed models fail fast with HTTP `403` JSON `{"error": "model_not_allowed", "model": ..., "reason": "model_blocked"|"model_not_in_allowlist"}` and log `allowed=False`.
- `--catalog-cache-ttl` / `CATALOG_CACHE_TTL` / `proxy.catalog_cache_ttl` (default `300.0` seconds, `0` disables) and `--enforce-catalog` / `ENFORCE_CATALOG` / `proxy.enforce_catalog` (default `false`):
  - Passively cache model IDs whenever `GET /v1/models` returns HTTP `200`.
  - Expose cached catalog summary via `GET /admin/catalog` and in `/status` and `/admin/metrics`.
  - When `enforce_catalog` is `true` and a valid non-expired catalog is cached, requests targeting an unknown `body.model` fail fast with HTTP `404` JSON `{"error": "model_not_in_catalog", "model": ...}` before consuming rate-limit tokens and log `allowed=False`.

---

## 4. Acceptance Criteria

1. `/status` and `/admin/metrics` report `deployment_mode: "trial"` when targeting `integrate.api.nvidia.com`, `"self_hosted"` for local/private NIM URLs, `"partner"` for external non-NVIDIA URLs, and respect explicit `--deployment-mode` overrides; `wwwroot/index.html` includes `#trialBanner`.
2. Configured `proxy.model_routes` forwards matching `body.model` requests to the graduated upstream URL (using `api_key_env` or keyless local NIM) while unmatched models use default `upstream_url`; raw `api_key` in YAML is rejected.
3. Requests for a model in `blocked_models` (or outside non-empty `allowed_models`) return HTTP `403` without consuming `TokenBucket` tokens and log `allowed=False`.
4. `GET /v1/models` passively populates `GET /admin/catalog`, and when `enforce_catalog=true` unknown models return HTTP `404` without consuming tokens.
