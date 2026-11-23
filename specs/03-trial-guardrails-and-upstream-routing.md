# Spec 03: Trial-Mode Guardrails, Model Governance & Upstream Graduation

## 1. Status

- **Planned**

---

## 2. Motivation (NVIDIA Build TOS)

- Free-tier access on `integrate.api.nvidia.com` is strictly for **prototyping, evaluation, and personal development**—commercial/production traffic is prohibited.
- Scaling past free-tier limits requires graduating models to **Self-Hosted NIM containers** or **Partner Endpoints**.
- Trial models have individual licenses and no continuity guarantee (models may be deprecated or removed without notice).

---

## 3. Requirements

### 3.1 Trial-Mode Detection & Surface Indicators

- Automatically classify `upstream_url` containing `integrate.api.nvidia.com` as `mode: "trial"` (unless overridden by `--deployment-mode self_hosted|partner|trial`).
- Expose `deployment_mode` in `/status` and `/admin/metrics`.
- Display a non-intrusive **"Trial / Prototyping Only — Not for Production"** badge/banner in `wwwroot/index.html` when in `trial` mode.

### 3.2 Per-Model Upstream Graduation Routing

- Support `proxy.model_routes` mapping (in `config.yaml` / `/admin/config`), e.g.:
  - `meta/llama-3.3-70b-instruct` → `http://nim-local:8000` (with optional separate env-var API key reference, never raw secret in YAML).
- Clients and `iron-proxy` continue calling `http://127.0.0.1:8000/v1/chat/completions` unchanged while individual models graduate off the trial tier.

### 3.3 Model Allowlist / Denylist & Catalog Health

- Optional `proxy.allowed_models` / `proxy.blocked_models` lists; requests for disallowed models return HTTP `403` locally before touching rate-limit tokens.
- Cache `GET /v1/models` responses (configurable TTL) to surface available/deprecated models in `/admin/` and optionally fail fast with `404` if a requested model is absent from the cached catalog.

---

## 4. Acceptance Criteria

1. `/status` reports `deployment_mode: "trial"` when targeting `integrate.api.nvidia.com` and `"self_hosted"` for local NIM URLs.
2. Configured `proxy.model_routes` forwards matching `body.model` requests to the graduated upstream URL while unmatched models use the default `upstream_url`.
3. Requests for a model in `blocked_models` (or outside non-empty `allowed_models`) return `403` without consuming `TokenBucket` tokens.
