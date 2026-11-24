# Spec 06: Separated Settings UI & Precedence Awareness

## 1. Status

- **Implemented:** `settings.py` (`build_settings_source_metadata`, `normalize_admin_config_patch`), `app.py` (`/admin/settings`, `/admin/settings/state`, `POST /admin/config` lock enforcement), `wwwroot/index.html`, `wwwroot/settings.html`

---

## 2. Motivation

- **Dedicated Observability vs. Configuration Surfaces:** As `config.yaml` has expanded across Specs #1–#5 (rate limits, TPM, daily caps, per-model overrides, governance allowlists/denylists, graduated upstream routing, and database pool settings), embedding a small inline form inside `/admin/` crowds the live metrics and request logs view.
- **Optional & Unset Values:** Many parameters (`tokens_per_minute`, `max_wait_seconds`, `daily_request_cap`, `daily_token_cap`, `allowed_models`, `blocked_models`, `model_routes`, `rate_limit.models`, `db.url_env`) are optional; operators MUST be able to leave them blank/empty in the UI without HTML validation errors.
- **Precedence Transparency:** Because configuration follows `argparse > env > dotenv > config.yaml > default`, editing a parameter in the UI when it is overridden by a CLI flag, environment variable, or `.env` entry would be misleading or silently reverted on restart. Higher-precedence overrides MUST appear filled in, disabled, and clearly labeled with where they were set.

---

## 3. Requirements

### 3.1 Separated Settings UI (`GET /admin/settings`, `wwwroot/settings.html`)

- Remove the inline Configuration sidebar (`#configForm`) from `wwwroot/index.html` so `/admin/` renders a full-width observability dashboard (cards + request logs + filters).
- Serve a dedicated Settings page at `GET /admin/settings` (and `/admin/settings/`) backed by `wwwroot/settings.html` with `{{ ADMIN_TITLE }}` placeholder replacement.
- Provide consistent header navigation links between **Dashboard (`/admin/`)** and **Settings (`/admin/settings`)** on both pages.

### 3.2 Precedence Awareness & Field Locking (`GET /admin/settings/state`)

- Keep `GET /admin/config` backward-compatible (returning the `config_data` dictionary).
- Add `GET /admin/settings/state` returning:
  - `config`: current effective non-secret `config_data`
  - `sources`: per-field metadata keyed by `"<section>.<key>"` (e.g., `"rate_limit.requests_per_minute"`, `"proxy.upstream_url"`, `"db.path"`), containing:
    - `source`: `"argparse" | "env" | "dotenv" | "config.yaml" | "default"`
    - `locked`: `true` when `source` is `"argparse"`, `"env"`, or `"dotenv"`, otherwise `false`
    - `cli_flag`: corresponding CLI flag (or `null`)
    - `env_var`: corresponding environment variable name (or `null`)
    - `detail`: human-readable origin description (e.g., `"Set via CLI flag --rpm"`, `"Set via environment variable RPM"`, `"Set via .env file (RPM)"`)
  - `runtime_info`: read-only startup context (`nvidia_api_key_source`, `db_backend`, `db_target` masked, `host`, `port` — never raw secrets).
- **UI Behavior (`wwwroot/settings.html`):**
  - Locked fields (`locked: true`) MUST be pre-filled with their active value, marked `disabled`, and display a small inline warning badge/note explaining where the override came from (`detail`).
  - Unlocked fields (`locked: false`) are editable; optional fields whose values are disabled/empty (`0`, `0.0`, `[]`, `{}`, or unset `db.url_env`) render cleanly as empty or optional inputs.
- **Server-Side Lock Enforcement (`POST /admin/config`):**
  - When `POST /admin/config` receives updates for fields that are currently `locked` (`argparse`, `env`, or `dotenv`), it MUST NOT overwrite those active values in memory or persist overridden values into `config.yaml`, and MUST include the skipped paths in `"ignored_locked_fields"` in the response JSON.

### 3.3 Full `config.yaml` Coverage & Optional Empty Values

- `wwwroot/settings.html` MUST expose organized sections covering all `config.yaml` settings:
  1. **Admin**: `admin.title`
  2. **Rate Limiting & Quotas**: `requests_per_minute`, `window_seconds`, `burst_capacity`, optional `tokens_per_minute`, optional `max_wait_seconds`, optional `daily_request_cap`, optional `daily_token_cap`, optional `rate_limit.models` (JSON/structured editor)
  3. **Proxy, Governance & Routing**: `upstream_url`, `concurrency_limit`, `deployment_mode`, `catalog_cache_ttl`, `enforce_catalog`, optional `allowed_models`, optional `blocked_models`, optional `model_routes` (JSON/structured editor, rejecting raw secrets)
  4. **Database**: `db.path`, optional `db.url_env` (env var reference only), `db.pool_min`, `db.pool_max`, `db.connect_timeout`
- Submitting empty strings (`""`) or `null` for optional fields in `POST /admin/config` MUST normalize them cleanly (`tokens_per_minute -> 0`, `max_wait_seconds -> 0.0`, `daily_request_cap -> 0`, `daily_token_cap -> 0`, `allowed_models -> []`, `blocked_models -> []`, `model_routes -> {}`, `rate_limit.models -> {}`, `db.url_env -> removed/omitted`) without failing validation.
- Raw secrets (`NVIDIA_API_KEY`, `DB_URL` passwords, or `api_key` inside `model_routes`) MUST NEVER be returned or persisted.

---

## 4. Acceptance Criteria

1. `GET /admin/` renders the full-width dashboard with a link to `/admin/settings` and without the inline `#configForm`.
2. `GET /admin/settings` serves `wwwroot/settings.html` with `{{ ADMIN_TITLE }}` injected and sections covering `admin`, `rate_limit`, `proxy`, and `db`.
3. `GET /admin/settings/state` marks parameters overridden by `argparse`, `env`, or `dotenv` as `locked: true` with `detail`, and parameters from `config.yaml` or `default` as `locked: false`.
4. Submitting empty (`""` / `null`) values for optional fields via `POST /admin/config` succeeds and resets them to their unset/disabled defaults.
5. Submitting new values for locked fields via `POST /admin/config` preserves the higher-precedence runtime values and lists those keys in `ignored_locked_fields`.
