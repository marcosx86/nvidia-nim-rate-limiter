# Spec 01: Configuration, CLI Entrypoint & Bind Guardrails

## 1. Status

- **Implemented:** `settings.py`, `app.py` (`configure_runtime`, `main`, `lifespan`), `.env.example`, `Dockerfile`, `docker-compose.yml`

---

## 2. Configuration Precedence (Implemented)

Every runtime parameter MUST resolve strictly in this order:

1. **CLI arguments (`argparse`)** — explicit flags passed to `python app.py`
2. **Environment variables (`os.environ`)** — non-empty values override `.env` and `config.yaml`
3. **Dotenv file (`.env`)** — loaded via `dotenv.dotenv_values()` without mutating `os.environ`
4. **`config.yaml`** — persisted non-secret runtime settings only
5. **Default value** — or **fail fast** (`ConfigError` / exit code `2`) if required

### Secret Isolation Rule

- `NVIDIA_API_KEY` (`--nvidia-api-key`) is **REQUIRED** and MUST NOT be read from or written to `config.yaml`.
- Empty or whitespace-only environment variables (e.g., unset Docker Compose substitutions) are treated as unset so `.env` fallback works cleanly.
- Passing an explicit empty CLI flag (e.g., `--nvidia-api-key ""`) fails validation immediately.

---

## 3. Bind & Worker Guardrails

- **Single Worker:** Container and CLI execution MUST use 1 worker (`uvicorn.run(app, ..., workers=1)` / `--workers 1`) because rate-limit and concurrency state live in process memory.
- **Loopback Default:** `--host` defaults to `127.0.0.1` so `iron-proxy` or local clients connect over loopback without exposing an unauthenticated `NVIDIA_API_KEY` injector on the LAN.
- **Non-Local Bind Guardrail (`--allow-non-local-bind` / `ALLOW_NON_LOCAL_BIND`, `--strict-trial-guardrails` / `STRICT_TRIAL_GUARDRAILS`):**
  - When `--host` is non-loopback (anything other than `localhost` or a loopback IP such as `127.0.0.1` / `::1`) and `--allow-non-local-bind` is `false` (default):
    - If `--strict-trial-guardrails` is `true`, startup fails fast (`ConfigError` / exit code `2`).
    - Otherwise, startup logs a prominent security warning to `stderr` reminding operators to bind to `127.0.0.1` or explicitly set `--allow-non-local-bind true`.
  - Container deployments that intentionally bind `0.0.0.0` behind Docker port mapping pass `--allow-non-local-bind true`.

---

## 4. Acceptance Criteria

1. `resolve_settings()` resolves `argparse > env > dotenv > config.yaml > default` for all parameters.
2. Missing `NVIDIA_API_KEY` raises `ConfigError` during `resolve_settings()`, FastAPI `lifespan`, and `app.main()` (exit code `2`).
3. `config.yaml` never stores `nvidia_api_key`, and `/admin/config` and `/status` never expose it.
4. Explicit invalid values (non-positive `RPM`, `MAX_CONCURRENT <= 0`, out-of-range `PORT`, missing explicit `--env-file`) raise `ConfigError`.
5. Repository includes a tracked `.env.example` documenting all supported environment variables with comments and safe placeholder values.
