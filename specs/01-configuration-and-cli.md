# Spec 01: Configuration, CLI Entrypoint & Bind Guardrails

## 1. Status

- **Implemented:** `settings.py`, `app.py` (`configure_runtime`, `main`, `lifespan`), `Dockerfile`, `docker-compose.yml`
- **Planned:** `--allow-non-local-bind` safety check

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

- **Single Worker:** Container and CLI execution MUST use 1 worker (`uvicorn.run(app, ...)` / `--workers 1`) because rate-limit and concurrency state live in process memory.
- **Loopback Default:** `--host` defaults to `127.0.0.1` so `iron-proxy` or local clients connect over loopback without exposing an unauthenticated `NVIDIA_API_KEY` injector on the LAN.
- **Planned (`--allow-non-local-bind` / `ALLOW_NON_LOCAL_BIND`):**
  - When `--host` is non-loopback (e.g., `0.0.0.0`) and no local client auth token is configured, startup logs a prominent security warning (or fails if `--strict-trial-guardrails` is enabled) unless `--allow-non-local-bind=true` is explicitly set.

---

## 4. Acceptance Criteria

1. `resolve_settings()` resolves `argparse > env > dotenv > config.yaml > default` for all parameters.
2. Missing `NVIDIA_API_KEY` raises `ConfigError` during `resolve_settings()`, FastAPI `lifespan`, and `app.main()` (exit code `2`).
3. `config.yaml` never stores `nvidia_api_key`, and `/admin/config` and `/status` never expose it.
4. Explicit invalid values (non-positive `RPM`, `MAX_CONCURRENT <= 0`, out-of-range `PORT`, missing explicit `--env-file`) raise `ConfigError`.
