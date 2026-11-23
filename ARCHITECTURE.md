# Architecture (`nvidia-nim-rate-limiter`)

This repository is an asynchronous FastAPI reverse proxy, token-bucket rate limiter, and observability dashboard for **NVIDIA NIM APIs** (`integrate.api.nvidia.com`).

## Core Components

- **`app.py`**: FastAPI application, upstream `httpx.AsyncClient` streaming proxy, `extract_tokens` SSE/JSON usage parser, CLI `main()` entrypoint, and `/healthz`, `/status`, `/admin/*` endpoints.
- **`rate_limiter.py`**: Global and per-model `TokenBucket` (RPM), `TPMBucket` (Tokens-Per-Minute), concurrency semaphores, adaptive `429`/`Retry-After` cooldowns, and `--max-wait-seconds` enforcement.
- **`routing.py`**: Trial/self-hosted/partner deployment mode detection, per-model upstream graduation routing, allowlist/denylist governance, and passive `/v1/models` catalog caching.
- **`settings.py`**: Declarative parameter specs, `argparse` CLI parser, bind guardrails, and strict configuration precedence resolver (`argparse > environment var > dotenv > config.yaml > default or fail if required`).
- **`database.py`**: Async repository layer (`DatabaseBackend`) supporting `SQLiteBackend` (`aiosqlite`) and `PostgresBackend` (`asyncpg`) for `request_logs`, aggregated metrics, log filtering, daily quota usage, and retention pruning.
- **`migrations.py`**: Dialect-agnostic, transactional, SHA-256-verified schema migration runner (`schema_migrations`) supporting SQLite legacy adoption and PostgreSQL advisory-lock execution.
- **`wwwroot/index.html`**: Static dark-mode Web Admin Dashboard served at `/admin/` with dynamic `admin.title` injection and interactive config/metrics/log filtering.
- **`config.yaml`**: Persisted runtime configuration (`proxy`, `rate_limit`, `db`, `admin`).
- **`tests/test_app.py`**: `pytest` + `pytest-asyncio` test suite.
- **`Dockerfile` & `docker-compose.yml`**: Containerized deployment (default port `8199:8000`, persistent `data/` volume).

## Architectural Invariants

1. **Credential Isolation**: Clients never send or receive raw `nvapi-*` keys. Inject `NVIDIA_API_KEY` only on outbound upstream requests, and never log, expose in `/admin/config`, or commit API keys.
2. **Low-Latency Streaming**: Preserve chunked/SSE streaming via `httpx.AsyncClient.stream` and `StreamingResponse` without buffering full LLM responses before forwarding.
3. **Config & Docs Sync**: When adding or changing environment variables, `config.yaml` keys, or API endpoints, keep `app.py`, `config.yaml`, `docker-compose.yml`, and `README.md` consistent.
4. **Safe Schema Evolution**: Any new columns or indexes on `request_logs` in `database.py` must include idempotent migration logic in `init_db` for existing SQLite databases.
