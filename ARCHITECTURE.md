# Architecture (`nvidia-nim-rate-limiter`)

This repository is an asynchronous FastAPI reverse proxy, token-bucket rate limiter, and observability dashboard for **NVIDIA NIM APIs** (`integrate.api.nvidia.com`).

## Core Components

- **`app.py`**: FastAPI application, `TokenBucket` rate limiter, concurrency semaphore, upstream `httpx.AsyncClient` streaming proxy, `extract_tokens` SSE/JSON usage parser, CLI `main()` entrypoint, and `/healthz`, `/status`, `/admin/*` endpoints.
- **`settings.py`**: Declarative parameter specs, `argparse` CLI parser, and strict configuration precedence resolver (`argparse > environment var > dotenv > config.yaml > default or fail if required`).
- **`database.py`**: Async SQLite (`aiosqlite`) persistence layer for `request_logs`, schema initialization/migrations, and aggregated metrics/log queries.
- **`wwwroot/index.html`**: Static dark-mode Web Admin Dashboard served at `/admin/` with dynamic `admin.title` injection and interactive config/metrics/log filtering.
- **`config.yaml`**: Persisted runtime configuration (`proxy`, `rate_limit`, `db`, `admin`).
- **`tests/test_app.py`**: `pytest` + `pytest-asyncio` test suite.
- **`Dockerfile` & `docker-compose.yml`**: Containerized deployment (default port `8199:8000`, persistent `data/` volume).

## Architectural Invariants

1. **Credential Isolation**: Clients never send or receive raw `nvapi-*` keys. Inject `NVIDIA_API_KEY` only on outbound upstream requests, and never log, expose in `/admin/config`, or commit API keys.
2. **Low-Latency Streaming**: Preserve chunked/SSE streaming via `httpx.AsyncClient.stream` and `StreamingResponse` without buffering full LLM responses before forwarding.
3. **Config & Docs Sync**: When adding or changing environment variables, `config.yaml` keys, or API endpoints, keep `app.py`, `config.yaml`, `docker-compose.yml`, and `README.md` consistent.
4. **Safe Schema Evolution**: Any new columns or indexes on `request_logs` in `database.py` must include idempotent migration logic in `init_db` for existing SQLite databases.
