# Repository Agent Directives (`nvidia-nim-rate-limiter`)

## 1. Project Overview & Architecture

This repository is an asynchronous FastAPI reverse proxy, token-bucket rate limiter, and observability dashboard for **NVIDIA NIM APIs** (`integrate.api.nvidia.com`).

### Core Components

- **`app.py`**: FastAPI application, `TokenBucket` rate limiter, concurrency semaphore, upstream `httpx.AsyncClient` streaming proxy, `extract_tokens` SSE/JSON usage parser, CLI `main()` entrypoint, and `/healthz`, `/status`, `/admin/*` endpoints.
- **`settings.py`**: Declarative parameter specs, `argparse` CLI parser, and strict configuration precedence resolver (`argparse > environment var > dotenv > config.yaml > default or fail if required`).
- **`database.py`**: Async SQLite (`aiosqlite`) persistence layer for `request_logs`, schema initialization/migrations, and aggregated metrics/log queries.
- **`wwwroot/index.html`**: Static dark-mode Web Admin Dashboard served at `/admin/` with dynamic `admin.title` injection and interactive config/metrics/log filtering.
- **`config.yaml`**: Persisted runtime configuration (`proxy`, `rate_limit`, `db`, `admin`).
- **`tests/test_app.py`**: `pytest` + `pytest-asyncio` test suite.
- **`Dockerfile` & `docker-compose.yml`**: Containerized deployment (default port `8199:8000`, persistent `data/` volume).

### Architectural Invariants

1. **Credential Isolation**: Clients never send or receive raw `nvapi-*` keys. Inject `NVIDIA_API_KEY` only on outbound upstream requests, and never log, expose in `/admin/config`, or commit API keys.
2. **Low-Latency Streaming**: Preserve chunked/SSE streaming via `httpx.AsyncClient.stream` and `StreamingResponse` without buffering full LLM responses before forwarding.
3. **Config & Docs Sync**: When adding or changing environment variables, `config.yaml` keys, or API endpoints, keep `app.py`, `config.yaml`, `docker-compose.yml`, and `README.md` consistent.
4. **Safe Schema Evolution**: Any new columns or indexes on `request_logs` in `database.py` must include idempotent migration logic in `init_db` for existing SQLite databases.

---

## 2. MANDATORY: Use-Case Driven Test Coverage

**Hard Directive:** Every feature, bug fix, or behavior change must have automated test coverage in `tests/` modeled around concrete proxy and observability use cases.

### Testing Rules

1. **Cover Real Project Use Cases:**
   - **Rate Limiting & Concurrency**: `TokenBucket` refill/acquire behavior, burst capacity, and `MAX_CONCURRENT` semaphore serialization.
   - **Reverse Proxy & Streaming**: Upstream forwarding, `NVIDIA_API_KEY` header injection, SSE/JSON chunk pass-through, and `extract_tokens` parsing for prompt/completion/total tokens.
   - **Database & Metrics**: `request_logs` insertion, idempotent schema migrations in `init_db`, `/admin/metrics` aggregations, and `/admin/logs` filtering/pagination.
   - **Configuration & Admin UI**: `config.yaml` + env var loading/fallbacks, dynamic updates via `/admin/config`, `/healthz`, `/status`, and `/admin/` HTML rendering.
2. **Verify Before Concluding:**
   - Run and pass the test suite (`pytest` or `.venv\Scripts\python.exe -m pytest`) after your final edit and before concluding any code or behavior change.

---

## 3. MANDATORY: Git Commit at End of Every Prompt Execution

**Hard Directive:** Whenever a prompt execution creates, modifies, or deletes any file in this repository, you **MUST** stage those changes and create a descriptive git commit **before finishing your final response**.

### Commit Rules & Hygiene

1. **One Logical Commit Per Prompt Round:**
   - Do not leave uncommitted changes in the working tree at the end of a prompt execution.
   - Stage only intended repository files (`git add ...`) and verify `git status --short` is clean before concluding.
2. **Conventional Commits Format:**
   - Use Conventional Commits headers:
     - `feat(<scope>): ...` — new features or capabilities
     - `fix(<scope>): ...` — bug fixes
     - `docs(<scope>): ...` — documentation updates
     - `refactor(<scope>): ...` — non-behavioral restructuring
     - `test(<scope>): ...` — tests and fixtures
     - `chore(<scope>): ...` — build/tooling/maintenance updates
   - Common scopes in this repository: `proxy`, `rate-limiter`, `db`, `config`, `admin`, `dashboard`, `docker`, `tests`, `docs`, `agent`.
3. **Never Commit Secrets or Generated Artifacts:**
   - Never commit `.env`, `*.env`, or any file containing `NVIDIA_API_KEY` / `nvapi-*` credentials.
   - Never commit SQLite databases (`*.db`, `*.db-journal`, `*.sqlite`, `limiter.db`, `data/`).
   - Never commit virtual environments (`.venv/`), test/coverage caches (`.pytest_cache/`, `.coverage`, `htmlcov/`), or Python bytecode (`__pycache__/`, `*.py[cod]`).
