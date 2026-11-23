# Repository Agent Directives (`nvidia-nim-rate-limiter`)

## 1. MANDATORY: Specification-Driven Development (`SPEC.md` -> `specs/` -> Implementation)

**Hard Directive:** All non-trivial feature additions, architectural changes, or behavioral modifications MUST follow a **two-phase, spec-first** workflow. See [ARCHITECTURE.md](ARCHITECTURE.md) for the current system architecture and invariants.

### Two-Phase Turn Protocol

1. **Phase 1 — Update `SPEC.md` and `specs/` First:**
   - Before writing or modifying application code for any new feature or behavioral change, update `SPEC.md` (and create/update the corresponding feature specification under `specs/`) so that requirements, API contracts, configuration changes, and acceptance criteria are explicitly documented.
   - Keep `SPEC.md` and `specs/*.md` in sync with the intended behavior.
2. **Phase 2 — Implement & Verify Against the Spec:**
   - Implement the code changes in `app.py`, `settings.py`, `database.py`, `wwwroot/index.html`, etc., strictly matching the updated specification.
   - Verify all acceptance criteria with automated tests in `tests/`.

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
