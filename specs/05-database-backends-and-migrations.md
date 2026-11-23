# Spec 05: PostgreSQL Backend Support & Universal Database Migrations

## 1. Status

- **Planned**

---

## 2. Motivation

- **Scaling Beyond Single-File SQLite:** While local SQLite (`limiter.db`) is ideal for lightweight single-host deployments, production, containerized, or long-retention deployments benefit from **PostgreSQL** for concurrent read/write throughput, centralized observability, and managed backups/replication.
- **Formal Cross-Backend Schema Evolution:** As `request_logs` and future observability tables evolve (e.g., daily quota tracking, retention pruning from Spec #4, privacy warning flags), ad-hoc `PRAGMA table_info` checks in SQLite alone are insufficient. A formal, transactional, versioned migration framework MUST support SQLite, PostgreSQL, and any future SQL backend uniformly.

---

## 3. Requirements

### 3.1 Backend Selection & Configuration (Strict Precedence)

All database parameters follow the project's strict precedence (`argparse > environment var > dotenv > config.yaml > default`):

- `--db-url` / `DB_URL` / `db.url_env` (optional string):
  - Supports `sqlite:///path/to/file.db` (or `sqlite:///:memory:`), `postgresql://user:pass@host:5432/dbname`, and `postgres://...`.
  - When unset, the proxy falls back to `--db-path` / `DB_PATH` / `db.path` (default `limiter.db`, SQLite) for full backward compatibility.
- Optional PostgreSQL pool settings:
  - `--db-pool-min` / `DB_POOL_MIN` / `db.pool_min` (positive integer, default `1`)
  - `--db-pool-max` / `DB_POOL_MAX` / `db.pool_max` (positive integer, default `5`, must be `>= pool_min`)
  - `--db-connect-timeout` / `DB_CONNECT_TIMEOUT` / `db.connect_timeout` (positive float seconds, default `10.0`)
- **Credential Isolation:**
  - Raw `DB_URL` / `--db-url` DSNs containing passwords MUST NEVER be persisted to `config.yaml` or returned unredacted in `/status` or `/admin/config`.
  - `config.yaml` may specify `db.url_env: "DATABASE_URL"` (referencing an environment/`.env` variable name), mirroring `proxy.model_routes.<model>.api_key_env` in Spec #3; validation MUST reject raw DSNs with embedded passwords in `config.yaml`.
  - Display sanitized targets (e.g., `postgresql://user:***@host:5432/dbname`) in logs, `/status`, and `/admin/metrics`.

### 3.2 Unified Async Repository Interface (`DatabaseBackend`)

- Define an async repository abstraction (`DatabaseBackend` protocol/ABC in `database.py` or a `db/` package) implemented by:
  - **`SQLiteBackend`** (`aiosqlite`)
  - **`PostgresBackend`** (`asyncpg` connection pool)
- Both backends MUST expose identical async method contracts:
  - `init_and_migrate()`
  - `log_request(...)`
  - `get_metrics()`
  - `get_recent_models_with_tokens(limit)`
  - `get_logs(limit, offset, status_code, path, model)`
  - `get_daily_usage(now=None)`
  - `prune_old_logs(retention_days)`
  - `get_migration_status()`
  - `close()`
- **Dialect Abstraction:**
  - Parameterized queries (`?` on SQLite vs `$1, $2, ...` on PostgreSQL).
  - DDL types: `INTEGER PRIMARY KEY AUTOINCREMENT` vs `BIGSERIAL PRIMARY KEY`, `REAL` vs `DOUBLE PRECISION`, `BIGINT` for `prompt_tokens`, `completion_tokens`, `total_tokens`, `request_bytes`, and `response_bytes` on PostgreSQL.
  - Case-insensitive substring matching (`LIKE` on SQLite vs `ILIKE` on PostgreSQL) for `/admin/logs` `path` and `model` filters.
- **Dependency Guardrail:** If a `postgresql://` / `postgres://` URL is configured when `asyncpg` is not installed, startup MUST fail fast with a clear `ConfigError` instructing the operator to install `asyncpg` (or update `requirements.txt`).

### 3.3 Backend-Agnostic Versioned Migrations

- **Version Tracking Table:** Every backend maintains:
  ```sql
  CREATE TABLE IF NOT EXISTS schema_migrations (
      version INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      applied_at DOUBLE PRECISION NOT NULL,
      checksum TEXT NOT NULL
  );
  ```
- **Dialect-Aware Migration Registry:** Ordered list of immutable migrations (`Migration(version, name, sqlite_sql, postgres_sql)` or callable hooks):
  - `001_initial_request_logs`: Baseline `request_logs` table.
  - `002_add_model_and_tokens`: Ensure `model`, `prompt_tokens`, `completion_tokens`, `total_tokens` columns exist.
  - `003_add_request_logs_indexes`: Ensure `idx_timestamp`, `idx_path`, `idx_model`, `idx_status_code` indexes exist.
  - Subsequent revisions (e.g., `004_*` for Spec #4 privacy warning columns or retention indexes) append sequentially without modifying earlier revisions.
- **Transactional & Concurrency-Safe Execution:**
  - Each pending migration runs inside an explicit transaction (`BEGIN` / `COMMIT` / `ROLLBACK`), recording its `(version, name, applied_at, checksum)` atomically.
  - PostgreSQL migration runs MUST acquire a session-level advisory lock (`SELECT pg_advisory_lock(...)` / `pg_advisory_unlock(...)`) so concurrent startup instances do not race on DDL.
- **Legacy SQLite Adoption:**
  - When `init_and_migrate()` opens an existing pre-versioned SQLite database that already has `request_logs` (with or without `model`/token columns) and no `schema_migrations` table, it MUST idempotently add any missing columns/indexes without losing rows and mark baseline revisions (`001`–`003`) as applied.
- **Checksum Verification & CLI Tooling:**
  - Compute SHA-256 checksums of migration SQL/definitions; if an already-applied version's checksum differs on startup, fail fast with a clear `MigrationError` / `ConfigError`.
  - Provide `--migrate-only` (run pending migrations and exit `0`) and `--migration-status` (print applied/pending revisions as JSON/table and exit `0`) CLI flags on `app.py` without starting `uvicorn`.

### 3.4 Observability & Lifecycle

- Expose `db: {"backend": "sqlite" | "postgresql", "schema_version": <int>, "target": "<sanitized>"}` in `/status` and `/admin/metrics`.
- Initialize the active `DatabaseBackend` in FastAPI `lifespan` startup and call `await backend.close()` cleanly during shutdown.

---

## 4. Acceptance Criteria

1. Default startup with no `DB_URL` continues using SQLite (`DB_PATH`) with zero breaking changes to existing deployments or tests.
2. Pre-versioned SQLite databases (both minimal `request_logs` and current schema) adopt `schema_migrations` cleanly without data loss.
3. Running `init_and_migrate()` multiple times is strictly idempotent on both SQLite and PostgreSQL, and modifying an applied migration triggers a checksum mismatch failure.
4. `PostgresBackend` satisfies the full repository test suite (`log_request`, `get_metrics`, `get_logs`, `get_daily_usage`, `get_recent_models_with_tokens`, `prune_old_logs`) and closes its pool cleanly on shutdown.
5. PostgreSQL credentials in `DB_URL` are never written to `config.yaml` and are always masked in `/status` and `/admin/config`.
6. `--migrate-only` and `--migration-status` execute migrations/reporting and exit `0` without binding an HTTP port.
