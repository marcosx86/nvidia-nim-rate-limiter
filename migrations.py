import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import aiosqlite


class MigrationError(RuntimeError):
    """Raised when schema migration execution or checksum validation fails."""


POSTGRES_MIGRATION_LOCK_KEY = 749201843219

SCHEMA_MIGRATIONS_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at DOUBLE PRECISION NOT NULL,
    checksum TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sqlite_statements: Tuple[str, ...]
    postgres_statements: Tuple[str, ...]

    def checksum(self) -> str:
        payload = json.dumps(
            {
                "version": self.version,
                "name": self.name,
                "sqlite": list(self.sqlite_statements),
                "postgres": list(self.postgres_statements),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


MIGRATIONS: Tuple[Migration, ...] = (
    Migration(
        version=1,
        name="001_initial_request_logs",
        sqlite_statements=(
            """
            CREATE TABLE IF NOT EXISTS request_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                model TEXT,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0,
                status_code INTEGER NOT NULL,
                latency_ms REAL NOT NULL,
                request_bytes INTEGER NOT NULL DEFAULT 0,
                response_bytes INTEGER NOT NULL DEFAULT 0,
                allowed INTEGER NOT NULL DEFAULT 1
            )
            """.strip(),
        ),
        postgres_statements=(
            """
            CREATE TABLE IF NOT EXISTS request_logs (
                id BIGSERIAL PRIMARY KEY,
                timestamp DOUBLE PRECISION NOT NULL,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                model TEXT,
                prompt_tokens BIGINT NOT NULL DEFAULT 0,
                completion_tokens BIGINT NOT NULL DEFAULT 0,
                total_tokens BIGINT NOT NULL DEFAULT 0,
                status_code INTEGER NOT NULL,
                latency_ms DOUBLE PRECISION NOT NULL,
                request_bytes BIGINT NOT NULL DEFAULT 0,
                response_bytes BIGINT NOT NULL DEFAULT 0,
                allowed INTEGER NOT NULL DEFAULT 1
            )
            """.strip(),
        ),
    ),
    Migration(
        version=2,
        name="002_add_model_and_tokens",
        sqlite_statements=(
            "ALTER TABLE request_logs ADD COLUMN model TEXT",
            "ALTER TABLE request_logs ADD COLUMN prompt_tokens INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE request_logs ADD COLUMN completion_tokens INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE request_logs ADD COLUMN total_tokens INTEGER NOT NULL DEFAULT 0",
        ),
        postgres_statements=(
            "ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS model TEXT",
            "ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS prompt_tokens BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS completion_tokens BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS total_tokens BIGINT NOT NULL DEFAULT 0",
        ),
    ),
    Migration(
        version=3,
        name="003_add_request_logs_indexes",
        sqlite_statements=(
            "CREATE INDEX IF NOT EXISTS idx_timestamp ON request_logs(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_path ON request_logs(path)",
            "CREATE INDEX IF NOT EXISTS idx_model ON request_logs(model)",
            "CREATE INDEX IF NOT EXISTS idx_status_code ON request_logs(status_code)",
        ),
        postgres_statements=(
            "CREATE INDEX IF NOT EXISTS idx_timestamp ON request_logs(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_path ON request_logs(path)",
            "CREATE INDEX IF NOT EXISTS idx_model ON request_logs(model)",
            "CREATE INDEX IF NOT EXISTS idx_status_code ON request_logs(status_code)",
        ),
    ),
)


def _extract_sqlite_alter_column(stmt: str) -> Optional[str]:
    prefix = "ALTER TABLE request_logs ADD COLUMN "
    if stmt.startswith(prefix):
        rest = stmt[len(prefix):].strip()
        return rest.split()[0] if rest else None
    return None


async def run_sqlite_migrations(
    db_path: str,
    migrations: Sequence[Migration] = MIGRATIONS,
) -> List[int]:
    """Apply pending migrations transactionally on SQLite, adopting legacy schemas safely."""
    applied_now: List[int] = []
    async with aiosqlite.connect(db_path) as db:
        await db.execute(SCHEMA_MIGRATIONS_SQL.strip())
        await db.commit()

        async with db.execute(
            "SELECT version, name, checksum FROM schema_migrations ORDER BY version ASC"
        ) as cursor:
            rows = await cursor.fetchall()
        applied_map = {int(row[0]): str(row[2]) for row in rows}

        for migration in migrations:
            expected_checksum = migration.checksum()
            if migration.version in applied_map:
                if applied_map[migration.version] != expected_checksum:
                    raise MigrationError(
                        f"Checksum mismatch for applied migration {migration.version} "
                        f"({migration.name}): expected {expected_checksum}, "
                        f"found {applied_map[migration.version]}."
                    )
                continue

            try:
                await db.execute("BEGIN")
                if migration.version == 2:
                    async with db.execute("PRAGMA table_info(request_logs)") as col_cur:
                        existing_cols = {row[1] for row in await col_cur.fetchall()}
                    for stmt in migration.sqlite_statements:
                        col_name = _extract_sqlite_alter_column(stmt)
                        if col_name and col_name in existing_cols:
                            continue
                        await db.execute(stmt)
                else:
                    for stmt in migration.sqlite_statements:
                        await db.execute(stmt)

                await db.execute(
                    "INSERT INTO schema_migrations (version, name, applied_at, checksum) VALUES (?, ?, ?, ?)",
                    (migration.version, migration.name, time.time(), expected_checksum),
                )
                await db.commit()
                applied_now.append(migration.version)
            except Exception as exc:
                await db.rollback()
                if isinstance(exc, MigrationError):
                    raise
                raise MigrationError(
                    f"SQLite migration {migration.version} ({migration.name}) failed: {exc}"
                ) from exc

    return applied_now


async def run_postgres_migrations(
    pool: Any,
    migrations: Sequence[Migration] = MIGRATIONS,
) -> List[int]:
    """Apply pending migrations on PostgreSQL under a session advisory lock."""
    applied_now: List[int] = []
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock($1)", POSTGRES_MIGRATION_LOCK_KEY)
        try:
            await conn.execute(SCHEMA_MIGRATIONS_SQL.strip())
            rows = await conn.fetch(
                "SELECT version, name, checksum FROM schema_migrations ORDER BY version ASC"
            )
            applied_map = {int(row["version"]): str(row["checksum"]) for row in rows}

            for migration in migrations:
                expected_checksum = migration.checksum()
                if migration.version in applied_map:
                    if applied_map[migration.version] != expected_checksum:
                        raise MigrationError(
                            f"Checksum mismatch for applied migration {migration.version} "
                            f"({migration.name}): expected {expected_checksum}, "
                            f"found {applied_map[migration.version]}."
                        )
                    continue

                try:
                    async with conn.transaction():
                        for stmt in migration.postgres_statements:
                            await conn.execute(stmt)
                        await conn.execute(
                            "INSERT INTO schema_migrations (version, name, applied_at, checksum) "
                            "VALUES ($1, $2, $3, $4)",
                            migration.version,
                            migration.name,
                            time.time(),
                            expected_checksum,
                        )
                    applied_now.append(migration.version)
                except Exception as exc:
                    if isinstance(exc, MigrationError):
                        raise
                    raise MigrationError(
                        f"PostgreSQL migration {migration.version} ({migration.name}) failed: {exc}"
                    ) from exc
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", POSTGRES_MIGRATION_LOCK_KEY)

    return applied_now


def _build_status_payload(
    applied_rows: Iterable[Tuple[int, str, float, str]],
    migrations: Sequence[Migration],
) -> Dict[str, Any]:
    applied_list: List[Dict[str, Any]] = []
    applied_versions = set()
    for version, name, applied_at, checksum in applied_rows:
        v = int(version)
        applied_versions.add(v)
        applied_list.append(
            {
                "version": v,
                "name": str(name),
                "applied_at": float(applied_at),
                "checksum": str(checksum),
            }
        )

    pending_list = [
        {"version": m.version, "name": m.name, "checksum": m.checksum()}
        for m in migrations
        if m.version not in applied_versions
    ]
    current_version = max(applied_versions) if applied_versions else 0
    return {
        "current_version": current_version,
        "applied": applied_list,
        "pending": pending_list,
    }


async def get_sqlite_migration_status(
    db_path: str,
    migrations: Sequence[Migration] = MIGRATIONS,
) -> Dict[str, Any]:
    """Inspect applied and pending migrations on a SQLite database without mutating data."""
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
        ) as cur:
            table_exists = await cur.fetchone() is not None
        if not table_exists:
            return _build_status_payload((), migrations)

        async with db.execute(
            "SELECT version, name, applied_at, checksum FROM schema_migrations ORDER BY version ASC"
        ) as cur:
            rows = await cur.fetchall()
        return _build_status_payload(
            ((r[0], r[1], r[2], r[3]) for r in rows),
            migrations,
        )


async def get_postgres_migration_status(
    pool: Any,
    migrations: Sequence[Migration] = MIGRATIONS,
) -> Dict[str, Any]:
    """Inspect applied and pending migrations on a PostgreSQL database."""
    async with pool.acquire() as conn:
        table_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'schema_migrations')"
        )
        if not table_exists:
            return _build_status_payload((), migrations)
        rows = await conn.fetch(
            "SELECT version, name, applied_at, checksum FROM schema_migrations ORDER BY version ASC"
        )
        return _build_status_payload(
            ((r["version"], r["name"], r["applied_at"], r["checksum"]) for r in rows),
            migrations,
        )
