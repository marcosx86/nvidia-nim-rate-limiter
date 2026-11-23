import datetime
import time
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable
from urllib.parse import urlparse

import aiosqlite

import migrations
from settings import ConfigError, sanitize_db_target


@runtime_checkable
class DatabaseBackend(Protocol):
    backend_name: str
    sanitized_target: str
    schema_version: int

    async def init_and_migrate(self) -> None: ...

    async def log_request(
        self,
        method: str,
        path: str,
        status_code: int,
        latency_ms: float,
        request_bytes: int = 0,
        response_bytes: int = 0,
        allowed: bool = True,
        model: Optional[str] = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
    ) -> None: ...

    async def get_metrics(self) -> Dict[str, Any]: ...

    async def get_recent_models_with_tokens(self, limit: int = 3) -> List[str]: ...

    async def get_logs(
        self,
        limit: int = 50,
        offset: int = 0,
        status_code: Optional[int] = None,
        path: Optional[str] = None,
        model: Optional[str] = None,
    ) -> List[Dict[str, Any]]: ...

    async def get_daily_usage(self, now: Optional[float] = None) -> Dict[str, Any]: ...

    async def prune_old_logs(self, retention_days: int) -> int: ...

    async def get_migration_status(self) -> Dict[str, Any]: ...

    async def close(self) -> None: ...


def _compute_utc_day_bounds(now: Optional[float] = None) -> tuple[float, float]:
    current_ts = time.time() if now is None else float(now)
    utc_now = datetime.datetime.fromtimestamp(current_ts, tz=datetime.timezone.utc)
    day_start = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)
    next_day = day_start + datetime.timedelta(days=1)
    seconds_until_reset = max(1.0, next_day.timestamp() - current_ts)
    return day_start.timestamp(), seconds_until_reset


class SQLiteBackend:
    """Async SQLite repository backed by aiosqlite."""

    backend_name = "sqlite"

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.sanitized_target = sanitize_db_target(None, db_path)
        self.schema_version = 0

    async def init_and_migrate(self) -> None:
        await migrations.run_sqlite_migrations(self.db_path)
        status = await self.get_migration_status()
        self.schema_version = int(status["current_version"])

    async def log_request(
        self,
        method: str,
        path: str,
        status_code: int,
        latency_ms: float,
        request_bytes: int = 0,
        response_bytes: int = 0,
        allowed: bool = True,
        model: Optional[str] = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
    ) -> None:
        now = time.time()
        if total_tokens == 0 and (prompt_tokens > 0 or completion_tokens > 0):
            total_tokens = prompt_tokens + completion_tokens

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO request_logs (
                    timestamp, method, path, model, prompt_tokens, completion_tokens,
                    total_tokens, status_code, latency_ms, request_bytes, response_bytes, allowed
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    method,
                    path,
                    model,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    status_code,
                    latency_ms,
                    request_bytes,
                    response_bytes,
                    1 if allowed else 0,
                ),
            )
            await db.commit()

    async def get_metrics(self) -> Dict[str, Any]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            async with db.execute(
                """
                SELECT
                    COUNT(*) as total_requests,
                    SUM(CASE WHEN allowed = 1 THEN 1 ELSE 0 END) as allowed_requests,
                    SUM(CASE WHEN allowed = 0 THEN 1 ELSE 0 END) as throttled_requests,
                    AVG(CASE WHEN allowed = 1 THEN latency_ms ELSE NULL END) as avg_latency_ms,
                    SUM(request_bytes) as total_request_bytes,
                    SUM(response_bytes) as total_response_bytes,
                    SUM(prompt_tokens) as total_prompt_tokens,
                    SUM(completion_tokens) as total_completion_tokens,
                    SUM(total_tokens) as total_tokens
                FROM request_logs
                """
            ) as cursor:
                summary = dict(await cursor.fetchone() or {})

            minute_ago = time.time() - 60.0
            async with db.execute(
                "SELECT COUNT(*) as rpm_last_minute FROM request_logs WHERE timestamp >= ?",
                (minute_ago,),
            ) as cursor:
                rpm_row = dict(await cursor.fetchone() or {})
                summary["rpm_last_minute"] = rpm_row.get("rpm_last_minute", 0)

            async with db.execute(
                """
                SELECT status_code, COUNT(*) as count
                FROM request_logs
                GROUP BY status_code
                ORDER BY count DESC
                """
            ) as cursor:
                rows = await cursor.fetchall()
                summary["status_codes"] = {row["status_code"]: row["count"] for row in rows}

            summary["recent_models"] = await self.get_recent_models_with_tokens(limit=3)
            return summary

    async def get_recent_models_with_tokens(self, limit: int = 3) -> List[str]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT model
                FROM request_logs
                WHERE model IS NOT NULL
                  AND model != ''
                  AND (prompt_tokens > 0 OR completion_tokens > 0 OR total_tokens > 0)
                GROUP BY model
                ORDER BY MAX(id) DESC
                LIMIT ?
                """,
                (limit,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [row["model"] for row in rows]

    async def get_logs(
        self,
        limit: int = 50,
        offset: int = 0,
        status_code: Optional[int] = None,
        path: Optional[str] = None,
        model: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query = """
        SELECT id, timestamp, method, path, model, prompt_tokens, completion_tokens,
               total_tokens, status_code, latency_ms, request_bytes, response_bytes, allowed
        FROM request_logs
        """
        conditions: List[str] = []
        params: List[Any] = []

        if status_code is not None:
            conditions.append("status_code = ?")
            params.append(status_code)
        if path is not None:
            conditions.append("path LIKE ?")
            params.append(f"%{path}%")
        if model is not None:
            conditions.append("model LIKE ?")
            params.append(f"%{model}%")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_daily_usage(self, now: Optional[float] = None) -> Dict[str, Any]:
        day_start_ts, seconds_until_reset = _compute_utc_day_bounds(now)
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT
                    COUNT(*) as requests,
                    COALESCE(SUM(total_tokens), 0) as tokens
                FROM request_logs
                WHERE allowed = 1 AND timestamp >= ?
                """,
                (day_start_ts,),
            ) as cursor:
                row = dict(await cursor.fetchone() or {})

        return {
            "requests": int(row.get("requests") or 0),
            "tokens": int(row.get("tokens") or 0),
            "seconds_until_reset": seconds_until_reset,
        }

    async def prune_old_logs(self, retention_days: int) -> int:
        if retention_days <= 0:
            return 0
        cutoff = time.time() - (float(retention_days) * 86400.0)
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "DELETE FROM request_logs WHERE timestamp < ?",
                (cutoff,),
            )
            await db.commit()
            return int(cursor.rowcount or 0)

    async def get_migration_status(self) -> Dict[str, Any]:
        return await migrations.get_sqlite_migration_status(self.db_path)

    async def close(self) -> None:
        return None


class PostgresBackend:
    """Async PostgreSQL repository backed by asyncpg connection pool."""

    backend_name = "postgresql"

    def __init__(
        self,
        dsn: str,
        pool_min: int = 1,
        pool_max: int = 5,
        connect_timeout: float = 10.0,
        pool: Optional[Any] = None,
    ) -> None:
        self.dsn = dsn
        self.pool_min = int(pool_min)
        self.pool_max = int(pool_max)
        self.connect_timeout = float(connect_timeout)
        self._pool = pool
        self.sanitized_target = sanitize_db_target(dsn, "")
        self.schema_version = 0

    async def _ensure_pool(self) -> Any:
        if self._pool is not None:
            return self._pool
        try:
            import asyncpg  # type: ignore
        except ImportError as exc:
            raise ConfigError(
                "PostgreSQL backend requires 'asyncpg'. Install it via 'pip install asyncpg'."
            ) from exc

        self._pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=self.pool_min,
            max_size=self.pool_max,
            timeout=self.connect_timeout,
        )
        return self._pool

    async def init_and_migrate(self) -> None:
        pool = await self._ensure_pool()
        await migrations.run_postgres_migrations(pool)
        status = await self.get_migration_status()
        self.schema_version = int(status["current_version"])

    async def log_request(
        self,
        method: str,
        path: str,
        status_code: int,
        latency_ms: float,
        request_bytes: int = 0,
        response_bytes: int = 0,
        allowed: bool = True,
        model: Optional[str] = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
    ) -> None:
        pool = await self._ensure_pool()
        now = time.time()
        if total_tokens == 0 and (prompt_tokens > 0 or completion_tokens > 0):
            total_tokens = prompt_tokens + completion_tokens

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO request_logs (
                    timestamp, method, path, model, prompt_tokens, completion_tokens,
                    total_tokens, status_code, latency_ms, request_bytes, response_bytes, allowed
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """,
                now,
                method,
                path,
                model,
                prompt_tokens,
                completion_tokens,
                total_tokens,
                status_code,
                latency_ms,
                request_bytes,
                response_bytes,
                1 if allowed else 0,
            )

    async def get_metrics(self) -> Dict[str, Any]:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS total_requests,
                    SUM(CASE WHEN allowed = 1 THEN 1 ELSE 0 END) AS allowed_requests,
                    SUM(CASE WHEN allowed = 0 THEN 1 ELSE 0 END) AS throttled_requests,
                    AVG(CASE WHEN allowed = 1 THEN latency_ms ELSE NULL END) AS avg_latency_ms,
                    SUM(request_bytes) AS total_request_bytes,
                    SUM(response_bytes) AS total_response_bytes,
                    SUM(prompt_tokens) AS total_prompt_tokens,
                    SUM(completion_tokens) AS total_completion_tokens,
                    SUM(total_tokens) AS total_tokens
                FROM request_logs
                """
            )
            summary = dict(row or {})

            minute_ago = time.time() - 60.0
            rpm_val = await conn.fetchval(
                "SELECT COUNT(*) FROM request_logs WHERE timestamp >= $1",
                minute_ago,
            )
            summary["rpm_last_minute"] = int(rpm_val or 0)

            status_rows = await conn.fetch(
                """
                SELECT status_code, COUNT(*) AS count
                FROM request_logs
                GROUP BY status_code
                ORDER BY count DESC
                """
            )
            summary["status_codes"] = {int(r["status_code"]): int(r["count"]) for r in status_rows}

        summary["recent_models"] = await self.get_recent_models_with_tokens(limit=3)
        return summary

    async def get_recent_models_with_tokens(self, limit: int = 3) -> List[str]:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT model
                FROM request_logs
                WHERE model IS NOT NULL
                  AND model != ''
                  AND (prompt_tokens > 0 OR completion_tokens > 0 OR total_tokens > 0)
                GROUP BY model
                ORDER BY MAX(id) DESC
                LIMIT $1
                """,
                limit,
            )
            return [str(r["model"]) for r in rows]

    async def get_logs(
        self,
        limit: int = 50,
        offset: int = 0,
        status_code: Optional[int] = None,
        path: Optional[str] = None,
        model: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        pool = await self._ensure_pool()
        query = """
        SELECT id, timestamp, method, path, model, prompt_tokens, completion_tokens,
               total_tokens, status_code, latency_ms, request_bytes, response_bytes, allowed
        FROM request_logs
        """
        conditions: List[str] = []
        params: List[Any] = []
        idx = 1

        if status_code is not None:
            conditions.append(f"status_code = ${idx}")
            params.append(status_code)
            idx += 1
        if path is not None:
            conditions.append(f"path ILIKE ${idx}")
            params.append(f"%{path}%")
            idx += 1
        if model is not None:
            conditions.append(f"model ILIKE ${idx}")
            params.append(f"%{model}%")
            idx += 1

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" ORDER BY id DESC LIMIT ${idx} OFFSET ${idx + 1}"
        params.extend([limit, offset])

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(r) for r in rows]

    async def get_daily_usage(self, now: Optional[float] = None) -> Dict[str, Any]:
        day_start_ts, seconds_until_reset = _compute_utc_day_bounds(now)
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS requests,
                    COALESCE(SUM(total_tokens), 0) AS tokens
                FROM request_logs
                WHERE allowed = 1 AND timestamp >= $1
                """,
                day_start_ts,
            )
            data = dict(row or {})

        return {
            "requests": int(data.get("requests") or 0),
            "tokens": int(data.get("tokens") or 0),
            "seconds_until_reset": seconds_until_reset,
        }

    async def prune_old_logs(self, retention_days: int) -> int:
        if retention_days <= 0:
            return 0
        cutoff = time.time() - (float(retention_days) * 86400.0)
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM request_logs WHERE timestamp < $1",
                cutoff,
            )
            if isinstance(result, str) and result.startswith("DELETE "):
                try:
                    return int(result.split()[-1])
                except ValueError:
                    return 0
            return 0

    async def get_migration_status(self) -> Dict[str, Any]:
        pool = await self._ensure_pool()
        return await migrations.get_postgres_migration_status(pool)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None


def create_backend(
    db_url: Optional[str],
    db_path: str,
    pool_min: int = 1,
    pool_max: int = 5,
    connect_timeout: float = 10.0,
) -> DatabaseBackend:
    """Instantiate SQLiteBackend or PostgresBackend from configuration."""
    raw_url = (db_url or "").strip()
    if not raw_url:
        return SQLiteBackend(db_path=db_path)

    parsed = urlparse(raw_url)
    scheme = parsed.scheme.lower()
    if scheme in ("postgresql", "postgres"):
        return PostgresBackend(
            dsn=raw_url,
            pool_min=pool_min,
            pool_max=pool_max,
            connect_timeout=connect_timeout,
        )
    if scheme == "sqlite":
        if raw_url.startswith("sqlite:///"):
            sqlite_path = raw_url[len("sqlite:///"):]
        else:
            sqlite_path = parsed.path or db_path
        if not sqlite_path:
            sqlite_path = db_path
        return SQLiteBackend(db_path=sqlite_path)

    raise ConfigError(
        f"Unsupported DB_URL scheme '{scheme}': expected 'sqlite:///' or 'postgresql://'."
    )


# --- Backward-Compatible SQLite Helper Functions ---
async def init_db(db_path: str) -> None:
    await SQLiteBackend(db_path).init_and_migrate()


async def log_request(
    db_path: str,
    method: str,
    path: str,
    status_code: int,
    latency_ms: float,
    request_bytes: int = 0,
    response_bytes: int = 0,
    allowed: bool = True,
    model: Optional[str] = None,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    total_tokens: int = 0,
) -> None:
    await SQLiteBackend(db_path).log_request(
        method=method,
        path=path,
        status_code=status_code,
        latency_ms=latency_ms,
        request_bytes=request_bytes,
        response_bytes=response_bytes,
        allowed=allowed,
        model=model,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
    )


async def get_metrics(db_path: str) -> Dict[str, Any]:
    return await SQLiteBackend(db_path).get_metrics()


async def get_recent_models_with_tokens(db_path: str, limit: int = 3) -> List[str]:
    return await SQLiteBackend(db_path).get_recent_models_with_tokens(limit=limit)


async def get_logs(
    db_path: str,
    limit: int = 50,
    offset: int = 0,
    status_code: Optional[int] = None,
    path: Optional[str] = None,
    model: Optional[str] = None,
) -> List[Dict[str, Any]]:
    return await SQLiteBackend(db_path).get_logs(
        limit=limit,
        offset=offset,
        status_code=status_code,
        path=path,
        model=model,
    )


async def get_daily_usage(db_path: str, now: Optional[float] = None) -> Dict[str, Any]:
    return await SQLiteBackend(db_path).get_daily_usage(now=now)


async def prune_old_logs(db_path: str, retention_days: int) -> int:
    return await SQLiteBackend(db_path).prune_old_logs(retention_days=retention_days)
