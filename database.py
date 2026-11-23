import asyncio
import datetime
import time
from typing import Any, Dict, List, Optional
import aiosqlite


CREATE_TABLE_SQL = """
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
);
"""

CREATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_timestamp ON request_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_path ON request_logs(path);
CREATE INDEX IF NOT EXISTS idx_model ON request_logs(model);
CREATE INDEX IF NOT EXISTS idx_status_code ON request_logs(status_code);
"""


async def init_db(db_path: str) -> None:
    """Initialize the SQLite database schema and migrate existing tables if needed."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute(CREATE_TABLE_SQL)

        # Ensure schema migrations for existing databases
        async with db.execute("PRAGMA table_info(request_logs)") as cursor:
            columns = {row[1] for row in await cursor.fetchall()}

        if "model" not in columns:
            await db.execute("ALTER TABLE request_logs ADD COLUMN model TEXT")
        if "prompt_tokens" not in columns:
            await db.execute("ALTER TABLE request_logs ADD COLUMN prompt_tokens INTEGER NOT NULL DEFAULT 0")
        if "completion_tokens" not in columns:
            await db.execute("ALTER TABLE request_logs ADD COLUMN completion_tokens INTEGER NOT NULL DEFAULT 0")
        if "total_tokens" not in columns:
            await db.execute("ALTER TABLE request_logs ADD COLUMN total_tokens INTEGER NOT NULL DEFAULT 0")

        await db.executescript(CREATE_INDEXES_SQL)
        await db.commit()


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
    """Record a proxy request log entry asynchronously."""
    now = time.time()
    if total_tokens == 0 and (prompt_tokens > 0 or completion_tokens > 0):
        total_tokens = prompt_tokens + completion_tokens

    async with aiosqlite.connect(db_path) as db:
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


async def get_metrics(db_path: str) -> Dict[str, Any]:
    """Calculate usage metrics and summary statistics from SQLite logs."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row

        # Total count, tokens & latency stats
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

        # Requests in the last 60 seconds (current RPM activity)
        now = time.time()
        minute_ago = now - 60.0
        async with db.execute(
            "SELECT COUNT(*) as rpm_last_minute FROM request_logs WHERE timestamp >= ?",
            (minute_ago,),
        ) as cursor:
            rpm_row = dict(await cursor.fetchone() or {})
            summary["rpm_last_minute"] = rpm_row.get("rpm_last_minute", 0)

        # Status code breakdown
        async with db.execute(
            """
            SELECT status_code, COUNT(*) as count 
            FROM request_logs 
            GROUP BY status_code 
            ORDER BY count DESC
            """
        ) as cursor:
            rows = await cursor.fetchall()
            status_breakdown = {row["status_code"]: row["count"] for row in rows}
            summary["status_codes"] = status_breakdown

        # Last 3 used models with tokens
        async with db.execute(
            """
            SELECT model
            FROM request_logs
            WHERE model IS NOT NULL 
              AND model != '' 
              AND (prompt_tokens > 0 OR completion_tokens > 0 OR total_tokens > 0)
            GROUP BY model
            ORDER BY MAX(id) DESC
            LIMIT 3
            """
        ) as cursor:
            rows = await cursor.fetchall()
            summary["recent_models"] = [row["model"] for row in rows]

        return summary


async def get_recent_models_with_tokens(db_path: str, limit: int = 3) -> List[str]:
    """Retrieve the last N distinct models that had tokens recorded."""
    async with aiosqlite.connect(db_path) as db:
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
    db_path: str,
    limit: int = 50,
    offset: int = 0,
    status_code: Optional[int] = None,
    path: Optional[str] = None,
    model: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Retrieve paginated request logs with model and token counts."""
    query = """
    SELECT id, timestamp, method, path, model, prompt_tokens, completion_tokens,
           total_tokens, status_code, latency_ms, request_bytes, response_bytes, allowed 
    FROM request_logs
    """
    conditions = []
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

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_daily_usage(db_path: str, now: Optional[float] = None) -> Dict[str, Any]:
    """Return allowed request count, total tokens, and seconds until UTC midnight reset."""
    current_ts = time.time() if now is None else float(now)
    utc_now = datetime.datetime.fromtimestamp(current_ts, tz=datetime.timezone.utc)
    day_start = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)
    next_day = day_start + datetime.timedelta(days=1)
    seconds_until_reset = max(1.0, next_day.timestamp() - current_ts)

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT
                COUNT(*) as requests,
                COALESCE(SUM(total_tokens), 0) as tokens
            FROM request_logs
            WHERE allowed = 1 AND timestamp >= ?
            """,
            (day_start.timestamp(),),
        ) as cursor:
            row = dict(await cursor.fetchone() or {})

    return {
        "requests": int(row.get("requests") or 0),
        "tokens": int(row.get("tokens") or 0),
        "seconds_until_reset": seconds_until_reset,
    }
