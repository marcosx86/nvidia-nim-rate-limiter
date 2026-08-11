import asyncio
import time
from typing import Any, Dict, List, Optional
import aiosqlite


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS request_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    latency_ms REAL NOT NULL,
    request_bytes INTEGER NOT NULL DEFAULT 0,
    response_bytes INTEGER NOT NULL DEFAULT 0,
    allowed INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_timestamp ON request_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_path ON request_logs(path);
CREATE INDEX IF NOT EXISTS idx_status_code ON request_logs(status_code);
"""


async def init_db(db_path: str) -> None:
    """Initialize the SQLite database schema."""
    async with aiosqlite.connect(db_path) as db:
        await db.executescript(CREATE_TABLE_SQL)
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
) -> None:
    """Record a proxy request log entry asynchronously."""
    now = time.time()
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """
            INSERT INTO request_logs (timestamp, method, path, status_code, latency_ms, request_bytes, response_bytes, allowed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                method,
                path,
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

        # Total count & latency stats
        async with db.execute(
            """
            SELECT 
                COUNT(*) as total_requests,
                SUM(CASE WHEN allowed = 1 THEN 1 ELSE 0 END) as allowed_requests,
                SUM(CASE WHEN allowed = 0 THEN 1 ELSE 0 END) as throttled_requests,
                AVG(CASE WHEN allowed = 1 THEN latency_ms ELSE NULL END) as avg_latency_ms,
                SUM(request_bytes) as total_request_bytes,
                SUM(response_bytes) as total_response_bytes
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

        return summary


async def get_logs(
    db_path: str,
    limit: int = 50,
    offset: int = 0,
    status_code: Optional[int] = None,
    path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Retrieve paginated request logs."""
    query = "SELECT id, timestamp, method, path, status_code, latency_ms, request_bytes, response_bytes, allowed FROM request_logs"
    conditions = []
    params: List[Any] = []

    if status_code is not None:
        conditions.append("status_code = ?")
        params.append(status_code)

    if path is not None:
        conditions.append("path LIKE ?")
        params.append(f"%{path}%")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
