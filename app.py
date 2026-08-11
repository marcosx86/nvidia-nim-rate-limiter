import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import httpx
import yaml
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

import database

CONFIG_FILE = os.getenv("CONFIG_FILE", "config.yaml")

# --- Configuration Management ---
def load_config() -> Dict[str, Any]:
    config: Dict[str, Any] = {
        "proxy": {
            "upstream_url": os.getenv("UPSTREAM_URL", "https://integrate.api.nvidia.com"),
            "concurrency_limit": int(os.getenv("MAX_CONCURRENT", "1")),
        },
        "rate_limit": {
            "requests_per_minute": float(os.getenv("RPM", "6.0")),
            "window_seconds": float(os.getenv("WINDOW_SECONDS", "60.0")),
            "burst_capacity": float(os.getenv("BURST_CAPACITY", "6.0")),
        },
        "db": {
            "path": os.getenv("DB_PATH", "limiter.db"),
        },
        "admin": {
            "title": "NVIDIA NIM Rate Limiter Dashboard",
        },
    }

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                file_config = yaml.safe_load(f) or {}
                # Merge file config
                if "proxy" in file_config:
                    config["proxy"].update(file_config["proxy"])
                if "rate_limit" in file_config:
                    config["rate_limit"].update(file_config["rate_limit"])
                if "db" in file_config:
                    config["db"].update(file_config["db"])
                if "admin" in file_config:
                    config["admin"].update(file_config["admin"])
        except Exception as e:
            print(f"Warning: Failed to load {CONFIG_FILE}: {e}")

    # Ensure config file exists with defaults if not present
    if not os.path.exists(CONFIG_FILE):
        save_config(config)

    return config


def save_config(config: Dict[str, Any]) -> None:
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            yaml.safe_dump(config, f, default_flow_style=False)
    except Exception as e:
        print(f"Error saving configuration to {CONFIG_FILE}: {e}")


# --- Token Bucket Rate Limiter ---
class TokenBucket:
    def __init__(self, rpm: float, window_seconds: float, capacity: float):
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.fill_rate = float(rpm) / float(window_seconds) if window_seconds > 0 else 1.0
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    def update_params(self, rpm: float, window_seconds: float, capacity: float):
        self.capacity = float(capacity)
        self.fill_rate = float(rpm) / float(window_seconds) if window_seconds > 0 else 1.0
        self.tokens = min(self.tokens, self.capacity)

    async def acquire(self) -> float:
        """Acquires a token, sleeping if necessary. Returns wait time in seconds."""
        wait_time = 0.0
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.last_update = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return wait_time

                # Calculate wait time needed for 1 full token
                needed = 1.0 - self.tokens
                wait_seconds = needed / self.fill_rate

            wait_time += wait_seconds
            await asyncio.sleep(wait_seconds)


# --- Global State ---
config_data = load_config()
NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "")

token_bucket = TokenBucket(
    rpm=config_data["rate_limit"]["requests_per_minute"],
    window_seconds=config_data["rate_limit"]["window_seconds"],
    capacity=config_data["rate_limit"]["burst_capacity"],
)

concurrency_semaphore = asyncio.Semaphore(
    config_data["proxy"]["concurrency_limit"]
)

http_client: Optional[httpx.AsyncClient] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    # Init DB schema
    db_path = config_data["db"]["path"]
    await database.init_db(db_path)

    # Init HTTP Client
    http_client = httpx.AsyncClient(
        base_url=config_data["proxy"]["upstream_url"],
        timeout=httpx.Timeout(
            connect=10.0,
            read=300.0,
            write=300.0,
            pool=300.0,
        ),
    )
    yield
    if http_client:
        await http_client.aclose()


app = FastAPI(
    title="NVIDIA NIM Rate Limiter Proxy",
    description="Rate Limiting and Observability Reverse Proxy for NVIDIA NIM APIs",
    lifespan=lifespan,
)


# --- Public Operational Endpoints ---
@app.get("/healthz", tags=["System"])
async def healthz():
    return {"status": "ok", "timestamp": time.time()}


@app.get("/status", tags=["System"])
async def status():
    return {
        "status": "healthy",
        "rate_limiter": {
            "tokens_available": round(token_bucket.tokens, 2),
            "capacity": token_bucket.capacity,
            "fill_rate_per_sec": round(token_bucket.fill_rate, 4),
        },
        "concurrency": {
            "limit": config_data["proxy"]["concurrency_limit"],
            "active_tasks": config_data["proxy"]["concurrency_limit"] - concurrency_semaphore._value,
        },
        "config": config_data,
    }


# --- Admin Dashboard & Metrics API ---
@app.get("/admin/metrics", tags=["Admin"])
async def admin_metrics():
    db_path = config_data["db"]["path"]
    metrics = await database.get_metrics(db_path)
    metrics["current_tokens"] = round(token_bucket.tokens, 2)
    metrics["capacity"] = token_bucket.capacity
    metrics["active_concurrency"] = config_data["proxy"]["concurrency_limit"] - concurrency_semaphore._value
    return metrics


@app.get("/admin/logs", tags=["Admin"])
async def admin_logs(limit: int = 50, offset: int = 0, status_code: Optional[int] = None, path: Optional[str] = None):
    db_path = config_data["db"]["path"]
    logs = await database.get_logs(db_path, limit=limit, offset=offset, status_code=status_code, path=path)
    return {"logs": logs, "limit": limit, "offset": offset}


@app.get("/admin/config", tags=["Admin"])
async def get_admin_config():
    return config_data


@app.post("/admin/config", tags=["Admin"])
async def update_admin_config(new_config: Dict[str, Any]):
    global config_data, concurrency_semaphore
    try:
        if "proxy" in new_config:
            config_data["proxy"].update(new_config["proxy"])
        if "rate_limit" in new_config:
            config_data["rate_limit"].update(new_config["rate_limit"])
        if "db" in new_config:
            config_data["db"].update(new_config["db"])

        save_config(config_data)

        # Update in-memory structures
        token_bucket.update_params(
            rpm=config_data["rate_limit"]["requests_per_minute"],
            window_seconds=config_data["rate_limit"]["window_seconds"],
            capacity=config_data["rate_limit"]["burst_capacity"],
        )
        concurrency_semaphore = asyncio.Semaphore(config_data["proxy"]["concurrency_limit"])

        return {"status": "updated", "config": config_data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to update configuration: {str(e)}")


@app.get("/admin", response_class=HTMLResponse, tags=["Admin"])
@app.get("/admin/", response_class=HTMLResponse, tags=["Admin"])
async def admin_dashboard():
    """Renders a modern, responsive Web Dashboard for observing NIM proxy metrics and configuring settings."""
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NVIDIA NIM Proxy Admin Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #0d1117;
            --card-bg: #161b22;
            --border: #30363d;
            --text: #c9d1d9;
            --text-muted: #8b949e;
            --accent: #76b900; /* NVIDIA Green */
            --accent-hover: #5c9300;
            --danger: #f85149;
            --warning: #d29922;
            --blue: #58a6ff;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background-color: var(--bg);
            color: var(--text);
            padding: 24px;
            line-height: 1.5;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--border);
            margin-bottom: 24px;
        }
        .header h1 {
            font-size: 1.5rem;
            color: #fff;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .badge {
            background-color: rgba(118, 185, 0, 0.15);
            color: var(--accent);
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.8rem;
            border: 1px solid rgba(118, 185, 0, 0.4);
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }
        .card {
            background-color: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 20px;
        }
        .card-title {
            font-size: 0.85rem;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }
        .card-value {
            font-size: 1.8rem;
            font-weight: 700;
            color: #fff;
        }
        .card-sub {
            font-size: 0.8rem;
            color: var(--text-muted);
            margin-top: 4px;
        }
        .section {
            background-color: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 24px;
        }
        .section-title {
            font-size: 1.1rem;
            color: #fff;
            margin-bottom: 16px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }
        th, td {
            padding: 10px 12px;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }
        th {
            color: var(--text-muted);
            font-weight: 500;
            background-color: rgba(255,255,255,0.02);
        }
        tr:hover {
            background-color: rgba(255,255,255,0.02);
        }
        .status-tag {
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        .status-2xx { background: rgba(46, 160, 67, 0.2); color: #3fb950; }
        .status-4xx { background: rgba(210, 153, 34, 0.2); color: #d29922; }
        .status-5xx { background: rgba(248, 81, 73, 0.2); color: #f85149; }
        
        .form-group {
            margin-bottom: 14px;
        }
        .form-group label {
            display: block;
            font-size: 0.85rem;
            color: var(--text-muted);
            margin-bottom: 6px;
        }
        .form-group input {
            width: 100%;
            padding: 8px 12px;
            background-color: var(--bg);
            border: 1px solid var(--border);
            border-radius: 6px;
            color: #fff;
            font-size: 0.9rem;
        }
        .btn {
            background-color: var(--accent);
            color: #000;
            font-weight: 600;
            border: none;
            padding: 10px 18px;
            border-radius: 6px;
            cursor: pointer;
            transition: background-color 0.2s;
        }
        .btn:hover {
            background-color: var(--accent-hover);
        }
        .flex-container {
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 24px;
        }
        @media (max-width: 900px) {
            .flex-container { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>

    <div class="header">
        <h1>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/></svg>
            NVIDIA NIM Rate Limiter
        </h1>
        <span class="badge" id="statusBadge">Active</span>
    </div>

    <div class="grid">
        <div class="card">
            <div class="card-title">Token Bucket Tokens</div>
            <div class="card-value" id="valTokens">-</div>
            <div class="card-sub" id="valCapacity">Capacity: -</div>
        </div>
        <div class="card">
            <div class="card-title">Total Requests</div>
            <div class="card-value" id="valTotalReq">-</div>
            <div class="card-sub" id="valRpmLastMin">RPM (Last Min): -</div>
        </div>
        <div class="card">
            <div class="card-title">Avg Latency</div>
            <div class="card-value" id="valAvgLatency">- ms</div>
            <div class="card-sub">Proxied Responses</div>
        </div>
        <div class="card">
            <div class="card-title">Active Concurrency</div>
            <div class="card-value" id="valConcurrency">-</div>
            <div class="card-sub" id="valMaxConcurrency">Max Allowed: -</div>
        </div>
    </div>

    <div class="flex-container">
        <div class="section">
            <div class="section-title">
                <span>Recent Request Logs</span>
                <button class="btn" style="padding: 4px 10px; font-size: 0.8rem;" onclick="fetchData()">Refresh</button>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Time</th>
                        <th>Method</th>
                        <th>Path</th>
                        <th>Status</th>
                        <th>Latency</th>
                    </tr>
                </thead>
                <tbody id="logsBody">
                    <tr><td colspan="5">Loading request logs...</td></tr>
                </tbody>
            </table>
        </div>

        <div class="section">
            <div class="section-title">Rate Limit Config</div>
            <form id="configForm" onsubmit="saveConfig(event)">
                <div class="form-group">
                    <label>Requests Per Minute (RPM)</label>
                    <input type="number" step="0.1" id="cfgRpm" required>
                </div>
                <div class="form-group">
                    <label>Window Seconds</label>
                    <input type="number" step="0.1" id="cfgWindow" required>
                </div>
                <div class="form-group">
                    <label>Burst Capacity</label>
                    <input type="number" step="0.1" id="cfgBurst" required>
                </div>
                <div class="form-group">
                    <label>Concurrency Limit</label>
                    <input type="number" id="cfgConcurrency" required>
                </div>
                <div class="form-group">
                    <label>Upstream URL</label>
                    <input type="text" id="cfgUpstream" required>
                </div>
                <button type="submit" class="btn" style="width: 100%;">Save & Apply Settings</button>
            </form>
        </div>
    </div>

    <script>
        async function fetchData() {
            try {
                const [metricsRes, logsRes, configRes] = await Promise.all([
                    fetch('/admin/metrics'),
                    fetch('/admin/logs?limit=10'),
                    fetch('/admin/config')
                ]);
                const metrics = await metricsRes.json();
                const logsData = await logsRes.json();
                const config = await configRes.json();

                document.getElementById('valTokens').innerText = metrics.current_tokens ?? '-';
                document.getElementById('valCapacity').innerText = 'Capacity: ' + (metrics.capacity ?? '-');
                document.getElementById('valTotalReq').innerText = metrics.total_requests ?? 0;
                document.getElementById('valRpmLastMin').innerText = 'RPM (Last Min): ' + (metrics.rpm_last_minute ?? 0);
                document.getElementById('valAvgLatency').innerText = metrics.avg_latency_ms ? Math.round(metrics.avg_latency_ms) + ' ms' : '0 ms';
                document.getElementById('valConcurrency').innerText = metrics.active_concurrency ?? 0;
                document.getElementById('valMaxConcurrency').innerText = 'Max Allowed: ' + (config.proxy?.concurrency_limit ?? '-');

                // Fill Config
                document.getElementById('cfgRpm').value = config.rate_limit?.requests_per_minute ?? 6;
                document.getElementById('cfgWindow').value = config.rate_limit?.window_seconds ?? 60;
                document.getElementById('cfgBurst').value = config.rate_limit?.burst_capacity ?? 6;
                document.getElementById('cfgConcurrency').value = config.proxy?.concurrency_limit ?? 1;
                document.getElementById('cfgUpstream').value = config.proxy?.upstream_url ?? '';

                // Fill Logs
                const tbody = document.getElementById('logsBody');
                tbody.innerHTML = '';
                if (!logsData.logs || logsData.logs.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="5">No request logs recorded yet.</td></tr>';
                } else {
                    logsData.logs.forEach(log => {
                        const tr = document.createElement('tr');
                        const dateStr = new Date(log.timestamp * 1000).toLocaleTimeString();
                        let statusClass = 'status-2xx';
                        if (log.status_code >= 400 && log.status_code < 500) statusClass = 'status-4xx';
                        if (log.status_code >= 500) statusClass = 'status-5xx';

                        tr.innerHTML = `
                            <td>${dateStr}</td>
                            <td><strong>${log.method}</strong></td>
                            <td>${log.path}</td>
                            <td><span class="status-tag ${statusClass}">${log.status_code}</span></td>
                            <td>${Math.round(log.latency_ms)} ms</td>
                        `;
                        tbody.appendChild(tr);
                    });
                }
            } catch (err) {
                console.error('Error fetching admin data:', err);
            }
        }

        async function saveConfig(e) {
            e.preventDefault();
            const payload = {
                rate_limit: {
                    requests_per_minute: parseFloat(document.getElementById('cfgRpm').value),
                    window_seconds: parseFloat(document.getElementById('cfgWindow').value),
                    burst_capacity: parseFloat(document.getElementById('cfgBurst').value)
                },
                proxy: {
                    concurrency_limit: parseInt(document.getElementById('cfgConcurrency').value, 10),
                    upstream_url: document.getElementById('cfgUpstream').value
                }
            };
            const res = await fetch('/admin/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (res.ok) {
                alert('Configuration updated successfully!');
                fetchData();
            } else {
                alert('Error updating configuration.');
            }
        }

        fetchData();
        setInterval(fetchData, 5000);
    </script>
</body>
</html>"""
    return html_content


# --- Main Proxy Middleware & Handler ---
@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def proxy_request(path: str, request: Request):
    # Bypass system / admin paths
    if path in ["healthz", "status"] or path.startswith("admin"):
        raise HTTPException(status_code=404, detail="Not Found")

    start_time = time.time()
    db_path = config_data["db"]["path"]

    # 1. Rate Limiting Wait
    await token_bucket.acquire()

    # 2. Concurrency Control & Proxying
    async with concurrency_semaphore:
        body = await request.body()
        headers = dict(request.headers)

        # Never forward client's Authorization header
        headers.pop("authorization", None)

        # Inject upstream NVIDIA credential
        api_key = os.getenv("NVIDIA_API_KEY", NVIDIA_API_KEY)
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        # Strip headers generated by client socket connection
        for hop_header in ("host", "content-length", "connection", "transfer-encoding"):
            headers.pop(hop_header, None)

        upstream_path = "/" + path
        if request.url.query:
            upstream_path += "?" + request.url.query

        if http_client is None:
            raise HTTPException(status_code=500, detail="HTTP client not initialized")

        try:
            req_builder = http_client.build_request(
                method=request.method,
                url=upstream_path,
                headers=headers,
                content=body,
            )
            upstream_response = await http_client.send(req_builder, stream=True)

            response_headers = dict(upstream_response.headers)
            for h in ("content-length", "transfer-encoding", "connection"):
                response_headers.pop(h, None)

            async def stream_generator():
                bytes_count = 0
                try:
                    async for chunk in upstream_response.aiter_bytes():
                        bytes_count += len(chunk)
                        yield chunk
                finally:
                    await upstream_response.aclose()
                    latency_ms = (time.time() - start_time) * 1000.0
                    asyncio.create_task(
                        database.log_request(
                            db_path=db_path,
                            method=request.method,
                            path="/" + path,
                            status_code=upstream_response.status_code,
                            latency_ms=latency_ms,
                            request_bytes=len(body),
                            response_bytes=bytes_count,
                            allowed=True,
                        )
                    )

            return StreamingResponse(
                stream_generator(),
                status_code=upstream_response.status_code,
                headers=response_headers,
                media_type=upstream_response.headers.get("content-type"),
            )

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000.0
            asyncio.create_task(
                database.log_request(
                    db_path=db_path,
                    method=request.method,
                    path="/" + path,
                    status_code=502,
                    latency_ms=latency_ms,
                    request_bytes=len(body),
                    response_bytes=0,
                    allowed=True,
                )
            )
            return JSONResponse(
                status_code=502,
                content={"error": "Bad Gateway", "details": str(e)},
            )
