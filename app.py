import asyncio
import json
import os
import re
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, Tuple

import httpx
import yaml
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from starlette.requests import ClientDisconnect

import database

CONFIG_FILE = os.getenv("CONFIG_FILE", "config.yaml")
WWWROOT_DIR = os.getenv("WWWROOT_DIR", "wwwroot")


def extract_tokens(text: str) -> Tuple[int, int, int]:
    """Extract prompt, completion, and total tokens from JSON or SSE streaming response text."""
    prompt_tokens = 0
    completion_tokens = 0
    total_tokens = 0
    if not text:
        return 0, 0, 0
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "usage" in data and isinstance(data["usage"], dict):
            u = data["usage"]
            prompt_tokens = int(u.get("prompt_tokens") or 0)
            completion_tokens = int(u.get("completion_tokens") or 0)
            total_tokens = int(u.get("total_tokens") or (prompt_tokens + completion_tokens))
            return prompt_tokens, completion_tokens, total_tokens
    except Exception:
        pass

    p_match = re.findall(r'"prompt_tokens"\s*:\s*(\d+)', text)
    if p_match:
        prompt_tokens = int(p_match[-1])
    c_match = re.findall(r'"completion_tokens"\s*:\s*(\d+)', text)
    if c_match:
        completion_tokens = int(c_match[-1])
    t_match = re.findall(r'"total_tokens"\s*:\s*(\d+)', text)
    if t_match:
        total_tokens = int(t_match[-1])
    elif prompt_tokens or completion_tokens:
        total_tokens = prompt_tokens + completion_tokens

    return prompt_tokens, completion_tokens, total_tokens

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


@app.exception_handler(ClientDisconnect)
async def client_disconnect_handler(request: Request, exc: ClientDisconnect):
    return Response(status_code=499)


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
async def admin_logs(
    limit: int = 50,
    offset: int = 0,
    status_code: Optional[int] = None,
    path: Optional[str] = None,
    model: Optional[str] = None,
):
    db_path = config_data["db"]["path"]
    logs = await database.get_logs(
        db_path,
        limit=limit,
        offset=offset,
        status_code=status_code,
        path=path,
        model=model,
    )
    return {"logs": logs, "limit": limit, "offset": offset}


@app.get("/admin/config", tags=["Admin"])
async def get_admin_config():
    return config_data


@app.post("/admin/config", tags=["Admin"])
async def update_admin_config(new_config: Dict[str, Any]):
    global config_data, concurrency_semaphore
    try:
        if "admin" in new_config:
            config_data["admin"].update(new_config["admin"])
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
    """Renders the Web Dashboard from wwwroot/index.html."""
    index_file = os.path.join(WWWROOT_DIR, "index.html")
    if os.path.exists(index_file):
        with open(index_file, "r", encoding="utf-8") as f:
            content = f.read()
            title = config_data.get("admin", {}).get("title", "NVIDIA NIM Rate Limiter Dashboard")
            content = content.replace("{{ ADMIN_TITLE }}", title)
            return HTMLResponse(content=content)
    return HTMLResponse(
        content="<h1>Admin Dashboard</h1><p>wwwroot/index.html not found.</p>",
        status_code=404,
    )


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
    try:
        await token_bucket.acquire()
    except (ClientDisconnect, asyncio.CancelledError):
        return Response(status_code=499)

    # 2. Concurrency Control & Proxying
    async with concurrency_semaphore:
        try:
            body = await request.body()
        except (ClientDisconnect, asyncio.CancelledError):
            return Response(status_code=499)

        model_name = None
        if body:
            try:
                body_json = json.loads(body.decode("utf-8"))
                if isinstance(body_json, dict):
                    model_name = body_json.get("model")
            except Exception:
                pass

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
                collected_chunks = []
                try:
                    async for chunk in upstream_response.aiter_bytes():
                        bytes_count += len(chunk)
                        if len(collected_chunks) < 40 or b"usage" in chunk or b"prompt_tokens" in chunk:
                            collected_chunks.append(chunk)
                        yield chunk
                except (ClientDisconnect, asyncio.CancelledError):
                    pass
                finally:
                    await upstream_response.aclose()
                    latency_ms = (time.time() - start_time) * 1000.0
                    response_text = b"".join(collected_chunks).decode("utf-8", errors="ignore")
                    prompt_tokens, completion_tokens, total_tokens = extract_tokens(response_text)
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
                            model=model_name,
                            prompt_tokens=prompt_tokens,
                            completion_tokens=completion_tokens,
                            total_tokens=total_tokens,
                        )
                    )

            return StreamingResponse(
                stream_generator(),
                status_code=upstream_response.status_code,
                headers=response_headers,
                media_type=upstream_response.headers.get("content-type"),
            )

        except (ClientDisconnect, asyncio.CancelledError):
            return Response(status_code=499)
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
                    model=model_name,
                )
            )
            return JSONResponse(
                status_code=502,
                content={"error": "Bad Gateway", "details": str(e)},
            )
