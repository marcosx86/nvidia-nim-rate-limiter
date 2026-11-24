import asyncio
import json
import math
import os
import re
import sys
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from starlette.requests import ClientDisconnect

import database
from rate_limiter import (
    RateLimitExceeded,
    RateLimitManager,
    TokenBucket,
    estimate_request_tokens,
)
from routing import ModelCatalogCache, ModelRouter, detect_deployment_mode
from settings import (
    AppSettings,
    ConfigError,
    build_arg_parser,
    build_settings_source_metadata,
    normalize_admin_config_patch,
    resolve_settings,
    validate_bind_guardrails,
    validate_db_yaml,
    validate_model_list,
    validate_model_rate_limits,
    validate_model_routes,
)


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
def save_config(config: Dict[str, Any], config_file: Optional[str] = None) -> None:
    target = config_file or CONFIG_FILE
    try:
        parent = os.path.dirname(target)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(target, "w", encoding="utf-8") as f:
            yaml.safe_dump(config, f, default_flow_style=False)
    except Exception as e:
        print(f"Error saving configuration to {target}: {e}")


def load_config(
    argv: Optional[Sequence[str]] = None,
    environ: Optional[Mapping[str, str]] = None,
    *,
    require_secrets: bool = False,
) -> Dict[str, Any]:
    """Load non-secret runtime configuration using strict precedence:
    argparse > environment var > dotenv > config.yaml > default.
    """
    resolved = resolve_settings(
        argv=argv,
        environ=environ,
        allow_yaml_fallback=True,
        require_secrets=require_secrets,
    )
    cfg = resolved.to_config_dict()
    if not os.path.exists(resolved.config_file):
        save_config(cfg, resolved.config_file)
    return cfg


# --- Global State ---
runtime_settings: AppSettings = resolve_settings(
    argv=[],
    allow_yaml_fallback=True,
    require_secrets=False,
)
CONFIG_FILE: str = runtime_settings.config_file
WWWROOT_DIR: str = runtime_settings.wwwroot_dir
config_data: Dict[str, Any] = runtime_settings.to_config_dict()
NVIDIA_API_KEY: str = runtime_settings.nvidia_api_key
_cli_configured: bool = False

rate_limit_manager = RateLimitManager.from_config(config_data)
token_bucket: TokenBucket = rate_limit_manager.global_rpm
concurrency_semaphore: asyncio.Semaphore = rate_limit_manager.global_semaphore
model_router = ModelRouter.from_config(config_data, env_file=runtime_settings.env_file)
catalog_cache = ModelCatalogCache(ttl_seconds=runtime_settings.catalog_cache_ttl)
db_backend: database.DatabaseBackend = database.create_backend(
    db_url=runtime_settings.db_url,
    db_path=runtime_settings.db_path,
    pool_min=runtime_settings.db_pool_min,
    pool_max=runtime_settings.db_pool_max,
    connect_timeout=runtime_settings.db_connect_timeout,
)

http_client: Optional[httpx.AsyncClient] = None


def configure_runtime(
    argv: Optional[Sequence[str]] = None,
    environ: Optional[Mapping[str, str]] = None,
    *,
    require_secrets: bool = True,
    from_cli: bool = False,
) -> AppSettings:
    """Resolve settings with strict precedence and apply them to runtime state."""
    global runtime_settings, CONFIG_FILE, WWWROOT_DIR, config_data
    global NVIDIA_API_KEY, token_bucket, concurrency_semaphore, _cli_configured, db_backend

    resolved = resolve_settings(
        argv=argv,
        environ=environ,
        allow_yaml_fallback=True,
        require_secrets=require_secrets,
    )
    runtime_settings = resolved
    CONFIG_FILE = resolved.config_file
    WWWROOT_DIR = resolved.wwwroot_dir
    config_data = resolved.to_config_dict()
    NVIDIA_API_KEY = resolved.nvidia_api_key
    _cli_configured = from_cli

    if not os.path.exists(CONFIG_FILE):
        save_config(config_data, CONFIG_FILE)

    rate_limit_manager.configure_from_dict(config_data)
    token_bucket = rate_limit_manager.global_rpm
    concurrency_semaphore = rate_limit_manager.global_semaphore
    model_router.configure_from_dict(config_data, env_file=resolved.env_file)
    catalog_cache.clear()
    catalog_cache.set_ttl(resolved.catalog_cache_ttl)
    db_backend = database.create_backend(
        db_url=resolved.db_url,
        db_path=resolved.db_path,
        pool_min=resolved.db_pool_min,
        pool_max=resolved.db_pool_max,
        connect_timeout=resolved.db_connect_timeout,
    )
    return resolved


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client

    # Re-resolve settings from env > dotenv > config.yaml > defaults (fail if required missing)
    # unless already explicitly configured via CLI main().
    if not _cli_configured:
        configure_runtime(argv=[], require_secrets=True, from_cli=False)

    # Init & migrate database backend
    await db_backend.init_and_migrate()

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
    await model_router.aclose_all()
    await db_backend.close()
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
    proxy_cfg = config_data.get("proxy", {})
    deployment_mode = detect_deployment_mode(
        proxy_cfg.get("upstream_url", ""),
        proxy_cfg.get("deployment_mode", "auto"),
    )
    return {
        "status": "healthy",
        "deployment_mode": deployment_mode,
        "db": {
            "backend": db_backend.backend_name,
            "schema_version": db_backend.schema_version,
            "target": db_backend.sanitized_target,
        },
        "rate_limiter": rate_limit_manager.status_snapshot(),
        "governance": model_router.status_snapshot(),
        "catalog": catalog_cache.snapshot(),
        "concurrency": {
            "limit": proxy_cfg["concurrency_limit"],
            "active_tasks": proxy_cfg["concurrency_limit"] - concurrency_semaphore._value,
        },
        "config": config_data,
    }


# --- Admin Dashboard & Metrics API ---
@app.get("/admin/metrics", tags=["Admin"])
async def admin_metrics():
    proxy_cfg = config_data.get("proxy", {})
    metrics = await db_backend.get_metrics()
    snapshot = rate_limit_manager.status_snapshot()
    metrics["deployment_mode"] = detect_deployment_mode(
        proxy_cfg.get("upstream_url", ""),
        proxy_cfg.get("deployment_mode", "auto"),
    )
    metrics["db"] = {
        "backend": db_backend.backend_name,
        "schema_version": db_backend.schema_version,
        "target": db_backend.sanitized_target,
    }
    metrics["current_tokens"] = snapshot["tokens_available"]
    metrics["capacity"] = snapshot["capacity"]
    metrics["tpm_available"] = snapshot["tpm_available"]
    metrics["tpm_capacity"] = snapshot["tpm_capacity"]
    metrics["active_cooldowns"] = snapshot["active_cooldowns"]
    metrics["upstream_throttled_count"] = snapshot["upstream_throttled_count"]
    metrics["catalog"] = catalog_cache.snapshot()
    metrics["active_concurrency"] = proxy_cfg["concurrency_limit"] - concurrency_semaphore._value
    return metrics


@app.get("/admin/catalog", tags=["Admin"])
async def admin_catalog():
    return catalog_cache.snapshot()


@app.get("/admin/logs", tags=["Admin"])
async def admin_logs(
    limit: int = 50,
    offset: int = 0,
    status_code: Optional[int] = None,
    path: Optional[str] = None,
    model: Optional[str] = None,
):
    logs = await db_backend.get_logs(
        limit=limit,
        offset=offset,
        status_code=status_code,
        path=path,
        model=model,
    )
    recent_models = await db_backend.get_recent_models_with_tokens(limit=3)
    return {"logs": logs, "limit": limit, "offset": offset, "recent_models": recent_models}


@app.get("/admin/config", tags=["Admin"])
async def get_admin_config():
    return config_data


@app.get("/admin/settings/state", tags=["Admin"])
async def get_admin_settings_state():
    return {
        "config": config_data,
        "sources": build_settings_source_metadata(runtime_settings),
        "runtime_info": {
            "nvidia_api_key_source": runtime_settings.sources.get("nvidia_api_key", "unset"),
            "db_backend": db_backend.backend_name,
            "db_target": db_backend.sanitized_target,
            "db_url_source": runtime_settings.sources.get("db_url", "default"),
            "host": runtime_settings.host,
            "port": runtime_settings.port,
        },
    }


@app.post("/admin/config", tags=["Admin"])
async def update_admin_config(new_config: Dict[str, Any]):
    global config_data, token_bucket, concurrency_semaphore, db_backend
    try:
        normalized = normalize_admin_config_patch(new_config)
        sources_meta = build_settings_source_metadata(runtime_settings)
        ignored_locked_fields: list[str] = []

        def _filter_locked(section: str, patch: Dict[str, Any]) -> Dict[str, Any]:
            allowed: Dict[str, Any] = {}
            for key, val in patch.items():
                path_key = f"{section}.{key}"
                if sources_meta.get(path_key, {}).get("locked"):
                    ignored_locked_fields.append(path_key)
                else:
                    allowed[key] = val
            return allowed

        if "admin" in normalized:
            admin_update = _filter_locked("admin", normalized["admin"])
            if admin_update:
                config_data["admin"].update(admin_update)

        if "proxy" in normalized:
            proxy_update = _filter_locked("proxy", dict(normalized["proxy"]))
            if "deployment_mode" in proxy_update:
                mode_val = str(proxy_update["deployment_mode"]).strip().lower()
                if mode_val not in ("auto", "trial", "self_hosted", "partner"):
                    raise ValueError("Invalid proxy.deployment_mode: must be auto, trial, self_hosted, or partner")
                proxy_update["deployment_mode"] = mode_val
            if "allowed_models" in proxy_update:
                proxy_update["allowed_models"] = validate_model_list(
                    proxy_update["allowed_models"], "allowed_models"
                )
            if "blocked_models" in proxy_update:
                proxy_update["blocked_models"] = validate_model_list(
                    proxy_update["blocked_models"], "blocked_models"
                )
            if "model_routes" in proxy_update:
                proxy_update["model_routes"] = validate_model_routes(proxy_update["model_routes"])
            if proxy_update:
                config_data["proxy"].update(proxy_update)

        if "rate_limit" in normalized:
            rl_update = _filter_locked("rate_limit", dict(normalized["rate_limit"]))
            if "models" in rl_update:
                rl_update["models"] = validate_model_rate_limits(rl_update["models"])
            if rl_update:
                config_data["rate_limit"].update(rl_update)

        db_changed = False
        if "db" in normalized:
            db_update = _filter_locked("db", dict(normalized["db"]))
            validate_db_yaml(db_update)
            if "url_env" in db_update and db_update["url_env"] is None:
                config_data["db"].pop("url_env", None)
                db_update.pop("url_env")
                db_changed = True
            if db_update:
                merged_db = dict(config_data["db"])
                merged_db.update(db_update)
                if int(merged_db.get("pool_max", 5)) < int(merged_db.get("pool_min", 1)):
                    raise ValueError("Invalid database pool settings: pool_max must be >= pool_min.")
                config_data["db"].update(db_update)
                db_changed = True

        save_config(config_data, CONFIG_FILE)

        # Update in-memory structures
        rate_limit_manager.configure_from_dict(config_data)
        token_bucket = rate_limit_manager.global_rpm
        concurrency_semaphore = rate_limit_manager.global_semaphore
        model_router.configure_from_dict(config_data, env_file=runtime_settings.env_file)
        catalog_cache.set_ttl(float(config_data["proxy"].get("catalog_cache_ttl", 300.0)))
        if db_changed and not runtime_settings.db_url:
            await db_backend.close()
            db_backend_new = database.create_backend(
                db_url="",
                db_path=config_data["db"]["path"],
                pool_min=int(config_data["db"].get("pool_min", 1)),
                pool_max=int(config_data["db"].get("pool_max", 5)),
                connect_timeout=float(config_data["db"].get("connect_timeout", 10.0)),
            )
            await db_backend_new.init_and_migrate()
            db_backend = db_backend_new
        if http_client is not None:
            http_client.base_url = httpx.URL(config_data["proxy"]["upstream_url"])

        return {
            "status": "updated",
            "config": config_data,
            "ignored_locked_fields": ignored_locked_fields,
        }
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


@app.get("/admin/settings", response_class=HTMLResponse, tags=["Admin"])
@app.get("/admin/settings/", response_class=HTMLResponse, tags=["Admin"])
async def admin_settings_page():
    """Renders the separated Settings UI from wwwroot/settings.html."""
    settings_file = os.path.join(WWWROOT_DIR, "settings.html")
    if os.path.exists(settings_file):
        with open(settings_file, "r", encoding="utf-8") as f:
            content = f.read()
            title = config_data.get("admin", {}).get("title", "NVIDIA NIM Rate Limiter Dashboard")
            content = content.replace("{{ ADMIN_TITLE }}", title)
            return HTMLResponse(content=content)
    return HTMLResponse(
        content="<h1>Admin Settings</h1><p>wwwroot/settings.html not found.</p>",
        status_code=404,
    )


def should_log_request(method: str, path: str) -> bool:
    """Return False for noisy browser root/favicon GET probes."""
    normalized_path = "/" + path.lstrip("/")
    return not (method.upper() == "GET" and normalized_path in ("/", "/favicon.ico"))


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
    log_enabled = should_log_request(request.method, path)

    try:
        body = await request.body()
    except (ClientDisconnect, asyncio.CancelledError):
        return Response(status_code=499)

    model_name: Optional[str] = None
    body_json: Optional[Dict[str, Any]] = None
    if body:
        try:
            parsed = json.loads(body.decode("utf-8"))
            if isinstance(parsed, dict):
                body_json = parsed
                model_name = parsed.get("model")
        except Exception:
            pass

    # 0. Model Governance & Catalog Enforcement (before consuming rate-limit tokens)
    policy_reason = model_router.check_model_policy(model_name)
    if policy_reason is not None:
        latency_ms = (time.time() - start_time) * 1000.0
        if log_enabled:
            await db_backend.log_request(
                method=request.method,
                path="/" + path,
                status_code=403,
                latency_ms=latency_ms,
                request_bytes=len(body),
                response_bytes=0,
                allowed=False,
                model=model_name,
            )
        return JSONResponse(
            status_code=403,
            content={
                "error": "model_not_allowed",
                "model": model_name,
                "reason": policy_reason,
            },
        )

    if config_data.get("proxy", {}).get("enforce_catalog") and catalog_cache.is_known(model_name) is False:
        latency_ms = (time.time() - start_time) * 1000.0
        if log_enabled:
            await db_backend.log_request(
                method=request.method,
                path="/" + path,
                status_code=404,
                latency_ms=latency_ms,
                request_bytes=len(body),
                response_bytes=0,
                allowed=False,
                model=model_name,
            )
        return JSONResponse(
            status_code=404,
            content={
                "error": "model_not_in_catalog",
                "model": model_name,
            },
        )

    # 1. Daily Quota Caps (UTC day)
    rate_cfg = config_data.get("rate_limit", {})
    daily_req_cap = int(rate_cfg.get("daily_request_cap", 0) or 0)
    daily_tok_cap = int(rate_cfg.get("daily_token_cap", 0) or 0)
    if daily_req_cap > 0 or daily_tok_cap > 0:
        usage = await db_backend.get_daily_usage()
        retry_sec = int(max(1, math.ceil(usage["seconds_until_reset"])))
        quota_payload: Optional[Dict[str, Any]] = None
        if daily_req_cap > 0 and usage["requests"] >= daily_req_cap:
            quota_payload = {
                "error": "daily_quota_exceeded",
                "quota_type": "requests",
                "limit": daily_req_cap,
                "used": usage["requests"],
                "retry_after": retry_sec,
            }
        elif daily_tok_cap > 0 and usage["tokens"] >= daily_tok_cap:
            quota_payload = {
                "error": "daily_quota_exceeded",
                "quota_type": "tokens",
                "limit": daily_tok_cap,
                "used": usage["tokens"],
                "retry_after": retry_sec,
            }
        if quota_payload is not None:
            latency_ms = (time.time() - start_time) * 1000.0
            if log_enabled:
                await db_backend.log_request(
                    method=request.method,
                    path="/" + path,
                    status_code=429,
                    latency_ms=latency_ms,
                    request_bytes=len(body),
                    response_bytes=0,
                    allowed=False,
                    model=model_name,
                )
            return JSONResponse(
                status_code=429,
                content=quota_payload,
                headers={"Retry-After": str(retry_sec)},
            )

    # 2. Adaptive Cooldown, Per-Model + Global RPM & TPM Acquisition
    estimated_tokens = estimate_request_tokens(body, body_json)
    try:
        reservation = await rate_limit_manager.acquire_request(
            model=model_name,
            estimated_tokens=estimated_tokens,
        )
    except RateLimitExceeded as exc:
        latency_ms = (time.time() - start_time) * 1000.0
        retry_sec = int(max(1, math.ceil(exc.retry_after)))
        if log_enabled:
            await db_backend.log_request(
                method=request.method,
                path="/" + path,
                status_code=429,
                latency_ms=latency_ms,
                request_bytes=len(body),
                response_bytes=0,
                allowed=False,
                model=model_name,
            )
        return JSONResponse(
            status_code=429,
            content=exc.detail,
            headers={"Retry-After": str(retry_sec)},
        )
    except (ClientDisconnect, asyncio.CancelledError):
        return Response(status_code=499)

    # 3. Concurrency Control & Proxying
    async with rate_limit_manager.semaphore_context(model_name):
        default_upstream = config_data["proxy"]["upstream_url"]
        target = model_router.resolve_target(
            model_name,
            default_upstream=default_upstream,
            default_api_key=NVIDIA_API_KEY,
        )
        target_client = model_router.get_client(target.base_url, http_client, default_upstream)

        headers = dict(request.headers)

        # Never forward client's Authorization header
        headers.pop("authorization", None)

        # Inject upstream credential for the resolved route (omit if keyless local NIM)
        if target.api_key:
            headers["Authorization"] = f"Bearer {target.api_key}"

        # Strip headers generated by client socket connection
        for hop_header in ("host", "content-length", "connection", "transfer-encoding"):
            headers.pop(hop_header, None)

        upstream_path = "/" + path
        if request.url.query:
            upstream_path += "?" + request.url.query

        if target_client is None:
            await rate_limit_manager.reconcile_tokens(reservation, None)
            raise HTTPException(status_code=500, detail="HTTP client not initialized")

        try:
            req_builder = target_client.build_request(
                method=request.method,
                url=upstream_path,
                headers=headers,
                content=body,
            )
            upstream_response = await target_client.send(req_builder, stream=True)
            rate_limit_manager.record_upstream_status(
                upstream_response.status_code,
                upstream_response.headers,
                model=model_name,
            )

            response_headers = dict(upstream_response.headers)
            for h in ("content-length", "transfer-encoding", "connection"):
                response_headers.pop(h, None)

            is_models_catalog_req = (
                request.method == "GET"
                and path.rstrip("/") == "v1/models"
                and upstream_response.status_code == 200
            )

            async def stream_generator():
                bytes_count = 0
                collected_chunks = []
                catalog_chunks = [] if is_models_catalog_req else None
                try:
                    async for chunk in upstream_response.aiter_bytes():
                        bytes_count += len(chunk)
                        if catalog_chunks is not None:
                            catalog_chunks.append(chunk)
                        elif len(collected_chunks) < 40 or b"usage" in chunk or b"prompt_tokens" in chunk:
                            collected_chunks.append(chunk)
                        yield chunk
                except (ClientDisconnect, asyncio.CancelledError):
                    pass
                finally:
                    await upstream_response.aclose()
                    if catalog_chunks is not None:
                        try:
                            catalog_payload = json.loads(b"".join(catalog_chunks).decode("utf-8"))
                            catalog_cache.update_from_payload(catalog_payload)
                        except Exception:
                            pass
                    latency_ms = (time.time() - start_time) * 1000.0
                    response_text = b"".join(collected_chunks).decode("utf-8", errors="ignore")
                    prompt_tokens, completion_tokens, total_tokens = extract_tokens(response_text)
                    reconcile_val = total_tokens if upstream_response.status_code < 400 else None
                    await rate_limit_manager.reconcile_tokens(reservation, reconcile_val)
                    if log_enabled:
                        asyncio.create_task(
                            db_backend.log_request(
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
            await rate_limit_manager.reconcile_tokens(reservation, None)
            return Response(status_code=499)
        except Exception as e:
            await rate_limit_manager.reconcile_tokens(reservation, None)
            latency_ms = (time.time() - start_time) * 1000.0
            if log_enabled:
                asyncio.create_task(
                    db_backend.log_request(
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


async def _run_cli_migration_command(resolved: AppSettings) -> Dict[str, Any]:
    backend = database.create_backend(
        db_url=resolved.db_url,
        db_path=resolved.db_path,
        pool_min=resolved.db_pool_min,
        pool_max=resolved.db_pool_max,
        connect_timeout=resolved.db_connect_timeout,
    )
    try:
        if resolved.migrate_only:
            await backend.init_and_migrate()
        status_payload = await backend.get_migration_status()
        status_payload["backend"] = backend.backend_name
        status_payload["target"] = backend.sanitized_target
        return status_payload
    finally:
        await backend.close()


def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entrypoint supporting argparse > env > dotenv > config.yaml > default/fail."""
    cli_args = sys.argv[1:] if argv is None else list(argv)
    try:
        pre_resolved = resolve_settings(
            argv=cli_args,
            allow_yaml_fallback=True,
            require_secrets=False,
        )
        if pre_resolved.migrate_only or pre_resolved.migration_status:
            payload = asyncio.run(_run_cli_migration_command(pre_resolved))
            print(json.dumps(payload, indent=2))
            return

        resolved = configure_runtime(argv=cli_args, require_secrets=True, from_cli=True)
        warning = validate_bind_guardrails(resolved)
        if warning:
            print(warning, file=sys.stderr)
    except (ConfigError, Exception) as exc:
        if isinstance(exc, ConfigError) or exc.__class__.__name__ == "MigrationError":
            print(f"Configuration error: {exc}", file=sys.stderr)
            raise SystemExit(2) from exc
        raise

    uvicorn.run(app, host=resolved.host, port=resolved.port, workers=1)


if __name__ == "__main__":
    main()
