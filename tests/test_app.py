import email.utils
import json
import os
import sys
import time

import aiosqlite
import httpx
import pytest
import yaml
from dotenv import dotenv_values
from fastapi.testclient import TestClient

import app as app_module
import database
import migrations
from app import TokenBucket, app, extract_tokens
from database import PostgresBackend, SQLiteBackend, create_backend
from migrations import Migration, MigrationError
from rate_limiter import (
    RateLimitExceeded,
    RateLimitManager,
    TPMBucket,
    parse_retry_after,
)
from routing import detect_deployment_mode
from settings import (
    ConfigError,
    is_loopback_host,
    resolve_settings,
    sanitize_db_target,
    validate_bind_guardrails,
)

TEST_DB = "test_limiter.db"


@pytest.fixture(autouse=True)
def setup_test_env(tmp_path, monkeypatch):
    """Isolate DB, config.yaml, .env, and runtime state for every test."""
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

    cfg_path = tmp_path / "config.yaml"
    db_path = tmp_path / "limiter.db"
    env_path = tmp_path / ".env"

    monkeypatch.setenv("NVIDIA_API_KEY", "nvapi-test-default-key")
    monkeypatch.setenv("CONFIG_FILE", str(cfg_path))
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("ENV_FILE", str(env_path))
    monkeypatch.delenv("RPM", raising=False)
    monkeypatch.delenv("WINDOW_SECONDS", raising=False)
    monkeypatch.delenv("BURST_CAPACITY", raising=False)
    monkeypatch.delenv("TPM", raising=False)
    monkeypatch.delenv("MAX_WAIT_SECONDS", raising=False)
    monkeypatch.delenv("DAILY_REQUEST_CAP", raising=False)
    monkeypatch.delenv("DAILY_TOKEN_CAP", raising=False)
    monkeypatch.delenv("MAX_CONCURRENT", raising=False)
    monkeypatch.delenv("UPSTREAM_URL", raising=False)
    monkeypatch.delenv("DEPLOYMENT_MODE", raising=False)
    monkeypatch.delenv("CATALOG_CACHE_TTL", raising=False)
    monkeypatch.delenv("ENFORCE_CATALOG", raising=False)
    monkeypatch.delenv("ADMIN_TITLE", raising=False)
    monkeypatch.delenv("HOST", raising=False)
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.delenv("ALLOW_NON_LOCAL_BIND", raising=False)
    monkeypatch.delenv("STRICT_TRIAL_GUARDRAILS", raising=False)
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("DB_POOL_MIN", raising=False)
    monkeypatch.delenv("DB_POOL_MAX", raising=False)
    monkeypatch.delenv("DB_CONNECT_TIMEOUT", raising=False)
    monkeypatch.setattr(app_module, "_cli_configured", False)
    app_module.rate_limit_manager.global_rpm.tokens = app_module.rate_limit_manager.global_rpm.capacity
    app_module.rate_limit_manager._cooldown_until.clear()
    app_module.rate_limit_manager._consecutive_429s.clear()
    app_module.rate_limit_manager.upstream_throttled_count = 0
    app_module.catalog_cache.clear()

    yield

    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def test_extract_tokens():
    # Standard JSON
    json_text = '{"id":"chat-1","choices":[],"usage":{"prompt_tokens":120,"completion_tokens":45,"total_tokens":165}}'
    p, c, t = extract_tokens(json_text)
    assert p == 120
    assert c == 45
    assert t == 165

    # SSE Chunk Stream Text
    sse_text = 'data: {"id":"chat-1","choices":[{"delta":{"content":"Hi"}}],"usage":{"prompt_tokens":85,"completion_tokens":12}}\n\ndata: [DONE]\n\n'
    p, c, t = extract_tokens(sse_text)
    assert p == 85
    assert c == 12
    assert t == 97


@pytest.mark.asyncio
async def test_database_logging():
    await database.init_db(TEST_DB)
    await database.log_request(
        db_path=TEST_DB,
        method="POST",
        path="/v1/chat/completions",
        status_code=200,
        latency_ms=120.5,
        request_bytes=50,
        response_bytes=200,
        allowed=True,
        model="z-ai/glm-5.2",
        prompt_tokens=150,
        completion_tokens=42,
        total_tokens=192,
    )

    logs = await database.get_logs(TEST_DB)
    assert len(logs) == 1
    assert logs[0]["method"] == "POST"
    assert logs[0]["path"] == "/v1/chat/completions"
    assert logs[0]["model"] == "z-ai/glm-5.2"
    assert logs[0]["prompt_tokens"] == 150
    assert logs[0]["completion_tokens"] == 42
    assert logs[0]["total_tokens"] == 192
    assert logs[0]["status_code"] == 200

    metrics = await database.get_metrics(TEST_DB)
    assert metrics["total_requests"] == 1
    assert metrics["allowed_requests"] == 1
    assert metrics["total_prompt_tokens"] == 150
    assert metrics["total_completion_tokens"] == 42
    assert metrics["total_tokens"] == 192
    assert metrics["status_codes"][200] == 1


@pytest.mark.asyncio
async def test_token_bucket():
    tb = TokenBucket(rpm=60.0, window_seconds=60.0, capacity=2.0)
    assert tb.tokens == 2.0

    # First token immediate
    wait1 = await tb.acquire()
    assert wait1 == 0.0
    assert abs(tb.tokens - 1.0) < 0.1

    # Second token immediate
    wait2 = await tb.acquire()
    assert wait2 == 0.0
    assert abs(tb.tokens - 0.0) < 0.1


def test_healthz():
    with TestClient(app) as client:
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"


def test_status():
    with TestClient(app) as client:
        response = client.get("/status")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "rate_limiter" in data
        assert "concurrency" in data
        # Credential isolation: API key must never be exposed in /status
        assert "nvapi-test-default-key" not in response.text


@pytest.mark.asyncio
async def test_recent_models_with_tokens():
    await database.init_db(TEST_DB)
    await database.log_request(
        db_path=TEST_DB,
        method="POST",
        path="/v1/chat/completions",
        status_code=200,
        latency_ms=100.0,
        model="meta/llama-3.1-8b-instruct",
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
    )
    await database.log_request(
        db_path=TEST_DB,
        method="GET",
        path="/v1/models",
        status_code=200,
        latency_ms=20.0,
        model="no-token-model",
        prompt_tokens=0,
        completion_tokens=0,
        total_tokens=0,
    )
    await database.log_request(
        db_path=TEST_DB,
        method="POST",
        path="/v1/chat/completions",
        status_code=200,
        latency_ms=150.0,
        model="mistralai/mixtral-8x7b-instruct-v0.1",
        prompt_tokens=50,
        completion_tokens=20,
        total_tokens=70,
    )
    await database.log_request(
        db_path=TEST_DB,
        method="POST",
        path="/v1/chat/completions",
        status_code=200,
        latency_ms=200.0,
        model="deepseek-ai/deepseek-r1",
        prompt_tokens=80,
        completion_tokens=40,
        total_tokens=120,
    )
    await database.log_request(
        db_path=TEST_DB,
        method="POST",
        path="/v1/chat/completions",
        status_code=200,
        latency_ms=110.0,
        model="meta/llama-3.3-70b-instruct",
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
    )

    models = await database.get_recent_models_with_tokens(TEST_DB, limit=3)
    assert len(models) == 3
    assert models[0] == "meta/llama-3.3-70b-instruct"
    assert models[1] == "deepseek-ai/deepseek-r1"
    assert models[2] == "mistralai/mixtral-8x7b-instruct-v0.1"

    metrics = await database.get_metrics(TEST_DB)
    assert metrics["recent_models"] == models


def test_admin_dashboard_and_config():
    with TestClient(app) as client:
        resp_html = client.get("/admin/")
        assert resp_html.status_code == 200
        assert "filter-get" in resp_html.text
        assert "filter-post" in resp_html.text
        assert "Success (2xx/3xx)" in resp_html.text
        assert "Errors (4xx/5xx)" in resp_html.text
        assert "modelFiltersContainer" in resp_html.text

        resp_cfg = client.get("/admin/config")
        assert resp_cfg.status_code == 200
        assert "proxy" in resp_cfg.json()
        assert "admin" in resp_cfg.json()
        assert "nvapi-test-default-key" not in resp_cfg.text

        update_payload = {
            "admin": {"title": "Custom Agent Rate Limiter"},
            "rate_limit": {
                "requests_per_minute": 10.0,
                "window_seconds": 60.0,
                "burst_capacity": 10.0,
            },
            "proxy": {
                "concurrency_limit": 2,
                "upstream_url": "https://integrate.api.nvidia.com",
            },
        }
        resp_post = client.post("/admin/config", json=update_payload)
        assert resp_post.status_code == 200
        assert resp_post.json()["status"] == "updated"

        resp_html_updated = client.get("/admin/")
        assert "<title>Custom Agent Rate Limiter</title>" in resp_html_updated.text

        resp_metrics = client.get("/admin/metrics")
        assert resp_metrics.status_code == 200
        assert "total_requests" in resp_metrics.json()
        assert "recent_models" in resp_metrics.json()


# --- Argument & Configuration Precedence Tests ---
def test_precedence_argparse_over_env_dotenv_yaml_default(tmp_path):
    dotenv_file = tmp_path / ".env"
    dotenv_file.write_text(
        "NVIDIA_API_KEY=nvapi-from-dotenv\n"
        "RPM=12\n"
        "WINDOW_SECONDS=45\n"
        "BURST_CAPACITY=14\n"
        "MAX_CONCURRENT=3\n"
        "ADMIN_TITLE=From Dotenv\n",
        encoding="utf-8",
    )
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        yaml.safe_dump(
            {
                "rate_limit": {
                    "requests_per_minute": 8.0,
                    "window_seconds": 30.0,
                    "burst_capacity": 9.0,
                },
                "proxy": {
                    "concurrency_limit": 2,
                    "upstream_url": "https://yaml.example.com",
                },
                "admin": {"title": "From YAML"},
            }
        ),
        encoding="utf-8",
    )

    environ = {
        "NVIDIA_API_KEY": "nvapi-from-env",
        "RPM": "20",
        "WINDOW_SECONDS": "50",
        "ENV_FILE": str(dotenv_file),
        "CONFIG_FILE": str(yaml_file),
    }

    resolved = resolve_settings(
        argv=["--nvidia-api-key", "nvapi-from-cli", "--rpm", "35", "--port", "9001"],
        environ=environ,
    )

    # 1. argparse beats env, dotenv, yaml, default
    assert resolved.nvidia_api_key == "nvapi-from-cli"
    assert resolved.sources["nvidia_api_key"] == "argparse"
    assert resolved.requests_per_minute == 35.0
    assert resolved.sources["requests_per_minute"] == "argparse"
    assert resolved.port == 9001
    assert resolved.sources["port"] == "argparse"

    # 2. env beats dotenv, yaml, default
    assert resolved.window_seconds == 50.0
    assert resolved.sources["window_seconds"] == "env"

    # 3. dotenv beats yaml and default (and does not mutate os.environ)
    assert resolved.burst_capacity == 14.0
    assert resolved.sources["burst_capacity"] == "dotenv"
    assert resolved.concurrency_limit == 3
    assert resolved.sources["concurrency_limit"] == "dotenv"
    assert resolved.admin_title == "From Dotenv"
    assert resolved.sources["admin_title"] == "dotenv"
    assert "ADMIN_TITLE" not in os.environ

    # 4. config.yaml beats default for non-secret settings
    assert resolved.upstream_url == "https://yaml.example.com"
    assert resolved.sources["upstream_url"] == "config.yaml"

    # 5. default used when absent everywhere
    assert resolved.host == "127.0.0.1"
    assert resolved.sources["host"] == "default"
    assert resolved.db_path == "limiter.db"
    assert resolved.sources["db_path"] == "default"


def test_precedence_empty_env_falls_back_to_dotenv(tmp_path):
    """Docker Compose injects empty env strings when host vars are unset; dotenv should win."""
    dotenv_file = tmp_path / ".env"
    dotenv_file.write_text("NVIDIA_API_KEY=nvapi-from-dotenv\nRPM=18.5\n", encoding="utf-8")

    resolved = resolve_settings(
        argv=["--env-file", str(dotenv_file)],
        environ={"NVIDIA_API_KEY": "", "RPM": "   "},
        allow_yaml_fallback=False,
    )
    assert resolved.nvidia_api_key == "nvapi-from-dotenv"
    assert resolved.sources["nvidia_api_key"] == "dotenv"
    assert resolved.requests_per_minute == 18.5
    assert resolved.sources["requests_per_minute"] == "dotenv"


def test_missing_required_nvidia_api_key_fails(tmp_path):
    empty_env = tmp_path / ".env"
    empty_env.write_text("RPM=10\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="nvidia_api_key"):
        resolve_settings(
            argv=["--env-file", str(empty_env)],
            environ={},
            allow_yaml_fallback=False,
        )


def test_required_key_cannot_come_from_yaml(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        yaml.safe_dump({"nvidia_api_key": "nvapi-should-be-ignored"}),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="nvidia_api_key"):
        resolve_settings(
            argv=["--config-file", str(yaml_file)],
            environ={},
            allow_yaml_fallback=True,
        )


def test_invalid_values_and_missing_explicit_env_file_raise_config_error(tmp_path):
    with pytest.raises(ConfigError, match="requests_per_minute"):
        resolve_settings(
            argv=["--nvidia-api-key", "nvapi-ok", "--rpm", "not-a-float"],
            environ={},
            allow_yaml_fallback=False,
        )

    with pytest.raises(ConfigError, match="concurrency_limit"):
        resolve_settings(
            argv=["--nvidia-api-key", "nvapi-ok", "--max-concurrent", "0"],
            environ={},
            allow_yaml_fallback=False,
        )

    with pytest.raises(ConfigError, match="port"):
        resolve_settings(
            argv=["--nvidia-api-key", "nvapi-ok", "--port", "70000"],
            environ={},
            allow_yaml_fallback=False,
        )

    with pytest.raises(ConfigError, match="nvidia_api_key"):
        resolve_settings(
            argv=["--nvidia-api-key", ""],
            environ={"NVIDIA_API_KEY": "nvapi-fallback"},
            allow_yaml_fallback=False,
        )

    with pytest.raises(ConfigError, match="--env-file"):
        resolve_settings(
            argv=["--env-file", str(tmp_path / "does-not-exist.env"), "--nvidia-api-key", "nvapi-ok"],
            environ={},
            allow_yaml_fallback=False,
        )


def test_lifespan_fails_fast_without_nvidia_api_key(monkeypatch, tmp_path):
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.setenv("ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setattr(app_module, "_cli_configured", False)

    with pytest.raises(ConfigError, match="nvidia_api_key"):
        with TestClient(app):
            pass


def test_cli_main_applies_argparse_and_handles_missing_required(monkeypatch, tmp_path):
    called = {}

    def fake_run(target_app, host, port, workers=1):
        called["host"] = host
        called["port"] = port
        called["workers"] = workers

    monkeypatch.setattr(app_module.uvicorn, "run", fake_run)
    cfg_path = tmp_path / "cli_config.yaml"
    db_path = tmp_path / "cli.db"

    app_module.main(
        [
            "--nvidia-api-key",
            "nvapi-cli-key",
            "--rpm",
            "25",
            "--max-concurrent",
            "4",
            "--host",
            "127.0.0.1",
            "--port",
            "8123",
            "--config-file",
            str(cfg_path),
            "--db-path",
            str(db_path),
        ]
    )

    assert called == {"host": "127.0.0.1", "port": 8123, "workers": 1}
    assert app_module.NVIDIA_API_KEY == "nvapi-cli-key"
    assert app_module.config_data["rate_limit"]["requests_per_minute"] == 25.0
    assert app_module.config_data["proxy"]["concurrency_limit"] == 4
    # Verify persisted config.yaml never contains the API key
    saved_yaml = cfg_path.read_text(encoding="utf-8")
    assert "nvapi-cli-key" not in saved_yaml

    # Missing required key exits with code 2
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        app_module.main(["--config-file", str(cfg_path), "--db-path", str(db_path)])
    assert exc_info.value.code == 2


def test_bind_guardrails_and_bool_precedence(monkeypatch, tmp_path, capsys):
    assert is_loopback_host("127.0.0.1") is True
    assert is_loopback_host("localhost") is True
    assert is_loopback_host("::1") is True
    assert is_loopback_host("0.0.0.0") is False
    assert is_loopback_host("192.168.1.10") is False

    # Loopback host produces no warning
    s_local = resolve_settings(
        argv=["--nvidia-api-key", "nvapi-ok", "--host", "127.0.0.1"],
        environ={},
        allow_yaml_fallback=False,
    )
    assert validate_bind_guardrails(s_local) is None

    # Non-loopback without strict mode returns warning
    s_warn = resolve_settings(
        argv=["--nvidia-api-key", "nvapi-ok", "--host", "0.0.0.0"],
        environ={},
        allow_yaml_fallback=False,
    )
    warning = validate_bind_guardrails(s_warn)
    assert warning is not None
    assert "0.0.0.0" in warning

    # Non-loopback with strict mode fails unless --allow-non-local-bind true
    s_strict = resolve_settings(
        argv=["--nvidia-api-key", "nvapi-ok", "--host", "0.0.0.0", "--strict-trial-guardrails", "true"],
        environ={},
        allow_yaml_fallback=False,
    )
    with pytest.raises(ConfigError, match="non-loopback"):
        validate_bind_guardrails(s_strict)

    s_allowed = resolve_settings(
        argv=[
            "--nvidia-api-key",
            "nvapi-ok",
            "--host",
            "0.0.0.0",
            "--strict-trial-guardrails",
            "true",
            "--allow-non-local-bind",
            "true",
        ],
        environ={},
        allow_yaml_fallback=False,
    )
    assert validate_bind_guardrails(s_allowed) is None

    # CLI main() prints warning on non-strict non-local bind and exits 2 on strict non-local bind
    monkeypatch.setattr(app_module.uvicorn, "run", lambda *a, **kw: None)
    cfg_path = tmp_path / "guard.yaml"
    db_path = tmp_path / "guard.db"

    app_module.main(
        [
            "--nvidia-api-key",
            "nvapi-ok",
            "--host",
            "0.0.0.0",
            "--config-file",
            str(cfg_path),
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert "Security warning" in captured.err

    with pytest.raises(SystemExit) as exc_info:
        app_module.main(
            [
                "--nvidia-api-key",
                "nvapi-ok",
                "--host",
                "0.0.0.0",
                "--strict-trial-guardrails",
                "true",
                "--config-file",
                str(cfg_path),
                "--db-path",
                str(db_path),
            ]
        )
    assert exc_info.value.code == 2


# --- Spec 01 (.env.example) & Spec 02 (Rate Limiting, TPM, Backoff, Quotas) ---
def test_env_example_is_documented_and_safe():
    env_example_path = os.path.join(os.path.dirname(__file__), "..", ".env.example")
    assert os.path.exists(env_example_path)
    raw = open(env_example_path, "r", encoding="utf-8").read()
    assert "NVIDIA_API_KEY=nvapi-YOUR_REAL_KEY_HERE" in raw
    for var in (
        "RPM",
        "TPM",
        "MAX_WAIT_SECONDS",
        "DAILY_REQUEST_CAP",
        "DAILY_TOKEN_CAP",
        "ALLOW_NON_LOCAL_BIND",
        "STRICT_TRIAL_GUARDRAILS",
    ):
        assert var in raw

    parsed = dotenv_values(env_example_path)
    assert parsed["NVIDIA_API_KEY"] == "nvapi-YOUR_REAL_KEY_HERE"


def test_parse_retry_after_formats():
    now = 1_700_000_000.0
    assert parse_retry_after({"Retry-After": "12"}, now=now) == 12.0
    assert parse_retry_after({"retry-after": "2.5"}, now=now) == 2.5
    assert parse_retry_after({"x-ratelimit-reset-requests": "500ms"}, now=now) == 0.5
    assert parse_retry_after({"x-ratelimit-reset-tokens": "1m30s"}, now=now) == 90.0
    assert parse_retry_after({"x-ratelimit-reset": str(now + 15.0)}, now=now) == pytest.approx(15.0, abs=0.01)

    http_date = email.utils.formatdate(now + 20.0, usegmt=True)
    assert parse_retry_after({"Retry-After": http_date}, now=now) == pytest.approx(20.0, abs=1.0)
    assert parse_retry_after({"Retry-After": "invalid-value"}, now=now) is None


@pytest.mark.asyncio
async def test_tpm_bucket_reservation_and_reconciliation():
    bucket = TPMBucket(tpm=1000, window_seconds=60.0)
    assert bucket.enabled is True
    assert bucket.tokens == 1000.0

    # Reserve 400 tokens
    wait_time, reserved = await bucket.acquire(400, max_wait_seconds=0.5)
    assert wait_time == 0.0
    assert reserved == 400
    assert bucket.tokens <= 605.0

    # Overestimate refund: actual was 150 -> refund 250
    await bucket.reconcile(reserved, actual_tokens=150)
    assert 840.0 <= bucket.tokens <= 860.0

    # Error refund: actual is None -> refund full reservation
    _, reserved2 = await bucket.acquire(300, max_wait_seconds=0.5)
    await bucket.reconcile(reserved2, actual_tokens=None)
    assert 840.0 <= bucket.tokens <= 865.0

    # Exceeding remaining tokens with tight max_wait_seconds raises RateLimitExceeded without deducting
    before = bucket.tokens
    with pytest.raises(RateLimitExceeded) as exc_info:
        await bucket.acquire(950, max_wait_seconds=0.05)
    assert exc_info.value.error == "rate_limit_wait_exceeded"
    assert abs(bucket.tokens - before) < 5.0


@pytest.mark.asyncio
async def test_per_model_overrides_and_yaml_validation(tmp_path):
    yaml_file = tmp_path / "models.yaml"
    yaml_file.write_text(
        yaml.safe_dump(
            {
                "rate_limit": {
                    "requests_per_minute": 60.0,
                    "window_seconds": 60.0,
                    "burst_capacity": 10.0,
                    "max_wait_seconds": 0.05,
                    "models": {
                        "deepseek-ai/deepseek-r1": {
                            "requests_per_minute": 1.0,
                            "burst_capacity": 1.0,
                            "tokens_per_minute": 500,
                            "concurrency_limit": 1,
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    resolved = resolve_settings(
        argv=["--nvidia-api-key", "nvapi-ok", "--config-file", str(yaml_file)],
        environ={},
    )
    assert "deepseek-ai/deepseek-r1" in resolved.model_rate_limits

    mgr = RateLimitManager.from_config(resolved.to_config_dict())
    # First request for deepseek-ai/deepseek-r1 succeeds
    res = await mgr.acquire_request("deepseek-ai/deepseek-r1", estimated_tokens=100)
    await mgr.reconcile_tokens(res, 100)

    # Second immediate request hits per-model 1-burst limit & fails fast due to max_wait_seconds=0.05
    with pytest.raises(RateLimitExceeded) as exc_info:
        await mgr.acquire_request("deepseek-ai/deepseek-r1", estimated_tokens=100)
    assert exc_info.value.detail["reason"] == "rpm_limit"

    # Unlisted model still uses global 10-burst limit
    res_other = await mgr.acquire_request("meta/llama-3.1-8b-instruct", estimated_tokens=100)
    assert res_other["model_tpm_reserved"] == 0

    # Invalid per-model config raises ConfigError
    bad_yaml = tmp_path / "bad_models.yaml"
    bad_yaml.write_text(
        yaml.safe_dump({"rate_limit": {"models": {"bad/model": {"requests_per_minute": -5}}}}),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="rate_limit.models"):
        resolve_settings(
            argv=["--nvidia-api-key", "nvapi-ok", "--config-file", str(bad_yaml)],
            environ={},
        )


def test_proxy_max_wait_seconds_fast_fail_and_logs(monkeypatch):
    monkeypatch.setenv("RPM", "1")
    monkeypatch.setenv("BURST_CAPACITY", "1")
    monkeypatch.setenv("MAX_WAIT_SECONDS", "0.05")

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"id": "ok", "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}},
        )

    transport = httpx.MockTransport(handler)
    with TestClient(app) as client:
        app_module.http_client = httpx.AsyncClient(
            base_url="https://mock.nvidia.test",
            transport=transport,
        )
        r1 = client.post("/v1/chat/completions", json={"model": "meta/llama-3.1-8b-instruct"})
        assert r1.status_code == 200

        # Second immediate request exceeds max_wait_seconds=0.05 -> local 429
        r2 = client.post("/v1/chat/completions", json={"model": "meta/llama-3.1-8b-instruct"})
        assert r2.status_code == 429
        assert r2.json()["error"] == "rate_limit_wait_exceeded"
        assert int(r2.headers["Retry-After"]) >= 1

        metrics = client.get("/admin/metrics").json()
        assert metrics["throttled_requests"] >= 1


def test_proxy_daily_request_and_token_caps(monkeypatch):
    monkeypatch.setenv("RPM", "60")
    monkeypatch.setenv("BURST_CAPACITY", "10")

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"id": "ok", "usage": {"prompt_tokens": 40, "completion_tokens": 20, "total_tokens": 60}},
        )

    transport = httpx.MockTransport(handler)
    with TestClient(app) as client:
        app_module.http_client = httpx.AsyncClient(
            base_url="https://mock.nvidia.test",
            transport=transport,
        )
        # Set daily_request_cap via /admin/config (unlocked) so it can be raised later in the test
        client.post("/admin/config", json={"rate_limit": {"daily_request_cap": 1}})
        r1 = client.post("/v1/chat/completions", json={"model": "meta/llama-3.1-8b-instruct"})
        assert r1.status_code == 200

        # Wait briefly for background log_request task from stream_generator to commit
        time.sleep(0.15)

        r2 = client.post("/v1/chat/completions", json={"model": "meta/llama-3.1-8b-instruct"})
        assert r2.status_code == 429
        body2 = r2.json()
        assert body2["error"] == "daily_quota_exceeded"
        assert body2["quota_type"] == "requests"
        assert int(r2.headers["Retry-After"]) >= 1

        # Raise request cap and enforce daily_token_cap (already used 60 tokens >= 50)
        client.post(
            "/admin/config",
            json={"rate_limit": {"daily_request_cap": 100, "daily_token_cap": 50}},
        )
        r3 = client.post("/v1/chat/completions", json={"model": "meta/llama-3.1-8b-instruct"})
        assert r3.status_code == 429
        body3 = r3.json()
        assert body3["error"] == "daily_quota_exceeded"
        assert body3["quota_type"] == "tokens"


def test_upstream_429_triggers_model_cooldown_and_backoff(monkeypatch):
    monkeypatch.setenv("RPM", "60")
    monkeypatch.setenv("BURST_CAPACITY", "10")
    monkeypatch.setenv("MAX_WAIT_SECONDS", "0.05")

    upstream_calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal upstream_calls
        upstream_calls += 1
        return httpx.Response(
            429,
            headers={"Retry-After": "10"},
            json={"error": "upstream rate limited"},
        )

    transport = httpx.MockTransport(handler)
    with TestClient(app) as client:
        app_module.http_client = httpx.AsyncClient(
            base_url="https://mock.nvidia.test",
            transport=transport,
        )

        # First call hits upstream and receives 429 Retry-After: 10
        r1 = client.post("/v1/chat/completions", json={"model": "deepseek-ai/deepseek-r1"})
        assert r1.status_code == 429
        assert upstream_calls == 1

        # Second call for same model hits adaptive cooldown locally without hitting upstream
        r2 = client.post("/v1/chat/completions", json={"model": "deepseek-ai/deepseek-r1"})
        assert r2.status_code == 429
        assert r2.json()["reason"] == "upstream_cooldown"
        assert upstream_calls == 1

        status_data = client.get("/status").json()
        assert status_data["rate_limiter"]["upstream_throttled_count"] == 1
        assert "deepseek-ai/deepseek-r1" in status_data["rate_limiter"]["active_cooldowns"]


# --- Spec 03: Trial Guardrails, Model Governance & Upstream Graduation ---
def test_deployment_mode_detection_and_trial_banner(monkeypatch):
    assert detect_deployment_mode("https://integrate.api.nvidia.com") == "trial"
    assert detect_deployment_mode("http://127.0.0.1:8001") == "self_hosted"
    assert detect_deployment_mode("http://nim-local:8000") == "self_hosted"
    assert detect_deployment_mode("https://api.together.xyz") == "partner"
    assert detect_deployment_mode("https://integrate.api.nvidia.com", "self_hosted") == "self_hosted"

    with TestClient(app) as client:
        status_json = client.get("/status").json()
        assert status_json["deployment_mode"] == "trial"
        metrics_json = client.get("/admin/metrics").json()
        assert metrics_json["deployment_mode"] == "trial"
        html_resp = client.get("/admin/")
        assert "trialBanner" in html_resp.text
        assert "Trial / Prototyping Only" in html_resp.text

    monkeypatch.setenv("UPSTREAM_URL", "http://nim-local:8000")
    with TestClient(app) as client:
        assert client.get("/status").json()["deployment_mode"] == "self_hosted"


def test_model_routes_graduation_and_secret_rejection(monkeypatch, tmp_path):
    bad_yaml = tmp_path / "bad_routes.yaml"
    bad_yaml.write_text(
        yaml.safe_dump(
            {
                "proxy": {
                    "model_routes": {
                        "meta/llama-3.3-70b-instruct": {
                            "upstream_url": "http://nim-local:8000",
                            "api_key": "nvapi-should-be-rejected",
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="Forbidden raw credential"):
        resolve_settings(
            argv=["--nvidia-api-key", "nvapi-ok", "--config-file", str(bad_yaml)],
            environ={},
        )

    good_yaml = tmp_path / "routes.yaml"
    good_yaml.write_text(
        yaml.safe_dump(
            {
                "proxy": {
                    "upstream_url": "https://integrate.api.nvidia.com",
                    "model_routes": {
                        "meta/llama-3.3-70b-instruct": {
                            "upstream_url": "http://nim-local:8000",
                            "api_key_env": "LOCAL_NIM_KEY",
                        },
                        "local/keyless-model": {
                            "upstream_url": "http://nim-keyless:8001",
                        },
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CONFIG_FILE", str(good_yaml))
    monkeypatch.setenv("LOCAL_NIM_KEY", "local-secret-token")

    seen_calls = []

    def make_transport(tag: str) -> httpx.MockTransport:
        async def handler(request: httpx.Request) -> httpx.Response:
            seen_calls.append(
                (tag, str(request.url), request.headers.get("authorization"))
            )
            return httpx.Response(
                200,
                json={"id": tag, "usage": {"prompt_tokens": 5, "completion_tokens": 5, "total_tokens": 10}},
            )

        return httpx.MockTransport(handler)

    with TestClient(app) as client:
        app_module.http_client = httpx.AsyncClient(
            base_url="https://integrate.api.nvidia.com",
            transport=make_transport("default"),
        )
        app_module.model_router._route_clients["http://nim-local:8000"] = httpx.AsyncClient(
            base_url="http://nim-local:8000",
            transport=make_transport("graduated"),
        )
        app_module.model_router._route_clients["http://nim-keyless:8001"] = httpx.AsyncClient(
            base_url="http://nim-keyless:8001",
            transport=make_transport("keyless"),
        )

        # Unmatched model uses default upstream + NVIDIA_API_KEY
        r_def = client.post("/v1/chat/completions", json={"model": "deepseek-ai/deepseek-r1"})
        assert r_def.status_code == 200
        # Graduated model uses nim-local + LOCAL_NIM_KEY
        r_grad = client.post("/v1/chat/completions", json={"model": "meta/llama-3.3-70b-instruct"})
        assert r_grad.status_code == 200
        # Keyless local model strips/omits Authorization
        r_keyless = client.post("/v1/chat/completions", json={"model": "local/keyless-model"})
        assert r_keyless.status_code == 200

        assert seen_calls[0] == (
            "default",
            "https://integrate.api.nvidia.com/v1/chat/completions",
            "Bearer nvapi-test-default-key",
        )
        assert seen_calls[1] == (
            "graduated",
            "http://nim-local:8000/v1/chat/completions",
            "Bearer local-secret-token",
        )
        assert seen_calls[2] == (
            "keyless",
            "http://nim-keyless:8001/v1/chat/completions",
            None,
        )


def test_model_allowlist_and_denylist_403_preserves_tokens(monkeypatch):
    monkeypatch.setenv("RPM", "60")
    monkeypatch.setenv("BURST_CAPACITY", "5")

    upstream_calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal upstream_calls
        upstream_calls += 1
        return httpx.Response(200, json={"id": "ok"})

    with TestClient(app) as client:
        app_module.http_client = httpx.AsyncClient(
            base_url="https://mock.nvidia.test",
            transport=httpx.MockTransport(handler),
        )
        client.post(
            "/admin/config",
            json={
                "proxy": {
                    "allowed_models": ["meta/llama-3.3-70b-instruct", "bad/blocked-model"],
                    "blocked_models": ["bad/blocked-model"],
                }
            },
        )
        tokens_before = app_module.token_bucket.tokens

        # Blocked model rejected with 403 without touching tokens or upstream
        r_blocked = client.post("/v1/chat/completions", json={"model": "bad/blocked-model"})
        assert r_blocked.status_code == 403
        assert r_blocked.json()["reason"] == "model_blocked"
        assert app_module.token_bucket.tokens == pytest.approx(tokens_before, abs=0.05)

        # Unlisted model rejected with 403
        r_unlisted = client.post("/v1/chat/completions", json={"model": "other/model"})
        assert r_unlisted.status_code == 403
        assert r_unlisted.json()["reason"] == "model_not_in_allowlist"
        assert app_module.token_bucket.tokens == pytest.approx(tokens_before, abs=0.05)
        assert upstream_calls == 0

        # Allowed model succeeds and consumes 1 token
        r_ok = client.post("/v1/chat/completions", json={"model": "meta/llama-3.3-70b-instruct"})
        assert r_ok.status_code == 200
        assert upstream_calls == 1
        assert app_module.token_bucket.tokens < tokens_before

        metrics = client.get("/admin/metrics").json()
        assert metrics["throttled_requests"] >= 2


def test_passive_catalog_cache_and_enforce_catalog_404(monkeypatch):
    monkeypatch.setenv("RPM", "60")
    monkeypatch.setenv("BURST_CAPACITY", "5")
    monkeypatch.setenv("ENFORCE_CATALOG", "true")

    upstream_calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal upstream_calls
        upstream_calls += 1
        if request.url.path == "/v1/models":
            return httpx.Response(
                200,
                json={"object": "list", "data": [{"id": "meta/llama-3.3-70b-instruct"}]},
            )
        return httpx.Response(200, json={"id": "ok"})

    with TestClient(app) as client:
        app_module.http_client = httpx.AsyncClient(
            base_url="https://mock.nvidia.test",
            transport=httpx.MockTransport(handler),
        )

        # Cold cache does not block before /v1/models is fetched
        assert client.get("/admin/catalog").json()["cached_count"] == 0

        # GET /v1/models passively populates catalog cache
        r_models = client.get("/v1/models")
        assert r_models.status_code == 200
        catalog = client.get("/admin/catalog").json()
        assert catalog["active"] is True
        assert catalog["models"] == ["meta/llama-3.3-70b-instruct"]

        tokens_before = app_module.token_bucket.tokens
        calls_before = upstream_calls

        # Unknown model fails fast with 404 without consuming tokens or hitting upstream
        r_missing = client.post("/v1/chat/completions", json={"model": "retired/model-v0"})
        assert r_missing.status_code == 404
        assert r_missing.json()["error"] == "model_not_in_catalog"
        assert app_module.token_bucket.tokens == pytest.approx(tokens_before, abs=0.05)
        assert upstream_calls == calls_before

        # Known catalog model proceeds normally
        r_known = client.post("/v1/chat/completions", json={"model": "meta/llama-3.3-70b-instruct"})
        assert r_known.status_code == 200
        assert upstream_calls == calls_before + 1


# --- Spec 05: PostgreSQL Backend & Universal Database Migrations ---
@pytest.mark.asyncio
async def test_sqlite_versioned_migrations_idempotent_and_checksum_drift(tmp_path):
    db_file = str(tmp_path / "mig.db")
    applied_first = await migrations.run_sqlite_migrations(db_file)
    assert applied_first == [1, 2, 3]

    applied_second = await migrations.run_sqlite_migrations(db_file)
    assert applied_second == []

    status = await migrations.get_sqlite_migration_status(db_file)
    assert status["current_version"] == 3
    assert [m["version"] for m in status["applied"]] == [1, 2, 3]
    assert status["pending"] == []

    # Modifying an already-applied migration's SQL triggers MigrationError on checksum drift
    tampered = (
        Migration(
            version=1,
            name="001_initial_request_logs",
            sqlite_statements=("CREATE TABLE IF NOT EXISTS tampered (id INTEGER)",),
            postgres_statements=migrations.MIGRATIONS[0].postgres_statements,
        ),
        *migrations.MIGRATIONS[1:],
    )
    with pytest.raises(MigrationError, match="Checksum mismatch"):
        await migrations.run_sqlite_migrations(db_file, migrations=tampered)


@pytest.mark.asyncio
async def test_legacy_sqlite_database_adoption_preserves_rows(tmp_path):
    legacy_db = str(tmp_path / "legacy.db")
    async with aiosqlite.connect(legacy_db) as db:
        await db.execute(
            """
            CREATE TABLE request_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                latency_ms REAL NOT NULL,
                request_bytes INTEGER NOT NULL DEFAULT 0,
                response_bytes INTEGER NOT NULL DEFAULT 0,
                allowed INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        await db.execute(
            "INSERT INTO request_logs (timestamp, method, path, status_code, latency_ms) "
            "VALUES (1700000000.0, 'POST', '/v1/chat/completions', 200, 42.0)"
        )
        await db.commit()

    backend = SQLiteBackend(legacy_db)
    await backend.init_and_migrate()
    assert backend.schema_version == 3

    logs = await backend.get_logs()
    assert len(logs) == 1
    assert logs[0]["path"] == "/v1/chat/completions"
    assert logs[0]["prompt_tokens"] == 0

    pruned = await backend.prune_old_logs(retention_days=1)
    assert pruned == 1
    assert await backend.get_logs() == []


def test_db_url_credential_redaction_and_yaml_validation(monkeypatch, tmp_path):
    assert (
        sanitize_db_target("postgresql://alice:s3cr3t@pg.internal:5432/nimdb", "limiter.db")
        == "postgresql://alice:***@pg.internal:5432/nimdb"
    )
    assert sanitize_db_target("", "limiter.db") == "sqlite:///limiter.db"

    bad_yaml = tmp_path / "bad_db.yaml"
    bad_yaml.write_text(
        yaml.safe_dump({"db": {"url": "postgresql://alice:s3cr3t@localhost:5432/nimdb"}}),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="Forbidden credential/DSN key"):
        resolve_settings(
            argv=["--nvidia-api-key", "nvapi-ok", "--config-file", str(bad_yaml)],
            environ={},
        )

    bad_pool_yaml = tmp_path / "bad_pool.yaml"
    bad_pool_yaml.write_text(
        yaml.safe_dump({"db": {"pool_min": 10, "pool_max": 2}}),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="db_pool_max"):
        resolve_settings(
            argv=["--nvidia-api-key", "nvapi-ok", "--config-file", str(bad_pool_yaml)],
            environ={},
        )

    # DB_URL is never written to config.yaml and password is masked in /status
    sqlite_url_path = str(tmp_path / "from_url.db").replace("\\", "/")
    cfg_path = tmp_path / "clean_cfg.yaml"
    monkeypatch.setenv("DB_URL", f"sqlite:///{sqlite_url_path}")
    monkeypatch.setenv("CONFIG_FILE", str(cfg_path))
    with TestClient(app) as client:
        status_json = client.get("/status").json()
        assert status_json["db"]["backend"] == "sqlite"
        assert status_json["db"]["schema_version"] == 3
        assert sqlite_url_path in status_json["db"]["target"]
        saved_yaml_text = cfg_path.read_text(encoding="utf-8")
        assert "from_url.db" not in saved_yaml_text
        saved_yaml = yaml.safe_load(saved_yaml_text)
        assert "url" not in saved_yaml["db"]
        assert "db_url" not in saved_yaml["db"]


@pytest.mark.asyncio
async def test_postgres_backend_advisory_lock_and_queries_mocked(monkeypatch):
    # 1. Missing asyncpg raises clear ConfigError
    monkeypatch.setitem(sys.modules, "asyncpg", None)
    pg_missing = create_backend("postgresql://u:pw@localhost:5432/db", "limiter.db")
    with pytest.raises(ConfigError, match="asyncpg"):
        await pg_missing.init_and_migrate()

    # 2. Full PostgresBackend + advisory-lock migration flow via mock pool
    class FakeTx:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def __init__(self, state):
            self.state = state

        def transaction(self):
            return FakeTx()

        async def execute(self, sql, *args):
            self.state["executed"].append((sql.strip(), args))
            if "SELECT pg_advisory_lock" in sql:
                self.state["locks"].append(("lock", args[0]))
            elif "SELECT pg_advisory_unlock" in sql:
                self.state["locks"].append(("unlock", args[0]))
            elif sql.strip().startswith("INSERT INTO schema_migrations"):
                self.state["migrations"].append(
                    {"version": args[0], "name": args[1], "applied_at": args[2], "checksum": args[3]}
                )
            elif sql.strip().startswith("INSERT INTO request_logs"):
                self.state["rows"].append(
                    {
                        "id": len(self.state["rows"]) + 1,
                        "timestamp": args[0],
                        "method": args[1],
                        "path": args[2],
                        "model": args[3],
                        "prompt_tokens": args[4],
                        "completion_tokens": args[5],
                        "total_tokens": args[6],
                        "status_code": args[7],
                        "latency_ms": args[8],
                        "request_bytes": args[9],
                        "response_bytes": args[10],
                        "allowed": args[11],
                    }
                )
            elif sql.strip().startswith("DELETE FROM request_logs"):
                cutoff = args[0]
                before = len(self.state["rows"])
                self.state["rows"] = [r for r in self.state["rows"] if r["timestamp"] >= cutoff]
                return f"DELETE {before - len(self.state['rows'])}"

        async def fetch(self, sql, *args):
            if "FROM schema_migrations" in sql:
                return list(self.state["migrations"])
            if "GROUP BY status_code" in sql:
                counts = {}
                for r in self.state["rows"]:
                    counts[r["status_code"]] = counts.get(r["status_code"], 0) + 1
                return [{"status_code": k, "count": v} for k, v in counts.items()]
            if "GROUP BY model" in sql:
                models = []
                for r in reversed(self.state["rows"]):
                    if r["model"] and r["total_tokens"] > 0 and r["model"] not in models:
                        models.append(r["model"])
                return [{"model": m} for m in models[: args[0]]]
            if "FROM request_logs" in sql:
                # Verify Postgres dialect uses $1/$2 and ILIKE when filters supplied
                self.state["last_logs_sql"] = sql
                return list(reversed(self.state["rows"]))
            return []

        async def fetchrow(self, sql, *args):
            if "COUNT(*) AS total_requests" in sql:
                rows = self.state["rows"]
                return {
                    "total_requests": len(rows),
                    "allowed_requests": sum(1 for r in rows if r["allowed"] == 1),
                    "throttled_requests": sum(1 for r in rows if r["allowed"] == 0),
                    "avg_latency_ms": sum(r["latency_ms"] for r in rows) / len(rows) if rows else 0.0,
                    "total_request_bytes": sum(r["request_bytes"] for r in rows),
                    "total_response_bytes": sum(r["response_bytes"] for r in rows),
                    "total_prompt_tokens": sum(r["prompt_tokens"] for r in rows),
                    "total_completion_tokens": sum(r["completion_tokens"] for r in rows),
                    "total_tokens": sum(r["total_tokens"] for r in rows),
                }
            if "COUNT(*) AS requests" in sql:
                cutoff = args[0]
                day_rows = [r for r in self.state["rows"] if r["allowed"] == 1 and r["timestamp"] >= cutoff]
                return {
                    "requests": len(day_rows),
                    "tokens": sum(r["total_tokens"] for r in day_rows),
                }
            return None

        async def fetchval(self, sql, *args):
            if "information_schema.tables" in sql:
                return True
            if "timestamp >= $1" in sql:
                return sum(1 for r in self.state["rows"] if r["timestamp"] >= args[0])
            return 0

    class FakeAcquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakePool:
        def __init__(self):
            self.state = {"executed": [], "locks": [], "migrations": [], "rows": [], "closed": False}

        def acquire(self):
            return FakeAcquire(FakeConn(self.state))

        async def close(self):
            self.state["closed"] = True

    pool = FakePool()
    pg = PostgresBackend("postgresql://user:secret@db.internal:5432/ratelimiter", pool=pool)
    assert pg.sanitized_target == "postgresql://user:***@db.internal:5432/ratelimiter"

    await pg.init_and_migrate()
    assert pg.schema_version == 3
    assert pool.state["locks"] == [
        ("lock", migrations.POSTGRES_MIGRATION_LOCK_KEY),
        ("unlock", migrations.POSTGRES_MIGRATION_LOCK_KEY),
    ]

    await pg.log_request(
        method="POST",
        path="/v1/chat/completions",
        status_code=200,
        latency_ms=88.0,
        model="meta/llama-3.3-70b-instruct",
        prompt_tokens=100,
        completion_tokens=50,
    )
    metrics = await pg.get_metrics()
    assert metrics["total_requests"] == 1
    assert metrics["total_tokens"] == 150
    assert metrics["recent_models"] == ["meta/llama-3.3-70b-instruct"]

    logs = await pg.get_logs(status_code=200, path="/v1/", model="llama")
    assert len(logs) == 1
    assert "ILIKE $2" in pool.state["last_logs_sql"]
    assert "ILIKE $3" in pool.state["last_logs_sql"]

    daily = await pg.get_daily_usage()
    assert daily["requests"] == 1
    assert daily["tokens"] == 150

    await pg.close()
    assert pool.state["closed"] is True


def test_cli_migrate_only_and_migration_status(monkeypatch, tmp_path, capsys):
    # Should succeed even without NVIDIA_API_KEY and without starting uvicorn
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.setattr(
        app_module.uvicorn,
        "run",
        lambda *a, **kw: pytest.fail("uvicorn.run should not be called for migration CLI flags"),
    )
    db_file = tmp_path / "cli_mig.db"
    cfg_file = tmp_path / "cli_mig.yaml"

    app_module.main(["--migration-status", "--db-path", str(db_file), "--config-file", str(cfg_file)])
    before_out = json.loads(capsys.readouterr().out)
    assert before_out["current_version"] == 0
    assert len(before_out["pending"]) == 3

    app_module.main(["--migrate-only", "--db-path", str(db_file), "--config-file", str(cfg_file)])
    after_out = json.loads(capsys.readouterr().out)
    assert after_out["backend"] == "sqlite"
    assert after_out["current_version"] == 3
    assert after_out["pending"] == []


# --- Spec 06: Separated Settings UI & Precedence Awareness ---
def test_settings_ui_separated_from_dashboard():
    with TestClient(app) as client:
        dash = client.get("/admin/")
        assert dash.status_code == 200
        assert 'href="/admin/settings"' in dash.text
        assert 'id="configForm"' not in dash.text

        settings_page = client.get("/admin/settings")
        assert settings_page.status_code == 200
        assert 'id="settingsForm"' in settings_page.text
        assert 'href="/admin/"' in settings_page.text
        assert "Rate Limiting" in settings_page.text
        assert "Database Storage" in settings_page.text
        assert "/admin/settings/state" in settings_page.text

        # Dynamic title propagates to /admin/settings as well
        resp_post = client.post("/admin/config", json={"admin": {"title": "Separated Settings Title"}})
        assert resp_post.status_code == 200
        settings_updated = client.get("/admin/settings")
        assert "<title>Separated Settings Title — Settings</title>" in settings_updated.text


def test_settings_state_precedence_locking_and_server_enforcement(monkeypatch, tmp_path):
    dotenv_file = tmp_path / "locked.env"
    dotenv_file.write_text("WINDOW_SECONDS=45\n", encoding="utf-8")
    cfg_file = tmp_path / "locked.yaml"

    monkeypatch.setenv("ENV_FILE", str(dotenv_file))
    monkeypatch.setenv("CONFIG_FILE", str(cfg_file))
    monkeypatch.setenv("RPM", "22")

    # Configure runtime as CLI with --burst-capacity 15 (argparse > env > dotenv > config.yaml)
    app_module.configure_runtime(
        argv=["--burst-capacity", "15"],
        require_secrets=True,
        from_cli=True,
    )

    with TestClient(app) as client:
        state = client.get("/admin/settings/state").json()
        sources = state["sources"]

        assert sources["rate_limit.burst_capacity"]["source"] == "argparse"
        assert sources["rate_limit.burst_capacity"]["locked"] is True
        assert "--burst-capacity" in sources["rate_limit.burst_capacity"]["detail"]

        assert sources["rate_limit.requests_per_minute"]["source"] == "env"
        assert sources["rate_limit.requests_per_minute"]["locked"] is True
        assert "RPM" in sources["rate_limit.requests_per_minute"]["detail"]

        assert sources["rate_limit.window_seconds"]["source"] == "dotenv"
        assert sources["rate_limit.window_seconds"]["locked"] is True
        assert ".env" in sources["rate_limit.window_seconds"]["detail"]

        # Unlocked fields remain editable
        assert sources["admin.title"]["locked"] is False
        assert sources["proxy.concurrency_limit"]["locked"] is False

        # Runtime info never leaks raw API key
        assert state["runtime_info"]["nvidia_api_key_source"] == "env"
        assert "nvapi-test-default-key" not in json.dumps(state)

        # Attempt to overwrite both locked and unlocked fields via POST /admin/config
        resp = client.post(
            "/admin/config",
            json={
                "admin": {"title": "Updated Unlocked Title"},
                "rate_limit": {
                    "requests_per_minute": 999.0,
                    "window_seconds": 999.0,
                    "burst_capacity": 999.0,
                },
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert set(body["ignored_locked_fields"]) == {
            "rate_limit.requests_per_minute",
            "rate_limit.window_seconds",
            "rate_limit.burst_capacity",
        }
        assert body["config"]["admin"]["title"] == "Updated Unlocked Title"
        assert body["config"]["rate_limit"]["requests_per_minute"] == 22.0
        assert body["config"]["rate_limit"]["window_seconds"] == 45.0
        assert body["config"]["rate_limit"]["burst_capacity"] == 15.0


def test_settings_optional_empty_fields_normalization():
    with TestClient(app) as client:
        # First set non-empty optional values
        r_set = client.post(
            "/admin/config",
            json={
                "rate_limit": {
                    "tokens_per_minute": 5000,
                    "max_wait_seconds": 2.5,
                    "daily_request_cap": 100,
                    "daily_token_cap": 50000,
                    "models": {"deepseek-ai/deepseek-r1": {"requests_per_minute": 5}},
                },
                "proxy": {
                    "allowed_models": "meta/llama-3.3-70b-instruct\ndeepseek-ai/deepseek-r1",
                    "blocked_models": "bad/model",
                    "model_routes": {
                        "meta/llama-3.3-70b-instruct": {"upstream_url": "http://nim-local:8000"}
                    },
                },
                "db": {"url_env": "CUSTOM_PG_DSN"},
            },
        )
        assert r_set.status_code == 200
        cfg1 = r_set.json()["config"]
        assert cfg1["rate_limit"]["tokens_per_minute"] == 5000
        assert cfg1["proxy"]["allowed_models"] == [
            "meta/llama-3.3-70b-instruct",
            "deepseek-ai/deepseek-r1",
        ]
        assert cfg1["db"]["url_env"] == "CUSTOM_PG_DSN"

        # Now submit empty strings / null for all optional fields -> cleanly resets
        r_clear = client.post(
            "/admin/config",
            json={
                "rate_limit": {
                    "tokens_per_minute": "",
                    "max_wait_seconds": None,
                    "daily_request_cap": "",
                    "daily_token_cap": "",
                    "models": "",
                },
                "proxy": {
                    "allowed_models": "",
                    "blocked_models": None,
                    "model_routes": "",
                    "catalog_cache_ttl": "",
                },
                "db": {"url_env": ""},
            },
        )
        assert r_clear.status_code == 200
        cfg2 = r_clear.json()["config"]
        assert cfg2["rate_limit"]["tokens_per_minute"] == 0
        assert cfg2["rate_limit"]["max_wait_seconds"] == 0.0
        assert cfg2["rate_limit"]["daily_request_cap"] == 0
        assert cfg2["rate_limit"]["daily_token_cap"] == 0
        assert cfg2["rate_limit"]["models"] == {}
        assert cfg2["proxy"]["allowed_models"] == []
        assert cfg2["proxy"]["blocked_models"] == []
        assert cfg2["proxy"]["model_routes"] == {}
        assert cfg2["proxy"]["catalog_cache_ttl"] == 300.0
        assert "url_env" not in cfg2["db"]


def test_skip_logging_root_and_favicon_get():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    with TestClient(app) as client:
        app_module.http_client = httpx.AsyncClient(
            base_url="https://mock.nvidia.test",
            transport=httpx.MockTransport(handler),
        )
        assert client.get("/").status_code == 200
        assert client.get("/favicon.ico").status_code == 200
        assert client.get("/v1/models").status_code == 200
        time.sleep(0.15)

        logs = client.get("/admin/logs").json()["logs"]
        logged_paths = [(entry["method"], entry["path"]) for entry in logs]
        assert ("GET", "/") not in logged_paths
        assert ("GET", "/favicon.ico") not in logged_paths
        assert ("GET", "/v1/models") in logged_paths
