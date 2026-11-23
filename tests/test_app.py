import email.utils
import json
import os
import time

import httpx
import pytest
import yaml
from dotenv import dotenv_values
from fastapi.testclient import TestClient

import app as app_module
import database
from app import TokenBucket, app, extract_tokens
from rate_limiter import (
    RateLimitExceeded,
    RateLimitManager,
    TPMBucket,
    parse_retry_after,
)
from settings import ConfigError, is_loopback_host, resolve_settings, validate_bind_guardrails

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
    monkeypatch.delenv("ADMIN_TITLE", raising=False)
    monkeypatch.delenv("HOST", raising=False)
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.delenv("ALLOW_NON_LOCAL_BIND", raising=False)
    monkeypatch.delenv("STRICT_TRIAL_GUARDRAILS", raising=False)
    monkeypatch.setattr(app_module, "_cli_configured", False)

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
    monkeypatch.setenv("DAILY_REQUEST_CAP", "1")

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
