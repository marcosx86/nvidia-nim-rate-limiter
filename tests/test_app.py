import os
import pytest
import yaml
from fastapi.testclient import TestClient

import app as app_module
import database
from app import TokenBucket, app, extract_tokens
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
    monkeypatch.delenv("MAX_CONCURRENT", raising=False)
    monkeypatch.delenv("UPSTREAM_URL", raising=False)
    monkeypatch.delenv("ADMIN_TITLE", raising=False)
    monkeypatch.delenv("HOST", raising=False)
    monkeypatch.delenv("PORT", raising=False)
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
