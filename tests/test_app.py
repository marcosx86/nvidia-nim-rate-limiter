import asyncio
import os
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

import database
from app import app, token_bucket, load_config, save_config, TokenBucket, extract_tokens

TEST_DB = "test_limiter.db"

@pytest.fixture(autouse=True)
def setup_test_db():
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
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


def test_admin_dashboard_and_config():
    with TestClient(app) as client:
        # Dashboard HTML
        resp_html = client.get("/admin/")
        assert resp_html.status_code == 200
        assert "<title>NVIDIA NIM Proxy Admin Dashboard</title>" in resp_html.text

        # Get config
        resp_cfg = client.get("/admin/config")
        assert resp_cfg.status_code == 200
        assert "proxy" in resp_cfg.json()

        # Update config
        update_payload = {
            "rate_limit": {
                "requests_per_minute": 10.0,
                "window_seconds": 60.0,
                "burst_capacity": 10.0,
            },
            "proxy": {
                "concurrency_limit": 2,
                "upstream_url": "https://integrate.api.nvidia.com"
            }
        }
        resp_post = client.post("/admin/config", json=update_payload)
        assert resp_post.status_code == 200
        assert resp_post.json()["status"] == "updated"

        # Verify updated metrics endpoint
        resp_metrics = client.get("/admin/metrics")
        assert resp_metrics.status_code == 200
        assert "total_requests" in resp_metrics.json()
