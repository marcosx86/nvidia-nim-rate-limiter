# NVIDIA NIM Rate Limiter & Observability Proxy

A lightweight, asynchronous reverse proxy and rate limiter designed for **NVIDIA NIM APIs** (`integrate.api.nvidia.com`). 

It provides strict token-bucket rate limiting, concurrency serialization, low-latency LLM chunk streaming, SQLite-backed request logging and metrics, YAML configuration persistence, and an interactive Web Admin Dashboard.

---

## 🌟 Key Features

- ⚡ **Low-Latency LLM Streaming**: Uses `httpx.AsyncClient.stream` and FastAPI `StreamingResponse` to pass Server-Sent Events (SSE) / streaming tokens immediately without buffering full responses.
- 🎯 **Token Bucket Rate Limiting**: Smooth, predictable rate-limiting prevents burst double-dipping while enforcing exact Requests-Per-Minute (RPM) limits.
- 🔒 **Credential Isolation**: Client applications communicate with the proxy without handling raw `nvapi-*` API keys. The proxy injects the `NVIDIA_API_KEY` upstream securely.
- 💾 **YAML Configuration Persistence**: Configuration stored in `config.yaml` can be read on startup and dynamically updated via `/admin/config`.
- 📊 **SQLite Usage Metrics & Logging**: Asynchronously logs every request (path, method, latency, status code, byte transfer count) to a local SQLite database (`limiter.db`).
- 🖥️ **Web Admin Dashboard (`/admin/`)**: Integrated dark-mode dashboard displaying live token levels, throughput, latency, request logs, and interactive configuration form.
- 🩺 **Health Checks**: Ready for container orchestrators via `/healthz` and `/status`.

---

## 🚀 Quick Start

### 1. Local Development Setup

Clone the repository and install dependencies:

```bash
python -m venv .venv
# On Windows
.venv\Scripts\activate
# On Linux/macOS
source .venv/bin/activate

pip install -r requirements-dev.txt
```

Set your NVIDIA API Key in `.env`:

```env
NVIDIA_API_KEY=nvapi-YOUR_REAL_KEY_HERE
```

Run the FastAPI application:

```bash
uvicorn app:app --host 127.0.0.1 --port 8000 --reload
```

---

### 2. Docker Compose Deployment

Build and run using Docker Compose:

```bash
docker compose up -d --build
```

Verify service health:

```bash
curl http://127.0.0.1:8199/healthz
```

---

## ⚙️ Configuration Reference (`config.yaml`)

Configuration can be specified in `config.yaml` or overridden via environment variables:

| Environment Variable | YAML Path | Default Value | Description |
| :--- | :--- | :--- | :--- |
| `NVIDIA_API_KEY` | - | *Required* | Raw NVIDIA NIM API key (`nvapi-...`) |
| `RPM` | `rate_limit.requests_per_minute` | `6.0` | Allowed requests per minute |
| `WINDOW_SECONDS` | `rate_limit.window_seconds` | `60.0` | Time window in seconds |
| `BURST_CAPACITY` | `rate_limit.burst_capacity` | `6.0` | Max token bucket capacity |
| `MAX_CONCURRENT` | `proxy.concurrency_limit` | `1` | Max simultaneous requests upstream |
| `UPSTREAM_URL` | `proxy.upstream_url` | `https://integrate.api.nvidia.com` | Base URL of upstream NIM API |
| `DB_PATH` | `db.path` | `limiter.db` | Path to SQLite database file |

---

## 🛠️ API & Admin Endpoints

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/{path}` | `GET/POST/...` | Reverse proxy route forwarding to NVIDIA NIM with key injection |
| `/healthz` | `GET` | System liveness probe (`status: ok`) |
| `/status` | `GET` | Real-time proxy status, token bucket state, and active concurrency |
| `/admin/` | `GET` | Web Dashboard interface |
| `/admin/metrics` | `GET` | Aggregated usage metrics JSON (RPM, avg latency, status codes) |
| `/admin/logs` | `GET` | Queryable request history (`limit`, `offset`, `status_code`, `path`) |
| `/admin/config` | `GET / POST` | Inspect or update proxy and rate limit configuration dynamically |

---

## 🧪 Running Tests

Execute the pytest suite:

```bash
pytest
```

---

## 🔒 Security Best Practices

1. **Never commit `.env` or `limiter.db`**: Both are listed in `.gitignore`.
2. **Local Port Binding**: Docker Compose binds port `8199` to `127.0.0.1` by default to prevent exposing the unauthenticated proxy publicly.
