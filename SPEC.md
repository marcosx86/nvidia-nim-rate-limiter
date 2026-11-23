# Master Specification (`nvidia-nim-rate-limiter`)

> **System Architecture & Invariants:** See [ARCHITECTURE.md](ARCHITECTURE.md).  
> **Feature Specifications:** See [`specs/`](specs/).

---

## 1. Purpose & Deployment Topology

`nvidia-nim-rate-limiter` is a single-worker, asynchronous FastAPI reverse proxy, rate limiter, and observability dashboard sitting between local AI agent stacks (e.g., **Hermes sandbox → `iron-proxy` → `nvidia-nim-rate-limiter`**) and **NVIDIA NIM APIs** (`https://integrate.api.nvidia.com` or self-hosted/partner NIM endpoints).

```text
UNTRUSTED SANDBOX                    TRUSTED HOST (127.0.0.1)
┌────────────────────┐               ┌────────────────────────────────────────┐
│ Hermes Sandbox     │               │ iron-proxy ──► nvidia-nim-rate-limiter │
│ (opaque token only)├──────────────►│                (owns NVIDIA_API_KEY)   │
└────────────────────┘               └────────────────────┬───────────────────┘
                                                          │ Authorization: Bearer nvapi-*
                                                          ▼
                                             integrate.api.nvidia.com
                                             (or self-hosted/partner NIM)
```

### Core Design Principles

1. **Credential Isolation**: The real `NVIDIA_API_KEY` (`nvapi-*`) never enters the agent sandbox, client responses, `/status`, `/admin/config`, or `config.yaml`. Client `Authorization` headers are stripped and replaced upstream.
2. **Single-Worker Determinism**: Token-bucket, TPM, and concurrency state are in-memory; deployments MUST run with `--workers 1` so limits cannot be bypassed across worker processes.
3. **Low-Latency Streaming**: Responses stream chunk-by-chunk (`httpx.AsyncClient.stream` → `StreamingResponse`) without buffering full LLM responses.
4. **Loopback-First Security**: Default bind is `127.0.0.1:8000` so the credential-injecting proxy is not exposed as an unauthenticated network endpoint.

---

## 2. NVIDIA Build (`build.nvidia.com/models`) TOS Alignment

Hosted trial endpoints on `integrate.api.nvidia.com` are governed by the **NVIDIA API Trial Terms of Service** and **NVIDIA Developer Program** policies:

| TOS / Platform Constraint | Proxy Requirement | Spec |
| :--- | :--- | :--- |
| **Prototyping & evaluation only** (no production/commercial use on free tier; scale via self-hosted NIM or partner endpoints) | Detect trial upstream, surface clear UI/startup indicators, guard non-loopback binds, and support per-model upstream graduation routing. | [`specs/03-trial-guardrails-and-upstream-routing.md`](specs/03-trial-guardrails-and-upstream-routing.md) |
| **Dynamic, model-dependent limits** (RPM/TPM vary by model & load; no free-tier increases; no multi-account circumvention) | Global + per-model `TokenBucket` RPM, **TPM** limiting, concurrency=1 default, adaptive `429` / `Retry-After` cooldown, and queue timeouts. | [`specs/02-rate-limiting-and-429-backoff.md`](specs/02-rate-limiting-and-429-backoff.md) |
| **Data retention & "No Confidential / Personal Data" warning** (trial inputs/outputs may be logged to operate/improve services) | Outbound secret/PII scanner (`warn` / `redact` / `block`), zero-content SQLite metadata logging, and configurable log retention purge. | [`specs/04-privacy-headers-and-observability.md`](specs/04-privacy-headers-and-observability.md) |
| **Per-model licenses & no continuity guarantee** | Model allowlist/denylist governance and `/v1/models` catalog health caching. | [`specs/03-trial-guardrails-and-upstream-routing.md`](specs/03-trial-guardrails-and-upstream-routing.md) |
| **Gateway header limits & strict config hygiene** | Header allowlist/8 KB size guard and strict `argparse > env > dotenv > config.yaml > default/fail` precedence. | [`specs/01-configuration-and-cli.md`](specs/01-configuration-and-cli.md), [`specs/04-privacy-headers-and-observability.md`](specs/04-privacy-headers-and-observability.md) |

---

## 3. Specification Index & Implementation Status

| Spec | Scope | Status |
| :--- | :--- | :--- |
| [`01-configuration-and-cli.md`](specs/01-configuration-and-cli.md) | Precedence (`argparse > env > dotenv > config.yaml > default/fail`), CLI entrypoint, bind guardrails | **Implemented** *(bind guardrail planned)* |
| [`02-rate-limiting-and-429-backoff.md`](specs/02-rate-limiting-and-429-backoff.md) | Global/per-model `TokenBucket`, TPM limiting, `429`/`Retry-After` backoff, `--max-wait-seconds`, quota caps | **Partial** *(global RPM & concurrency implemented; per-model, TPM, `429` backoff planned)* |
| [`03-trial-guardrails-and-upstream-routing.md`](specs/03-trial-guardrails-and-upstream-routing.md) | Trial-mode detection & banner, per-model upstream routing, model allowlist/denylist, catalog health | **Planned** |
| [`04-privacy-headers-and-observability.md`](specs/04-privacy-headers-and-observability.md) | Secret/PII outbound guardrail, header hygiene, SQLite log retention purge, `/admin/` & `/status` updates | **Partial** *(SQLite metrics/logs & `/admin/` implemented; PII/header/retention planned)* |
