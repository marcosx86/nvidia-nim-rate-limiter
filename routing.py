import ipaddress
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Set
from urllib.parse import urlparse

import httpx
from dotenv import dotenv_values


def detect_deployment_mode(upstream_url: str, configured_mode: str = "auto") -> str:
    """Classify the upstream deployment mode as 'trial', 'self_hosted', or 'partner'."""
    mode = (configured_mode or "auto").strip().lower()
    if mode in ("trial", "self_hosted", "partner"):
        return mode

    parsed = urlparse((upstream_url or "").strip())
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return "trial"

    if "integrate.api.nvidia.com" in host:
        return "trial"

    if host == "localhost" or host.endswith(".local"):
        return "self_hosted"

    try:
        ip = ipaddress.ip_address(host)
        if ip.is_loopback or ip.is_private or ip.is_link_local:
            return "self_hosted"
    except ValueError:
        # Single-label container/service names (e.g., "nim-local")
        if "." not in host:
            return "self_hosted"

    return "partner"


@dataclass(frozen=True)
class UpstreamTarget:
    base_url: str
    api_key: str
    route_name: str


class ModelRouter:
    """Enforces model allowlist/denylist governance and per-model upstream routing."""

    def __init__(
        self,
        allowed_models: Optional[List[str]] = None,
        blocked_models: Optional[List[str]] = None,
        model_routes: Optional[Mapping[str, Mapping[str, Any]]] = None,
        env_file: str = ".env",
    ) -> None:
        self.allowed_models: List[str] = list(allowed_models or [])
        self.blocked_models: List[str] = list(blocked_models or [])
        self.model_routes: Dict[str, Dict[str, Any]] = {
            k: dict(v) for k, v in (model_routes or {}).items()
        }
        self.env_file = env_file
        self._route_clients: Dict[str, httpx.AsyncClient] = {}

    @classmethod
    def from_config(
        cls,
        config_data: Mapping[str, Any],
        env_file: str = ".env",
    ) -> "ModelRouter":
        proxy_cfg = config_data.get("proxy", {})
        return cls(
            allowed_models=proxy_cfg.get("allowed_models") or [],
            blocked_models=proxy_cfg.get("blocked_models") or [],
            model_routes=proxy_cfg.get("model_routes") or {},
            env_file=env_file,
        )

    def configure_from_dict(
        self,
        config_data: Mapping[str, Any],
        env_file: Optional[str] = None,
    ) -> None:
        proxy_cfg = config_data.get("proxy", {})
        self.allowed_models = list(proxy_cfg.get("allowed_models") or [])
        self.blocked_models = list(proxy_cfg.get("blocked_models") or [])
        self.model_routes = {
            k: dict(v) for k, v in (proxy_cfg.get("model_routes") or {}).items()
        }
        if env_file is not None:
            self.env_file = env_file

    def check_model_policy(self, model: Optional[str]) -> Optional[str]:
        """Return rejection reason if model is blocked or outside allowlist, else None."""
        if not model:
            return None
        if model in self.blocked_models:
            return "model_blocked"
        if self.allowed_models and model not in self.allowed_models:
            return "model_not_in_allowlist"
        return None

    def resolve_target(
        self,
        model: Optional[str],
        default_upstream: str,
        default_api_key: str,
        environ: Optional[Mapping[str, str]] = None,
        dotenv_map: Optional[Mapping[str, Optional[str]]] = None,
    ) -> UpstreamTarget:
        """Resolve the upstream URL and credential for a given model."""
        if not model or model not in self.model_routes:
            return UpstreamTarget(
                base_url=default_upstream.rstrip("/"),
                api_key=default_api_key,
                route_name="default",
            )

        route = self.model_routes[model]
        base_url = str(route["upstream_url"]).strip().rstrip("/")
        strip_auth = bool(route.get("strip_auth", False))
        api_key_env = route.get("api_key_env")

        if strip_auth:
            return UpstreamTarget(base_url=base_url, api_key="", route_name=model)

        if api_key_env:
            env_src = os.environ if environ is None else environ
            key_val = (env_src.get(api_key_env) or "").strip()
            if not key_val:
                if dotenv_map is None and self.env_file and os.path.exists(self.env_file):
                    dotenv_map = dotenv_values(self.env_file)
                if dotenv_map:
                    key_val = (dotenv_map.get(api_key_env) or "").strip()
            return UpstreamTarget(base_url=base_url, api_key=key_val, route_name=model)

        # Keyless local/self-hosted upstream by default when no api_key_env is specified
        return UpstreamTarget(base_url=base_url, api_key="", route_name=model)

    def get_client(
        self,
        base_url: str,
        default_client: Optional[httpx.AsyncClient],
        default_upstream: str,
    ) -> httpx.AsyncClient:
        normalized = base_url.rstrip("/")
        if normalized == default_upstream.rstrip("/") and default_client is not None:
            return default_client

        client = self._route_clients.get(normalized)
        if client is None:
            client = httpx.AsyncClient(
                base_url=normalized,
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=300.0,
                    write=300.0,
                    pool=300.0,
                ),
            )
            self._route_clients[normalized] = client
        return client

    async def aclose_all(self) -> None:
        for client in list(self._route_clients.values()):
            await client.aclose()
        self._route_clients.clear()

    def status_snapshot(self) -> Dict[str, Any]:
        safe_routes: Dict[str, Dict[str, Any]] = {}
        for model_id, route in self.model_routes.items():
            entry: Dict[str, Any] = {"upstream_url": route["upstream_url"]}
            if route.get("api_key_env"):
                entry["api_key_env"] = route["api_key_env"]
            if route.get("strip_auth"):
                entry["strip_auth"] = True
            safe_routes[model_id] = entry
        return {
            "allowed_models": list(self.allowed_models),
            "blocked_models": list(self.blocked_models),
            "model_routes": safe_routes,
        }


class ModelCatalogCache:
    """Passive cache of /v1/models catalog IDs with configurable TTL."""

    def __init__(self, ttl_seconds: float = 300.0) -> None:
        self.ttl_seconds = float(ttl_seconds)
        self.models: List[str] = []
        self._model_set: Set[str] = set()
        self.fetched_at: float = 0.0

    def set_ttl(self, ttl_seconds: float) -> None:
        self.ttl_seconds = float(ttl_seconds)

    def clear(self) -> None:
        self.models = []
        self._model_set = set()
        self.fetched_at = 0.0

    def is_expired(self, now: Optional[float] = None) -> bool:
        if self.ttl_seconds <= 0 or self.fetched_at <= 0:
            return True
        current = time.time() if now is None else now
        return (current - self.fetched_at) > self.ttl_seconds

    def update_from_payload(self, payload: Any, now: Optional[float] = None) -> int:
        if not isinstance(payload, Mapping):
            return 0
        data = payload.get("data")
        if not isinstance(data, list):
            return 0

        ids: List[str] = []
        seen: Set[str] = set()
        for item in data:
            if isinstance(item, Mapping):
                mid = str(item.get("id") or "").strip()
                if mid and mid not in seen:
                    seen.add(mid)
                    ids.append(mid)

        if ids:
            self.models = ids
            self._model_set = seen
            self.fetched_at = time.time() if now is None else now
        return len(ids)

    def is_known(self, model: Optional[str], now: Optional[float] = None) -> Optional[bool]:
        """Return True if model is in active cache, False if absent, None if cache unavailable."""
        if not model or self.is_expired(now=now) or not self._model_set:
            return None
        return model in self._model_set

    def snapshot(self, now: Optional[float] = None) -> Dict[str, Any]:
        current = time.time() if now is None else now
        age = round(max(0.0, current - self.fetched_at), 2) if self.fetched_at > 0 else None
        return {
            "cached_count": len(self.models),
            "models": list(self.models),
            "ttl_seconds": self.ttl_seconds,
            "age_seconds": age,
            "active": not self.is_expired(now=current) and bool(self.models),
        }
