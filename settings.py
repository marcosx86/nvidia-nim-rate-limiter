import argparse
import ipaddress
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlparse, urlunparse

import yaml
from dotenv import dotenv_values


class ConfigError(ValueError):
    """Raised when a required configuration parameter is missing or invalid."""


class _RequiredSentinel:
    def __repr__(self) -> str:
        return "<REQUIRED>"


REQUIRED = _RequiredSentinel()


def _parse_non_empty_str(value: Any) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError("must be a non-empty string")
    return text


def _parse_positive_int(value: Any) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError("must be a positive integer")
    return parsed


def _parse_positive_float(value: Any) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise ValueError("must be a positive number")
    return parsed


def _parse_non_negative_int(value: Any) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ValueError("must be a non-negative integer")
    return parsed


def _parse_non_negative_float(value: Any) -> float:
    parsed = float(value)
    if parsed < 0:
        raise ValueError("must be a non-negative number")
    return parsed


def _parse_port(value: Any) -> int:
    parsed = int(value)
    if not (1 <= parsed <= 65535):
        raise ValueError("must be between 1 and 65535")
    return parsed


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    raise ValueError("must be a boolean (true/false, 1/0, yes/no, on/off)")


def _parse_deployment_mode(value: Any) -> str:
    text = str(value).strip().lower()
    if text in ("auto", "trial", "self_hosted", "partner"):
        return text
    raise ValueError("must be one of: auto, trial, self_hosted, partner")


@dataclass(frozen=True)
class ParamSpec:
    name: str
    cli_flag: str
    env_var: str
    parser: Callable[[Any], Any]
    default: Any = REQUIRED
    yaml_section: Optional[str] = None
    yaml_key: Optional[str] = None
    secret: bool = False
    help: str = ""

    @property
    def is_required(self) -> bool:
        return self.default is REQUIRED


ENV_FILE_SPEC = ParamSpec(
    name="env_file",
    cli_flag="--env-file",
    env_var="ENV_FILE",
    parser=_parse_non_empty_str,
    default=".env",
    help="Path to .env file for dotenv fallback (default: .env).",
)

CONFIG_FILE_SPEC = ParamSpec(
    name="config_file",
    cli_flag="--config-file",
    env_var="CONFIG_FILE",
    parser=_parse_non_empty_str,
    default="config.yaml",
    help="Path to YAML config file for runtime persistence (default: config.yaml).",
)

PARAM_SPECS: Tuple[ParamSpec, ...] = (
    ParamSpec(
        name="nvidia_api_key",
        cli_flag="--nvidia-api-key",
        env_var="NVIDIA_API_KEY",
        parser=_parse_non_empty_str,
        default=REQUIRED,
        secret=True,
        help="Required NVIDIA NIM API key (nvapi-...). Never persisted to config.yaml.",
    ),
    ParamSpec(
        name="upstream_url",
        cli_flag="--upstream-url",
        env_var="UPSTREAM_URL",
        parser=_parse_non_empty_str,
        default="https://integrate.api.nvidia.com",
        yaml_section="proxy",
        yaml_key="upstream_url",
        help="Base URL of upstream NVIDIA NIM API (default: https://integrate.api.nvidia.com).",
    ),
    ParamSpec(
        name="deployment_mode",
        cli_flag="--deployment-mode",
        env_var="DEPLOYMENT_MODE",
        parser=_parse_deployment_mode,
        default="auto",
        yaml_section="proxy",
        yaml_key="deployment_mode",
        help="Deployment mode: auto, trial, self_hosted, or partner (default: auto).",
    ),
    ParamSpec(
        name="catalog_cache_ttl",
        cli_flag="--catalog-cache-ttl",
        env_var="CATALOG_CACHE_TTL",
        parser=_parse_non_negative_float,
        default=300.0,
        yaml_section="proxy",
        yaml_key="catalog_cache_ttl",
        help="Seconds to cache /v1/models catalog responses; 0 disables (default: 300.0).",
    ),
    ParamSpec(
        name="enforce_catalog",
        cli_flag="--enforce-catalog",
        env_var="ENFORCE_CATALOG",
        parser=_parse_bool,
        default=False,
        yaml_section="proxy",
        yaml_key="enforce_catalog",
        help="Reject requests with 404 when model is absent from cached /v1/models catalog (default: false).",
    ),
    ParamSpec(
        name="concurrency_limit",
        cli_flag="--max-concurrent",
        env_var="MAX_CONCURRENT",
        parser=_parse_positive_int,
        default=1,
        yaml_section="proxy",
        yaml_key="concurrency_limit",
        help="Maximum simultaneous upstream requests (default: 1).",
    ),
    ParamSpec(
        name="requests_per_minute",
        cli_flag="--rpm",
        env_var="RPM",
        parser=_parse_positive_float,
        default=6.0,
        yaml_section="rate_limit",
        yaml_key="requests_per_minute",
        help="Allowed requests per window (default: 6.0).",
    ),
    ParamSpec(
        name="window_seconds",
        cli_flag="--window-seconds",
        env_var="WINDOW_SECONDS",
        parser=_parse_positive_float,
        default=60.0,
        yaml_section="rate_limit",
        yaml_key="window_seconds",
        help="Rate-limit window duration in seconds (default: 60.0).",
    ),
    ParamSpec(
        name="burst_capacity",
        cli_flag="--burst-capacity",
        env_var="BURST_CAPACITY",
        parser=_parse_positive_float,
        default=6.0,
        yaml_section="rate_limit",
        yaml_key="burst_capacity",
        help="Maximum token bucket burst capacity (default: 6.0).",
    ),
    ParamSpec(
        name="tokens_per_minute",
        cli_flag="--tpm",
        env_var="TPM",
        parser=_parse_non_negative_int,
        default=0,
        yaml_section="rate_limit",
        yaml_key="tokens_per_minute",
        help="Global Tokens-Per-Minute limit; 0 disables TPM limiting (default: 0).",
    ),
    ParamSpec(
        name="max_wait_seconds",
        cli_flag="--max-wait-seconds",
        env_var="MAX_WAIT_SECONDS",
        parser=_parse_non_negative_float,
        default=0.0,
        yaml_section="rate_limit",
        yaml_key="max_wait_seconds",
        help="Max seconds to wait in queue before returning local 429; 0 waits indefinitely (default: 0.0).",
    ),
    ParamSpec(
        name="daily_request_cap",
        cli_flag="--daily-request-cap",
        env_var="DAILY_REQUEST_CAP",
        parser=_parse_non_negative_int,
        default=0,
        yaml_section="rate_limit",
        yaml_key="daily_request_cap",
        help="Max allowed requests per UTC day before local 429; 0 disables (default: 0).",
    ),
    ParamSpec(
        name="daily_token_cap",
        cli_flag="--daily-token-cap",
        env_var="DAILY_TOKEN_CAP",
        parser=_parse_non_negative_int,
        default=0,
        yaml_section="rate_limit",
        yaml_key="daily_token_cap",
        help="Max allowed LLM tokens per UTC day before local 429; 0 disables (default: 0).",
    ),
    ParamSpec(
        name="db_path",
        cli_flag="--db-path",
        env_var="DB_PATH",
        parser=_parse_non_empty_str,
        default="limiter.db",
        yaml_section="db",
        yaml_key="path",
        help="Path to SQLite request log database (default: limiter.db).",
    ),
    ParamSpec(
        name="db_url",
        cli_flag="--db-url",
        env_var="DB_URL",
        parser=_parse_non_empty_str,
        default="",
        secret=True,
        help="Optional database URL (sqlite:///... or postgresql://...). Never persisted to config.yaml.",
    ),
    ParamSpec(
        name="db_pool_min",
        cli_flag="--db-pool-min",
        env_var="DB_POOL_MIN",
        parser=_parse_positive_int,
        default=1,
        yaml_section="db",
        yaml_key="pool_min",
        help="Minimum PostgreSQL connection pool size (default: 1).",
    ),
    ParamSpec(
        name="db_pool_max",
        cli_flag="--db-pool-max",
        env_var="DB_POOL_MAX",
        parser=_parse_positive_int,
        default=5,
        yaml_section="db",
        yaml_key="pool_max",
        help="Maximum PostgreSQL connection pool size (default: 5).",
    ),
    ParamSpec(
        name="db_connect_timeout",
        cli_flag="--db-connect-timeout",
        env_var="DB_CONNECT_TIMEOUT",
        parser=_parse_positive_float,
        default=10.0,
        yaml_section="db",
        yaml_key="connect_timeout",
        help="Database connection timeout in seconds (default: 10.0).",
    ),
    ParamSpec(
        name="admin_title",
        cli_flag="--admin-title",
        env_var="ADMIN_TITLE",
        parser=_parse_non_empty_str,
        default="NVIDIA NIM Rate Limiter Dashboard",
        yaml_section="admin",
        yaml_key="title",
        help="Title displayed in the /admin/ dashboard.",
    ),
    ParamSpec(
        name="wwwroot_dir",
        cli_flag="--wwwroot-dir",
        env_var="WWWROOT_DIR",
        parser=_parse_non_empty_str,
        default="wwwroot",
        help="Directory serving static dashboard assets (default: wwwroot).",
    ),
    ParamSpec(
        name="host",
        cli_flag="--host",
        env_var="HOST",
        parser=_parse_non_empty_str,
        default="127.0.0.1",
        help="Bind host when launched via CLI (default: 127.0.0.1).",
    ),
    ParamSpec(
        name="port",
        cli_flag="--port",
        env_var="PORT",
        parser=_parse_port,
        default=8000,
        help="Bind port when launched via CLI (default: 8000).",
    ),
    ParamSpec(
        name="allow_non_local_bind",
        cli_flag="--allow-non-local-bind",
        env_var="ALLOW_NON_LOCAL_BIND",
        parser=_parse_bool,
        default=False,
        help="Allow binding to non-loopback addresses such as 0.0.0.0 (default: false).",
    ),
    ParamSpec(
        name="strict_trial_guardrails",
        cli_flag="--strict-trial-guardrails",
        env_var="STRICT_TRIAL_GUARDRAILS",
        parser=_parse_bool,
        default=False,
        help="Fail fast on unsafe trial/bind configurations instead of warning (default: false).",
    ),
)

ALL_SPECS: Tuple[ParamSpec, ...] = (ENV_FILE_SPEC, CONFIG_FILE_SPEC, *PARAM_SPECS)


@dataclass
class AppSettings:
    env_file: str
    config_file: str
    nvidia_api_key: str
    upstream_url: str
    deployment_mode: str
    catalog_cache_ttl: float
    enforce_catalog: bool
    concurrency_limit: int
    requests_per_minute: float
    window_seconds: float
    burst_capacity: float
    tokens_per_minute: int
    max_wait_seconds: float
    daily_request_cap: int
    daily_token_cap: int
    db_path: str
    db_url: str
    db_pool_min: int
    db_pool_max: int
    db_connect_timeout: float
    admin_title: str
    wwwroot_dir: str
    host: str
    port: int
    allow_non_local_bind: bool = False
    strict_trial_guardrails: bool = False
    migrate_only: bool = False
    migration_status: bool = False
    db_url_env: Optional[str] = None
    model_rate_limits: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    allowed_models: List[str] = field(default_factory=list)
    blocked_models: List[str] = field(default_factory=list)
    model_routes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    sources: Dict[str, str] = field(default_factory=dict)

    def to_config_dict(self) -> Dict[str, Any]:
        """Return non-secret runtime config dict matching config.yaml structure."""
        db_section: Dict[str, Any] = {
            "path": self.db_path,
            "pool_min": self.db_pool_min,
            "pool_max": self.db_pool_max,
            "connect_timeout": self.db_connect_timeout,
        }
        if self.db_url_env:
            db_section["url_env"] = self.db_url_env
        return {
            "proxy": {
                "upstream_url": self.upstream_url,
                "deployment_mode": self.deployment_mode,
                "catalog_cache_ttl": self.catalog_cache_ttl,
                "enforce_catalog": self.enforce_catalog,
                "concurrency_limit": self.concurrency_limit,
                "allowed_models": list(self.allowed_models),
                "blocked_models": list(self.blocked_models),
                "model_routes": {k: dict(v) for k, v in self.model_routes.items()},
            },
            "rate_limit": {
                "requests_per_minute": self.requests_per_minute,
                "window_seconds": self.window_seconds,
                "burst_capacity": self.burst_capacity,
                "tokens_per_minute": self.tokens_per_minute,
                "max_wait_seconds": self.max_wait_seconds,
                "daily_request_cap": self.daily_request_cap,
                "daily_token_cap": self.daily_token_cap,
                "models": {k: dict(v) for k, v in self.model_rate_limits.items()},
            },
            "db": db_section,
            "admin": {
                "title": self.admin_title,
            },
        }


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for the NVIDIA NIM rate-limiter proxy."""
    parser = argparse.ArgumentParser(
        prog="nvidia-nim-rate-limiter",
        description=(
            "NVIDIA NIM Rate Limiter & Observability Proxy. "
            "Precedence: CLI args > environment variables > .env file > config.yaml > defaults "
            "(or fail if a required parameter is missing)."
        ),
    )
    for spec in ALL_SPECS:
        env_hint = f" [env: {spec.env_var}]" if spec.env_var else ""
        req_hint = " (REQUIRED)" if spec.is_required else ""
        parser.add_argument(
            spec.cli_flag,
            dest=spec.name,
            default=None,
            type=str,
            help=f"{spec.help}{env_hint}{req_hint}",
        )
    parser.add_argument(
        "--migrate-only",
        dest="migrate_only",
        action="store_true",
        default=False,
        help="Run pending database migrations, print status JSON, and exit without starting server.",
    )
    parser.add_argument(
        "--migration-status",
        dest="migration_status",
        action="store_true",
        default=False,
        help="Inspect applied/pending database migrations, print status JSON, and exit.",
    )
    return parser


def _has_value(raw: Any) -> bool:
    if raw is None:
        return False
    if isinstance(raw, str) and not raw.strip():
        return False
    return True


def _coerce(spec: ParamSpec, raw: Any, source: str) -> Any:
    try:
        return spec.parser(raw)
    except Exception as exc:
        raise ConfigError(
            f"Invalid value for '{spec.name}' ({spec.cli_flag} / {spec.env_var}) "
            f"from {source}: {exc}"
        ) from exc


def _resolve_single(
    spec: ParamSpec,
    cli_values: Mapping[str, Any],
    environ: Mapping[str, str],
    dotenv_map: Mapping[str, Optional[str]],
    yaml_config: Mapping[str, Any],
    require_secrets: bool,
) -> Tuple[Any, str]:
    """Resolve one parameter with precedence:
    argparse > environment var > dotenv > config.yaml (non-secret) > default or fail.
    """
    # 1. argparse (if flag was explicitly passed, validate it directly)
    cli_raw = cli_values.get(spec.name)
    if cli_raw is not None:
        return _coerce(spec, cli_raw, "argparse"), "argparse"

    # 2. environment variable
    env_raw = environ.get(spec.env_var) if spec.env_var else None
    if _has_value(env_raw):
        return _coerce(spec, env_raw, f"env:{spec.env_var}"), "env"

    # 3. dotenv
    dotenv_raw = dotenv_map.get(spec.env_var) if spec.env_var else None
    if _has_value(dotenv_raw):
        return _coerce(spec, dotenv_raw, f"dotenv:{spec.env_var}"), "dotenv"

    # 4. config.yaml (strictly non-secret persisted settings only)
    if not spec.secret and spec.yaml_section and spec.yaml_key:
        section_data = yaml_config.get(spec.yaml_section)
        if isinstance(section_data, Mapping) and spec.yaml_key in section_data:
            yaml_raw = section_data[spec.yaml_key]
            if _has_value(yaml_raw):
                return _coerce(spec, yaml_raw, "config.yaml"), "config.yaml"

    # 5. default value
    if not spec.is_required:
        return spec.default, "default"

    # 6. required parameter missing -> fail (unless deferred during module import)
    if not require_secrets and spec.secret:
        return "", "unset"

    raise ConfigError(
        f"Missing required parameter '{spec.name}': provide {spec.cli_flag}, "
        f"set environment variable {spec.env_var}, or define {spec.env_var} in the .env file."
    )


def validate_model_rate_limits(raw_models: Any) -> Dict[str, Dict[str, Any]]:
    """Validate per-model rate limit overrides from config.yaml or /admin/config."""
    if raw_models is None:
        return {}
    if not isinstance(raw_models, Mapping):
        raise ConfigError("Invalid 'rate_limit.models': must be a mapping of model_id -> settings.")

    validated: Dict[str, Dict[str, Any]] = {}
    parsers: Dict[str, Callable[[Any], Any]] = {
        "requests_per_minute": _parse_positive_float,
        "window_seconds": _parse_positive_float,
        "burst_capacity": _parse_positive_float,
        "tokens_per_minute": _parse_non_negative_int,
        "concurrency_limit": _parse_positive_int,
    }
    for model_name, model_cfg in raw_models.items():
        model_id = str(model_name).strip()
        if not model_id:
            raise ConfigError("Invalid 'rate_limit.models' key: model name must be non-empty.")
        if not isinstance(model_cfg, Mapping):
            raise ConfigError(f"Invalid 'rate_limit.models.{model_id}': must be a mapping.")
        cleaned: Dict[str, Any] = {}
        for key, raw_val in model_cfg.items():
            if key not in parsers:
                raise ConfigError(
                    f"Unsupported key '{key}' in 'rate_limit.models.{model_id}'."
                )
            try:
                cleaned[key] = parsers[key](raw_val)
            except Exception as exc:
                raise ConfigError(
                    f"Invalid value for 'rate_limit.models.{model_id}.{key}': {exc}"
                ) from exc
        validated[model_id] = cleaned
    return validated


def validate_model_list(raw_list: Any, field_name: str) -> List[str]:
    """Validate allowed_models / blocked_models string lists."""
    if raw_list is None:
        return []
    if not isinstance(raw_list, list):
        raise ConfigError(f"Invalid 'proxy.{field_name}': must be a list of model names.")
    cleaned: List[str] = []
    for item in raw_list:
        val = str(item).strip()
        if not val:
            raise ConfigError(f"Invalid 'proxy.{field_name}' entry: must be a non-empty string.")
        if val not in cleaned:
            cleaned.append(val)
    return cleaned


def validate_model_routes(raw_routes: Any) -> Dict[str, Dict[str, Any]]:
    """Validate proxy.model_routes while strictly forbidding raw secrets in YAML."""
    if raw_routes is None:
        return {}
    if not isinstance(raw_routes, Mapping):
        raise ConfigError("Invalid 'proxy.model_routes': must be a mapping of model_id -> route.")

    validated: Dict[str, Dict[str, Any]] = {}
    for model_name, route_cfg in raw_routes.items():
        model_id = str(model_name).strip()
        if not model_id:
            raise ConfigError("Invalid 'proxy.model_routes' key: model name must be non-empty.")
        if not isinstance(route_cfg, Mapping):
            raise ConfigError(f"Invalid 'proxy.model_routes.{model_id}': must be a mapping.")

        for forbidden_key in ("api_key", "nvidia_api_key", "token", "secret", "authorization"):
            if forbidden_key in route_cfg:
                raise ConfigError(
                    f"Forbidden raw credential key '{forbidden_key}' in 'proxy.model_routes.{model_id}'. "
                    "Use 'api_key_env' to reference an environment variable instead."
                )

        upstream_url = str(route_cfg.get("upstream_url") or "").strip()
        if not upstream_url or not (
            upstream_url.startswith("http://") or upstream_url.startswith("https://")
        ):
            raise ConfigError(
                f"Invalid 'proxy.model_routes.{model_id}.upstream_url': must be an http(s) URL."
            )

        cleaned: Dict[str, Any] = {"upstream_url": upstream_url.rstrip("/")}

        if "api_key_env" in route_cfg and route_cfg["api_key_env"] is not None:
            env_name = str(route_cfg["api_key_env"]).strip()
            if not env_name or env_name.startswith("nvapi-"):
                raise ConfigError(
                    f"Invalid 'proxy.model_routes.{model_id}.api_key_env': must be an env var name, not a raw key."
                )
            cleaned["api_key_env"] = env_name

        if "strip_auth" in route_cfg and route_cfg["strip_auth"] is not None:
            try:
                cleaned["strip_auth"] = _parse_bool(route_cfg["strip_auth"])
            except Exception as exc:
                raise ConfigError(
                    f"Invalid 'proxy.model_routes.{model_id}.strip_auth': {exc}"
                ) from exc

        for extra_key in route_cfg:
            if extra_key not in ("upstream_url", "api_key_env", "strip_auth"):
                raise ConfigError(
                    f"Unsupported key '{extra_key}' in 'proxy.model_routes.{model_id}'."
                )

        validated[model_id] = cleaned
    return validated


def validate_db_yaml(db_yaml: Any) -> Optional[str]:
    """Validate config.yaml 'db' section, rejecting embedded credentials and returning url_env."""
    if not isinstance(db_yaml, Mapping):
        return None
    for forbidden_key in ("url", "dsn", "password", "db_url"):
        if forbidden_key in db_yaml:
            raise ConfigError(
                f"Forbidden credential/DSN key '{forbidden_key}' in 'db' config.yaml section. "
                "Set DB_URL via environment/.env or use 'db.url_env' to reference an env var name."
            )
    if "url_env" in db_yaml and db_yaml["url_env"] is not None:
        env_name = str(db_yaml["url_env"]).strip()
        if not env_name or "://" in env_name:
            raise ConfigError(
                "Invalid 'db.url_env': must be an environment variable name, not a raw URL."
            )
        return env_name
    return None


def sanitize_db_target(db_url: Optional[str], db_path: str) -> str:
    """Return a credential-masked database target string for logs and /status."""
    raw = (db_url or "").strip()
    if not raw:
        return f"sqlite:///{db_path}"
    parsed = urlparse(raw)
    if parsed.scheme.lower() == "sqlite":
        return raw
    if parsed.password is not None:
        user = parsed.username or ""
        host = parsed.hostname or ""
        port_part = f":{parsed.port}" if parsed.port else ""
        netloc = f"{user}:***@{host}{port_part}"
        return urlunparse(
            (parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
        )
    return raw


def _load_yaml_file(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
            return loaded if isinstance(loaded, dict) else {}
    except Exception as exc:
        raise ConfigError(f"Failed to parse YAML config '{path}': {exc}") from exc


def resolve_settings(
    argv: Optional[Sequence[str]] = None,
    environ: Optional[Mapping[str, str]] = None,
    *,
    allow_yaml_fallback: bool = True,
    require_secrets: bool = True,
) -> AppSettings:
    """Resolve all application settings with strict precedence:
    argparse > environment var > dotenv > config.yaml (non-secret) > default or fail.
    """
    env_map: Mapping[str, str] = os.environ if environ is None else environ
    parser = build_arg_parser()
    parsed_ns = parser.parse_args([] if argv is None else list(argv))
    cli_values = vars(parsed_ns)

    # Step 1: Resolve env_file (argparse > env > default)
    env_file, env_file_source = _resolve_single(
        ENV_FILE_SPEC,
        cli_values=cli_values,
        environ=env_map,
        dotenv_map={},
        yaml_config={},
        require_secrets=require_secrets,
    )
    if env_file_source == "argparse" and not os.path.exists(env_file):
        raise ConfigError(f"Specified --env-file does not exist: {env_file}")

    dotenv_map: Dict[str, Optional[str]] = (
        dict(dotenv_values(env_file)) if os.path.exists(env_file) else {}
    )

    # Step 2: Resolve config_file (argparse > env > dotenv > default)
    config_file, _config_file_source = _resolve_single(
        CONFIG_FILE_SPEC,
        cli_values=cli_values,
        environ=env_map,
        dotenv_map=dotenv_map,
        yaml_config={},
        require_secrets=require_secrets,
    )

    yaml_config = _load_yaml_file(config_file) if allow_yaml_fallback else {}

    # Step 3: Resolve all remaining parameters
    resolved: Dict[str, Any] = {
        "env_file": env_file,
        "config_file": config_file,
    }
    sources: Dict[str, str] = {
        "env_file": env_file_source,
        "config_file": _config_file_source,
    }

    for spec in PARAM_SPECS:
        value, source = _resolve_single(
            spec,
            cli_values=cli_values,
            environ=env_map,
            dotenv_map=dotenv_map,
            yaml_config=yaml_config,
            require_secrets=require_secrets,
        )
        resolved[spec.name] = value
        sources[spec.name] = source

    raw_models = (
        yaml_config.get("rate_limit", {}).get("models")
        if isinstance(yaml_config.get("rate_limit"), Mapping)
        else None
    )
    model_rate_limits = validate_model_rate_limits(raw_models)
    sources["model_rate_limits"] = "config.yaml" if model_rate_limits else "default"

    proxy_yaml = yaml_config.get("proxy") if isinstance(yaml_config.get("proxy"), Mapping) else {}
    allowed_models = validate_model_list(proxy_yaml.get("allowed_models"), "allowed_models")
    blocked_models = validate_model_list(proxy_yaml.get("blocked_models"), "blocked_models")
    model_routes = validate_model_routes(proxy_yaml.get("model_routes"))
    sources["allowed_models"] = "config.yaml" if allowed_models else "default"
    sources["blocked_models"] = "config.yaml" if blocked_models else "default"
    sources["model_routes"] = "config.yaml" if model_routes else "default"

    db_yaml = yaml_config.get("db") if isinstance(yaml_config.get("db"), Mapping) else {}
    db_url_env = validate_db_yaml(db_yaml)
    if not resolved["db_url"] and db_url_env:
        env_candidate = env_map.get(db_url_env)
        if _has_value(env_candidate):
            resolved["db_url"] = str(env_candidate).strip()
            sources["db_url"] = f"env:{db_url_env}"
        else:
            dotenv_candidate = dotenv_map.get(db_url_env)
            if _has_value(dotenv_candidate):
                resolved["db_url"] = str(dotenv_candidate).strip()
                sources["db_url"] = f"dotenv:{db_url_env}"

    if resolved["db_pool_max"] < resolved["db_pool_min"]:
        raise ConfigError(
            f"Invalid database pool settings: db_pool_max ({resolved['db_pool_max']}) "
            f"must be >= db_pool_min ({resolved['db_pool_min']})."
        )

    return AppSettings(
        migrate_only=bool(cli_values.get("migrate_only", False)),
        migration_status=bool(cli_values.get("migration_status", False)),
        db_url_env=db_url_env,
        model_rate_limits=model_rate_limits,
        allowed_models=allowed_models,
        blocked_models=blocked_models,
        model_routes=model_routes,
        sources=sources,
        **resolved,
    )


def is_loopback_host(host: str) -> bool:
    """Return True if host refers to a loopback address (localhost, 127.x, ::1)."""
    normalized = host.strip().lower()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def validate_bind_guardrails(settings: AppSettings) -> Optional[str]:
    """Check non-loopback bind safety. Raises ConfigError in strict mode, else returns a warning."""
    if is_loopback_host(settings.host) or settings.allow_non_local_bind:
        return None
    msg = (
        f"Security warning: binding proxy to non-loopback host '{settings.host}' "
        "exposes an unauthenticated NVIDIA_API_KEY injector on the network. "
        "Bind to 127.0.0.1 or pass --allow-non-local-bind true (ALLOW_NON_LOCAL_BIND=true)."
    )
    if settings.strict_trial_guardrails:
        raise ConfigError(msg)
    return msg


def build_settings_source_metadata(resolved: AppSettings) -> Dict[str, Dict[str, Any]]:
    """Build per-'section.key' precedence & lock metadata for the Settings UI."""
    meta: Dict[str, Dict[str, Any]] = {}

    for spec in PARAM_SPECS:
        if not (spec.yaml_section and spec.yaml_key):
            continue
        path_key = f"{spec.yaml_section}.{spec.yaml_key}"
        raw_src = resolved.sources.get(spec.name, "default")
        locked = raw_src in ("argparse", "env", "dotenv")
        detail: Optional[str] = None
        if raw_src == "argparse":
            detail = f"Set via CLI flag {spec.cli_flag}"
        elif raw_src == "env":
            detail = f"Set via environment variable {spec.env_var}"
        elif raw_src == "dotenv":
            detail = f"Set via .env file ({spec.env_var})"

        meta[path_key] = {
            "source": raw_src,
            "locked": locked,
            "cli_flag": spec.cli_flag,
            "env_var": spec.env_var,
            "detail": detail,
        }

    # YAML-only structured/optional keys
    for path_key, src_name in (
        ("rate_limit.models", "model_rate_limits"),
        ("proxy.allowed_models", "allowed_models"),
        ("proxy.blocked_models", "blocked_models"),
        ("proxy.model_routes", "model_routes"),
    ):
        meta[path_key] = {
            "source": resolved.sources.get(src_name, "default"),
            "locked": False,
            "cli_flag": None,
            "env_var": None,
            "detail": None,
        }

    db_url_src = resolved.sources.get("db_url", "default")
    db_url_locked = db_url_src in ("argparse", "env", "dotenv")
    db_url_detail: Optional[str] = None
    if db_url_src == "argparse":
        db_url_detail = "Overridden via CLI flag --db-url"
    elif db_url_src == "env":
        db_url_detail = "Overridden via environment variable DB_URL"
    elif db_url_src == "dotenv":
        db_url_detail = "Overridden via .env file (DB_URL)"

    meta["db.url_env"] = {
        "source": db_url_src if db_url_locked else ("config.yaml" if resolved.db_url_env else "default"),
        "locked": db_url_locked,
        "cli_flag": "--db-url" if db_url_locked else None,
        "env_var": "DB_URL" if db_url_locked else None,
        "detail": db_url_detail,
    }

    return meta


def _is_empty_optional(val: Any) -> bool:
    return val is None or (isinstance(val, str) and not val.strip())


def normalize_admin_config_patch(new_config: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize optional/empty UI inputs before validation and config.yaml persistence."""
    normalized: Dict[str, Any] = {}

    if isinstance(new_config.get("admin"), Mapping):
        admin_out: Dict[str, Any] = {}
        for k, v in new_config["admin"].items():
            if k == "title" and _is_empty_optional(v):
                admin_out[k] = "NVIDIA NIM Rate Limiter Dashboard"
            elif isinstance(v, str):
                admin_out[k] = v.strip()
            else:
                admin_out[k] = v
        normalized["admin"] = admin_out

    if isinstance(new_config.get("rate_limit"), Mapping):
        rl_out: Dict[str, Any] = {}
        for k, v in new_config["rate_limit"].items():
            if k in ("tokens_per_minute", "daily_request_cap", "daily_token_cap"):
                rl_out[k] = 0 if _is_empty_optional(v) else _parse_non_negative_int(v)
            elif k == "max_wait_seconds":
                rl_out[k] = 0.0 if _is_empty_optional(v) else _parse_non_negative_float(v)
            elif k == "models":
                if _is_empty_optional(v):
                    rl_out[k] = {}
                elif isinstance(v, str):
                    rl_out[k] = json.loads(v)
                else:
                    rl_out[k] = v
            elif k in ("requests_per_minute", "window_seconds", "burst_capacity"):
                rl_out[k] = _parse_positive_float(v)
            else:
                rl_out[k] = v
        normalized["rate_limit"] = rl_out

    if isinstance(new_config.get("proxy"), Mapping):
        proxy_out: Dict[str, Any] = {}
        for k, v in new_config["proxy"].items():
            if k == "catalog_cache_ttl":
                proxy_out[k] = 300.0 if _is_empty_optional(v) else _parse_non_negative_float(v)
            elif k in ("allowed_models", "blocked_models"):
                if _is_empty_optional(v):
                    proxy_out[k] = []
                elif isinstance(v, str):
                    stripped = v.strip()
                    if stripped.startswith("["):
                        proxy_out[k] = json.loads(stripped)
                    else:
                        proxy_out[k] = [
                            item.strip()
                            for item in re.split(r"[\r\n,]+", stripped)
                            if item.strip()
                        ]
                else:
                    proxy_out[k] = v
            elif k == "model_routes":
                if _is_empty_optional(v):
                    proxy_out[k] = {}
                elif isinstance(v, str):
                    proxy_out[k] = json.loads(v)
                else:
                    proxy_out[k] = v
            elif k == "concurrency_limit":
                proxy_out[k] = _parse_positive_int(v)
            elif k == "upstream_url":
                proxy_out[k] = _parse_non_empty_str(v)
            elif k == "enforce_catalog":
                proxy_out[k] = _parse_bool(v)
            else:
                proxy_out[k] = v
        normalized["proxy"] = proxy_out

    if isinstance(new_config.get("db"), Mapping):
        db_out: Dict[str, Any] = {}
        for k, v in new_config["db"].items():
            if k == "url_env":
                db_out[k] = None if _is_empty_optional(v) else str(v).strip()
            elif k in ("pool_min", "pool_max"):
                default_pool = 1 if k == "pool_min" else 5
                db_out[k] = default_pool if _is_empty_optional(v) else _parse_positive_int(v)
            elif k == "connect_timeout":
                db_out[k] = 10.0 if _is_empty_optional(v) else _parse_positive_float(v)
            elif k == "path":
                db_out[k] = _parse_non_empty_str(v)
            else:
                db_out[k] = v
        normalized["db"] = db_out

    return normalized
