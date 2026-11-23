import argparse
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

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


def _parse_port(value: Any) -> int:
    parsed = int(value)
    if not (1 <= parsed <= 65535):
        raise ValueError("must be between 1 and 65535")
    return parsed


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
)

ALL_SPECS: Tuple[ParamSpec, ...] = (ENV_FILE_SPEC, CONFIG_FILE_SPEC, *PARAM_SPECS)


@dataclass
class AppSettings:
    env_file: str
    config_file: str
    nvidia_api_key: str
    upstream_url: str
    concurrency_limit: int
    requests_per_minute: float
    window_seconds: float
    burst_capacity: float
    db_path: str
    admin_title: str
    wwwroot_dir: str
    host: str
    port: int
    sources: Dict[str, str] = field(default_factory=dict)

    def to_config_dict(self) -> Dict[str, Any]:
        """Return non-secret runtime config dict matching config.yaml structure."""
        return {
            "proxy": {
                "upstream_url": self.upstream_url,
                "concurrency_limit": self.concurrency_limit,
            },
            "rate_limit": {
                "requests_per_minute": self.requests_per_minute,
                "window_seconds": self.window_seconds,
                "burst_capacity": self.burst_capacity,
            },
            "db": {
                "path": self.db_path,
            },
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

    return AppSettings(sources=sources, **resolved)
