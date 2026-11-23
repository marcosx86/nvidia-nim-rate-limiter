import asyncio
import email.utils
import math
import random
import re
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Mapping, Optional, Tuple


class RateLimitExceeded(Exception):
    """Raised when a local queue wait timeout or quota cap rejects a request."""

    def __init__(
        self,
        error: str,
        retry_after: float,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.error = error
        self.retry_after = max(0.0, float(retry_after))
        self.detail = detail or {
            "error": error,
            "retry_after": round(self.retry_after, 3),
        }
        super().__init__(error)


class TokenBucket:
    """Async token-bucket limiter for request-per-window (RPM) enforcement."""

    def __init__(self, rpm: float, window_seconds: float, capacity: float):
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.fill_rate = float(rpm) / float(window_seconds) if window_seconds > 0 else 1.0
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    def update_params(self, rpm: float, window_seconds: float, capacity: float) -> None:
        self.capacity = float(capacity)
        self.fill_rate = float(rpm) / float(window_seconds) if window_seconds > 0 else 1.0
        self.tokens = min(self.tokens, self.capacity)

    async def acquire(self, max_wait_seconds: float = 0.0) -> float:
        """Acquire 1 request token, sleeping if necessary. Returns wait time in seconds."""
        wait_time = 0.0
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self.last_update)
                self.last_update = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return wait_time

                needed = 1.0 - self.tokens
                wait_seconds = needed / self.fill_rate if self.fill_rate > 0 else 1.0

                if max_wait_seconds > 0.0 and (wait_time + wait_seconds) > max_wait_seconds:
                    raise RateLimitExceeded(
                        "rate_limit_wait_exceeded",
                        retry_after=wait_seconds,
                        detail={
                            "error": "rate_limit_wait_exceeded",
                            "reason": "rpm_limit",
                            "retry_after": round(wait_seconds, 3),
                            "max_wait_seconds": max_wait_seconds,
                        },
                    )

            wait_time += wait_seconds
            await asyncio.sleep(wait_seconds)


class TPMBucket:
    """Async token-bucket limiter for Tokens-Per-Minute (TPM) budgeting and reconciliation."""

    def __init__(self, tpm: int, window_seconds: float = 60.0):
        self.tpm = int(tpm)
        self.window_seconds = float(window_seconds) if window_seconds > 0 else 60.0
        self.capacity = float(max(0, self.tpm))
        self.tokens = float(max(0, self.tpm))
        self.fill_rate = self.capacity / self.window_seconds if self.tpm > 0 else 0.0
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    @property
    def enabled(self) -> bool:
        return self.tpm > 0

    def update_params(self, tpm: int, window_seconds: float = 60.0) -> None:
        was_disabled = self.tpm <= 0
        self.tpm = int(tpm)
        self.window_seconds = float(window_seconds) if window_seconds > 0 else 60.0
        self.capacity = float(max(0, self.tpm))
        self.fill_rate = self.capacity / self.window_seconds if self.tpm > 0 else 0.0
        if self.tpm <= 0:
            self.tokens = 0.0
        elif was_disabled:
            self.tokens = self.capacity
        else:
            self.tokens = min(self.tokens, self.capacity)

    async def acquire(self, estimated_tokens: int, max_wait_seconds: float = 0.0) -> Tuple[float, int]:
        """Reserve estimated tokens before forwarding upstream. Returns (wait_time, reserved)."""
        if not self.enabled:
            return 0.0, 0

        reserve = min(max(1, int(estimated_tokens)), int(self.capacity))
        wait_time = 0.0

        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self.last_update)
                self.last_update = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)

                if self.tokens >= reserve:
                    self.tokens -= reserve
                    return wait_time, reserve

                needed = reserve - self.tokens
                wait_seconds = needed / self.fill_rate if self.fill_rate > 0 else 1.0

                if max_wait_seconds > 0.0 and (wait_time + wait_seconds) > max_wait_seconds:
                    raise RateLimitExceeded(
                        "rate_limit_wait_exceeded",
                        retry_after=wait_seconds,
                        detail={
                            "error": "rate_limit_wait_exceeded",
                            "reason": "tpm_limit",
                            "retry_after": round(wait_seconds, 3),
                            "max_wait_seconds": max_wait_seconds,
                        },
                    )

            wait_time += wait_seconds
            await asyncio.sleep(wait_seconds)

    async def reconcile(self, reserved: int, actual_tokens: Optional[int]) -> None:
        """Adjust available TPM tokens after upstream response completes or fails."""
        if not self.enabled or reserved <= 0:
            return

        async with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.last_update)
            self.last_update = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)

            if actual_tokens is None:
                # Refund reservation on failed/errored request without usage
                self.tokens = min(self.capacity, self.tokens + reserved)
            else:
                delta = reserved - int(actual_tokens)
                self.tokens = min(self.capacity, self.tokens + delta)


def estimate_request_tokens(
    body: bytes,
    body_json: Optional[Dict[str, Any]] = None,
) -> int:
    """Estimate pre-flight LLM token consumption from request payload size and max_tokens."""
    prompt_est = max(1, len(body) // 4) if body else 1
    completion_est = 0
    if isinstance(body_json, dict):
        raw_max = body_json.get("max_tokens")
        if raw_max is None:
            raw_max = body_json.get("max_completion_tokens")
        if raw_max is not None:
            try:
                completion_est = max(0, int(raw_max))
            except (TypeError, ValueError):
                completion_est = 256
        else:
            completion_est = 256
    return max(1, prompt_est + completion_est)


_DURATION_PART_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(ms|s|m|h)", re.IGNORECASE)


def _parse_reset_value(raw: str, now: float) -> Optional[float]:
    text = raw.strip()
    if not text:
        return None

    # 1. Numeric seconds or Unix epoch timestamp
    try:
        num = float(text)
        if num > 1_000_000_000:
            return max(0.0, num - now)
        return max(0.0, num)
    except ValueError:
        pass

    # 2. Duration strings like "500ms", "2s", "1m30s"
    parts = _DURATION_PART_RE.findall(text)
    if parts and "".join(f"{v}{u}" for v, u in parts).lower() == re.sub(r"\s+", "", text).lower():
        total = 0.0
        for val_str, unit in parts:
            val = float(val_str)
            u = unit.lower()
            if u == "ms":
                total += val / 1000.0
            elif u == "s":
                total += val
            elif u == "m":
                total += val * 60.0
            elif u == "h":
                total += val * 3600.0
        return max(0.0, total)

    # 3. HTTP-date (RFC 7231)
    try:
        dt = email.utils.parsedate_to_datetime(text)
        if dt is not None:
            return max(0.0, dt.timestamp() - now)
    except Exception:
        pass

    return None


def parse_retry_after(
    headers: Mapping[str, str],
    now: Optional[float] = None,
) -> Optional[float]:
    """Parse Retry-After or x-ratelimit-reset* headers into seconds to wait."""
    current_time = time.time() if now is None else now
    lower_headers = {str(k).lower(): str(v) for k, v in headers.items()}

    candidates = (
        "retry-after",
        "x-ratelimit-reset-requests",
        "x-ratelimit-reset-tokens",
        "x-ratelimit-reset",
    )
    parsed_delays: List[float] = []
    for key in candidates:
        if key in lower_headers:
            delay = _parse_reset_value(lower_headers[key], current_time)
            if delay is not None:
                if key == "retry-after":
                    return delay
                parsed_delays.append(delay)

    return max(parsed_delays) if parsed_delays else None


class RateLimitManager:
    """Coordinates global and per-model RPM, TPM, concurrency, and adaptive 429 cooldowns."""

    GLOBAL_KEY = "__global__"

    def __init__(self, config_data: Dict[str, Any]):
        rate_cfg = config_data.get("rate_limit", {})
        proxy_cfg = config_data.get("proxy", {})

        self.window_seconds = float(rate_cfg.get("window_seconds", 60.0))
        self.max_wait_seconds = float(rate_cfg.get("max_wait_seconds", 0.0))

        self.global_rpm = TokenBucket(
            rpm=float(rate_cfg.get("requests_per_minute", 6.0)),
            window_seconds=self.window_seconds,
            capacity=float(rate_cfg.get("burst_capacity", 6.0)),
        )
        self.global_tpm = TPMBucket(
            tpm=int(rate_cfg.get("tokens_per_minute", 0)),
            window_seconds=self.window_seconds,
        )
        self.global_semaphore = asyncio.Semaphore(int(proxy_cfg.get("concurrency_limit", 1)))

        self.model_rpm_buckets: Dict[str, TokenBucket] = {}
        self.model_tpm_buckets: Dict[str, TPMBucket] = {}
        self.model_semaphores: Dict[str, asyncio.Semaphore] = {}

        self._cooldown_until: Dict[str, float] = {}
        self._consecutive_429s: Dict[str, int] = {}
        self.upstream_throttled_count: int = 0

        self._rebuild_model_limiters(rate_cfg.get("models") or {})

    @classmethod
    def from_config(cls, config_data: Dict[str, Any]) -> "RateLimitManager":
        return cls(config_data)

    def _rebuild_model_limiters(self, models_cfg: Mapping[str, Any]) -> None:
        new_rpm: Dict[str, TokenBucket] = {}
        new_tpm: Dict[str, TPMBucket] = {}
        new_sem: Dict[str, asyncio.Semaphore] = {}

        for model_name, m_cfg in models_cfg.items():
            if not isinstance(m_cfg, Mapping):
                continue
            m_window = float(m_cfg.get("window_seconds", self.window_seconds))
            if "requests_per_minute" in m_cfg or "burst_capacity" in m_cfg:
                m_rpm = float(m_cfg.get("requests_per_minute", self.global_rpm.fill_rate * self.window_seconds))
                m_burst = float(m_cfg.get("burst_capacity", m_rpm))
                if model_name in self.model_rpm_buckets:
                    bucket = self.model_rpm_buckets[model_name]
                    bucket.update_params(m_rpm, m_window, m_burst)
                else:
                    bucket = TokenBucket(m_rpm, m_window, m_burst)
                new_rpm[model_name] = bucket

            if "tokens_per_minute" in m_cfg and int(m_cfg["tokens_per_minute"]) > 0:
                m_tpm_val = int(m_cfg["tokens_per_minute"])
                if model_name in self.model_tpm_buckets:
                    t_bucket = self.model_tpm_buckets[model_name]
                    t_bucket.update_params(m_tpm_val, m_window)
                else:
                    t_bucket = TPMBucket(m_tpm_val, m_window)
                new_tpm[model_name] = t_bucket

            if "concurrency_limit" in m_cfg and int(m_cfg["concurrency_limit"]) > 0:
                new_sem[model_name] = asyncio.Semaphore(int(m_cfg["concurrency_limit"]))

        self.model_rpm_buckets = new_rpm
        self.model_tpm_buckets = new_tpm
        self.model_semaphores = new_sem

    def configure_from_dict(self, config_data: Dict[str, Any]) -> None:
        rate_cfg = config_data.get("rate_limit", {})
        proxy_cfg = config_data.get("proxy", {})

        self.window_seconds = float(rate_cfg.get("window_seconds", 60.0))
        self.max_wait_seconds = float(rate_cfg.get("max_wait_seconds", 0.0))

        self.global_rpm.update_params(
            rpm=float(rate_cfg.get("requests_per_minute", 6.0)),
            window_seconds=self.window_seconds,
            capacity=float(rate_cfg.get("burst_capacity", 6.0)),
        )
        self.global_tpm.update_params(
            tpm=int(rate_cfg.get("tokens_per_minute", 0)),
            window_seconds=self.window_seconds,
        )
        self.global_semaphore = asyncio.Semaphore(int(proxy_cfg.get("concurrency_limit", 1)))
        self._rebuild_model_limiters(rate_cfg.get("models") or {})

    def get_cooldown_remaining(self, model: Optional[str] = None) -> float:
        now = time.monotonic()
        remaining = 0.0
        for key in (self.GLOBAL_KEY, model):
            if not key:
                continue
            until = self._cooldown_until.get(key, 0.0)
            if until > now:
                remaining = max(remaining, until - now)
            elif key in self._cooldown_until:
                self._cooldown_until.pop(key, None)
        return remaining

    def record_upstream_status(
        self,
        status_code: int,
        headers: Mapping[str, str],
        model: Optional[str] = None,
        jitter_ratio: Optional[float] = None,
    ) -> Optional[float]:
        """Track upstream 429/503 responses and set per-model/global cooldowns."""
        key = model or self.GLOBAL_KEY
        if 200 <= status_code < 400:
            self._consecutive_429s.pop(key, None)
            self._cooldown_until.pop(key, None)
            return None

        if status_code not in (429, 503):
            return None

        self.upstream_throttled_count += 1
        streak = self._consecutive_429s.get(key, 0) + 1
        self._consecutive_429s[key] = streak

        header_delay = parse_retry_after(headers)
        if header_delay is not None:
            delay = min(300.0, max(0.0, header_delay))
        else:
            base = min(60.0, 2.0 * (2 ** (streak - 1)))
            ratio = random.uniform(0.0, 0.25) if jitter_ratio is None else max(0.0, float(jitter_ratio))
            delay = min(60.0, base * (1.0 + ratio))

        if delay > 0:
            until = time.monotonic() + delay
            self._cooldown_until[key] = max(self._cooldown_until.get(key, 0.0), until)
        return delay

    async def acquire_request(
        self,
        model: Optional[str],
        estimated_tokens: int,
    ) -> Dict[str, Any]:
        """Wait for cooldowns and acquire per-model + global RPM/TPM buckets."""
        remaining_budget = self.max_wait_seconds

        # 1. Adaptive upstream cooldown
        while True:
            cooldown = self.get_cooldown_remaining(model)
            if cooldown <= 0:
                break
            if self.max_wait_seconds > 0.0 and cooldown > remaining_budget:
                raise RateLimitExceeded(
                    "rate_limit_wait_exceeded",
                    retry_after=cooldown,
                    detail={
                        "error": "rate_limit_wait_exceeded",
                        "reason": "upstream_cooldown",
                        "retry_after": round(cooldown, 3),
                        "max_wait_seconds": self.max_wait_seconds,
                    },
                )
            await asyncio.sleep(cooldown)
            if self.max_wait_seconds > 0.0:
                remaining_budget = max(0.0001, remaining_budget - cooldown)

        reservation: Dict[str, Any] = {
            "model": model,
            "model_tpm_reserved": 0,
            "global_tpm_reserved": 0,
        }

        try:
            # 2. Per-model RPM & TPM (if configured)
            if model and model in self.model_rpm_buckets:
                waited = await self.model_rpm_buckets[model].acquire(max_wait_seconds=remaining_budget)
                if self.max_wait_seconds > 0.0:
                    remaining_budget = max(0.0001, remaining_budget - waited)

            if model and model in self.model_tpm_buckets:
                waited, reserved = await self.model_tpm_buckets[model].acquire(
                    estimated_tokens,
                    max_wait_seconds=remaining_budget,
                )
                reservation["model_tpm_reserved"] = reserved
                if self.max_wait_seconds > 0.0:
                    remaining_budget = max(0.0001, remaining_budget - waited)

            # 3. Global RPM & TPM
            waited = await self.global_rpm.acquire(max_wait_seconds=remaining_budget)
            if self.max_wait_seconds > 0.0:
                remaining_budget = max(0.0001, remaining_budget - waited)

            if self.global_tpm.enabled:
                _, reserved = await self.global_tpm.acquire(
                    estimated_tokens,
                    max_wait_seconds=remaining_budget,
                )
                reservation["global_tpm_reserved"] = reserved

            return reservation
        except BaseException:
            await self.reconcile_tokens(reservation, None)
            raise

    async def reconcile_tokens(
        self,
        reservation: Optional[Dict[str, Any]],
        actual_tokens: Optional[int],
    ) -> None:
        if not reservation:
            return
        model = reservation.get("model")
        model_res = int(reservation.get("model_tpm_reserved") or 0)
        global_res = int(reservation.get("global_tpm_reserved") or 0)

        if model and model in self.model_tpm_buckets and model_res > 0:
            await self.model_tpm_buckets[model].reconcile(model_res, actual_tokens)
        if global_res > 0:
            await self.global_tpm.reconcile(global_res, actual_tokens)

    @asynccontextmanager
    async def semaphore_context(self, model: Optional[str]):
        model_sem = self.model_semaphores.get(model) if model else None
        if model_sem is not None:
            async with model_sem:
                async with self.global_semaphore:
                    yield
        else:
            async with self.global_semaphore:
                yield

    def active_cooldowns_snapshot(self) -> Dict[str, float]:
        now = time.monotonic()
        active: Dict[str, float] = {}
        for key, until in list(self._cooldown_until.items()):
            rem = until - now
            if rem > 0:
                label = "global" if key == self.GLOBAL_KEY else key
                active[label] = round(rem, 2)
            else:
                self._cooldown_until.pop(key, None)
        return active

    def status_snapshot(self) -> Dict[str, Any]:
        return {
            "tokens_available": round(self.global_rpm.tokens, 2),
            "capacity": self.global_rpm.capacity,
            "fill_rate_per_sec": round(self.global_rpm.fill_rate, 4),
            "tpm_enabled": self.global_tpm.enabled,
            "tpm_available": round(self.global_tpm.tokens, 2) if self.global_tpm.enabled else None,
            "tpm_capacity": self.global_tpm.capacity if self.global_tpm.enabled else 0,
            "max_wait_seconds": self.max_wait_seconds,
            "active_cooldowns": self.active_cooldowns_snapshot(),
            "upstream_throttled_count": self.upstream_throttled_count,
            "model_overrides": sorted(
                set(self.model_rpm_buckets) | set(self.model_tpm_buckets) | set(self.model_semaphores)
            ),
        }
