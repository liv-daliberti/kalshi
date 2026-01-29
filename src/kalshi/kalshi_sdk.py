"""Thin wrapper for Kalshi SDK interactions."""

from __future__ import annotations

import importlib
import inspect
import logging
import os
import pkgutil
import random
import threading
import tempfile
import time
from dataclasses import dataclass
from collections.abc import Mapping
from typing import Any, Callable, Iterable, Optional, Tuple

from ..core.loop_utils import log_metric as _log_metric
from .kalshi_rest_rate_limit import (
    _candlesticks_apply_cooldown as _candlesticks_apply_cooldown_impl,
    _candlesticks_wait as _candlesticks_wait_impl,
    _extract_retry_after,
    configure_rest_rate_limit as _configure_rest_rate_limit,
    current_rest_key,
    rest_apply_cooldown,
    rest_backoff_remaining as _rest_backoff_remaining,
    rest_register_rate_limit as _rest_register_rate_limit,
    select_rest_key,
    set_current_rest_key,
    set_rest_key_rotation,
    rest_wait,
)
from .kalshi_sdk_utils import (
    _extract_items as _extract_items_impl,
    _filter_kwargs as _filter_kwargs_impl,
    _to_plain_dict as _to_plain_dict_impl,
)
from .kalshi_sdk_events import (
    EventSdkHooks,
    _prepare_cursor_kwargs as _prepare_cursor_kwargs_impl,
    _resolve_events_method as _resolve_events_method_impl,
    _iter_events_stream as _iter_events_stream_impl,
    _iter_events_paged as _iter_events_paged_impl,
    iter_events as _iter_events_impl,
)
from .kalshi_sdk_candles import (
    CandlesSdkHooks,
    _build_candlestick_kwargs as _build_candlestick_kwargs_impl,
    _normalize_candlesticks_response as _normalize_candlesticks_response_impl,
    _resolve_candlesticks_method as _resolve_candlesticks_method_impl,
    get_market_candlesticks as _get_market_candlesticks_impl,
)

logger = logging.getLogger(__name__)

_TEMP_KEY_PATH: dict[str, str] = {}
_LAST_HOST_OVERRIDE: Optional[str] = None


def _event_hooks() -> EventSdkHooks:
    return EventSdkHooks(
        ensure_method_host=_ensure_method_host,
        call_with_retries=_call_with_retries,
        load_retry_config=_load_retry_config,
        kalshi_error=KalshiSdkError,
    )


def _candles_hooks() -> CandlesSdkHooks:
    return CandlesSdkHooks(
        ensure_method_host=_ensure_method_host,
        call_with_retries=_call_with_retries,
        load_retry_config=_load_retry_config,
        candlesticks_wait=_candlesticks_wait,
        candlesticks_apply_cooldown=_candlesticks_apply_cooldown,
        kalshi_error=KalshiSdkError,
    )


def _extract_items(resp: Any) -> tuple[list[Any], Optional[str]]:
    return _extract_items_impl(resp)


def _filter_kwargs(func: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    return _filter_kwargs_impl(func, kwargs)


def _to_plain_dict(obj: Any) -> Any:
    return _to_plain_dict_impl(obj)


def _resolve_events_method(client):
    return _resolve_events_method_impl(client)


def _prepare_cursor_kwargs(
    method: Callable[..., Any],
    base_kwargs: dict[str, Any],
    cursor: Optional[str],
) -> dict[str, Any]:
    return _prepare_cursor_kwargs_impl(method, base_kwargs, cursor)


def _resolve_candlesticks_method(client):
    return _resolve_candlesticks_method_impl(client)


def _build_candlestick_kwargs(
    method: Callable[..., Any],
    series_ticker: str,
    market_ticker: str,
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    _ensure_method_host(method)
    return _build_candlestick_kwargs_impl(method, series_ticker, market_ticker, kwargs)


def _normalize_candlesticks_response(resp: Any) -> dict:
    return _normalize_candlesticks_response_impl(resp)


def _iter_events_stream(
    method: Callable[..., Any],
    kwargs: dict[str, Any],
    retry_cfg: RetryConfig,
    rate_limit_hook: Optional[Callable[[float], None]] = None,
) -> Iterable[dict]:
    return _iter_events_stream_impl(
        method,
        kwargs,
        retry_cfg,
        _event_hooks(),
        rate_limit_hook=rate_limit_hook,
    )


def _iter_events_paged(
    method: Callable[..., Any],
    kwargs: dict[str, Any],
    retry_cfg: RetryConfig,
    rate_limit_hook: Optional[Callable[[float], None]] = None,
) -> Iterable[dict]:
    return _iter_events_paged_impl(
        method,
        kwargs,
        retry_cfg,
        _event_hooks(),
        rate_limit_hook=rate_limit_hook,
    )


def iter_events(
    client,
    *,
    rate_limit_hook: Optional[Callable[[float], None]] = None,
    **kwargs,
) -> Iterable[dict]:
    """Iterate Kalshi events with shared retry/rate-limit hooks applied."""
    return _iter_events_impl(
        client,
        rate_limit_hook=rate_limit_hook,
        hooks=_event_hooks(),
        **kwargs,
    )


def get_market_candlesticks(
    client,
    series_ticker: str,
    market_ticker: str,
    **kwargs,
) -> dict:
    """Fetch candlestick data for a market with shared hooks applied."""
    return _get_market_candlesticks_impl(
        client,
        series_ticker,
        market_ticker,
        hooks=_candles_hooks(),
        **kwargs,
    )


def configure_rest_rate_limit(
    backend: str | None = None,
    db_url: str | None = None,
) -> None:
    """Configure the REST rate limiter backend and DB URL overrides."""
    _configure_rest_rate_limit(backend=backend, db_url=db_url)


def rest_register_rate_limit(
    exc: Exception | None = None,
    cooldown_sec: float | None = None,
) -> None:
    """Register a REST rate limit based on an exception or fallback."""
    _rest_register_rate_limit(exc=exc, cooldown_sec=cooldown_sec)


def rest_backoff_remaining() -> float:
    """Return remaining global REST backoff seconds."""
    return _rest_backoff_remaining()


def _candlesticks_wait() -> None:
    """Wait for the next allowed candlestick request."""
    _candlesticks_wait_impl()


def _candlesticks_apply_cooldown(cooldown_sec: float) -> None:
    """Apply a cooldown to candlestick requests."""
    _candlesticks_apply_cooldown_impl(cooldown_sec)


def extract_http_status(exc: Exception) -> int | None:
    """Extract an HTTP status code from an exception when possible."""
    status = getattr(exc, "status", None) or getattr(exc, "status_code", None)
    if status is not None:
        try:
            return int(status)
        except (TypeError, ValueError):
            return None
    http_resp = getattr(exc, "http_resp", None)
    if http_resp is not None:
        status = getattr(http_resp, "status", None) or getattr(http_resp, "status_code", None)
        if status is not None:
            try:
                return int(status)
            except (TypeError, ValueError):
                return None
    return None


def coerce_payload(value: Any) -> dict[str, Any] | None:
    """Convert SDK payloads into dicts when possible."""
    payload = None
    if value is None:
        payload = None
    elif isinstance(value, dict):
        payload = value
    elif hasattr(value, "model_dump"):
        try:
            payload = value.model_dump(mode="json")
        except TypeError:
            payload = value.model_dump()
    elif hasattr(value, "dict"):
        try:
            payload = value.dict()
        except TypeError:
            payload = value.dict
    elif hasattr(value, "__dict__"):
        payload = dict(value.__dict__)
    return payload


def _is_validation_error(exc: Exception) -> bool:
    name = exc.__class__.__name__
    if name != "ValidationError":
        return False
    module = exc.__class__.__module__
    return module.startswith("pydantic") or module.startswith("pydantic_core")


class KalshiSdkError(RuntimeError):
    """Raised when the Kalshi SDK is missing or unsupported."""


@dataclass(frozen=True)
class RetryConfig:
    """Retry configuration for API calls.

    :ivar max_retries: Maximum number of retries for rate limits.
    :ivar base_sleep: Base sleep duration in seconds.
    :ivar max_sleep: Maximum sleep duration in seconds.
    """

    max_retries: int
    base_sleep: float
    max_sleep: float


def _write_temp_key(private_key_pem: str) -> str:
    """Write the private key to a temp file and return the path.

    :param private_key_pem: Private key PEM contents.
    :type private_key_pem: str
    :return: Filesystem path to the temporary PEM file.
    :rtype: str
    """
    global _TEMP_KEY_PATH  # pylint: disable=global-statement
    if _TEMP_KEY_PATH is None:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            temp_file.write(private_key_pem)
            _TEMP_KEY_PATH = temp_file.name
        return _TEMP_KEY_PATH
    if isinstance(_TEMP_KEY_PATH, str):
        return _TEMP_KEY_PATH
    if not isinstance(_TEMP_KEY_PATH, dict):
        _TEMP_KEY_PATH = {}
    existing = _TEMP_KEY_PATH.get(private_key_pem)
    if existing:
        return existing
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        temp_file.write(private_key_pem)
        _TEMP_KEY_PATH[private_key_pem] = temp_file.name
    return _TEMP_KEY_PATH[private_key_pem]


def _read_private_key(path: str) -> str:
    path = os.path.expandvars(os.path.expanduser(path))
    with open(path, "r", encoding="utf-8") as pem_file:
        return pem_file.read()


def _load_secondary_credentials() -> list[tuple[str, str]]:
    import sys  # pylint: disable=import-outside-toplevel

    if "PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules:
        return []
    api_key_id = os.getenv("KALSHI_API_KEY_ID_2", "").strip()
    pem_path = os.getenv("KALSHI_PRIVATE_KEY_PEM_PATH_2", "").strip()
    if not api_key_id and not pem_path:
        return []
    if not api_key_id or not pem_path:
        logger.warning(
            "Secondary Kalshi credentials require both KALSHI_API_KEY_ID_2 and "
            "KALSHI_PRIVATE_KEY_PEM_PATH_2; ignoring."
        )
        return []
    try:
        private_key_pem = _read_private_key(pem_path)
    except OSError as exc:
        logger.warning("Secondary Kalshi private key could not be read: %s", exc)
        return []
    return [(api_key_id, private_key_pem)]


def _import_sdk():
    """Import the Kalshi SDK, raising a friendly error if unavailable.

    :return: Imported SDK module.
    :rtype: module
    :raises KalshiSdkError: When the SDK is missing.
    """
    try:
        # pylint: disable=import-error,import-outside-toplevel
        import kalshi_python_sync as sdk  # type: ignore
    except Exception as exc:  # pylint: disable=broad-exception-caught
        raise KalshiSdkError(
            "kalshi_python_sync is not installed; rebuild the image or install it locally."
        ) from exc
    return sdk


def _patch_sdk_models() -> None:
    """Patch SDK models to accept newer enum values.

    :return: None.
    :rtype: None
    """
    try:
        # pylint: disable=import-error,import-outside-toplevel
        from kalshi_python_sync.models.market import Market as market_cls  # type: ignore
    except Exception:  # pylint: disable=broad-exception-caught
        market_cls = None  # type: ignore[assignment]

    try:
        # pylint: disable=import-error,import-outside-toplevel
        from kalshi_python_sync.models.event import Event as event_cls  # type: ignore
    except Exception:  # pylint: disable=broad-exception-caught
        event_cls = None  # type: ignore[assignment]

    def _patch_model(cls, patch_price_ranges: bool) -> None:
        if cls is None or getattr(
            cls,
            "_kalshi_patch_applied",
            False,
        ):  # pylint: disable=protected-access
            return

        orig_from_dict = cls.from_dict

        def _from_dict(_cls, obj):  # type: ignore[override]
            if not isinstance(obj, Mapping):
                return orig_from_dict(obj)
            updated = obj if isinstance(obj, dict) else dict(obj)

            def _set_value(key: str, value: Any) -> None:
                nonlocal updated
                if updated is obj:
                    updated = dict(obj)
                updated[key] = value

            status = obj.get("status")
            if status == "finalized":
                _set_value("status", "settled")
            elif status == "inactive":
                _set_value("status", "initialized")

            if patch_price_ranges:
                if obj.get("price_ranges") is None:
                    _set_value("price_ranges", [])
                if obj.get("category") is None:
                    _set_value("category", "")
                if obj.get("risk_limit_cents") is None:
                    _set_value("risk_limit_cents", 0)

            return orig_from_dict(updated)

        cls.from_dict = classmethod(_from_dict)
        cls._kalshi_patch_applied = True  # pylint: disable=protected-access
        logger.debug("Patched Kalshi SDK model: %s", cls.__name__)

    _patch_model(market_cls, patch_price_ranges=True)
    _patch_model(event_cls, patch_price_ranges=False)


def _looks_like_client(obj: Any) -> bool:
    """Return True when the object looks like an SDK client.

    :param obj: Object to inspect.
    :type obj: Any
    :return: True if it exposes expected client methods.
    :rtype: bool
    """
    return any(hasattr(obj, name) for name in ("get_events", "list_events", "events"))


def _candidate_factories(sdk) -> Iterable[Tuple[str, Any]]:
    """Yield candidate SDK factories for client construction.

    :param sdk: Imported SDK module.
    :type sdk: module
    :return: Iterable of (name, factory) pairs.
    :rtype: collections.abc.Iterable[tuple[str, Any]]
    """
    preferred = (
        "KalshiClient",
        "Client",
        "KalshiApi",
        "KalshiAPI",
        "KalshiApiClient",
        "ApiClient",
    )
    for name in preferred:
        if hasattr(sdk, name):
            yield name, getattr(sdk, name)

    if getattr(sdk, "__path__", None):
        for info in pkgutil.walk_packages(sdk.__path__, sdk.__name__ + "."):
            if "client" not in info.name and "api" not in info.name:
                continue
            try:
                module = importlib.import_module(info.name)
            except Exception:  # pylint: disable=broad-exception-caught
                continue
            for name, obj in module.__dict__.items():
                if (inspect.isclass(obj) or inspect.isfunction(obj)) and _looks_like_client(obj):
                    yield f"{info.name}:{name}", obj


def _resolve_environment(sdk, host: str):
    """Resolve the SDK environment enum from a host URL.

    :param sdk: Imported SDK module.
    :type sdk: module
    :param host: Kalshi API base URL.
    :type host: str
    :return: Environment enum value or None.
    :rtype: Any | None
    """
    for name in ("Environment", "KalshiEnvironment"):
        env_cls = getattr(sdk, name, None)
        if env_cls is None:
            continue
        attr = None
        if "demo" in host or "sandbox" in host:
            attr = getattr(env_cls, "DEMO", None) or getattr(env_cls, "SANDBOX", None)
        if attr is None:
            attr = getattr(env_cls, "PROD", None) or getattr(env_cls, "PRODUCTION", None)
        if attr is not None:
            return attr
    return None


_HOST_PARAM_NAMES = {"host", "base_url", "api_host", "kalshi_host"}
_API_KEY_PARAM_NAMES = {"api_key_id", "key_id", "access_key", "access_key_id"}
_PRIVATE_KEY_PARAM_NAMES = {"private_key", "private_key_pem", "private_key_data", "pem"}
_PRIVATE_KEY_PATH_PARAM_NAMES = {"private_key_path", "pem_path", "private_key_pem_path"}
_ENV_PARAM_NAMES = {"environment", "env"}
_ENV_IMPORT_FAILED = object()


def _resolve_env_value(params: dict[str, inspect.Parameter], host: str) -> Any | None:
    if not any(name in _ENV_PARAM_NAMES for name in params):
        return None
    try:
        sdk = _import_sdk()
    except KalshiSdkError:
        return _ENV_IMPORT_FAILED
    return _resolve_environment(sdk=sdk, host=host)


def _resolve_factory_arg(
    name: str,
    host: str,
    api_key_id: str,
    private_key_pem: str,
    env_value: Any | None,
):
    if name in _HOST_PARAM_NAMES:
        return host
    if name in _API_KEY_PARAM_NAMES:
        return api_key_id
    if name in _PRIVATE_KEY_PARAM_NAMES:
        return private_key_pem
    if name in _PRIVATE_KEY_PATH_PARAM_NAMES:
        return _write_temp_key(private_key_pem)
    if name in _ENV_PARAM_NAMES:
        return env_value
    return None


def _build_client(factory: Callable[..., Any], host: str, api_key_id: str, private_key_pem: str):
    """Build a client by matching factory parameters.

    :param factory: Candidate client factory.
    :type factory: collections.abc.Callable[..., Any]
    :param host: Kalshi API base URL.
    :type host: str
    :param api_key_id: API key identifier.
    :type api_key_id: str
    :param private_key_pem: Private key PEM contents.
    :type private_key_pem: str
    :return: Instantiated client or None.
    :rtype: Any | None
    """
    sig = inspect.signature(factory)
    params = sig.parameters
    env_value = _resolve_env_value(params, host)
    if env_value is _ENV_IMPORT_FAILED:
        return None
    kwargs: dict[str, Any] = {}
    for name, param in params.items():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            continue
        value = _resolve_factory_arg(name, host, api_key_id, private_key_pem, env_value)
        if value is None:
            if param.default is not param.empty:
                continue
            return None
        kwargs[name] = value
    return factory(**kwargs)


def _apply_host_override(client: Any, host: str) -> None:
    """Best-effort override of the SDK client's base URL."""
    if not host:
        return
    updated = False
    # Common OpenAPI client layouts.
    api_client = getattr(client, "api_client", None)
    if api_client is not None:
        if hasattr(api_client, "host"):
            try:
                api_client.host = host
                updated = True
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        config = getattr(api_client, "configuration", None)
        if config is not None and hasattr(config, "host"):
            try:
                config.host = host
                updated = True
            except Exception:  # pylint: disable=broad-exception-caught
                pass

    for attr in ("host", "base_url", "api_host", "kalshi_host"):
        if hasattr(client, attr):
            try:
                setattr(client, attr, host)
                updated = True
            except Exception:  # pylint: disable=broad-exception-caught
                pass

    if updated:
        logger.info("Kalshi SDK host override set to %s", host)
    else:
        logger.warning("Kalshi SDK host override failed for %s", type(client).__name__)


def _host_override_from_env() -> str:
    """Return the host override from env (if any)."""
    return os.getenv("KALSHI_HOST", "").strip()


def _ensure_method_host(method: Callable[..., Any]) -> None:
    """Ensure the bound method's api_client uses the env host override."""
    host = _host_override_from_env()
    if not host:
        return
    target = getattr(method, "__self__", None)
    if target is None:
        return
    api_client = getattr(target, "api_client", None)
    if api_client is None:
        return
    updated = False
    if hasattr(api_client, "host"):
        try:
            api_client.host = host
            updated = True
        except Exception:  # pylint: disable=broad-exception-caught
            pass
    config = getattr(api_client, "configuration", None)
    if config is not None and hasattr(config, "host"):
        try:
            config.host = host
            updated = True
        except Exception:  # pylint: disable=broad-exception-caught
            pass
    global _LAST_HOST_OVERRIDE  # pylint: disable=global-statement
    if updated and _LAST_HOST_OVERRIDE != host:
        _LAST_HOST_OVERRIDE = host
        logger.info("Kalshi SDK method host override set to %s", host)


def _extract_status(exc: Exception) -> Optional[int]:
    """Extract an HTTP status code from a SDK exception when possible."""
    status = getattr(exc, "status", None) or getattr(exc, "status_code", None)
    if status is not None:
        return int(status)
    http_resp = getattr(exc, "http_resp", None)
    if http_resp is not None:
        status = getattr(http_resp, "status", None) or getattr(http_resp, "status_code", None)
        if status is not None:
            return int(status)
    return None


class _KalshiClientPool:
    """Rotate across multiple Kalshi clients to avoid per-key rate limits."""

    _kalshi_client_pool = True

    def __init__(self, clients: list[Any], labels: list[str]):
        if not clients:
            raise KalshiSdkError("Kalshi client pool requires at least one client.")
        self._clients = list(clients)
        self._labels = list(labels)
        self._key_index = {label: idx for idx, label in enumerate(self._labels)}
        self._lock = threading.Lock()
        self._cooldowns = [0.0 for _ in self._clients]
        self._next_index = 0

    def _default_cooldown(self) -> float:
        try:
            return float(os.getenv("KALSHI_KEY_COOLDOWN_SECONDS", "60"))
        except ValueError:
            return 60.0

    def _mark_cooldown(self, idx: int, exc: Exception) -> None:
        retry_after = _extract_retry_after(exc)
        cooldown = retry_after if retry_after is not None else self._default_cooldown()
        if cooldown <= 0:
            return
        until = time.monotonic() + cooldown
        with self._lock:
            if until > self._cooldowns[idx]:
                self._cooldowns[idx] = until
        label = self._labels[idx] if idx < len(self._labels) else f"key{idx+1}"
        logger.warning(
            "Kalshi key %s rate limited; cooling down for %.1fs",
            label,
            cooldown,
        )

    def _pick_index(self) -> int:
        now = time.monotonic()
        with self._lock:
            client_count = len(self._clients)
            for _ in range(client_count):
                idx = self._next_index % client_count
                self._next_index += 1
                if self._cooldowns[idx] <= now:
                    return idx
            idx = self._next_index % client_count
            self._next_index += 1
            return idx

    def _preferred_index(self) -> Optional[int]:
        key_id = current_rest_key()
        if not key_id:
            key_id = select_rest_key()
        if not key_id:
            return None
        idx = self._key_index.get(key_id)
        if idx is None:
            return None
        now = time.monotonic()
        if self._cooldowns[idx] > now:
            return None
        return idx

    def _call_method(self, name: str, *args, **kwargs):
        last_exc: Exception | None = None
        client_count = len(self._clients)
        preferred_idx = self._preferred_index()
        attempted: set[int] = set()
        if preferred_idx is not None:
            idx_sequence = [preferred_idx]
        else:
            idx_sequence = []
        while len(idx_sequence) < client_count:
            idx_sequence.append(self._pick_index())
        for idx in idx_sequence:
            if idx in attempted:
                continue
            attempted.add(idx)
            client = self._clients[idx]
            method = getattr(client, name)
            try:
                if idx < len(self._labels):
                    set_current_rest_key(self._labels[idx])
                return method(*args, **kwargs)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                last_exc = exc
                status = _extract_status(exc)
                if status == 429:
                    self._mark_cooldown(idx, exc)
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        raise KalshiSdkError("All Kalshi API keys are unavailable.")

    def client_count(self) -> int:
        """Return the number of clients in the pool."""
        return len(self._clients)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        sample = getattr(self._clients[0], name)
        if not callable(sample):
            return sample

        def _wrapped(*args, **kwargs):
            return self._call_method(name, *args, **kwargs)

        _wrapped.__name__ = getattr(sample, "__name__", name)
        _wrapped.__qualname__ = getattr(sample, "__qualname__", name)
        _wrapped.__doc__ = getattr(sample, "__doc__", None)
        try:
            _wrapped.__signature__ = inspect.signature(sample)
        except (TypeError, ValueError):
            pass
        return _wrapped




def _load_retry_config(prefix: str) -> RetryConfig:
    """Load retry configuration from environment.

    :param prefix: Environment variable prefix.
    :type prefix: str
    :return: Retry configuration.
    :rtype: RetryConfig
    """
    max_retries = int(os.getenv(f"{prefix}_RETRIES", "5"))
    base_sleep = float(os.getenv(f"{prefix}_RETRY_SECONDS", "1.0"))
    max_sleep = float(os.getenv(f"{prefix}_RETRY_MAX_SECONDS", "30.0"))
    return RetryConfig(
        max_retries=max_retries,
        base_sleep=base_sleep,
        max_sleep=max_sleep,
    )


def _is_retryable_status(status: Optional[int]) -> bool:
    if status is None:
        return False
    if status in {408, 425, 429}:
        return True
    return 500 <= status <= 599


def _is_transient_exception(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    name = exc.__class__.__name__.lower()
    if "timeout" in name or "temporarilyunavailable" in name:
        return True
    module = exc.__class__.__module__.lower()
    return module.startswith(("urllib3", "requests", "httpx", "socket"))


def _call_with_retries(
    func: Callable[[], Any],
    retry_cfg: RetryConfig,
    context: str,
    rate_limit_hook: Optional[Callable[[float], None]] = None,
) -> tuple[bool, Any]:
    """Call a function with 429 + transient retry handling.

    :param func: Zero-arg callable to invoke.
    :type func: collections.abc.Callable[[], Any]
    :param retry_cfg: Retry configuration.
    :type retry_cfg: RetryConfig
    :param context: Log context label.
    :type context: str
    :param rate_limit_hook: Optional callback invoked with sleep seconds on 429.
    :type rate_limit_hook: collections.abc.Callable[[float], None] | None
    :return: (success, result) tuple.
    :rtype: tuple[bool, Any]
    """
    attempt = 0
    while True:
        try:
            rest_wait()
            return True, func()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if _is_validation_error(exc):
                logger.warning(
                    "%s validation error; skipping batch: %s",
                    context,
                    exc,
                )
                return False, None
            status = _extract_status(exc)
            if status == 429:
                if attempt >= retry_cfg.max_retries:
                    logger.warning(
                        "%s rate limited; giving up after %d retries",
                        context,
                        retry_cfg.max_retries,
                    )
                    return False, None
                sleep_s = min(retry_cfg.max_sleep, retry_cfg.base_sleep * (2 ** attempt))
                sleep_s += random.uniform(0.0, min(retry_cfg.base_sleep, 1.0))
                retry_after = _extract_retry_after(exc)
                if retry_after is not None:
                    sleep_s = max(sleep_s, retry_after)
                    suffix = f" (retry-after={retry_after:.2f}s)"
                else:
                    suffix = ""
                if rate_limit_hook is not None:
                    rate_limit_hook(sleep_s)
                rest_apply_cooldown(sleep_s)
                _log_metric(
                    logger,
                    "kalshi.retry",
                    context=context,
                    kind="rate_limit",
                    attempt=attempt + 1,
                    sleep_s=round(sleep_s, 2),
                    status=429,
                )
                logger.warning(
                    "%s rate limited; retrying in %.2fs%s",
                    context,
                    sleep_s,
                    suffix,
                )
                time.sleep(sleep_s)
                attempt += 1
                continue
            if not (_is_retryable_status(status) or _is_transient_exception(exc)):
                raise
            if attempt >= retry_cfg.max_retries:
                logger.warning(
                    "%s transient error; giving up after %d retries (status=%s)",
                    context,
                    retry_cfg.max_retries,
                    status,
                )
                return False, None
            sleep_s = min(retry_cfg.max_sleep, retry_cfg.base_sleep * (2 ** attempt))
            sleep_s += random.uniform(0.0, min(retry_cfg.base_sleep, 1.0))
            _log_metric(
                logger,
                "kalshi.retry",
                context=context,
                kind="transient",
                attempt=attempt + 1,
                sleep_s=round(sleep_s, 2),
                status=status,
            )
            logger.warning(
                "%s transient error; retrying in %.2fs (status=%s)",
                context,
                sleep_s,
                status,
            )
            time.sleep(sleep_s)
            attempt += 1


def _make_single_client(
    sdk,
    host: str,
    api_key_id: str,
    private_key_pem: str,
):
    last_err: Optional[Exception] = None
    for name, factory in _candidate_factories(sdk):
        try:
            client = _build_client(factory, host, api_key_id, private_key_pem)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            last_err = exc
            continue
        if client is not None:
            _apply_host_override(client, host)
            logger.info("Kalshi SDK client initialized via %s", name)
            return client
    raise KalshiSdkError(
        "Could not initialize Kalshi SDK client; update kalshi_python_sync or adjust adapters."
    ) from last_err


def make_client(host: str, api_key_id: str, private_key_pem: str):
    """Create a Kalshi API client (optionally with key rotation).

    :param host: Kalshi API base URL.
    :type host: str
    :param api_key_id: API key identifier.
    :type api_key_id: str
    :param private_key_pem: Private key PEM contents.
    :type private_key_pem: str
    :return: Initialized Kalshi SDK client (or rotating pool).
    :rtype: Any
    :raises KalshiSdkError: If the client cannot be initialized.
    """
    sdk = _import_sdk()
    _patch_sdk_models()
    credentials = [(api_key_id, private_key_pem)]
    credentials.extend(_load_secondary_credentials())

    clients: list[Any] = []
    labels: list[str] = []
    last_err: Optional[Exception] = None
    for idx, (key_id, key_pem) in enumerate(credentials):
        try:
            client = _make_single_client(sdk, host, key_id, key_pem)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            last_err = exc
            logger.warning("Kalshi client init failed for key %s: %s", key_id, exc)
            continue
        clients.append(client)
        labels.append(key_id or f"key{idx+1}")

    if not clients:
        raise KalshiSdkError(
            "Could not initialize Kalshi SDK client; update kalshi_python_sync or adjust adapters."
        ) from last_err
    if len(clients) == 1:
        set_rest_key_rotation(False)
        return clients[0]
    set_rest_key_rotation(True)
    logger.info("Kalshi SDK client pool initialized with %d keys.", len(clients))
    return _KalshiClientPool(clients, labels)
