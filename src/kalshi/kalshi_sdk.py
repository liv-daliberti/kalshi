"""Thin wrapper for Kalshi SDK interactions."""

from __future__ import annotations

import importlib
import inspect
import logging
import os
import pkgutil
import random
import tempfile
import time
from dataclasses import dataclass
import functools
from collections.abc import Mapping
from typing import Any, Callable, Iterable, Iterator, Optional, Tuple

from src.kalshi.kalshi_rest_rate_limit import (
    _candlesticks_apply_cooldown,
    _candlesticks_wait,
    _extract_retry_after,
    configure_rest_rate_limit as _configure_rest_rate_limit,
    rest_apply_cooldown,
    rest_backoff_remaining as _rest_backoff_remaining,
    rest_register_rate_limit as _rest_register_rate_limit,
    rest_wait,
)

logger = logging.getLogger(__name__)

_TEMP_KEY_PATH: Optional[str] = None
_LAST_HOST_OVERRIDE: Optional[str] = None


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
    if _TEMP_KEY_PATH:
        return _TEMP_KEY_PATH
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        temp_file.write(private_key_pem)
        _TEMP_KEY_PATH = temp_file.name
    return _TEMP_KEY_PATH


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


def _call_with_retries(
    func: Callable[[], Any],
    retry_cfg: RetryConfig,
    context: str,
    rate_limit_hook: Optional[Callable[[float], None]] = None,
) -> tuple[bool, Any]:
    """Call a function with 429 retry handling.

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
            if status != 429:
                raise
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
            logger.warning(
                "%s rate limited; retrying in %.2fs%s",
                context,
                sleep_s,
                suffix,
            )
            time.sleep(sleep_s)
            attempt += 1


def make_client(host: str, api_key_id: str, private_key_pem: str):
    """Create a Kalshi API client.

    :param host: Kalshi API base URL.
    :type host: str
    :param api_key_id: API key identifier.
    :type api_key_id: str
    :param private_key_pem: Private key PEM contents.
    :type private_key_pem: str
    :return: Initialized Kalshi SDK client.
    :rtype: Any
    :raises KalshiSdkError: If the client cannot be initialized.
    """
    sdk = _import_sdk()
    _patch_sdk_models()
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


def _filter_kwargs(func: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    """Filter kwargs to the function signature.

    :param func: Target callable.
    :type func: collections.abc.Callable[..., Any]
    :param kwargs: Keyword arguments to filter.
    :type kwargs: dict[str, Any]
    :return: Filtered kwargs compatible with the signature.
    :rtype: dict[str, Any]
    """
    sig = inspect.signature(func)
    if any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values()):
        return kwargs
    return {k: v for k, v in kwargs.items() if k in sig.parameters}


def _extract_items(resp: Any) -> Tuple[list[Any], Optional[str]]:
    """Extract items and cursor from a response payload.

    :param resp: API response payload.
    :type resp: Any
    :return: Tuple of (items, cursor).
    :rtype: tuple[list[Any], str | None]
    """
    if isinstance(resp, dict):
        items = resp.get("events") or resp.get("items") or resp.get("data") or []
        cursor = resp.get("next_cursor") or resp.get("cursor") or resp.get("next")
        return list(items or []), cursor
    if isinstance(resp, (list, tuple)):
        if len(resp) == 2 and isinstance(resp[0], list):
            return resp[0], resp[1]
        return list(resp), None
    items = getattr(resp, "events", None) or getattr(resp, "items", None)
    cursor = getattr(resp, "next_cursor", None) or getattr(resp, "cursor", None)
    return list(items or []), cursor


def _to_plain_dict(obj: Any) -> Any:
    """Convert SDK objects to plain dictionaries when possible.

    :param obj: SDK model or dict-like object.
    :type obj: Any
    :return: Plain dict or original object.
    :rtype: Any
    """
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump(mode="json")
        except TypeError:
            return obj.model_dump()
    if hasattr(obj, "to_dict"):
        try:
            return obj.to_dict()
        except Exception:  # pylint: disable=broad-exception-caught
            pass
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    return obj


def _resolve_events_method(client) -> Optional[Callable[..., Any]]:
    """Resolve an events method from the client.

    :param client: Kalshi API client.
    :type client: Any
    :return: Callable method or None.
    :rtype: collections.abc.Callable[..., Any] | None
    """
    for name in ("get_events", "list_events", "events"):
        if hasattr(client, name):
            return getattr(client, name)
    return None


def _prepare_cursor_kwargs(
    method: Callable[..., Any],
    base_kwargs: dict[str, Any],
    cursor: Optional[str],
) -> dict[str, Any]:
    """Prepare cursor-aware kwargs for a paged request.

    :param method: Paged API method.
    :type method: collections.abc.Callable[..., Any]
    :param base_kwargs: Base keyword arguments.
    :type base_kwargs: dict[str, Any]
    :param cursor: Cursor token to include.
    :type cursor: str | None
    :return: Filtered kwargs including cursor fields.
    :rtype: dict[str, Any]
    """
    call_kwargs = dict(base_kwargs)
    if cursor is not None:
        sig = inspect.signature(method)
        if "cursor" in sig.parameters:
            call_kwargs["cursor"] = cursor
        elif "next_cursor" in sig.parameters:
            call_kwargs["next_cursor"] = cursor
    return _filter_kwargs(method, call_kwargs)


def _iter_events_stream(
    method: Callable[..., Any],
    kwargs: dict[str, Any],
    retry_cfg: RetryConfig,
    rate_limit_hook: Optional[Callable[[float], None]] = None,
) -> Iterator[dict]:
    """Yield events using a streaming iter_events method.

    :param method: iter_events-style method.
    :type method: collections.abc.Callable[..., Any]
    :param kwargs: API query parameters.
    :type kwargs: dict[str, Any]
    :param retry_cfg: Retry configuration.
    :type retry_cfg: RetryConfig
    :return: Event iterator.
    :rtype: collections.abc.Iterator[dict]
    """
    _ensure_method_host(method)
    call_kwargs = _filter_kwargs(method, kwargs)
    logger.debug(
        "iter_events: method=%s kwargs=%s",
        getattr(method, "__name__", "iter_events"),
        call_kwargs,
    )
    call = functools.partial(method, **call_kwargs)
    success, response = _call_with_retries(
        call,
        retry_cfg,
        "iter_events",
        rate_limit_hook=rate_limit_hook,
    )
    if not success or response is None:
        return
    for event in response:
        yield _to_plain_dict(event)


def _iter_events_paged(
    method: Callable[..., Any],
    kwargs: dict[str, Any],
    retry_cfg: RetryConfig,
    rate_limit_hook: Optional[Callable[[float], None]] = None,
) -> Iterator[dict]:
    """Yield events using a paginated get_events-style method.

    :param method: Paged events method.
    :type method: collections.abc.Callable[..., Any]
    :param kwargs: API query parameters.
    :type kwargs: dict[str, Any]
    :param retry_cfg: Retry configuration.
    :type retry_cfg: RetryConfig
    :return: Event iterator.
    :rtype: collections.abc.Iterator[dict]
    """
    cursor = None
    while True:
        _ensure_method_host(method)
        call_kwargs = _prepare_cursor_kwargs(method, kwargs, cursor)
        logger.debug(
            "iter_events: method=%s kwargs=%s cursor=%s",
            getattr(method, "__name__", "events"),
            call_kwargs,
            cursor,
        )
        call = functools.partial(method, **call_kwargs)
        success, response = _call_with_retries(
            call,
            retry_cfg,
            "iter_events",
            rate_limit_hook=rate_limit_hook,
        )
        if not success:
            return
        events, next_cursor = _extract_items(response)
        logger.debug("iter_events: fetched=%d next_cursor=%s", len(events), next_cursor)
        for event in events:
            yield _to_plain_dict(event)
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor


def iter_events(
    client,
    *,
    rate_limit_hook: Optional[Callable[[float], None]] = None,
    **kwargs,
) -> Iterator[dict]:
    """Yield events from the Kalshi API.

    :param client: Kalshi API client.
    :type client: Any
    :param kwargs: API query parameters.
    :type kwargs: Any
    :return: Iterable of event payloads.
    :rtype: collections.abc.Iterator[dict]
    :raises KalshiSdkError: If the client lacks an events method.
    """
    retry_cfg = _load_retry_config("KALSHI_EVENTS")
    if hasattr(client, "iter_events"):
        method = getattr(client, "iter_events")
        yield from _iter_events_stream(
            method,
            kwargs,
            retry_cfg,
            rate_limit_hook=rate_limit_hook,
        )
        return

    method = _resolve_events_method(client)
    if method is None:
        raise KalshiSdkError("Client does not expose an events method.")
    yield from _iter_events_paged(
        method,
        kwargs,
        retry_cfg,
        rate_limit_hook=rate_limit_hook,
    )


def _resolve_candlesticks_method(client) -> Optional[Callable[..., Any]]:
    """Resolve a candlesticks method from the client.

    :param client: Kalshi API client.
    :type client: Any
    :return: Callable method or None.
    :rtype: collections.abc.Callable[..., Any] | None
    """
    for name in (
        "get_market_candlesticks",
        "get_market_candlestick",
        "get_candlesticks",
        "get_candles",
    ):
        if hasattr(client, name):
            return getattr(client, name)
    return None


def _build_candlestick_kwargs(
    method: Callable[..., Any],
    series_ticker: str,
    market_ticker: str,
    extra_kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Build keyword args for candlestick calls.

    :param method: Candlesticks API method.
    :type method: collections.abc.Callable[..., Any]
    :param series_ticker: Series ticker symbol.
    :type series_ticker: str
    :param market_ticker: Market ticker symbol.
    :type market_ticker: str
    :param extra_kwargs: Additional query parameters.
    :type extra_kwargs: dict[str, Any]
    :return: Filtered kwargs for the API method.
    :rtype: dict[str, Any]
    """
    period_minutes = extra_kwargs.get("period_interval_minutes")
    if "period_interval" not in extra_kwargs and period_minutes is not None:
        extra_kwargs = dict(extra_kwargs)
        extra_kwargs["period_interval"] = period_minutes
    call_kwargs = {
        "series_ticker": series_ticker,
        "market_ticker": market_ticker,
        "series": series_ticker,
        "ticker": market_ticker,
        **extra_kwargs,
    }
    _ensure_method_host(method)
    return _filter_kwargs(method, call_kwargs)


def _normalize_candlesticks_response(resp: Any) -> dict:
    """Normalize various candlesticks response shapes into a dict.

    :param resp: Raw API response.
    :type resp: Any
    :return: Normalized candlesticks response.
    :rtype: dict
    """
    if isinstance(resp, list):
        return {"candlesticks": [_to_plain_dict(c) for c in resp]}
    if isinstance(resp, dict):
        if "candlesticks" in resp or "candles" in resp:
            normalized = dict(resp)
            candles = normalized.get("candlesticks")
            if candles is None:
                candles = normalized.pop("candles", None)
            normalized["candlesticks"] = [
                _to_plain_dict(c) for c in (candles or [])
            ]
            normalized.pop("candles", None)
            return normalized
        return resp
    candles = getattr(resp, "candlesticks", None)
    if candles is None:
        candles = getattr(resp, "candles", None)
    if candles is not None:
        return {"candlesticks": [_to_plain_dict(c) for c in candles]}
    return {"candlesticks": []}


def get_market_candlesticks(
    client,
    series_ticker: str,
    market_ticker: str,
    **kwargs,
):
    """Fetch candlesticks for a market.

    :param client: Kalshi API client.
    :type client: Any
    :param series_ticker: Series ticker symbol.
    :type series_ticker: str
    :param market_ticker: Market ticker symbol.
    :type market_ticker: str
    :param kwargs: API query parameters.
    :type kwargs: Any
    :return: API response payload with candlesticks.
    :rtype: dict
    :raises KalshiSdkError: If the client lacks a candlesticks method.
    """
    method = _resolve_candlesticks_method(client)
    if method is None:
        raise KalshiSdkError("Client does not expose a candlesticks method.")

    call_kwargs = _build_candlestick_kwargs(
        method,
        series_ticker,
        market_ticker,
        kwargs,
    )
    logger.debug(
        "candlesticks: method=%s kwargs=%s",
        getattr(method, "__name__", "candlesticks"),
        call_kwargs,
    )
    retry_cfg = _load_retry_config("KALSHI_CANDLE")

    def _call_candlesticks():
        _candlesticks_wait()
        return method(**call_kwargs)

    success, response = _call_with_retries(
        _call_candlesticks,
        retry_cfg,
        "candlesticks",
        rate_limit_hook=_candlesticks_apply_cooldown,
    )
    if not success:
        return {"candlesticks": []}
    return _normalize_candlesticks_response(response)
