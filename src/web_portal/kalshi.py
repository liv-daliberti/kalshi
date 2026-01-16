"""Kalshi client and metadata cache helpers for the web portal."""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

from .kalshi_sdk import (
    KalshiSdkError,
    extract_http_status,
    make_client,
    rest_register_rate_limit,
    rest_wait,
)

from .config import _env_int
from .portal_utils import portal_attr as _portal_attr
from .portal_utils import portal_func as _portal_func
from .portal_utils import portal_module as _portal_module

_KALSHI_CLIENT = None
_KALSHI_CLIENT_ERROR: str | None = None
_EVENT_METADATA_CACHE: dict[str, tuple[float, Any | None, str | None]] = {}
_EVENT_METADATA_CACHE_LOCK = threading.Lock()


def _state_get(name: str, default: Any):
    portal = _portal_module()
    if portal is not None and hasattr(portal, name):
        return getattr(portal, name)
    return globals().get(name, default)


def _state_set(name: str, value: Any) -> None:
    globals()[name] = value
    portal = _portal_module()
    if portal is not None:
        setattr(portal, name, value)


def _portal_logger() -> logging.Logger:
    return _portal_attr("logger", logging.getLogger(__name__))


def _portal_time():
    return _portal_attr("time", time)


_extract_http_status = extract_http_status


def _load_kalshi_client():
    """Initialize and cache a Kalshi SDK client for live snapshots."""
    client = _state_get("_KALSHI_CLIENT", _KALSHI_CLIENT)
    err = _state_get("_KALSHI_CLIENT_ERROR", _KALSHI_CLIENT_ERROR)
    if client or err:
        return client, err

    api_key_id = os.getenv("KALSHI_API_KEY_ID")
    pem_path = os.getenv("KALSHI_PRIVATE_KEY_PEM_PATH")
    if pem_path:
        pem_path = os.path.expandvars(os.path.expanduser(pem_path))
    host = os.getenv("KALSHI_HOST", "https://api.elections.kalshi.com/trade-api/v2")

    if not api_key_id or not pem_path:
        err = "Kalshi API credentials are not configured."
        _state_set("_KALSHI_CLIENT_ERROR", err)
        return None, err

    open_fn = _portal_attr("open", open)
    try:
        with open_fn(pem_path, "r", encoding="utf-8") as pem_file:
            private_key_pem = pem_file.read()
    except OSError:
        err = "Kalshi private key PEM could not be read."
        _state_set("_KALSHI_CLIENT_ERROR", err)
        return None, err

    make_client_fn = _portal_attr("make_client", make_client)
    sdk_error = _portal_attr("KalshiSdkError", KalshiSdkError)
    try:
        client = make_client_fn(host, api_key_id, private_key_pem)
    except sdk_error:
        err = "Kalshi SDK client could not be initialized."
        _state_set("_KALSHI_CLIENT_ERROR", err)
        return None, err

    _state_set("_KALSHI_CLIENT", client)
    _state_set("_KALSHI_CLIENT_ERROR", None)
    return client, None


def _get_market_data(
    ticker: str,
) -> tuple[dict[str, Any] | None, str | None, int | None]:
    """Fetch raw market data from the Kalshi API."""
    load_client = _portal_func("_load_kalshi_client", _load_kalshi_client)
    client, err = load_client()
    if err:
        return None, err, None

    rest_wait_fn = _portal_func("rest_wait", rest_wait)
    rest_register_fn = _portal_func("rest_register_rate_limit", rest_register_rate_limit)
    extract_status = _portal_func("_extract_http_status", _extract_http_status)
    try:
        rest_wait_fn()
        resp = client.get_market(ticker)
        market = resp.market if resp else None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        status = extract_status(exc)
        if status == 429:
            _portal_logger().warning("market fetch rate limited for %s", ticker)
            rest_register_fn(exc)
        else:
            _portal_logger().exception("market fetch failed")
        return None, str(exc) or "Market request failed.", status

    if market is None:
        return None, "Market not found.", None

    if isinstance(market, dict):
        market_data = market
    elif hasattr(market, "model_dump"):
        market_data = market.model_dump(mode="json")
    else:
        market_data = {}
    return market_data, None, None


def _event_metadata_cache_ttl() -> int:
    return _env_int("WEB_PORTAL_EVENT_METADATA_CACHE_SEC", 900, minimum=0)


def _load_event_metadata_cache(
    event_ticker: str,
) -> tuple[Any | None, str | None] | None:
    ttl_sec = _event_metadata_cache_ttl()
    if ttl_sec <= 0:
        return None
    now = _portal_time().monotonic()
    cache = _state_get("_EVENT_METADATA_CACHE", _EVENT_METADATA_CACHE)
    lock = _state_get("_EVENT_METADATA_CACHE_LOCK", _EVENT_METADATA_CACHE_LOCK)
    with lock:
        cached = cache.get(event_ticker)
        if not cached:
            return None
        cache_ts, metadata, err = cached
        if now - cache_ts > ttl_sec:
            cache.pop(event_ticker, None)
            return None
        return metadata, err


def _store_event_metadata_cache(
    event_ticker: str,
    metadata: Any | None,
    err: str | None,
) -> None:
    ttl_sec = _event_metadata_cache_ttl()
    if ttl_sec <= 0:
        return
    now = _portal_time().monotonic()
    cache = _state_get("_EVENT_METADATA_CACHE", _EVENT_METADATA_CACHE)
    lock = _state_get("_EVENT_METADATA_CACHE_LOCK", _EVENT_METADATA_CACHE_LOCK)
    with lock:
        cache[event_ticker] = (now, metadata, err)


def _event_metadata_from_payload(event: Any) -> tuple[Any | None, str | None]:
    if event is None:
        return None, "Event not found."
    if isinstance(event, dict):
        event_data = event
    elif hasattr(event, "model_dump"):
        event_data = event.model_dump(mode="json")
    else:
        event_data = {}
    for key in ("product_metadata", "event_metadata"):
        if event_data.get(key) is not None:
            return event_data.get(key), None
    return None, "Event metadata missing."


def _fetch_event_payload(event_ticker: str) -> tuple[Any | None, str | None]:
    load_client = _portal_func("_load_kalshi_client", _load_kalshi_client)
    client, err = load_client()
    if err:
        return None, err
    if not hasattr(client, "get_event"):
        return None, "Event metadata API unavailable."

    rest_wait_fn = _portal_func("rest_wait", rest_wait)
    rest_register_fn = _portal_func("rest_register_rate_limit", rest_register_rate_limit)
    extract_status = _portal_func("_extract_http_status", _extract_http_status)
    try:
        rest_wait_fn()
        resp = client.get_event(event_ticker)
        return (resp.event if resp else None), None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        status = extract_status(exc)
        if status == 429:
            rest_register_fn(exc)
        _portal_logger().exception("event fetch failed")
        return None, str(exc) or "Event request failed."


def _get_event_metadata(event_ticker: str | None) -> tuple[Any | None, str | None]:
    """Fetch event metadata from the Kalshi API when available."""
    if not event_ticker:
        return None, "Event ticker missing."
    load_cache = _portal_func("_load_event_metadata_cache", _load_event_metadata_cache)
    store_cache = _portal_func("_store_event_metadata_cache", _store_event_metadata_cache)
    cached = load_cache(event_ticker)
    if cached is not None:
        return cached
    event, err = _fetch_event_payload(event_ticker)
    if err:
        store_cache(event_ticker, None, err)
        return None, err
    metadata, err = _event_metadata_from_payload(event)
    store_cache(event_ticker, metadata, err)
    return metadata, err
