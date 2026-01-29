"""Candlestick helpers for Kalshi SDK."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .kalshi_sdk_utils import _filter_kwargs, _to_plain_dict

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CandlesSdkHooks:
    """Dependencies supplied by kalshi_sdk to avoid cyclic imports."""

    ensure_method_host: Callable[[Callable[..., Any]], None]
    call_with_retries: Callable[..., tuple[bool, Any]]
    load_retry_config: Callable[[str], Any]
    candlesticks_wait: Callable[[], None]
    candlesticks_apply_cooldown: Callable[[float], None]
    kalshi_error: type[Exception]


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
        "candlesticks",
        "candles",
    ):
        if hasattr(client, name):
            return getattr(client, name)
    return None


def _build_candlestick_kwargs(
    method: Callable[..., Any],
    series_ticker: str,
    market_ticker: str,
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Build keyword args for candlestick calls.

    :param method: Candlestick API method.
    :type method: collections.abc.Callable[..., Any]
    :param series_ticker: Series ticker symbol.
    :type series_ticker: str
    :param market_ticker: Market ticker symbol.
    :type market_ticker: str
    :param kwargs: API keyword arguments.
    :type kwargs: dict[str, Any]
    :return: Filtered keyword arguments.
    :rtype: dict[str, Any]
    """
    call_kwargs = dict(kwargs)
    if "period_interval" not in call_kwargs and "period_interval_minutes" in call_kwargs:
        try:
            import inspect  # pylint: disable=import-outside-toplevel

            params = inspect.signature(method).parameters
        except (ValueError, TypeError):
            params = {}
        if "period_interval" in params and "period_interval_minutes" not in params:
            call_kwargs["period_interval"] = call_kwargs["period_interval_minutes"]
    if "period_interval" not in call_kwargs and "period_interval_minutes" in call_kwargs:
        call_kwargs["period_interval"] = call_kwargs["period_interval_minutes"]
    if series_ticker is not None:
        call_kwargs.setdefault("series_ticker", series_ticker)
        call_kwargs.setdefault("series", series_ticker)
    if market_ticker is not None:
        call_kwargs.setdefault("market_ticker", market_ticker)
        call_kwargs.setdefault("market", market_ticker)
        call_kwargs.setdefault("ticker", market_ticker)
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
    *,
    hooks: CandlesSdkHooks,
    **kwargs,
):
    """Fetch candlesticks for a market.

    :param client: Kalshi API client.
    :type client: Any
    :param series_ticker: Series ticker symbol.
    :type series_ticker: str
    :param market_ticker: Market ticker symbol.
    :type market_ticker: str
    :param hooks: SDK dependency hooks.
    :type hooks: CandlesSdkHooks
    :param kwargs: API query parameters.
    :type kwargs: Any
    :return: API response payload with candlesticks.
    :rtype: dict
    :raises KalshiSdkError: If the client lacks a candlesticks method.
    """
    method = _resolve_candlesticks_method(client)
    if method is None:
        raise hooks.kalshi_error("Client does not expose a candlesticks method.")

    hooks.ensure_method_host(method)
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
    retry_cfg = hooks.load_retry_config("KALSHI_CANDLE")

    def _call_candlesticks():
        hooks.candlesticks_wait()
        return method(**call_kwargs)

    success, response = hooks.call_with_retries(
        _call_candlesticks,
        retry_cfg,
        "candlesticks",
        rate_limit_hook=hooks.candlesticks_apply_cooldown,
    )
    if not success:
        return {"candlesticks": []}
    return _normalize_candlesticks_response(response)
