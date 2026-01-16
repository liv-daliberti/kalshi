"""Market discovery job for placeholder tickers."""

from __future__ import annotations

import logging
from typing import Any

from src.db.db import upsert_event, upsert_market
from src.kalshi.kalshi_sdk import rest_register_rate_limit, rest_wait

logger = logging.getLogger(__name__)


def _extract_http_status(exc: Exception) -> int | None:
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


def _coerce_payload(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump(mode="json")
        except TypeError:
            return value.model_dump()
    if hasattr(value, "dict"):
        try:
            return value.dict()
        except TypeError:
            return value.dict
    if hasattr(value, "__dict__"):
        return dict(value.__dict__)
    return None


def _extract_market_payload(response: Any) -> dict[str, Any] | None:
    if response is None:
        return None
    if isinstance(response, dict):
        market = response.get("market") or response.get("data") or response
    elif hasattr(response, "market"):
        market = response.market
    else:
        market = response
    return _coerce_payload(market)


def _extract_event_payload(response: Any) -> dict[str, Any] | None:
    if response is None:
        return None
    if isinstance(response, dict):
        event = response.get("event") or response.get("data") or response
    elif hasattr(response, "event"):
        event = response.event
    else:
        event = response
    return _coerce_payload(event)


def _fetch_market(client, ticker: str) -> dict[str, Any] | None:
    try:
        rest_wait()
        response = client.get_market(ticker)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if _extract_http_status(exc) == 429:
            rest_register_rate_limit(exc)
        raise
    return _extract_market_payload(response)


def _fetch_event(client, event_ticker: str) -> dict[str, Any] | None:
    try:
        rest_wait()
        response = client.get_event(event_ticker)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if _extract_http_status(exc) == 429:
            rest_register_rate_limit(exc)
        raise
    return _extract_event_payload(response)


def discover_market(conn, client, ticker: str) -> int:
    """Fetch a market/event via REST and upsert metadata rows."""
    if not ticker:
        raise ValueError("discover_market requires a market ticker")
    try:
        market = _fetch_market(client, ticker)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        status = _extract_http_status(exc)
        if status == 404:
            logger.warning("discover_market: market not found ticker=%s", ticker)
            return 0
        raise
    if not market:
        logger.warning("discover_market: market payload missing ticker=%s", ticker)
        return 0

    event_ticker = market.get("event_ticker")
    if event_ticker:
        event = None
        if hasattr(client, "get_event"):
            try:
                event = _fetch_event(client, event_ticker)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                status = _extract_http_status(exc)
                if status != 404:
                    raise
        if event:
            upsert_event(conn, event)
        else:
            upsert_event(conn, {"event_ticker": event_ticker})

    upsert_market(conn, market)
    return 1
