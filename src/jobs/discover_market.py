"""Market discovery job for placeholder tickers."""

from __future__ import annotations

import logging
from typing import Any

from src.db.db import upsert_event, upsert_market
from src.kalshi.kalshi_sdk import (
    coerce_payload,
    extract_http_status,
    rest_register_rate_limit,
    rest_wait,
)

logger = logging.getLogger(__name__)


def _log_missing_market(status: int | None, ticker: str) -> None:
    if status == 404:
        logger.warning("discover_market: market not found ticker=%s", ticker)
    else:
        logger.warning("discover_market: market payload missing ticker=%s", ticker)


def _maybe_upsert_event(conn, client, event_ticker: str | None) -> None:
    if not event_ticker:
        return
    event = None
    if hasattr(client, "get_event"):
        try:
            event = _fetch_event(client, event_ticker)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            status = extract_http_status(exc)
            if status != 404:
                raise
    if event:
        upsert_event(conn, event)
    else:
        upsert_event(conn, {"event_ticker": event_ticker})


def _extract_market_payload(response: Any) -> dict[str, Any] | None:
    if response is None:
        return None
    if isinstance(response, dict):
        market = response.get("market") or response.get("data") or response
    elif hasattr(response, "market"):
        market = response.market
    else:
        market = response
    return coerce_payload(market)


def _extract_event_payload(response: Any) -> dict[str, Any] | None:
    if response is None:
        return None
    if isinstance(response, dict):
        event = response.get("event") or response.get("data") or response
    elif hasattr(response, "event"):
        event = response.event
    else:
        event = response
    return coerce_payload(event)


def _fetch_market(client, ticker: str) -> dict[str, Any] | None:
    try:
        rest_wait()
        response = client.get_market(ticker)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if extract_http_status(exc) == 429:
            rest_register_rate_limit(exc)
        raise
    return _extract_market_payload(response)


def _fetch_event(client, event_ticker: str) -> dict[str, Any] | None:
    try:
        rest_wait()
        response = client.get_event(event_ticker)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if extract_http_status(exc) == 429:
            rest_register_rate_limit(exc)
        raise
    return _extract_event_payload(response)


def discover_market(conn, client, ticker: str) -> int:
    """Fetch a market/event via REST and upsert metadata rows."""
    if not ticker:
        raise ValueError("discover_market requires a market ticker")
    status = None
    try:
        market = _fetch_market(client, ticker)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        status = extract_http_status(exc)
        if status != 404:
            raise
        market = None

    result = 0
    if not market:
        _log_missing_market(status, ticker)
    else:
        _maybe_upsert_event(conn, client, market.get("event_ticker"))
        upsert_market(conn, market)
        result = 1

    return result
