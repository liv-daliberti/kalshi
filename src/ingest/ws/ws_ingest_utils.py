"""Helpers for WebSocket ingestion normalization."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from ...db.db import implied_yes_mid_cents, normalize_prob_dollars, parse_ts_iso
from ...core.number_utils import (
    coerce_int as _coerce_int,
    dollars_from_cents as _dollars_from_cents,
    infer_price_dollars_from_cents_spread as _infer_price_dollars_from_cents,
    to_cents as _to_cents,
)


def _parse_ts(value: Any) -> datetime | None:
    """Parse timestamps from ints/strings into timezone-aware datetimes."""
    if value is None:
        return None
    parsed = None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit():
            value = int(stripped)
        else:
            try:
                parsed = parse_ts_iso(stripped)
            except Exception:  # pylint: disable=broad-exception-caught
                parsed = None
            return parsed
    if isinstance(value, datetime):
        parsed = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    elif isinstance(value, (int, float, Decimal)):
        timestamp = float(value)
        if timestamp > 1e12:
            timestamp = timestamp / 1000.0
        parsed = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return parsed


def _extract_channel(payload: dict) -> str | None:
    """Extract the channel/type label from a WS payload."""
    channel = payload.get("type") or payload.get("channel") or payload.get("event")
    if channel is None and payload.get("msg"):
        channel = payload["msg"].get("type")
    return channel


def _extract_payload(message: dict) -> dict:
    """Extract the inner payload for a WS message."""
    return message.get("msg") or message.get("data") or message.get("payload") or message


def _extract_market_id(payload: dict) -> str | None:
    """Extract a market id from WS payloads."""
    market_id = (
        payload.get("market_id")
        or payload.get("marketId")
        or payload.get("marketID")
    )
    if market_id:
        return str(market_id)
    market = payload.get("market") or {}
    if not isinstance(market, dict):
        market = {}
    market_id = (
        market.get("market_id")
        or market.get("marketId")
        or market.get("marketID")
        or market.get("id")
    )
    if market_id:
        return str(market_id)
    has_ticker = any(
        (
            payload.get("market_ticker"),
            payload.get("marketTicker"),
            payload.get("ticker"),
            market.get("ticker"),
            market.get("market_ticker"),
            market.get("marketTicker"),
        )
    )
    if has_ticker:
        market_id = payload.get("id")
        if market_id:
            return str(market_id)
    return None


def _resolve_market_ticker(
    payload: dict,
    market_id_map: dict[str, str] | None,
) -> str | None:
    """Resolve a market ticker from WS payloads."""
    market = payload.get("market") or {}
    ticker = (
        payload.get("market_ticker")
        or payload.get("marketTicker")
        or payload.get("ticker")
        or market.get("ticker")
        or market.get("market_ticker")
        or market.get("marketTicker")
    )
    if ticker:
        return str(ticker)
    if market_id_map:
        market_id = _extract_market_id(payload)
        if market_id:
            return market_id_map.get(market_id)
    return None


def _extract_tick_quotes(payload: dict) -> dict[str, int | None]:
    price = _to_cents(
        payload.get("price")
        or payload.get("last_price")
        or payload.get("last_price_cents")
    )
    yes_bid = _to_cents(
        payload.get("yes_bid")
        or payload.get("yes_bid_cents")
        or payload.get("yes_bid_price")
    )
    yes_ask = _to_cents(
        payload.get("yes_ask")
        or payload.get("yes_ask_cents")
        or payload.get("yes_ask_price")
    )
    no_bid = _to_cents(payload.get("no_bid") or payload.get("no_bid_cents"))
    return {
        "price": price,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
    }


def _extract_tick_dollars(
    payload: dict,
    quotes: dict[str, int | None],
) -> dict[str, Decimal | None]:
    price_dollars = normalize_prob_dollars(
        payload.get("price_dollars") or payload.get("last_price_dollars")
    )
    if price_dollars is None:
        price_dollars = _dollars_from_cents(quotes["price"])
    yes_bid_dollars = normalize_prob_dollars(payload.get("yes_bid_dollars"))
    if yes_bid_dollars is None:
        yes_bid_dollars = _dollars_from_cents(quotes["yes_bid"])
    yes_ask_dollars = normalize_prob_dollars(payload.get("yes_ask_dollars"))
    if yes_ask_dollars is None:
        yes_ask_dollars = _dollars_from_cents(quotes["yes_ask"])
    no_bid_dollars = normalize_prob_dollars(payload.get("no_bid_dollars"))
    if no_bid_dollars is None:
        no_bid_dollars = _dollars_from_cents(quotes["no_bid"])

    price_dollars = _infer_price_dollars_from_cents(
        price_dollars,
        yes_bid_dollars,
        yes_ask_dollars,
        quotes["yes_bid"],
        quotes["yes_ask"],
    )
    return {
        "price_dollars": price_dollars,
        "yes_bid_dollars": yes_bid_dollars,
        "yes_ask_dollars": yes_ask_dollars,
        "no_bid_dollars": no_bid_dollars,
    }


def _extract_tick_metrics(payload: dict) -> dict[str, int | None]:
    return {
        "volume": _coerce_int(payload.get("volume") or payload.get("vol")),
        "open_interest": _coerce_int(payload.get("open_interest") or payload.get("oi")),
        "dollar_volume": _coerce_int(payload.get("dollar_volume")),
        "dollar_open_interest": _coerce_int(payload.get("dollar_open_interest")),
        "sid": _coerce_int(payload.get("sid") or payload.get("sequence_id")),
    }


def _normalize_tick(
    payload: dict,
    market_id_map: dict[str, str] | None = None,
) -> dict | None:
    """Normalize a ticker payload for database insertion."""
    ticker = _resolve_market_ticker(payload, market_id_map)
    if not ticker:
        return None

    tick_ts = _parse_ts(
        payload.get("ts")
        or payload.get("timestamp")
        or payload.get("time")
        or payload.get("received_time")
    ) or datetime.now(timezone.utc)

    quotes = _extract_tick_quotes(payload)
    dollars = _extract_tick_dollars(payload, quotes)
    metrics = _extract_tick_metrics(payload)
    implied_mid = implied_yes_mid_cents(quotes["yes_bid"], quotes["yes_ask"])

    return {
        "ts": tick_ts,
        "ticker": ticker,
        "price": quotes["price"],
        "yes_bid": quotes["yes_bid"],
        "yes_ask": quotes["yes_ask"],
        "price_dollars": dollars["price_dollars"],
        "yes_bid_dollars": dollars["yes_bid_dollars"],
        "yes_ask_dollars": dollars["yes_ask_dollars"],
        "no_bid_dollars": dollars["no_bid_dollars"],
        "volume": metrics["volume"],
        "open_interest": metrics["open_interest"],
        "dollar_volume": metrics["dollar_volume"],
        "dollar_open_interest": metrics["dollar_open_interest"],
        "implied_yes_mid": implied_mid,
        "sid": metrics["sid"],
        "raw": payload,
    }


def _normalize_lifecycle(payload: dict) -> dict | None:
    """Normalize a lifecycle payload for database insertion."""
    market_ticker = _resolve_market_ticker(payload, None)
    event_type = payload.get("event_type") or payload.get("type") or payload.get("status")
    lifecycle_ts = _parse_ts(payload.get("ts") or payload.get("timestamp")) or datetime.now(
        timezone.utc
    )
    open_ts = _coerce_int(payload.get("open_ts") or payload.get("open_time"))
    close_ts = _coerce_int(payload.get("close_ts") or payload.get("close_time"))
    return {
        "ts": lifecycle_ts,
        "market_ticker": market_ticker,
        "event_type": event_type,
        "open_ts": open_ts,
        "close_ts": close_ts,
        "raw": payload,
    }


def _is_terminal_lifecycle(event_type: str | None) -> bool:
    """Return True if a lifecycle event indicates the market is no longer active."""
    if not event_type:
        return False
    normalized = event_type.lower()
    terminal_keys = (
        "settled",
        "determined",
        "deactivated",
        "closed",
        "finalized",
        "resolved",
    )
    return any(key in normalized for key in terminal_keys)
