"""Snapshot helpers for the web portal."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg  # pylint: disable=import-error
from flask import request  # pylint: disable=import-error
from psycopg.rows import dict_row  # pylint: disable=import-error

from src.db.db import normalize_prob_dollars
from src.core.number_utils import (
    dollars_from_cents as _dollars_from_cents,
    infer_price_dollars_from_cents_spread as _infer_price_dollars_from_cents,
)

from .config import _env_bool
from .db_timing import timed_cursor
from .db_utils import implied_yes_mid_cents
from .formatters import _coerce_int, _to_cents, fmt_money, fmt_num, fmt_ts
from .kalshi import _get_market_data
from .kalshi_sdk import rest_apply_cooldown, rest_backoff_remaining


def _load_latest_tick(conn: psycopg.Connection, ticker: str) -> dict[str, Any] | None:
    with timed_cursor(conn, row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT ts, implied_yes_mid, price_dollars, yes_bid_dollars, yes_ask_dollars,
                   volume, open_interest
            FROM market_ticks
            WHERE ticker = %s
            ORDER BY ts DESC, id DESC
            LIMIT 1
            """,
            (ticker,),
        )
        return cur.fetchone()


def _snapshot_from_tick(tick: dict[str, Any]) -> dict[str, Any]:
    tick_ts = tick.get("ts")
    yes_mid = tick.get("implied_yes_mid")
    if yes_mid is None:
        bid = tick.get("yes_bid_dollars")
        ask = tick.get("yes_ask_dollars")
        if bid is not None and ask is not None:
            yes_mid = (bid + ask) / Decimal(2)
        elif bid is not None:
            yes_mid = bid
        elif ask is not None:
            yes_mid = ask
    return {
        "status": "",
        "yes_mid": fmt_money(yes_mid, digits=4),
        "last_price": fmt_money(tick.get("price_dollars"), digits=4),
        "yes_bid": fmt_money(tick.get("yes_bid_dollars"), digits=4),
        "yes_ask": fmt_money(tick.get("yes_ask_dollars"), digits=4),
        "volume": fmt_num(tick.get("volume")),
        "open_interest": fmt_num(tick.get("open_interest")),
        "last_update": fmt_ts(tick_ts),
        "active_last_seen": fmt_ts(tick_ts),
        "snapshot_source": "market_ticks",
    }


def _prefer_tick_snapshot(
    conn: psycopg.Connection,
    ticker: str,
    freshness_sec: int,
    allow_stale: bool = False,
) -> dict[str, Any] | None:
    tick = _load_latest_tick(conn, ticker)
    if not tick:
        return None
    tick_ts = tick.get("ts")
    if tick_ts and not allow_stale and freshness_sec > 0:
        ts_value = tick_ts if tick_ts.tzinfo else tick_ts.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - ts_value).total_seconds()
        if age > freshness_sec:
            return None
    return _snapshot_from_tick(tick)


def _market_is_closed(conn: psycopg.Connection, ticker: str) -> bool:
    """Return True when the market has closed."""
    with timed_cursor(conn) as cur:
        cur.execute(
            """
            SELECT close_time
            FROM markets
            WHERE ticker = %s
            """,
            (ticker,),
        )
        row = cur.fetchone()
    if not row:
        return False
    close_time = row[0]
    if close_time is None:
        return False
    now = datetime.now(timezone.utc)
    if close_time.tzinfo is None:
        close_time = close_time.replace(tzinfo=timezone.utc)
    return close_time <= now


def _snapshot_allows_closed() -> bool:
    """Return True when snapshot endpoints should include closed markets."""
    raw = request.args.get("include_closed")
    if raw is not None:
        return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return _env_bool("WEB_PORTAL_SNAPSHOT_ALLOW_CLOSED", False)


def _snapshot_backoff_remaining() -> float:
    """Return remaining backoff seconds after a rate limit."""
    return rest_backoff_remaining()


def _set_snapshot_backoff(cooldown_sec: int) -> None:
    """Set a rate-limit backoff window for snapshot polling."""
    rest_apply_cooldown(cooldown_sec)


def _snapshot_error_payload(err: str, status: int | None) -> dict[str, Any]:
    payload = {"error": err, "status_code": status}
    if status == 429:
        payload["rate_limited"] = True
    return payload


def _snapshot_market_data(
    ticker: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    market_data, err, status = _get_market_data(ticker)
    if err:
        return None, _snapshot_error_payload(err, status)
    return market_data, None


def _infer_yes_mid(
    yes_bid: Decimal | None,
    yes_ask: Decimal | None,
    last_price: Decimal | None,
) -> Decimal | None:
    if yes_bid is not None and yes_ask is not None:
        return (yes_bid + yes_ask) / Decimal(2)
    if yes_bid is not None:
        return yes_bid
    if yes_ask is not None:
        return yes_ask
    return last_price


def _snapshot_quote_values(market_data: dict[str, Any]) -> dict[str, Decimal | None]:
    yes_bid = normalize_prob_dollars(market_data.get("yes_bid_dollars"))
    if yes_bid is None:
        yes_bid = _dollars_from_cents(market_data.get("yes_bid"))
    yes_ask = normalize_prob_dollars(market_data.get("yes_ask_dollars"))
    if yes_ask is None:
        yes_ask = _dollars_from_cents(market_data.get("yes_ask"))
    last_price = normalize_prob_dollars(market_data.get("last_price_dollars"))
    if last_price is None:
        last_price = _dollars_from_cents(market_data.get("last_price"))
    yes_mid = _infer_yes_mid(yes_bid, yes_ask, last_price)
    return {
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "last_price": last_price,
        "yes_mid": yes_mid,
    }


def _snapshot_quote_cents(
    market_data: dict[str, Any],
    primary_key: str,
    cents_key: str,
    dollars_key: str,
) -> int | None:
    cents = _to_cents(market_data.get(primary_key) or market_data.get(cents_key))
    if cents is None:
        cents = _to_cents(market_data.get(dollars_key))
    return cents


def _snapshot_price_cents(market_data: dict[str, Any]) -> int | None:
    raw_price = (
        market_data.get("last_price")
        or market_data.get("last_price_cents")
        or market_data.get("price")
    )
    cents = _to_cents(raw_price)
    if cents is None:
        cents = _to_cents(market_data.get("last_price_dollars"))
    return cents


def _snapshot_tick_from_market(
    market_data: dict[str, Any],
    ticker: str,
    snapshot_ts: datetime,
    quotes: dict[str, Decimal | None],
) -> dict[str, Any]:
    yes_bid_cents = _snapshot_quote_cents(
        market_data,
        "yes_bid",
        "yes_bid_cents",
        "yes_bid_dollars",
    )
    yes_ask_cents = _snapshot_quote_cents(
        market_data,
        "yes_ask",
        "yes_ask_cents",
        "yes_ask_dollars",
    )
    price_cents = _snapshot_price_cents(market_data)
    price_dollars = quotes["last_price"]
    if price_dollars is None:
        price_dollars = _dollars_from_cents(price_cents)
    yes_bid_dollars = quotes["yes_bid"]
    if yes_bid_dollars is None:
        yes_bid_dollars = _dollars_from_cents(yes_bid_cents)
    yes_ask_dollars = quotes["yes_ask"]
    if yes_ask_dollars is None:
        yes_ask_dollars = _dollars_from_cents(yes_ask_cents)
    price_dollars = _infer_price_dollars_from_cents(
        price_dollars,
        yes_bid_dollars,
        yes_ask_dollars,
        yes_bid_cents,
        yes_ask_cents,
    )
    implied_mid = implied_yes_mid_cents(yes_bid_cents, yes_ask_cents)
    if implied_mid is None and quotes["yes_mid"] is not None:
        implied_mid = quotes["yes_mid"].quantize(Decimal("0.000001"))
    return {
        "ts": snapshot_ts,
        "ticker": market_data.get("ticker") or market_data.get("market_ticker") or ticker,
        "price": price_cents,
        "yes_bid": yes_bid_cents,
        "yes_ask": yes_ask_cents,
        "price_dollars": price_dollars,
        "yes_bid_dollars": yes_bid_dollars,
        "yes_ask_dollars": yes_ask_dollars,
        "no_bid_dollars": normalize_prob_dollars(market_data.get("no_bid_dollars"))
        or _dollars_from_cents(market_data.get("no_bid")),
        "volume": _coerce_int(market_data.get("volume")),
        "open_interest": _coerce_int(market_data.get("open_interest")),
        "dollar_volume": _coerce_int(market_data.get("dollar_volume")),
        "dollar_open_interest": _coerce_int(market_data.get("dollar_open_interest")),
        "implied_yes_mid": implied_mid,
        "sid": None,
        "raw": {
            "source": "live_snapshot",
            "fetched_at": snapshot_ts.isoformat(),
            "market": market_data,
        },
    }


def _snapshot_payload(
    market_data: dict[str, Any],
    snapshot_ts: datetime,
    quotes: dict[str, Decimal | None],
) -> dict[str, Any]:
    return {
        "status": market_data.get("status") or "",
        "yes_mid": fmt_money(quotes["yes_mid"], digits=4),
        "last_price": fmt_money(quotes["last_price"], digits=4),
        "yes_bid": fmt_money(quotes["yes_bid"], digits=4),
        "yes_ask": fmt_money(quotes["yes_ask"], digits=4),
        "volume": fmt_num(market_data.get("volume")),
        "open_interest": fmt_num(market_data.get("open_interest")),
        "last_update": fmt_ts(snapshot_ts),
        "active_last_seen": fmt_ts(snapshot_ts),
    }


def fetch_live_snapshot(ticker: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Fetch a live snapshot for a market ticker."""
    market_data, error_payload = _snapshot_market_data(ticker)
    if error_payload is not None:
        return error_payload, None
    assert market_data is not None
    snapshot_ts = datetime.now(timezone.utc)
    quotes = _snapshot_quote_values(market_data)
    snapshot_tick = _snapshot_tick_from_market(
        market_data,
        ticker,
        snapshot_ts,
        quotes,
    )
    return _snapshot_payload(market_data, snapshot_ts, quotes), snapshot_tick
