"""Database helpers and upsert utilities for the ingestor."""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import psycopg  # pylint: disable=import-error
from dateutil.parser import isoparse

from ..core.env_utils import _env_bool, _env_int
from ..core.json_utils import (
    maybe_parse_json as _maybe_parse_json_core,
    normalize_metadata_value as _normalize_metadata_value,
)
from ..core.loop_utils import log_metric as _log_metric
from . import ingest_writes as _ingest_writes
from . import json_utils as _json_utils
from . import portal_rollup as _portal_rollup
from .portal_rollup import _portal_rollup_app_refresh_enabled
from . import state_utils as _state_utils

try:
    from ..queue.work_queue import enqueue_job
except ImportError:
    enqueue_job = None
    _ENQUEUE_JOB_IMPORT_ERROR = sys.exc_info()
else:
    _ENQUEUE_JOB_IMPORT_ERROR = None
SCHEMA_VERSION = 3

logger = logging.getLogger(__name__)
_json_default = _json_utils.json_default
to_json_value = _json_utils.to_json_value
_PORTAL_ROLLUP_REFRESH_WARNED = False
get_state = _state_utils.get_state
set_state = _state_utils.set_state
PredictionRunSpec = _ingest_writes.PredictionRunSpec
insert_lifecycle_events = _ingest_writes.insert_lifecycle_events
insert_prediction_run = _ingest_writes.insert_prediction_run
update_prediction_run = _ingest_writes.update_prediction_run
insert_market_predictions = _ingest_writes.insert_market_predictions


def init_schema(conn: psycopg.Connection, schema_path: str) -> None:
    """Initialize the database schema from a SQL file.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param schema_path: Filesystem path to the schema SQL.
    :type schema_path: str
    """
    with open(schema_path, "r", encoding="utf-8") as schema_file:
        sql = schema_file.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def maybe_init_schema(conn: psycopg.Connection, schema_path: str) -> None:
    """Initialize schema when DB_INIT_SCHEMA is enabled (default true)."""
    if not _env_bool("DB_INIT_SCHEMA", True):
        return
    init_schema(conn, schema_path)


def _portal_rollup_refresh_events(
    conn: psycopg.Connection,
    event_tickers: list[str] | set[str] | tuple[str, ...],
) -> None:
    global _PORTAL_ROLLUP_REFRESH_WARNED  # pylint: disable=global-statement
    _PORTAL_ROLLUP_REFRESH_WARNED = _portal_rollup.portal_rollup_refresh_events(
        conn,
        event_tickers,
        logger_override=logger,
        warned=_PORTAL_ROLLUP_REFRESH_WARNED,
    )


def _event_tickers_for_market_tickers(
    conn: psycopg.Connection,
    tickers: list[str] | set[str] | tuple[str, ...],
) -> list[str]:
    unique = sorted({ticker for ticker in tickers if ticker})
    if not unique:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT event_ticker
            FROM markets
            WHERE ticker = ANY(%s)
              AND event_ticker IS NOT NULL
            """,
            (unique,),
        )
        rows = cur.fetchall()
    return [row[0] for row in rows if row and row[0]]


def _schema_compat_range() -> tuple[int, int]:
    min_version = _env_int("SCHEMA_COMPAT_MIN", SCHEMA_VERSION, minimum=1)
    max_version = _env_int("SCHEMA_COMPAT_MAX", SCHEMA_VERSION, minimum=min_version)
    return min_version, max_version


def _fetch_schema_version(conn: psycopg.Connection) -> Optional[int]:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
            )
            row = cur.fetchone()
    except psycopg.errors.UndefinedTable:
        return None
    return int(row[0]) if row and row[0] is not None else None


def ensure_schema_compatible(conn: psycopg.Connection) -> int:
    """Validate schema version compatibility before running services."""
    if not _env_bool("SCHEMA_VALIDATE", True):
        return -1
    min_version, max_version = _schema_compat_range()
    version = _fetch_schema_version(conn)
    if version is None:
        raise RuntimeError(
            "schema_version table missing; run the migrator before starting services."
        )
    if version < min_version or version > max_version:
        raise RuntimeError(
            "Schema version mismatch: "
            f"db={version}, expected [{min_version}, {max_version}]."
        )
    return version


def get_event_updated_at(
    conn: psycopg.Connection,
    event_ticker: str,
) -> Optional[datetime]:
    """Fetch the updated_at timestamp for an event."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT updated_at FROM events WHERE event_ticker=%s",
            (event_ticker,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def get_market_updated_at(
    conn: psycopg.Connection,
    ticker: str,
) -> Optional[datetime]:
    """Fetch the updated_at timestamp for a market."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT updated_at FROM markets WHERE ticker=%s",
            (ticker,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def parse_ts_iso(value: Any) -> Optional[datetime]:
    """Parse an ISO timestamp or passthrough a datetime, ensuring timezone.

    :param value: ISO string or datetime-like input.
    :type value: Any
    :return: Parsed datetime in UTC when possible.
    :rtype: datetime.datetime | None
    """
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    # ISO strings like "...Z" appear in REST responses :contentReference[oaicite:15]{index=15}
    return isoparse(str(value))


def dec(value: Any) -> Optional[Decimal]:
    """Parse a decimal-compatible value, returning None when invalid.

    :param value: Value to parse.
    :type value: Any
    :return: Decimal value or None.
    :rtype: decimal.Decimal | None
    """
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def normalize_prob_dollars(value: Any) -> Optional[Decimal]:
    """Normalize a price/probability value into 0-1 dollars.

    Kalshi APIs sometimes return cent-style values in *_dollars fields.
    Convert values in the 1-100 range to dollars.
    """
    dec_value = dec(value)
    if dec_value is None:
        return None
    if Decimal("1") < dec_value <= Decimal("100"):
        return dec_value / Decimal(100)
    return dec_value


def _maybe_parse_json(value: Any) -> Any | None:
    """Parse JSON strings into Python values when possible."""
    return _maybe_parse_json_core(value)


def _coerce_metadata_value(value: Any) -> Any | None:
    """Normalize metadata values for JSON storage."""
    return _normalize_metadata_value(value)


def _derive_custom_strike(market: dict) -> dict[str, Any] | None:
    """Build custom strike metadata from strike fields."""
    payload = {
        "strike_type": market.get("strike_type"),
        "floor_strike": market.get("floor_strike"),
        "cap_strike": market.get("cap_strike"),
        "functional_strike": market.get("functional_strike"),
    }
    if all(value is None for value in payload.values()):
        return None
    return {key: value for key, value in payload.items() if value is not None}


def _extract_event_metadata(event: dict) -> Any | None:
    """Extract event metadata from common payload keys."""
    metadata = event.get("product_metadata")
    if metadata is None:
        metadata = event.get("event_metadata")
    return _coerce_metadata_value(metadata)


def _resolve_market_metadata(market: dict) -> tuple[Any | None, Any | None, Any | None]:
    """Resolve market metadata fields from a payload."""
    price_ranges = _coerce_metadata_value(market.get("price_ranges"))
    if price_ranges is None:
        price_ranges = _maybe_parse_json(market.get("price_level_structure"))

    custom_strike = _coerce_metadata_value(market.get("custom_strike"))
    if custom_strike is None:
        custom_strike = _derive_custom_strike(market)

    mve_selected_legs = _coerce_metadata_value(market.get("mve_selected_legs"))
    return price_ranges, custom_strike, mve_selected_legs


def implied_yes_mid_cents(
    yes_bid: Optional[int],
    yes_ask: Optional[int],
) -> Optional[Decimal]:
    """Compute implied yes mid in dollars from bid/ask in cents.

    :param yes_bid: Bid price in cents.
    :type yes_bid: int | None
    :param yes_ask: Ask price in cents.
    :type yes_ask: int | None
    :return: Mid price in dollars or None.
    :rtype: decimal.Decimal | None
    """
    if yes_bid is None and yes_ask is None:
        return None
    if yes_bid is not None and yes_ask is not None:
        mid = (Decimal(yes_bid) + Decimal(yes_ask)) / Decimal(2)
    else:
        mid = Decimal(yes_bid if yes_bid is not None else yes_ask)
    return (mid / Decimal(100)).quantize(Decimal("0.000001"))


def upsert_event(conn: psycopg.Connection, event: dict) -> None:
    """Insert or update an event row.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param event: Event payload from the API.
    :type event: dict
    """
    sql = """
    INSERT INTO events(
      event_ticker, series_ticker, title, sub_title, category,
      mutually_exclusive, collateral_return_type, available_on_brokers,
      product_metadata, strike_date, strike_period, updated_at
    )
    VALUES(
      %(event_ticker)s, %(series_ticker)s, %(title)s, %(sub_title)s, %(category)s,
      %(mutually_exclusive)s, %(collateral_return_type)s, %(available_on_brokers)s,
      %(product_metadata)s, %(strike_date)s, %(strike_period)s, NOW()
    )
    ON CONFLICT (event_ticker) DO UPDATE SET
      series_ticker=EXCLUDED.series_ticker,
      title=EXCLUDED.title,
      sub_title=EXCLUDED.sub_title,
      category=EXCLUDED.category,
      mutually_exclusive=EXCLUDED.mutually_exclusive,
      collateral_return_type=EXCLUDED.collateral_return_type,
      available_on_brokers=EXCLUDED.available_on_brokers,
      product_metadata=EXCLUDED.product_metadata,
      strike_date=EXCLUDED.strike_date,
      strike_period=EXCLUDED.strike_period,
      updated_at=NOW();
    """
    product_metadata = _extract_event_metadata(event)
    payload = {
        "event_ticker": event.get("event_ticker"),
        "series_ticker": event.get("series_ticker"),
        "title": event.get("title"),
        "sub_title": event.get("sub_title"),
        "category": event.get("category"),
        "mutually_exclusive": event.get("mutually_exclusive"),
        "collateral_return_type": event.get("collateral_return_type"),
        "available_on_brokers": event.get("available_on_brokers"),
        "product_metadata": to_json_value(product_metadata),
        "strike_date": parse_ts_iso(event.get("strike_date")),
        "strike_period": event.get("strike_period"),
    }
    with conn.cursor() as cur:
        cur.execute(sql, payload)
    if payload.get("event_ticker"):
        _portal_rollup_refresh_events(conn, {payload["event_ticker"]})
    conn.commit()


def upsert_market(conn: psycopg.Connection, market: dict) -> None:
    """Insert or update a market row.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param market: Market payload from the API.
    :type market: dict
    """
    sql = """
    INSERT INTO markets(
      ticker, event_ticker, market_type, title, subtitle, yes_sub_title, no_sub_title,
      category, response_price_units,
      created_time, open_time, close_time, expiration_time,
      latest_expiration_time, expected_expiration_time,
      settlement_timer_seconds, can_close_early, early_close_condition,
      rules_primary, rules_secondary,
      tick_size, risk_limit_cents,
      price_level_structure, price_ranges,
      strike_type, floor_strike, cap_strike, functional_strike, custom_strike,
      mve_collection_ticker, mve_selected_legs, primary_participant_key,
      settlement_value, settlement_value_dollars, settlement_ts,
      updated_at
    )
    VALUES(
      %(ticker)s, %(event_ticker)s, %(market_type)s, %(title)s, %(subtitle)s,
      %(yes_sub_title)s, %(no_sub_title)s,
      %(category)s, %(response_price_units)s,
      %(created_time)s, %(open_time)s, %(close_time)s, %(expiration_time)s,
      %(latest_expiration_time)s, %(expected_expiration_time)s,
      %(settlement_timer_seconds)s, %(can_close_early)s, %(early_close_condition)s,
      %(rules_primary)s, %(rules_secondary)s,
      %(tick_size)s, %(risk_limit_cents)s,
      %(price_level_structure)s, %(price_ranges)s,
      %(strike_type)s, %(floor_strike)s, %(cap_strike)s, %(functional_strike)s, %(custom_strike)s,
      %(mve_collection_ticker)s, %(mve_selected_legs)s, %(primary_participant_key)s,
      %(settlement_value)s, %(settlement_value_dollars)s, %(settlement_ts)s,
      NOW()
    )
    ON CONFLICT (ticker) DO UPDATE SET
      event_ticker=EXCLUDED.event_ticker,
      market_type=EXCLUDED.market_type,
      title=EXCLUDED.title,
      subtitle=EXCLUDED.subtitle,
      yes_sub_title=EXCLUDED.yes_sub_title,
      no_sub_title=EXCLUDED.no_sub_title,
      category=EXCLUDED.category,
      response_price_units=EXCLUDED.response_price_units,
      created_time=EXCLUDED.created_time,
      open_time=EXCLUDED.open_time,
      close_time=EXCLUDED.close_time,
      expiration_time=EXCLUDED.expiration_time,
      latest_expiration_time=EXCLUDED.latest_expiration_time,
      expected_expiration_time=EXCLUDED.expected_expiration_time,
      settlement_timer_seconds=EXCLUDED.settlement_timer_seconds,
      can_close_early=EXCLUDED.can_close_early,
      early_close_condition=EXCLUDED.early_close_condition,
      rules_primary=EXCLUDED.rules_primary,
      rules_secondary=EXCLUDED.rules_secondary,
      tick_size=EXCLUDED.tick_size,
      risk_limit_cents=EXCLUDED.risk_limit_cents,
      price_level_structure=EXCLUDED.price_level_structure,
      price_ranges=EXCLUDED.price_ranges,
      strike_type=EXCLUDED.strike_type,
      floor_strike=EXCLUDED.floor_strike,
      cap_strike=EXCLUDED.cap_strike,
      functional_strike=EXCLUDED.functional_strike,
      custom_strike=EXCLUDED.custom_strike,
      mve_collection_ticker=EXCLUDED.mve_collection_ticker,
      mve_selected_legs=EXCLUDED.mve_selected_legs,
      primary_participant_key=EXCLUDED.primary_participant_key,
      settlement_value=EXCLUDED.settlement_value,
      settlement_value_dollars=EXCLUDED.settlement_value_dollars,
      settlement_ts=EXCLUDED.settlement_ts,
      updated_at=NOW();
    """
    price_ranges, custom_strike, mve_selected_legs = _resolve_market_metadata(market)
    payload = {
        "ticker": market.get("ticker"),
        "event_ticker": market.get("event_ticker"),
        "market_type": market.get("market_type"),
        "title": market.get("title"),
        "subtitle": market.get("subtitle"),
        "yes_sub_title": market.get("yes_sub_title"),
        "no_sub_title": market.get("no_sub_title"),
        "category": market.get("category"),
        "response_price_units": market.get("response_price_units"),

        "created_time": parse_ts_iso(market.get("created_time")),
        "open_time": parse_ts_iso(market.get("open_time")),
        "close_time": parse_ts_iso(market.get("close_time")),
        "expiration_time": parse_ts_iso(market.get("expiration_time")),
        "latest_expiration_time": parse_ts_iso(market.get("latest_expiration_time")),
        "expected_expiration_time": parse_ts_iso(market.get("expected_expiration_time")),

        "settlement_timer_seconds": market.get("settlement_timer_seconds"),
        "can_close_early": market.get("can_close_early"),
        "early_close_condition": market.get("early_close_condition"),

        "rules_primary": market.get("rules_primary"),
        "rules_secondary": market.get("rules_secondary"),

        "tick_size": market.get("tick_size"),
        "risk_limit_cents": market.get("risk_limit_cents"),

        "price_level_structure": market.get("price_level_structure"),
        "price_ranges": to_json_value(price_ranges),

        "strike_type": market.get("strike_type"),
        "floor_strike": market.get("floor_strike"),
        "cap_strike": market.get("cap_strike"),
        "functional_strike": market.get("functional_strike"),
        "custom_strike": to_json_value(custom_strike),

        "mve_collection_ticker": market.get("mve_collection_ticker"),
        "mve_selected_legs": to_json_value(mve_selected_legs),
        "primary_participant_key": market.get("primary_participant_key"),

        "settlement_value": market.get("settlement_value"),
        "settlement_value_dollars": dec(market.get("settlement_value_dollars")),
        "settlement_ts": parse_ts_iso(market.get("settlement_ts")),
    }
    with conn.cursor() as cur:
        cur.execute(sql, payload)
    if payload.get("event_ticker"):
        _portal_rollup_refresh_events(conn, {payload["event_ticker"]})
    conn.commit()


def market_is_active(market: dict) -> tuple[bool, str]:
    """Determine whether a market should be treated as active."""
    market_status = (market.get("status") or "").lower()
    if market_status in {"open", "paused", "active"}:
        return True, market_status
    if market_status:
        return False, market_status
    open_time = parse_ts_iso(market.get("open_time"))
    close_time = parse_ts_iso(market.get("close_time"))
    now = datetime.now(timezone.utc)
    is_active = (
        (open_time is None or open_time <= now)
        and (close_time is None or close_time > now)
    )
    return is_active, market_status


def upsert_active_market(
    conn: psycopg.Connection,
    ticker: str,
    event_ticker: str,
    close_time,
    status: str | None,
) -> None:
    """Insert or update an active market row.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param ticker: Market ticker symbol.
    :type ticker: str
    :param event_ticker: Parent event ticker symbol.
    :type event_ticker: str
    :param close_time: Market close time.
    :type close_time: datetime.datetime | None
    :param status: Market status string.
    :type status: str | None
    """
    sql = """
    INSERT INTO active_markets(ticker, event_ticker, close_time, status, last_seen_ts, updated_at)
    VALUES (%s, %s, %s, %s, NOW(), NOW())
    ON CONFLICT (ticker) DO UPDATE SET
      event_ticker=EXCLUDED.event_ticker,
      close_time=EXCLUDED.close_time,
      status=EXCLUDED.status,
      last_seen_ts=NOW(),
      updated_at=NOW();
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ticker, event_ticker, close_time, status))
    if event_ticker:
        _portal_rollup_refresh_events(conn, {event_ticker})
    conn.commit()


def maybe_upsert_active_market_from_market(
    conn: psycopg.Connection,
    market: dict,
) -> bool:
    """Upsert active market metadata when the market is active."""
    ticker = market.get("ticker")
    event_ticker = market.get("event_ticker")
    if not ticker or not event_ticker:
        return False
    is_active, market_status = market_is_active(market)
    if not is_active:
        return False
    status = market.get("status") or market_status or None
    upsert_active_market(
        conn,
        ticker=ticker,
        event_ticker=event_ticker,
        close_time=parse_ts_iso(market.get("close_time")),
        status=status,
    )
    return True


def delete_active_market(conn: psycopg.Connection, ticker: str) -> None:
    """Remove a market from the active set.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param ticker: Market ticker symbol.
    :type ticker: str
    """
    if not _portal_rollup_app_refresh_enabled():
        with conn.cursor() as cur:
            cur.execute("DELETE FROM active_markets WHERE ticker=%s", (ticker,))
        conn.commit()
        return
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM active_markets WHERE ticker=%s RETURNING event_ticker",
            (ticker,),
        )
        rows = cur.fetchall()
    _portal_rollup_refresh_events(conn, {row[0] for row in rows if row and row[0]})
    conn.commit()


def cleanup_active_markets(conn: psycopg.Connection, grace_minutes: int = 30) -> int:
    """Delete active markets that are past their close time.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param grace_minutes: Grace period after close before cleanup.
    :type grace_minutes: int
    :return: Number of rows deleted.
    :rtype: int
    """
    sql = """
    DELETE FROM active_markets am
    USING markets m
    WHERE am.ticker = m.ticker
      AND COALESCE(am.close_time, m.close_time) IS NOT NULL
      AND COALESCE(am.close_time, m.close_time) <= NOW() - (%s * INTERVAL '1 minute')
    """
    if not _portal_rollup_app_refresh_enabled():
        with conn.cursor() as cur:
            cur.execute(sql, (grace_minutes,))
            deleted = cur.rowcount
        conn.commit()
        return deleted
    sql = """
    DELETE FROM active_markets am
    USING markets m
    WHERE am.ticker = m.ticker
      AND COALESCE(am.close_time, m.close_time) IS NOT NULL
      AND COALESCE(am.close_time, m.close_time) <= NOW() - (%s * INTERVAL '1 minute')
    RETURNING am.event_ticker
    """
    with conn.cursor() as cur:
        cur.execute(sql, (grace_minutes,))
        rows = cur.fetchall()
    _portal_rollup_refresh_events(conn, {row[0] for row in rows if row and row[0]})
    conn.commit()
    return len(rows)


def load_active_tickers(conn: psycopg.Connection, limit: int) -> list[str]:
    """Backward-compatible wrapper for src.db.tickers.load_active_tickers."""
    from . import tickers as _tickers  # pylint: disable=import-outside-toplevel

    return _tickers.load_active_tickers(conn, limit)


def load_active_tickers_shard(
    conn: psycopg.Connection,
    limit: int,
    shard_count: int,
    shard_id: int,
    *,
    shard_key: str = "event",
    round_robin: bool = False,
    cursor_key: str | None = None,
    round_robin_step: int | None = None,
) -> list[str]:
    """Backward-compatible wrapper for src.db.tickers.load_active_tickers_shard."""
    if shard_count <= 1:
        return load_active_tickers(conn, limit)
    from . import tickers as _tickers  # pylint: disable=import-outside-toplevel

    return _tickers.load_active_tickers_shard(
        conn,
        limit,
        shard_count,
        shard_id,
        shard_key=shard_key,
        round_robin=round_robin,
        cursor_key=cursor_key,
        round_robin_step=round_robin_step,
    )


def upsert_active_markets_from_markets(
    conn: psycopg.Connection,
    tickers: list[str],
) -> int:
    """Populate active_markets rows using data from markets."""
    if not tickers:
        return 0
    sql = """
    INSERT INTO active_markets(ticker, event_ticker, close_time, status, last_seen_ts, updated_at)
    SELECT m.ticker, m.event_ticker, m.close_time, NULL, NOW(), NOW()
    FROM markets m
    WHERE m.ticker = ANY(%s)
      AND m.event_ticker IS NOT NULL
      AND (m.open_time IS NULL OR m.open_time <= NOW())
      AND (m.close_time IS NULL OR m.close_time > NOW() - INTERVAL '30 minutes')
    ON CONFLICT (ticker) DO UPDATE SET
      event_ticker=EXCLUDED.event_ticker,
      close_time=EXCLUDED.close_time,
      status=COALESCE(active_markets.status, EXCLUDED.status),
      last_seen_ts=NOW(),
      updated_at=NOW()
    RETURNING event_ticker
    """
    with conn.cursor() as cur:
        cur.execute(sql, (tickers,))
        rows = cur.fetchall()
    _portal_rollup_refresh_events(conn, {row[0] for row in rows if row and row[0]})
    conn.commit()
    return len(rows)


def seed_active_markets_from_markets(conn: psycopg.Connection) -> int:
    """Populate active_markets from markets table when active set is empty."""
    sql = """
    INSERT INTO active_markets(ticker, event_ticker, close_time, status, last_seen_ts, updated_at)
    SELECT m.ticker, m.event_ticker, m.close_time, NULL, NOW(), NOW()
    FROM markets m
    WHERE m.event_ticker IS NOT NULL
      AND (m.open_time IS NULL OR m.open_time <= NOW())
      AND (m.close_time IS NULL OR m.close_time > NOW() - INTERVAL '30 minutes')
    ON CONFLICT (ticker) DO UPDATE SET
      event_ticker=EXCLUDED.event_ticker,
      close_time=EXCLUDED.close_time,
      status=COALESCE(active_markets.status, EXCLUDED.status),
      last_seen_ts=NOW(),
      updated_at=NOW()
    RETURNING event_ticker
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        inserted = len(rows)
    _portal_rollup_refresh_events(conn, {row[0] for row in rows if row and row[0]})
    conn.commit()
    return inserted


def _market_tick_payload(tick: dict) -> dict:
    return {
        "ts": tick.get("ts"),
        "ticker": tick.get("ticker"),
        "price": tick.get("price"),
        "yes_bid": tick.get("yes_bid"),
        "yes_ask": tick.get("yes_ask"),
        "price_dollars": tick.get("price_dollars"),
        "yes_bid_dollars": tick.get("yes_bid_dollars"),
        "yes_ask_dollars": tick.get("yes_ask_dollars"),
        "no_bid_dollars": tick.get("no_bid_dollars"),
        "volume": tick.get("volume"),
        "open_interest": tick.get("open_interest"),
        "dollar_volume": tick.get("dollar_volume"),
        "dollar_open_interest": tick.get("dollar_open_interest"),
        "implied_yes_mid": tick.get("implied_yes_mid"),
        "sid": tick.get("sid"),
        "raw": to_json_value(tick.get("raw")),
    }


def _latest_tick_payloads(
    ticks: list[dict],
    payloads: list[dict],
) -> list[dict]:
    """Return one payload per ticker using the latest timestamp in the batch."""
    latest_by_ticker: dict[str, tuple[datetime, dict]] = {}
    for tick, payload in zip(ticks, payloads):
        ticker = payload.get("ticker")
        if not isinstance(ticker, str) or not ticker:
            continue
        ts_value = parse_ts_iso(payload.get("ts") or tick.get("ts"))
        if ts_value is None:
            continue
        previous = latest_by_ticker.get(ticker)
        if previous is None or ts_value >= previous[0]:
            latest_by_ticker[ticker] = (ts_value, payload)
    return [entry[1] for entry in latest_by_ticker.values()]


def _ensure_markets_exist(conn: psycopg.Connection, tickers: set[str]) -> list[str]:
    if not tickers:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO markets (ticker, updated_at)
            SELECT unnest(%s::text[]), NOW()
            ON CONFLICT (ticker) DO NOTHING
            RETURNING ticker
            """,
            (sorted(tickers),),
        )
        rows = cur.fetchall()
    return [row[0] for row in rows]


def _enqueue_discover_market_jobs(conn: psycopg.Connection, tickers: list[str]) -> None:
    if not tickers:
        return
    if enqueue_job is None:
        if _ENQUEUE_JOB_IMPORT_ERROR:
            logger.error(
                "discover_market enqueue unavailable",
                exc_info=_ENQUEUE_JOB_IMPORT_ERROR,
            )
        else:
            logger.error("discover_market enqueue unavailable")
        return
    try:
        for ticker in tickers:
            enqueue_job(conn, "discover_market", {"ticker": ticker}, commit=False)
        conn.commit()
    except PermissionError:
        logger.warning("discover_market enqueue blocked by guardrails")
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("discover_market enqueue failed")
        try:
            conn.rollback()
        except Exception:  # pylint: disable=broad-exception-caught
            logger.warning("discover_market enqueue rollback failed")


def _log_placeholder_inserts(tickers: list[str]) -> None:
    if not tickers:
        return
    sample = ",".join(tickers[:25])
    _log_metric(
        logger,
        "ws.placeholder_markets",
        placeholders=len(tickers),
        unique_tickers=len(tickers),
        tickers=sample,
    )


_MARKET_TICKS_INSERT_SQL = """
INSERT INTO market_ticks(
  ts, ticker, price, yes_bid, yes_ask,
  price_dollars, yes_bid_dollars, yes_ask_dollars, no_bid_dollars,
  volume, open_interest, dollar_volume, dollar_open_interest,
  implied_yes_mid, sid, raw
)
VALUES(
  %(ts)s, %(ticker)s, %(price)s, %(yes_bid)s, %(yes_ask)s,
  %(price_dollars)s, %(yes_bid_dollars)s, %(yes_ask_dollars)s, %(no_bid_dollars)s,
  %(volume)s, %(open_interest)s, %(dollar_volume)s, %(dollar_open_interest)s,
  %(implied_yes_mid)s, %(sid)s, %(raw)s
)
"""
_MARKET_TICKS_LATEST_SQL = """
INSERT INTO market_ticks_latest(
  ticker, ts, price, yes_bid, yes_ask,
  price_dollars, yes_bid_dollars, yes_ask_dollars, no_bid_dollars,
  volume, open_interest, dollar_volume, dollar_open_interest,
  implied_yes_mid, sid, raw, updated_at
)
VALUES(
  %(ticker)s, %(ts)s, %(price)s, %(yes_bid)s, %(yes_ask)s,
  %(price_dollars)s, %(yes_bid_dollars)s, %(yes_ask_dollars)s, %(no_bid_dollars)s,
  %(volume)s, %(open_interest)s, %(dollar_volume)s, %(dollar_open_interest)s,
  %(implied_yes_mid)s, %(sid)s, %(raw)s, NOW()
)
ON CONFLICT (ticker) DO UPDATE SET
  ts = EXCLUDED.ts,
  price = EXCLUDED.price,
  yes_bid = EXCLUDED.yes_bid,
  yes_ask = EXCLUDED.yes_ask,
  price_dollars = EXCLUDED.price_dollars,
  yes_bid_dollars = EXCLUDED.yes_bid_dollars,
  yes_ask_dollars = EXCLUDED.yes_ask_dollars,
  no_bid_dollars = EXCLUDED.no_bid_dollars,
  volume = EXCLUDED.volume,
  open_interest = EXCLUDED.open_interest,
  dollar_volume = EXCLUDED.dollar_volume,
  dollar_open_interest = EXCLUDED.dollar_open_interest,
  implied_yes_mid = EXCLUDED.implied_yes_mid,
  sid = EXCLUDED.sid,
  raw = EXCLUDED.raw,
  updated_at = NOW()
WHERE market_ticks_latest.ts IS NULL
   OR EXCLUDED.ts >= market_ticks_latest.ts
"""


def _collect_tick_tickers(ticks: list[dict]) -> set[str]:
    tickers: set[str] = set()
    for tick in ticks:
        ticker = tick.get("ticker")
        if isinstance(ticker, str) and ticker:
            tickers.add(ticker)
    return tickers


def _last_tick_times(
    ticks: list[dict],
) -> tuple[Optional[datetime], Optional[datetime]]:
    last_ts: Optional[datetime] = None
    last_ws_ts: Optional[datetime] = None
    for tick in ticks:
        ts_value = parse_ts_iso(tick.get("ts"))
        if ts_value is None:
            continue
        if last_ts is None or ts_value > last_ts:
            last_ts = ts_value
        raw = tick.get("raw")
        source = raw.get("source") if isinstance(raw, dict) else None
        if source != "live_snapshot":
            if last_ws_ts is None or ts_value > last_ws_ts:
                last_ws_ts = ts_value
    return last_ts, last_ws_ts


def insert_market_tick(conn: psycopg.Connection, tick: dict) -> None:
    """Insert a market tick row.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param tick: Normalized tick payload.
    :type tick: dict
    """
    insert_market_ticks(conn, [tick])


def insert_lifecycle_event(conn: psycopg.Connection, event: dict) -> None:
    """Insert a market lifecycle event row."""
    insert_lifecycle_events(conn, [event])


def insert_market_prediction(conn: psycopg.Connection, prediction: dict) -> None:
    """Insert a single market prediction row."""
    insert_market_predictions(conn, [prediction])


def insert_market_ticks(conn: psycopg.Connection, ticks: list[dict]) -> None:
    """Insert multiple market tick rows in one transaction."""
    if not ticks:
        return
    tickers = _collect_tick_tickers(ticks)
    inserted = _ensure_markets_exist(conn, tickers)
    payloads = [_market_tick_payload(tick) for tick in ticks]
    latest_payloads = _latest_tick_payloads(ticks, payloads)
    last_ts, last_ws_ts = _last_tick_times(ticks)
    with conn.cursor() as cur:
        cur.executemany(_MARKET_TICKS_INSERT_SQL, payloads)
        if latest_payloads:
            cur.executemany(_MARKET_TICKS_LATEST_SQL, latest_payloads)
        if last_ts is not None:
            cur.execute(
                """
                INSERT INTO ingest_state(key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = NOW()
                """,
                ("last_tick_ts", last_ts.isoformat()),
            )
        if last_ws_ts is not None:
            cur.execute(
                """
                INSERT INTO ingest_state(key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = NOW()
                """,
                ("last_ws_tick_ts", last_ws_ts.isoformat()),
            )
        if _portal_rollup_app_refresh_enabled():
            _portal_rollup_refresh_events(
                conn,
                set(_event_tickers_for_market_tickers(conn, tickers)),
            )
    conn.commit()
    _log_placeholder_inserts(inserted)
    _enqueue_discover_market_jobs(conn, inserted)
