import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Optional

import psycopg

def init_schema(conn: psycopg.Connection, schema_path: str) -> None:
    with open(schema_path, "r") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def get_state(conn: psycopg.Connection, key: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM ingest_state WHERE key=%s", (key,))
        row = cur.fetchone()
        return row[0] if row else None

def set_state(conn: psycopg.Connection, key: str, value: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_state(key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
            """,
            (key, value),
        )
    conn.commit()

def parse_ts_iso(x: Any) -> Optional[datetime]:
    if not x:
        return None
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    # ISO strings like "...Z" appear in REST responses :contentReference[oaicite:15]{index=15}
    from dateutil.parser import isoparse
    return isoparse(str(x))

def dec(x: Any) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except Exception:
        return None

def implied_yes_mid_cents(yes_bid: Optional[int], yes_ask: Optional[int]) -> Optional[Decimal]:
    if yes_bid is None and yes_ask is None:
        return None
    if yes_bid is not None and yes_ask is not None:
        mid = (Decimal(yes_bid) + Decimal(yes_ask)) / Decimal(2)
    else:
        mid = Decimal(yes_bid if yes_bid is not None else yes_ask)
    return (mid / Decimal(100)).quantize(Decimal("0.000001"))

def upsert_event(conn: psycopg.Connection, e: dict) -> None:
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
    payload = {
        "event_ticker": e.get("event_ticker"),
        "series_ticker": e.get("series_ticker"),
        "title": e.get("title"),
        "sub_title": e.get("sub_title"),
        "category": e.get("category"),
        "mutually_exclusive": e.get("mutually_exclusive"),
        "collateral_return_type": e.get("collateral_return_type"),
        "available_on_brokers": e.get("available_on_brokers"),
        "product_metadata": e.get("product_metadata"),
        "strike_date": parse_ts_iso(e.get("strike_date")),
        "strike_period": e.get("strike_period"),
    }
    with conn.cursor() as cur:
        cur.execute(sql, payload)
    conn.commit()

def upsert_market(conn: psycopg.Connection, m: dict) -> None:
    sql = """
    INSERT INTO markets(
      ticker, event_ticker, market_type, title, subtitle, yes_sub_title, no_sub_title,
      category, response_price_units,
      created_time, open_time, close_time, expiration_time, latest_expiration_time, expected_expiration_time,
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
      %(ticker)s, %(event_ticker)s, %(market_type)s, %(title)s, %(subtitle)s, %(yes_sub_title)s, %(no_sub_title)s,
      %(category)s, %(response_price_units)s,
      %(created_time)s, %(open_time)s, %(close_time)s, %(expiration_time)s, %(latest_expiration_time)s, %(expected_expiration_time)s,
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
    payload = {
        "ticker": m.get("ticker"),
        "event_ticker": m.get("event_ticker"),
        "market_type": m.get("market_type"),
        "title": m.get("title"),
        "subtitle": m.get("subtitle"),
        "yes_sub_title": m.get("yes_sub_title"),
        "no_sub_title": m.get("no_sub_title"),
        "category": m.get("category"),
        "response_price_units": m.get("response_price_units"),

        "created_time": parse_ts_iso(m.get("created_time")),
        "open_time": parse_ts_iso(m.get("open_time")),
        "close_time": parse_ts_iso(m.get("close_time")),
        "expiration_time": parse_ts_iso(m.get("expiration_time")),
        "latest_expiration_time": parse_ts_iso(m.get("latest_expiration_time")),
        "expected_expiration_time": parse_ts_iso(m.get("expected_expiration_time")),

        "settlement_timer_seconds": m.get("settlement_timer_seconds"),
        "can_close_early": m.get("can_close_early"),
        "early_close_condition": m.get("early_close_condition"),

        "rules_primary": m.get("rules_primary"),
        "rules_secondary": m.get("rules_secondary"),

        "tick_size": m.get("tick_size"),
        "risk_limit_cents": m.get("risk_limit_cents"),

        "price_level_structure": m.get("price_level_structure"),
        "price_ranges": m.get("price_ranges"),

        "strike_type": m.get("strike_type"),
        "floor_strike": m.get("floor_strike"),
        "cap_strike": m.get("cap_strike"),
        "functional_strike": m.get("functional_strike"),
        "custom_strike": m.get("custom_strike"),

        "mve_collection_ticker": m.get("mve_collection_ticker"),
        "mve_selected_legs": m.get("mve_selected_legs"),
        "primary_participant_key": m.get("primary_participant_key"),

        "settlement_value": m.get("settlement_value"),
        "settlement_value_dollars": dec(m.get("settlement_value_dollars")),
        "settlement_ts": parse_ts_iso(m.get("settlement_ts")),
    }
    with conn.cursor() as cur:
        cur.execute(sql, payload)
    conn.commit()

def upsert_active_market(conn: psycopg.Connection, ticker: str, event_ticker: str, close_time, status: str) -> None:
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
    conn.commit()

def delete_active_market(conn: psycopg.Connection, ticker: str) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM active_markets WHERE ticker=%s", (ticker,))
    conn.commit()

def load_active_tickers(conn: psycopg.Connection, limit: int) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker
            FROM active_markets
            WHERE close_time IS NULL OR close_time > NOW() - INTERVAL '30 minutes'
            ORDER BY close_time NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        return [r[0] for r in cur.fetchall()]
