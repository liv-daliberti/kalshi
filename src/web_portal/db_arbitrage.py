"""Market arbitrage queries for the web portal."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

import psycopg  # pylint: disable=import-error
from psycopg.rows import dict_row  # pylint: disable=import-error

from ..db.sql_fragments import (
    event_core_columns_sql,
    last_tick_lateral_sql,
    market_identity_columns_sql,
    tick_columns_sql,
)

from .db_timing import timed_cursor
from .filter_sql import build_filter_where, normalize_search
from .formatters import (
    _adjust_quote_inputs,
    _format_age_minutes,
    _market_label,
    _parse_ts,
    clamp_probability,
    get_event_url,
    get_market_url,
)


@dataclass(frozen=True)
class ArbitrageFilterConfig:
    """Normalized filter thresholds for arbitrage queries."""

    max_tick_age_minutes: float | None


def build_arbitrage_filters(
    max_tick_age_minutes: float | None,
) -> ArbitrageFilterConfig:
    """Build normalized arbitrage filter thresholds."""
    return ArbitrageFilterConfig(max_tick_age_minutes=max_tick_age_minutes)


def _fmt_percent_any(value: Decimal | None) -> str:
    if value is None:
        return "N/A"
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return "N/A"
    if dec < 0:
        return "N/A"
    if dec == 0:
        return "0%"
    if dec < Decimal("0.01"):
        return "<1%"
    pct = int((dec * Decimal(100)).to_integral_value(rounding=ROUND_HALF_UP))
    return f"{pct}%"


def _build_arbitrage_where(
    filters: "PortalFilters",
) -> tuple[str, list[Any]]:
    """Build SQL WHERE fragments for arbitrage filters."""
    return build_filter_where(
        filters,
        [
            "e.search_text",
            "m.search_text",
        ],
        search_override=normalize_search(filters.search),
    )


def _fetch_arbitrage_rows(
    conn: psycopg.Connection,
    filters: "PortalFilters",
) -> list[dict[str, Any]]:
    """Load candidate arbitrage rows from the database."""
    where_sql, params = _build_arbitrage_where(filters)
    query = f"""
        SELECT
          {event_core_columns_sql()},
          e.series_ticker,
          e.strike_period,
          e.mutually_exclusive,
          {market_identity_columns_sql(ticker_alias="market_ticker")},
          m.open_time,
          m.close_time,
          {tick_columns_sql()}
        FROM events e
        JOIN markets m ON m.event_ticker = e.event_ticker
        JOIN active_markets am ON am.ticker = m.ticker
        {last_tick_lateral_sql()}
        WHERE e.mutually_exclusive IS TRUE
          AND (m.open_time IS NULL OR m.open_time <= NOW())
          AND (m.close_time IS NULL OR m.close_time > NOW())
          AND t.ts IS NOT NULL
          {where_sql}
        ORDER BY e.event_ticker, m.open_time NULLS LAST, m.ticker
        """
    with timed_cursor(conn, row_factory=dict_row) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def _get_price_with_fallback(row: dict[str, Any]) -> tuple[Decimal | None, str]:
    """
    Get a price from bid/ask, falling back to implied_yes_mid or price_dollars.
    Returns (price, basis) where basis is 'bid/ask', 'mid', or 'last'.
    """
    yes_bid = clamp_probability(row.get("yes_bid_dollars"))
    yes_ask = clamp_probability(row.get("yes_ask_dollars"))
    last_price = clamp_probability(row.get("price_dollars"))
    implied_mid = clamp_probability(row.get("implied_yes_mid"))

    # For sums, prefer bid/ask. If missing, try mid. If missing, use last.
    if yes_ask is not None:
        return yes_ask, "ask"
    if yes_bid is not None:
        return yes_bid, "bid"
    if implied_mid is not None:
        return implied_mid, "mid"
    if last_price is not None:
        return last_price, "last"
    return None, ""


def _build_arbitrage_item(
    event_rows: list[dict[str, Any]],
    now: datetime,
    filters: ArbitrageFilterConfig,
) -> dict[str, Any] | None:
    if not event_rows or len(event_rows) < 2:
        return None
    event_row = event_rows[0]
    event_ticker = event_row.get("event_ticker")
    if not event_ticker:
        return None

    total_markets = len(event_rows)
    sum_yes_ask = Decimal("0")
    sum_yes_bid = Decimal("0")
    sum_yes_fallback = Decimal("0")
    ask_count = 0
    bid_count = 0
    priced_count = 0  # Any price (including fallback)
    oldest_tick_ts = None
    market_tags: list[dict[str, Any]] = []
    price_basis_set: set[str] = set()

    for row in event_rows:
        tick_ts = _parse_ts(row.get("last_tick_ts"))
        if tick_ts is None:
            continue
        if oldest_tick_ts is None or tick_ts < oldest_tick_ts:
            oldest_tick_ts = tick_ts

        yes_bid = clamp_probability(row.get("yes_bid_dollars"))
        yes_ask = clamp_probability(row.get("yes_ask_dollars"))
        last_price = clamp_probability(row.get("price_dollars"))
        implied_mid = clamp_probability(row.get("implied_yes_mid"))

        yes_bid, yes_ask, _, wide_spread = _adjust_quote_inputs(
            yes_bid,
            yes_ask,
            last_price,
            implied_mid,
        )

        # Skip markets with wide spreads entirely
        if wide_spread:
            continue

        # Track bid/ask coverage
        if yes_ask is not None:
            sum_yes_ask += yes_ask
            ask_count += 1
        if yes_bid is not None:
            sum_yes_bid += yes_bid
            bid_count += 1

        # Fallback for partial coverage: use mid or last if no bid/ask
        price_fallback, basis = _get_price_with_fallback(row)
        if price_fallback is not None:
            sum_yes_fallback += price_fallback
            priced_count += 1
            price_basis_set.add(basis)

        market_tags.append(
            {
                "label": _market_label(row),
                "ticker": row.get("market_ticker"),
                "url": get_market_url(
                    row.get("market_ticker"),
                    event_ticker=event_ticker,
                    event_title=event_row.get("event_title"),
                    series_ticker=event_row.get("series_ticker"),
                ),
            }
        )

    # Filter by tick freshness
    if oldest_tick_ts is None:
        return None
    stale_age_minutes, stale_age_label = _format_age_minutes(oldest_tick_ts, now)
    if filters.max_tick_age_minutes is not None:
        if stale_age_minutes is None or stale_age_minutes > filters.max_tick_age_minutes:
            return None

    # Determine coverage quality
    coverage_pct = (priced_count / total_markets * 100) if total_markets > 0 else 0
    quality = "unknown"
    action = None
    action_key = None
    edge = None
    sum_for_edge = None

    # Confirmed: 95%+ coverage with full bid/ask
    if ask_count == total_markets and bid_count == total_markets:
        quality = "confirmed"
        # Try for arbitrage on full ask coverage (buy YES) or full bid (buy NO)
        if sum_yes_ask < Decimal("1"):
            action = "Buy YES basket"
            action_key = "arb-yes"
            edge = Decimal("1") - sum_yes_ask
            sum_for_edge = sum_yes_ask
        elif sum_yes_bid > Decimal("1"):
            action = "Buy NO basket"
            action_key = "arb-no"
            edge = sum_yes_bid - Decimal("1")
            sum_for_edge = sum_yes_bid
    # Indicative: 60%+ coverage with any price (bid/ask or fallback)
    elif coverage_pct >= 60 and priced_count >= 2:
        quality = "indicative"
        if sum_yes_fallback < Decimal("1"):
            action = "Buy YES basket"
            action_key = "arb-yes"
            edge = Decimal("1") - sum_yes_fallback
            sum_for_edge = sum_yes_fallback
        elif sum_yes_fallback > Decimal("1"):
            action = "Buy NO basket"
            action_key = "arb-no"
            edge = sum_yes_fallback - Decimal("1")
            sum_for_edge = sum_yes_fallback

    # No opportunity found
    if action is None or edge is None:
        return None

    # Format price basis label
    price_basis = "/".join(sorted(price_basis_set)) if price_basis_set else "unknown"

    return {
        "event_title": event_row.get("event_title") or event_ticker,
        "event_ticker": event_ticker,
        "event_category": event_row.get("event_category"),
        "strike_period": event_row.get("strike_period"),
        "event_url": get_event_url(
            event_ticker,
            event_row.get("series_ticker"),
            event_row.get("event_title"),
        ),
        "market_tags": market_tags,
        "market_count": len(market_tags),
        "arb_action": action,
        "arb_action_key": action_key,
        "edge_percent": _fmt_percent_any(edge),
        "sum_ask_bid_percent": _fmt_percent_any(sum_for_edge),
        "tick_age": stale_age_label or "N/A",
        "edge_value": edge,
        "stale_age_minutes": stale_age_minutes,
        "coverage_pct": int(coverage_pct),
        "coverage_label": f"{priced_count}/{total_markets}",
        "quality": quality,
        "price_basis": price_basis,
    }


def fetch_arbitrage_opportunities(
    conn: psycopg.Connection,
    limit: int,
    filters: "PortalFilters",
    *,
    criteria: ArbitrageFilterConfig,
) -> list[dict[str, Any]]:
    """Fetch market arbitrage opportunities across mutually exclusive events."""
    if limit <= 0:
        return []
    rows = _fetch_arbitrage_rows(conn, filters)
    now = datetime.now(timezone.utc)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        event_ticker = row.get("event_ticker")
        if not event_ticker:
            continue
        grouped.setdefault(event_ticker, []).append(row)
    opportunities: list[dict[str, Any]] = []
    for event_rows in grouped.values():
        item = _build_arbitrage_item(event_rows, now, criteria)
        if item is not None:
            opportunities.append(item)
    opportunities.sort(
        key=lambda item: item.get("edge_value") or Decimal("0"),
        reverse=True,
    )
    return opportunities[:limit]
