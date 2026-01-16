"""Opportunity queries for the web portal."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg  # pylint: disable=import-error
from psycopg.rows import dict_row  # pylint: disable=import-error

from src.db.db import dec
from src.db.sql_fragments import (
    event_core_columns_sql,
    last_prediction_lateral_sql,
    last_tick_lateral_sql,
    market_identity_columns_sql,
    tick_columns_sql,
)

from .config import _env_int
from .db_timing import timed_cursor
from .filter_sql import build_filter_where
from .formatters import (
    _derive_yes_price,
    _format_age_minutes,
    _market_label,
    _market_window,
    _parse_ts,
    clamp_probability,
    fmt_num,
    fmt_percent,
    fmt_time_remaining,
    get_event_url,
    get_market_url,
)

_MIN_SORT_TS = datetime.min.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class OpportunityFilterConfig:
    """Normalized filter thresholds for opportunities."""

    min_gap: Decimal | None
    min_confidence: Decimal | None
    max_prediction_age_minutes: float | None
    max_tick_age_minutes: float | None


@dataclass(frozen=True)
class OpportunityPriceContext:
    """Computed pricing context for a candidate opportunity."""

    yes_price: Decimal
    predicted: Decimal
    gap: Decimal
    confidence: Decimal | None
    market_close_dt: datetime | None


@dataclass(frozen=True)
class OpportunityAgeContext:
    """Computed age context for predictions and ticks."""

    prediction_ts: datetime | None
    prediction_age: str
    tick_ts: datetime | None
    tick_age: str


def _build_opportunity_where(
    filters: "PortalFilters",
) -> tuple[str, list[Any]]:
    """Build SQL WHERE fragments for opportunity filters."""
    return build_filter_where(
        filters,
        [
            "e.title",
            "e.sub_title",
            "e.event_ticker",
            "e.category",
            "m.title",
            "m.subtitle",
            "m.yes_sub_title",
            "m.ticker",
        ],
    )


def _opportunity_scan_limit(limit: int) -> int:
    fallback_scan = max(limit * 5, 500)
    scan_limit = _env_int(
        "WEB_PORTAL_OPPORTUNITY_SCAN_LIMIT",
        fallback_scan,
        minimum=limit,
    )
    return min(scan_limit, 5000)


def _build_opportunity_criteria_where(
    filters: OpportunityFilterConfig,
) -> tuple[str, list[Any]]:
    """Build SQL WHERE fragments for opportunity criteria filters."""
    clauses: list[str] = []
    params: list[Any] = []
    if filters.min_confidence is not None:
        clauses.append("p.confidence >= %s")
        params.append(filters.min_confidence)
    if filters.max_prediction_age_minutes is not None:
        clauses.append("p.created_at >= NOW() - (%s * INTERVAL '1 minute')")
        params.append(filters.max_prediction_age_minutes)
    if filters.max_tick_age_minutes is not None:
        clauses.append("t.ts >= NOW() - (%s * INTERVAL '1 minute')")
        params.append(filters.max_tick_age_minutes)
    if not clauses:
        return "", params
    return " AND " + " AND ".join(clauses), params


def build_opportunity_filters(
    min_gap: float | None,
    min_confidence: float | None,
    max_prediction_age_minutes: float | None,
    max_tick_age_minutes: float | None,
) -> OpportunityFilterConfig:
    """Build normalized opportunity filter thresholds."""
    return OpportunityFilterConfig(
        min_gap=dec(min_gap),
        min_confidence=dec(min_confidence),
        max_prediction_age_minutes=max_prediction_age_minutes,
        max_tick_age_minutes=max_tick_age_minutes,
    )


def _age_label(
    ts_value: datetime | None,
    now: datetime,
    max_minutes: float | None,
) -> str | None:
    age_minutes, age_label = _format_age_minutes(ts_value, now)
    if max_minutes is not None:
        if age_minutes is None or age_minutes > max_minutes:
            return None
    return age_label or "N/A"


def _direction_label(
    predicted: Decimal,
    yes_price: Decimal,
) -> tuple[str, str]:
    if predicted > yes_price:
        return "Model > Market", "up"
    return "Model < Market", "down"


def _opportunity_price_context(
    row: dict[str, Any],
    now: datetime,
    filters: OpportunityFilterConfig,
) -> OpportunityPriceContext | None:
    """Build price context for an opportunity row."""
    _, market_close_dt, market_is_open = _market_window(
        row,
        now,
        open_key="open_time",
        close_key="close_time",
    )
    yes_price, _, _ = _derive_yes_price(row, market_is_open)
    predicted = clamp_probability(row.get("predicted_yes_prob"))
    if yes_price is None or predicted is None:
        return None
    gap = abs(predicted - yes_price)
    if filters.min_gap is not None and gap < filters.min_gap:
        return None
    confidence = clamp_probability(row.get("prediction_confidence"))
    if filters.min_confidence is not None and (
        confidence is None or confidence < filters.min_confidence
    ):
        return None
    return OpportunityPriceContext(
        yes_price=yes_price,
        predicted=predicted,
        gap=gap,
        confidence=confidence,
        market_close_dt=market_close_dt,
    )


def _opportunity_age_context(
    row: dict[str, Any],
    now: datetime,
    filters: OpportunityFilterConfig,
) -> OpportunityAgeContext | None:
    """Build age context for an opportunity row."""
    prediction_ts = _parse_ts(row.get("prediction_ts"))
    prediction_age = _age_label(
        prediction_ts,
        now,
        filters.max_prediction_age_minutes,
    )
    if prediction_age is None:
        return None
    tick_ts = _parse_ts(row.get("last_tick_ts"))
    tick_age = _age_label(tick_ts, now, filters.max_tick_age_minutes)
    if tick_age is None:
        return None
    return OpportunityAgeContext(
        prediction_ts=prediction_ts,
        prediction_age=prediction_age,
        tick_ts=tick_ts,
        tick_age=tick_age,
    )


def _build_opportunity_item(
    row: dict[str, Any],
    now: datetime,
    filters: OpportunityFilterConfig,
) -> dict[str, Any] | None:
    price_ctx = _opportunity_price_context(row, now, filters)
    if price_ctx is None:
        return None
    age_ctx = _opportunity_age_context(row, now, filters)
    if age_ctx is None:
        return None
    direction, direction_key = _direction_label(price_ctx.predicted, price_ctx.yes_price)
    label = _market_label(row)
    return {
        "event_title": row.get("event_title") or "Unknown event",
        "event_ticker": row.get("event_ticker") or "N/A",
        "event_category": row.get("event_category"),
        "strike_period": row.get("strike_period"),
        "market_ticker": row.get("market_ticker") or "N/A",
        "market_label": label,
        "kalshi_percent": fmt_percent(price_ctx.yes_price),
        "gpt_percent": fmt_percent(price_ctx.predicted),
        "gap_percent": fmt_percent(price_ctx.gap),
        "gap_value": price_ctx.gap,
        "confidence_percent": fmt_percent(price_ctx.confidence),
        "prediction_age": age_ctx.prediction_age,
        "tick_age": age_ctx.tick_age,
        "prediction_ts": age_ctx.prediction_ts,
        "last_tick_ts": age_ctx.tick_ts,
        "time_remaining": fmt_time_remaining(price_ctx.market_close_dt, now=now),
        "volume": fmt_num(row.get("volume")),
        "direction": direction,
        "direction_key": direction_key,
        "event_url": get_event_url(
            row.get("event_ticker"),
            row.get("series_ticker"),
            row.get("event_title"),
        ),
        "market_url": get_market_url(
            row.get("market_ticker"),
            event_ticker=row.get("event_ticker"),
            event_title=row.get("event_title"),
            series_ticker=row.get("series_ticker"),
        ),
    }


def _fetch_opportunity_rows(
    conn: psycopg.Connection,
    filters: "PortalFilters",
    criteria: OpportunityFilterConfig,
    scan_limit: int,
) -> list[dict[str, Any]]:
    """Load candidate opportunity rows from the database."""
    where_sql, params = _build_opportunity_where(filters)
    criteria_sql, criteria_params = _build_opportunity_criteria_where(criteria)
    query = f"""
        SELECT
          {event_core_columns_sql()},
          e.series_ticker,
          e.strike_period,
          {market_identity_columns_sql(ticker_alias="market_ticker")},
          m.open_time,
          m.close_time,
          {tick_columns_sql()},
          p.predicted_yes_prob,
          p.confidence AS prediction_confidence,
          p.created_at AS prediction_ts
        FROM events e
        JOIN markets m ON m.event_ticker = e.event_ticker
        JOIN active_markets am ON am.ticker = m.ticker
        {last_tick_lateral_sql()}
        {last_prediction_lateral_sql()}
        WHERE (m.open_time IS NULL OR m.open_time <= NOW())
          AND (m.close_time IS NULL OR m.close_time > NOW())
          AND p.predicted_yes_prob IS NOT NULL
          AND t.ts IS NOT NULL
          {where_sql}
          {criteria_sql}
        ORDER BY p.created_at DESC NULLS LAST, t.ts DESC NULLS LAST
        LIMIT %s
        """
    with timed_cursor(conn, row_factory=dict_row) as cur:
        cur.execute(query, (*params, *criteria_params, scan_limit))
        return cur.fetchall()


def _build_opportunity_list(
    rows: list[dict[str, Any]],
    now: datetime,
    filters: OpportunityFilterConfig,
    limit: int,
) -> list[dict[str, Any]]:
    """Filter, score, and order opportunity rows."""
    opportunities: list[dict[str, Any]] = []
    for row in rows:
        item = _build_opportunity_item(row, now, filters)
        if item is not None:
            opportunities.append(item)
    opportunities.sort(
        key=lambda item: (
            item.get("gap_value") or Decimal("0"),
            item.get("prediction_ts") or _MIN_SORT_TS,
        ),
        reverse=True,
    )
    return opportunities[:limit]


def fetch_opportunities(
    conn: psycopg.Connection,
    limit: int,
    filters: "PortalFilters",
    *,
    criteria: OpportunityFilterConfig,
) -> list[dict[str, Any]]:
    """Fetch market opportunities based on prediction vs. market gaps."""
    if limit <= 0:
        return []
    scan_limit = _opportunity_scan_limit(limit)
    rows = _fetch_opportunity_rows(conn, filters, criteria, scan_limit)
    now = datetime.now(timezone.utc)
    return _build_opportunity_list(rows, now, criteria, limit)
