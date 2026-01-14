"""Detailed event and market queries for the web portal."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from src.db.sql_fragments import (
    last_candle_lateral_sql,
    last_prediction_lateral_sql,
    last_tick_lateral_sql,
    market_identity_columns_sql,
    tick_columns_sql,
)

from .config import _env_int
from .formatters import (
    _compute_event_outcome_label,
    _derive_yes_price,
    _format_age_minutes,
    _is_wide_spread,
    _market_label,
    _market_window,
    _parse_ts,
    _pricing_error_for_row,
    clamp_probability,
    fmt_bool,
    fmt_cents,
    fmt_json,
    fmt_money,
    fmt_num,
    fmt_outcome,
    fmt_percent,
    fmt_ts,
    get_event_url,
    get_market_url,
)
from .market_metadata import (
    _resolve_market_metadata,
    _update_event_metadata,
    _update_market_extras,
)
from .db_event_series import _build_event_forecast_series, _build_event_sparklines
from .db_row import DICT_ROW
from .portal_utils import portal_func as _portal_func
from .portal_types import PsycopgConnection


def _fetch_event_market_rows(
    conn: PsycopgConnection,
    event_ticker: str,
) -> list[dict[str, Any]]:
    """Fetch market rows for an event with pricing context."""
    tick_cols = (
        "t.ts, t.implied_yes_mid, t.price_dollars, t.yes_bid_dollars, "
        "t.yes_ask_dollars, t.volume, t.raw"
    )
    with conn.cursor(row_factory=DICT_ROW) as cur:
        cur.execute(
            f"""
            SELECT
              {market_identity_columns_sql()},
              m.open_time::text AS market_open_time,
              m.close_time::text AS market_close_time,
              m.settlement_value,
              m.settlement_value_dollars,
              t.raw ->> 'source' AS tick_source,
              {tick_columns_sql()},
              c.end_period_ts AS candle_end_ts,
              c.close AS candle_close,
              p.predicted_yes_prob,
              p.confidence AS prediction_confidence,
              p.created_at AS prediction_ts
            FROM markets m
            {last_tick_lateral_sql(tick_cols)}
            {last_candle_lateral_sql()}
            {last_prediction_lateral_sql()}
            WHERE m.event_ticker = %s
            ORDER BY m.open_time NULLS LAST, m.ticker
            """,
            (event_ticker,),
        )
        return cur.fetchall()


def _fetch_event_detail_row(
    conn: PsycopgConnection,
    event_ticker: str,
) -> dict[str, Any] | None:
    """Fetch the base event row for the detail view."""
    with conn.cursor(row_factory=DICT_ROW) as cur:
        cur.execute(
            """
            SELECT
              e.event_ticker,
              e.title AS event_title,
              e.sub_title AS event_sub_title,
              e.category AS event_category,
              e.series_ticker,
              e.strike_date::text AS strike_date,
              e.strike_period,
              e.mutually_exclusive,
              e.available_on_brokers,
              e.product_metadata,
              MIN(m.open_time)::text AS open_time,
              MAX(m.close_time)::text AS close_time,
              COUNT(*) AS market_count
            FROM events e
            JOIN markets m ON m.event_ticker = e.event_ticker
            WHERE e.event_ticker = %s
            GROUP BY
              e.event_ticker, e.title, e.sub_title, e.category, e.series_ticker,
              e.strike_date, e.strike_period, e.mutually_exclusive,
              e.available_on_brokers, e.product_metadata
            """,
            (event_ticker,),
        )
        return cur.fetchone()


def _event_time_context(
    event_row: dict[str, Any],
    now: datetime,
) -> tuple[bool, bool]:
    """Return (event_is_open, event_is_closed) based on timestamps."""
    open_dt = _parse_ts(event_row.get("open_time"))
    close_dt = _parse_ts(event_row.get("close_time"))
    event_is_open = (open_dt is None or open_dt <= now) and (
        close_dt is None or close_dt > now
    )
    event_is_closed = close_dt is not None and close_dt <= now
    return event_is_open, event_is_closed


@dataclass(frozen=True)
class OutcomePriceContext:
    """Pricing context for an event outcome."""

    yes_price: Decimal | None
    yes_bid: Decimal | None
    no_bid: Decimal | None
    wide_spread: bool
    market_is_open: bool


@dataclass(frozen=True)
class OutcomeFreshness:
    """Freshness context for an event outcome."""

    source: str
    label: str
    age_label: str | None


@dataclass(frozen=True)
class EventOutcomeContext:
    """Context for building event outcomes."""

    event_row: dict[str, Any]
    now: datetime
    yes_price_fn: Callable[
        [dict[str, Any], bool],
        tuple[Decimal | None, Decimal | None, Decimal | None],
    ]
    wide_spread_fn: Callable[[dict[str, Any]], bool]
    pricing_error_fn: Callable[
        [dict[str, Any], Decimal | None, Decimal | None, Decimal | None],
        Any,
    ]
    sparkline_points: dict[str, list[float]]


def _outcome_prices(
    row: dict[str, Any],
    now: datetime,
    yes_price_fn,
    wide_spread_fn,
) -> OutcomePriceContext:
    """Return pricing context for a market row."""
    _, _, market_is_open = _market_window(row, now)
    wide_spread = wide_spread_fn(row)
    yes_price, yes_bid, yes_ask = yes_price_fn(row, market_is_open)
    if wide_spread:
        yes_bid = None
        yes_ask = None

    allow_fill = not wide_spread or not market_is_open
    if yes_bid is None and yes_price is not None and allow_fill:
        yes_bid = yes_price
    if yes_ask is None and yes_price is not None and allow_fill:
        yes_ask = yes_price

    no_bid = None
    if yes_ask is not None:
        no_bid = clamp_probability(Decimal("1") - yes_ask)
    elif yes_bid is not None:
        no_bid = clamp_probability(Decimal("1") - yes_bid)
    return OutcomePriceContext(
        yes_price=yes_price,
        yes_bid=yes_bid,
        no_bid=no_bid,
        wide_spread=wide_spread,
        market_is_open=market_is_open,
    )


def _outcome_freshness(
    row: dict[str, Any],
    now: datetime,
) -> OutcomeFreshness:
    """Return (source, label, age_label) for the freshest data."""
    last_tick_ts = _parse_ts(row.get("last_tick_ts"))
    candle_end_ts = _parse_ts(row.get("candle_end_ts"))
    tick_source = (row.get("tick_source") or "").strip().lower()
    freshness_ts = None
    freshness_source = None
    freshness_label = None
    if last_tick_ts is not None:
        freshness_ts = last_tick_ts
        if tick_source == "live_snapshot":
            freshness_source = "snapshot"
            freshness_label = "Snapshot"
        else:
            freshness_source = "ws"
            freshness_label = "WS"
    elif candle_end_ts is not None:
        freshness_ts = candle_end_ts
        freshness_source = "backfill"
        freshness_label = "Backfill"
    _, freshness_age_label = _format_age_minutes(freshness_ts, now)
    return OutcomeFreshness(
        source=freshness_source or "unknown",
        label=freshness_label or "No data",
        age_label=freshness_age_label,
    )


def _build_event_outcome(
    row: dict[str, Any],
    context: EventOutcomeContext,
) -> tuple[dict[str, Any], bool]:
    """Build an outcome row for the event detail page."""
    price_ctx = _outcome_prices(
        row,
        context.now,
        context.yes_price_fn,
        context.wide_spread_fn,
    )
    pricing_error = context.pricing_error_fn(
        row,
        price_ctx.yes_price,
        price_ctx.yes_bid,
        price_ctx.no_bid,
    )
    liquidity_note = (
        "No liquidity (wide spread)"
        if price_ctx.wide_spread and price_ctx.market_is_open
        else None
    )
    freshness = _outcome_freshness(row, context.now)
    points = context.sparkline_points.get(row.get("ticker") or "", [])
    outcome = {
        "chance_label": _market_label(row),
        "chance_percent": fmt_percent(price_ctx.yes_price),
        "yes_bid": fmt_cents(price_ctx.yes_bid),
        "no_bid": fmt_cents(price_ctx.no_bid),
        "agent_percent": fmt_percent(row.get("predicted_yes_prob")),
        "agent_confidence": fmt_percent(row.get("prediction_confidence")),
        "agent_updated": fmt_ts(row.get("prediction_ts")),
        "volume": fmt_num(row.get("volume")),
        "pricing_error": pricing_error,
        "liquidity_note": liquidity_note,
        "market_ticker": row.get("ticker"),
        "market_url": get_market_url(
            row.get("ticker"),
            event_ticker=context.event_row.get("event_ticker"),
            event_title=context.event_row.get("event_title"),
            series_ticker=context.event_row.get("series_ticker"),
        ),
        "last_update": fmt_ts(row.get("last_tick_ts")),
        "freshness_source": freshness.source,
        "freshness_label": freshness.label,
        "freshness_age_label": freshness.age_label,
        "sparkline_points": points,
        "sparkline_json": json.dumps(points, ensure_ascii=True),
    }
    return outcome, bool(pricing_error)


def _sum_event_volume(market_rows: list[dict[str, Any]]) -> int | None:
    """Sum available market volumes for event-level metrics."""
    total = 0
    has_value = False
    for row in market_rows:
        value = row.get("volume")
        if value is None:
            continue
        try:
            total += int(value)
            has_value = True
        except (TypeError, ValueError):
            continue
    return total if has_value else None


def _build_event_outcomes(
    market_rows: list[dict[str, Any]],
    sparkline_points: dict[str, list[float]],
    event_row: dict[str, Any],
    now: datetime,
) -> tuple[list[dict[str, Any]], bool, bool]:
    """Build outcome rows plus tick/pricing flags."""
    context = EventOutcomeContext(
        event_row=event_row,
        now=now,
        yes_price_fn=_portal_func("_derive_yes_price", _derive_yes_price),
        wide_spread_fn=_portal_func("_is_wide_spread", _is_wide_spread),
        pricing_error_fn=_portal_func("_pricing_error_for_row", _pricing_error_for_row),
        sparkline_points=sparkline_points,
    )
    has_ticks = any(row.get("last_tick_ts") for row in market_rows)
    has_pricing_errors = False
    outcomes = []
    for row in market_rows:
        outcome, pricing_error = _build_event_outcome(row, context)
        if pricing_error:
            has_pricing_errors = True
        outcomes.append(outcome)
    return outcomes, has_ticks, has_pricing_errors


def _event_fields(
    event_row: dict[str, Any],
    event_outcome_label: str,
    event_volume: int | None,
) -> list[tuple[str, str]]:
    """Build the detail field list for an event."""
    return [
        ("Event title", event_row.get("event_title")),
        ("Event outcome", event_outcome_label),
        ("Event ticker", event_row.get("event_ticker")),
        ("Event subtitle", event_row.get("event_sub_title")),
        ("Category", event_row.get("event_category")),
        ("Series ticker", event_row.get("series_ticker")),
        ("Strike date", fmt_ts(event_row.get("strike_date"))),
        ("Strike period", event_row.get("strike_period")),
        ("Mutually exclusive", fmt_bool(event_row.get("mutually_exclusive"))),
        ("Available on brokers", fmt_bool(event_row.get("available_on_brokers"))),
        ("Markets", fmt_num(event_row.get("market_count"))),
        ("Volume", fmt_num(event_volume)),
        ("Open time", fmt_ts(event_row.get("open_time"))),
        ("Close time", fmt_ts(event_row.get("close_time"))),
    ]


def fetch_event_detail(
    conn: PsycopgConnection,
    event_ticker: str,
) -> dict[str, Any] | None:
    """Fetch event detail with outcomes and latest prices."""
    event_row = _fetch_event_detail_row(conn, event_ticker)
    if not event_row:
        return None

    market_rows = _portal_func(
        "_fetch_event_market_rows",
        _fetch_event_market_rows,
    )(conn, event_ticker)
    sparkline_points = _portal_func(
        "_build_event_sparklines",
        _build_event_sparklines,
    )(
        conn,
        [row.get("ticker") for row in market_rows if row.get("ticker")],
    )

    now = datetime.now(timezone.utc)
    event_time = _event_time_context(event_row, now)
    outcomes, has_ticks, has_pricing_errors = _build_event_outcomes(
        market_rows,
        sparkline_points,
        event_row,
        now,
    )

    event_outcome_label = _portal_func(
        "_compute_event_outcome_label",
        _compute_event_outcome_label,
    )(
        market_rows,
        event_row.get("mutually_exclusive"),
    )

    forecast_series, forecast_note = _portal_func(
        "_build_event_forecast_series",
        _build_event_forecast_series,
    )(conn, market_rows)

    return {
        "event_title": event_row.get("event_title") or event_row.get("event_ticker"),
        "event_ticker": event_row.get("event_ticker"),
        "event_subtitle": event_row.get("event_sub_title") or "",
        "event_url": get_event_url(
            event_row.get("event_ticker"),
            event_row.get("series_ticker"),
            event_row.get("event_title"),
        ),
        "event_fields": _event_fields(
            event_row,
            event_outcome_label,
            _sum_event_volume(market_rows),
        ),
        "event_outcome_label": event_outcome_label,
        "outcomes": outcomes,
        "event_is_open": event_time[0],
        "event_is_closed": event_time[1],
        "ticks_missing": (not has_ticks) and event_time[0],
        "pricing_errors": has_pricing_errors,
        "forecast_series": forecast_series,
        "forecast_series_json": json.dumps(forecast_series, ensure_ascii=True),
        "forecast_note": forecast_note,
    }


def _fetch_market_detail_row(
    conn: PsycopgConnection,
    ticker: str,
) -> dict[str, Any] | None:
    """Fetch the raw market detail row."""
    tick_columns = (
        "t.ts, t.implied_yes_mid, t.price_dollars, t.yes_bid_dollars, "
        "t.yes_ask_dollars, t.volume, t.open_interest"
    )
    with conn.cursor(row_factory=DICT_ROW) as cur:
        cur.execute(
            f"""
            SELECT
              m.ticker,
              m.event_ticker,
              m.title,
              m.subtitle,
              m.yes_sub_title,
              m.no_sub_title,
              m.category,
              m.response_price_units,
              m.tick_size,
              m.risk_limit_cents,
              m.strike_type,
              m.floor_strike,
              m.cap_strike,
              m.functional_strike,
              m.settlement_value,
              m.settlement_value_dollars,
              m.rules_primary,
              m.rules_secondary,
              m.price_level_structure,
              m.price_ranges,
              m.custom_strike,
              m.mve_selected_legs,
              m.open_time::text AS open_time,
              m.close_time::text AS close_time,
              m.expiration_time::text AS expiration_time,
              m.settlement_ts::text AS settlement_ts,
              CASE
                WHEN m.close_time IS NOT NULL AND m.close_time <= NOW() THEN 'Closed'
                WHEN m.open_time IS NOT NULL AND m.open_time > NOW() THEN 'Scheduled'
                WHEN am.status IS NOT NULL AND am.status <> '' THEN initcap(am.status)
                ELSE 'Inactive'
              END AS status_label,
              CASE
                WHEN m.close_time IS NOT NULL AND m.close_time <= NOW() THEN 'closed'
                WHEN m.open_time IS NOT NULL AND m.open_time > NOW() THEN 'scheduled'
                WHEN am.status IS NOT NULL AND am.status <> '' THEN lower(am.status)
                ELSE 'inactive'
              END AS status_class,
              e.title AS event_title,
              e.sub_title AS event_sub_title,
              e.category AS event_category,
              e.series_ticker,
              e.strike_date::text AS strike_date,
              e.strike_period,
              e.mutually_exclusive,
              e.available_on_brokers,
              e.product_metadata,
              am.status AS active_status,
              am.last_seen_ts::text AS active_last_seen,
              t.ts AS last_tick_ts,
              t.implied_yes_mid,
              t.price_dollars,
              t.yes_bid_dollars,
              t.yes_ask_dollars,
              t.volume,
              t.open_interest,
              c.end_period_ts AS candle_end_ts,
              c.close AS candle_close
            FROM markets m
            JOIN events e ON e.event_ticker = m.event_ticker
            LEFT JOIN active_markets am ON am.ticker = m.ticker
            {last_tick_lateral_sql(tick_columns)}
            {last_candle_lateral_sql()}
            WHERE m.ticker = %s
            """,
            (ticker,),
        )
        return cur.fetchone()


def _market_event_fields(row: dict[str, Any]) -> list[tuple[str, str]]:
    """Build event fields for the market detail page."""
    return [
        ("Event title", row.get("event_title") or row.get("event_ticker")),
        ("Event ticker", row.get("event_ticker")),
        ("Event subtitle", row.get("event_sub_title")),
        ("Category", row.get("event_category")),
        ("Series ticker", row.get("series_ticker")),
        ("Strike date", fmt_ts(row.get("strike_date"))),
        ("Strike period", row.get("strike_period")),
        ("Mutually exclusive", fmt_bool(row.get("mutually_exclusive"))),
        ("Available on brokers", fmt_bool(row.get("available_on_brokers"))),
    ]


def _market_detail_fields(row: dict[str, Any]) -> list[tuple[str, str]]:
    """Build market fields for the market detail page."""
    return [
        ("Market ticker", row.get("ticker")),
        ("Category", row.get("category")),
        ("Open time", fmt_ts(row.get("open_time"))),
        ("Close time", fmt_ts(row.get("close_time"))),
        ("Expiration time", fmt_ts(row.get("expiration_time"))),
        ("Settlement time", fmt_ts(row.get("settlement_ts"))),
        ("Response units", row.get("response_price_units")),
        ("Tick size", fmt_num(row.get("tick_size"))),
        ("Risk limit", fmt_num(row.get("risk_limit_cents"))),
        ("Strike type", row.get("strike_type")),
        ("Floor strike", fmt_num(row.get("floor_strike"))),
        ("Cap strike", fmt_num(row.get("cap_strike"))),
        ("Functional strike", fmt_num(row.get("functional_strike"))),
    ]


def _market_stats_fields(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Build snapshot stats fields for the market detail page."""
    snapshot_source = "none"
    if row.get("last_tick_ts") is not None:
        snapshot_source = "market_ticks"
    elif row.get("candle_close") is not None:
        snapshot_source = "market_candles"
    return [
        {
            "label": "Status",
            "key": "status",
            "value": row.get("active_status") or row.get("status_label") or "N/A",
        },
        {
            "label": "Yes mid",
            "key": "yes_mid",
            "value": fmt_money(row.get("implied_yes_mid"), digits=4),
        },
        {
            "label": "Last price",
            "key": "last_price",
            "value": fmt_money(row.get("price_dollars"), digits=4),
        },
        {
            "label": "Yes bid",
            "key": "yes_bid",
            "value": fmt_money(row.get("yes_bid_dollars"), digits=4),
        },
        {
            "label": "Yes ask",
            "key": "yes_ask",
            "value": fmt_money(row.get("yes_ask_dollars"), digits=4),
        },
        {
            "label": "Volume",
            "key": "volume",
            "value": fmt_num(row.get("volume")),
        },
        {
            "label": "Open interest",
            "key": "open_interest",
            "value": fmt_num(row.get("open_interest")),
        },
        {
            "label": "Last update",
            "key": "last_update",
            "value": fmt_ts(row.get("last_tick_ts")),
        },
        {
            "label": "Active last seen",
            "key": "active_last_seen",
            "value": fmt_ts(row.get("active_last_seen")),
        },
        {
            "label": "Snapshot source",
            "key": "snapshot_source",
            "value": snapshot_source,
        },
    ]


def _market_extra_fields(row: dict[str, Any]) -> list[tuple[str, str]]:
    """Build extra JSON fields for the market detail page."""
    return [
        ("Price ranges", fmt_json(row.get("price_ranges"))),
        ("Custom strike", fmt_json(row.get("custom_strike"))),
        ("MVE selected legs", fmt_json(row.get("mve_selected_legs"))),
        ("Product metadata", fmt_json(row.get("product_metadata"))),
    ]


def fetch_market_detail(
    conn: PsycopgConnection,
    ticker: str,
) -> dict[str, Any] | None:
    """Fetch a market detail row with metadata and latest prices."""
    row = _fetch_market_detail_row(conn, ticker)

    if not row:
        return None

    market_ticker = row.get("ticker") or ticker
    price_ranges, custom_strike, mve_selected_legs, product_metadata = (
        _resolve_market_metadata(row, market_ticker)
    )

    _update_market_extras(
        conn,
        market_ticker,
        price_ranges=price_ranges,
        custom_strike=custom_strike,
        mve_selected_legs=mve_selected_legs,
    )
    _update_event_metadata(conn, row.get("event_ticker"), product_metadata)

    row["price_ranges"] = price_ranges
    row["custom_strike"] = custom_strike
    row["mve_selected_legs"] = mve_selected_legs
    row["product_metadata"] = product_metadata
    now = datetime.now(timezone.utc)
    market_open_dt, market_close_dt, market_is_open = _market_window(
        row,
        now,
        open_key="open_time",
        close_key="close_time",
    )
    return {
        **row,
        "market_ticker": market_ticker,
        "market_title": row.get("title") or row.get("market_title") or market_ticker,
        "market_subtitle": row.get("subtitle") or row.get("market_subtitle") or "",
        "market_url": get_market_url(
            market_ticker,
            event_ticker=row.get("event_ticker"),
            event_title=row.get("event_title"),
            series_ticker=row.get("series_ticker"),
        ),
        "event_url": get_event_url(
            row.get("event_ticker"),
            row.get("series_ticker"),
            row.get("event_title"),
        ),
        "event_fields": _market_event_fields(row),
        "market_fields": _market_detail_fields(row),
        "stats_fields": _market_stats_fields(row),
        "extra_fields": _market_extra_fields(row),
        "rules_primary": row.get("rules_primary") or "N/A",
        "rules_secondary": row.get("rules_secondary") or "N/A",
        "is_closed": market_close_dt is not None and market_close_dt <= now,
        "market_is_open": market_is_open,
        "outcome_label": fmt_outcome(
            row.get("settlement_value"),
            row.get("settlement_value_dollars"),
        ),
        "snapshot_active": False,
        "predictions": [],
        "prediction_limit": 0,
        "candles": [],
        "candles_json": "[]",
        "candle_interval_minutes": None,
        "market_open_ts": market_open_dt.isoformat() if market_open_dt else None,
        "market_close_ts": market_close_dt.isoformat() if market_close_dt else None,
        "candle_auto_refresh_seconds": _env_int(
            "WEB_PORTAL_CANDLE_AUTO_REFRESH_SECONDS",
            900,
            minimum=60,
        ),
    }
