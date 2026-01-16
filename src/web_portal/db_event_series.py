"""Event series helpers for the web portal."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .config import _env_int
from .formatters import (
    _derive_yes_price,
    _market_label,
    _market_window,
    clamp_probability,
)
from .db_row import DICT_ROW
from .db_timing import timed_cursor
from .portal_utils import portal_func as _portal_func
from .portal_types import PsycopgConnection

_EVENT_FORECAST_COLORS = [
    "#2e7d6f",
    "#e06b2f",
    "#2e78a3",
    "#8b5b12",
    "#5c7c92",
    "#b24a2a",
    "#4b5f7a",
    "#6b8b3b",
    "#b56576",
    "#6d6875",
]


def _sparkline_value(row: dict[str, Any]) -> float | None:
    """Select a price value for sparkline rendering."""
    for key in ("implied_yes_mid", "price_dollars"):
        value = row.get(key)
        if value is not None:
            return float(value)
    yes_bid = row.get("yes_bid_dollars")
    yes_ask = row.get("yes_ask_dollars")
    if yes_bid is not None and yes_ask is not None:
        return float((yes_bid + yes_ask) / Decimal(2))
    if yes_bid is not None:
        return float(yes_bid)
    if yes_ask is not None:
        return float(yes_ask)
    return None


def _build_event_sparklines(
    conn: PsycopgConnection,
    tickers: list[str],
) -> dict[str, list[float]]:
    """Load sparkline points for event outcomes."""
    if not tickers:
        return {}
    max_points = _env_int("WEB_PORTAL_EVENT_SPARKLINE_POINTS", 24, minimum=4)
    with timed_cursor(conn, row_factory=DICT_ROW) as cur:
        cur.execute(
            """
            SELECT ticker, ts, implied_yes_mid, price_dollars, yes_bid_dollars, yes_ask_dollars
            FROM (
              SELECT
                ticker,
                ts,
                implied_yes_mid,
                price_dollars,
                yes_bid_dollars,
                yes_ask_dollars,
                ROW_NUMBER() OVER (
                  PARTITION BY ticker
                  ORDER BY ts DESC, id DESC
                ) AS rn
              FROM market_ticks
              WHERE ticker = ANY(%s)
            ) ticks
            WHERE rn <= %s
            ORDER BY ticker, ts
            """,
            (tickers, max_points),
        )
        rows = cur.fetchall()
    points_by_ticker = {ticker: [] for ticker in tickers}
    for row in rows:
        ticker = row.get("ticker")
        if not ticker:
            continue
        value = _sparkline_value(row)
        if value is None:
            continue
        value = max(0.0, min(1.0, value))
        points_by_ticker.setdefault(ticker, []).append(round(value, 4))
    return points_by_ticker


def _forecast_candidates(
    market_rows: list[dict[str, Any]],
    now: datetime,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    yes_price_fn = _portal_func("_derive_yes_price", _derive_yes_price)
    for row in market_rows:
        ticker = row.get("ticker")
        if not ticker:
            continue
        label = _market_label(row)
        _, _, market_is_open = _market_window(row, now)
        yes_price, _, _ = yes_price_fn(row, market_is_open)
        predicted = clamp_probability(row.get("predicted_yes_prob"))
        if yes_price is not None:
            score = yes_price
        elif predicted is not None:
            score = predicted
        else:
            score = Decimal("-1")
        candidates.append(
            {
                "ticker": ticker,
                "label": label,
                "score": score,
            }
        )
    return candidates


def _select_forecast_candidates(
    candidates: list[dict[str, Any]],
    max_series: int,
) -> tuple[list[dict[str, Any]], list[str], int]:
    candidates.sort(key=lambda item: (item["score"], item["label"]), reverse=True)
    total_outcomes = len(candidates)
    selected = candidates[:max_series] if max_series else candidates
    tickers = [item["ticker"] for item in selected]
    return selected, tickers, total_outcomes


def _series_with_colors(
    selected: list[dict[str, Any]],
    points_by_ticker: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    series: list[dict[str, Any]] = []
    color_index = 0
    for item in selected:
        ticker = item["ticker"]
        points = points_by_ticker.get(ticker) or []
        if not points:
            continue
        series.append(
            {
                "label": item["label"],
                "ticker": ticker,
                "color": _EVENT_FORECAST_COLORS[color_index % len(_EVENT_FORECAST_COLORS)],
                "points": points,
            }
        )
        color_index += 1
    return series


def _series_note(
    total_outcomes: int,
    selected_count: int,
    series_count: int,
    *,
    missing_label: str,
    source_note: str | None = None,
) -> str | None:
    note_parts = []
    if total_outcomes > selected_count:
        note_parts.append(
            f"Showing top {selected_count} of {total_outcomes} outcomes by latest price."
        )
    missing = selected_count - series_count
    if missing:
        note_parts.append(missing_label.format(missing=missing))
    if source_note:
        note_parts.append(source_note)
    return " ".join(note_parts) if note_parts else None


def _series_from_candles(
    conn: PsycopgConnection,
    tickers: list[str],
    selected: list[dict[str, Any]],
    *,
    max_points: int,
    total_outcomes: int,
) -> tuple[list[dict[str, Any]], str | None]:
    with timed_cursor(conn, row_factory=DICT_ROW) as cur:
        cur.execute(
            """
            SELECT market_ticker, end_period_ts, close
            FROM (
              SELECT market_ticker, end_period_ts, close,
                     ROW_NUMBER() OVER (
                       PARTITION BY market_ticker
                       ORDER BY end_period_ts DESC
                     ) AS rn
              FROM market_candles
              WHERE market_ticker = ANY(%s)
            ) candles
            WHERE rn <= %s
            ORDER BY market_ticker, end_period_ts
            """,
            (tickers, max_points),
        )
        candle_rows = cur.fetchall()

    points_by_ticker: dict[str, list[dict[str, Any]]] = {ticker: [] for ticker in tickers}
    for row in candle_rows:
        ticker = row.get("market_ticker")
        end_ts = row.get("end_period_ts")
        close = clamp_probability(row.get("close"))
        if not ticker or end_ts is None or close is None:
            continue
        if ticker not in points_by_ticker:
            continue
        points_by_ticker[ticker].append(
            {
                "ts": end_ts.isoformat(),
                "value": float(close),
            }
        )

    series = _series_with_colors(selected, points_by_ticker)
    if not series:
        return [], None

    note = _series_note(
        total_outcomes,
        len(selected),
        len(series),
        missing_label="No candle history for {missing} outcome(s).",
    )
    return series, note


def _series_from_ticks(
    conn: PsycopgConnection,
    tickers: list[str],
    selected: list[dict[str, Any]],
    *,
    max_points: int,
    total_outcomes: int,
) -> tuple[list[dict[str, Any]], str | None]:
    yes_price_fn = _portal_func("_derive_yes_price", _derive_yes_price)
    with timed_cursor(conn, row_factory=DICT_ROW) as cur:
        cur.execute(
            """
            SELECT ticker, ts, implied_yes_mid, price_dollars, yes_bid_dollars, yes_ask_dollars
            FROM (
              SELECT ticker, ts, implied_yes_mid, price_dollars, yes_bid_dollars, yes_ask_dollars,
                     ROW_NUMBER() OVER (
                       PARTITION BY ticker
                       ORDER BY ts DESC, id DESC
                     ) AS rn
              FROM market_ticks
              WHERE ticker = ANY(%s)
            ) ticks
            WHERE rn <= %s
            ORDER BY ticker, ts
            """,
            (tickers, max_points),
        )
        tick_rows = cur.fetchall()

    points_by_ticker: dict[str, list[dict[str, Any]]] = {ticker: [] for ticker in tickers}
    for row in tick_rows:
        ticker = row.get("ticker")
        tick_ts = row.get("ts")
        yes_price, _, _ = yes_price_fn(row, True)
        if not ticker or tick_ts is None or yes_price is None:
            continue
        points_by_ticker.setdefault(ticker, []).append(
            {
                "ts": tick_ts.isoformat(),
                "value": float(yes_price),
            }
        )

    series = _series_with_colors(selected, points_by_ticker)
    if not series:
        return [], None

    note = _series_note(
        total_outcomes,
        len(selected),
        len(series),
        missing_label="No tick history for {missing} outcome(s).",
        source_note="Using tick history (no candle history available).",
    )
    return series, note


def _build_event_forecast_series(
    conn: PsycopgConnection,
    market_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str | None]:
    """Build time-series data for event-level forecast charts."""
    max_series = _env_int("WEB_PORTAL_EVENT_FORECAST_SERIES_LIMIT", 8, minimum=1)
    max_points = _env_int("WEB_PORTAL_EVENT_FORECAST_POINTS", 200, minimum=2)
    candidates = _forecast_candidates(market_rows, datetime.now(timezone.utc))
    if not candidates:
        return [], None
    selected, tickers, total_outcomes = _select_forecast_candidates(
        candidates,
        max_series,
    )
    if not tickers:
        return [], None
    series, note = _series_from_candles(
        conn,
        tickers,
        selected,
        max_points=max_points,
        total_outcomes=total_outcomes,
    )
    if series:
        return series, note
    return _series_from_ticks(
        conn,
        tickers,
        selected,
        max_points=max_points,
        total_outcomes=total_outcomes,
    )
