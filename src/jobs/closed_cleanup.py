"""Closed market cleanup pass to fill missing data."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg  # pylint: disable=import-error

from .backfill import (
    BackfillRangeRequest,
    _build_backfill_ranges,
    _fetch_candle_bounds,
    _insert_candle,
    _iter_time_chunks,
    _market_time_window,
)
from .backfill_config import BackfillConfig
from ..db.db import (
    cleanup_active_markets,
    maybe_upsert_active_market_from_market,
    upsert_event,
    upsert_market,
)
from ..core.env_utils import _env_float
from ..kalshi.kalshi_sdk import get_market_candlesticks, iter_events
from ..core.logging_utils import configure_logging as configure_service_logging, parse_log_level
from ..core.loop_utils import schema_path
from ..core.service_utils import open_client_and_conn
from ..core.settings import load_settings
from ..core.time_utils import (
    ensure_utc,
    infer_strike_period_from_times,
    normalize_strike_period,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClosedCleanupConfig(BackfillConfig):
    """Configuration for the closed-market cleanup pass."""

    grace_minutes: int


@dataclass(frozen=True)
class CandleBounds:
    """Candlestick coverage for a market."""

    period_minutes: Optional[int]
    first_candle_end: Optional[datetime]
    last_candle_end: Optional[datetime]


@dataclass(frozen=True, init=False)
class MissingMarket:
    """Row describing a closed market with missing data."""

    ticker: str
    event_ticker: str
    series_ticker: str
    strike_period: Optional[str]
    open_time: Optional[datetime]
    close_time: Optional[datetime]
    candle_bounds: CandleBounds

    def __init__(
        self,
        *,
        ticker: str,
        event_ticker: str,
        series_ticker: str,
        strike_period: Optional[str],
        open_time: Optional[datetime],
        close_time: Optional[datetime],
        candle_bounds: CandleBounds | None = None,
        period_minutes: Optional[int] = None,
        first_candle_end: Optional[datetime] = None,
        last_candle_end: Optional[datetime] = None,
    ) -> None:
        if candle_bounds is None:
            candle_bounds = CandleBounds(
                period_minutes=period_minutes,
                first_candle_end=first_candle_end,
                last_candle_end=last_candle_end,
            )
        object.__setattr__(self, "ticker", ticker)
        object.__setattr__(self, "event_ticker", event_ticker)
        object.__setattr__(self, "series_ticker", series_ticker)
        object.__setattr__(self, "strike_period", strike_period)
        object.__setattr__(self, "open_time", open_time)
        object.__setattr__(self, "close_time", close_time)
        object.__setattr__(self, "candle_bounds", candle_bounds)

    @property
    def period_minutes(self) -> Optional[int]:
        """Return the inferred candlestick interval in minutes."""
        return self.candle_bounds.period_minutes

    @property
    def first_candle_end(self) -> Optional[datetime]:
        """Return the earliest candle end time seen for the market."""
        return self.candle_bounds.first_candle_end

    @property
    def last_candle_end(self) -> Optional[datetime]:
        """Return the latest candle end time seen for the market."""
        return self.candle_bounds.last_candle_end


@dataclass(frozen=True)
class StrikeResolutionContext:
    """Inputs for resolving strike period and candle bounds."""

    ticker: str
    strike_period: Optional[str]
    open_time: Optional[datetime]
    close_time: Optional[datetime]
    first_candle_end: Optional[datetime]
    last_candle_end: Optional[datetime]


@dataclass(frozen=True)
class StrikeResolutionLimits:
    """Limits used for inferring strike period."""

    hour_max: float
    day_max: float


@dataclass(frozen=True)
class SettlementRefreshContext:
    """Context for settlement refresh passes."""

    status: str
    min_close_ts: Optional[int]
    missing_settlement: dict[str, "MissingMarket"]
    missing_event_tickers: set[str]


def build_closed_cleanup_config(settings) -> ClosedCleanupConfig:
    """Build a closed-cleanup config from settings."""
    return ClosedCleanupConfig(
        strike_periods=settings.strike_periods,
        event_statuses=settings.closed_cleanup_event_statuses,
        minutes_hour=settings.candle_minutes_for_hour,
        minutes_day=settings.candle_minutes_for_day,
        lookback_hours=settings.candle_lookback_hours,
        grace_minutes=settings.closed_cleanup_grace_minutes,
    )


_MISSING_CLOSED_MARKETS_SQL = """
SELECT
  m.ticker,
  m.event_ticker,
  m.open_time,
  m.close_time,
  e.series_ticker,
  e.strike_period,
  m.settlement_value,
  m.settlement_value_dollars,
  m.settlement_ts,
  MIN(mc.end_period_ts) AS first_candle_end_ts,
  MAX(mc.end_period_ts) AS last_candle_end_ts
FROM markets m
JOIN events e ON e.event_ticker = m.event_ticker
LEFT JOIN market_candles mc
  ON mc.market_ticker = m.ticker
 AND mc.period_interval_minutes = CASE
     WHEN LOWER(COALESCE(e.strike_period, '')) = 'hour' THEN %s
     WHEN LOWER(COALESCE(e.strike_period, '')) = 'day' THEN %s
     ELSE %s
 END
WHERE m.close_time IS NOT NULL
  AND m.close_time <= NOW() - (%s * INTERVAL '1 minute')
GROUP BY m.ticker, m.event_ticker, m.open_time, m.close_time,
         e.series_ticker, e.strike_period,
         m.settlement_value, m.settlement_value_dollars, m.settlement_ts
"""

_ROW_TICKER = 0
_ROW_EVENT_TICKER = 1
_ROW_OPEN_TIME = 2
_ROW_CLOSE_TIME = 3
_ROW_SERIES_TICKER = 4
_ROW_STRIKE_PERIOD = 5
_ROW_SETTLEMENT_VALUE = 6
_ROW_SETTLEMENT_VALUE_DOLLARS = 7
_ROW_SETTLEMENT_TS = 8
_ROW_FIRST_CANDLE_END = 9
_ROW_LAST_CANDLE_END = 10


def configure_logging() -> None:
    """Initialize logging based on LOG_LEVEL."""
    level_raw = os.getenv("LOG_LEVEL", "INFO")
    configure_service_logging(
        service_name="closed-cleanup",
        logger=logger,
        basic_config=logging.basicConfig,
        level_raw=level_raw,
    )


def _parse_log_level(raw: str) -> int:
    return parse_log_level(raw, logging.INFO)


def _ensure_tz(value: datetime | None) -> datetime | None:
    return ensure_utc(value)


def _infer_strike_period_from_times(
    open_time: datetime | None,
    close_time: datetime | None,
    *,
    hour_max: float,
    day_max: float,
) -> str | None:
    return infer_strike_period_from_times(
        open_time,
        close_time,
        hour_max=hour_max,
        day_max=day_max,
    )


def _epoch_to_dt(value: Optional[int]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc)


def _dt_to_epoch(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    value = _ensure_tz(value)
    if value is None:
        return None
    return int(value.timestamp())


def _period_minutes(
    strike_period: Optional[str],
    minutes_hour: int,
    minutes_day: int,
) -> Optional[int]:
    if not strike_period:
        return None
    strike_period = strike_period.strip().lower()
    if strike_period == "hour":
        return minutes_hour
    if strike_period == "day":
        return minutes_day
    return None


def _candles_missing(market: MissingMarket) -> bool:
    bounds = market.candle_bounds
    if bounds.period_minutes is None or market.close_time is None:
        return False
    period_delta = timedelta(seconds=bounds.period_minutes * 60)
    if bounds.last_candle_end is None:
        return True
    if bounds.last_candle_end < market.close_time - period_delta:
        return True
    if market.open_time is None or bounds.first_candle_end is None:
        return False
    return bounds.first_candle_end > market.open_time + period_delta


def _fetch_missing_closed_rows(
    conn: psycopg.Connection,
    cfg: ClosedCleanupConfig,
) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            _MISSING_CLOSED_MARKETS_SQL,
            (cfg.minutes_hour, cfg.minutes_day, cfg.minutes_day, cfg.grace_minutes),
        )
        return cur.fetchall()


def _settlement_missing(row: tuple) -> bool:
    settlement_ts = row[_ROW_SETTLEMENT_TS]
    if settlement_ts is None:
        return True
    return (
        row[_ROW_SETTLEMENT_VALUE] is None
        and row[_ROW_SETTLEMENT_VALUE_DOLLARS] is None
    )


def _normalize_strike_period(strike_period: Optional[str]) -> Optional[str]:
    return normalize_strike_period(strike_period)


def _resolve_period_minutes(
    cfg: ClosedCleanupConfig,
    context: StrikeResolutionContext,
    limits: StrikeResolutionLimits,
) -> tuple[Optional[str], Optional[int], bool]:
    strike_period_norm = _normalize_strike_period(context.strike_period)
    period_minutes = _period_minutes(
        strike_period_norm,
        cfg.minutes_hour,
        cfg.minutes_day,
    )
    inferred = False
    if period_minutes is None:
        inferred_period = infer_strike_period_from_times(
            context.open_time,
            context.close_time,
            limits.hour_max,
            limits.day_max,
        )
        if inferred_period:
            strike_period_norm = inferred_period
            period_minutes = _period_minutes(
                strike_period_norm,
                cfg.minutes_hour,
                cfg.minutes_day,
            )
            inferred = period_minutes is not None
    return strike_period_norm, period_minutes, inferred


def _load_candle_bounds_from_db(
    conn: psycopg.Connection,
    ticker: str,
    period_minutes: int,
) -> tuple[Optional[datetime], Optional[datetime]]:
    min_s, max_s = _fetch_candle_bounds(conn, ticker, period_minutes)
    return _epoch_to_dt(min_s), _epoch_to_dt(max_s)


def _resolve_strike_period_and_bounds(
    conn: psycopg.Connection,
    cfg: ClosedCleanupConfig,
    context: StrikeResolutionContext,
    limits: StrikeResolutionLimits,
) -> tuple[Optional[str], CandleBounds]:
    strike_period_norm, period_minutes, inferred = _resolve_period_minutes(
        cfg,
        context,
        limits,
    )
    first_candle_end = context.first_candle_end
    last_candle_end = context.last_candle_end
    if inferred and period_minutes is not None:
        first_candle_end, last_candle_end = _load_candle_bounds_from_db(
            conn,
            context.ticker,
            period_minutes,
        )
    candle_bounds = CandleBounds(
        period_minutes=period_minutes,
        first_candle_end=first_candle_end,
        last_candle_end=last_candle_end,
    )
    return strike_period_norm, candle_bounds


def _build_missing_market(
    conn: psycopg.Connection,
    cfg: ClosedCleanupConfig,
    row: tuple,
    hour_max: float,
    day_max: float,
) -> MissingMarket:
    open_time = ensure_utc(row[_ROW_OPEN_TIME])
    close_time = ensure_utc(row[_ROW_CLOSE_TIME])
    ticker = row[_ROW_TICKER]
    context = StrikeResolutionContext(
        ticker=ticker,
        strike_period=row[_ROW_STRIKE_PERIOD],
        open_time=open_time,
        close_time=close_time,
        first_candle_end=ensure_utc(row[_ROW_FIRST_CANDLE_END]),
        last_candle_end=ensure_utc(row[_ROW_LAST_CANDLE_END]),
    )
    limits = StrikeResolutionLimits(hour_max=hour_max, day_max=day_max)
    strike_period_norm, candle_bounds = _resolve_strike_period_and_bounds(
        conn,
        cfg,
        context,
        limits,
    )
    return MissingMarket(
        ticker=ticker,
        event_ticker=row[_ROW_EVENT_TICKER],
        series_ticker=row[_ROW_SERIES_TICKER],
        strike_period=strike_period_norm,
        open_time=open_time,
        close_time=close_time,
        candle_bounds=candle_bounds,
    )


def _load_missing_closed_markets(
    conn: psycopg.Connection,
    cfg: ClosedCleanupConfig,
) -> tuple[dict[str, MissingMarket], dict[str, MissingMarket]]:
    hour_max = _env_float("STRIKE_HOUR_MAX_HOURS", 2.0, minimum=0.01)
    day_max = _env_float("STRIKE_DAY_MAX_HOURS", 36.0, minimum=hour_max)
    rows = _fetch_missing_closed_rows(conn, cfg)

    missing_settlement: dict[str, MissingMarket] = {}
    missing_candles: dict[str, MissingMarket] = {}
    for row in rows:
        market = _build_missing_market(conn, cfg, row, hour_max, day_max)
        if _settlement_missing(row):
            missing_settlement[market.ticker] = market
        if _candles_missing(market):
            missing_candles[market.ticker] = market

    return missing_settlement, missing_candles


def _missing_event_tickers(missing_settlement: dict[str, MissingMarket]) -> set[str]:
    return {
        market.event_ticker
        for market in missing_settlement.values()
        if market.event_ticker
    }


def _min_close_ts(missing_settlement: dict[str, MissingMarket]) -> Optional[int]:
    close_times = [m.close_time for m in missing_settlement.values() if m.close_time]
    if not close_times:
        return None
    return int(min(close_times).timestamp())


def _refresh_settlements_for_status(
    conn: psycopg.Connection,
    client,
    context: SettlementRefreshContext,
) -> tuple[int, int, int]:
    params = {"with_nested_markets": True, "status": context.status}
    if context.min_close_ts is not None:
        params["min_close_ts"] = context.min_close_ts
    events_seen = 0
    events_updated = 0
    markets_updated = 0
    for event in iter_events(client, **params):
        events_seen += 1
        event_ticker = (event.get("event_ticker") or event.get("ticker") or "").strip()
        if context.missing_event_tickers and event_ticker not in context.missing_event_tickers:
            continue
        upsert_event(conn, event)
        events_updated += 1
        for market in (event.get("markets") or []):
            ticker = market.get("ticker")
            if ticker in context.missing_settlement:
                upsert_market(conn, market)
                maybe_upsert_active_market_from_market(conn, market)
                markets_updated += 1
                context.missing_settlement.pop(ticker, None)
        if not context.missing_settlement:
            break
    return events_updated, markets_updated, events_seen


def _refresh_missing_settlements(
    conn: psycopg.Connection,
    client,
    cfg: ClosedCleanupConfig,
    missing_settlement: dict[str, MissingMarket],
) -> tuple[int, int]:
    if not missing_settlement:
        return 0, 0
    missing_event_tickers = _missing_event_tickers(missing_settlement)
    min_close_ts = _min_close_ts(missing_settlement)
    statuses = tuple(s for s in cfg.event_statuses if s) or ("closed", "settled")
    logger.info(
        "closed_cleanup_settlement: statuses=%s min_close_ts=%s markets=%d",
        ",".join(statuses),
        min_close_ts,
        len(missing_settlement),
    )

    events_updated = 0
    markets_updated = 0
    for status in statuses:
        refresh_context = SettlementRefreshContext(
            status=status,
            min_close_ts=min_close_ts,
            missing_settlement=missing_settlement,
            missing_event_tickers=missing_event_tickers,
        )
        added_events, added_markets, events_seen = _refresh_settlements_for_status(
            conn,
            client,
            refresh_context,
        )
        events_updated += added_events
        markets_updated += added_markets
        logger.info(
            "closed_cleanup_settlement: status=%s events_seen=%d remaining=%d",
            status,
            events_seen,
            len(missing_settlement),
        )
        if not missing_settlement:
            break
    return events_updated, markets_updated


def _build_candle_ranges(
    market: MissingMarket,
    cfg: ClosedCleanupConfig,
) -> list[tuple[int, int]]:
    bounds = market.candle_bounds
    if bounds.period_minutes is None:
        return []
    start_s, end_s, _is_closed = _market_time_window(
        market.close_time,
        market.open_time,
        cfg.lookback_hours,
    )
    if start_s >= end_s:
        return []
    period_seconds = bounds.period_minutes * 60
    existing_min_s = _dt_to_epoch(bounds.first_candle_end)
    existing_max_s = _dt_to_epoch(bounds.last_candle_end)
    return _build_backfill_ranges(
        BackfillRangeRequest(
            start_s,
            end_s,
            existing_min_s,
            existing_max_s,
            period_seconds,
            False,
        ),
    )


def _backfill_market_candles(
    conn: psycopg.Connection,
    client,
    cfg: ClosedCleanupConfig,
    market: MissingMarket,
    chunk_seconds: int,
) -> tuple[int, int]:
    if not market.series_ticker or not market.ticker:
        return 0, 0
    ranges = _build_candle_ranges(market, cfg)
    if not ranges:
        return 0, 0
    logger.debug(
        "closed_cleanup_candles: ticker=%s ranges=%d",
        market.ticker,
        len(ranges),
    )
    candles_inserted = 0
    with conn.cursor() as cur:
        for range_start, range_end in ranges:
            for window_start, window_end in _iter_time_chunks(
                range_start,
                range_end,
                chunk_seconds,
            ):
                response = get_market_candlesticks(
                    client,
                    market.series_ticker,
                    market.ticker,
                    start_ts=window_start,
                    end_ts=window_end,
                    period_interval_minutes=market.candle_bounds.period_minutes,
                )
                candles = response.get("candlesticks") if isinstance(response, dict) else None
                for candle in candles or []:
                    _insert_candle(
                        cur,
                        market.ticker,
                        market.candle_bounds.period_minutes,
                        candle,
                    )
                    candles_inserted += 1
    conn.commit()
    return 1, candles_inserted


def _backfill_missing_candles(
    conn: psycopg.Connection,
    client,
    cfg: ClosedCleanupConfig,
    missing_candles: dict[str, MissingMarket],
) -> tuple[int, int]:
    if not missing_candles:
        return 0, 0
    logger.info("closed_cleanup_candles: markets=%d", len(missing_candles))
    chunk_seconds = max(cfg.lookback_hours, 1) * 3600
    markets_touched = 0
    candles_inserted = 0
    for market in missing_candles.values():
        touched, inserted = _backfill_market_candles(
            conn,
            client,
            cfg,
            market,
            chunk_seconds,
        )
        markets_touched += touched
        candles_inserted += inserted
    logger.info(
        "closed_cleanup_candles: markets_touched=%d candles_inserted=%d",
        markets_touched,
        candles_inserted,
    )
    return markets_touched, candles_inserted


def closed_cleanup_pass(
    conn: psycopg.Connection,
    client,
    cfg: ClosedCleanupConfig,
) -> tuple[int, int, int]:
    """Refresh closed markets missing settlement data or candlesticks."""
    missing_settlement, missing_candles = _load_missing_closed_markets(conn, cfg)
    logger.info(
        "closed_cleanup_pass: missing_settlement=%d missing_candles=%d",
        len(missing_settlement),
        len(missing_candles),
    )
    if not missing_settlement and not missing_candles:
        cleaned = cleanup_active_markets(conn, grace_minutes=cfg.grace_minutes)
        logger.info("closed_cleanup_pass: no missing data; active_removed=%d", cleaned)
        return 0, 0, 0

    events_updated = 0
    markets_updated = 0
    if missing_settlement:
        events_updated, markets_updated = _refresh_missing_settlements(
            conn,
            client,
            cfg,
            missing_settlement,
        )
    candle_markets, candles_inserted = _backfill_missing_candles(
        conn,
        client,
        cfg,
        missing_candles,
    )
    cleaned = cleanup_active_markets(conn, grace_minutes=cfg.grace_minutes)
    logger.info(
        "closed_cleanup_pass: settlement_events=%d settlement_markets=%d candle_markets=%d "
        "candles=%d active_removed=%d",
        events_updated,
        markets_updated,
        candle_markets,
        candles_inserted,
        cleaned,
    )
    return events_updated, markets_updated, candles_inserted


def run_once() -> None:
    """Run a single closed-cleanup pass using environment settings."""
    settings = load_settings()
    configure_logging()
    client, conn = open_client_and_conn(
        settings,
        schema_path_override=schema_path(__file__),
    )
    cfg = build_closed_cleanup_config(settings)
    closed_cleanup_pass(conn, client, cfg)


if __name__ == "__main__":
    run_once()
