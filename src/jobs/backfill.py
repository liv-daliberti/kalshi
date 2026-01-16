"""Backfill pass for market candlesticks."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterator, Optional

import psycopg  # pylint: disable=import-error

from src.jobs.backfill_config import BackfillConfig
from src.db.db import (
    dec,
    get_state,
    maybe_upsert_active_market_from_market,
    parse_ts_iso,
    set_state,
    upsert_event,
    upsert_market,
)
from src.jobs.event_filter import EventScanStats, accept_event
from src.core.guardrails import assert_service_role
from src.core.loop_utils import log_metric as _log_metric
from src.kalshi.kalshi_sdk import get_market_candlesticks, iter_events
from src.jobs.job_utils import log_item_error as _log_item_error
from src.queue.work_queue import QueueConfig, QueuePublisher, enqueue_job

logger = logging.getLogger(__name__)


def _safe_rollback(conn: psycopg.Connection) -> None:
    try:
        conn.rollback()
    except Exception:  # pylint: disable=broad-exception-caught
        logger.warning("backfill_pass: rollback failed")


@dataclass(frozen=True)
class MarketContext:
    """Context for a single market backfill request.

    :ivar series_ticker: Series ticker for the event.
    :ivar market: Market payload.
    :ivar strike_period: Strike period for the market.
    """

    series_ticker: str
    market: dict
    strike_period: str


@dataclass(frozen=True)
class BackfillRangeRequest:
    """Inputs for computing backfill ranges."""

    target_start_s: int
    target_end_s: int
    existing_min_s: Optional[int]
    existing_max_s: Optional[int]
    period_seconds: int
    force_full: bool


@dataclass(frozen=True)
class MarketBackfillWindow:
    """Resolved backfill window and cursor state for a market."""

    market_ticker: str
    period_minutes: int
    start_s: int
    end_s: int
    state_cursor_s: Optional[int]
    last_cursor_s: Optional[int]
    chunk_seconds: int


@dataclass(frozen=True)
class EventBackfillContext:
    """Context for backfilling a single event."""

    event: dict
    strike_period: str
    queue_cfg: Optional[QueueConfig]
    publisher: Optional[QueuePublisher]


@dataclass(frozen=True)
class BackfillScanContext:
    """Context for scanning events during a backfill pass."""

    conn: psycopg.Connection
    client: object
    cfg: BackfillConfig
    queue_cfg: Optional[QueueConfig]
    publisher: Optional[QueuePublisher]
    last_min_close_ts: int


@dataclass
class BackfillCounts:
    """Counters for a backfill pass."""

    events: int = 0
    markets: int = 0
    candles: int = 0
    queued: int = 0


def _now_s() -> int:
    """Return current UTC time in seconds.

    :return: Epoch seconds in UTC.
    :rtype: int
    """
    return int(datetime.now(timezone.utc).timestamp())


def _market_is_closed(market: dict) -> bool:
    """Return True when a market is closed/settled or past its close time."""
    status = (market.get("status") or "").lower()
    if status in {"closed", "settled", "finalized"}:
        return True
    close_dt = parse_ts_iso(market.get("close_time"))
    if close_dt is None:
        return False
    return close_dt <= datetime.now(timezone.utc)


def _backfill_state_key(market_ticker: str, period_minutes: int) -> str:
    return f"backfill_last_ts:{market_ticker}:{period_minutes}"


def _parse_state_epoch(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        parsed = parse_ts_iso(raw)
        if parsed is None:
            return None
        return int(parsed.timestamp())


def _load_backfill_cursor(
    conn: psycopg.Connection,
    market_ticker: str,
    period_minutes: int,
) -> Optional[int]:
    raw = get_state(conn, _backfill_state_key(market_ticker, period_minutes))
    return _parse_state_epoch(raw)


def _store_backfill_cursor(
    conn: psycopg.Connection,
    market_ticker: str,
    period_minutes: int,
    cursor_s: int,
) -> None:
    set_state(
        conn,
        _backfill_state_key(market_ticker, period_minutes),
        str(cursor_s),
    )


def _interval_minutes_for_strike(
    strike_period: str,
    minutes_hour: int,
    minutes_day: int,
) -> int:
    """Pick candle resolution based on the strike period.

    :param strike_period: Strike period string.
    :type strike_period: str
    :param minutes_hour: Resolution for hourly markets.
    :type minutes_hour: int
    :param minutes_day: Resolution for daily markets.
    :type minutes_day: int
    :return: Candlestick interval in minutes.
    :rtype: int
    """
    strike_period = strike_period.lower()
    return minutes_hour if strike_period == "hour" else minutes_day


def _iter_events(
    client,
    event_statuses: tuple[str, ...],
    last_min_close_ts: int,
) -> Iterator[tuple[dict, str]]:
    """Yield events for the configured statuses.

    :param client: Kalshi REST client.
    :type client: Any
    :param event_statuses: Event statuses to scan.
    :type event_statuses: tuple[str, ...]
    :param last_min_close_ts: Minimum close timestamp to scan from.
    :type last_min_close_ts: int
    :return: Iterable of (event, status) pairs.
    :rtype: collections.abc.Iterator[tuple[dict, str]]
    """
    statuses = event_statuses or ("closed", "settled")
    for status in statuses:
        try:
            for event in iter_events(
                client,
                status=status,
                with_nested_markets=True,
                min_close_ts=last_min_close_ts,
            ):
                yield event, status
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("backfill_pass: events query failed (status=%s)", status)
            continue


def _market_time_window(
    close_dt: Optional[datetime],
    open_dt: Optional[datetime],
    lookback_hours: int,
) -> tuple[int, int, bool]:
    """Compute a backfill window in epoch seconds.

    :param close_dt: Market close time.
    :type close_dt: datetime.datetime | None
    :param open_dt: Market open time.
    :type open_dt: datetime.datetime | None
    :param lookback_hours: Chunk size in hours for API calls.
    :type lookback_hours: int
    :return: (start_ts, end_ts, is_closed) in epoch seconds.
    :rtype: tuple[int, int, bool]
    """
    now_dt = datetime.now(timezone.utc)
    is_closed = close_dt is not None and close_dt <= now_dt
    end_dt = close_dt if close_dt and close_dt <= now_dt else now_dt
    if open_dt is not None:
        start_dt = open_dt
    elif close_dt is not None:
        start_dt = close_dt - timedelta(hours=lookback_hours)
    else:
        start_dt = end_dt - timedelta(hours=lookback_hours)
    start_dt = min(start_dt, end_dt)
    return int(start_dt.timestamp()), int(end_dt.timestamp()), is_closed


def _dt_to_epoch(value: Optional[datetime]) -> Optional[int]:
    """Convert a datetime to epoch seconds."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp())


def _fetch_candle_bounds(
    conn: psycopg.Connection,
    market_ticker: str,
    period_minutes: int,
) -> tuple[Optional[int], Optional[int]]:
    """Fetch min/max candlestick end timestamps for a market."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT end_period_ts
            FROM market_candles
            WHERE market_ticker = %s AND period_interval_minutes = %s
            ORDER BY end_period_ts ASC
            LIMIT 1
            """,
            (market_ticker, period_minutes),
        )
        row = cur.fetchone()
        min_dt = row[0] if row else None
        cur.execute(
            """
            SELECT end_period_ts
            FROM market_candles
            WHERE market_ticker = %s AND period_interval_minutes = %s
            ORDER BY end_period_ts DESC
            LIMIT 1
            """,
            (market_ticker, period_minutes),
        )
        row = cur.fetchone()
        max_dt = row[0] if row else None
    return _dt_to_epoch(min_dt), _dt_to_epoch(max_dt)


def _iter_time_chunks(
    start_s: int,
    end_s: int,
    chunk_seconds: int,
) -> Iterator[tuple[int, int]]:
    """Yield (start, end) ranges split into chunk_seconds windows."""
    if end_s <= start_s:
        return
    if chunk_seconds <= 0:
        yield start_s, end_s
        return
    window_start = start_s
    while window_start < end_s:
        window_end = min(end_s, window_start + chunk_seconds)
        yield window_start, window_end
        if window_end == end_s:
            break
        window_start = window_end


def _build_backfill_ranges(request: BackfillRangeRequest) -> list[tuple[int, int]]:
    """Compute backfill ranges based on target window and existing data."""
    target_start_s = request.target_start_s
    target_end_s = request.target_end_s
    existing_min_s = request.existing_min_s
    existing_max_s = request.existing_max_s
    period_seconds = request.period_seconds
    force_full = request.force_full
    if target_end_s <= target_start_s:
        return []
    if force_full or existing_min_s is None or existing_max_s is None:
        return [(target_start_s, target_end_s)]
    if existing_max_s < target_start_s or existing_min_s > target_end_s:
        return [(target_start_s, target_end_s)]

    ranges: list[tuple[int, int]] = []
    if existing_min_s > target_start_s + period_seconds:
        ranges.append((target_start_s, existing_min_s))
    if existing_max_s < target_end_s - period_seconds:
        overlap_start = max(existing_max_s - period_seconds, target_start_s)
        ranges.append((overlap_start, target_end_s))
    return ranges


def _insert_candle(cur, market_ticker: str, period_minutes: int, candle: dict) -> None:
    """Insert a single candle row if not already present.

    :param cur: Database cursor.
    :type cur: psycopg.Cursor
    :param market_ticker: Market ticker symbol.
    :type market_ticker: str
    :param period_minutes: Candlestick interval in minutes.
    :type period_minutes: int
    :param candle: Candlestick payload.
    :type candle: dict
    """
    end_period_ts = int(candle["end_period_ts"])
    end_dt = datetime.fromtimestamp(end_period_ts, tz=timezone.utc)
    start_dt = datetime.fromtimestamp(
        end_period_ts - period_minutes * 60 + 1,
        tz=timezone.utc,
    )

    price = candle.get("price")
    if not isinstance(price, dict):
        price = {}

    def _price_dollars(key: str) -> Optional[Decimal]:
        value = price.get(f"{key}_dollars")
        if value is None:
            value = price.get(key)
        if value is None:
            value = candle.get(f"{key}_dollars")
        if value is None:
            value = candle.get(key)
        dec_value = dec(value)
        if dec_value is None:
            return None
        if dec_value > 1:
            return dec_value / Decimal(100)
        return dec_value

    open_d = _price_dollars("open")
    high_d = _price_dollars("high")
    low_d = _price_dollars("low")
    close_d = _price_dollars("close")

    cur.execute(
        """
        INSERT INTO market_candles(
          market_ticker, period_interval_minutes, end_period_ts, start_period_ts,
          open, high, low, close, volume, open_interest, raw
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
        """,
        (
            market_ticker,
            period_minutes,
            end_dt,
            start_dt,
            open_d,
            high_d,
            low_d,
            close_d,
            candle.get("volume"),
            candle.get("open_interest"),
            json.dumps(candle),
        ),
    )


def _resolve_last_cursor(
    state_cursor_s: Optional[int],
    existing_max_s: Optional[int],
    force_full: bool,
) -> Optional[int]:
    """Resolve the last cursor value used for incremental backfills."""
    if force_full:
        return None
    if state_cursor_s is not None:
        return state_cursor_s
    return existing_max_s


def _resolve_market_bounds(
    context: MarketContext,
    cfg: BackfillConfig,
) -> Optional[tuple[str, int, int, int]]:
    market_ticker = context.market.get("ticker")
    if not market_ticker:
        return None
    period_minutes = _interval_minutes_for_strike(
        context.strike_period,
        cfg.minutes_hour,
        cfg.minutes_day,
    )
    close_dt = parse_ts_iso(context.market.get("close_time"))
    open_dt = parse_ts_iso(context.market.get("open_time"))
    start_s, end_s, _ = _market_time_window(
        close_dt,
        open_dt,
        cfg.lookback_hours,
    )
    return market_ticker, period_minutes, start_s, end_s


def _apply_cursor_window(
    conn: psycopg.Connection,
    market_ticker: str,
    period_minutes: int,
    start_s: int,
    force_full: bool,
) -> tuple[int, Optional[int], Optional[int]]:
    _, existing_max_s = _fetch_candle_bounds(
        conn,
        market_ticker,
        period_minutes,
    )
    state_cursor_s = _load_backfill_cursor(conn, market_ticker, period_minutes)
    last_cursor_s = _resolve_last_cursor(state_cursor_s, existing_max_s, force_full)
    if last_cursor_s is not None and not force_full:
        start_s = max(start_s, last_cursor_s + period_minutes * 60)
    return start_s, state_cursor_s, last_cursor_s


def _build_market_backfill_window(
    conn: psycopg.Connection,
    cfg: BackfillConfig,
    context: MarketContext,
    force_full: bool,
) -> Optional[MarketBackfillWindow]:
    """Resolve the time window and cursor state for a market backfill."""
    bounds = _resolve_market_bounds(context, cfg)
    if bounds is None:
        return None
    market_ticker, period_minutes, start_s, end_s = bounds
    start_s, state_cursor_s, last_cursor_s = _apply_cursor_window(
        conn,
        market_ticker,
        period_minutes,
        start_s,
        force_full,
    )
    chunk_seconds = max(cfg.lookback_hours, 1) * 3600
    return MarketBackfillWindow(
        market_ticker=market_ticker,
        period_minutes=period_minutes,
        start_s=start_s,
        end_s=end_s,
        state_cursor_s=state_cursor_s,
        last_cursor_s=last_cursor_s,
        chunk_seconds=chunk_seconds,
    )


def _store_cursor_if_needed(
    conn: psycopg.Connection,
    window: MarketBackfillWindow,
) -> None:
    """Persist cursor state when no new candle data is fetched."""
    if window.last_cursor_s is None:
        return
    if window.last_cursor_s == window.state_cursor_s:
        return
    _store_backfill_cursor(
        conn,
        window.market_ticker,
        window.period_minutes,
        window.last_cursor_s,
    )


def _fetch_and_insert_candles(
    conn: psycopg.Connection,
    client,
    series_ticker: str,
    window: MarketBackfillWindow,
) -> tuple[int, Optional[int]]:
    """Fetch candlesticks and insert them into the database."""
    candles_n = 0
    max_seen_end_s = None
    with conn.cursor() as cur:
        for window_start, window_end in _iter_time_chunks(
            window.start_s,
            window.end_s,
            window.chunk_seconds,
        ):
            response = get_market_candlesticks(
                client,
                series_ticker,
                window.market_ticker,
                start_ts=window_start,
                end_ts=window_end,
                period_interval_minutes=window.period_minutes,
            )
            candles = response.get("candlesticks") or []
            for candle in candles:
                _insert_candle(cur, window.market_ticker, window.period_minutes, candle)
                candles_n += 1
                candle_end_s = int(candle.get("end_period_ts") or 0)
                if candle_end_s:
                    if max_seen_end_s is None or candle_end_s > max_seen_end_s:
                        max_seen_end_s = candle_end_s
    return candles_n, max_seen_end_s


def _update_backfill_cursor(
    conn: psycopg.Connection,
    window: MarketBackfillWindow,
    max_seen_end_s: Optional[int],
) -> None:
    """Update the cursor state after a backfill pass."""
    new_cursor_s = window.last_cursor_s
    if max_seen_end_s is not None:
        if new_cursor_s is None:
            new_cursor_s = max_seen_end_s
        else:
            new_cursor_s = max(new_cursor_s, max_seen_end_s)
    if new_cursor_s is not None and new_cursor_s != window.state_cursor_s:
        _store_backfill_cursor(
            conn,
            window.market_ticker,
            window.period_minutes,
            new_cursor_s,
        )


def _backfill_market(
    conn: psycopg.Connection,
    client,
    cfg: BackfillConfig,
    context: MarketContext,
    force_full: bool = False,
) -> int:
    """Backfill candlesticks for a single market.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param client: Kalshi REST client.
    :type client: Any
    :param cfg: Backfill configuration.
    :type cfg: BackfillConfig
    :param context: Market context payload.
    :type context: MarketContext
    :param force_full: Ignore stored cursors and backfill the full window.
    :type force_full: bool
    :return: Number of candles inserted.
    :rtype: int
    """
    window = _build_market_backfill_window(conn, cfg, context, force_full)
    if window is None:
        return 0
    if window.start_s >= window.end_s:
        _store_cursor_if_needed(conn, window)
        return 0

    candles_n, max_seen_end_s = _fetch_and_insert_candles(
        conn,
        client,
        context.series_ticker,
        window,
    )
    conn.commit()
    _update_backfill_cursor(conn, window, max_seen_end_s)
    return candles_n


def backfill_market(
    conn: psycopg.Connection,
    client,
    cfg: BackfillConfig,
    context: MarketContext,
    force_full: bool = False,
) -> int:
    """Backfill candlesticks for a single market payload."""
    return _backfill_market(conn, client, cfg, context, force_full=force_full)


def _queue_backfill_market(
    conn: psycopg.Connection,
    queue_cfg: QueueConfig,
    publisher: Optional[QueuePublisher],
    context: MarketContext,
) -> int:
    """Enqueue a market backfill job and publish it when possible."""
    payload = {
        "series_ticker": context.series_ticker,
        "strike_period": context.strike_period,
        "market": {
            "ticker": context.market.get("ticker"),
            "open_time": context.market.get("open_time"),
            "close_time": context.market.get("close_time"),
        },
    }
    job_id = enqueue_job(
        conn,
        "backfill_market",
        payload,
        max_attempts=queue_cfg.timing.max_attempts,
    )
    if publisher is not None:
        try:
            publisher.publish(job_id, job_type="backfill_market")
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("RabbitMQ publish failed for job %s", job_id)
            publisher.disable()
    return 1


def _backfill_event(
    conn: psycopg.Connection,
    client,
    cfg: BackfillConfig,
    context: EventBackfillContext,
) -> tuple[int, int, int]:
    """Backfill markets for an event and return (markets, candles).

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param client: Kalshi REST client.
    :type client: Any
    :param cfg: Backfill configuration.
    :type cfg: BackfillConfig
    :param context: Event backfill context.
    :type context: EventBackfillContext
    :return: (markets, candles, queued) counts for this event.
    :rtype: tuple[int, int, int]
    """
    event = context.event
    try:
        upsert_event(conn, event)
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception(
            "backfill_pass: event upsert failed event_ticker=%s",
            event.get("event_ticker"),
        )
        _log_item_error(
            logger,
            "backfill.item_error",
            kind="event_upsert",
            event_ticker=event.get("event_ticker"),
            market_ticker=None,
            rollback=_safe_rollback,
            conn=conn,
        )
        return 0, 0, 0

    series = event.get("series_ticker")
    if not series:
        return 0, 0, 0

    counts = BackfillCounts()
    queue_cfg = context.queue_cfg
    queue_enabled = queue_cfg is not None and queue_cfg.enabled
    for market in (event.get("markets") or []):
        try:
            upsert_market(conn, market)
            maybe_upsert_active_market_from_market(conn, market)
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception(
                "backfill_pass: market upsert failed event=%s market=%s",
                event.get("event_ticker"),
                market.get("ticker"),
            )
            _log_item_error(
                logger,
                "backfill.item_error",
                kind="market_upsert",
                event_ticker=event.get("event_ticker"),
                market_ticker=market.get("ticker"),
                rollback=_safe_rollback,
                conn=conn,
            )
            continue
        counts.markets += 1
        market_ctx = MarketContext(series, market, context.strike_period)
        if queue_enabled:
            enqueue = True
            try:
                window = _build_market_backfill_window(
                    conn,
                    cfg,
                    market_ctx,
                    force_full=False,
                )
            except Exception:  # pylint: disable=broad-exception-caught
                logger.exception(
                    "backfill_pass: market window check failed; enqueueing anyway"
                )
            else:
                if window is None or window.start_s >= window.end_s:
                    enqueue = False
            if enqueue:
                try:
                    counts.queued += _queue_backfill_market(
                        conn,
                        queue_cfg,
                        context.publisher,
                        market_ctx,
                    )
                except Exception:  # pylint: disable=broad-exception-caught
                    logger.exception(
                        "backfill_pass: enqueue failed event=%s market=%s",
                        event.get("event_ticker"),
                        market.get("ticker"),
                    )
                    _log_metric(
                        logger,
                        "backfill.item_error",
                        kind="enqueue",
                        event_ticker=event.get("event_ticker"),
                        market_ticker=market.get("ticker"),
                    )
                    _safe_rollback(conn)
        else:
            try:
                counts.candles += _backfill_market(conn, client, cfg, market_ctx)
            except Exception:  # pylint: disable=broad-exception-caught
                logger.exception(
                    "backfill_pass: market backfill failed event=%s market=%s",
                    event.get("event_ticker"),
                    market.get("ticker"),
                )
                _log_metric(
                    logger,
                    "backfill.item_error",
                    kind="market_backfill",
                    event_ticker=event.get("event_ticker"),
                    market_ticker=market.get("ticker"),
                )
                _safe_rollback(conn)
    return counts.markets, counts.candles, counts.queued


def _load_min_close_ts(conn: psycopg.Connection, now_s: int) -> int:
    """Load the minimum close-time cursor for event scans."""
    default_cursor = str(now_s - 7 * 86400)  # start a week back on first run
    return int(get_state(conn, "last_min_close_ts", default_cursor) or default_cursor)


def backfill_pass(
    conn: psycopg.Connection,
    client,
    cfg: BackfillConfig,
    queue_cfg: Optional[QueueConfig] = None,
    publisher: Optional[QueuePublisher] = None,
) -> tuple[int, int, int]:
    """Backfill candlesticks for markets matching configured statuses.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param client: Kalshi REST client.
    :type client: Any
    :param cfg: Backfill configuration.
    :type cfg: BackfillConfig
    :param queue_cfg: Optional queue configuration for enqueuing work.
    :type queue_cfg: QueueConfig | None
    :param publisher: Optional RabbitMQ publisher.
    :type publisher: QueuePublisher | None
    :return: Counts of events, markets, and candles inserted.
    :rtype: tuple[int, int, int]
    """
    assert_service_role("rest", "backfill_pass")
    now_s = _now_s()
    last_min_close_ts = _load_min_close_ts(conn, now_s)
    scan_ctx = BackfillScanContext(
        conn=conn,
        client=client,
        cfg=cfg,
        queue_cfg=queue_cfg,
        publisher=publisher,
        last_min_close_ts=last_min_close_ts,
    )
    counts, stats = _scan_backfill_events(scan_ctx)
    queue_enabled = queue_cfg is not None and queue_cfg.enabled

    # Advance cursor: next run looks for events whose markets close after this time
    # :contentReference[oaicite:35]{index=35}
    set_state(conn, "last_min_close_ts", str(now_s))
    if queue_enabled:
        logger.info(
            "backfill_pass: events=%d markets=%d queued=%d min_close_ts->%d",
            counts.events,
            counts.markets,
            counts.queued,
            now_s,
        )
    else:
        logger.info(
            "backfill_pass: events=%d markets=%d candles=%d min_close_ts->%d",
            counts.events,
            counts.markets,
            counts.candles,
            now_s,
        )
    logger.debug(
        "backfill_debug: raw_events=%d unique_events=%d filtered=%d dupes=%d statuses=%s",
        stats.raw_events,
        len(stats.seen_events),
        stats.filtered_events,
        stats.dup_events,
        ",".join(s for s in cfg.event_statuses if s) or "none",
    )
    logger.debug(
        "backfill_debug: strike_periods=%s allowed=%s",
        stats.summarize_strike_counts(),
        ",".join(cfg.strike_periods) or "none",
    )
    logger.debug(
        "backfill_debug: inferred=%d inferred_strike_periods=%s",
        stats.inferred_events,
        stats.summarize_inferred_counts(),
    )
    return counts.events, counts.markets, counts.candles


def _scan_backfill_events(
    scan_ctx: BackfillScanContext,
) -> tuple[BackfillCounts, EventScanStats]:
    counts = BackfillCounts()
    stats = EventScanStats()
    # Scan configured statuses (some markets transition across statuses).
    for event, _status in _iter_events(
        scan_ctx.client,
        scan_ctx.cfg.event_statuses,
        scan_ctx.last_min_close_ts,
    ):
        strike_period = accept_event(event, scan_ctx.cfg.strike_periods, stats)
        if strike_period is None:
            continue
        counts.events += 1
        event_ctx = EventBackfillContext(
            event=event,
            strike_period=strike_period,
            queue_cfg=scan_ctx.queue_cfg,
            publisher=scan_ctx.publisher,
        )
        try:
            added_markets, added_candles, added_queued = _backfill_event(
                scan_ctx.conn,
                scan_ctx.client,
                scan_ctx.cfg,
                event_ctx,
            )
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception(
                "backfill_pass: event processing failed event_ticker=%s",
                event.get("event_ticker"),
            )
            _log_metric(
                logger,
                "backfill.item_error",
                kind="event_process",
                event_ticker=event.get("event_ticker"),
            )
            _safe_rollback(scan_ctx.conn)
            continue
        else:
            counts.markets += added_markets
            counts.candles += added_candles
            counts.queued += added_queued
    return counts, stats
