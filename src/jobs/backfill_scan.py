"""Backfill event scanning and enqueue logic."""

from __future__ import annotations

import functools
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, Optional

import psycopg  # pylint: disable=import-error

from .backfill_config import BackfillConfig
from .backfill_market import (
    MarketContext,
    _backfill_market,
    _build_market_backfill_window,
    _queue_backfill_market,
)
from .event_filter import EventScanStats, accept_event
from ..core.env_utils import _env_int
from ..core.guardrails import assert_service_role
from ..core.loop_utils import log_metric as _log_metric
from ..db.db import (
    get_state,
    maybe_upsert_active_market_from_market,
    set_state,
    upsert_event,
    upsert_market,
)
from ..kalshi.kalshi_sdk import iter_events
from .job_utils import (
    handle_event_upsert_error as _handle_event_upsert_error,
    handle_market_upsert_error as _handle_market_upsert_error,
    is_connection_error as _is_connection_error,
    safe_rollback as _safe_rollback_fn,
)
from ..queue.work_queue import QueueConfig, QueuePublisher, job_type_where_clause

logger = logging.getLogger(__name__)
_safe_rollback = functools.partial(
    _safe_rollback_fn,
    logger=logger,
    label="backfill_pass",
)


@dataclass(frozen=True)
class EventBackfillContext:
    """Context for backfilling a single event."""

    event: dict
    strike_period: str
    queue_cfg: Optional[QueueConfig]
    publisher: Optional[QueuePublisher]
    allow_enqueue: bool = True


@dataclass(frozen=True)
class BackfillScanContext:
    """Context for scanning events during a backfill pass."""

    conn: psycopg.Connection
    client: object
    cfg: BackfillConfig
    queue_cfg: Optional[QueueConfig]
    publisher: Optional[QueuePublisher]
    last_min_close_ts: int
    allow_enqueue: bool = True


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


def _queue_backpressure_limit() -> int:
    """Return the max pending jobs allowed before skipping enqueues."""
    return _env_int("WORK_QUEUE_BACKPRESSURE_MAX_PENDING", 0, minimum=0)


def _queue_pending_count(
    conn: psycopg.Connection,
    queue_cfg: QueueConfig,
) -> int:
    where_clause, params = job_type_where_clause(queue_cfg.job_types)
    query = f"""
        SELECT COUNT(*) FILTER (
          WHERE status = 'pending' AND available_at <= NOW()
        ) AS pending
        FROM work_queue
        {where_clause}
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    return int(row[0] or 0)


def _allow_enqueue(
    conn: psycopg.Connection,
    queue_cfg: Optional[QueueConfig],
) -> tuple[bool, int, int]:
    """Check queue depth and decide whether enqueues should proceed."""
    max_pending = _queue_backpressure_limit()
    if queue_cfg is None or not queue_cfg.enabled or max_pending <= 0:
        return True, 0, max_pending
    try:
        pending = _queue_pending_count(conn, queue_cfg)
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("backfill_pass: queue pending count failed; skipping backpressure")
        return True, 0, max_pending
    return pending <= max_pending, pending, max_pending


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


def _upsert_event_or_skip(conn: psycopg.Connection, event: dict) -> bool:
    event_ticker = event.get("event_ticker") if isinstance(event, dict) else None
    try:
        upsert_event(conn, event)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if _handle_event_upsert_error(
            logger=logger,
            metric_name="backfill.item_error",
            event_ticker=event_ticker,
            rollback=_safe_rollback,
            conn=conn,
            exc=exc,
            source="backfill_pass",
        ):
            raise
        return False
    return True


def _upsert_market_or_skip(
    conn: psycopg.Connection,
    event_ticker: str | None,
    market: dict,
) -> bool:
    try:
        upsert_market(conn, market)
        maybe_upsert_active_market_from_market(conn, market)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if _handle_market_upsert_error(
            logger=logger,
            metric_name="backfill.item_error",
            event_ticker=event_ticker,
            market_ticker=market.get("ticker"),
            rollback=_safe_rollback,
            conn=conn,
            exc=exc,
            source="backfill_pass",
        ):
            raise
        return False
    return True


def _should_enqueue_market(
    conn: psycopg.Connection,
    cfg: BackfillConfig,
    market_ctx: MarketContext,
) -> bool:
    try:
        window = _build_market_backfill_window(
            conn,
            cfg,
            market_ctx,
            force_full=False,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("backfill_pass: market window check failed; enqueueing anyway")
        if _is_connection_error(conn, exc):
            raise
        return True
    if window is None or window.start_s >= window.end_s:
        return False
    return True


def _enqueue_market_backfill(
    conn: psycopg.Connection,
    event_ctx: EventBackfillContext,
    market_ctx: MarketContext,
) -> int:
    try:
        return _queue_backfill_market(
            conn,
            event_ctx.queue_cfg,
            event_ctx.publisher,
            market_ctx,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception(
            "backfill_pass: enqueue failed event=%s market=%s",
            event_ctx.event.get("event_ticker"),
            market_ctx.market.get("ticker"),
        )
        _log_metric(
            logger,
            "backfill.item_error",
            kind="enqueue",
            event_ticker=event_ctx.event.get("event_ticker"),
            market_ticker=market_ctx.market.get("ticker"),
        )
        _safe_rollback(conn)
        if _is_connection_error(conn, exc):
            raise
    return 0


def _backfill_market_with_errors(
    conn: psycopg.Connection,
    client,
    cfg: BackfillConfig,
    event_ctx: EventBackfillContext,
    market_ctx: MarketContext,
) -> int:
    try:
        return _backfill_market(conn, client, cfg, market_ctx)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception(
            "backfill_pass: market backfill failed event=%s market=%s",
            event_ctx.event.get("event_ticker"),
            market_ctx.market.get("ticker"),
        )
        _log_metric(
            logger,
            "backfill.item_error",
            kind="market_backfill",
            event_ticker=event_ctx.event.get("event_ticker"),
            market_ticker=market_ctx.market.get("ticker"),
        )
        _safe_rollback(conn)
        if _is_connection_error(conn, exc):
            raise
    return 0


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
    event_ticker = event.get("event_ticker")
    if not _upsert_event_or_skip(conn, event):
        return 0, 0, 0
    series = event.get("series_ticker")
    if not series:
        return 0, 0, 0

    counts = BackfillCounts()
    queue_cfg = context.queue_cfg
    queue_enabled = queue_cfg is not None and queue_cfg.enabled
    for market in (event.get("markets") or []):
        if not _upsert_market_or_skip(conn, event_ticker, market):
            continue
        counts.markets += 1
        market_ctx = MarketContext(series, market, context.strike_period)
        if queue_enabled and context.allow_enqueue:
            if _should_enqueue_market(conn, cfg, market_ctx):
                counts.queued += _enqueue_market_backfill(
                    conn,
                    context,
                    market_ctx,
                )
        elif not queue_enabled:
            counts.candles += _backfill_market_with_errors(
                conn,
                client,
                cfg,
                context,
                market_ctx,
            )
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
    allow_enqueue, pending, max_pending = _allow_enqueue(conn, queue_cfg)
    if queue_cfg is not None and queue_cfg.enabled and not allow_enqueue:
        logger.warning(
            "backfill_pass: queue backpressure active pending=%d max_pending=%d; skipping enqueues",
            pending,
            max_pending,
        )
    scan_ctx = BackfillScanContext(
        conn=conn,
        client=client,
        cfg=cfg,
        queue_cfg=queue_cfg,
        publisher=publisher,
        last_min_close_ts=last_min_close_ts,
        allow_enqueue=allow_enqueue,
    )
    counts, stats = _scan_backfill_events(scan_ctx)
    queue_enabled = queue_cfg is not None and queue_cfg.enabled

    # Advance cursor only when we are not backpressured.
    next_min_close_ts = now_s
    if queue_enabled and not allow_enqueue:
        next_min_close_ts = last_min_close_ts
    set_state(conn, "last_min_close_ts", str(next_min_close_ts))
    if queue_enabled:
        logger.info(
            "backfill_pass: events=%d markets=%d queued=%d min_close_ts->%d",
            counts.events,
            counts.markets,
            counts.queued,
            next_min_close_ts,
        )
    else:
        logger.info(
            "backfill_pass: events=%d markets=%d candles=%d min_close_ts->%d",
            counts.events,
            counts.markets,
            counts.candles,
            next_min_close_ts,
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
        if getattr(scan_ctx.conn, "closed", False):
            raise psycopg.OperationalError("backfill_pass: DB connection closed")
        strike_period = accept_event(event, scan_ctx.cfg.strike_periods, stats)
        if strike_period is None:
            continue
        counts.events += 1
        event_ctx = EventBackfillContext(
            event=event,
            strike_period=strike_period,
            queue_cfg=scan_ctx.queue_cfg,
            publisher=scan_ctx.publisher,
            allow_enqueue=scan_ctx.allow_enqueue,
        )
        markets, candles, queued = _backfill_event(
            scan_ctx.conn,
            scan_ctx.client,
            scan_ctx.cfg,
            event_ctx,
        )
        counts.markets += markets
        counts.candles += candles
        counts.queued += queued
    return counts, stats
