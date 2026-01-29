"""Shared helpers for job error handling."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import psycopg  # pylint: disable=import-error

from ..core.loop_utils import log_metric

_DB_RECONNECT_ERRORS = (
    psycopg.OperationalError,
    psycopg.InterfaceError,
)


@dataclass(frozen=True)
class ItemErrorContext:
    """Context for logging item errors and rolling back."""

    kind: str
    event_ticker: str | None
    market_ticker: str | None
    rollback: Callable[[Any], None]
    conn: Any


def log_item_error(
    logger,
    metric_name: str,
    ctx: ItemErrorContext,
) -> None:
    """Record an item error metric and attempt a rollback."""
    log_metric(
        logger,
        metric_name,
        kind=ctx.kind,
        event_ticker=ctx.event_ticker,
        market_ticker=ctx.market_ticker,
    )
    ctx.rollback(ctx.conn)


def log_item_error_and_should_raise(
    logger,
    metric_name: str,
    ctx: ItemErrorContext,
    exc: Exception,
) -> bool:
    """Log an item error and return True when the error should be re-raised."""
    log_item_error(logger, metric_name, ctx)
    return is_connection_error(ctx.conn, exc)


def handle_event_upsert_error(
    *,
    logger,
    metric_name: str,
    event_ticker: str | None,
    rollback: Callable[[Any], None],
    conn: Any,
    exc: Exception,
    source: str,
) -> bool:
    """Log an event upsert error and return True when it should be re-raised."""
    logger.exception("%s: event upsert failed event_ticker=%s", source, event_ticker)
    ctx = ItemErrorContext(
        kind="event_upsert",
        event_ticker=event_ticker,
        market_ticker=None,
        rollback=rollback,
        conn=conn,
    )
    return log_item_error_and_should_raise(logger, metric_name, ctx, exc)


def upsert_event_with_errors(
    conn: Any,
    event: dict,
    *,
    logger,
    metric_name: str,
    rollback: Callable[[Any], None],
    source: str,
) -> bool:
    """Upsert an event and handle errors consistently."""
    from ..db.db import upsert_event  # pylint: disable=import-outside-toplevel

    event_ticker = event.get("event_ticker") if isinstance(event, dict) else None
    try:
        upsert_event(conn, event)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if handle_event_upsert_error(
            logger=logger,
            metric_name=metric_name,
            event_ticker=event_ticker,
            rollback=rollback,
            conn=conn,
            exc=exc,
            source=source,
        ):
            raise
        return False
    return True


def handle_market_upsert_error(
    *,
    logger,
    metric_name: str,
    event_ticker: str | None,
    market_ticker: str | None,
    rollback: Callable[[Any], None],
    conn: Any,
    exc: Exception,
    source: str,
) -> bool:
    """Log a market upsert error and return True when it should be re-raised."""
    logger.exception(
        "%s: market upsert failed event=%s market=%s",
        source,
        event_ticker,
        market_ticker,
    )
    ctx = ItemErrorContext(
        kind="market_upsert",
        event_ticker=event_ticker,
        market_ticker=market_ticker,
        rollback=rollback,
        conn=conn,
    )
    return log_item_error_and_should_raise(logger, metric_name, ctx, exc)


def safe_rollback(
    conn: Any,
    *,
    logger,
    label: str,
) -> None:
    """Safely roll back a DB transaction with a warning on failure."""
    if getattr(conn, "closed", False):
        return
    try:
        conn.rollback()
    except Exception:  # pylint: disable=broad-exception-caught
        logger.warning("%s: rollback failed", label)


def is_connection_error(conn: Any, exc: Exception) -> bool:
    """Return True when an error indicates a stale DB connection."""
    return isinstance(exc, _DB_RECONNECT_ERRORS) or getattr(conn, "closed", False)
