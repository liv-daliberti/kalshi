"""Shared helpers for job error handling."""

from __future__ import annotations

from typing import Any, Callable

from src.core.loop_utils import log_metric


def log_item_error(
    logger,
    metric_name: str,
    *,
    kind: str,
    event_ticker: str | None,
    market_ticker: str | None,
    rollback: Callable[[Any], None],
    conn: Any,
) -> None:
    """Record an item error metric and attempt a rollback."""
    log_metric(
        logger,
        metric_name,
        kind=kind,
        event_ticker=event_ticker,
        market_ticker=market_ticker,
    )
    rollback(conn)
