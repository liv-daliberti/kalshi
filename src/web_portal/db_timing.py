"""Helpers for timing portal database queries."""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Iterator

try:  # Flask isn't always available in CLI contexts.
    from flask import (  # pylint: disable=import-error
        g as FLASK_G,
        has_request_context as _has_request_context,
    )
except Exception:  # pylint: disable=broad-exception-caught
    FLASK_G = None

    def _has_request_context() -> bool:
        """Return False when Flask request context is unavailable."""
        return False


logger = logging.getLogger(__name__)


def _get_request_g() -> Any | None:
    """Return the Flask request context object when available."""
    if not _has_request_context() or FLASK_G is None:
        return None
    return FLASK_G


def _slow_query_threshold_ms() -> float | None:
    """Return the slow query threshold (ms) if configured."""
    raw = os.getenv("WEB_PORTAL_DB_SLOW_QUERY_MS")
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return value


def reset_db_metrics() -> None:
    """Reset per-request DB timing totals."""
    request_g = _get_request_g()
    if request_g is None:
        return
    request_g.db_time_ms = 0.0
    request_g.db_query_count = 0


def get_db_metrics() -> tuple[float, int]:
    """Return the current DB time (ms) and query count."""
    request_g = _get_request_g()
    if request_g is None:
        return 0.0, 0
    return (
        float(getattr(request_g, "db_time_ms", 0.0)),
        int(getattr(request_g, "db_query_count", 0)),
    )


def _record_db_time(elapsed_ms: float) -> None:
    request_g = _get_request_g()
    if request_g is None:
        return
    request_g.db_time_ms = float(getattr(request_g, "db_time_ms", 0.0)) + elapsed_ms
    request_g.db_query_count = int(getattr(request_g, "db_query_count", 0)) + 1


def _log_slow_query(query: Any, elapsed_ms: float) -> None:
    snippet = query
    if isinstance(snippet, bytes):
        snippet = snippet.decode("utf-8", errors="replace")
    snippet = " ".join(str(snippet).split())
    if len(snippet) > 400:
        snippet = f"{snippet[:400]}..."
    logger.warning("Slow DB query: %.1fms sql=%s", elapsed_ms, snippet)


class TimedCursor:
    """Cursor wrapper that tracks execute timing."""

    def __init__(self, cursor, slow_ms: float | None) -> None:
        self._cursor = cursor
        self._slow_ms = slow_ms

    def execute(self, query: Any, *args, **kwargs):
        """Execute a query while recording timing metrics."""
        params = None
        extra_args = args
        if "params" in kwargs:
            params = kwargs.pop("params")
        elif args:
            params = args[0]
            extra_args = args[1:]
        start = time.perf_counter()
        try:
            if params is None:
                return self._cursor.execute(query, *extra_args, **kwargs)
            return self._cursor.execute(query, params, *extra_args, **kwargs)
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            _record_db_time(elapsed_ms)
            if self._slow_ms is not None and elapsed_ms >= self._slow_ms:
                _log_slow_query(query, elapsed_ms)

    def executemany(self, query: Any, params_seq: Any, *args, **kwargs):
        """Execute a batch query while recording timing metrics."""
        start = time.perf_counter()
        try:
            return self._cursor.executemany(query, params_seq, *args, **kwargs)
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            _record_db_time(elapsed_ms)
            if self._slow_ms is not None and elapsed_ms >= self._slow_ms:
                _log_slow_query(query, elapsed_ms)

    def __getattr__(self, name: str):
        return getattr(self._cursor, name)


@contextmanager
def timed_cursor(conn, **kwargs) -> Iterator[TimedCursor]:
    """Yield a cursor that records DB timing metrics."""
    slow_ms = _slow_query_threshold_ms()
    with conn.cursor(**kwargs) as cursor:
        yield TimedCursor(cursor, slow_ms)
