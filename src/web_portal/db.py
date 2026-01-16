"""Database helpers for the web portal."""

from __future__ import annotations

import importlib
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from typing import Any

import psycopg  # pylint: disable=import-error
from psycopg.rows import dict_row  # pylint: disable=import-error

from src.core.loop_utils import schema_path
from src.db.db import maybe_init_schema, normalize_prob_dollars
from src.db.sql_fragments import event_core_columns_sql

from .config import EVENT_SORT_SQL, EVENT_STATUS_CASE, _env_bool, _env_float, _env_int
from .db_details import _fetch_event_market_rows, fetch_event_detail, fetch_market_detail
from .db_event_series import (
    _EVENT_FORECAST_COLORS,
    _build_event_forecast_series,
    _build_event_sparklines,
    _sparkline_value,
)
from .market_metadata import (
    _derive_custom_strike,
    _extract_event_metadata,
    _maybe_parse_json,
    _update_event_metadata,
    _update_market_extras,
)
from .db_opportunities import _build_opportunity_where, fetch_opportunities
from .filter_sql import build_filter_where
from .formatters import (
    fmt_num,
    fmt_percent,
    fmt_time_remaining,
    fmt_ts,
)
from .db_timing import timed_cursor
from .portal_utils import portal_attr as _portal_attr
from .portal_utils import portal_func as _portal_func
from .portal_utils import portal_module as _portal_module

_PSYCOPG_POOL = None
try:
    _PSYCOPG_POOL = importlib.import_module("psycopg_pool")
except Exception:  # pylint: disable=broad-exception-caught
    _PSYCOPG_POOL = None

if _PSYCOPG_POOL is not None:
    class ConnectionPool(  # pylint: disable=too-few-public-methods
        _PSYCOPG_POOL.ConnectionPool
    ):  # type: ignore[misc]
        """Alias for psycopg_pool.ConnectionPool."""

        def is_open(self) -> bool:
            """Return True when the pool is open."""
            return not getattr(self, "closed", False)

    class PoolTimeout(_PSYCOPG_POOL.PoolTimeout):  # type: ignore[misc]
        """Alias for psycopg_pool.PoolTimeout."""

        def is_timeout(self) -> bool:
            """Return True when this exception signals a timeout."""
            return True

        def status(self) -> str:
            """Return a status label for this exception."""
            return "timeout"
else:

    class ConnectionPool:  # pylint: disable=too-few-public-methods
        """Fallback ConnectionPool when psycopg_pool is unavailable."""

        def __init__(self, *_args, **_kwargs) -> None:
            raise RuntimeError("psycopg_pool is unavailable.")

        def close(self) -> None:
            """No-op close for the fallback implementation."""
            return None

        def is_open(self) -> bool:
            """Return False because the fallback pool cannot open connections."""
            return False

    class PoolTimeout(Exception):  # pylint: disable=too-few-public-methods
        """Fallback PoolTimeout when psycopg_pool is unavailable."""

        def is_timeout(self) -> bool:
            """Return True when this exception signals a timeout."""
            return True

        def status(self) -> str:
            """Return a status label for this exception."""
            return "unavailable"

__all__ = [
    "ConnectionPool",
    "PoolTimeout",
    "normalize_prob_dollars",
    "_EVENT_FORECAST_COLORS",
    "_build_event_forecast_series",
    "_build_event_sparklines",
    "_derive_custom_strike",
    "_extract_event_metadata",
    "_fetch_event_market_rows",
    "_maybe_parse_json",
    "_sparkline_value",
    "_update_event_metadata",
    "_update_market_extras",
    "_build_opportunity_where",
    "fetch_portal_snapshot",
    "maybe_refresh_portal_snapshot",
    "fetch_event_detail",
    "fetch_market_detail",
    "fetch_opportunities",
]

_DB_POOL = None
_DB_POOL_LOCK = threading.Lock()
_PORTAL_SNAPSHOT_REFRESH_LOCK = threading.Lock()
_PORTAL_SNAPSHOT_REFRESH_LAST = 0.0
_PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT = False
_PORTAL_SNAPSHOT_REFRESH_WARNED = False
_PORTAL_SNAPSHOT_SCHEMA_LOCK = threading.Lock()
_PORTAL_SNAPSHOT_SCHEMA_CHECKED = False
_PORTAL_SNAPSHOT_SCHEMA_READY = False

EVENT_HAS_TICKS_SQL = "COALESCE(BOOL_OR(mt.has_tick), false)"
EVENT_OPEN_WINDOW_SQL = (
    "(MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW()) "
    "AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())"
)
EVENT_FUTURE_WINDOW_SQL = (
    "MIN(m.open_time) IS NOT NULL AND MIN(m.open_time) > NOW() "
    "AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())"
)
EVENT_TICK_JOIN_SQL = """
        LEFT JOIN LATERAL (
          SELECT TRUE AS has_tick
          FROM market_ticks t
          WHERE t.ticker = m.ticker
          LIMIT 1
        ) mt ON TRUE
"""
EVENT_VOLUME_JOIN_SQL = """
        LEFT JOIN LATERAL (
          SELECT t.volume
          FROM market_ticks t
          WHERE t.ticker = m.ticker
          ORDER BY t.ts DESC, t.id DESC
          LIMIT 1
        ) tv ON TRUE
"""


def _set_portal_attr(name: str, value: Any) -> None:
    portal = _portal_module()
    if portal is not None:
        setattr(portal, name, value)
    else:
        globals()[name] = value


def _portal_logger() -> logging.Logger:
    return _portal_attr("logger", logging.getLogger(__name__))


def _db_pool_enabled() -> bool:
    return _env_bool("WEB_DB_POOL_ENABLE", True)


def _db_pool_sizes() -> tuple[int, int]:
    min_size = _env_int("WEB_DB_POOL_MIN", 2, minimum=0)
    max_size = _env_int("WEB_DB_POOL_MAX", 8, minimum=1)
    max_size = max(max_size, min_size)
    return min_size, max_size


def _db_pool_timeout() -> float:
    return _env_float("WEB_DB_POOL_TIMEOUT", 3.0, minimum=0.1)


def _portal_snapshot_enabled() -> bool:
    return _env_bool("WEB_PORTAL_DB_SNAPSHOT_ENABLE", False)


def _portal_snapshot_refresh_sec() -> int:
    return _env_int("WEB_PORTAL_DB_SNAPSHOT_REFRESH_SEC", 60, minimum=5)


def _portal_snapshot_refresh_on_queue() -> bool:
    return _env_bool("WEB_PORTAL_DB_SNAPSHOT_REFRESH_ON_QUEUE", False)


def _portal_snapshot_view_exists(conn: psycopg.Connection) -> bool:
    with timed_cursor(conn) as cur:
        cur.execute("SELECT to_regclass('portal_event_rollup')")
        row = cur.fetchone()
    return bool(row and row[0])


def _ensure_portal_snapshot_schema(conn: psycopg.Connection) -> bool:
    global _PORTAL_SNAPSHOT_SCHEMA_CHECKED  # pylint: disable=global-statement
    global _PORTAL_SNAPSHOT_SCHEMA_READY  # pylint: disable=global-statement
    if _PORTAL_SNAPSHOT_SCHEMA_CHECKED:
        return _PORTAL_SNAPSHOT_SCHEMA_READY
    with _PORTAL_SNAPSHOT_SCHEMA_LOCK:
        if _PORTAL_SNAPSHOT_SCHEMA_CHECKED:
            return _PORTAL_SNAPSHOT_SCHEMA_READY
        _PORTAL_SNAPSHOT_SCHEMA_CHECKED = True
        if _portal_snapshot_view_exists(conn):
            _PORTAL_SNAPSHOT_SCHEMA_READY = True
            return True
        try:
            maybe_init_schema(conn, schema_path=schema_path(__file__))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if not conn.autocommit:
                conn.rollback()
            _portal_logger().warning(
                "Portal snapshot schema init failed; run migrator if needed: %s",
                exc,
            )
            _PORTAL_SNAPSHOT_SCHEMA_READY = False
            return False
        _PORTAL_SNAPSHOT_SCHEMA_READY = _portal_snapshot_view_exists(conn)
        if not _PORTAL_SNAPSHOT_SCHEMA_READY:
            _portal_logger().warning(
                "Portal snapshot schema missing: portal_event_rollup"
            )
        return _PORTAL_SNAPSHOT_SCHEMA_READY


def _get_db_pool(db_url: str | None):
    pool_cls = _portal_attr("ConnectionPool", ConnectionPool)
    if not db_url or pool_cls is None or not _db_pool_enabled():
        return None
    pool = _portal_attr("_DB_POOL", _DB_POOL)
    if pool is not None:
        return pool
    lock = _portal_attr("_DB_POOL_LOCK", _DB_POOL_LOCK)
    with lock:
        pool = _portal_attr("_DB_POOL", _DB_POOL)
        if pool is not None:
            return pool
        min_size, max_size = _db_pool_sizes()
        timeout = _db_pool_timeout()
        try:
            pool = pool_cls(
                db_url,
                min_size=min_size,
                max_size=max_size,
                timeout=timeout,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _portal_logger().warning(
                "DB pool init failed; falling back to direct connections: %s",
                exc,
            )
            pool = None
        _set_portal_attr("_DB_POOL", pool)
    return pool


@contextmanager
def _managed_connection(conn: psycopg.Connection, autocommit: bool):
    prev_autocommit = conn.autocommit
    conn.autocommit = autocommit
    try:
        yield conn
        if not conn.autocommit:
            conn.commit()
    except Exception:
        if not conn.autocommit:
            conn.rollback()
        raise
    finally:
        conn.autocommit = prev_autocommit


@contextmanager
def _pool_connection(pool, autocommit: bool):
    try:
        with pool.connection() as conn:
            with _managed_connection(conn, autocommit) as managed:
                yield managed
    except _portal_attr("PoolTimeout", PoolTimeout) as exc:
        raise RuntimeError(
            "Database connection pool exhausted; try again shortly."
        ) from exc


@contextmanager
def _direct_connection(
    db_url: str,
    autocommit: bool,
    connect_timeout: int | None,
):
    connect_kwargs: dict[str, Any] = {}
    if connect_timeout is not None:
        connect_kwargs["connect_timeout"] = connect_timeout
    conn = psycopg.connect(db_url, **connect_kwargs)
    try:
        with _managed_connection(conn, autocommit) as managed:
            yield managed
    finally:
        conn.close()


@contextmanager
def _db_connection(
    autocommit: bool = False,
    connect_timeout: int | None = None,
    force_direct: bool = False,
):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set.")
    pool_func = _portal_attr("_get_db_pool", _get_db_pool)
    pool = None if force_direct else pool_func(db_url)
    if pool is not None:
        with _pool_connection(pool, autocommit) as conn:
            yield conn
        return

    with _direct_connection(db_url, autocommit, connect_timeout) as conn:
        yield conn


def refresh_portal_snapshot(*, concurrent: bool = True) -> None:
    """Refresh the portal_event_rollup materialized view."""
    query = "REFRESH MATERIALIZED VIEW CONCURRENTLY portal_event_rollup"
    fallback_query = "REFRESH MATERIALIZED VIEW portal_event_rollup"
    try:
        with _db_connection(autocommit=True, force_direct=True) as conn:
            if not _ensure_portal_snapshot_schema(conn):
                raise RuntimeError(
                    "portal_event_rollup missing; run migrator or enable DB_INIT_SCHEMA."
                )
            with timed_cursor(conn) as cur:
                cur.execute(query if concurrent else fallback_query)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if concurrent:
            with _db_connection(autocommit=True, force_direct=True) as conn:
                if not _ensure_portal_snapshot_schema(conn):
                    raise RuntimeError(
                        "portal_event_rollup missing; run migrator or enable DB_INIT_SCHEMA."
                    ) from exc
                with timed_cursor(conn) as cur:
                    cur.execute(fallback_query)
            return
        raise


def _finish_portal_snapshot_refresh(success: bool) -> None:
    with _PORTAL_SNAPSHOT_REFRESH_LOCK:
        global _PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT  # pylint: disable=global-statement
        global _PORTAL_SNAPSHOT_REFRESH_LAST  # pylint: disable=global-statement
        _PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT = False
        if success:
            _PORTAL_SNAPSHOT_REFRESH_LAST = time.monotonic()


def _refresh_portal_snapshot(reason: str) -> bool:
    try:
        refresh_portal_snapshot()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        global _PORTAL_SNAPSHOT_REFRESH_WARNED  # pylint: disable=global-statement
        if not _PORTAL_SNAPSHOT_REFRESH_WARNED:
            _portal_logger().warning(
                "Portal snapshot refresh failed (%s): %s",
                reason,
                exc,
            )
            _PORTAL_SNAPSHOT_REFRESH_WARNED = True
        _finish_portal_snapshot_refresh(False)
        return False
    _finish_portal_snapshot_refresh(True)
    return True


def maybe_refresh_portal_snapshot(*, reason: str, background: bool = False) -> bool:
    """Refresh portal snapshot on demand with throttling."""
    if not _portal_snapshot_enabled():
        return False
    if reason == "queue" and not _portal_snapshot_refresh_on_queue():
        return False
    min_interval = _portal_snapshot_refresh_sec()
    now = time.monotonic()
    with _PORTAL_SNAPSHOT_REFRESH_LOCK:
        global _PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT  # pylint: disable=global-statement
        if _PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT:
            return False
        if now - _PORTAL_SNAPSHOT_REFRESH_LAST < min_interval:
            return False
        _PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT = True
    if background:
        thread = threading.Thread(
            target=_refresh_portal_snapshot,
            args=(reason,),
            name="portal-snapshot-refresh",
            daemon=True,
        )
        thread.start()
        return True
    return _refresh_portal_snapshot(reason)


PortalSnapshotOffsets = tuple[int, int, int]


def fetch_portal_snapshot(
    conn: psycopg.Connection,
    limit: int,
    filters: "PortalFilters",
    offsets: PortalSnapshotOffsets = (0, 0, 0),
) -> dict[str, Any] | None:
    """Fetch the portal snapshot JSON payload from the database."""
    active_offset, scheduled_offset, closed_offset = offsets
    close_window_hours = filters.close_window_hours
    categories = list(filters.categories) if filters.categories else None
    queue_timeout = _env_int("WORK_QUEUE_LOCK_TIMEOUT_SECONDS", 900, minimum=10)
    if not _ensure_portal_snapshot_schema(conn):
        return None
    with timed_cursor(conn) as cur:
        cur.execute(
            """
            SELECT portal_snapshot_json(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                limit,
                filters.search,
                categories,
                filters.strike_period,
                close_window_hours,
                filters.status,
                filters.sort,
                filters.order,
                queue_timeout,
                active_offset,
                scheduled_offset,
                closed_offset,
            ),
        )
        row = cur.fetchone()
    payload = row[0] if row else None
    if payload is None:
        return None
    if isinstance(payload, str):
        payload = json.loads(payload)
    return payload


def _maybe_prewarm_db_pool() -> None:
    if not _env_bool("WEB_DB_POOL_PREWARM", True):
        return
    db_url = os.getenv("DATABASE_URL")
    pool_func = _portal_attr("_get_db_pool", _get_db_pool)
    pool = pool_func(db_url)
    if pool is None:
        return
    try:
        with pool.connection():
            pass
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _portal_logger().warning("DB pool prewarm failed: %s", exc)


def _build_event_where(
    filters: "PortalFilters",
    *,
    include_category: bool = True,
) -> tuple[str, list[Any]]:
    """Build SQL WHERE fragments for event filters."""
    return build_filter_where(
        filters,
        ["e.title", "e.sub_title", "e.event_ticker", "e.category"],
        include_category=include_category,
    )


def _build_order_by(
    sort: str | None,
    order: str | None,
    default_sort: str,
    default_order: str,
) -> str:
    """Build a safe ORDER BY clause for event lists."""
    sort_key = sort if sort in EVENT_SORT_SQL else default_sort
    order_key = order if order in {"asc", "desc"} else default_order
    return f"{EVENT_SORT_SQL[sort_key]} {order_key} NULLS LAST"


def _fetch_state_rows(
    conn: psycopg.Connection,
    keys: list[str],
) -> dict[str, dict[str, Any]]:
    """Fetch ingest_state rows keyed by name."""
    if not keys:
        return {}
    with timed_cursor(conn, row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT key, value, updated_at
            FROM ingest_state
            WHERE key = ANY(%s)
            """,
            (keys,),
        )
        rows = cur.fetchall()
    return {row["key"]: row for row in rows if row and row.get("key")}


def _fetch_event_count(
    conn: psycopg.Connection,
    bucket: str,
    filters: "PortalFilters",
) -> int:
    """Fetch a count of events for a status bucket."""
    where_sql, params = _build_event_where(filters)
    having_clauses: list[str] = []
    having_params: list[Any] = []
    if bucket == "scheduled":
        having_clauses.append(
            f"({EVENT_FUTURE_WINDOW_SQL}) OR "
            f"({EVENT_OPEN_WINDOW_SQL} AND NOT {EVENT_HAS_TICKS_SQL})"
        )
    elif bucket == "closed":
        having_clauses.append(
            "COUNT(*) = COUNT(m.close_time) AND MAX(m.close_time) <= NOW()"
        )
    if filters.status:
        having_clauses.append(f"{EVENT_STATUS_CASE} = %s")
        having_params.append(filters.status)
    if filters.close_window_hours:
        if bucket == "closed":
            having_clauses.append(
                "MAX(m.close_time) IS NOT NULL "
                "AND MAX(m.close_time) >= NOW() - (%s * INTERVAL '1 hour')"
            )
        else:
            having_clauses.append(
                "MAX(m.close_time) IS NOT NULL "
                "AND MAX(m.close_time) <= NOW() + (%s * INTERVAL '1 hour')"
            )
        having_params.append(filters.close_window_hours)

    if bucket == "active":
        having_clauses.append(f"{EVENT_OPEN_WINDOW_SQL} AND {EVENT_HAS_TICKS_SQL}")
    base_where = "WHERE TRUE"
    having_sql = ""
    if having_clauses:
        having_sql = "HAVING " + " AND ".join(having_clauses)

    query = f"""
        SELECT COUNT(*) FROM (
          SELECT e.event_ticker
          FROM events e
          JOIN markets m ON m.event_ticker = e.event_ticker
          LEFT JOIN active_markets am ON am.ticker = m.ticker
          {EVENT_TICK_JOIN_SQL}
          {base_where}
          {where_sql}
          GROUP BY e.event_ticker
          {having_sql}
        ) counted
        """
    with timed_cursor(conn) as cur:
        cur.execute(query, (*params, *having_params))
        row = cur.fetchone()
    return int(row[0] or 0)


def fetch_counts(conn: psycopg.Connection, filters: "PortalFilters") -> tuple[int, int, int]:
    """Fetch counts for active, scheduled, and closed events."""
    return (
        _fetch_event_count(conn, "active", filters),
        _fetch_event_count(conn, "scheduled", filters),
        _fetch_event_count(conn, "closed", filters),
    )


def build_event_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize an event row for portal tables."""
    return {
        "event_title": row.get("event_title") or "Unknown event",
        "event_ticker": row.get("event_ticker") or "N/A",
        "open_time": fmt_ts(row.get("open_time")),
        "close_time": fmt_ts(row.get("close_time")),
        "market_count": fmt_num(row.get("market_count")),
        "volume": fmt_num(row.get("volume")),
        "agent_yes_prob": fmt_percent(row.get("agent_yes_prob")),
        "agent_confidence": fmt_percent(row.get("agent_confidence")),
        "agent_prediction_ts": fmt_ts(row.get("agent_prediction_ts")),
        "agent_market_ticker": row.get("agent_market_ticker") or "",
    }


def _active_event_having(filters: "PortalFilters") -> tuple[str, list[Any]]:
    clauses: list[str] = [f"{EVENT_OPEN_WINDOW_SQL} AND {EVENT_HAS_TICKS_SQL}"]
    params: list[Any] = []
    if filters.status:
        clauses.append(f"{EVENT_STATUS_CASE} = %s")
        params.append(filters.status)
    if filters.close_window_hours:
        clauses.append(
            "MAX(m.close_time) IS NOT NULL "
            "AND MAX(m.close_time) <= NOW() + (%s * INTERVAL '1 hour')"
        )
        params.append(filters.close_window_hours)
    having_sql = "HAVING " + " AND ".join(clauses) if clauses else ""
    return having_sql, params


def fetch_active_events(
    conn: psycopg.Connection,
    limit: int,
    filters: "PortalFilters",
    *,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Fetch active event rows (at least one market is currently open)."""
    where_sql, params = _build_event_where(filters)
    having_sql, having_params = _active_event_having(filters)
    order_by = _build_order_by(filters.sort, filters.order, "close_time", "asc")

    query = f"""
        SELECT
          {event_core_columns_sql()},
          e.strike_period,
          MIN(m.open_time) AS open_time,
          MAX(m.close_time) AS close_time,
          COUNT(*) AS market_count,
          SUM(tv.volume) AS volume,
          p.predicted_yes_prob AS agent_yes_prob,
          p.confidence AS agent_confidence,
          p.created_at AS agent_prediction_ts,
          p.market_ticker AS agent_market_ticker
        FROM events e
        JOIN markets m ON m.event_ticker = e.event_ticker
        LEFT JOIN active_markets am ON am.ticker = m.ticker
        {EVENT_TICK_JOIN_SQL}
        {EVENT_VOLUME_JOIN_SQL}
        LEFT JOIN LATERAL (
          SELECT
            p.predicted_yes_prob,
            p.confidence,
            p.created_at,
            p.market_ticker
          FROM market_predictions p
          WHERE p.event_ticker = e.event_ticker
          ORDER BY p.created_at DESC, p.id DESC
          LIMIT 1
        ) p ON TRUE
        WHERE TRUE
          {where_sql}
        GROUP BY
          e.event_ticker,
          e.title,
          e.category,
          e.strike_period,
          p.predicted_yes_prob,
          p.confidence,
          p.created_at,
          p.market_ticker
        {having_sql}
        ORDER BY {order_by}
        LIMIT %s
        OFFSET %s
        """
    with timed_cursor(conn, row_factory=dict_row) as cur:
        cur.execute(query, (*params, *having_params, limit, offset))
        rows = cur.fetchall()
    time_remaining = _portal_func("fmt_time_remaining", fmt_time_remaining)
    return [
        {
            **build_event_snapshot(row),
            "time_remaining": time_remaining(row.get("close_time")),
        }
        for row in rows
    ]


def fetch_scheduled_events(
    conn: psycopg.Connection,
    limit: int,
    filters: "PortalFilters",
    *,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Fetch scheduled event rows (all markets open in the future)."""
    where_sql, params = _build_event_where(filters)
    having_clauses: list[str] = [
        f"({EVENT_FUTURE_WINDOW_SQL}) OR "
        f"({EVENT_OPEN_WINDOW_SQL} AND NOT {EVENT_HAS_TICKS_SQL})"
    ]
    having_params: list[Any] = []
    if filters.status:
        having_clauses.append(f"{EVENT_STATUS_CASE} = %s")
        having_params.append(filters.status)
    if filters.close_window_hours:
        having_clauses.append(
            "MAX(m.close_time) IS NOT NULL "
            "AND MAX(m.close_time) <= NOW() + (%s * INTERVAL '1 hour')"
        )
        having_params.append(filters.close_window_hours)
    having_sql = "HAVING " + " AND ".join(having_clauses)
    order_by = _build_order_by(filters.sort, filters.order, "open_time", "asc")

    query = f"""
        SELECT
          {event_core_columns_sql()},
          e.strike_period,
          MIN(m.open_time) AS open_time,
          MAX(m.close_time) AS close_time,
          COUNT(*) AS market_count,
          SUM(tv.volume) AS volume
        FROM events e
        JOIN markets m ON m.event_ticker = e.event_ticker
        LEFT JOIN active_markets am ON am.ticker = m.ticker
        {EVENT_TICK_JOIN_SQL}
        {EVENT_VOLUME_JOIN_SQL}
        WHERE TRUE
          {where_sql}
        GROUP BY e.event_ticker, e.title, e.category, e.strike_period
        {having_sql}
        ORDER BY {order_by}
        LIMIT %s
        OFFSET %s
        """
    with timed_cursor(conn, row_factory=dict_row) as cur:
        cur.execute(query, (*params, *having_params, limit, offset))
        rows = cur.fetchall()
    return [build_event_snapshot(row) for row in rows]


def fetch_closed_events(
    conn: psycopg.Connection,
    limit: int,
    filters: "PortalFilters",
    *,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Fetch closed event rows (all markets closed)."""
    where_sql, params = _build_event_where(filters)
    having_clauses: list[str] = [
        "COUNT(*) = COUNT(m.close_time) AND MAX(m.close_time) <= NOW()"
    ]
    having_params: list[Any] = []
    if filters.status:
        having_clauses.append(f"{EVENT_STATUS_CASE} = %s")
        having_params.append(filters.status)
    if filters.close_window_hours:
        having_clauses.append(
            "MAX(m.close_time) IS NOT NULL "
            "AND MAX(m.close_time) >= NOW() - (%s * INTERVAL '1 hour')"
        )
        having_params.append(filters.close_window_hours)
    having_sql = "HAVING " + " AND ".join(having_clauses)
    order_by = _build_order_by("close_time", "desc", "close_time", "desc")

    query = f"""
        SELECT
          {event_core_columns_sql()},
          e.strike_period,
          MIN(m.open_time) AS open_time,
          MAX(m.close_time) AS close_time,
          COUNT(*) AS market_count,
          SUM(tv.volume) AS volume
        FROM events e
        JOIN markets m ON m.event_ticker = e.event_ticker
        LEFT JOIN active_markets am ON am.ticker = m.ticker
        {EVENT_TICK_JOIN_SQL}
        {EVENT_VOLUME_JOIN_SQL}
        WHERE TRUE
          {where_sql}
        GROUP BY e.event_ticker, e.title, e.category, e.strike_period
        {having_sql}
        ORDER BY {order_by}
        LIMIT %s
        OFFSET %s
        """
    with timed_cursor(conn, row_factory=dict_row) as cur:
        cur.execute(query, (*params, *having_params, limit, offset))
        rows = cur.fetchall()
    return [build_event_snapshot(row) for row in rows]


def fetch_event_categories(conn: psycopg.Connection) -> list[str]:
    """Fetch distinct event categories."""
    with timed_cursor(conn) as cur:
        cur.execute(
            """
            SELECT DISTINCT category
            FROM events
            WHERE category IS NOT NULL AND category <> ''
            ORDER BY category
            """
        )
        rows = cur.fetchall()
    return [row[0] for row in rows if row and row[0]]


def fetch_strike_periods(conn: psycopg.Connection) -> list[str]:
    """Fetch distinct strike periods."""
    with timed_cursor(conn) as cur:
        cur.execute(
            """
            SELECT DISTINCT strike_period
            FROM events
            WHERE strike_period IS NOT NULL AND strike_period <> ''
            ORDER BY strike_period
            """
        )
        rows = cur.fetchall()
    return [row[0] for row in rows if row and row[0]]


def fetch_active_event_categories(
    conn: psycopg.Connection,
    filters: "PortalFilters",
) -> list[str]:
    """Fetch active event categories for quick filters."""
    where_sql, params = _build_event_where(filters, include_category=False)
    having_clauses: list[str] = [EVENT_HAS_TICKS_SQL]
    having_params: list[Any] = []
    if filters.status:
        having_clauses.append(f"{EVENT_STATUS_CASE} = %s")
        having_params.append(filters.status)
    if filters.close_window_hours:
        having_clauses.append(
            "MAX(m.close_time) IS NOT NULL "
            "AND MAX(m.close_time) <= NOW() + (%s * INTERVAL '1 hour')"
        )
        having_params.append(filters.close_window_hours)
    having_sql = ""
    if having_clauses:
        having_sql = "HAVING " + " AND ".join(having_clauses)

    query = f"""
        SELECT DISTINCT event_category
        FROM (
          SELECT e.event_ticker, e.category AS event_category
          FROM events e
          JOIN markets m ON m.event_ticker = e.event_ticker
          LEFT JOIN active_markets am ON am.ticker = m.ticker
          {EVENT_TICK_JOIN_SQL}
          WHERE (m.open_time IS NULL OR m.open_time <= NOW())
            AND (m.close_time IS NULL OR m.close_time > NOW())
            {where_sql}
          GROUP BY e.event_ticker, e.category
          {having_sql}
        ) active_events
        WHERE event_category IS NOT NULL AND event_category <> ''
        ORDER BY event_category
        """
    with timed_cursor(conn) as cur:
        cur.execute(query, (*params, *having_params))
        rows = cur.fetchall()
    return [row[0] for row in rows if row and row[0]]
