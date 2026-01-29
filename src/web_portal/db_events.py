"""Event query helpers for the web portal."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg  # pylint: disable=import-error
from psycopg.rows import dict_row  # pylint: disable=import-error

from ..db.sql_fragments import event_core_columns_sql
from .config import EVENT_SORT_COLUMNS, EVENT_STATUS_CASE, _env_int
from .db_timing import timed_cursor as _default_timed_cursor
from .filter_sql import build_filter_where
from .formatters import fmt_num, fmt_percent, fmt_time_remaining, fmt_ts
from .portal_utils import portal_func as _portal_func

EVENT_HAS_TICKS_SQL = "COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false)"
EVENT_OPEN_WINDOW_SQL = (
    "(MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW()) "
    "AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())"
)
EVENT_FUTURE_WINDOW_SQL = (
    "MIN(m.open_time) IS NOT NULL AND MIN(m.open_time) > NOW() "
    "AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())"
)
EVENT_TICK_JOIN_SQL = """
        LEFT JOIN market_ticks_latest mt
          ON mt.ticker = m.ticker
"""
EVENT_VOLUME_JOIN_SQL = """
        LEFT JOIN market_ticks_latest tv
          ON tv.ticker = m.ticker
"""

_TIMED_CURSOR = _default_timed_cursor


def set_timed_cursor(cursor_fn) -> None:
    """Override the timed_cursor helper (used by tests)."""
    global _TIMED_CURSOR  # pylint: disable=global-statement
    _TIMED_CURSOR = cursor_fn


def timed_cursor(conn, **kwargs):
    """Return a timed cursor, allowing test overrides."""
    return _TIMED_CURSOR(conn, **kwargs)


def _event_open_window_sql() -> str:
    try:
        return EVENT_OPEN_WINDOW_SQL
    except NameError:
        return (
            "(MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW()) "
            "AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())"
        )


@dataclass(frozen=True)
class EventCursor:
    """Cursor state for keyset pagination."""

    value: Any | None
    event_ticker: str


@dataclass(frozen=True)
class _EventQueryParts:  # pylint: disable=too-many-instance-attributes
    where_sql: str
    params: tuple[Any, ...]
    having_sql: str
    having_params: tuple[Any, ...]
    order_by: str
    cursor_sql: str
    cursor_params: tuple[Any, ...]
    sort_key: str


@dataclass(frozen=True)
class _EventQuery:
    query: str
    params: tuple[Any, ...]
    sort_key: str


def _build_event_where(
    filters: "PortalFilters",
    *,
    include_category: bool = True,
) -> tuple[str, list[Any]]:
    """Build SQL WHERE fragments for event filters."""
    search_fields = [
        "e.title",
        "e.sub_title",
        "e.event_ticker",
        "e.category",
    ]
    return build_filter_where(
        filters,
        search_fields,
        include_category=include_category,
    )


def build_event_where(
    filters: "PortalFilters",
    *,
    include_category: bool = True,
) -> tuple[str, list[Any]]:
    """Public wrapper for _build_event_where."""
    return _build_event_where(filters, include_category=include_category)


def _normalize_cursor_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        ts_value = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return ts_value.astimezone(timezone.utc).isoformat()
    return value


def _cursor_from_rows(
    rows: list[dict[str, Any]],
    sort_key: str,
) -> EventCursor | None:
    if not rows:
        return None
    last = rows[-1]
    event_ticker = last.get("event_ticker")
    if not event_ticker:
        return None
    sort_col = EVENT_SORT_COLUMNS.get(sort_key)
    sort_value = last.get(sort_col) if sort_col else None
    return EventCursor(
        value=_normalize_cursor_value(sort_value),
        event_ticker=event_ticker,
    )


def _build_cursor_clause(
    sort_col: str,
    order_key: str,
    cursor: EventCursor | None,
) -> tuple[str, list[Any]]:
    if cursor is None or not cursor.event_ticker:
        return "", []
    comp = "<" if order_key == "desc" else ">"
    if cursor.value is None:
        return (
            f"AND ({sort_col} IS NULL AND event_ticker {comp} %s)",
            [cursor.event_ticker],
        )
    return (
        (
            f"AND (({sort_col} {comp} %s) OR "
            f"({sort_col} = %s AND event_ticker {comp} %s) OR "
            f"{sort_col} IS NULL)"
        ),
        [cursor.value, cursor.value, cursor.event_ticker],
    )


def build_cursor_clause(
    sort_col: str,
    order_key: str,
    cursor: EventCursor | None,
) -> tuple[str, list[Any]]:
    """Public wrapper for _build_cursor_clause."""
    return _build_cursor_clause(sort_col, order_key, cursor)


def _build_order_by(
    sort: str | None,
    order: str | None,
    default_sort: str,
    default_order: str,
) -> tuple[str, str, str, str]:
    """Resolve sort inputs into stable ORDER BY clauses for event lists."""
    sort_key = sort if sort in EVENT_SORT_COLUMNS else default_sort
    order_key = order if order in {"asc", "desc"} else default_order
    sort_col = EVENT_SORT_COLUMNS[sort_key]
    order_sql = f"{sort_col} {order_key} NULLS LAST, event_ticker {order_key}"
    return sort_key, order_key, sort_col, order_sql


def build_order_by(
    sort: str | None,
    order: str | None,
    default_sort: str,
    default_order: str,
) -> tuple[str, str, str, str]:
    """Public wrapper for _build_order_by."""
    return _build_order_by(sort, order, default_sort, default_order)


def _event_query_parts(
    filters: "PortalFilters",
    default_sort: str,
    default_order: str,
    cursor: EventCursor | None,
    *,
    having_sql: str,
    having_params: list[Any],
) -> _EventQueryParts:
    where_sql, params = _build_event_where(filters)
    sort_key, order_key, sort_col, order_by = _build_order_by(
        filters.sort,
        filters.order,
        default_sort,
        default_order,
    )
    cursor_sql, cursor_params = _build_cursor_clause(sort_col, order_key, cursor)
    return _EventQueryParts(
        where_sql=where_sql,
        params=tuple(params),
        having_sql=having_sql,
        having_params=tuple(having_params),
        order_by=order_by,
        cursor_sql=cursor_sql,
        cursor_params=tuple(cursor_params),
        sort_key=sort_key,
    )


def _fetch_event_rows(
    conn: psycopg.Connection,
    query: str,
    params: tuple[Any, ...],
) -> list[dict[str, Any]]:
    with timed_cursor(conn, row_factory=dict_row) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def _event_payload(
    rows: list[dict[str, Any]],
    *,
    include_time_remaining: bool = False,
) -> list[dict[str, Any]]:
    if not include_time_remaining:
        return [build_event_snapshot(row) for row in rows]
    time_remaining = _portal_func("fmt_time_remaining", fmt_time_remaining)
    return [
        {
            **build_event_snapshot(row),
            "time_remaining": time_remaining(row.get("close_time")),
        }
        for row in rows
    ]


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
        if hasattr(cur, "fetchall"):
            rows = cur.fetchall()
        else:
            row = cur.fetchone() if hasattr(cur, "fetchone") else None
            rows = [row] if row else []
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
            f"({_event_open_window_sql()} AND NOT {EVENT_HAS_TICKS_SQL})"
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
        having_clauses.append(f"{_event_open_window_sql()} AND {EVENT_HAS_TICKS_SQL}")
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


def fetch_event_count(
    conn: psycopg.Connection,
    bucket: str,
    filters: "PortalFilters",
) -> int:
    """Public wrapper for _fetch_event_count."""
    return _fetch_event_count(conn, bucket, filters)


def fetch_counts(conn: psycopg.Connection, filters: "PortalFilters") -> tuple[int, int, int]:
    """Fetch counts for active, scheduled, and closed events."""
    return (
        _fetch_event_count(conn, "active", filters),
        _fetch_event_count(conn, "scheduled", filters),
        _fetch_event_count(conn, "closed", filters),
    )


def _candle_interval_case_sql() -> str:
    return (
        "CASE "
        "WHEN LOWER(COALESCE(e.strike_period, '')) IN "
        "('hour', 'hourly', 'hr', 'hrs', 'hours') THEN %s "
        "WHEN LOWER(COALESCE(e.strike_period, '')) ~ "
        "'^[0-9]+(m|min|minute|minutes)$' THEN %s "
        "WHEN LOWER(COALESCE(e.strike_period, '')) IN "
        "('day', 'daily', 'days', 'd') THEN %s "
        "ELSE %s "
        "END"
    )


def fetch_closed_filled_count(conn: psycopg.Connection, filters: "PortalFilters") -> int:
    """Return the count of closed events with complete candle coverage."""
    where_sql, params = _build_event_where(filters)
    having_sql, having_params = _closed_event_having(filters)
    minutes_hour = _env_int("CANDLE_MINUTES_FOR_HOUR", 1)
    minutes_day = _env_int("CANDLE_MINUTES_FOR_DAY", 60)
    period_case = _candle_interval_case_sql()
    query = f"""
        WITH closed_events AS (
          SELECT e.event_ticker
          FROM events e
          JOIN markets m ON m.event_ticker = e.event_ticker
          LEFT JOIN active_markets am ON am.ticker = m.ticker
          {EVENT_TICK_JOIN_SQL}
          WHERE TRUE
            {where_sql}
          GROUP BY e.event_ticker, e.strike_period
          {having_sql}
        ),
        market_bounds AS (
          SELECT
            m.event_ticker,
            m.open_time,
            m.close_time,
            m.settlement_value,
            m.settlement_value_dollars,
            {period_case} AS period_minutes,
            MIN(mc.end_period_ts) AS first_candle_end,
            MAX(mc.end_period_ts) AS last_candle_end
          FROM closed_events ce
          JOIN events e ON e.event_ticker = ce.event_ticker
          JOIN markets m ON m.event_ticker = ce.event_ticker
          LEFT JOIN market_candles mc
            ON mc.market_ticker = m.ticker
           AND mc.period_interval_minutes = {period_case}
          GROUP BY
            m.event_ticker,
            m.open_time,
            m.close_time,
            m.settlement_value,
            m.settlement_value_dollars,
            period_minutes
        ),
        event_missing AS (
          SELECT
            event_ticker,
            SUM(
              CASE
                WHEN close_time IS NULL THEN 0
                WHEN settlement_value IS NULL AND settlement_value_dollars IS NULL THEN 1
                WHEN last_candle_end IS NULL THEN 1
                WHEN last_candle_end < close_time - (period_minutes * INTERVAL '1 minute') THEN 1
                WHEN open_time IS NOT NULL AND first_candle_end IS NOT NULL
                     AND first_candle_end > open_time + (period_minutes * INTERVAL '1 minute')
                  THEN 1
                ELSE 0
              END
            ) AS missing_count
          FROM market_bounds
          GROUP BY event_ticker
        )
        SELECT COUNT(*)
        FROM event_missing
        WHERE missing_count = 0
        """
    period_params = (minutes_hour, minutes_hour, minutes_day, minutes_day)
    all_params = (
        *params,
        *having_params,
        *period_params,
        *period_params,
    )
    with timed_cursor(conn) as cur:
        cur.execute(query, all_params)
        row = cur.fetchone()
    return int(row[0] or 0)


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
    clauses: list[str] = [f"{_event_open_window_sql()} AND {EVENT_HAS_TICKS_SQL}"]
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


def _scheduled_event_having(filters: "PortalFilters") -> tuple[str, list[Any]]:
    clauses: list[str] = [
        f"({EVENT_FUTURE_WINDOW_SQL}) OR "
        f"({_event_open_window_sql()} AND NOT {EVENT_HAS_TICKS_SQL})"
    ]
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
    return "HAVING " + " AND ".join(clauses), params


def _closed_event_having(filters: "PortalFilters") -> tuple[str, list[Any]]:
    clauses: list[str] = [
        "COUNT(*) = COUNT(m.close_time) AND MAX(m.close_time) <= NOW()"
    ]
    params: list[Any] = []
    if filters.status:
        clauses.append(f"{EVENT_STATUS_CASE} = %s")
        params.append(filters.status)
    if filters.close_window_hours:
        clauses.append(
            "MAX(m.close_time) IS NOT NULL "
            "AND MAX(m.close_time) >= NOW() - (%s * INTERVAL '1 hour')"
        )
        params.append(filters.close_window_hours)
    return "HAVING " + " AND ".join(clauses), params


def _active_events_query(parts: _EventQueryParts) -> _EventQuery:
    query = f"""
        WITH base AS (
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
            {parts.where_sql}
          GROUP BY
            e.event_ticker,
            e.title,
            e.category,
            e.strike_period,
            p.predicted_yes_prob,
            p.confidence,
            p.created_at,
            p.market_ticker
          {parts.having_sql}
        )
        SELECT *
        FROM base
        WHERE TRUE
          {parts.cursor_sql}
        ORDER BY {parts.order_by}
        LIMIT %s
        """
    params = (*parts.params, *parts.having_params, *parts.cursor_params)
    return _EventQuery(query=query, params=params, sort_key=parts.sort_key)


def _scheduled_events_query(parts: _EventQueryParts) -> _EventQuery:
    query = f"""
        WITH base AS (
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
            {parts.where_sql}
          GROUP BY e.event_ticker, e.title, e.category, e.strike_period
          {parts.having_sql}
        )
        SELECT *
        FROM base
        WHERE TRUE
          {parts.cursor_sql}
        ORDER BY {parts.order_by}
        LIMIT %s
        """
    params = (*parts.params, *parts.having_params, *parts.cursor_params)
    return _EventQuery(query=query, params=params, sort_key=parts.sort_key)


def _closed_events_query(parts: _EventQueryParts) -> _EventQuery:
    query = f"""
        WITH base AS (
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
            {parts.where_sql}
          GROUP BY e.event_ticker, e.title, e.category, e.strike_period
          {parts.having_sql}
        )
        SELECT *
        FROM base
        WHERE TRUE
          {parts.cursor_sql}
        ORDER BY {parts.order_by}
        LIMIT %s
        """
    params = (*parts.params, *parts.having_params, *parts.cursor_params)
    return _EventQuery(query=query, params=params, sort_key=parts.sort_key)


def fetch_active_events(
    conn: psycopg.Connection,
    limit: int,
    filters: "PortalFilters",
    *,
    cursor: EventCursor | None = None,
    return_cursor: bool = False,
) -> list[dict[str, Any]] | tuple[list[dict[str, Any]], EventCursor | None]:
    """Fetch active event rows (at least one market is currently open)."""
    having_sql, having_params = _active_event_having(filters)
    parts = _event_query_parts(
        filters,
        "close_time",
        "asc",
        cursor,
        having_sql=having_sql,
        having_params=having_params,
    )
    query = _active_events_query(parts)
    rows = _fetch_event_rows(conn, query.query, (*query.params, limit))
    payload = _event_payload(rows, include_time_remaining=True)
    if return_cursor:
        return payload, _cursor_from_rows(rows, query.sort_key)
    return payload


def fetch_scheduled_events(
    conn: psycopg.Connection,
    limit: int,
    filters: "PortalFilters",
    *,
    cursor: EventCursor | None = None,
    return_cursor: bool = False,
) -> list[dict[str, Any]] | tuple[list[dict[str, Any]], EventCursor | None]:
    """Fetch scheduled event rows (all markets open in the future)."""
    having_sql, having_params = _scheduled_event_having(filters)
    parts = _event_query_parts(
        filters,
        "open_time",
        "asc",
        cursor,
        having_sql=having_sql,
        having_params=having_params,
    )
    query = _scheduled_events_query(parts)
    rows = _fetch_event_rows(conn, query.query, (*query.params, limit))
    payload = _event_payload(rows)
    if return_cursor:
        return payload, _cursor_from_rows(rows, query.sort_key)
    return payload


def fetch_closed_events(
    conn: psycopg.Connection,
    limit: int,
    filters: "PortalFilters",
    *,
    cursor: EventCursor | None = None,
    return_cursor: bool = False,
) -> list[dict[str, Any]] | tuple[list[dict[str, Any]], EventCursor | None]:
    """Fetch closed event rows (all markets closed)."""
    having_sql, having_params = _closed_event_having(filters)
    parts = _event_query_parts(
        filters,
        "close_time",
        "desc",
        cursor,
        having_sql=having_sql,
        having_params=having_params,
    )
    query = _closed_events_query(parts)
    rows = _fetch_event_rows(conn, query.query, (*query.params, limit))
    payload = _event_payload(rows)
    if return_cursor:
        return payload, _cursor_from_rows(rows, query.sort_key)
    return payload


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
