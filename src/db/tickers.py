"""Ticker loading helpers for ingest workflows."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg  # pylint: disable=import-error

from .state_utils import get_state, set_state


def _active_tickers_where(
    shard_count: int,
    shard_id: int,
    shard_key: str = "event",
) -> tuple[str, list]:
    clauses = [
        "(close_time IS NULL OR close_time > NOW() - INTERVAL '30 minutes')",
    ]
    params: list = []
    normalized_key = (shard_key or "event").strip().lower()
    if normalized_key not in {"event", "market"}:
        raise ValueError(f"Invalid shard_key={shard_key!r}; expected 'event' or 'market'")
    shard_expr = "coalesce(event_ticker, ticker)" if normalized_key == "event" else "ticker"
    if shard_count > 1:
        clauses.append(f"mod(abs(hashtext({shard_expr})), %s) = %s")
        params.extend([shard_count, shard_id])
    return "WHERE " + " AND ".join(clauses), params


def _active_tickers_count(conn: psycopg.Connection, where_sql: str, params: list) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM active_markets {where_sql}", params)
        row = cur.fetchone()
    return int(row[0] or 0)


def _load_state_int(conn: psycopg.Connection, key: str, default: int = 0) -> int:
    raw = get_state(conn, key, str(default)) or str(default)
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return default


def _store_state_int(conn: psycopg.Connection, key: str, value: int) -> None:
    set_state(conn, key, str(max(0, value)))


@dataclass(frozen=True)
class _ActiveTickerQuery:
    """Parameters for querying active tickers."""

    where_sql: str
    params: list
    limit: int


def _load_active_tickers_slice(
    conn: psycopg.Connection,
    where_sql: str,
    params: list,
    limit: int | None,
    offset: int = 0,
) -> list[str]:
    sql = f"""
    SELECT ticker
    FROM active_markets
    {where_sql}
    ORDER BY close_time NULLS LAST, ticker
    """
    use_tuple = not params
    if limit is not None and limit > 0:
        sql += " LIMIT %s"
        params = [*params, limit]
    if offset > 0:
        sql += " OFFSET %s"
        params = [*params, offset]
    with conn.cursor() as cur:
        exec_params = tuple(params) if use_tuple else params
        cur.execute(sql, exec_params)
        return [r[0] for r in cur.fetchall()]


def _load_active_tickers_round_robin(
    conn: psycopg.Connection,
    query: _ActiveTickerQuery,
    cursor_key: str,
    step: int | None = None,
) -> list[str]:
    if query.limit <= 0:
        return _load_active_tickers_slice(conn, query.where_sql, query.params, None)
    advance = step if step is not None and step > 0 else query.limit
    total = _active_tickers_count(conn, query.where_sql, query.params)
    if total <= query.limit:
        return _load_active_tickers_slice(
            conn,
            query.where_sql,
            query.params,
            query.limit,
        )
    cursor = _load_state_int(conn, cursor_key, 0)
    offset = cursor % total
    first_count = min(query.limit, total - offset)
    rows = _load_active_tickers_slice(
        conn,
        query.where_sql,
        query.params,
        first_count,
        offset,
    )
    if first_count < query.limit:
        rows.extend(
            _load_active_tickers_slice(
                conn,
                query.where_sql,
                query.params,
                query.limit - first_count,
                0,
            )
        )
    _store_state_int(conn, cursor_key, cursor + advance)
    return rows


def load_active_tickers(
    conn: psycopg.Connection,
    limit: int,
    *,
    round_robin: bool = False,
    cursor_key: str | None = None,
    round_robin_step: int | None = None,
) -> list[str]:
    """Load active market tickers ordered by close time."""
    where_sql, params = _active_tickers_where(1, 0)
    if round_robin:
        key = cursor_key or "ws_active_cursor:1:0"
        return _load_active_tickers_round_robin(
            conn,
            _ActiveTickerQuery(where_sql=where_sql, params=params, limit=limit),
            key,
            step=round_robin_step,
        )
    if limit <= 0:
        return _load_active_tickers_slice(conn, where_sql, params, None)
    return _load_active_tickers_slice(conn, where_sql, params, limit)


def load_active_tickers_shard(
    conn: psycopg.Connection,
    limit: int,
    shard_count: int,
    shard_id: int,
    *,
    shard_key: str = "event",
    round_robin: bool = False,
    cursor_key: str | None = None,
    round_robin_step: int | None = None,
) -> list[str]:
    """Load active market tickers for a shard, grouped by the shard key."""
    if shard_count <= 1:
        return load_active_tickers(
            conn,
            limit,
            round_robin=round_robin,
            cursor_key=cursor_key,
            round_robin_step=round_robin_step,
        )
    if shard_id < 0 or shard_id >= shard_count:
        raise ValueError(f"Invalid shard id {shard_id} for shard_count {shard_count}")
    where_sql, params = _active_tickers_where(shard_count, shard_id, shard_key)
    if round_robin:
        key = cursor_key or f"ws_active_cursor:{shard_count}:{shard_id}"
        return _load_active_tickers_round_robin(
            conn,
            _ActiveTickerQuery(where_sql=where_sql, params=params, limit=limit),
            key,
            step=round_robin_step,
        )
    if limit <= 0:
        return _load_active_tickers_slice(conn, where_sql, params, None)
    return _load_active_tickers_slice(conn, where_sql, params, limit)


def _market_tickers_where(
    shard_count: int,
    shard_id: int,
    shard_key: str = "event",
) -> tuple[str, list]:
    clauses = [
        "event_ticker IS NOT NULL",
        "(open_time IS NULL OR open_time <= NOW())",
        "(close_time IS NULL OR close_time > NOW() - INTERVAL '30 minutes')",
    ]
    params: list = []
    normalized_key = (shard_key or "event").strip().lower()
    if normalized_key not in {"event", "market"}:
        raise ValueError(f"Invalid shard_key={shard_key!r}; expected 'event' or 'market'")
    shard_expr = "coalesce(event_ticker, ticker)" if normalized_key == "event" else "ticker"
    if shard_count > 1:
        clauses.append(f"mod(abs(hashtext({shard_expr})), %s) = %s")
        params.extend([shard_count, shard_id])
    return "WHERE " + " AND ".join(clauses), params


def _market_tickers_count(conn: psycopg.Connection, where_sql: str, params: list) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM markets {where_sql}", params)
        row = cur.fetchone()
    return int(row[0] or 0)


def _load_market_tickers_slice(
    conn: psycopg.Connection,
    where_sql: str,
    params: list,
    limit: int | None,
    offset: int = 0,
) -> list[str]:
    sql = f"""
    SELECT ticker
    FROM markets
    {where_sql}
    ORDER BY close_time NULLS LAST, ticker
    """
    use_tuple = not params
    if limit is not None and limit > 0:
        sql += " LIMIT %s"
        params = [*params, limit]
    if offset > 0:
        sql += " OFFSET %s"
        params = [*params, offset]
    with conn.cursor() as cur:
        exec_params = tuple(params) if use_tuple else params
        cur.execute(sql, exec_params)
        return [r[0] for r in cur.fetchall()]


def _load_market_tickers_round_robin(
    conn: psycopg.Connection,
    query: _ActiveTickerQuery,
    cursor_key: str,
    step: int | None = None,
) -> list[str]:
    if query.limit <= 0:
        return _load_market_tickers_slice(conn, query.where_sql, query.params, None)
    advance = step if step is not None and step > 0 else query.limit
    total = _market_tickers_count(conn, query.where_sql, query.params)
    if total <= query.limit:
        return _load_market_tickers_slice(
            conn,
            query.where_sql,
            query.params,
            query.limit,
        )
    cursor = _load_state_int(conn, cursor_key, 0)
    offset = cursor % total
    first_count = min(query.limit, total - offset)
    rows = _load_market_tickers_slice(
        conn,
        query.where_sql,
        query.params,
        first_count,
        offset,
    )
    if first_count < query.limit:
        rows.extend(
            _load_market_tickers_slice(
                conn,
                query.where_sql,
                query.params,
                query.limit - first_count,
                0,
            )
        )
    _store_state_int(conn, cursor_key, cursor + advance)
    return rows


def load_market_tickers(
    conn: psycopg.Connection,
    limit: int,
    *,
    round_robin: bool = False,
    cursor_key: str | None = None,
    round_robin_step: int | None = None,
) -> list[str]:
    """Load market tickers from markets table ordered by close time."""
    where_sql, params = _market_tickers_where(1, 0)
    if round_robin:
        key = cursor_key or "ws_market_cursor:1:0"
        return _load_market_tickers_round_robin(
            conn,
            _ActiveTickerQuery(where_sql=where_sql, params=params, limit=limit),
            key,
            step=round_robin_step,
        )
    if limit <= 0:
        return _load_market_tickers_slice(conn, where_sql, params, None)
    return _load_market_tickers_slice(conn, where_sql, params, limit)


def load_market_tickers_shard(
    conn: psycopg.Connection,
    limit: int,
    shard_count: int,
    shard_id: int,
    *,
    shard_key: str = "event",
    round_robin: bool = False,
    cursor_key: str | None = None,
    round_robin_step: int | None = None,
) -> list[str]:
    """Load market tickers for a shard from markets table."""
    if shard_count <= 1:
        return load_market_tickers(
            conn,
            limit,
            round_robin=round_robin,
            cursor_key=cursor_key,
            round_robin_step=round_robin_step,
        )
    if shard_id < 0 or shard_id >= shard_count:
        raise ValueError(f"Invalid shard id {shard_id} for shard_count {shard_count}")
    where_sql, params = _market_tickers_where(shard_count, shard_id, shard_key)
    if round_robin:
        key = cursor_key or f"ws_market_cursor:{shard_count}:{shard_id}"
        return _load_market_tickers_round_robin(
            conn,
            _ActiveTickerQuery(where_sql=where_sql, params=params, limit=limit),
            key,
            step=round_robin_step,
        )
    if limit <= 0:
        return _load_market_tickers_slice(conn, where_sql, params, None)
    return _load_market_tickers_slice(conn, where_sql, params, limit)
