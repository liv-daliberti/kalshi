"""Portal snapshot helpers for the web portal database."""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional, Tuple

import psycopg  # pylint: disable=import-error

from ..core.loop_utils import schema_path
from ..db.db import init_schema, maybe_init_schema
from .config import _env_bool, _env_int
from .db_events import EventCursor
from .db_pool import _db_connection
from .db_timing import timed_cursor
from .portal_utils import portal_logger as _portal_logger

_PORTAL_SNAPSHOT_REFRESH_LOCK = threading.Lock()
_PORTAL_SNAPSHOT_REFRESH_LAST = 0.0
_PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT = False
_PORTAL_SNAPSHOT_REFRESH_WARNED = False
_PORTAL_SNAPSHOT_SCHEMA_LOCK = threading.Lock()
_PORTAL_SNAPSHOT_SCHEMA_CHECKED = False
_PORTAL_SNAPSHOT_SCHEMA_READY = False
_PORTAL_SNAPSHOT_READY_WARNED = False

_PORTAL_SNAPSHOT_FUNC_SIGNATURE = (
    "portal_snapshot_json("
    "integer, text, text[], text, double precision, text, text, text, "
    "integer, text, text, text, text, text, text, boolean, boolean)"
)


PortalSnapshotCursors = Tuple[
    Optional[EventCursor],
    Optional[EventCursor],
    Optional[EventCursor],
]


def _snapshot_cursor_params(
    cursor: EventCursor | None,
) -> tuple[Any | None, str | None]:
    if cursor is None:
        return None, None
    return cursor.value, cursor.event_ticker


@dataclass(frozen=True)
class _SnapshotCursorParams:
    active_value: Any | None
    active_ticker: str | None
    scheduled_value: Any | None
    scheduled_ticker: str | None
    closed_value: Any | None
    closed_ticker: str | None

    @classmethod
    def from_cursors(
        cls,
        cursors: PortalSnapshotCursors,
    ) -> "_SnapshotCursorParams":
        """Create cursor params from the active/scheduled/closed cursors."""
        active, scheduled, closed = cursors
        return cls(
            *_snapshot_cursor_params(active),
            *_snapshot_cursor_params(scheduled),
            *_snapshot_cursor_params(closed),
        )


def _snapshot_categories(filters: "PortalFilters") -> list[str] | None:
    if filters.categories:
        return list(filters.categories)
    return None


def _portal_snapshot_args(
    limit: int,
    filters: "PortalFilters",
    cursor_params: _SnapshotCursorParams,
    *,
    use_status: bool | None = None,
) -> tuple[Any, ...]:
    queue_timeout = _env_int("WORK_QUEUE_LOCK_TIMEOUT_SECONDS", 900, minimum=10)
    if use_status is None:
        use_status = _portal_snapshot_use_status()
    return (
        limit,
        filters.search,
        _snapshot_categories(filters),
        filters.strike_period,
        filters.close_window_hours,
        filters.status,
        filters.sort,
        filters.order,
        queue_timeout,
        cursor_params.active_value,
        cursor_params.active_ticker,
        cursor_params.scheduled_value,
        cursor_params.scheduled_ticker,
        cursor_params.closed_value,
        cursor_params.closed_ticker,
        _portal_snapshot_include_health(),
        use_status,
    )


def _decode_portal_snapshot_payload(payload: Any | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


def _fetch_portal_snapshot_payload(
    conn: psycopg.Connection,
    args: tuple[Any, ...],
) -> dict[str, Any] | None:
    with timed_cursor(conn) as cur:
        cur.execute(
            """
            SELECT portal_snapshot_json(
              %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s,
              %s, %s,
              %s, %s,
              %s, %s
            )
            """,
            args,
        )
        row = cur.fetchone()
    payload = row[0] if row else None
    return _decode_portal_snapshot_payload(payload)


def _logger():
    return _portal_logger(__name__)


def _portal_snapshot_enabled() -> bool:
    return _env_bool("WEB_PORTAL_DB_SNAPSHOT_ENABLE", False)


def _portal_snapshot_require() -> bool:
    return _env_bool("WEB_PORTAL_DB_SNAPSHOT_REQUIRE", False)


def _portal_snapshot_auto_init() -> bool:
    return _env_bool("WEB_PORTAL_DB_SNAPSHOT_AUTO_INIT", False)


def _portal_snapshot_include_health() -> bool:
    return _env_bool("WEB_PORTAL_DB_SNAPSHOT_INCLUDE_HEALTH", True)


def _portal_snapshot_use_status() -> bool:
    return _env_bool("WEB_PORTAL_DB_SNAPSHOT_USE_STATUS", True)


def _portal_snapshot_refresh_sec() -> int:
    return _env_int("WEB_PORTAL_DB_SNAPSHOT_REFRESH_SEC", 60, minimum=5)


def _portal_snapshot_refresh_on_queue() -> bool:
    return _env_bool("WEB_PORTAL_DB_SNAPSHOT_REFRESH_ON_QUEUE", False)


def portal_snapshot_enabled() -> bool:
    """Return whether the portal snapshot feature is enabled."""
    return _portal_snapshot_enabled()


def portal_snapshot_require() -> bool:
    """Return whether portal snapshots are required to serve requests."""
    return _portal_snapshot_require()


def portal_snapshot_auto_init() -> bool:
    """Return whether portal snapshot schema should auto-init."""
    return _portal_snapshot_auto_init()


def portal_snapshot_include_health() -> bool:
    """Return whether portal snapshot payloads include health data."""
    return _portal_snapshot_include_health()


def portal_snapshot_use_status() -> bool:
    """Return whether portal snapshot payloads rely on status fields."""
    return _portal_snapshot_use_status()


def _portal_snapshot_status_columns_available(conn: psycopg.Connection) -> bool:
    with timed_cursor(conn) as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'portal_event_rollup'
              AND column_name IN (
                'status',
                'is_active',
                'is_scheduled',
                'is_closed'
              )
            """
        )
        row = cur.fetchone()
    return bool(row and row[0] == 4)


def _portal_snapshot_use_status_for_conn(conn: psycopg.Connection) -> bool:
    if not _portal_snapshot_use_status():
        return False
    try:
        return _portal_snapshot_status_columns_available(conn)
    except Exception:  # pylint: disable=broad-exception-caught
        return False


def portal_snapshot_refresh_sec() -> int:
    """Return the minimum refresh interval (seconds)."""
    return _portal_snapshot_refresh_sec()


def portal_snapshot_refresh_on_queue() -> bool:
    """Return whether queue events can trigger snapshot refresh."""
    return _portal_snapshot_refresh_on_queue()


def _running_tests() -> bool:
    return "PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules


def _portal_snapshot_table_exists(conn: psycopg.Connection) -> bool:
    with timed_cursor(conn) as cur:
        cur.execute(
            """
            SELECT relkind
            FROM pg_class
            WHERE oid = to_regclass('portal_event_rollup')
            """
        )
        row = cur.fetchone()
    if not row or not row[0]:
        return False
    return row[0] in {"r", "p"}


def _portal_snapshot_function_exists(conn: psycopg.Connection) -> bool:
    with timed_cursor(conn) as cur:
        cur.execute(
            "SELECT to_regprocedure(%s)",
            (_PORTAL_SNAPSHOT_FUNC_SIGNATURE,),
        )
        row = cur.fetchone()
    return bool(row and row[0])


def portal_snapshot_table_exists(conn: psycopg.Connection) -> bool:
    """Return True when the portal snapshot table exists."""
    return _portal_snapshot_table_exists(conn)


def portal_snapshot_function_exists(conn: psycopg.Connection) -> bool:
    """Return True when the portal snapshot function exists."""
    return _portal_snapshot_function_exists(conn)


def ensure_portal_snapshot_ready() -> bool:
    """Ensure portal snapshot table + function exist when enabled."""
    global _PORTAL_SNAPSHOT_READY_WARNED  # pylint: disable=global-statement
    if not _portal_snapshot_enabled():
        return False
    require = _portal_snapshot_require()
    auto_init = _portal_snapshot_auto_init()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        if require:
            raise RuntimeError("DATABASE_URL is not set; portal snapshot requires a DB.")
        _logger().warning(
            "Portal snapshot check skipped; DATABASE_URL is not set."
        )
        return False

    def _check_ready(conn: psycopg.Connection) -> bool:
        return _portal_snapshot_table_exists(conn) and _portal_snapshot_function_exists(
            conn
        )

    try:
        with _db_connection(connect_timeout=3, force_direct=True) as conn:
            ready = _check_ready(conn)
            if not ready and auto_init:
                init_schema(conn, schema_path=schema_path(__file__))
                ready = _check_ready(conn)
            if not ready:
                message = (
                    "Portal snapshot schema missing; run the migrator or set "
                    "WEB_PORTAL_DB_SNAPSHOT_AUTO_INIT=1."
                )
                if require:
                    raise RuntimeError(message)
                if not _PORTAL_SNAPSHOT_READY_WARNED:
                    _logger().warning(message)
                    _PORTAL_SNAPSHOT_READY_WARNED = True
            return ready
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if require:
            raise
        if not _PORTAL_SNAPSHOT_READY_WARNED:
            _logger().warning(
                "Portal snapshot readiness check failed: %s",
                exc,
            )
            _PORTAL_SNAPSHOT_READY_WARNED = True
        return False


def _ensure_portal_snapshot_schema(conn: psycopg.Connection) -> bool:
    global _PORTAL_SNAPSHOT_SCHEMA_CHECKED  # pylint: disable=global-statement
    global _PORTAL_SNAPSHOT_SCHEMA_READY  # pylint: disable=global-statement
    if _PORTAL_SNAPSHOT_SCHEMA_CHECKED:
        return _PORTAL_SNAPSHOT_SCHEMA_READY
    with _PORTAL_SNAPSHOT_SCHEMA_LOCK:
        if _PORTAL_SNAPSHOT_SCHEMA_CHECKED:
            return _PORTAL_SNAPSHOT_SCHEMA_READY
        _PORTAL_SNAPSHOT_SCHEMA_CHECKED = True
        if _portal_snapshot_table_exists(conn):
            _PORTAL_SNAPSHOT_SCHEMA_READY = True
            return True
        try:
            maybe_init_schema(conn, schema_path=schema_path(__file__))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if not conn.autocommit:
                conn.rollback()
            _logger().warning(
                "Portal snapshot schema init failed; run migrator if needed: %s",
                exc,
            )
            _PORTAL_SNAPSHOT_SCHEMA_READY = False
            return False
        _PORTAL_SNAPSHOT_SCHEMA_READY = _portal_snapshot_table_exists(conn)
        if not _PORTAL_SNAPSHOT_SCHEMA_READY:
            _logger().warning(
                "Portal snapshot schema missing: portal_event_rollup"
            )
        return _PORTAL_SNAPSHOT_SCHEMA_READY


def ensure_portal_snapshot_schema(conn: psycopg.Connection) -> bool:
    """Ensure portal snapshot schema exists for the connection."""
    return _ensure_portal_snapshot_schema(conn)


def refresh_portal_snapshot(*, concurrent: bool = True) -> None:
    """No-op for incremental rollups; keep schema check for safety."""
    _ = concurrent
    with _db_connection(autocommit=True, force_direct=True) as conn:
        if not _ensure_portal_snapshot_schema(conn):
            raise RuntimeError(
                "portal_event_rollup missing; run migrator or enable DB_INIT_SCHEMA."
            )


def _finish_portal_snapshot_refresh(success: bool) -> None:
    with _PORTAL_SNAPSHOT_REFRESH_LOCK:
        global _PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT  # pylint: disable=global-statement
        global _PORTAL_SNAPSHOT_REFRESH_LAST  # pylint: disable=global-statement
        _PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT = False
        if success:
            _PORTAL_SNAPSHOT_REFRESH_LAST = time.monotonic()


def finish_portal_snapshot_refresh(success: bool) -> None:
    """Finish a portal snapshot refresh, updating state."""
    _finish_portal_snapshot_refresh(success)


def _refresh_portal_snapshot(reason: str) -> bool:
    try:
        refresh_portal_snapshot()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        global _PORTAL_SNAPSHOT_REFRESH_WARNED  # pylint: disable=global-statement
        if not _PORTAL_SNAPSHOT_REFRESH_WARNED:
            _logger().warning(
                "Portal snapshot refresh failed (%s): %s",
                reason,
                exc,
            )
            _PORTAL_SNAPSHOT_REFRESH_WARNED = True
        _finish_portal_snapshot_refresh(False)
        return False
    _finish_portal_snapshot_refresh(True)
    return True


def refresh_portal_snapshot_internal(reason: str) -> bool:
    """Refresh the portal snapshot and update refresh state."""
    return _refresh_portal_snapshot(reason)


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


def fetch_portal_snapshot(
    conn: psycopg.Connection,
    limit: int,
    filters: "PortalFilters",
    cursors: PortalSnapshotCursors = (None, None, None),
) -> dict[str, Any] | None:
    """Fetch the portal snapshot JSON payload from the database."""
    if not _ensure_portal_snapshot_schema(conn):
        return None
    cursor_params = _SnapshotCursorParams.from_cursors(cursors)
    use_status = _portal_snapshot_use_status_for_conn(conn)
    args = _portal_snapshot_args(limit, filters, cursor_params, use_status=use_status)
    return _fetch_portal_snapshot_payload(conn, args)
