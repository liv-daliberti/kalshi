"""Database helpers for the web portal."""

from __future__ import annotations

from ..db.db import normalize_prob_dollars
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
from .db_arbitrage import fetch_arbitrage_opportunities
from .db_opportunities import _build_opportunity_where, fetch_opportunities
from .db_pool import (
    ConnectionPool,
    PoolTimeout,
    _db_connection,
    _get_db_pool,
    _maybe_prewarm_db_pool,
)
from .db_snapshot import (
    PortalSnapshotCursors,
    ensure_portal_snapshot_ready,
    fetch_portal_snapshot,
    maybe_refresh_portal_snapshot,
    refresh_portal_snapshot,
    ensure_portal_snapshot_schema as _ensure_portal_snapshot_schema_impl,
    finish_portal_snapshot_refresh as _finish_portal_snapshot_refresh_impl,
    portal_snapshot_auto_init as _portal_snapshot_auto_init_impl,
    portal_snapshot_enabled as _portal_snapshot_enabled_impl,
    portal_snapshot_function_exists as _portal_snapshot_function_exists_impl,
    portal_snapshot_include_health as _portal_snapshot_include_health_impl,
    portal_snapshot_refresh_on_queue as _portal_snapshot_refresh_on_queue_impl,
    portal_snapshot_refresh_sec as _portal_snapshot_refresh_sec_impl,
    portal_snapshot_require as _portal_snapshot_require_impl,
    portal_snapshot_table_exists as _portal_snapshot_table_exists_impl,
    portal_snapshot_use_status as _portal_snapshot_use_status_impl,
    refresh_portal_snapshot_internal as _refresh_portal_snapshot_impl,
)
from .db_events import (
    EventCursor,
    _cursor_from_rows,
    _fetch_state_rows,
    _normalize_cursor_value,
    build_event_snapshot,
    fetch_active_event_categories,
    fetch_active_events,
    fetch_closed_events,
    fetch_closed_filled_count,
    fetch_counts,
    fetch_event_categories,
    fetch_scheduled_events,
    fetch_strike_periods,
)

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
    "_db_connection",
    "_get_db_pool",
    "_maybe_prewarm_db_pool",
    "PortalSnapshotCursors",
    "EventCursor",
    "_cursor_from_rows",
    "_fetch_state_rows",
    "_normalize_cursor_value",
    "build_event_snapshot",
    "fetch_active_event_categories",
    "fetch_active_events",
    "fetch_closed_events",
    "fetch_closed_filled_count",
    "fetch_counts",
    "fetch_event_categories",
    "fetch_scheduled_events",
    "fetch_strike_periods",
    "fetch_portal_snapshot",
    "ensure_portal_snapshot_ready",
    "maybe_refresh_portal_snapshot",
    "refresh_portal_snapshot",
    "fetch_event_detail",
    "fetch_market_detail",
    "fetch_arbitrage_opportunities",
    "fetch_opportunities",
]


def _portal_snapshot_enabled() -> bool:
    return _portal_snapshot_enabled_impl()


def _portal_snapshot_require() -> bool:
    return _portal_snapshot_require_impl()


def _portal_snapshot_auto_init() -> bool:
    return _portal_snapshot_auto_init_impl()


def _portal_snapshot_include_health() -> bool:
    return _portal_snapshot_include_health_impl()


def _portal_snapshot_use_status() -> bool:
    return _portal_snapshot_use_status_impl()


def _portal_snapshot_refresh_sec() -> int:
    return _portal_snapshot_refresh_sec_impl()


def _portal_snapshot_refresh_on_queue() -> bool:
    return _portal_snapshot_refresh_on_queue_impl()


def _portal_snapshot_table_exists(conn):
    return _portal_snapshot_table_exists_impl(conn)


def _portal_snapshot_function_exists(conn):
    return _portal_snapshot_function_exists_impl(conn)


def _ensure_portal_snapshot_schema(conn) -> bool:
    return _ensure_portal_snapshot_schema_impl(conn)


def _finish_portal_snapshot_refresh(success: bool) -> None:
    _finish_portal_snapshot_refresh_impl(success)


def _refresh_portal_snapshot(reason: str) -> bool:
    return _refresh_portal_snapshot_impl(reason)
