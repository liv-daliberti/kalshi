"""Simple web portal for browsing active and closed Kalshi markets."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

from flask import (  # pylint: disable=import-error
    Flask,
    jsonify,
    render_template,
    request,
    url_for,
)

from .auth_utils import is_authenticated, require_password
from .category_utils import build_category_filters
from .config import (
    CLOSE_WINDOW_OPTIONS,
    EVENT_ORDER_OPTIONS,
    EVENT_SORT_OPTIONS,
    EVENT_STATUS_OPTIONS,
    _env_bool,
    _env_int,
    _parse_csv,
)
from ..db.db import ensure_schema_compatible
from .db import (
    _db_connection,
    build_event_snapshot,
    EventCursor,
    fetch_active_event_categories,
    fetch_active_events,
    fetch_closed_events,
    fetch_closed_filled_count,
    fetch_counts,
    fetch_portal_snapshot,
    fetch_scheduled_events,
    fetch_strike_periods,
)
from .filter_params import build_filter_params
from .health_utils import build_portal_health_from_snapshot
from .portal_filters import PortalFilters, _parse_portal_filters
from .portal_limits import clamp_limit
from .portal_app_factory import _create_portal_app
from .portal_models import (
    PortalContextArgs,
    PortalCursorTokens,
    PortalData,
    PortalDataContext,
    PortalDiagnostics,
    PortalFilterContext,
    PortalPaging,
    PortalRows,
    PortalTotals,
)
from .portal_paging import (
    _decode_cursor_token,
    _decode_portal_cursors,
    _encode_cursor_token,
    _parse_portal_paging,
    _portal_data_cache_key,
    _snapshot_cursor_tokens,
)
from .portal_utils import portal_func as _portal_func
from .formatters import fmt_ts

if __name__.endswith("._portal_part2") or __name__ == "__main__":
    from ._portal_part1 import (  # pragma: no cover - lint-only imports
        _PORTAL_DATA_CACHE,
        _PORTAL_DATA_CACHE_LOCK,
        _load_portal_health,
        describe_event_scope,
        fmt_time_remaining,
        logger,
    )

_PORTAL_SNAPSHOT_REFRESH_DISABLED_WARNED = False
_PORTAL_SNAPSHOT_QUERY_WARNED = False


def _portal_data_cache_ttl() -> int:
    return _env_int("WEB_PORTAL_DATA_CACHE_SEC", 60, minimum=0)


def _portal_lazy_load_enabled() -> bool:
    """Return True when the portal should lazy-load table data."""
    return _env_bool("WEB_PORTAL_LAZY_LOAD", True)


def _load_portal_data_cache(
    cache_key: tuple[Any, ...],
    ttl_sec: int,
) -> PortalData | None:
    if ttl_sec <= 0:
        return None
    now = time.monotonic()
    with _PORTAL_DATA_CACHE_LOCK:
        cached = _PORTAL_DATA_CACHE.get(cache_key)
        if not cached:
            return None
        cached_ts, payload = cached
        if now - cached_ts > ttl_sec:
            _PORTAL_DATA_CACHE.pop(cache_key, None)
            return None
        return payload


def _store_portal_data_cache(
    cache_key: tuple[Any, ...],
    ttl_sec: int,
    payload: PortalData,
) -> None:
    if ttl_sec <= 0:
        return
    with _PORTAL_DATA_CACHE_LOCK:
        _PORTAL_DATA_CACHE[cache_key] = (time.monotonic(), payload)


def _portal_db_snapshot_enabled() -> bool:
    return _env_bool("WEB_PORTAL_DB_SNAPSHOT_ENABLE", False)


def _portal_include_health() -> bool:
    return _env_bool("WEB_PORTAL_DB_SNAPSHOT_INCLUDE_HEALTH", True)


def _portal_db_snapshot_refresh_sec() -> int:
    return _env_int("WEB_PORTAL_DB_SNAPSHOT_REFRESH_SEC", 60, minimum=5)


def _start_portal_db_snapshot_refresh() -> None:
    """No-op now that rollups are incremental; keep for backwards compat."""
    global _PORTAL_SNAPSHOT_REFRESH_DISABLED_WARNED  # pylint: disable=global-statement
    if not _portal_db_snapshot_enabled():
        return
    if _PORTAL_SNAPSHOT_REFRESH_DISABLED_WARNED:
        return
    logger.info(
        "portal_event_rollup is incremental; background refresh thread disabled"
    )
    _PORTAL_SNAPSHOT_REFRESH_DISABLED_WARNED = True


def _portal_filter_fields(filters: PortalFilters) -> list[dict[str, Any]]:
    """Build filter field entries for template rendering."""
    fields = [
        {"name": "search", "value": filters.search},
        {"name": "strike_period", "value": filters.strike_period},
        {"name": "close_window", "value": filters.close_window},
        {"name": "status", "value": filters.status},
        {"name": "sort", "value": filters.sort},
        {"name": "order", "value": filters.order},
    ]
    for category in filters.categories:
        fields.append({"name": "category", "value": category})
    return fields


def _empty_portal_data(error: str | None = None) -> PortalData:
    """Return an empty portal payload with an optional error."""
    return PortalData(
        rows=PortalRows(active=[], scheduled=[], closed=[]),
        totals=PortalTotals(active=0, scheduled=0, closed=0),
        strike_periods=[],
        active_categories=[],
        diagnostics=PortalDiagnostics(health=None, error=error),
        closed_filled_total=0,
    )


def _portal_shell_data(filters: PortalFilters) -> PortalData:
    """Return a minimal portal payload for lazy-loading."""
    strike_periods = list(_parse_csv(os.getenv("STRIKE_PERIODS", "hour,day")))
    if filters.strike_period and filters.strike_period not in strike_periods:
        strike_periods.append(filters.strike_period)
    return PortalData(
        rows=PortalRows(active=[], scheduled=[], closed=[]),
        totals=PortalTotals(active=0, scheduled=0, closed=0),
        strike_periods=strike_periods,
        active_categories=[],
        diagnostics=PortalDiagnostics(health=None, error=None),
        closed_filled_total=0,
    )


def _snapshot_rows_and_cursors(
    payload: dict[str, Any],
    filters: PortalFilters,
) -> tuple[PortalRows, PortalCursorTokens]:
    """Build row payloads and cursor tokens from snapshot data."""
    active_raw = payload.get("active_rows") or []
    scheduled_raw = payload.get("scheduled_rows") or []
    closed_raw = payload.get("closed_rows") or []
    cursor_tokens = _snapshot_cursor_tokens(
        active_raw,
        scheduled_raw,
        closed_raw,
        filters,
    )
    time_remaining = _portal_func("fmt_time_remaining", fmt_time_remaining)
    active_rows = [
        {
            **build_event_snapshot(row),
            "time_remaining": time_remaining(row.get("close_time")),
        }
        for row in active_raw
    ]
    scheduled_rows = [build_event_snapshot(row) for row in scheduled_raw]
    closed_rows = [build_event_snapshot(row) for row in closed_raw]
    rows = PortalRows(
        active=active_rows,
        scheduled=scheduled_rows,
        closed=closed_rows,
    )
    return rows, cursor_tokens


def _portal_data_from_snapshot(payload: dict[str, Any], filters: PortalFilters) -> PortalData:
    """Build PortalData from a DB snapshot payload."""
    rows, cursor_tokens = _snapshot_rows_and_cursors(payload, filters)
    totals = PortalTotals(
        active=int(payload.get("active_total") or 0),
        scheduled=int(payload.get("scheduled_total") or 0),
        closed=int(payload.get("closed_total") or 0),
    )
    closed_filled_total = payload.get("closed_filled_total")
    if closed_filled_total is None:
        try:
            with _db_connection() as conn:
                closed_filled_total = fetch_closed_filled_count(conn, filters)
        except Exception:  # pylint: disable=broad-exception-caught
            closed_filled_total = 0
    else:
        closed_filled_total = int(closed_filled_total or 0)
    health_raw = payload.get("health_raw")
    return PortalData(
        rows=rows,
        totals=totals,
        closed_filled_total=closed_filled_total,
        strike_periods=list(payload.get("strike_periods") or []),
        active_categories=list(payload.get("active_categories") or []),
        diagnostics=PortalDiagnostics(
            health=build_portal_health_from_snapshot(health_raw),
            error=payload.get("error"),
        ),
        cursors=cursor_tokens,
    )


def _split_rows_and_cursor(result: Any) -> tuple[list[dict[str, Any]], EventCursor | None]:
    if isinstance(result, tuple) and len(result) == 2:
        return result
    if result is None:
        return [], None
    return result, None


def _fetch_portal_snapshot_data(
    limit: int,
    filters: PortalFilters,
    cursors: tuple[EventCursor | None, EventCursor | None, EventCursor | None],
) -> PortalData | None:
    try:
        with _db_connection() as conn:
            payload = fetch_portal_snapshot(
                conn,
                limit,
                filters,
                cursors=cursors,
            )
        if payload:
            return _portal_data_from_snapshot(payload, filters)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        global _PORTAL_SNAPSHOT_QUERY_WARNED  # pylint: disable=global-statement
        if not _PORTAL_SNAPSHOT_QUERY_WARNED:
            logger.warning(
                "Portal snapshot query failed; falling back to live queries: %s",
                exc,
            )
            _PORTAL_SNAPSHOT_QUERY_WARNED = True
    return None


def _fetch_portal_rows(
    conn,
    limit: int,
    filters: PortalFilters,
    cursors: tuple[EventCursor | None, EventCursor | None, EventCursor | None],
) -> tuple[PortalRows, PortalTotals, PortalCursorTokens]:
    totals = PortalTotals(*fetch_counts(conn, filters))
    active_rows, active_page_cursor = _split_rows_and_cursor(
        fetch_active_events(
            conn,
            limit,
            filters,
            cursor=cursors[0],
            return_cursor=True,
        )
    )
    scheduled_rows, scheduled_page_cursor = _split_rows_and_cursor(
        fetch_scheduled_events(
            conn,
            limit,
            filters,
            cursor=cursors[1],
            return_cursor=True,
        )
    )
    closed_rows, closed_page_cursor = _split_rows_and_cursor(
        fetch_closed_events(
            conn,
            limit,
            filters,
            cursor=cursors[2],
            return_cursor=True,
        )
    )
    cursor_tokens = PortalCursorTokens(
        active=_encode_cursor_token(active_page_cursor),
        scheduled=_encode_cursor_token(scheduled_page_cursor),
        closed=_encode_cursor_token(closed_page_cursor),
    )
    rows = PortalRows(
        active=active_rows,
        scheduled=scheduled_rows,
        closed=closed_rows,
    )
    return rows, totals, cursor_tokens


def _fetch_portal_live_data(
    limit: int,
    filters: PortalFilters,
    cursors: tuple[EventCursor | None, EventCursor | None, EventCursor | None],
) -> PortalData:
    with _db_connection() as conn:
        rows, totals, cursor_tokens = _fetch_portal_rows(conn, limit, filters, cursors)
        try:
            closed_filled_total = fetch_closed_filled_count(conn, filters)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Portal closed filled count failed: %s", exc)
            closed_filled_total = 0
        return PortalData(
            rows=rows,
            totals=totals,
            closed_filled_total=closed_filled_total,
            strike_periods=fetch_strike_periods(conn),
            active_categories=fetch_active_event_categories(conn, filters),
            diagnostics=PortalDiagnostics(
                health=_load_portal_health(conn) if _portal_include_health() else None,
                error=None,
            ),
            cursors=cursor_tokens,
        )


def _fetch_portal_data(
    limit: int,
    filters: PortalFilters,
    paging: PortalPaging,
) -> PortalData:
    """Fetch portal data rows and counts."""
    ttl_sec = _portal_data_cache_ttl()
    cache_key = _portal_data_cache_key(limit, filters, paging)
    cached = _load_portal_data_cache(cache_key, ttl_sec)
    if cached is not None:
        return cached
    try:
        cursors = _decode_portal_cursors(paging)
        if _portal_db_snapshot_enabled():
            snapshot = _fetch_portal_snapshot_data(limit, filters, cursors)
            if snapshot is not None:
                _store_portal_data_cache(cache_key, ttl_sec, snapshot)
                return snapshot
        data = _fetch_portal_live_data(limit, filters, cursors)
        _store_portal_data_cache(cache_key, ttl_sec, data)
        return data
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("web portal query failed")
        data = _empty_portal_data(str(exc))
        _store_portal_data_cache(cache_key, ttl_sec, data)
        return data


def _portal_context(
    *,
    args: PortalContextArgs,
) -> dict[str, Any]:
    """Build the render context for the portal template."""
    refreshed_at = fmt_ts(datetime.now(timezone.utc))
    data = args.data_context.data
    filter_ctx = args.filter_context
    active_shown = args.paging.active_offset + len(data.rows.active)
    scheduled_shown = args.paging.scheduled_offset + len(data.rows.scheduled)
    closed_shown = args.paging.closed_offset + len(data.rows.closed)
    active_has_more = active_shown < data.totals.active
    scheduled_has_more = scheduled_shown < data.totals.scheduled
    closed_has_more = closed_shown < data.totals.closed
    return {
        "error": data.error,
        "active_rows": data.rows.active,
        "scheduled_rows": data.rows.scheduled,
        "closed_rows": data.rows.closed,
        "active_total": data.totals.active,
        "scheduled_total": data.totals.scheduled,
        "closed_total": data.totals.closed,
        "closed_filled_total": data.closed_filled_total,
        "limit": args.limit,
        "active_page": args.paging.active_page,
        "scheduled_page": args.paging.scheduled_page,
        "closed_page": args.paging.closed_page,
        "active_has_more": active_has_more,
        "scheduled_has_more": scheduled_has_more,
        "closed_has_more": closed_has_more,
        "load_more_links": args.data_context.load_more_links,
        "refreshed_at": refreshed_at,
        "logged_in": is_authenticated(),
        "scope_note": args.scope_note,
        "search": args.filters.search or "",
        "selected_categories": filter_ctx.selected_categories,
        "selected_strike_period": args.filters.strike_period or "",
        "selected_close_window": args.filters.close_window or "",
        "selected_status": args.filters.status or "",
        "selected_sort": args.filters.sort or "",
        "selected_order": args.filters.order or "",
        "strike_periods": data.strike_periods,
        "category_filters": filter_ctx.category_filters,
        "filter_fields": filter_ctx.filter_fields,
        "close_window_options": CLOSE_WINDOW_OPTIONS,
        "status_options": EVENT_STATUS_OPTIONS,
        "sort_options": EVENT_SORT_OPTIONS,
        "order_options": EVENT_ORDER_OPTIONS,
        "health": data.health,
        "lazy_load": args.lazy_load,
    }


def _build_load_more_links(
    *,
    limit: int,
    filters: PortalFilters,
    paging: PortalPaging,
    cursors: PortalCursorTokens | None,
) -> dict[str, str]:
    if not cursors:
        return {}
    base_params = build_filter_params(
        limit,
        filters,
        include_category=True,
        page_params=paging.as_params(),
    )

    def _link(page_key: str, cursor_key: str, next_page: int, token: str | None) -> str:
        if not token:
            return ""
        params = {**base_params, page_key: next_page, cursor_key: token}
        return url_for("index", **params)

    return {
        "active": _link(
            "active_page",
            "active_after",
            paging.active_page + 1,
            cursors.active,
        ),
        "scheduled": _link(
            "scheduled_page",
            "scheduled_after",
            paging.scheduled_page + 1,
            cursors.scheduled,
        ),
        "closed": _link(
            "closed_page",
            "closed_after",
            paging.closed_page + 1,
            cursors.closed,
        ),
    }


def index():
    """Render the main portal view.

    :return: Rendered HTML response.
    :rtype: flask.Response
    """
    scope_note = describe_event_scope()
    password_error = None
    try:
        require_password()
    except RuntimeError as exc:
        password_error = str(exc)

    limit = clamp_limit(request.args.get("limit") or os.getenv("WEB_PORTAL_LIMIT"))
    filters = _parse_portal_filters(request.args)
    paging = _parse_portal_paging(request.args, limit)
    filter_fields = _portal_filter_fields(filters)
    selected_categories = list(filters.categories)
    lazy_load = _portal_lazy_load_enabled()
    if password_error or not os.getenv("DATABASE_URL"):
        error_message = password_error or "DATABASE_URL is not set."
        data = _empty_portal_data(error_message)
        return render_template(
            "portal.html",
            **_portal_context(
                args=PortalContextArgs(
                    limit=limit,
                    filters=filters,
                    paging=paging,
                    scope_note=scope_note,
                    filter_context=PortalFilterContext(
                        selected_categories=selected_categories,
                        category_filters=[],
                        filter_fields=filter_fields,
                    ),
                    data_context=PortalDataContext(
                        data=data,
                        load_more_links={},
                    ),
                    lazy_load=False,
                ),
            ),
        )

    if lazy_load:
        data = _portal_shell_data(filters)
        category_filters = build_category_filters(
            active_categories=data.active_categories,
            selected_categories=selected_categories,
            base_params=build_filter_params(limit, filters, include_category=False),
            endpoint="index",
            url_for=url_for,
        )
        load_more_links = _build_load_more_links(
            limit=limit,
            filters=filters,
            paging=paging,
            cursors=data.cursors,
        )
        return render_template(
            "portal.html",
            **_portal_context(
                args=PortalContextArgs(
                    limit=limit,
                    filters=filters,
                    paging=paging,
                    scope_note=scope_note,
                    filter_context=PortalFilterContext(
                        selected_categories=selected_categories,
                        category_filters=category_filters,
                        filter_fields=filter_fields,
                    ),
                    data_context=PortalDataContext(
                        data=data,
                        load_more_links=load_more_links,
                    ),
                    lazy_load=True,
                ),
            ),
        )

    data = _fetch_portal_data(limit, filters, paging)
    category_filters = build_category_filters(
        active_categories=data.active_categories,
        selected_categories=selected_categories,
        base_params=build_filter_params(limit, filters, include_category=False),
        endpoint="index",
        url_for=url_for,
    )
    load_more_links = _build_load_more_links(
        limit=limit,
        filters=filters,
        paging=paging,
        cursors=data.cursors,
    )
    return render_template(
        "portal.html",
        **_portal_context(
            args=PortalContextArgs(
                limit=limit,
                filters=filters,
                paging=paging,
                scope_note=scope_note,
                filter_context=PortalFilterContext(
                    selected_categories=selected_categories,
                    category_filters=category_filters,
                    filter_fields=filter_fields,
                ),
                data_context=PortalDataContext(
                    data=data,
                    load_more_links=load_more_links,
                ),
                lazy_load=False,
            ),
        ),
    )


def _portal_data_payload(context: dict[str, Any]) -> dict[str, Any]:
    """Build JSON payload for portal data refreshes."""
    return {
        "refreshed_at": context.get("refreshed_at"),
        "error": context.get("error"),
        "health": context.get("health"),
        "rows": {
            "active": context.get("active_rows") or [],
            "scheduled": context.get("scheduled_rows") or [],
            "closed": context.get("closed_rows") or [],
        },
        "totals": {
            "active": context.get("active_total") or 0,
            "scheduled": context.get("scheduled_total") or 0,
            "closed": context.get("closed_total") or 0,
            "closed_filled": context.get("closed_filled_total") or 0,
        },
        "load_more_links": context.get("load_more_links") or {},
        "category_filters": context.get("category_filters") or [],
        "has_more": {
            "active": bool(context.get("active_has_more")),
            "scheduled": bool(context.get("scheduled_has_more")),
            "closed": bool(context.get("closed_has_more")),
        },
        "limit": context.get("limit"),
        "paging": {
            "active_page": context.get("active_page"),
            "scheduled_page": context.get("scheduled_page"),
            "closed_page": context.get("closed_page"),
        },
        "scope_note": context.get("scope_note"),
    }


def portal_data():
    """Return portal data as JSON for incremental updates."""
    scope_note = describe_event_scope()
    password_error = None
    try:
        require_password()
    except RuntimeError as exc:
        password_error = str(exc)

    limit = clamp_limit(request.args.get("limit") or os.getenv("WEB_PORTAL_LIMIT"))
    filters = _parse_portal_filters(request.args)
    paging = _parse_portal_paging(request.args, limit)
    filter_fields = _portal_filter_fields(filters)
    selected_categories = list(filters.categories)

    if password_error or not os.getenv("DATABASE_URL"):
        error_message = password_error or "DATABASE_URL is not set."
        data = _empty_portal_data(error_message)
        context = _portal_context(
            args=PortalContextArgs(
                limit=limit,
                filters=filters,
                paging=paging,
                scope_note=scope_note,
                filter_context=PortalFilterContext(
                    selected_categories=selected_categories,
                    category_filters=[],
                    filter_fields=filter_fields,
                ),
                data_context=PortalDataContext(
                    data=data,
                    load_more_links={},
                ),
                lazy_load=False,
            ),
        )
        return jsonify(_portal_data_payload(context))

    data = _fetch_portal_data(limit, filters, paging)
    category_filters = build_category_filters(
        active_categories=data.active_categories,
        selected_categories=selected_categories,
        base_params=build_filter_params(limit, filters, include_category=False),
        endpoint="index",
        url_for=url_for,
    )
    load_more_links = _build_load_more_links(
        limit=limit,
        filters=filters,
        paging=paging,
        cursors=data.cursors,
    )
    context = _portal_context(
        args=PortalContextArgs(
            limit=limit,
            filters=filters,
            paging=paging,
            scope_note=scope_note,
            filter_context=PortalFilterContext(
                selected_categories=selected_categories,
                category_filters=category_filters,
                filter_fields=filter_fields,
            ),
            data_context=PortalDataContext(
                data=data,
                load_more_links=load_more_links,
            ),
            lazy_load=False,
        ),
    )
    return jsonify(_portal_data_payload(context))


def main() -> None:
    """Run the web portal server.

    :return: None.
    :rtype: None
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set; cannot start portal.")
    try:
        with _db_connection(connect_timeout=3) as conn:
            ensure_schema_compatible(conn)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Schema compatibility check failed: %s", exc)
        raise
    port = int(os.getenv("WEB_PORTAL_PORT", "8000"))
    host = os.getenv("WEB_PORTAL_HOST", "0.0.0.0")
    threads = _env_int("WEB_PORTAL_THREADS", 1, minimum=1)
    threaded = _env_bool("WEB_PORTAL_THREADED", threads > 1)
    portal_app = globals().get("app")
    if not isinstance(portal_app, Flask):
        portal_app = _create_portal_app()
        globals()["app"] = portal_app
    portal_app.run(host=host, port=port, threaded=threaded)


app = _create_portal_app()  # Module-level app for tests and WSGI imports.
if __name__ == "__main__":
    main()
