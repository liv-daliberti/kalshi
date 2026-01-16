"""Simple web portal for browsing active and closed Kalshi markets."""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg  # pylint: disable=import-error
from flask import (  # pylint: disable=import-error
    Flask,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from src.core.time_utils import infer_strike_period_from_times
from .auth_utils import is_authenticated, require_password
from .category_utils import build_category_filters
from .config import (
    CLOSE_WINDOW_OPTIONS,
    EVENT_ORDER_OPTIONS,
    EVENT_SORT_OPTIONS,
    EVENT_STATUS_OPTIONS,
    _env_bool,
    _env_float,
    _env_int,
    _fmt_hours,
    _parse_csv,
)
from . import formatters as _formatters
from .db_utils import ensure_schema_compatible, insert_market_tick
from .db import (
    _db_connection,
    _maybe_prewarm_db_pool,
    build_event_snapshot,
    fetch_active_event_categories,
    fetch_active_events,
    fetch_closed_events,
    fetch_counts,
    fetch_portal_snapshot,
    fetch_scheduled_events,
    fetch_strike_periods,
    maybe_refresh_portal_snapshot,
)
from .db_timing import get_db_metrics, reset_db_metrics, timed_cursor
from .market_metadata import (
    _derive_custom_strike,
    _extract_event_metadata,
    _maybe_parse_json,
    _update_event_metadata,
    _update_market_extras,
)
from .filter_params import build_filter_params
from .health_utils import _load_portal_health, build_portal_health_from_snapshot
from .portal_filters import PortalFilters, _parse_portal_filters
from .portal_limits import clamp_limit, clamp_page
from .portal_utils import portal_func as _portal_func
from .queue_stream_utils import _queue_stream_enabled, _queue_stream_min_reload_ms
from .formatters import _now_utc, _parse_ts, fmt_ts
from .snapshot_utils import (
    _set_snapshot_backoff,
    _snapshot_backoff_remaining,
    fetch_live_snapshot,
)

logger = logging.getLogger(__name__)

_MARKET_METADATA_EXPORTS = (
    _derive_custom_strike,
    _extract_event_metadata,
    _maybe_parse_json,
    _update_event_metadata,
    _update_market_extras,
)

_DB_POOL = None
_DB_POOL_LOCK = threading.Lock()

_KALSHI_CLIENT = None
_KALSHI_CLIENT_ERROR: str | None = None
_SNAPSHOT_THREAD_STARTED = False
_SNAPSHOT_THREAD_LOCK = threading.Lock()
_SNAPSHOT_LAST_ATTEMPT: dict[str, float] = {}
_EVENT_METADATA_CACHE: dict[str, tuple[float, Any | None, str | None]] = {}
_EVENT_METADATA_CACHE_LOCK = threading.Lock()
_PORTAL_DATA_CACHE: dict[tuple[Any, ...], tuple[float, "PortalData"]] = {}
_PORTAL_DATA_CACHE_LOCK = threading.Lock()
_PORTAL_SNAPSHOT_REFRESH_THREAD_STARTED = False
_PORTAL_SNAPSHOT_REFRESH_THREAD_LOCK = threading.Lock()
_PORTAL_SNAPSHOT_QUERY_WARNED = False


def inject_queue_stream_enabled() -> dict[str, bool]:
    """Expose queue stream flags to templates."""
    return {
        "queue_stream_enabled": _queue_stream_enabled(),
        "queue_stream_min_reload_ms": _queue_stream_min_reload_ms(),
    }


def register_routes(app: Flask) -> None:
    """Register portal routes and request hooks on the Flask app."""
    _maybe_prewarm_db_pool()
    app.context_processor(inject_queue_stream_enabled)
    app.before_request(start_request_timer)
    app.before_request(ensure_snapshot_polling)
    app.before_request(enforce_login)
    app.after_request(log_request_timing)
    app.add_url_rule("/", "index", index, methods=["GET"])


def start_request_timer() -> None:
    """Start request timing and reset DB counters."""
    g.request_start = time.perf_counter()
    reset_db_metrics()


def log_request_timing(response):
    """Log total and DB time for the request."""
    start = getattr(g, "request_start", None)
    if start is None:
        return response
    total_ms = (time.perf_counter() - start) * 1000
    db_ms, db_queries = get_db_metrics()
    logger.info(
        "request timing: method=%s path=%s endpoint=%s status=%s "
        "total_ms=%.1f db_ms=%.1f db_queries=%d",
        request.method,
        request.path,
        request.endpoint,
        response.status_code,
        total_ms,
        db_ms,
        db_queries,
    )
    return response


def _human_join(items: list[str]) -> str:
    """Join a list into a human-friendly phrase."""
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return f"{', '.join(items[:-1])}, and {items[-1]}"


def describe_event_scope() -> str | None:
    """Describe the strike-period window applied to events."""
    strike_periods = _parse_csv(os.getenv("STRIKE_PERIODS", "hour,day"))
    if not strike_periods:
        return None
    label_map = {
        "hour": "hourly",
        "day": "daily",
        "week": "weekly",
        "month": "monthly",
        "year": "yearly",
    }
    labels = [label_map.get(period, period) for period in strike_periods]
    label_text = _human_join(labels)
    note = f"Scope: {label_text} strike-period events only."
    duration_bits = []
    if "hour" in strike_periods:
        hour_max = _fmt_hours(os.getenv("STRIKE_HOUR_MAX_HOURS"), 2.0)
        duration_bits.append(f"hourly <= {hour_max}h")
    if "day" in strike_periods:
        day_max = _fmt_hours(os.getenv("STRIKE_DAY_MAX_HOURS"), 36.0)
        duration_bits.append(f"daily <= {day_max}h")
    if duration_bits:
        note += f"\nInferred duration cutoffs: {'; '.join(duration_bits)}."
    return note


def _load_open_market_tickers(
    conn: psycopg.Connection,
    limit: int,
    min_age_sec: int,
) -> list[str]:
    """Load tickers for markets that have not closed and are stale enough."""
    with timed_cursor(conn) as cur:
        cur.execute(
            """
            SELECT am.ticker
            FROM active_markets am
            LEFT JOIN LATERAL (
              SELECT t.ts
              FROM market_ticks t
              WHERE t.ticker = am.ticker
              ORDER BY t.ts DESC, t.id DESC
              LIMIT 1
            ) lt ON TRUE
            WHERE (am.close_time IS NULL OR am.close_time > NOW())
              AND (
                %s = 0
                OR lt.ts IS NULL
                OR lt.ts <= NOW() - (%s || ' seconds')::interval
              )
            ORDER BY am.close_time NULLS LAST
            LIMIT %s
            """,
            (min_age_sec, min_age_sec, limit),
        )
        return [r[0] for r in cur.fetchall()]


def _snapshot_poll_enabled() -> bool:
    """Return True if background snapshot polling is enabled."""
    raw = os.getenv("WEB_PORTAL_SNAPSHOT_POLL_ENABLE")
    if raw is None:
        return True
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class SnapshotDelayConfig:
    """Delay configuration for snapshot polling."""

    delay_ms: int
    jitter_ms: int


@dataclass(frozen=True)
class SnapshotPollConfig:
    """Configuration for the snapshot polling loop."""

    interval: int
    limit: int
    min_age_sec: int
    cooldown_sec: int
    jitter_sec: float
    min_attempt_sec: int
    delay: SnapshotDelayConfig


def _snapshot_poll_config() -> SnapshotPollConfig:
    return SnapshotPollConfig(
        interval=_env_int("WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC", 60, minimum=10),
        limit=_env_int("WEB_PORTAL_SNAPSHOT_POLL_LIMIT", 200, minimum=1),
        min_age_sec=_env_int("WEB_PORTAL_SNAPSHOT_MIN_AGE_SEC", 60, minimum=0),
        cooldown_sec=_env_int("WEB_PORTAL_SNAPSHOT_POLL_COOLDOWN_SEC", 30, minimum=5),
        jitter_sec=_env_float("WEB_PORTAL_SNAPSHOT_POLL_JITTER_SEC", 0.0, minimum=0.0),
        min_attempt_sec=_env_int("WEB_PORTAL_SNAPSHOT_MIN_ATTEMPT_SEC", 0, minimum=0),
        delay=SnapshotDelayConfig(
            delay_ms=_env_int("WEB_PORTAL_SNAPSHOT_POLL_DELAY_MS", 0, minimum=0),
            jitter_ms=_env_int(
                "WEB_PORTAL_SNAPSHOT_POLL_DELAY_JITTER_MS",
                0,
                minimum=0,
            ),
        ),
    )


def _log_snapshot_poll_config(config: SnapshotPollConfig) -> None:
    logger.info(
        "snapshot poller enabled (interval=%ss, jitter=%ss, limit=%s, min_age=%ss, "
        "min_attempt=%ss, delay_ms=%s, delay_jitter_ms=%s)",
        config.interval,
        config.jitter_sec,
        config.limit,
        config.min_age_sec,
        config.min_attempt_sec,
        config.delay.delay_ms,
        config.delay.jitter_ms,
    )


def _snapshot_poll_backoff() -> float:
    backoff = _snapshot_backoff_remaining()
    if backoff:
        logger.warning("snapshot poll backing off for %.1fs", backoff)
        time.sleep(backoff)
    return backoff


def _snapshot_attempt_allowed(ticker: str, min_attempt_sec: int) -> bool:
    if min_attempt_sec <= 0:
        return True
    now = time.monotonic()
    last_attempt = _SNAPSHOT_LAST_ATTEMPT.get(ticker)
    if last_attempt is not None and now - last_attempt < min_attempt_sec:
        return False
    _SNAPSHOT_LAST_ATTEMPT[ticker] = now
    return True


def _snapshot_delay(config: SnapshotPollConfig) -> None:
    if config.delay.delay_ms <= 0:
        return
    jitter_ms = (
        random.uniform(0, config.delay.jitter_ms)
        if config.delay.jitter_ms > 0
        else 0.0
    )
    time.sleep((config.delay.delay_ms + jitter_ms) / 1000)


def _snapshot_poll_result(
    conn: psycopg.Connection,
    ticker: str,
    config: SnapshotPollConfig,
) -> tuple[bool, bool]:
    data, snapshot_tick = fetch_live_snapshot(ticker)
    if data.get("rate_limited"):
        _set_snapshot_backoff(config.cooldown_sec)
        logger.warning(
            "snapshot poll rate limited; cooling down for %ss",
            config.cooldown_sec,
        )
        return False, True
    if "error" in data:
        logger.warning("snapshot poll failed for %s: %s", ticker, data.get("error"))
        return False, False
    if not snapshot_tick:
        logger.warning("snapshot poll returned no data for %s", ticker)
        return False, False
    try:
        insert_market_tick(conn, snapshot_tick)
        return True, False
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("snapshot poll insert failed for %s", ticker)
        return False, False


def _snapshot_poll_tickers(
    conn: psycopg.Connection,
    tickers: list[str],
    config: SnapshotPollConfig,
) -> tuple[int, int]:
    updated = 0
    errors = 0
    for ticker in tickers:
        if not _snapshot_attempt_allowed(ticker, config.min_attempt_sec):
            continue
        if _snapshot_poll_backoff():
            break
        did_update, rate_limited = _snapshot_poll_result(conn, ticker, config)
        if rate_limited:
            errors += 1
            break
        if did_update:
            updated += 1
        else:
            errors += 1
        _snapshot_delay(config)
    return updated, errors


def _snapshot_poll_cycle(config: SnapshotPollConfig) -> None:
    if _snapshot_poll_backoff():
        return
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.warning("snapshot poll skipped: DATABASE_URL is not set")
        return
    try:
        with _db_connection() as conn:
            tickers = _load_open_market_tickers(conn, config.limit, config.min_age_sec)
            if not tickers:
                logger.info("snapshot poll: no active tickers to update")
                return
            updated, errors = _snapshot_poll_tickers(conn, tickers, config)
            logger.info(
                "snapshot poll: updated %s/%s (errors=%s)",
                updated,
                len(tickers),
                errors,
            )
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("snapshot poll failed")


def _snapshot_poll_sleep(config: SnapshotPollConfig, started: float) -> None:
    elapsed = time.monotonic() - started
    sleep_for = max(0, config.interval - elapsed)
    if config.jitter_sec > 0:
        sleep_for += random.uniform(0, config.jitter_sec)
    time.sleep(sleep_for)


def _snapshot_poll_loop() -> None:
    """Poll live snapshots and persist ticks on an interval."""
    config = _snapshot_poll_config()
    _log_snapshot_poll_config(config)
    while True:
        started = time.monotonic()
        _snapshot_poll_cycle(config)
        _snapshot_poll_sleep(config, started)


def _start_snapshot_polling() -> None:
    """Start the snapshot polling thread once per process."""
    global _SNAPSHOT_THREAD_STARTED  # pylint: disable=global-statement
    if not _snapshot_poll_enabled():
        return
    if _SNAPSHOT_THREAD_STARTED:
        return
    with _SNAPSHOT_THREAD_LOCK:
        if _SNAPSHOT_THREAD_STARTED:
            return
        thread = threading.Thread(
            target=_snapshot_poll_loop, name="snapshot-poller", daemon=True
        )
        thread.start()
        _SNAPSHOT_THREAD_STARTED = True


def _infer_strike_period_from_times(
    open_time: Any,
    close_time: Any,
    hour_max: float,
    day_max: float,
) -> str | None:
    """Infer strike period from open/close timestamps."""
    return infer_strike_period_from_times(
        _parse_ts(open_time),
        _parse_ts(close_time),
        hour_max,
        day_max,
    )


def fmt_time_remaining(close_time: Any) -> str:
    """Format time remaining until close."""
    return _formatters.fmt_time_remaining(close_time, now=_now_utc())


def normalize_status(active_status: str | None, open_time, close_time) -> tuple[str, str]:
    """Normalize status label + class from data."""
    if active_status:
        label = active_status.title()
        return label, active_status.lower()
    now = datetime.now(timezone.utc)
    if close_time and close_time <= now:
        return "Closed", "closed"
    if open_time and open_time > now:
        return "Scheduled", "scheduled"
    return "Inactive", "inactive"


def ensure_snapshot_polling():
    """Kick off background snapshot polling when enabled."""
    _start_snapshot_polling()
    _start_portal_db_snapshot_refresh()


def enforce_login():
    """Redirect unauthenticated users to the login page.

    :return: Redirect response or None to continue.
    :rtype: flask.Response | None
    """
    if request.endpoint in {"auth.login", "auth.logout", "health.health", "static"}:
        return None
    if request.endpoint in {
        "event.event_snapshot",
        "market.market_snapshot",
        "market.market_backfill",
        "stream.queue_stream",
    }:
        if not is_authenticated():
            return jsonify({"error": "Authentication required."}), 401
        return None
    if not is_authenticated():
        return redirect(url_for("auth.login", next=request.path))
    return None


@dataclass(frozen=True)
class PortalRows:
    """Container for portal event rows."""

    active: list[dict[str, Any]]
    scheduled: list[dict[str, Any]]
    closed: list[dict[str, Any]]


@dataclass(frozen=True)
class PortalTotals:
    """Container for portal event totals."""

    active: int
    scheduled: int
    closed: int


@dataclass(frozen=True)
class PortalData:
    """Container for portal query results."""

    rows: PortalRows
    totals: PortalTotals
    strike_periods: list[str]
    active_categories: list[str]
    health: dict[str, Any] | None
    error: str | None


@dataclass(frozen=True)
class PortalPaging:
    """Pagination state for portal event tables."""

    limit: int
    active_page: int
    scheduled_page: int
    closed_page: int

    @property
    def active_offset(self) -> int:
        """Return offset for the active events page."""
        return self.active_page * self.limit

    @property
    def scheduled_offset(self) -> int:
        """Return offset for the scheduled events page."""
        return self.scheduled_page * self.limit

    @property
    def closed_offset(self) -> int:
        """Return offset for the closed events page."""
        return self.closed_page * self.limit

    def as_params(self) -> dict[str, int]:
        """Serialize paging to URL parameters."""
        return {
            "active_page": self.active_page,
            "scheduled_page": self.scheduled_page,
            "closed_page": self.closed_page,
        }


def _portal_data_cache_ttl() -> int:
    return _env_int("WEB_PORTAL_DATA_CACHE_SEC", 60, minimum=0)


def _portal_data_cache_key(
    limit: int,
    filters: PortalFilters,
    paging: PortalPaging,
) -> tuple[Any, ...]:
    return (
        limit,
        filters.search,
        filters.categories,
        filters.strike_period,
        filters.close_window,
        filters.status,
        filters.sort,
        filters.order,
        paging.active_page,
        paging.scheduled_page,
        paging.closed_page,
    )


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


def _portal_db_snapshot_refresh_sec() -> int:
    return _env_int("WEB_PORTAL_DB_SNAPSHOT_REFRESH_SEC", 60, minimum=5)


def _portal_db_snapshot_refresh_loop() -> None:
    while True:
        interval = _portal_db_snapshot_refresh_sec()
        maybe_refresh_portal_snapshot(reason="timer")
        time.sleep(max(1, interval))


def _start_portal_db_snapshot_refresh() -> None:
    global _PORTAL_SNAPSHOT_REFRESH_THREAD_STARTED  # pylint: disable=global-statement
    if not _portal_db_snapshot_enabled():
        return
    if _PORTAL_SNAPSHOT_REFRESH_THREAD_STARTED:
        return
    with _PORTAL_SNAPSHOT_REFRESH_THREAD_LOCK:
        if _PORTAL_SNAPSHOT_REFRESH_THREAD_STARTED:
            return
        thread = threading.Thread(
            target=_portal_db_snapshot_refresh_loop,
            name="portal-db-snapshot-refresh",
            daemon=True,
        )
        thread.start()
        _PORTAL_SNAPSHOT_REFRESH_THREAD_STARTED = True


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


def _parse_portal_paging(args: dict[str, Any], limit: int) -> PortalPaging:
    """Parse pagination arguments for the portal tables."""
    return PortalPaging(
        limit=limit,
        active_page=clamp_page(args.get("active_page")),
        scheduled_page=clamp_page(args.get("scheduled_page")),
        closed_page=clamp_page(args.get("closed_page")),
    )


def _empty_portal_data(error: str | None = None) -> PortalData:
    """Return an empty portal payload with an optional error."""
    return PortalData(
        rows=PortalRows(active=[], scheduled=[], closed=[]),
        totals=PortalTotals(active=0, scheduled=0, closed=0),
        strike_periods=[],
        active_categories=[],
        health=None,
        error=error,
    )


def _portal_data_from_snapshot(payload: dict[str, Any]) -> PortalData:
    """Build PortalData from a DB snapshot payload."""
    active_raw = payload.get("active_rows") or []
    scheduled_raw = payload.get("scheduled_rows") or []
    closed_raw = payload.get("closed_rows") or []
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
    health_raw = payload.get("health_raw")
    return PortalData(
        rows=PortalRows(
            active=active_rows,
            scheduled=scheduled_rows,
            closed=closed_rows,
        ),
        totals=PortalTotals(
            active=int(payload.get("active_total") or 0),
            scheduled=int(payload.get("scheduled_total") or 0),
            closed=int(payload.get("closed_total") or 0),
        ),
        strike_periods=list(payload.get("strike_periods") or []),
        active_categories=list(payload.get("active_categories") or []),
        health=build_portal_health_from_snapshot(health_raw),
        error=payload.get("error"),
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
        if _portal_db_snapshot_enabled():
            try:
                with _db_connection() as conn:
                    payload = fetch_portal_snapshot(
                        conn,
                        limit,
                        filters,
                        paging.active_offset,
                        paging.scheduled_offset,
                        paging.closed_offset,
                    )
                if payload:
                    data = _portal_data_from_snapshot(payload)
                    _store_portal_data_cache(cache_key, ttl_sec, data)
                    return data
            except Exception as exc:  # pylint: disable=broad-exception-caught
                global _PORTAL_SNAPSHOT_QUERY_WARNED  # pylint: disable=global-statement
                if not _PORTAL_SNAPSHOT_QUERY_WARNED:
                    logger.warning(
                        "Portal snapshot query failed; falling back to live queries: %s",
                        exc,
                    )
                    _PORTAL_SNAPSHOT_QUERY_WARNED = True
        with _db_connection() as conn:
            active_total, scheduled_total, closed_total = fetch_counts(conn, filters)
            data = PortalData(
                rows=PortalRows(
                    active=fetch_active_events(
                        conn,
                        limit,
                        filters,
                        offset=paging.active_offset,
                    ),
                    scheduled=fetch_scheduled_events(
                        conn,
                        limit,
                        filters,
                        offset=paging.scheduled_offset,
                    ),
                    closed=fetch_closed_events(
                        conn,
                        limit,
                        filters,
                        offset=paging.closed_offset,
                    ),
                ),
                totals=PortalTotals(
                    active=active_total,
                    scheduled=scheduled_total,
                    closed=closed_total,
                ),
                strike_periods=fetch_strike_periods(conn),
                active_categories=fetch_active_event_categories(conn, filters),
                health=_load_portal_health(conn),
                error=None,
            )
            _store_portal_data_cache(cache_key, ttl_sec, data)
            return data
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("web portal query failed")
        data = _empty_portal_data(str(exc))
        _store_portal_data_cache(cache_key, ttl_sec, data)
        return data


def _portal_context(
    *,
    limit: int,
    filters: PortalFilters,
    paging: PortalPaging,
    scope_note: str | None,
    selected_categories: list[str],
    category_filters: list[dict[str, Any]],
    filter_fields: list[dict[str, Any]],
    data: PortalData,
    load_more_links: dict[str, str],
) -> dict[str, Any]:
    """Build the render context for the portal template."""
    refreshed_at = fmt_ts(datetime.now(timezone.utc))
    active_has_more = paging.active_offset + limit < data.totals.active
    scheduled_has_more = paging.scheduled_offset + limit < data.totals.scheduled
    closed_has_more = paging.closed_offset + limit < data.totals.closed
    return {
        "error": data.error,
        "active_rows": data.rows.active,
        "scheduled_rows": data.rows.scheduled,
        "closed_rows": data.rows.closed,
        "active_total": data.totals.active,
        "scheduled_total": data.totals.scheduled,
        "closed_total": data.totals.closed,
        "limit": limit,
        "active_page": paging.active_page,
        "scheduled_page": paging.scheduled_page,
        "closed_page": paging.closed_page,
        "active_has_more": active_has_more,
        "scheduled_has_more": scheduled_has_more,
        "closed_has_more": closed_has_more,
        "load_more_links": load_more_links,
        "refreshed_at": refreshed_at,
        "logged_in": is_authenticated(),
        "scope_note": scope_note,
        "search": filters.search or "",
        "selected_categories": selected_categories,
        "selected_strike_period": filters.strike_period or "",
        "selected_close_window": filters.close_window or "",
        "selected_status": filters.status or "",
        "selected_sort": filters.sort or "",
        "selected_order": filters.order or "",
        "strike_periods": data.strike_periods,
        "category_filters": category_filters,
        "filter_fields": filter_fields,
        "close_window_options": CLOSE_WINDOW_OPTIONS,
        "status_options": EVENT_STATUS_OPTIONS,
        "sort_options": EVENT_SORT_OPTIONS,
        "order_options": EVENT_ORDER_OPTIONS,
        "health": data.health,
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

    db_url = os.getenv("DATABASE_URL")
    limit = clamp_limit(request.args.get("limit") or os.getenv("WEB_PORTAL_LIMIT"))
    filters = _parse_portal_filters(request.args)
    paging = _parse_portal_paging(request.args, limit)
    filter_fields = _portal_filter_fields(filters)
    category_params = build_filter_params(limit, filters, include_category=False)
    selected_categories = list(filters.categories)
    if password_error or not db_url:
        error_message = password_error or "DATABASE_URL is not set."
        data = _empty_portal_data(error_message)
        context = _portal_context(
            limit=limit,
            filters=filters,
            paging=paging,
            scope_note=scope_note,
            selected_categories=selected_categories,
            category_filters=[],
            filter_fields=filter_fields,
            data=data,
            load_more_links={},
        )
        return render_template("portal.html", **context)

    data = _fetch_portal_data(limit, filters, paging)
    category_filters = build_category_filters(
        active_categories=data.active_categories,
        selected_categories=selected_categories,
        base_params=category_params,
        endpoint="index",
        url_for=url_for,
    )
    base_params = build_filter_params(
        limit,
        filters,
        include_category=True,
        page_params=paging.as_params(),
    )
    load_more_links = {
        "active": url_for(
            "index",
            **{**base_params, "active_page": paging.active_page + 1},
        ),
        "scheduled": url_for(
            "index",
            **{**base_params, "scheduled_page": paging.scheduled_page + 1},
        ),
        "closed": url_for(
            "index",
            **{**base_params, "closed_page": paging.closed_page + 1},
        ),
    }
    context = _portal_context(
        limit=limit,
        filters=filters,
        paging=paging,
        scope_note=scope_note,
        selected_categories=selected_categories,
        category_filters=category_filters,
        filter_fields=filter_fields,
        data=data,
        load_more_links=load_more_links,
    )
    return render_template("portal.html", **context)


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
    from .app import create_app
    app = create_app()
    app.run(host=host, port=port, threaded=threaded)


if __name__ == "__main__":
    main()
