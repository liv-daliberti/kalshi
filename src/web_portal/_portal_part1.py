"""Simple web portal for browsing active and closed Kalshi markets."""

from __future__ import annotations

import builtins
import importlib
import logging
import os
import random
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

import psycopg  # pylint: disable=import-error
from flask import (  # pylint: disable=import-error
    Flask,
    g,
    jsonify,
    redirect,
    request,
    url_for,
)

from ..core.time_utils import infer_strike_period_from_times
from .auth_utils import is_authenticated
from .config import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    EVENT_SORT_COLUMNS,
    _env_bool,
    _env_float,
    _env_int,
    _fmt_hours,
    _parse_csv,
)
from .db_utils import insert_market_tick
from .db import (
    _db_connection,
    _maybe_prewarm_db_pool,
)
from .db_timing import get_db_metrics, reset_db_metrics, timed_cursor
from .market_metadata import (
    MarketExtrasPayload,
    _derive_custom_strike,
    _extract_event_metadata,
    _maybe_parse_json,
    _update_event_metadata,
    _update_market_extras,
)
from .filter_params import build_filter_params
from .portal_filters import (
    PortalFilters,
    _clean_filter_value,
    _parse_category_filters,
    _parse_close_window,
    _parse_order_value,
    _parse_portal_filters,
    _parse_sort_value,
)
from .queue_stream_utils import _queue_stream_enabled, _queue_stream_min_reload_ms
from .formatters import _now_utc, _parse_ts, fmt_time_remaining as _fmt_time_remaining
from .snapshot_utils import (
    _set_snapshot_backoff,
    _snapshot_backoff_remaining,
    fetch_live_snapshot,
)

if TYPE_CHECKING:
    from ._portal_part2 import _start_portal_db_snapshot_refresh, index, portal_data

_PACKAGE = __package__ or "src.web_portal"
_health_utils = importlib.import_module(f"{_PACKAGE}.health_utils")

_HEALTH_FETCH_LATEST_PREDICTION_TS = _health_utils.fetch_latest_prediction_ts


def _build_filter_params(
    limit: int,
    filters: PortalFilters,
    *,
    include_category: bool = True,
    page_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_filter_params(
        limit,
        filters,
        include_category=include_category,
        page_params=page_params,
    )


def _build_order_by(
    sort: str | None,
    order: str | None,
    default_sort: str,
    default_order: str,
) -> str:
    sort_key = sort if sort in EVENT_SORT_COLUMNS else default_sort
    order_key = order if order in {"asc", "desc"} else default_order
    sort_col = EVENT_SORT_COLUMNS.get(sort_key, EVENT_SORT_COLUMNS[default_sort])
    return f"{sort_col} {order_key} NULLS LAST, event_ticker {order_key}"

logger = logging.getLogger(__name__)
_SYS = sys

_MARKET_METADATA_EXPORTS = (
    MarketExtrasPayload,
    _derive_custom_strike,
    _extract_event_metadata,
    _maybe_parse_json,
    _update_event_metadata,
    _update_market_extras,
)
_PORTAL_FILTER_EXPORTS = (
    _clean_filter_value,
    _parse_category_filters,
    _parse_close_window,
    _parse_order_value,
    _parse_portal_filters,
    _parse_sort_value,
)
_PORTAL_LIMIT_EXPORTS = (DEFAULT_LIMIT, MAX_LIMIT)
_PORTAL_ENV_EXPORTS = (_env_bool,)

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
_PORTAL_SNAPSHOT_REFRESH_DISABLED_WARNED = False


def _missing_portal_handler(*_args, **_kwargs):
    raise RuntimeError("Portal routes are not loaded.")


def _start_portal_db_snapshot_refresh() -> None:
    return None


index = _missing_portal_handler
portal_data = _missing_portal_handler


def _portal_route_handler(name: str):
    handler = globals().get(name)
    if callable(handler):
        return handler
    return _missing_portal_handler


def _invoke_portal_snapshot_refresh() -> None:
    handler = globals().get("_start_portal_db_snapshot_refresh")
    if callable(handler):
        handler()

if __name__.endswith(("._portal_part1", "._portal_part2")):
    _PORTAL_MODULE_PREFIX = __name__.rsplit(".", 1)[0]
elif __name__.endswith("web_portal"):
    _PORTAL_MODULE_PREFIX = __name__
else:
    _PORTAL_MODULE_PREFIX = "src.web_portal"
_FALLBACK_MODULES = tuple(
    f"{_PORTAL_MODULE_PREFIX}.{suffix}"
    for suffix in (
        "formatters",
        "snapshot_utils",
        "db_details",
        "db_event_series",
        "db_events",
        "db_snapshot",
        "backfill_config",
        "health_utils",
        "kalshi",
        "kalshi_sdk",
        "db",
        "db_utils",
        "routes.auth",
        "routes.event",
        "routes.health",
        "routes.market",
        "routes.opportunities",
        "routes.stream",
    )
)


def __getattr__(name: str):
    if name == "open":
        value = builtins.open
        globals()[name] = value
        return value
    if name == "routes":
        module_name = f"{_PORTAL_MODULE_PREFIX}.routes"
        try:
            module = _SYS.modules.get(module_name)
            if module is None:
                module = __import__(module_name, fromlist=["*"])
        except Exception:  # pragma: no cover - best-effort fallback
            module = None
        if module is not None:
            globals()[name] = module
            return module
    for module_name in _FALLBACK_MODULES:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name == module_name:
                continue
            raise
        except ImportError:
            continue
        except (AttributeError, TypeError):
            # Best-effort fallback for patched importlib in tests.
            continue
        if hasattr(module, name):
            value = getattr(module, name)
            globals()[name] = value
            return value
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def _make_proxy(name: str):
    def _proxy(*args, **kwargs):
        module = _SYS.modules[__name__]
        target = module.__dict__.get(name)
        if target is None:
            try:
                target = getattr(module, name)
            except AttributeError as exc:
                raise AttributeError(
                    f"module '{__name__}' has no attribute '{name}'"
                ) from exc
        return target(*args, **kwargs)

    _proxy.__name__ = name
    return _proxy


def _wire_route_module(module, call_names: tuple[str, ...], attr_names: tuple[str, ...] = ()):
    for name in call_names:
        setattr(module, name, _make_proxy(name))
    for name in attr_names:
        if name in globals():
            setattr(module, name, globals()[name])


def _wire_route_modules() -> None:
    routes_prefix = f"{_PORTAL_MODULE_PREFIX}.routes"
    module_specs = (
        (
            f"{routes_prefix}.market",
            (
                "_db_connection",
                "fetch_market_detail",
                "_snapshot_allows_closed",
                "_market_is_closed",
                "_prefer_tick_snapshot",
                "fetch_live_snapshot",
                "insert_market_tick",
                "_set_snapshot_backoff",
                "_load_backfill_config",
                "enqueue_job",
                "_env_bool",
                "_env_float",
                "_env_int",
            ),
            ("BackfillConfig",),
        ),
        (
            f"{routes_prefix}.event",
            (
                "_db_connection",
                "fetch_event_detail",
                "_snapshot_allows_closed",
                "_prefer_tick_snapshot",
                "fetch_live_snapshot",
                "insert_market_tick",
                "_set_snapshot_backoff",
                "_env_int",
            ),
            (),
        ),
        (
            f"{routes_prefix}.health",
            ("_build_health_cards", "fmt_ts"),
            (),
        ),
        (
            f"{routes_prefix}.stream",
            ("_db_connection", "maybe_refresh_portal_snapshot", "_queue_stream_enabled"),
            (),
        ),
    )
    for module_name, call_names, attr_names in module_specs:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name == module_name:
                continue
            raise
        except ImportError:
            continue
        _wire_route_module(module, call_names, attr_names)
        if module_name == f"{routes_prefix}.market":
            module.infer_strike_period_from_times = _make_proxy(
                "_infer_strike_period_from_times"
            )


_wire_route_modules()


def _fetch_latest_prediction_ts(conn: psycopg.Connection) -> datetime | None:
    return _HEALTH_FETCH_LATEST_PREDICTION_TS(conn)


def _load_portal_health(conn: psycopg.Connection) -> dict[str, Any]:
    _health_utils.logger = logger
    _health_utils.fetch_latest_prediction_ts = _fetch_latest_prediction_ts
    return _health_utils.load_portal_health(conn)


def _build_health_cards() -> list[dict[str, Any]]:
    _health_utils.logger = logger
    cards = _health_utils.build_health_cards()
    for card in cards:
        if card.get("title") != "Snapshot Poller":
            continue
        raw_details = card.get("details")
        if raw_details is None:
            details = []
        elif isinstance(raw_details, str):
            details = [raw_details]
        elif isinstance(raw_details, list):
            details = raw_details
        else:
            try:
                details = list(raw_details)
            except TypeError:
                details = [raw_details]
        details = [str(detail) for detail in details]
        if not any("Backoff remaining" in detail for detail in details):
            try:
                backoff_val = _snapshot_backoff_remaining()
            except Exception:  # pylint: disable=broad-exception-caught
                backoff_val = None
            if isinstance(backoff_val, (int, float)):
                details.append(f"Backoff remaining: {backoff_val:.1f}s")
            else:
                details.append("Backoff remaining: N/A")
        card["details"] = details
        break
    return cards


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
    app.add_url_rule("/", "index", _portal_route_handler("index"), methods=["GET"])
    app.add_url_rule(
        "/portal/data",
        "portal_data",
        _portal_route_handler("portal_data"),
        methods=["GET"],
    )


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


def _snapshot_poll_backoff(*, mid_cycle: bool = False) -> float:
    backoff = _snapshot_backoff_remaining()
    if backoff:
        message = (
            "snapshot poll backing off mid-cycle for %.1fs"
            if mid_cycle
            else "snapshot poll backing off for %.1fs"
        )
        logger.warning(message, backoff)
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
        if _snapshot_poll_backoff(mid_cycle=True):
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
    return _fmt_time_remaining(close_time, now=_now_utc())


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
    _invoke_portal_snapshot_refresh()


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
        "health.health_data",
        "opportunities.opportunities_data",
        "portal_data",
        "stream.queue_stream",
    }:
        if not is_authenticated():
            return jsonify({"error": "Authentication required."}), 401
        return None
    if not is_authenticated():
        return redirect(url_for("auth.login", next=request.path))
    return None
