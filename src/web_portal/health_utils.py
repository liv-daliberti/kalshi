"""Health checks and status card helpers for the web portal."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg  # pylint: disable=import-error
from psycopg.rows import dict_row  # pylint: disable=import-error

from src.core.time_utils import age_seconds as _age_seconds

from .config import _env_bool, _env_float, _env_int
from .db import _db_connection, _fetch_state_rows
from .db_timing import timed_cursor
from .formatters import (
    _format_age_minutes,
    _format_pg_version,
    _parse_epoch_seconds,
    _parse_ts,
    fmt_bool,
    fmt_ts,
)
from .kalshi import _load_kalshi_client
from .portal_utils import portal_attr as _portal_attr
from .portal_utils import portal_func as _portal_func
from .snapshot_utils import _snapshot_backoff_remaining

logger = logging.getLogger(__name__)

_WS_LAG_LAST_ALERT = 0.0
_RAG_CALL_WINDOW_KEY = "rag_24h_calls"
_RAG_CALL_WINDOW_SECONDS = 24 * 60 * 60
_RAG_CALL_LIMIT_DEFAULT = 50000


@dataclass(frozen=True)
class PortalHealthContext:
    """Shared inputs for portal health cards."""

    db_url: str | None
    db_details: list[str]
    db_error: str | None
    rag_error: str | None
    health_payload: dict[str, Any] | None


def _portal_auth_card() -> dict[str, Any]:
    password_set = bool(os.getenv("WEB_PORTAL_PASSWORD"))
    summary = (
        "Login password is configured."
        if password_set
        else "Login password is missing; portal access will fail."
    )
    return {
        "title": "Portal Authentication",
        "level": "ok" if password_set else "error",
        "label": "Ready" if password_set else "Missing",
        "summary": summary,
        "details": [f"WEB_PORTAL_PASSWORD set: {fmt_bool(password_set)}"],
    }


def _load_portal_health_context() -> PortalHealthContext:
    db_url = os.getenv("DATABASE_URL")
    db_details = [f"DATABASE_URL set: {fmt_bool(bool(db_url))}"]
    if not db_url:
        return PortalHealthContext(
            db_url=db_url,
            db_details=db_details,
            db_error=None,
            rag_error=None,
            health_payload=None,
        )
    server_version = None
    db_error = None
    rag_error = None
    health_payload = None
    try:
        with _db_connection(connect_timeout=3) as conn:
            with timed_cursor(conn) as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            server_version = _format_pg_version(conn.info.server_version)
            try:
                health_payload = _load_portal_health(conn)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                rag_error = str(exc) or "RAG health query failed."
                logger.exception("portal health state load failed")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        db_error = str(exc) or "Database connection failed."
    if server_version:
        db_details.append(f"Server version: {server_version}")
    return PortalHealthContext(
        db_url=db_url,
        db_details=db_details,
        db_error=db_error,
        rag_error=rag_error,
        health_payload=health_payload,
    )


def _db_health_card(context: PortalHealthContext) -> dict[str, Any]:
    if not context.db_url:
        return {
            "title": "Database Connection",
            "level": "error",
            "label": "Missing",
            "summary": "DATABASE_URL is not set.",
            "details": context.db_details,
        }
    if context.db_error:
        details = context.db_details + [f"Error: {context.db_error}"]
        return {
            "title": "Database Connection",
            "level": "error",
            "label": "Down",
            "summary": "Database connection failed.",
            "details": details,
        }
    return {
        "title": "Database Connection",
        "level": "ok",
        "label": "Connected",
        "summary": "Database connection is healthy.",
        "details": context.db_details,
    }


def _rag_health_card(context: PortalHealthContext) -> dict[str, Any]:
    rag_level = "warn"
    rag_label = "Unknown"
    rag_summary = "RAG health data could not be loaded."
    rag_details = list(context.db_details)
    if not context.db_url:
        rag_level = "error"
        rag_label = "Missing"
        rag_summary = "DATABASE_URL is not set; cannot check RAG predictions."
    elif context.db_error:
        rag_level = "error"
        rag_label = "Down"
        rag_summary = "Database connection failed; cannot check RAG predictions."
        rag_details.append(f"Error: {context.db_error}")
    elif context.health_payload and context.health_payload.get("rag"):
        rag = context.health_payload["rag"]
        rag_enabled = bool(rag.get("enabled"))
        rag_label = rag.get("label") or rag_label
        if not rag_enabled:
            rag_level = "warn"
            rag_label = "Disabled"
            rag_summary = "RAG predictions are disabled."
        elif rag.get("status") == "ok":
            rag_level = "ok"
            rag_summary = "RAG predictions are running."
        else:
            rag_level = "warn"
            rag_summary = "RAG predictions are stale."
        rag_details = [
            f"PREDICTION_ENABLE set: {fmt_bool(rag_enabled)}",
            f"Last run: {rag.get('age_text') or 'N/A'}",
        ]
        if rag.get("call_count") is not None and rag.get("call_limit") is not None:
            rag_details.append(
                f"24h calls: {rag.get('call_count')} / {rag.get('call_limit')}"
            )
        if rag.get("stale_seconds") is not None:
            rag_details.append(f"Stale threshold: {rag.get('stale_seconds')}s")
    elif context.rag_error:
        rag_details.append(f"Error: {context.rag_error}")
    return {
        "title": "RAG Predictions",
        "level": rag_level,
        "label": rag_label,
        "summary": rag_summary,
        "details": rag_details,
    }


def _kalshi_health_card() -> dict[str, Any]:
    client, err = _load_kalshi_client()
    kalshi_ok = err is None and client is not None
    kalshi_details = [
        f"KALSHI_API_KEY_ID set: {fmt_bool(bool(os.getenv('KALSHI_API_KEY_ID')))}",
        (
            "KALSHI_PRIVATE_KEY_PEM_PATH set: "
            f"{fmt_bool(bool(os.getenv('KALSHI_PRIVATE_KEY_PEM_PATH')))}"
        ),
    ]
    if err:
        kalshi_details.append(f"Error: {err}")
    return {
        "title": "Kalshi API Client",
        "level": "ok" if kalshi_ok else "warn",
        "label": "Ready" if kalshi_ok else "Check",
        "summary": (
            "Kalshi API client initialized."
            if kalshi_ok
            else "Kalshi API client is not ready."
        ),
        "details": kalshi_details,
    }


def _snapshot_poller_status(
    poll_enabled: bool,
    poll_thread: bool,
    backoff: float,
) -> tuple[str, str, str]:
    if poll_enabled and poll_thread and not backoff:
        return "ok", "Running", "Snapshot poller is running normally."
    if poll_enabled:
        return "warn", "Starting", "Snapshot poller is enabled but not running cleanly."
    return "warn", "Disabled", "Snapshot poller is disabled."


def _snapshot_poller_card() -> dict[str, Any]:
    poll_enabled = _portal_func("_snapshot_poll_enabled", lambda: False)()
    poll_thread = bool(_portal_attr("_SNAPSHOT_THREAD_STARTED", False))
    config = _portal_func("_snapshot_poll_config", lambda: None)()
    backoff = _snapshot_backoff_remaining()
    poll_details = [
        f"Enabled: {fmt_bool(poll_enabled)}",
        f"Thread started: {fmt_bool(poll_thread)}",
        f"Interval: {getattr(config, 'interval', 0)}s",
        f"Limit: {getattr(config, 'limit', 0)} tickers",
    ]
    if backoff:
        poll_details.append(f"Backoff remaining: {backoff:.1f}s")
    poll_level, poll_label, poll_summary = _snapshot_poller_status(
        poll_enabled,
        poll_thread,
        backoff,
    )
    return {
        "title": "Snapshot Poller",
        "level": poll_level,
        "label": poll_label,
        "summary": poll_summary,
        "details": poll_details,
    }


def _ws_ingest_unavailable(
    context: PortalHealthContext,
) -> tuple[str, str, str, list[str]] | None:
    """Return status values when WS ingestion cannot be inspected."""
    if not context.db_url:
        return (
            "error",
            "Missing",
            "DATABASE_URL is not set; cannot check WS ingestion.",
            context.db_details,
        )
    if context.db_error:
        return (
            "error",
            "Down",
            "Database connection failed; cannot check WS ingestion.",
            context.db_details + [f"Error: {context.db_error}"],
        )
    return None


def _ws_ingest_status(
    ws_enabled: bool,
    ws_status: str,
    ws_label: str,
    heartbeat: dict[str, Any],
) -> tuple[str, str, str]:
    """Derive WS ingestion status from flags and heartbeat data."""
    missing_subs = heartbeat.get("missing_subscriptions")
    stale_ticks = heartbeat.get("stale_tick_count")
    if not ws_enabled:
        return "warn", "Disabled", "WebSocket ingestion is disabled."
    if ws_status == "ok":
        level = "ok"
        summary = "WebSocket ingestion is healthy."
    else:
        level = "warn"
        summary = "WebSocket ingestion is stale."
    if missing_subs or (stale_ticks is not None and stale_ticks > 0):
        return (
            "warn",
            "Degraded",
            "WebSocket ingestion is missing subscriptions or stale ticks.",
        )
    return level, ws_label, summary


def _ws_ingest_details(
    ws_enabled: bool,
    ws_payload: dict[str, Any],
    heartbeat: dict[str, Any],
) -> list[str]:
    """Build detail rows for the WS ingestion card."""
    details = [
        f"KALSHI_WS_ENABLE set: {fmt_bool(ws_enabled)}",
        f"Last WS tick: {ws_payload.get('age_label', 'N/A')} ago",
    ]
    heartbeat_age = heartbeat.get("age_text")
    if heartbeat_age:
        details.append(f"Heartbeat: {heartbeat_age}")
    if heartbeat.get("active_tickers") is not None:
        details.append(f"Active tickers (shard): {heartbeat.get('active_tickers')}")
    if heartbeat.get("subscribed") is not None:
        details.append(f"Subscribed: {heartbeat.get('subscribed')}")
    if heartbeat.get("missing_subscriptions") is not None:
        details.append(f"Missing subscriptions: {heartbeat.get('missing_subscriptions')}")
    if heartbeat.get("stale_tick_window_s") is not None:
        details.append(
            "Stale ticks > "
            f"{heartbeat.get('stale_tick_window_s')}s: {heartbeat.get('stale_tick_count')}"
        )
    if heartbeat.get("pending_subscriptions") is not None:
        details.append(f"Pending subscriptions: {heartbeat.get('pending_subscriptions')}")
    if heartbeat.get("pending_updates") is not None:
        details.append(f"Pending updates: {heartbeat.get('pending_updates')}")
    if not heartbeat:
        details.append("Heartbeat: missing")
    return details


def _ws_ingest_payload_card(
    health_payload: dict[str, Any],
) -> tuple[str, str, str, list[str]]:
    """Build the WS ingestion card data from cached payloads."""
    ws_payload = health_payload.get("ws") or {}
    heartbeat = health_payload.get("ws_heartbeat") or {}
    ws_enabled = bool(ws_payload.get("enabled"))
    ws_status = ws_payload.get("status") or "stale"
    ws_label = ws_payload.get("label") or "Unknown"
    ws_level, ws_label, ws_summary = _ws_ingest_status(
        ws_enabled,
        ws_status,
        ws_label,
        heartbeat,
    )
    details = _ws_ingest_details(ws_enabled, ws_payload, heartbeat)
    return ws_level, ws_label, ws_summary, details


def _ws_ingest_card(context: PortalHealthContext) -> dict[str, Any]:
    details: list[str] = []
    ws_level = "warn"
    ws_label = "Unknown"
    ws_summary = "WebSocket ingestion status is unavailable."
    unavailable = _ws_ingest_unavailable(context)
    if unavailable is not None:
        ws_level, ws_label, ws_summary, details = unavailable
    elif context.health_payload:
        ws_level, ws_label, ws_summary, details = _ws_ingest_payload_card(
            context.health_payload
        )
    return {
        "title": "WebSocket Ingestion",
        "level": ws_level,
        "label": ws_label,
        "summary": ws_summary,
        "details": details,
    }


def _build_health_cards() -> list[dict[str, Any]]:
    context = _load_portal_health_context()
    return [
        _portal_auth_card(),
        _db_health_card(context),
        _rag_health_card(context),
        _kalshi_health_card(),
        _ws_ingest_card(context),
        _snapshot_poller_card(),
    ]


def _health_time_payload(ts_value: datetime | None, now: datetime) -> dict[str, Any]:
    age_minutes, age_label = _format_age_minutes(ts_value, now)
    display = fmt_ts(ts_value) if ts_value else "N/A"
    age_text = f"{age_label} ago" if age_label else "N/A"
    return {
        "ts": ts_value,
        "display": display,
        "age_minutes": age_minutes,
        "age_label": age_label or "N/A",
        "age_text": age_text,
    }


def _status_from_age(
    age_seconds: float | None,
    stale_seconds: float,
    *,
    ok_label: str = "Running",
    stale_label: str = "Stale",
    missing_label: str = "Missing",
) -> tuple[str, str]:
    if age_seconds is None:
        return "stale", missing_label
    if age_seconds <= stale_seconds:
        return "ok", ok_label
    return "stale", stale_label


def _fetch_latest_prediction_ts(conn: psycopg.Connection) -> datetime | None:
    """Return the most recent prediction timestamp across prediction tables."""
    latest_ts = None
    queries = (
        ("prediction_runs", "SELECT MAX(run_ts) FROM prediction_runs"),
        ("market_predictions", "SELECT MAX(created_at) FROM market_predictions"),
    )
    for label, query in queries:
        try:
            with timed_cursor(conn) as cur:
                cur.execute(query)
                row = cur.fetchone()
            ts_value = row[0] if row else None
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("portal health prediction timestamp lookup failed (%s)", label)
            continue
        parsed = _parse_ts(ts_value)
        if parsed is not None and (latest_ts is None or parsed > latest_ts):
            latest_ts = parsed
    return latest_ts


@dataclass(frozen=True)
class PortalStateTimestamps:
    """Timestamp snapshot for portal health checks."""

    discovery_ts: datetime | None
    discovery_heartbeat_ts: datetime | None
    backfill_ts: datetime | None
    last_tick_ts: datetime | None
    last_ws_tick_ts: datetime | None
    last_prediction_ts: datetime | None
    ws_heartbeat_row: dict[str, Any] | None


@dataclass(frozen=True)
class PortalSnapshotState:
    """Timestamp snapshot derived from a portal health snapshot."""

    discovery_ts: datetime | None
    backfill_ts: datetime | None
    last_tick_ts: datetime | None
    last_ws_tick_ts: datetime | None
    last_prediction_ts: datetime | None
    ws_heartbeat_row: dict[str, Any] | None
    rag_call_row: dict[str, Any] | None


def _state_ts(
    row: dict[str, Any] | None,
    *,
    parse_epoch: bool = False,
) -> datetime | None:
    if not row:
        return None
    raw_value = row.get("value")
    parsed = _parse_epoch_seconds(raw_value) if parse_epoch else _parse_ts(raw_value)
    if parsed is None:
        parsed = _parse_ts(row.get("updated_at"))
    return parsed


def _snapshot_state_row(
    state_rows: dict[str, Any],
    key: str,
) -> dict[str, Any] | None:
    """Return a state row dict if present in a snapshot."""
    raw = state_rows.get(key)
    return raw if isinstance(raw, dict) else None


def _load_snapshot_state(snapshot: dict[str, Any]) -> PortalSnapshotState:
    """Build timestamp state from a portal health snapshot payload."""
    state_rows = snapshot.get("state_rows") or {}
    discovery_ts = _state_ts(_snapshot_state_row(state_rows, "last_discovery_ts"))
    backfill_ts = _state_ts(
        _snapshot_state_row(state_rows, "last_min_close_ts"),
        parse_epoch=True,
    )
    last_tick_ts = _state_ts(_snapshot_state_row(state_rows, "last_tick_ts"))
    if last_tick_ts is None:
        last_tick_ts = _parse_ts(snapshot.get("latest_tick_ts"))
    last_ws_tick_ts = _state_ts(_snapshot_state_row(state_rows, "last_ws_tick_ts"))
    if last_ws_tick_ts is None:
        last_ws_tick_ts = last_tick_ts
    last_prediction_ts = _state_ts(
        _snapshot_state_row(state_rows, "last_prediction_ts")
    )
    latest_prediction_ts = _parse_ts(snapshot.get("latest_prediction_ts"))
    if latest_prediction_ts is not None and (
        last_prediction_ts is None or latest_prediction_ts > last_prediction_ts
    ):
        last_prediction_ts = latest_prediction_ts
    return PortalSnapshotState(
        discovery_ts=discovery_ts,
        backfill_ts=backfill_ts,
        last_tick_ts=last_tick_ts,
        last_ws_tick_ts=last_ws_tick_ts,
        last_prediction_ts=last_prediction_ts,
        ws_heartbeat_row=_snapshot_state_row(state_rows, "ws_heartbeat"),
        rag_call_row=_snapshot_state_row(state_rows, "rag_24h_calls"),
    )


def _snapshot_queue_payload(snapshot: dict[str, Any]) -> dict[str, int]:
    """Normalize queue stats for the snapshot health payload."""
    queue_stats = snapshot.get("queue") or {}
    return {
        "pending": int(queue_stats.get("pending") or 0),
        "running": int(queue_stats.get("running") or 0),
        "failed": int(queue_stats.get("failed") or 0),
        "workers": int(queue_stats.get("workers") or 0),
    }


def _build_snapshot_health_payload(
    snapshot: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    """Assemble portal health data from a snapshot payload."""
    state = _load_snapshot_state(snapshot)
    ws_payload = _ws_health(now, state.last_ws_tick_ts)
    ws_heartbeat = _load_ws_heartbeat(state.ws_heartbeat_row, now)
    call_payload = _rag_call_window_payload(state.rag_call_row, now)
    rag_payload = _rag_health_from_ts(now, state.last_prediction_ts, call_payload)
    discovery_stale_seconds, backfill_stale_seconds = _ingest_stale_seconds()
    discovery_payload = _ingest_health_payload(
        state.discovery_ts,
        now,
        discovery_stale_seconds,
    )
    backfill_payload = _ingest_health_payload(
        state.backfill_ts,
        now,
        backfill_stale_seconds,
    )
    return {
        "discovery": discovery_payload,
        "backfill": backfill_payload,
        "last_tick": _health_time_payload(state.last_tick_ts, now),
        "ws": ws_payload,
        "ws_heartbeat": ws_heartbeat,
        "rag": rag_payload,
        "queue": _snapshot_queue_payload(snapshot),
    }


def _query_latest_tick_ts(conn: psycopg.Connection) -> datetime | None:
    with timed_cursor(conn) as cur:
        cur.execute("SELECT MAX(ts) FROM market_ticks")
        row = cur.fetchone()
    return row[0] if row else None


def _fetch_state_timestamps(conn: psycopg.Connection) -> PortalStateTimestamps:
    state_rows = _fetch_state_rows(
        conn,
        [
            "last_discovery_ts",
            "last_discovery_heartbeat_ts",
            "last_min_close_ts",
            "last_prediction_ts",
            "last_tick_ts",
            "last_ws_tick_ts",
            "ws_heartbeat",
        ],
    )
    discovery_ts = _state_ts(state_rows.get("last_discovery_ts"))
    discovery_heartbeat_ts = _state_ts(state_rows.get("last_discovery_heartbeat_ts"))
    backfill_ts = _state_ts(state_rows.get("last_min_close_ts"), parse_epoch=True)
    last_tick_ts = _state_ts(state_rows.get("last_tick_ts"))
    if last_tick_ts is None:
        last_tick_ts = _query_latest_tick_ts(conn)
    last_ws_tick_ts = _state_ts(state_rows.get("last_ws_tick_ts")) or last_tick_ts
    last_prediction_ts = _state_ts(state_rows.get("last_prediction_ts"))
    return PortalStateTimestamps(
        discovery_ts=discovery_ts,
        discovery_heartbeat_ts=discovery_heartbeat_ts,
        backfill_ts=backfill_ts,
        last_tick_ts=last_tick_ts,
        last_ws_tick_ts=last_ws_tick_ts,
        last_prediction_ts=last_prediction_ts,
        ws_heartbeat_row=state_rows.get("ws_heartbeat"),
    )


def _fetch_queue_stats(conn: psycopg.Connection) -> dict[str, int]:
    queue_stats = {
        "pending": 0,
        "running": 0,
        "failed": 0,
        "workers": 0,
    }
    lock_timeout_seconds = _env_int(
        "WORK_QUEUE_LOCK_TIMEOUT_SECONDS",
        900,
        minimum=10,
    )
    try:
        with timed_cursor(conn, row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) FILTER (
                    WHERE status = 'pending' AND available_at <= NOW()
                  ) AS pending,
                  COUNT(*) FILTER (
                    WHERE status = 'running'
                      AND locked_at >= NOW() - (%s * INTERVAL '1 second')
                  ) AS running,
                  COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                  COUNT(DISTINCT locked_by) FILTER (
                    WHERE status = 'running'
                      AND locked_by IS NOT NULL
                      AND locked_at >= NOW() - (%s * INTERVAL '1 second')
                  ) AS workers
                FROM work_queue
                """,
                (lock_timeout_seconds, lock_timeout_seconds),
            )
            row = cur.fetchone()
        if row:
            queue_stats.update(
                {
                    "pending": int(row.get("pending") or 0),
                    "running": int(row.get("running") or 0),
                    "failed": int(row.get("failed") or 0),
                    "workers": int(row.get("workers") or 0),
                }
            )
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("portal health queue stats failed")
    return queue_stats


def _ws_alert_flag(
    age_seconds: float | None,
    alert_seconds: float,
    alert_cooldown: float,
) -> bool:
    global _WS_LAG_LAST_ALERT  # pylint: disable=global-statement
    if age_seconds is None or alert_seconds <= 0.0:
        return False
    if age_seconds < alert_seconds:
        return False
    now_mono = time.monotonic()
    if now_mono - _WS_LAG_LAST_ALERT >= alert_cooldown:
        logger.warning(
            "WS lag alert: last_ws_tick_ts age=%.1fs (threshold=%.1fs)",
            age_seconds,
            alert_seconds,
        )
        _WS_LAG_LAST_ALERT = now_mono
    return True


def _ws_status_labels(ws_enabled: bool, ws_connected: bool) -> tuple[str, str]:
    if not ws_enabled:
        return "disabled", "Disabled"
    if ws_connected:
        return "ok", "Connected"
    return "stale", "Stale"


def _ws_health(now: datetime, last_ws_tick_ts: datetime | None) -> dict[str, Any]:
    ws_enabled = _env_bool("KALSHI_WS_ENABLE", False)
    ws_stale_seconds = _env_int("WEB_PORTAL_WS_STALE_SECONDS", 120, minimum=10)
    payload = _health_time_payload(last_ws_tick_ts, now)
    age_seconds = _age_seconds(last_ws_tick_ts, now)
    ws_alert_seconds = _env_float("WEB_PORTAL_WS_LAG_ALERT_SECONDS", 0.0, minimum=0.0)
    ws_alert_cooldown = _env_float(
        "WEB_PORTAL_WS_LAG_ALERT_COOLDOWN_SECONDS",
        60.0,
        minimum=0.0,
    )
    ws_alert = _ws_alert_flag(age_seconds, ws_alert_seconds, ws_alert_cooldown)
    ws_connected = bool(
        ws_enabled and age_seconds is not None and age_seconds <= ws_stale_seconds
    )
    ws_status, ws_label = _ws_status_labels(ws_enabled, ws_connected)
    return {
        "enabled": ws_enabled,
        "connected": ws_connected,
        "status": ws_status,
        "label": ws_label,
        "age_minutes": payload["age_minutes"],
        "age_label": payload["age_label"],
        "age_seconds": age_seconds,
        "alert": ws_alert,
        "alert_seconds": ws_alert_seconds or None,
    }


def _latest_prediction_ts(
    conn: psycopg.Connection,
    last_prediction_ts: datetime | None,
) -> datetime | None:
    db_prediction_ts = _fetch_latest_prediction_ts(conn)
    if db_prediction_ts is not None and (
        last_prediction_ts is None or db_prediction_ts > last_prediction_ts
    ):
        return db_prediction_ts
    return last_prediction_ts


def _rag_call_window_payload(
    row: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    count = 0
    limit = _RAG_CALL_LIMIT_DEFAULT
    window_start = None
    if row:
        raw = row.get("value")
        if raw:
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            window_start = _parse_ts(payload.get("window_start"))
            try:
                count = int(payload.get("count", 0))
            except (TypeError, ValueError):
                count = 0
            try:
                limit = int(payload.get("limit", _RAG_CALL_LIMIT_DEFAULT))
            except (TypeError, ValueError):
                limit = _RAG_CALL_LIMIT_DEFAULT
    if limit < 1:
        limit = _RAG_CALL_LIMIT_DEFAULT
    if window_start is None:
        count = 0
    else:
        age_seconds = (now - window_start).total_seconds()
        if age_seconds >= _RAG_CALL_WINDOW_SECONDS:
            count = 0
    return {
        "count": max(0, count),
        "limit": max(1, limit),
        "remaining": max(0, limit - count),
    }


def _rag_health(
    conn: psycopg.Connection,
    now: datetime,
    last_prediction_ts: datetime | None,
) -> dict[str, Any]:
    last_prediction_ts = _latest_prediction_ts(conn, last_prediction_ts)
    call_payload = _rag_call_window_payload(
        _fetch_state_rows(conn, [_RAG_CALL_WINDOW_KEY]).get(_RAG_CALL_WINDOW_KEY),
        now,
    )
    return _rag_health_from_ts(now, last_prediction_ts, call_payload)


def _rag_health_from_ts(
    now: datetime,
    last_prediction_ts: datetime | None,
    call_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rag_enabled = _env_bool("PREDICTION_ENABLE") or _env_bool("PREDICTIONS_ENABLE")
    if not rag_enabled and last_prediction_ts is not None:
        rag_enabled = True
    rag_stale_seconds = _env_int("WEB_PORTAL_RAG_STALE_SECONDS", 300, minimum=30)
    age_seconds = _age_seconds(last_prediction_ts, now)
    if not rag_enabled:
        rag_status, rag_label = "disabled", "Disabled"
    elif age_seconds is not None and age_seconds <= rag_stale_seconds:
        rag_status, rag_label = "ok", "Running"
    else:
        rag_status, rag_label = "stale", "Stale"
    payload = _health_time_payload(last_prediction_ts, now)
    if call_payload is None:
        call_payload = {
            "count": 0,
            "limit": _RAG_CALL_LIMIT_DEFAULT,
            "remaining": _RAG_CALL_LIMIT_DEFAULT,
        }
    return {
        "enabled": rag_enabled,
        "status": rag_status,
        "label": rag_label,
        "age_minutes": payload["age_minutes"],
        "age_label": payload["age_label"],
        "age_text": payload["age_text"],
        "age_seconds": age_seconds,
        "stale_seconds": rag_stale_seconds if rag_enabled else None,
        "call_count": call_payload["count"],
        "call_limit": call_payload["limit"],
        "call_remaining": call_payload["remaining"],
    }


def _ingest_stale_seconds() -> tuple[int, int]:
    discovery_interval = _env_int("DISCOVERY_SECONDS", 1800, minimum=60)
    backfill_interval = _env_int("BACKFILL_SECONDS", 900, minimum=60)
    discovery_default = max(discovery_interval * 2, 600)
    backfill_default = max(backfill_interval * 2, 600)
    discovery_stale_seconds = _env_int(
        "WEB_PORTAL_DISCOVERY_STALE_SECONDS",
        discovery_default,
        minimum=60,
    )
    backfill_stale_seconds = _env_int(
        "WEB_PORTAL_BACKFILL_STALE_SECONDS",
        backfill_default,
        minimum=60,
    )
    return discovery_stale_seconds, backfill_stale_seconds


def _ingest_health_payload(
    ts_value: datetime | None,
    now: datetime,
    stale_seconds: int,
) -> dict[str, Any]:
    age_seconds = _age_seconds(ts_value, now)
    status, label = _status_from_age(age_seconds, stale_seconds)
    payload = _health_time_payload(ts_value, now)
    payload.update(
        {
            "status": status,
            "label": label,
            "age_seconds": age_seconds,
            "stale_seconds": stale_seconds,
        }
    )
    return payload


def _load_portal_health(conn: psycopg.Connection) -> dict[str, Any]:
    """Load portal health stats from ingest_state and the work queue."""
    now = datetime.now(timezone.utc)
    timestamps = _fetch_state_timestamps(conn)
    queue_stats = _fetch_queue_stats(conn)
    ws_payload = _ws_health(now, timestamps.last_ws_tick_ts)
    ws_heartbeat = _load_ws_heartbeat(timestamps.ws_heartbeat_row, now)
    rag_payload = _rag_health(conn, now, timestamps.last_prediction_ts)
    discovery_stale_seconds, backfill_stale_seconds = _ingest_stale_seconds()
    discovery_payload = _ingest_health_payload(
        timestamps.discovery_ts,
        now,
        discovery_stale_seconds,
    )
    heartbeat_interval_s = _env_int("DISCOVERY_HEARTBEAT_SECONDS", 60, minimum=5)
    heartbeat_default = max(heartbeat_interval_s * 2, 120)
    heartbeat_stale_seconds = _env_int(
        "WEB_PORTAL_DISCOVERY_HEARTBEAT_STALE_SECONDS",
        heartbeat_default,
        minimum=30,
    )
    discovery_payload["heartbeat"] = _ingest_health_payload(
        timestamps.discovery_heartbeat_ts,
        now,
        heartbeat_stale_seconds,
    )
    backfill_payload = _ingest_health_payload(
        timestamps.backfill_ts,
        now,
        backfill_stale_seconds,
    )
    return {
        "discovery": discovery_payload,
        "backfill": backfill_payload,
        "last_tick": _health_time_payload(timestamps.last_tick_ts, now),
        "ws": ws_payload,
        "ws_heartbeat": ws_heartbeat,
        "rag": rag_payload,
        "queue": queue_stats,
    }


def build_portal_health_from_snapshot(
    snapshot: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Build portal health payload from a precomputed snapshot payload."""
    if not snapshot:
        return None
    now = datetime.now(timezone.utc)
    return _build_snapshot_health_payload(snapshot, now)


def _load_ws_heartbeat(
    row: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any] | None:
    if not row:
        return None
    raw = row.get("value")
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        logger.warning("portal health ws heartbeat parse failed")
        return None
    updated_at = row.get("updated_at")
    updated_ts = _parse_ts(updated_at)
    age_payload = _health_time_payload(updated_ts, now)
    payload["age_text"] = age_payload["age_text"]
    payload["age_minutes"] = age_payload["age_minutes"]
    return payload
