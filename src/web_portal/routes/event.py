"""Event routes."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from flask import Blueprint, jsonify, render_template  # pylint: disable=import-error
from psycopg.rows import dict_row  # pylint: disable=import-error

from src.db.db import get_state, set_state

from ..config import _env_int
from ..db import _db_connection
from ..db_details import fetch_event_detail
from ..db_utils import insert_market_tick
from ..snapshot_utils import (
    _prefer_tick_snapshot,
    _set_snapshot_backoff,
    _snapshot_allows_closed,
    fetch_live_snapshot,
)

bp = Blueprint("event", __name__)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SnapshotSettings:
    """Settings for event snapshot collection."""

    allow_closed: bool
    delay_ms: int
    prefer_ticks_sec: int
    cooldown_sec: int
    max_markets: int
    now: datetime


def _snapshot_cursor_key(event_ticker: str) -> str:
    return f"portal_snapshot_cursor:{event_ticker}"


def _load_snapshot_cursor(conn: "psycopg.Connection", event_ticker: str) -> int:
    raw = get_state(conn, _snapshot_cursor_key(event_ticker), "0") or "0"
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 0


def _store_snapshot_cursor(conn: "psycopg.Connection", event_ticker: str, value: int) -> None:
    set_state(conn, _snapshot_cursor_key(event_ticker), str(max(0, value)))


def _snapshot_settings() -> SnapshotSettings:
    return SnapshotSettings(
        allow_closed=_snapshot_allows_closed(),
        delay_ms=_env_int("WEB_PORTAL_SNAPSHOT_POLL_DELAY_MS", 0, minimum=0),
        prefer_ticks_sec=_env_int(
            "WEB_PORTAL_SNAPSHOT_PREFER_TICKS_SEC",
            60,
            minimum=0,
        ),
        cooldown_sec=_env_int(
            "WEB_PORTAL_SNAPSHOT_POLL_COOLDOWN_SEC",
            30,
            minimum=5,
        ),
        max_markets=_env_int("WEB_PORTAL_EVENT_SNAPSHOT_LIMIT", 3, minimum=0),
        now=datetime.now(timezone.utc),
    )


def _load_event_market_rows(
    conn: "psycopg.Connection",
    event_ticker: str,
    allow_closed: bool,
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        if allow_closed:
            cur.execute(
                """
                SELECT ticker, close_time
                FROM markets
                WHERE event_ticker = %s
                ORDER BY ticker
                """,
                (event_ticker,),
            )
        else:
            cur.execute(
                """
                SELECT ticker, close_time
                FROM markets
                WHERE event_ticker = %s
                  AND (close_time IS NULL OR close_time > NOW())
                ORDER BY ticker
                """,
                (event_ticker,),
            )
        return list(cur.fetchall())


def _event_exists(conn: "psycopg.Connection", event_ticker: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM events WHERE event_ticker = %s", (event_ticker,))
        return cur.fetchone() is not None


def _limit_snapshot_rows(
    conn: "psycopg.Connection",
    event_ticker: str,
    rows: list[dict[str, Any]],
    max_markets: int,
) -> list[dict[str, Any]]:
    if not max_markets or len(rows) <= max_markets:
        return rows
    start = _load_snapshot_cursor(conn, event_ticker)
    total = len(rows)
    selected = [rows[(start + idx) % total] for idx in range(max_markets)]
    _store_snapshot_cursor(conn, event_ticker, start + max_markets)
    return selected


def _missing_snapshot_response(
    event_ticker: str,
    *,
    exists: bool,
    allow_closed: bool,
) -> tuple[dict[str, Any], int]:
    if exists:
        if allow_closed:
            return (
                {"error": "No markets found for event.", "event_ticker": event_ticker},
                409,
            )
        return (
            {
                "error": "Event is closed; snapshot skipped.",
                "event_ticker": event_ticker,
            },
            409,
        )
    return {"error": "Event not found.", "event_ticker": event_ticker}, 404


def _snapshot_event_markets(
    conn: "psycopg.Connection",
    rows: list[dict[str, Any]],
    settings: SnapshotSettings,
) -> tuple[int, int, list[dict[str, str]]]:
    updated = 0
    cached = 0
    errors: list[dict[str, str]] = []
    for row in rows:
        ticker = row.get("ticker")
        if not ticker:
            continue
        close_dt = row.get("close_time")
        is_closed = close_dt is not None and close_dt <= settings.now
        cached_payload = _prefer_tick_snapshot(
            conn,
            ticker,
            settings.prefer_ticks_sec,
            allow_stale=is_closed,
        )
        if cached_payload is not None:
            cached += 1
            continue
        if is_closed and not settings.allow_closed:
            errors.append(
                {"ticker": ticker, "error": "Market is closed; snapshot skipped."}
            )
            continue
        data, snapshot_tick = fetch_live_snapshot(ticker)
        if data.get("rate_limited"):
            _set_snapshot_backoff(settings.cooldown_sec)
            errors.append(
                {
                    "ticker": ticker,
                    "error": "Rate limited; snapshot halted.",
                }
            )
            break
        if "error" in data:
            errors.append({"ticker": ticker, "error": data.get("error") or "Error"})
            continue
        if snapshot_tick:
            try:
                insert_market_tick(conn, snapshot_tick)
                updated += 1
            except Exception:  # pylint: disable=broad-exception-caught
                logger.exception("live snapshot insert failed for %s", ticker)
                errors.append({"ticker": ticker, "error": "Snapshot insert failed."})
        else:
            errors.append({"ticker": ticker, "error": "No snapshot data returned."})
        if settings.delay_ms > 0:
            time.sleep(settings.delay_ms / 1000)
    return updated, cached, errors


@bp.get("/event/<event_ticker>")
def event_detail(event_ticker: str):
    """Render the event detail view."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return render_template(
            "event_detail.html",
            error="DATABASE_URL is not set.",
            event=None,
        )

    error = None
    event = None
    try:
        with _db_connection() as conn:
            event = fetch_event_detail(conn, event_ticker)
            if event is None:
                error = "Event not found."
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("event detail query failed")
        error = str(exc)

    return render_template(
        "event_detail.html",
        error=error,
        event=event,
        auto_snapshot_sec=_env_int("WEB_PORTAL_EVENT_AUTO_SNAPSHOT_SEC", 120, minimum=10),
    )


@bp.get("/event/<event_ticker>/snapshot")
def event_snapshot(event_ticker: str):
    """Fetch live snapshots for all markets in an event."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return jsonify({"error": "DATABASE_URL is not set."}), 503
    settings = _snapshot_settings()

    try:
        with _db_connection() as conn:
            rows = _load_event_market_rows(conn, event_ticker, settings.allow_closed)
            if not rows:
                payload, status = _missing_snapshot_response(
                    event_ticker,
                    exists=_event_exists(conn, event_ticker),
                    allow_closed=settings.allow_closed,
                )
                return jsonify(payload), status

            rows = _limit_snapshot_rows(
                conn,
                event_ticker,
                rows,
                settings.max_markets,
            )
            updated, cached, errors = _snapshot_event_markets(conn, rows, settings)
            return jsonify(
                {
                    "event_ticker": event_ticker,
                    "requested": len(rows),
                    "updated": updated,
                    "cached": cached,
                    "errors": errors,
                }
            )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("event snapshot fetch failed")
        return jsonify({"error": str(exc) or "Event snapshot request failed."}), 503
