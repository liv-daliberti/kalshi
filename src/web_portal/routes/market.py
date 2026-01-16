"""Market routes."""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime

from flask import Blueprint, jsonify, render_template, request  # pylint: disable=import-error
from psycopg.rows import dict_row  # pylint: disable=import-error

from src.core.time_utils import infer_strike_period_from_times
from src.queue.work_queue import enqueue_job

from ..backfill_config import BackfillConfig, _load_backfill_config
from ..config import _env_bool, _env_float, _env_int
from ..db import _db_connection, fetch_market_detail
from ..db_timing import timed_cursor
from ..db_utils import insert_market_tick
from ..formatters import _parse_ts
from ..snapshot_utils import (
    _market_is_closed,
    _prefer_tick_snapshot,
    _set_snapshot_backoff,
    _snapshot_allows_closed,
    fetch_live_snapshot,
)

bp = Blueprint("market", __name__)
logger = logging.getLogger(__name__)
_MARKET_DETAIL_CACHE: dict[str, tuple[float, dict[str, object]]] = {}
_MARKET_DETAIL_CACHE_LOCK = threading.Lock()


def _market_detail_cache_ttl() -> int:
    return _env_int("WEB_PORTAL_MARKET_DETAIL_CACHE_SEC", 60, minimum=0)


def _load_market_detail_cache(ticker: str) -> dict[str, object] | None:
    ttl_sec = _market_detail_cache_ttl()
    if ttl_sec <= 0:
        return None
    now = time.monotonic()
    with _MARKET_DETAIL_CACHE_LOCK:
        cached = _MARKET_DETAIL_CACHE.get(ticker)
        if not cached:
            return None
        cached_ts, payload = cached
        if now - cached_ts > ttl_sec:
            _MARKET_DETAIL_CACHE.pop(ticker, None)
            return None
        return payload


def _store_market_detail_cache(ticker: str, payload: dict[str, object]) -> None:
    ttl_sec = _market_detail_cache_ttl()
    if ttl_sec <= 0:
        return
    with _MARKET_DETAIL_CACHE_LOCK:
        _MARKET_DETAIL_CACHE[ticker] = (time.monotonic(), payload)


def _backfill_mode() -> str:
    return os.getenv("WEB_PORTAL_BACKFILL_MODE", "disabled").strip().lower()


def _backfill_mode_error(mode: str) -> tuple[dict[str, str], int] | None:
    if mode in {"", "disabled", "off", "false", "0"}:
        return (
            {
                "error": (
                    "Backfill endpoint is disabled in the portal. "
                    "Use the REST/worker services to run backfills."
                )
            },
            403,
        )
    if mode != "queue":
        return ({"error": f"Unsupported backfill mode: {mode}"}, 400)
    return None


def _fetch_market_row(conn, ticker: str) -> dict | None:
    with timed_cursor(conn, row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
              m.ticker,
              m.open_time,
              m.close_time,
              e.series_ticker,
              e.strike_period
            FROM markets m
            JOIN events e ON e.event_ticker = m.event_ticker
            WHERE m.ticker = %s
            """,
            (ticker,),
        )
        return cur.fetchone()


def _infer_strike_period_from_row(
    row: dict,
    hour_max: float,
    day_max: float,
) -> str | None:
    return infer_strike_period_from_times(
        _parse_ts(row.get("open_time")),
        _parse_ts(row.get("close_time")),
        hour_max,
        day_max,
    )


def _resolve_strike_period(
    cfg: BackfillConfig,
    row: dict,
) -> tuple[str | None, dict[str, object] | None]:
    strike_period = (row.get("strike_period") or "").strip().lower()
    if strike_period in cfg.strike_periods:
        return strike_period, None
    hour_max = _env_float("STRIKE_HOUR_MAX_HOURS", 2.0, minimum=0.01)
    day_max = _env_float("STRIKE_DAY_MAX_HOURS", 36.0, minimum=hour_max)
    inferred = _infer_strike_period_from_row(row, hour_max, day_max)
    if inferred and inferred in cfg.strike_periods:
        return inferred, None
    label = strike_period or "unknown"
    return None, {
        "error": f"Strike period '{label}' not enabled for backfill.",
        "strike_period": label,
        "inferred": inferred,
    }


def _market_payload(row: dict) -> dict[str, object]:
    def _value(key: str):
        value = row.get(key)
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    return {
        "ticker": row.get("ticker"),
        "open_time": _value("open_time"),
        "close_time": _value("close_time"),
    }


def _snapshot_error(message: str, status: int) -> tuple[dict[str, str], int]:
    return {"error": message}, status


def _market_snapshot_payload(ticker: str) -> tuple[dict[str, object], int]:
    payload: dict[str, object] = {}
    status = 200
    if not os.getenv("DATABASE_URL"):
        return _snapshot_error("DATABASE_URL is not set. Live snapshot not saved.", 503)
    try:
        with _db_connection() as conn:
            allow_closed = _snapshot_allows_closed()
            is_closed = _market_is_closed(conn, ticker)
            if not allow_closed and is_closed:
                payload, status = _snapshot_error(
                    "Market is closed; snapshot skipped.",
                    409,
                )
            else:
                prefer_ticks_sec = _env_int(
                    "WEB_PORTAL_SNAPSHOT_PREFER_TICKS_SEC", 60, minimum=0
                )
                cached_payload = _prefer_tick_snapshot(
                    conn,
                    ticker,
                    prefer_ticks_sec,
                    allow_stale=is_closed,
                )
                if cached_payload is not None:
                    cached_payload["db_saved"] = False
                    payload = cached_payload
                else:
                    data, snapshot_tick = fetch_live_snapshot(ticker)
                    if "error" in data:
                        status = 429 if data.get("rate_limited") else 503
                        if data.get("rate_limited"):
                            _set_snapshot_backoff(
                                _env_int(
                                    "WEB_PORTAL_SNAPSHOT_POLL_COOLDOWN_SEC",
                                    30,
                                    minimum=5,
                                )
                            )
                        payload = data
                    elif not snapshot_tick:
                        payload, status = _snapshot_error(
                            "Live snapshot data missing.",
                            503,
                        )
                    else:
                        insert_market_tick(conn, snapshot_tick)
                        data["db_saved"] = True
                        payload = data
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("live snapshot insert failed for %s", ticker)
        payload, status = _snapshot_error(
            str(exc) or "Live snapshot save failed.",
            503,
        )
    return payload, status


@bp.get("/market/<ticker>")
def market_detail(ticker: str):
    """Render a market detail view."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return render_template(
            "market_detail.html",
            error="DATABASE_URL is not set.",
            market=None,
        )

    cached = _load_market_detail_cache(ticker)
    if cached is not None:
        return render_template(
            "market_detail.html",
            error=None,
            market=cached,
        )

    error = None
    market = None
    try:
        with _db_connection() as conn:
            market = fetch_market_detail(conn, ticker)
            if market is None:
                error = "Market not found."
            else:
                _store_market_detail_cache(ticker, market)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("market detail query failed")
        error = str(exc)

    return render_template(
        "market_detail.html",
        error=error,
        market=market,
    )


@bp.get("/market/<ticker>/snapshot")
def market_snapshot(ticker: str):
    """Return a live snapshot for a market ticker."""
    payload, status = _market_snapshot_payload(ticker)
    return jsonify(payload), status


def _enqueue_market_backfill(
    conn,
    ticker: str,
    cfg: BackfillConfig,
    force_full: bool,
) -> tuple[dict[str, object], int]:
    row = _fetch_market_row(conn, ticker)
    if not row:
        return {"error": "Market not found."}, 404
    series_ticker = row.get("series_ticker")
    if not series_ticker:
        return {"error": "Series ticker missing."}, 503
    strike_period, error_payload = _resolve_strike_period(cfg, row)
    if error_payload:
        return error_payload, 400
    market_payload = _market_payload(row)
    job_id = enqueue_job(
        conn,
        "backfill_market",
        {
            "series_ticker": series_ticker,
            "strike_period": strike_period,
            "market": market_payload,
            "force_full": force_full,
        },
    )
    return {"ticker": ticker, "queued": True, "job_id": job_id}, 200


@bp.post("/market/<ticker>/backfill")
def market_backfill(ticker: str):
    """Backfill candlesticks for a market ticker."""
    mode = _backfill_mode()
    mode_error = _backfill_mode_error(mode)
    if mode_error:
        payload, status = mode_error
        return jsonify(payload), status
    if not _env_bool("WORK_QUEUE_ENABLE", False):
        return jsonify({"error": "Work queue is disabled; cannot enqueue backfill."}), 409
    if not os.getenv("DATABASE_URL"):
        return jsonify({"error": "DATABASE_URL is not set."}), 503

    payload = request.get_json(silent=True) or {}
    force_full = bool(payload.get("force_full"))

    cfg = _load_backfill_config()
    try:
        with _db_connection() as conn:
            response, status = _enqueue_market_backfill(
                conn,
                ticker,
                cfg,
                force_full,
            )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("market backfill failed for %s", ticker)
        response, status = {"error": str(exc) or "Market backfill failed."}, 503

    return jsonify(response), status
