#!/usr/bin/env python3
"""Periodic refresh for precomputed event sparklines."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(value, minimum)


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(value, minimum)


def _load_env() -> None:
    env_file = os.getenv("ENV_FILE")
    if env_file:
        load_dotenv(dotenv_path=env_file)


def _get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set; cannot refresh sparklines.")
    return os.path.expanduser(os.path.expandvars(db_url))


def _min_tick_ts(lookback_hours: float | None) -> datetime | None:
    if lookback_hours is None or lookback_hours <= 0:
        return None
    return datetime.now(timezone.utc) - timedelta(hours=lookback_hours)


def _run_once(db_url: str, max_points: int, lookback_hours: float | None) -> None:
    min_ts = _min_tick_ts(lookback_hours)
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT refresh_event_sparklines(%s, %s)",
                (max_points, min_ts),
            )
            row = cur.fetchone()
    refreshed = row[0] if row else 0
    logging.info(
        "sparkline refresh: updated=%s max_points=%s lookback_hours=%s",
        refreshed,
        max_points,
        lookback_hours if lookback_hours is not None else "all",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh event sparklines.")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run refresh repeatedly on an interval.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=_env_int("WEB_PORTAL_EVENT_SPARKLINE_REFRESH_SECONDS", 900, minimum=60),
        help="Sleep interval between runs when --loop is set (default: 900).",
    )
    parser.add_argument(
        "--lookback-hours",
        type=float,
        default=_env_float("WEB_PORTAL_EVENT_SPARKLINE_LOOKBACK_HOURS", 24.0, minimum=0.0),
        help="Only refresh tickers with recent activity in this lookback window.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Refresh all tickers (ignores lookback window).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | sparkline_refresh | %(message)s",
    )

    _load_env()
    db_url = _get_database_url()
    max_points = _env_int("WEB_PORTAL_EVENT_SPARKLINE_POINTS", 24, minimum=1)
    lookback_hours = None if args.all else args.lookback_hours

    while True:
        try:
            _run_once(db_url, max_points, lookback_hours)
        except Exception:  # pylint: disable=broad-exception-caught
            logging.exception("sparkline refresh failed")
        if not args.loop:
            break
        time.sleep(max(args.interval_seconds, 60))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
