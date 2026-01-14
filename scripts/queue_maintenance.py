#!/usr/bin/env python3
"""Periodic maintenance for the DB-backed work queue."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import psycopg
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.queue import work_queue


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(value, minimum)


def _load_env() -> None:
    env_file = os.getenv("ENV_FILE")
    if env_file:
        load_dotenv(dotenv_path=env_file)


def _run_once(db_url: str) -> None:
    lock_timeout_seconds = _env_int("WORK_QUEUE_LOCK_TIMEOUT_SECONDS", 900, minimum=10)
    cleanup_done_hours = _env_int("WORK_QUEUE_CLEANUP_HOURS", 24, minimum=1)
    with psycopg.connect(db_url) as conn:
        stale = work_queue.requeue_stale_jobs(conn, lock_timeout_seconds)
        cleaned = work_queue.cleanup_finished_jobs(conn, cleanup_done_hours)
    if stale or cleaned:
        logging.info(
            "queue maintenance: requeued=%s cleaned=%s lock_timeout_s=%s cleanup_hours=%s",
            stale,
            cleaned,
            lock_timeout_seconds,
            cleanup_done_hours,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Work queue maintenance runner.")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run maintenance repeatedly on an interval.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=_env_int("WORK_QUEUE_MAINTENANCE_SECONDS", 900, minimum=60),
        help="Sleep interval between runs when --loop is set (default: 900).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | queue_maintenance | %(message)s",
    )

    _load_env()
    if os.getenv("WORK_QUEUE_ENABLE", "0") != "1":
        logging.info("WORK_QUEUE_ENABLE is not set; skipping maintenance.")
        return 0

    os.environ.setdefault("SERVICE_ROLE", "worker")
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logging.error("DATABASE_URL is not set; cannot run maintenance.")
        return 1

    while True:
        try:
            _run_once(db_url)
        except Exception:  # pylint: disable=broad-exception-caught
            logging.exception("queue maintenance failed")
        if not args.loop:
            break
        time.sleep(max(args.interval_seconds, 60))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
