#!/usr/bin/env python3
"""Delete closed events older than a cutoff along with related data."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


def _load_env() -> None:
    env_path = os.getenv("ENV_FILE", str(Path(__file__).resolve().parents[1] / ".env"))
    load_dotenv(dotenv_path=env_path)


def _get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set; cannot run purge.")
    db_url = os.path.expandvars(db_url)
    return os.path.expanduser(db_url)


def _prepare_targets(conn: psycopg.Connection, cutoff_hours: float) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE purge_events (
              event_ticker TEXT PRIMARY KEY
            ) ON COMMIT DROP
            """
        )
        cur.execute(
            """
            INSERT INTO purge_events (event_ticker)
            SELECT e.event_ticker
            FROM events e
            JOIN markets m ON m.event_ticker = e.event_ticker
            GROUP BY e.event_ticker
            HAVING COUNT(*) = COUNT(m.close_time)
              AND MAX(m.close_time) <= NOW() - (%s * INTERVAL '1 hour')
            """,
            (cutoff_hours,),
        )
        cur.execute(
            """
            CREATE TEMP TABLE purge_markets (
              ticker TEXT PRIMARY KEY,
              event_ticker TEXT NOT NULL
            ) ON COMMIT DROP
            """
        )
        cur.execute(
            """
            INSERT INTO purge_markets (ticker, event_ticker)
            SELECT m.ticker, m.event_ticker
            FROM markets m
            JOIN purge_events pe ON pe.event_ticker = m.event_ticker
            """
        )
        cur.execute("SELECT COUNT(*) FROM purge_events")
        event_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM purge_markets")
        market_count = cur.fetchone()[0]
    return event_count, market_count


def _drain_cursor(cur: psycopg.Cursor, batch_size: int = 1000) -> None:
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break


def _lock_targets(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT e.event_ticker
            FROM events e
            JOIN purge_events pe ON pe.event_ticker = e.event_ticker
            FOR UPDATE
            """
        )
        _drain_cursor(cur)
        cur.execute(
            """
            SELECT m.ticker
            FROM markets m
            JOIN purge_markets pm ON pm.ticker = m.ticker
            FOR UPDATE
            """
        )
        _drain_cursor(cur)


def _delete_related_rows(conn: psycopg.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM work_queue w
            USING purge_markets pm
            WHERE w.job_type = 'backfill_market'
              AND (w.payload->'market'->>'ticker') = pm.ticker
            """
        )
        counts["work_queue"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM market_predictions mp
            USING purge_markets pm
            WHERE mp.market_ticker = pm.ticker
            """
        )
        counts["market_predictions"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM prediction_runs pr
            USING purge_events pe
            WHERE pr.event_ticker = pe.event_ticker
            """
        )
        counts["prediction_runs"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM rag_documents rd
            USING purge_markets pm
            WHERE rd.market_ticker = pm.ticker
            """
        )
        counts["rag_documents_market"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM rag_documents rd
            USING purge_events pe
            WHERE rd.event_ticker = pe.event_ticker
            """
        )
        counts["rag_documents_event"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM active_markets am
            USING purge_markets pm
            WHERE am.ticker = pm.ticker
            """
        )
        counts["active_markets"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM market_ticks mt
            USING purge_markets pm
            WHERE mt.ticker = pm.ticker
            """
        )
        counts["market_ticks"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM market_candles mc
            USING purge_markets pm
            WHERE mc.market_ticker = pm.ticker
            """
        )
        counts["market_candles"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM lifecycle_events le
            USING purge_markets pm
            WHERE le.market_ticker = pm.ticker
            """
        )
        counts["lifecycle_events"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM ingest_state s
            USING purge_markets pm
            WHERE s.key LIKE ('backfill_last_ts:' || pm.ticker || ':%')
            """
        )
        counts["ingest_state"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM markets m
            USING purge_markets pm
            WHERE m.ticker = pm.ticker
            """
        )
        counts["markets"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM events e
            USING purge_events pe
            WHERE e.event_ticker = pe.event_ticker
            """
        )
        counts["events"] = cur.rowcount
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Delete closed events older than a cutoff.",
    )
    parser.add_argument(
        "--older-than-hours",
        type=float,
        default=1.0,
        help="Delete events closed before now - N hours (default: 1).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log counts without deleting any rows.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | purge_closed_events | %(message)s",
    )

    _load_env()
    db_url = _get_database_url()

    with psycopg.connect(db_url) as conn:
        try:
            events, markets = _prepare_targets(conn, args.older_than_hours)
            logging.info(
                "targets: events=%d markets=%d cutoff_hours=%.2f dry_run=%s",
                events,
                markets,
                args.older_than_hours,
                args.dry_run,
            )
            if events == 0:
                conn.rollback()
                return 0

            if args.dry_run:
                conn.rollback()
                return 0

            logging.info("locking target events/markets to block concurrent inserts")
            _lock_targets(conn)
            counts = _delete_related_rows(conn)
            conn.commit()
        except Exception:  # pylint: disable=broad-exception-caught
            conn.rollback()
            logging.exception("purge failed; rolled back transaction")
            return 1

    for table, removed in counts.items():
        logging.info("deleted: table=%s rows=%d", table, removed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
