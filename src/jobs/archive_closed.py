"""Archive closed events to a backup database."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import psycopg  # pylint: disable=import-error
from src.db.db import ensure_schema_compatible
from src.jobs.closed_cleanup import _load_missing_closed_markets, build_closed_cleanup_config

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArchiveClosedConfig:
    """Configuration for archiving closed events."""

    cutoff_hours: float
    require_terminal_lifecycle: bool
    batch_size: int


@dataclass(frozen=True)
class ArchiveClosedStats:
    """Stats returned from an archive pass."""

    events: int
    markets: int
    archived_rows: dict[str, int]
    deleted_rows: dict[str, int]


_TERMINAL_LIFECYCLE_MATCH = (
    "%settled%",
    "%determined%",
    "%deactivated%",
    "%closed%",
    "%resolved%",
    "%finalized%",
)


def build_archive_closed_config(settings) -> ArchiveClosedConfig:
    """Build archive settings from loaded Settings."""
    return ArchiveClosedConfig(
        cutoff_hours=settings.archive_closed_hours,
        require_terminal_lifecycle=settings.archive_require_terminal_lifecycle,
        batch_size=settings.archive_batch_size,
    )


def _copy_rows(
    primary_conn: psycopg.Connection,
    backup_conn: psycopg.Connection,
    *,
    select_sql: str,
    insert_sql: str,
    batch_size: int,
    label: str,
) -> int:
    copied = 0
    with primary_conn.cursor() as src_cur, backup_conn.cursor() as dst_cur:
        src_cur.execute(select_sql)
        while True:
            rows = src_cur.fetchmany(batch_size)
            if not rows:
                break
            dst_cur.executemany(insert_sql, rows)
            copied += len(rows)
    backup_conn.commit()
    logger.info("archive copy: table=%s rows=%d", label, copied)
    return copied


def _prepare_archive_targets(
    conn: psycopg.Connection,
    *,
    cutoff_hours: float,
    missing_tickers: set[str],
    require_terminal_lifecycle: bool,
) -> tuple[list[str], list[tuple[str, str]]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE eligible_events (
              event_ticker TEXT PRIMARY KEY
            ) ON COMMIT DROP
            """
        )
        cur.execute(
            """
            INSERT INTO eligible_events (event_ticker)
            SELECT e.event_ticker
            FROM events e
            JOIN markets m ON m.event_ticker = e.event_ticker
            GROUP BY e.event_ticker
            HAVING COUNT(*) = COUNT(m.close_time)
              AND MAX(m.close_time) <= NOW() - (%s * INTERVAL '1 hour')
            """,
            (cutoff_hours,),
        )

        if missing_tickers:
            cur.execute(
                """
                CREATE TEMP TABLE missing_markets (
                  ticker TEXT PRIMARY KEY
                ) ON COMMIT DROP
                """
            )
            cur.executemany(
                "INSERT INTO missing_markets (ticker) VALUES (%s)",
                [(ticker,) for ticker in sorted(missing_tickers)],
            )
            cur.execute(
                """
                DELETE FROM eligible_events ee
                USING markets m
                JOIN missing_markets mm ON mm.ticker = m.ticker
                WHERE m.event_ticker = ee.event_ticker
                """
            )

        if require_terminal_lifecycle:
            cur.execute(
                """
                CREATE TEMP TABLE missing_ws_markets (
                  ticker TEXT PRIMARY KEY,
                  event_ticker TEXT NOT NULL
                ) ON COMMIT DROP
                """
            )
            cur.execute(
                """
                INSERT INTO missing_ws_markets (ticker, event_ticker)
                SELECT m.ticker, m.event_ticker
                FROM markets m
                JOIN eligible_events ee ON ee.event_ticker = m.event_ticker
                LEFT JOIN (
                  SELECT market_ticker
                  FROM lifecycle_events
                  WHERE event_type ILIKE ANY (%s)
                  GROUP BY market_ticker
                ) le ON le.market_ticker = m.ticker
                WHERE le.market_ticker IS NULL
                """,
                (_TERMINAL_LIFECYCLE_MATCH,),
            )
            cur.execute(
                """
                DELETE FROM eligible_events ee
                USING missing_ws_markets mw
                WHERE mw.event_ticker = ee.event_ticker
                """
            )

        cur.execute(
            """
            CREATE TEMP TABLE eligible_markets (
              ticker TEXT PRIMARY KEY,
              event_ticker TEXT NOT NULL
            ) ON COMMIT DROP
            """
        )
        cur.execute(
            """
            INSERT INTO eligible_markets (ticker, event_ticker)
            SELECT m.ticker, m.event_ticker
            FROM markets m
            JOIN eligible_events ee ON ee.event_ticker = m.event_ticker
            """
        )

        cur.execute("SELECT event_ticker FROM eligible_events")
        events = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT ticker, event_ticker FROM eligible_markets")
        markets = cur.fetchall()
    return events, markets


def _create_backup_targets(
    conn: psycopg.Connection,
    events: list[str],
    markets: list[tuple[str, str]],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE archive_events (
              event_ticker TEXT PRIMARY KEY
            ) ON COMMIT PRESERVE ROWS
            """
        )
        cur.executemany(
            "INSERT INTO archive_events (event_ticker) VALUES (%s)",
            [(event_ticker,) for event_ticker in events],
        )
        cur.execute(
            """
            CREATE TEMP TABLE archive_markets (
              ticker TEXT PRIMARY KEY,
              event_ticker TEXT NOT NULL
            ) ON COMMIT PRESERVE ROWS
            """
        )
        cur.executemany(
            "INSERT INTO archive_markets (ticker, event_ticker) VALUES (%s, %s)",
            markets,
        )


def _count_tables(conn: psycopg.Connection, *, event_table: str, market_table: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM events e
            JOIN {event_table} ee ON ee.event_ticker = e.event_ticker
            """
        )
        counts["events"] = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM markets m
            JOIN {market_table} em ON em.ticker = m.ticker
            """
        )
        counts["markets"] = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM active_markets am
            JOIN {market_table} em ON em.ticker = am.ticker
            """
        )
        counts["active_markets"] = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM market_ticks mt
            JOIN {market_table} em ON em.ticker = mt.ticker
            """
        )
        counts["market_ticks"] = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM market_candles mc
            JOIN {market_table} em ON em.ticker = mc.market_ticker
            """
        )
        counts["market_candles"] = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM lifecycle_events le
            JOIN {market_table} em ON em.ticker = le.market_ticker
            """
        )
        counts["lifecycle_events"] = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM rag_documents rd
            LEFT JOIN {market_table} em ON em.ticker = rd.market_ticker
            LEFT JOIN {event_table} ee ON ee.event_ticker = rd.event_ticker
            WHERE em.ticker IS NOT NULL OR ee.event_ticker IS NOT NULL
            """
        )
        counts["rag_documents"] = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM prediction_runs pr
            JOIN {event_table} ee ON ee.event_ticker = pr.event_ticker
            """
        )
        counts["prediction_runs"] = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM market_predictions mp
            LEFT JOIN {market_table} em ON em.ticker = mp.market_ticker
            LEFT JOIN {event_table} ee ON ee.event_ticker = mp.event_ticker
            WHERE em.ticker IS NOT NULL OR ee.event_ticker IS NOT NULL
            """
        )
        counts["market_predictions"] = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM work_queue w
            JOIN {market_table} em
              ON w.job_type = 'backfill_market'
             AND (w.payload->'market'->>'ticker') = em.ticker
            """
        )
        counts["work_queue"] = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM ingest_state s
            JOIN {market_table} em
              ON s.key LIKE ('backfill_last_ts:' || em.ticker || ':%')
            """
        )
        counts["ingest_state"] = int(cur.fetchone()[0] or 0)
    return counts


def _lock_targets(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT e.event_ticker
            FROM events e
            JOIN eligible_events ee ON ee.event_ticker = e.event_ticker
            FOR UPDATE
            """
        )
        while cur.fetchmany(1000):
            continue
        cur.execute(
            """
            SELECT m.ticker
            FROM markets m
            JOIN eligible_markets em ON em.ticker = m.ticker
            FOR UPDATE
            """
        )
        while cur.fetchmany(1000):
            continue


def _delete_archived_rows(conn: psycopg.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM work_queue w
            USING eligible_markets em
            WHERE w.job_type = 'backfill_market'
              AND (w.payload->'market'->>'ticker') = em.ticker
            """
        )
        counts["work_queue"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM market_predictions mp
            USING eligible_markets em
            WHERE mp.market_ticker = em.ticker
            """
        )
        counts["market_predictions"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM prediction_runs pr
            USING eligible_events ee
            WHERE pr.event_ticker = ee.event_ticker
            """
        )
        counts["prediction_runs"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM rag_documents rd
            USING eligible_markets em
            WHERE rd.market_ticker = em.ticker
            """
        )
        counts["rag_documents_market"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM rag_documents rd
            USING eligible_events ee
            WHERE rd.event_ticker = ee.event_ticker
            """
        )
        counts["rag_documents_event"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM active_markets am
            USING eligible_markets em
            WHERE am.ticker = em.ticker
            """
        )
        counts["active_markets"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM market_ticks mt
            USING eligible_markets em
            WHERE mt.ticker = em.ticker
            """
        )
        counts["market_ticks"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM market_candles mc
            USING eligible_markets em
            WHERE mc.market_ticker = em.ticker
            """
        )
        counts["market_candles"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM lifecycle_events le
            USING eligible_markets em
            WHERE le.market_ticker = em.ticker
            """
        )
        counts["lifecycle_events"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM ingest_state s
            USING eligible_markets em
            WHERE s.key LIKE ('backfill_last_ts:' || em.ticker || ':%')
            """
        )
        counts["ingest_state"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM markets m
            USING eligible_markets em
            WHERE m.ticker = em.ticker
            """
        )
        counts["markets"] = cur.rowcount

        cur.execute(
            """
            DELETE FROM events e
            USING eligible_events ee
            WHERE e.event_ticker = ee.event_ticker
            """
        )
        counts["events"] = cur.rowcount
    return counts


def archive_closed_events(
    primary_conn: psycopg.Connection,
    backup_conn: psycopg.Connection,
    settings,
    cfg: ArchiveClosedConfig,
) -> ArchiveClosedStats | None:
    """Archive closed events older than the cutoff into the backup DB."""
    ensure_schema_compatible(backup_conn)
    cleanup_cfg = build_closed_cleanup_config(settings)
    missing_settlement, missing_candles = _load_missing_closed_markets(primary_conn, cleanup_cfg)
    missing_tickers = set(missing_settlement) | set(missing_candles)

    try:
        events, markets = _prepare_archive_targets(
            primary_conn,
            cutoff_hours=cfg.cutoff_hours,
            missing_tickers=missing_tickers,
            require_terminal_lifecycle=cfg.require_terminal_lifecycle,
        )
        if not events:
            primary_conn.rollback()
            return None

        _create_backup_targets(backup_conn, events, markets)
        _lock_targets(primary_conn)
        expected_counts = _count_tables(
            primary_conn,
            event_table="eligible_events",
            market_table="eligible_markets",
        )

        archived_rows: dict[str, int] = {}
        archived_rows["events"] = _copy_rows(
            primary_conn,
            backup_conn,
            select_sql="""
                SELECT event_ticker, series_ticker, title, sub_title, category,
                       mutually_exclusive, collateral_return_type, available_on_brokers,
                       product_metadata, strike_date, strike_period, updated_at
                FROM events e
                JOIN eligible_events ee ON ee.event_ticker = e.event_ticker
            """,
            insert_sql="""
                INSERT INTO events(
                  event_ticker, series_ticker, title, sub_title, category,
                  mutually_exclusive, collateral_return_type, available_on_brokers,
                  product_metadata, strike_date, strike_period, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
            batch_size=cfg.batch_size,
            label="events",
        )
        archived_rows["markets"] = _copy_rows(
            primary_conn,
            backup_conn,
            select_sql="""
                SELECT ticker, event_ticker, market_type, title, subtitle,
                       yes_sub_title, no_sub_title, category, response_price_units,
                       created_time, open_time, close_time, expiration_time,
                       latest_expiration_time, expected_expiration_time,
                       settlement_timer_seconds, can_close_early, early_close_condition,
                       rules_primary, rules_secondary, tick_size, risk_limit_cents,
                       price_level_structure, price_ranges, strike_type, floor_strike,
                       cap_strike, functional_strike, custom_strike,
                       mve_collection_ticker, mve_selected_legs,
                       primary_participant_key,
                       settlement_value, settlement_value_dollars, settlement_ts,
                       updated_at
                FROM markets m
                JOIN eligible_markets em ON em.ticker = m.ticker
            """,
            insert_sql="""
                INSERT INTO markets(
                  ticker, event_ticker, market_type, title, subtitle,
                  yes_sub_title, no_sub_title, category, response_price_units,
                  created_time, open_time, close_time, expiration_time,
                  latest_expiration_time, expected_expiration_time,
                  settlement_timer_seconds, can_close_early, early_close_condition,
                  rules_primary, rules_secondary, tick_size, risk_limit_cents,
                  price_level_structure, price_ranges, strike_type, floor_strike,
                  cap_strike, functional_strike, custom_strike,
                  mve_collection_ticker, mve_selected_legs,
                  primary_participant_key,
                  settlement_value, settlement_value_dollars, settlement_ts,
                  updated_at
                )
                VALUES (
                  %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s,
                  %s, %s,
                  %s,
                  %s, %s, %s,
                  %s
                )
                ON CONFLICT DO NOTHING
            """,
            batch_size=cfg.batch_size,
            label="markets",
        )
        archived_rows["prediction_runs"] = _copy_rows(
            primary_conn,
            backup_conn,
            select_sql="""
                SELECT id, event_ticker, run_ts, prompt, agent, model, status, error, metadata
                FROM prediction_runs pr
                JOIN eligible_events ee ON ee.event_ticker = pr.event_ticker
            """,
            insert_sql="""
                INSERT INTO prediction_runs(
                  id, event_ticker, run_ts, prompt, agent, model, status, error, metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
            batch_size=cfg.batch_size,
            label="prediction_runs",
        )
        archived_rows["market_predictions"] = _copy_rows(
            primary_conn,
            backup_conn,
            select_sql="""
                SELECT mp.id, mp.run_id, mp.event_ticker, mp.market_ticker,
                       mp.predicted_yes_prob, mp.confidence, mp.rationale, mp.raw,
                       mp.created_at
                FROM market_predictions mp
                LEFT JOIN eligible_markets em ON em.ticker = mp.market_ticker
                LEFT JOIN eligible_events ee ON ee.event_ticker = mp.event_ticker
                WHERE em.ticker IS NOT NULL OR ee.event_ticker IS NOT NULL
            """,
            insert_sql="""
                INSERT INTO market_predictions(
                  id, run_id, event_ticker, market_ticker,
                  predicted_yes_prob, confidence, rationale, raw, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
            batch_size=cfg.batch_size,
            label="market_predictions",
        )
        archived_rows["rag_documents"] = _copy_rows(
            primary_conn,
            backup_conn,
            select_sql="""
                SELECT rd.id, rd.source, rd.source_id, rd.event_ticker, rd.market_ticker,
                       rd.content, rd.embedding, rd.metadata, rd.created_at, rd.updated_at
                FROM rag_documents rd
                LEFT JOIN eligible_markets em ON em.ticker = rd.market_ticker
                LEFT JOIN eligible_events ee ON ee.event_ticker = rd.event_ticker
                WHERE em.ticker IS NOT NULL OR ee.event_ticker IS NOT NULL
            """,
            insert_sql="""
                INSERT INTO rag_documents(
                  id, source, source_id, event_ticker, market_ticker,
                  content, embedding, metadata, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
            batch_size=cfg.batch_size,
            label="rag_documents",
        )
        archived_rows["active_markets"] = _copy_rows(
            primary_conn,
            backup_conn,
            select_sql="""
                SELECT am.ticker, am.event_ticker, am.close_time, am.status,
                       am.last_seen_ts, am.updated_at
                FROM active_markets am
                JOIN eligible_markets em ON em.ticker = am.ticker
            """,
            insert_sql="""
                INSERT INTO active_markets(
                  ticker, event_ticker, close_time, status, last_seen_ts, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
            batch_size=cfg.batch_size,
            label="active_markets",
        )
        archived_rows["market_ticks"] = _copy_rows(
            primary_conn,
            backup_conn,
            select_sql="""
                SELECT id, ts, ticker, price, yes_bid, yes_ask,
                       price_dollars, yes_bid_dollars, yes_ask_dollars, no_bid_dollars,
                       volume, open_interest, dollar_volume, dollar_open_interest,
                       implied_yes_mid, sid, raw
                FROM market_ticks mt
                JOIN eligible_markets em ON em.ticker = mt.ticker
            """,
            insert_sql="""
                INSERT INTO market_ticks(
                  id, ts, ticker, price, yes_bid, yes_ask,
                  price_dollars, yes_bid_dollars, yes_ask_dollars, no_bid_dollars,
                  volume, open_interest, dollar_volume, dollar_open_interest,
                  implied_yes_mid, sid, raw
                )
                VALUES (
                  %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s,
                  %s, %s, %s, %s,
                  %s, %s, %s
                )
                ON CONFLICT DO NOTHING
            """,
            batch_size=cfg.batch_size,
            label="market_ticks",
        )
        archived_rows["market_candles"] = _copy_rows(
            primary_conn,
            backup_conn,
            select_sql="""
                SELECT market_ticker, period_interval_minutes, end_period_ts, start_period_ts,
                       open, high, low, close, volume, open_interest, raw
                FROM market_candles mc
                JOIN eligible_markets em ON em.ticker = mc.market_ticker
            """,
            insert_sql="""
                INSERT INTO market_candles(
                  market_ticker, period_interval_minutes, end_period_ts, start_period_ts,
                  open, high, low, close, volume, open_interest, raw
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
            batch_size=cfg.batch_size,
            label="market_candles",
        )
        archived_rows["lifecycle_events"] = _copy_rows(
            primary_conn,
            backup_conn,
            select_sql="""
                SELECT id, ts, market_ticker, event_type, open_ts, close_ts, raw
                FROM lifecycle_events le
                JOIN eligible_markets em ON em.ticker = le.market_ticker
            """,
            insert_sql="""
                INSERT INTO lifecycle_events(
                  id, ts, market_ticker, event_type, open_ts, close_ts, raw
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
            batch_size=cfg.batch_size,
            label="lifecycle_events",
        )
        archived_rows["work_queue"] = _copy_rows(
            primary_conn,
            backup_conn,
            select_sql="""
                SELECT id, job_type, payload, status, attempts, max_attempts,
                       available_at, locked_by, locked_at, last_error, created_at, updated_at
                FROM work_queue w
                JOIN eligible_markets em
                  ON w.job_type = 'backfill_market'
                 AND (w.payload->'market'->>'ticker') = em.ticker
            """,
            insert_sql="""
                INSERT INTO work_queue(
                  id, job_type, payload, status, attempts, max_attempts,
                  available_at, locked_by, locked_at, last_error, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
            batch_size=cfg.batch_size,
            label="work_queue",
        )
        archived_rows["ingest_state"] = _copy_rows(
            primary_conn,
            backup_conn,
            select_sql="""
                SELECT s.key, s.value, s.updated_at
                FROM ingest_state s
                JOIN eligible_markets em
                  ON s.key LIKE ('backfill_last_ts:' || em.ticker || ':%')
            """,
            insert_sql="""
                INSERT INTO ingest_state(key, value, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
            batch_size=cfg.batch_size,
            label="ingest_state",
        )

        backup_counts = _count_tables(
            backup_conn,
            event_table="archive_events",
            market_table="archive_markets",
        )
        for table, expected in expected_counts.items():
            if backup_counts.get(table, 0) < expected:
                raise RuntimeError(
                    "archive verification failed: "
                    f"table={table} expected={expected} backup={backup_counts.get(table, 0)}"
                )

        deleted_rows = _delete_archived_rows(primary_conn)
        primary_conn.commit()
        logger.info(
            "archive delete: events=%d markets=%d",
            len(events),
            len(markets),
        )
        return ArchiveClosedStats(
            events=len(events),
            markets=len(markets),
            archived_rows=archived_rows,
            deleted_rows=deleted_rows,
        )
    except Exception:
        primary_conn.rollback()
        backup_conn.rollback()
        logger.exception("archive failed; rolled back")
        raise
