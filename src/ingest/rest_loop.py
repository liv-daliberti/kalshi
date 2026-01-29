"""REST discovery/backfill/cleanup loops."""

from __future__ import annotations

import logging
import random
import time

import psycopg  # pylint: disable=import-error

from ..jobs.backfill import backfill_pass
from ..jobs.backfill_config import build_backfill_config_from_settings
from ..jobs.archive_closed import archive_closed_events, build_archive_closed_config
from ..jobs.closed_cleanup import build_closed_cleanup_config, closed_cleanup_pass
from ..db.db import ensure_schema_compatible, maybe_init_schema
from ..core.db_utils import safe_close
from ..core.env_utils import _env_float, _env_int
from ..jobs.discovery import discovery_pass
from ..kalshi.kalshi_sdk import make_client
from ..core.loop_utils import LoopFailureContext, log_metric, schema_path
from ..queue.work_queue import QueuePublisher

logger = logging.getLogger(__name__)

_SCHEMA_PATH = schema_path(__file__)
_DB_RECONNECT_ERRORS = (
    psycopg.OperationalError,
    psycopg.InterfaceError,
)


def _safe_rollback(conn: psycopg.Connection | None) -> None:
    if conn is None:
        return
    try:
        conn.rollback()
    except Exception:  # pylint: disable=broad-exception-caught
        logger.warning("DB rollback failed after error")


def _should_reconnect(exc: Exception) -> bool:
    return isinstance(exc, _DB_RECONNECT_ERRORS)


def _connect_rest_resources(settings, private_key_pem: str):
    base_sleep = _env_float("REST_RECONNECT_BASE_SECONDS", 1.0, minimum=0.1)
    max_sleep = _env_float("REST_RECONNECT_MAX_SECONDS", 30.0, minimum=1.0)
    attempt = 0
    while True:
        conn = None
        try:
            client = make_client(
                settings.kalshi_host,
                settings.kalshi_api_key_id,
                private_key_pem,
            )
            conn = psycopg.connect(settings.database_url)
            maybe_init_schema(conn, schema_path=_SCHEMA_PATH)
            ensure_schema_compatible(conn)
            return client, conn
        except Exception:  # pylint: disable=broad-exception-caught
            _safe_rollback(conn)
            safe_close(conn, logger=logger, warn_message="DB close failed during reconnect")
            logger.exception("REST setup failed; retrying")
            sleep_s = min(max_sleep, base_sleep * (2 ** attempt))
            sleep_s += random.uniform(0.0, min(base_sleep, 1.0))
            time.sleep(sleep_s)
            attempt += 1


def rest_discovery_loop(settings, private_key_pem: str) -> None:
    """Run the REST discovery polling loop."""
    client = None
    conn = None
    failure_threshold = _env_int("REST_FAILURE_THRESHOLD", 5, minimum=1)
    breaker_seconds = _env_float("REST_CIRCUIT_BREAKER_SECONDS", 60.0, minimum=1.0)
    failure_ctx = LoopFailureContext(
        name="rest.discovery",
        failure_threshold=failure_threshold,
        breaker_seconds=breaker_seconds,
    )
    while True:
        if conn is None or client is None:
            client, conn = _connect_rest_resources(settings, private_key_pem)
        start = time.monotonic()
        try:
            events, markets, active = discovery_pass(
                conn,
                client,
                settings.strike_periods,
                settings.discovery_event_statuses,
            )
            duration = time.monotonic() - start
            failure_ctx.record_success(logger, "discovery recovered after %d failures")
            log_metric(
                logger,
                "rest.discovery",
                duration_s=round(duration, 2),
                events=events,
                markets=markets,
                active=active,
                errors_total=failure_ctx.error_total,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.exception("discovery failed")
            _safe_rollback(conn)
            if _should_reconnect(exc):
                safe_close(conn, logger=logger, warn_message="DB close failed during reconnect")
                conn = None
                client = None
            if failure_ctx.handle_exception(logger, start):
                continue
            if conn is None or client is None:
                continue
        elapsed = time.monotonic() - start
        sleep_for = max(0.0, settings.discovery_seconds - elapsed)
        time.sleep(sleep_for)


def rest_backfill_loop(settings, private_key_pem: str, queue_cfg) -> None:
    """Run the REST backfill polling loop."""
    client = None
    conn = None
    failure_threshold = _env_int("REST_FAILURE_THRESHOLD", 5, minimum=1)
    breaker_seconds = _env_float("REST_CIRCUIT_BREAKER_SECONDS", 60.0, minimum=1.0)
    failure_ctx = LoopFailureContext(
        name="rest.backfill",
        failure_threshold=failure_threshold,
        breaker_seconds=breaker_seconds,
    )
    backfill_cfg = build_backfill_config_from_settings(settings)
    queue_publisher = None
    if queue_cfg.enabled and queue_cfg.rabbitmq.publish:
        try:
            queue_publisher = QueuePublisher(queue_cfg)
            logger.info("Queue enabled: publishing to %s", queue_cfg.rabbitmq.queue_name)
        except Exception:  # pylint: disable=broad-exception-caught
            queue_publisher = None
            logger.exception("Queue enabled but RabbitMQ unavailable; DB-only queue")

    while True:
        if conn is None or client is None:
            client, conn = _connect_rest_resources(settings, private_key_pem)
        start = time.monotonic()
        try:
            stats = backfill_pass(
                conn,
                client,
                backfill_cfg,
                queue_cfg=queue_cfg,
                publisher=queue_publisher,
            )
            duration = time.monotonic() - start
            failure_ctx.record_success(logger, "backfill recovered after %d failures")
            log_metric(
                logger,
                "rest.backfill",
                duration_s=round(duration, 2),
                events=stats[0],
                markets=stats[1],
                candles=stats[2],
                errors_total=failure_ctx.error_total,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.exception("backfill failed")
            _safe_rollback(conn)
            if _should_reconnect(exc):
                safe_close(conn, logger=logger, warn_message="DB close failed during reconnect")
                conn = None
                client = None
            if failure_ctx.handle_exception(logger, start):
                continue
            if conn is None or client is None:
                continue
        time.sleep(settings.backfill_seconds)


def _maybe_archive_closed(conn, settings) -> None:
    backup_url = getattr(settings, "backup_database_url", None)
    if not backup_url:
        return
    try:
        archive_cfg = build_archive_closed_config(settings)
        with psycopg.connect(backup_url) as backup_conn:
            stats = archive_closed_events(conn, backup_conn, settings, archive_cfg)
            if stats:
                log_metric(
                    logger,
                    "rest.archive_closed",
                    events=stats.events,
                    markets=stats.markets,
                )
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("archive closed failed")


def rest_closed_cleanup_loop(settings, private_key_pem: str) -> None:
    """Run the REST closed-market cleanup loop."""
    client = None
    conn = None
    failure_ctx = LoopFailureContext(
        name="rest.closed_cleanup",
        failure_threshold=_env_int("REST_FAILURE_THRESHOLD", 5, minimum=1),
        breaker_seconds=_env_float("REST_CIRCUIT_BREAKER_SECONDS", 60.0, minimum=1.0),
    )
    cfg = build_closed_cleanup_config(settings)
    while True:
        if conn is None or client is None:
            client, conn = _connect_rest_resources(settings, private_key_pem)
        start = time.monotonic()
        try:
            stats = closed_cleanup_pass(conn, client, cfg)
            failure_ctx.record_success(
                logger,
                "closed cleanup recovered after %d failures",
            )
            log_metric(
                logger,
                "rest.closed_cleanup",
                duration_s=round(time.monotonic() - start, 2),
                events=stats[0],
                markets=stats[1],
                candles=stats[2],
                errors_total=failure_ctx.error_total,
            )
            _maybe_archive_closed(conn, settings)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.exception("closed cleanup failed")
            _safe_rollback(conn)
            if _should_reconnect(exc):
                safe_close(conn, logger=logger, warn_message="DB close failed during reconnect")
                conn = None
                client = None
            if failure_ctx.handle_exception(logger, start):
                continue
            if conn is None or client is None:
                continue
        time.sleep(settings.closed_cleanup_seconds)
