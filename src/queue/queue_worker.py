"""Worker process for RabbitMQ + DB-backed work queue."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg  # pylint: disable=import-error

from src.jobs.backfill import BackfillConfig, MarketContext, backfill_market
from src.jobs.backfill_config import build_backfill_config_from_settings
from src.jobs.discover_market import discover_market
from src.db.db import maybe_init_schema, set_state
from src.core.env_utils import _env_int
from src.core.guardrails import assert_service_role
from src.core.logging_utils import configure_logging as configure_service_logging, parse_log_level
from src.core.loop_utils import schema_path
from src.core.service_utils import open_client_and_conn
from src.core.settings import Settings, load_settings
from src.queue.work_queue import (
    QueueConfig,
    WorkItem,
    claim_job_by_id,
    claim_next_job,
    cleanup_finished_jobs,
    load_queue_config,
    mark_done,
    mark_failed,
    open_rabbitmq_channel,
    requeue_stale_jobs,
)

logger = logging.getLogger(__name__)

# Backward-compat alias for tests and older imports.
init_schema = maybe_init_schema


@dataclass
class WorkerLoopState:
    """Track transient state for the worker loop."""

    connection: Any | None = None
    channel: Any | None = None
    last_maintenance: float = 0.0
    last_metrics: float = 0.0
    last_heartbeat: float = 0.0
    rabbitmq_retry_at: float = 0.0


@dataclass(frozen=True)
class WorkerLoopTiming:
    """Timing intervals for worker loop housekeeping."""

    maintenance_every: float
    heartbeat_every: float
    metrics_every: float


def configure_logging() -> None:
    """Initialize logging based on LOG_LEVEL."""
    level_raw = os.getenv("LOG_LEVEL", "INFO")
    configure_service_logging(
        service_name="worker",
        logger=logger,
        basic_config=logging.basicConfig,
        level_raw=level_raw,
    )


def _parse_log_level(raw: str) -> int:
    return parse_log_level(raw, logging.INFO)


def _process_item(
    conn: psycopg.Connection,
    client,
    cfg: BackfillConfig,
    item: WorkItem,
) -> int:
    if item.job_type == "backfill_market":
        payload = item.payload or {}
        series_ticker = payload.get("series_ticker")
        strike_period = payload.get("strike_period")
        market = payload.get("market") or {}
        force_full = bool(payload.get("force_full"))
        if not series_ticker or not strike_period or not market.get("ticker"):
            raise ValueError(f"Invalid backfill payload for job {item.job_id}")
        ctx = MarketContext(series_ticker, market, strike_period)
        return backfill_market(
            conn,
            client,
            cfg,
            ctx,
            force_full=force_full,
        )
    if item.job_type == "discover_market":
        payload = item.payload or {}
        ticker = payload.get("ticker") or payload.get("market_ticker")
        if not ticker:
            raise ValueError(f"Invalid discover_market payload for job {item.job_id}")
        return discover_market(conn, client, str(ticker))
    raise ValueError(f"Unknown job type {item.job_type}")


def _safe_close(handle) -> None:
    if not handle:
        return
    try:
        handle.close()
    except Exception:  # pylint: disable=broad-exception-caught
        pass


def _log_queue_metrics(conn: psycopg.Connection, queue_cfg: QueueConfig) -> None:
    where_clause = ""
    params = ()
    if queue_cfg.job_types:
        where_clause = "WHERE job_type = ANY(%s)"
        params = (list(queue_cfg.job_types),)
    query = f"""
        SELECT
          COUNT(*) FILTER (
            WHERE status = 'pending' AND available_at <= NOW()
          ) AS pending,
          COUNT(*) FILTER (WHERE status = 'running') AS running,
          COUNT(*) FILTER (WHERE status = 'failed') AS failed,
          MAX(EXTRACT(EPOCH FROM (NOW() - available_at))) FILTER (
            WHERE status = 'pending' AND available_at <= NOW()
          ) AS oldest_pending_seconds
        FROM work_queue
        {where_clause}
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
        if not row:
            return
        pending = int(row[0] or 0)
        running = int(row[1] or 0)
        failed = int(row[2] or 0)
        oldest_pending = row[3]
        oldest_pending = round(float(oldest_pending), 1) if oldest_pending is not None else 0.0
        logger.info(
            "metric=worker.queue pending=%d running=%d failed=%d oldest_pending_s=%s",
            pending,
            running,
            failed,
            oldest_pending,
        )
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("queue metrics failed")


def _maybe_connect_rabbitmq(
    queue_cfg: QueueConfig,
    connection,
    channel,
    rabbitmq_retry_at: float,
    now: float,
):
    if (
        not queue_cfg.rabbitmq.publish
        or not queue_cfg.rabbitmq.url
        or channel is not None
        or now < rabbitmq_retry_at
    ):
        return connection, channel, rabbitmq_retry_at
    try:
        connection, channel = open_rabbitmq_channel(queue_cfg)
        logger.info("Connected to RabbitMQ queue=%s", queue_cfg.rabbitmq.queue_name)
        return connection, channel, rabbitmq_retry_at
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("RabbitMQ connection failed; retrying")
        _safe_close(connection)
        return None, None, time.monotonic() + queue_cfg.timing.poll_seconds


def _fetch_rabbitmq_item(
    conn: psycopg.Connection,
    queue_cfg: QueueConfig,
    connection,
    channel,
):
    if channel is None:
        return connection, channel, None, False
    try:
        method_frame, _props, body = channel.basic_get(
            queue=queue_cfg.rabbitmq.queue_name,
            auto_ack=False,
        )
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("RabbitMQ read failed; reconnecting")
        _safe_close(channel)
        _safe_close(connection)
        time.sleep(queue_cfg.timing.poll_seconds)
        return None, None, None, True

    if not method_frame:
        return connection, channel, None, False

    try:
        job_id = int(body.decode("utf-8").strip())
    except (ValueError, AttributeError):
        logger.warning("Invalid job id payload: %s", body)
        channel.basic_ack(method_frame.delivery_tag)
        return connection, channel, None, False

    item = claim_job_by_id(conn, job_id, queue_cfg.worker_id, queue_cfg.job_types)
    if item is None and queue_cfg.job_types:
        logger.warning(
            "Job %s not claimed (job_types=%s); leaving for matching worker",
            job_id,
            ",".join(queue_cfg.job_types),
        )
    channel.basic_ack(method_frame.delivery_tag)
    return connection, channel, item, False


def _run_queue_maintenance(
    conn: psycopg.Connection,
    queue_cfg: QueueConfig,
    last_maintenance: float,
    now: float,
    maintenance_every: float,
) -> float:
    if now - last_maintenance < maintenance_every:
        return last_maintenance
    stale = requeue_stale_jobs(conn, queue_cfg.timing.lock_timeout_seconds)
    cleaned = cleanup_finished_jobs(conn, queue_cfg.timing.cleanup_done_hours)
    if stale:
        logger.info("Requeued stale jobs=%d", stale)
    if cleaned:
        logger.info("Cleaned finished jobs=%d", cleaned)
    return now


def _handle_item(
    conn: psycopg.Connection,
    client,
    cfg: BackfillConfig,
    queue_cfg: QueueConfig,
    item: WorkItem,
) -> None:
    try:
        _process_item(conn, client, cfg, item)
        mark_done(conn, item.job_id)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("Job failed id=%s type=%s", item.job_id, item.job_type)
        try:
            conn.rollback()
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        mark_failed(conn, item, exc, queue_cfg.timing.retry_delay_seconds)


def _init_worker_loop_state(queue_cfg: QueueConfig) -> tuple[WorkerLoopState, WorkerLoopTiming]:
    poll_seconds = float(queue_cfg.timing.poll_seconds)
    timing = WorkerLoopTiming(
        maintenance_every=max(30.0, poll_seconds),
        heartbeat_every=max(30.0, poll_seconds),
        metrics_every=_env_int("WORKER_QUEUE_METRICS_SECONDS", 60, minimum=10),
    )
    started_at = time.monotonic()
    state = WorkerLoopState(
        last_maintenance=started_at,
        last_metrics=started_at,
    )
    return state, timing


def _run_worker_loop(
    conn: psycopg.Connection,
    client,
    cfg: BackfillConfig,
    queue_cfg: QueueConfig,
) -> None:
    state, timing = _init_worker_loop_state(queue_cfg)

    while True:
        now = time.monotonic()
        if now - state.last_metrics >= timing.metrics_every:
            _log_queue_metrics(conn, queue_cfg)
            state.last_metrics = now
        state.connection, state.channel, state.rabbitmq_retry_at = _maybe_connect_rabbitmq(
            queue_cfg,
            state.connection,
            state.channel,
            state.rabbitmq_retry_at,
            now,
        )

        state.connection, state.channel, item, should_continue = _fetch_rabbitmq_item(
            conn,
            queue_cfg,
            state.connection,
            state.channel,
        )
        if should_continue:
            continue
        if item is None:
            item = claim_next_job(conn, queue_cfg.worker_id, queue_cfg.job_types)

        if item is None:
            state.last_maintenance = _run_queue_maintenance(
                conn,
                queue_cfg,
                state.last_maintenance,
                now,
                timing.maintenance_every,
            )
            if now - state.last_heartbeat >= timing.heartbeat_every:
                set_state(conn, "last_worker_ts", datetime.now(timezone.utc).isoformat())
                state.last_heartbeat = now
            time.sleep(queue_cfg.timing.poll_seconds)
            continue

        _handle_item(conn, client, cfg, queue_cfg, item)
        now = time.monotonic()
        if now - state.last_heartbeat >= timing.heartbeat_every:
            set_state(conn, "last_worker_ts", datetime.now(timezone.utc).isoformat())
            state.last_heartbeat = now


def run_worker(
    settings: Optional[Settings] = None,
    private_key_pem: Optional[str] = None,
    queue_cfg: Optional[QueueConfig] = None,
    configure_log: bool = True,
) -> None:
    """Run a queue worker loop for backfill jobs."""
    assert_service_role("worker", "run_worker")
    if configure_log:
        configure_logging()

    if settings is None:
        settings = load_settings()
    if queue_cfg is None:
        queue_cfg = load_queue_config()
    if not queue_cfg.enabled:
        logger.error("WORK_QUEUE_ENABLE not set; worker exiting")
        return
    if not queue_cfg.rabbitmq.publish or not queue_cfg.rabbitmq.url:
        logger.info("RabbitMQ disabled; worker will poll DB queue")
    client, conn = open_client_and_conn(
        settings,
        private_key_pem=private_key_pem,
        schema_path_override=schema_path(__file__),
    )
    cfg = build_backfill_config_from_settings(settings)
    logger.info(
        "Queue worker started id=%s queue=%s job_types=%s",
        queue_cfg.worker_id,
        queue_cfg.rabbitmq.queue_name,
        ",".join(queue_cfg.job_types) if queue_cfg.job_types else "all",
    )
    _run_worker_loop(conn, client, cfg, queue_cfg)


if __name__ == "__main__":
    run_worker()
