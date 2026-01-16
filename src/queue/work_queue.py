"""DB-backed work queue with optional RabbitMQ signaling."""

from __future__ import annotations

import json
import logging
import os
import socket
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import psycopg  # pylint: disable=import-error

from src.core.guardrails import assert_queue_op_allowed

try:
    import pika as PIKA  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    PIKA = None

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QueueRabbitMQConfig:
    """RabbitMQ settings for the work queue."""

    url: str
    queue_name: str
    prefetch: int
    publish: bool


@dataclass(frozen=True)
class QueueTimingConfig:
    """Timing and retry settings for the work queue."""

    poll_seconds: int
    retry_delay_seconds: int
    lock_timeout_seconds: int
    max_attempts: int
    cleanup_done_hours: int


@dataclass(frozen=True)
class QueueConfig:
    """Configuration for the work queue and RabbitMQ."""

    enabled: bool
    job_types: tuple[str, ...]
    worker_id: str
    rabbitmq: QueueRabbitMQConfig
    timing: QueueTimingConfig


@dataclass(frozen=True)
class WorkItem:
    """Work item payload from the queue."""

    job_id: int
    job_type: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int


def _parse_bool(raw: Optional[str]) -> bool:
    if not raw:
        return False
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_int(raw: Optional[str], default: int) -> int:
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _parse_csv(raw: Optional[str]) -> tuple[str, ...]:
    if raw is None:
        return tuple()
    items = []
    for part in raw.split(","):
        value = part.strip().lower()
        if value:
            items.append(value)
    return tuple(items)


def _default_worker_id() -> str:
    host = socket.gethostname()
    return f"{host}-{os.getpid()}"


def _queue_name_env_key(job_type: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in job_type.strip().upper())
    return f"WORK_QUEUE_NAME_{cleaned}"


def queue_name_for_job_type(job_type: str, default_queue: str) -> str:
    """Resolve a queue name for a specific job type."""
    if not job_type:
        return default_queue
    env_key = _queue_name_env_key(job_type)
    override = os.getenv(env_key)
    if override:
        return override
    return default_queue


def _default_queue_for_job_types(job_types: tuple[str, ...]) -> str:
    job_set = set(job_types)
    if job_set and job_set.issubset({"backfill_market", "discover_market"}):
        override = os.getenv(_queue_name_env_key("backfill_market"))
        if override:
            return override
        if "backfill_market" in job_set:
            return "kalshi.backfill"
    if len(job_types) == 1:
        job_type = job_types[0]
        override = os.getenv(_queue_name_env_key(job_type))
        if override:
            return override
        if job_type == "cleanup_market":
            return "kalshi.cleanup"
    return "kalshi.ingest"


def _load_job_types() -> tuple[str, ...]:
    raw_job_types = os.getenv("WORK_QUEUE_JOB_TYPES")
    if raw_job_types is None:
        return ("backfill_market", "discover_market")
    return _parse_csv(raw_job_types)


def _load_rabbitmq_config(job_types: tuple[str, ...]) -> QueueRabbitMQConfig:
    rabbitmq_url_env = os.getenv("RABBITMQ_URL")
    rabbitmq_url_env = rabbitmq_url_env.strip() if rabbitmq_url_env else ""
    publish_default = "1" if rabbitmq_url_env else "0"
    publish = _parse_bool(os.getenv("WORK_QUEUE_PUBLISH", publish_default))
    rabbitmq_url = rabbitmq_url_env or (
        "amqp://guest:guest@localhost:5672/" if publish else ""
    )
    queue_name = os.getenv("WORK_QUEUE_NAME") or _default_queue_for_job_types(job_types)
    prefetch = _parse_int(os.getenv("WORK_QUEUE_PREFETCH"), 4)
    return QueueRabbitMQConfig(
        url=rabbitmq_url,
        queue_name=queue_name,
        prefetch=prefetch,
        publish=publish,
    )


def _load_queue_timing() -> QueueTimingConfig:
    poll_seconds = _parse_int(os.getenv("WORK_QUEUE_POLL_SECONDS"), 10)
    retry_delay_seconds = _parse_int(os.getenv("WORK_QUEUE_RETRY_SECONDS"), 60)
    lock_timeout_seconds = _parse_int(os.getenv("WORK_QUEUE_LOCK_TIMEOUT_SECONDS"), 900)
    max_attempts = _parse_int(os.getenv("WORK_QUEUE_MAX_ATTEMPTS"), 5)
    cleanup_done_hours = _parse_int(os.getenv("WORK_QUEUE_CLEANUP_HOURS"), 24)
    return QueueTimingConfig(
        poll_seconds=poll_seconds,
        retry_delay_seconds=retry_delay_seconds,
        lock_timeout_seconds=lock_timeout_seconds,
        max_attempts=max_attempts,
        cleanup_done_hours=cleanup_done_hours,
    )


def load_queue_config() -> QueueConfig:
    """Load queue settings from the environment."""
    enabled = _parse_bool(os.getenv("WORK_QUEUE_ENABLE"))
    job_types = _load_job_types()
    rabbitmq = _load_rabbitmq_config(job_types)
    timing = _load_queue_timing()
    worker_id = os.getenv("WORK_QUEUE_WORKER_ID") or _default_worker_id()
    return QueueConfig(
        enabled=enabled,
        job_types=job_types,
        worker_id=worker_id,
        rabbitmq=rabbitmq,
        timing=timing,
    )


def _parse_payload(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return dict(raw)


class QueuePublisher:
    """RabbitMQ publisher for notifying workers of new jobs."""

    def __init__(self, cfg: QueueConfig) -> None:
        self.cfg = cfg
        self._disabled = False
        self._declared: set[str] = set()
        if PIKA is None:
            raise RuntimeError("pika is required for RabbitMQ publishing")
        params = PIKA.URLParameters(cfg.rabbitmq.url)
        self.connection = PIKA.BlockingConnection(params)
        self.channel = self.connection.channel()
        self._declare_queue(cfg.rabbitmq.queue_name)

    def _declare_queue(self, queue_name: str) -> None:
        if queue_name in self._declared:
            return
        self.channel.queue_declare(queue=queue_name, durable=True)
        self._declared.add(queue_name)

    def publish(self, job_id: int, job_type: Optional[str] = None) -> None:
        """Publish a job notification to RabbitMQ."""
        if self._disabled:
            return
        queue_name = queue_name_for_job_type(job_type or "", self.cfg.rabbitmq.queue_name)
        self._declare_queue(queue_name)
        self.channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=str(job_id).encode("utf-8"),
            properties=PIKA.BasicProperties(delivery_mode=2),
        )

    def disable(self) -> None:
        """Disable publishing after persistent failures."""
        self._disabled = True

    def close(self) -> None:
        """Close the RabbitMQ connection."""
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Failed to close RabbitMQ connection")

    def __enter__(self) -> "QueuePublisher":
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        self.close()


def open_rabbitmq_channel(cfg: QueueConfig) -> tuple[Any, Any]:
    """Open a RabbitMQ channel for consuming messages."""
    if PIKA is None:
        raise RuntimeError("pika is required for RabbitMQ consumption")
    params = PIKA.URLParameters(cfg.rabbitmq.url)
    connection = PIKA.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=cfg.rabbitmq.queue_name, durable=True)
    channel.basic_qos(prefetch_count=max(cfg.rabbitmq.prefetch, 1))
    return connection, channel


def enqueue_job(
    conn: psycopg.Connection,
    job_type: str,
    payload: dict[str, Any],
    available_at: Optional[datetime] = None,
    max_attempts: Optional[int] = None,
    *,
    commit: bool = True,
) -> int:
    """Insert a new job into the work queue and return its id."""
    assert_queue_op_allowed("enqueue")
    payload_json = json.dumps(payload, separators=(",", ":"))
    if available_at is None:
        available_at = datetime.now(timezone.utc)
    if max_attempts is None:
        max_attempts = 5
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO work_queue(
              job_type, payload, status, available_at, max_attempts, created_at, updated_at
            )
            VALUES (%s, %s, 'pending', %s, %s, NOW(), NOW())
            RETURNING id
            """,
            (job_type, payload_json, available_at, max_attempts),
        )
        job_id = cur.fetchone()[0]
        notify_payload = json.dumps(
            {
                "job_id": int(job_id),
                "job_type": job_type,
                "ts": datetime.now(timezone.utc).isoformat(),
            },
            separators=(",", ":"),
            ensure_ascii=True,
        )
        try:
            cur.execute("SELECT pg_notify('work_queue_update', %s)", (notify_payload,))
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Queue notify failed for job %s", job_id)
    if commit:
        conn.commit()
    return int(job_id)


def claim_job_by_id(
    conn: psycopg.Connection,
    job_id: int,
    worker_id: str,
    job_types: Optional[tuple[str, ...]] = None,
) -> Optional[WorkItem]:
    """Claim a specific job id if pending."""
    assert_queue_op_allowed("claim")
    with conn.cursor() as cur:
        job_filter = ""
        params = [worker_id, job_id]
        if job_types:
            job_filter = "AND job_type = ANY(%s)"
            params.append(list(job_types))
        cur.execute(
            f"""
            UPDATE work_queue
            SET status='running',
                locked_by=%s,
                locked_at=NOW(),
                attempts=attempts + 1,
                updated_at=NOW()
            WHERE id=%s
              AND status='pending'
              AND available_at <= NOW()
              {job_filter}
            RETURNING id, job_type, payload, attempts, max_attempts
            """,
            params,
        )
        row = cur.fetchone()
    conn.commit()
    if not row:
        return None
    return WorkItem(
        job_id=int(row[0]),
        job_type=row[1],
        payload=_parse_payload(row[2]),
        attempts=int(row[3]),
        max_attempts=int(row[4]),
    )


def claim_next_job(
    conn: psycopg.Connection,
    worker_id: str,
    job_types: Optional[tuple[str, ...]] = None,
) -> Optional[WorkItem]:
    """Claim the next available job."""
    assert_queue_op_allowed("claim")
    with conn.cursor() as cur:
        job_filter = ""
        params = [worker_id]
        if job_types:
            job_filter = "AND job_type = ANY(%s)"
            params.append(list(job_types))
        cur.execute(
            f"""
            UPDATE work_queue
            SET status='running',
                locked_by=%s,
                locked_at=NOW(),
                attempts=attempts + 1,
                updated_at=NOW()
            WHERE id = (
              SELECT id
              FROM work_queue
              WHERE status='pending'
                AND available_at <= NOW()
                {job_filter}
              ORDER BY available_at, id
              FOR UPDATE SKIP LOCKED
              LIMIT 1
            )
            RETURNING id, job_type, payload, attempts, max_attempts
            """,
            params,
        )
        row = cur.fetchone()
    conn.commit()
    if not row:
        return None
    return WorkItem(
        job_id=int(row[0]),
        job_type=row[1],
        payload=_parse_payload(row[2]),
        attempts=int(row[3]),
        max_attempts=int(row[4]),
    )


def mark_done(conn: psycopg.Connection, job_id: int) -> None:
    """Mark a job as completed."""
    assert_queue_op_allowed("complete")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE work_queue
            SET status='done',
                updated_at=NOW(),
                last_error=NULL
            WHERE id=%s
            """,
            (job_id,),
        )
    conn.commit()


def mark_failed(
    conn: psycopg.Connection,
    item: WorkItem,
    error: Exception,
    retry_delay_seconds: int,
) -> None:
    """Record an error and either retry or mark as failed."""
    assert_queue_op_allowed("complete")
    err_text = str(error)
    if len(err_text) > 2000:
        err_text = err_text[:2000] + "..."
    if item.attempts >= item.max_attempts:
        status = "failed"
        available_at = None
    else:
        status = "pending"
        backoff = max(1, min(item.attempts, 10))
        available_at = datetime.now(timezone.utc) + timedelta(
            seconds=retry_delay_seconds * backoff
        )
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE work_queue
            SET status=%s,
                available_at=COALESCE(%s, available_at),
                last_error=%s,
                updated_at=NOW()
            WHERE id=%s
            """,
            (status, available_at, err_text, item.job_id),
        )
    conn.commit()


def requeue_stale_jobs(conn: psycopg.Connection, lock_timeout_seconds: int) -> int:
    """Requeue jobs that have been running past the lock timeout."""
    assert_queue_op_allowed("maintenance")
    if lock_timeout_seconds <= 0:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=lock_timeout_seconds)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE work_queue
            SET status='pending',
                locked_by=NULL,
                locked_at=NULL,
                updated_at=NOW()
            WHERE status='running'
              AND locked_at < %s
            RETURNING id
            """,
            (cutoff,),
        )
        rows = cur.fetchall()
    conn.commit()
    return len(rows or [])


def cleanup_finished_jobs(conn: psycopg.Connection, cleanup_done_hours: int) -> int:
    """Delete completed/failed jobs older than the retention window."""
    assert_queue_op_allowed("maintenance")
    if cleanup_done_hours <= 0:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(hours=cleanup_done_hours)
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM work_queue
            WHERE status IN ('done', 'failed')
              AND updated_at < %s
            RETURNING id
            """,
            (cutoff,),
        )
        rows = cur.fetchall()
    conn.commit()
    return len(rows or [])
