"""RAG prediction loop."""

from __future__ import annotations

import logging
import time

import psycopg  # pylint: disable=import-error

from src.db.db import ensure_schema_compatible, maybe_init_schema
from src.core.env_utils import _env_float, _env_int
from src.core.loop_utils import LoopFailureContext, log_metric, schema_path
from src.predictions.predictions import prediction_pass, resolve_prediction_handler

logger = logging.getLogger(__name__)

_SCHEMA_PATH = schema_path(__file__)


def _open_connection(
    database_url: str,
    schema_ready: bool,
) -> tuple[psycopg.Connection, bool]:
    conn = psycopg.connect(database_url)
    if not schema_ready:
        maybe_init_schema(conn, schema_path=_SCHEMA_PATH)
        ensure_schema_compatible(conn)
        schema_ready = True
    return conn, schema_ready


def _ensure_connection(
    conn: psycopg.Connection | None,
    database_url: str,
    schema_ready: bool,
) -> tuple[psycopg.Connection, bool]:
    if conn is not None and not getattr(conn, "closed", False):
        return conn, schema_ready
    if conn is not None:
        try:
            conn.close()
        except Exception:  # pylint: disable=broad-exception-caught
            logger.debug("Failed to close stale DB connection", exc_info=True)
    return _open_connection(database_url, schema_ready)


def rag_prediction_loop(prediction_cfg, database_url: str) -> None:
    """Run the RAG prediction loop with retry and metrics logging."""
    conn: psycopg.Connection | None = None
    schema_ready = False
    handler = resolve_prediction_handler(prediction_cfg)
    failure_threshold = _env_int("RAG_FAILURE_THRESHOLD", 3, minimum=1)
    breaker_seconds = _env_float("RAG_CIRCUIT_BREAKER_SECONDS", 120.0, minimum=1.0)
    failure_ctx = LoopFailureContext(
        name="rag.predictions",
        failure_threshold=failure_threshold,
        breaker_seconds=breaker_seconds,
    )
    while True:
        conn, schema_ready = _ensure_connection(conn, database_url, schema_ready)
        start = time.monotonic()
        try:
            stored = prediction_pass(conn, prediction_cfg, handler=handler)
            duration = time.monotonic() - start
            failure_ctx.record_success(logger, "prediction recovered after %d failures")
            log_metric(
                logger,
                "rag.predictions",
                duration_s=round(duration, 2),
                stored=stored,
                errors_total=failure_ctx.error_total,
            )
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("prediction failed")
            if getattr(conn, "closed", False):
                logger.warning("rag.predictions DB connection closed; reconnecting")
                conn = None
            if failure_ctx.handle_exception(logger, start):
                continue
        time.sleep(prediction_cfg.intervals.poll_seconds)
