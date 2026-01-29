"""Main entrypoint for the Kalshi ingestor."""

from __future__ import annotations

import asyncio
import logging
import os

import psycopg  # pylint: disable=import-error
from dotenv import load_dotenv

from ..jobs.backfill import backfill_pass
from ..jobs.backfill_config import build_backfill_config_from_settings
from ..jobs.closed_cleanup import build_closed_cleanup_config, closed_cleanup_pass
from ..db.db import ensure_schema_compatible, maybe_init_schema
from ..jobs.discovery import discovery_pass
from ..core.logging_utils import configure_logging as configure_service_logging, parse_log_level
from ..core.loop_utils import schema_path
from ..predictions.predictions import load_prediction_config
from ..queue.queue_worker import run_worker
from ..rag.rag_loop import rag_prediction_loop
from ..ingest.rest_loop import rest_backfill_loop, rest_closed_cleanup_loop, rest_discovery_loop
from ..core.settings import ENV_FILE, load_settings
from ..queue.work_queue import load_queue_config
from ..ingest.ws.ws_ingest import ws_loop

logger = logging.getLogger(__name__)

_RUN_MODES = {
    "all": "all",
    "combined": "all",
    "rest": "rest",
    "rest-only": "rest",
    "rest_only": "rest",
    "ws": "ws",
    "websocket": "ws",
    "websockets": "ws",
    "worker": "worker",
    "queue": "worker",
    "queue-worker": "worker",
    "queue_worker": "worker",
    "prediction": "rag",
    "predictions": "rag",
    "rag": "rag",
    "rag-only": "rag",
    "rag_only": "rag",
}
_TRUTHY = {"1", "true", "t", "yes", "y", "on"}


def configure_logging() -> None:
    """Initialize logging based on LOG_LEVEL."""
    level_raw = os.getenv("LOG_LEVEL", "INFO")
    configure_service_logging(
        service_name="main",
        logger=logger,
        basic_config=logging.basicConfig,
        level_raw=level_raw,
    )
    logger.debug("Logging initialized level=%s", level_raw)


def _parse_log_level(raw: str) -> int:
    return parse_log_level(raw, logging.INFO)


def _env_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY


def _guardrails_enabled() -> bool:
    return _env_truthy(os.getenv("SERVICE_GUARDRAILS"))


def _parse_run_mode(raw: str) -> str:
    """Normalize run mode strings into all/rest/ws/worker/rag."""
    if not raw:
        return "all"
    normalized = raw.strip().lower()
    return _RUN_MODES.get(normalized, "all")


async def periodic_discovery(conn, client, settings):
    """Continuously run discovery on a fixed interval.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param client: Kalshi REST client.
    :type client: Any
    :param settings: Loaded settings.
    :type settings: Settings
    :return: None.
    :rtype: None
    """
    while True:
        try:
            discovery_pass(
                conn,
                client,
                settings.strike_periods,
                settings.discovery_event_statuses,
            )
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("discovery failed")
        await asyncio.sleep(settings.discovery_seconds)


async def periodic_backfill(conn, client, settings, queue_cfg=None, publisher=None):
    """Continuously run backfill on a fixed interval.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param client: Kalshi REST client.
    :type client: Any
    :param settings: Loaded settings.
    :type settings: Settings
    :param queue_cfg: Optional queue configuration for enqueuing work.
    :type queue_cfg: QueueConfig | None
    :param publisher: Optional RabbitMQ publisher.
    :type publisher: QueuePublisher | None
    :return: None.
    :rtype: None
    """
    cfg = build_backfill_config_from_settings(settings)
    while True:
        try:
            backfill_pass(conn, client, cfg, queue_cfg=queue_cfg, publisher=publisher)
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("backfill failed")
        await asyncio.sleep(settings.backfill_seconds)


async def periodic_closed_cleanup(conn, client, settings):
    """Continuously run closed cleanup on a fixed interval."""
    cfg = build_closed_cleanup_config(settings)
    while True:
        try:
            closed_cleanup_pass(conn, client, cfg)
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("closed cleanup failed")
        await asyncio.sleep(settings.closed_cleanup_seconds)


def _schema_path() -> str:
    return schema_path(__file__)


async def main():
    """Run discovery, backfill, and websocket ingestion.

    :return: None.
    :rtype: None
    """
    load_dotenv(dotenv_path=ENV_FILE)
    configure_logging()
    run_mode_raw = os.getenv("KALSHI_RUN_MODE", "all")
    run_mode = _parse_run_mode(run_mode_raw)
    if run_mode == "all" and run_mode_raw.strip().lower() not in _RUN_MODES:
        logger.warning("Unknown KALSHI_RUN_MODE=%s; defaulting to all", run_mode_raw)
    if _guardrails_enabled() and run_mode == "all":
        logger.error(
            "SERVICE_GUARDRAILS=1 forbids KALSHI_RUN_MODE=all; "
            "run per-service entrypoints instead."
        )
        return
    logger.info("Ingestor mode: %s", run_mode)
    if run_mode == "rag":
        prediction_cfg = load_prediction_config()
        if not prediction_cfg.enabled:
            logger.error("RAG mode requires PREDICTION_ENABLE=1; exiting")
            return
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            logger.error("RAG mode requires DATABASE_URL; exiting")
            return
        await asyncio.to_thread(
            rag_prediction_loop,
            prediction_cfg,
            database_url,
        )
        return

    settings = load_settings()
    logger.info("Kalshi host: %s", settings.kalshi_host)
    queue_cfg = load_queue_config()

    # Load private key for SDK + WS
    with open(
        settings.kalshi_private_key_pem_path,
        "r",
        encoding="utf-8",
    ) as key_file:
        private_key_pem = key_file.read()

    if run_mode == "worker":
        run_worker(
            settings=settings,
            private_key_pem=private_key_pem,
            queue_cfg=queue_cfg,
            configure_log=False,
        )
        return

    tasks = []
    if run_mode in {"all", "rest"}:
        tasks.extend(
            [
                asyncio.to_thread(rest_discovery_loop, settings, private_key_pem),
                asyncio.to_thread(rest_backfill_loop, settings, private_key_pem, queue_cfg),
            ]
        )
        if settings.closed_cleanup_seconds > 0:
            tasks.append(
                asyncio.to_thread(rest_closed_cleanup_loop, settings, private_key_pem)
            )

    if run_mode in {"all", "ws"}:
        conn = psycopg.connect(settings.database_url)
        maybe_init_schema(conn, schema_path=_schema_path())
        ensure_schema_compatible(conn)
        logger.info("DB schema ready.")
        tasks.append(
            ws_loop(
                conn,
                api_key_id=settings.kalshi_api_key_id,
                private_key_pem=private_key_pem,
                max_active_tickers=settings.max_active_tickers,
                ws_batch_size=settings.ws_batch_size,
                refresh_seconds=settings.ws_sub_refresh_seconds,
                kalshi_host=settings.kalshi_host,
                database_url=settings.database_url,
            )
        )
    if not tasks:
        logger.error("No tasks configured for KALSHI_RUN_MODE=%s", run_mode_raw)
        return

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
