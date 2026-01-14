"""Queue worker service entrypoint."""

from __future__ import annotations

import asyncio
import os

from src.services import main as ingestor_main
from src.core.guardrails import assert_env_isolated
from src.services.healthcheck import start_health_server


def _ensure_env() -> None:
    """Seed environment defaults for the worker service."""
    os.environ["KALSHI_RUN_MODE"] = "worker"
    os.environ.setdefault("SERVICE_ROLE", "worker")
    os.environ.setdefault("DB_INIT_SCHEMA", "0")


def main() -> None:
    """Run the queue worker service."""
    _ensure_env()
    assert_env_isolated(
        "worker",
        forbidden_prefixes=("RAG_", "AZURE_", "PREDICTION_", "DATABASE_URL_"),
    )
    start_health_server("worker")
    asyncio.run(ingestor_main.main())


if __name__ == "__main__":
    main()
