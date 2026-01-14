"""RAG predictions service entrypoint."""

from __future__ import annotations

import asyncio
import os

from src.services import main as ingestor_main
from src.core.guardrails import assert_env_isolated
from src.services.healthcheck import start_health_server


def _ensure_env() -> None:
    os.environ["KALSHI_RUN_MODE"] = "rag"
    os.environ.setdefault("SERVICE_ROLE", "rag")
    os.environ.setdefault("DB_INIT_SCHEMA", "0")


def main() -> None:
    """Run the RAG service entrypoint."""
    _ensure_env()
    assert_env_isolated(
        "rag",
        forbidden_prefixes=("DATABASE_URL_",),
        forbidden_keys=("KALSHI_API_KEY_ID", "KALSHI_PRIVATE_KEY_PEM_PATH"),
    )
    start_health_server("rag")
    asyncio.run(ingestor_main.main())


if __name__ == "__main__":
    main()
