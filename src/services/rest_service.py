"""REST discovery/backfill service entrypoint."""

from __future__ import annotations

import asyncio
import importlib
import os
from types import SimpleNamespace

try:
    run_service = importlib.import_module(
        ".service_entrypoint", __package__
    ).run_service
except (ImportError, TypeError):  # pragma: no cover - fallback for __main__ execution
    run_service = importlib.import_module("src.services.service_entrypoint").run_service

try:  # Allow running as a script without a package context.
    ingestor_main = importlib.import_module(".main", __package__)
except (ImportError, TypeError):  # pragma: no cover - fallback for __main__ execution
    try:  # pragma: no cover - fallback for tests
        ingestor_main = importlib.import_module("src.services.main")
    except ImportError:  # pragma: no cover - last-resort stub
        ingestor_main = SimpleNamespace(main=lambda: None)

try:
    _start_health_server = importlib.import_module(
        ".healthcheck", __package__
    ).start_health_server
except (ImportError, AttributeError, TypeError):  # pragma: no cover - optional dependency fallback
    def _start_health_server(_role: str) -> None:
        return None


def _ensure_env() -> None:
    """Ensure required env vars are set for the service."""
    os.environ["KALSHI_RUN_MODE"] = "rest"
    os.environ.setdefault("SERVICE_ROLE", "rest")
    os.environ.setdefault("DB_INIT_SCHEMA", "0")


def main() -> None:
    """Run the REST service entrypoint."""
    run_service(
        role="rest",
        run_mode="rest",
        forbidden_prefixes=("RAG_", "AZURE_", "PREDICTION_", "DATABASE_URL_"),
        main_fn=ingestor_main.main,
        asyncio_run=asyncio.run,
        start_health=_start_health_server,
    )


if __name__ == "__main__":
    main()
