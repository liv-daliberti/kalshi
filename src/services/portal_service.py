"""Web portal service entrypoint."""

from __future__ import annotations

import os

from src import web_portal
from src.core.guardrails import assert_env_isolated


def _ensure_env() -> None:
    """Seed environment defaults for the portal service."""
    os.environ.setdefault("SERVICE_ROLE", "portal")
    os.environ.setdefault("DB_INIT_SCHEMA", "0")


def main() -> None:
    """Run the web portal service."""
    _ensure_env()
    assert_env_isolated(
        "portal",
        forbidden_prefixes=("RAG_", "AZURE_", "PREDICTION_", "DATABASE_URL_"),
    )
    web_portal.main()


if __name__ == "__main__":
    main()
