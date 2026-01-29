"""Web portal service entrypoint."""

from __future__ import annotations

import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

try:  # Allow running as a script without a package context.
    from ..core.guardrails import assert_env_isolated
except ImportError:  # pragma: no cover - fallback for __main__ execution
    from src.core.guardrails import assert_env_isolated


def _apply_portal_db_url() -> None:
    """Prefer a portal-specific DB URL and strip DATABASE_URL_* overrides."""
    portal_url = os.getenv("DATABASE_URL_PORTAL")
    if portal_url:
        os.environ["DATABASE_URL"] = portal_url
    for key in list(os.environ):
        if key.startswith("DATABASE_URL_"):
            os.environ.pop(key, None)


def _ensure_env() -> None:
    """Seed environment defaults for the portal service."""
    os.environ.setdefault("SERVICE_ROLE", "portal")
    os.environ.setdefault("DB_INIT_SCHEMA", "0")
    _apply_portal_db_url()


def _load_web_portal():
    # Late import so env vars are set before portal modules load.
    try:
        from .. import web_portal  # pylint: disable=import-outside-toplevel
    except ImportError:  # pragma: no cover - fallback for __main__ execution
        from src import web_portal  # pylint: disable=import-outside-toplevel

    return web_portal


def main() -> None:
    """Run the web portal service."""
    _ensure_env()
    assert_env_isolated(
        "portal",
        forbidden_prefixes=("RAG_", "AZURE_", "PREDICTION_", "DATABASE_URL_"),
    )
    _load_web_portal().main()


if __name__ == "__main__":
    main()
