"""Database connection helpers shared across services."""

from __future__ import annotations

import logging
from typing import Any


def safe_close(
    conn: Any,
    *,
    logger: logging.Logger | None = None,
    warn_message: str | None = None,
) -> None:
    """Close a connection and optionally warn on failure."""
    if conn is None:
        return
    try:
        conn.close()
    except Exception:  # pylint: disable=broad-exception-caught
        if logger is not None:
            logger.warning(warn_message or "DB close failed")
