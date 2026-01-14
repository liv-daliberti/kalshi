"""Helpers for portal paging limits."""

from __future__ import annotations

from typing import Any

from .config import DEFAULT_LIMIT, MAX_LIMIT


def clamp_limit(raw: Any) -> int:
    """Clamp a raw limit value to sane bounds."""
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    return max(1, min(limit, MAX_LIMIT))
