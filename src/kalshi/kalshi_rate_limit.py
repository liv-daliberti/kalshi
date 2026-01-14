"""Shared REST rate-limit helpers."""

from __future__ import annotations

import os
from typing import Callable


def register_rest_rate_limit(
    apply_cooldown: Callable[[float], None],
    extract_retry_after: Callable[[Exception], float | None] | None = None,
    exc: Exception | None = None,
    cooldown_sec: float | None = None,
) -> None:
    """Apply a REST cooldown based on retry headers or a fallback."""
    if exc is not None and extract_retry_after is not None:
        retry_after = extract_retry_after(exc)
        if retry_after is not None:
            apply_cooldown(retry_after)
            return
    if cooldown_sec is None:
        try:
            cooldown_sec = float(os.getenv("KALSHI_REST_COOLDOWN_SEC", "30"))
        except ValueError:
            cooldown_sec = 30.0
    apply_cooldown(cooldown_sec)
