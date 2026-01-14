"""Queue stream configuration helpers."""

from __future__ import annotations

from .config import _env_bool, _env_int


def _queue_stream_enabled() -> bool:
    """Enable SSE queue updates when configured."""
    return _env_bool("WEB_PORTAL_QUEUE_STREAM_ENABLE", True)


def _queue_stream_min_reload_ms() -> int:
    """Minimum ms between queue-triggered reloads."""
    return _env_int("WEB_PORTAL_QUEUE_STREAM_MIN_RELOAD_MS", 5000, minimum=1000)
