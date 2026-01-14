"""Gunicorn configuration for the Kalshi web portal."""

from __future__ import annotations

import os


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _get_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


workers = _get_int("WEB_PORTAL_WORKERS", 2)
worker_class = _get_str("WEB_PORTAL_WORKER_CLASS", "gthread")
threads = _get_int("WEB_PORTAL_THREADS", 4)
timeout = _get_int("WEB_PORTAL_TIMEOUT", 120)
graceful_timeout = _get_int("WEB_PORTAL_GRACEFUL_TIMEOUT", 30)
keepalive = _get_int("WEB_PORTAL_KEEPALIVE", 5)
accesslog = _get_str("WEB_PORTAL_ACCESS_LOG", "-")
errorlog = _get_str("WEB_PORTAL_ERROR_LOG", "-")

bind = os.getenv("WEB_PORTAL_BIND")
if not bind:
    host = os.getenv("WEB_PORTAL_HOST")
    port = os.getenv("WEB_PORTAL_PORT")
    if host or port:
        bind = f"{host or '0.0.0.0'}:{port or '8000'}"
