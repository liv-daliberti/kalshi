"""Logging helpers for consistent service tags."""

from __future__ import annotations

import logging
import os
import sys
from typing import Callable
from pathlib import Path

LOG_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


def service_label(default: str = "unknown") -> str:
    """Return a normalized service label for log formatting."""
    for key in ("SERVICE_ROLE", "KALSHI_RUN_MODE", "KALSHI_SERVICE"):
        value = os.getenv(key)
        if value:
            return value.strip().lower()
    return default


def log_format(default: str = "unknown") -> str:
    """Return the log format string with a service label."""
    return f"%(asctime)s | %(levelname)s | {service_label(default)} | %(message)s"


def parse_log_level(raw: str | None, fallback: int = logging.INFO) -> int:
    """Parse a log level string into a logging constant."""
    if not raw:
        return fallback
    raw = raw.strip().upper()
    if raw.isdigit():
        return int(raw)
    return LOG_LEVELS.get(raw, fallback)


def is_known_log_level(raw: str | None) -> bool:
    """Return True if raw is a valid log level name or numeric value."""
    if not raw:
        return False
    raw = raw.strip().upper()
    return raw.isdigit() or raw in LOG_LEVELS


def _truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _sanitize_filename(value: str) -> str:
    safe = []
    for char in value:
        if char.isalnum() or char in {"-", "_"}:
            safe.append(char)
        else:
            safe.append("_")
    return "".join(safe) or "service"


def _resolve_log_file(default_label: str) -> str | None:
    raw_file = os.getenv("LOG_FILE")
    raw_dir = os.getenv("LOG_DIR")
    if raw_file:
        return os.path.expandvars(os.path.expanduser(raw_file))
    if raw_dir:
        log_dir = Path(os.path.expandvars(os.path.expanduser(raw_dir)))
        filename = f"{_sanitize_filename(service_label(default_label))}.log"
        return str(log_dir / filename)
    return None


def _ensure_log_dir(path: Path) -> bool:
    parent = path.parent
    if not parent:
        return True
    candidate = parent
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    if candidate.exists() and not os.access(candidate, os.W_OK):
        print(
            f"Warning: log directory is not writable: {parent}. "
            "Falling back to stdout logging.",
            file=sys.stderr,
        )
        return False
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(
            f"Warning: unable to create log directory at {parent}: {exc}. "
            "Falling back to stdout logging.",
            file=sys.stderr,
        )
        return False
    return True


def log_handlers(default_label: str = "unknown") -> list[logging.Handler]:
    """Build log handlers for stdout and optional file logging."""
    handlers: list[logging.Handler] = []
    log_file = _resolve_log_file(default_label)
    log_stdout = _truthy(os.getenv("LOG_STDOUT", "1"))
    if log_file:
        log_path = Path(log_file)
        if _ensure_log_dir(log_path):
            try:
                handlers.append(logging.FileHandler(log_path))
            except Exception as exc:  # pylint: disable=broad-exception-caught
                print(
                    f"Warning: unable to create log file at {log_path}: {exc}. "
                    "Falling back to stdout logging.",
                    file=sys.stderr,
                )
    if log_stdout or not handlers:
        handlers.append(logging.StreamHandler())
    return handlers


def configure_logging(
    *,
    service_name: str,
    logger: logging.Logger,
    basic_config: Callable[..., None],
    format_default: str | None = None,
    level_raw: str | None = None,
) -> int:
    """Configure logging with standard handlers and warning on unknown levels."""
    if level_raw is None:
        level_raw = os.getenv("LOG_LEVEL", "INFO")
    level = parse_log_level(level_raw, logging.INFO)
    fmt = log_format(default=format_default) if format_default is not None else log_format()
    basic_config(level=level, format=fmt, handlers=log_handlers(service_name))
    configure_http_logging(level)
    configure_ws_logging(level)
    if not is_known_log_level(level_raw):
        logger.warning("Unknown LOG_LEVEL=%s; defaulting to INFO", level_raw)
    return level


def configure_http_logging(default_level: int | None = None) -> None:
    """Reduce noise from HTTP client libraries unless explicitly overridden."""
    raw = os.getenv("LOG_HTTP_LEVEL") or os.getenv("HTTP_LOG_LEVEL")
    if raw:
        level = parse_log_level(raw, logging.WARNING)
    elif default_level is not None and default_level <= logging.DEBUG:
        level = logging.WARNING
    else:
        return
    for logger_name in ("urllib3", "requests", "httpx"):
        logging.getLogger(logger_name).setLevel(level)


def configure_ws_logging(default_level: int | None = None) -> None:
    """Reduce noise from WebSocket libraries unless explicitly overridden."""
    raw = os.getenv("LOG_WS_LEVEL") or os.getenv("LOG_WEBSOCKETS_LEVEL")
    if raw:
        level = parse_log_level(raw, logging.WARNING)
    elif default_level is not None and default_level <= logging.DEBUG:
        level = logging.WARNING
    else:
        return
    for logger_name in (
        "websockets",
        "websockets.client",
        "websockets.server",
        "websockets.protocol",
    ):
        logging.getLogger(logger_name).setLevel(level)
