"""Environment variable parsing helpers."""

from __future__ import annotations

import os
from typing import Optional

_TRUTHY = {"1", "true", "t", "yes", "y", "on"}


def _parse_int(raw: str | None, fallback: int) -> int:
    if not raw:
        return fallback
    try:
        return int(raw)
    except (TypeError, ValueError):
        return fallback


def _parse_float(raw: str | None, fallback: float) -> float:
    if not raw:
        return fallback
    try:
        return float(raw)
    except (TypeError, ValueError):
        return fallback


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUTHY


def _env_int(name: str, fallback: int, minimum: Optional[int] = None) -> int:
    value = _parse_int(os.getenv(name), fallback)
    if minimum is not None and value < minimum:
        return minimum
    return value


def _env_int_fallback(name: str, fallback: int, minimum: int = 1) -> int:
    value = _parse_int(os.getenv(name), fallback)
    return value if value >= minimum else fallback


def _env_float(name: str, fallback: float, minimum: Optional[float] = None) -> float:
    value = _parse_float(os.getenv(name), fallback)
    if minimum is not None and value < minimum:
        return minimum
    return value


def _env_float_fallback(name: str, fallback: float, minimum: float = 0.0) -> float:
    value = _parse_float(os.getenv(name), fallback)
    return value if value >= minimum else fallback


def env_bool(name: str, default: bool = False) -> bool:
    """Read a boolean environment variable."""
    return _env_bool(name, default)


def env_int(name: str, fallback: int, minimum: Optional[int] = None) -> int:
    """Read an integer environment variable with an optional minimum."""
    return _env_int(name, fallback, minimum)


def env_int_fallback(name: str, fallback: int, minimum: int = 1) -> int:
    """Read an integer environment variable, falling back if too small."""
    return _env_int_fallback(name, fallback, minimum)


def env_float(name: str, fallback: float, minimum: Optional[float] = None) -> float:
    """Read a float environment variable with an optional minimum."""
    return _env_float(name, fallback, minimum)


def env_float_fallback(name: str, fallback: float, minimum: float = 0.0) -> float:
    """Read a float environment variable, falling back if too small."""
    return _env_float_fallback(name, fallback, minimum)


def parse_bool(raw: str | None, default: bool = False) -> bool:
    """Parse a boolean-ish string value."""
    if raw is None:
        return default
    return raw.strip().lower() in _TRUTHY


def parse_int(raw: str | None, fallback: int, minimum: Optional[int] = None) -> int:
    """Parse an integer-ish string value."""
    value = _parse_int(raw, fallback)
    if minimum is not None and value < minimum:
        return minimum
    return value
