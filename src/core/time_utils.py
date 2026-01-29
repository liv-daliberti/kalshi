"""Shared time parsing and inference helpers."""

from __future__ import annotations

import importlib
import re
from datetime import datetime, timezone
from typing import Any, Callable, Optional

_ISOPARSE: Callable[[str], datetime] | None = None
_ISOPARSE_CHECKED = False

_STRIKE_MINUTE_RE = re.compile(r"^\s*\d+\s*(m|min|minute|minutes)\s*$", re.IGNORECASE)
_STRIKE_HOUR_ALIASES = {"hour", "hourly", "hr", "hrs", "hours"}
_STRIKE_DAY_ALIASES = {"day", "daily", "days", "d"}


def normalize_strike_period(value: Any) -> Optional[str]:
    """Normalize strike-period strings to canonical values (hour/day) when possible."""
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw:
        return None
    if raw in _STRIKE_HOUR_ALIASES:
        return "hour"
    if raw in _STRIKE_DAY_ALIASES:
        return "day"
    if _STRIKE_MINUTE_RE.match(raw):
        return "hour"
    return raw


def _resolve_isoparse() -> Callable[[str], datetime] | None:
    global _ISOPARSE_CHECKED, _ISOPARSE  # pylint: disable=global-statement
    if _ISOPARSE_CHECKED:
        return _ISOPARSE
    _ISOPARSE_CHECKED = True
    try:
        parser_module = importlib.import_module("dateutil.parser")
    except ImportError:
        return None
    _ISOPARSE = getattr(parser_module, "isoparse", None)
    return _ISOPARSE


def ensure_utc(value: Optional[datetime]) -> Optional[datetime]:
    """Ensure a datetime is timezone-aware (UTC)."""
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def parse_ts(
    value: Any,
    *,
    parser: Callable[[str], datetime] | None = None,
) -> datetime | None:
    """Parse a timestamp into a timezone-aware datetime."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if parser is None:
        parser = _resolve_isoparse()
        if parser is None:
            return None
    try:
        parsed = parser(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_epoch_seconds(value: Any) -> datetime | None:
    """Parse epoch seconds into a timezone-aware datetime."""
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError):
        return None


def age_seconds(ts_value: Optional[datetime], now: datetime) -> float | None:
    """Return a non-negative age in seconds for a timestamp."""
    ts_value = ensure_utc(ts_value)
    if ts_value is None:
        return None
    return max(0.0, (now - ts_value).total_seconds())


def infer_strike_period_from_times(
    open_time: Optional[datetime],
    close_time: Optional[datetime],
    hour_max: float,
    day_max: float,
) -> Optional[str]:
    """Infer strike period from open/close timestamps."""
    open_dt = ensure_utc(open_time)
    close_dt = ensure_utc(close_time)
    if open_dt is None or close_dt is None:
        return None
    delta_s = (close_dt - open_dt).total_seconds()
    if delta_s <= 0:
        return None
    hours = delta_s / 3600.0
    if hours <= hour_max:
        return "hour"
    if hours <= day_max:
        return "day"
    return None
