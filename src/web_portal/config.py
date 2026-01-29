"""Configuration helpers and constants for the web portal."""

from __future__ import annotations

from ..core.env_utils import env_bool, env_float, env_int_fallback

_env_bool = env_bool
_env_float = env_float
_env_int = env_int_fallback

DEFAULT_LIMIT = 75
MAX_LIMIT = 2000

EVENT_SORT_OPTIONS = [
    ("", "Default order"),
    ("close_time", "Close time"),
    ("open_time", "Open time"),
    ("title", "Event title"),
    ("category", "Category"),
    ("strike_period", "Strike period"),
    ("market_count", "Market count"),
]
EVENT_ORDER_OPTIONS = [
    ("", "Default"),
    ("asc", "Ascending"),
    ("desc", "Descending"),
]
EVENT_STATUS_OPTIONS = [
    ("", "Any status"),
    ("open", "Open"),
    ("paused", "Paused"),
    ("scheduled", "Scheduled"),
    ("closed", "Closed"),
    ("inactive", "Inactive"),
]
CLOSE_WINDOW_OPTIONS = [
    ("", "Any time"),
    ("15m", "Within 15 minutes"),
    ("1h", "Within 1 hour"),
    ("4h", "Within 4 hours"),
    ("8h", "Within 8 hours"),
    ("24h", "Within 24 hours"),
]
CLOSE_WINDOW_HOURS = {
    "15m": 0.25,
    "1h": 1,
    "4h": 4,
    "8h": 8,
    "24h": 24,
}
EVENT_SORT_SQL = {
    "close_time": "MAX(m.close_time)",
    "open_time": "MIN(m.open_time)",
    "title": "e.title",
    "category": "e.category",
    "strike_period": "e.strike_period",
    "market_count": "COUNT(*)",
}
EVENT_SORT_COLUMNS = {
    "close_time": "close_time",
    "open_time": "open_time",
    "title": "event_title",
    "category": "event_category",
    "strike_period": "strike_period",
    "market_count": "market_count",
}
EVENT_STATUS_CASE = """
CASE
  WHEN COALESCE(BOOL_OR(LOWER(am.status) IN ('open', 'active')), false)
   AND COALESCE(BOOL_OR(mt.has_tick), false) THEN 'open'
  WHEN COALESCE(BOOL_OR(LOWER(am.status) = 'paused'), false)
   AND COALESCE(BOOL_OR(mt.has_tick), false) THEN 'paused'
  WHEN MAX(m.close_time) IS NOT NULL AND MAX(m.close_time) <= NOW() THEN 'closed'
  WHEN MIN(m.open_time) IS NOT NULL AND MIN(m.open_time) > NOW() THEN 'scheduled'
  WHEN (MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW())
   AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())
   AND COALESCE(BOOL_OR(mt.has_tick), false) THEN 'open'
  WHEN (MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW())
   AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())
   AND NOT COALESCE(BOOL_OR(mt.has_tick), false) THEN 'scheduled'
  ELSE 'inactive'
END
"""


def _parse_csv(raw: str | None) -> tuple[str, ...]:
    """Parse a comma-delimited string into lowercase tokens."""
    if not raw:
        return ()
    return tuple(p.strip().lower() for p in raw.split(",") if p.strip())


def _fmt_hours(raw: str | None, fallback: float) -> str:
    """Format a float hour value with minimal precision."""
    try:
        hours = float(raw) if raw is not None else fallback
    except (TypeError, ValueError):
        hours = fallback
    if hours.is_integer():
        return str(int(hours))
    return f"{hours:g}"
