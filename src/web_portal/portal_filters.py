"""Portal filter parsing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import CLOSE_WINDOW_HOURS, EVENT_SORT_SQL


@dataclass(frozen=True)
class PortalFilters:
    """Parsed filters for the main portal view."""

    search: str | None
    categories: tuple[str, ...]
    strike_period: str | None
    close_window: str | None
    status: str | None
    sort: str | None
    order: str | None

    @property
    def close_window_hours(self) -> float | None:
        """Return close-window hours derived from the filter token."""
        return _parse_close_window(self.close_window)[1]


def _clean_filter_value(raw: str | None) -> str | None:
    """Normalize filter values from query parameters."""
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None
    if value.lower() in {"all", "any"}:
        return None
    return value


def _parse_category_filters(args: Any) -> tuple[str, ...]:
    """Parse category filters (supporting multi-select)."""
    raw_values: list[str] = []
    if hasattr(args, "getlist"):
        raw_values = args.getlist("category")
    else:
        raw = None
        if isinstance(args, dict):
            raw = args.get("category")
        if isinstance(raw, (list, tuple)):
            raw_values = list(raw)
        elif raw is not None:
            raw_values = [raw]

    values: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        if raw is None:
            continue
        for token in str(raw).split(","):
            value = _clean_filter_value(token)
            if not value:
                continue
            key = value.lower()
            if key in seen:
                continue
            values.append(value)
            seen.add(key)
    return tuple(values)


def _parse_close_window(raw: str | None) -> tuple[str | None, float | None]:
    """Parse a close-window token into hours."""
    value = _clean_filter_value(raw)
    if not value:
        return None, None
    key = value.lower()
    hours = CLOSE_WINDOW_HOURS.get(key)
    if hours is None:
        return None, None
    return key, hours


def _parse_sort_value(raw: str | None) -> str | None:
    """Validate a sort key."""
    value = _clean_filter_value(raw)
    if not value:
        return None
    key = value.lower()
    return key if key in EVENT_SORT_SQL else None


def _parse_order_value(raw: str | None) -> str | None:
    """Validate a sort direction."""
    value = _clean_filter_value(raw)
    if not value:
        return None
    key = value.lower()
    return key if key in {"asc", "desc"} else None


def _parse_portal_filters(args: dict[str, Any]) -> PortalFilters:
    """Parse query parameters into portal filters."""
    search = _clean_filter_value(args.get("search"))
    categories = _parse_category_filters(args)
    strike_period = _clean_filter_value(args.get("strike_period"))
    status = _clean_filter_value(args.get("status"))
    if status:
        status = status.lower()
    sort = _parse_sort_value(args.get("sort"))
    order = _parse_order_value(args.get("order"))
    close_window = _parse_close_window(args.get("close_window"))[0]
    return PortalFilters(
        search=search,
        categories=categories,
        strike_period=strike_period,
        close_window=close_window,
        status=status,
        sort=sort,
        order=order,
    )
