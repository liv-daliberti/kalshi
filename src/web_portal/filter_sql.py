"""Shared SQL WHERE clause helpers for portal filters."""

from __future__ import annotations

import re
from typing import Any, Iterable

_ALIAS_RE = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)\.")


def normalize_search(raw: str | None) -> str | None:
    """Normalize a raw search string for SQL usage."""
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    return " ".join(value.split())


def append_search_clause(
    clauses: list[str],
    params: list[Any],
    search: str | None,
    fields: Iterable[str],
) -> None:
    """Append an ILIKE search clause across the provided fields."""
    if not search:
        return
    pattern = f"%{search}%"
    field_list = list(fields)
    if not field_list:
        return
    fragments = [f"{field} ILIKE %s" for field in field_list]
    clauses.append(f"({' OR '.join(fragments)})")
    params.extend([pattern] * len(fragments))


def append_category_clause(
    clauses: list[str],
    params: list[Any],
    categories: Iterable[str] | None,
) -> None:
    """Append a category filter clause when categories are provided."""
    if not categories:
        return
    category_list = [category.lower() for category in categories]
    if not category_list:
        return
    placeholders = ", ".join(["%s"] * len(category_list))
    clauses.append(f"LOWER(e.category) IN ({placeholders})")
    params.extend(category_list)


def append_strike_period_clause(
    clauses: list[str],
    params: list[Any],
    strike_period: str | None,
) -> None:
    """Append a strike-period filter clause when provided."""
    if not strike_period:
        return
    clauses.append("LOWER(e.strike_period) = %s")
    params.append(strike_period.lower())


def _finalize_filter_where(
    clauses: list[str],
    params: list[Any],
    filters: "PortalFilters",
    *,
    include_category: bool = True,
) -> tuple[str, list[Any]]:
    if include_category:
        append_category_clause(clauses, params, filters.categories)
    append_strike_period_clause(clauses, params, filters.strike_period)
    if not clauses:
        return "", params
    return " AND " + " AND ".join(clauses), params


def build_filter_where(
    filters: "PortalFilters",
    search_fields: Iterable[str],
    *,
    include_category: bool = True,
    search_override: str | None = None,
) -> tuple[str, list[Any]]:
    """Build SQL WHERE fragments for portal filters."""
    clauses: list[str] = []
    params: list[Any] = []
    search_raw = search_override if search_override is not None else filters.search
    search_value = normalize_search(search_raw)
    append_search_clause(clauses, params, search_value, search_fields)
    return _finalize_filter_where(
        clauses,
        params,
        filters,
        include_category=include_category,
    )
