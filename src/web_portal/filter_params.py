"""Helpers for building filter query parameters."""

from __future__ import annotations

from typing import Any


def build_filter_params(
    limit: int,
    filters: "PortalFilters",
    *,
    include_category: bool = True,
) -> dict[str, Any]:
    """Build query parameters for filter links."""
    params: dict[str, Any] = {"limit": limit}
    if filters.search:
        params["search"] = filters.search
    if include_category and filters.categories:
        params["category"] = list(filters.categories)
    if filters.strike_period:
        params["strike_period"] = filters.strike_period
    if filters.close_window:
        params["close_window"] = filters.close_window
    if filters.status:
        params["status"] = filters.status
    if filters.sort:
        params["sort"] = filters.sort
    if filters.order:
        params["order"] = filters.order
    return params
