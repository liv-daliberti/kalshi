"""Category filter helpers for the web portal."""

from __future__ import annotations

from itertools import chain
from typing import Any, Callable, Iterable


def build_category_choices(
    active_categories: Iterable[str],
    selected_categories: Iterable[str],
) -> tuple[list[str], set[str]]:
    """Return (choices, selected_keys) preserving order and uniqueness."""
    active_list = list(active_categories)
    selected_list = list(selected_categories)
    selected_keys = {category.lower() for category in selected_list}
    category_choices: list[str] = []
    seen_category_keys: set[str] = set()
    for category in chain(active_list, selected_list):
        key = category.lower()
        if key in seen_category_keys:
            continue
        seen_category_keys.add(key)
        category_choices.append(category)
    return category_choices, selected_keys


def build_category_filters(
    *,
    active_categories: Iterable[str],
    selected_categories: Iterable[str],
    base_params: dict[str, Any],
    endpoint: str,
    url_for: Callable[..., str],
) -> list[dict[str, Any]]:
    """Build category filter link metadata for templates."""
    category_choices, selected_keys = build_category_choices(
        active_categories,
        selected_categories,
    )
    selected_list = list(selected_categories)
    category_filters: list[dict[str, Any]] = []
    for category in category_choices:
        params = dict(base_params)
        key = category.lower()
        if key in selected_keys:
            next_categories = [
                value for value in selected_list if value.lower() != key
            ]
        else:
            next_categories = list(selected_list)
            next_categories.append(category)
        if next_categories:
            params["category"] = next_categories
        category_filters.append(
            {
                "label": category,
                "url": url_for(endpoint, **params),
                "active": key in selected_keys,
            }
        )
    return category_filters
