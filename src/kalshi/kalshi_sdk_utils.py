"""Shared helpers for Kalshi SDK helpers."""

from __future__ import annotations

import inspect
from typing import Any, Optional, Tuple


def _filter_kwargs(func, kwargs: dict[str, Any]) -> dict[str, Any]:
    """Filter kwargs to the function signature.

    :param func: Target callable.
    :type func: collections.abc.Callable[..., Any]
    :param kwargs: Keyword arguments to filter.
    :type kwargs: dict[str, Any]
    :return: Filtered kwargs compatible with the signature.
    :rtype: dict[str, Any]
    """
    sig = inspect.signature(func)
    if any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values()):
        return kwargs
    return {k: v for k, v in kwargs.items() if k in sig.parameters}


def _extract_items(resp: Any) -> Tuple[list[Any], Optional[str]]:
    """Extract items and cursor from a response payload.

    :param resp: API response payload.
    :type resp: Any
    :return: Tuple of (items, cursor).
    :rtype: tuple[list[Any], str | None]
    """
    if isinstance(resp, dict):
        items = resp.get("events") or resp.get("items") or resp.get("data") or []
        cursor = resp.get("next_cursor") or resp.get("cursor") or resp.get("next")
        return list(items or []), cursor
    if isinstance(resp, (list, tuple)):
        if len(resp) == 2 and isinstance(resp[0], list):
            return resp[0], resp[1]
        return list(resp), None
    items = getattr(resp, "events", None) or getattr(resp, "items", None)
    cursor = getattr(resp, "next_cursor", None) or getattr(resp, "cursor", None)
    return list(items or []), cursor


def _to_plain_dict(obj: Any) -> Any:
    """Convert SDK objects to plain dictionaries when possible.

    :param obj: SDK model or dict-like object.
    :type obj: Any
    :return: Plain dict or original object.
    :rtype: Any
    """
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump(mode="json")
        except TypeError:
            return obj.model_dump()
    if hasattr(obj, "to_dict"):
        try:
            return obj.to_dict()
        except Exception:  # pylint: disable=broad-exception-caught
            pass
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    return obj
