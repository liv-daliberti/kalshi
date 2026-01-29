"""Paging and cursor helpers for portal tables."""

from __future__ import annotations

import base64
import binascii
import json
from typing import Any

from .config import EVENT_SORT_COLUMNS
from .db import EventCursor, _cursor_from_rows, _normalize_cursor_value
from .portal_filters import PortalFilters
from .portal_limits import clamp_page
from .portal_models import PortalCursorTokens, PortalPaging


def _portal_data_cache_key(
    limit: int,
    filters: PortalFilters,
    paging: PortalPaging,
) -> tuple[Any, ...]:
    return (
        limit,
        filters.search,
        filters.categories,
        filters.strike_period,
        filters.close_window,
        filters.status,
        filters.sort,
        filters.order,
        paging.active_page,
        paging.scheduled_page,
        paging.closed_page,
        paging.active_cursor,
        paging.scheduled_cursor,
        paging.closed_cursor,
    )


def _resolve_sort_key(sort: str | None, default: str) -> str:
    return sort if sort in EVENT_SORT_COLUMNS else default


def _encode_cursor_token(cursor: EventCursor | None) -> str | None:
    if cursor is None or not cursor.event_ticker:
        return None
    payload = {
        "t": cursor.event_ticker,
        "v": _normalize_cursor_value(cursor.value),
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def _decode_cursor_token(raw: str | None) -> EventCursor | None:
    if not raw:
        return None
    try:
        padded = raw + ("=" * (-len(raw) % 4))
        decoded = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        payload = json.loads(decoded)
    except (ValueError, UnicodeDecodeError, binascii.Error):
        return None
    if not isinstance(payload, dict):
        return None
    ticker = payload.get("t")
    if not isinstance(ticker, str) or not ticker:
        return None
    value = payload.get("v")
    if value is not None and not isinstance(value, (str, int, float)):
        return None
    return EventCursor(
        value=value,
        event_ticker=ticker,
    )


def _parse_portal_paging(args: dict[str, Any], limit: int) -> PortalPaging:
    """Parse pagination arguments for the portal tables."""
    active_token_raw = args.get("active_after")
    active_cursor = _encode_cursor_token(_decode_cursor_token(active_token_raw))
    scheduled_token_raw = args.get("scheduled_after")
    scheduled_cursor = _encode_cursor_token(_decode_cursor_token(scheduled_token_raw))
    closed_token_raw = args.get("closed_after")
    closed_cursor = _encode_cursor_token(_decode_cursor_token(closed_token_raw))

    active_page = clamp_page(args.get("active_page"))
    scheduled_page = clamp_page(args.get("scheduled_page"))
    closed_page = clamp_page(args.get("closed_page"))
    if active_page and not active_cursor:
        active_page = 0
    if scheduled_page and not scheduled_cursor:
        scheduled_page = 0
    if closed_page and not closed_cursor:
        closed_page = 0
    return PortalPaging(
        limit=limit,
        active_page=active_page,
        scheduled_page=scheduled_page,
        closed_page=closed_page,
        active_cursor=active_cursor,
        scheduled_cursor=scheduled_cursor,
        closed_cursor=closed_cursor,
    )


def _decode_portal_cursors(
    paging: PortalPaging,
) -> tuple[EventCursor | None, EventCursor | None, EventCursor | None]:
    return (
        _decode_cursor_token(paging.active_cursor),
        _decode_cursor_token(paging.scheduled_cursor),
        _decode_cursor_token(paging.closed_cursor),
    )


def _snapshot_cursor_tokens(
    active_rows: list[dict[str, Any]],
    scheduled_rows: list[dict[str, Any]],
    closed_rows: list[dict[str, Any]],
    filters: PortalFilters,
) -> PortalCursorTokens:
    active_sort = _resolve_sort_key(filters.sort, "close_time")
    scheduled_sort = _resolve_sort_key(filters.sort, "open_time")
    active_cursor = _cursor_from_rows(active_rows, active_sort)
    scheduled_cursor = _cursor_from_rows(scheduled_rows, scheduled_sort)
    closed_cursor = _cursor_from_rows(closed_rows, "close_time")
    return PortalCursorTokens(
        active=_encode_cursor_token(active_cursor),
        scheduled=_encode_cursor_token(scheduled_cursor),
        closed=_encode_cursor_token(closed_cursor),
    )
