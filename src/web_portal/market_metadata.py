"""Market metadata helpers for the web portal."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..core.json_utils import (
    maybe_parse_json as _maybe_parse_json,
    normalize_metadata_value as _normalize_metadata_value,
)
from .config import _env_bool
from .db_utils import to_json_value
from .kalshi import _get_event_metadata as _fetch_event_metadata
from .kalshi import _get_market_data as _fetch_market_data
from .portal_utils import portal_func as _portal_func
from .portal_utils import portal_logger as _portal_logger_util
from .portal_types import PsycopgConnection
from .db_timing import timed_cursor

_UNSET = object()


@dataclass(frozen=True)
class MarketExtrasPayload:
    """Payload for market metadata updates."""

    price_ranges: Any | None = None
    custom_strike: Any | None = None
    mve_selected_legs: Any | None = None


def _portal_logger():
    return _portal_logger_util(__name__)


def _metadata_changed(existing: Any | object, updated: Any | None) -> bool:
    """Return True when an update would change the stored value."""
    if updated is None:
        return False
    if existing is _UNSET:
        return True
    return _normalize_metadata_value(existing) != updated


def _derive_custom_strike(row: dict[str, Any]) -> dict[str, Any] | None:
    """Build a custom strike payload from strike fields."""
    payload = {
        "strike_type": row.get("strike_type"),
        "floor_strike": row.get("floor_strike"),
        "cap_strike": row.get("cap_strike"),
        "functional_strike": row.get("functional_strike"),
    }
    if all(value is None for value in payload.values()):
        return None
    return {key: value for key, value in payload.items() if value is not None}


def _extract_event_metadata(market_data: dict[str, Any]) -> Any | None:
    """Extract event metadata from a market payload when present."""
    if not market_data:
        return None
    for key in ("product_metadata", "event_metadata"):
        if market_data.get(key) is not None:
            return market_data.get(key)
    event_data = market_data.get("event")
    if isinstance(event_data, dict):
        for key in ("product_metadata", "event_metadata"):
            if event_data.get(key) is not None:
                return event_data.get(key)
    return None


def _update_market_extras(
    conn: PsycopgConnection,
    ticker: str,
    payload: MarketExtrasPayload | None = None,
    existing: MarketExtrasPayload | object = _UNSET,
    *,
    price_ranges: Any | None = None,
    custom_strike: Any | None = None,
    mve_selected_legs: Any | None = None,
    existing_price_ranges: Any | object = _UNSET,
    existing_custom_strike: Any | object = _UNSET,
    existing_mve_selected_legs: Any | object = _UNSET,
) -> None:
    """Update extra JSON fields for a market without overwriting existing data."""
    if payload is None:
        payload = MarketExtrasPayload(
            price_ranges=price_ranges,
            custom_strike=custom_strike,
            mve_selected_legs=mve_selected_legs,
        )
    existing_payload = existing if isinstance(existing, MarketExtrasPayload) else None
    if existing_price_ranges is _UNSET:
        existing_price_ranges = (
            existing_payload.price_ranges if existing_payload else _UNSET
        )
    if existing_custom_strike is _UNSET:
        existing_custom_strike = (
            existing_payload.custom_strike if existing_payload else _UNSET
        )
    if existing_mve_selected_legs is _UNSET:
        existing_mve_selected_legs = (
            existing_payload.mve_selected_legs if existing_payload else _UNSET
        )
    updates = []
    params: dict[str, Any] = {"ticker": ticker}
    if payload.price_ranges is not None:
        if _metadata_changed(existing_price_ranges, payload.price_ranges):
            updates.append("price_ranges = COALESCE(%(price_ranges)s, price_ranges)")
            params["price_ranges"] = to_json_value(payload.price_ranges)
    if payload.custom_strike is not None:
        if _metadata_changed(existing_custom_strike, payload.custom_strike):
            updates.append("custom_strike = COALESCE(%(custom_strike)s, custom_strike)")
            params["custom_strike"] = to_json_value(payload.custom_strike)
    if payload.mve_selected_legs is not None:
        if _metadata_changed(existing_mve_selected_legs, payload.mve_selected_legs):
            updates.append(
                "mve_selected_legs = COALESCE(%(mve_selected_legs)s, mve_selected_legs)"
            )
            params["mve_selected_legs"] = to_json_value(payload.mve_selected_legs)
    if not updates:
        return
    updates.append("updated_at = NOW()")
    sql = f"""
    UPDATE markets
    SET {", ".join(updates)}
    WHERE ticker = %(ticker)s
    """
    with timed_cursor(conn) as cur:
        cur.execute(sql, params)


def _update_event_metadata(
    conn: PsycopgConnection,
    event_ticker: str,
    product_metadata: Any | None,
    existing_product_metadata: Any | object = _UNSET,
) -> None:
    """Update event metadata when available."""
    if product_metadata is None:
        return
    if existing_product_metadata is not _UNSET:
        if not _metadata_changed(existing_product_metadata, product_metadata):
            return
    sql = """
    UPDATE events
    SET product_metadata = COALESCE(%(product_metadata)s, product_metadata),
        updated_at = NOW()
    WHERE event_ticker = %(event_ticker)s
    """
    with timed_cursor(conn) as cur:
        cur.execute(
            sql,
            {
                "event_ticker": event_ticker,
                "product_metadata": to_json_value(product_metadata),
            },
        )


def _needs_market_metadata(*values: Any) -> bool:
    """Return True when any metadata value is missing."""
    return any(value is None for value in values)


def _market_metadata_fetch_enabled() -> bool:
    return _env_bool("WEB_PORTAL_MARKET_METADATA_FETCH", True)


def _event_metadata_fetch_enabled() -> bool:
    return _env_bool("WEB_PORTAL_EVENT_METADATA_FETCH", True)


def _fetch_market_metadata_payload(
    row_ticker: str,
) -> tuple[dict[str, Any] | None, str | None, int | None]:
    """Fetch market metadata from the API when available."""
    market_data_fn = _portal_func("_get_market_data", _fetch_market_data)
    return market_data_fn(row_ticker)


def _log_market_metadata_error(err: str | None, status: int | None) -> None:
    if not err:
        return
    if status == 429:
        _portal_logger().warning("market metadata fetch rate limited")
    else:
        _portal_logger().warning("market metadata fetch failed: %s", err)


def _apply_market_metadata(
    price_ranges: Any | None,
    custom_strike: Any | None,
    mve_selected_legs: Any | None,
    product_metadata: Any | None,
    market_data: dict[str, Any],
) -> tuple[Any | None, Any | None, Any | None, Any | None]:
    """Fill missing metadata values from a market payload."""
    if price_ranges is None:
        price_ranges = market_data.get("price_ranges")
        if price_ranges is None:
            price_ranges = _maybe_parse_json(market_data.get("price_level_structure"))
    if custom_strike is None:
        custom_strike = market_data.get("custom_strike")
    if mve_selected_legs is None:
        mve_selected_legs = market_data.get("mve_selected_legs")
    if product_metadata is None:
        product_metadata = _extract_event_metadata(market_data)
    return price_ranges, custom_strike, mve_selected_legs, product_metadata


def _fetch_event_metadata_payload(
    event_ticker: str | None,
) -> tuple[Any | None, str | None]:
    """Fetch event metadata from the API when available."""
    if not event_ticker:
        return None, "Event metadata API unavailable."
    event_meta_fn = _portal_func("_get_event_metadata", _fetch_event_metadata)
    return event_meta_fn(event_ticker)


def _resolve_price_ranges(
    price_ranges: Any | None,
    row: dict[str, Any],
    row_ticker: str,
) -> Any | None:
    if price_ranges is not None:
        return price_ranges
    price_ranges = _maybe_parse_json(row.get("price_level_structure"))
    if price_ranges is None and row.get("price_level_structure"):
        _portal_logger().warning("Unable to parse price ranges for %s", row_ticker)
    return price_ranges


def _resolve_market_metadata(
    row: dict[str, Any],
    row_ticker: str,
) -> tuple[Any | None, Any | None, Any | None, Any | None]:
    """Resolve market metadata fields, fetching from APIs as needed."""
    price_ranges = row.get("price_ranges")
    custom_strike = row.get("custom_strike")
    mve_selected_legs = row.get("mve_selected_legs")
    product_metadata = row.get("product_metadata")

    if _market_metadata_fetch_enabled() and _needs_market_metadata(
        price_ranges,
        custom_strike,
        mve_selected_legs,
        product_metadata,
    ):
        market_data, err, status = _fetch_market_metadata_payload(row_ticker)
        _log_market_metadata_error(err, status)
        if market_data:
            price_ranges, custom_strike, mve_selected_legs, product_metadata = (
                _apply_market_metadata(
                    price_ranges,
                    custom_strike,
                    mve_selected_legs,
                    product_metadata,
                    market_data,
                )
            )
    if product_metadata is None and _event_metadata_fetch_enabled():
        metadata, meta_err = _fetch_event_metadata_payload(row.get("event_ticker"))
        if meta_err:
            _portal_logger().warning("event metadata fetch failed: %s", meta_err)
        else:
            product_metadata = metadata

    price_ranges = _resolve_price_ranges(price_ranges, row, row_ticker)
    custom_strike = custom_strike or _derive_custom_strike(row)
    mve_selected_legs = _maybe_parse_json(mve_selected_legs)
    product_metadata = _maybe_parse_json(product_metadata)
    return price_ranges, custom_strike, mve_selected_legs, product_metadata
