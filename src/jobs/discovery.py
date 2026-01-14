"""Discovery pass for open events and markets."""

from __future__ import annotations

import inspect
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg  # pylint: disable=import-error

from src.db.db import (
    cleanup_active_markets,
    get_event_updated_at,
    get_market_updated_at,
    get_state,
    parse_ts_iso,
    set_state,
    upsert_active_market,
    upsert_event,
    upsert_market,
)
from src.core.env_utils import env_int
from src.core.guardrails import assert_service_role
from src.jobs.event_filter import EventScanStats, accept_event
from src.kalshi.kalshi_sdk import iter_events

logger = logging.getLogger(__name__)

_DISCOVERY_HEARTBEAT_KEY = "last_discovery_heartbeat_ts"


@dataclass
class DiscoveryHeartbeat:
    """Heartbeat helper for long-running discovery passes."""

    conn: psycopg.Connection
    interval_s: int
    last_monotonic: float = 0.0

    def beat(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if force or now - self.last_monotonic >= self.interval_s:
            set_state(
                self.conn,
                _DISCOVERY_HEARTBEAT_KEY,
                datetime.now(timezone.utc).isoformat(),
            )
            self.last_monotonic = now


@dataclass(frozen=True)
class EventProcessContext:
    """Immutable context for event discovery processing."""

    conn: psycopg.Connection
    client: Any
    strike_periods: tuple[str, ...]
    stats: EventScanStats
    last_discovery_dt: datetime | None
    updated_since_params: dict
    heartbeat: DiscoveryHeartbeat


def _map_event_status(status: str) -> str:
    """Map user-facing statuses to API-accepted values."""
    if status == "active":
        return "open"
    return status


_UPDATED_KEYS = (
    "updated_time",
    "updated_at",
    "updated_ts",
    "last_updated_time",
    "last_updated_ts",
    "last_update_time",
)


def _parse_timestamp(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        timestamp_s = int(value)
        if timestamp_s > 1_000_000_000_000:
            timestamp_s = timestamp_s // 1000
        return datetime.fromtimestamp(timestamp_s, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            timestamp_s = int(text)
            if timestamp_s > 1_000_000_000_000:
                timestamp_s = timestamp_s // 1000
            return datetime.fromtimestamp(timestamp_s, tz=timezone.utc)
    try:
        return parse_ts_iso(value)
    except (TypeError, ValueError):
        return None


def _extract_updated_dt(payload: dict) -> datetime | None:
    for key in _UPDATED_KEYS:
        if key in payload and payload.get(key):
            updated = _parse_timestamp(payload.get(key))
            if updated is not None:
                return updated
    return None


def _resolve_events_method(client):
    if hasattr(client, "iter_events"):
        return getattr(client, "iter_events")
    for name in ("get_events", "list_events", "events"):
        if hasattr(client, name):
            return getattr(client, name)
    return None


def _build_updated_since_params(client, last_discovery_dt: datetime | None) -> dict:
    params: dict[str, int | str] = {}
    if last_discovery_dt is None:
        return params
    method = _resolve_events_method(client)
    if method is None:
        return params
    sig = inspect.signature(method)
    if any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values()):
        override = _updated_since_override(last_discovery_dt)
        return override or params
    epoch_s = int(last_discovery_dt.timestamp())
    iso_value = last_discovery_dt.isoformat()
    if "updated_after_ts" in sig.parameters:
        params["updated_after_ts"] = epoch_s
    elif "updated_after" in sig.parameters:
        params["updated_after"] = iso_value
    elif "updated_since" in sig.parameters:
        params["updated_since"] = iso_value
    return params


def _updated_since_override(last_discovery_dt: datetime) -> dict[str, int | str]:
    raw = os.getenv("DISCOVERY_UPDATED_SINCE_PARAM", "").strip().lower()
    if not raw or raw == "none":
        return {}
    epoch_s = int(last_discovery_dt.timestamp())
    iso_value = last_discovery_dt.isoformat()
    if raw == "updated_after_ts":
        return {"updated_after_ts": epoch_s}
    if raw == "updated_after":
        return {"updated_after": iso_value}
    if raw == "updated_since":
        return {"updated_since": iso_value}
    logger.warning("Unknown DISCOVERY_UPDATED_SINCE_PARAM=%s; ignoring", raw)
    return {}


def _parse_discovery_cursor(raw: str | None) -> datetime | None:
    if not raw:
        return None
    return _parse_timestamp(raw)


def _should_skip_event(
    conn: psycopg.Connection,
    event: dict,
    last_discovery_dt: datetime | None,
) -> bool:
    if last_discovery_dt is None:
        return False
    updated_dt = _extract_updated_dt(event)
    if updated_dt is not None:
        return updated_dt <= last_discovery_dt
    event_ticker = event.get("event_ticker")
    if not event_ticker:
        return False
    db_updated = get_event_updated_at(conn, event_ticker)
    return db_updated is not None and db_updated <= last_discovery_dt


def _should_skip_market(
    conn: psycopg.Connection,
    market: dict,
    last_discovery_dt: datetime | None,
) -> bool:
    if last_discovery_dt is None:
        return False
    updated_dt = _extract_updated_dt(market)
    if updated_dt is not None:
        return updated_dt <= last_discovery_dt
    ticker = market.get("ticker")
    if not ticker:
        return False
    db_updated = get_market_updated_at(conn, ticker)
    return db_updated is not None and db_updated <= last_discovery_dt


def _market_is_active(market: dict) -> tuple[bool, str]:
    """Determine whether a market should be considered active."""
    market_status = (market.get("status") or "").lower()
    if market_status in {"open", "paused", "active"}:
        return True, market_status
    if market_status:
        return False, market_status
    open_time = parse_ts_iso(market.get("open_time"))
    close_time = parse_ts_iso(market.get("close_time"))
    now = datetime.now(timezone.utc)
    is_active = (
        (open_time is None or open_time <= now)
        and (close_time is None or close_time > now)
    )
    return is_active, market_status


def _process_market(
    conn: psycopg.Connection,
    market: dict,
    last_discovery_dt: datetime | None,
) -> tuple[int, int]:
    """Upsert a market and return (market_updates, active_updates)."""
    if _should_skip_market(conn, market, last_discovery_dt):
        return 0, 0
    upsert_market(conn, market)
    is_active, market_status = _market_is_active(market)
    if is_active:
        upsert_active_market(
            conn,
            ticker=market["ticker"],
            event_ticker=market["event_ticker"],
            close_time=parse_ts_iso(market.get("close_time")),
            status=market.get("status") or market_status,
        )
        return 1, 1
    return 1, 0


def _process_event(
    conn: psycopg.Connection,
    event: dict,
    strike_periods: tuple[str, ...],
    stats: EventScanStats,
    last_discovery_dt: datetime | None,
) -> tuple[int, int, int]:
    """Upsert an event and its markets, returning (events, markets, active)."""
    if accept_event(event, strike_periods, stats) is None:
        return 0, 0, 0
    if _should_skip_event(conn, event, last_discovery_dt):
        return 0, 0, 0
    upsert_event(conn, event)
    markets = event.get("markets") or []
    market_updates = active_updates = 0
    for market in markets:
        market_delta, active_delta = _process_market(
            conn,
            market,
            last_discovery_dt,
        )
        market_updates += market_delta
        active_updates += active_delta
    return 1, market_updates, active_updates


def _process_event_status(
    context: EventProcessContext,
    event_status: str,
) -> tuple[int, int, int]:
    """Process events for a single status and return update counts."""
    query_status = _map_event_status(event_status)
    params = {"with_nested_markets": True, **context.updated_since_params}
    if query_status:
        params["status"] = query_status
    counts = [0, 0, 0]

    def _rate_limit_heartbeat(_: float) -> None:
        context.heartbeat.beat()

    try:
        context.heartbeat.beat()
        for event in iter_events(
            context.client,
            rate_limit_hook=_rate_limit_heartbeat,
            **params,
        ):  # with_nested_markets supported
            context.heartbeat.beat()
            deltas = _process_event(
                context.conn,
                event,
                context.strike_periods,
                context.stats,
                context.last_discovery_dt,
            )
            for idx, delta in enumerate(deltas):
                counts[idx] += delta
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception(
            "discovery_pass: events query failed (status=%s mapped=%s)",
            event_status,
            query_status,
        )
    return counts[0], counts[1], counts[2]


def discovery_pass(
    conn: psycopg.Connection,
    client,
    strike_periods: tuple[str, ...],
    event_statuses: tuple[str, ...],
) -> tuple[int, int, int]:
    """Discover hourly/daily events and keep active market subscriptions.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param client: Kalshi REST client.
    :type client: Any
    :param strike_periods: Allowed strike periods.
    :type strike_periods: tuple[str, ...]
    :param event_statuses: Event statuses to query.
    :type event_statuses: tuple[str, ...]
    :return: Counts of events, markets, and active market upserts.
    :rtype: tuple[int, int, int]
    """
    assert_service_role("rest", "discovery_pass")
    stats = EventScanStats()
    statuses = event_statuses or ("active",)
    last_discovery_dt = _parse_discovery_cursor(
        get_state(conn, "last_discovery_ts")
    )
    heartbeat_interval_s = env_int("DISCOVERY_HEARTBEAT_SECONDS", 60, minimum=5)
    heartbeat = DiscoveryHeartbeat(conn=conn, interval_s=heartbeat_interval_s)
    heartbeat.beat(force=True)
    updated_since_params = _build_updated_since_params(client, last_discovery_dt)
    if updated_since_params:
        logger.info(
            "discovery_pass: using updated-since filter %s",
            updated_since_params,
        )

    context = EventProcessContext(
        conn=conn,
        client=client,
        strike_periods=strike_periods,
        stats=stats,
        last_discovery_dt=last_discovery_dt,
        updated_since_params=updated_since_params,
        heartbeat=heartbeat,
    )
    counts = [0, 0, 0]
    for event_status in statuses:
        ev_delta, mk_delta, act_delta = _process_event_status(context, event_status)
        counts[0] += ev_delta
        counts[1] += mk_delta
        counts[2] += act_delta

    logger.info(
        "discovery_pass: events=%d markets=%d active_upserts=%d",
        counts[0],
        counts[1],
        counts[2],
    )
    logger.debug(
        "discovery_debug: raw_events=%d unique_events=%d filtered=%d dupes=%d statuses=%s",
        stats.raw_events,
        len(stats.seen_events),
        stats.filtered_events,
        stats.dup_events,
        ",".join(s for s in statuses if s) or "none",
    )
    logger.debug(
        "discovery_debug: strike_periods=%s allowed=%s",
        stats.summarize_strike_counts(),
        ",".join(strike_periods) or "none",
    )
    logger.debug(
        "discovery_debug: inferred=%d inferred_strike_periods=%s",
        stats.inferred_events,
        stats.summarize_inferred_counts(),
    )
    try:
        cleaned = cleanup_active_markets(conn)
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("cleanup_active_markets failed")
    else:
        if cleaned:
            logger.info("cleanup_active_markets: removed=%d", cleaned)
    set_state(conn, "last_discovery_ts", datetime.now(timezone.utc).isoformat())
    return counts[0], counts[1], counts[2]
