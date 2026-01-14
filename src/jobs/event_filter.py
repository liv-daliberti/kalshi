"""Shared helpers for filtering and de-duplicating events."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from src.core.time_utils import infer_strike_period_from_times


@dataclass
class EventScanStats:
    """Track event scan metrics and seen tickers.

    :ivar raw_events: Total events observed.
    :ivar filtered_events: Events filtered out by strike period.
    :ivar dup_events: Duplicate events skipped by ticker.
    :ivar inferred_events: Events accepted via duration inference.
    :ivar seen_events: Set of event tickers already processed.
    :ivar strike_counts: Counts of strike_period values observed.
    :ivar inferred_counts: Counts of inferred strike periods.
    """

    raw_events: int = 0
    filtered_events: int = 0
    dup_events: int = 0
    inferred_events: int = 0
    seen_events: set[str] = field(default_factory=set)
    strike_counts: dict[str, int] = field(default_factory=dict)
    inferred_counts: dict[str, int] = field(default_factory=dict)

    def summarize_strike_counts(self, limit: int = 8) -> str:
        """Return a compact summary of strike period counts."""
        if not self.strike_counts:
            return "none"
        items = sorted(self.strike_counts.items(), key=lambda kv: kv[1], reverse=True)
        parts = [f"{key or 'unknown'}={val}" for key, val in items[:limit]]
        if len(items) > limit:
            parts.append("...")
        return ",".join(parts)

    def summarize_inferred_counts(self, limit: int = 8) -> str:
        """Return a compact summary of inferred strike period counts."""
        if not self.inferred_counts:
            return "none"
        items = sorted(self.inferred_counts.items(), key=lambda kv: kv[1], reverse=True)
        parts = [f"{key or 'unknown'}={val}" for key, val in items[:limit]]
        if len(items) > limit:
            parts.append("...")
        return ",".join(parts)


def _parse_iso(value: Optional[object]) -> Optional[datetime]:
    """Parse an ISO timestamp with optional Z suffix."""
    if not value:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _infer_strike_period(event: dict, hour_max: float, day_max: float) -> Optional[str]:
    """Infer strike period from open/close timestamps."""
    open_dt = _parse_iso(event.get("open_time"))
    close_dt = _parse_iso(event.get("close_time"))

    if open_dt is None or close_dt is None:
        min_open = None
        max_close = None
        for market in event.get("markets") or []:
            m_open = _parse_iso(market.get("open_time"))
            m_close = _parse_iso(market.get("close_time"))
            if m_open and m_close:
                if min_open is None or m_open < min_open:
                    min_open = m_open
                if max_close is None or m_close > max_close:
                    max_close = m_close
        open_dt = min_open
        close_dt = max_close

    return infer_strike_period_from_times(open_dt, close_dt, hour_max, day_max)


def accept_event(
    event: dict,
    strike_periods: tuple[str, ...],
    stats: EventScanStats,
) -> Optional[str]:
    """Return the strike period if an event should be processed.

    :param event: Event payload.
    :type event: dict
    :param strike_periods: Allowed strike periods.
    :type strike_periods: tuple[str, ...]
    :param stats: Mutable stats tracker.
    :type stats: EventScanStats
    :return: Strike period (explicit or inferred) when accepted, else None.
    :rtype: str | None
    """
    stats.raw_events += 1
    event_ticker = (event.get("event_ticker") or event.get("ticker") or "").strip()
    if event_ticker:
        if event_ticker in stats.seen_events:
            stats.dup_events += 1
            return None
        stats.seen_events.add(event_ticker)

    strike_period = (event.get("strike_period") or "").strip().lower()
    stats.strike_counts[strike_period] = stats.strike_counts.get(strike_period, 0) + 1
    if strike_period in strike_periods:
        return strike_period

    hour_max = float(os.getenv("STRIKE_HOUR_MAX_HOURS", "2"))
    day_max = float(os.getenv("STRIKE_DAY_MAX_HOURS", "36"))
    inferred = _infer_strike_period(event, hour_max, day_max)
    if inferred and inferred in strike_periods:
        stats.inferred_events += 1
        stats.inferred_counts[inferred] = stats.inferred_counts.get(inferred, 0) + 1
        return inferred

    stats.filtered_events += 1
    return None
