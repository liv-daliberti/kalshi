"""Shared backfill configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BackfillConfig:
    """Configuration for backfill timings and strike filtering."""

    strike_periods: tuple[str, ...]
    event_statuses: tuple[str, ...]
    minutes_hour: int
    minutes_day: int
    lookback_hours: int


def build_backfill_config_from_settings(
    settings: Any,
    *,
    event_statuses: tuple[str, ...] | None = None,
) -> BackfillConfig:
    """Build a backfill config from the standard settings shape."""
    if event_statuses is None:
        event_statuses = settings.backfill_event_statuses
    return BackfillConfig(
        strike_periods=settings.strike_periods,
        event_statuses=event_statuses,
        minutes_hour=settings.candle_minutes_for_hour,
        minutes_day=settings.candle_minutes_for_day,
        lookback_hours=settings.candle_lookback_hours,
    )
