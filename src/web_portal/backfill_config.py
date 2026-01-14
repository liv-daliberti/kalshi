"""Portal-specific backfill configuration."""

from __future__ import annotations

import os

from src.jobs.backfill_config import BackfillConfig

from .config import _env_int, _parse_csv

__all__ = ["BackfillConfig", "_load_backfill_config"]


def _load_backfill_config() -> BackfillConfig:
    """Load backfill config values from the environment."""
    strike_periods = _parse_csv(os.getenv("STRIKE_PERIODS", "hour,day"))
    backfill_statuses = _parse_csv(
        os.getenv("BACKFILL_EVENT_STATUSES", "open,closed,settled")
    )
    return BackfillConfig(
        strike_periods=strike_periods,
        event_statuses=backfill_statuses,
        minutes_hour=_env_int("CANDLE_MINUTES_FOR_HOUR", 1),
        minutes_day=_env_int("CANDLE_MINUTES_FOR_DAY", 60),
        lookback_hours=_env_int("CANDLE_LOOKBACK_HOURS", 72),
    )
