"""Shared helpers for prediction baselines."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable, Optional


def baseline_market_prob(
    market: dict[str, Any],
    normalize: Callable[[Any], Optional[Decimal]],
) -> tuple[Optional[Decimal], Optional[str]]:
    """Return a baseline probability and its source from market pricing."""
    yes_prob = normalize(market.get("implied_yes_mid"))
    source = "implied_yes_mid"
    if yes_prob is None:
        source = "candle_close"
        yes_prob = normalize(market.get("candle_close"))
    if yes_prob is None:
        return None, None
    return yes_prob, source
