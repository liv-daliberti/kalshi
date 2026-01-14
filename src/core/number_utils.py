"""Numeric parsing helpers shared across services."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


def normalize_probability(
    value: Any,
    *,
    quantize: Decimal | None = None,
) -> Decimal | None:
    """Normalize a value into a 0-1 probability."""
    if value is None:
        return None
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if dec < 0 or dec > 1:
        return None
    if quantize is None:
        return dec
    return dec.quantize(quantize)


def coerce_int(value: Any) -> int | None:
    """Parse an int-like value, returning None on failure."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _parse_cents_literal(value: Any) -> int | None:
    """Parse a string cents literal without a decimal point."""
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped or "." in stripped:
        return None
    try:
        return int(stripped)
    except ValueError:
        return None


def _parse_decimal_value(value: Any) -> Decimal | None:
    """Parse a Decimal from a numeric-like value."""
    if isinstance(value, float):
        return Decimal(str(value))
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _decimal_to_cents(dec_value: Decimal) -> int:
    """Convert a Decimal to cents using the expected rounding rules."""
    if dec_value <= Decimal("1.0") and dec_value != dec_value.to_integral_value():
        dec_value = (dec_value * Decimal(100)).to_integral_value(rounding=ROUND_HALF_UP)
    else:
        dec_value = dec_value.to_integral_value(rounding=ROUND_HALF_UP)
    return int(dec_value)


def to_cents(value: Any) -> int | None:
    """Parse a cents value, accepting dollar-style decimals."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    cents = _parse_cents_literal(value)
    if cents is not None:
        return cents
    dec_value = _parse_decimal_value(value)
    if dec_value is None:
        return None
    return _decimal_to_cents(dec_value)


def dollars_from_cents(value: Any) -> Decimal | None:
    """Convert a cents value to dollars."""
    if value is None:
        return None
    try:
        dec_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return dec_value / Decimal(100)


def infer_price_dollars(
    price_dollars: Decimal | None,
    yes_bid_dollars: Decimal | None,
    yes_ask_dollars: Decimal | None,
    *,
    wide_spread: bool,
) -> Decimal | None:
    """Infer a price from bid/ask data when none is provided."""
    if price_dollars is None and not wide_spread:
        if yes_bid_dollars is not None and yes_ask_dollars is not None:
            return (yes_bid_dollars + yes_ask_dollars) / Decimal(2)
        if yes_bid_dollars is not None:
            return yes_bid_dollars
        if yes_ask_dollars is not None:
            return yes_ask_dollars
    return price_dollars


def infer_price_dollars_from_quotes(
    price_dollars: Decimal | None,
    yes_bid_dollars: Decimal | None,
    yes_ask_dollars: Decimal | None,
    *,
    wide_spread: bool,
) -> Decimal | None:
    """Wrapper for inferring price dollars from quoted bid/ask values."""
    return infer_price_dollars(
        price_dollars,
        yes_bid_dollars,
        yes_ask_dollars,
        wide_spread=wide_spread,
    )


def infer_price_dollars_from_spread(
    price_dollars: Decimal | None,
    yes_bid_dollars: Decimal | None,
    yes_ask_dollars: Decimal | None,
    wide_spread: bool,
) -> Decimal | None:
    """Infer price dollars from bid/ask values with spread handling."""
    return infer_price_dollars_from_quotes(
        price_dollars,
        yes_bid_dollars,
        yes_ask_dollars,
        wide_spread=wide_spread,
    )


def infer_price_dollars_from_cents_spread(
    price_dollars: Decimal | None,
    yes_bid_dollars: Decimal | None,
    yes_ask_dollars: Decimal | None,
    yes_bid_cents: int | None,
    yes_ask_cents: int | None,
) -> Decimal | None:
    """Infer price dollars using cents to detect wide spreads."""
    wide_spread = yes_bid_cents == 0 and yes_ask_cents == 100
    return infer_price_dollars_from_spread(
        price_dollars,
        yes_bid_dollars,
        yes_ask_dollars,
        wide_spread,
    )
