"""Formatting and parsing helpers for the web portal."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from src.core.number_utils import (
    coerce_int as _coerce_int,
    normalize_probability,
    to_cents as _to_cents,
)
from src.core.time_utils import parse_epoch_seconds, parse_ts

_parse_epoch_seconds = parse_epoch_seconds


def _money_quant(digits: int) -> Decimal:
    """Return the Decimal quantization scale for a given precision.

    :param digits: Number of fractional digits.
    :type digits: int
    :return: Decimal scale for quantization.
    :rtype: decimal.Decimal
    """
    if digits <= 0:
        return Decimal("1")
    return Decimal("1").scaleb(-digits)


def fmt_money(value: Any, digits: int = 4) -> str:
    """Format a numeric value as dollars.

    :param value: Numeric value to format.
    :type value: Any
    :param digits: Fractional digits to include.
    :type digits: int
    :return: Formatted currency string.
    :rtype: str
    """
    if value is None:
        return "N/A"
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return "N/A"
    sign = "-" if dec < 0 else ""
    dec = abs(dec).quantize(_money_quant(digits))
    return f"{sign}${dec:,.{digits}f}"


def fmt_cents(value: Any) -> str:
    """Format a probability/price as cents."""
    if value is None:
        return "--"
    cents = _to_cents(value)
    if cents is None:
        return "N/A"
    return f"{cents}¢"


def fmt_percent(value: Any) -> str:
    """Format a probability as percent."""
    result = "--"
    dec = None
    if value is not None:
        try:
            dec = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            dec = None
    if dec is not None and Decimal("0") <= dec <= Decimal("1"):
        if dec == 0:
            result = "0%"
        elif dec == 1:
            result = "100%"
        elif dec < Decimal("0.01"):
            result = "<1%"
        elif dec > Decimal("0.99"):
            result = ">99%"
        else:
            pct = int((dec * Decimal(100)).to_integral_value(rounding=ROUND_HALF_UP))
            result = f"{pct}%"
    return result


def clamp_probability(value: Any) -> Decimal | None:
    """Normalize a value into a 0-1 probability."""
    return normalize_probability(value)


def _invalid_probability(value: Any) -> bool:
    """Return True when a value cannot be interpreted as a 0-1 probability."""
    if value is None:
        return False
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return True
    return dec < 0 or dec > 1


def _pricing_tick_details(row: dict[str, Any]) -> list[str]:
    details: list[str] = []
    tick_fields = (
        "implied_yes_mid",
        "price_dollars",
        "yes_bid_dollars",
        "yes_ask_dollars",
    )
    if row.get("last_tick_ts") is None:
        details.append("no market_ticks rows")
        return details
    missing_tick_fields = [field for field in tick_fields if row.get(field) is None]
    if missing_tick_fields:
        details.append(f"missing tick fields: {', '.join(missing_tick_fields)}")
    invalid_tick_fields = [
        field for field in tick_fields if _invalid_probability(row.get(field))
    ]
    if invalid_tick_fields:
        details.append(f"invalid tick fields: {', '.join(invalid_tick_fields)}")
    return details


def _pricing_candle_details(row: dict[str, Any]) -> list[str]:
    details: list[str] = []
    candle_close = row.get("candle_close")
    if candle_close is None:
        details.append("no market_candles close")
    elif _invalid_probability(candle_close):
        details.append("invalid market_candles close")
    return details


def _pricing_settlement_details(row: dict[str, Any]) -> list[str]:
    details: list[str] = []
    settlement_dollars = row.get("settlement_value_dollars")
    settlement_cents = row.get("settlement_value")
    if settlement_dollars is None and settlement_cents is None:
        details.append("no settlement value")
        return details
    if settlement_dollars is not None and _invalid_probability(settlement_dollars):
        details.append("invalid settlement value (dollars)")
    if settlement_cents is not None:
        try:
            cents = int(settlement_cents)
        except (ValueError, TypeError):
            details.append("invalid settlement value (cents)")
        else:
            if cents < 0 or cents > 100:
                details.append("invalid settlement value (cents)")
    return details


def _pricing_error_for_row(
    row: dict[str, Any],
    yes_price: Decimal | None,
    yes_bid: Decimal | None,
    no_bid: Decimal | None,
) -> str | None:
    if yes_price is not None and yes_bid is not None and no_bid is not None:
        return None

    details = []
    details.extend(_pricing_tick_details(row))
    details.extend(_pricing_candle_details(row))
    details.extend(_pricing_settlement_details(row))
    if not details:
        details.append("pricing data incomplete")
    return f"Pricing unavailable: {'; '.join(details)}"


def fmt_num(value: Any) -> str:
    """Format integer-ish values for display.

    :param value: Numeric-ish value to format.
    :type value: Any
    :return: Formatted number string.
    :rtype: str
    """
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return f"{int(value):,}"
    value_int = _coerce_int(value)
    if value_int is not None:
        return f"{value_int:,}"
    return str(value)


def fmt_bool(value: Any) -> str:
    """Format boolean-ish values."""
    if value is None:
        return "N/A"
    return "Yes" if bool(value) else "No"


def fmt_json(value: Any) -> str:
    """Pretty-print JSON-ish values."""
    if value is None:
        return "N/A"
    try:
        return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True)
    except (TypeError, ValueError):
        return str(value)


def fmt_outcome(
    settlement_value: Any,
    settlement_value_dollars: Any,
) -> str:
    """Format an outcome label from settlement values."""
    result = "Pending"
    cents = None
    if settlement_value is not None:
        try:
            cents = int(settlement_value)
        except (TypeError, ValueError):
            cents = None
    if cents is not None:
        if cents == 100:
            result = "YES"
        elif cents == 0:
            result = "NO"
        else:
            result = str(cents)
        return result
    if settlement_value_dollars is not None:
        try:
            dec = Decimal(str(settlement_value_dollars))
        except (InvalidOperation, ValueError, TypeError):
            dec = None
        if dec is not None:
            if dec == Decimal("1") or dec == Decimal("1.0000"):
                result = "YES"
            elif dec == Decimal("0") or dec == Decimal("0.0000"):
                result = "NO"
            else:
                result = f"{dec.normalize()}"
    return result


def _market_label(row: dict[str, Any]) -> str:
    """Select the best label for a market row."""
    market_title = row.get("market_title") or row.get("title")
    market_subtitle = row.get("market_subtitle") or row.get("subtitle")
    yes_subtitle = row.get("yes_sub_title")
    return (
        market_subtitle
        or yes_subtitle
        or market_title
        or row.get("market_ticker")
        or row.get("ticker")
        or "N/A"
    )


def _settlement_is_yes(
    settlement_value: Any,
    settlement_value_dollars: Any,
) -> bool:
    """Return True if settlement indicates a YES outcome."""
    if settlement_value is not None:
        try:
            cents = int(settlement_value)
        except (TypeError, ValueError):
            cents = None
        if cents == 100:
            return True
    if settlement_value_dollars is not None:
        try:
            dec = Decimal(str(settlement_value_dollars))
        except (InvalidOperation, ValueError, TypeError):
            dec = None
        if dec is not None and dec == Decimal("1"):
            return True
    return False


def _adjust_quote_inputs(
    yes_bid: Decimal | None,
    yes_ask: Decimal | None,
    last_price: Decimal | None,
    implied_mid: Decimal | None,
) -> tuple[Decimal | None, Decimal | None, Decimal | None, bool]:
    wide_spread = yes_bid == Decimal("0") and yes_ask == Decimal("1")
    if wide_spread:
        implied_mid = None
    empty_bid = (
        yes_bid == Decimal("0")
        and yes_ask is None
        and last_price is None
    )
    empty_ask = (
        yes_ask == Decimal("1")
        and yes_bid is None
        and last_price is None
    )
    if empty_bid:
        if implied_mid == Decimal("0"):
            implied_mid = None
        yes_bid = None
    if empty_ask:
        if implied_mid == Decimal("1"):
            implied_mid = None
        yes_ask = None
    return yes_bid, yes_ask, implied_mid, wide_spread


def _infer_yes_price(
    yes_bid: Decimal | None,
    yes_ask: Decimal | None,
    last_price: Decimal | None,
    implied_mid: Decimal | None,
    *,
    wide_spread: bool,
) -> Decimal | None:
    yes_price = implied_mid
    if yes_price is None:
        if yes_bid is not None and yes_ask is not None and not wide_spread:
            yes_price = clamp_probability((yes_bid + yes_ask) / Decimal(2))
        elif last_price is not None:
            yes_price = last_price
        elif yes_bid is not None and not wide_spread:
            yes_price = yes_bid
        elif yes_ask is not None and not wide_spread:
            yes_price = yes_ask
    return yes_price


def _closed_market_yes_price(row: dict[str, Any]) -> Decimal | None:
    yes_price = clamp_probability(row.get("candle_close"))
    if yes_price is not None:
        return yes_price
    yes_price = clamp_probability(row.get("settlement_value_dollars"))
    if yes_price is not None:
        return yes_price
    settlement_cents = row.get("settlement_value")
    if settlement_cents is None:
        return None
    return clamp_probability(Decimal(str(settlement_cents)) / Decimal(100))


def _derive_yes_price(
    row: dict[str, Any],
    market_is_open: bool,
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    """Calculate a best-effort yes price with related bid/ask values."""
    yes_bid = clamp_probability(row.get("yes_bid_dollars"))
    yes_ask = clamp_probability(row.get("yes_ask_dollars"))
    last_price = clamp_probability(row.get("price_dollars"))
    implied_mid = clamp_probability(row.get("implied_yes_mid"))
    yes_bid, yes_ask, implied_mid, wide_spread = _adjust_quote_inputs(
        yes_bid,
        yes_ask,
        last_price,
        implied_mid,
    )
    yes_price = _infer_yes_price(
        yes_bid,
        yes_ask,
        last_price,
        implied_mid,
        wide_spread=wide_spread,
    )
    if yes_price is None and not market_is_open:
        yes_price = _closed_market_yes_price(row)
    return yes_price, yes_bid, yes_ask


def _is_wide_spread(row: dict[str, Any]) -> bool:
    """Return True when the quoted bid/ask is a 0/1 wide spread."""
    yes_bid = clamp_probability(row.get("yes_bid_dollars"))
    yes_ask = clamp_probability(row.get("yes_ask_dollars"))
    return yes_bid == Decimal("0") and yes_ask == Decimal("1")


def _format_event_outcome_label(
    settled: list[str],
    leader: tuple[Decimal, str] | None,
    mutually_exclusive: bool | None,
) -> str:
    """Format the global event outcome based on settlements or price leader."""
    if settled:
        if len(settled) == 1:
            return settled[0]
        if mutually_exclusive:
            return "Multiple outcomes settled (check data)"
        return "Multiple outcomes settled"
    if leader:
        pct = fmt_percent(leader[0])
        if pct != "--":
            return f"Leading: {leader[1]} ({pct})"
        return f"Leading: {leader[1]}"
    return "Pending"


def _compute_event_outcome_label(
    market_rows: list[dict[str, Any]],
    mutually_exclusive: bool | None,
) -> str:
    """Compute the event-level outcome label from market rows."""
    settled: list[str] = []
    leader: tuple[Decimal, str] | None = None
    now = datetime.now(timezone.utc)
    for row in market_rows:
        label = _market_label(row)
        if _settlement_is_yes(
            row.get("settlement_value"),
            row.get("settlement_value_dollars"),
        ):
            settled.append(label)
        _, _, market_is_open = _market_window(row, now)
        yes_price, _, _ = _derive_yes_price(row, market_is_open)
        if yes_price is not None:
            if leader is None:
                leader = (yes_price, label)
            elif yes_price > leader[0]:
                leader = (yes_price, label)
    return _format_event_outcome_label(settled, leader, mutually_exclusive)


def _market_window(
    row: dict[str, Any],
    now: datetime,
    *,
    open_key: str = "market_open_time",
    close_key: str = "market_close_time",
) -> tuple[datetime | None, datetime | None, bool]:
    """Return open/close timestamps and open/closed status."""
    market_open_dt = _parse_ts(row.get(open_key))
    market_close_dt = _parse_ts(row.get(close_key))
    market_is_open = (
        (market_open_dt is None or market_open_dt <= now)
        and (market_close_dt is None or market_close_dt > now)
    )
    return market_open_dt, market_close_dt, market_is_open


def slugify(text: str) -> str:
    """Convert text into a URL-friendly slug."""
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug


def derive_series_ticker(event_ticker: str | None) -> str:
    """Extract the series ticker prefix from an event ticker."""
    if not event_ticker:
        return ""
    return event_ticker.split("-", 1)[0].lower()


def get_market_url(
    ticker: str | None,
    event_ticker: str | None = None,
    event_title: str | None = None,
    series_ticker: str | None = None,
) -> str | None:
    """Build a Kalshi market URL for a ticker."""
    if not ticker:
        return None
    series = (series_ticker or derive_series_ticker(event_ticker)).lower()
    event_slug = slugify(event_title or "") if event_title else ""
    if not event_slug and event_ticker:
        event_slug = event_ticker.lower()
    template = os.getenv("WEB_PORTAL_KALSHI_MARKET_URL_TEMPLATE")
    if template:
        context = {
            "ticker": ticker.lower(),
            "market_ticker": ticker.lower(),
            "event_ticker": (event_ticker or "").lower(),
            "event_slug": event_slug,
            "series_ticker": series,
        }
        try:
            return template.format(**context)
        except KeyError:
            pass

    if series and event_slug:
        return (
            "https://kalshi.com/markets/"
            f"{series}/{event_slug}/{ticker.lower()}"
        )
    if event_ticker and event_slug:
        return (
            "https://kalshi.com/markets/"
            f"{event_ticker.lower()}/{event_slug}/{ticker.lower()}"
        )
    return f"https://kalshi.com/markets/{ticker.lower()}"


def get_event_url(
    event_ticker: str | None,
    series_ticker: str | None,
    event_title: str | None,
) -> str | None:
    """Build a Kalshi event URL."""
    if not event_ticker:
        return None
    template = os.getenv("WEB_PORTAL_KALSHI_EVENT_URL_TEMPLATE")
    series = (series_ticker or derive_series_ticker(event_ticker)).lower()
    event_slug = slugify(event_title or "") if event_title else ""
    context = {
        "event_ticker": event_ticker.lower(),
        "series_ticker": series,
        "event_slug": event_slug or event_ticker.lower(),
    }
    if template:
        try:
            return template.format(**context)
        except KeyError:
            pass
    return (
        "https://kalshi.com/markets/"
        f"{series}/{context['event_slug']}/{context['event_ticker']}"
    )


def fmt_ts(value: Any) -> str:
    """Format a timestamp for display in UTC.

    :param value: Timestamp value or datetime.
    :type value: Any
    :return: Formatted timestamp string.
    :rtype: str
    """
    if value is None:
        return "N/A"
    if isinstance(value, datetime):
        ts_value = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return ts_value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return str(value)


def _format_pg_version(version: int | None) -> str | None:
    if not version:
        return None
    major = version // 10000
    minor = (version % 10000) // 100
    patch = version % 100
    return f"{major}.{minor}.{patch}"


def _parse_ts(value: Any) -> datetime | None:
    """Parse a timestamp into a timezone-aware datetime."""
    return parse_ts(value)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _format_age_minutes(
    ts_value: datetime | None,
    now: datetime,
) -> tuple[int | None, str | None]:
    """Return (age_minutes, label) for a timestamp."""
    if ts_value is None:
        return None, None
    ts_value = ts_value if ts_value.tzinfo else ts_value.replace(tzinfo=timezone.utc)
    age_seconds = max(0.0, (now - ts_value).total_seconds())
    age_minutes = int(age_seconds // 60)
    if age_minutes < 1:
        return 0, "<1m"
    return age_minutes, f"{age_minutes}m"


def fmt_time_remaining(close_time: Any, *, now: datetime | None = None) -> str:
    """Format time remaining until close."""
    result = "N/A"
    if not close_time:
        return result
    if isinstance(close_time, datetime):
        close_dt = close_time if close_time.tzinfo else close_time.replace(tzinfo=timezone.utc)
    else:
        try:
            close_dt = datetime.fromisoformat(str(close_time))
        except ValueError:
            return result
        if close_dt.tzinfo is None:
            close_dt = close_dt.replace(tzinfo=timezone.utc)

    if now is None:
        now = _now_utc()
    seconds = int((close_dt - now).total_seconds())
    if seconds <= 0:
        result = "Closed"
    else:
        days, rem = divmod(seconds, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, _ = divmod(rem, 60)
        if days:
            result = f"{days}d {hours}h" if hours else f"{days}d"
        elif hours:
            result = f"{hours}h {minutes}m" if minutes else f"{hours}h"
        elif minutes:
            result = f"{minutes}m"
        else:
            result = "<1m"
    return result
