"""Shared SQL fragments for common query blocks."""

from __future__ import annotations


def last_tick_lateral_sql(columns: str | None = None) -> str:
    """Return a lateral join for the latest market tick."""
    if columns is None:
        columns = (
            "t.ts, t.implied_yes_mid, t.price_dollars, t.yes_bid_dollars, "
            "t.yes_ask_dollars, t.volume"
        )
    return f"""
    LEFT JOIN LATERAL (
      SELECT {columns}
      FROM market_ticks t
      WHERE t.ticker = m.ticker
      ORDER BY t.ts DESC, t.id DESC
      LIMIT 1
    ) t ON TRUE
    """


def last_candle_lateral_sql() -> str:
    """Return a lateral join for the latest market candle."""
    return """
    LEFT JOIN LATERAL (
      SELECT c.end_period_ts, c.close
      FROM market_candles c
      WHERE c.market_ticker = m.ticker
      ORDER BY c.end_period_ts DESC
      LIMIT 1
    ) c ON TRUE
    """


def last_prediction_lateral_sql(columns: str | None = None) -> str:
    """Return a lateral join for the latest prediction row."""
    if columns is None:
        columns = "p.predicted_yes_prob, p.confidence, p.created_at"
    return f"""
    LEFT JOIN LATERAL (
      SELECT {columns}
      FROM market_predictions p
      WHERE p.market_ticker = m.ticker
      ORDER BY p.created_at DESC, p.id DESC
      LIMIT 1
    ) p ON TRUE
    """


def tick_columns_sql(prefix: str = "t", ts_alias: str = "last_tick_ts") -> str:
    """Return a column list for the latest tick values."""
    return (
        f"{prefix}.ts AS {ts_alias},\n"
        f"{prefix}.implied_yes_mid,\n"
        f"{prefix}.price_dollars,\n"
        f"{prefix}.yes_bid_dollars,\n"
        f"{prefix}.yes_ask_dollars,\n"
        f"{prefix}.volume"
    )


def market_identity_columns_sql(prefix: str = "m", *, ticker_alias: str | None = None) -> str:
    """Return common market identity columns."""
    ticker_expr = f"{prefix}.ticker"
    if ticker_alias:
        ticker_expr = f"{prefix}.ticker AS {ticker_alias}"
    return (
        f"{ticker_expr},\n"
        f"{prefix}.title AS market_title,\n"
        f"{prefix}.subtitle AS market_subtitle,\n"
        f"{prefix}.yes_sub_title,\n"
        f"{prefix}.no_sub_title"
    )


def event_core_columns_sql(prefix: str = "e") -> str:
    """Return common event columns used in portal listings."""
    return (
        f"{prefix}.event_ticker,\n"
        f"{prefix}.title AS event_title,\n"
        f"{prefix}.category AS event_category"
    )
