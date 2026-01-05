import logging
import json
from datetime import datetime, timezone
from typing import Tuple

import psycopg

from .db import (
    get_state, set_state, parse_ts_iso, upsert_event, upsert_market, dec
)
from .kalshi_sdk import iter_events, get_market_candlesticks

logger = logging.getLogger(__name__)

def _now_s() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def _interval_minutes_for_strike(sp: str, minutes_hour: int, minutes_day: int) -> int:
    sp = sp.lower()
    return minutes_hour if sp == "hour" else minutes_day

def backfill_pass(
    conn: psycopg.Connection,
    client,
    strike_periods: tuple[str, ...],
    minutes_hour: int,
    minutes_day: int,
    lookback_hours: int,
) -> Tuple[int, int, int]:
    """
    Returns: (events, markets, candles_inserted)
    """
    now_s = _now_s()
    default_cursor = str(now_s - 7 * 86400)  # start a week back on first run
    last_min_close_ts = int(get_state(conn, "last_min_close_ts", default_cursor) or default_cursor)

    ev_n = mk_n = candles_n = 0

    # We scan both closed and settled (some markets transition).
    # status accepted: open/closed/settled :contentReference[oaicite:32]{index=32}
    for status in ("closed", "settled"):
        for e in iter_events(client, status=status, with_nested_markets=True, min_close_ts=last_min_close_ts):
            sp = (e.get("strike_period") or "").strip().lower()
            if sp not in strike_periods:
                continue

            upsert_event(conn, e)
            ev_n += 1

            series = e.get("series_ticker")
            if not series:
                continue

            for m in (e.get("markets") or []):
                upsert_market(conn, m)
                mk_n += 1

                mt = m.get("ticker")
                if not mt:
                    continue

                # Pick candle resolution based on hour/day; period interval is minutes: 1/60/1440 :contentReference[oaicite:33]{index=33}
                period_minutes = _interval_minutes_for_strike(sp, minutes_hour, minutes_day)

                # Backfill window anchored to market open/close if present
                close_dt = parse_ts_iso(m.get("close_time"))
                open_dt = parse_ts_iso(m.get("open_time"))
                end_s = int((close_dt or datetime.now(timezone.utc)).timestamp())
                start_s = int(((open_dt or close_dt or datetime.now(timezone.utc)).timestamp()) - lookback_hours * 3600)

                # Fetch candles
                c = get_market_candlesticks(client, series, mt, start_ts=start_s, end_ts=end_s, period_interval_minutes=period_minutes)
                candles = c.get("candlesticks") or []

                with conn.cursor() as cur:
                    for cd in candles:
                        end_period_ts = int(cd["end_period_ts"])
                        end_dt = datetime.fromtimestamp(end_period_ts, tz=timezone.utc)
                        start_dt = datetime.fromtimestamp(end_period_ts - period_minutes * 60 + 1, tz=timezone.utc)

                        price = cd.get("price") or {}
                        # Extract close/open/high/low dollars if present (strings) :contentReference[oaicite:34]{index=34}
                        open_d = dec(price.get("open_dollars"))
                        high_d = dec(price.get("high_dollars"))
                        low_d  = dec(price.get("low_dollars"))
                        close_d= dec(price.get("close_dollars"))

                        cur.execute(
                            """
                            INSERT INTO market_candles(
                              market_ticker, period_interval_minutes, end_period_ts, start_period_ts,
                              open, high, low, close, volume, open_interest, raw
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING
                            """,
                            (
                                mt, period_minutes, end_dt, start_dt,
                                open_d, high_d, low_d, close_d,
                                cd.get("volume"), cd.get("open_interest"),
                                json.dumps(cd),
                            ),
                        )
                        candles_n += 1

                conn.commit()

    # Advance cursor: next run looks for events whose markets close after this time :contentReference[oaicite:35]{index=35}
    set_state(conn, "last_min_close_ts", str(now_s))
    logger.info("backfill_pass: events=%d markets=%d candles=%d min_close_ts->%d", ev_n, mk_n, candles_n, now_s)
    return ev_n, mk_n, candles_n
