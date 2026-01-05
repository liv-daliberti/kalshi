import logging
import psycopg
from typing import Tuple

from .db import parse_ts_iso, upsert_event, upsert_market, upsert_active_market
from .kalshi_sdk import iter_events

logger = logging.getLogger(__name__)

def discovery_pass(conn: psycopg.Connection, client, strike_periods: tuple[str, ...]) -> Tuple[int, int, int]:
    """
    Discover open hourly/daily events and their outcomes (nested markets),
    and maintain active_markets for WS subscription.
    """
    ev_n = mk_n = act_n = 0

    for e in iter_events(client, status="open", with_nested_markets=True):  # with_nested_markets supported :contentReference[oaicite:18]{index=18}
        sp = (e.get("strike_period") or "").strip().lower()
        if sp not in strike_periods:
            continue

        upsert_event(conn, e)
        ev_n += 1

        for m in (e.get("markets") or []):
            upsert_market(conn, m)
            mk_n += 1

            st = (m.get("status") or "").lower()
            if st in {"open", "paused"}:  # market status supports paused :contentReference[oaicite:19]{index=19}
                upsert_active_market(
                    conn,
                    ticker=m["ticker"],
                    event_ticker=m["event_ticker"],
                    close_time=parse_ts_iso(m.get("close_time")),
                    status=m.get("status") or st,
                )
                act_n += 1

    logger.info("discovery_pass: events=%d markets=%d active_upserts=%d", ev_n, mk_n, act_n)
    return ev_n, mk_n, act_n
