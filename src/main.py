import asyncio
import logging
import os
import psycopg
from datetime import datetime, timezone

from .settings import load_settings
from .db import init_schema, set_state
from .kalshi_sdk import make_client
from .discovery import discovery_pass
from .backfill import backfill_pass
from .ws_ingest import ws_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

async def periodic_discovery(conn, client, s):
    while True:
        try:
            discovery_pass(conn, client, s.strike_periods)
            set_state(conn, "last_discovery_ts", datetime.now(timezone.utc).isoformat())
        except Exception:
            logger.exception("discovery failed")
        await asyncio.sleep(s.discovery_seconds)

async def periodic_backfill(conn, client, s):
    while True:
        try:
            backfill_pass(
                conn, client, s.strike_periods,
                s.candle_minutes_for_hour, s.candle_minutes_for_day,
                s.candle_lookback_hours,
            )
        except Exception:
            logger.exception("backfill failed")
        await asyncio.sleep(s.backfill_seconds)

async def main():
    s = load_settings()

    # Load private key for SDK + WS
    with open(s.kalshi_private_key_pem_path, "r") as f:
        private_key_pem = f.read()

    # REST client (SDK)
    client = make_client(s.kalshi_host, s.kalshi_api_key_id, private_key_pem)

    # DB
    conn = psycopg.connect(s.database_url)
    init_schema(conn, schema_path=os.path.join(os.path.dirname(__file__), "..", "sql", "schema.sql"))
    logger.info("DB schema ready.")

    # Do a first discovery + backfill immediately
    discovery_pass(conn, client, s.strike_periods)
    backfill_pass(conn, client, s.strike_periods, s.candle_minutes_for_hour, s.candle_minutes_for_day, s.candle_lookback_hours)

    # Run: discovery + backfill + websockets concurrently
    await asyncio.gather(
        periodic_discovery(conn, client, s),
        periodic_backfill(conn, client, s),
        ws_loop(
            conn,
            api_key_id=s.kalshi_api_key_id,
            private_key_pem=private_key_pem,
            max_active_tickers=s.max_active_tickers,
            ws_batch_size=s.ws_batch_size,
            refresh_seconds=s.ws_sub_refresh_seconds,
        ),
    )

if __name__ == "__main__":
    asyncio.run(main())
