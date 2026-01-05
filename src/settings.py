from dataclasses import dataclass
import os
from dotenv import load_dotenv

@dataclass(frozen=True)
class Settings:
    database_url: str
    kalshi_host: str
    kalshi_api_key_id: str
    kalshi_private_key_pem_path: str

    strike_periods: tuple[str, ...]
    discovery_seconds: int
    ws_sub_refresh_seconds: int
    backfill_seconds: int

    candle_minutes_for_hour: int
    candle_minutes_for_day: int
    candle_lookback_hours: int

    max_active_tickers: int
    ws_batch_size: int

def load_settings() -> Settings:
    load_dotenv()
    sp = tuple(p.strip().lower() for p in os.getenv("STRIKE_PERIODS", "hour,day").split(",") if p.strip())
    return Settings(
        database_url=os.environ["DATABASE_URL"],
        kalshi_host=os.getenv("KALSHI_HOST", "https://api.elections.kalshi.com/trade-api/v2"),
        kalshi_api_key_id=os.environ["KALSHI_API_KEY_ID"],
        kalshi_private_key_pem_path=os.environ["KALSHI_PRIVATE_KEY_PEM_PATH"],

        strike_periods=sp,
        discovery_seconds=int(os.getenv("DISCOVERY_SECONDS", "1800")),
        ws_sub_refresh_seconds=int(os.getenv("WS_SUB_REFRESH_SECONDS", "60")),
        backfill_seconds=int(os.getenv("BACKFILL_SECONDS", "900")),

        candle_minutes_for_hour=int(os.getenv("CANDLE_MINUTES_FOR_HOUR", "1")),
        candle_minutes_for_day=int(os.getenv("CANDLE_MINUTES_FOR_DAY", "60")),
        candle_lookback_hours=int(os.getenv("CANDLE_LOOKBACK_HOURS", "72")),

        max_active_tickers=int(os.getenv("MAX_ACTIVE_TICKERS", "5000")),
        ws_batch_size=int(os.getenv("WS_BATCH_SIZE", "500")),
    )
