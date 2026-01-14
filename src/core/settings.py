"""Settings loader for the Kalshi ingestor."""

import os
from dataclasses import dataclass

from src.core.env_utils import env_bool, env_float, env_int

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
ENV_FILE = os.getenv("ENV_FILE") or os.path.abspath(os.path.join(BASE_DIR, "..", ".env"))


@dataclass(frozen=True)
class Settings:
    """Runtime configuration parsed from environment variables.

    :ivar database_url: Postgres connection string.
    :ivar backup_database_url: Backup Postgres connection string (optional).
    :ivar kalshi_host: Kalshi REST API base URL.
    :ivar kalshi_api_key_id: Kalshi API key identifier.
    :ivar kalshi_private_key_pem_path: Filesystem path to private key PEM.
    :ivar strike_periods: Allowed strike periods (e.g., hour/day).
    :ivar discovery_event_statuses: Event statuses to include during discovery.
    :ivar discovery_seconds: Sleep interval for discovery loop.
    :ivar ws_sub_refresh_seconds: Interval to refresh WS subscriptions.
    :ivar backfill_seconds: Sleep interval for backfill loop.
    :ivar backfill_event_statuses: Event statuses to include during backfill.
    :ivar candle_minutes_for_hour: Candlestick resolution for hourly markets.
    :ivar candle_minutes_for_day: Candlestick resolution for daily markets.
    :ivar candle_lookback_hours: Chunk size in hours for candlestick backfill calls.
    :ivar max_active_tickers: Max active tickers to subscribe to.
    :ivar ws_batch_size: Batch size for WS subscription updates.
    :ivar closed_cleanup_seconds: Sleep interval for closed cleanup loop.
    :ivar closed_cleanup_event_statuses: Event statuses to include in closed cleanup.
    :ivar closed_cleanup_grace_minutes: Grace period before treating markets as closed.
    :ivar archive_closed_hours: Hours after close before archiving events.
    :ivar archive_require_terminal_lifecycle: Require terminal WS lifecycle before archiving.
    :ivar archive_batch_size: Batch size for archive inserts.
    """

    # pylint: disable=too-many-instance-attributes
    database_url: str
    backup_database_url: str | None
    kalshi_host: str
    kalshi_api_key_id: str
    kalshi_private_key_pem_path: str

    strike_periods: tuple[str, ...]
    discovery_event_statuses: tuple[str, ...]
    discovery_seconds: int
    ws_sub_refresh_seconds: int
    backfill_seconds: int
    backfill_event_statuses: tuple[str, ...]

    candle_minutes_for_hour: int
    candle_minutes_for_day: int
    candle_lookback_hours: int

    max_active_tickers: int
    ws_batch_size: int

    closed_cleanup_seconds: int
    closed_cleanup_event_statuses: tuple[str, ...]
    closed_cleanup_grace_minutes: int
    archive_closed_hours: float
    archive_require_terminal_lifecycle: bool
    archive_batch_size: int


def _parse_csv(raw: str) -> tuple[str, ...]:
    """Parse a comma-delimited string into lowercase tokens."""
    return tuple(p.strip().lower() for p in raw.split(",") if p.strip())


def load_settings() -> Settings:
    """Load settings from environment and .env defaults.

    :return: Parsed settings dataclass.
    :rtype: Settings
    :raises KeyError: If required environment variables are missing.
    """
    load_dotenv(dotenv_path=ENV_FILE)
    strike_periods = _parse_csv(os.getenv("STRIKE_PERIODS", "hour,day"))
    discovery_statuses = _parse_csv(os.getenv("DISCOVERY_EVENT_STATUSES", "open"))
    backfill_statuses = _parse_csv(
        os.getenv("BACKFILL_EVENT_STATUSES", "open,closed,settled")
    )
    closed_cleanup_statuses = _parse_csv(
        os.getenv("CLOSED_CLEANUP_EVENT_STATUSES", "closed,settled")
    )
    backup_database_url = os.getenv("BACKUP_DATABASE_URL")
    if backup_database_url:
        backup_database_url = os.path.expandvars(os.path.expanduser(backup_database_url))
    return Settings(
        database_url=os.path.expandvars(os.path.expanduser(os.environ["DATABASE_URL"])),
        backup_database_url=backup_database_url,
        kalshi_host=os.getenv(
            "KALSHI_HOST",
            "https://api.elections.kalshi.com/trade-api/v2",
        ),
        kalshi_api_key_id=os.environ["KALSHI_API_KEY_ID"],
        kalshi_private_key_pem_path=os.path.expandvars(
            os.path.expanduser(os.environ["KALSHI_PRIVATE_KEY_PEM_PATH"])
        ),

        strike_periods=strike_periods,
        discovery_event_statuses=discovery_statuses,
        discovery_seconds=int(os.getenv("DISCOVERY_SECONDS", "1800")),
        ws_sub_refresh_seconds=int(os.getenv("WS_SUB_REFRESH_SECONDS", "60")),
        backfill_seconds=int(os.getenv("BACKFILL_SECONDS", "900")),
        backfill_event_statuses=backfill_statuses,

        candle_minutes_for_hour=int(os.getenv("CANDLE_MINUTES_FOR_HOUR", "1")),
        candle_minutes_for_day=int(os.getenv("CANDLE_MINUTES_FOR_DAY", "60")),
        candle_lookback_hours=int(os.getenv("CANDLE_LOOKBACK_HOURS", "72")),

        max_active_tickers=int(os.getenv("MAX_ACTIVE_TICKERS", "5000")),
        ws_batch_size=int(os.getenv("WS_BATCH_SIZE", "500")),

        closed_cleanup_seconds=int(os.getenv("CLOSED_CLEANUP_SECONDS", "3600")),
        closed_cleanup_event_statuses=closed_cleanup_statuses,
        closed_cleanup_grace_minutes=int(os.getenv("CLOSED_CLEANUP_GRACE_MINUTES", "30")),

        archive_closed_hours=env_float("ARCHIVE_CLOSED_HOURS", 24.0, minimum=1.0),
        archive_require_terminal_lifecycle=env_bool(
            "ARCHIVE_REQUIRE_TERMINAL_LIFECYCLE",
            True,
        ),
        archive_batch_size=env_int("ARCHIVE_BATCH_SIZE", 2000, minimum=100),
    )
