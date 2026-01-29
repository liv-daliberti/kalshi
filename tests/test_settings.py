import os
import unittest
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

settings = importlib.import_module("src.core.settings")


class TestSettings(unittest.TestCase):
    @patch.dict(
        os.environ,
        {
            "DATABASE_URL": "postgres://user:pass@host/db",
            "BACKUP_DATABASE_URL": "postgres://user:pass@host/backup",
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY_PEM_PATH": "/tmp/key.pem",
            "STRIKE_PERIODS": " Hour , day,Week ",
            "DISCOVERY_EVENT_STATUSES": "Active, open",
            "BACKFILL_EVENT_STATUSES": "closed , settled",
            "DISCOVERY_SECONDS": "123",
            "WS_SUB_REFRESH_SECONDS": "45",
            "BACKFILL_SECONDS": "67",
            "CANDLE_MINUTES_FOR_HOUR": "5",
            "CANDLE_MINUTES_FOR_DAY": "55",
            "CANDLE_LOOKBACK_HOURS": "24",
            "MAX_ACTIVE_TICKERS": "500",
            "WS_BATCH_SIZE": "80",
            "CLOSED_CLEANUP_SECONDS": "360",
            "CLOSED_CLEANUP_EVENT_STATUSES": "closed, settled",
            "CLOSED_CLEANUP_GRACE_MINUTES": "15",
            "ARCHIVE_CLOSED_HOURS": "24",
            "ARCHIVE_REQUIRE_TERMINAL_LIFECYCLE": "0",
            "ARCHIVE_BATCH_SIZE": "1500",
        },
        clear=True,
    )
    def test_load_settings_parses_values(self) -> None:
        s = settings.load_settings()
        self.assertEqual(s.database_url, "postgres://user:pass@host/db")
        self.assertEqual(s.backup_database_url, "postgres://user:pass@host/backup")
        self.assertEqual(s.kalshi_api_key_id, "key-id")
        self.assertEqual(s.kalshi_private_key_pem_path, "/tmp/key.pem")
        self.assertEqual(s.strike_periods, ("hour", "day", "week"))
        self.assertEqual(s.discovery_event_statuses, ("active", "open"))
        self.assertEqual(s.discovery_seconds, 123)
        self.assertEqual(s.ws_sub_refresh_seconds, 45)
        self.assertEqual(s.backfill_seconds, 67)
        self.assertEqual(s.backfill_event_statuses, ("closed", "settled"))
        self.assertEqual(s.candle_minutes_for_hour, 5)
        self.assertEqual(s.candle_minutes_for_day, 55)
        self.assertEqual(s.candle_lookback_hours, 24)
        self.assertEqual(s.max_active_tickers, 500)
        self.assertEqual(s.ws_batch_size, 80)
        self.assertEqual(s.closed_cleanup_seconds, 360)
        self.assertEqual(s.closed_cleanup_event_statuses, ("closed", "settled"))
        self.assertEqual(s.closed_cleanup_grace_minutes, 15)
        self.assertEqual(s.archive_closed_hours, 24.0)
        self.assertFalse(s.archive_require_terminal_lifecycle)
        self.assertEqual(s.archive_batch_size, 1500)
