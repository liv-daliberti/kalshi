import unittest
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

db_details = importlib.import_module("src.web_portal.db_details")


class FakeCursor:
    def __init__(self, fetchone_queue=None, fetchall_queue=None):
        self.fetchone_queue = list(fetchone_queue or [])
        self.fetchall_queue = list(fetchall_queue or [])
        self.executes = []

    def execute(self, sql, params=None):
        self.executes.append((sql, params))

    def fetchone(self):
        if self.fetchone_queue:
            return self.fetchone_queue.pop(0)
        return None

    def fetchall(self):
        if self.fetchall_queue:
            return self.fetchall_queue.pop(0)
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self, **_kwargs):
        return self._cursor


class TestOutcomeHelpers(unittest.TestCase):
    def test_outcome_prices_no_bid_from_yes_bid_without_ask(self) -> None:
        row = {}

        def yes_price_fn(_row, _is_open):
            return None, Decimal("0.2"), None

        def wide_spread_fn(_row):
            return False

        with patch.object(db_details, "_market_window", return_value=(None, None, True)):
            ctx = db_details._outcome_prices(row, datetime.now(timezone.utc), yes_price_fn, wide_spread_fn)
        self.assertEqual(ctx.no_bid, Decimal("0.8"))

    def test_outcome_prices_no_bid_from_yes_bid(self) -> None:
        row = {}

        def yes_price_fn(_row, _is_open):
            return Decimal("0.4"), Decimal("0.4"), None

        def wide_spread_fn(_row):
            return False

        with patch.object(db_details, "_market_window", return_value=(None, None, True)):
            ctx = db_details._outcome_prices(row, datetime.now(timezone.utc), yes_price_fn, wide_spread_fn)
        self.assertEqual(ctx.no_bid, Decimal("0.6"))

    def test_outcome_freshness_ws_source(self) -> None:
        row = {"last_tick_ts": "2024-01-01T00:00:00Z", "tick_source": "websocket"}
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch.object(db_details, "_format_age_minutes", return_value=(0, "0m")):
            freshness = db_details._outcome_freshness(row, now)
        self.assertEqual(freshness.source, "ws")
        self.assertEqual(freshness.label, "WS")

    def test_sum_event_volume_handles_invalid(self) -> None:
        rows = [{"volume": "10"}, {"volume": "bad"}, {"volume": None}]
        self.assertEqual(db_details._sum_event_volume(rows), 10)


class TestMarketFormatHelpers(unittest.TestCase):
    def test_market_stats_fields_snapshot_source(self) -> None:
        row = {"last_tick_ts": "2024-01-01T00:00:00Z", "candle_close": 1}
        fields = db_details._market_stats_fields(row)
        snapshot = next(field for field in fields if field["key"] == "snapshot_source")
        self.assertEqual(snapshot["value"], "market_ticks")

        row = {"last_tick_ts": None, "candle_close": 1}
        fields = db_details._market_stats_fields(row)
        snapshot = next(field for field in fields if field["key"] == "snapshot_source")
        self.assertEqual(snapshot["value"], "market_candles")

    def test_format_candle_ts_pass_through(self) -> None:
        self.assertEqual(db_details._format_candle_ts("raw"), "raw")

    def test_format_candle_num_none_or_invalid(self) -> None:
        self.assertIsNone(db_details._format_candle_num(None))
        self.assertIsNone(db_details._format_candle_num("bad"))

    def test_format_candle_int_none_or_invalid(self) -> None:
        self.assertIsNone(db_details._format_candle_int(None))
        self.assertIsNone(db_details._format_candle_int("bad"))


class TestMarketQueries(unittest.TestCase):
    def test_fetch_market_candles_limit_zero(self) -> None:
        candles, interval = db_details._fetch_market_candles(
            conn=FakeConn(FakeCursor()),
            market_ticker="M1",
            limit=0,
        )
        self.assertEqual(candles, [])
        self.assertIsNone(interval)

    def test_fetch_market_candles_no_interval(self) -> None:
        cursor = FakeCursor(fetchone_queue=[{"period_interval_minutes": None}])
        candles, interval = db_details._fetch_market_candles(
            conn=FakeConn(cursor),
            market_ticker="M1",
            limit=5,
        )
        self.assertEqual(candles, [])
        self.assertIsNone(interval)

    def test_fetch_market_predictions_limit_zero(self) -> None:
        preds = db_details._fetch_market_predictions(
            conn=FakeConn(FakeCursor()),
            market_ticker="M1",
            limit=0,
        )
        self.assertEqual(preds, [])

    def test_fetch_market_detail_none(self) -> None:
        with patch.object(db_details, "_fetch_market_detail_row", return_value=None):
            self.assertIsNone(db_details.fetch_market_detail(object(), "M1"))
