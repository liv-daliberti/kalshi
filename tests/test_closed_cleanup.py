import logging
import os
import runpy
import sys
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_kalshi_sdk_stub, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()
ensure_kalshi_sdk_stub()

closed_cleanup = importlib.import_module("src.jobs.closed_cleanup")


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executes = []

    def execute(self, sql, params=None):
        self.executes.append((sql, params))

    def fetchone(self):
        if self.rows:
            return self.rows[0]
        return (1,)

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else (1,)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.cursors = []
        self.commits = 0

    def cursor(self):
        cursor = FakeCursor(self.rows)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        self.commits += 1


class TestClosedCleanupHelpers(unittest.TestCase):
    def test_parse_log_level(self) -> None:
        self.assertEqual(closed_cleanup._parse_log_level("DEBUG"), logging.DEBUG)
        self.assertEqual(closed_cleanup._parse_log_level("15"), 15)
        self.assertEqual(closed_cleanup._parse_log_level("unknown"), logging.INFO)
        self.assertEqual(closed_cleanup._parse_log_level(""), logging.INFO)

    def test_env_float_minimum(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(closed_cleanup._env_float("X", 1.5), 1.5)
        with patch.dict(os.environ, {"X": "bad"}):
            self.assertEqual(closed_cleanup._env_float("X", 1.5), 1.5)
        with patch.dict(os.environ, {"X": "0.1"}):
            self.assertEqual(closed_cleanup._env_float("X", 1.5, minimum=0.5), 0.5)

    def test_configure_logging_unknown_level(self) -> None:
        with patch.dict(os.environ, {"LOG_LEVEL": "mystery"}), \
             patch("src.jobs.closed_cleanup.logger.warning") as warn, \
             patch("logging.basicConfig") as basic_config:
            closed_cleanup.configure_logging()

        self.assertEqual(basic_config.call_count, 1)
        warn.assert_called_once()

    def test_time_helpers(self) -> None:
        naive = datetime(2024, 1, 1, 0, 0, 0)
        aware = closed_cleanup._ensure_tz(naive)
        self.assertEqual(aware.tzinfo, timezone.utc)
        self.assertIsNone(closed_cleanup._ensure_tz(None))
        self.assertIsNone(closed_cleanup._epoch_to_dt(None))
        self.assertIsNone(closed_cleanup._dt_to_epoch(None))
        self.assertEqual(
            closed_cleanup._epoch_to_dt(1704067200),
            datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(closed_cleanup._dt_to_epoch(naive), 1704067200)
        with patch("src.jobs.closed_cleanup._ensure_tz", return_value=None):
            self.assertIsNone(closed_cleanup._dt_to_epoch(naive))

    def test_infer_strike_period_and_minutes(self) -> None:
        open_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        close_hour = open_time + timedelta(hours=1)
        close_day = open_time + timedelta(hours=10)
        self.assertEqual(
            closed_cleanup._infer_strike_period_from_times(
                open_time,
                close_hour,
                hour_max=2.0,
                day_max=36.0,
            ),
            "hour",
        )
        self.assertEqual(
            closed_cleanup._infer_strike_period_from_times(
                open_time,
                close_day,
                hour_max=2.0,
                day_max=12.0,
            ),
            "day",
        )
        self.assertIsNone(
            closed_cleanup._infer_strike_period_from_times(
                close_day,
                open_time,
                hour_max=2.0,
                day_max=12.0,
            )
        )
        self.assertIsNone(
            closed_cleanup._infer_strike_period_from_times(
                open_time,
                open_time + timedelta(days=3),
                hour_max=2.0,
                day_max=36.0,
            )
        )
        self.assertIsNone(
            closed_cleanup._infer_strike_period_from_times(
                None,
                close_day,
                hour_max=2.0,
                day_max=12.0,
            )
        )
        self.assertIsNone(closed_cleanup._period_minutes(None, 1, 60))
        self.assertEqual(closed_cleanup._period_minutes("Hour", 1, 60), 1)
        self.assertEqual(closed_cleanup._period_minutes("day", 1, 60), 60)
        self.assertIsNone(closed_cleanup._period_minutes("week", 1, 60))

    def test_min_close_ts_empty(self) -> None:
        self.assertIsNone(closed_cleanup._min_close_ts({}))
        missing = {
            "M1": closed_cleanup.MissingMarket(
                ticker="M1",
                event_ticker="EV1",
                series_ticker="SR1",
                strike_period="hour",
                open_time=None,
                close_time=None,
                period_minutes=1,
                first_candle_end=None,
                last_candle_end=None,
            )
        }
        self.assertIsNone(closed_cleanup._min_close_ts(missing))

    def test_candles_missing_logic(self) -> None:
        open_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        close_time = open_time + timedelta(hours=2)

        def build_market(**kwargs):
            data = dict(
                ticker="M1",
                event_ticker="EV1",
                series_ticker="SR1",
                strike_period="hour",
                open_time=open_time,
                close_time=close_time,
                period_minutes=60,
                first_candle_end=open_time + timedelta(hours=1),
                last_candle_end=close_time,
            )
            data.update(kwargs)
            return closed_cleanup.MissingMarket(**data)

        self.assertFalse(closed_cleanup._candles_missing(build_market(period_minutes=None)))
        self.assertFalse(closed_cleanup._candles_missing(build_market(close_time=None)))
        self.assertTrue(closed_cleanup._candles_missing(build_market(last_candle_end=None)))
        self.assertTrue(
            closed_cleanup._candles_missing(
                build_market(last_candle_end=close_time - timedelta(hours=2))
            )
        )
        self.assertTrue(
            closed_cleanup._candles_missing(
                build_market(first_candle_end=open_time + timedelta(hours=2))
            )
        )
        self.assertFalse(closed_cleanup._candles_missing(build_market(open_time=None)))
        self.assertFalse(closed_cleanup._candles_missing(build_market(first_candle_end=None)))
        self.assertFalse(closed_cleanup._candles_missing(build_market()))


class TestClosedCleanupQueries(unittest.TestCase):
    def test_load_missing_closed_markets_infers_and_filters(self) -> None:
        open_hour = datetime(2024, 1, 1, tzinfo=timezone.utc)
        close_hour = open_hour + timedelta(hours=1)
        open_day = datetime(2024, 1, 1, tzinfo=timezone.utc)
        close_day = open_day + timedelta(days=1)
        rows = [
            (
                "M1",
                "EV1",
                open_hour,
                close_hour,
                "SR1",
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "M2",
                "EV2",
                open_day,
                close_day,
                "SR2",
                "day",
                42,
                None,
                close_day,
                open_day + timedelta(minutes=60),
                close_day - timedelta(minutes=120),
            ),
        ]
        conn = FakeConn(rows)
        cfg = closed_cleanup.ClosedCleanupConfig(
            strike_periods=("hour", "day"),
            event_statuses=("closed", "settled"),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
            grace_minutes=10,
        )
        inferred_min = int((open_hour + timedelta(minutes=1)).timestamp())
        inferred_max = int(close_hour.timestamp())
        with patch(
            "src.jobs.closed_cleanup._fetch_candle_bounds",
            return_value=(inferred_min, inferred_max),
        ) as fetch_bounds:
            missing_settlement, missing_candles = closed_cleanup._load_missing_closed_markets(
                conn,
                cfg,
            )

        self.assertIn("M1", missing_settlement)
        self.assertNotIn("M1", missing_candles)
        market1 = missing_settlement["M1"]
        self.assertEqual(market1.strike_period, "hour")
        self.assertEqual(market1.period_minutes, 1)
        self.assertEqual(market1.first_candle_end, open_hour + timedelta(minutes=1))
        self.assertEqual(market1.last_candle_end, close_hour)
        fetch_bounds.assert_called_once_with(conn, "M1", 1)

        self.assertIn("M2", missing_candles)
        self.assertNotIn("M2", missing_settlement)
        market2 = missing_candles["M2"]
        self.assertEqual(market2.strike_period, "day")

    def test_refresh_missing_settlements_updates(self) -> None:
        close_one = datetime(2024, 1, 1, tzinfo=timezone.utc)
        close_two = close_one + timedelta(hours=1)
        missing = {
            "M1": closed_cleanup.MissingMarket(
                ticker="M1",
                event_ticker="EV1",
                series_ticker="SR1",
                strike_period="hour",
                open_time=None,
                close_time=close_one,
                period_minutes=1,
                first_candle_end=None,
                last_candle_end=None,
            ),
            "M2": closed_cleanup.MissingMarket(
                ticker="M2",
                event_ticker="EV2",
                series_ticker="SR2",
                strike_period="day",
                open_time=None,
                close_time=close_two,
                period_minutes=60,
                first_candle_end=None,
                last_candle_end=None,
            ),
        }
        cfg = closed_cleanup.ClosedCleanupConfig(
            strike_periods=("hour", "day"),
            event_statuses=("closed", "settled"),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
            grace_minutes=10,
        )
        event_closed = {"event_ticker": "EV1", "markets": [{"ticker": "M1"}]}
        event_settled = {"event_ticker": "EV2", "markets": [{"ticker": "M2"}]}

        def fake_iter_events(client, **params):
            status = params.get("status")
            if status == "closed":
                return [event_closed]
            if status == "settled":
                return [event_settled]
            return []

        with patch("src.jobs.closed_cleanup.iter_events", side_effect=fake_iter_events) as iter_events, \
             patch("src.jobs.closed_cleanup.upsert_event") as upsert_event, \
             patch("src.jobs.closed_cleanup.upsert_market") as upsert_market:
            events_updated, markets_updated = closed_cleanup._refresh_missing_settlements(
                conn=object(),
                client=object(),
                cfg=cfg,
                missing_settlement=missing,
            )

        self.assertEqual(events_updated, 2)
        self.assertEqual(markets_updated, 2)
        self.assertEqual(len(missing), 0)
        self.assertEqual(upsert_event.call_count, 2)
        self.assertEqual(upsert_market.call_count, 2)
        expected_min = int(close_one.timestamp())
        for call in iter_events.call_args_list:
            self.assertEqual(call.kwargs["min_close_ts"], expected_min)
            self.assertTrue(call.kwargs["with_nested_markets"])

    def test_refresh_missing_settlements_empty(self) -> None:
        cfg = closed_cleanup.ClosedCleanupConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
            grace_minutes=10,
        )
        self.assertEqual(
            closed_cleanup._refresh_missing_settlements(
                conn=object(),
                client=object(),
                cfg=cfg,
                missing_settlement={},
            ),
            (0, 0),
        )

    def test_refresh_missing_settlements_filters_event_tickers(self) -> None:
        close_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        missing = {
            "M1": closed_cleanup.MissingMarket(
                ticker="M1",
                event_ticker="EV1",
                series_ticker="SR1",
                strike_period="hour",
                open_time=None,
                close_time=close_time,
                period_minutes=1,
                first_candle_end=None,
                last_candle_end=None,
            )
        }
        cfg = closed_cleanup.ClosedCleanupConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
            grace_minutes=10,
        )
        event_other = {"event_ticker": "EV2", "markets": [{"ticker": "M2"}]}

        with patch("src.jobs.closed_cleanup.iter_events", return_value=[event_other]) as iter_events, \
             patch("src.jobs.closed_cleanup.upsert_event") as upsert_event, \
             patch("src.jobs.closed_cleanup.upsert_market") as upsert_market:
            events_updated, markets_updated = closed_cleanup._refresh_missing_settlements(
                conn=object(),
                client=object(),
                cfg=cfg,
                missing_settlement=missing,
            )

        self.assertEqual(events_updated, 0)
        self.assertEqual(markets_updated, 0)
        self.assertEqual(upsert_event.call_count, 0)
        self.assertEqual(upsert_market.call_count, 0)
        self.assertEqual(len(iter_events.call_args_list), 1)

    def test_backfill_market_candles_missing_tickers(self) -> None:
        open_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        close_time = open_time + timedelta(hours=1)
        cfg = closed_cleanup.ClosedCleanupConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
            grace_minutes=10,
        )
        conn = FakeConn()
        market = closed_cleanup.MissingMarket(
            ticker="",
            event_ticker="EV1",
            series_ticker="SR1",
            strike_period="hour",
            open_time=open_time,
            close_time=close_time,
            period_minutes=1,
            first_candle_end=None,
            last_candle_end=None,
        )
        with patch("src.jobs.closed_cleanup._build_candle_ranges") as build_ranges:
            self.assertEqual(closed_cleanup._backfill_market_candles(conn, object(), cfg, market, 60), (0, 0))
        build_ranges.assert_not_called()
        self.assertEqual(conn.commits, 0)

        market = closed_cleanup.MissingMarket(
            ticker="M1",
            event_ticker="EV1",
            series_ticker="",
            strike_period="hour",
            open_time=open_time,
            close_time=close_time,
            period_minutes=1,
            first_candle_end=None,
            last_candle_end=None,
        )
        with patch("src.jobs.closed_cleanup._build_candle_ranges") as build_ranges:
            self.assertEqual(closed_cleanup._backfill_market_candles(conn, object(), cfg, market, 60), (0, 0))
        build_ranges.assert_not_called()
        self.assertEqual(conn.commits, 0)

    def test_backfill_missing_candles_empty(self) -> None:
        cfg = closed_cleanup.ClosedCleanupConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
            grace_minutes=10,
        )
        self.assertEqual(
            closed_cleanup._backfill_missing_candles(
                conn=object(),
                client=object(),
                cfg=cfg,
                missing_candles={},
            ),
            (0, 0),
        )

    def test_backfill_missing_candles_start_after_end(self) -> None:
        open_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        close_time = open_time + timedelta(hours=1)
        market = closed_cleanup.MissingMarket(
            ticker="M1",
            event_ticker="EV1",
            series_ticker="SR1",
            strike_period="hour",
            open_time=open_time,
            close_time=close_time,
            period_minutes=1,
            first_candle_end=None,
            last_candle_end=None,
        )
        cfg = closed_cleanup.ClosedCleanupConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
            grace_minutes=10,
        )
        conn = FakeConn()
        with patch("src.jobs.closed_cleanup._market_time_window", return_value=(200, 100, True)):
            result = closed_cleanup._backfill_missing_candles(
                conn=conn,
                client=object(),
                cfg=cfg,
                missing_candles={"M1": market},
            )

        self.assertEqual(result, (0, 0))
        self.assertEqual(conn.commits, 0)

    def test_backfill_missing_candles_no_ranges(self) -> None:
        open_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        close_time = open_time + timedelta(hours=1)
        market = closed_cleanup.MissingMarket(
            ticker="M1",
            event_ticker="EV1",
            series_ticker="SR1",
            strike_period="hour",
            open_time=open_time,
            close_time=close_time,
            period_minutes=1,
            first_candle_end=None,
            last_candle_end=None,
        )
        cfg = closed_cleanup.ClosedCleanupConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
            grace_minutes=10,
        )
        conn = FakeConn()
        with patch("src.jobs.closed_cleanup._market_time_window", return_value=(100, 200, True)), \
             patch("src.jobs.closed_cleanup._build_backfill_ranges", return_value=[]):
            result = closed_cleanup._backfill_missing_candles(
                conn=conn,
                client=object(),
                cfg=cfg,
                missing_candles={"M1": market},
            )

        self.assertEqual(result, (0, 0))
        self.assertEqual(conn.commits, 0)

    def test_backfill_missing_candles(self) -> None:
        open_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        close_time = open_time + timedelta(hours=1)
        market = closed_cleanup.MissingMarket(
            ticker="M1",
            event_ticker="EV1",
            series_ticker="SR1",
            strike_period="hour",
            open_time=open_time,
            close_time=close_time,
            period_minutes=1,
            first_candle_end=None,
            last_candle_end=None,
        )
        skipped_market = closed_cleanup.MissingMarket(
            ticker="M2",
            event_ticker="EV2",
            series_ticker="SR2",
            strike_period="hour",
            open_time=open_time,
            close_time=close_time,
            period_minutes=None,
            first_candle_end=None,
            last_candle_end=None,
        )
        missing_candles = {"M1": market, "M2": skipped_market}
        cfg = closed_cleanup.ClosedCleanupConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
            grace_minutes=10,
        )
        conn = FakeConn()
        inserted = []

        def fake_insert(_cur, ticker, period_minutes, candle):
            inserted.append((ticker, period_minutes, candle))

        with patch("src.jobs.closed_cleanup._market_time_window", return_value=(100, 200, True)), \
             patch("src.jobs.closed_cleanup._build_backfill_ranges", return_value=[(100, 200)]), \
             patch("src.jobs.closed_cleanup._iter_time_chunks", return_value=[(100, 150), (150, 200)]), \
             patch("src.jobs.closed_cleanup.get_market_candlesticks", return_value={"candlesticks": [{}, {}]}), \
             patch("src.jobs.closed_cleanup._insert_candle", side_effect=fake_insert):
            markets_touched, candles_inserted = closed_cleanup._backfill_missing_candles(
                conn=conn,
                client=object(),
                cfg=cfg,
                missing_candles=missing_candles,
            )

        self.assertEqual(markets_touched, 1)
        self.assertEqual(candles_inserted, 4)
        self.assertEqual(len(inserted), 4)
        self.assertEqual(conn.commits, 1)

    def test_closed_cleanup_pass_with_missing(self) -> None:
        cfg = closed_cleanup.ClosedCleanupConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
            grace_minutes=10,
        )
        missing_settlement = {
            "M1": closed_cleanup.MissingMarket(
                ticker="M1",
                event_ticker="EV1",
                series_ticker="SR1",
                strike_period="hour",
                open_time=None,
                close_time=None,
                period_minutes=1,
                first_candle_end=None,
                last_candle_end=None,
            )
        }
        missing_candles = {
            "M2": closed_cleanup.MissingMarket(
                ticker="M2",
                event_ticker="EV2",
                series_ticker="SR2",
                strike_period="hour",
                open_time=None,
                close_time=None,
                period_minutes=1,
                first_candle_end=None,
                last_candle_end=None,
            )
        }
        with patch(
            "src.jobs.closed_cleanup._load_missing_closed_markets",
            return_value=(missing_settlement, missing_candles),
        ), \
            patch("src.jobs.closed_cleanup._refresh_missing_settlements", return_value=(2, 1)) as refresh, \
            patch("src.jobs.closed_cleanup._backfill_missing_candles", return_value=(1, 5)) as backfill, \
            patch("src.jobs.closed_cleanup.cleanup_active_markets", return_value=4) as cleanup_active:
            result = closed_cleanup.closed_cleanup_pass(
                conn=object(),
                client=object(),
                cfg=cfg,
            )

        self.assertEqual(result, (2, 1, 5))
        self.assertEqual(refresh.call_count, 1)
        self.assertEqual(backfill.call_count, 1)
        self.assertEqual(cleanup_active.call_count, 1)

    def test_closed_cleanup_pass_no_missing(self) -> None:
        cfg = closed_cleanup.ClosedCleanupConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
            grace_minutes=10,
        )
        with patch(
            "src.jobs.closed_cleanup._load_missing_closed_markets",
            return_value=({}, {}),
        ) as load_missing, \
            patch("src.jobs.closed_cleanup.cleanup_active_markets", return_value=3) as cleanup_active, \
            patch("src.jobs.closed_cleanup._refresh_missing_settlements") as refresh_settlements, \
            patch("src.jobs.closed_cleanup._backfill_missing_candles") as backfill_candles:
            result = closed_cleanup.closed_cleanup_pass(
                conn=object(),
                client=object(),
                cfg=cfg,
            )

        self.assertEqual(result, (0, 0, 0))
        self.assertEqual(load_missing.call_count, 1)
        self.assertEqual(cleanup_active.call_count, 1)
        self.assertEqual(refresh_settlements.call_count, 0)
        self.assertEqual(backfill_candles.call_count, 0)

    def test_run_once_uses_settings(self) -> None:
        settings = SimpleNamespace(
            kalshi_private_key_pem_path="key.pem",
            kalshi_host="host",
            kalshi_api_key_id="key",
            database_url="db",
            strike_periods=("hour",),
            closed_cleanup_event_statuses=("closed",),
            candle_minutes_for_hour=1,
            candle_minutes_for_day=60,
            candle_lookback_hours=2,
            closed_cleanup_grace_minutes=10,
        )
        conn = FakeConn()
        client = object()
        with patch("src.jobs.closed_cleanup.load_settings", return_value=settings), \
             patch("src.jobs.closed_cleanup.configure_logging") as configure_logging, \
             patch(
                 "src.jobs.closed_cleanup.open_client_and_conn",
                 return_value=(client, conn),
             ) as open_client_and_conn, \
             patch("src.jobs.closed_cleanup.closed_cleanup_pass") as cleanup_pass:
            closed_cleanup.run_once()

        self.assertEqual(configure_logging.call_count, 1)
        self.assertEqual(open_client_and_conn.call_count, 1)
        _, kwargs = open_client_and_conn.call_args
        self.assertEqual(
            kwargs.get("schema_path_override"),
            closed_cleanup.schema_path(closed_cleanup.__file__),
        )
        self.assertEqual(cleanup_pass.call_count, 1)
        args = cleanup_pass.call_args[0]
        self.assertIs(args[0], conn)
        self.assertIs(args[1], client)
        cfg = args[2]
        self.assertEqual(cfg.strike_periods, settings.strike_periods)
        self.assertEqual(cfg.event_statuses, settings.closed_cleanup_event_statuses)
        self.assertEqual(cfg.minutes_hour, settings.candle_minutes_for_hour)
        self.assertEqual(cfg.minutes_day, settings.candle_minutes_for_day)
        self.assertEqual(cfg.lookback_hours, settings.candle_lookback_hours)
        self.assertEqual(cfg.grace_minutes, settings.closed_cleanup_grace_minutes)

    def test_main_invokes_run_once(self) -> None:
        settings = SimpleNamespace(
            kalshi_private_key_pem_path="key.pem",
            kalshi_host="host",
            kalshi_api_key_id="key",
            database_url="db",
            strike_periods=("hour",),
            closed_cleanup_event_statuses=("closed",),
            candle_minutes_for_hour=1,
            candle_minutes_for_day=60,
            candle_lookback_hours=2,
            closed_cleanup_grace_minutes=10,
        )

        class RunnerConn:
            def cursor(self):
                return FakeCursor([])

            def commit(self):
                return None

        with patch("src.core.settings.load_settings", return_value=settings), \
             patch(
                 "src.core.service_utils.open_client_and_conn",
                 return_value=(object(), RunnerConn()),
             ) as open_client_and_conn, \
             patch("src.db.db.cleanup_active_markets", return_value=0):
            prev = sys.modules.pop("src.jobs.closed_cleanup", None)
            try:
                runpy.run_module("src.jobs.closed_cleanup", run_name="__main__")
            finally:
                if prev is not None:
                    sys.modules["src.jobs.closed_cleanup"] = prev
        self.assertEqual(open_client_and_conn.call_count, 1)
