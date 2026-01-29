import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_kalshi_sdk_stub, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()
ensure_kalshi_sdk_stub()

backfill = importlib.import_module("src.jobs.backfill")
backfill_market = importlib.import_module("src.jobs.backfill_market")
backfill_scan = importlib.import_module("src.jobs.backfill_scan")
work_queue = importlib.import_module("src.queue.work_queue")


class FakeCursor:
    def __init__(self, executes):
        self.executes = executes

    def execute(self, sql, params=None):
        self.executes.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self):
        self.executes = []
        self.commits = 0

    def cursor(self):
        return FakeCursor(self.executes)

    def commit(self):
        self.commits += 1


class FakeFetchCursor:
    def __init__(self, executes, rows):
        self.executes = executes
        self.rows = rows
        self.index = 0

    def execute(self, sql, params=None):
        self.executes.append((sql, params))

    def fetchone(self):
        if self.index >= len(self.rows):
            return None
        row = self.rows[self.index]
        self.index += 1
        return row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeFetchConn:
    def __init__(self, rows):
        self.executes = []
        self.rows = rows

    def cursor(self):
        return FakeFetchCursor(self.executes, self.rows)


class TestBackfillPass(unittest.TestCase):
    def test_backfill_pass_counts_and_candles(self) -> None:
        event = {
            "strike_period": "hour",
            "event_ticker": "EV1",
            "series_ticker": "SR1",
            "markets": [
                {
                    "ticker": "M1",
                    "open_time": "2024-01-01T00:00:00Z",
                    "close_time": "2024-01-01T01:00:00Z",
                }
            ],
        }
        candles = {
            "candlesticks": [
                {
                    "end_period_ts": 1704070800,
                    "price": {
                        "open_dollars": "0.10",
                        "high_dollars": "0.20",
                        "low_dollars": "0.05",
                        "close_dollars": "0.15",
                    },
                    "volume": 10,
                    "open_interest": 4,
                }
            ]
        }

        def fake_iter_events(client, status, with_nested_markets, min_close_ts):
            return [event] if status == "closed" else []

        def fake_get_state(conn, key, default=None):
            return default

        conn = FakeConn()

        with patch("src.jobs.backfill_scan.iter_events", side_effect=fake_iter_events), \
             patch("src.jobs.backfill_market.get_market_candlesticks", return_value=candles) as get_candles, \
             patch("src.jobs.backfill_market._fetch_candle_bounds", return_value=(None, None)), \
             patch("src.jobs.backfill_scan.get_state", side_effect=fake_get_state), \
             patch("src.jobs.backfill_market.get_state", side_effect=fake_get_state), \
             patch("src.jobs.backfill_scan.set_state") as set_state_scan, \
             patch("src.jobs.backfill_market.set_state") as set_state_market, \
             patch("src.jobs.backfill_scan.upsert_event") as upsert_event, \
             patch("src.jobs.backfill_scan.upsert_market") as upsert_market, \
             patch("src.jobs.backfill_scan._now_s", return_value=1_700_000_000):
            cfg = backfill.BackfillConfig(
                strike_periods=("hour",),
                event_statuses=("closed",),
                minutes_hour=1,
                minutes_day=60,
                lookback_hours=2,
            )
            result = backfill_scan.backfill_pass(conn=conn, client=object(), cfg=cfg)

        self.assertEqual(result, (1, 1, 1))
        self.assertEqual(upsert_event.call_count, 1)
        self.assertEqual(upsert_market.call_count, 1)
        self.assertEqual(len(conn.executes), 2)
        self.assertEqual(conn.commits, 1)
        set_state_market.assert_called_once_with(conn, "backfill_last_ts:M1:1", "1704070800")
        set_state_scan.assert_called_once_with(conn, "last_min_close_ts", "1700000000")
        self.assertGreaterEqual(get_candles.call_count, 1)
        for _, kwargs in get_candles.call_args_list:
            self.assertEqual(kwargs["period_interval_minutes"], 1)

    def test_backfill_event_market_failure_isolated(self) -> None:
        event = {
            "strike_period": "hour",
            "event_ticker": "EV1",
            "series_ticker": "SR1",
            "markets": [
                {"ticker": "M1"},
                {"ticker": "M2"},
            ],
        }

        class RollbackConn:
            def __init__(self):
                self.rollbacks = 0

            def rollback(self):
                self.rollbacks += 1

        def fake_upsert_market(_conn, market):
            if market.get("ticker") == "M1":
                raise RuntimeError("boom")

        def fake_backfill_market(_conn, _client, _cfg, context, force_full=False):
            if context.market.get("ticker") == "M2":
                return 2
            return 0

        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
        )
        conn = RollbackConn()
        ctx = backfill_scan.EventBackfillContext(
            event=event,
            strike_period="hour",
            queue_cfg=None,
            publisher=None,
        )

        with patch("src.jobs.backfill_scan.upsert_event"), \
             patch("src.jobs.backfill_scan.upsert_market", side_effect=fake_upsert_market), \
             patch("src.jobs.backfill_scan._backfill_market", side_effect=fake_backfill_market):
            markets, candles, queued = backfill_scan._backfill_event(
                conn,
                client=object(),
                cfg=cfg,
                context=ctx,
            )

        self.assertEqual(markets, 1)
        self.assertEqual(candles, 2)
        self.assertEqual(queued, 0)

    def test_interval_minutes_for_strike(self) -> None:
        self.assertEqual(backfill_market._interval_minutes_for_strike("hour", 1, 60), 1)
        self.assertEqual(backfill_market._interval_minutes_for_strike("day", 1, 60), 60)


class TestBackfillHelpers(unittest.TestCase):
    def _cfg(self) -> backfill.BackfillConfig:
        return backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )

    def _queue_cfg(self) -> backfill.QueueConfig:
        return backfill.QueueConfig(
            enabled=True,
            job_types=("backfill_market",),
            worker_id="worker",
            rabbitmq=work_queue.QueueRabbitMQConfig(
                url="amqp://",
                queue_name="q",
                prefetch=1,
                publish=True,
            ),
            timing=work_queue.QueueTimingConfig(
                poll_seconds=1,
                retry_delay_seconds=1,
                lock_timeout_seconds=1,
                max_attempts=3,
                cleanup_done_hours=1,
            ),
        )

    def test_state_helpers(self) -> None:
        self.assertEqual(backfill_market._backfill_state_key("M1", 5), "backfill_last_ts:M1:5")
        self.assertEqual(backfill_market._parse_state_epoch("123"), 123)
        parsed_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch("src.jobs.backfill_market.parse_ts_iso", return_value=parsed_dt):
            self.assertEqual(
                backfill_market._parse_state_epoch("not-a-number"),
                int(parsed_dt.timestamp()),
            )
        with patch("src.jobs.backfill_market.parse_ts_iso", return_value=None):
            self.assertIsNone(backfill_market._parse_state_epoch("nope"))

    def test_market_is_closed_status_and_time(self) -> None:
        self.assertTrue(backfill_market._market_is_closed({"status": "Closed"}))
        with patch("src.jobs.backfill_market.parse_ts_iso", return_value=None):
            self.assertFalse(backfill_market._market_is_closed({"status": "open"}))
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        with patch("src.jobs.backfill_market.parse_ts_iso", return_value=past):
            self.assertTrue(backfill_market._market_is_closed({"status": "open", "close_time": "x"}))
        with patch("src.jobs.backfill_market.parse_ts_iso", return_value=future):
            self.assertFalse(backfill_market._market_is_closed({"status": "open", "close_time": "x"}))

    def test_now_s_returns_int(self) -> None:
        now_s = backfill_scan._now_s()
        self.assertIsInstance(now_s, int)

    def test_iter_events_handles_exceptions(self) -> None:
        def fake_iter_events(client, status, with_nested_markets, min_close_ts):
            if status == "closed":
                raise RuntimeError("boom")
            return [{"event_ticker": "EV1"}]

        with patch("src.jobs.backfill_scan.iter_events", side_effect=fake_iter_events):
            events = list(backfill_scan._iter_events(object(), ("closed", "settled"), 0))

        self.assertEqual(events, [({"event_ticker": "EV1"}, "settled")])

    def test_market_time_window_variants(self) -> None:
        now = datetime.now(timezone.utc)
        close_dt = now - timedelta(hours=1)
        start_s, end_s, is_closed = backfill_market._market_time_window(
            close_dt,
            None,
            lookback_hours=2,
        )
        self.assertTrue(is_closed)
        self.assertLess(start_s, end_s)

        future_open = now + timedelta(hours=1)
        start_s, end_s, is_closed = backfill_market._market_time_window(
            None,
            future_open,
            lookback_hours=1,
        )
        self.assertFalse(is_closed)
        self.assertEqual(start_s, end_s)

        start_s, end_s, is_closed = backfill_market._market_time_window(
            None,
            None,
            lookback_hours=1,
        )
        self.assertFalse(is_closed)
        self.assertGreaterEqual(end_s - start_s, 3590)
        self.assertLessEqual(end_s - start_s, 3610)

    def test_dt_to_epoch(self) -> None:
        self.assertIsNone(backfill_market._dt_to_epoch(None))
        naive_dt = datetime(2024, 1, 1, 0, 0, 0)
        expected = int(datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())
        self.assertEqual(backfill_market._dt_to_epoch(naive_dt), expected)

    def test_fetch_candle_bounds(self) -> None:
        min_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        max_dt = datetime(2024, 1, 2, tzinfo=timezone.utc)
        conn = FakeFetchConn(rows=[(min_dt,), (max_dt,)])
        min_s, max_s = backfill_market._fetch_candle_bounds(conn, "M1", 1)
        self.assertEqual(min_s, int(min_dt.timestamp()))
        self.assertEqual(max_s, int(max_dt.timestamp()))
        self.assertEqual(len(conn.executes), 2)

    def test_iter_time_chunks(self) -> None:
        self.assertEqual(list(backfill_market._iter_time_chunks(5, 5, 10)), [])
        self.assertEqual(list(backfill_market._iter_time_chunks(0, 10, 0)), [(0, 10)])
        self.assertEqual(
            list(backfill_market._iter_time_chunks(0, 10, 4)),
            [(0, 4), (4, 8), (8, 10)],
        )

    def test_build_backfill_ranges(self) -> None:
        self.assertEqual(
            backfill_market._build_backfill_ranges(
                backfill_market.BackfillRangeRequest(0, 10, None, None, 60, False),
            ),
            [(0, 10)],
        )
        self.assertEqual(
            backfill_market._build_backfill_ranges(
                backfill_market.BackfillRangeRequest(0, 10, 1, 2, 60, True),
            ),
            [(0, 10)],
        )
        self.assertEqual(
            backfill_market._build_backfill_ranges(
                backfill_market.BackfillRangeRequest(0, 10, 20, 30, 60, False),
            ),
            [(0, 10)],
        )
        self.assertEqual(
            backfill_market._build_backfill_ranges(
                backfill_market.BackfillRangeRequest(0, 100, 30, 70, 10, False),
            ),
            [(0, 30), (60, 100)],
        )
        self.assertEqual(
            backfill_market._build_backfill_ranges(
                backfill_market.BackfillRangeRequest(10, 10, None, None, 60, False),
            ),
            [],
        )

    def test_build_market_window_skips_when_closed_filled(self) -> None:
        cfg = self._cfg()
        ctx = backfill_market.MarketContext(
            "SR1",
            {"ticker": "M1", "status": "closed"},
            "hour",
        )
        conn = object()
        with patch("src.jobs.backfill_market._market_time_window", return_value=(0, 3600, True)), \
             patch("src.jobs.backfill_market._fetch_candle_bounds", return_value=(60, 3600)), \
             patch("src.jobs.backfill_market._load_backfill_cursor", return_value=120):
            window = backfill_market._build_market_backfill_window(
                conn,
                cfg,
                ctx,
                force_full=False,
            )
        self.assertIsNotNone(window)
        self.assertEqual(window.start_s, 3600)
        self.assertEqual(window.end_s, 3600)
        self.assertEqual(window.state_cursor_s, 120)
        self.assertEqual(window.last_cursor_s, 3600)

    def test_resolve_last_cursor_force_full(self) -> None:
        self.assertIsNone(backfill_market._resolve_last_cursor(10, 20, True))

    def test_store_cursor_if_needed_skips(self) -> None:
        window_missing = backfill_market.MarketBackfillWindow(
            market_ticker="M1",
            period_minutes=1,
            start_s=0,
            end_s=10,
            state_cursor_s=5,
            last_cursor_s=None,
            chunk_seconds=3600,
        )
        window_same = backfill_market.MarketBackfillWindow(
            market_ticker="M1",
            period_minutes=1,
            start_s=0,
            end_s=10,
            state_cursor_s=5,
            last_cursor_s=5,
            chunk_seconds=3600,
        )
        with patch("src.jobs.backfill_market._store_backfill_cursor") as store_cursor:
            backfill_market._store_cursor_if_needed(object(), window_missing)
            backfill_market._store_cursor_if_needed(object(), window_same)
        store_cursor.assert_not_called()

    def test_insert_candle_price_handling(self) -> None:
        cur = FakeCursor([])
        candle = {
            "end_period_ts": 1000,
            "price": "bad",
            "open_dollars": "10",
            "high": "0.2",
            "close": "1.2",
            "volume": 5,
            "open_interest": 2,
        }
        backfill_market._insert_candle(cur, "M1", 1, candle)
        _, params = cur.executes[0]
        self.assertEqual(params[0], "M1")
        self.assertEqual(params[1], 1)
        self.assertEqual(params[4], Decimal("0.1"))
        self.assertEqual(params[5], Decimal("0.2"))
        self.assertIsNone(params[6])
        self.assertEqual(params[7], Decimal("0.012"))

    def test_backfill_market_no_ticker(self) -> None:
        cfg = self._cfg()
        ctx = backfill_market.MarketContext("SR1", {}, "hour")
        self.assertEqual(backfill_market._backfill_market(object(), object(), cfg, ctx), 0)

    def test_backfill_market_store_cursor_on_no_work(self) -> None:
        cfg = self._cfg()
        ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        conn = object()
        with patch("src.jobs.backfill_market._market_time_window", return_value=(200, 100, False)), \
             patch("src.jobs.backfill_market._fetch_candle_bounds", return_value=(None, 100)), \
             patch("src.jobs.backfill_market._load_backfill_cursor", return_value=None), \
             patch("src.jobs.backfill_market._store_backfill_cursor") as store_cursor:
            result = backfill_market._backfill_market(conn, object(), cfg, ctx)
        self.assertEqual(result, 0)
        store_cursor.assert_called_once_with(conn, "M1", 1, 100)

    def test_backfill_market_inserts_and_updates_cursor(self) -> None:
        cfg = self._cfg()
        ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        conn = FakeConn()
        candles = {
            "candlesticks": [
                {"end_period_ts": 10, "price": {"open": "0.1"}},
                {"end_period_ts": 20, "price": {"open": "0.2"}},
            ]
        }
        with patch("src.jobs.backfill_market._market_time_window", return_value=(0, 100, False)), \
             patch("src.jobs.backfill_market._fetch_candle_bounds", return_value=(None, None)), \
             patch("src.jobs.backfill_market._load_backfill_cursor", return_value=None), \
             patch("src.jobs.backfill_market.get_market_candlesticks", return_value=candles), \
             patch("src.jobs.backfill_market._store_backfill_cursor") as store_cursor:
            inserted = backfill_market._backfill_market(conn, object(), cfg, ctx)
        self.assertEqual(inserted, 2)
        self.assertEqual(conn.commits, 1)
        store_cursor.assert_called_once_with(conn, "M1", 1, 20)

    def test_backfill_market_updates_cursor_max_seen(self) -> None:
        cfg = self._cfg()
        ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        conn = FakeConn()
        candles = {"candlesticks": [{"end_period_ts": 150, "price": {"open": "0.1"}}]}
        with patch("src.jobs.backfill_market._market_time_window", return_value=(0, 200, False)), \
             patch("src.jobs.backfill_market._fetch_candle_bounds", return_value=(None, None)), \
             patch("src.jobs.backfill_market._load_backfill_cursor", return_value=50), \
             patch("src.jobs.backfill_market.get_market_candlesticks", return_value=candles), \
             patch("src.jobs.backfill_market._store_backfill_cursor") as store_cursor:
            inserted = backfill_market._backfill_market(conn, object(), cfg, ctx)
        self.assertEqual(inserted, 1)
        store_cursor.assert_called_once_with(conn, "M1", 1, 150)

    def test_backfill_market_wrapper(self) -> None:
        cfg = self._cfg()
        ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        with patch("src.jobs.backfill_market._backfill_market", return_value=5) as backfill_market_mock:
            result = backfill_market.backfill_market(
                conn=object(),
                client=object(),
                cfg=cfg,
                context=ctx,
            )
        self.assertEqual(result, 5)
        backfill_market_mock.assert_called_once()

    def test_queue_backfill_market_publishes(self) -> None:
        queue_cfg = self._queue_cfg()
        ctx = backfill_market.MarketContext(
            "SR1",
            {
                "ticker": "M1",
                "open_time": "2024-01-01T00:00:00Z",
                "close_time": "2024-01-01T01:00:00Z",
            },
            "hour",
        )
        conn = object()

        class RecordingPublisher:
            def __init__(self) -> None:
                self.calls = []

            def publish(self, job_id, job_type=None):
                self.calls.append((job_id, job_type))

            def disable(self) -> None:
                raise AssertionError("disable should not be called")

        publisher = RecordingPublisher()
        with patch("src.jobs.backfill_market.enqueue_job", return_value=42) as enqueue_job:
            result = backfill_market._queue_backfill_market(conn, queue_cfg, publisher, ctx)

        self.assertEqual(result, 1)
        enqueue_job.assert_called_once()
        args, kwargs = enqueue_job.call_args
        self.assertEqual(args[0], conn)
        self.assertEqual(args[1], "backfill_market")
        self.assertEqual(
            args[2],
            {
                "series_ticker": "SR1",
                "strike_period": "hour",
                "market": {
                    "ticker": "M1",
                    "open_time": "2024-01-01T00:00:00Z",
                    "close_time": "2024-01-01T01:00:00Z",
                },
            },
        )
        self.assertEqual(kwargs["max_attempts"], queue_cfg.timing.max_attempts)
        self.assertEqual(publisher.calls, [(42, "backfill_market")])

    def test_queue_backfill_market_publish_failure_disables(self) -> None:
        queue_cfg = self._queue_cfg()
        ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")

        class FailingPublisher:
            def __init__(self) -> None:
                self.disabled = False
                self.calls = 0

            def publish(self, job_id, job_type=None):
                self.calls += 1
                raise RuntimeError("boom")

            def disable(self) -> None:
                self.disabled = True

        publisher = FailingPublisher()
        with patch("src.jobs.backfill_market.enqueue_job", return_value=7):
            result = backfill_market._queue_backfill_market(object(), queue_cfg, publisher, ctx)

        self.assertEqual(result, 1)
        self.assertEqual(publisher.calls, 1)
        self.assertTrue(publisher.disabled)

    def test_backfill_event_missing_series(self) -> None:
        cfg = self._cfg()
        event = {"event_ticker": "EV1", "markets": [{"ticker": "M1"}]}
        conn = object()
        ctx = backfill_scan.EventBackfillContext(event, "hour", None, None)
        with patch("src.jobs.backfill_scan.upsert_event") as upsert_event, \
             patch("src.jobs.backfill_scan.upsert_market") as upsert_market:
            result = backfill_scan._backfill_event(conn, object(), cfg, ctx)
        self.assertEqual(result, (0, 0, 0))
        upsert_event.assert_called_once_with(conn, event)
        upsert_market.assert_not_called()

    def test_backfill_event_queue_publishes(self) -> None:
        cfg = self._cfg()
        queue_cfg = backfill.QueueConfig(
            enabled=True,
            job_types=("backfill_market",),
            worker_id="worker",
            rabbitmq=work_queue.QueueRabbitMQConfig(
                url="amqp://",
                queue_name="q",
                prefetch=1,
                publish=True,
            ),
            timing=work_queue.QueueTimingConfig(
                poll_seconds=1,
                retry_delay_seconds=1,
                lock_timeout_seconds=1,
                max_attempts=3,
                cleanup_done_hours=1,
            ),
        )
        event = {
            "series_ticker": "SR1",
            "markets": [
                {
                    "ticker": "M1",
                    "open_time": "2024-01-01T00:00:00Z",
                    "close_time": "2024-01-01T01:00:00Z",
                }
            ],
        }

        class FailingPublisher:
            def __init__(self):
                self.disabled = False

            def publish(self, job_id, job_type=None):
                raise RuntimeError("boom")

            def disable(self):
                self.disabled = True

        publisher = FailingPublisher()
        ctx = backfill_scan.EventBackfillContext(event, "hour", queue_cfg, publisher)
        with patch("src.jobs.backfill_market.enqueue_job", return_value=7) as enqueue_job, \
             patch("src.jobs.backfill_scan.upsert_event") as upsert_event, \
             patch("src.jobs.backfill_scan.upsert_market") as upsert_market, \
             patch("src.jobs.backfill_scan._build_market_backfill_window") as build_window, \
             patch("src.jobs.backfill_scan._backfill_market") as backfill_market_mock:
            build_window.return_value = backfill_market.MarketBackfillWindow(
                market_ticker="M1",
                period_minutes=1,
                start_s=0,
                end_s=60,
                state_cursor_s=None,
                last_cursor_s=None,
                chunk_seconds=3600,
            )
            result = backfill_scan._backfill_event(conn=object(), client=object(), cfg=cfg, context=ctx)
        self.assertEqual(result, (1, 0, 1))
        self.assertEqual(enqueue_job.call_count, 1)
        self.assertEqual(upsert_event.call_count, 1)
        self.assertEqual(upsert_market.call_count, 1)
        backfill_market_mock.assert_not_called()
        self.assertTrue(publisher.disabled)

    def test_backfill_event_queue_skips_when_no_window(self) -> None:
        cfg = self._cfg()
        queue_cfg = backfill.QueueConfig(
            enabled=True,
            job_types=("backfill_market",),
            worker_id="worker",
            rabbitmq=work_queue.QueueRabbitMQConfig(
                url="amqp://",
                queue_name="q",
                prefetch=1,
                publish=True,
            ),
            timing=work_queue.QueueTimingConfig(
                poll_seconds=1,
                retry_delay_seconds=1,
                lock_timeout_seconds=1,
                max_attempts=3,
                cleanup_done_hours=1,
            ),
        )
        event = {
            "series_ticker": "SR1",
            "markets": [{"ticker": "M1"}],
        }
        ctx = backfill_scan.EventBackfillContext(event, "hour", queue_cfg, None)
        with patch("src.jobs.backfill_market.enqueue_job") as enqueue_job, \
             patch("src.jobs.backfill_scan.upsert_event") as upsert_event, \
             patch("src.jobs.backfill_scan.upsert_market") as upsert_market, \
             patch(
                 "src.jobs.backfill_scan._build_market_backfill_window",
                 return_value=backfill_market.MarketBackfillWindow(
                     market_ticker="M1",
                     period_minutes=1,
                     start_s=100,
                     end_s=100,
                     state_cursor_s=90,
                     last_cursor_s=90,
                     chunk_seconds=3600,
                 ),
             ):
            result = backfill_scan._backfill_event(conn=object(), client=object(), cfg=cfg, context=ctx)
        self.assertEqual(result, (1, 0, 0))
        enqueue_job.assert_not_called()
        self.assertEqual(upsert_event.call_count, 1)
        self.assertEqual(upsert_market.call_count, 1)


class TestBackfillPassBranches(unittest.TestCase):
    def _cfg(self) -> backfill.BackfillConfig:
        return backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )

    def _queue_cfg(self) -> backfill.QueueConfig:
        return backfill.QueueConfig(
            enabled=True,
            job_types=("backfill_market",),
            worker_id="worker",
            rabbitmq=work_queue.QueueRabbitMQConfig(
                url="amqp://",
                queue_name="q",
                prefetch=1,
                publish=True,
            ),
            timing=work_queue.QueueTimingConfig(
                poll_seconds=1,
                retry_delay_seconds=1,
                lock_timeout_seconds=1,
                max_attempts=3,
                cleanup_done_hours=1,
            ),
        )

    def test_backfill_pass_skips_unaccepted_events(self) -> None:
        cfg = self._cfg()
        events = [({"event_ticker": "EV1"}, "closed")]
        conn = object()
        with patch("src.jobs.backfill_scan._iter_events", return_value=events), \
             patch("src.jobs.backfill_scan.accept_event", return_value=None), \
             patch("src.jobs.backfill_scan._backfill_event") as backfill_event, \
             patch("src.jobs.backfill_scan.get_state", return_value="100"), \
             patch("src.jobs.backfill_scan.set_state") as set_state, \
             patch("src.jobs.backfill_scan._now_s", return_value=200):
            result = backfill_scan.backfill_pass(conn=conn, client=object(), cfg=cfg)
        self.assertEqual(result, (0, 0, 0))
        backfill_event.assert_not_called()
        set_state.assert_called_once_with(conn, "last_min_close_ts", "200")

    def test_backfill_pass_logs_queue_branch(self) -> None:
        cfg = self._cfg()
        queue_cfg = self._queue_cfg()
        conn = object()
        with patch("src.jobs.backfill_scan._iter_events", return_value=[]), \
             patch("src.jobs.backfill_scan.get_state", return_value="100"), \
             patch("src.jobs.backfill_scan.set_state") as set_state, \
             patch("src.jobs.backfill_scan._now_s", return_value=200), \
             patch("src.jobs.backfill_scan.logger.info") as logger_info:
            result = backfill_scan.backfill_pass(
                conn=conn,
                client=object(),
                cfg=cfg,
                queue_cfg=queue_cfg,
                publisher=None,
            )
        self.assertEqual(result, (0, 0, 0))
        set_state.assert_called_once_with(conn, "last_min_close_ts", "200")
        logger_info.assert_called_once()
        args, _ = logger_info.call_args
        self.assertIn("queued=%d", args[0])
        self.assertEqual(args[1:], (0, 0, 0, 200))


class TestBackfillScanCoverage(unittest.TestCase):
    def _queue_cfg(self) -> backfill.QueueConfig:
        return backfill.QueueConfig(
            enabled=True,
            job_types=("backfill_market",),
            worker_id="worker",
            rabbitmq=work_queue.QueueRabbitMQConfig(
                url="amqp://",
                queue_name="q",
                prefetch=1,
                publish=True,
            ),
            timing=work_queue.QueueTimingConfig(
                poll_seconds=1,
                retry_delay_seconds=1,
                lock_timeout_seconds=1,
                max_attempts=3,
                cleanup_done_hours=1,
            ),
        )

    def test_queue_pending_count(self) -> None:
        queue_cfg = self._queue_cfg()
        conn = FakeFetchConn(rows=[(5,)])
        pending = backfill_scan._queue_pending_count(conn, queue_cfg)
        self.assertEqual(pending, 5)
        self.assertIn("FROM work_queue", conn.executes[0][0])

    def test_allow_enqueue_pending_count_error(self) -> None:
        queue_cfg = self._queue_cfg()
        with patch("src.jobs.backfill_scan._queue_backpressure_limit", return_value=5), \
             patch("src.jobs.backfill_scan._queue_pending_count", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan.logger.exception") as log_exc:
            allow, pending, max_pending = backfill_scan._allow_enqueue(object(), queue_cfg)
        self.assertTrue(allow)
        self.assertEqual(pending, 0)
        self.assertEqual(max_pending, 5)
        log_exc.assert_called_once()

    def test_allow_enqueue_pending_count_limit(self) -> None:
        queue_cfg = self._queue_cfg()
        with patch("src.jobs.backfill_scan._queue_backpressure_limit", return_value=5), \
             patch("src.jobs.backfill_scan._queue_pending_count", return_value=4):
            allow, pending, max_pending = backfill_scan._allow_enqueue(object(), queue_cfg)
        self.assertTrue(allow)
        self.assertEqual(pending, 4)
        self.assertEqual(max_pending, 5)

        with patch("src.jobs.backfill_scan._queue_backpressure_limit", return_value=5), \
             patch("src.jobs.backfill_scan._queue_pending_count", return_value=6):
            allow, pending, max_pending = backfill_scan._allow_enqueue(object(), queue_cfg)
        self.assertFalse(allow)
        self.assertEqual(pending, 6)
        self.assertEqual(max_pending, 5)

    def test_upsert_event_or_skip_success(self) -> None:
        event = {"event_ticker": "EV1"}
        conn = object()
        with patch("src.jobs.backfill_scan.upsert_event") as upsert_event, \
             patch("src.jobs.backfill_scan._handle_event_upsert_error") as handler:
            result = backfill_scan._upsert_event_or_skip(conn, event)
        self.assertTrue(result)
        upsert_event.assert_called_once_with(conn, event)
        handler.assert_not_called()

    def test_upsert_event_or_skip_handles_error(self) -> None:
        event = {"event_ticker": "EV1"}
        with patch("src.jobs.backfill_scan.upsert_event", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan._handle_event_upsert_error", return_value=False) as handler:
            result = backfill_scan._upsert_event_or_skip(object(), event)
        self.assertFalse(result)
        handler.assert_called_once()

    def test_upsert_event_or_skip_raises_on_handler(self) -> None:
        event = {"event_ticker": "EV1"}
        with patch("src.jobs.backfill_scan.upsert_event", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan._handle_event_upsert_error", return_value=True):
            with self.assertRaises(RuntimeError):
                backfill_scan._upsert_event_or_skip(object(), event)

    def test_upsert_market_or_skip_handles_error(self) -> None:
        market = {"ticker": "M1"}
        with patch("src.jobs.backfill_scan.upsert_market", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan._handle_market_upsert_error", return_value=False) as handler:
            result = backfill_scan._upsert_market_or_skip(object(), "EV1", market)
        self.assertFalse(result)
        handler.assert_called_once()

    def test_upsert_market_or_skip_passes_event_ticker(self) -> None:
        conn = object()
        market = {"ticker": "M1"}
        with patch("src.jobs.backfill_scan.upsert_market", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan.maybe_upsert_active_market_from_market") as maybe_active, \
             patch("src.jobs.backfill_scan._handle_market_upsert_error", return_value=False) as handler:
            result = backfill_scan._upsert_market_or_skip(conn, "EV1", market)
        self.assertFalse(result)
        maybe_active.assert_not_called()
        _, kwargs = handler.call_args
        self.assertEqual(kwargs["event_ticker"], "EV1")
        self.assertEqual(kwargs["market_ticker"], "M1")

    def test_upsert_market_or_skip_raises_on_handler(self) -> None:
        market = {"ticker": "M1"}
        with patch("src.jobs.backfill_scan.upsert_market", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan._handle_market_upsert_error", return_value=True):
            with self.assertRaises(RuntimeError):
                backfill_scan._upsert_market_or_skip(object(), "EV1", market)

    def test_should_enqueue_market_on_window_error(self) -> None:
        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )
        market_ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        with patch("src.jobs.backfill_scan._build_market_backfill_window", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan._is_connection_error", return_value=False), \
             patch("src.jobs.backfill_scan.logger.exception") as log_exc:
            allowed = backfill_scan._should_enqueue_market(object(), cfg, market_ctx)
        self.assertTrue(allowed)
        log_exc.assert_called_once()

    def test_should_enqueue_market_returns_true_for_window(self) -> None:
        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )
        market_ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        window = backfill_market.MarketBackfillWindow(
            market_ticker="M1",
            period_minutes=1,
            start_s=0,
            end_s=10,
            state_cursor_s=None,
            last_cursor_s=None,
            chunk_seconds=3600,
        )
        with patch("src.jobs.backfill_scan._build_market_backfill_window", return_value=window):
            allowed = backfill_scan._should_enqueue_market(object(), cfg, market_ctx)
        self.assertTrue(allowed)

    def test_should_enqueue_market_returns_false_for_empty_window(self) -> None:
        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )
        market_ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        with patch("src.jobs.backfill_scan._build_market_backfill_window", return_value=None):
            allowed = backfill_scan._should_enqueue_market(object(), cfg, market_ctx)
        self.assertFalse(allowed)

    def test_should_enqueue_market_raises_on_connection_error(self) -> None:
        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )
        market_ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        with patch("src.jobs.backfill_scan._build_market_backfill_window", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan._is_connection_error", return_value=True):
            with self.assertRaises(RuntimeError):
                backfill_scan._should_enqueue_market(object(), cfg, market_ctx)

    def test_enqueue_market_backfill_handles_error(self) -> None:
        event_ctx = backfill_scan.EventBackfillContext(
            event={"event_ticker": "EV1"},
            strike_period="hour",
            queue_cfg=None,
            publisher=None,
        )
        market_ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        with patch("src.jobs.backfill_scan._queue_backfill_market", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan._is_connection_error", return_value=False), \
             patch("src.jobs.backfill_scan._log_metric") as log_metric, \
             patch("src.jobs.backfill_scan._safe_rollback") as safe_rollback:
            result = backfill_scan._enqueue_market_backfill(object(), event_ctx, market_ctx)
        self.assertEqual(result, 0)
        log_metric.assert_called_once()
        safe_rollback.assert_called_once()

    def test_enqueue_market_backfill_raises_on_connection_error(self) -> None:
        event_ctx = backfill_scan.EventBackfillContext(
            event={"event_ticker": "EV1"},
            strike_period="hour",
            queue_cfg=None,
            publisher=None,
        )
        market_ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        with patch("src.jobs.backfill_scan._queue_backfill_market", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan._is_connection_error", return_value=True), \
             patch("src.jobs.backfill_scan._safe_rollback") as safe_rollback:
            with self.assertRaises(RuntimeError):
                backfill_scan._enqueue_market_backfill(object(), event_ctx, market_ctx)
        safe_rollback.assert_called_once()

    def test_backfill_market_with_errors_handles_error(self) -> None:
        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )
        event_ctx = backfill_scan.EventBackfillContext(
            event={"event_ticker": "EV1"},
            strike_period="hour",
            queue_cfg=None,
            publisher=None,
        )
        market_ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        with patch("src.jobs.backfill_scan._backfill_market", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan._is_connection_error", return_value=False), \
             patch("src.jobs.backfill_scan._log_metric") as log_metric, \
             patch("src.jobs.backfill_scan._safe_rollback") as safe_rollback:
            result = backfill_scan._backfill_market_with_errors(
                object(),
                object(),
                cfg,
                event_ctx,
                market_ctx,
            )
        self.assertEqual(result, 0)
        log_metric.assert_called_once()
        safe_rollback.assert_called_once()

    def test_backfill_market_with_errors_raises_on_connection_error(self) -> None:
        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )
        event_ctx = backfill_scan.EventBackfillContext(
            event={"event_ticker": "EV1"},
            strike_period="hour",
            queue_cfg=None,
            publisher=None,
        )
        market_ctx = backfill_market.MarketContext("SR1", {"ticker": "M1"}, "hour")
        with patch("src.jobs.backfill_scan._backfill_market", side_effect=RuntimeError("boom")), \
             patch("src.jobs.backfill_scan._is_connection_error", return_value=True):
            with self.assertRaises(RuntimeError):
                backfill_scan._backfill_market_with_errors(
                    object(),
                    object(),
                    cfg,
                    event_ctx,
                    market_ctx,
                )

    def test_backfill_event_skips_on_event_upsert_failure(self) -> None:
        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )
        ctx = backfill_scan.EventBackfillContext(
            event={"event_ticker": "EV1", "series_ticker": "SR1"},
            strike_period="hour",
            queue_cfg=None,
            publisher=None,
        )
        with patch("src.jobs.backfill_scan._upsert_event_or_skip", return_value=False), \
             patch("src.jobs.backfill_scan._upsert_market_or_skip") as upsert_market:
            result = backfill_scan._backfill_event(object(), object(), cfg, ctx)
        self.assertEqual(result, (0, 0, 0))
        upsert_market.assert_not_called()

    def test_backfill_event_queue_enqueues_markets(self) -> None:
        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )
        queue_cfg = self._queue_cfg()
        event = {
            "event_ticker": "EV1",
            "series_ticker": "SR1",
            "markets": [{"ticker": "M1"}],
        }
        ctx = backfill_scan.EventBackfillContext(
            event=event,
            strike_period="hour",
            queue_cfg=queue_cfg,
            publisher=None,
            allow_enqueue=True,
        )
        with patch("src.jobs.backfill_scan._upsert_event_or_skip", return_value=True), \
             patch("src.jobs.backfill_scan._upsert_market_or_skip", return_value=True), \
             patch("src.jobs.backfill_scan._should_enqueue_market", return_value=True), \
             patch("src.jobs.backfill_scan._enqueue_market_backfill", return_value=2) as enqueue_market, \
             patch("src.jobs.backfill_scan._backfill_market_with_errors") as backfill_market_mock:
            result = backfill_scan._backfill_event(object(), object(), cfg, ctx)
        self.assertEqual(result, (1, 0, 2))
        enqueue_market.assert_called_once()
        backfill_market_mock.assert_not_called()

    def test_backfill_pass_backpressure_keeps_cursor(self) -> None:
        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )
        queue_cfg = self._queue_cfg()
        counts = backfill_scan.BackfillCounts()
        stats = backfill_scan.EventScanStats()
        conn = object()
        with patch("src.jobs.backfill_scan._allow_enqueue", return_value=(False, 10, 5)), \
             patch("src.jobs.backfill_scan._load_min_close_ts", return_value=900), \
             patch("src.jobs.backfill_scan._now_s", return_value=1000), \
             patch("src.jobs.backfill_scan._scan_backfill_events", return_value=(counts, stats)), \
             patch("src.jobs.backfill_scan.set_state") as set_state, \
             patch("src.jobs.backfill_scan.logger.warning") as log_warn:
            result = backfill_scan.backfill_pass(
                conn=conn,
                client=object(),
                cfg=cfg,
                queue_cfg=queue_cfg,
            )
        self.assertEqual(result, (0, 0, 0))
        set_state.assert_called_once_with(conn, "last_min_close_ts", "900")
        log_warn.assert_called_once()

    def test_scan_backfill_events_raises_on_closed_conn(self) -> None:
        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )

        class ClosedConn:
            closed = True

        scan_ctx = backfill_scan.BackfillScanContext(
            conn=ClosedConn(),
            client=object(),
            cfg=cfg,
            queue_cfg=None,
            publisher=None,
            last_min_close_ts=0,
            allow_enqueue=True,
        )
        events = [({"event_ticker": "EV1"}, "closed")]
        with patch("src.jobs.backfill_scan._iter_events", return_value=events):
            with self.assertRaises(backfill_scan.psycopg.OperationalError):
                backfill_scan._scan_backfill_events(scan_ctx)

    def test_scan_backfill_events_calls_backfill_event(self) -> None:
        cfg = backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )
        scan_ctx = backfill_scan.BackfillScanContext(
            conn=object(),
            client=object(),
            cfg=cfg,
            queue_cfg=None,
            publisher=None,
            last_min_close_ts=0,
            allow_enqueue=True,
        )
        event = {"event_ticker": "EV1", "series_ticker": "SR1"}
        with patch("src.jobs.backfill_scan._iter_events", return_value=[(event, "closed")]), \
             patch("src.jobs.backfill_scan.accept_event", return_value="hour"), \
             patch("src.jobs.backfill_scan._backfill_event", return_value=(1, 2, 3)) as backfill_event:
            counts, stats = backfill_scan._scan_backfill_events(scan_ctx)
        self.assertEqual(counts.events, 1)
        self.assertEqual(counts.markets, 1)
        self.assertEqual(counts.candles, 2)
        self.assertEqual(counts.queued, 3)
        self.assertIsInstance(stats, backfill_scan.EventScanStats)
        self.assertEqual(backfill_event.call_count, 1)
        event_ctx = backfill_event.call_args[0][3]
        self.assertTrue(event_ctx.allow_enqueue)
