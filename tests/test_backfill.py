import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from _test_utils import add_src_to_path, ensure_kalshi_sdk_stub, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()
ensure_kalshi_sdk_stub()

import src.jobs.backfill as backfill
import src.queue.work_queue as work_queue


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

        with patch("src.jobs.backfill.iter_events", side_effect=fake_iter_events), \
             patch("src.jobs.backfill.get_market_candlesticks", return_value=candles) as get_candles, \
             patch("src.jobs.backfill._fetch_candle_bounds", return_value=(None, None)), \
             patch("src.jobs.backfill.get_state", side_effect=fake_get_state), \
             patch("src.jobs.backfill.set_state") as set_state, \
             patch("src.jobs.backfill.upsert_event") as upsert_event, \
             patch("src.jobs.backfill.upsert_market") as upsert_market, \
             patch("src.jobs.backfill._now_s", return_value=1_700_000_000):
            cfg = backfill.BackfillConfig(
                strike_periods=("hour",),
                event_statuses=("closed",),
                minutes_hour=1,
                minutes_day=60,
                lookback_hours=2,
            )
            result = backfill.backfill_pass(conn=conn, client=object(), cfg=cfg)

        self.assertEqual(result, (1, 1, 1))
        self.assertEqual(upsert_event.call_count, 1)
        self.assertEqual(upsert_market.call_count, 1)
        self.assertEqual(len(conn.executes), 1)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(set_state.call_count, 2)
        set_state.assert_any_call(conn, "backfill_last_ts:M1:1", "1704070800")
        set_state.assert_any_call(conn, "last_min_close_ts", "1700000000")
        self.assertGreaterEqual(get_candles.call_count, 1)
        for _, kwargs in get_candles.call_args_list:
            self.assertEqual(kwargs["period_interval_minutes"], 1)

    def test_interval_minutes_for_strike(self) -> None:
        self.assertEqual(backfill._interval_minutes_for_strike("hour", 1, 60), 1)
        self.assertEqual(backfill._interval_minutes_for_strike("day", 1, 60), 60)


class TestBackfillHelpers(unittest.TestCase):
    def _cfg(self) -> backfill.BackfillConfig:
        return backfill.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=1,
        )

    def test_state_helpers(self) -> None:
        self.assertEqual(backfill._backfill_state_key("M1", 5), "backfill_last_ts:M1:5")
        self.assertEqual(backfill._parse_state_epoch("123"), 123)
        parsed_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch("src.jobs.backfill.parse_ts_iso", return_value=parsed_dt):
            self.assertEqual(
                backfill._parse_state_epoch("not-a-number"),
                int(parsed_dt.timestamp()),
            )
        with patch("src.jobs.backfill.parse_ts_iso", return_value=None):
            self.assertIsNone(backfill._parse_state_epoch("nope"))

    def test_now_s_returns_int(self) -> None:
        now_s = backfill._now_s()
        self.assertIsInstance(now_s, int)

    def test_iter_events_handles_exceptions(self) -> None:
        def fake_iter_events(client, status, with_nested_markets, min_close_ts):
            if status == "closed":
                raise RuntimeError("boom")
            return [{"event_ticker": "EV1"}]

        with patch("src.jobs.backfill.iter_events", side_effect=fake_iter_events):
            events = list(backfill._iter_events(object(), ("closed", "settled"), 0))

        self.assertEqual(events, [({"event_ticker": "EV1"}, "settled")])

    def test_market_time_window_variants(self) -> None:
        now = datetime.now(timezone.utc)
        close_dt = now - timedelta(hours=1)
        start_s, end_s, is_closed = backfill._market_time_window(
            close_dt,
            None,
            lookback_hours=2,
        )
        self.assertTrue(is_closed)
        self.assertLess(start_s, end_s)

        future_open = now + timedelta(hours=1)
        start_s, end_s, is_closed = backfill._market_time_window(
            None,
            future_open,
            lookback_hours=1,
        )
        self.assertFalse(is_closed)
        self.assertEqual(start_s, end_s)

        start_s, end_s, is_closed = backfill._market_time_window(
            None,
            None,
            lookback_hours=1,
        )
        self.assertFalse(is_closed)
        self.assertGreaterEqual(end_s - start_s, 3590)
        self.assertLessEqual(end_s - start_s, 3610)

    def test_dt_to_epoch(self) -> None:
        self.assertIsNone(backfill._dt_to_epoch(None))
        naive_dt = datetime(2024, 1, 1, 0, 0, 0)
        expected = int(datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())
        self.assertEqual(backfill._dt_to_epoch(naive_dt), expected)

    def test_fetch_candle_bounds(self) -> None:
        min_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        max_dt = datetime(2024, 1, 2, tzinfo=timezone.utc)
        conn = FakeFetchConn(rows=[(min_dt,), (max_dt,)])
        min_s, max_s = backfill._fetch_candle_bounds(conn, "M1", 1)
        self.assertEqual(min_s, int(min_dt.timestamp()))
        self.assertEqual(max_s, int(max_dt.timestamp()))
        self.assertEqual(len(conn.executes), 2)

    def test_iter_time_chunks(self) -> None:
        self.assertEqual(list(backfill._iter_time_chunks(5, 5, 10)), [])
        self.assertEqual(list(backfill._iter_time_chunks(0, 10, 0)), [(0, 10)])
        self.assertEqual(
            list(backfill._iter_time_chunks(0, 10, 4)),
            [(0, 4), (4, 8), (8, 10)],
        )

    def test_build_backfill_ranges(self) -> None:
        self.assertEqual(
            backfill._build_backfill_ranges(
                backfill.BackfillRangeRequest(0, 10, None, None, 60, False),
            ),
            [(0, 10)],
        )
        self.assertEqual(
            backfill._build_backfill_ranges(
                backfill.BackfillRangeRequest(0, 10, 1, 2, 60, True),
            ),
            [(0, 10)],
        )
        self.assertEqual(
            backfill._build_backfill_ranges(
                backfill.BackfillRangeRequest(0, 10, 20, 30, 60, False),
            ),
            [(0, 10)],
        )
        self.assertEqual(
            backfill._build_backfill_ranges(
                backfill.BackfillRangeRequest(0, 100, 30, 70, 10, False),
            ),
            [(0, 30), (60, 100)],
        )
        self.assertEqual(
            backfill._build_backfill_ranges(
                backfill.BackfillRangeRequest(10, 10, None, None, 60, False),
            ),
            [],
        )

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
        backfill._insert_candle(cur, "M1", 1, candle)
        _, params = cur.executes[0]
        self.assertEqual(params[0], "M1")
        self.assertEqual(params[1], 1)
        self.assertEqual(params[4], Decimal("0.1"))
        self.assertEqual(params[5], Decimal("0.2"))
        self.assertIsNone(params[6])
        self.assertEqual(params[7], Decimal("0.012"))

    def test_backfill_market_no_ticker(self) -> None:
        cfg = self._cfg()
        ctx = backfill.MarketContext("SR1", {}, "hour")
        self.assertEqual(backfill._backfill_market(object(), object(), cfg, ctx), 0)

    def test_backfill_market_store_cursor_on_no_work(self) -> None:
        cfg = self._cfg()
        ctx = backfill.MarketContext("SR1", {"ticker": "M1"}, "hour")
        conn = object()
        with patch("src.jobs.backfill._market_time_window", return_value=(200, 100, False)), \
             patch("src.jobs.backfill._fetch_candle_bounds", return_value=(None, 100)), \
             patch("src.jobs.backfill._load_backfill_cursor", return_value=None), \
             patch("src.jobs.backfill._store_backfill_cursor") as store_cursor:
            result = backfill._backfill_market(conn, object(), cfg, ctx)
        self.assertEqual(result, 0)
        store_cursor.assert_called_once_with(conn, "M1", 1, 100)

    def test_backfill_market_inserts_and_updates_cursor(self) -> None:
        cfg = self._cfg()
        ctx = backfill.MarketContext("SR1", {"ticker": "M1"}, "hour")
        conn = FakeConn()
        candles = {
            "candlesticks": [
                {"end_period_ts": 10, "price": {"open": "0.1"}},
                {"end_period_ts": 20, "price": {"open": "0.2"}},
            ]
        }
        with patch("src.jobs.backfill._market_time_window", return_value=(0, 100, False)), \
             patch("src.jobs.backfill._fetch_candle_bounds", return_value=(None, None)), \
             patch("src.jobs.backfill._load_backfill_cursor", return_value=None), \
             patch("src.jobs.backfill.get_market_candlesticks", return_value=candles), \
             patch("src.jobs.backfill._store_backfill_cursor") as store_cursor:
            inserted = backfill._backfill_market(conn, object(), cfg, ctx)
        self.assertEqual(inserted, 2)
        self.assertEqual(conn.commits, 1)
        store_cursor.assert_called_once_with(conn, "M1", 1, 20)

    def test_backfill_market_updates_cursor_max_seen(self) -> None:
        cfg = self._cfg()
        ctx = backfill.MarketContext("SR1", {"ticker": "M1"}, "hour")
        conn = FakeConn()
        candles = {"candlesticks": [{"end_period_ts": 150, "price": {"open": "0.1"}}]}
        with patch("src.jobs.backfill._market_time_window", return_value=(0, 200, False)), \
             patch("src.jobs.backfill._fetch_candle_bounds", return_value=(None, None)), \
             patch("src.jobs.backfill._load_backfill_cursor", return_value=50), \
             patch("src.jobs.backfill.get_market_candlesticks", return_value=candles), \
             patch("src.jobs.backfill._store_backfill_cursor") as store_cursor:
            inserted = backfill._backfill_market(conn, object(), cfg, ctx)
        self.assertEqual(inserted, 1)
        store_cursor.assert_called_once_with(conn, "M1", 1, 150)

    def test_backfill_market_wrapper(self) -> None:
        cfg = self._cfg()
        ctx = backfill.MarketContext("SR1", {"ticker": "M1"}, "hour")
        with patch("src.jobs.backfill._backfill_market", return_value=5) as backfill_market:
            result = backfill.backfill_market(
                conn=object(),
                client=object(),
                cfg=cfg,
                context=ctx,
            )
        self.assertEqual(result, 5)
        backfill_market.assert_called_once()

    def test_backfill_event_missing_series(self) -> None:
        cfg = self._cfg()
        event = {"event_ticker": "EV1", "markets": [{"ticker": "M1"}]}
        conn = object()
        ctx = backfill.EventBackfillContext(event, "hour", None, None)
        with patch("src.jobs.backfill.upsert_event") as upsert_event, \
             patch("src.jobs.backfill.upsert_market") as upsert_market:
            result = backfill._backfill_event(conn, object(), cfg, ctx)
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
        ctx = backfill.EventBackfillContext(event, "hour", queue_cfg, publisher)
        with patch("src.jobs.backfill.enqueue_job", return_value=7) as enqueue_job, \
             patch("src.jobs.backfill.upsert_event") as upsert_event, \
             patch("src.jobs.backfill.upsert_market") as upsert_market, \
             patch("src.jobs.backfill._build_market_backfill_window") as build_window, \
             patch("src.jobs.backfill._backfill_market") as backfill_market:
            build_window.return_value = backfill.MarketBackfillWindow(
                market_ticker="M1",
                period_minutes=1,
                start_s=0,
                end_s=60,
                state_cursor_s=None,
                last_cursor_s=None,
                chunk_seconds=3600,
            )
            result = backfill._backfill_event(conn=object(), client=object(), cfg=cfg, context=ctx)
        self.assertEqual(result, (1, 0, 1))
        self.assertEqual(enqueue_job.call_count, 1)
        self.assertEqual(upsert_event.call_count, 1)
        self.assertEqual(upsert_market.call_count, 1)
        backfill_market.assert_not_called()
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
        ctx = backfill.EventBackfillContext(event, "hour", queue_cfg, None)
        with patch("src.jobs.backfill.enqueue_job") as enqueue_job, \
             patch("src.jobs.backfill.upsert_event") as upsert_event, \
             patch("src.jobs.backfill.upsert_market") as upsert_market, \
             patch(
                 "src.jobs.backfill._build_market_backfill_window",
                 return_value=backfill.MarketBackfillWindow(
                     market_ticker="M1",
                     period_minutes=1,
                     start_s=100,
                     end_s=100,
                     state_cursor_s=90,
                     last_cursor_s=90,
                     chunk_seconds=3600,
                 ),
             ):
            result = backfill._backfill_event(conn=object(), client=object(), cfg=cfg, context=ctx)
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
        with patch("src.jobs.backfill._iter_events", return_value=events), \
             patch("src.jobs.backfill.accept_event", return_value=None), \
             patch("src.jobs.backfill._backfill_event") as backfill_event, \
             patch("src.jobs.backfill.get_state", return_value="100"), \
             patch("src.jobs.backfill.set_state") as set_state, \
             patch("src.jobs.backfill._now_s", return_value=200):
            result = backfill.backfill_pass(conn=conn, client=object(), cfg=cfg)
        self.assertEqual(result, (0, 0, 0))
        backfill_event.assert_not_called()
        set_state.assert_called_once_with(conn, "last_min_close_ts", "200")

    def test_backfill_pass_logs_queue_branch(self) -> None:
        cfg = self._cfg()
        queue_cfg = self._queue_cfg()
        conn = object()
        with patch("src.jobs.backfill._iter_events", return_value=[]), \
             patch("src.jobs.backfill.get_state", return_value="100"), \
             patch("src.jobs.backfill.set_state") as set_state, \
             patch("src.jobs.backfill._now_s", return_value=200), \
             patch("src.jobs.backfill.logger.info") as logger_info:
            result = backfill.backfill_pass(
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
