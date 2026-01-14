import asyncio
import json
import os
import queue
import threading
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

import src.ingest.ws.ws_ingest as ws_ingest
import src.ingest.ws.ws_ingest_config as ws_ingest_config
import src.ingest.ws.ws_ingest_protocol as ws_ingest_protocol
import src.ingest.ws.ws_ingest_subscriptions as ws_ingest_subscriptions
import src.ingest.ws.ws_ingest_utils as ws_ingest_utils
import src.ingest.ws.ws_ingest_writer as ws_ingest_writer
import src.ingest.ws.ws_ingest_models as ws_ingest_models


class StopLoop(Exception):
    pass


class FakeConn:
    def __init__(self, dsn=None, rollback_raises=False):
        self.info = SimpleNamespace(dsn=dsn)
        self.rollback_raises = rollback_raises
        self.closed = False

    def rollback(self):
        if self.rollback_raises:
            raise RuntimeError("rollback failed")

    def close(self):
        self.closed = True


class FakeWS:
    def __init__(self, messages=None):
        self.messages = list(messages or [])
        self.sent = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)

    async def send(self, msg):
        self.sent.append(msg)


class FakeThread:
    def __init__(self, target=None, args=(), daemon=False):
        self.target = target
        self.args = args
        self.daemon = daemon
        self.started = False
        self.joined = False
        self.join_timeout = None

    def start(self):
        self.started = True

    def join(self, timeout=None):
        self.joined = True
        self.join_timeout = timeout


class ToggleEvent:
    def __init__(self):
        self.calls = 0

    def is_set(self):
        self.calls += 1
        return self.calls > 1

    def set(self):
        return None


class TestWsIngestHelpers(unittest.TestCase):
    def test_fast_json_loads_orjson(self) -> None:
        seen = {}

        def loads(raw):
            seen["raw"] = raw
            return {"ok": True}

        with patch.object(ws_ingest, "_load_orjson", return_value=SimpleNamespace(loads=loads)):
            result = ws_ingest._fast_json_loads('{"a": 1}')
        self.assertEqual(result, {"ok": True})
        self.assertIsInstance(seen["raw"], (bytes, bytearray))

    def test_fast_json_loads_json(self) -> None:
        with patch.object(ws_ingest, "_load_orjson", return_value=None):
            result = ws_ingest._fast_json_loads('{"a": 1}')
        self.assertEqual(result, {"a": 1})

    def test_env_helpers(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(ws_ingest_config._env_int("X", 5), 5)
            self.assertEqual(ws_ingest_config._env_float("X", 1.5), 1.5)
            self.assertFalse(ws_ingest_config._bool_env("X", False))
        with patch.dict(os.environ, {"X": "bad"}):
            self.assertEqual(ws_ingest_config._env_int("X", 5), 5)
            self.assertEqual(ws_ingest_config._env_float("X", 1.5), 1.5)
        with patch.dict(os.environ, {"X": "0"}):
            self.assertEqual(ws_ingest_config._env_int("X", 5, minimum=1), 5)
        with patch.dict(os.environ, {"X": "0.05"}):
            self.assertEqual(ws_ingest_config._env_float("X", 1.5, minimum=0.1), 1.5)
        with patch.dict(os.environ, {"X": "yes"}):
            self.assertTrue(ws_ingest_config._bool_env("X", False))

    def test_parse_csv(self) -> None:
        self.assertEqual(ws_ingest_config._parse_csv("a, b,,c"), ("a", "b", "c"))

    def test_resolve_ws_url(self) -> None:
        with patch.dict(os.environ, {"KALSHI_WS_URL": "wss://override"}):
            self.assertEqual(ws_ingest_config._resolve_ws_url(None), "wss://override")
        with patch.dict(os.environ, {"KALSHI_WS_URL": ""}, clear=True):
            url = ws_ingest_config._resolve_ws_url("api.elections.kalshi.com/trade-api/v2")
        self.assertEqual(
            url,
            "wss://api.elections.kalshi.com/trade-api/ws/v2",
        )

    def test_ws_signature_payload(self) -> None:
        self.assertEqual(ws_ingest_protocol._ws_signature_payload(123, "/path"), b"123GET/path")

    def test_build_ws_headers(self) -> None:
        class FakeKey:
            def __init__(self):
                self.message = None

            def sign(self, message, *_args, **_kwargs):
                self.message = message
                return b"sig"

        key = FakeKey()
        with patch("src.ingest.ws.ws_ingest_protocol.serialization.load_pem_private_key", return_value=key), \
             patch("src.ingest.ws.ws_ingest_protocol.time.time", return_value=1.0):
            headers = ws_ingest._build_ws_headers("key", "pem", "wss://host/path")
        self.assertEqual(headers["KALSHI-ACCESS-KEY"], "key")
        self.assertEqual(headers["KALSHI-ACCESS-SIGNATURE"], "c2ln")
        self.assertEqual(headers["KALSHI-ACCESS-TIMESTAMP"], "1000")
        self.assertEqual(key.message, b"1000GET/path")

    def test_chunked(self) -> None:
        chunks = ws_ingest_protocol._chunked(["a", "b", "c"], 2)
        self.assertEqual(chunks, [["a", "b"], ["c"]])

    def test_coerce_and_to_cents(self) -> None:
        self.assertIsNone(ws_ingest._coerce_int(True))
        self.assertEqual(ws_ingest._coerce_int("2"), 2)
        self.assertIsNone(ws_ingest._coerce_int("bad"))
        self.assertEqual(ws_ingest_utils._to_cents(5), 5)
        self.assertEqual(ws_ingest_utils._to_cents(0.5), 50)
        self.assertEqual(ws_ingest_utils._to_cents(1.5), 2)
        self.assertEqual(ws_ingest_utils._to_cents("0.25"), 25)
        self.assertEqual(ws_ingest_utils._to_cents("10"), 10)
        self.assertEqual(ws_ingest_utils._to_cents(Decimal("1.2")), 1)
        self.assertIsNone(ws_ingest_utils._to_cents(""))
        self.assertIsNone(ws_ingest_utils._to_cents("bad"))

    def test_dollars_from_cents(self) -> None:
        self.assertEqual(ws_ingest_utils._dollars_from_cents(25), Decimal("0.25"))
        self.assertIsNone(ws_ingest_utils._dollars_from_cents(None))

    def test_parse_ts(self) -> None:
        self.assertIsNone(ws_ingest_utils._parse_ts(None))
        naive = datetime(2024, 1, 1, 0, 0, 0)
        parsed = ws_ingest_utils._parse_ts(naive)
        self.assertEqual(parsed.tzinfo, timezone.utc)
        parsed = ws_ingest_utils._parse_ts(1_704_067_200_000)
        self.assertEqual(parsed, datetime(2024, 1, 1, tzinfo=timezone.utc))
        parsed = ws_ingest_utils._parse_ts("1704067200")
        self.assertEqual(parsed, datetime(2024, 1, 1, tzinfo=timezone.utc))
        self.assertIsNone(ws_ingest_utils._parse_ts("  "))
        self.assertIsNone(ws_ingest_utils._parse_ts(object()))
        with patch("src.ingest.ws.ws_ingest_utils.parse_ts_iso", side_effect=Exception("fail")):
            self.assertIsNone(ws_ingest_utils._parse_ts("bad"))

    def test_extract_helpers(self) -> None:
        self.assertEqual(ws_ingest_utils._extract_channel({"msg": {"type": "ticker"}}), "ticker")
        self.assertEqual(ws_ingest_utils._extract_payload({"data": {"a": 1}}), {"a": 1})
        self.assertIsNone(ws_ingest_utils._extract_market_id({"market": {}}))
        market_id_map = {"1": "M1"}
        payload = {"market_id": "1"}
        self.assertEqual(ws_ingest_utils._resolve_market_ticker(payload, market_id_map), "M1")
        self.assertIsNone(ws_ingest_utils._resolve_market_ticker({}, market_id_map))

    def test_normalize_tick_missing_ticker(self) -> None:
        self.assertIsNone(ws_ingest_utils._normalize_tick({"price": 10}))

    def test_normalize_tick_price_fallbacks(self) -> None:
        payload = {
            "market_ticker": "M1",
            "ts": 1_700_000_000,
            "yes_bid": 40,
            "yes_ask": 60,
        }
        tick = ws_ingest_utils._normalize_tick(payload)
        self.assertEqual(tick["price_dollars"], Decimal("0.5"))

        payload = {
            "market_ticker": "M1",
            "ts": 1_700_000_000,
            "yes_bid": 40,
        }
        tick = ws_ingest_utils._normalize_tick(payload)
        self.assertEqual(tick["price_dollars"], Decimal("0.4"))

        payload = {
            "market_ticker": "M1",
            "ts": 1_700_000_000,
            "yes_ask": 70,
        }
        tick = ws_ingest_utils._normalize_tick(payload)
        self.assertEqual(tick["price_dollars"], Decimal("0.7"))

    def test_normalize_lifecycle(self) -> None:
        payload = {"market_ticker": "M1", "status": "open", "open_time": "1"}
        lifecycle = ws_ingest_utils._normalize_lifecycle(payload)
        self.assertEqual(lifecycle["market_ticker"], "M1")
        self.assertEqual(lifecycle["event_type"], "open")

    def test_terminal_lifecycle(self) -> None:
        self.assertFalse(ws_ingest_utils._is_terminal_lifecycle(None))
        self.assertTrue(ws_ingest_utils._is_terminal_lifecycle("Settled"))

    def test_build_messages(self) -> None:
        msg = ws_ingest_protocol._build_subscribe_message(1, ("ticker",), ["M1"])
        self.assertEqual(msg["params"]["market_tickers"], ["M1"])

        with patch.dict(os.environ, {"KALSHI_WS_UPDATE_STYLE": "legacy", "KALSHI_WS_UPDATE_INCLUDE_CHANNELS": "1"}):
            msg = ws_ingest_subscriptions._build_update_message(1, ("ticker",), ["A"], ["B"])
        self.assertIn("market_tickers", msg["params"])
        self.assertIn("channels", msg["params"])

        with patch.dict(os.environ, {"KALSHI_WS_UPDATE_STYLE": "markets"}, clear=True):
            msg = ws_ingest_subscriptions._build_update_message(1, ("ticker",), ["A"], ["B"])
        self.assertIn("add_markets", msg["params"])
        self.assertIn("delete_markets", msg["params"])

        with patch.dict(os.environ, {"KALSHI_WS_SUBSCRIPTION_ID_FIELD": "subscription_id"}, clear=True):
            msg = ws_ingest_subscriptions._build_update_message(
                1,
                ("ticker",),
                ["A"],
                ["B"],
                subscription_id=42,
            )
        self.assertEqual(msg["params"]["subscription_id"], 42)
        self.assertNotIn("sid", msg)


class TestWsIngestBatcher(unittest.TestCase):
    def test_batcher_add_tick_flushes(self) -> None:
        conn = FakeConn()
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=1, lifecycle_batch_size=1, flush_seconds=1.0)
        with patch.object(batcher, "flush_ticks") as flush_ticks:
            batcher.add_tick({"a": 1})
        flush_ticks.assert_called_once_with(force=True)

    def test_batcher_add_lifecycle_flushes(self) -> None:
        conn = FakeConn()
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=1, lifecycle_batch_size=1, flush_seconds=1.0)
        with patch.object(batcher, "flush_lifecycles") as flush_lifecycles:
            batcher.add_lifecycle({"a": 1})
        flush_lifecycles.assert_called_once_with(force=True)

    def test_batcher_drain_lifecycles_empty(self) -> None:
        conn = FakeConn()
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=1, lifecycle_batch_size=1, flush_seconds=1.0)
        self.assertEqual(batcher._drain_lifecycles(), [])

    def test_batcher_flush_ticks_timing(self) -> None:
        conn = FakeConn()
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=2, lifecycle_batch_size=1, flush_seconds=1.0)
        batcher._tick_buffer = [{"a": 1}]
        batcher._last_tick_flush = 100.0
        with patch("src.ingest.ws.ws_ingest_writer.time.monotonic", return_value=100.5):
            batcher.flush_ticks(force=False)
        self.assertEqual(len(batcher._tick_buffer), 1)

    def test_batcher_flush_ticks_empty(self) -> None:
        conn = FakeConn()
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=2, lifecycle_batch_size=1, flush_seconds=1.0)
        batcher.flush_ticks(force=True)
        self.assertEqual(batcher._tick_buffer, [])

    def test_batcher_flush_ticks_error(self) -> None:
        conn = FakeConn(rollback_raises=True)
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=1, lifecycle_batch_size=1, flush_seconds=1.0)
        batcher._tick_buffer = [{"a": 1}]
        with patch("src.ingest.ws.ws_ingest_writer.insert_market_ticks", side_effect=RuntimeError("fail")):
            batcher.flush_ticks(force=True)
        self.assertEqual(len(batcher._tick_buffer), 1)

    def test_batcher_flush_ticks_success_updates_last_flush(self) -> None:
        conn = FakeConn()
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=1, lifecycle_batch_size=1, flush_seconds=1.0)
        batcher._tick_buffer = [{"a": 1}]
        with patch("src.ingest.ws.ws_ingest_writer.insert_market_ticks"), \
             patch("src.ingest.ws.ws_ingest_writer.time.monotonic", return_value=123.0):
            batcher.flush_ticks(force=True)
        self.assertEqual(batcher._last_tick_flush, 123.0)

    def test_batcher_flush_lifecycles_error(self) -> None:
        conn = FakeConn(rollback_raises=True)
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=1, lifecycle_batch_size=1, flush_seconds=1.0)
        batcher._lifecycle_buffer = [{"a": 1}]
        with patch("src.ingest.ws.ws_ingest_writer.insert_lifecycle_events", side_effect=RuntimeError("fail")):
            batcher.flush_lifecycles(force=True)
        self.assertEqual(len(batcher._lifecycle_buffer), 1)

    def test_batcher_flush_lifecycles_timing(self) -> None:
        conn = FakeConn()
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=1, lifecycle_batch_size=2, flush_seconds=1.0)
        batcher._lifecycle_buffer = [{"a": 1}]
        batcher._last_lifecycle_flush = 100.0
        with patch("src.ingest.ws.ws_ingest_writer.time.monotonic", return_value=100.5):
            batcher.flush_lifecycles(force=False)
        self.assertEqual(len(batcher._lifecycle_buffer), 1)

    def test_batcher_flush_lifecycles_empty(self) -> None:
        conn = FakeConn()
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=1, lifecycle_batch_size=1, flush_seconds=1.0)
        batcher._last_lifecycle_flush = 10.0
        batcher.flush_lifecycles(force=True)
        self.assertEqual(batcher._last_lifecycle_flush, 10.0)

    def test_batcher_flush_lifecycles_success_updates_last_flush(self) -> None:
        conn = FakeConn()
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=1, lifecycle_batch_size=1, flush_seconds=1.0)
        batcher._lifecycle_buffer = [{"a": 1}]
        with patch("src.ingest.ws.ws_ingest_writer.insert_lifecycle_events"), \
             patch("src.ingest.ws.ws_ingest_writer.time.monotonic", return_value=456.0):
            batcher.flush_lifecycles(force=True)
        self.assertEqual(batcher._last_lifecycle_flush, 456.0)

    def test_batcher_flush_due_and_all(self) -> None:
        conn = FakeConn()
        batcher = ws_ingest_writer._DbBatcher(conn, tick_batch_size=1, lifecycle_batch_size=1, flush_seconds=1.0)
        with patch.object(batcher, "flush_ticks") as flush_ticks, \
             patch.object(batcher, "flush_lifecycles") as flush_lifecycles:
            batcher.flush_due()
            batcher.flush_all()
        self.assertEqual(flush_ticks.call_count, 2)
        self.assertEqual(flush_lifecycles.call_count, 2)


class TestWsIngestDeduper(unittest.TestCase):
    def test_deduper_basic(self) -> None:
        deduper = ws_ingest_writer._TickDeduper(enabled=False, max_age_seconds=0.0, fields=("price",))
        self.assertTrue(deduper.should_emit({"ticker": "M1"}))
        deduper = ws_ingest_writer._TickDeduper(enabled=True, max_age_seconds=0.0, fields=("price",))
        self.assertTrue(deduper.should_emit({}))

    def test_deduper_updates(self) -> None:
        deduper = ws_ingest_writer._TickDeduper(enabled=True, max_age_seconds=0.0, fields=("price",))
        with patch("src.ingest.ws.ws_ingest_writer.time.monotonic", side_effect=[0.0, 1.0, 2.0]):
            self.assertTrue(deduper.should_emit({"ticker": "M1", "price": 1}))
            self.assertFalse(deduper.should_emit({"ticker": "M1", "price": 1}))
            self.assertTrue(deduper.should_emit({"ticker": "M1", "price": 2}))

    def test_deduper_max_age(self) -> None:
        deduper = ws_ingest_writer._TickDeduper(enabled=True, max_age_seconds=1.0, fields=("price",))
        with patch("src.ingest.ws.ws_ingest_writer.time.monotonic", side_effect=[0.0, 2.0]):
            self.assertTrue(deduper.should_emit({"ticker": "M1", "price": 1}))
            self.assertTrue(deduper.should_emit({"ticker": "M1", "price": 1}))

    def test_deduper_forget(self) -> None:
        deduper = ws_ingest_writer._TickDeduper(enabled=True, max_age_seconds=0.0, fields=("price",))
        deduper.forget(None)

    def test_deduper_forget_removes(self) -> None:
        deduper = ws_ingest_writer._TickDeduper(enabled=True, max_age_seconds=0.0, fields=("price",))
        deduper._state["M1"] = ((1,), 0.0)
        deduper.forget("M1")
        self.assertNotIn("M1", deduper._state)

    def test_queue_put_nowait_full(self) -> None:
        work_queue = queue.Queue(maxsize=1)
        work_queue.put_nowait(("item",))
        with patch("src.ingest.ws.ws_ingest_writer.logger.warning") as warn:
            result = ws_ingest_writer._queue_put_nowait(work_queue, ("item",), "tick")
        self.assertFalse(result)
        warn.assert_called_once()

    def test_update_market_id_map(self) -> None:
        market_id_map = {}
        ws_ingest_writer._update_market_id_map({"market": {}}, market_id_map)
        self.assertEqual(market_id_map, {})
        ws_ingest_writer._update_market_id_map({"market_id": "1", "market": {}}, market_id_map)
        self.assertNotIn("1", market_id_map)
        ws_ingest_writer._update_market_id_map(
            {"market_id": "1", "market_ticker": "M1"},
            market_id_map,
        )
        self.assertEqual(market_id_map["1"], "M1")


class TestWsIngestDbWriter(unittest.TestCase):
    def test_db_writer_loop_empty_queue(self) -> None:
        class EmptyQueue:
            def get(self, timeout=None):
                raise queue.Empty

            def task_done(self):
                return None

        class FakeBatcher:
            def __init__(self, *_args, **_kwargs):
                self.flush_due_calls = 0
                self.flush_all_calls = 0

            def flush_due(self):
                self.flush_due_calls += 1

            def flush_all(self):
                self.flush_all_calls += 1

        class FakeDeduper:
            def __init__(self, *args, **kwargs):
                return None

        stop_event = ToggleEvent()
        batcher = FakeBatcher()
        config = ws_ingest_models.WriterConfig(
            database_url="db",
            tick_batch_size=1,
            lifecycle_batch_size=1,
            flush_seconds=0.1,
            dedup_enabled=True,
            dedup_max_age_seconds=0.0,
            dedup_fields=("price",),
        )
        fake_psycopg = SimpleNamespace(connect=lambda _dsn: FakeConn("db"))
        with patch("src.ingest.ws.ws_ingest_writer._require_psycopg", return_value=fake_psycopg), \
             patch("src.ingest.ws.ws_ingest_writer._DbBatcher", return_value=batcher), \
             patch("src.ingest.ws.ws_ingest_writer._TickDeduper", FakeDeduper):
            ws_ingest_writer._db_writer_loop(
                EmptyQueue(),
                config,
                stop_event,
            )
        self.assertEqual(batcher.flush_due_calls, 1)
        self.assertEqual(batcher.flush_all_calls, 1)

    def test_db_writer_loop_process_items(self) -> None:
        work_queue = queue.Queue()
        work_queue.put("bad")
        work_queue.put(("unknown", {}, None, None))
        work_queue.put((ws_ingest_writer._DB_WORK_TICK, {"ticker": "M1"}, None, None))
        work_queue.put((ws_ingest_writer._DB_WORK_LIFECYCLE, {"market_ticker": "M1"}, {"ticker": "M1"}, "M1"))
        work_queue.put(ws_ingest_writer._DB_WORK_STOP)
        stop_event = threading.Event()

        class FakeBatcher:
            def __init__(self, *_args, **_kwargs):
                return None

            def add_tick(self, _tick):
                raise RuntimeError("boom")

            def add_lifecycle(self, _lifecycle):
                return None

            def flush_due(self):
                return None

            def flush_all(self):
                return None

        class FakeDeduper:
            def __init__(self, *args, **kwargs):
                return None

            def should_emit(self, _payload):
                return True

            def forget(self, _ticker):
                return None

        config = ws_ingest_models.WriterConfig(
            database_url="db",
            tick_batch_size=1,
            lifecycle_batch_size=1,
            flush_seconds=0.1,
            dedup_enabled=True,
            dedup_max_age_seconds=0.0,
            dedup_fields=("price",),
        )
        fake_psycopg = SimpleNamespace(connect=lambda _dsn: FakeConn("db"))
        with patch("src.ingest.ws.ws_ingest_writer._require_psycopg", return_value=fake_psycopg), \
             patch("src.ingest.ws.ws_ingest_writer._DbBatcher", FakeBatcher), \
             patch("src.ingest.ws.ws_ingest_writer._TickDeduper", FakeDeduper), \
             patch("src.ingest.ws.ws_ingest_writer.upsert_market", side_effect=RuntimeError("fail")), \
             patch("src.ingest.ws.ws_ingest_writer.delete_active_market", side_effect=RuntimeError("fail")), \
             patch("src.ingest.ws.ws_ingest_writer.logger.exception") as log_exc:
            ws_ingest_writer._db_writer_loop(
                work_queue,
                config,
                stop_event,
            )
        self.assertGreaterEqual(log_exc.call_count, 2)

    def test_db_writer_loop_flush_due_and_update(self) -> None:
        work_queue = queue.Queue()
        work_queue.put((ws_ingest_writer._DB_WORK_TICK, {"ticker": "M1"}, None, None))
        work_queue.put(ws_ingest_writer._DB_WORK_STOP)
        stop_event = threading.Event()

        class FakeBatcher:
            def __init__(self, *_args, **_kwargs):
                self.flush_due_calls = 0
                self.flush_all_calls = 0

            def add_tick(self, _tick):
                return None

            def add_lifecycle(self, _lifecycle):
                return None

            def flush_due(self):
                self.flush_due_calls += 1

            def flush_all(self):
                self.flush_all_calls += 1

        class FakeDeduper:
            def __init__(self, *args, **kwargs):
                return None

            def should_emit(self, _payload):
                return True

        batcher = FakeBatcher()

        class MonotonicSeq:
            def __init__(self, values):
                self.values = values
                self.index = 0

            def __call__(self):
                if self.index < len(self.values):
                    value = self.values[self.index]
                    self.index += 1
                    return value
                return self.values[-1]

        config = ws_ingest_models.WriterConfig(
            database_url="db",
            tick_batch_size=1,
            lifecycle_batch_size=1,
            flush_seconds=0.1,
            dedup_enabled=True,
            dedup_max_age_seconds=0.0,
            dedup_fields=("price",),
        )
        fake_psycopg = SimpleNamespace(connect=lambda _dsn: FakeConn("db"))
        with patch("src.ingest.ws.ws_ingest_writer._require_psycopg", return_value=fake_psycopg), \
             patch("src.ingest.ws.ws_ingest_writer._DbBatcher", return_value=batcher), \
             patch("src.ingest.ws.ws_ingest_writer._TickDeduper", FakeDeduper), \
             patch("src.ingest.ws.ws_ingest_writer.time.monotonic", new=MonotonicSeq([0.0, 0.0, 0.2, 0.2, 0.2])):
            ws_ingest_writer._db_writer_loop(
                work_queue,
                config,
                stop_event,
            )
        self.assertEqual(batcher.flush_due_calls, 1)
        self.assertEqual(batcher.flush_all_calls, 1)

    def test_db_writer_loop_crash_and_finalize(self) -> None:
        work_queue = queue.Queue()
        work_queue.put((ws_ingest_writer._DB_WORK_TICK, {"ticker": "M1"}, None, None))
        stop_event = threading.Event()

        class FakeBatcher:
            def __init__(self, *_args, **_kwargs):
                return None

            def add_tick(self, _tick):
                return None

            def add_lifecycle(self, _lifecycle):
                return None

            def flush_due(self):
                raise RuntimeError("flush failed")

            def flush_all(self):
                raise RuntimeError("final flush failed")

        class FakeDeduper:
            def __init__(self, *args, **kwargs):
                return None

            def should_emit(self, _payload):
                return True

        class BadConn(FakeConn):
            def close(self):
                raise RuntimeError("close failed")

        class MonotonicSeq:
            def __init__(self, values):
                self.values = values
                self.index = 0

            def __call__(self):
                if self.index < len(self.values):
                    value = self.values[self.index]
                    self.index += 1
                    return value
                return self.values[-1]

        config = ws_ingest_models.WriterConfig(
            database_url="db",
            tick_batch_size=1,
            lifecycle_batch_size=1,
            flush_seconds=0.1,
            dedup_enabled=True,
            dedup_max_age_seconds=0.0,
            dedup_fields=("price",),
        )
        fake_psycopg = SimpleNamespace(connect=lambda _dsn: BadConn("db"))
        with patch("src.ingest.ws.ws_ingest_writer._require_psycopg", return_value=fake_psycopg), \
             patch("src.ingest.ws.ws_ingest_writer._DbBatcher", FakeBatcher), \
             patch("src.ingest.ws.ws_ingest_writer._TickDeduper", FakeDeduper), \
             patch("src.ingest.ws.ws_ingest_writer.time.monotonic", new=MonotonicSeq([0.0, 0.0, 0.2, 0.2])), \
             patch("src.ingest.ws.ws_ingest_writer.logger.exception") as log_exc:
            ws_ingest_writer._db_writer_loop(
                work_queue,
                config,
                stop_event,
            )
        self.assertGreaterEqual(log_exc.call_count, 2)

class TestWsIngestAsync(unittest.IsolatedAsyncioTestCase):
    async def test_listen_loop(self) -> None:
        messages = [
            "{",
            json.dumps(["not dict"]),
            json.dumps({"type": "ticker", "msg": {"market_ticker": "M1", "price": 10}}),
            json.dumps(
                {
                    "type": "market_lifecycle_v2",
                    "msg": {
                        "market_ticker": "M2",
                        "event_type": "settled",
                        "market": {"ticker": "M2", "market_id": "2"},
                    },
                }
            ),
            json.dumps({"type": "error", "error": "bad"}),
        ]
        ws = FakeWS(messages)
        work_queue = queue.Queue()
        market_id_map = {}
        with patch("src.ingest.ws.ws_ingest.logger.warning") as warn:
            await ws_ingest._listen_loop(ws, work_queue, market_id_map)
        self.assertEqual(work_queue.qsize(), 2)
        self.assertEqual(market_id_map.get("2"), "M2")
        self.assertGreaterEqual(warn.call_count, 2)

    async def test_listen_loop_skips_invalid_payloads(self) -> None:
        messages = [
            json.dumps({"type": "ticker", "msg": {"price": 10}}),
            json.dumps({"type": "market_lifecycle_v2", "msg": {"market_id": "1"}}),
        ]
        ws = FakeWS(messages)
        work_queue = queue.Queue()
        market_id_map = {}
        with patch("src.ingest.ws.ws_ingest._normalize_lifecycle", return_value=None):
            await ws_ingest._listen_loop(ws, work_queue, market_id_map)
        self.assertEqual(work_queue.qsize(), 0)

    async def test_refresh_subscriptions_error(self) -> None:
        ws = FakeWS()
        lock = asyncio.Lock()
        request_id = iter([1])
        calls = {"sleep": 0}
        shard = ws_ingest_models.ShardConfig(
            count=1,
            shard_id=0,
            round_robin=True,
            round_robin_step=0,
        )
        state = ws_ingest_models.SubscriptionState(
            subscribed=set(),
            lock=lock,
            request_id=request_id,
        )
        config = ws_ingest_models.SubscriptionConfig(
            channels=("ticker",),
            max_active_tickers=10,
            shard=shard,
            ws_batch_size=1,
        )
        context = ws_ingest_models.SubscriptionContext(
            conn=object(),
            config=config,
            state=state,
        )

        async def fake_sleep(_seconds):
            calls["sleep"] += 1
            if calls["sleep"] > 1:
                raise StopLoop()

        with patch("src.ingest.ws.ws_ingest_subscriptions.asyncio.sleep", new=fake_sleep), \
             patch("src.ingest.ws.ws_ingest_subscriptions.load_active_tickers_shard", side_effect=Exception("boom")), \
             patch("src.ingest.ws.ws_ingest_subscriptions.logger.exception") as log_exc:
            with self.assertRaises(StopLoop):
                await ws_ingest_subscriptions._refresh_subscriptions(ws, context, 1)
        log_exc.assert_called_once()

    async def test_refresh_subscriptions_add_remove(self) -> None:
        ws = FakeWS()
        lock = asyncio.Lock()
        request_id = iter([1, 2])
        calls = {"sleep": 0}
        shard = ws_ingest_models.ShardConfig(
            count=1,
            shard_id=0,
            round_robin=True,
            round_robin_step=0,
        )
        state = ws_ingest_models.SubscriptionState(
            subscribed={"OLD"},
            lock=lock,
            request_id=request_id,
            sid_tickers={1: {"OLD"}},
        )
        config = ws_ingest_models.SubscriptionConfig(
            channels=("ticker",),
            max_active_tickers=10,
            shard=shard,
            ws_batch_size=1,
        )
        context = ws_ingest_models.SubscriptionContext(
            conn=object(),
            config=config,
            state=state,
        )

        async def fake_sleep(_seconds):
            calls["sleep"] += 1
            if calls["sleep"] > 1:
                raise StopLoop()

        with patch("src.ingest.ws.ws_ingest_subscriptions.asyncio.sleep", new=fake_sleep), \
             patch("src.ingest.ws.ws_ingest_subscriptions.load_active_tickers_shard", return_value=["NEW"]):
            with self.assertRaises(StopLoop):
                await ws_ingest_subscriptions._refresh_subscriptions(ws, context, 1)
        self.assertIn("NEW", state.subscribed)
        self.assertNotIn("OLD", state.subscribed)
        self.assertEqual(len(ws.sent), 2)

    async def test_refresh_subscriptions_no_changes(self) -> None:
        ws = FakeWS()
        lock = asyncio.Lock()
        request_id = iter([1])
        calls = {"sleep": 0}
        shard = ws_ingest_models.ShardConfig(
            count=1,
            shard_id=0,
            round_robin=True,
            round_robin_step=0,
        )
        state = ws_ingest_models.SubscriptionState(
            subscribed={"A"},
            lock=lock,
            request_id=request_id,
        )
        config = ws_ingest_models.SubscriptionConfig(
            channels=("ticker",),
            max_active_tickers=10,
            shard=shard,
            ws_batch_size=1,
        )
        context = ws_ingest_models.SubscriptionContext(
            conn=object(),
            config=config,
            state=state,
        )

        async def fake_sleep(_seconds):
            calls["sleep"] += 1
            if calls["sleep"] > 1:
                raise StopLoop()

        with patch("src.ingest.ws.ws_ingest_subscriptions.asyncio.sleep", new=fake_sleep), \
             patch("src.ingest.ws.ws_ingest_subscriptions.load_active_tickers_shard", return_value=["A"]):
            with self.assertRaises(StopLoop):
                await ws_ingest_subscriptions._refresh_subscriptions(ws, context, 1)
        self.assertEqual(ws.sent, [])

    async def test_ws_loop_disabled(self) -> None:
        conn = FakeConn("db")
        with patch.dict(os.environ, {"KALSHI_WS_ENABLE": "0"}, clear=True), \
             patch("src.ingest.ws.ws_ingest.assert_service_role"), \
             patch("src.ingest.ws.ws_ingest.asyncio.sleep", side_effect=StopLoop):
            with self.assertRaises(StopLoop):
                await ws_ingest.ws_loop(conn, "key", "pem", database_url="db")

    async def test_ws_loop_invalid_shard(self) -> None:
        conn = FakeConn("db")
        with patch.dict(
            os.environ,
            {"KALSHI_WS_ENABLE": "1", "WS_SHARD_COUNT": "1", "WS_SHARD_ID": "2"},
            clear=True,
        ), patch("src.ingest.ws.ws_ingest.assert_service_role"):
            with self.assertRaises(ValueError):
                await ws_ingest.ws_loop(conn, "key", "pem", database_url="db")

    async def test_ws_loop_missing_dsn(self) -> None:
        conn = FakeConn(None)
        with patch.dict(os.environ, {"KALSHI_WS_ENABLE": "1"}, clear=True), \
             patch("src.ingest.ws.ws_ingest.assert_service_role"):
            with self.assertRaises(ValueError):
                await ws_ingest.ws_loop(conn, "key", "pem")

    async def test_ws_loop_connect_cycle(self) -> None:
        conn = FakeConn("db")
        thread_holder = {}
        sent_kwargs = {}

        def thread_factory(*args, **kwargs):
            thread_holder["thread"] = FakeThread(*args, **kwargs)
            return thread_holder["thread"]

        class FakeContext:
            async def __aenter__(self):
                return FakeWS()

            async def __aexit__(self, exc_type, exc, tb):
                return False

        def fake_connect(url, additional_headers=None, max_queue=None, max_size=None, **kwargs):
            sent_kwargs["additional_headers"] = additional_headers
            sent_kwargs["max_queue"] = max_queue
            sent_kwargs["max_size"] = max_size
            return FakeContext()

        async def fake_listener(*_args, **_kwargs):
            raise StopLoop()

        async def fake_refresher(*_args, **_kwargs):
            await asyncio.Future()

        with patch.dict(
            os.environ,
            {
                "KALSHI_WS_ENABLE": "1",
                "WS_TICK_DEDUP_FIELDS": "",
                "WS_SHARD_COUNT": "1",
                "WS_SHARD_ID": "0",
            },
            clear=True,
        ), patch("src.ingest.ws.ws_ingest.assert_service_role"), \
             patch("src.ingest.ws.ws_ingest.threading.Thread", side_effect=thread_factory), \
             patch("src.ingest.ws.ws_ingest._build_ws_headers", return_value={"X": "Y"}), \
             patch("src.ingest.ws.ws_ingest._load_websockets", return_value=SimpleNamespace(connect=fake_connect)), \
             patch("src.ingest.ws.ws_ingest_subscriptions.load_active_tickers_shard", side_effect=Exception("boom")), \
             patch("src.ingest.ws.ws_ingest._listen_loop", new=fake_listener), \
             patch("src.ingest.ws.ws_ingest._refresh_subscriptions", new=fake_refresher), \
             patch("src.ingest.ws.ws_ingest.logger.exception"), \
             patch("src.ingest.ws.ws_ingest.asyncio.sleep", side_effect=StopLoop):
            with self.assertRaises(StopLoop):
                await ws_ingest.ws_loop(conn, "key", "pem", database_url="db")

        self.assertTrue(thread_holder["thread"].started)
        self.assertTrue(thread_holder["thread"].joined)
        self.assertIn("additional_headers", sent_kwargs)
        self.assertIsNotNone(sent_kwargs.get("max_queue"))
        self.assertIsNotNone(sent_kwargs.get("max_size"))
        self.assertEqual(len(thread_holder["thread"].args), 3)

    async def test_ws_loop_subscribe_defaults_and_extra_headers(self) -> None:
        conn = FakeConn("db")
        thread_holder = {}
        captured = {}

        def thread_factory(*args, **kwargs):
            thread_holder["thread"] = FakeThread(*args, **kwargs)
            return thread_holder["thread"]

        class FakeContext:
            async def __aenter__(self):
                return FakeWS()

            async def __aexit__(self, exc_type, exc, tb):
                return False

        def fake_connect(url, extra_headers=None, **kwargs):
            captured["extra_headers"] = extra_headers
            return FakeContext()

        async def fake_listener(*_args, **_kwargs):
            raise StopLoop()

        async def fake_refresher(*_args, **_kwargs):
            await asyncio.Future()

        def capture_subscribe(req_id, channels, market_tickers):
            captured["channels"] = list(channels)
            return {
                "id": req_id,
                "cmd": "subscribe",
                "params": {"channels": list(channels), "market_tickers": market_tickers},
            }

        with patch.dict(
            os.environ,
            {
                "KALSHI_WS_ENABLE": "1",
                "KALSHI_WS_CHANNELS": "",
                "WS_SHARD_COUNT": "2",
                "WS_SHARD_ID": "0",
            },
            clear=True,
        ), patch("src.ingest.ws.ws_ingest.assert_service_role"), \
             patch("src.ingest.ws.ws_ingest.threading.Thread", side_effect=thread_factory), \
             patch("src.ingest.ws.ws_ingest._build_ws_headers", return_value={"X": "Y"}), \
             patch("src.ingest.ws.ws_ingest._load_websockets", return_value=SimpleNamespace(connect=fake_connect)), \
             patch("src.ingest.ws.ws_ingest_subscriptions.load_active_tickers_shard", return_value=["M1"]), \
             patch("src.ingest.ws.ws_ingest_subscriptions._build_subscribe_message", side_effect=capture_subscribe), \
             patch("src.ingest.ws.ws_ingest._listen_loop", new=fake_listener), \
             patch("src.ingest.ws.ws_ingest._refresh_subscriptions", new=fake_refresher), \
             patch("src.ingest.ws.ws_ingest.logger.info") as log_info, \
             patch("src.ingest.ws.ws_ingest.asyncio.sleep", side_effect=StopLoop):
            with self.assertRaises(StopLoop):
                await ws_ingest.ws_loop(conn, "key", "pem", database_url="db")

        self.assertIn("extra_headers", captured)
        self.assertEqual(captured["channels"], list(ws_ingest_config._DEFAULT_WS_CHANNELS))
        log_info.assert_called()

    async def test_ws_loop_signature_error_uses_extra_headers(self) -> None:
        conn = FakeConn("db")
        thread_holder = {}
        captured = {}

        def thread_factory(*args, **kwargs):
            thread_holder["thread"] = FakeThread(*args, **kwargs)
            return thread_holder["thread"]

        class FakeContext:
            async def __aenter__(self):
                return FakeWS()

            async def __aexit__(self, exc_type, exc, tb):
                return False

        def fake_connect(url, **kwargs):
            captured.update(kwargs)
            return FakeContext()

        async def fake_listener(*_args, **_kwargs):
            raise StopLoop()

        async def fake_refresher(*_args, **_kwargs):
            await asyncio.Future()

        with patch.dict(
            os.environ,
            {"KALSHI_WS_ENABLE": "1"},
            clear=True,
        ), patch("src.ingest.ws.ws_ingest.assert_service_role"), \
             patch("src.ingest.ws.ws_ingest.threading.Thread", side_effect=thread_factory), \
             patch("src.ingest.ws.ws_ingest._build_ws_headers", return_value={"X": "Y"}), \
             patch("src.ingest.ws.ws_ingest.inspect.signature", side_effect=ValueError("bad")), \
             patch("src.ingest.ws.ws_ingest._load_websockets", return_value=SimpleNamespace(connect=fake_connect)), \
             patch("src.ingest.ws.ws_ingest_subscriptions.load_active_tickers_shard", return_value=[]), \
             patch("src.ingest.ws.ws_ingest._listen_loop", new=fake_listener), \
             patch("src.ingest.ws.ws_ingest._refresh_subscriptions", new=fake_refresher), \
             patch("src.ingest.ws.ws_ingest.asyncio.sleep", side_effect=StopLoop):
            with self.assertRaises(StopLoop):
                await ws_ingest.ws_loop(conn, "key", "pem", database_url="db")

        self.assertEqual(captured.get("extra_headers"), {"X": "Y"})
        self.assertNotIn("additional_headers", captured)

    async def test_ws_loop_missing_dsn_from_conn_info(self) -> None:
        class BrokenConn:
            @property
            def info(self):
                raise RuntimeError("no info")

        conn = BrokenConn()
        with patch.dict(os.environ, {"KALSHI_WS_ENABLE": "1"}, clear=True), \
             patch("src.ingest.ws.ws_ingest.assert_service_role"):
            with self.assertRaises(ValueError):
                await ws_ingest.ws_loop(conn, "key", "pem")

    async def test_ws_loop_backoff_and_queue_full(self) -> None:
        conn = FakeConn("db")
        thread_holder = {}

        def thread_factory(*args, **kwargs):
            thread_holder["thread"] = FakeThread(*args, **kwargs)
            return thread_holder["thread"]

        def fake_connect(*_args, **_kwargs):
            raise RuntimeError("boom")

        sleep_calls = []

        async def fake_sleep(seconds):
            sleep_calls.append(seconds)
            if len(sleep_calls) > 1:
                raise StopLoop()

        class FullQueue:
            def __init__(self, *args, **kwargs):
                return None

            def put_nowait(self, _item):
                raise queue.Full

        with patch.dict(os.environ, {"KALSHI_WS_ENABLE": "1"}, clear=True), \
             patch("src.ingest.ws.ws_ingest.assert_service_role"), \
             patch("src.ingest.ws.ws_ingest.threading.Thread", side_effect=thread_factory), \
             patch("src.ingest.ws.ws_ingest._build_ws_headers", return_value={"X": "Y"}), \
             patch("src.ingest.ws.ws_ingest._load_websockets", return_value=SimpleNamespace(connect=fake_connect)), \
             patch("src.ingest.ws.ws_ingest.queue.Queue", new=FullQueue), \
             patch("src.ingest.ws.ws_ingest.asyncio.sleep", new=fake_sleep):
            with self.assertRaises(StopLoop):
                await ws_ingest.ws_loop(conn, "key", "pem", database_url="db")
        self.assertEqual(sleep_calls[0], 1)
        self.assertEqual(sleep_calls[1], 2)
