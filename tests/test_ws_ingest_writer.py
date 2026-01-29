import os
import queue
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

ws_ingest_writer = importlib.import_module("src.ingest.ws.ws_ingest_writer")
ws_ingest_models = importlib.import_module("src.ingest.ws.ws_ingest_models")


class FakeConn:
    def __init__(self):
        self.closed = False
        self.rolled_back = False

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class ToggleEvent:
    def __init__(self, limit=1):
        self.calls = 0
        self.limit = limit

    def is_set(self):
        self.calls += 1
        return self.calls > self.limit

    def set(self):
        return None


def _writer_config() -> ws_ingest_models.WriterConfig:
    return ws_ingest_models.WriterConfig(
        database_url="db",
        tick_batch_size=1,
        lifecycle_batch_size=1,
        flush_seconds=0.1,
        dedup_enabled=True,
        dedup_max_age_seconds=0.0,
        dedup_fields=("price",),
    )


class TestWsIngestWriterHelpers(unittest.TestCase):
    def test_drop_log_interval_invalid_env(self) -> None:
        with patch.dict(os.environ, {"WS_QUEUE_DROP_METRIC_SECONDS": "bad"}):
            self.assertEqual(ws_ingest_writer._drop_log_interval(), 60.0)

    def test_restore_buffer_empty_items(self) -> None:
        batcher = ws_ingest_writer._DbBatcher(FakeConn(), 1, 1, 1.0)
        batcher._tick_buffer = [{"a": 1}]
        batcher._restore_buffer(ws_ingest_writer._DB_WORK_TICK, [])
        self.assertEqual(batcher._tick_buffer, [{"a": 1}])

    def test_pop_error_clears(self) -> None:
        batcher = ws_ingest_writer._DbBatcher(FakeConn(), 1, 1, 1.0)
        err = ValueError("boom")
        batcher._last_error = err
        self.assertIs(batcher.pop_error(), err)
        self.assertIsNone(batcher.pop_error())

    def test_queue_put_nowait_success(self) -> None:
        work_queue = queue.Queue(maxsize=1)
        item = ("item",)
        self.assertTrue(ws_ingest_writer._queue_put_nowait(work_queue, item, "tick"))
        self.assertEqual(work_queue.get_nowait(), item)

    def test_writer_is_healthy_paths(self) -> None:
        status = ws_ingest_models.WriterStatus()
        self.assertTrue(ws_ingest_writer.writer_is_healthy(status, 0))
        status.last_heartbeat = 0.0
        self.assertFalse(ws_ingest_writer.writer_is_healthy(status, 10.0))
        status.last_heartbeat = 100.0
        with patch("src.ingest.ws.ws_ingest_writer.time.monotonic", return_value=100.5):
            self.assertTrue(ws_ingest_writer.writer_is_healthy(status, 1.0))

    def test_safe_close_none(self) -> None:
        ws_ingest_writer._safe_close(None)

    def test_safe_rollback_conn(self) -> None:
        with patch("src.ingest.ws.ws_ingest_writer._safe_rollback") as safe_rollback:
            ws_ingest_writer._safe_rollback_conn(None)
            safe_rollback.assert_not_called()
            conn = FakeConn()
            ws_ingest_writer._safe_rollback_conn(conn)
            safe_rollback.assert_called_once_with(conn)

    def test_upsert_market_safe_success(self) -> None:
        conn = FakeConn()
        market = {"ticker": "M1"}
        with patch("src.ingest.ws.ws_ingest_writer.upsert_market") as upsert, \
             patch("src.ingest.ws.ws_ingest_writer.maybe_upsert_active_market_from_market") as maybe_upsert:
            ws_ingest_writer._upsert_market_safe(conn, market)
        upsert.assert_called_once_with(conn, market)
        maybe_upsert.assert_called_once_with(conn, market)

    def test_delete_market_safe_privilege_error(self) -> None:
        conn = FakeConn()
        error_type = ws_ingest_writer._psycopg_privilege_error_type()
        with patch(
            "src.ingest.ws.ws_ingest_writer.delete_active_market",
            side_effect=error_type("nope"),
        ), patch(
            "src.ingest.ws.ws_ingest_writer._safe_rollback",
        ) as safe_rollback, patch(
            "src.ingest.ws.ws_ingest_writer.logger.warning",
        ) as warn:
            ws_ingest_writer._delete_market_safe(conn, "M1")
        warn.assert_called_once()
        safe_rollback.assert_called_once_with(conn)

    def test_reset_writer_connection(self) -> None:
        runtime = ws_ingest_writer._WriterRuntime(conn=FakeConn())
        conn = runtime.conn
        with patch("src.ingest.ws.ws_ingest_writer._safe_rollback_conn") as safe_rb, \
             patch("src.ingest.ws.ws_ingest_writer._safe_close") as safe_close:
            ws_ingest_writer._reset_writer_connection(runtime)
        safe_rb.assert_called_once_with(conn)
        safe_close.assert_called_once_with(conn)
        self.assertIsNone(runtime.conn)

    def test_setup_writer_connection_existing_conn(self) -> None:
        runtime = ws_ingest_writer._WriterRuntime(conn=object())
        config = _writer_config()
        status = ws_ingest_models.WriterStatus()

        class FakePsycopg:
            def connect(self, _dsn):
                raise AssertionError("connect should not be called")

        self.assertTrue(
            ws_ingest_writer._setup_writer_connection(
                runtime,
                FakePsycopg(),
                config,
                status,
                max_backoff=5.0,
            )
        )

    def test_setup_writer_connection_connect_failure(self) -> None:
        runtime = ws_ingest_writer._WriterRuntime()
        config = _writer_config()
        status = ws_ingest_models.WriterStatus()

        def raise_connect(_dsn):
            raise RuntimeError("boom")

        fake_psycopg = SimpleNamespace(connect=raise_connect)
        with patch("src.ingest.ws.ws_ingest_writer.time.sleep") as sleep, \
             patch("src.ingest.ws.ws_ingest_writer.time.monotonic", return_value=123.0):
            result = ws_ingest_writer._setup_writer_connection(
                runtime,
                fake_psycopg,
                config,
                status,
                max_backoff=10.0,
            )
        self.assertFalse(result)
        self.assertEqual(status.last_error, "connect: boom")
        self.assertEqual(status.last_heartbeat, 123.0)
        sleep.assert_called_once_with(1.0)
        self.assertEqual(runtime.backoff, 2.0)

    def test_setup_writer_connection_updates_batcher_conn(self) -> None:
        class FakeBatcher:
            def __init__(self):
                self.conn = "old"

        runtime = ws_ingest_writer._WriterRuntime(batcher=FakeBatcher())
        config = _writer_config()
        status = ws_ingest_models.WriterStatus()
        new_conn = FakeConn()
        fake_psycopg = SimpleNamespace(connect=lambda _dsn: new_conn)
        with patch("src.ingest.ws.ws_ingest_writer._TickDeduper", return_value=object()):
            result = ws_ingest_writer._setup_writer_connection(
                runtime,
                fake_psycopg,
                config,
                status,
                max_backoff=5.0,
            )
        self.assertTrue(result)
        self.assertIs(runtime.batcher.conn, new_conn)

    def test_flush_and_check_error_resets(self) -> None:
        class FakeBatcher:
            def __init__(self):
                self.flush_due_calls = 0

            def flush_due(self):
                self.flush_due_calls += 1

            def pop_error(self):
                return RuntimeError("bad")

        runtime = ws_ingest_writer._WriterRuntime(conn=FakeConn(), batcher=FakeBatcher())
        status = ws_ingest_models.WriterStatus()
        with patch("src.ingest.ws.ws_ingest_writer._reset_writer_connection") as reset:
            result = ws_ingest_writer._flush_and_check(runtime, status)
        self.assertFalse(result)
        self.assertTrue(status.last_error.startswith("flush: "))
        reset.assert_called_once_with(runtime)

    def test_handle_queue_item_psycopg_error(self) -> None:
        class FakeBatcher:
            def pop_error(self):
                return None

        runtime = ws_ingest_writer._WriterRuntime(conn=FakeConn(), batcher=FakeBatcher())
        status = ws_ingest_models.WriterStatus()
        error_type = ws_ingest_writer._psycopg_error_type()
        with patch(
            "src.ingest.ws.ws_ingest_writer._handle_db_item",
            side_effect=error_type("boom"),
        ), patch(
            "src.ingest.ws.ws_ingest_writer._safe_rollback_conn",
        ) as safe_rb:
            should_stop, reconnect = ws_ingest_writer._handle_queue_item(runtime, status, ("item",))
        self.assertFalse(should_stop)
        self.assertTrue(reconnect)
        self.assertTrue(status.last_error.startswith("db item: "))
        safe_rb.assert_called_once_with(runtime.conn)

    def test_handle_queue_item_flush_error(self) -> None:
        class FakeBatcher:
            def pop_error(self):
                return RuntimeError("flush")

        runtime = ws_ingest_writer._WriterRuntime(conn=FakeConn(), batcher=FakeBatcher())
        status = ws_ingest_models.WriterStatus()
        with patch("src.ingest.ws.ws_ingest_writer._handle_db_item", return_value=False):
            should_stop, reconnect = ws_ingest_writer._handle_queue_item(runtime, status, ("item",))
        self.assertFalse(should_stop)
        self.assertTrue(reconnect)
        self.assertTrue(status.last_error.startswith("flush: "))

    def test_run_writer_queue_restart_event(self) -> None:
        runtime = ws_ingest_writer._WriterRuntime(conn=FakeConn())
        config = _writer_config()
        stop_event = threading.Event()
        restart_event = threading.Event()
        restart_event.set()
        control = ws_ingest_writer._WriterControl(
            stop_event=stop_event,
            restart_event=restart_event,
            status=ws_ingest_models.WriterStatus(),
        )
        with patch("src.ingest.ws.ws_ingest_writer._reset_writer_connection") as reset:
            result = ws_ingest_writer._run_writer_queue(runtime, queue.Queue(), config, control)
        self.assertFalse(result)
        self.assertFalse(restart_event.is_set())
        reset.assert_called_once_with(runtime)

    def test_run_writer_queue_queue_empty_flush_failure(self) -> None:
        class EmptyQueue:
            def get(self, timeout=None):
                raise queue.Empty

        runtime = ws_ingest_writer._WriterRuntime(conn=FakeConn(), batcher=object())
        config = _writer_config()
        control = ws_ingest_writer._WriterControl(
            stop_event=threading.Event(),
            restart_event=threading.Event(),
            status=ws_ingest_models.WriterStatus(),
        )
        with patch("src.ingest.ws.ws_ingest_writer._flush_and_check", return_value=False):
            self.assertFalse(ws_ingest_writer._run_writer_queue(runtime, EmptyQueue(), config, control))

    def test_run_writer_queue_reconnect_path(self) -> None:
        runtime = ws_ingest_writer._WriterRuntime(conn=FakeConn())
        config = _writer_config()
        control = ws_ingest_writer._WriterControl(
            stop_event=threading.Event(),
            restart_event=threading.Event(),
            status=ws_ingest_models.WriterStatus(),
        )
        work_queue = queue.Queue()
        work_queue.put(("item",))
        with patch("src.ingest.ws.ws_ingest_writer._handle_queue_item", return_value=(False, True)), \
             patch("src.ingest.ws.ws_ingest_writer._reset_writer_connection") as reset:
            result = ws_ingest_writer._run_writer_queue(runtime, work_queue, config, control)
        self.assertFalse(result)
        reset.assert_called_once_with(runtime)

    def test_run_writer_queue_flush_check_failure(self) -> None:
        runtime = ws_ingest_writer._WriterRuntime(conn=FakeConn())
        config = _writer_config()
        control = ws_ingest_writer._WriterControl(
            stop_event=threading.Event(),
            restart_event=threading.Event(),
            status=ws_ingest_models.WriterStatus(),
        )
        work_queue = queue.Queue()
        work_queue.put(("item",))
        with patch("src.ingest.ws.ws_ingest_writer._handle_queue_item", return_value=(False, False)), \
             patch("src.ingest.ws.ws_ingest_writer._flush_and_check", return_value=False), \
             patch(
                 "src.ingest.ws.ws_ingest_writer.time.monotonic",
                 side_effect=[0.0, 0.0, 1.0],
             ):
            result = ws_ingest_writer._run_writer_queue(runtime, work_queue, config, control)
        self.assertFalse(result)

    def test_db_writer_loop_setup_returns_false(self) -> None:
        stop_event = ToggleEvent(limit=1)
        restart_event = threading.Event()
        status = ws_ingest_models.WriterStatus()
        config = _writer_config()
        with patch("src.ingest.ws.ws_ingest_writer._require_psycopg", return_value=object()), \
             patch("src.ingest.ws.ws_ingest_writer._setup_writer_connection", return_value=False) as setup, \
             patch("src.ingest.ws.ws_ingest_writer._run_writer_queue") as run_queue:
            ws_ingest_writer._db_writer_loop(
                queue.Queue(),
                config,
                stop_event,
                restart_event,
                status,
            )
        setup.assert_called_once()
        run_queue.assert_not_called()

    def test_db_writer_loop_runtime_error(self) -> None:
        stop_event = ToggleEvent(limit=1)
        restart_event = threading.Event()
        status = ws_ingest_models.WriterStatus()
        config = _writer_config()
        with patch("src.ingest.ws.ws_ingest_writer._require_psycopg", return_value=object()), \
             patch("src.ingest.ws.ws_ingest_writer._setup_writer_connection", return_value=True), \
             patch("src.ingest.ws.ws_ingest_writer._run_writer_queue", side_effect=RuntimeError("boom")), \
             patch("src.ingest.ws.ws_ingest_writer._reset_writer_connection") as reset, \
             patch("src.ingest.ws.ws_ingest_writer.time.sleep") as sleep:
            ws_ingest_writer._db_writer_loop(
                queue.Queue(),
                config,
                stop_event,
                restart_event,
                status,
            )
        self.assertTrue(status.last_error.startswith("loop: "))
        reset.assert_called_once()
        sleep.assert_called_once()

    def test_db_writer_loop_value_error(self) -> None:
        stop_event = ToggleEvent(limit=1)
        restart_event = threading.Event()
        status = ws_ingest_models.WriterStatus()
        config = _writer_config()
        with patch("src.ingest.ws.ws_ingest_writer._require_psycopg", return_value=object()), \
             patch("src.ingest.ws.ws_ingest_writer._setup_writer_connection", return_value=True), \
             patch("src.ingest.ws.ws_ingest_writer._run_writer_queue", side_effect=ValueError("boom")), \
             patch("src.ingest.ws.ws_ingest_writer._reset_writer_connection") as reset, \
             patch("src.ingest.ws.ws_ingest_writer.time.sleep") as sleep:
            ws_ingest_writer._db_writer_loop(
                queue.Queue(),
                config,
                stop_event,
                restart_event,
                status,
            )
        self.assertTrue(status.last_error.startswith("fatal: "))
        reset.assert_called_once()
        sleep.assert_called_once()
