import unittest
from types import SimpleNamespace
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

rag_loop = importlib.import_module("src.rag.rag_loop")


class FakeConn:
    def __init__(self, closed=False, close_raises=False):
        self.closed = closed
        self.closed_calls = 0
        self._close_raises = close_raises

    def close(self):
        self.closed_calls += 1
        if self._close_raises:
            raise RuntimeError("close failed")


class TestRagLoopConnections(unittest.TestCase):
    def test_ensure_connection_reuses_open(self) -> None:
        conn = FakeConn(closed=False)
        with patch("src.rag.rag_loop._open_connection") as open_conn:
            out_conn, ready = rag_loop._ensure_connection(conn, "postgres://db", True)
        self.assertIs(out_conn, conn)
        self.assertTrue(ready)
        open_conn.assert_not_called()

    def test_ensure_connection_closes_stale(self) -> None:
        conn = FakeConn(closed=True, close_raises=True)
        with patch("src.rag.rag_loop._open_connection", return_value=("new", True)) as open_conn, \
             patch.object(rag_loop.logger, "debug") as log_debug:
            out_conn, ready = rag_loop._ensure_connection(conn, "postgres://db", False)
        self.assertEqual(out_conn, "new")
        self.assertTrue(ready)
        open_conn.assert_called_once()
        log_debug.assert_called_once()


class TestRagLoopExecution(unittest.TestCase):
    def test_rag_prediction_loop_success_logs_metrics(self) -> None:
        prediction_cfg = SimpleNamespace(intervals=SimpleNamespace(poll_seconds=1))

        class FakeFailureContext:
            def __init__(self, *args, **kwargs):
                self.error_total = 0
                self.recorded = False

            def record_success(self, _logger, _msg):
                self.recorded = True

            def handle_exception(self, _logger, _start):
                return False

        with patch("src.rag.rag_loop._ensure_connection", return_value=("conn", True)), \
             patch("src.rag.rag_loop.resolve_prediction_handler", return_value="handler"), \
             patch("src.rag.rag_loop.prediction_pass", return_value=5), \
             patch("src.rag.rag_loop.LoopFailureContext", FakeFailureContext), \
             patch("src.rag.rag_loop.log_metric") as log_metric, \
             patch("src.rag.rag_loop.time.monotonic", side_effect=[0.0, 1.2]), \
             patch("src.rag.rag_loop.time.sleep", side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                rag_loop.rag_prediction_loop(prediction_cfg, "postgres://db")

        log_metric.assert_called_once()
        args, kwargs = log_metric.call_args
        self.assertEqual(args[1], "rag.predictions")
        self.assertEqual(kwargs["stored"], 5)

    def test_rag_prediction_loop_exception_closed_continue(self) -> None:
        prediction_cfg = SimpleNamespace(intervals=SimpleNamespace(poll_seconds=1))
        conn = FakeConn(closed=True)

        class FakeFailureContext:
            def __init__(self, *args, **kwargs):
                self.error_total = 0

            def record_success(self, _logger, _msg):
                raise AssertionError("record_success should not be called")

            def handle_exception(self, _logger, _start):
                return True

        calls = {"count": 0}

        def ensure_conn(_conn, _db, _ready):
            calls["count"] += 1
            if calls["count"] == 1:
                return conn, True
            raise SystemExit

        with patch("src.rag.rag_loop._ensure_connection", side_effect=ensure_conn), \
             patch("src.rag.rag_loop.resolve_prediction_handler", return_value="handler"), \
             patch("src.rag.rag_loop.prediction_pass", side_effect=RuntimeError("boom")), \
             patch("src.rag.rag_loop.LoopFailureContext", FakeFailureContext), \
             patch.object(rag_loop.logger, "warning") as log_warn:
            with self.assertRaises(SystemExit):
                rag_loop.rag_prediction_loop(prediction_cfg, "postgres://db")

        log_warn.assert_called_once()
