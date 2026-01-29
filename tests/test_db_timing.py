import builtins
import os
import unittest
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from _test_utils import add_src_to_path

add_src_to_path()

DB_TIMING_PATH = (
    Path(__file__).resolve().parents[1] / "src" / "web_portal" / "db_timing.py"
)


def _load_db_timing_module(name: str, *, block_flask: bool = False):
    spec = spec_from_file_location(name, DB_TIMING_PATH)
    assert spec and spec.loader
    module = module_from_spec(spec)
    if block_flask:
        original_import = builtins.__import__

        def fake_import(module_name, globals=None, locals=None, fromlist=(), level=0):
            if module_name == "flask":
                raise ModuleNotFoundError("No module named 'flask'")
            return original_import(module_name, globals, locals, fromlist, level)

        with patch("builtins.__import__", new=fake_import):
            spec.loader.exec_module(module)
    else:
        spec.loader.exec_module(module)
    return module


class DummyCursor:
    def __init__(self):
        self.execute_args = None
        self.execute_kwargs = None
        self.executemany_args = None
        self.executemany_kwargs = None
        self.some_attr = "value"

    def execute(self, *args, **kwargs):
        self.execute_args = args
        self.execute_kwargs = kwargs
        return "ok"

    def executemany(self, *args, **kwargs):
        self.executemany_args = args
        self.executemany_kwargs = kwargs
        return "many"


class DummyCursorContext:
    def __init__(self, cursor):
        self.cursor = cursor
        self.entered = False
        self.exited = False

    def __enter__(self):
        self.entered = True
        return self.cursor

    def __exit__(self, exc_type, exc, tb):
        self.exited = True
        return False


class DummyConn:
    def __init__(self, cursor_ctx):
        self.cursor_ctx = cursor_ctx
        self.cursor_kwargs = None

    def cursor(self, **kwargs):
        self.cursor_kwargs = kwargs
        return self.cursor_ctx


class TestDbTiming(unittest.TestCase):
    def test_fallback_import_without_flask(self) -> None:
        mod = _load_db_timing_module("db_timing_no_flask", block_flask=True)
        self.assertIsNone(mod.FLASK_G)
        self.assertFalse(mod._has_request_context())
        self.assertIsNone(mod._get_request_g())

    def test_slow_query_threshold_ms_variants(self) -> None:
        mod = _load_db_timing_module("db_timing_thresholds")
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(mod._slow_query_threshold_ms())
        with patch.dict(os.environ, {"WEB_PORTAL_DB_SLOW_QUERY_MS": "nope"}, clear=True):
            self.assertIsNone(mod._slow_query_threshold_ms())
        with patch.dict(os.environ, {"WEB_PORTAL_DB_SLOW_QUERY_MS": "0"}, clear=True):
            self.assertIsNone(mod._slow_query_threshold_ms())
        with patch.dict(os.environ, {"WEB_PORTAL_DB_SLOW_QUERY_MS": "-3"}, clear=True):
            self.assertIsNone(mod._slow_query_threshold_ms())
        with patch.dict(os.environ, {"WEB_PORTAL_DB_SLOW_QUERY_MS": "12.5"}, clear=True):
            self.assertEqual(mod._slow_query_threshold_ms(), 12.5)

    def test_metrics_without_request_context(self) -> None:
        mod = _load_db_timing_module("db_timing_metrics_noctx")
        with patch.object(mod, "_has_request_context", return_value=False), \
             patch.object(mod, "FLASK_G", object()):
            mod.reset_db_metrics()
            mod._record_db_time(12.5)
            self.assertEqual(mod.get_db_metrics(), (0.0, 0))

    def test_metrics_with_request_context(self) -> None:
        mod = _load_db_timing_module("db_timing_metrics_ctx")
        g_obj = SimpleNamespace()
        with patch.object(mod, "_has_request_context", return_value=True), \
             patch.object(mod, "FLASK_G", g_obj):
            self.assertEqual(mod.get_db_metrics(), (0.0, 0))
            mod.reset_db_metrics()
            self.assertEqual(g_obj.db_time_ms, 0.0)
            self.assertEqual(g_obj.db_query_count, 0)
            mod._record_db_time(12.5)
            mod._record_db_time(7.5)
            self.assertEqual(mod.get_db_metrics(), (20.0, 2))

    def test_log_slow_query_truncates_bytes(self) -> None:
        mod = _load_db_timing_module("db_timing_log")
        query = (b"a " * 500) + b"\n"
        with patch.object(mod.logger, "warning") as warning:
            mod._log_slow_query(query, 123.4)
        warning.assert_called_once()
        args = warning.call_args[0]
        self.assertIn("Slow DB query", args[0])
        self.assertAlmostEqual(args[1], 123.4, places=3)
        snippet = args[2]
        self.assertTrue(snippet.endswith("..."))
        self.assertEqual(len(snippet), 403)

    def test_timed_cursor_execute_params_and_slow_log(self) -> None:
        mod = _load_db_timing_module("db_timing_execute_params")
        cursor = DummyCursor()
        timed = mod.TimedCursor(cursor, slow_ms=50)
        with patch.object(mod.time, "perf_counter", side_effect=[10.0, 10.1]), \
             patch.object(mod, "_record_db_time") as record_time, \
             patch.object(mod, "_log_slow_query") as log_slow:
            result = timed.execute("SELECT 1", params={"a": 1}, foo="bar")
        self.assertEqual(result, "ok")
        self.assertEqual(cursor.execute_args, ("SELECT 1", {"a": 1}))
        self.assertEqual(cursor.execute_kwargs, {"foo": "bar"})
        record_time.assert_called_once()
        elapsed = record_time.call_args[0][0]
        self.assertAlmostEqual(elapsed, 100.0, places=3)
        log_slow.assert_called_once()
        self.assertEqual(log_slow.call_args[0][0], "SELECT 1")

    def test_timed_cursor_execute_positional_params(self) -> None:
        mod = _load_db_timing_module("db_timing_execute_positional")
        cursor = DummyCursor()
        timed = mod.TimedCursor(cursor, slow_ms=None)
        with patch.object(mod.time, "perf_counter", side_effect=[2.0, 2.05]), \
             patch.object(mod, "_record_db_time") as record_time, \
             patch.object(mod, "_log_slow_query") as log_slow:
            timed.execute("SELECT 2", {"x": 1}, "extra", flag=True)
        self.assertEqual(cursor.execute_args, ("SELECT 2", {"x": 1}, "extra"))
        self.assertEqual(cursor.execute_kwargs, {"flag": True})
        record_time.assert_called_once()
        log_slow.assert_not_called()

    def test_timed_cursor_execute_without_params(self) -> None:
        mod = _load_db_timing_module("db_timing_execute_no_params")
        cursor = DummyCursor()
        timed = mod.TimedCursor(cursor, slow_ms=None)
        with patch.object(mod.time, "perf_counter", side_effect=[3.0, 3.005]), \
             patch.object(mod, "_record_db_time") as record_time:
            timed.execute("SELECT 3")
        self.assertEqual(cursor.execute_args, ("SELECT 3",))
        self.assertEqual(cursor.execute_kwargs, {})
        record_time.assert_called_once()

    def test_timed_cursor_executemany_logs_slow_query(self) -> None:
        mod = _load_db_timing_module("db_timing_executemany")
        cursor = DummyCursor()
        timed = mod.TimedCursor(cursor, slow_ms=10)
        with patch.object(mod.time, "perf_counter", side_effect=[1.0, 1.02]), \
             patch.object(mod, "_record_db_time") as record_time, \
             patch.object(mod, "_log_slow_query") as log_slow:
            result = timed.executemany("INSERT", [(1,), (2,)], batch=True)
        self.assertEqual(result, "many")
        self.assertEqual(cursor.executemany_args, ("INSERT", [(1,), (2,)]))
        self.assertEqual(cursor.executemany_kwargs, {"batch": True})
        record_time.assert_called_once()
        log_slow.assert_called_once()
        self.assertEqual(log_slow.call_args[0][0], "INSERT")

    def test_timed_cursor_contextmanager_and_getattr(self) -> None:
        mod = _load_db_timing_module("db_timing_context")
        cursor = DummyCursor()
        ctx = DummyCursorContext(cursor)
        conn = DummyConn(ctx)
        with patch.object(mod, "_slow_query_threshold_ms", return_value=33.3):
            with mod.timed_cursor(conn, row_factory="row") as cur:
                self.assertTrue(ctx.entered)
                self.assertIs(cur._cursor, cursor)
                self.assertEqual(cur._slow_ms, 33.3)
                self.assertEqual(cur.some_attr, "value")
        self.assertTrue(ctx.exited)
        self.assertEqual(conn.cursor_kwargs, {"row_factory": "row"})


if __name__ == "__main__":
    unittest.main()
