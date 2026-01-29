import json
import os
import sys
import types
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from unittest.mock import Mock, patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

def _load_portal_db_fallback() -> types.ModuleType:
    root = Path(__file__).resolve().parents[1]
    src_path = root / "src"
    web_portal_path = src_path / "web_portal"
    for key in list(sys.modules):
        if key == "src.web_portal" or key.startswith("src.web_portal."):
            sys.modules.pop(key, None)

    src_pkg = sys.modules.get("src")
    if src_pkg is None or not getattr(src_pkg, "__path__", None):
        src_pkg = types.ModuleType("src")
        src_pkg.__path__ = [str(src_path)]
        sys.modules["src"] = src_pkg
    else:
        src_root = str(src_path)
        if src_root not in src_pkg.__path__:
            src_pkg.__path__.append(src_root)

    portal_pkg = types.ModuleType("src.web_portal")
    portal_pkg.__path__ = [str(web_portal_path)]
    portal_pkg.__file__ = str(web_portal_path / "__init__.py")
    sys.modules["src.web_portal"] = portal_pkg

    spec = spec_from_file_location("src.web_portal.db", web_portal_path / "db.py")
    assert spec and spec.loader
    module = module_from_spec(spec)
    sys.modules["src.web_portal.db"] = module
    spec.loader.exec_module(module)
    return module


try:
    db = importlib.import_module("src.web_portal.db")
    db_pool = importlib.import_module("src.web_portal.db_pool")
    db_snapshot = importlib.import_module("src.web_portal.db_snapshot")
    db_events = importlib.import_module("src.web_portal.db_events")
except ModuleNotFoundError as exc:
    if exc.name != "flask":
        raise
    db = _load_portal_db_fallback()
    db_pool = importlib.import_module("src.web_portal.db_pool")
    db_snapshot = importlib.import_module("src.web_portal.db_snapshot")
    db_events = importlib.import_module("src.web_portal.db_events")


class FakeCursor:
    def __init__(self, row=None, rows=None):
        self._row = row
        self._rows = list(rows or [])
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._row

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@contextmanager
def fake_timed_cursor(row=None, rows=None):
    yield FakeCursor(row=row, rows=rows)


@contextmanager
def fake_connection_context(conn):
    yield conn


class FakeConn:
    def __init__(self, autocommit=False):
        self.autocommit = autocommit
        self.commit_calls = 0
        self.rollback_calls = 0

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1


class DummyFilters:
    def __init__(
        self,
        *,
        search=None,
        categories=None,
        strike_period=None,
        status=None,
        close_window_hours=None,
        sort=None,
        order=None,
    ):
        self.search = search
        self.categories = categories
        self.strike_period = strike_period
        self.status = status
        self.close_window_hours = close_window_hours
        self.sort = sort
        self.order = order


class TestConnectionPoolFallback(unittest.TestCase):
    def test_fallback_pool_init_raises(self) -> None:
        missing = object()
        saved = sys.modules.get("psycopg_pool", missing)
        original_import = importlib.import_module

        def fake_import(name, *args, **kwargs):
            if name == "psycopg_pool":
                raise ImportError("psycopg_pool unavailable")
            return original_import(name, *args, **kwargs)

        try:
            with patch("importlib.import_module", side_effect=fake_import):
                importlib.reload(db_pool)
                with self.assertRaises(RuntimeError):
                    db_pool.ConnectionPool("db")
        finally:
            if saved is missing:
                sys.modules.pop("psycopg_pool", None)
            else:
                sys.modules["psycopg_pool"] = saved
            importlib.reload(db_pool)

    def test_fallback_pool_methods(self) -> None:
        if db_pool._PSYCOPG_POOL is not None:
            self.skipTest("psycopg_pool available; fallback branch not active")
        inst = object.__new__(db_pool.ConnectionPool)
        self.assertIsNone(inst.close())
        self.assertFalse(inst.is_open())
        timeout = db_pool.PoolTimeout("oops")
        self.assertTrue(timeout.is_timeout())
        self.assertEqual(timeout.status(), "unavailable")

    def test_real_pool_methods_with_stub(self) -> None:
        stub = types.ModuleType("psycopg_pool")

        class StubPool:
            def __init__(self, *_args, **_kwargs):
                self.closed = False

        class StubTimeout(Exception):
            pass

        stub.ConnectionPool = StubPool
        stub.PoolTimeout = StubTimeout
        missing = object()
        saved = sys.modules.get("psycopg_pool", missing)
        sys.modules["psycopg_pool"] = stub
        try:
            importlib.reload(db_pool)
            pool = db_pool.ConnectionPool("db")
            self.assertTrue(pool.is_open())
            pool.closed = True
            self.assertFalse(pool.is_open())
            timeout = db_pool.PoolTimeout("boom")
            self.assertTrue(timeout.is_timeout())
            self.assertEqual(timeout.status(), "timeout")
        finally:
            if saved is missing:
                sys.modules.pop("psycopg_pool", None)
            else:
                sys.modules["psycopg_pool"] = saved
            importlib.reload(db_pool)


class TestPortalFlags(unittest.TestCase):
    def test_set_portal_attr_fallbacks_to_globals(self) -> None:
        with patch("src.web_portal.db_pool._portal_module", return_value=None):
            db_pool._set_portal_attr("_TEST_ATTR", 123)
        self.assertEqual(getattr(db_pool, "_TEST_ATTR"), 123)
        delattr(db_pool, "_TEST_ATTR")

    def test_snapshot_flags_and_running_tests(self) -> None:
        with patch.dict(os.environ, {"WEB_PORTAL_DB_SNAPSHOT_INCLUDE_HEALTH": "0"}):
            self.assertFalse(db_snapshot._portal_snapshot_include_health())
        with patch.dict(os.environ, {"WEB_PORTAL_DB_SNAPSHOT_USE_STATUS": "0"}):
            self.assertFalse(db_snapshot._portal_snapshot_use_status())
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": "1"}):
            self.assertTrue(db_snapshot._running_tests())
        with patch.dict(os.environ, {}, clear=True), patch.dict(sys.modules, {"pytest": object()}):
            self.assertTrue(db_snapshot._running_tests())


class TestSnapshotHelpers(unittest.TestCase):
    def test_snapshot_categories_none(self) -> None:
        filters = DummyFilters()
        self.assertIsNone(db_snapshot._snapshot_categories(filters))

    def test_decode_payload_none(self) -> None:
        self.assertIsNone(db_snapshot._decode_portal_snapshot_payload(None))

    def test_logger_passthrough(self) -> None:
        sentinel = object()
        with patch("src.web_portal.db_snapshot._portal_logger", return_value=sentinel) as portal_logger:
            self.assertIs(db_snapshot._logger(), sentinel)
        portal_logger.assert_called_once_with(db_snapshot.__name__)

    def test_portal_snapshot_public_wrappers(self) -> None:
        with patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_require", return_value=False), \
             patch("src.web_portal.db_snapshot._portal_snapshot_auto_init", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_include_health", return_value=False), \
             patch("src.web_portal.db_snapshot._portal_snapshot_use_status", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_refresh_sec", return_value=42):
            self.assertTrue(db_snapshot.portal_snapshot_enabled())
            self.assertFalse(db_snapshot.portal_snapshot_require())
            self.assertTrue(db_snapshot.portal_snapshot_auto_init())
            self.assertFalse(db_snapshot.portal_snapshot_include_health())
            self.assertTrue(db_snapshot.portal_snapshot_use_status())
            self.assertEqual(db_snapshot.portal_snapshot_refresh_sec(), 42)

    def test_portal_snapshot_refresh_on_queue(self) -> None:
        with patch.dict(os.environ, {"WEB_PORTAL_DB_SNAPSHOT_REFRESH_ON_QUEUE": "1"}):
            self.assertTrue(db_snapshot.portal_snapshot_refresh_on_queue())
        with patch.dict(os.environ, {"WEB_PORTAL_DB_SNAPSHOT_REFRESH_ON_QUEUE": "0"}):
            self.assertFalse(db_snapshot.portal_snapshot_refresh_on_queue())


class TestPortalDbWrappers(unittest.TestCase):
    def test_portal_snapshot_flag_wrappers(self) -> None:
        cases = [
            ("_portal_snapshot_enabled", "_portal_snapshot_enabled_impl", True, ()),
            ("_portal_snapshot_require", "_portal_snapshot_require_impl", False, ()),
            ("_portal_snapshot_auto_init", "_portal_snapshot_auto_init_impl", True, ()),
            ("_portal_snapshot_include_health", "_portal_snapshot_include_health_impl", False, ()),
            ("_portal_snapshot_use_status", "_portal_snapshot_use_status_impl", True, ()),
            ("_portal_snapshot_refresh_sec", "_portal_snapshot_refresh_sec_impl", 15, ()),
            ("_portal_snapshot_refresh_on_queue", "_portal_snapshot_refresh_on_queue_impl", False, ()),
        ]
        for wrapper_name, impl_name, return_value, args in cases:
            with self.subTest(wrapper=wrapper_name):
                with patch(f"src.web_portal.db.{impl_name}", return_value=return_value) as impl:
                    wrapper = getattr(db, wrapper_name)
                    self.assertEqual(wrapper(*args), return_value)
                impl.assert_called_once_with(*args)

    def test_portal_snapshot_connection_wrappers(self) -> None:
        conn = object()
        cases = [
            ("_portal_snapshot_table_exists", "_portal_snapshot_table_exists_impl", True),
            ("_portal_snapshot_function_exists", "_portal_snapshot_function_exists_impl", False),
            ("_ensure_portal_snapshot_schema", "_ensure_portal_snapshot_schema_impl", True),
        ]
        for wrapper_name, impl_name, return_value in cases:
            with self.subTest(wrapper=wrapper_name):
                with patch(f"src.web_portal.db.{impl_name}", return_value=return_value) as impl:
                    wrapper = getattr(db, wrapper_name)
                    self.assertEqual(wrapper(conn), return_value)
                impl.assert_called_once_with(conn)

    def test_portal_snapshot_action_wrappers(self) -> None:
        with patch("src.web_portal.db._finish_portal_snapshot_refresh_impl", return_value=None) as impl:
            self.assertIsNone(db._finish_portal_snapshot_refresh(True))
        impl.assert_called_once_with(True)

        with patch("src.web_portal.db._refresh_portal_snapshot_impl", return_value=True) as impl:
            self.assertTrue(db._refresh_portal_snapshot("manual"))
        impl.assert_called_once_with("manual")


class TestPortalSnapshotChecks(unittest.TestCase):
    def test_portal_snapshot_table_exists(self) -> None:
        with patch("src.web_portal.db_snapshot.timed_cursor", side_effect=lambda *_a, **_k: fake_timed_cursor(row=None)):
            self.assertFalse(db_snapshot._portal_snapshot_table_exists(object()))
        with patch("src.web_portal.db_snapshot.timed_cursor", side_effect=lambda *_a, **_k: fake_timed_cursor(row=("r",))):
            self.assertTrue(db_snapshot._portal_snapshot_table_exists(object()))
        with patch("src.web_portal.db_snapshot.timed_cursor", side_effect=lambda *_a, **_k: fake_timed_cursor(row=("x",))):
            self.assertFalse(db_snapshot._portal_snapshot_table_exists(object()))

    def test_portal_snapshot_function_exists(self) -> None:
        with patch("src.web_portal.db_snapshot.timed_cursor", side_effect=lambda *_a, **_k: fake_timed_cursor(row=(None,))):
            self.assertFalse(db_snapshot._portal_snapshot_function_exists(object()))
        with patch("src.web_portal.db_snapshot.timed_cursor", side_effect=lambda *_a, **_k: fake_timed_cursor(row=("fn",))):
            self.assertTrue(db_snapshot._portal_snapshot_function_exists(object()))

    def test_portal_snapshot_table_function_wrappers(self) -> None:
        dummy_conn = object()
        with patch("src.web_portal.db_snapshot._portal_snapshot_table_exists", return_value=True) as table_exists:
            self.assertTrue(db_snapshot.portal_snapshot_table_exists(dummy_conn))
        table_exists.assert_called_once_with(dummy_conn)
        with patch("src.web_portal.db_snapshot._portal_snapshot_function_exists", return_value=False) as func_exists:
            self.assertFalse(db_snapshot.portal_snapshot_function_exists(dummy_conn))
        func_exists.assert_called_once_with(dummy_conn)

    def test_ensure_portal_snapshot_ready_disabled(self) -> None:
        with patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=False):
            self.assertFalse(db_snapshot.ensure_portal_snapshot_ready())

    def test_ensure_portal_snapshot_ready_no_db(self) -> None:
        logger = Mock()
        with patch.dict(os.environ, {}, clear=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_require", return_value=False), \
             patch("src.web_portal.db_snapshot._logger", return_value=logger):
            self.assertFalse(db_snapshot.ensure_portal_snapshot_ready())
        logger.warning.assert_called()

    def test_ensure_portal_snapshot_ready_require_raises(self) -> None:
        with patch.dict(os.environ, {}, clear=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_require", return_value=True):
            with self.assertRaises(RuntimeError):
                db_snapshot.ensure_portal_snapshot_ready()

    def test_ensure_portal_snapshot_ready_warns_when_missing(self) -> None:
        logger = Mock()
        dummy_conn = object()
        with patch.dict(os.environ, {"DATABASE_URL": "db"}), \
             patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_require", return_value=False), \
             patch("src.web_portal.db_snapshot._portal_snapshot_auto_init", return_value=False), \
             patch("src.web_portal.db_snapshot._portal_snapshot_table_exists", return_value=False), \
             patch("src.web_portal.db_snapshot._portal_snapshot_function_exists", return_value=False), \
             patch("src.web_portal.db_snapshot._db_connection", return_value=fake_connection_context(dummy_conn)), \
             patch("src.web_portal.db_snapshot._logger", return_value=logger):
            db_snapshot._PORTAL_SNAPSHOT_READY_WARNED = False
            self.assertFalse(db_snapshot.ensure_portal_snapshot_ready())
        logger.warning.assert_called()

    def test_ensure_portal_snapshot_ready_exception(self) -> None:
        logger = Mock()
        with patch.dict(os.environ, {"DATABASE_URL": "db"}), \
             patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_require", return_value=False), \
             patch("src.web_portal.db_snapshot._db_connection", side_effect=RuntimeError("boom")), \
             patch("src.web_portal.db_snapshot._logger", return_value=logger):
            db_snapshot._PORTAL_SNAPSHOT_READY_WARNED = False
            self.assertFalse(db_snapshot.ensure_portal_snapshot_ready())
        logger.warning.assert_called()

    def test_ensure_portal_snapshot_schema_paths(self) -> None:
        conn = FakeConn(autocommit=False)
        logger = Mock()
        with patch("src.web_portal.db_snapshot._logger", return_value=logger), \
             patch("src.web_portal.db_snapshot._portal_snapshot_table_exists", return_value=True):
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_CHECKED = False
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_READY = False
            self.assertTrue(db_snapshot._ensure_portal_snapshot_schema(conn))

        with patch("src.web_portal.db_snapshot._portal_snapshot_table_exists", return_value=False), \
             patch("src.web_portal.db_snapshot.maybe_init_schema", side_effect=RuntimeError("bad")), \
             patch("src.web_portal.db_snapshot._logger", return_value=logger):
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_CHECKED = False
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_READY = False
            self.assertFalse(db_snapshot._ensure_portal_snapshot_schema(conn))
            self.assertEqual(conn.rollback_calls, 1)

        with patch("src.web_portal.db_snapshot._portal_snapshot_table_exists", return_value=False), \
             patch("src.web_portal.db_snapshot.maybe_init_schema", return_value=None), \
             patch("src.web_portal.db_snapshot._logger", return_value=logger):
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_CHECKED = False
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_READY = False
            self.assertFalse(db_snapshot._ensure_portal_snapshot_schema(conn))
            logger.warning.assert_called()


class TestConnectionsAndRefresh(unittest.TestCase):
    def test_managed_connection_requires_conn(self) -> None:
        with self.assertRaises(RuntimeError):
            with db_pool._managed_connection(None, autocommit=True):
                pass

    def test_direct_connection_raises_on_none(self) -> None:
        with patch("src.web_portal.db_pool.psycopg.connect", return_value=None) as mocked:
            with self.assertRaises(RuntimeError):
                with db_pool._direct_connection("postgres://example", autocommit=True, connect_timeout=None):
                    pass
        mocked.assert_called_once_with("postgres://example")

    def test_refresh_portal_snapshot_raises_when_schema_missing(self) -> None:
        dummy_conn = object()
        with patch("src.web_portal.db_snapshot._db_connection", return_value=fake_connection_context(dummy_conn)), \
             patch("src.web_portal.db_snapshot._ensure_portal_snapshot_schema", return_value=False):
            with self.assertRaises(RuntimeError):
                db_snapshot.refresh_portal_snapshot()

    def test_finish_portal_snapshot_refresh_updates_last(self) -> None:
        with patch("src.web_portal.db_snapshot.time.monotonic", return_value=123.0):
            db_snapshot._PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT = True
            db_snapshot._PORTAL_SNAPSHOT_REFRESH_LAST = 0.0
            db_snapshot._finish_portal_snapshot_refresh(True)
        self.assertFalse(db_snapshot._PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT)
        self.assertEqual(db_snapshot._PORTAL_SNAPSHOT_REFRESH_LAST, 123.0)

    def test_refresh_portal_snapshot_error_path(self) -> None:
        logger = Mock()
        with patch("src.web_portal.db_snapshot.refresh_portal_snapshot", side_effect=RuntimeError("boom")), \
             patch("src.web_portal.db_snapshot._logger", return_value=logger):
            db_snapshot._PORTAL_SNAPSHOT_REFRESH_WARNED = False
            self.assertFalse(db_snapshot._refresh_portal_snapshot("manual"))
        logger.warning.assert_called()

    def test_refresh_portal_snapshot_success(self) -> None:
        with patch("src.web_portal.db_snapshot.refresh_portal_snapshot", return_value=None):
            self.assertTrue(db_snapshot._refresh_portal_snapshot("manual"))


class TestMaybeRefreshPortalSnapshot(unittest.TestCase):
    def test_maybe_refresh_portal_snapshot_paths(self) -> None:
        with patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=False):
            self.assertFalse(db_snapshot.maybe_refresh_portal_snapshot(reason="manual"))

        with patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_refresh_on_queue", return_value=False):
            self.assertFalse(db_snapshot.maybe_refresh_portal_snapshot(reason="queue"))

        db_snapshot._PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT = True
        with patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=True):
            self.assertFalse(db_snapshot.maybe_refresh_portal_snapshot(reason="manual"))
        db_snapshot._PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT = False

        with patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_refresh_sec", return_value=9999), \
             patch("src.web_portal.db_snapshot.time.monotonic", return_value=1000.0):
            db_snapshot._PORTAL_SNAPSHOT_REFRESH_LAST = 999.5
            self.assertFalse(db_snapshot.maybe_refresh_portal_snapshot(reason="manual"))

        class FakeThread:
            def __init__(self, target=None, args=(), name=None, daemon=None):
                self.target = target
                self.args = args
                self.name = name
                self.daemon = daemon
                self.started = False

            def start(self):
                self.started = True

        with patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_refresh_sec", return_value=0), \
             patch("src.web_portal.db_snapshot.time.monotonic", return_value=1000.0), \
             patch("src.web_portal.db_snapshot.threading.Thread", side_effect=FakeThread) as thread_cls:
            db_snapshot._PORTAL_SNAPSHOT_REFRESH_LAST = 0.0
            db_snapshot._PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT = False
            self.assertTrue(db_snapshot.maybe_refresh_portal_snapshot(reason="manual", background=True))
        thread_cls.assert_called_once()

        with patch("src.web_portal.db_snapshot._portal_snapshot_enabled", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_refresh_sec", return_value=0), \
             patch("src.web_portal.db_snapshot.time.monotonic", return_value=1000.0), \
             patch("src.web_portal.db_snapshot._refresh_portal_snapshot", return_value=True):
            db_snapshot._PORTAL_SNAPSHOT_REFRESH_LAST = 0.0
            db_snapshot._PORTAL_SNAPSHOT_REFRESH_IN_FLIGHT = False
            self.assertTrue(db_snapshot.maybe_refresh_portal_snapshot(reason="manual", background=False))


class TestSnapshotPayloads(unittest.TestCase):
    def test_snapshot_status_columns_available(self) -> None:
        with patch(
            "src.web_portal.db_snapshot.timed_cursor",
            side_effect=lambda *_a, **_k: fake_timed_cursor(row=(4,)),
        ):
            self.assertTrue(
                db_snapshot._portal_snapshot_status_columns_available(object())
            )

        with patch(
            "src.web_portal.db_snapshot.timed_cursor",
            side_effect=lambda *_a, **_k: fake_timed_cursor(row=(3,)),
        ):
            self.assertFalse(
                db_snapshot._portal_snapshot_status_columns_available(object())
            )

    def test_fetch_portal_snapshot(self) -> None:
        filters = DummyFilters(search=None, categories=("Sports",), strike_period=None, status=None, close_window_hours=None, sort=None, order=None)
        payload_dict = {"ok": True}
        payload_json = json.dumps(payload_dict)
        cursor = db_events.EventCursor(value="v", event_ticker="E1")
        with patch("src.web_portal.db_snapshot._ensure_portal_snapshot_schema", return_value=True), \
             patch("src.web_portal.db_snapshot.timed_cursor", side_effect=lambda *_a, **_k: fake_timed_cursor(row=(payload_json,))):
            result = db_snapshot.fetch_portal_snapshot(object(), 5, filters, cursors=(cursor, None, None))
        self.assertEqual(result, payload_dict)

        with patch("src.web_portal.db_snapshot._ensure_portal_snapshot_schema", return_value=True), \
             patch("src.web_portal.db_snapshot.timed_cursor", side_effect=lambda *_a, **_k: fake_timed_cursor(row=(payload_dict,))):
            result = db_snapshot.fetch_portal_snapshot(object(), 5, filters)
        self.assertEqual(result, payload_dict)

        with patch("src.web_portal.db_snapshot._ensure_portal_snapshot_schema", return_value=False):
            self.assertIsNone(db_snapshot.fetch_portal_snapshot(object(), 5, filters))

    def test_fetch_portal_snapshot_disables_status_when_columns_missing(self) -> None:
        filters = DummyFilters(search=None, categories=None, strike_period=None, status=None, close_window_hours=None, sort=None, order=None)
        payload_dict = {"ok": True}
        cursor = FakeCursor(row=(payload_dict,))

        @contextmanager
        def cursor_ctx(*_args, **_kwargs):
            yield cursor

        with patch("src.web_portal.db_snapshot._ensure_portal_snapshot_schema", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_use_status", return_value=True), \
             patch("src.web_portal.db_snapshot._portal_snapshot_status_columns_available", return_value=False), \
             patch("src.web_portal.db_snapshot.timed_cursor", side_effect=cursor_ctx):
            result = db_snapshot.fetch_portal_snapshot(object(), 5, filters)

        self.assertEqual(result, payload_dict)
        self.assertTrue(cursor.executed)
        _, params = cursor.executed[0]
        self.assertFalse(params[-1])


class TestPortalSnapshotSchemaCache(unittest.TestCase):
    def test_cached_schema_ready_short_circuit(self) -> None:
        prev_checked = db_snapshot._PORTAL_SNAPSHOT_SCHEMA_CHECKED
        prev_ready = db_snapshot._PORTAL_SNAPSHOT_SCHEMA_READY
        try:
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_CHECKED = True
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_READY = True
            with patch("src.web_portal.db_snapshot._portal_snapshot_table_exists", side_effect=AssertionError("should not be called")):
                self.assertTrue(db_snapshot._ensure_portal_snapshot_schema(object()))
        finally:
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_CHECKED = prev_checked
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_READY = prev_ready

    def test_cached_schema_ready_within_lock(self) -> None:
        class FakeLock:
            def __enter__(self):
                db_snapshot._PORTAL_SNAPSHOT_SCHEMA_CHECKED = True
                db_snapshot._PORTAL_SNAPSHOT_SCHEMA_READY = False
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        prev_checked = db_snapshot._PORTAL_SNAPSHOT_SCHEMA_CHECKED
        prev_ready = db_snapshot._PORTAL_SNAPSHOT_SCHEMA_READY
        prev_lock = db_snapshot._PORTAL_SNAPSHOT_SCHEMA_LOCK
        try:
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_CHECKED = False
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_READY = True
            with patch("src.web_portal.db_snapshot._PORTAL_SNAPSHOT_SCHEMA_LOCK", new=FakeLock()), \
                 patch("src.web_portal.db_snapshot._portal_snapshot_table_exists", side_effect=AssertionError("should not be called")):
                self.assertFalse(db_snapshot._ensure_portal_snapshot_schema(object()))
        finally:
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_CHECKED = prev_checked
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_READY = prev_ready
            db_snapshot._PORTAL_SNAPSHOT_SCHEMA_LOCK = prev_lock

    def test_ensure_portal_snapshot_schema_wrapper(self) -> None:
        with patch("src.web_portal.db_snapshot._ensure_portal_snapshot_schema", return_value=True) as ensure_schema:
            self.assertTrue(db_snapshot.ensure_portal_snapshot_schema(object()))
        ensure_schema.assert_called_once()

    def test_refresh_wrappers(self) -> None:
        with patch("src.web_portal.db_snapshot._finish_portal_snapshot_refresh") as finish_refresh:
            db_snapshot.finish_portal_snapshot_refresh(True)
        finish_refresh.assert_called_once_with(True)
        with patch("src.web_portal.db_snapshot._refresh_portal_snapshot", return_value=True) as refresh:
            self.assertTrue(db_snapshot.refresh_portal_snapshot_internal("manual"))
        refresh.assert_called_once_with("manual")


class TestCursorHelpers(unittest.TestCase):
    def test_normalize_cursor_value(self) -> None:
        self.assertIsNone(db_events._normalize_cursor_value(None))
        naive = datetime(2024, 1, 1, 0, 0, 0)
        normalized = db_events._normalize_cursor_value(naive)
        self.assertTrue(str(normalized).endswith("+00:00"))
        aware = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(db_events._normalize_cursor_value(aware), aware.isoformat())

    def test_cursor_from_rows(self) -> None:
        self.assertIsNone(db_events._cursor_from_rows([], "close_time"))
        self.assertIsNone(db_events._cursor_from_rows([{ "event_title": "x"}], "close_time"))
        row = {
            "event_ticker": "EVT",
            "close_time": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        }
        cursor = db_events._cursor_from_rows([row], "close_time")
        self.assertIsNotNone(cursor)
        self.assertEqual(cursor.event_ticker, "EVT")

    def test_build_cursor_clause(self) -> None:
        self.assertEqual(db_events._build_cursor_clause("close_time", "desc", None), ("", []))
        cursor = db_events.EventCursor(value=None, event_ticker="EVT")
        clause, params = db_events._build_cursor_clause("close_time", "desc", cursor)
        self.assertIn("event_ticker", clause)
        self.assertEqual(params, ["EVT"])
        cursor = db_events.EventCursor(value="v", event_ticker="EVT")
        clause, params = db_events._build_cursor_clause("close_time", "asc", cursor)
        self.assertIn("close_time", clause)
        self.assertEqual(params, ["v", "v", "EVT"])


class TestEventHelperWrappers(unittest.TestCase):
    def test_set_timed_cursor_override(self) -> None:
        sentinel = object()

        def fake_cursor(_conn, **_kwargs):
            return sentinel

        original = db_events._TIMED_CURSOR
        try:
            db_events.set_timed_cursor(fake_cursor)
            self.assertIs(db_events.timed_cursor(object()), sentinel)
        finally:
            db_events.set_timed_cursor(original)

    def test_event_open_window_sql_fallback(self) -> None:
        original = db_events.EVENT_OPEN_WINDOW_SQL
        try:
            delattr(db_events, "EVENT_OPEN_WINDOW_SQL")
            fallback = db_events._event_open_window_sql()
        finally:
            setattr(db_events, "EVENT_OPEN_WINDOW_SQL", original)
        self.assertIn("MIN(m.open_time)", fallback)
        self.assertIn("MAX(m.close_time)", fallback)

    def test_build_event_where_wrapper(self) -> None:
        filters = DummyFilters()
        with patch.object(db_events, "_build_event_where", return_value=("SQL", [1])) as helper:
            result = db_events.build_event_where(filters, include_category=False)
        self.assertEqual(result, ("SQL", [1]))
        helper.assert_called_once_with(filters, include_category=False)

    def test_build_cursor_clause_wrapper(self) -> None:
        cursor = db_events.EventCursor(value=None, event_ticker="EVT")
        with patch.object(db_events, "_build_cursor_clause", return_value=("SQL", [1])) as helper:
            result = db_events.build_cursor_clause("close_time", "asc", cursor)
        self.assertEqual(result, ("SQL", [1]))
        helper.assert_called_once_with("close_time", "asc", cursor)

    def test_build_order_by_wrapper(self) -> None:
        with patch.object(
            db_events, "_build_order_by", return_value=("key", "asc", "col", "ORDER")
        ) as helper:
            result = db_events.build_order_by("close_time", "desc", "open_time", "asc")
        self.assertEqual(result, ("key", "asc", "col", "ORDER"))
        helper.assert_called_once_with("close_time", "desc", "open_time", "asc")

    def test_fetch_event_count_wrapper(self) -> None:
        filters = DummyFilters()
        with patch.object(db_events, "_fetch_event_count", return_value=7) as helper:
            conn = object()
            result = db_events.fetch_event_count(conn, "active", filters)
        self.assertEqual(result, 7)
        helper.assert_called_once_with(conn, "active", filters)


class TestClosedFilledCount(unittest.TestCase):
    def test_fetch_closed_filled_count_executes_query(self) -> None:
        cursor = FakeCursor(row=(4,))

        @contextmanager
        def fake_cursor(_conn):
            yield cursor

        filters = DummyFilters()
        with patch.object(db_events, "timed_cursor", side_effect=fake_cursor), \
             patch.object(
                 db_events,
                 "_build_event_where",
                 return_value=(" AND e.category = %s", ["Sports"]),
             ), \
             patch.object(
                 db_events,
                 "_closed_event_having",
                 return_value=("HAVING COUNT(*) > 0", [1]),
             ), \
             patch.object(db_events, "_env_int", side_effect=[2, 90]):
            result = db_events.fetch_closed_filled_count(object(), filters)

        self.assertEqual(result, 4)
        self.assertEqual(len(cursor.executed), 1)
        query, params = cursor.executed[0]
        self.assertIn("WITH closed_events AS", query)
        self.assertIn("event_missing", query)
        self.assertEqual(
            params,
            ("Sports", 1, 2, 2, 90, 90, 2, 2, 90, 90),
        )


class TestEventFetchers(unittest.TestCase):
    def _row(self, close_time=None, open_time=None):
        return {
            "event_title": "Event",
            "event_ticker": "EVT",
            "open_time": open_time,
            "close_time": close_time,
            "market_count": 1,
            "volume": 2,
            "agent_yes_prob": None,
            "agent_confidence": None,
            "agent_prediction_ts": None,
            "agent_market_ticker": "",
        }

    def test_fetch_active_events_return_cursor(self) -> None:
        row = self._row(close_time=datetime.now(timezone.utc))
        filters = DummyFilters()
        with patch("src.web_portal.db_events.timed_cursor", side_effect=lambda *_a, **_k: fake_timed_cursor(rows=[row])), \
             patch("src.web_portal.db_events._portal_func", side_effect=lambda _n, fallback: fallback):
            payload, cursor = db_events.fetch_active_events(
                object(),
                5,
                filters,
                return_cursor=True,
            )
        self.assertEqual(len(payload), 1)
        self.assertIsNotNone(cursor)

    def test_fetch_scheduled_events_return_cursor(self) -> None:
        row = self._row(open_time=datetime.now(timezone.utc) + timedelta(hours=1))
        filters = DummyFilters()
        with patch("src.web_portal.db_events.timed_cursor", side_effect=lambda *_a, **_k: fake_timed_cursor(rows=[row])):
            payload, cursor = db_events.fetch_scheduled_events(
                object(),
                5,
                filters,
                return_cursor=True,
            )
        self.assertEqual(len(payload), 1)
        self.assertIsNotNone(cursor)

    def test_fetch_closed_events_return_cursor(self) -> None:
        row = self._row(close_time=datetime.now(timezone.utc) - timedelta(hours=1))
        filters = DummyFilters()
        with patch("src.web_portal.db_events.timed_cursor", side_effect=lambda *_a, **_k: fake_timed_cursor(rows=[row])):
            payload, cursor = db_events.fetch_closed_events(
                object(),
                5,
                filters,
                return_cursor=True,
            )
        self.assertEqual(len(payload), 1)
        self.assertIsNotNone(cursor)


if __name__ == "__main__":
    unittest.main()
