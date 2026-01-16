import json
import os
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from _test_utils import add_src_to_path

add_src_to_path()

import src.kalshi.kalshi_sdk as kalshi_sdk
import src.kalshi.kalshi_rest_rate_limit as rest_rate_limit


class StopLoop(Exception):
    pass


class FakeCursor:
    def __init__(self, row=None, raise_on_execute=False):
        self.row = row
        self.raise_on_execute = raise_on_execute
        self.executes = []

    def execute(self, sql, params=None):
        if self.raise_on_execute:
            raise RuntimeError("execute failed")
        self.executes.append((sql, params))

    def fetchone(self):
        return self.row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, row=None, raise_on_execute=False):
        self.cursor_obj = FakeCursor(row=row, raise_on_execute=raise_on_execute)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FailingConn(FakeConn):
    def transaction(self):
        raise RuntimeError("transaction failed")


class KalshiSdkTestCase(unittest.TestCase):
    def setUp(self) -> None:
        rest_rate_limit._REST_RATE_LIMIT_BACKEND_OVERRIDE = None
        rest_rate_limit._REST_RATE_LIMIT_DB_URL_OVERRIDE = None
        rest_rate_limit._REST_RATE_LIMIT_WARNED.clear()
        rest_rate_limit._DB_RATE_LIMIT_ERROR_TS = 0.0
        kalshi_sdk._LAST_HOST_OVERRIDE = None
        rest_rate_limit._REST_NEXT_ALLOWED = 0.0
        rest_rate_limit._REST_LAST_REFILL = 0.0
        rest_rate_limit._REST_TOKENS = 0.0
        rest_rate_limit._CANDLE_NEXT_ALLOWED = 0.0
        if hasattr(rest_rate_limit._DB_RATE_LIMIT_LOCAL, "conn"):
            rest_rate_limit._DB_RATE_LIMIT_LOCAL.conn = None


class TestRestLimits(KalshiSdkTestCase):
    def test_rest_limits_invalid_rate_and_min_burst(self) -> None:
        with patch.dict(
            os.environ,
            {"KALSHI_REST_RATE_PER_SEC": "bad", "KALSHI_REST_BURST": "0"},
        ):
            rate, burst = rest_rate_limit._rest_limits()
        self.assertEqual(rate, 0.0)
        self.assertEqual(burst, 1)

    def test_rest_limits_invalid_burst(self) -> None:
        with patch.dict(
            os.environ,
            {"KALSHI_REST_RATE_PER_SEC": "1.5", "KALSHI_REST_BURST": "bad"},
        ):
            rate, burst = rest_rate_limit._rest_limits()
        self.assertEqual(rate, 1.5)
        self.assertEqual(burst, 5)

    def test_rest_rate_limit_db_url_missing(self) -> None:
        with patch.object(rest_rate_limit, "_REST_RATE_LIMIT_DB_URL_OVERRIDE", None), \
             patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(rest_rate_limit._rest_rate_limit_db_url())

    def test_rest_rate_limit_db_url_blank(self) -> None:
        with patch.object(rest_rate_limit, "_REST_RATE_LIMIT_DB_URL_OVERRIDE", None), \
             patch.dict(os.environ, {"KALSHI_REST_RATE_LIMIT_DB_URL": "   "}):
            self.assertIsNone(rest_rate_limit._rest_rate_limit_db_url())

    def test_configure_rest_rate_limit_overrides(self) -> None:
        kalshi_sdk.configure_rest_rate_limit(backend=" DB ", db_url=" url ")
        self.assertEqual(rest_rate_limit._REST_RATE_LIMIT_BACKEND_OVERRIDE, "db")
        self.assertEqual(rest_rate_limit._REST_RATE_LIMIT_DB_URL_OVERRIDE, "url")
        kalshi_sdk.configure_rest_rate_limit(backend="", db_url="")
        self.assertIsNone(rest_rate_limit._REST_RATE_LIMIT_BACKEND_OVERRIDE)
        self.assertIsNone(rest_rate_limit._REST_RATE_LIMIT_DB_URL_OVERRIDE)

    def test_rest_rate_limit_db_url_override(self) -> None:
        rest_rate_limit._REST_RATE_LIMIT_DB_URL_OVERRIDE = "override"
        with patch.dict(os.environ, {"KALSHI_REST_RATE_LIMIT_DB_URL": "env"}):
            self.assertEqual(rest_rate_limit._rest_rate_limit_db_url(), "override")

    def test_rate_limit_warn_once(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit.logger.warning") as warn:
            rest_rate_limit._rate_limit_warn_once("key", "message")
            rest_rate_limit._rate_limit_warn_once("key", "message")
        self.assertEqual(warn.call_count, 1)

    def test_rest_rate_limit_backend_memory(self) -> None:
        with patch.dict(os.environ, {"KALSHI_REST_RATE_LIMIT_BACKEND": "memory"}):
            self.assertEqual(rest_rate_limit._rest_rate_limit_backend(), "memory")

    def test_rest_rate_limit_backend_variants(self) -> None:
        with patch.dict(os.environ, {"KALSHI_REST_RATE_LIMIT_BACKEND": "redis"}), \
             patch("src.kalshi.kalshi_rest_rate_limit._rate_limit_warn_once") as warn:
            self.assertEqual(rest_rate_limit._rest_rate_limit_backend(), "memory")
        self.assertEqual(warn.call_count, 1)

        with patch.dict(os.environ, {"KALSHI_REST_RATE_LIMIT_BACKEND": "db"}):
            self.assertEqual(rest_rate_limit._rest_rate_limit_backend(), "db")

        with patch.dict(os.environ, {"KALSHI_REST_RATE_LIMIT_BACKEND": "weird"}), \
             patch("src.kalshi.kalshi_rest_rate_limit._rate_limit_warn_once") as warn:
            self.assertEqual(rest_rate_limit._rest_rate_limit_backend(), "memory")
        self.assertEqual(warn.call_count, 1)

        with patch.dict(os.environ, {}, clear=True), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_db_url", return_value="db"):
            self.assertEqual(rest_rate_limit._rest_rate_limit_backend(), "db")


class TestDbRateLimitConn(KalshiSdkTestCase):
    def test_db_rate_limit_conn_backend_memory(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_backend", return_value="memory"):
            self.assertIsNone(rest_rate_limit._db_rate_limit_conn())

    def test_db_rate_limit_conn_missing_url(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_backend", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_db_url", return_value=None), \
             patch("src.kalshi.kalshi_rest_rate_limit._rate_limit_warn_once") as warn:
            self.assertIsNone(rest_rate_limit._db_rate_limit_conn())
        self.assertEqual(warn.call_count, 1)

    def test_db_rate_limit_conn_existing(self) -> None:
        conn = SimpleNamespace(closed=False)
        rest_rate_limit._DB_RATE_LIMIT_LOCAL.conn = conn
        with patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_backend", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_db_url", return_value="db"):
            self.assertIs(rest_rate_limit._db_rate_limit_conn(), conn)

    def test_db_rate_limit_conn_import_error(self) -> None:
        orig_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "psycopg":
                raise ImportError("missing")
            return orig_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_backend", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_db_url", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._rate_limit_warn_once") as warn:
            self.assertIsNone(rest_rate_limit._db_rate_limit_conn())
        self.assertEqual(warn.call_count, 1)

    def test_db_rate_limit_conn_connect_error(self) -> None:
        class FakePsycopg:
            def connect(self, _url):
                raise RuntimeError("nope")

        with patch.dict(sys.modules, {"psycopg": FakePsycopg()}), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_backend", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_db_url", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._rate_limit_warn_once") as warn:
            self.assertIsNone(rest_rate_limit._db_rate_limit_conn())
        self.assertEqual(warn.call_count, 1)

    def test_db_rate_limit_conn_success(self) -> None:
        conn = SimpleNamespace(closed=False)

        class FakePsycopg:
            def connect(self, _url):
                return conn

        with patch.dict(sys.modules, {"psycopg": FakePsycopg()}), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_backend", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_db_url", return_value="db"):
            self.assertIs(rest_rate_limit._db_rate_limit_conn(), conn)
        self.assertIs(rest_rate_limit._DB_RATE_LIMIT_LOCAL.conn, conn)

    def test_reset_db_rate_limit_conn(self) -> None:
        rest_rate_limit._DB_RATE_LIMIT_LOCAL.conn = None
        rest_rate_limit._reset_db_rate_limit_conn()

        class BadConn:
            def close(self):
                raise RuntimeError("fail")

        rest_rate_limit._DB_RATE_LIMIT_LOCAL.conn = BadConn()
        rest_rate_limit._reset_db_rate_limit_conn()
        self.assertIsNone(rest_rate_limit._DB_RATE_LIMIT_LOCAL.conn)

    def test_to_float_fallback(self) -> None:
        self.assertEqual(rest_rate_limit._to_float("bad", 1.5), 1.5)

    def test_parse_db_rate_state_fallbacks(self) -> None:
        now = 100.0
        state = rest_rate_limit._parse_db_rate_state(None, now, burst=5)
        self.assertEqual(state["tokens"], 5.0)
        state = rest_rate_limit._parse_db_rate_state("{", now, burst=5)
        self.assertEqual(state["tokens"], 5.0)
        state = rest_rate_limit._parse_db_rate_state("[]", now, burst=5)
        self.assertEqual(state["tokens"], 5.0)
        raw = json.dumps({"tokens": 10, "last_refill_ts": 200, "next_allowed_ts": -5})
        state = rest_rate_limit._parse_db_rate_state(raw, now, burst=5)
        self.assertEqual(state["tokens"], 5.0)
        self.assertEqual(state["last_refill_ts"], now)
        self.assertEqual(state["next_allowed_ts"], 0.0)

    def test_db_rate_limit_error_throttled(self) -> None:
        with patch("src.kalshi.kalshi_sdk.time.monotonic", side_effect=[100.0, 110.0]), \
             patch("src.kalshi.kalshi_rest_rate_limit.logger.warning") as warn:
            rest_rate_limit._db_rate_limit_error(RuntimeError("fail"))
            rest_rate_limit._db_rate_limit_error(RuntimeError("fail"))
        self.assertEqual(warn.call_count, 1)


class TestDbRateLimitState(KalshiSdkTestCase):
    def test_db_rate_limit_fetch_state_default(self) -> None:
        conn = FakeConn(row=None)
        state = rest_rate_limit._db_rate_limit_fetch_state(conn, now=123.0, burst=5)
        self.assertEqual(state["tokens"], 5.0)
        self.assertEqual(len(conn.cursor_obj.executes), 2)

    def test_db_rate_limit_write_state(self) -> None:
        conn = FakeConn()
        state = {"tokens": 1.0, "last_refill_ts": 0.0, "next_allowed_ts": 0.0}
        rest_rate_limit._db_rate_limit_write_state(conn, state)
        self.assertEqual(len(conn.cursor_obj.executes), 1)


class TestDbRestLimits(KalshiSdkTestCase):
    def test_db_rest_backoff_remaining_success(self) -> None:
        raw = json.dumps(
            {"tokens": 0.0, "last_refill_ts": 1000.0, "next_allowed_ts": 1005.0}
        )
        conn = FakeConn(row=(raw,))
        with patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_conn", return_value=conn), \
             patch("src.kalshi.kalshi_sdk.time.time", return_value=1000.0):
            remaining = rest_rate_limit._db_rest_backoff_remaining()
        self.assertEqual(remaining, 5.0)

    def test_db_rest_backoff_remaining_error(self) -> None:
        conn = FakeConn(row=None, raise_on_execute=True)
        with patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_conn", return_value=conn), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_error") as log_err, \
             patch("src.kalshi.kalshi_rest_rate_limit._reset_db_rate_limit_conn") as reset:
            remaining = rest_rate_limit._db_rest_backoff_remaining()
        self.assertIsNone(remaining)
        self.assertEqual(log_err.call_count, 1)
        self.assertEqual(reset.call_count, 1)

    def test_db_rest_backoff_remaining_no_conn(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_conn", return_value=None):
            self.assertIsNone(rest_rate_limit._db_rest_backoff_remaining())

    def test_db_rest_apply_cooldown_noop(self) -> None:
        self.assertTrue(rest_rate_limit._db_rest_apply_cooldown(0))

    def test_db_rest_apply_cooldown_no_conn(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_conn", return_value=None):
            self.assertFalse(rest_rate_limit._db_rest_apply_cooldown(5.0))

    def test_db_rest_apply_cooldown_updates_state(self) -> None:
        conn = FakeConn()
        with patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_conn", return_value=conn), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_limits", return_value=(0.0, 3)), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_fetch_state", return_value={
                 "tokens": 1.0,
                 "last_refill_ts": 0.0,
                 "next_allowed_ts": 0.0,
             }) as fetch_state, \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_write_state") as write_state, \
             patch("src.kalshi.kalshi_sdk.time.time", return_value=100.0):
            result = rest_rate_limit._db_rest_apply_cooldown(5.0)
        self.assertTrue(result)
        self.assertEqual(fetch_state.call_count, 1)
        _, updated_state = write_state.call_args[0]
        self.assertEqual(updated_state["tokens"], 3.0)
        self.assertEqual(updated_state["last_refill_ts"], 100.0)

    def test_db_rest_apply_cooldown_error(self) -> None:
        conn = FailingConn()
        with patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_conn", return_value=conn), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_error") as log_err, \
             patch("src.kalshi.kalshi_rest_rate_limit._reset_db_rate_limit_conn") as reset:
            result = rest_rate_limit._db_rest_apply_cooldown(5.0)
        self.assertFalse(result)
        self.assertEqual(log_err.call_count, 1)
        self.assertEqual(reset.call_count, 1)

    def test_db_rest_wait_no_conn(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_conn", return_value=None):
            self.assertFalse(rest_rate_limit._db_rest_wait())

    def test_db_rest_wait_rate_zero(self) -> None:
        conn = FakeConn()
        state = {"tokens": 0.0, "last_refill_ts": 900.0, "next_allowed_ts": 0.0}
        with patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_conn", return_value=conn), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_limits", return_value=(0.0, 5)), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_fetch_state", return_value=state), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_write_state") as write_state, \
             patch("src.kalshi.kalshi_sdk.time.time", return_value=1000.0):
            self.assertTrue(rest_rate_limit._db_rest_wait())
        _, updated_state = write_state.call_args[0]
        self.assertEqual(updated_state["tokens"], 5.0)
        self.assertEqual(updated_state["last_refill_ts"], 1000.0)

    def test_db_rest_wait_tokens_waits(self) -> None:
        conn = FakeConn()
        state = {"tokens": 0.0, "last_refill_ts": 1000.0, "next_allowed_ts": 0.0}
        with patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_conn", return_value=conn), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_limits", return_value=(1.0, 5)), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_fetch_state", return_value=state), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_write_state"), \
             patch("src.kalshi.kalshi_sdk.time.time", return_value=1000.0), \
             patch("src.kalshi.kalshi_sdk.time.sleep", side_effect=StopLoop), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_error") as log_err, \
             patch("src.kalshi.kalshi_rest_rate_limit._reset_db_rate_limit_conn") as reset:
            result = rest_rate_limit._db_rest_wait()
        self.assertFalse(result)
        log_err.assert_called_once()
        reset.assert_called_once()

    def test_db_rest_wait_backoff(self) -> None:
        conn = FakeConn()
        state = {"tokens": 1.0, "last_refill_ts": 1000.0, "next_allowed_ts": 1010.0}
        with patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_conn", return_value=conn), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_limits", return_value=(1.0, 5)), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_fetch_state", return_value=state), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_write_state"), \
             patch("src.kalshi.kalshi_sdk.time.time", return_value=1000.0), \
             patch("src.kalshi.kalshi_sdk.time.sleep", side_effect=StopLoop), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_error") as log_err, \
             patch("src.kalshi.kalshi_rest_rate_limit._reset_db_rate_limit_conn") as reset:
            result = rest_rate_limit._db_rest_wait()
        self.assertFalse(result)
        log_err.assert_called_once()
        reset.assert_called_once()

    def test_db_rest_wait_tokens_available(self) -> None:
        conn = FakeConn()
        state = {"tokens": 2.0, "last_refill_ts": 1000.0, "next_allowed_ts": 0.0}
        with patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_conn", return_value=conn), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_limits", return_value=(1.0, 5)), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_fetch_state", return_value=state), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rate_limit_write_state") as write_state, \
             patch("src.kalshi.kalshi_sdk.time.time", return_value=1000.0):
            self.assertTrue(rest_rate_limit._db_rest_wait())
        _, updated_state = write_state.call_args[0]
        self.assertEqual(updated_state["tokens"], 1.0)


class TestRestWaitLocal(KalshiSdkTestCase):
    def test_rest_backoff_remaining_local(self) -> None:
        rest_rate_limit._REST_NEXT_ALLOWED = 100.0
        with patch("src.kalshi.kalshi_sdk.time.monotonic", return_value=90.0):
            remaining = rest_rate_limit._rest_backoff_remaining_local()
        self.assertEqual(remaining, 10.0)

    def test_rest_apply_cooldown_local_updates(self) -> None:
        rest_rate_limit._REST_NEXT_ALLOWED = 50.0
        with patch("src.kalshi.kalshi_sdk.time.monotonic", return_value=40.0):
            rest_rate_limit._rest_apply_cooldown_local(5.0)
        self.assertEqual(rest_rate_limit._REST_NEXT_ALLOWED, 50.0)
        with patch("src.kalshi.kalshi_sdk.time.monotonic", return_value=60.0):
            rest_rate_limit._rest_apply_cooldown_local(5.0)
        self.assertEqual(rest_rate_limit._REST_NEXT_ALLOWED, 65.0)

    def test_rest_apply_cooldown_local_noop(self) -> None:
        rest_rate_limit._REST_NEXT_ALLOWED = 12.0
        rest_rate_limit._rest_apply_cooldown_local(0)
        self.assertEqual(rest_rate_limit._REST_NEXT_ALLOWED, 12.0)

    def test_rest_wait_local_rate_zero(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._rest_limits", return_value=(0.0, 5)):
            rest_rate_limit._rest_wait_local()

    def test_rest_wait_local_zero_wait(self) -> None:
        rest_rate_limit._REST_NEXT_ALLOWED = 0.0
        rest_rate_limit._REST_LAST_REFILL = 0.0
        rest_rate_limit._REST_TOKENS = 0.0
        with patch("src.kalshi.kalshi_rest_rate_limit._rest_limits", return_value=(float("inf"), 0)), \
             patch("src.kalshi.kalshi_sdk.time.monotonic", return_value=1000.0):
            rest_rate_limit._rest_wait_local()

    def test_rest_wait_local_backoff(self) -> None:
        rest_rate_limit._REST_NEXT_ALLOWED = 105.0
        with patch("src.kalshi.kalshi_sdk.time.monotonic", return_value=100.0), \
             patch("src.kalshi.kalshi_sdk.time.sleep", side_effect=StopLoop):
            with self.assertRaises(StopLoop):
                rest_rate_limit._rest_wait_local()

    def test_rest_wait_local_tokens_available(self) -> None:
        rest_rate_limit._REST_NEXT_ALLOWED = 0.0
        rest_rate_limit._REST_LAST_REFILL = 100.0
        rest_rate_limit._REST_TOKENS = 2.0
        with patch("src.kalshi.kalshi_rest_rate_limit._rest_limits", return_value=(1.0, 5)), \
             patch("src.kalshi.kalshi_sdk.time.monotonic", return_value=100.0):
            rest_rate_limit._rest_wait_local()
        self.assertEqual(rest_rate_limit._REST_TOKENS, 1.0)

    def test_rest_register_rate_limit_invalid_env(self) -> None:
        with patch.dict(os.environ, {"KALSHI_REST_COOLDOWN_SEC": "bad"}), \
             patch("src.kalshi.kalshi_rest_rate_limit.rest_apply_cooldown") as apply_cooldown:
            kalshi_sdk.rest_register_rate_limit()
        apply_cooldown.assert_called_once_with(30.0)

    def test_rest_backoff_remaining_db(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_backend", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rest_backoff_remaining", return_value=2.5):
            self.assertEqual(kalshi_sdk.rest_backoff_remaining(), 2.5)

    def test_rest_backoff_remaining_fallback(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_backend", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rest_backoff_remaining", return_value=None), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_backoff_remaining_local", return_value=1.0):
            self.assertEqual(kalshi_sdk.rest_backoff_remaining(), 1.0)

    def test_rest_apply_cooldown_db(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_backend", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rest_apply_cooldown") as db_apply:
            kalshi_sdk.rest_apply_cooldown(1.5)
        db_apply.assert_called_once_with(1.5)

    def test_rest_wait_db(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_backend", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rest_wait", return_value=True), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_wait_local") as wait_local:
            kalshi_sdk.rest_wait()
        self.assertEqual(wait_local.call_count, 0)

    def test_rest_wait_db_fallback(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._rest_rate_limit_backend", return_value="db"), \
             patch("src.kalshi.kalshi_rest_rate_limit._db_rest_wait", return_value=False), \
             patch("src.kalshi.kalshi_rest_rate_limit._rest_wait_local") as wait_local:
            kalshi_sdk.rest_wait()
        self.assertEqual(wait_local.call_count, 1)

    def test_rest_register_rate_limit_retry_after(self) -> None:
        with patch("src.kalshi.kalshi_rest_rate_limit._extract_retry_after", return_value=4.0), \
             patch("src.kalshi.kalshi_rest_rate_limit.rest_apply_cooldown") as apply_cooldown:
            kalshi_sdk.rest_register_rate_limit(exc=Exception("boom"))
        apply_cooldown.assert_called_once_with(4.0)


class TestCandlesticksRateLimit(KalshiSdkTestCase):
    def test_candlesticks_wait_invalid_interval(self) -> None:
        with patch.dict(os.environ, {"KALSHI_CANDLE_MIN_INTERVAL_SECONDS": "bad"}), \
             patch("src.kalshi.kalshi_sdk.time.monotonic", return_value=100.0):
            rest_rate_limit._candlesticks_wait()

    def test_candlesticks_wait_sets_next_allowed(self) -> None:
        with patch.dict(os.environ, {"KALSHI_CANDLE_MIN_INTERVAL_SECONDS": "1"}), \
             patch("src.kalshi.kalshi_sdk.time.monotonic", return_value=100.0):
            rest_rate_limit._candlesticks_wait()
        self.assertEqual(rest_rate_limit._CANDLE_NEXT_ALLOWED, 101.0)

    def test_candlesticks_wait_sleeps(self) -> None:
        rest_rate_limit._CANDLE_NEXT_ALLOWED = 110.0
        with patch.dict(os.environ, {"KALSHI_CANDLE_MIN_INTERVAL_SECONDS": "0"}), \
             patch("src.kalshi.kalshi_sdk.time.monotonic", return_value=100.0), \
             patch("src.kalshi.kalshi_sdk.time.sleep") as sleep:
            rest_rate_limit._candlesticks_wait()
        sleep.assert_called_once_with(10.0)

    def test_candlesticks_apply_cooldown_noop(self) -> None:
        rest_rate_limit._CANDLE_NEXT_ALLOWED = 5.0
        rest_rate_limit._candlesticks_apply_cooldown(0.0)
        self.assertEqual(rest_rate_limit._CANDLE_NEXT_ALLOWED, 5.0)

    def test_candlesticks_apply_cooldown_updates(self) -> None:
        with patch("src.kalshi.kalshi_sdk.time.monotonic", return_value=10.0):
            rest_rate_limit._candlesticks_apply_cooldown(5.0)
        self.assertEqual(rest_rate_limit._CANDLE_NEXT_ALLOWED, 15.0)


class TestSdkPatching(KalshiSdkTestCase):
    def test_write_temp_key_caches(self) -> None:
        kalshi_sdk._TEMP_KEY_PATH = None
        path1 = kalshi_sdk._write_temp_key("pem")
        path2 = kalshi_sdk._write_temp_key("pem2")
        self.assertEqual(path1, path2)

    def test_import_sdk_missing(self) -> None:
        orig_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "kalshi_python_sync":
                raise ImportError("missing")
            return orig_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(kalshi_sdk.KalshiSdkError):
                kalshi_sdk._import_sdk()

    def test_import_sdk_success(self) -> None:
        module = types.ModuleType("kalshi_python_sync")
        with patch.dict(sys.modules, {"kalshi_python_sync": module}):
            self.assertIs(kalshi_sdk._import_sdk(), module)

    def test_patch_sdk_models_missing_imports(self) -> None:
        orig_import = __import__

        def fake_import(name, *args, **kwargs):
            if name.startswith("kalshi_python_sync"):
                raise ModuleNotFoundError("missing")
            return orig_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            kalshi_sdk._patch_sdk_models()

    def test_patch_sdk_models_applies(self) -> None:
        market_module = types.ModuleType("kalshi_python_sync.models.market")
        event_module = types.ModuleType("kalshi_python_sync.models.event")

        class Market:
            def __init__(self, value):
                self.value = value

            @classmethod
            def from_dict(cls, obj):
                return cls(obj)

        class Event:
            def __init__(self, value):
                self.value = value

            @classmethod
            def from_dict(cls, obj):
                return cls(obj)

        market_module.Market = Market
        event_module.Event = Event
        with patch.dict(
            sys.modules,
            {
                "kalshi_python_sync": types.ModuleType("kalshi_python_sync"),
                "kalshi_python_sync.models": types.ModuleType("kalshi_python_sync.models"),
                "kalshi_python_sync.models.market": market_module,
                "kalshi_python_sync.models.event": event_module,
            },
        ):
            kalshi_sdk._patch_sdk_models()
            market_obj = Market.from_dict({"status": "finalized", "price_ranges": None})
            event_obj = Event.from_dict({"status": "inactive"})
        self.assertEqual(market_obj.value["status"], "settled")
        self.assertEqual(market_obj.value["price_ranges"], [])
        self.assertEqual(event_obj.value["status"], "initialized")


class TestSdkHelpers(KalshiSdkTestCase):
    def test_looks_like_client(self) -> None:
        class Client:
            def get_events(self):
                return []

        self.assertTrue(kalshi_sdk._looks_like_client(Client()))
        self.assertFalse(kalshi_sdk._looks_like_client(object()))

    def test_candidate_factories_preferred(self) -> None:
        sdk = SimpleNamespace(KalshiClient=lambda: "client")
        factories = list(kalshi_sdk._candidate_factories(sdk))
        self.assertEqual(factories[0][0], "KalshiClient")

    def test_candidate_factories_skips_and_import_errors(self) -> None:
        sdk = SimpleNamespace(__path__=["/tmp"], __name__="dummy_sdk")
        info_skip = SimpleNamespace(name="dummy_sdk.other")
        info_fail = SimpleNamespace(name="dummy_sdk.client_api")
        with patch("src.kalshi.kalshi_sdk.pkgutil.walk_packages", return_value=[info_skip, info_fail]), \
             patch("src.kalshi.kalshi_sdk.importlib.import_module", side_effect=RuntimeError("boom")):
            factories = list(kalshi_sdk._candidate_factories(sdk))
        self.assertEqual(factories, [])

    def test_candidate_factories_walk_packages(self) -> None:
        sdk = SimpleNamespace(__path__=["/tmp"], __name__="dummy_sdk")
        info = SimpleNamespace(name="dummy_sdk.client_api")

        class Client:
            def get_events(self):
                return []

        module = types.ModuleType("dummy_sdk.client_api")
        module.Client = Client
        with patch("src.kalshi.kalshi_sdk.pkgutil.walk_packages", return_value=[info]), \
             patch("src.kalshi.kalshi_sdk.importlib.import_module", return_value=module):
            factories = list(kalshi_sdk._candidate_factories(sdk))
        self.assertEqual(factories[0][0], "dummy_sdk.client_api:Client")

    def test_resolve_environment_missing(self) -> None:
        sdk = SimpleNamespace()
        self.assertIsNone(kalshi_sdk._resolve_environment(sdk, host="https://example"))

    def test_resolve_environment_demo(self) -> None:
        class Env:
            DEMO = "demo"
            PROD = "prod"

        sdk = SimpleNamespace(Environment=Env)
        self.assertEqual(kalshi_sdk._resolve_environment(sdk, host="demo"), "demo")

    def test_resolve_environment_fallback_prod(self) -> None:
        class Env:
            PROD = "prod"

        sdk = SimpleNamespace(Environment=Env)
        self.assertEqual(kalshi_sdk._resolve_environment(sdk, host="demo"), "prod")

    def test_build_client_varargs_and_pem(self) -> None:
        def factory_varargs(*args, **kwargs):
            return kwargs

        result = kalshi_sdk._build_client(
            factory_varargs,
            host="host",
            api_key_id="key",
            private_key_pem="pem",
        )
        self.assertEqual(result, {})

        def factory_pem(private_key_pem):
            return private_key_pem

        result = kalshi_sdk._build_client(
            factory_pem,
            host="host",
            api_key_id="key",
            private_key_pem="pem",
        )
        self.assertEqual(result, "pem")

    def test_build_client_host_and_key(self) -> None:
        def factory(host, api_key_id):
            return {"host": host, "key": api_key_id}

        result = kalshi_sdk._build_client(
            factory,
            host="host",
            api_key_id="key",
            private_key_pem="pem",
        )
        self.assertEqual(result, {"host": "host", "key": "key"})

    def test_build_client_env_missing(self) -> None:
        def factory_env(environment):
            return environment

        with patch("src.kalshi.kalshi_sdk._resolve_environment", return_value=None):
            self.assertIsNone(
                kalshi_sdk._build_client(
                    factory_env,
                    host="host",
                    api_key_id="key",
                    private_key_pem="pem",
                )
            )

    def test_build_client_env_missing_after_import(self) -> None:
        def factory_env(environment, host=None):
            return environment

        with patch("src.kalshi.kalshi_sdk._import_sdk", return_value=SimpleNamespace()), \
             patch("src.kalshi.kalshi_sdk._resolve_environment", return_value=None):
            self.assertIsNone(
                kalshi_sdk._build_client(
                    factory_env,
                    host="host",
                    api_key_id="key",
                    private_key_pem="pem",
                )
            )

    def test_build_client_env_default(self) -> None:
        def factory_env(environment="default", host=None):
            return {"environment": environment, "host": host}

        with patch("src.kalshi.kalshi_sdk._import_sdk", return_value=SimpleNamespace()), \
             patch("src.kalshi.kalshi_sdk._resolve_environment", return_value=None):
            result = kalshi_sdk._build_client(
                factory_env,
                host="host",
                api_key_id="key",
                private_key_pem="pem",
            )
        self.assertEqual(result["environment"], "default")
        self.assertEqual(result["host"], "host")

    def test_build_client_env_resolved(self) -> None:
        def factory_env(environment, host=None):
            return {"environment": environment, "host": host}

        with patch("src.kalshi.kalshi_sdk._import_sdk", return_value=SimpleNamespace()), \
             patch("src.kalshi.kalshi_sdk._resolve_environment", return_value="demo"):
            result = kalshi_sdk._build_client(
                factory_env,
                host="host",
                api_key_id="key",
                private_key_pem="pem",
            )
        self.assertEqual(result["environment"], "demo")

    def test_build_client_env_alias_resolved(self) -> None:
        def factory_env(env, host=None):
            return {"env": env, "host": host}

        with patch("src.kalshi.kalshi_sdk._import_sdk", return_value=SimpleNamespace()), \
             patch("src.kalshi.kalshi_sdk._resolve_environment", return_value="prod"):
            result = kalshi_sdk._build_client(
                factory_env,
                host="host",
                api_key_id="key",
                private_key_pem="pem",
            )
        self.assertEqual(result["env"], "prod")

    def test_build_client_unknown_param_default(self) -> None:
        def factory(foo="bar"):
            return foo

        result = kalshi_sdk._build_client(
            factory,
            host="host",
            api_key_id="key",
            private_key_pem="pem",
        )
        self.assertEqual(result, "bar")

    def test_build_client_unknown_param_required(self) -> None:
        def factory(foo):
            return foo

        self.assertIsNone(
            kalshi_sdk._build_client(
                factory,
                host="host",
                api_key_id="key",
                private_key_pem="pem",
            )
        )

    def test_build_client_unknown_param_required_with_host(self) -> None:
        def factory(host, foo):
            return {"host": host, "foo": foo}

        self.assertIsNone(
            kalshi_sdk._build_client(
                factory,
                host="host",
                api_key_id="key",
                private_key_pem="pem",
            )
        )

    def test_build_client_env_import_fails(self) -> None:
        def factory_env(environment):
            return environment

        with patch("src.kalshi.kalshi_sdk._import_sdk", side_effect=kalshi_sdk.KalshiSdkError("boom")):
            self.assertIsNone(
                kalshi_sdk._build_client(
                    factory_env,
                    host="host",
                    api_key_id="key",
                    private_key_pem="pem",
                )
            )

    def test_build_client_private_key_path(self) -> None:
        def factory_path(private_key_path):
            return private_key_path

        with patch("src.kalshi.kalshi_sdk._write_temp_key", return_value="path"):
            result = kalshi_sdk._build_client(
                factory_path,
                host="host",
                api_key_id="key",
                private_key_pem="pem",
            )
        self.assertEqual(result, "path")

    def test_apply_host_override(self) -> None:
        api_client = SimpleNamespace(host=None, configuration=SimpleNamespace(host=None))
        client = SimpleNamespace(api_client=api_client, host=None)
        with patch("src.kalshi.kalshi_sdk.logger.info") as info:
            kalshi_sdk._apply_host_override(client, "host")
        self.assertEqual(client.host, "host")
        self.assertEqual(api_client.host, "host")
        self.assertEqual(api_client.configuration.host, "host")
        info.assert_called_once()

    def test_apply_host_override_empty_host(self) -> None:
        client = SimpleNamespace(host=None)
        with patch("src.kalshi.kalshi_sdk.logger.warning") as warn, \
             patch("src.kalshi.kalshi_sdk.logger.info") as info:
            kalshi_sdk._apply_host_override(client, "")
        warn.assert_not_called()
        info.assert_not_called()

    def test_apply_host_override_setter_errors(self) -> None:
        class BadConfig:
            @property
            def host(self):
                return None

            @host.setter
            def host(self, _value):
                raise RuntimeError("boom")

        class BadApiClient:
            def __init__(self):
                self.configuration = BadConfig()

            @property
            def host(self):
                return None

            @host.setter
            def host(self, _value):
                raise RuntimeError("boom")

        class BadClient:
            def __init__(self):
                self.api_client = BadApiClient()

            @property
            def host(self):
                return None

            @host.setter
            def host(self, _value):
                raise RuntimeError("boom")

        client = BadClient()
        with patch("src.kalshi.kalshi_sdk.logger.warning") as warn:
            kalshi_sdk._apply_host_override(client, "host")
        warn.assert_called_once()

    def test_apply_host_override_failed(self) -> None:
        client = SimpleNamespace()
        with patch("src.kalshi.kalshi_sdk.logger.warning") as warn:
            kalshi_sdk._apply_host_override(client, "host")
        warn.assert_called_once()

    def test_host_override_from_env(self) -> None:
        with patch.dict(os.environ, {"KALSHI_HOST": "host"}):
            self.assertEqual(kalshi_sdk._host_override_from_env(), "host")

    def test_ensure_method_host_updates(self) -> None:
        api_client = SimpleNamespace(host=None, configuration=SimpleNamespace(host=None))
        client = SimpleNamespace(api_client=api_client)
        method = getattr(client, "api_client")  # placeholder
        method = (lambda self=client: None).__get__(client, client.__class__)
        with patch.dict(os.environ, {"KALSHI_HOST": "host"}), \
             patch("src.kalshi.kalshi_sdk.logger.info") as info:
            kalshi_sdk._ensure_method_host(method)
        self.assertEqual(api_client.host, "host")
        self.assertEqual(api_client.configuration.host, "host")
        info.assert_called_once()

    def test_ensure_method_host_no_env(self) -> None:
        client = SimpleNamespace(api_client=SimpleNamespace(host=None))
        method = (lambda self=client: None).__get__(client, client.__class__)
        with patch.dict(os.environ, {}, clear=True):
            kalshi_sdk._ensure_method_host(method)
        self.assertIsNone(client.api_client.host)

    def test_ensure_method_host_setter_errors(self) -> None:
        class BadConfig:
            @property
            def host(self):
                return None

            @host.setter
            def host(self, _value):
                raise RuntimeError("boom")

        class BadApiClient:
            def __init__(self):
                self.configuration = BadConfig()

            @property
            def host(self):
                return None

            @host.setter
            def host(self, _value):
                raise RuntimeError("boom")

        class BadClient:
            def __init__(self):
                self.api_client = BadApiClient()

            def events(self):
                return None

        client = BadClient()
        method = client.events
        with patch.dict(os.environ, {"KALSHI_HOST": "host"}), \
             patch("src.kalshi.kalshi_sdk.logger.info") as info:
            kalshi_sdk._ensure_method_host(method)
        info.assert_not_called()

    def test_extract_status(self) -> None:
        exc = SimpleNamespace(status=404)
        self.assertEqual(kalshi_sdk._extract_status(exc), 404)
        http_resp = SimpleNamespace(status_code=500)
        exc = SimpleNamespace(http_resp=http_resp)
        self.assertEqual(kalshi_sdk._extract_status(exc), 500)

    def test_extract_headers(self) -> None:
        exc = SimpleNamespace(headers={"Retry-After": "1"})
        self.assertEqual(rest_rate_limit._extract_headers(exc), {"Retry-After": "1"})
        http_resp = SimpleNamespace(headers={"X": "1"})
        exc = SimpleNamespace(http_resp=http_resp)
        self.assertEqual(rest_rate_limit._extract_headers(exc), {"X": "1"})

        class Resp:
            def getheaders(self):
                return [("X", "2")]

        exc = SimpleNamespace(http_resp=Resp())
        self.assertEqual(rest_rate_limit._extract_headers(exc), [("X", "2")])

        class BadResp:
            def getheaders(self):
                raise RuntimeError("fail")

        exc = SimpleNamespace(http_resp=BadResp())
        self.assertIsNone(rest_rate_limit._extract_headers(exc))
        exc = SimpleNamespace(http_resp=SimpleNamespace())
        self.assertIsNone(rest_rate_limit._extract_headers(exc))

    def test_header_lookup(self) -> None:
        self.assertEqual(rest_rate_limit._header_lookup({"X": "1"}, "x"), "1")
        headers = [("Retry-After", "2")]
        self.assertEqual(rest_rate_limit._header_lookup(headers, "retry-after"), "2")
        headers = [("A", "1"), ("bad",)]
        self.assertIsNone(rest_rate_limit._header_lookup(headers, "missing"))

        class HeaderObj:
            def get(self, _name):
                return "3"

        self.assertEqual(rest_rate_limit._header_lookup(HeaderObj(), "X"), "3")

        class HeaderNone:
            def get(self, _name):
                return None

        self.assertIsNone(rest_rate_limit._header_lookup(HeaderNone(), "X"))

        class BadHeader:
            def get(self, _name):
                raise RuntimeError("boom")

        self.assertIsNone(rest_rate_limit._header_lookup(BadHeader(), "X"))

    def test_parse_retry_after(self) -> None:
        with patch("src.kalshi.kalshi_sdk.time.time", return_value=100.0):
            self.assertEqual(rest_rate_limit._parse_retry_after("5"), 5.0)
            self.assertEqual(rest_rate_limit._parse_retry_after(1e10), 1e10 - 100.0)
            parsed = rest_rate_limit._parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT")
            self.assertIsNotNone(parsed)
            self.assertGreater(parsed, 0.0)
        self.assertIsNone(rest_rate_limit._parse_retry_after("bad"))
        self.assertIsNone(rest_rate_limit._parse_retry_after(None))
        with patch("src.kalshi.kalshi_sdk.time.time", return_value=0.0):
            parsed = rest_rate_limit._parse_retry_after("Thu, 01 Jan 1970 00:20:00")
        self.assertEqual(parsed, 1200.0)

    def test_extract_retry_after(self) -> None:
        exc = SimpleNamespace(headers={"Retry-After": "5"})
        self.assertEqual(kalshi_sdk._extract_retry_after(exc), 5.0)
        exc = SimpleNamespace(headers={"X-Rate-Limit-Reset": "5"})
        self.assertEqual(kalshi_sdk._extract_retry_after(exc), 5.0)

    def test_load_retry_config(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TEST_RETRIES": "2",
                "TEST_RETRY_SECONDS": "0.5",
                "TEST_RETRY_MAX_SECONDS": "1.5",
            },
        ):
            cfg = kalshi_sdk._load_retry_config("TEST")
        self.assertEqual(cfg.max_retries, 2)
        self.assertEqual(cfg.base_sleep, 0.5)
        self.assertEqual(cfg.max_sleep, 1.5)

    def test_call_with_retries_success(self) -> None:
        with patch("src.kalshi.kalshi_sdk.rest_wait") as wait:
            ok, result = kalshi_sdk._call_with_retries(lambda: "ok", kalshi_sdk.RetryConfig(0, 1, 1), "ctx")
        self.assertTrue(ok)
        self.assertEqual(result, "ok")
        wait.assert_called_once()

    def test_call_with_retries_non_429(self) -> None:
        class Err(Exception):
            pass

        def boom():
            raise Err("fail")

        with patch("src.kalshi.kalshi_sdk.rest_wait"):
            with self.assertRaises(Err):
                kalshi_sdk._call_with_retries(boom, kalshi_sdk.RetryConfig(1, 1, 1), "ctx")

    def test_call_with_retries_gives_up(self) -> None:
        class TooMany(Exception):
            status = 429

        def boom():
            raise TooMany()

        with patch("src.kalshi.kalshi_sdk.rest_wait"), \
             patch("src.kalshi.kalshi_sdk.rest_apply_cooldown") as cooldown, \
             patch("src.kalshi.kalshi_sdk.time.sleep"):
            ok, result = kalshi_sdk._call_with_retries(
                boom, kalshi_sdk.RetryConfig(0, 1, 1), "ctx"
            )
        self.assertFalse(ok)
        self.assertIsNone(result)
        self.assertEqual(cooldown.call_count, 0)

    def test_call_with_retries_retries(self) -> None:
        class TooMany(Exception):
            status = 429

        calls = {"count": 0}

        def flake():
            if calls["count"] == 0:
                calls["count"] += 1
                raise TooMany()
            return "ok"

        with patch("src.kalshi.kalshi_sdk.rest_wait"), \
             patch("src.kalshi.kalshi_sdk.rest_apply_cooldown"), \
             patch("src.kalshi.kalshi_sdk.time.sleep"), \
             patch("src.kalshi.kalshi_sdk.random.uniform", return_value=0.0):
            ok, result = kalshi_sdk._call_with_retries(
                flake, kalshi_sdk.RetryConfig(1, 1.0, 5.0), "ctx"
            )
        self.assertTrue(ok)
        self.assertEqual(result, "ok")

    def test_call_with_retries_retry_after(self) -> None:
        class TooMany(Exception):
            status = 429

        def boom():
            raise TooMany()

        with patch("src.kalshi.kalshi_sdk.rest_wait"), \
             patch("src.kalshi.kalshi_sdk.rest_apply_cooldown"), \
             patch("src.kalshi.kalshi_sdk._extract_retry_after", return_value=5.0), \
             patch("src.kalshi.kalshi_sdk.time.sleep"), \
             patch("src.kalshi.kalshi_sdk.random.uniform", return_value=0.0):
            ok, _result = kalshi_sdk._call_with_retries(
                boom, kalshi_sdk.RetryConfig(1, 1.0, 5.0), "ctx"
            )
        self.assertFalse(ok)

    def test_call_with_retries_rate_limit_hook(self) -> None:
        class TooMany(Exception):
            status = 429

        calls = {"count": 0}

        def flake():
            if calls["count"] == 0:
                calls["count"] += 1
                raise TooMany()
            return "ok"

        hook = Mock()
        with patch("src.kalshi.kalshi_sdk.rest_wait"), \
             patch("src.kalshi.kalshi_sdk.rest_apply_cooldown"), \
             patch("src.kalshi.kalshi_sdk.time.sleep"), \
             patch("src.kalshi.kalshi_sdk.random.uniform", return_value=0.0):
            ok, result = kalshi_sdk._call_with_retries(
                flake,
                kalshi_sdk.RetryConfig(1, 1.0, 5.0),
                "ctx",
                rate_limit_hook=hook,
            )
        self.assertTrue(ok)
        hook.assert_called_once()

    def test_call_with_retries_transient_status(self) -> None:
        class ServerError(Exception):
            status = 500

        calls = {"count": 0}

        def flake():
            if calls["count"] == 0:
                calls["count"] += 1
                raise ServerError()
            return "ok"

        with patch("src.kalshi.kalshi_sdk.rest_wait"), \
             patch("src.kalshi.kalshi_sdk.time.sleep"), \
             patch("src.kalshi.kalshi_sdk.random.uniform", return_value=0.0):
            ok, result = kalshi_sdk._call_with_retries(
                flake, kalshi_sdk.RetryConfig(1, 1.0, 5.0), "ctx"
            )
        self.assertTrue(ok)
        self.assertEqual(result, "ok")

    def test_call_with_retries_transient_exception(self) -> None:
        calls = {"count": 0}

        def flake():
            if calls["count"] == 0:
                calls["count"] += 1
                raise TimeoutError("boom")
            return "ok"

        with patch("src.kalshi.kalshi_sdk.rest_wait"), \
             patch("src.kalshi.kalshi_sdk.time.sleep"), \
             patch("src.kalshi.kalshi_sdk.random.uniform", return_value=0.0):
            ok, result = kalshi_sdk._call_with_retries(
                flake, kalshi_sdk.RetryConfig(1, 1.0, 5.0), "ctx"
            )
        self.assertTrue(ok)
        self.assertEqual(result, "ok")

    def test_make_client_success(self) -> None:
        client = SimpleNamespace()
        with patch("src.kalshi.kalshi_sdk._import_sdk", return_value=object()), \
             patch("src.kalshi.kalshi_sdk._patch_sdk_models"), \
             patch("src.kalshi.kalshi_sdk._candidate_factories", return_value=[("X", object())]), \
             patch("src.kalshi.kalshi_sdk._build_client", return_value=client), \
             patch("src.kalshi.kalshi_sdk._apply_host_override") as apply_override:
            result = kalshi_sdk.make_client("host", "key", "pem")
        self.assertIs(result, client)
        apply_override.assert_called_once()

    def test_make_client_failure(self) -> None:
        with patch("src.kalshi.kalshi_sdk._import_sdk", return_value=object()), \
             patch("src.kalshi.kalshi_sdk._patch_sdk_models"), \
             patch("src.kalshi.kalshi_sdk._candidate_factories", return_value=[]):
            with self.assertRaises(kalshi_sdk.KalshiSdkError):
                kalshi_sdk.make_client("host", "key", "pem")

    def test_make_client_factory_exception(self) -> None:
        with patch("src.kalshi.kalshi_sdk._import_sdk", return_value=object()), \
             patch("src.kalshi.kalshi_sdk._patch_sdk_models"), \
             patch("src.kalshi.kalshi_sdk._candidate_factories", return_value=[("X", object())]), \
             patch("src.kalshi.kalshi_sdk._build_client", side_effect=RuntimeError("boom")):
            with self.assertRaises(kalshi_sdk.KalshiSdkError):
                kalshi_sdk.make_client("host", "key", "pem")

    def test_filter_kwargs(self) -> None:
        def func(a, b):
            return a + b

        self.assertEqual(kalshi_sdk._filter_kwargs(func, {"a": 1, "b": 2, "c": 3}), {"a": 1, "b": 2})

        def func_kwargs(**kwargs):
            return kwargs

        self.assertEqual(kalshi_sdk._filter_kwargs(func_kwargs, {"a": 1}), {"a": 1})

    def test_extract_items(self) -> None:
        items, cursor = kalshi_sdk._extract_items({"events": [1], "next_cursor": "c"})
        self.assertEqual(items, [1])
        self.assertEqual(cursor, "c")
        items, cursor = kalshi_sdk._extract_items(([1], "c"))
        self.assertEqual(items, [1])
        self.assertEqual(cursor, "c")
        items, cursor = kalshi_sdk._extract_items([1, 2])
        self.assertEqual(items, [1, 2])
        self.assertIsNone(cursor)
        resp = SimpleNamespace(events=[3], cursor="c2")
        items, cursor = kalshi_sdk._extract_items(resp)
        self.assertEqual(items, [3])
        self.assertEqual(cursor, "c2")

    def test_to_plain_dict(self) -> None:
        self.assertEqual(kalshi_sdk._to_plain_dict({"a": 1}), {"a": 1})

        class Model:
            def model_dump(self, mode=None):
                return {"a": 1}

        self.assertEqual(kalshi_sdk._to_plain_dict(Model()), {"a": 1})

        class ModelNoArgs:
            def model_dump(self):
                return {"z": 9}

        self.assertEqual(kalshi_sdk._to_plain_dict(ModelNoArgs()), {"z": 9})

        class WithDict:
            def to_dict(self):
                return {"b": 2}

        self.assertEqual(kalshi_sdk._to_plain_dict(WithDict()), {"b": 2})

        class WithDictError:
            def to_dict(self):
                raise RuntimeError("fail")

        obj = WithDictError()
        obj.value = 3
        self.assertEqual(kalshi_sdk._to_plain_dict(obj), {"value": 3})
        self.assertIs(kalshi_sdk._to_plain_dict((1, 2)), (1, 2))

    def test_resolve_events_method(self) -> None:
        client = SimpleNamespace(get_events=lambda: None)
        self.assertIsNotNone(kalshi_sdk._resolve_events_method(client))

    def test_prepare_cursor_kwargs(self) -> None:
        def method(cursor=None):
            return cursor

        kwargs = kalshi_sdk._prepare_cursor_kwargs(method, {"a": 1}, cursor="c")
        self.assertEqual(kwargs["cursor"], "c")

        def method_next(next_cursor=None):
            return next_cursor

        kwargs = kalshi_sdk._prepare_cursor_kwargs(method_next, {"a": 1}, cursor="c")
        self.assertEqual(kwargs["next_cursor"], "c")

    def test_iter_events_stream(self) -> None:
        def method(**kwargs):
            return [{"a": 1}]

        with patch("src.kalshi.kalshi_sdk._call_with_retries", return_value=(True, [{"a": 1}])), \
             patch("src.kalshi.kalshi_sdk._ensure_method_host"):
            events = list(kalshi_sdk._iter_events_stream(method, {}, kalshi_sdk.RetryConfig(0, 1, 1)))
        self.assertEqual(events, [{"a": 1}])

    def test_iter_events_stream_not_ok(self) -> None:
        def method(**kwargs):
            return [{"a": 1}]

        with patch("src.kalshi.kalshi_sdk._call_with_retries", return_value=(False, None)), \
             patch("src.kalshi.kalshi_sdk._ensure_method_host"):
            events = list(kalshi_sdk._iter_events_stream(method, {}, kalshi_sdk.RetryConfig(0, 1, 1)))
        self.assertEqual(events, [])

    def test_iter_events_paged(self) -> None:
        calls = []

        def method(cursor=None):
            calls.append(cursor)
            if cursor is None:
                return {"events": [{"a": 1}], "next_cursor": "c"}
            return {"events": [{"b": 2}], "next_cursor": None}

        def call_with_retries(func, _cfg, _ctx, **_kwargs):
            return True, func()

        with patch("src.kalshi.kalshi_sdk._call_with_retries", side_effect=call_with_retries), \
             patch("src.kalshi.kalshi_sdk._ensure_method_host"):
            events = list(kalshi_sdk._iter_events_paged(method, {}, kalshi_sdk.RetryConfig(0, 1, 1)))
        self.assertEqual(events, [{"a": 1}, {"b": 2}])
        self.assertEqual(calls, [None, "c"])

    def test_iter_events_paged_not_ok(self) -> None:
        def method(cursor=None):
            return {"events": [{"a": 1}], "next_cursor": "c"}

        with patch("src.kalshi.kalshi_sdk._call_with_retries", return_value=(False, None)), \
             patch("src.kalshi.kalshi_sdk._ensure_method_host"):
            events = list(kalshi_sdk._iter_events_paged(method, {}, kalshi_sdk.RetryConfig(0, 1, 1)))
        self.assertEqual(events, [])

    def test_iter_events_missing_method(self) -> None:
        with self.assertRaises(kalshi_sdk.KalshiSdkError):
            list(kalshi_sdk.iter_events(object()))

    def test_iter_events_stream_client(self) -> None:
        class Client:
            def iter_events(self, **_kwargs):
                return [{"a": 1}]

        with patch("src.kalshi.kalshi_sdk._call_with_retries", return_value=(True, [{"a": 1}])):
            events = list(kalshi_sdk.iter_events(Client()))
        self.assertEqual(events, [{"a": 1}])

    def test_iter_events_paged_client(self) -> None:
        class Client:
            def get_events(self, **_kwargs):
                return {"events": [{"a": 1}], "next_cursor": None}

        def call_with_retries(func, _cfg, _ctx, **_kwargs):
            return True, func()

        with patch("src.kalshi.kalshi_sdk._call_with_retries", side_effect=call_with_retries):
            events = list(kalshi_sdk.iter_events(Client()))
        self.assertEqual(events, [{"a": 1}])

    def test_resolve_candlesticks_method(self) -> None:
        class Client:
            def get_candles(self):
                return []

        self.assertIsNotNone(kalshi_sdk._resolve_candlesticks_method(Client()))

    def test_build_candlestick_kwargs(self) -> None:
        def method(series_ticker, market_ticker, period_interval=None):
            return (series_ticker, market_ticker, period_interval)

        kwargs = kalshi_sdk._build_candlestick_kwargs(
            method,
            "S",
            "M",
            {"period_interval_minutes": 5},
        )
        self.assertEqual(kwargs["period_interval"], 5)
        self.assertEqual(kwargs["series_ticker"], "S")
        self.assertEqual(kwargs["market_ticker"], "M")

    def test_normalize_candlesticks_response(self) -> None:
        self.assertEqual(
            kalshi_sdk._normalize_candlesticks_response([{"a": 1}]),
            {"candlesticks": [{"a": 1}]},
        )
        resp = kalshi_sdk._normalize_candlesticks_response({"candlesticks": [{"a": 9}]})
        self.assertEqual(resp["candlesticks"], [{"a": 9}])
        resp = kalshi_sdk._normalize_candlesticks_response({"candles": [{"a": 2}]})
        self.assertEqual(resp["candlesticks"], [{"a": 2}])
        resp = kalshi_sdk._normalize_candlesticks_response({"other": 1})
        self.assertEqual(resp, {"other": 1})

        class Resp:
            candlesticks = [{"a": 3}]

        resp = kalshi_sdk._normalize_candlesticks_response(Resp())
        self.assertEqual(resp["candlesticks"], [{"a": 3}])
        resp = kalshi_sdk._normalize_candlesticks_response(SimpleNamespace(candles=[{"a": 4}]))
        self.assertEqual(resp["candlesticks"], [{"a": 4}])
        resp = kalshi_sdk._normalize_candlesticks_response(SimpleNamespace())
        self.assertEqual(resp["candlesticks"], [])

    def test_get_market_candlesticks_missing(self) -> None:
        with self.assertRaises(kalshi_sdk.KalshiSdkError):
            kalshi_sdk.get_market_candlesticks(object(), "S", "M")

    def test_get_market_candlesticks_ok_false(self) -> None:
        class Client:
            def get_candles(self, **_kwargs):
                return []

        with patch("src.kalshi.kalshi_sdk._call_with_retries", return_value=(False, None)), \
             patch("src.kalshi.kalshi_sdk._load_retry_config", return_value=kalshi_sdk.RetryConfig(0, 1, 1)):
            resp = kalshi_sdk.get_market_candlesticks(Client(), "S", "M")
        self.assertEqual(resp, {"candlesticks": []})

    def test_get_market_candlesticks_success(self) -> None:
        class Client:
            def get_candles(self, **_kwargs):
                return {"candles": [{"a": 1}]}

        with patch("src.kalshi.kalshi_sdk._call_with_retries", return_value=(True, {"candles": [{"a": 1}]})), \
             patch("src.kalshi.kalshi_sdk._load_retry_config", return_value=kalshi_sdk.RetryConfig(0, 1, 1)):
            resp = kalshi_sdk.get_market_candlesticks(Client(), "S", "M")
        self.assertEqual(resp["candlesticks"], [{"a": 1}])

    def test_get_market_candlesticks_calls_wait(self) -> None:
        class Client:
            def __init__(self):
                self.calls = 0

            def get_candles(self, **_kwargs):
                self.calls += 1
                return {"candles": []}

        client = Client()

        def call_with_retries(func, _cfg, _ctx, rate_limit_hook=None):
            return True, func()

        with patch("src.kalshi.kalshi_sdk._call_with_retries", side_effect=call_with_retries), \
             patch("src.kalshi.kalshi_sdk._candlesticks_wait") as wait:
            kalshi_sdk.get_market_candlesticks(client, "S", "M")
        wait.assert_called_once()
        self.assertEqual(client.calls, 1)
