import io
import os
import runpy
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

healthcheck = importlib.import_module("src.services.healthcheck")


class FakeCursor:
    def __init__(self, rows=None, fetchone_value=None):
        self.rows = rows or []
        self.fetchone_value = fetchone_value
        self.executes = []

    def execute(self, sql, params=None):
        self.executes.append((sql, params))

    def fetchall(self):
        return self.rows

    def fetchone(self):
        if self.fetchone_value is not None:
            return self.fetchone_value
        return self.rows[0] if self.rows else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, cursors=None):
        self.cursors = list(cursors) if cursors is not None else [FakeCursor()]

    def cursor(self):
        return self.cursors.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestHealthcheckHelpers(unittest.TestCase):
    def test_env_parsers(self) -> None:
        with patch.dict(os.environ, {"X_BOOL": "yes", "X_INT": "5"}):
            self.assertTrue(healthcheck._env_bool("X_BOOL"))
            self.assertEqual(healthcheck._env_int("X_INT", 1), 5)
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(healthcheck._env_bool("X_BOOL", False))
            self.assertEqual(healthcheck._env_int("X_INT", 2), 2)
        with patch.dict(os.environ, {"X_INT": "bad"}):
            self.assertEqual(healthcheck._env_int("X_INT", 3), 3)
        with patch.dict(os.environ, {"X_INT": "1"}):
            self.assertEqual(healthcheck._env_int("X_INT", 3, minimum=5), 5)

    def test_parse_ts_and_epoch(self) -> None:
        naive = datetime(2024, 1, 1, 0, 0, 0)
        parsed = healthcheck._parse_ts(naive)
        self.assertEqual(parsed.tzinfo, timezone.utc)
        self.assertIsNone(healthcheck._parse_ts("bad"))
        epoch = healthcheck._parse_epoch_seconds("100")
        self.assertIsNotNone(epoch)
        self.assertIsNone(healthcheck._parse_epoch_seconds("bad"))

    def test_age_seconds(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.assertIsNone(healthcheck._age_seconds(None, now))
        future = now + timedelta(seconds=10)
        self.assertEqual(healthcheck._age_seconds(future, now), 0.0)

    def test_fetch_state_rows(self) -> None:
        rows = [("a", "1", "t"), ("b", "2", "u")]
        conn = FakeConn([FakeCursor(rows=rows)])
        data = healthcheck._fetch_state_rows(conn, ["a", "b"])
        self.assertIn("a", data)
        self.assertEqual(data["b"]["value"], "2")
        self.assertEqual(healthcheck._fetch_state_rows(conn, []), {})

    def test_status_from_age(self) -> None:
        self.assertEqual(healthcheck._status_from_age(None, 10), (False, "missing"))
        self.assertEqual(
            healthcheck._status_from_age(None, 10, allow_missing=True),
            (True, "starting"),
        )
        self.assertEqual(healthcheck._status_from_age(5, 10), (True, "ok"))
        self.assertEqual(healthcheck._status_from_age(20, 10), (False, "stale"))

    def test_resolve_stale_thresholds(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DISCOVERY_SECONDS": "100",
                "BACKFILL_SECONDS": "200",
                "WORK_QUEUE_POLL_SECONDS": "2",
            },
            clear=True,
        ):
            rest = healthcheck._resolve_stale_thresholds("rest")
            self.assertIn("last_discovery_ts", rest)
            ws = healthcheck._resolve_stale_thresholds("ws")
            self.assertIn("last_ws_tick_ts", ws)
            worker = healthcheck._resolve_stale_thresholds("worker")
            self.assertIn("last_worker_ts", worker)
            rag = healthcheck._resolve_stale_thresholds("rag")
            self.assertIn("last_prediction_ts", rag)
        with self.assertRaises(ValueError):
            healthcheck._resolve_stale_thresholds("bad")

    def test_load_service_timestamps_rest(self) -> None:
        rows = [
            ("last_discovery_ts", "2024-01-01T00:00:00Z", None),
            ("last_min_close_ts", "1700000000", "2024-01-02T00:00:00Z"),
        ]
        conn = FakeConn([FakeCursor(rows=rows)])
        timestamps = healthcheck._load_service_timestamps(conn, "rest")
        self.assertIsNotNone(timestamps["last_discovery_ts"])
        self.assertIsNotNone(timestamps["last_min_close_ts"])

    def test_load_service_timestamps_ws_fallback(self) -> None:
        rows = [
            ("last_tick_ts", "2024-01-01T00:00:00Z", None),
        ]
        conn = FakeConn([FakeCursor(rows=rows)])
        timestamps = healthcheck._load_service_timestamps(conn, "ws")
        self.assertIsNotNone(timestamps["last_ws_tick_ts"])

        fallback_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        conn = FakeConn(
            [
                FakeCursor(rows=[]),
                FakeCursor(rows=[], fetchone_value=(fallback_ts,)),
            ]
        )
        timestamps = healthcheck._load_service_timestamps(conn, "ws")
        self.assertEqual(timestamps["last_ws_tick_ts"], fallback_ts)


class TestHealthcheckService(unittest.TestCase):
    def test_check_service_ok(self) -> None:
        fixed_now = datetime(2024, 1, 1, tzinfo=timezone.utc)

        class FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return fixed_now

        with patch("src.services.healthcheck.datetime", FixedDatetime):
            with patch("src.services.healthcheck.psycopg.connect", return_value=FakeConn()) as connect:
                with patch("src.services.healthcheck.ensure_schema_compatible", return_value=3):
                    with patch(
                        "src.services.healthcheck._load_service_timestamps",
                        return_value={"last_discovery_ts": fixed_now},
                    ):
                        with patch(
                            "src.services.healthcheck._resolve_stale_thresholds",
                            return_value={"last_discovery_ts": 60},
                        ):
                            payload = healthcheck.check_service("rest", "db")
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["schema_version"], 3)
        connect.assert_called_once()

    def test_check_service_allow_missing(self) -> None:
        with patch("src.services.healthcheck.psycopg.connect", return_value=FakeConn()):
            with patch("src.services.healthcheck.ensure_schema_compatible", return_value=3):
                with patch(
                    "src.services.healthcheck._load_service_timestamps",
                    return_value={"last_discovery_ts": None},
                ):
                    with patch(
                        "src.services.healthcheck._resolve_stale_thresholds",
                        return_value={"last_discovery_ts": 60},
                    ):
                        payload = healthcheck.check_service("rest", "db", allow_missing=True)
        self.assertTrue(payload["ok"])

    def test_health_http_response(self) -> None:
        class Handler:
            def __init__(self):
                self.status = None
                self.headers = []
                self.wfile = io.BytesIO()

            def send_response(self, status):
                self.status = status

            def send_header(self, name, value):
                self.headers.append((name, value))

            def end_headers(self):
                return None

        handler = Handler()
        healthcheck._health_http_response(handler, {"ok": True})
        self.assertEqual(handler.status, 200)
        handler = Handler()
        healthcheck._health_http_response(handler, {"ok": False})
        self.assertEqual(handler.status, 503)

    def test_start_health_server_disabled(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(healthcheck.start_health_server("rest"))
        with patch.dict(os.environ, {"SERVICE_HEALTH_PORT": "bad"}):
            with patch.object(healthcheck.logger, "error") as log_err:
                self.assertIsNone(healthcheck.start_health_server("rest"))
            log_err.assert_called_once()
        with patch.dict(os.environ, {"SERVICE_HEALTH_PORT": "8000"}, clear=True):
            with patch.object(healthcheck.logger, "error") as log_err:
                self.assertIsNone(healthcheck.start_health_server("rest"))
            log_err.assert_called_once()

    def test_start_health_server_errors(self) -> None:
        with patch.dict(
            os.environ,
            {"DATABASE_URL": "db"},
            clear=True,
        ):
            with patch("src.services.healthcheck.ThreadingHTTPServer", side_effect=OSError("boom")):
                with patch.object(healthcheck.logger, "error") as log_err:
                    server = healthcheck.start_health_server("rest", host="127.0.0.1", port=8000)
        self.assertIsNone(server)
        log_err.assert_called_once()

    def test_start_health_server_success(self) -> None:
        class FakeServer:
            def __init__(self, addr, handler_cls):
                self.addr = addr
                self.handler_cls = handler_cls

            def serve_forever(self):
                return None

            def server_close(self):
                return None

        class FakeThread:
            def __init__(self, target, name, daemon):
                self.target = target
                self.name = name
                self.daemon = daemon
                self.started = False

            def start(self):
                self.started = True

        threads = []

        def make_thread(*args, **kwargs):
            thread = FakeThread(*args, **kwargs)
            threads.append(thread)
            return thread

        with patch.dict(os.environ, {"DATABASE_URL": "db"}, clear=True):
            with patch("src.services.healthcheck.ThreadingHTTPServer", FakeServer):
                with patch("src.services.healthcheck.threading.Thread", side_effect=make_thread):
                    with patch.object(healthcheck.logger, "info") as log_info:
                        server = healthcheck.start_health_server("rest", host="127.0.0.1", port=8001)
        self.assertIsInstance(server, FakeServer)
        self.assertTrue(threads[0].started)
        log_info.assert_called_once()

    def test_main_json_and_text(self) -> None:
        args = SimpleNamespace(service="rest", database_url="db", json=True)
        with patch("src.services.healthcheck.argparse.ArgumentParser.parse_args", return_value=args):
            with patch("src.services.healthcheck.check_service", return_value={"ok": True}):
                with patch("builtins.print") as printer:
                    with self.assertRaises(SystemExit) as exc:
                        healthcheck.main()
        self.assertEqual(exc.exception.code, 0)
        self.assertTrue(printer.called)

        args = SimpleNamespace(service="rest", database_url="db", json=False)
        with patch("src.services.healthcheck.argparse.ArgumentParser.parse_args", return_value=args):
            with patch("src.services.healthcheck.check_service", return_value={"ok": False}):
                with patch("builtins.print") as printer:
                    with self.assertRaises(SystemExit) as exc:
                        healthcheck.main()
        self.assertEqual(exc.exception.code, 1)
        printed = printer.call_args[0][0]
        self.assertIn("health=failed", printed)

    def test_main_requires_database_url(self) -> None:
        args = SimpleNamespace(service="rest", database_url=None, json=False)
        with patch("src.services.healthcheck.argparse.ArgumentParser.parse_args", return_value=args):
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(RuntimeError):
                    healthcheck.main()


class TestHealthcheckServerHandler(unittest.TestCase):
    def _make_handler(self, handler_cls, path):
        handler = handler_cls.__new__(handler_cls)
        handler.path = path
        handler.requestline = f"GET {path} HTTP/1.1"
        handler.wfile = io.BytesIO()
        captured = {"status": None, "headers": [], "ended": False}

        def send_response(code):
            captured["status"] = code

        def send_header(name, value):
            captured["headers"].append((name, value))

        def end_headers():
            captured["ended"] = True

        handler.send_response = send_response
        handler.send_header = send_header
        handler.end_headers = end_headers
        return handler, captured

    def test_handler_404_and_ok_and_error(self) -> None:
        class FakeServer:
            def __init__(self, addr, handler_cls):
                self.handler_cls = handler_cls

            def serve_forever(self):
                return None

            def server_close(self):
                return None

        class FakeThread:
            def __init__(self, target, name, daemon):
                self.target = target

            def start(self):
                return None

        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "db",
                "HEALTH_CONNECT_TIMEOUT": "5",
                "HEALTH_ALLOW_MISSING": "1",
            },
            clear=True,
        ):
            with patch("src.services.healthcheck.ThreadingHTTPServer", FakeServer):
                with patch("src.services.healthcheck.threading.Thread", FakeThread):
                    server = healthcheck.start_health_server("rest", host="127.0.0.1", port=8002)
        handler_cls = server.handler_cls

        handler, captured = self._make_handler(handler_cls, "/nope")
        handler.do_GET()
        self.assertEqual(captured["status"], 404)
        self.assertTrue(captured["ended"])

        handler, _captured = self._make_handler(handler_cls, "/health")
        payloads = {}

        def fake_response(_handler, payload):
            payloads["payload"] = payload

        with patch.object(healthcheck, "check_service", return_value={"ok": True}) as check_service:
            with patch.object(healthcheck, "_health_http_response", side_effect=fake_response):
                handler.do_GET()
        self.assertTrue(payloads["payload"]["ok"])
        check_service.assert_called_once_with(
            "rest",
            "db",
            connect_timeout=5,
            allow_missing=True,
        )

        handler, _captured = self._make_handler(handler_cls, "/healthz")
        payloads.clear()
        with patch.object(healthcheck, "check_service", side_effect=RuntimeError("boom")):
            with patch.object(healthcheck, "_health_http_response", side_effect=fake_response):
                handler.do_GET()
        self.assertFalse(payloads["payload"]["ok"])
        self.assertEqual(payloads["payload"]["error"], "boom")

    def test_handler_log_request(self) -> None:
        class FakeServer:
            def __init__(self, addr, handler_cls):
                self.handler_cls = handler_cls

            def serve_forever(self):
                return None

            def server_close(self):
                return None

        class FakeThread:
            def __init__(self, target, name, daemon):
                self.target = target

            def start(self):
                return None

        with patch.object(healthcheck.logger, "info") as log_info:
            with patch.dict(os.environ, {"DATABASE_URL": "db"}, clear=True):
                with patch("src.services.healthcheck.ThreadingHTTPServer", FakeServer):
                    with patch("src.services.healthcheck.threading.Thread", FakeThread):
                        server = healthcheck.start_health_server(
                            "rest", host="127.0.0.1", port=8003
                        )
            handler_cls = server.handler_cls
            handler = handler_cls.__new__(handler_cls)
            handler.requestline = "GET /healthz HTTP/1.1"
            handler.log_request(200, 12)
        self.assertTrue(
            any('healthz "' in call.args[0] for call in log_info.call_args_list)
        )


class TestHealthcheckServerRun(unittest.TestCase):
    def test_server_thread_runs_and_closes(self) -> None:
        class FakeServer:
            def __init__(self, addr, handler_cls):
                self.handler_cls = handler_cls
                self.served = False
                self.closed = False

            def serve_forever(self):
                self.served = True

            def server_close(self):
                self.closed = True

        class ImmediateThread:
            def __init__(self, target, name, daemon):
                self.target = target
                self.started = False

            def start(self):
                self.started = True
                self.target()

        with patch.dict(os.environ, {"DATABASE_URL": "db"}, clear=True):
            with patch("src.services.healthcheck.ThreadingHTTPServer", FakeServer):
                with patch("src.services.healthcheck.threading.Thread", ImmediateThread):
                    server = healthcheck.start_health_server("rest", host="127.0.0.1", port=8004)
        self.assertTrue(server.served)
        self.assertTrue(server.closed)


class TestHealthcheckMainExtra(unittest.TestCase):
    def test_main_handles_exception(self) -> None:
        args = SimpleNamespace(service="rest", database_url="db", json=True)
        with patch("src.services.healthcheck.argparse.ArgumentParser.parse_args", return_value=args):
            with patch("src.services.healthcheck.check_service", side_effect=RuntimeError("boom")):
                with patch("builtins.print") as printer:
                    with self.assertRaises(SystemExit) as exc:
                        healthcheck.main()
        self.assertEqual(exc.exception.code, 1)
        printed = printer.call_args[0][0]
        self.assertIn('"error"', printed)

    def test_main_guard_runpy(self) -> None:
        args = SimpleNamespace(service="rest", database_url=None, json=False)
        with patch("argparse.ArgumentParser.parse_args", return_value=args):
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(RuntimeError):
                    runpy.run_module("src.services.healthcheck", run_name="__main__")
