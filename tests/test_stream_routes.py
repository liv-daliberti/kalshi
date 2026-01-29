import importlib
import os
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from flask import Flask  # pylint: disable=import-error

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

stream_routes = importlib.import_module("src.web_portal.routes.stream")


class DummyCursor:
    def execute(self, *_args, **_kwargs) -> None:
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyConn:
    def notifies(self, timeout=1.0):
        return iter([])


class TestStreamRoutes(unittest.TestCase):
    def test_queue_stream_logs_refresh_failure(self) -> None:
        class Notify:
            def __init__(self, payload):
                self.payload = payload

        class NotifyConn:
            def notifies(self, timeout=1.0):
                return iter([Notify('{"ok": true}')])

        @contextmanager
        def fake_db(*_args, **_kwargs):
            yield NotifyConn()

        @contextmanager
        def fake_cursor(_conn):
            yield DummyCursor()

        def fake_portal_func(name, fallback):
            if name == "maybe_refresh_portal_snapshot":
                def raiser(**_kwargs):
                    raise RuntimeError("boom")
                return raiser
            return fallback

        app = Flask(__name__)
        with app.test_request_context("/stream/queue"):
            with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                with patch.object(stream_routes, "_portal_func", side_effect=fake_portal_func):
                    with patch.object(stream_routes, "_queue_stream_enabled", return_value=True):
                        with patch.object(stream_routes, "_db_connection", side_effect=fake_db):
                            with patch.object(stream_routes, "timed_cursor", side_effect=fake_cursor):
                                with patch.object(stream_routes.logger, "exception") as log_exc:
                                    resp = stream_routes.queue_stream()
                                    iterator = resp.response
                                    chunk = next(iterator)
                                    if hasattr(iterator, "close"):
                                        iterator.close()
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf-8")
        self.assertIn("event: queue", chunk)
        log_exc.assert_called_once_with("queue stream snapshot refresh failed")

    def test_queue_stream_ping_branch(self) -> None:
        @contextmanager
        def fake_db(*_args, **_kwargs):
            yield DummyConn()

        @contextmanager
        def fake_cursor(_conn):
            yield DummyCursor()

        app = Flask(__name__)
        with app.test_request_context("/stream/queue"):
            with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                with patch.object(stream_routes, "_portal_func", side_effect=lambda _name, fallback: fallback):
                    with patch.object(stream_routes, "_queue_stream_enabled", return_value=True):
                        with patch.object(stream_routes, "_db_connection", side_effect=fake_db):
                            with patch.object(stream_routes, "timed_cursor", side_effect=fake_cursor):
                                with patch.object(stream_routes.time, "monotonic", side_effect=[0.0, 20.0]):
                                    resp = stream_routes.queue_stream()
                                    chunk = next(resp.response)
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf-8")
        self.assertIn("event: ping", chunk)
