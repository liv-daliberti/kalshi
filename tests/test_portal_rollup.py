import os
import unittest
from typing import Optional
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

portal_rollup = importlib.import_module("src.db.portal_rollup")


class FakeCursor:
    def __init__(self, conn, raise_exc: Optional[Exception] = None):
        self.conn = conn
        self.raise_exc = raise_exc

    def executemany(self, sql, params):
        if self.raise_exc:
            raise self.raise_exc
        self.conn.executemanys.append((sql, list(params)))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, raise_exc: Optional[Exception] = None):
        self.executemanys = []
        self.raise_exc = raise_exc

    def cursor(self):
        return FakeCursor(self, raise_exc=self.raise_exc)


class TestPortalRollup(unittest.TestCase):
    def setUp(self) -> None:
        self._warned = portal_rollup._PORTAL_ROLLUP_REFRESH_WARNED
        portal_rollup._PORTAL_ROLLUP_REFRESH_WARNED = False

    def tearDown(self) -> None:
        portal_rollup._PORTAL_ROLLUP_REFRESH_WARNED = self._warned

    def test_refresh_events_disabled(self) -> None:
        conn = FakeConn()
        with patch.dict(os.environ, {}, clear=True):
            portal_rollup._portal_rollup_refresh_events(conn, ["EV1"])
        self.assertEqual(conn.executemanys, [])

    def test_refresh_events_filters_and_sorts(self) -> None:
        conn = FakeConn()
        with patch.dict(os.environ, {"WEB_PORTAL_ROLLUP_APP_REFRESH": "1"}):
            portal_rollup._portal_rollup_refresh_events(
                conn,
                ["B", "A", "A", None, ""],
            )
        self.assertEqual(
            conn.executemanys,
            [("SELECT portal_refresh_event_rollup(%s)", [("A",), ("B",)])],
        )

    def test_refresh_events_empty_after_filter(self) -> None:
        conn = FakeConn()
        with patch.dict(os.environ, {"WEB_PORTAL_ROLLUP_APP_REFRESH": "1"}):
            portal_rollup._portal_rollup_refresh_events(conn, [None, "", ""])
        self.assertEqual(conn.executemanys, [])

    def test_refresh_events_warns_once(self) -> None:
        conn = FakeConn(raise_exc=RuntimeError("boom"))
        with patch.dict(os.environ, {"WEB_PORTAL_ROLLUP_APP_REFRESH": "1"}):
            with patch.object(portal_rollup.logger, "warning") as warn:
                portal_rollup._portal_rollup_refresh_events(conn, ["EV1"])
                portal_rollup._portal_rollup_refresh_events(conn, ["EV1"])
        warn.assert_called_once()
