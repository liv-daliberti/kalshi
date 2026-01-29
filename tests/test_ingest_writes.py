import json
import unittest
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

ingest_writes = importlib.import_module("src.db.ingest_writes")


class FakeCursor:
    def __init__(self, conn, fetchone_value=None):
        self.conn = conn
        self.fetchone_value = fetchone_value

    def execute(self, sql, params=None):
        self.conn.executes.append((sql, params))

    def executemany(self, sql, params):
        self.conn.executemanys.append((sql, list(params)))

    def fetchone(self):
        return self.fetchone_value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, fetchone_value=None):
        self.executes = []
        self.executemanys = []
        self.commits = 0
        self.cursor_obj = FakeCursor(self, fetchone_value=fetchone_value)

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class TestIngestWrites(unittest.TestCase):
    def test_lifecycle_payload_defaults(self) -> None:
        payload = ingest_writes._lifecycle_payload(
            {"ts": "t1", "market_ticker": "M1", "raw": {"a": 1}}
        )
        self.assertEqual(payload["event_type"], "unknown")
        self.assertEqual(payload["market_ticker"], "M1")
        self.assertEqual(json.loads(payload["raw"]), {"a": 1})

    def test_insert_lifecycle_events_empty(self) -> None:
        conn = FakeConn()
        ingest_writes.insert_lifecycle_events(conn, [])
        self.assertEqual(conn.executemanys, [])
        self.assertEqual(conn.commits, 0)

    def test_insert_lifecycle_events_executes(self) -> None:
        conn = FakeConn()
        ingest_writes.insert_lifecycle_events(
            conn,
            [
                {"ts": "t1", "market_ticker": "M1", "event_type": "created"},
                {"ts": "t2", "market_ticker": "M2", "raw": {"x": 2}},
            ],
        )
        self.assertEqual(len(conn.executemanys), 1)
        self.assertEqual(conn.commits, 1)
        _, payloads = conn.executemanys[0]
        self.assertEqual(payloads[0]["event_type"], "created")
        self.assertEqual(payloads[1]["event_type"], "unknown")
        self.assertEqual(json.loads(payloads[1]["raw"]), {"x": 2})

    def test_insert_lifecycle_event_wrapper(self) -> None:
        with patch.object(ingest_writes, "insert_lifecycle_events") as insert_events:
            ingest_writes.insert_lifecycle_event(object(), {"market_ticker": "M1"})
        insert_events.assert_called_once()
        args, _ = insert_events.call_args
        self.assertEqual(args[1], [{"market_ticker": "M1"}])

    def test_insert_prediction_run_returns_id(self) -> None:
        conn = FakeConn(fetchone_value=(42,))
        spec = ingest_writes.PredictionRunSpec(
            event_ticker="EV1",
            prompt="p",
            agent="a",
            model="m",
            status="done",
            metadata={"k": "v"},
        )
        run_id = ingest_writes.insert_prediction_run(conn, spec)
        self.assertEqual(run_id, 42)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.executes), 1)

    def test_update_prediction_run_handles_null_metadata(self) -> None:
        conn = FakeConn()
        ingest_writes.update_prediction_run(conn, run_id=7, status="failed", error="boom")
        self.assertEqual(conn.commits, 1)
        _, params = conn.executes[0]
        self.assertEqual(params[2], None)

    def test_insert_market_predictions_empty(self) -> None:
        conn = FakeConn()
        ingest_writes.insert_market_predictions(conn, [])
        self.assertEqual(conn.executemanys, [])
        self.assertEqual(conn.commits, 0)

    def test_insert_market_predictions_rollup(self) -> None:
        conn = FakeConn()
        predictions = [
            {"event_ticker": "EV2", "market_ticker": "MK2", "raw": {"a": 1}},
            {"event_ticker": "EV1", "market_ticker": "MK1", "raw": {"b": 2}},
            {"event_ticker": None, "market_ticker": "MK3", "raw": {"c": 3}},
        ]
        with patch.object(ingest_writes, "_portal_rollup_refresh_events") as refresh:
            ingest_writes.insert_market_predictions(conn, predictions)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.executemanys), 1)
        refresh.assert_called_once()
        _, tickers = refresh.call_args[0]
        self.assertEqual(set(tickers), {"EV1", "EV2"})

    def test_insert_market_prediction_wrapper(self) -> None:
        with patch.object(ingest_writes, "insert_market_predictions") as insert_predictions:
            ingest_writes.insert_market_prediction(object(), {"market_ticker": "M1"})
        insert_predictions.assert_called_once()
        args, _ = insert_predictions.call_args
        self.assertEqual(args[1], [{"market_ticker": "M1"}])
