import json
import os
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

db = importlib.import_module("src.db.db")


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = conn.rowcount

    def execute(self, sql, params=None):
        self.conn.executes.append((sql, params))

    def executemany(self, sql, params):
        self.conn.executemanys.append((sql, list(params)))

    def fetchone(self):
        if self.conn.fetchone_queue:
            return self.conn.fetchone_queue.pop(0)
        return None

    def fetchall(self):
        if self.conn.fetchall_queue:
            return self.conn.fetchall_queue.pop(0)
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, fetchone_queue=None, fetchall_queue=None, rowcount=0):
        self.executes = []
        self.executemanys = []
        self.commits = 0
        self.fetchone_queue = list(fetchone_queue or [])
        self.fetchall_queue = list(fetchall_queue or [])
        self.rowcount = rowcount

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1


class TestDbHelpers(unittest.TestCase):
    def test_parse_ts_iso_none(self) -> None:
        self.assertIsNone(db.parse_ts_iso(None))

    def test_parse_ts_iso_datetime(self) -> None:
        naive = datetime(2024, 1, 1, 0, 0, 0)
        parsed = db.parse_ts_iso(naive)
        self.assertEqual(parsed.tzinfo, timezone.utc)

    def test_parse_ts_iso_string(self) -> None:
        parsed = db.parse_ts_iso("2024-01-01T00:00:00Z")
        self.assertEqual(int(parsed.timestamp()), 1704067200)
        self.assertEqual(parsed.utcoffset(), timezone.utc.utcoffset(parsed))

    def test_dec(self) -> None:
        self.assertEqual(db.dec("1.23"), Decimal("1.23"))
        self.assertIsNone(db.dec(None))
        self.assertIsNone(db.dec("not-a-number"))

    def test_implied_yes_mid_cents(self) -> None:
        self.assertIsNone(db.implied_yes_mid_cents(None, None))
        self.assertEqual(db.implied_yes_mid_cents(70, None), Decimal("0.700000"))
        self.assertEqual(db.implied_yes_mid_cents(None, 40), Decimal("0.400000"))
        self.assertEqual(db.implied_yes_mid_cents(40, 60), Decimal("0.500000"))

    def test_json_default(self) -> None:
        naive = datetime(2024, 1, 1, 0, 0, 0)
        self.assertEqual(
            db._json_default(naive),
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat(),
        )
        self.assertEqual(db._json_default(Decimal("1.23")), "1.23")
        self.assertEqual(db._json_default(5), "5")

    def test_insert_market_ticks_skips_missing_ts(self) -> None:
        class FakeCursor:
            def __init__(self):
                self.executemany_calls = []
                self.execute_calls = []

            def executemany(self, sql, payloads):
                self.executemany_calls.append((sql, payloads))

            def execute(self, sql, params):
                self.execute_calls.append((sql, params))

            def fetchall(self):
                return []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConn:
            def __init__(self):
                self.cursor_obj = FakeCursor()
                self.commits = 0

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                self.commits += 1

        conn = FakeConn()
        ticks = [
            {"ts": None, "ticker": "M1"},
            {"ts": "2024-01-01T00:00:00Z", "ticker": "M1", "raw": {"source": "stream"}},
        ]
        db.insert_market_ticks(conn, ticks)
        self.assertEqual(len(conn.cursor_obj.executemany_calls), 2)
        self.assertEqual(len(conn.cursor_obj.execute_calls), 3)
        self.assertEqual(conn.commits, 1)

    def test_insert_lifecycle_event_wrapper(self) -> None:
        with patch("src.db.db.insert_lifecycle_events") as insert_events:
            db.insert_lifecycle_event(conn=object(), event={"market_ticker": "M1"})
        insert_events.assert_called_once()

    def test_insert_market_prediction_wrapper(self) -> None:
        with patch("src.db.db.insert_market_predictions") as insert_predictions:
            db.insert_market_prediction(conn=object(), prediction={"market_ticker": "M1"})
        insert_predictions.assert_called_once()

    def test_to_json_value(self) -> None:
        self.assertIsNone(db.to_json_value(None))
        payload = db.to_json_value({"a": 1, "b": [2, 3]})
        self.assertEqual(json.loads(payload), {"a": 1, "b": [2, 3]})
        self.assertEqual(db.to_json_value("plain"), "plain")

    def test_rollup_refresh_on_ticks_when_enabled(self) -> None:
        conn = FakeConn(fetchall_queue=[[], [("EV1",)]])
        ticks = [{"ts": "2024-01-01T00:00:00Z", "ticker": "MK1"}]
        with patch.dict(os.environ, {"WEB_PORTAL_ROLLUP_APP_REFRESH": "1"}):
            db.insert_market_ticks(conn, ticks)
        refresh_calls = [
            call for call in conn.executemanys
            if "portal_refresh_event_rollup" in call[0]
        ]
        self.assertEqual(refresh_calls, [("SELECT portal_refresh_event_rollup(%s)", [("EV1",)])])

    def test_rollup_refresh_on_predictions_when_enabled(self) -> None:
        conn = FakeConn()
        predictions = [
            {"event_ticker": "EV1", "market_ticker": "MK1"},
            {"event_ticker": "EV2", "market_ticker": "MK2"},
        ]
        with patch.dict(os.environ, {"WEB_PORTAL_ROLLUP_APP_REFRESH": "1"}):
            db.insert_market_predictions(conn, predictions)
        refresh_calls = [
            call for call in conn.executemanys
            if "portal_refresh_event_rollup" in call[0]
        ]
        self.assertEqual(
            refresh_calls,
            [("SELECT portal_refresh_event_rollup(%s)", [("EV1",), ("EV2",)])],
        )


class TestDbSchemaAndState(unittest.TestCase):
    def test_env_bool_defaults(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(db._env_bool("DB_INIT_SCHEMA", True))
            self.assertFalse(db._env_bool("DB_INIT_SCHEMA", False))
        with patch.dict(os.environ, {"DB_INIT_SCHEMA": "0"}):
            self.assertFalse(db._env_bool("DB_INIT_SCHEMA", True))
        with patch.dict(os.environ, {"DB_INIT_SCHEMA": "yes"}):
            self.assertTrue(db._env_bool("DB_INIT_SCHEMA", False))

    def test_env_int_invalid_and_minimum(self) -> None:
        with patch.dict(os.environ, {"TEST_INT": "bad"}):
            self.assertEqual(db._env_int("TEST_INT", 3), 3)
        with patch.dict(os.environ, {"TEST_INT": "0"}):
            self.assertEqual(db._env_int("TEST_INT", 3, minimum=1), 1)

    def test_init_schema_executes_sql(self) -> None:
        conn = FakeConn()
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write("SELECT 1;")
            schema_path = handle.name
        try:
            db.init_schema(conn, schema_path)
        finally:
            os.unlink(schema_path)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.executes), 1)
        self.assertIn("SELECT 1", conn.executes[0][0])

    def test_maybe_init_schema(self) -> None:
        conn = FakeConn()
        with patch.dict(os.environ, {"DB_INIT_SCHEMA": "0"}), \
             patch("src.db.db.init_schema") as init_schema:
            db.maybe_init_schema(conn, "schema.sql")
        init_schema.assert_not_called()

        with patch.dict(os.environ, {"DB_INIT_SCHEMA": "1"}), \
             patch("src.db.db.init_schema") as init_schema:
            db.maybe_init_schema(conn, "schema.sql")
        init_schema.assert_called_once_with(conn, "schema.sql")

    def test_fetch_schema_version_missing_table(self) -> None:
        class ErrorCursor:
            def execute(self, *_args, **_kwargs):
                raise db.psycopg.errors.UndefinedTable()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class ErrorConn:
            def cursor(self):
                return ErrorCursor()

        self.assertIsNone(db._fetch_schema_version(ErrorConn()))

    def test_fetch_schema_version_null_row(self) -> None:
        conn = FakeConn(fetchone_queue=[(None,)])
        self.assertIsNone(db._fetch_schema_version(conn))

    def test_ensure_schema_compatible_disabled(self) -> None:
        with patch.dict(os.environ, {"SCHEMA_VALIDATE": "0"}):
            self.assertEqual(db.ensure_schema_compatible(object()), -1)

    def test_ensure_schema_compatible_missing_version(self) -> None:
        with patch.object(db, "_fetch_schema_version", return_value=None):
            with self.assertRaises(RuntimeError):
                db.ensure_schema_compatible(object())

    def test_ensure_schema_compatible_mismatch(self) -> None:
        with patch.object(db, "_fetch_schema_version", return_value=3), \
             patch.object(db, "_schema_compat_range", return_value=(1, 1)):
            with self.assertRaises(RuntimeError):
                db.ensure_schema_compatible(object())

    def test_get_state_returns_default_when_missing(self) -> None:
        conn = FakeConn(fetchone_queue=[None])
        result = db.get_state(conn, "missing", default="fallback")
        self.assertEqual(result, "fallback")
        self.assertEqual(conn.commits, 0)
        self.assertEqual(len(conn.executes), 1)

    def test_get_state_returns_value(self) -> None:
        conn = FakeConn(fetchone_queue=[("value",)])
        result = db.get_state(conn, "key", default="fallback")
        self.assertEqual(result, "value")

    def test_set_state_executes_upsert(self) -> None:
        conn = FakeConn()
        db.set_state(conn, "key", "value")
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.executes), 1)
        sql, params = conn.executes[0]
        self.assertIn("INSERT INTO ingest_state", sql)
        self.assertEqual(params, ("key", "value"))

    def test_get_event_updated_at(self) -> None:
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        conn = FakeConn(fetchone_queue=[(ts,)])
        self.assertEqual(db.get_event_updated_at(conn, "EV1"), ts)

    def test_get_market_updated_at_missing(self) -> None:
        conn = FakeConn(fetchone_queue=[None])
        self.assertIsNone(db.get_market_updated_at(conn, "MK1"))


class TestDbUpserts(unittest.TestCase):
    def test_upsert_event_payload(self) -> None:
        conn = FakeConn()
        event = {
            "event_ticker": "EV1",
            "series_ticker": "SR1",
            "title": "Title",
            "sub_title": "Sub",
            "category": "Cat",
            "mutually_exclusive": True,
            "collateral_return_type": "cash",
            "available_on_brokers": ["B1"],
            "product_metadata": {"meta": "value"},
            "strike_date": "2024-01-01T00:00:00Z",
            "strike_period": "day",
        }
        db.upsert_event(conn, event)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.executes), 1)
        _, payload = conn.executes[0]
        self.assertEqual(payload["event_ticker"], "EV1")
        self.assertEqual(json.loads(payload["product_metadata"]), {"meta": "value"})
        self.assertIsInstance(payload["strike_date"], datetime)

    def test_upsert_event_falls_back_to_event_metadata(self) -> None:
        conn = FakeConn()
        event = {
            "event_ticker": "EV1",
            "event_metadata": {"fallback": True},
        }
        db.upsert_event(conn, event)
        _, payload = conn.executes[0]
        self.assertEqual(payload["event_ticker"], "EV1")
        self.assertEqual(json.loads(payload["product_metadata"]), {"fallback": True})

    def test_upsert_market_payload(self) -> None:
        conn = FakeConn()
        market = {
            "ticker": "MK1",
            "event_ticker": "EV1",
            "market_type": "binary",
            "title": "Title",
            "subtitle": "Sub",
            "yes_sub_title": "Yes",
            "no_sub_title": "No",
            "category": "Cat",
            "response_price_units": "cents",
            "created_time": "2024-01-01T00:00:00Z",
            "open_time": "2024-01-01T01:00:00Z",
            "close_time": "2024-01-01T02:00:00Z",
            "expiration_time": "2024-01-01T03:00:00Z",
            "latest_expiration_time": "2024-01-01T04:00:00Z",
            "expected_expiration_time": "2024-01-01T05:00:00Z",
            "settlement_timer_seconds": 10,
            "can_close_early": True,
            "early_close_condition": "cond",
            "rules_primary": "rules",
            "rules_secondary": "rules2",
            "tick_size": 1,
            "risk_limit_cents": 100,
            "price_level_structure": "levels",
            "price_ranges": [{"min": 1, "max": 2}],
            "strike_type": "range",
            "floor_strike": "1",
            "cap_strike": "2",
            "functional_strike": "3",
            "custom_strike": {"custom": True},
            "mve_collection_ticker": "COL",
            "mve_selected_legs": [{"leg": 1}],
            "primary_participant_key": "key",
            "settlement_value": "settled",
            "settlement_value_dollars": "0.12",
            "settlement_ts": "2024-01-01T06:00:00Z",
        }
        db.upsert_market(conn, market)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.executes), 1)
        _, payload = conn.executes[0]
        self.assertIsInstance(payload["created_time"], datetime)
        self.assertEqual(payload["settlement_value_dollars"], Decimal("0.12"))
        self.assertEqual(json.loads(payload["price_ranges"]), [{"min": 1, "max": 2}])
        self.assertEqual(json.loads(payload["custom_strike"]), {"custom": True})
        self.assertEqual(json.loads(payload["mve_selected_legs"]), [{"leg": 1}])

    def test_upsert_market_derives_metadata(self) -> None:
        conn = FakeConn()
        market = {
            "ticker": "MK1",
            "event_ticker": "EV1",
            "price_level_structure": json.dumps({"levels": [{"min": 0, "max": 1}]}),
            "strike_type": "range",
            "floor_strike": 1,
            "cap_strike": 2,
            "mve_selected_legs": json.dumps(["L1"]),
        }
        db.upsert_market(conn, market)
        _, payload = conn.executes[0]
        self.assertEqual(
            json.loads(payload["price_ranges"]),
            {"levels": [{"min": 0, "max": 1}]},
        )
        self.assertEqual(
            json.loads(payload["custom_strike"]),
            {"strike_type": "range", "floor_strike": 1, "cap_strike": 2},
        )
        self.assertEqual(json.loads(payload["mve_selected_legs"]), ["L1"])


class TestActiveMarkets(unittest.TestCase):
    def test_upsert_active_market(self) -> None:
        conn = FakeConn()
        close_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        db.upsert_active_market(conn, "MK1", "EV1", close_time, "open")
        self.assertEqual(conn.commits, 1)
        sql, params = conn.executes[0]
        self.assertIn("INSERT INTO active_markets", sql)
        self.assertEqual(params, ("MK1", "EV1", close_time, "open"))

    def test_delete_active_market(self) -> None:
        conn = FakeConn()
        db.delete_active_market(conn, "MK1")
        self.assertEqual(conn.commits, 1)
        sql, params = conn.executes[0]
        self.assertIn("DELETE FROM active_markets", sql)
        self.assertEqual(params, ("MK1",))

    def test_cleanup_active_markets(self) -> None:
        conn = FakeConn(rowcount=3)
        deleted = db.cleanup_active_markets(conn, grace_minutes=15)
        self.assertEqual(deleted, 3)
        self.assertEqual(conn.commits, 1)
        _, params = conn.executes[0]
        self.assertEqual(params, (15,))

    def test_load_active_tickers(self) -> None:
        conn = FakeConn(fetchall_queue=[[("MK1",), ("MK2",)]])
        tickers = db.load_active_tickers(conn, limit=2)
        self.assertEqual(tickers, ["MK1", "MK2"])
        _, params = conn.executes[0]
        self.assertEqual(params, (2,))

    def test_load_active_tickers_shard_default(self) -> None:
        with patch("src.db.db.load_active_tickers", return_value=["MK1"]) as load:
            result = db.load_active_tickers_shard(
                conn=object(),
                limit=1,
                shard_count=1,
                shard_id=0,
            )
        self.assertEqual(result, ["MK1"])
        load.assert_called_once()

    def test_load_active_tickers_shard_invalid(self) -> None:
        with self.assertRaises(ValueError):
            db.load_active_tickers_shard(
                conn=object(),
                limit=1,
                shard_count=2,
                shard_id=2,
            )

    def test_load_active_tickers_shard_with_limit(self) -> None:
        conn = FakeConn(fetchall_queue=[[("MK1",), ("MK2",)]])
        tickers = db.load_active_tickers_shard(
            conn=conn,
            limit=2,
            shard_count=4,
            shard_id=1,
        )
        self.assertEqual(tickers, ["MK1", "MK2"])
        sql, params = conn.executes[0]
        self.assertIn("LIMIT %s", sql)
        self.assertEqual(params, [4, 1, 2])

    def test_load_active_tickers_shard_market_key(self) -> None:
        conn = FakeConn(fetchall_queue=[[("MK1",)]])
        tickers = db.load_active_tickers_shard(
            conn=conn,
            limit=1,
            shard_count=2,
            shard_id=1,
            shard_key="market",
        )
        self.assertEqual(tickers, ["MK1"])
        sql, params = conn.executes[0]
        self.assertIn("hashtext(ticker)", sql)
        self.assertEqual(params, [2, 1, 1])

    def test_load_active_tickers_shard_without_limit(self) -> None:
        conn = FakeConn(fetchall_queue=[[("MK1",)]])
        tickers = db.load_active_tickers_shard(
            conn=conn,
            limit=0,
            shard_count=3,
            shard_id=1,
        )
        self.assertEqual(tickers, ["MK1"])
        sql, params = conn.executes[0]
        self.assertNotIn("LIMIT %s", sql)
        self.assertEqual(params, [3, 1])


class TestMarketTicks(unittest.TestCase):
    def test_latest_tick_payloads_selects_latest(self) -> None:
        ticks = [
            {"ts": "2024-01-01T00:00:10Z", "ticker": "MK1", "price": 11},
            {"ts": "2024-01-01T00:00:05Z", "ticker": "MK1", "price": 10},
            {"ts": "2024-01-01T00:00:03Z", "ticker": "MK2", "price": 20},
        ]
        payloads = [db._market_tick_payload(tick) for tick in ticks]
        latest = db._latest_tick_payloads(ticks, payloads)
        latest_map = {payload["ticker"]: payload["ts"] for payload in latest}
        self.assertEqual(latest_map["MK1"], "2024-01-01T00:00:10Z")
        self.assertEqual(latest_map["MK2"], "2024-01-01T00:00:03Z")

    def test_insert_market_tick_delegates(self) -> None:
        with patch("src.db.db.insert_market_ticks") as insert_ticks:
            db.insert_market_tick(conn=object(), tick={"ticker": "MK1"})
        insert_ticks.assert_called_once()
        args, _ = insert_ticks.call_args
        self.assertEqual(args[1], [{"ticker": "MK1"}])

    def test_insert_market_ticks_empty(self) -> None:
        conn = FakeConn()
        db.insert_market_ticks(conn, [])
        self.assertEqual(conn.commits, 0)
        self.assertEqual(conn.executes, [])
        self.assertEqual(conn.executemanys, [])

    def test_insert_market_ticks_updates_state(self) -> None:
        conn = FakeConn()
        ticks = [
            {
                "ts": "2024-01-01T00:00:00Z",
                "ticker": "MK1",
                "price": 10,
                "raw": {"source": "live_snapshot"},
            },
            {
                "ts": "2024-01-01T00:00:05Z",
                "ticker": "MK1",
                "price": 11,
                "raw": {"source": "websocket"},
            },
        ]
        db.insert_market_ticks(conn, ticks)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.executemanys), 2)
        _, payloads = conn.executemanys[0]
        self.assertEqual(len(payloads), 2)
        self.assertEqual(json.loads(payloads[0]["raw"]), {"source": "live_snapshot"})
        self.assertEqual(json.loads(payloads[1]["raw"]), {"source": "websocket"})
        self.assertEqual(len(conn.executes), 3)
        keys = {
            params[0]
            for _, params in conn.executes
            if params and isinstance(params[0], str)
        }
        self.assertEqual(keys, {"last_tick_ts", "last_ws_tick_ts"})

    def test_insert_market_ticks_live_snapshot_only(self) -> None:
        conn = FakeConn()
        ticks = [
            {
                "ts": "2024-01-01T00:00:00Z",
                "ticker": "MK1",
                "raw": {"source": "live_snapshot"},
            },
        ]
        db.insert_market_ticks(conn, ticks)
        self.assertEqual(len(conn.executes), 2)
        keys = {
            params[0]
            for _, params in conn.executes
            if params and isinstance(params[0], str)
        }
        self.assertEqual(keys, {"last_tick_ts"})

    def test_insert_market_ticks_updates_latest(self) -> None:
        conn = FakeConn()
        ticks = [
            {"ts": "2024-01-01T00:00:00Z", "ticker": "MK1", "price": 10},
            {"ts": "2024-01-01T00:00:05Z", "ticker": "MK1", "price": 11},
            {"ts": "2024-01-01T00:00:03Z", "ticker": "MK2", "price": 20},
        ]
        db.insert_market_ticks(conn, ticks)
        self.assertEqual(len(conn.executemanys), 2)
        _, latest_payloads = conn.executemanys[1]
        latest_map = {payload["ticker"]: payload["ts"] for payload in latest_payloads}
        self.assertEqual(latest_map["MK1"], "2024-01-01T00:00:05Z")
        self.assertEqual(latest_map["MK2"], "2024-01-01T00:00:03Z")


class TestLifecycleAndPredictions(unittest.TestCase):
    def test_insert_lifecycle_events_empty(self) -> None:
        conn = FakeConn()
        db.insert_lifecycle_events(conn, [])
        self.assertEqual(conn.commits, 0)
        self.assertEqual(conn.executemanys, [])

    def test_insert_lifecycle_events_payload(self) -> None:
        conn = FakeConn()
        events = [
            {
                "ts": "2024-01-01T00:00:00Z",
                "market_ticker": "MK1",
                "raw": {"source": "api"},
            }
        ]
        db.insert_lifecycle_events(conn, events)
        self.assertEqual(conn.commits, 1)
        _, payloads = conn.executemanys[0]
        self.assertEqual(payloads[0]["event_type"], "unknown")
        self.assertEqual(json.loads(payloads[0]["raw"]), {"source": "api"})

    def test_insert_prediction_run_returns_id(self) -> None:
        conn = FakeConn(fetchone_queue=[(7,)])
        run_id = db.insert_prediction_run(
            conn,
            db.PredictionRunSpec(
                event_ticker="EV1",
                prompt="prompt",
                agent="agent",
                model="model",
                status="running",
                error=None,
                metadata={"m": 1},
            ),
        )
        self.assertEqual(run_id, 7)
        self.assertEqual(conn.commits, 1)
        _, params = conn.executes[0]
        self.assertEqual(params[0], "EV1")
        self.assertEqual(json.loads(params[-1]), {"m": 1})

    def test_update_prediction_run_without_metadata(self) -> None:
        conn = FakeConn()
        db.update_prediction_run(conn, run_id=5, status="done", error="err", metadata=None)
        self.assertEqual(conn.commits, 1)
        _, params = conn.executes[0]
        self.assertEqual(params, ("done", "err", None, 5))

    def test_update_prediction_run_with_metadata(self) -> None:
        conn = FakeConn()
        db.update_prediction_run(conn, run_id=5, status="done", error=None, metadata={"a": 1})
        _, params = conn.executes[0]
        self.assertEqual(json.loads(params[2]), {"a": 1})

    def test_insert_market_predictions_empty(self) -> None:
        conn = FakeConn()
        db.insert_market_predictions(conn, [])
        self.assertEqual(conn.commits, 0)
        self.assertEqual(conn.executemanys, [])

    def test_insert_market_predictions_payload(self) -> None:
        conn = FakeConn()
        predictions = [
            {
                "run_id": 1,
                "event_ticker": "EV1",
                "market_ticker": "MK1",
                "predicted_yes_prob": 0.7,
                "confidence": 0.9,
                "rationale": "reason",
                "raw": {"foo": "bar"},
            }
        ]
        db.insert_market_predictions(conn, predictions)
        self.assertEqual(conn.commits, 1)
        _, payloads = conn.executemanys[0]
        self.assertEqual(json.loads(payloads[0]["raw"]), {"foo": "bar"})


class TestDbAdditionalCoverage(unittest.TestCase):
    def test_import_error_sets_enqueue_job_none(self) -> None:
        original_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name.endswith("queue.work_queue"):
                raise ImportError("boom")
            return original_import(name, globals, locals, fromlist, level)

        try:
            with patch("builtins.__import__", side_effect=fake_import):
                reloaded = importlib.reload(db)
                self.assertIsNone(reloaded.enqueue_job)
                self.assertIsNotNone(reloaded._ENQUEUE_JOB_IMPORT_ERROR)
        finally:
            importlib.reload(db)

    def test_portal_rollup_refresh_events_empty(self) -> None:
        conn = FakeConn()
        with patch.dict(os.environ, {"WEB_PORTAL_ROLLUP_APP_REFRESH": "1"}):
            db._portal_rollup_refresh_events(conn, ["", None])
        self.assertEqual(conn.executemanys, [])

    def test_portal_rollup_refresh_events_exception_warn_once(self) -> None:
        class FailCursor:
            def executemany(self, *_args, **_kwargs):
                raise RuntimeError("boom")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FailConn:
            def cursor(self):
                return FailCursor()

        db._PORTAL_ROLLUP_REFRESH_WARNED = False
        try:
            with patch.dict(os.environ, {"WEB_PORTAL_ROLLUP_APP_REFRESH": "1"}), \
                 patch.object(db.logger, "warning") as warn:
                db._portal_rollup_refresh_events(FailConn(), ["EV1"])
                db._portal_rollup_refresh_events(FailConn(), ["EV2"])
            self.assertEqual(warn.call_count, 1)
        finally:
            db._PORTAL_ROLLUP_REFRESH_WARNED = False

    def test_event_tickers_for_market_tickers_empty(self) -> None:
        conn = FakeConn()
        self.assertEqual(db._event_tickers_for_market_tickers(conn, []), [])
        self.assertEqual(conn.executes, [])

    def test_ensure_schema_compatible_success(self) -> None:
        with patch.object(db, "_fetch_schema_version", return_value=3), \
             patch.object(db, "_schema_compat_range", return_value=(1, 3)), \
             patch.dict(os.environ, {"SCHEMA_VALIDATE": "1"}):
            self.assertEqual(db.ensure_schema_compatible(object()), 3)

    def test_normalize_prob_dollars_converts_cents(self) -> None:
        self.assertEqual(db.normalize_prob_dollars("50"), Decimal("0.5"))

    def test_coerce_metadata_value_string_paths(self) -> None:
        self.assertIsNone(db._coerce_metadata_value("   "))
        with patch.object(db, "_maybe_parse_json", return_value=None):
            self.assertEqual(db._coerce_metadata_value("plain"), "plain")
        self.assertEqual(db._coerce_metadata_value(123), 123)

    def test_derive_custom_strike_all_none(self) -> None:
        self.assertIsNone(
            db._derive_custom_strike(
                {
                    "strike_type": None,
                    "floor_strike": None,
                    "cap_strike": None,
                    "functional_strike": None,
                }
            )
        )

    def test_market_is_active_branches(self) -> None:
        is_active, status = db.market_is_active({"status": "open"})
        self.assertTrue(is_active)
        self.assertEqual(status, "open")

        is_active, status = db.market_is_active({"status": "closed"})
        self.assertFalse(is_active)
        self.assertEqual(status, "closed")

        now = datetime.now(timezone.utc)
        is_active, status = db.market_is_active(
            {"open_time": now - timedelta(hours=1), "close_time": now + timedelta(hours=1)}
        )
        self.assertTrue(is_active)
        self.assertEqual(status, "")

    def test_maybe_upsert_active_market_from_market_paths(self) -> None:
        self.assertFalse(db.maybe_upsert_active_market_from_market(object(), {}))
        with patch.object(db, "market_is_active", return_value=(False, "")):
            self.assertFalse(
                db.maybe_upsert_active_market_from_market(
                    object(), {"ticker": "M1", "event_ticker": "E1"}
                )
            )
        with patch.object(db, "market_is_active", return_value=(True, "open")), \
             patch.object(db, "upsert_active_market") as upsert:
            result = db.maybe_upsert_active_market_from_market(
                object(),
                {"ticker": "M1", "event_ticker": "E1", "close_time": "2024-01-01T00:00:00Z"},
            )
        self.assertTrue(result)
        upsert.assert_called_once()

    def test_delete_active_market_with_rollup_refresh(self) -> None:
        conn = FakeConn(fetchall_queue=[[("EV1",), (None,)]])
        with patch.dict(os.environ, {"WEB_PORTAL_ROLLUP_APP_REFRESH": "1"}), \
             patch.object(db, "_portal_rollup_refresh_events") as refresh:
            db.delete_active_market(conn, "MK1")
        refresh.assert_called_once_with(conn, {"EV1"})
        self.assertEqual(conn.commits, 1)

    def test_cleanup_active_markets_with_rollup_refresh(self) -> None:
        conn = FakeConn(fetchall_queue=[[("EV1",), ("EV2",)]])
        with patch.dict(os.environ, {"WEB_PORTAL_ROLLUP_APP_REFRESH": "1"}), \
             patch.object(db, "_portal_rollup_refresh_events") as refresh:
            deleted = db.cleanup_active_markets(conn, grace_minutes=15)
        self.assertEqual(deleted, 2)
        refresh.assert_called_once_with(conn, {"EV1", "EV2"})
        self.assertEqual(conn.commits, 1)

    def test_upsert_active_markets_from_markets_paths(self) -> None:
        conn = FakeConn(fetchall_queue=[[("EV1",), ("EV2",)]])
        self.assertEqual(db.upsert_active_markets_from_markets(conn, []), 0)
        with patch.object(db, "_portal_rollup_refresh_events") as refresh:
            inserted = db.upsert_active_markets_from_markets(conn, ["M1", "M2"])
        self.assertEqual(inserted, 2)
        refresh.assert_called_once_with(conn, {"EV1", "EV2"})

    def test_seed_active_markets_from_markets(self) -> None:
        conn = FakeConn(fetchall_queue=[[("EV1",)]])
        with patch.object(db, "_portal_rollup_refresh_events") as refresh:
            inserted = db.seed_active_markets_from_markets(conn)
        self.assertEqual(inserted, 1)
        refresh.assert_called_once_with(conn, {"EV1"})

    def test_latest_tick_payloads_skips_invalid_ticker(self) -> None:
        ticks = [
            {"ts": "2024-01-01T00:00:00Z"},
            {"ts": "2024-01-01T00:00:01Z"},
        ]
        payloads = [
            {"ticker": None, "ts": "2024-01-01T00:00:00Z"},
            {"ticker": "MK1", "ts": "2024-01-01T00:00:01Z"},
        ]
        latest = db._latest_tick_payloads(ticks, payloads)
        self.assertEqual(latest, [payloads[1]])

    def test_ensure_markets_exist_empty(self) -> None:
        self.assertEqual(db._ensure_markets_exist(FakeConn(), set()), [])

    def test_enqueue_discover_market_jobs_no_enqueue_job(self) -> None:
        conn = FakeConn()
        original = db.enqueue_job
        original_err = db._ENQUEUE_JOB_IMPORT_ERROR
        try:
            db.enqueue_job = None
            db._ENQUEUE_JOB_IMPORT_ERROR = ("boom",)
            with patch.object(db.logger, "error") as log_error:
                db._enqueue_discover_market_jobs(conn, ["M1"])
            log_error.assert_called_once()
        finally:
            db.enqueue_job = original
            db._ENQUEUE_JOB_IMPORT_ERROR = original_err

    def test_enqueue_discover_market_jobs_no_import_error(self) -> None:
        conn = FakeConn()
        original = db.enqueue_job
        original_err = db._ENQUEUE_JOB_IMPORT_ERROR
        try:
            db.enqueue_job = None
            db._ENQUEUE_JOB_IMPORT_ERROR = None
            with patch.object(db.logger, "error") as log_error:
                db._enqueue_discover_market_jobs(conn, ["M1"])
            log_error.assert_called_once()
        finally:
            db.enqueue_job = original
            db._ENQUEUE_JOB_IMPORT_ERROR = original_err

    def test_enqueue_discover_market_jobs_permission_error(self) -> None:
        conn = FakeConn()

        def raise_perm(*_args, **_kwargs):
            raise PermissionError("blocked")

        original = db.enqueue_job
        try:
            db.enqueue_job = raise_perm
            with patch.object(db.logger, "warning") as warn:
                db._enqueue_discover_market_jobs(conn, ["M1"])
            warn.assert_called_once()
            self.assertEqual(conn.commits, 0)
        finally:
            db.enqueue_job = original

    def test_enqueue_discover_market_jobs_success(self) -> None:
        conn = FakeConn()
        calls = []

        def enqueue(_conn, job_type, payload, commit=False):
            calls.append((job_type, payload, commit))

        original = db.enqueue_job
        try:
            db.enqueue_job = enqueue
            db._enqueue_discover_market_jobs(conn, ["M1", "M2"])
        finally:
            db.enqueue_job = original
        self.assertEqual(len(calls), 2)
        self.assertEqual(conn.commits, 1)

    def test_enqueue_discover_market_jobs_rollback_failure(self) -> None:
        class RollbackConn(FakeConn):
            def rollback(self):
                raise RuntimeError("rollback failed")

        def raise_error(*_args, **_kwargs):
            raise RuntimeError("boom")

        conn = RollbackConn()
        original = db.enqueue_job
        try:
            db.enqueue_job = raise_error
            with patch.object(db.logger, "exception") as log_exc, \
                 patch.object(db.logger, "warning") as log_warn:
                db._enqueue_discover_market_jobs(conn, ["M1"])
            log_exc.assert_called_once()
            log_warn.assert_called_once()
        finally:
            db.enqueue_job = original

    def test_log_placeholder_inserts(self) -> None:
        with patch.object(db, "_log_metric") as metric:
            db._log_placeholder_inserts(["A", "B"])
        metric.assert_called_once()
