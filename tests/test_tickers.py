import unittest
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

tickers = importlib.import_module("src.db.tickers")


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        self.conn.executes.append((sql, params))

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
    def __init__(self, fetchone_queue=None, fetchall_queue=None):
        self.executes = []
        self.fetchone_queue = list(fetchone_queue or [])
        self.fetchall_queue = list(fetchall_queue or [])

    def cursor(self):
        return FakeCursor(self)


class TestTickerHelpers(unittest.TestCase):
    def test_active_tickers_where_default(self) -> None:
        where_sql, params = tickers._active_tickers_where(1, 0)
        self.assertIn("close_time", where_sql)
        self.assertNotIn("hashtext", where_sql)
        self.assertEqual(params, [])

    def test_active_tickers_where_sharded_event(self) -> None:
        where_sql, params = tickers._active_tickers_where(3, 1, shard_key="event")
        self.assertIn("hashtext(coalesce(event_ticker, ticker))", where_sql)
        self.assertEqual(params, [3, 1])

    def test_active_tickers_where_sharded_market(self) -> None:
        where_sql, params = tickers._active_tickers_where(2, 0, shard_key="market")
        self.assertIn("hashtext(ticker)", where_sql)
        self.assertEqual(params, [2, 0])

    def test_active_tickers_where_invalid_key(self) -> None:
        with self.assertRaises(ValueError):
            tickers._active_tickers_where(2, 0, shard_key="bad")

    def test_active_tickers_count(self) -> None:
        conn = FakeConn(fetchone_queue=[(5,)])
        count = tickers._active_tickers_count(conn, "WHERE TRUE", [])
        self.assertEqual(count, 5)
        self.assertTrue(conn.executes)

    def test_load_state_int_defaults(self) -> None:
        with patch("src.db.tickers.get_state", return_value="7"):
            self.assertEqual(tickers._load_state_int(object(), "k", 0), 7)
        with patch("src.db.tickers.get_state", return_value="-3"):
            self.assertEqual(tickers._load_state_int(object(), "k", 0), 0)
        with patch("src.db.tickers.get_state", return_value="bad"):
            self.assertEqual(tickers._load_state_int(object(), "k", 4), 4)

    def test_store_state_int_clamps(self) -> None:
        conn = object()
        with patch("src.db.tickers.set_state") as set_state:
            tickers._store_state_int(conn, "k", -5)
            tickers._store_state_int(conn, "k", 6)
        calls = [call.args for call in set_state.call_args_list]
        self.assertEqual(calls, [(conn, "k", "0"), (conn, "k", "6")])

    def test_load_active_tickers_slice_params_tuple(self) -> None:
        conn = FakeConn(fetchall_queue=[[("A",), ("B",)]])
        rows = tickers._load_active_tickers_slice(conn, "WHERE TRUE", [], 2)
        self.assertEqual(rows, ["A", "B"])
        _, params = conn.executes[0]
        self.assertIsInstance(params, tuple)

    def test_load_active_tickers_slice_with_offset(self) -> None:
        conn = FakeConn(fetchall_queue=[[("C",)]])
        rows = tickers._load_active_tickers_slice(conn, "WHERE TRUE", ["X"], 1, offset=3)
        self.assertEqual(rows, ["C"])
        _, params = conn.executes[0]
        self.assertEqual(params, ["X", 1, 3])

    def test_load_active_tickers_round_robin_limit_zero(self) -> None:
        query = tickers._ActiveTickerQuery("WHERE TRUE", [], 0)
        with patch.object(
            tickers,
            "_load_active_tickers_slice",
            return_value=["A"],
        ) as load_slice:
            rows = tickers._load_active_tickers_round_robin(object(), query, "k")
        self.assertEqual(rows, ["A"])
        load_slice.assert_called_once()

    def test_load_active_tickers_round_robin_wrap(self) -> None:
        query = tickers._ActiveTickerQuery("WHERE TRUE", [], 3)
        conn = object()
        with patch.object(tickers, "_active_tickers_count", return_value=5), \
            patch.object(tickers, "_load_state_int", return_value=4), \
            patch.object(tickers, "_load_active_tickers_slice", side_effect=[["T4"], ["T0", "T1"]]), \
            patch.object(tickers, "_store_state_int") as store_state:
            rows = tickers._load_active_tickers_round_robin(conn, query, "k")
        self.assertEqual(rows, ["T4", "T0", "T1"])
        store_state.assert_called_once_with(conn, "k", 7)

    def test_load_active_tickers_round_robin_total_leq_limit(self) -> None:
        query = tickers._ActiveTickerQuery("WHERE TRUE", [], 5)
        with patch.object(tickers, "_active_tickers_count", return_value=2), \
            patch.object(tickers, "_load_active_tickers_slice", return_value=["A", "B"]) as load_slice:
            rows = tickers._load_active_tickers_round_robin(object(), query, "k")
        self.assertEqual(rows, ["A", "B"])
        load_slice.assert_called_once()

    def test_load_active_tickers_paths(self) -> None:
        with patch.object(tickers, "_load_active_tickers_slice", return_value=["A"]) as load_slice:
            rows = tickers.load_active_tickers(object(), 0)
        self.assertEqual(rows, ["A"])
        load_slice.assert_called_once()

    def test_load_active_tickers_round_robin_default_key(self) -> None:
        with patch.object(
            tickers,
            "_load_active_tickers_round_robin",
            return_value=["A"],
        ) as rr:
            rows = tickers.load_active_tickers(object(), 5, round_robin=True)
        self.assertEqual(rows, ["A"])
        args, _ = rr.call_args
        self.assertEqual(args[2], "ws_active_cursor:1:0")

    def test_load_active_tickers_shard_invalid(self) -> None:
        with self.assertRaises(ValueError):
            tickers.load_active_tickers_shard(object(), 10, shard_count=2, shard_id=2)

    def test_load_active_tickers_shard_count_one(self) -> None:
        with patch.object(tickers, "load_active_tickers", return_value=["A"]) as load_active:
            rows = tickers.load_active_tickers_shard(object(), 5, shard_count=1, shard_id=0)
        self.assertEqual(rows, ["A"])
        load_active.assert_called_once()

    def test_load_active_tickers_shard_round_robin(self) -> None:
        with patch.object(tickers, "_active_tickers_where", return_value=("WHERE TRUE", [])), \
            patch.object(tickers, "_load_active_tickers_round_robin", return_value=["A"]) as rr:
            rows = tickers.load_active_tickers_shard(object(), 5, shard_count=2, shard_id=1, round_robin=True)
        self.assertEqual(rows, ["A"])
        rr.assert_called_once()

    def test_market_tickers_where_invalid_key(self) -> None:
        with self.assertRaises(ValueError):
            tickers._market_tickers_where(2, 0, shard_key="bad")

    def test_market_tickers_where_sharded_event(self) -> None:
        where_sql, params = tickers._market_tickers_where(3, 1, shard_key="event")
        self.assertIn("hashtext(coalesce(event_ticker, ticker))", where_sql)
        self.assertEqual(params, [3, 1])

    def test_market_tickers_count(self) -> None:
        conn = FakeConn(fetchone_queue=[(4,)])
        count = tickers._market_tickers_count(conn, "WHERE TRUE", [])
        self.assertEqual(count, 4)
        self.assertTrue(conn.executes)

    def test_load_market_tickers_slice_params_tuple(self) -> None:
        conn = FakeConn(fetchall_queue=[[("M1",)]])
        rows = tickers._load_market_tickers_slice(conn, "WHERE TRUE", [], 1)
        self.assertEqual(rows, ["M1"])
        _, params = conn.executes[0]
        self.assertIsInstance(params, tuple)

    def test_load_market_tickers_slice_with_offset(self) -> None:
        conn = FakeConn(fetchall_queue=[[("M2",)]])
        rows = tickers._load_market_tickers_slice(conn, "WHERE TRUE", ["X"], 1, offset=2)
        self.assertEqual(rows, ["M2"])
        _, params = conn.executes[0]
        self.assertEqual(params, ["X", 1, 2])

    def test_load_market_tickers_round_robin_limit_zero(self) -> None:
        query = tickers._ActiveTickerQuery("WHERE TRUE", [], 0)
        with patch.object(
            tickers,
            "_load_market_tickers_slice",
            return_value=["M1"],
        ) as load_slice:
            rows = tickers._load_market_tickers_round_robin(object(), query, "k")
        self.assertEqual(rows, ["M1"])
        load_slice.assert_called_once()

    def test_load_market_tickers_round_robin_total_leq_limit(self) -> None:
        query = tickers._ActiveTickerQuery("WHERE TRUE", [], 5)
        with patch.object(tickers, "_market_tickers_count", return_value=2), \
            patch.object(tickers, "_load_market_tickers_slice", return_value=["M1", "M2"]) as load_slice:
            rows = tickers._load_market_tickers_round_robin(object(), query, "k")
        self.assertEqual(rows, ["M1", "M2"])
        load_slice.assert_called_once()

    def test_load_market_tickers_round_robin_wrap(self) -> None:
        query = tickers._ActiveTickerQuery("WHERE TRUE", [], 2)
        conn = object()
        with patch.object(tickers, "_market_tickers_count", return_value=3), \
            patch.object(tickers, "_load_state_int", return_value=2), \
            patch.object(tickers, "_load_market_tickers_slice", side_effect=[["M2"], ["M0"]]), \
            patch.object(tickers, "_store_state_int") as store_state:
            rows = tickers._load_market_tickers_round_robin(conn, query, "k")
        self.assertEqual(rows, ["M2", "M0"])
        store_state.assert_called_once_with(conn, "k", 4)

    def test_load_market_tickers(self) -> None:
        with patch.object(tickers, "_load_market_tickers_slice", return_value=["M1"]) as load_slice:
            rows = tickers.load_market_tickers(object(), 0)
        self.assertEqual(rows, ["M1"])
        load_slice.assert_called_once()

    def test_load_market_tickers_limit_positive(self) -> None:
        with patch.object(tickers, "_load_market_tickers_slice", return_value=["M1"]) as load_slice:
            rows = tickers.load_market_tickers(object(), 3)
        self.assertEqual(rows, ["M1"])
        args, _ = load_slice.call_args
        self.assertEqual(args[3], 3)

    def test_load_market_tickers_shard_invalid(self) -> None:
        with self.assertRaises(ValueError):
            tickers.load_market_tickers_shard(object(), 10, shard_count=2, shard_id=2)

    def test_load_market_tickers_shard_limit_zero(self) -> None:
        conn = object()
        with patch.object(tickers, "_market_tickers_where", return_value=("WHERE TRUE", [])), \
            patch.object(tickers, "_load_market_tickers_slice", return_value=["M1"]) as load_slice:
            rows = tickers.load_market_tickers_shard(
                conn,
                0,
                shard_count=2,
                shard_id=1,
            )
        self.assertEqual(rows, ["M1"])
        load_slice.assert_called_once_with(conn, "WHERE TRUE", [], None)

    def test_load_market_tickers_shard_round_robin(self) -> None:
        with patch.object(tickers, "_market_tickers_where", return_value=("WHERE TRUE", [])), \
            patch.object(tickers, "_load_market_tickers_round_robin", return_value=["M1"]) as rr:
            rows = tickers.load_market_tickers_shard(object(), 5, shard_count=2, shard_id=1, round_robin=True)
        self.assertEqual(rows, ["M1"])
        rr.assert_called_once()

    def test_load_market_tickers_shard_limit_positive(self) -> None:
        conn = object()
        with patch.object(tickers, "_market_tickers_where", return_value=("WHERE TRUE", [])), \
            patch.object(tickers, "_load_market_tickers_slice", return_value=["M1"]) as load_slice:
            rows = tickers.load_market_tickers_shard(
                conn,
                5,
                shard_count=2,
                shard_id=1,
            )
        self.assertEqual(rows, ["M1"])
        load_slice.assert_called_once_with(conn, "WHERE TRUE", [], 5)
