import unittest
from types import SimpleNamespace
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

market_routes = importlib.import_module("src.web_portal.routes.market")


class DummyContext:
    def __init__(self, value=None, raise_on_enter=None, raise_on_exit=None):
        self._value = value
        self._raise_on_enter = raise_on_enter
        self._raise_on_exit = raise_on_exit

    def __enter__(self):
        if self._raise_on_enter:
            raise self._raise_on_enter
        return self._value

    def __exit__(self, exc_type, exc, tb):
        if self._raise_on_exit:
            raise self._raise_on_exit
        return False


class TestMarketCacheHelpers(unittest.TestCase):
    def tearDown(self) -> None:
        market_routes._MARKET_DETAIL_CACHE.clear()

    def test_load_market_detail_cache_disabled(self) -> None:
        with patch.object(market_routes, "_market_detail_cache_ttl", return_value=0):
            self.assertIsNone(market_routes._load_market_detail_cache("M1"))

    def test_load_market_detail_cache_expired(self) -> None:
        market_routes._MARKET_DETAIL_CACHE["M1"] = (0.0, {"x": 1})
        with patch.object(market_routes, "_market_detail_cache_ttl", return_value=1), \
             patch.object(market_routes.time, "monotonic", return_value=2.0):
            self.assertIsNone(market_routes._load_market_detail_cache("M1"))
        self.assertNotIn("M1", market_routes._MARKET_DETAIL_CACHE)

    def test_load_market_detail_cache_hit(self) -> None:
        market_routes._MARKET_DETAIL_CACHE["M1"] = (1.0, {"x": 2})
        with patch.object(market_routes, "_market_detail_cache_ttl", return_value=10), \
             patch.object(market_routes.time, "monotonic", return_value=2.0):
            self.assertEqual(market_routes._load_market_detail_cache("M1"), {"x": 2})

    def test_store_market_detail_cache(self) -> None:
        with patch.object(market_routes, "_market_detail_cache_ttl", return_value=5), \
             patch.object(market_routes.time, "monotonic", return_value=123.0):
            market_routes._store_market_detail_cache("M1", {"a": 1})
        self.assertEqual(market_routes._MARKET_DETAIL_CACHE["M1"][1], {"a": 1})

    def test_store_market_detail_cache_disabled(self) -> None:
        with patch.object(market_routes, "_market_detail_cache_ttl", return_value=0):
            market_routes._store_market_detail_cache("M1", {"a": 1})
        self.assertNotIn("M1", market_routes._MARKET_DETAIL_CACHE)


class TestMarketHelpers(unittest.TestCase):
    def test_market_payload_passthrough(self) -> None:
        row = {"ticker": "M1", "open_time": "2024-01-01T00:00:00Z"}
        payload = market_routes._market_payload(row)
        self.assertEqual(payload["open_time"], "2024-01-01T00:00:00Z")


class TestMarketSnapshotPayload(unittest.TestCase):
    def test_snapshot_payload_exception_with_error_payload(self) -> None:
        handlers = market_routes.MarketSnapshotHandlers(
            db_connection=lambda: DummyContext(value="conn", raise_on_exit=RuntimeError("exit")),
            snapshot_allows_closed=lambda: True,
            market_is_closed=lambda _conn, _ticker: False,
            prefer_tick_snapshot=lambda *_args, **_kwargs: None,
            fetch_snapshot=lambda *_args, **_kwargs: ({}, None),
            insert_tick=lambda *_args, **_kwargs: None,
            set_backoff=lambda *_args, **_kwargs: None,
        )
        error_payload = {"error": "oops", "rate_limited": True}
        with patch.object(market_routes, "_market_snapshot_handlers", return_value=handlers), \
             patch.object(
                 market_routes,
                 "_snapshot_payload_from_remote",
                 return_value=({}, 200, error_payload),
             ), \
             patch.dict("os.environ", {"DATABASE_URL": "postgres://db"}):
            payload, status = market_routes._market_snapshot_payload("M1")
        self.assertEqual(status, 429)
        self.assertEqual(payload, error_payload)

    def test_snapshot_payload_exception_without_error_payload(self) -> None:
        handlers = market_routes.MarketSnapshotHandlers(
            db_connection=lambda: DummyContext(value="conn", raise_on_exit=RuntimeError("exit")),
            snapshot_allows_closed=lambda: True,
            market_is_closed=lambda _conn, _ticker: False,
            prefer_tick_snapshot=lambda *_args, **_kwargs: None,
            fetch_snapshot=lambda *_args, **_kwargs: ({}, None),
            insert_tick=lambda *_args, **_kwargs: None,
            set_backoff=lambda *_args, **_kwargs: None,
        )
        with patch.object(market_routes, "_market_snapshot_handlers", return_value=handlers), \
             patch.object(
                 market_routes,
                 "_snapshot_payload_from_remote",
                 return_value=({}, 200, None),
             ), \
             patch.dict("os.environ", {"DATABASE_URL": "postgres://db"}):
            payload, status = market_routes._market_snapshot_payload("M1")
        self.assertEqual(status, 503)
        self.assertIn("Live snapshot save failed", payload.get("error", ""))


class TestMarketDetailRoute(unittest.TestCase):
    def tearDown(self) -> None:
        market_routes._MARKET_DETAIL_CACHE.clear()

    def test_market_detail_uses_cache(self) -> None:
        def portal_func(name, default):
            if name == "render_template":
                return lambda *_args, **_kwargs: ("render", _kwargs.get("market"))
            return default

        with patch.dict("os.environ", {"DATABASE_URL": "postgres://db"}), \
             patch.object(market_routes, "_portal_func", side_effect=portal_func), \
             patch.object(market_routes, "_load_market_detail_cache", return_value={"t": 1}):
            result = market_routes.market_detail("M1")
        self.assertEqual(result, ("render", {"t": 1}))

    def test_market_detail_exception(self) -> None:
        def portal_func(name, default):
            if name == "render_template":
                return lambda *_args, **_kwargs: ("render", _kwargs.get("error"))
            if name == "_db_connection":
                return lambda: DummyContext(raise_on_enter=RuntimeError("boom"))
            if name == "fetch_market_detail":
                return lambda *_args, **_kwargs: {}
            return default

        with patch.dict("os.environ", {"DATABASE_URL": "postgres://db"}), \
             patch.object(market_routes, "_portal_func", side_effect=portal_func), \
             patch.object(market_routes, "_load_market_detail_cache", return_value=None):
            result = market_routes.market_detail("M1")
        self.assertEqual(result, ("render", "boom"))

    def test_market_detail_store_cache(self) -> None:
        def portal_func(name, default):
            if name == "render_template":
                return lambda *_args, **_kwargs: ("render", _kwargs.get("market"))
            if name == "_db_connection":
                return lambda: DummyContext(value="conn")
            if name == "fetch_market_detail":
                return lambda *_args, **_kwargs: {"ticker": "M1"}
            return default

        with patch.dict("os.environ", {"DATABASE_URL": "postgres://db"}), \
             patch.object(market_routes, "_portal_func", side_effect=portal_func), \
             patch.object(market_routes, "_load_market_detail_cache", return_value=None), \
             patch.object(market_routes, "_store_market_detail_cache") as store_cache:
            result = market_routes.market_detail("M1")
        self.assertEqual(result, ("render", {"ticker": "M1"}))
        store_cache.assert_called_once_with("M1", {"ticker": "M1"})


class TestMarketBackfillRoute(unittest.TestCase):
    def test_market_backfill_exception_returns_503(self) -> None:
        def portal_func(name, default):
            if name == "_db_connection":
                return lambda: DummyContext(raise_on_enter=RuntimeError("boom"))
            if name == "_load_backfill_config":
                return lambda: SimpleNamespace(strike_periods=("hour",))
            return default

        with patch.dict(
            "os.environ",
            {
                "WEB_PORTAL_BACKFILL_MODE": "queue",
                "WORK_QUEUE_ENABLE": "1",
                "DATABASE_URL": "postgres://db",
            },
        ), \
            patch.object(market_routes, "_portal_func", side_effect=portal_func), \
            patch.object(market_routes, "request", SimpleNamespace(get_json=lambda silent=True: {})), \
            patch.object(market_routes, "jsonify", side_effect=lambda payload: payload):
            result = market_routes.market_backfill("M1")
        self.assertEqual(result[1], 503)

    def test_market_backfill_exception_uses_existing_response(self) -> None:
        def portal_func(name, default):
            if name == "_db_connection":
                return lambda: DummyContext(value="conn", raise_on_exit=RuntimeError("boom"))
            if name == "_load_backfill_config":
                return lambda: SimpleNamespace(strike_periods=("hour",))
            return default

        with patch.dict(
            "os.environ",
            {
                "WEB_PORTAL_BACKFILL_MODE": "queue",
                "WORK_QUEUE_ENABLE": "1",
                "DATABASE_URL": "postgres://db",
            },
        ), \
            patch.object(market_routes, "_portal_func", side_effect=portal_func), \
            patch.object(
                market_routes,
                "_enqueue_market_backfill",
                return_value=({"queued": True}, 200),
            ), \
            patch.object(market_routes, "request", SimpleNamespace(get_json=lambda silent=True: {})), \
            patch.object(market_routes, "jsonify", side_effect=lambda payload: payload):
            result = market_routes.market_backfill("M1")
        self.assertEqual(result, ({"queued": True}, 200))
