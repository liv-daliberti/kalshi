import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_kalshi_sdk_stub, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()
ensure_kalshi_sdk_stub()

discovery = importlib.import_module("src.jobs.discovery")


class TestDiscoveryPass(unittest.TestCase):
    def test_discovery_pass_filters_and_counts(self) -> None:
        events = [
            {
                "strike_period": "Hour",
                "event_ticker": "EV1",
                "markets": [
                    {
                        "ticker": "M1",
                        "event_ticker": "EV1",
                        "status": "open",
                        "close_time": "2024-01-01T01:00:00Z",
                    },
                    {
                        "ticker": "M2",
                        "event_ticker": "EV1",
                        "status": "closed",
                    },
                ],
            },
            {
                "strike_period": "week",
                "event_ticker": "EV2",
                "markets": [
                    {"ticker": "M3", "event_ticker": "EV2", "status": "open"},
                ],
            },
        ]

        with patch("src.jobs.discovery.iter_events", return_value=events), \
             patch("src.jobs.discovery.upsert_event") as upsert_event, \
             patch("src.jobs.discovery.upsert_market") as upsert_market, \
             patch("src.jobs.discovery.upsert_active_market") as upsert_active_market, \
             patch("src.jobs.discovery.get_state", return_value=None), \
             patch("src.jobs.discovery.set_state") as set_state, \
             patch("src.jobs.discovery.cleanup_active_markets", return_value=0), \
             patch("src.jobs.discovery.parse_ts_iso", return_value="parsed"):
            result = discovery.discovery_pass(
                conn=object(),
                client=object(),
                strike_periods=("hour",),
                event_statuses=("open",),
            )

        self.assertEqual(result, (1, 2, 1))
        self.assertEqual(upsert_event.call_count, 1)
        self.assertEqual(upsert_market.call_count, 2)
        self.assertEqual(upsert_active_market.call_count, 1)
        _, kwargs = upsert_active_market.call_args
        self.assertEqual(kwargs["ticker"], "M1")
        self.assertEqual(kwargs["event_ticker"], "EV1")
        self.assertEqual(kwargs["close_time"], "parsed")
        set_state.assert_any_call(
            unittest.mock.ANY,
            "last_discovery_ts",
            unittest.mock.ANY,
        )

    def test_discovery_pass_event_failure_isolated(self) -> None:
        events = [
            {
                "strike_period": "hour",
                "event_ticker": "EV_BAD",
                "markets": [
                    {"ticker": "MB1", "event_ticker": "EV_BAD", "status": "open"},
                ],
            },
            {
                "strike_period": "hour",
                "event_ticker": "EV_OK",
                "markets": [
                    {
                        "ticker": "M2",
                        "event_ticker": "EV_OK",
                        "status": "open",
                        "close_time": "2024-01-01T01:00:00Z",
                    },
                ],
            },
        ]

        def fake_upsert_event(_conn, event):
            if event.get("event_ticker") == "EV_BAD":
                raise RuntimeError("boom")

        with patch("src.jobs.discovery.iter_events", return_value=events), \
             patch("src.jobs.discovery.upsert_event", side_effect=fake_upsert_event) as upsert_event, \
             patch("src.jobs.discovery.upsert_market") as upsert_market, \
             patch("src.jobs.discovery.upsert_active_market") as upsert_active_market, \
             patch("src.jobs.discovery.get_state", return_value=None), \
             patch("src.jobs.discovery.set_state"), \
             patch("src.jobs.discovery.cleanup_active_markets", return_value=0), \
             patch("src.jobs.discovery.parse_ts_iso", return_value="parsed"):
            result = discovery.discovery_pass(
                conn=object(),
                client=object(),
                strike_periods=("hour",),
                event_statuses=("open",),
            )

        self.assertEqual(result, (1, 1, 1))
        self.assertEqual(upsert_event.call_count, 2)
        self.assertEqual(upsert_market.call_count, 1)
        self.assertEqual(upsert_active_market.call_count, 1)

    def test_resolve_events_method(self) -> None:
        class ClientWithIter:
            def iter_events(self):
                return []

        class ClientWithGet:
            def get_events(self):
                return []

        class ClientWithList:
            def list_events(self):
                return []

        class ClientEmpty:
            pass

        self.assertEqual(
            discovery._resolve_events_method(ClientWithIter()).__name__,
            "iter_events",
        )
        self.assertEqual(
            discovery._resolve_events_method(ClientWithGet()).__name__,
            "get_events",
        )
        self.assertEqual(
            discovery._resolve_events_method(ClientWithList()).__name__,
            "list_events",
        )
        self.assertIsNone(discovery._resolve_events_method(ClientEmpty()))

    def test_build_updated_since_params_fallbacks(self) -> None:
        last_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)

        class ClientNoMethod:
            pass

        class ClientNoUpdated:
            def events(self, status):
                return []

        self.assertEqual(
            discovery._build_updated_since_params(ClientNoMethod(), last_dt),
            {},
        )
        self.assertEqual(
            discovery._build_updated_since_params(ClientNoUpdated(), last_dt),
            {},
        )


class TestDiscoveryHelpers(unittest.TestCase):
    def test_map_event_status(self) -> None:
        self.assertEqual(discovery._map_event_status("active"), "open")
        self.assertEqual(discovery._map_event_status("closed"), "closed")

    def test_parse_timestamp_variants(self) -> None:
        ts = 1_700_000_000
        ms = 1_700_000_000_000
        self.assertIsNone(discovery._parse_timestamp(None))
        self.assertEqual(
            discovery._parse_timestamp(ts),
            datetime.fromtimestamp(ts, tz=timezone.utc),
        )
        self.assertEqual(
            discovery._parse_timestamp(float(ts)),
            datetime.fromtimestamp(ts, tz=timezone.utc),
        )
        self.assertEqual(
            discovery._parse_timestamp(ms),
            datetime.fromtimestamp(ms // 1000, tz=timezone.utc),
        )
        self.assertEqual(
            discovery._parse_timestamp(str(ts)),
            datetime.fromtimestamp(ts, tz=timezone.utc),
        )
        self.assertEqual(
            discovery._parse_timestamp(str(ms)),
            datetime.fromtimestamp(ms // 1000, tz=timezone.utc),
        )
        parsed = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch("src.jobs.discovery.parse_ts_iso", return_value=parsed):
            self.assertEqual(
                discovery._parse_timestamp("2024-01-01T00:00:00Z"),
                parsed,
            )
        with patch("src.jobs.discovery.parse_ts_iso", side_effect=ValueError("bad")):
            self.assertIsNone(discovery._parse_timestamp("bad"))

    def test_extract_updated_dt(self) -> None:
        ts = 1_700_000_000
        payload = {"updated_time": "", "updated_at": str(ts)}
        result = discovery._extract_updated_dt(payload)
        self.assertEqual(result, datetime.fromtimestamp(ts, tz=timezone.utc))

    def test_resolve_events_method(self) -> None:
        class ClientIter:
            def iter_events(self):
                return None

        class ClientGet:
            def get_events(self):
                return None

        class ClientList:
            def list_events(self):
                return None

        client_iter = ClientIter()
        client_get = ClientGet()
        client_list = ClientList()
        method = discovery._resolve_events_method(client_iter)
        self.assertIs(method.__self__, client_iter)
        self.assertIs(method.__func__, client_iter.iter_events.__func__)
        method = discovery._resolve_events_method(client_get)
        self.assertIs(method.__self__, client_get)
        self.assertIs(method.__func__, client_get.get_events.__func__)
        method = discovery._resolve_events_method(client_list)
        self.assertIs(method.__self__, client_list)
        self.assertIs(method.__func__, client_list.list_events.__func__)

    def test_build_updated_since_params(self) -> None:
        last_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)

        class ClientKwargs:
            def iter_events(self, **kwargs):
                return None

        class ClientUpdatedAfterTs:
            def iter_events(self, status=None, updated_after_ts=None):
                return None

        class ClientUpdatedAfter:
            def iter_events(self, updated_after=None):
                return None

        class ClientUpdatedSince:
            def iter_events(self, updated_since=None):
                return None

        self.assertEqual(
            discovery._build_updated_since_params(ClientUpdatedAfterTs(), None),
            {},
        )
        self.assertEqual(
            discovery._build_updated_since_params(ClientKwargs(), last_dt),
            {},
        )
        with patch.dict(os.environ, {"DISCOVERY_UPDATED_SINCE_PARAM": "updated_after_ts"}):
            self.assertEqual(
                discovery._build_updated_since_params(ClientKwargs(), last_dt),
                {"updated_after_ts": int(last_dt.timestamp())},
            )
        self.assertEqual(
            discovery._build_updated_since_params(ClientUpdatedAfterTs(), last_dt),
            {"updated_after_ts": int(last_dt.timestamp())},
        )
        self.assertEqual(
            discovery._build_updated_since_params(ClientUpdatedAfter(), last_dt),
            {"updated_after": last_dt.isoformat()},
        )
        self.assertEqual(
            discovery._build_updated_since_params(ClientUpdatedSince(), last_dt),
            {"updated_since": last_dt.isoformat()},
        )

    def test_updated_since_override_unknown(self) -> None:
        last_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch.dict(os.environ, {"DISCOVERY_UPDATED_SINCE_PARAM": "mystery"}), \
             patch("src.jobs.discovery.logger.warning") as warn:
            result = discovery._updated_since_override(last_dt)
        self.assertEqual(result, {})
        warn.assert_called_once()

    def test_updated_since_override_known_values(self) -> None:
        last_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch.dict(os.environ, {"DISCOVERY_UPDATED_SINCE_PARAM": "updated_after"}):
            self.assertEqual(
                discovery._updated_since_override(last_dt),
                {"updated_after": last_dt.isoformat()},
            )
        with patch.dict(os.environ, {"DISCOVERY_UPDATED_SINCE_PARAM": "updated_since"}):
            self.assertEqual(
                discovery._updated_since_override(last_dt),
                {"updated_since": last_dt.isoformat()},
            )

    def test_parse_discovery_cursor(self) -> None:
        self.assertIsNone(discovery._parse_discovery_cursor(None))
        ts = 1_700_000_000
        self.assertEqual(
            discovery._parse_discovery_cursor(str(ts)),
            datetime.fromtimestamp(ts, tz=timezone.utc),
        )

    def test_should_skip_event_and_market(self) -> None:
        last_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        event_old = {"updated_time": int(last_dt.timestamp()) - 10}
        event_new = {"updated_time": int(last_dt.timestamp()) + 10}
        self.assertTrue(discovery._should_skip_event(object(), event_old, last_dt))
        self.assertFalse(discovery._should_skip_event(object(), event_new, last_dt))
        event_db = {"event_ticker": "EV1"}
        with patch("src.jobs.discovery.get_event_updated_at", return_value=last_dt - timedelta(seconds=1)):
            self.assertTrue(discovery._should_skip_event(object(), event_db, last_dt))
        with patch("src.jobs.discovery.get_event_updated_at", return_value=last_dt + timedelta(seconds=1)):
            self.assertFalse(discovery._should_skip_event(object(), event_db, last_dt))
        self.assertFalse(discovery._should_skip_event(object(), {}, last_dt))

        market_old = {"updated_ts": int(last_dt.timestamp()) - 10}
        market_new = {"updated_ts": int(last_dt.timestamp()) + 10}
        self.assertTrue(discovery._should_skip_market(object(), market_old, last_dt))
        self.assertFalse(discovery._should_skip_market(object(), market_new, last_dt))
        market_db = {"ticker": "M1"}
        with patch("src.jobs.discovery.get_market_updated_at", return_value=last_dt - timedelta(seconds=1)):
            self.assertTrue(discovery._should_skip_market(object(), market_db, last_dt))
        with patch("src.jobs.discovery.get_market_updated_at", return_value=last_dt + timedelta(seconds=1)):
            self.assertFalse(discovery._should_skip_market(object(), market_db, last_dt))
        self.assertFalse(discovery._should_skip_market(object(), {}, last_dt))

    def test_should_skip_event_and_market_without_cursor(self) -> None:
        event = {"updated_time": 1700000000}
        market = {"updated_time": 1700000000}
        self.assertFalse(discovery._should_skip_event(object(), event, None))
        self.assertFalse(discovery._should_skip_market(object(), market, None))

    def test_upsert_active_market_wrapper_kwargs(self) -> None:
        with patch("src.jobs.discovery.maybe_upsert_active_market_from_market", return_value=True) as upsert:
            result = discovery.upsert_active_market(
                object(),
                ticker="M1",
                event_ticker="EV1",
            )
        self.assertTrue(result)
        upsert.assert_called_once_with(
            unittest.mock.ANY,
            {"ticker": "M1", "event_ticker": "EV1"},
        )

    def test_market_is_active_status_and_time(self) -> None:
        now = datetime.now(timezone.utc)
        self.assertTrue(discovery._market_is_active({"status": "open"}, None, None))
        self.assertTrue(discovery._market_is_active({"status": "paused"}, None, None))
        self.assertFalse(discovery._market_is_active({"status": "closed"}, None, None))
        self.assertTrue(
            discovery._market_is_active(
                {},
                now - timedelta(days=1),
                now + timedelta(days=1),
            )
        )
        self.assertFalse(
            discovery._market_is_active(
                {},
                now + timedelta(days=1),
                None,
            )
        )
        self.assertFalse(
            discovery._market_is_active(
                {},
                None,
                now - timedelta(days=1),
            )
        )


class TestDiscoveryProcessMarket(unittest.TestCase):
    def test_process_market_skips(self) -> None:
        with patch("src.jobs.discovery._should_skip_market", return_value=True), \
             patch("src.jobs.discovery.upsert_market") as upsert_market:
            result = discovery._process_market(
                object(),
                {"ticker": "M1"},
                datetime.now(timezone.utc),
            )
        self.assertEqual(result, (0, 0))
        upsert_market.assert_not_called()

    def test_process_market_inactive(self) -> None:
        market = {"ticker": "M1", "status": "closed"}
        with patch("src.jobs.discovery._should_skip_market", return_value=False), \
             patch("src.jobs.discovery.parse_ts_iso", return_value=None), \
             patch("src.jobs.discovery.upsert_market") as upsert_market, \
             patch("src.jobs.discovery.upsert_active_market") as upsert_active_market:
            result = discovery._process_market(object(), market, None)
        self.assertEqual(result, (1, 0))
        upsert_market.assert_called_once()
        upsert_active_market.assert_not_called()

    def test_process_market_active(self) -> None:
        market = {
            "ticker": "M1",
            "event_ticker": "EV1",
            "status": "open",
            "open_time": "open",
            "close_time": "close",
        }
        with patch("src.jobs.discovery._should_skip_market", return_value=False), \
             patch("src.jobs.discovery.parse_ts_iso", side_effect=["open_dt", "close_dt"]), \
             patch("src.jobs.discovery.upsert_market") as upsert_market, \
             patch("src.jobs.discovery.upsert_active_market", return_value=True) as upsert_active_market:
            result = discovery._process_market(object(), market, None)
        self.assertEqual(result, (1, 1))
        upsert_market.assert_called_once()
        upsert_active_market.assert_called_once_with(
            unittest.mock.ANY,
            ticker="M1",
            event_ticker="EV1",
            open_time="open_dt",
            close_time="close_dt",
            status="open",
        )


class TestDiscoveryProcessEvent(unittest.TestCase):
    def test_process_event_skips_when_filtered(self) -> None:
        stats = discovery.EventScanStats()
        event = {"event_ticker": "EV1", "markets": []}
        with patch("src.jobs.discovery.accept_event", return_value=None), \
             patch("src.jobs.discovery.upsert_event") as upsert_event:
            result = discovery._process_event(object(), event, ("hour",), stats, None)
        self.assertEqual(result, (0, 0, 0))
        upsert_event.assert_not_called()

    def test_process_event_upsert_error_returns_zero(self) -> None:
        stats = discovery.EventScanStats()
        event = {"event_ticker": "EV1", "markets": []}
        with patch("src.jobs.discovery.accept_event", return_value="hour"), \
             patch("src.jobs.discovery._should_skip_event", return_value=False), \
             patch("src.jobs.discovery.upsert_event", side_effect=RuntimeError("boom")), \
             patch("src.jobs.discovery._handle_event_upsert_error", return_value=False):
            result = discovery._process_event(object(), event, ("hour",), stats, None)
        self.assertEqual(result, (0, 0, 0))

    def test_process_event_raises_on_event_upsert_error(self) -> None:
        stats = discovery.EventScanStats()
        event = {"event_ticker": "EV1", "markets": []}
        with patch("src.jobs.discovery.accept_event", return_value="hour"), \
             patch("src.jobs.discovery._should_skip_event", return_value=False), \
             patch("src.jobs.discovery.upsert_event", side_effect=RuntimeError("boom")), \
             patch("src.jobs.discovery._handle_event_upsert_error", return_value=True):
            with self.assertRaises(RuntimeError):
                discovery._process_event(object(), event, ("hour",), stats, None)

    def test_process_event_raises_on_market_upsert_error(self) -> None:
        stats = discovery.EventScanStats()
        event = {"event_ticker": "EV1", "markets": [{"ticker": "M1"}]}
        with patch("src.jobs.discovery.accept_event", return_value="hour"), \
             patch("src.jobs.discovery._should_skip_event", return_value=False), \
             patch("src.jobs.discovery.upsert_event"), \
             patch("src.jobs.discovery._process_market", side_effect=RuntimeError("boom")), \
             patch("src.jobs.discovery._handle_market_upsert_error", return_value=True):
            with self.assertRaises(RuntimeError):
                discovery._process_event(object(), event, ("hour",), stats, None)

    def test_process_event_market_error_continues(self) -> None:
        stats = discovery.EventScanStats()
        event = {
            "event_ticker": "EV1",
            "markets": [{"ticker": "M1"}, {"ticker": "M2"}],
        }
        with patch("src.jobs.discovery.accept_event", return_value="hour"), \
             patch("src.jobs.discovery._should_skip_event", return_value=False), \
             patch("src.jobs.discovery.upsert_event") as upsert_event, \
             patch(
                 "src.jobs.discovery._process_market",
                 side_effect=[RuntimeError("boom"), (1, 0)],
             ) as process_market, \
             patch("src.jobs.discovery._handle_market_upsert_error", return_value=False):
            result = discovery._process_event(object(), event, ("hour",), stats, None)
        self.assertEqual(result, (1, 1, 0))
        upsert_event.assert_called_once()
        self.assertEqual(process_market.call_count, 2)


class TestDiscoveryEventStatus(unittest.TestCase):
    def test_process_event_status_handles_event_errors(self) -> None:
        class FakeHeartbeat:
            def __init__(self):
                self.calls = 0

            def beat(self, *args, **kwargs):
                self.calls += 1

        heartbeat = FakeHeartbeat()
        context = discovery.EventProcessContext(
            conn=object(),
            client=object(),
            strike_periods=(),
            stats=discovery.EventScanStats(),
            last_discovery_dt=None,
            updated_since_params={},
            heartbeat=heartbeat,
        )

        def fake_iter_events(client, rate_limit_hook=None, **params):
            if rate_limit_hook is not None:
                rate_limit_hook(0.0)
            return [{"event_ticker": "EV1"}]

        with patch("src.jobs.discovery.iter_events", new=fake_iter_events), \
             patch("src.jobs.discovery._process_event", side_effect=RuntimeError("boom")), \
             patch("src.jobs.discovery._is_connection_error", return_value=False), \
             patch("src.jobs.discovery._safe_rollback") as rollback, \
             patch("src.jobs.discovery._log_metric") as log_metric:
            result = discovery._process_event_status(context, "open")

        self.assertEqual(result, (0, 0, 0))
        self.assertGreaterEqual(heartbeat.calls, 2)
        log_metric.assert_called_once()
        rollback.assert_called_once()

    def test_process_event_status_raises_on_connection_error(self) -> None:
        class FakeHeartbeat:
            def __init__(self):
                self.calls = 0

            def beat(self, *args, **kwargs):
                self.calls += 1

        context = discovery.EventProcessContext(
            conn=object(),
            client=object(),
            strike_periods=(),
            stats=discovery.EventScanStats(),
            last_discovery_dt=None,
            updated_since_params={},
            heartbeat=FakeHeartbeat(),
        )

        def boom_iter_events(*_args, **_kwargs):
            raise RuntimeError("boom")

        with patch("src.jobs.discovery.iter_events", new=boom_iter_events), \
             patch("src.jobs.discovery._is_connection_error", return_value=True), \
             patch("src.jobs.discovery._safe_rollback") as rollback:
            with self.assertRaises(RuntimeError):
                discovery._process_event_status(context, "open")

        rollback.assert_called_once()

    def test_process_event_status_raises_on_event_connection_error(self) -> None:
        class FakeHeartbeat:
            def beat(self, *args, **kwargs):
                return None

        context = discovery.EventProcessContext(
            conn=object(),
            client=object(),
            strike_periods=(),
            stats=discovery.EventScanStats(),
            last_discovery_dt=None,
            updated_since_params={},
            heartbeat=FakeHeartbeat(),
        )

        with patch("src.jobs.discovery.iter_events", return_value=[{"event_ticker": "EV1"}]), \
             patch("src.jobs.discovery._process_event", side_effect=RuntimeError("boom")), \
             patch("src.jobs.discovery._is_connection_error", return_value=True), \
             patch("src.jobs.discovery._safe_rollback") as rollback:
            with self.assertRaises(RuntimeError):
                discovery._process_event_status(context, "open")

        self.assertGreaterEqual(rollback.call_count, 1)

    def test_process_event_status_accumulates_counts(self) -> None:
        class FakeHeartbeat:
            def beat(self, *args, **kwargs):
                return None

        context = discovery.EventProcessContext(
            conn=object(),
            client=object(),
            strike_periods=(),
            stats=discovery.EventScanStats(),
            last_discovery_dt=None,
            updated_since_params={},
            heartbeat=FakeHeartbeat(),
        )

        events = [{"event_ticker": "EV1"}, {"event_ticker": "EV2"}]
        with patch("src.jobs.discovery.iter_events", return_value=events), \
             patch(
                 "src.jobs.discovery._process_event",
                 side_effect=[(1, 2, 3), (0, 1, 1)],
             ):
            result = discovery._process_event_status(context, "active")

        self.assertEqual(result, (1, 3, 4))


class TestDiscoveryContext(unittest.TestCase):
    def test_build_discovery_context_logs_updated_since(self) -> None:
        last_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch("src.jobs.discovery.get_state", return_value="raw"), \
             patch("src.jobs.discovery._parse_discovery_cursor", return_value=last_dt), \
             patch("src.jobs.discovery.env_int", return_value=60), \
             patch("src.jobs.discovery.set_state") as set_state, \
             patch(
                 "src.jobs.discovery._build_updated_since_params",
                 return_value={"updated_after_ts": 123},
             ), \
             patch("src.jobs.discovery.logger.info") as info:
            context = discovery._build_discovery_context(object(), object(), ("hour",))

        self.assertEqual(context.updated_since_params, {"updated_after_ts": 123})
        set_state.assert_called_once()
        self.assertTrue(
            any(
                call.args[:2]
                == ("discovery_pass: using updated-since filter %s", {"updated_after_ts": 123})
                for call in info.call_args_list
            )
        )


class TestDiscoveryPassEdgeCases(unittest.TestCase):
    def test_discovery_pass_updated_since_and_skips(self) -> None:
        now = datetime.now(timezone.utc)
        last_raw = str(int(now.timestamp()))
        last_dt = datetime.fromtimestamp(int(last_raw), tz=timezone.utc)

        class ClientUpdatedAfterTs:
            def iter_events(self, status=None, updated_after_ts=None, with_nested_markets=None):
                return None

        events = [
            {
                "strike_period": "hour",
                "event_ticker": "EV_SKIP",
                "updated_time": int(now.timestamp()) - 10,
                "markets": [],
            },
            {
                "strike_period": "hour",
                "event_ticker": "EV_KEEP",
                "updated_time": int(now.timestamp()) + 10,
                "markets": [
                    {"ticker": "M_SKIP", "event_ticker": "EV_KEEP"},
                    {
                        "ticker": "M_ACTIVE",
                        "event_ticker": "EV_KEEP",
                        "open_time": "open",
                        "close_time": "close",
                    },
                ],
            },
        ]

        def fake_parse_ts_iso(value):
            if value == "open":
                return now - timedelta(hours=1)
            if value == "close":
                return now + timedelta(hours=1)
            return None

        def fake_market_updated_at(_conn, ticker):
            return last_dt if ticker == "M_SKIP" else None

        with patch("src.jobs.discovery.iter_events", return_value=events) as iter_events, \
             patch("src.jobs.discovery.accept_event", return_value="hour"), \
             patch("src.jobs.discovery.get_state", return_value=last_raw), \
             patch("src.jobs.discovery.get_market_updated_at", side_effect=fake_market_updated_at), \
             patch("src.jobs.discovery.parse_ts_iso", side_effect=fake_parse_ts_iso), \
             patch("src.jobs.discovery.upsert_event") as upsert_event, \
             patch("src.jobs.discovery.upsert_market") as upsert_market, \
             patch("src.jobs.discovery.upsert_active_market") as upsert_active_market, \
             patch("src.jobs.discovery.set_state") as set_state, \
             patch("src.jobs.discovery.cleanup_active_markets", return_value=0):
            result = discovery.discovery_pass(
                conn=object(),
                client=ClientUpdatedAfterTs(),
                strike_periods=("hour",),
                event_statuses=("active",),
            )

        self.assertEqual(result, (1, 1, 1))
        self.assertEqual(upsert_event.call_count, 1)
        self.assertEqual(upsert_market.call_count, 1)
        self.assertEqual(upsert_active_market.call_count, 1)
        _, kwargs = upsert_active_market.call_args
        self.assertEqual(kwargs["ticker"], "M_ACTIVE")
        self.assertEqual(kwargs["event_ticker"], "EV_KEEP")
        self.assertEqual(kwargs["close_time"], now + timedelta(hours=1))
        _, kwargs = iter_events.call_args
        self.assertEqual(kwargs["status"], "open")
        self.assertEqual(kwargs["updated_after_ts"], int(last_dt.timestamp()))
        self.assertTrue(kwargs["with_nested_markets"])
        set_state.assert_any_call(
            unittest.mock.ANY,
            "last_discovery_ts",
            unittest.mock.ANY,
        )

    def test_discovery_pass_handles_iter_errors_and_cleanup_errors(self) -> None:
        with patch("src.jobs.discovery.iter_events", side_effect=RuntimeError("boom")), \
             patch("src.jobs.discovery.get_state", return_value=None), \
             patch("src.jobs.discovery.cleanup_active_markets", side_effect=RuntimeError("boom")), \
             patch("src.jobs.discovery.set_state") as set_state:
            result = discovery.discovery_pass(
                conn=object(),
                client=object(),
                strike_periods=("hour",),
                event_statuses=("open",),
            )
        self.assertEqual(result, (0, 0, 0))
        set_state.assert_any_call(
            unittest.mock.ANY,
            "last_discovery_ts",
            unittest.mock.ANY,
        )

    def test_discovery_pass_logs_cleanup_count(self) -> None:
        with patch("src.jobs.discovery.iter_events", return_value=[]), \
             patch("src.jobs.discovery.get_state", return_value=None), \
             patch("src.jobs.discovery.cleanup_active_markets", return_value=2), \
             patch("src.jobs.discovery.set_state") as set_state:
            result = discovery.discovery_pass(
                conn=object(),
                client=object(),
                strike_periods=("hour",),
                event_statuses=("open",),
            )
        self.assertEqual(result, (0, 0, 0))
        set_state.assert_any_call(
            unittest.mock.ANY,
            "last_discovery_ts",
            unittest.mock.ANY,
        )

    def test_discovery_pass_logs_seeded_active_markets(self) -> None:
        with patch("src.jobs.discovery.load_active_tickers", return_value=[]), \
             patch("src.jobs.discovery.seed_active_markets_from_markets", return_value=2), \
             patch("src.jobs.discovery._run_discovery", return_value=[0, 0, 0]), \
             patch("src.jobs.discovery.cleanup_active_markets", return_value=0), \
             patch("src.jobs.discovery.set_state"), \
             patch("src.jobs.discovery.get_state", return_value=None), \
             patch("src.jobs.discovery.logger.info") as info:
            discovery.discovery_pass(
                conn=object(),
                client=object(),
                strike_periods=("hour",),
                event_statuses=("open",),
            )

        self.assertTrue(
            any(
                call.args[:2]
                == ("discovery_pass: seeded active_markets from markets rows=%d", 2)
                for call in info.call_args_list
            )
        )
