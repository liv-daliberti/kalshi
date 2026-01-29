import importlib
import sys
import unittest
from unittest.mock import patch

from _test_utils import add_src_to_path, ensure_kalshi_sdk_stub, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()
ensure_kalshi_sdk_stub()

kalshi_sdk = sys.modules.get("src.kalshi.kalshi_sdk")
if kalshi_sdk is not None:
    if not hasattr(kalshi_sdk, "coerce_payload"):
        kalshi_sdk.coerce_payload = lambda payload: payload
    if not hasattr(kalshi_sdk, "extract_http_status"):
        kalshi_sdk.extract_http_status = lambda exc: getattr(exc, "status", None)
    if not hasattr(kalshi_sdk, "rest_register_rate_limit"):
        kalshi_sdk.rest_register_rate_limit = lambda *args, **kwargs: None
    if not hasattr(kalshi_sdk, "rest_wait"):
        kalshi_sdk.rest_wait = lambda *args, **kwargs: None

discover_market = importlib.import_module("src.jobs.discover_market")


class TestLogMissingMarket(unittest.TestCase):
    def test_log_missing_market_404(self) -> None:
        with self.assertLogs("src.jobs.discover_market", level="WARNING") as logs:
            discover_market._log_missing_market(404, "MKT1")
        self.assertIn("market not found ticker=MKT1", logs.output[0])

    def test_log_missing_market_payload_missing(self) -> None:
        with self.assertLogs("src.jobs.discover_market", level="WARNING") as logs:
            discover_market._log_missing_market(None, "MKT2")
        self.assertIn("market payload missing ticker=MKT2", logs.output[0])


class TestExtractPayloads(unittest.TestCase):
    def test_extract_market_payload_variants(self) -> None:
        market = {"ticker": "MKT1"}

        class MarketResponse:
            def __init__(self, payload):
                self.market = payload

        with patch("src.jobs.discover_market.coerce_payload", side_effect=lambda x: x) as coerce:
            self.assertIsNone(discover_market._extract_market_payload(None))
            self.assertEqual(
                discover_market._extract_market_payload({"market": market}),
                market,
            )
            self.assertEqual(
                discover_market._extract_market_payload({"data": market}),
                market,
            )
            self.assertEqual(
                discover_market._extract_market_payload(market),
                market,
            )
            self.assertEqual(
                discover_market._extract_market_payload(MarketResponse(market)),
                market,
            )
            self.assertEqual(coerce.call_count, 4)

    def test_extract_event_payload_variants(self) -> None:
        event = {"event_ticker": "EV1"}

        class EventResponse:
            def __init__(self, payload):
                self.event = payload

        with patch("src.jobs.discover_market.coerce_payload", side_effect=lambda x: x) as coerce:
            self.assertIsNone(discover_market._extract_event_payload(None))
            self.assertEqual(
                discover_market._extract_event_payload({"event": event}),
                event,
            )
            self.assertEqual(
                discover_market._extract_event_payload({"data": event}),
                event,
            )
            self.assertEqual(
                discover_market._extract_event_payload(event),
                event,
            )
            self.assertEqual(
                discover_market._extract_event_payload(EventResponse(event)),
                event,
            )
            self.assertEqual(coerce.call_count, 4)

    def test_extract_market_payload_passthrough(self) -> None:
        payload = ["MKT1"]
        with patch("src.jobs.discover_market.coerce_payload", side_effect=lambda x: x) as coerce:
            self.assertEqual(discover_market._extract_market_payload(payload), payload)
        coerce.assert_called_once_with(payload)

    def test_extract_event_payload_passthrough(self) -> None:
        payload = ("EV1",)
        with patch("src.jobs.discover_market.coerce_payload", side_effect=lambda x: x) as coerce:
            self.assertEqual(discover_market._extract_event_payload(payload), payload)
        coerce.assert_called_once_with(payload)


class TestFetchMarket(unittest.TestCase):
    def test_fetch_market_success(self) -> None:
        class Client:
            def __init__(self):
                self.calls = []

            def get_market(self, ticker):
                self.calls.append(ticker)
                return {"market": {"ticker": ticker}}

        client = Client()
        sentinel = {"ticker": "MKT1"}

        with patch("src.jobs.discover_market.rest_wait") as rest_wait, \
             patch("src.jobs.discover_market._extract_market_payload", return_value=sentinel) as extract:
            result = discover_market._fetch_market(client, "MKT1")

        self.assertEqual(result, sentinel)
        self.assertEqual(client.calls, ["MKT1"])
        rest_wait.assert_called_once()
        extract.assert_called_once_with({"market": {"ticker": "MKT1"}})

    def test_fetch_market_rate_limit_registers(self) -> None:
        class Client:
            def get_market(self, ticker):
                raise RuntimeError("boom")

        client = Client()

        with patch("src.jobs.discover_market.rest_wait") as rest_wait, \
             patch("src.jobs.discover_market.extract_http_status", return_value=429), \
             patch("src.jobs.discover_market.rest_register_rate_limit") as register:
            with self.assertRaises(RuntimeError):
                discover_market._fetch_market(client, "MKT1")

        rest_wait.assert_called_once()
        register.assert_called_once()

    def test_fetch_event_success(self) -> None:
        class Client:
            def __init__(self):
                self.calls = []

            def get_event(self, ticker):
                self.calls.append(ticker)
                return {"event": {"event_ticker": ticker}}

        client = Client()
        sentinel = {"event_ticker": "EV1"}

        with patch("src.jobs.discover_market.rest_wait") as rest_wait, \
             patch("src.jobs.discover_market._extract_event_payload", return_value=sentinel) as extract:
            result = discover_market._fetch_event(client, "EV1")

        self.assertEqual(result, sentinel)
        self.assertEqual(client.calls, ["EV1"])
        rest_wait.assert_called_once()
        extract.assert_called_once_with({"event": {"event_ticker": "EV1"}})

    def test_fetch_event_rate_limit_registers(self) -> None:
        class Client:
            def get_event(self, ticker):
                raise RuntimeError("boom")

        client = Client()

        with patch("src.jobs.discover_market.rest_wait") as rest_wait, \
             patch("src.jobs.discover_market.extract_http_status", return_value=429), \
             patch("src.jobs.discover_market.rest_register_rate_limit") as register:
            with self.assertRaises(RuntimeError):
                discover_market._fetch_event(client, "EV1")

        rest_wait.assert_called_once()
        register.assert_called_once()


class TestMaybeUpsertEvent(unittest.TestCase):
    def test_maybe_upsert_event_missing_ticker(self) -> None:
        with patch("src.jobs.discover_market.upsert_event") as upsert_event, \
             patch("src.jobs.discover_market._fetch_event") as fetch_event:
            discover_market._maybe_upsert_event(object(), object(), None)

        upsert_event.assert_not_called()
        fetch_event.assert_not_called()

    def test_maybe_upsert_event_uses_fetched_event(self) -> None:
        class Client:
            def get_event(self, ticker):
                return {"event_ticker": ticker}

        client = Client()
        event = {"event_ticker": "EV1"}

        with patch("src.jobs.discover_market._fetch_event", return_value=event) as fetch_event, \
             patch("src.jobs.discover_market.upsert_event") as upsert_event:
            discover_market._maybe_upsert_event(object(), client, "EV1")

        fetch_event.assert_called_once_with(client, "EV1")
        upsert_event.assert_called_once_with(unittest.mock.ANY, event)

    def test_maybe_upsert_event_uses_placeholder_on_404(self) -> None:
        class Client:
            def get_event(self, ticker):
                return {"event_ticker": ticker}

        client = Client()

        with patch("src.jobs.discover_market._fetch_event", side_effect=RuntimeError("boom")), \
             patch("src.jobs.discover_market.extract_http_status", return_value=404), \
             patch("src.jobs.discover_market.upsert_event") as upsert_event:
            discover_market._maybe_upsert_event(object(), client, "EV2")

        upsert_event.assert_called_once_with(
            unittest.mock.ANY,
            {"event_ticker": "EV2"},
        )

    def test_maybe_upsert_event_raises_non_404(self) -> None:
        class Client:
            def get_event(self, ticker):
                return {"event_ticker": ticker}

        client = Client()

        with patch("src.jobs.discover_market._fetch_event", side_effect=RuntimeError("boom")), \
             patch("src.jobs.discover_market.extract_http_status", return_value=500), \
             patch("src.jobs.discover_market.upsert_event") as upsert_event:
            with self.assertRaises(RuntimeError):
                discover_market._maybe_upsert_event(object(), client, "EV3")

        upsert_event.assert_not_called()

    def test_maybe_upsert_event_client_without_get(self) -> None:
        client = object()

        with patch("src.jobs.discover_market.upsert_event") as upsert_event:
            discover_market._maybe_upsert_event(object(), client, "EV4")

        upsert_event.assert_called_once_with(unittest.mock.ANY, {"event_ticker": "EV4"})


class TestDiscoverMarket(unittest.TestCase):
    def test_discover_market_success(self) -> None:
        market = {"ticker": "MKT1", "event_ticker": "EV1"}

        with patch("src.jobs.discover_market._fetch_market", return_value=market) as fetch_market, \
             patch("src.jobs.discover_market._maybe_upsert_event") as maybe_upsert, \
             patch("src.jobs.discover_market.upsert_market") as upsert_market, \
             patch("src.jobs.discover_market._log_missing_market") as log_missing:
            result = discover_market.discover_market(object(), object(), "MKT1")

        self.assertEqual(result, 1)
        fetch_market.assert_called_once_with(unittest.mock.ANY, "MKT1")
        maybe_upsert.assert_called_once_with(unittest.mock.ANY, unittest.mock.ANY, "EV1")
        upsert_market.assert_called_once_with(unittest.mock.ANY, market)
        log_missing.assert_not_called()

    def test_discover_market_missing_404(self) -> None:
        with patch("src.jobs.discover_market._fetch_market", side_effect=RuntimeError("boom")), \
             patch("src.jobs.discover_market.extract_http_status", return_value=404), \
             patch("src.jobs.discover_market._log_missing_market") as log_missing, \
             patch("src.jobs.discover_market.upsert_market") as upsert_market:
            result = discover_market.discover_market(object(), object(), "MKT404")

        self.assertEqual(result, 0)
        log_missing.assert_called_once_with(404, "MKT404")
        upsert_market.assert_not_called()

    def test_discover_market_raises_non_404(self) -> None:
        with patch("src.jobs.discover_market._fetch_market", side_effect=RuntimeError("boom")), \
             patch("src.jobs.discover_market.extract_http_status", return_value=500), \
             patch("src.jobs.discover_market._log_missing_market") as log_missing:
            with self.assertRaises(RuntimeError):
                discover_market.discover_market(object(), object(), "MKT500")

        log_missing.assert_not_called()

    def test_discover_market_requires_ticker(self) -> None:
        with self.assertRaises(ValueError):
            discover_market.discover_market(object(), object(), "")
