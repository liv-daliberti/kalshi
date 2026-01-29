import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch
import importlib

from _test_utils import add_src_to_path, ensure_cryptography_stub, ensure_psycopg_stub

ensure_cryptography_stub()
ensure_psycopg_stub()
add_src_to_path()

ws_ingest_models = importlib.import_module("src.ingest.ws.ws_ingest_models")
ws_ingest_subscriptions = importlib.import_module("src.ingest.ws.ws_ingest_subscriptions")


class TestUpdateMessage(unittest.TestCase):
    def test_build_update_message_default_markets(self) -> None:
        req = ws_ingest_subscriptions.UpdateMessageRequest(
            request_id=1,
            channels=("ticker",),
            add_tickers=["A"],
            remove_tickers=["B"],
        )
        with patch.dict(os.environ, {}, clear=True):
            msg = ws_ingest_subscriptions._build_update_message(req)
        self.assertEqual(msg["cmd"], "update_subscription")
        self.assertIn("add_markets", msg["params"])
        self.assertIn("delete_markets", msg["params"])
        self.assertNotIn("market_tickers", msg["params"])

    def test_build_update_message_legacy_with_channels(self) -> None:
        req = ws_ingest_subscriptions.UpdateMessageRequest(
            request_id=2,
            channels=("ticker", "market_lifecycle_v2"),
            add_tickers=["A"],
            remove_tickers=["B"],
        )
        with patch.dict(
            os.environ,
            {
                "KALSHI_WS_UPDATE_STYLE": "legacy",
                "KALSHI_WS_UPDATE_INCLUDE_CHANNELS": "1",
            },
            clear=True,
        ):
            msg = ws_ingest_subscriptions._build_update_message(req)
        self.assertIn("market_tickers", msg["params"])
        self.assertIn("channels", msg["params"])
        self.assertNotIn("add_markets", msg["params"])

    def test_build_update_message_subscription_id_field(self) -> None:
        req = ws_ingest_subscriptions.UpdateMessageRequest(
            request_id=3,
            channels=("ticker",),
            add_tickers=["A"],
            remove_tickers=["B"],
            options=ws_ingest_subscriptions.UpdateMessageOptions(
                subscription_id=42,
            ),
        )
        with patch.dict(os.environ, {"KALSHI_WS_SUBSCRIPTION_ID_FIELD": "subscription_id"}, clear=True):
            msg = ws_ingest_subscriptions._build_update_message(req)
        self.assertEqual(msg["params"]["subscription_id"], 42)
        self.assertNotIn("sid", msg["params"])


class TestUpdateErrorFallback(unittest.IsolatedAsyncioTestCase):
    async def test_handle_update_error_resubscribes_on_15(self) -> None:
        lock = asyncio.Lock()
        state = ws_ingest_models.SubscriptionState(
            subscribed=set(),
            lock=lock,
            request_id=iter([1]),
        )
        shard = ws_ingest_models.ShardConfig(
            count=1,
            shard_id=0,
            round_robin=True,
            round_robin_step=0,
        )
        config = ws_ingest_models.SubscriptionConfig(
            channels=("ticker",),
            max_active_tickers=10,
            shard=shard,
            ws_batch_size=2,
        )
        pending = ws_ingest_models.PendingUpdate(
            action="remove",
            sid=7,
            tickers=("A", "B"),
            sid_field="sid",
            attempts=1,
        )
        async with state.lock:
            state.pending_updates[12] = pending

        with patch(
            "src.ingest.ws.ws_ingest_subscriptions._send_subscribe_batches",
            new=AsyncMock(),
        ) as send_batches:
            await ws_ingest_subscriptions._handle_update_error(
                websocket=object(),
                state=state,
                config=config,
                request_id=12,
                error_code=15,
            )
            send_batches.assert_awaited_once()

        async with state.lock:
            self.assertIn("A", state.subscribed)
            self.assertIn("B", state.subscribed)
            self.assertNotIn(12, state.pending_updates)

    async def test_handle_update_error_without_request_id_uses_single_pending(self) -> None:
        lock = asyncio.Lock()
        state = ws_ingest_models.SubscriptionState(
            subscribed=set(),
            lock=lock,
            request_id=iter([1]),
        )
        shard = ws_ingest_models.ShardConfig(
            count=1,
            shard_id=0,
            round_robin=True,
            round_robin_step=0,
        )
        config = ws_ingest_models.SubscriptionConfig(
            channels=("ticker",),
            max_active_tickers=10,
            shard=shard,
            ws_batch_size=2,
        )
        pending = ws_ingest_models.PendingUpdate(
            action="remove",
            sid=7,
            tickers=("A", "B"),
            sid_field="sid",
            attempts=1,
        )
        async with state.lock:
            state.pending_updates[12] = pending

        with patch(
            "src.ingest.ws.ws_ingest_subscriptions._send_subscribe_batches",
            new=AsyncMock(),
        ) as send_batches:
            await ws_ingest_subscriptions._handle_update_error(
                websocket=object(),
                state=state,
                config=config,
                request_id=None,
                error_code=1,
            )
            send_batches.assert_awaited_once()

        async with state.lock:
            self.assertTrue(state.update_state.update_disabled)
            self.assertFalse(state.pending_updates)
            self.assertIn("A", state.subscribed)
            self.assertIn("B", state.subscribed)

    async def test_handle_update_error_without_request_id_multiple_pending(self) -> None:
        lock = asyncio.Lock()
        state = ws_ingest_models.SubscriptionState(
            subscribed=set(),
            lock=lock,
            request_id=iter([1]),
        )
        shard = ws_ingest_models.ShardConfig(
            count=1,
            shard_id=0,
            round_robin=True,
            round_robin_step=0,
        )
        config = ws_ingest_models.SubscriptionConfig(
            channels=("ticker",),
            max_active_tickers=10,
            shard=shard,
            ws_batch_size=2,
        )
        pending_a = ws_ingest_models.PendingUpdate(
            action="remove",
            sid=7,
            tickers=("A", "B"),
            sid_field="sid",
            attempts=1,
        )
        pending_b = ws_ingest_models.PendingUpdate(
            action="remove",
            sid=8,
            tickers=("C",),
            sid_field="sid",
            attempts=1,
        )
        async with state.lock:
            state.pending_updates[12] = pending_a
            state.pending_updates[13] = pending_b

        with patch(
            "src.ingest.ws.ws_ingest_subscriptions._send_subscribe_batches",
            new=AsyncMock(),
        ) as send_batches:
            await ws_ingest_subscriptions._handle_update_error(
                websocket=object(),
                state=state,
                config=config,
                request_id=None,
                error_code=1,
            )
            send_batches.assert_awaited_once()
            tickers = send_batches.await_args.args[3]
            self.assertEqual(tickers, ["A", "B", "C"])

        async with state.lock:
            self.assertTrue(state.update_state.update_disabled)
            self.assertFalse(state.pending_updates)
            self.assertEqual(state.subscribed, {"A", "B", "C"})
