import asyncio
import itertools
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import importlib

from _test_utils import add_src_to_path, ensure_cryptography_stub, ensure_psycopg_stub

ensure_psycopg_stub()
ensure_cryptography_stub()
add_src_to_path()

subs = importlib.import_module("src.ingest.ws.ws_ingest_subscriptions")
models = importlib.import_module("src.ingest.ws.ws_ingest_models")


class FakeWS:
    def __init__(self):
        self.sent = []

    async def send(self, msg):
        self.sent.append(msg)


def _make_shard():
    return models.ShardConfig(
        count=1,
        shard_id=0,
        round_robin=False,
        round_robin_step=0,
        key="event",
    )


def _make_subscription_config():
    return models.SubscriptionConfig(
        channels=("ticker",),
        max_active_tickers=5,
        shard=_make_shard(),
        ws_batch_size=2,
    )


def _make_ws_loop_config():
    runtime = models.WsRuntimeConfig(
        max_active_tickers=5,
        ws_batch_size=2,
        refresh_seconds=1,
        queue_maxsize=1,
        ws_max_queue=1,
        ws_max_size=1,
    )
    writer = models.WriterConfig(
        database_url="db",
        tick_batch_size=1,
        lifecycle_batch_size=1,
        flush_seconds=1.0,
        dedup_enabled=False,
        dedup_max_age_seconds=1.0,
        dedup_fields=(),
    )
    failure = models.FailureConfig(threshold=1, cooldown=1.0)
    return models.WsLoopConfig(
        ws_url="wss://x",
        channels=("ticker",),
        runtime=runtime,
        shard=_make_shard(),
        writer=writer,
        failure=failure,
    )


class TestSubscriptionHelpers(unittest.TestCase):
    def tearDown(self) -> None:
        subs.set_active_ticker_limit_override(None)

    def test_active_limit_override(self) -> None:
        subs.set_active_ticker_limit_override(3)
        self.assertEqual(subs._resolve_active_limit(10), 3)
        subs.set_active_ticker_limit_override(None)
        self.assertEqual(subs._resolve_active_limit(10), 10)

    def test_load_market_tickers_error(self) -> None:
        with patch("src.ingest.ws.ws_ingest_subscriptions.load_market_tickers_shard", side_effect=ValueError("bad")), \
            patch.object(subs.logger, "exception") as exc:
            result = subs._load_market_tickers(object(), 1, _make_subscription_config())
        self.assertIsNone(result)
        exc.assert_called_once()

    def test_seed_active_markets_paths(self) -> None:
        with patch("src.ingest.ws.ws_ingest_subscriptions.upsert_active_markets_from_markets") as upsert:
            subs._seed_active_markets(object(), [])
        upsert.assert_not_called()

        with patch("src.ingest.ws.ws_ingest_subscriptions.upsert_active_markets_from_markets", side_effect=ValueError("bad")), \
            patch.object(subs.logger, "exception") as exc:
            subs._seed_active_markets(object(), ["M1"])
        exc.assert_called_once()

        with patch("src.ingest.ws.ws_ingest_subscriptions.upsert_active_markets_from_markets", return_value=2), \
            patch.object(subs.logger, "info") as info:
            subs._seed_active_markets(object(), ["M1"])
        info.assert_called_once()

    def test_normalize_and_update_style(self) -> None:
        self.assertEqual(subs._normalize_subscription_id_field(" sid "), "sid")
        self.assertIsNone(subs._normalize_subscription_id_field(" "))
        self.assertIsNone(subs._normalize_subscription_id_field("bad"))
        self.assertEqual(subs._resolve_update_style("legacy"), "legacy")
        self.assertEqual(subs._resolve_update_style("tickers"), "legacy")
        self.assertEqual(subs._resolve_update_style("markets"), "markets")
        self.assertEqual(subs._alternate_update_style("legacy"), "markets")
        with patch.dict(os.environ, {"KALSHI_WS_UPDATE_INCLUDE_CHANNELS": "1"}):
            self.assertTrue(subs._resolve_update_include_channels(None))

    def test_extract_subscription_id_field(self) -> None:
        message = {"msg": {"params": {"subscriptionId": "5"}}}
        self.assertEqual(subs._extract_subscription_id_field(message), "subscriptionId")

    def test_extract_subscription_id_field_variants(self) -> None:
        self.assertEqual(subs._extract_subscription_id_field({"sid": "1"}), "sid")
        self.assertEqual(
            subs._extract_subscription_id_field({"msg": {"subscription_id": "2"}}),
            "subscription_id",
        )
        self.assertEqual(
            subs._extract_subscription_id_field({"params": {"subscriptionId": "3"}}),
            "subscriptionId",
        )
        self.assertEqual(
            subs._extract_subscription_id_field({"msg": {"params": {"sid": "4"}}}),
            "sid",
        )
        self.assertIsNone(subs._extract_subscription_id_field({"msg": "bad"}))

    def test_resolve_update_style_env_none(self) -> None:
        with patch("src.ingest.ws.ws_ingest_subscriptions.os.getenv", return_value=None):
            self.assertEqual(subs._resolve_update_style(None), "markets")

    def test_fallback_sid_field_none(self) -> None:
        class AlwaysEqual:
            def __eq__(self, other):
                return True

            def __ne__(self, other):
                return False

        self.assertIsNone(subs._fallback_sid_field(AlwaysEqual()))

    def test_extract_subscription_ids_missing(self) -> None:
        message = {"id": "5", "msg": "bad", "params": "oops"}
        self.assertIsNone(subs._extract_subscription_ids(message))

    def test_build_update_message_variants(self) -> None:
        request = subs.UpdateMessageRequest(
            request_id=1,
            channels=("ticker",),
            add_tickers=["M1"],
            remove_tickers=["M2"],
            options=subs.UpdateMessageOptions(update_style="legacy", include_channels=True),
        )
        payload = subs._build_update_message(request)
        self.assertIn("market_tickers", payload["params"])
        self.assertIn("channels", payload["params"])

        with patch.dict(os.environ, {"KALSHI_WS_SUBSCRIPTION_ID_FIELD": "subscription_id"}):
            payload = subs._build_update_message(
                2,
                channels=("ticker",),
                add_tickers=["M1"],
                remove_tickers=[],
                subscription_id=7,
                subscription_id_field="bad",
            )
        self.assertIn("add_markets", payload["params"])
        self.assertEqual(payload["params"]["subscription_id"], 7)

    def test_extract_error_code_and_ids(self) -> None:
        self.assertIsNone(subs._extract_error_code({"msg": "bad"}))
        self.assertEqual(subs._extract_error_code({"msg": {"code": "2"}}), 2)
        self.assertIsNone(subs._extract_subscription_ids({"id": None}))
        message = {"id": "5", "msg": {"subscription_id": "8"}}
        self.assertEqual(subs._extract_subscription_ids(message), (5, 8))


class TestSubscriptionHelpersAsync(unittest.IsolatedAsyncioTestCase):
    async def test_record_and_resolve_sid_field(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        state.update_state.sid_field = "sid"
        with patch.object(subs.logger, "debug") as debug:
            await subs._record_subscription_id_field(state, "subscription_id")
        self.assertEqual(state.update_state.sid_field, "subscription_id")
        debug.assert_called_once()
        self.assertEqual(await subs._resolve_update_sid_field(state, None), "subscription_id")
        with patch.dict(os.environ, {"KALSHI_WS_SUBSCRIPTION_ID_FIELD": "subscriptionId"}):
            self.assertEqual(await subs._resolve_update_sid_field(None, None), "subscriptionId")
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(await subs._resolve_update_sid_field(None, None), "sid")

    async def test_pending_update_helpers(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        state.pending_updates[1] = models.PendingUpdate(action="add", sid=1, tickers=("A",))
        self.assertEqual(await subs._resolve_single_pending_update_id(state), 1)
        pending = await subs._pop_pending_update(state, 1)
        self.assertIsNotNone(pending)
        state.pending_updates[2] = models.PendingUpdate(action="add", sid=1, tickers=("B",))
        drained = await subs._drain_pending_updates(state)
        self.assertEqual(len(drained), 1)
        self.assertEqual(state.pending_updates, {})

    async def test_register_subscription_sid(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        state.pending_subscriptions[1] = {"A", "B"}
        count = await subs._register_subscription_sid(state, 1, 5)
        self.assertEqual(count, 2)
        count = await subs._register_subscription_sid(state, 2, 6)
        self.assertEqual(count, 0)

    async def test_disable_and_restore_updates(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        pending = models.PendingUpdate(action="remove", sid=1, tickers=("A",))
        with patch.object(subs.logger, "warning") as warn:
            await subs._disable_update_subscription(state, "msg %s", 1)
            await subs._disable_update_subscription(state, "msg %s", 1)
        warn.assert_called_once()
        await subs._restore_removed_updates(state, [pending])
        self.assertIn("A", state.subscribed)
        self.assertIn(1, state.sid_tickers)

    async def test_resolve_update_request_id_paths(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        config = _make_subscription_config()
        self.assertEqual(await subs._resolve_update_request_id(object(), state, config, 7, None), 7)
        state.pending_updates[2] = models.PendingUpdate(action="add", sid=1, tickers=("A",))
        self.assertEqual(await subs._resolve_update_request_id(object(), state, config, None, None), 2)
        state.pending_updates[3] = models.PendingUpdate(action="remove", sid=1, tickers=("B",))
        with patch.object(subs, "_send_subscribe_batches", new=AsyncMock()) as send_batches:
            result = await subs._resolve_update_request_id(object(), state, config, None, 1)
        self.assertIsNone(result)
        send_batches.assert_called_once()

    async def test_retry_update_variants(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        config = _make_subscription_config()
        pending = models.PendingUpdate(action="add", sid=1, tickers=("A",), sid_field="sid")
        context = subs.UpdateErrorContext(
            websocket=FakeWS(),
            state=state,
            config=config,
            pending=pending,
            request_id=1,
            error_code=12,
        )
        with patch.dict(os.environ, {}, clear=True):
            with patch.object(subs, "_send_update_retry", new=AsyncMock()) as retry:
                self.assertTrue(await subs._retry_update_with_sid_field(context))
        retry.assert_called_once()

        pending = models.PendingUpdate(action="add", sid=1, tickers=("A",), update_style="legacy", include_channels=False)
        context = subs.UpdateErrorContext(
            websocket=FakeWS(),
            state=state,
            config=config,
            pending=pending,
            request_id=1,
            error_code=1,
        )
        with patch.object(subs, "_send_update_retry", new=AsyncMock()) as retry:
            self.assertTrue(await subs._retry_update_with_alt_style(context))
        retry.assert_called_once()

    async def test_handle_update_fallback(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        config = _make_subscription_config()
        pending = models.PendingUpdate(action="remove", sid=1, tickers=("A",), attempts=1)
        context = subs.UpdateErrorContext(
            websocket=FakeWS(),
            state=state,
            config=config,
            pending=pending,
            request_id=2,
            error_code=1,
        )
        with patch.object(subs, "_disable_update_subscription", new=AsyncMock()) as disable, \
            patch.object(subs, "_send_subscribe_batches", new=AsyncMock()) as send_batches:
            await subs._handle_update_fallback(context)
        disable.assert_called_once()
        send_batches.assert_called_once()

    async def test_handle_update_error_path(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        config = _make_subscription_config()
        state.pending_updates[1] = models.PendingUpdate(action="add", sid=1, tickers=("A",))
        with patch.object(subs, "_retry_update_with_sid_field", new=AsyncMock(return_value=False)), \
            patch.object(subs, "_retry_update_with_alt_style", new=AsyncMock(return_value=False)), \
            patch.object(subs, "_handle_update_fallback", new=AsyncMock()) as fallback:
            await subs._handle_update_error(object(), state, config, 1, 1)
        fallback.assert_called_once()

    async def test_record_subscription_ticker(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        await subs._record_subscription_ticker(state, 1, None)
        await subs._record_subscription_ticker(state, 1, "A")
        self.assertIn("A", state.sid_tickers[1])

    async def test_send_subscribe_batches(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        ws = FakeWS()
        with patch("src.ingest.ws.ws_ingest_subscriptions._build_subscribe_message", side_effect=lambda req, ch, batch: {"id": req, "batch": batch}):
            await subs._send_subscribe_batches(ws, ["ticker"], 2, ["A", "B", "C"], state)
        self.assertEqual(len(ws.sent), 2)
        self.assertIn("A", state.subscribed)
        self.assertIn(1, state.pending_subscriptions)

    async def test_subscribe_initial_fallback(self) -> None:
        ws = FakeWS()
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        config = _make_ws_loop_config()
        with patch.object(subs, "_load_active_tickers", return_value=None), \
            patch.object(subs, "_load_market_tickers", return_value=["A", "B"]), \
            patch.object(subs, "_seed_active_markets") as seed, \
            patch.object(subs, "_send_subscribe_batches", new=AsyncMock()) as send_batches:
            await subs._subscribe_initial(ws, object(), config, state)
        seed.assert_called_once()
        send_batches.assert_called_once()

    async def test_build_subscription_context(self) -> None:
        ws = FakeWS()
        context = models.WsSessionContext(
            conn=object(),
            work_queue=asyncio.Queue(),
            market_id_map={},
            config=_make_ws_loop_config(),
            api_key_id="k",
            private_key_pem="pem",
        )
        with patch.object(subs, "_subscribe_initial", new=AsyncMock()) as subscribe:
            result = await subs._build_subscription_context(ws, context)
        subscribe.assert_called_once()
        self.assertEqual(result.config.channels, context.config.channels)

    async def test_load_active_ticker_set_paths(self) -> None:
        config = _make_subscription_config()
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        context = models.SubscriptionContext(conn=object(), config=config, state=state)
        with patch.object(subs, "_load_active_tickers", return_value=[]), \
            patch.object(subs, "_load_market_tickers", return_value=["A"]), \
            patch.object(subs, "_seed_active_markets") as seed:
            result = await subs._load_active_ticker_set(context)
        self.assertEqual(result, {"A"})
        seed.assert_called_once()

    async def test_snapshot_and_delta(self) -> None:
        state = models.SubscriptionState(subscribed={"A"}, lock=asyncio.Lock(), request_id=itertools.count(1))
        state.sid_tickers[1] = {"A"}
        state.pending_subscriptions[2] = {"B"}
        subscribed, sid_tickers, pending = await subs._snapshot_subscription_state(state)
        self.assertEqual(subscribed, {"A"})
        self.assertIn(1, sid_tickers)
        self.assertEqual(pending, {"B"})
        to_add, to_remove = subs._compute_subscription_delta({"B"}, subscribed, pending)
        self.assertEqual(to_add, ["B"])
        self.assertEqual(to_remove, ["A"])

    async def test_apply_additions_and_removals(self) -> None:
        ws = FakeWS()
        config = _make_subscription_config()
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        context = models.SubscriptionContext(conn=object(), config=config, state=state)
        with patch.object(subs, "_send_subscribe_batches", new=AsyncMock()) as send_batches:
            await subs._apply_subscription_additions(ws, context, ["A"], {})
        send_batches.assert_called_once()

        state.sid_tickers[1] = {"A", "B"}
        await subs._apply_subscription_additions(ws, context, ["C"], state.sid_tickers)
        self.assertTrue(state.pending_updates)

        state.update_state.update_disabled = True
        state.subscribed = {"A", "B"}
        state.sid_tickers = {1: {"A", "B"}}
        await subs._apply_subscription_removals(ws, context, ["A"], state.sid_tickers)
        self.assertNotIn("A", state.subscribed)

    async def test_apply_removals_update_paths(self) -> None:
        ws = FakeWS()
        config = _make_subscription_config()
        state = models.SubscriptionState(subscribed={"A", "B"}, lock=asyncio.Lock(), request_id=itertools.count(1))
        state.sid_tickers = {1: {"A", "B"}}
        context = models.SubscriptionContext(conn=object(), config=config, state=state)
        await subs._apply_subscription_removals(ws, context, ["A", "C"], state.sid_tickers)
        self.assertTrue(state.pending_updates)

    async def test_refresh_subscriptions_loop(self) -> None:
        ws = FakeWS()
        config = _make_subscription_config()
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        context = models.SubscriptionContext(conn=object(), config=config, state=state)
        wake = asyncio.Event()
        wake.set()

        async def load_active(_context):
            return {"A"}

        async def snapshot_state(_state):
            return {"B"}, {1: {"B"}}, set()

        async def apply_add(*_args, **_kwargs):
            return None

        async def apply_remove(*_args, **_kwargs):
            raise asyncio.CancelledError()

        with patch.object(subs, "_load_active_ticker_set", side_effect=load_active), \
            patch.object(subs, "_snapshot_subscription_state", side_effect=snapshot_state), \
            patch.object(subs, "_apply_subscription_additions", side_effect=apply_add), \
            patch.object(subs, "_apply_subscription_removals", side_effect=apply_remove):
            with self.assertRaises(asyncio.CancelledError):
                await subs._refresh_subscriptions(ws, context, 0, wake_event=wake)

    async def test_record_subscription_id_field_invalid(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        state.update_state.sid_field = "sid"
        await subs._record_subscription_id_field(state, "bad")
        self.assertEqual(state.update_state.sid_field, "sid")

    async def test_resolve_update_sid_field_direct(self) -> None:
        self.assertEqual(await subs._resolve_update_sid_field(None, "subscription_id"), "subscription_id")

    async def test_resolve_update_request_id_no_pending(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        config = _make_subscription_config()
        self.assertIsNone(await subs._resolve_update_request_id(object(), state, config, None, 1))

    async def test_resolve_update_request_id_empty_tickers(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        config = _make_subscription_config()
        state.pending_updates[1] = models.PendingUpdate(action="add", sid=1, tickers=())
        state.pending_updates[2] = models.PendingUpdate(action="add", sid=2, tickers=())
        with patch.object(subs, "_disable_update_subscription", new=AsyncMock()) as disable, \
            patch.object(subs, "_restore_removed_updates", new=AsyncMock()) as restore, \
            patch.object(subs, "_send_subscribe_batches", new=AsyncMock()) as send_batches:
            result = await subs._resolve_update_request_id(object(), state, config, None, 1)
        self.assertIsNone(result)
        disable.assert_called_once()
        restore.assert_called_once()
        send_batches.assert_not_called()

    async def test_retry_update_with_sid_field_no_fallback(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        config = _make_subscription_config()
        pending = models.PendingUpdate(action="add", sid=1, tickers=("A",), sid_field="sid")
        context = subs.UpdateErrorContext(
            websocket=FakeWS(),
            state=state,
            config=config,
            pending=pending,
            request_id=1,
            error_code=12,
        )
        with patch.object(subs, "_fallback_sid_field", return_value=None), \
            patch.object(subs, "_send_update_retry", new=AsyncMock()) as retry:
            self.assertFalse(await subs._retry_update_with_sid_field(context))
        retry.assert_not_called()

    async def test_handle_update_fallback_no_tickers(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        config = _make_subscription_config()
        pending = models.PendingUpdate(action="remove", sid=1, tickers=(), attempts=1)
        context = subs.UpdateErrorContext(
            websocket=FakeWS(),
            state=state,
            config=config,
            pending=pending,
            request_id=2,
            error_code=1,
        )
        with patch.object(subs, "_disable_update_subscription", new=AsyncMock()) as disable, \
            patch.object(subs, "_restore_removed_updates", new=AsyncMock()) as restore, \
            patch.object(subs, "_send_subscribe_batches", new=AsyncMock()) as send_batches:
            await subs._handle_update_fallback(context)
        disable.assert_called_once()
        restore.assert_called_once()
        send_batches.assert_not_called()

    async def test_handle_update_error_invalid_code(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        config = _make_subscription_config()
        with patch.object(subs, "_resolve_update_request_id", new=AsyncMock()) as resolve:
            await subs._handle_update_error(object(), state, config, 1, 2)
        resolve.assert_not_called()

    async def test_handle_update_error_missing_pending(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        config = _make_subscription_config()
        with patch.object(subs, "_resolve_update_request_id", new=AsyncMock(return_value=1)), \
            patch.object(subs, "_retry_update_with_sid_field", new=AsyncMock()) as retry:
            await subs._handle_update_error(object(), state, config, None, 1)
        retry.assert_not_called()

    async def test_handle_update_error_short_circuit_retry(self) -> None:
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        state.pending_updates[1] = models.PendingUpdate(action="add", sid=1, tickers=("A",))
        config = _make_subscription_config()
        with patch.object(subs, "_resolve_update_request_id", new=AsyncMock(return_value=1)), \
            patch.object(subs, "_retry_update_with_sid_field", new=AsyncMock(return_value=True)) as retry, \
            patch.object(subs, "_retry_update_with_alt_style", new=AsyncMock()) as alt_retry, \
            patch.object(subs, "_handle_update_fallback", new=AsyncMock()) as fallback:
            await subs._handle_update_error(object(), state, config, None, 1)
        retry.assert_called_once()
        alt_retry.assert_not_called()
        fallback.assert_not_called()

    async def test_subscribe_initial_fallback_none(self) -> None:
        ws = FakeWS()
        config = _make_ws_loop_config()
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        with patch.object(subs, "_load_active_tickers", return_value=[]), \
            patch.object(subs, "_load_market_tickers", return_value=None), \
            patch.object(subs, "_send_subscribe_batches", new=AsyncMock()) as send_batches, \
            patch.object(subs.logger, "warning") as warning:
            await subs._subscribe_initial(ws, object(), config, state)
        warning.assert_called_once()
        send_batches.assert_called_once()

    async def test_subscribe_initial_active_none_and_fallback_none(self) -> None:
        ws = FakeWS()
        config = _make_ws_loop_config()
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        with patch.object(subs, "_load_active_tickers", return_value=None), \
            patch.object(subs, "_load_market_tickers", return_value=None), \
            patch.object(subs, "_send_subscribe_batches", new=AsyncMock()) as send_batches, \
            patch.object(subs.logger, "warning") as warning:
            await subs._subscribe_initial(ws, object(), config, state)
        self.assertEqual(warning.call_count, 2)
        send_batches.assert_called_once()
        self.assertEqual(send_batches.call_args[0][3], [])

    async def test_load_active_ticker_set_fallback_none(self) -> None:
        config = _make_subscription_config()
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        context = models.SubscriptionContext(conn=object(), config=config, state=state)
        with patch.object(subs, "_load_active_tickers", return_value=[]), \
            patch.object(subs, "_load_market_tickers", return_value=None):
            result = await subs._load_active_ticker_set(context)
        self.assertIsNone(result)

    async def test_apply_removals_prune_sid_bucket_disabled(self) -> None:
        ws = FakeWS()
        config = _make_subscription_config()
        state = models.SubscriptionState(subscribed={"A"}, lock=asyncio.Lock(), request_id=itertools.count(1))
        state.update_state.update_disabled = True
        state.sid_tickers = {1: {"A"}}
        context = models.SubscriptionContext(conn=object(), config=config, state=state)
        await subs._apply_subscription_removals(ws, context, ["A"], state.sid_tickers)
        self.assertNotIn(1, state.sid_tickers)

    async def test_apply_removals_prune_sid_bucket(self) -> None:
        ws = FakeWS()
        config = _make_subscription_config()
        state = models.SubscriptionState(subscribed={"A"}, lock=asyncio.Lock(), request_id=itertools.count(1))
        state.sid_tickers = {1: {"A"}}
        context = models.SubscriptionContext(conn=object(), config=config, state=state)
        await subs._apply_subscription_removals(ws, context, ["A"], state.sid_tickers)
        self.assertNotIn(1, state.sid_tickers)

    async def test_refresh_subscriptions_wake_event_clear(self) -> None:
        ws = FakeWS()
        config = _make_subscription_config()
        state = models.SubscriptionState(subscribed=set(), lock=asyncio.Lock(), request_id=itertools.count(1))
        context = models.SubscriptionContext(conn=object(), config=config, state=state)
        wake = asyncio.Event()
        wake.set()
        with patch.object(subs, "_load_active_ticker_set", side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):
                await subs._refresh_subscriptions(ws, context, 5, wake_event=wake)
        self.assertFalse(wake.is_set())
