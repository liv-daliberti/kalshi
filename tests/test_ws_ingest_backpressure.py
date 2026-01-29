import asyncio
import os
import queue
import unittest
from unittest.mock import AsyncMock, patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

bp = importlib.import_module("src.ingest.ws.ws_ingest_backpressure")


class FakeQueue:
    def __init__(self, failures_before_success=0):
        self.failures_before_success = failures_before_success
        self.calls = 0
        self.items = []

    def put_nowait(self, item):
        if self.calls < self.failures_before_success:
            self.calls += 1
            raise queue.Full
        self.items.append(item)

    def qsize(self):
        return len(self.items)


def _make_state(
    *,
    mode="drop",
    active_limit=100,
    last_resubscribe=0.0,
    block_seconds=1.0,
    block_sleep=0.01,
    throttle_seconds=0.5,
    resubscribe_factor=0.5,
    resubscribe_min=10,
    resubscribe_cooldown=60.0,
):
    config = bp.QueueBackpressureConfig(
        mode=mode,
        block_seconds=block_seconds,
        block_sleep=block_sleep,
        throttle_seconds=throttle_seconds,
        resubscribe_factor=resubscribe_factor,
        resubscribe_min=resubscribe_min,
        resubscribe_cooldown=resubscribe_cooldown,
    )
    return bp.QueueBackpressureState(
        config=config,
        active_limit=active_limit,
        resubscribe_event=asyncio.Event(),
        last_resubscribe=last_resubscribe,
    )


class TestBackpressureHelpers(unittest.TestCase):
    def test_parse_backpressure_mode_unknown_warns(self) -> None:
        with patch.dict(os.environ, {"WS_QUEUE_BACKPRESSURE": "weird"}):
            with patch.object(bp.logger, "warning") as warn:
                result = bp._parse_backpressure_mode()
        self.assertEqual(result, "drop")
        warn.assert_called_once()

    def test_parse_backpressure_mode_valid(self) -> None:
        with patch.dict(os.environ, {"WS_QUEUE_BACKPRESSURE": "THROTTLE"}):
            self.assertEqual(bp._parse_backpressure_mode(), "throttle")

    def test_request_resubscribe_skips_wrong_mode(self) -> None:
        state = _make_state(mode="drop")
        with patch("src.ingest.ws.ws_ingest_backpressure.set_active_ticker_limit_override") as override:
            state.request_resubscribe()
        override.assert_not_called()
        self.assertFalse(state.resubscribe_event.is_set())

    def test_request_resubscribe_respects_cooldown(self) -> None:
        state = _make_state(
            mode="resubscribe",
            active_limit=100,
            resubscribe_factor=0.5,
            resubscribe_min=10,
            resubscribe_cooldown=60.0,
            last_resubscribe=100.0,
        )
        with patch("src.ingest.ws.ws_ingest_backpressure.time.monotonic", return_value=120.0), \
            patch("src.ingest.ws.ws_ingest_backpressure.set_active_ticker_limit_override") as override:
            state.request_resubscribe()
        override.assert_not_called()
        self.assertEqual(state.active_limit, 100)
        self.assertFalse(state.resubscribe_event.is_set())

    def test_request_resubscribe_no_increase(self) -> None:
        state = _make_state(
            mode="resubscribe",
            active_limit=100,
            resubscribe_factor=1.0,
            resubscribe_min=150,
        )
        with patch("src.ingest.ws.ws_ingest_backpressure.time.monotonic", return_value=200.0), \
            patch("src.ingest.ws.ws_ingest_backpressure.set_active_ticker_limit_override") as override:
            state.request_resubscribe()
        override.assert_not_called()
        self.assertEqual(state.active_limit, 100)

    def test_request_resubscribe_updates_state(self) -> None:
        state = _make_state(
            mode="resubscribe",
            active_limit=100,
            resubscribe_factor=0.5,
            resubscribe_min=10,
            resubscribe_cooldown=60.0,
        )
        with patch("src.ingest.ws.ws_ingest_backpressure.time.monotonic", return_value=300.0), \
            patch("src.ingest.ws.ws_ingest_backpressure.set_active_ticker_limit_override") as override, \
            patch.object(bp.logger, "warning") as warn:
            state.request_resubscribe()
        self.assertEqual(state.active_limit, 50)
        self.assertEqual(state.last_resubscribe, 300.0)
        self.assertTrue(state.resubscribe_event.is_set())
        override.assert_called_once_with(50)
        warn.assert_called_once()

    def test_reset_backpressure_override(self) -> None:
        state = _make_state(mode="resubscribe")
        with patch("src.ingest.ws.ws_ingest_backpressure.set_active_ticker_limit_override") as override:
            bp.reset_backpressure_override(state)
        override.assert_called_once_with(None)


class TestEnqueueWsItem(unittest.IsolatedAsyncioTestCase):
    async def test_enqueue_ws_item_fast_path(self) -> None:
        work_queue = queue.Queue(maxsize=1)
        item = ("tick",)
        result = await bp.enqueue_ws_item(work_queue, item, "kind", None)
        self.assertTrue(result)
        self.assertEqual(work_queue.get_nowait(), item)

    async def test_enqueue_ws_item_backpressure_none_falls_back(self) -> None:
        work_queue = queue.Queue(maxsize=1)
        work_queue.put_nowait(("existing",))
        with patch("src.ingest.ws.ws_ingest_backpressure._queue_put_nowait", return_value=False) as qput:
            result = await bp.enqueue_ws_item(work_queue, ("tick",), "kind", None)
        self.assertFalse(result)
        qput.assert_called_once_with(work_queue, ("tick",), "kind")

    async def test_enqueue_ws_item_block_retries(self) -> None:
        work_queue = FakeQueue(failures_before_success=1)
        backpressure = _make_state(mode="block", block_seconds=1.0, block_sleep=0.01)
        with patch("src.ingest.ws.ws_ingest_backpressure.asyncio.sleep", new=AsyncMock()) as sleep, \
            patch("src.ingest.ws.ws_ingest_backpressure.time.monotonic", side_effect=[0.0, 0.0]):
            result = await bp.enqueue_ws_item(work_queue, ("tick",), "kind", backpressure)
        self.assertTrue(result)
        self.assertEqual(work_queue.items, [("tick",)])
        self.assertTrue(sleep.await_count >= 1)

    async def test_enqueue_ws_item_block_falls_back(self) -> None:
        work_queue = FakeQueue(failures_before_success=999)
        backpressure = _make_state(mode="block", block_seconds=0.5, block_sleep=0.01)
        with patch("src.ingest.ws.ws_ingest_backpressure.asyncio.sleep", new=AsyncMock()) as sleep, \
            patch("src.ingest.ws.ws_ingest_backpressure.time.monotonic", side_effect=[0.0, 0.0, 1.0]), \
            patch("src.ingest.ws.ws_ingest_backpressure._queue_put_nowait", return_value=False) as qput:
            result = await bp.enqueue_ws_item(work_queue, ("tick",), "kind", backpressure)
        self.assertFalse(result)
        self.assertTrue(sleep.await_count >= 1)
        qput.assert_called_once_with(work_queue, ("tick",), "kind")

    async def test_enqueue_ws_item_throttle_then_falls_back(self) -> None:
        work_queue = FakeQueue(failures_before_success=999)
        backpressure = _make_state(mode="throttle", throttle_seconds=0.25)
        with patch("src.ingest.ws.ws_ingest_backpressure.asyncio.sleep", new=AsyncMock()) as sleep, \
            patch("src.ingest.ws.ws_ingest_backpressure._queue_put_nowait", return_value=False) as qput:
            result = await bp.enqueue_ws_item(work_queue, ("tick",), "kind", backpressure)
        self.assertFalse(result)
        sleep.assert_awaited_once_with(0.25)
        qput.assert_called_once_with(work_queue, ("tick",), "kind")

    async def test_enqueue_ws_item_throttle_success(self) -> None:
        work_queue = FakeQueue(failures_before_success=1)
        backpressure = _make_state(mode="throttle", throttle_seconds=0.25)
        with patch("src.ingest.ws.ws_ingest_backpressure.asyncio.sleep", new=AsyncMock()) as sleep, \
            patch("src.ingest.ws.ws_ingest_backpressure._queue_put_nowait") as qput:
            result = await bp.enqueue_ws_item(work_queue, ("tick",), "kind", backpressure)
        self.assertTrue(result)
        self.assertEqual(work_queue.items, [("tick",)])
        sleep.assert_awaited_once_with(0.25)
        qput.assert_not_called()

    async def test_enqueue_ws_item_resubscribe_requests(self) -> None:
        work_queue = FakeQueue(failures_before_success=999)
        backpressure = _make_state(mode="resubscribe")
        with patch.object(backpressure, "request_resubscribe") as request, \
            patch("src.ingest.ws.ws_ingest_backpressure._queue_put_nowait", return_value=True) as qput:
            result = await bp.enqueue_ws_item(work_queue, ("tick",), "kind", backpressure)
        self.assertTrue(result)
        request.assert_called_once()
        qput.assert_called_once_with(work_queue, ("tick",), "kind")
