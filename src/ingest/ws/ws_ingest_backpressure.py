"""Backpressure controls for WS ingestion queues."""

from __future__ import annotations

import asyncio
import logging
import os
import queue
import time
from dataclasses import dataclass

from src.core.env_utils import env_float, env_int
from src.ingest.ws.ws_ingest_models import WsLoopConfig
from src.ingest.ws.ws_ingest_subscriptions import set_active_ticker_limit_override
from src.ingest.ws.ws_ingest_writer import _queue_put_nowait

logger = logging.getLogger(__name__)

_BACKPRESSURE_MODES = {"drop", "block", "throttle", "resubscribe"}


@dataclass(frozen=True)
class QueueBackpressureConfig:
    """Configuration for WS queue backpressure behavior."""

    mode: str
    block_seconds: float
    block_sleep: float
    throttle_seconds: float
    resubscribe_factor: float
    resubscribe_min: int
    resubscribe_cooldown: float


@dataclass
class QueueBackpressureState:
    """Runtime state for WS queue backpressure handling."""

    config: QueueBackpressureConfig
    active_limit: int
    resubscribe_event: asyncio.Event
    last_resubscribe: float = 0.0

    def request_resubscribe(self) -> None:
        """Reduce active ticker limit and trigger subscription refresh."""
        if self.config.mode != "resubscribe":
            return
        now = time.monotonic()
        if now - self.last_resubscribe < self.config.resubscribe_cooldown:
            return
        new_limit = max(
            self.config.resubscribe_min,
            int(self.active_limit * self.config.resubscribe_factor),
        )
        if new_limit >= self.active_limit:
            return
        self.active_limit = new_limit
        self.last_resubscribe = now
        set_active_ticker_limit_override(self.active_limit)
        self.resubscribe_event.set()
        logger.warning(
            "WS queue backpressure: resubscribe limit=%d",
            self.active_limit,
        )


def _parse_backpressure_mode() -> str:
    raw = os.getenv("WS_QUEUE_BACKPRESSURE", "drop").strip().lower()
    if raw in _BACKPRESSURE_MODES:
        return raw
    if raw:
        logger.warning("Unknown WS_QUEUE_BACKPRESSURE=%s; defaulting to drop", raw)
    return "drop"


def load_backpressure_state(config: WsLoopConfig) -> QueueBackpressureState:
    """Load backpressure configuration and initialize runtime state."""
    mode = _parse_backpressure_mode()
    block_seconds = env_float("WS_QUEUE_BLOCK_SECONDS", 1.0, minimum=0.0)
    block_sleep = env_float("WS_QUEUE_BLOCK_SLEEP_SECONDS", 0.05, minimum=0.005)
    throttle_seconds = env_float("WS_QUEUE_THROTTLE_SECONDS", 0.1, minimum=0.0)
    resubscribe_factor = env_float("WS_QUEUE_RESUBSCRIBE_FACTOR", 0.8, minimum=0.1)
    resubscribe_factor = min(resubscribe_factor, 1.0)
    resubscribe_min_default = max(100, config.runtime.max_active_tickers // 10)
    resubscribe_min = env_int(
        "WS_QUEUE_RESUBSCRIBE_MIN",
        resubscribe_min_default,
        minimum=1,
    )
    resubscribe_cooldown = env_float(
        "WS_QUEUE_RESUBSCRIBE_COOLDOWN_SECONDS",
        60.0,
        minimum=1.0,
    )
    if mode != "resubscribe":
        set_active_ticker_limit_override(None)
    bp_config = QueueBackpressureConfig(
        mode=mode,
        block_seconds=block_seconds,
        block_sleep=block_sleep,
        throttle_seconds=throttle_seconds,
        resubscribe_factor=resubscribe_factor,
        resubscribe_min=resubscribe_min,
        resubscribe_cooldown=resubscribe_cooldown,
    )
    return QueueBackpressureState(
        config=bp_config,
        active_limit=config.runtime.max_active_tickers,
        resubscribe_event=asyncio.Event(),
    )


def reset_backpressure_override(backpressure: QueueBackpressureState) -> None:
    """Clear any active ticker override when shutting down."""
    if backpressure.config.mode == "resubscribe":
        set_active_ticker_limit_override(None)


async def enqueue_ws_item(
    work_queue: queue.Queue,
    item: tuple,
    kind: str,
    backpressure: QueueBackpressureState | None,
) -> bool:
    """Enqueue a WS item honoring backpressure settings."""
    try:
        work_queue.put_nowait(item)
        return True
    except queue.Full:
        pass
    if backpressure is None:
        return _queue_put_nowait(work_queue, item, kind)
    if backpressure.config.mode == "block" and backpressure.config.block_seconds > 0:
        deadline = time.monotonic() + backpressure.config.block_seconds
        while time.monotonic() < deadline:
            await asyncio.sleep(backpressure.config.block_sleep)
            try:
                work_queue.put_nowait(item)
                return True
            except queue.Full:
                continue
    elif (
        backpressure.config.mode == "throttle"
        and backpressure.config.throttle_seconds > 0
    ):
        await asyncio.sleep(backpressure.config.throttle_seconds)
        try:
            work_queue.put_nowait(item)
            return True
        except queue.Full:
            pass
    elif backpressure.config.mode == "resubscribe":
        backpressure.request_resubscribe()
    return _queue_put_nowait(work_queue, item, kind)
