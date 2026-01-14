"""Dataclasses for WebSocket ingestion configuration/state."""

from __future__ import annotations

import asyncio
import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Iterable


@dataclass(frozen=True)
class WriterConfig:
    """Configuration for the WS DB writer thread."""

    database_url: str
    tick_batch_size: int
    lifecycle_batch_size: int
    flush_seconds: float
    dedup_enabled: bool
    dedup_max_age_seconds: float
    dedup_fields: tuple[str, ...]


@dataclass(frozen=True)
class WsLoopOptions:
    """Optional overrides for WS loop settings."""

    max_active_tickers: int = 5000
    ws_batch_size: int = 500
    refresh_seconds: int = 60
    kalshi_host: str | None = None
    database_url: str | None = None


@dataclass(frozen=True)
class WsRuntimeConfig:
    """Runtime limits and queue settings for the WS loop."""

    max_active_tickers: int
    ws_batch_size: int
    refresh_seconds: int
    queue_maxsize: int
    ws_max_queue: int
    ws_max_size: int


@dataclass(frozen=True)
class ShardConfig:
    """Shard selection for WS subscriptions."""

    count: int
    shard_id: int
    round_robin: bool
    round_robin_step: int
    key: str = "event"


@dataclass(frozen=True)
class FailureConfig:
    """Reconnect failure thresholds and cooldowns."""

    threshold: int
    cooldown: float


@dataclass(frozen=True)
class WsLoopConfig:
    """Resolved configuration for the WS loop."""

    ws_url: str
    channels: tuple[str, ...]
    runtime: WsRuntimeConfig
    shard: ShardConfig
    writer: WriterConfig
    failure: FailureConfig


@dataclass(frozen=True)
class WriterState:
    """Runtime state for the DB writer thread."""

    work_queue: queue.Queue
    stop_event: threading.Event
    thread: threading.Thread


@dataclass(frozen=True)
class SubscriptionConfig:
    """Subscription settings for WS refresh operations."""

    channels: tuple[str, ...]
    max_active_tickers: int
    shard: ShardConfig
    ws_batch_size: int


@dataclass(frozen=True)
class PendingUpdate:
    """Track pending update_subscription requests."""

    action: str
    sid: int | None
    tickers: tuple[str, ...]
    sid_field: str | None = None
    attempts: int = 0


@dataclass
class SubscriptionState:
    """Mutable subscription state for WS refresh operations."""

    subscribed: set[str]
    lock: asyncio.Lock
    request_id: Iterable[int]
    sid_field: str | None = None
    sid_tickers: dict[int, set[str]] = field(default_factory=dict)
    pending_subscriptions: dict[int, set[str]] = field(default_factory=dict)
    pending_updates: dict[int, PendingUpdate] = field(default_factory=dict)


@dataclass(frozen=True)
class SubscriptionContext:
    """Context for maintaining WS subscriptions."""

    conn: Any
    config: SubscriptionConfig
    state: SubscriptionState


@dataclass
class WsLoopState:
    """Mutable state for WS reconnect attempts."""

    backoff: int = 1
    consecutive_failures: int = 0
    error_total: int = 0


@dataclass(frozen=True)
class WsSessionContext:
    """Context for a single WS session."""

    conn: Any
    work_queue: queue.Queue
    market_id_map: dict[str, str]
    config: WsLoopConfig
    api_key_id: str
    private_key_pem: str
