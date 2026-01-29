"""WebSocket ingestion loop."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from ...core.guardrails import assert_service_role
from ...core.loop_utils import log_metric as _log_metric
from ...core.number_utils import coerce_int as _coerce_int
from ...core.env_utils import env_float, env_int
from ...db.db import set_state
from ...db.tickers import load_active_tickers_shard
from .ws_ingest_config import (
    _DEFAULT_WS_PATH,
    _bool_env,
    _build_ws_loop_config,
    _resolve_ws_loop_options,
)
from .ws_ingest_utils import (
    _extract_channel,
    _extract_payload,
    _is_terminal_lifecycle,
    _normalize_lifecycle,
    _normalize_tick,
    _resolve_market_ticker,
)
from .ws_ingest_db_utils import (
    PsycopgError,
)
from .ws_ingest_models import (
    SubscriptionConfig,
    SubscriptionContext,
    SubscriptionState,
    WriterState,
    WriterStatus,
    WsLoopConfig,
    WsLoopOptions,
    WsLoopState,
    WsSessionContext,
)
from .ws_ingest_protocol import (
    _build_ws_headers as _build_ws_headers_impl,
)
from .ws_ingest_subscriptions import (
    _build_subscription_context,
    _extract_error_code,
    _extract_subscription_id_field,
    _extract_subscription_ids,
    _handle_update_error,
    _pop_pending_update,
    _record_subscription_id_field,
    _record_subscription_ticker,
    _register_subscription_sid,
    _refresh_subscriptions,
)
from .ws_ingest_backpressure import (
    QueueBackpressureState,
    enqueue_ws_item,
    load_backpressure_state,
    reset_backpressure_override,
)
from .ws_ingest_writer import (
    _DB_WORK_LIFECYCLE,
    _DB_WORK_STOP,
    _DB_WORK_TICK,
    _db_writer_loop,
    _update_market_id_map,
    writer_is_healthy,
)


logger = logging.getLogger(__name__)


@dataclass
class _TickDropStats:
    total: int = 0
    window: int = 0
    last_log: float = 0.0


_TICK_DROP_STATS: dict[str, _TickDropStats] = {}


def _tick_drop_log_interval() -> float:
    return env_float("WS_TICK_DROP_METRIC_SECONDS", 60.0, minimum=0.0)


def _tick_drop_payload_flags(payload: dict | None) -> dict[str, int]:
    if not isinstance(payload, dict):
        return {}
    market = payload.get("market")
    if not isinstance(market, dict):
        market = {}

    def _flag(value: Any) -> int:
        return 1 if value else 0

    return {
        "has_market_id": _flag(
            payload.get("market_id") or payload.get("marketId") or payload.get("marketID")
        ),
        "has_id": _flag(payload.get("id")),
        "has_ticker": _flag(payload.get("ticker")),
        "has_market_ticker": _flag(
            payload.get("market_ticker") or payload.get("marketTicker")
        ),
        "has_market_obj": _flag(bool(market)),
        "market_has_id": _flag(
            market.get("market_id")
            or market.get("marketId")
            or market.get("marketID")
            or market.get("id")
        ),
        "market_has_ticker": _flag(
            market.get("ticker")
            or market.get("market_ticker")
            or market.get("marketTicker")
        ),
    }


def _record_tick_drop(reason: str, payload: dict | None) -> None:
    stats = _TICK_DROP_STATS.setdefault(reason, _TickDropStats())
    stats.total += 1
    stats.window += 1
    interval = _tick_drop_log_interval()
    now = time.monotonic()
    if interval <= 0 or now - stats.last_log >= interval:
        _log_metric(
            logger,
            "ws.tick_drop",
            reason=reason,
            drops=stats.window,
            drops_total=stats.total,
            **_tick_drop_payload_flags(payload),
        )
        stats.window = 0
        stats.last_log = now


@lru_cache(maxsize=1)
def _load_orjson():
    try:
        return importlib.import_module("orjson")
    except ImportError:  # pragma: no cover
        return None


def _fast_json_loads(raw: Any) -> dict | None:
    orjson_module = _load_orjson()
    if orjson_module is not None:
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        return orjson_module.loads(raw)
    return json.loads(raw)


def _build_ws_headers(
    api_key_id: str,
    private_key_pem: str,
    ws_url: str,
    default_path: str | None = None,
) -> dict[str, str]:
    return _build_ws_headers_impl(
        api_key_id,
        private_key_pem,
        ws_url,
        default_path or _DEFAULT_WS_PATH,
    )


@lru_cache(maxsize=1)
def _load_websockets():
    try:
        return importlib.import_module("websockets")
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "websockets is required for WS ingestion; install websockets."
        ) from exc


def _ws_error_types(ws_lib) -> tuple[type[BaseException], ...]:
    errors: list[type[BaseException]] = [OSError, RuntimeError, ValueError, PsycopgError]
    ws_exceptions = getattr(ws_lib, "exceptions", None)
    if ws_exceptions is not None:
        for name in ("WebSocketException", "ConnectionClosed", "ConnectionClosedError"):
            exc_type = getattr(ws_exceptions, name, None)
            if isinstance(exc_type, type) and issubclass(exc_type, BaseException):
                errors.append(exc_type)
    return tuple(dict.fromkeys(errors))


def _ws_expected_error_types(ws_lib) -> tuple[type[BaseException], ...]:
    ws_exceptions = getattr(ws_lib, "exceptions", None)
    expected_errors: list[type[BaseException]] = [
        ConnectionResetError,
        asyncio.TimeoutError,
    ]
    if ws_exceptions is None:
        return tuple(expected_errors)
    for name in ("ConnectionClosed", "ConnectionClosedError", "ConnectionClosedOK"):
        exc_type = getattr(ws_exceptions, name, None)
        if isinstance(exc_type, type) and issubclass(exc_type, BaseException):
            expected_errors.append(exc_type)
    return tuple(dict.fromkeys(expected_errors))


def _start_db_writer(
    config: WsLoopConfig,
    *,
    work_queue: queue.Queue | None = None,
    status: WriterStatus | None = None,
) -> WriterState:
    if work_queue is None:
        work_queue = queue.Queue(maxsize=config.runtime.queue_maxsize)
    if status is None:
        status = WriterStatus()
    stop_event = threading.Event()
    restart_event = threading.Event()
    writer_thread = threading.Thread(
        target=_db_writer_loop,
        args=(
            work_queue,
            config.writer,
            stop_event,
            restart_event,
            status,
        ),
        daemon=True,
    )
    writer_thread.start()
    return WriterState(
        work_queue=work_queue,
        stop_event=stop_event,
        restart_event=restart_event,
        status=status,
        thread=writer_thread,
    )


async def _monitor_writer_loop(writer_ref: dict[str, WriterState], config: WsLoopConfig) -> None:
    interval = env_int("WS_WRITER_MONITOR_SECONDS", 30, minimum=5)
    stale_seconds = env_int("WS_WRITER_STALE_SECONDS", max(interval * 4, 60), minimum=10)
    while True:
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break
        state = writer_ref["state"]
        if not state.thread.is_alive():
            logger.warning("WS DB writer thread stopped; restarting")
            writer_ref["state"] = _start_db_writer(
                config,
                work_queue=state.work_queue,
            )
            continue
        if not writer_is_healthy(state.status, stale_seconds):
            logger.warning("WS DB writer heartbeat stale; requesting restart")
            state.restart_event.set()


def _build_ws_connect_kwargs(
    ws_lib,
    headers: dict[str, str],
    config: WsLoopConfig,
) -> dict[str, Any]:
    connect_kwargs: dict[str, Any] = {
        "ping_interval": 20,
        "ping_timeout": 20,
        "close_timeout": 10,
    }
    try:
        params = inspect.signature(ws_lib.connect).parameters
        if "additional_headers" in params:
            connect_kwargs["additional_headers"] = headers
        else:
            connect_kwargs["extra_headers"] = headers
        if "max_queue" in params:
            connect_kwargs["max_queue"] = config.runtime.ws_max_queue
        if "max_size" in params:
            connect_kwargs["max_size"] = config.runtime.ws_max_size
    except (TypeError, ValueError):
        connect_kwargs["extra_headers"] = headers
    return connect_kwargs


async def _run_ws_connection(
    ws_lib,
    context: WsSessionContext,
    state: WsLoopState,
    backpressure: QueueBackpressureState | None,
) -> None:
    headers = _build_ws_headers(
        context.api_key_id,
        context.private_key_pem,
        context.config.ws_url,
        default_path=_DEFAULT_WS_PATH,
    )
    connect_kwargs = _build_ws_connect_kwargs(ws_lib, headers, context.config)
    async with ws_lib.connect(context.config.ws_url, **connect_kwargs) as websocket:
        logger.info("WS connected: %s", context.config.ws_url)
        if state.consecutive_failures:
            logger.info("WS recovered after %d failures", state.consecutive_failures)
        state.consecutive_failures = 0
        state.backoff = 1
        _log_metric(
            logger,
            "ws.connected",
            url=context.config.ws_url,
            errors_total=state.error_total,
        )
        subscription_context = await _build_subscription_context(websocket, context)
        task_context = WsTaskContext(
            work_queue=context.work_queue,
            market_id_map=context.market_id_map,
            refresh_seconds=context.config.runtime.refresh_seconds,
            backpressure=backpressure,
        )
        await _run_ws_tasks(
            websocket,
            subscription_context,
            task_context,
        )


@dataclass(frozen=True)
class WsTaskContext:
    """Parameters shared across WS listener/refresher tasks."""

    work_queue: queue.Queue
    market_id_map: dict[str, str]
    refresh_seconds: int
    backpressure: QueueBackpressureState | None


@dataclass(frozen=True)
class WsMessageContext:
    """Context shared while handling WS messages."""

    work_queue: queue.Queue
    market_id_map: dict[str, str]
    subscription_state: SubscriptionState | None
    subscription_config: SubscriptionConfig | None
    backpressure: QueueBackpressureState | None


async def _run_ws_tasks(
    websocket,
    subscription_context: SubscriptionContext,
    task_context: WsTaskContext,
) -> None:
    heartbeat_seconds = env_int("WS_HEARTBEAT_SECONDS", 60, minimum=0)
    message_context = WsMessageContext(
        work_queue=task_context.work_queue,
        market_id_map=task_context.market_id_map,
        subscription_state=subscription_context.state,
        subscription_config=subscription_context.config,
        backpressure=task_context.backpressure,
    )
    listener = asyncio.create_task(
        _listen_loop(
            websocket,
            message_context,
        )
    )
    wake_event = None
    if (
        task_context.backpressure is not None
        and task_context.backpressure.config.mode == "resubscribe"
    ):
        wake_event = task_context.backpressure.resubscribe_event
    refresher = asyncio.create_task(
        _refresh_subscriptions(
            websocket,
            subscription_context,
            task_context.refresh_seconds,
            wake_event,
        )
    )
    heartbeat = None
    if heartbeat_seconds > 0:
        heartbeat = asyncio.create_task(
            _heartbeat_loop(
                subscription_context,
                task_context.work_queue,
                task_context.market_id_map,
                heartbeat_seconds,
                task_context.refresh_seconds,
            )
        )
    done, pending = await asyncio.wait(
        {listener, refresher},
        return_when=asyncio.FIRST_EXCEPTION,
    )
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    if heartbeat:
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)
    for task in done:
        exc = task.exception()
        if exc:
            raise exc


@dataclass(frozen=True)
class HeartbeatMetrics:
    """Snapshot of subscription metrics for heartbeat reporting."""

    subscribed: int
    sid_count: int
    pending_subs: int
    pending_updates: int


@dataclass(frozen=True)
class HeartbeatContext:
    """Derived heartbeat values for metrics/log payloads."""

    active_count: int | None
    missing: int | None
    stale_window: int
    stale_count: int | None
    work_queue: queue.Queue
    market_id_map: dict[str, str]


def _load_active_tickers_for_heartbeat(
    subscription_context: SubscriptionContext,
) -> tuple[list[str] | None, int | None]:
    try:
        active = load_active_tickers_shard(
            subscription_context.conn,
            subscription_context.config.max_active_tickers,
            subscription_context.config.shard.count,
            subscription_context.config.shard.shard_id,
            shard_key=subscription_context.config.shard.key,
            round_robin=subscription_context.config.shard.round_robin,
            round_robin_step=subscription_context.config.shard.round_robin_step,
        )
        return active, len(active)
    except (PsycopgError, ValueError, TypeError, RuntimeError):
        return None, None


def _compute_stale_tick_count(
    conn,
    active: list[str] | None,
    stale_window: int,
) -> int | None:
    if not active or stale_window <= 0:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM unnest(%s::text[]) AS a(ticker)
                LEFT JOIN market_ticks_latest mt
                  ON mt.ticker = a.ticker
                WHERE mt.updated_at IS NULL
                   OR mt.updated_at < NOW() - (%s * INTERVAL '1 second')
                """,
                (active, stale_window),
            )
            return int(cur.fetchone()[0] or 0)
    except (PsycopgError, ValueError, TypeError, RuntimeError):
        return None


async def _snapshot_subscription_metrics(state: SubscriptionState) -> HeartbeatMetrics:
    async with state.lock:
        return HeartbeatMetrics(
            subscribed=len(state.subscribed),
            sid_count=len(state.sid_tickers),
            pending_subs=sum(len(tickers) for tickers in state.pending_subscriptions.values()),
            pending_updates=len(state.pending_updates),
        )


def _build_heartbeat_payload(
    metrics: HeartbeatMetrics,
    context: HeartbeatContext,
) -> dict[str, Any]:
    return {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "subscribed": metrics.subscribed,
        "active_tickers": context.active_count,
        "missing_subscriptions": context.missing,
        "subscription_ids": metrics.sid_count,
        "pending_subscriptions": metrics.pending_subs,
        "pending_updates": metrics.pending_updates,
        "queue_size": context.work_queue.qsize(),
        "queue_max": context.work_queue.maxsize,
        "market_id_map": len(context.market_id_map),
        "stale_tick_window_s": context.stale_window,
        "stale_tick_count": context.stale_count,
    }


def _log_heartbeat_metrics(
    metrics: HeartbeatMetrics | None,
    context: HeartbeatContext,
) -> None:
    payload = {
        "active_tickers": context.active_count,
        "stale_tick_window_s": context.stale_window,
        "stale_tick_count": context.stale_count,
        "queue_size": context.work_queue.qsize(),
        "queue_max": context.work_queue.maxsize,
        "market_id_map": len(context.market_id_map),
    }
    if metrics is not None:
        payload.update(
            {
                "subscribed": metrics.subscribed,
                "missing_subscriptions": context.missing,
                "subscription_ids": metrics.sid_count,
                "pending_subscriptions": metrics.pending_subs,
                "pending_updates": metrics.pending_updates,
            }
        )
    _log_metric(logger, "ws.heartbeat", **payload)


async def _heartbeat_loop(
    subscription_context: SubscriptionContext,
    work_queue: queue.Queue,
    market_id_map: dict[str, str],
    interval_seconds: int,
    stale_window_seconds: int,
) -> None:
    while True:
        try:
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            break
        state = subscription_context.state
        active, active_count = _load_active_tickers_for_heartbeat(subscription_context)
        stale_window = env_int(
            "WS_TICK_STALE_SECONDS",
            stale_window_seconds,
            minimum=0,
        )
        stale_count = _compute_stale_tick_count(
            subscription_context.conn,
            active,
            stale_window,
        )
        context = HeartbeatContext(
            active_count=active_count,
            missing=None,
            stale_window=stale_window,
            stale_count=stale_count,
            work_queue=work_queue,
            market_id_map=market_id_map,
        )
        if state is None:
            _log_heartbeat_metrics(metrics=None, context=context)
            continue
        metrics = await _snapshot_subscription_metrics(state)
        missing = None
        if active_count is not None:
            missing = max(0, active_count - (metrics.subscribed + metrics.pending_subs))
        context = HeartbeatContext(
            active_count=active_count,
            missing=missing,
            stale_window=stale_window,
            stale_count=stale_count,
            work_queue=work_queue,
            market_id_map=market_id_map,
        )
        heartbeat_payload = _build_heartbeat_payload(metrics, context)
        _record_ws_heartbeat(subscription_context.conn, heartbeat_payload)
        _log_heartbeat_metrics(metrics, context)
        if missing:
            logger.warning(
                "ws heartbeat: missing subscriptions=%d active=%s subscribed=%d pending=%d",
                missing,
                active_count,
                metrics.subscribed,
                metrics.pending_subs,
            )


def _record_ws_heartbeat(conn, payload: dict[str, Any]) -> None:
    try:
        set_state(
            conn,
            "ws_heartbeat",
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
        )
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("ws heartbeat state write failed")


def _decode_ws_message(raw: Any) -> dict | None:
    try:
        message = _fast_json_loads(raw)
    except (ValueError, TypeError):
        logger.warning("WS message decode failed")
        return None
    if not isinstance(message, dict):
        return None
    return message


async def _handle_subscription_ack(
    message: dict,
    subscription_state: SubscriptionState | None,
) -> bool:
    if subscription_state is None:
        return False
    ids = _extract_subscription_ids(message)
    if ids is None:
        return False
    logger.debug("WS subscription ack payload: %s", message)
    request_id, sid = ids
    sid_field = _extract_subscription_id_field(message)
    if sid_field is not None:
        await _record_subscription_id_field(subscription_state, sid_field)
    tracked = await _register_subscription_sid(
        subscription_state,
        request_id,
        sid,
    )
    pending_update = await _pop_pending_update(
        subscription_state,
        request_id,
    )
    if pending_update is not None:
        logger.debug("WS update_subscription ack: id=%s sid=%s", request_id, sid)
    logger.debug(
        "WS subscription ack: id=%s sid=%s tickers=%s",
        request_id,
        sid,
        tracked,
    )
    return True


async def _handle_error_message(
    websocket,
    message: dict,
    subscription_state: SubscriptionState | None,
    subscription_config: SubscriptionConfig | None,
) -> bool:
    msg_type = message.get("type")
    is_error = msg_type == "error" or message.get("error")
    if not is_error:
        return False
    if subscription_state is not None and subscription_config is not None:
        request_id = _coerce_int(message.get("id"))
        error_code = _extract_error_code(message)
        await _handle_update_error(
            websocket,
            subscription_state,
            subscription_config,
            request_id,
            error_code,
        )
    logger.warning("WS error payload: %s", message)
    return True


async def _handle_update_ack(
    message: dict,
    subscription_state: SubscriptionState | None,
) -> None:
    if subscription_state is None:
        return
    request_id = _coerce_int(message.get("id"))
    if request_id is None:
        return
    pending_update = await _pop_pending_update(
        subscription_state,
        request_id,
    )
    if pending_update is not None:
        if pending_update.sid_field is not None:
            await _record_subscription_id_field(
                subscription_state,
                pending_update.sid_field,
            )
        logger.debug("WS update_subscription ack payload: %s", message)
        logger.debug("WS update_subscription ack: id=%s", request_id)


async def _handle_ticker_payload(
    message: dict,
    payload: dict,
    context: WsMessageContext,
) -> None:
    _update_market_id_map(payload, context.market_id_map)
    ticker = _resolve_market_ticker(payload, context.market_id_map)
    if not ticker:
        _record_tick_drop("missing_ticker", payload)
        return
    tick = _normalize_tick(payload, context.market_id_map)
    if tick is None:
        return
    if context.subscription_state is not None:
        sid = _coerce_int(message.get("sid"))
        if sid is not None:
            await _record_subscription_ticker(
                context.subscription_state,
                sid,
                tick.get("ticker"),
            )
    await enqueue_ws_item(
        context.work_queue,
        (_DB_WORK_TICK, tick, None, None),
        "tick",
        context.backpressure,
    )


async def _handle_lifecycle_payload(
    message: dict,
    payload: dict,
    context: WsMessageContext,
) -> None:
    _update_market_id_map(payload, context.market_id_map)
    lifecycle = _normalize_lifecycle(payload)
    if lifecycle is None:
        return
    if context.subscription_state is not None:
        sid = _coerce_int(message.get("sid"))
        if sid is not None:
            await _record_subscription_ticker(
                context.subscription_state,
                sid,
                lifecycle.get("market_ticker"),
            )
    market = payload.get("market")
    delete_ticker = None
    if _is_terminal_lifecycle(lifecycle.get("event_type")):
        delete_ticker = lifecycle.get("market_ticker")
    await enqueue_ws_item(
        context.work_queue,
        (_DB_WORK_LIFECYCLE, lifecycle, market, delete_ticker),
        "lifecycle",
        context.backpressure,
    )


async def _handle_channel_payload(
    message: dict,
    context: WsMessageContext,
) -> None:
    channel = _extract_channel(message)
    payload = _extract_payload(message)
    if channel == "ticker":
        await _handle_ticker_payload(
            message,
            payload,
            context,
        )
    elif channel == "market_lifecycle_v2":
        await _handle_lifecycle_payload(
            message,
            payload,
            context,
        )


async def _listen_loop(
    websocket,
    context: WsMessageContext | queue.Queue,
    market_id_map: dict[str, str] | None = None,
) -> None:
    """Receive and persist WS messages."""
    if not isinstance(context, WsMessageContext):
        context = WsMessageContext(
            work_queue=context,
            market_id_map=market_id_map or {},
            subscription_state=None,
            subscription_config=None,
            backpressure=None,
        )
    async for raw in websocket:
        message = _decode_ws_message(raw)
        if message is None:
            continue
        if await _handle_subscription_ack(message, context.subscription_state):
            continue
        if await _handle_error_message(
            websocket,
            message,
            context.subscription_state,
            context.subscription_config,
        ):
            continue
        await _handle_update_ack(message, context.subscription_state)
        await _handle_channel_payload(message, context)


@dataclass(frozen=True)
class WsRuntime:
    """Runtime wiring for websocket ingestion connections."""

    config: WsLoopConfig
    backpressure: QueueBackpressureState
    session_context: WsSessionContext
    writer: "WsWriterRuntime"
    connection: "WsConnectionRuntime"


@dataclass(frozen=True)
class WsWriterRuntime:
    """Writer thread handles for the WS runtime."""

    writer_ref: dict[str, WriterState]
    monitor_task: asyncio.Task


@dataclass(frozen=True)
class WsConnectionRuntime:
    """Websocket library and error metadata."""

    ws_lib: Any
    ws_errors: tuple[type[BaseException], ...]
    ws_expected_errors: tuple[type[BaseException], ...]


def _build_ws_runtime(
    conn,
    api_key_id: str,
    private_key_pem: str,
    options: WsLoopOptions | None,
    **kwargs: Any,
) -> WsRuntime:
    options = _resolve_ws_loop_options(options, **kwargs)
    config = _build_ws_loop_config(conn, options)
    backpressure = load_backpressure_state(config)
    writer_state = _start_db_writer(config)
    writer_ref = {"state": writer_state}
    monitor_task = asyncio.create_task(_monitor_writer_loop(writer_ref, config))
    writer_runtime = WsWriterRuntime(
        writer_ref=writer_ref,
        monitor_task=monitor_task,
    )
    ws_lib = _load_websockets()
    connection_runtime = WsConnectionRuntime(
        ws_lib=ws_lib,
        ws_errors=_ws_error_types(ws_lib),
        ws_expected_errors=_ws_expected_error_types(ws_lib),
    )
    session_context = WsSessionContext(
        conn=conn,
        work_queue=writer_state.work_queue,
        market_id_map={},
        config=config,
        api_key_id=api_key_id,
        private_key_pem=private_key_pem,
    )
    return WsRuntime(
        config=config,
        backpressure=backpressure,
        session_context=session_context,
        writer=writer_runtime,
        connection=connection_runtime,
    )


async def _shutdown_ws_runtime(runtime: WsRuntime) -> None:
    runtime.writer.monitor_task.cancel()
    await asyncio.gather(runtime.writer.monitor_task, return_exceptions=True)
    current_writer = runtime.writer.writer_ref["state"]
    current_writer.stop_event.set()
    try:
        current_writer.work_queue.put_nowait(_DB_WORK_STOP)
    except queue.Full:
        pass
    current_writer.thread.join(timeout=5)
    reset_backpressure_override(runtime.backpressure)


async def _sleep_ws_backoff(state: WsLoopState, config: WsLoopConfig) -> None:
    if state.consecutive_failures >= config.failure.threshold:
        logger.warning(
            "WS circuit open failures=%d cooldown_s=%.1f",
            state.consecutive_failures,
            config.failure.cooldown,
        )
        await asyncio.sleep(config.failure.cooldown)
        state.consecutive_failures = 0
        state.backoff = 1
        return
    await asyncio.sleep(state.backoff)
    state.backoff = min(state.backoff * 2, 60)


async def _handle_ws_expected_disconnect(
    state: WsLoopState,
    config: WsLoopConfig,
    exc: BaseException,
) -> None:
    state.error_total += 1
    state.consecutive_failures += 1
    logger.warning("WS disconnected; reconnecting soon: %s", exc)
    _log_metric(
        logger,
        "ws.reconnect",
        errors_total=state.error_total,
        consecutive_failures=state.consecutive_failures,
        backoff_s=state.backoff,
    )
    await _sleep_ws_backoff(state, config)


async def _handle_ws_error(state: WsLoopState, config: WsLoopConfig) -> None:
    state.error_total += 1
    state.consecutive_failures += 1
    logger.exception("WS loop error; reconnecting")
    _log_metric(
        logger,
        "ws.reconnect",
        errors_total=state.error_total,
        consecutive_failures=state.consecutive_failures,
        backoff_s=state.backoff,
    )
    await _sleep_ws_backoff(state, config)


async def _handle_ws_fatal_error(state: WsLoopState, exc: BaseException) -> None:
    state.error_total += 1
    state.consecutive_failures += 1
    logger.exception("WS fatal error; restarting loop: %s", exc)
    await asyncio.sleep(5)


async def _run_ws_forever(runtime: WsRuntime, state: WsLoopState) -> None:
    fatal_errors = (TypeError, AttributeError, KeyError)
    while True:
        try:
            await _run_ws_connection(
                runtime.connection.ws_lib,
                runtime.session_context,
                state,
                runtime.backpressure,
            )
        except asyncio.CancelledError:
            raise
        except runtime.connection.ws_expected_errors as exc:
            await _handle_ws_expected_disconnect(state, runtime.config, exc)
        except runtime.connection.ws_errors:
            await _handle_ws_error(state, runtime.config)
        except fatal_errors as exc:
            await _handle_ws_fatal_error(state, exc)


async def ws_loop(
    conn,
    api_key_id: str,
    private_key_pem: str,
    options: WsLoopOptions | None = None,
    **kwargs: Any,
):
    """Run the websocket ingestion loop.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param api_key_id: Kalshi API key identifier.
    :type api_key_id: str
    :param private_key_pem: Private key PEM contents.
    :type private_key_pem: str
    :param options: Optional WS loop options.
    :type options: WsLoopOptions | None
    :return: None.
    :rtype: None
    """
    assert_service_role("ws", "ws_loop")
    if not _bool_env("KALSHI_WS_ENABLE", False):
        logger.warning(
            "WebSocket ingestion disabled. Set KALSHI_WS_ENABLE=1 to enable it."
        )
        while True:
            await asyncio.sleep(3600)

    runtime = _build_ws_runtime(conn, api_key_id, private_key_pem, options, **kwargs)
    state = WsLoopState()
    try:
        await _run_ws_forever(runtime, state)
    finally:
        await _shutdown_ws_runtime(runtime)
