"""WebSocket ingestion loop."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
import queue
import threading
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from src.core.guardrails import assert_service_role
from src.core.loop_utils import log_metric as _log_metric
from src.core.number_utils import coerce_int as _coerce_int
from src.core.env_utils import env_int
from src.db.db import load_active_tickers_shard, set_state
from src.ingest.ws.ws_ingest_config import (
    _DEFAULT_WS_PATH,
    _bool_env,
    _build_ws_loop_config,
    _resolve_ws_loop_options,
)
from src.ingest.ws.ws_ingest_utils import (
    _extract_channel,
    _extract_payload,
    _is_terminal_lifecycle,
    _normalize_lifecycle,
    _normalize_tick,
)
from src.ingest.ws.ws_ingest_db_utils import (
    PsycopgError,
)
from src.ingest.ws.ws_ingest_models import (
    SubscriptionConfig,
    SubscriptionContext,
    SubscriptionState,
    WriterState,
    WsLoopConfig,
    WsLoopOptions,
    WsLoopState,
    WsSessionContext,
)
from src.ingest.ws.ws_ingest_protocol import (
    _build_ws_headers as _build_ws_headers_impl,
)
from src.ingest.ws.ws_ingest_subscriptions import (
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
from src.ingest.ws.ws_ingest_writer import (
    _DB_WORK_LIFECYCLE,
    _DB_WORK_STOP,
    _DB_WORK_TICK,
    _db_writer_loop,
    _queue_put_nowait,
    _update_market_id_map,
)


logger = logging.getLogger(__name__)

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


def _start_db_writer(config: WsLoopConfig) -> WriterState:
    work_queue: queue.Queue = queue.Queue(maxsize=config.runtime.queue_maxsize)
    stop_event = threading.Event()
    writer_thread = threading.Thread(
        target=_db_writer_loop,
        args=(
            work_queue,
            config.writer,
            stop_event,
        ),
        daemon=True,
    )
    writer_thread.start()
    return WriterState(
        work_queue=work_queue,
        stop_event=stop_event,
        thread=writer_thread,
    )


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
        await _run_ws_tasks(
            websocket,
            subscription_context,
            context.work_queue,
            context.market_id_map,
            context.config.runtime.refresh_seconds,
        )


async def _run_ws_tasks(
    websocket,
    subscription_context: SubscriptionContext,
    work_queue: queue.Queue,
    market_id_map: dict[str, str],
    refresh_seconds: int,
) -> None:
    heartbeat_seconds = env_int("WS_HEARTBEAT_SECONDS", 60, minimum=0)
    listener = asyncio.create_task(
        _listen_loop(
            websocket,
            work_queue,
            market_id_map,
            subscription_context.state,
            subscription_context.config,
        )
    )
    refresher = asyncio.create_task(
        _refresh_subscriptions(
            websocket,
            subscription_context,
            refresh_seconds,
        )
    )
    heartbeat = None
    if heartbeat_seconds > 0:
        heartbeat = asyncio.create_task(
            _heartbeat_loop(
                subscription_context,
                work_queue,
                market_id_map,
                heartbeat_seconds,
                refresh_seconds,
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
        active = None
        active_count = None
        missing = None
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
            active_count = len(active)
        except Exception:  # pylint: disable=broad-exception-caught
            active = None
            active_count = None

        stale_window = env_int(
            "WS_TICK_STALE_SECONDS",
            stale_window_seconds,
            minimum=0,
        )
        stale_count = None
        if active and stale_window > 0:
            try:
                with subscription_context.conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM unnest(%s::text[]) AS a(ticker)
                        LEFT JOIN LATERAL (
                          SELECT ts
                          FROM market_ticks mt
                          WHERE mt.ticker = a.ticker
                          ORDER BY mt.ts DESC
                          LIMIT 1
                        ) mt ON TRUE
                        WHERE mt.ts IS NULL
                           OR mt.ts < NOW() - (%s * INTERVAL '1 second')
                        """,
                        (active, stale_window),
                    )
                    stale_count = int(cur.fetchone()[0] or 0)
            except Exception:  # pylint: disable=broad-exception-caught
                stale_count = None

        if state is None:
            _log_metric(
                logger,
                "ws.heartbeat",
                active_tickers=active_count,
                stale_tick_window_s=stale_window,
                stale_tick_count=stale_count,
                queue_size=work_queue.qsize(),
                queue_max=work_queue.maxsize,
                market_id_map=len(market_id_map),
            )
            continue
        async with state.lock:
            subscribed = len(state.subscribed)
            sid_count = len(state.sid_tickers)
            pending_subs = sum(
                len(tickers) for tickers in state.pending_subscriptions.values()
            )
            pending_updates = len(state.pending_updates)
        if active_count is not None:
            missing = max(0, active_count - (subscribed + pending_subs))
        heartbeat_payload = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "subscribed": subscribed,
            "active_tickers": active_count,
            "missing_subscriptions": missing,
            "subscription_ids": sid_count,
            "pending_subscriptions": pending_subs,
            "pending_updates": pending_updates,
            "queue_size": work_queue.qsize(),
            "queue_max": work_queue.maxsize,
            "market_id_map": len(market_id_map),
            "stale_tick_window_s": stale_window,
            "stale_tick_count": stale_count,
        }
        _record_ws_heartbeat(subscription_context.conn, heartbeat_payload)
        _log_metric(
            logger,
            "ws.heartbeat",
            subscribed=subscribed,
            active_tickers=active_count,
            missing_subscriptions=missing,
            subscription_ids=sid_count,
            pending_subscriptions=pending_subs,
            pending_updates=pending_updates,
            stale_tick_window_s=stale_window,
            stale_tick_count=stale_count,
            queue_size=work_queue.qsize(),
            queue_max=work_queue.maxsize,
            market_id_map=len(market_id_map),
        )
        if missing:
            logger.warning(
                "ws heartbeat: missing subscriptions=%d active=%s subscribed=%d pending=%d",
                missing,
                active_count,
                subscribed,
                pending_subs,
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
        logger.debug("WS update_subscription ack: id=%s", request_id)


async def _handle_ticker_payload(
    message: dict,
    payload: dict,
    work_queue: queue.Queue,
    market_id_map: dict[str, str],
    subscription_state: SubscriptionState | None,
) -> None:
    tick = _normalize_tick(payload, market_id_map)
    if tick is None:
        return
    if subscription_state is not None:
        sid = _coerce_int(message.get("sid"))
        if sid is not None:
            await _record_subscription_ticker(
                subscription_state,
                sid,
                tick.get("ticker"),
            )
    _queue_put_nowait(
        work_queue,
        (_DB_WORK_TICK, tick, None, None),
        "tick",
    )


async def _handle_lifecycle_payload(
    message: dict,
    payload: dict,
    work_queue: queue.Queue,
    market_id_map: dict[str, str],
    subscription_state: SubscriptionState | None,
) -> None:
    _update_market_id_map(payload, market_id_map)
    lifecycle = _normalize_lifecycle(payload)
    if lifecycle is None:
        return
    if subscription_state is not None:
        sid = _coerce_int(message.get("sid"))
        if sid is not None:
            await _record_subscription_ticker(
                subscription_state,
                sid,
                lifecycle.get("market_ticker"),
            )
    market = payload.get("market")
    delete_ticker = None
    if _is_terminal_lifecycle(lifecycle.get("event_type")):
        delete_ticker = lifecycle.get("market_ticker")
    _queue_put_nowait(
        work_queue,
        (_DB_WORK_LIFECYCLE, lifecycle, market, delete_ticker),
        "lifecycle",
    )


async def _handle_channel_payload(
    message: dict,
    work_queue: queue.Queue,
    market_id_map: dict[str, str],
    subscription_state: SubscriptionState | None,
) -> None:
    channel = _extract_channel(message)
    payload = _extract_payload(message)
    if channel == "ticker":
        await _handle_ticker_payload(
            message,
            payload,
            work_queue,
            market_id_map,
            subscription_state,
        )
    elif channel == "market_lifecycle_v2":
        await _handle_lifecycle_payload(
            message,
            payload,
            work_queue,
            market_id_map,
            subscription_state,
        )


async def _listen_loop(
    websocket,
    work_queue: queue.Queue,
    market_id_map: dict[str, str],
    subscription_state: SubscriptionState | None = None,
    subscription_config: SubscriptionConfig | None = None,
) -> None:
    """Receive and persist WS messages."""
    async for raw in websocket:
        message = _decode_ws_message(raw)
        if message is None:
            continue
        if await _handle_subscription_ack(message, subscription_state):
            continue
        if await _handle_error_message(
            websocket,
            message,
            subscription_state,
            subscription_config,
        ):
            continue
        await _handle_update_ack(message, subscription_state)
        await _handle_channel_payload(
            message,
            work_queue,
            market_id_map,
            subscription_state,
        )


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

    options = _resolve_ws_loop_options(options, **kwargs)
    config = _build_ws_loop_config(conn, options)
    writer_state = _start_db_writer(config)
    state = WsLoopState()
    ws_lib = _load_websockets()
    ws_errors = _ws_error_types(ws_lib)
    session_context = WsSessionContext(
        conn=conn,
        work_queue=writer_state.work_queue,
        market_id_map={},
        config=config,
        api_key_id=api_key_id,
        private_key_pem=private_key_pem,
    )
    try:
        while True:
            try:
                await _run_ws_connection(ws_lib, session_context, state)
            except ws_errors:
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
                if state.consecutive_failures >= config.failure.threshold:
                    logger.warning(
                        "WS circuit open failures=%d cooldown_s=%.1f",
                        state.consecutive_failures,
                        config.failure.cooldown,
                    )
                    await asyncio.sleep(config.failure.cooldown)
                    state.consecutive_failures = 0
                    state.backoff = 1
                    continue
                await asyncio.sleep(state.backoff)
                state.backoff = min(state.backoff * 2, 60)
    finally:
        writer_state.stop_event.set()
        try:
            writer_state.work_queue.put_nowait(_DB_WORK_STOP)
        except queue.Full:
            pass
        writer_state.thread.join(timeout=5)
