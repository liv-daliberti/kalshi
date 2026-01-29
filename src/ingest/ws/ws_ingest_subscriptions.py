"""Subscription helpers for WS ingestion."""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Iterable

from ...db.db import upsert_active_markets_from_markets
from ...db.tickers import load_active_tickers_shard, load_market_tickers_shard
from ...core.number_utils import coerce_int as _coerce_int
from .ws_ingest_config import _bool_env
from .ws_ingest_db_utils import _psycopg_error_type
from .ws_ingest_models import (
    PendingUpdate,
    SubscriptionConfig,
    SubscriptionContext,
    SubscriptionState,
    WsLoopConfig,
    WsSessionContext,
)
from .ws_ingest_protocol import _build_subscribe_message, _chunked

logger = logging.getLogger(__name__)

_ACTIVE_TICKER_LIMIT_OVERRIDE: int | None = None


def set_active_ticker_limit_override(limit: int | None) -> None:
    """Set an optional override for max active tickers during WS subscriptions."""
    global _ACTIVE_TICKER_LIMIT_OVERRIDE  # pylint: disable=global-statement
    if limit is None:
        _ACTIVE_TICKER_LIMIT_OVERRIDE = None
        return
    _ACTIVE_TICKER_LIMIT_OVERRIDE = max(1, int(limit))


def _resolve_active_limit(base_limit: int) -> int:
    limit = base_limit
    if _ACTIVE_TICKER_LIMIT_OVERRIDE:
        limit = min(limit, _ACTIVE_TICKER_LIMIT_OVERRIDE)
    return limit


def _load_active_tickers(
    conn,
    limit: int,
    config: SubscriptionConfig | WsLoopConfig,
) -> list[str] | None:
    try:
        return load_active_tickers_shard(
            conn,
            limit,
            config.shard.count,
            config.shard.shard_id,
            shard_key=config.shard.key,
            round_robin=config.shard.round_robin,
            round_robin_step=config.shard.round_robin_step,
        )
    except (_psycopg_error_type(), ValueError, TypeError, RuntimeError):
        logger.exception("load_active_tickers failed")
        return None


def _load_market_tickers(
    conn,
    limit: int,
    config: SubscriptionConfig | WsLoopConfig,
) -> list[str] | None:
    try:
        return load_market_tickers_shard(
            conn,
            limit,
            config.shard.count,
            config.shard.shard_id,
            shard_key=config.shard.key,
            round_robin=config.shard.round_robin,
            round_robin_step=config.shard.round_robin_step,
        )
    except (_psycopg_error_type(), ValueError, TypeError, RuntimeError):
        logger.exception("load_market_tickers failed")
        return None


def _seed_active_markets(conn, tickers: list[str]) -> None:
    if not tickers:
        return
    try:
        seeded = upsert_active_markets_from_markets(conn, tickers)
    except (_psycopg_error_type(), ValueError, TypeError, RuntimeError):
        logger.exception("active_markets upsert failed for WS fallback")
        return
    if seeded:
        logger.info("WS fallback seeded active_markets rows=%d", seeded)

_SUBSCRIPTION_ID_FIELDS = ("sid", "subscription_id", "subscriptionId")


@dataclass(frozen=True)
class UpdateMessageOptions:
    """Optional settings for update_subscription payloads."""

    subscription_id: int | None = None
    subscription_id_field: str | None = None
    update_style: str | None = None
    include_channels: bool | None = None


@dataclass(frozen=True)
class UpdateMessageRequest:
    """Input values for update_subscription payloads."""

    request_id: int
    channels: Iterable[str]
    add_tickers: list[str]
    remove_tickers: list[str]
    options: UpdateMessageOptions = field(default_factory=UpdateMessageOptions)


@dataclass(frozen=True)
class UpdateErrorContext:
    """Inputs needed to handle update_subscription errors."""

    websocket: Any
    state: SubscriptionState
    config: SubscriptionConfig
    pending: PendingUpdate
    request_id: int
    error_code: int | None


def _normalize_subscription_id_field(value: str | None) -> str | None:
    if value is None:
        return None
    field_name = value.strip()
    if not field_name:
        return None
    if field_name in _SUBSCRIPTION_ID_FIELDS:
        return field_name
    return None


def _resolve_subscription_id_field_env() -> str | None:
    return _normalize_subscription_id_field(os.getenv("KALSHI_WS_SUBSCRIPTION_ID_FIELD"))


def _resolve_update_style(value: str | None = None) -> str:
    if value is None:
        value = os.getenv("KALSHI_WS_UPDATE_STYLE", "markets")
    if value is None:
        return "markets"
    style = value.strip().lower()
    if style in {"market_tickers", "tickers", "legacy"}:
        return "legacy"
    return "markets"


def _alternate_update_style(value: str | None) -> str:
    return "markets" if _resolve_update_style(value) == "legacy" else "legacy"


def _resolve_update_include_channels(value: bool | None = None) -> bool:
    if value is not None:
        return value
    return _bool_env("KALSHI_WS_UPDATE_INCLUDE_CHANNELS", False)


def _extract_subscription_id_field(message: dict) -> str | None:
    """Extract which subscription id field is present in a WS payload."""
    msg = message.get("msg")
    params = message.get("params")
    msg_params = msg.get("params") if isinstance(msg, dict) else None
    for field_name in _SUBSCRIPTION_ID_FIELDS:
        if _coerce_int(message.get(field_name)) is not None:
            return field_name
    if isinstance(msg, dict):
        for field_name in _SUBSCRIPTION_ID_FIELDS:
            if _coerce_int(msg.get(field_name)) is not None:
                return field_name
    if isinstance(params, dict):
        for field_name in _SUBSCRIPTION_ID_FIELDS:
            if _coerce_int(params.get(field_name)) is not None:
                return field_name
    if isinstance(msg_params, dict):
        for field_name in _SUBSCRIPTION_ID_FIELDS:
            if _coerce_int(msg_params.get(field_name)) is not None:
                return field_name
    return None


async def _record_subscription_id_field(
    state: SubscriptionState,
    field_name: str | None,
) -> None:
    """Record the field name used for subscription ids."""
    field_name = _normalize_subscription_id_field(field_name)
    if field_name is None:
        return
    async with state.lock:
        if state.update_state.sid_field != field_name:
            if state.update_state.sid_field is not None:
                logger.debug(
                    "WS subscription id field changed: %s -> %s",
                    state.update_state.sid_field,
                    field_name,
                )
            else:
                logger.debug("WS subscription id field set: %s", field_name)
            state.update_state.sid_field = field_name


async def _resolve_update_sid_field(
    state: SubscriptionState | None,
    subscription_id_field: str | None = None,
) -> str:
    sid_field = _normalize_subscription_id_field(subscription_id_field)
    if sid_field is not None:
        return sid_field
    if state is not None:
        async with state.lock:
            if state.update_state.sid_field is not None:
                return state.update_state.sid_field
    env_field = _resolve_subscription_id_field_env()
    if env_field is not None:
        return env_field
    return "sid"


def _fallback_sid_field(current: str | None) -> str | None:
    for sid_field in ("subscription_id", "subscriptionId", "sid"):
        if sid_field != current:
            return sid_field
    return None


def _build_update_message(
    request: UpdateMessageRequest | int,
    channels: Iterable[str] | None = None,
    add_tickers: list[str] | None = None,
    remove_tickers: list[str] | None = None,
    *,
    subscription_id: int | None = None,
    subscription_id_field: str | None = None,
    update_style: str | None = None,
    include_channels: bool | None = None,
) -> dict:
    """Build a subscription update request for the WS API."""
    if not isinstance(request, UpdateMessageRequest):
        request = UpdateMessageRequest(
            request_id=int(request),
            channels=tuple(channels or ()),
            add_tickers=list(add_tickers or []),
            remove_tickers=list(remove_tickers or []),
            options=UpdateMessageOptions(
                subscription_id=subscription_id,
                subscription_id_field=subscription_id_field,
                update_style=update_style,
                include_channels=include_channels,
            ),
        )
    options = request.options
    update_style = _resolve_update_style(options.update_style)
    include_channels = _resolve_update_include_channels(options.include_channels)
    params: dict[str, Any] = {}
    if update_style == "legacy":
        market_tickers: dict[str, list[str]] = {}
        if request.add_tickers:
            market_tickers["add"] = request.add_tickers
        if request.remove_tickers:
            market_tickers["remove"] = request.remove_tickers
        params["market_tickers"] = market_tickers
    else:
        if request.add_tickers:
            params["add_markets"] = request.add_tickers
        if request.remove_tickers:
            params["delete_markets"] = request.remove_tickers
    if include_channels:
        params["channels"] = list(request.channels)
    payload = {
        "id": request.request_id,
        "cmd": "update_subscription",
        "params": params,
    }
    if options.subscription_id is not None:
        sid_field = _normalize_subscription_id_field(options.subscription_id_field)
        if sid_field is None:
            sid_field = _resolve_subscription_id_field_env() or "sid"
        params[sid_field] = options.subscription_id
    return payload


def _extract_error_code(message: dict) -> int | None:
    """Extract an error code from a WS error payload."""
    msg = message.get("msg")
    if not isinstance(msg, dict):
        return None
    return _coerce_int(msg.get("code"))


def _extract_subscription_ids(message: dict) -> tuple[int, int] | None:
    """Extract request/subscription IDs from a WS response payload."""
    request_id = _coerce_int(message.get("id"))
    if request_id is None:
        return None
    msg = message.get("msg")
    if not isinstance(msg, dict):
        msg = {}
    params = message.get("params")
    if not isinstance(params, dict):
        params = {}
    msg_params = msg.get("params")
    if not isinstance(msg_params, dict):
        msg_params = {}
    for payload in (message, msg, params, msg_params):
        for field_name in _SUBSCRIPTION_ID_FIELDS:
            sid = _coerce_int(payload.get(field_name))
            if sid is not None:
                return request_id, sid
    return None


async def _pop_pending_update(
    state: SubscriptionState,
    request_id: int,
) -> PendingUpdate | None:
    """Remove and return a pending update_subscription entry."""
    async with state.lock:
        return state.pending_updates.pop(request_id, None)


async def _resolve_single_pending_update_id(
    state: SubscriptionState,
) -> int | None:
    """Return the only pending update request id when exactly one exists."""
    async with state.lock:
        if len(state.pending_updates) == 1:
            return next(iter(state.pending_updates.keys()))
    return None


async def _drain_pending_updates(
    state: SubscriptionState,
) -> list[PendingUpdate]:
    """Drain all pending update entries."""
    async with state.lock:
        pending = list(state.pending_updates.values())
        state.pending_updates.clear()
    return pending


async def _register_subscription_sid(
    state: SubscriptionState,
    request_id: int,
    sid: int,
) -> int:
    """Record the sid assigned for a subscribe call."""
    async with state.lock:
        tickers = state.pending_subscriptions.pop(request_id, None)
        if tickers is None:
            state.sid_tickers.setdefault(sid, set())
            return 0
        state.sid_tickers.setdefault(sid, set()).update(tickers)
        return len(tickers)


def _pending_update_batches(pending: PendingUpdate) -> tuple[list[str], list[str]]:
    add_tickers = list(pending.tickers) if pending.action == "add" else []
    remove_tickers = list(pending.tickers) if pending.action == "remove" else []
    return add_tickers, remove_tickers


def _build_update_request_from_pending(
    *,
    request_id: int,
    channels: Iterable[str],
    pending: PendingUpdate,
    sid_field: str | None,
    update_style: str | None,
    include_channels: bool | None,
) -> UpdateMessageRequest:
    add_tickers, remove_tickers = _pending_update_batches(pending)
    return UpdateMessageRequest(
        request_id=request_id,
        channels=channels,
        add_tickers=add_tickers,
        remove_tickers=remove_tickers,
        options=UpdateMessageOptions(
            subscription_id=pending.sid,
            subscription_id_field=sid_field,
            update_style=update_style,
            include_channels=include_channels,
        ),
    )


async def _disable_update_subscription(
    state: SubscriptionState,
    message: str,
    *args: Any,
) -> None:
    async with state.lock:
        if not state.update_state.update_disabled:
            logger.warning(message, *args)
        state.update_state.update_disabled = True


async def _restore_removed_updates(
    state: SubscriptionState,
    pending_updates: Iterable[PendingUpdate],
) -> None:
    async with state.lock:
        for pending in pending_updates:
            if pending.action != "remove":
                continue
            state.subscribed.update(pending.tickers)
            if pending.sid is not None:
                state.sid_tickers.setdefault(pending.sid, set()).update(pending.tickers)


def _collect_pending_tickers(pending_updates: Iterable[PendingUpdate]) -> set[str]:
    tickers: set[str] = set()
    for pending in pending_updates:
        tickers.update(pending.tickers)
    return tickers


async def _resolve_update_request_id(
    websocket,
    state: SubscriptionState,
    config: SubscriptionConfig,
    request_id: int | None,
    error_code: int | None,
) -> int | None:
    if request_id is not None:
        return request_id
    resolved = await _resolve_single_pending_update_id(state)
    if resolved is not None:
        return resolved
    pending_updates = await _drain_pending_updates(state)
    if not pending_updates:
        return None
    await _disable_update_subscription(
        state,
        "WS update_subscription disabled after error code=%s (missing id)",
        error_code,
    )
    await _restore_removed_updates(state, pending_updates)
    tickers = _collect_pending_tickers(pending_updates)
    if not tickers:
        return None
    logger.warning(
        "WS update_subscription error code=%s missing id; re-subscribing %s tickers",
        error_code,
        len(tickers),
    )
    await _send_subscribe_batches(
        websocket,
        config.channels,
        config.ws_batch_size,
        sorted(tickers),
        state,
    )
    return None


async def _send_update_retry(
    websocket,
    state: SubscriptionState,
    config: SubscriptionConfig,
    pending: PendingUpdate,
    *,
    sid_field: str | None,
    update_style: str | None,
    include_channels: bool | None,
) -> None:
    req_id = next(state.request_id)
    msg = _build_update_message(
        _build_update_request_from_pending(
            request_id=req_id,
            channels=config.channels,
            pending=pending,
            sid_field=sid_field,
            update_style=update_style,
            include_channels=include_channels,
        )
    )
    logger.debug("WS update_subscription retry payload: %s", msg)
    await websocket.send(json.dumps(msg))
    async with state.lock:
        state.pending_updates[req_id] = PendingUpdate(
            action=pending.action,
            sid=pending.sid,
            tickers=pending.tickers,
            sid_field=sid_field,
            update_style=update_style,
            include_channels=include_channels,
            attempts=pending.attempts + 1,
        )


async def _retry_update_with_sid_field(context: UpdateErrorContext) -> bool:
    pending = context.pending
    error_code = context.error_code
    if (
        error_code != 12
        or pending.sid is None
        or pending.attempts >= 2
        or _resolve_subscription_id_field_env() is not None
    ):
        return False
    sid_field = _fallback_sid_field(pending.sid_field)
    if sid_field is None:
        return False
    logger.warning(
        "WS update_subscription error code=%s id=%s; retrying with sid_field=%s",
        error_code,
        context.request_id,
        sid_field,
    )
    await _send_update_retry(
        context.websocket,
        context.state,
        context.config,
        pending,
        sid_field=sid_field,
        update_style=pending.update_style,
        include_channels=pending.include_channels,
    )
    return True


async def _retry_update_with_alt_style(context: UpdateErrorContext) -> bool:
    pending = context.pending
    error_code = context.error_code
    if error_code not in {1, 15} or pending.sid is None or pending.attempts >= 1:
        return False
    alt_style = _alternate_update_style(pending.update_style)
    include_channels = pending.include_channels
    if include_channels is None:
        include_channels = _resolve_update_include_channels()
    if not include_channels:
        include_channels = True
    sid_field = pending.sid_field or _resolve_subscription_id_field_env()
    logger.warning(
        "WS update_subscription error code=%s id=%s; retrying with update_style=%s "
        "include_channels=%s sid_field=%s action=%s tickers=%s",
        error_code,
        context.request_id,
        alt_style,
        include_channels,
        sid_field,
        pending.action,
        len(pending.tickers),
    )
    await _send_update_retry(
        context.websocket,
        context.state,
        context.config,
        pending,
        sid_field=sid_field,
        update_style=alt_style,
        include_channels=include_channels,
    )
    return True


async def _handle_update_fallback(context: UpdateErrorContext) -> None:
    pending = context.pending
    error_code = context.error_code
    if error_code in {1, 15} and pending.attempts >= 1:
        await _disable_update_subscription(
            context.state,
            "WS update_subscription disabled after error code=%s id=%s attempts=%s",
            error_code,
            context.request_id,
            pending.attempts,
        )
    if pending.action == "remove":
        await _restore_removed_updates(context.state, [pending])
    if not pending.tickers:
        return
    logger.warning(
        "WS update_subscription error code=%s id=%s action=%s; re-subscribing %s tickers",
        error_code,
        context.request_id,
        pending.action,
        len(pending.tickers),
    )
    await _send_subscribe_batches(
        context.websocket,
        context.config.channels,
        context.config.ws_batch_size,
        list(pending.tickers),
        context.state,
    )


async def _handle_update_error(
    websocket,
    state: SubscriptionState,
    config: SubscriptionConfig,
    request_id: int | None,
    error_code: int | None,
) -> None:
    """Fallback to re-subscribe when update_subscription errors occur."""
    if error_code not in {1, 12, 15}:
        return
    request_id = await _resolve_update_request_id(
        websocket,
        state,
        config,
        request_id,
        error_code,
    )
    if request_id is None:
        return
    pending = await _pop_pending_update(state, request_id)
    if pending is None:
        return
    context = UpdateErrorContext(
        websocket=websocket,
        state=state,
        config=config,
        pending=pending,
        request_id=request_id,
        error_code=error_code,
    )
    if await _retry_update_with_sid_field(context):
        return
    if await _retry_update_with_alt_style(context):
        return
    await _handle_update_fallback(context)


async def _record_subscription_ticker(
    state: SubscriptionState,
    sid: int,
    ticker: str | None,
) -> None:
    """Track ticker membership for a subscription sid."""
    if not ticker:
        return
    async with state.lock:
        state.sid_tickers.setdefault(sid, set()).add(ticker)


async def _send_subscribe_batches(
    websocket,
    channels: Iterable[str],
    batch_size: int,
    tickers: list[str],
    state: SubscriptionState,
) -> None:
    for batch in _chunked(tickers, batch_size):
        req_id = next(state.request_id)
        msg = _build_subscribe_message(req_id, channels, batch)
        logger.debug(
            "WS subscribe: id=%s tickers=%s channels=%s",
            req_id,
            len(batch),
            ",".join(channels),
        )
        async with state.lock:
            state.subscribed.update(batch)
            state.pending_subscriptions[req_id] = set(batch)
        await websocket.send(json.dumps(msg))


async def _subscribe_initial(
    websocket,
    conn,
    config: WsLoopConfig,
    state: SubscriptionState,
) -> None:
    limit = _resolve_active_limit(config.runtime.max_active_tickers)
    active = _load_active_tickers(conn, limit, config)
    if active is None:
        logger.warning("load_active_tickers failed during subscribe")
    if not active:
        fallback = _load_market_tickers(conn, limit, config)
        if fallback is None:
            logger.warning("load_market_tickers failed during subscribe fallback")
        else:
            if fallback:
                logger.warning(
                    "WS active_markets empty; falling back to markets tickers=%d",
                    len(fallback),
                )
                _seed_active_markets(conn, fallback)
            active = fallback
    if active is None:
        active = []
    await _send_subscribe_batches(
        websocket,
        config.channels,
        config.runtime.ws_batch_size,
        active,
        state,
    )


async def _build_subscription_context(
    websocket,
    context: WsSessionContext,
) -> SubscriptionContext:
    lock = asyncio.Lock()
    request_id = itertools.count(1)
    state = SubscriptionState(
        subscribed=set(),
        lock=lock,
        request_id=request_id,
    )
    await _subscribe_initial(
        websocket,
        context.conn,
        context.config,
        state,
    )
    return SubscriptionContext(
        conn=context.conn,
        config=SubscriptionConfig(
            channels=context.config.channels,
            max_active_tickers=context.config.runtime.max_active_tickers,
            shard=context.config.shard,
            ws_batch_size=context.config.runtime.ws_batch_size,
        ),
        state=state,
    )


async def _load_active_ticker_set(context: SubscriptionContext) -> set[str] | None:
    limit = _resolve_active_limit(context.config.max_active_tickers)
    active = _load_active_tickers(context.conn, limit, context.config)
    if active is None:
        return None
    if not active:
        fallback = _load_market_tickers(context.conn, limit, context.config)
        if fallback is None:
            return None
        if fallback:
            logger.warning(
                "WS active_markets empty; falling back to markets tickers=%d",
                len(fallback),
            )
            _seed_active_markets(context.conn, fallback)
        active = fallback
    return set(active)


async def _snapshot_subscription_state(
    state: SubscriptionState,
) -> tuple[set[str], dict[int, set[str]], set[str]]:
    async with state.lock:
        subscribed = set(state.subscribed)
        sid_tickers = {sid: set(tickers) for sid, tickers in state.sid_tickers.items()}
        pending_tickers: set[str] = set()
        for tickers in state.pending_subscriptions.values():
            pending_tickers.update(tickers)
    return subscribed, sid_tickers, pending_tickers


def _compute_subscription_delta(
    active_set: set[str],
    subscribed: set[str],
    pending_tickers: set[str],
) -> tuple[list[str], list[str]]:
    to_add = sorted(active_set - subscribed)
    to_remove = sorted((subscribed - active_set) - pending_tickers)
    return to_add, to_remove


async def _apply_subscription_additions(
    websocket,
    context: SubscriptionContext,
    to_add: list[str],
    sid_tickers: dict[int, set[str]],
) -> None:
    if not sid_tickers or context.state.update_state.update_disabled:
        await _send_subscribe_batches(
            websocket,
            context.config.channels,
            context.config.ws_batch_size,
            to_add,
            context.state,
        )
        return
    sid_field = await _resolve_update_sid_field(context.state)
    update_style = _resolve_update_style()
    include_channels = _resolve_update_include_channels()
    sid_cycle = itertools.cycle(sorted(sid_tickers.keys()))
    for batch in _chunked(to_add, context.config.ws_batch_size):
        sid = next(sid_cycle)
        req_id = next(context.state.request_id)
        msg = _build_update_message(
            UpdateMessageRequest(
                request_id=req_id,
                channels=context.config.channels,
                add_tickers=batch,
                remove_tickers=[],
                options=UpdateMessageOptions(
                    subscription_id=sid,
                    subscription_id_field=sid_field,
                    update_style=update_style,
                    include_channels=include_channels,
                ),
            )
        )
        logger.debug("WS update_subscription add payload: %s", msg)
        logger.debug(
            "WS update_subscription add: id=%s sid=%s tickers=%s",
            req_id,
            sid,
            len(batch),
        )
        await websocket.send(json.dumps(msg))
        async with context.state.lock:
            context.state.subscribed.update(batch)
            context.state.sid_tickers.setdefault(sid, set()).update(batch)
            context.state.pending_updates[req_id] = PendingUpdate(
                action="add",
                sid=sid,
                tickers=tuple(batch),
                sid_field=sid_field,
                update_style=update_style,
                include_channels=include_channels,
            )


def _build_remove_batches(
    to_remove: list[str],
    sid_tickers: dict[int, set[str]],
) -> tuple[dict[int, list[str]], int]:
    ticker_to_sid: dict[str, int] = {}
    for sid, tickers in sid_tickers.items():
        for ticker in tickers:
            ticker_to_sid.setdefault(ticker, sid)
    remove_by_sid: dict[int, list[str]] = {}
    skipped = 0
    for ticker in to_remove:
        sid = ticker_to_sid.get(ticker)
        if sid is None:
            skipped += 1
            continue
        remove_by_sid.setdefault(sid, []).append(ticker)
    return remove_by_sid, skipped


async def _apply_subscription_removals(
    websocket,
    context: SubscriptionContext,
    to_remove: list[str],
    sid_tickers: dict[int, set[str]],
) -> None:
    if context.state.update_state.update_disabled:
        async with context.state.lock:
            context.state.subscribed.difference_update(to_remove)
            for sid, tickers in list(context.state.sid_tickers.items()):
                tickers.difference_update(to_remove)
                if not tickers:
                    context.state.sid_tickers.pop(sid, None)
        logger.debug(
            "WS update_subscription disabled; dropping %s tickers locally",
            len(to_remove),
        )
        return
    remove_by_sid, skipped = _build_remove_batches(to_remove, sid_tickers)
    if skipped:
        logger.debug(
            "WS update_subscription skip remove (missing sid) tickers=%s",
            skipped,
        )
    if remove_by_sid:
        sid_field = await _resolve_update_sid_field(context.state)
        update_style = _resolve_update_style()
        include_channels = _resolve_update_include_channels()
    for sid, tickers in remove_by_sid.items():
        for batch in _chunked(tickers, context.config.ws_batch_size):
            req_id = next(context.state.request_id)
            msg = _build_update_message(
                UpdateMessageRequest(
                    request_id=req_id,
                    channels=context.config.channels,
                    add_tickers=[],
                    remove_tickers=batch,
                    options=UpdateMessageOptions(
                        subscription_id=sid,
                        subscription_id_field=sid_field,
                        update_style=update_style,
                        include_channels=include_channels,
                    ),
                )
            )
            logger.debug("WS update_subscription remove payload: %s", msg)
            logger.debug(
                "WS update_subscription remove: id=%s sid=%s tickers=%s",
                req_id,
                sid,
                len(batch),
            )
            await websocket.send(json.dumps(msg))
            async with context.state.lock:
                context.state.subscribed.difference_update(batch)
                sid_bucket = context.state.sid_tickers.get(sid)
                if sid_bucket is not None:
                    sid_bucket.difference_update(batch)
                    if not sid_bucket:
                        context.state.sid_tickers.pop(sid, None)
                context.state.pending_updates[req_id] = PendingUpdate(
                    action="remove",
                    sid=sid,
                    tickers=tuple(batch),
                    sid_field=sid_field,
                    update_style=update_style,
                    include_channels=include_channels,
                )


async def _refresh_subscriptions(
    websocket,
    context: SubscriptionContext,
    refresh_seconds: int,
    wake_event: asyncio.Event | None = None,
) -> None:
    """Periodically refresh WS subscriptions."""
    while True:
        if wake_event is None:
            await asyncio.sleep(refresh_seconds)
        else:
            try:
                await asyncio.wait_for(wake_event.wait(), timeout=refresh_seconds)
                wake_event.clear()
            except asyncio.TimeoutError:
                pass
        active_set = await _load_active_ticker_set(context)
        if active_set is None:
            continue
        subscribed, sid_tickers, pending_tickers = await _snapshot_subscription_state(
            context.state
        )
        to_add, to_remove = _compute_subscription_delta(
            active_set,
            subscribed,
            pending_tickers,
        )
        if not to_add and not to_remove:
            continue
        if to_add:
            await _apply_subscription_additions(websocket, context, to_add, sid_tickers)
        if to_remove and sid_tickers:
            await _apply_subscription_removals(websocket, context, to_remove, sid_tickers)
