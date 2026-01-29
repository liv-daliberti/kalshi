"""Event iteration helpers for Kalshi SDK."""

from __future__ import annotations

import functools
import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional

from .kalshi_sdk_utils import _extract_items, _filter_kwargs, _to_plain_dict

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EventSdkHooks:
    """Dependencies supplied by kalshi_sdk to avoid cyclic imports."""

    ensure_method_host: Callable[[Callable[..., Any]], None]
    call_with_retries: Callable[..., tuple[bool, Any]]
    load_retry_config: Callable[[str], Any]
    kalshi_error: type[Exception]


def _resolve_events_method(client) -> Optional[Callable[..., Any]]:
    """Resolve an events method from the client.

    :param client: Kalshi API client.
    :type client: Any
    :return: Callable method or None.
    :rtype: collections.abc.Callable[..., Any] | None
    """
    for name in ("get_events", "list_events", "events"):
        if hasattr(client, name):
            return getattr(client, name)
    return None


def _prepare_cursor_kwargs(
    method: Callable[..., Any],
    base_kwargs: dict[str, Any],
    cursor: Optional[str],
) -> dict[str, Any]:
    """Prepare cursor-aware kwargs for a paged request.

    :param method: Paged API method.
    :type method: collections.abc.Callable[..., Any]
    :param base_kwargs: Base keyword arguments.
    :type base_kwargs: dict[str, Any]
    :param cursor: Cursor token to include.
    :type cursor: str | None
    :return: Filtered kwargs including cursor fields.
    :rtype: dict[str, Any]
    """
    call_kwargs = dict(base_kwargs)
    if cursor is not None:
        sig = inspect.signature(method)
        if "cursor" in sig.parameters:
            call_kwargs["cursor"] = cursor
        elif "next_cursor" in sig.parameters:
            call_kwargs["next_cursor"] = cursor
    return _filter_kwargs(method, call_kwargs)


def _iter_events_stream(
    method: Callable[..., Any],
    kwargs: dict[str, Any],
    retry_cfg,
    hooks: EventSdkHooks,
    rate_limit_hook: Optional[Callable[[float], None]] = None,
) -> Iterator[dict]:
    """Yield events using a streaming iter_events method.

    :param method: iter_events-style method.
    :type method: collections.abc.Callable[..., Any]
    :param kwargs: API query parameters.
    :type kwargs: dict[str, Any]
    :param retry_cfg: Retry configuration.
    :type retry_cfg: RetryConfig
    :return: Event iterator.
    :rtype: collections.abc.Iterator[dict]
    """
    hooks.ensure_method_host(method)
    call_kwargs = _filter_kwargs(method, kwargs)
    logger.debug(
        "iter_events: method=%s kwargs=%s",
        getattr(method, "__name__", "iter_events"),
        call_kwargs,
    )
    call = functools.partial(method, **call_kwargs)
    success, response = hooks.call_with_retries(
        call,
        retry_cfg,
        "iter_events",
        rate_limit_hook=rate_limit_hook,
    )
    if not success or response is None:
        return
    for event in response:
        yield _to_plain_dict(event)


def _iter_events_paged(
    method: Callable[..., Any],
    kwargs: dict[str, Any],
    retry_cfg,
    hooks: EventSdkHooks,
    rate_limit_hook: Optional[Callable[[float], None]] = None,
) -> Iterator[dict]:
    """Yield events using a paginated get_events-style method.

    :param method: Paged events method.
    :type method: collections.abc.Callable[..., Any]
    :param kwargs: API query parameters.
    :type kwargs: dict[str, Any]
    :param retry_cfg: Retry configuration.
    :type retry_cfg: RetryConfig
    :return: Event iterator.
    :rtype: collections.abc.Iterator[dict]
    """
    cursor = None
    while True:
        hooks.ensure_method_host(method)
        call_kwargs = _prepare_cursor_kwargs(method, kwargs, cursor)
        logger.debug(
            "iter_events: method=%s kwargs=%s cursor=%s",
            getattr(method, "__name__", "events"),
            call_kwargs,
            cursor,
        )
        call = functools.partial(method, **call_kwargs)
        success, response = hooks.call_with_retries(
            call,
            retry_cfg,
            "iter_events",
            rate_limit_hook=rate_limit_hook,
        )
        if not success:
            return
        events, next_cursor = _extract_items(response)
        logger.debug("iter_events: fetched=%d next_cursor=%s", len(events), next_cursor)
        for event in events:
            yield _to_plain_dict(event)
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor


def iter_events(
    client,
    *,
    rate_limit_hook: Optional[Callable[[float], None]] = None,
    hooks: EventSdkHooks,
    **kwargs,
) -> Iterator[dict]:
    """Yield events from the Kalshi API.

    :param client: Kalshi API client.
    :type client: Any
    :param hooks: SDK dependency hooks.
    :type hooks: EventSdkHooks
    :param kwargs: API query parameters.
    :type kwargs: Any
    :return: Iterable of event payloads.
    :rtype: collections.abc.Iterator[dict]
    :raises KalshiSdkError: If the client lacks an events method.
    """
    retry_cfg = hooks.load_retry_config("KALSHI_EVENTS")
    is_pool = bool(getattr(client, "_kalshi_client_pool", False))
    if hasattr(client, "iter_events") and not is_pool:
        method = getattr(client, "iter_events")
        yield from _iter_events_stream(
            method,
            kwargs,
            retry_cfg,
            hooks,
            rate_limit_hook=rate_limit_hook,
        )
        return

    method = _resolve_events_method(client)
    if method is None:
        raise hooks.kalshi_error("Client does not expose an events method.")
    yield from _iter_events_paged(
        method,
        kwargs,
        retry_cfg,
        hooks,
        rate_limit_hook=rate_limit_hook,
    )
