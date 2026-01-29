"""Configuration helpers for WS ingestion."""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urlparse

from ...core.env_utils import _env_float_fallback as _env_float
from ...core.env_utils import _env_int_fallback as _env_int
from .ws_ingest_db_utils import _psycopg_error_type
from .ws_ingest_models import (
    FailureConfig,
    ShardConfig,
    WriterConfig,
    WsLoopConfig,
    WsLoopOptions,
    WsRuntimeConfig,
)

logger = logging.getLogger(__name__)

_DEFAULT_WS_PATH = "/trade-api/ws/v2"
_DEFAULT_WS_CHANNELS = ("ticker", "market_lifecycle_v2")


def _bool_env(name: str, default: bool = False) -> bool:
    """Read a boolean environment variable.

    :param name: Environment variable name.
    :type name: str
    :param default: Default value when unset.
    :type default: bool
    :return: Parsed boolean value.
    :rtype: bool
    """
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _parse_csv(raw: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def _resolve_ws_url(kalshi_host: str | None) -> str:
    """Resolve the WebSocket URL from env/host configuration."""
    override = os.getenv("KALSHI_WS_URL", "").strip()
    if override:
        return override

    host = kalshi_host or os.getenv(
        "KALSHI_HOST",
        "https://api.elections.kalshi.com/trade-api/v2",
    )
    parsed = urlparse(host)
    if not parsed.scheme:
        scheme = "wss"
    else:
        scheme = "wss" if parsed.scheme in {"https", "wss"} else "ws"
    netloc = parsed.netloc
    path = parsed.path or ""
    if not netloc:
        # Handle hosts provided without scheme (treat first path segment as netloc).
        parts = (parsed.path or "").split("/", 1)
        netloc = parts[0]
        path = f"/{parts[1]}" if len(parts) > 1 else ""
    if path.endswith("/trade-api/v2"):
        path = path[: -len("/trade-api/v2")]
    ws_path = f"{path}{_DEFAULT_WS_PATH}"
    return f"{scheme}://{netloc}{ws_path}"


def _resolve_ws_loop_options(
    options: WsLoopOptions | None,
    **kwargs: Any,
) -> WsLoopOptions:
    base = options or WsLoopOptions()
    return WsLoopOptions(
        max_active_tickers=kwargs.get("max_active_tickers", base.max_active_tickers),
        ws_batch_size=kwargs.get("ws_batch_size", base.ws_batch_size),
        refresh_seconds=kwargs.get("refresh_seconds", base.refresh_seconds),
        kalshi_host=kwargs.get("kalshi_host", base.kalshi_host),
        database_url=kwargs.get("database_url", base.database_url),
    )


def _resolve_ws_channels() -> tuple[str, ...]:
    channels = _parse_csv(
        os.getenv("KALSHI_WS_CHANNELS", ",".join(_DEFAULT_WS_CHANNELS))
    )
    if not channels:
        return _DEFAULT_WS_CHANNELS
    return channels


def _resolve_writer_dsn(conn, database_url: str | None) -> str:
    writer_dsn = database_url
    if not writer_dsn:
        try:
            writer_dsn = conn.info.dsn
        except (AttributeError, _psycopg_error_type(), RuntimeError):
            writer_dsn = None
    if not writer_dsn:
        raise ValueError("WS writer requires database_url or a connection with DSN info")
    return writer_dsn


def _resolve_dedup_fields() -> tuple[str, ...]:
    default_dedup_fields = (
        "price",
        "yes_bid",
        "yes_ask",
        "volume",
        "open_interest",
        "dollar_volume",
        "dollar_open_interest",
        "implied_yes_mid",
    )
    raw_fields = os.getenv("WS_TICK_DEDUP_FIELDS", ",".join(default_dedup_fields))
    dedup_fields = _parse_csv(raw_fields)
    return dedup_fields or default_dedup_fields


def _resolve_writer_config(conn, options: WsLoopOptions) -> WriterConfig:
    tick_batch_size = _env_int("WS_DB_BATCH_SIZE", 200, minimum=1)
    lifecycle_batch_size = _env_int(
        "WS_DB_LIFECYCLE_BATCH_SIZE",
        tick_batch_size,
        minimum=1,
    )
    flush_seconds = _env_float("WS_DB_FLUSH_SECONDS", 1.0, minimum=0.1)
    dedup_enabled = _bool_env("WS_TICK_DEDUP_ENABLE", True)
    dedup_max_age_seconds = _env_float(
        "WS_TICK_DEDUP_MAX_AGE_SECONDS",
        1.0,
        minimum=0.0,
    )
    return WriterConfig(
        database_url=_resolve_writer_dsn(conn, options.database_url),
        tick_batch_size=tick_batch_size,
        lifecycle_batch_size=lifecycle_batch_size,
        flush_seconds=flush_seconds,
        dedup_enabled=dedup_enabled,
        dedup_max_age_seconds=dedup_max_age_seconds,
        dedup_fields=_resolve_dedup_fields(),
    )


def _resolve_runtime_config(options: WsLoopOptions) -> WsRuntimeConfig:
    queue_maxsize = _env_int("WS_QUEUE_MAXSIZE", 20000, minimum=100)
    ws_max_queue = _env_int("WS_MAX_QUEUE", 2048, minimum=1)
    ws_max_size_mb = _env_int("WS_MAX_SIZE_MB", 4, minimum=1)
    return WsRuntimeConfig(
        max_active_tickers=options.max_active_tickers,
        ws_batch_size=options.ws_batch_size,
        refresh_seconds=options.refresh_seconds,
        queue_maxsize=queue_maxsize,
        ws_max_queue=ws_max_queue,
        ws_max_size=ws_max_size_mb * 1024 * 1024,
    )


def _resolve_shard_key() -> str:
    raw = (os.getenv("WS_SHARD_KEY") or "event").strip().lower()
    if raw in {"event", "event_ticker", "event-ticker"}:
        return "event"
    if raw in {"market", "market_ticker", "market-ticker", "ticker"}:
        return "market"
    raise ValueError("WS_SHARD_KEY must be one of: event, market")


def _resolve_shard_config() -> ShardConfig:
    shard_count = _env_int("WS_SHARD_COUNT", 1, minimum=1)
    shard_id = _env_int("WS_SHARD_ID", 0, minimum=0)
    round_robin = _bool_env("WS_ACTIVE_ROUND_ROBIN", True)
    round_robin_step = _env_int("WS_ACTIVE_ROUND_ROBIN_STEP", 0, minimum=0)
    shard_key = _resolve_shard_key()
    if shard_id >= shard_count:
        raise ValueError(f"WS_SHARD_ID={shard_id} must be < WS_SHARD_COUNT={shard_count}")
    if shard_count > 1:
        logger.info("WS shard enabled: shard=%s/%s", shard_id + 1, shard_count)
    return ShardConfig(
        count=shard_count,
        shard_id=shard_id,
        round_robin=round_robin,
        round_robin_step=round_robin_step,
        key=shard_key,
    )


def _resolve_failure_config() -> FailureConfig:
    return FailureConfig(
        threshold=_env_int("WS_FAILURE_THRESHOLD", 5, minimum=1),
        cooldown=_env_float("WS_FAILURE_COOLDOWN_SECONDS", 60.0, minimum=1.0),
    )


def _build_ws_loop_config(
    conn,
    options: WsLoopOptions,
) -> WsLoopConfig:
    return WsLoopConfig(
        ws_url=_resolve_ws_url(options.kalshi_host),
        channels=_resolve_ws_channels(),
        runtime=_resolve_runtime_config(options),
        shard=_resolve_shard_config(),
        writer=_resolve_writer_config(conn, options),
        failure=_resolve_failure_config(),
    )
