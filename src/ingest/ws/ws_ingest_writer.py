"""DB writer helpers for WS ingestion."""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any

from ...db.db import (
    delete_active_market,
    insert_lifecycle_events,
    insert_market_ticks,
    maybe_upsert_active_market_from_market,
    upsert_market,
)
from ...core.loop_utils import log_metric as _log_metric
from .ws_ingest_db_utils import (
    _psycopg_error_type,
    _psycopg_privilege_error_type,
    _require_psycopg,
    _safe_rollback,
)
from .ws_ingest_models import WriterConfig, WriterStatus
from .ws_ingest_utils import _extract_market_id, _resolve_market_ticker

logger = logging.getLogger(__name__)

_DB_WORK_STOP = object()
_DB_WORK_TICK = "tick"
_DB_WORK_LIFECYCLE = "lifecycle"


@dataclass
class _QueueDropStats:
    total: int = 0
    window: int = 0
    last_log: float = 0.0


_DROP_STATS: dict[str, _QueueDropStats] = {}


def _drop_log_interval() -> float:
    raw = os.getenv("WS_QUEUE_DROP_METRIC_SECONDS", "60")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = 60.0
    return max(0.0, value)


def _record_queue_drop(kind: str, work_queue: queue.Queue) -> None:
    now = time.monotonic()
    stats = _DROP_STATS.setdefault(kind, _QueueDropStats())
    stats.total += 1
    stats.window += 1
    interval = _drop_log_interval()
    if interval <= 0 or now - stats.last_log >= interval:
        _log_metric(
            logger,
            "ws.queue_drop",
            kind=kind,
            drops=stats.window,
            drops_total=stats.total,
            queue_size=work_queue.qsize(),
            queue_max=work_queue.maxsize,
        )
        stats.window = 0
        stats.last_log = now

@dataclass(frozen=True)
class _BatchConfig:
    tick_batch_size: int
    lifecycle_batch_size: int
    flush_seconds: float


class _DbBatcher:
    def __init__(
        self,
        conn,
        tick_batch_size: int,
        lifecycle_batch_size: int,
        flush_seconds: float,
    ) -> None:
        self.conn = conn
        self.config = _BatchConfig(
            tick_batch_size=max(1, tick_batch_size),
            lifecycle_batch_size=max(1, lifecycle_batch_size),
            flush_seconds=max(0.1, flush_seconds),
        )
        self._buffers: dict[str, list[dict]] = {
            _DB_WORK_TICK: [],
            _DB_WORK_LIFECYCLE: [],
        }
        self._last_error: Exception | None = None
        now = time.monotonic()
        self._last_flush = {
            _DB_WORK_TICK: now,
            _DB_WORK_LIFECYCLE: now,
        }

    @property
    def _tick_buffer(self) -> list[dict]:
        return self._buffers[_DB_WORK_TICK]

    @_tick_buffer.setter
    def _tick_buffer(self, value: list[dict]) -> None:
        self._buffers[_DB_WORK_TICK] = value

    @property
    def _lifecycle_buffer(self) -> list[dict]:
        return self._buffers[_DB_WORK_LIFECYCLE]

    @_lifecycle_buffer.setter
    def _lifecycle_buffer(self, value: list[dict]) -> None:
        self._buffers[_DB_WORK_LIFECYCLE] = value

    @property
    def _last_tick_flush(self) -> float:
        return self._last_flush[_DB_WORK_TICK]

    @_last_tick_flush.setter
    def _last_tick_flush(self, value: float) -> None:
        self._last_flush[_DB_WORK_TICK] = value

    @property
    def _last_lifecycle_flush(self) -> float:
        return self._last_flush[_DB_WORK_LIFECYCLE]

    @_last_lifecycle_flush.setter
    def _last_lifecycle_flush(self, value: float) -> None:
        self._last_flush[_DB_WORK_LIFECYCLE] = value

    def add_tick(self, tick: dict) -> None:
        """Buffer a tick and flush if the batch is full."""
        self._buffers[_DB_WORK_TICK].append(tick)
        if len(self._buffers[_DB_WORK_TICK]) >= self.config.tick_batch_size:
            self.flush_ticks(force=True)

    def add_lifecycle(self, lifecycle: dict) -> None:
        """Buffer a lifecycle event and flush if the batch is full."""
        self._buffers[_DB_WORK_LIFECYCLE].append(lifecycle)
        if len(self._buffers[_DB_WORK_LIFECYCLE]) >= self.config.lifecycle_batch_size:
            self.flush_lifecycles(force=True)

    def _drain_buffer(self, kind: str) -> list[dict]:
        if not self._buffers[kind]:
            return []
        drained = self._buffers[kind]
        self._buffers[kind] = []
        return drained

    def _restore_buffer(self, kind: str, items: list[dict]) -> None:
        if not items:
            return
        self._buffers[kind] = items + self._buffers[kind]

    def _drain_ticks(self) -> list[dict]:
        return self._drain_buffer(_DB_WORK_TICK)

    def _restore_ticks(self, ticks: list[dict]) -> None:
        self._restore_buffer(_DB_WORK_TICK, ticks)

    def _drain_lifecycles(self) -> list[dict]:
        return self._drain_buffer(_DB_WORK_LIFECYCLE)

    def _restore_lifecycles(self, lifecycles: list[dict]) -> None:
        self._restore_buffer(_DB_WORK_LIFECYCLE, lifecycles)

    def _record_error(self, exc: Exception) -> None:
        self._last_error = exc

    def pop_error(self) -> Exception | None:
        """Return and clear the last DB batcher error."""
        err = self._last_error
        self._last_error = None
        return err

    def flush_ticks(self, force: bool = False) -> None:
        """Flush tick batches based on time or when forced."""
        if not force:
            now = time.monotonic()
            if now - self._last_flush[_DB_WORK_TICK] < self.config.flush_seconds:
                return
        ticks = self._drain_ticks()
        if not ticks:
            return
        try:
            insert_market_ticks(self.conn, ticks)
            self._last_flush[_DB_WORK_TICK] = time.monotonic()
        except (_psycopg_error_type(), ValueError, TypeError, RuntimeError) as exc:
            logger.exception("batch insert_market_ticks failed (count=%s)", len(ticks))
            _safe_rollback(self.conn)
            self._record_error(exc)
            self._restore_ticks(ticks)

    def flush_lifecycles(self, force: bool = False) -> None:
        """Flush lifecycle batches based on time or when forced."""
        if not force:
            now = time.monotonic()
            if now - self._last_flush[_DB_WORK_LIFECYCLE] < self.config.flush_seconds:
                return
        lifecycles = self._drain_lifecycles()
        if not lifecycles:
            return
        try:
            insert_lifecycle_events(self.conn, lifecycles)
            self._last_flush[_DB_WORK_LIFECYCLE] = time.monotonic()
        except (_psycopg_error_type(), ValueError, TypeError, RuntimeError) as exc:
            logger.exception(
                "batch insert_lifecycle_events failed (count=%s)",
                len(lifecycles),
            )
            _safe_rollback(self.conn)
            self._record_error(exc)
            self._restore_lifecycles(lifecycles)

    def flush_due(self) -> None:
        """Flush batches if enough time has elapsed."""
        self.flush_ticks(force=False)
        self.flush_lifecycles(force=False)

    def flush_all(self) -> None:
        """Force flush all buffered items."""
        self.flush_ticks(force=True)
        self.flush_lifecycles(force=True)


class _TickDeduper:
    def __init__(
        self,
        enabled: bool,
        max_age_seconds: float,
        fields: tuple[str, ...],
    ) -> None:
        self.enabled = enabled
        self.max_age_seconds = max(0.0, max_age_seconds)
        self.fields = fields
        self._state: dict[str, tuple[tuple[Any, ...], float]] = {}

    def should_emit(self, tick: dict) -> bool:
        """Return True when a tick differs from recent values."""
        if not self.enabled:
            return True
        ticker = tick.get("ticker")
        if not ticker:
            return True
        signature = tuple(tick.get(field) for field in self.fields)
        now = time.monotonic()
        last = self._state.get(ticker)
        if last is None:
            self._state[ticker] = (signature, now)
            return True
        last_sig, last_ts = last
        if signature != last_sig:
            self._state[ticker] = (signature, now)
            return True
        if self.max_age_seconds and (now - last_ts) >= self.max_age_seconds:
            self._state[ticker] = (signature, now)
            return True
        return False

    def forget(self, ticker: str | None) -> None:
        """Drop cached tick state for a ticker."""
        if not ticker:
            return
        self._state.pop(ticker, None)


def _queue_put_nowait(
    work_queue: queue.Queue,
    item: tuple,
    kind: str,
) -> bool:
    try:
        work_queue.put_nowait(item)
        return True
    except queue.Full:
        logger.warning(
            "WS DB queue full; dropping %s (qsize=%s)",
            kind,
            work_queue.qsize(),
        )
        _record_queue_drop(kind, work_queue)
    return False


def _update_market_id_map(payload: dict, market_id_map: dict[str, str]) -> None:
    """Store market_id -> ticker mappings from lifecycle payloads."""
    market_id = _extract_market_id(payload)
    if not market_id:
        return
    ticker = _resolve_market_ticker(payload, None)
    if not ticker:
        return
    market_id_map[market_id] = ticker


def writer_is_healthy(status: WriterStatus, stale_seconds: float) -> bool:
    """Return True when the writer heartbeat is within the stale window."""
    if stale_seconds <= 0:
        return True
    last = status.last_heartbeat
    if last <= 0:
        return False
    return (time.monotonic() - last) <= stale_seconds


def _touch_status(status: WriterStatus, *, flushed: bool = False) -> None:
    now = time.monotonic()
    status.last_heartbeat = now
    if flushed:
        status.last_flush = now


def _safe_close(conn) -> None:
    if not conn:
        return
    try:
        conn.close()
    except (_psycopg_error_type(), RuntimeError):
        logger.exception("WS DB writer connection close failed")


def _safe_rollback_conn(conn) -> None:
    if not conn:
        return
    _safe_rollback(conn)


def _upsert_market_safe(conn, market: dict) -> None:
    try:
        upsert_market(conn, market)
        maybe_upsert_active_market_from_market(conn, market)
    except (_psycopg_error_type(), RuntimeError):
        logger.exception("upsert_market failed from lifecycle payload")
        _safe_rollback(conn)


def _delete_market_safe(conn, delete_ticker: str) -> None:
    try:
        delete_active_market(conn, delete_ticker)
    except _psycopg_privilege_error_type():
        logger.warning("delete_active_market skipped (permissions) for %s", delete_ticker)
        _safe_rollback(conn)
    except (_psycopg_error_type(), RuntimeError):
        logger.exception("delete_active_market failed for %s", delete_ticker)
        _safe_rollback(conn)


@dataclass(frozen=True)
class LifecycleContext:
    """Context for handling lifecycle events."""

    conn: Any
    batcher: "_DbBatcher"
    deduper: "_TickDeduper"


def _handle_tick_item(
    batcher: "_DbBatcher",
    deduper: "_TickDeduper",
    payload: dict,
) -> None:
    if deduper.should_emit(payload):
        batcher.add_tick(payload)


def _handle_lifecycle_item(
    context: "LifecycleContext",
    payload: dict,
    market: Any,
    delete_ticker: str | None,
) -> None:
    context.batcher.add_lifecycle(payload)
    if isinstance(market, dict):
        _upsert_market_safe(context.conn, market)
    if delete_ticker:
        _delete_market_safe(context.conn, delete_ticker)
        context.deduper.forget(delete_ticker)


def _handle_db_item(
    conn,
    batcher: "_DbBatcher",
    deduper: "_TickDeduper",
    item: tuple,
) -> bool:
    if item is _DB_WORK_STOP:
        return True
    try:
        kind, payload, market, delete_ticker = item
    except (TypeError, ValueError):
        logger.warning("WS DB writer received invalid item: %s", item)
        return False
    if kind == _DB_WORK_TICK:
        _handle_tick_item(batcher, deduper, payload)
    elif kind == _DB_WORK_LIFECYCLE:
        _handle_lifecycle_item(
            LifecycleContext(conn=conn, batcher=batcher, deduper=deduper),
            payload,
            market,
            delete_ticker,
        )
    else:
        logger.warning("WS DB writer received unknown kind=%s", kind)
    return False


@dataclass
class _WriterRuntime:
    conn: Any | None = None
    batcher: _DbBatcher | None = None
    deduper: _TickDeduper | None = None
    backoff: float = 1.0


@dataclass(frozen=True)
class _WriterControl:
    stop_event: threading.Event
    restart_event: threading.Event
    status: WriterStatus


def _reset_writer_connection(runtime: _WriterRuntime) -> None:
    _safe_rollback_conn(runtime.conn)
    _safe_close(runtime.conn)
    runtime.conn = None


def _setup_writer_connection(
    runtime: _WriterRuntime,
    psycopg_module,
    config: WriterConfig,
    status: WriterStatus,
    max_backoff: float,
) -> bool:
    if runtime.conn is not None:
        return True
    try:
        runtime.conn = psycopg_module.connect(config.database_url)
    except (_psycopg_error_type(), OSError, RuntimeError, ValueError) as exc:
        status.last_error = f"connect: {exc}"
        _touch_status(status)
        time.sleep(runtime.backoff)
        runtime.backoff = min(runtime.backoff * 2, max_backoff)
        return False
    if runtime.batcher is None:
        runtime.batcher = _DbBatcher(
            runtime.conn,
            tick_batch_size=config.tick_batch_size,
            lifecycle_batch_size=config.lifecycle_batch_size,
            flush_seconds=config.flush_seconds,
        )
    else:
        runtime.batcher.conn = runtime.conn
    if runtime.deduper is None:
        runtime.deduper = _TickDeduper(
            enabled=config.dedup_enabled,
            max_age_seconds=config.dedup_max_age_seconds,
            fields=config.dedup_fields,
        )
    status.last_error = None
    status.last_connect = time.monotonic()
    status.reconnects += 1
    _touch_status(status)
    runtime.backoff = 1.0
    return True


def _flush_and_check(runtime: _WriterRuntime, status: WriterStatus) -> bool:
    runtime.batcher.flush_due()
    _touch_status(status, flushed=True)
    error = runtime.batcher.pop_error()
    if error:
        status.last_error = f"flush: {error}"
        _reset_writer_connection(runtime)
        return False
    return True


def _handle_queue_item(
    runtime: _WriterRuntime,
    status: WriterStatus,
    item: tuple,
) -> tuple[bool, bool]:
    reconnect = False
    should_stop = False
    try:
        should_stop = _handle_db_item(runtime.conn, runtime.batcher, runtime.deduper, item)
    except _psycopg_error_type() as exc:
        status.last_error = f"db item: {exc}"
        logger.exception("WS DB writer failed to process item: %s", item)
        _safe_rollback_conn(runtime.conn)
        reconnect = True
    except (ValueError, TypeError, RuntimeError) as exc:
        status.last_error = f"item: {exc}"
        logger.exception("WS DB writer failed to process item: %s", item)
    _touch_status(status)
    error = runtime.batcher.pop_error()
    if error:
        status.last_error = f"flush: {error}"
        reconnect = True
    return should_stop, reconnect


def _run_writer_queue(
    runtime: _WriterRuntime,
    work_queue: queue.Queue,
    config: WriterConfig,
    control: _WriterControl,
) -> bool:
    next_flush_at = time.monotonic() + config.flush_seconds
    while not control.stop_event.is_set():
        if control.restart_event.is_set():
            control.restart_event.clear()
            logger.warning("WS DB writer restart requested")
            _reset_writer_connection(runtime)
            return False
        timeout = max(0.0, next_flush_at - time.monotonic())
        try:
            item = work_queue.get(timeout=timeout)
        except queue.Empty:
            if not _flush_and_check(runtime, control.status):
                return False
            next_flush_at = time.monotonic() + config.flush_seconds
            continue
        try:
            should_stop, reconnect = _handle_queue_item(runtime, control.status, item)
        finally:
            work_queue.task_done()
        if should_stop:
            control.stop_event.set()
            return True
        if reconnect:
            _reset_writer_connection(runtime)
            return False
        if time.monotonic() >= next_flush_at:
            if not _flush_and_check(runtime, control.status):
                return False
            next_flush_at = time.monotonic() + config.flush_seconds
    return True


def _db_writer_loop(
    work_queue: queue.Queue,
    config: WriterConfig,
    stop_event: threading.Event,
    restart_event: threading.Event,
    status: WriterStatus,
) -> None:
    """Drain WS work items and write them using a dedicated DB connection."""
    psycopg_module = _require_psycopg()
    runtime = _WriterRuntime()
    control = _WriterControl(
        stop_event=stop_event,
        restart_event=restart_event,
        status=status,
    )
    max_backoff = 30.0
    _touch_status(status)
    while not control.stop_event.is_set():
        try:
            if not _setup_writer_connection(
                runtime,
                psycopg_module,
                config,
                status,
                max_backoff,
            ):
                continue
            if _run_writer_queue(
                runtime,
                work_queue,
                config,
                control,
            ):
                break
        except (_psycopg_error_type(), RuntimeError) as exc:
            status.last_error = f"loop: {exc}"
            logger.exception("WS DB writer loop error; reconnecting")
            _reset_writer_connection(runtime)
            time.sleep(runtime.backoff)
            runtime.backoff = min(runtime.backoff * 2, max_backoff)
        except (OSError, ValueError, TypeError, AttributeError, KeyError) as exc:
            status.last_error = f"fatal: {exc}"
            logger.exception("WS DB writer crashed; retrying")
            _reset_writer_connection(runtime)
            time.sleep(runtime.backoff)
            runtime.backoff = min(runtime.backoff * 2, max_backoff)
    if runtime.batcher is not None:
        try:
            runtime.batcher.flush_all()
        except (_psycopg_error_type(), RuntimeError):
            logger.exception("WS DB writer final flush failed")
    _safe_close(runtime.conn)
