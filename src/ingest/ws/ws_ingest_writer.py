"""DB writer helpers for WS ingestion."""

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any

from src.db.db import (
    delete_active_market,
    insert_lifecycle_events,
    insert_market_ticks,
    upsert_market,
)
from src.ingest.ws.ws_ingest_db_utils import (
    _psycopg_error_type,
    _psycopg_privilege_error_type,
    _require_psycopg,
    _safe_rollback,
)
from src.ingest.ws.ws_ingest_models import WriterConfig
from src.ingest.ws.ws_ingest_utils import _extract_market_id, _resolve_market_ticker

logger = logging.getLogger(__name__)

_DB_WORK_STOP = object()
_DB_WORK_TICK = "tick"
_DB_WORK_LIFECYCLE = "lifecycle"


class _DbBatcher:
    def __init__(
        self,
        conn,
        tick_batch_size: int,
        lifecycle_batch_size: int,
        flush_seconds: float,
    ) -> None:
        self.conn = conn
        self.tick_batch_size = max(1, tick_batch_size)
        self.lifecycle_batch_size = max(1, lifecycle_batch_size)
        self.flush_seconds = max(0.1, flush_seconds)
        self._tick_buffer: list[dict] = []
        self._lifecycle_buffer: list[dict] = []
        now = time.monotonic()
        self._last_flush = {
            _DB_WORK_TICK: now,
            _DB_WORK_LIFECYCLE: now,
        }

    def add_tick(self, tick: dict) -> None:
        """Buffer a tick and flush if the batch is full."""
        self._tick_buffer.append(tick)
        if len(self._tick_buffer) >= self.tick_batch_size:
            self.flush_ticks(force=True)

    def add_lifecycle(self, lifecycle: dict) -> None:
        """Buffer a lifecycle event and flush if the batch is full."""
        self._lifecycle_buffer.append(lifecycle)
        if len(self._lifecycle_buffer) >= self.lifecycle_batch_size:
            self.flush_lifecycles(force=True)

    def _drain_ticks(self) -> list[dict]:
        if not self._tick_buffer:
            return []
        drained = self._tick_buffer
        self._tick_buffer = []
        return drained

    def _restore_ticks(self, ticks: list[dict]) -> None:
        self._tick_buffer = ticks + self._tick_buffer

    def _drain_lifecycles(self) -> list[dict]:
        if not self._lifecycle_buffer:
            return []
        drained = self._lifecycle_buffer
        self._lifecycle_buffer = []
        return drained

    def _restore_lifecycles(self, lifecycles: list[dict]) -> None:
        self._lifecycle_buffer = lifecycles + self._lifecycle_buffer

    def flush_ticks(self, force: bool = False) -> None:
        """Flush tick batches based on time or when forced."""
        if not force:
            now = time.monotonic()
            if now - self._last_flush[_DB_WORK_TICK] < self.flush_seconds:
                return
        ticks = self._drain_ticks()
        if not ticks:
            return
        try:
            insert_market_ticks(self.conn, ticks)
            self._last_flush[_DB_WORK_TICK] = time.monotonic()
        except (_psycopg_error_type(), ValueError, TypeError, RuntimeError):
            logger.exception("batch insert_market_ticks failed (count=%s)", len(ticks))
            _safe_rollback(self.conn)
            self._restore_ticks(ticks)

    def flush_lifecycles(self, force: bool = False) -> None:
        """Flush lifecycle batches based on time or when forced."""
        if not force:
            now = time.monotonic()
            if now - self._last_flush[_DB_WORK_LIFECYCLE] < self.flush_seconds:
                return
        lifecycles = self._drain_lifecycles()
        if not lifecycles:
            return
        try:
            insert_lifecycle_events(self.conn, lifecycles)
            self._last_flush[_DB_WORK_LIFECYCLE] = time.monotonic()
        except (_psycopg_error_type(), ValueError, TypeError, RuntimeError):
            logger.exception(
                "batch insert_lifecycle_events failed (count=%s)",
                len(lifecycles),
            )
            _safe_rollback(self.conn)
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


def _upsert_market_safe(conn, market: dict) -> None:
    try:
        upsert_market(conn, market)
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


def _db_writer_loop(
    work_queue: queue.Queue,
    config: WriterConfig,
    stop_event: threading.Event,
) -> None:
    """Drain WS work items and write them using a dedicated DB connection."""
    psycopg_module = _require_psycopg()
    conn = psycopg_module.connect(config.database_url)
    batcher = _DbBatcher(
        conn,
        tick_batch_size=config.tick_batch_size,
        lifecycle_batch_size=config.lifecycle_batch_size,
        flush_seconds=config.flush_seconds,
    )
    deduper = _TickDeduper(
        enabled=config.dedup_enabled,
        max_age_seconds=config.dedup_max_age_seconds,
        fields=config.dedup_fields,
    )
    next_flush_at = time.monotonic() + config.flush_seconds
    try:
        while not stop_event.is_set():
            timeout = max(0.0, next_flush_at - time.monotonic())
            try:
                item = work_queue.get(timeout=timeout)
            except queue.Empty:
                batcher.flush_due()
                next_flush_at = time.monotonic() + config.flush_seconds
                continue
            try:
                should_stop = _handle_db_item(conn, batcher, deduper, item)
            except (_psycopg_error_type(), ValueError, TypeError, RuntimeError):
                logger.exception("WS DB writer failed to process item: %s", item)
                should_stop = False
            finally:
                work_queue.task_done()
            if should_stop:
                break
            if time.monotonic() >= next_flush_at:
                batcher.flush_due()
                next_flush_at = time.monotonic() + config.flush_seconds
    except (_psycopg_error_type(), RuntimeError):
        logger.exception("WS DB writer crashed")
    finally:
        try:
            batcher.flush_all()
        except (_psycopg_error_type(), RuntimeError):
            logger.exception("WS DB writer final flush failed")
        try:
            conn.close()
        except (_psycopg_error_type(), RuntimeError):
            logger.exception("WS DB writer connection close failed")
