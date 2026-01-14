"""Prediction scheduler and pluggable agent handler."""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable, Optional

import psycopg  # pylint: disable=import-error
from psycopg.rows import dict_row  # pylint: disable=import-error

from src.db.db import (
    PredictionRunSpec,
    insert_market_predictions,
    insert_prediction_run,
    set_state,
    update_prediction_run,
)
from src.core.env_utils import parse_bool, parse_int
from src.core.guardrails import assert_service_role
from src.core.number_utils import normalize_probability
from src.predictions.prediction_utils import baseline_market_prob
from src.rag.rag_documents import fetch_rag_documents
from src.db.sql_fragments import (
    last_candle_lateral_sql,
    last_tick_lateral_sql,
    market_identity_columns_sql,
    tick_columns_sql,
)
from src.core.time_utils import ensure_utc, infer_strike_period_from_times

logger = logging.getLogger(__name__)


def _parse_bool(raw: str | None, default: bool = False) -> bool:
    return parse_bool(raw, default)


def _parse_int(raw: str | None, fallback: int, *, minimum: int | None = None) -> int:
    return parse_int(raw, fallback, minimum=minimum)

DEFAULT_PROMPT = (
    "You are forecasting Kalshi markets. Use the provided event context and any "
    "retrieved documents to estimate the probability that each market resolves YES. "
    "Return a JSON array of {market_ticker, yes_prob, confidence, rationale}."
)


@dataclass(frozen=True)
class PredictionIntervals:
    """Timing configuration for prediction scheduling."""

    poll_seconds: int
    interval_hour_minutes: int
    interval_day_minutes: int
    interval_default_minutes: int


@dataclass(frozen=True)
class PredictionLimits:
    """Limits for prediction scheduling and context size."""

    max_events_per_pass: int
    max_markets_per_event: int
    context_doc_limit: int


@dataclass(frozen=True)
class PredictionStrikeConfig:
    """Strike period configuration for prediction scheduling."""

    strike_periods: tuple[str, ...]
    hour_max_hours: float
    day_max_hours: float


@dataclass(frozen=True)
class PredictionModelConfig:
    """Model configuration for prediction handlers."""

    handler_path: Optional[str]
    agent_name: Optional[str]
    model_name: Optional[str]
    prompt: str


@dataclass(frozen=True)
class PredictionConfig:
    """Configuration for prediction scheduling and handlers."""

    enabled: bool
    intervals: PredictionIntervals
    limits: PredictionLimits
    strike: PredictionStrikeConfig
    model: PredictionModelConfig


PredictionHandler = Callable[
    [dict[str, Any], list[dict[str, Any]], dict[str, Any], str],
    list[dict[str, Any]],
]


@dataclass(frozen=True)
class PredictionEvent:
    """Minimal event info for prediction scheduling."""

    event_ticker: str
    event_title: Optional[str]
    strike_period: Optional[str]
    open_time: Optional[datetime]
    close_time: Optional[datetime]
    last_prediction_ts: Optional[datetime]


@dataclass(frozen=True)
class PredictionTask:
    """Context for executing a single prediction task."""

    event: PredictionEvent
    interval_minutes: int
    now: datetime


@dataclass
class PredictionRunState:
    """Mutable run state for prediction attempts."""

    event_ticker: str
    interval_minutes: int
    run_id: int | None = None


@dataclass(frozen=True)
class PredictionInputs:
    """Resolved inputs for a prediction run."""

    event_meta: dict[str, Any]
    markets: list[dict[str, Any]]
    market_tickers: set[str]
    context: dict[str, Any]
    run_metadata: dict[str, Any]


def _parse_float(raw: Optional[str], default: float, minimum: float | None = None) -> float:
    if not raw:
        value = default
    else:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            value = default
    if minimum is not None and value < minimum:
        return minimum
    return value


def _prediction_heartbeat(
    database_url: str,
    interval_seconds: float,
    stop_event: threading.Event,
) -> None:
    while not stop_event.wait(interval_seconds):
        try:
            with psycopg.connect(database_url) as conn:
                set_state(conn, "last_prediction_ts", datetime.now(timezone.utc).isoformat())
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Failed to update prediction health state (heartbeat)")


def _parse_csv(raw: Optional[str]) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(p.strip().lower() for p in raw.split(",") if p.strip())


def _load_prompt() -> str:
    path = os.getenv("PREDICTION_PROMPT_PATH")
    if path:
        expanded = os.path.expandvars(os.path.expanduser(path))
        try:
            with open(expanded, "r", encoding="utf-8") as handle:
                text = handle.read().strip()
                return text or DEFAULT_PROMPT
        except OSError as exc:
            logger.warning("Prediction prompt file read failed: %s", exc)
    raw = os.getenv("PREDICTION_PROMPT")
    if raw:
        return raw.strip() or DEFAULT_PROMPT
    return DEFAULT_PROMPT


def load_prediction_config() -> PredictionConfig:
    """Load prediction configuration from environment variables."""
    enabled = parse_bool(
        os.getenv("PREDICTION_ENABLE") or os.getenv("PREDICTIONS_ENABLE")
    )
    strike_periods = _parse_csv(
        os.getenv("PREDICTION_STRIKE_PERIODS")
        or os.getenv("STRIKE_PERIODS", "hour,day")
    )
    intervals = PredictionIntervals(
        poll_seconds=parse_int(os.getenv("PREDICTION_POLL_SECONDS"), 60, minimum=5),
        interval_hour_minutes=parse_int(
            os.getenv("PREDICTION_INTERVAL_HOUR_MINUTES"), 10, minimum=1
        ),
        interval_day_minutes=parse_int(
            os.getenv("PREDICTION_INTERVAL_DAY_MINUTES"), 30, minimum=1
        ),
        interval_default_minutes=parse_int(
            os.getenv("PREDICTION_INTERVAL_DEFAULT_MINUTES"), 30, minimum=1
        ),
    )
    limits = PredictionLimits(
        max_events_per_pass=parse_int(
            os.getenv("PREDICTION_MAX_EVENTS"), 25, minimum=1
        ),
        max_markets_per_event=parse_int(
            os.getenv("PREDICTION_MAX_MARKETS"), 200, minimum=1
        ),
        context_doc_limit=parse_int(
            os.getenv("PREDICTION_CONTEXT_DOCS"), 20, minimum=0
        ),
    )
    strike = PredictionStrikeConfig(
        strike_periods=strike_periods,
        hour_max_hours=_parse_float(os.getenv("STRIKE_HOUR_MAX_HOURS"), 2.0, minimum=0.1),
        day_max_hours=_parse_float(os.getenv("STRIKE_DAY_MAX_HOURS"), 36.0, minimum=0.1),
    )
    model = PredictionModelConfig(
        handler_path=os.getenv("PREDICTION_HANDLER"),
        agent_name=os.getenv("PREDICTION_AGENT_NAME") or "baseline:market_mid",
        model_name=os.getenv("PREDICTION_MODEL_NAME"),
        prompt=_load_prompt(),
    )
    return PredictionConfig(
        enabled=enabled,
        intervals=intervals,
        limits=limits,
        strike=strike,
        model=model,
    )


def _normalize_prob(value: Any) -> Optional[Decimal]:
    return normalize_probability(value, quantize=Decimal("0.000001"))


def _infer_strike_period(
    open_time: Optional[datetime],
    close_time: Optional[datetime],
    hour_max: float,
    day_max: float,
) -> Optional[str]:
    return infer_strike_period_from_times(open_time, close_time, hour_max, day_max)


def _event_interval_minutes(event: PredictionEvent, cfg: PredictionConfig) -> Optional[int]:
    strike_period = (event.strike_period or "").strip().lower()
    if not strike_period:
        strike_period = _infer_strike_period(
            event.open_time,
            event.close_time,
            cfg.strike.hour_max_hours,
            cfg.strike.day_max_hours,
        ) or ""
    if cfg.strike.strike_periods and strike_period not in cfg.strike.strike_periods:
        return None
    if strike_period == "hour":
        return cfg.intervals.interval_hour_minutes
    if strike_period == "day":
        return cfg.intervals.interval_day_minutes
    return cfg.intervals.interval_default_minutes


def _fetch_candidate_events(
    conn: psycopg.Connection,
    limit: int,
) -> list[PredictionEvent]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
              e.event_ticker,
              e.title AS event_title,
              e.strike_period,
              MIN(m.open_time) AS open_time,
              MAX(m.close_time) AS close_time,
              MAX(p.created_at) AS last_prediction_ts
            FROM events e
            JOIN markets m ON m.event_ticker = e.event_ticker
            LEFT JOIN market_predictions p ON p.event_ticker = e.event_ticker
            WHERE (m.close_time IS NULL OR m.close_time > NOW())
            GROUP BY e.event_ticker, e.title, e.strike_period
            HAVING COUNT(m.open_time) = COUNT(*)
               AND MAX(m.open_time) <= NOW()
            ORDER BY last_prediction_ts NULLS FIRST
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    now = datetime.now(timezone.utc)
    candidates: list[PredictionEvent] = []
    for row in rows:
        event_ticker = row.get("event_ticker")
        if not event_ticker:
            continue
        open_time = ensure_utc(row.get("open_time"))
        if open_time is None or open_time > now:
            continue
        candidates.append(
            PredictionEvent(
                event_ticker=event_ticker,
                event_title=row.get("event_title"),
                strike_period=row.get("strike_period"),
                open_time=open_time,
                close_time=ensure_utc(row.get("close_time")),
                last_prediction_ts=row.get("last_prediction_ts"),
            )
        )
    return candidates


def _fetch_event_metadata(conn: psycopg.Connection, event_ticker: str) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
              event_ticker,
              title,
              sub_title,
              category,
              series_ticker,
              strike_date,
              strike_period,
              mutually_exclusive,
              available_on_brokers,
              product_metadata
            FROM events
            WHERE event_ticker = %s
            """,
            (event_ticker,),
        )
        row = cur.fetchone()
    return dict(row) if row else {}


def _fetch_event_markets(
    conn: psycopg.Connection,
    event_ticker: str,
    limit: int,
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT
              {market_identity_columns_sql()},
              m.open_time,
              m.close_time,
              m.rules_primary,
              m.rules_secondary,
              {tick_columns_sql()},
              c.end_period_ts AS candle_end_ts,
              c.close AS candle_close
            FROM markets m
            {last_tick_lateral_sql()}
            {last_candle_lateral_sql()}
            WHERE m.event_ticker = %s
            ORDER BY m.open_time NULLS LAST, m.ticker
            LIMIT %s
            """,
            (event_ticker, limit),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def _has_volume_value(market: dict[str, Any]) -> bool:
    value = market.get("volume")
    if value is None:
        return False
    if isinstance(value, str) and value.strip().upper() == "N/A":
        return False
    return True


def _fetch_rag_documents(
    conn: psycopg.Connection,
    event_ticker: str,
    market_tickers: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    return fetch_rag_documents(conn, event_ticker, market_tickers, limit)


def _load_handler(path: str) -> PredictionHandler:
    module_name, attr = path.rsplit(":", 1)
    module = importlib.import_module(module_name)
    handler = getattr(module, attr)
    if not callable(handler):
        raise TypeError(f"PREDICTION_HANDLER is not callable: {path}")
    return handler


def default_prediction_handler(
    _event: dict[str, Any],
    markets: list[dict[str, Any]],
    _context: dict[str, Any],
    _prompt: str,
) -> list[dict[str, Any]]:
    """Fallback handler using latest market pricing."""
    results = []
    for market in markets:
        ticker = market.get("ticker")
        if not ticker:
            continue
        yes_prob, source = baseline_market_prob(market, _normalize_prob)
        if yes_prob is None:
            continue
        results.append(
            {
                "market_ticker": ticker,
                "yes_prob": yes_prob,
                "confidence": None,
                "rationale": f"baseline from {source}",
                "raw": {
                    "source": source,
                    "last_tick_ts": market.get("last_tick_ts"),
                    "candle_end_ts": market.get("candle_end_ts"),
                },
            }
        )
    return results


def resolve_prediction_handler(cfg: PredictionConfig) -> PredictionHandler:
    """Resolve prediction handler from config, defaulting to baseline."""
    if not cfg.model.handler_path:
        return default_prediction_handler
    try:
        return _load_handler(cfg.model.handler_path)
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception(
            "Failed to load PREDICTION_HANDLER=%s; using baseline",
            cfg.model.handler_path,
        )
        return default_prediction_handler


def _coerce_predictions(
    event_ticker: str,
    run_id: int,
    results: Iterable[Any],
    market_tickers: set[str],
) -> list[dict[str, Any]]:
    predictions = []
    for item in results or []:
        if isinstance(item, dict):
            market_ticker = item.get("market_ticker") or item.get("ticker")
            yes_value = item.get("yes_prob")
            if yes_value is None:
                yes_value = item.get("predicted_yes_prob") or item.get("probability")
            confidence = item.get("confidence")
            rationale = item.get("rationale")
            raw = item.get("raw") if "raw" in item else item
        else:
            continue
        if not market_ticker or market_ticker not in market_tickers:
            continue
        yes_prob = _normalize_prob(yes_value)
        if yes_prob is None:
            continue
        conf_prob = _normalize_prob(confidence)
        predictions.append(
            {
                "run_id": run_id,
                "event_ticker": event_ticker,
                "market_ticker": market_ticker,
                "predicted_yes_prob": yes_prob,
                "confidence": conf_prob,
                "rationale": rationale,
                "raw": raw,
            }
        )
    return predictions


def _start_prediction_heartbeat(
    database_url: str | None,
    interval_seconds: float,
) -> tuple[threading.Event | None, threading.Thread | None]:
    if not database_url or interval_seconds <= 0:
        return None, None
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_prediction_heartbeat,
        args=(database_url, interval_seconds, stop_event),
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def _stop_prediction_heartbeat(
    stop_event: threading.Event | None,
    thread: threading.Thread | None,
    timeout_seconds: float,
) -> None:
    if stop_event is not None:
        stop_event.set()
    if thread is not None:
        thread.join(timeout=timeout_seconds)


def _should_predict(
    event: PredictionEvent,
    interval_minutes: int,
    now: datetime,
) -> bool:
    last_ts = ensure_utc(event.last_prediction_ts)
    if last_ts is None:
        return True
    return last_ts <= now - timedelta(minutes=interval_minutes)


def _collect_due_events(
    conn: psycopg.Connection,
    cfg: PredictionConfig,
    now: datetime,
) -> list[tuple[PredictionEvent, int]]:
    due: list[tuple[PredictionEvent, int]] = []
    candidates = _fetch_candidate_events(conn, cfg.limits.max_events_per_pass)
    for event in candidates:
        interval_minutes = _event_interval_minutes(event, cfg)
        if interval_minutes is None:
            continue
        if _should_predict(event, interval_minutes, now):
            due.append((event, interval_minutes))
    return due


def _build_prediction_context(
    event_meta: dict[str, Any],
    markets: list[dict[str, Any]],
    docs: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    return {
        "event": event_meta,
        "markets": markets,
        "documents": docs,
        "generated_at": now.isoformat(),
    }


def _build_run_metadata(interval_minutes: int, market_tickers: set[str]) -> dict[str, Any]:
    return {
        "interval_minutes": interval_minutes,
        "market_count": len(market_tickers),
    }


def _insert_prediction_run(
    conn: psycopg.Connection,
    cfg: PredictionConfig,
    event_ticker: str,
    *,
    status: str,
    metadata: dict[str, Any],
    error: str | None = None,
) -> int:
    return insert_prediction_run(
        conn,
        PredictionRunSpec(
            event_ticker=event_ticker,
            prompt=cfg.model.prompt,
            agent=cfg.model.agent_name,
            model=cfg.model.model_name,
            status=status,
            error=error,
            metadata=metadata,
        ),
    )


def _record_prediction_failure(
    conn: psycopg.Connection,
    cfg: PredictionConfig,
    state: PredictionRunState,
    exc: Exception,
) -> None:
    if state.run_id is None:
        _insert_prediction_run(
            conn,
            cfg,
            state.event_ticker,
            status="failed",
            error=str(exc),
            metadata={"interval_minutes": state.interval_minutes},
        )
    else:
        update_prediction_run(conn, state.run_id, status="failed", error=str(exc))


def _prepare_prediction_inputs(
    conn: psycopg.Connection,
    cfg: PredictionConfig,
    task: PredictionTask,
) -> PredictionInputs | None:
    event_meta = _fetch_event_metadata(conn, task.event.event_ticker)
    markets = _fetch_event_markets(
        conn,
        task.event.event_ticker,
        cfg.limits.max_markets_per_event,
    )
    if not markets:
        return None
    markets = [market for market in markets if _has_volume_value(market)]
    if not markets:
        return None
    market_tickers = {m.get("ticker") for m in markets if m.get("ticker")}
    if not market_tickers:
        return None
    docs = _fetch_rag_documents(
        conn,
        task.event.event_ticker,
        sorted(market_tickers),
        cfg.limits.context_doc_limit,
    )
    context = _build_prediction_context(event_meta, markets, docs, task.now)
    run_metadata = _build_run_metadata(task.interval_minutes, market_tickers)
    return PredictionInputs(
        event_meta=event_meta,
        markets=markets,
        market_tickers=market_tickers,
        context=context,
        run_metadata=run_metadata,
    )


def _run_event_prediction(
    conn: psycopg.Connection,
    cfg: PredictionConfig,
    handler: PredictionHandler,
    task: PredictionTask,
) -> int:
    state = PredictionRunState(task.event.event_ticker, task.interval_minutes)
    try:
        inputs = _prepare_prediction_inputs(conn, cfg, task)
        if inputs is None:
            return 0
        state.run_id = _insert_prediction_run(
            conn,
            cfg,
            task.event.event_ticker,
            status="running",
            metadata=inputs.run_metadata,
        )
        results = handler(
            inputs.event_meta,
            inputs.markets,
            inputs.context,
            cfg.model.prompt,
        )
        predictions = _coerce_predictions(
            task.event.event_ticker,
            state.run_id,
            results,
            inputs.market_tickers,
        )
        if predictions:
            insert_market_predictions(conn, predictions)
        update_prediction_run(
            conn,
            state.run_id,
            status="completed",
            metadata={
                **inputs.run_metadata,
                "predictions": len(predictions),
            },
        )
        return len(predictions)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("Prediction failed for event=%s", task.event.event_ticker)
        _record_prediction_failure(conn, cfg, state, exc)
        return 0


def prediction_pass(
    conn: psycopg.Connection,
    cfg: PredictionConfig,
    handler: PredictionHandler | None = None,
) -> int:
    """Run a single prediction pass across due events."""
    if not cfg.enabled:
        return 0
    assert_service_role("rag", "prediction_pass")
    if handler is None:
        handler = resolve_prediction_handler(cfg)
    database_url = os.getenv("DATABASE_URL")
    heartbeat_seconds = _parse_float(
        os.getenv("RAG_HEARTBEAT_SECONDS") or os.getenv("PREDICTION_HEARTBEAT_SECONDS"),
        60.0,
        minimum=5.0,
    )
    heartbeat_event, heartbeat_thread = _start_prediction_heartbeat(
        database_url,
        heartbeat_seconds,
    )
    try:
        try:
            set_state(conn, "last_prediction_ts", datetime.now(timezone.utc).isoformat())
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Failed to update prediction health state (start)")
        now = datetime.now(timezone.utc)
        due = _collect_due_events(conn, cfg, now)
        total_predictions = 0
        for event, interval_minutes in due:
            task = PredictionTask(event, interval_minutes, now)
            total_predictions += _run_event_prediction(
                conn,
                cfg,
                handler,
                task,
            )
        try:
            finished_at = datetime.now(timezone.utc)
            set_state(conn, "last_prediction_ts", finished_at.isoformat())
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Failed to update prediction health state")
        return total_predictions
    finally:
        _stop_prediction_heartbeat(heartbeat_event, heartbeat_thread, heartbeat_seconds)


async def periodic_predictions(conn: psycopg.Connection, cfg: PredictionConfig) -> None:
    """Run predictions on a fixed interval."""
    if not cfg.enabled:
        return
    handler = resolve_prediction_handler(cfg)
    while True:
        try:
            count = prediction_pass(conn, cfg, handler=handler)
            if count:
                logger.info("prediction_pass: stored=%d", count)
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("prediction_pass failed")
        await asyncio.sleep(cfg.intervals.poll_seconds)
