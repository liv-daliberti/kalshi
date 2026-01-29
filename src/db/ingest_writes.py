"""Insert helpers for ingest write paths."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import psycopg  # pylint: disable=import-error

from .json_utils import to_json_value
from .portal_rollup import _portal_rollup_refresh_events


@dataclass(frozen=True)
class PredictionRunSpec:
    """Input fields for prediction run inserts."""

    event_ticker: Optional[str]
    prompt: Optional[str] = None
    agent: Optional[str] = None
    model: Optional[str] = None
    status: str = "running"
    error: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


def _lifecycle_payload(event: dict) -> dict:
    return {
        "ts": event.get("ts"),
        "market_ticker": event.get("market_ticker"),
        "event_type": event.get("event_type") or "unknown",
        "open_ts": event.get("open_ts"),
        "close_ts": event.get("close_ts"),
        "raw": to_json_value(event.get("raw")),
    }


def insert_lifecycle_event(conn: psycopg.Connection, event: dict) -> None:
    """Insert a market lifecycle event row."""
    insert_lifecycle_events(conn, [event])


def insert_lifecycle_events(conn: psycopg.Connection, events: list[dict]) -> None:
    """Insert multiple lifecycle rows in one transaction."""
    if not events:
        return
    sql = """
    INSERT INTO lifecycle_events(
      ts, market_ticker, event_type, open_ts, close_ts, raw
    )
    VALUES(
      %(ts)s, %(market_ticker)s, %(event_type)s, %(open_ts)s, %(close_ts)s, %(raw)s
    )
    """
    payloads = [_lifecycle_payload(event) for event in events]
    with conn.cursor() as cur:
        cur.executemany(sql, payloads)
    conn.commit()


def insert_prediction_run(conn: psycopg.Connection, spec: PredictionRunSpec) -> int:
    """Insert a prediction run and return its id."""
    payload = to_json_value(spec.metadata)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO prediction_runs(
              event_ticker, run_ts, prompt, agent, model, status, error, metadata
            )
            VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                spec.event_ticker,
                spec.prompt,
                spec.agent,
                spec.model,
                spec.status,
                spec.error,
                payload,
            ),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return int(run_id)


def update_prediction_run(
    conn: psycopg.Connection,
    run_id: int,
    status: str,
    error: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> None:
    """Update a prediction run status and optional metadata."""
    payload = to_json_value(metadata) if metadata is not None else None
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE prediction_runs
            SET status=%s,
                error=%s,
                metadata=COALESCE(%s, metadata)
            WHERE id=%s
            """,
            (status, error, payload, run_id),
        )
    conn.commit()


def insert_market_prediction(conn: psycopg.Connection, prediction: dict) -> None:
    """Insert a single market prediction row."""
    insert_market_predictions(conn, [prediction])


def insert_market_predictions(conn: psycopg.Connection, predictions: list[dict]) -> None:
    """Insert multiple market prediction rows."""
    if not predictions:
        return
    sql = """
    INSERT INTO market_predictions(
      run_id, event_ticker, market_ticker, predicted_yes_prob,
      confidence, rationale, raw, created_at
    )
    VALUES(
      %(run_id)s, %(event_ticker)s, %(market_ticker)s, %(predicted_yes_prob)s,
      %(confidence)s, %(rationale)s, %(raw)s, NOW()
    )
    """
    payloads = []
    for prediction in predictions:
        payloads.append(
            {
                "run_id": prediction.get("run_id"),
                "event_ticker": prediction.get("event_ticker"),
                "market_ticker": prediction.get("market_ticker"),
                "predicted_yes_prob": prediction.get("predicted_yes_prob"),
                "confidence": prediction.get("confidence"),
                "rationale": prediction.get("rationale"),
                "raw": to_json_value(prediction.get("raw")),
            }
        )
    with conn.cursor() as cur:
        cur.executemany(sql, payloads)
    _portal_rollup_refresh_events(
        conn,
        {payload["event_ticker"] for payload in payloads if payload.get("event_ticker")},
    )
    conn.commit()
