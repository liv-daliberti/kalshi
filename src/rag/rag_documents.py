"""Shared helpers for RAG document retrieval."""

from __future__ import annotations

from typing import Any

import psycopg  # pylint: disable=import-error
from psycopg.rows import dict_row  # pylint: disable=import-error


def fetch_rag_documents(
    conn: psycopg.Connection,
    event_ticker: str,
    market_tickers: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    """Fetch RAG documents for an event and its markets."""
    if limit <= 0:
        return []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
              id,
              source,
              source_id,
              event_ticker,
              market_ticker,
              content,
              embedding,
              metadata,
              updated_at
            FROM rag_documents
            WHERE event_ticker = %s
               OR market_ticker = ANY(%s)
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (event_ticker, market_tickers, limit),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]
