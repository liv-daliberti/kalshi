"""Shared helpers for ingest_state access."""

from __future__ import annotations

from typing import Optional

import psycopg  # pylint: disable=import-error

from ..core.guardrails import assert_state_write_allowed


def get_state(
    conn: psycopg.Connection,
    key: str,
    default: Optional[str] = None,
) -> Optional[str]:
    """Fetch a state value by key, returning default when missing.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param key: State key to look up.
    :type key: str
    :param default: Value to return when key is absent.
    :type default: str | None
    :return: Stored value or default.
    :rtype: str | None
    """
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM ingest_state WHERE key=%s", (key,))
        row = cur.fetchone()
        return row[0] if row else default


def set_state(conn: psycopg.Connection, key: str, value: str) -> None:
    """Upsert a state value by key.

    :param conn: Open database connection.
    :type conn: psycopg.Connection
    :param key: State key to update.
    :type key: str
    :param value: State value to store.
    :type value: str
    """
    assert_state_write_allowed(key)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_state(key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
            """,
            (key, value),
        )
    conn.commit()
