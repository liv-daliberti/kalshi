"""Shared service bootstrap helpers."""

from __future__ import annotations

from typing import Any

import psycopg  # pylint: disable=import-error

from ..db.db import ensure_schema_compatible, maybe_init_schema
from .loop_utils import schema_path
from ..kalshi.kalshi_sdk import make_client


def _default_schema_path() -> str:
    return schema_path(__file__)


def load_private_key(pem_path: str) -> str:
    """Read a private key PEM from disk."""
    with open(pem_path, "r", encoding="utf-8") as key_file:
        return key_file.read()


def open_client_and_conn(
    settings: Any,
    *,
    private_key_pem: str | None = None,
    schema_path_override: str | None = None,
) -> tuple[Any, psycopg.Connection]:
    """Create a Kalshi client and schema-ready DB connection."""
    if private_key_pem is None:
        private_key_pem = load_private_key(settings.kalshi_private_key_pem_path)
    client = make_client(settings.kalshi_host, settings.kalshi_api_key_id, private_key_pem)
    conn = psycopg.connect(settings.database_url)
    maybe_init_schema(conn, schema_path=schema_path_override or _default_schema_path())
    ensure_schema_compatible(conn)
    return client, conn
