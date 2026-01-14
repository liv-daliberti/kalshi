"""Shared typing helpers for web portal modules."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    import psycopg as psycopg_type

    PsycopgConnection = psycopg_type.Connection
else:
    PsycopgConnection = Any
