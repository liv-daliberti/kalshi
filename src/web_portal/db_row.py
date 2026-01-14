"""Shared row factory for psycopg dict rows in the portal."""

from __future__ import annotations

try:
    from psycopg.rows import dict_row as DICT_ROW
except ModuleNotFoundError:
    DICT_ROW = None

__all__ = ["DICT_ROW"]
