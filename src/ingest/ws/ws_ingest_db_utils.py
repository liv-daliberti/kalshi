"""Shared psycopg helpers for WS ingestion."""

from __future__ import annotations

import importlib
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


class PsycopgError(Exception):
    """Fallback for psycopg errors when psycopg is unavailable."""


class InsufficientPrivilegeError(PsycopgError):
    """Fallback for psycopg privilege errors when psycopg is unavailable."""


@lru_cache(maxsize=1)
def _load_psycopg():
    try:
        return importlib.import_module("psycopg")
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg is required for WS ingestion; install psycopg.") from exc


def _require_psycopg():
    return _load_psycopg()


def _psycopg_error_type() -> type[BaseException]:
    try:
        return _load_psycopg().Error
    except RuntimeError:
        return PsycopgError


def _psycopg_privilege_error_type() -> type[BaseException]:
    try:
        return _load_psycopg().errors.InsufficientPrivilege
    except RuntimeError:
        return InsufficientPrivilegeError


def _safe_rollback(conn) -> None:
    db_error = _psycopg_error_type()
    try:
        conn.rollback()
    except (db_error, RuntimeError):
        logger.exception("DB rollback failed")
