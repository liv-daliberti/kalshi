"""Database connection pool helpers for the web portal."""

from __future__ import annotations

import importlib
import logging
import os
import sys
import threading
from contextlib import contextmanager
from typing import Any

import psycopg  # pylint: disable=import-error

from .config import _env_bool, _env_float, _env_int
from .portal_utils import portal_attr as _portal_attr
from .portal_utils import portal_logger as _portal_logger
from .portal_utils import portal_module as _portal_module

_PSYCOPG_POOL = None
try:
    _PSYCOPG_POOL = importlib.import_module("psycopg_pool")
except Exception:  # pylint: disable=broad-exception-caught
    _PSYCOPG_POOL = None

_parent = sys.modules.get("src")
_pkg = sys.modules.get("src.web_portal")
if _parent is not None and _pkg is not None and not hasattr(_parent, "web_portal"):
    setattr(_parent, "web_portal", _pkg)

if _PSYCOPG_POOL is not None:

    class ConnectionPool(  # pylint: disable=too-few-public-methods
        _PSYCOPG_POOL.ConnectionPool
    ):  # type: ignore[misc]
        """Alias for psycopg_pool.ConnectionPool."""

        def is_open(self) -> bool:
            """Return True when the pool is open."""
            return not getattr(self, "closed", False)

    class PoolTimeout(_PSYCOPG_POOL.PoolTimeout):  # type: ignore[misc]
        """Alias for psycopg_pool.PoolTimeout."""

        def is_timeout(self) -> bool:
            """Return True when this exception signals a timeout."""
            return True

        def status(self) -> str:
            """Return a status label for this exception."""
            return "timeout"
else:

    class ConnectionPool:  # pylint: disable=too-few-public-methods
        """Fallback ConnectionPool when psycopg_pool is unavailable."""

        def __init__(self, *_args, **_kwargs) -> None:
            raise RuntimeError("psycopg_pool is unavailable.")

        def close(self) -> None:
            """No-op close for the fallback implementation."""
            return None

        def is_open(self) -> bool:
            """Return False because the fallback pool cannot open connections."""
            return False

    class PoolTimeout(Exception):  # pylint: disable=too-few-public-methods
        """Fallback PoolTimeout when psycopg_pool is unavailable."""

        def is_timeout(self) -> bool:
            """Return True when this exception signals a timeout."""
            return True

        def status(self) -> str:
            """Return a status label for this exception."""
            return "unavailable"

_DB_POOL = None
_DB_POOL_LOCK = threading.Lock()


def _logger() -> logging.Logger:
    return _portal_logger(__name__)


def _set_portal_attr(name: str, value: Any) -> None:
    globals()[name] = value
    portal = _portal_module()
    if portal is not None:
        setattr(portal, name, value)


def _db_pool_enabled() -> bool:
    return _env_bool("WEB_DB_POOL_ENABLE", True)


def _db_pool_sizes() -> tuple[int, int]:
    min_size = _env_int("WEB_DB_POOL_MIN", 2, minimum=0)
    max_size = _env_int("WEB_DB_POOL_MAX", 8, minimum=1)
    max_size = max(max_size, min_size)
    return min_size, max_size


def _db_pool_timeout() -> float:
    return _env_float("WEB_DB_POOL_TIMEOUT", 3.0, minimum=0.1)


def _get_db_pool(db_url: str | None):
    pool_cls = _portal_attr("ConnectionPool", ConnectionPool)
    if not db_url or pool_cls is None or not _db_pool_enabled():
        return None
    pool = _DB_POOL
    if pool is not None:
        return pool
    lock = _DB_POOL_LOCK
    with lock:
        pool = _DB_POOL
        if pool is not None:
            return pool
        min_size, max_size = _db_pool_sizes()
        timeout = _db_pool_timeout()
        try:
            pool = pool_cls(
                db_url,
                min_size=min_size,
                max_size=max_size,
                timeout=timeout,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _logger().warning(
                "DB pool init failed; falling back to direct connections: %s",
                exc,
            )
            pool = None
        _set_portal_attr("_DB_POOL", pool)
    return pool


@contextmanager
def _managed_connection(conn: psycopg.Connection, autocommit: bool):
    if conn is None:
        raise RuntimeError("Database connection unavailable.")
    prev_autocommit = conn.autocommit
    conn.autocommit = autocommit
    try:
        yield conn
        if not conn.autocommit:
            conn.commit()
    except Exception:
        if not conn.autocommit:
            conn.rollback()
        raise
    finally:
        conn.autocommit = prev_autocommit


@contextmanager
def _pool_connection(pool, autocommit: bool):
    try:
        with pool.connection() as conn:
            with _managed_connection(conn, autocommit) as managed:
                yield managed
    except _portal_attr("PoolTimeout", PoolTimeout) as exc:
        raise RuntimeError(
            "Database connection pool exhausted; try again shortly."
        ) from exc


@contextmanager
def _direct_connection(
    db_url: str,
    autocommit: bool,
    connect_timeout: int | None,
):
    connect_kwargs: dict[str, Any] = {}
    if connect_timeout is not None:
        connect_kwargs["connect_timeout"] = connect_timeout
    conn = psycopg.connect(db_url, **connect_kwargs)
    if conn is None:
        raise RuntimeError("Database connection failed (psycopg.connect returned None).")
    try:
        with _managed_connection(conn, autocommit) as managed:
            yield managed
    finally:
        if conn is not None:
            conn.close()


@contextmanager
def _db_connection(
    autocommit: bool = False,
    connect_timeout: int | None = None,
    force_direct: bool = False,
):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set.")
    pool_func = _portal_attr("_get_db_pool", _get_db_pool)
    pool = None if force_direct else pool_func(db_url)
    if pool is not None:
        with _pool_connection(pool, autocommit) as conn:
            yield conn
        return

    with _direct_connection(db_url, autocommit, connect_timeout) as conn:
        yield conn


def _maybe_prewarm_db_pool() -> None:
    if not _env_bool("WEB_DB_POOL_PREWARM", True):
        return
    db_url = os.getenv("DATABASE_URL")
    pool_func = _portal_attr("_get_db_pool", _get_db_pool)
    pool = pool_func(db_url)
    if pool is None:
        return
    try:
        with pool.connection():
            pass
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _logger().warning("DB pool prewarm failed: %s", exc)
