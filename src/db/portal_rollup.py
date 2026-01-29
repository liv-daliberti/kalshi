"""Portal rollup refresh helpers."""

from __future__ import annotations

import logging

import psycopg  # pylint: disable=import-error

from ..core.env_utils import _env_bool

logger = logging.getLogger(__name__)
_PORTAL_ROLLUP_REFRESH_WARNED = False


def _portal_rollup_app_refresh_enabled() -> bool:
    return _env_bool("WEB_PORTAL_ROLLUP_APP_REFRESH", False)


def _portal_rollup_refresh_events(
    conn: psycopg.Connection,
    event_tickers: list[str] | set[str] | tuple[str, ...],
    *,
    logger_override: logging.Logger | None = None,
) -> None:
    if not _portal_rollup_app_refresh_enabled():
        return
    active_logger = logger_override or logger
    unique = sorted({ticker for ticker in event_tickers if ticker})
    if not unique:
        return
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "SELECT portal_refresh_event_rollup(%s)",
                [(ticker,) for ticker in unique],
            )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        global _PORTAL_ROLLUP_REFRESH_WARNED  # pylint: disable=global-statement
        if not _PORTAL_ROLLUP_REFRESH_WARNED:
            active_logger.warning("portal rollup refresh failed: %s", exc)
            _PORTAL_ROLLUP_REFRESH_WARNED = True


def portal_rollup_refresh_events(
    conn: psycopg.Connection,
    event_tickers: list[str] | set[str] | tuple[str, ...],
    *,
    logger_override: logging.Logger | None = None,
    warned: bool | None = None,
) -> bool:
    """Public wrapper for refreshing portal rollups with optional overrides."""
    global _PORTAL_ROLLUP_REFRESH_WARNED  # pylint: disable=global-statement
    if warned is not None:
        _PORTAL_ROLLUP_REFRESH_WARNED = bool(warned)
    _portal_rollup_refresh_events(
        conn,
        event_tickers,
        logger_override=logger_override,
    )
    return _PORTAL_ROLLUP_REFRESH_WARNED
