"""REST rate-limiting helpers for Kalshi SDK calls."""

from __future__ import annotations

import importlib
import json
import logging
import os
import threading
import time
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Any, Optional

from src.core.db_utils import safe_close
from src.kalshi.kalshi_rate_limit import register_rest_rate_limit

logger = logging.getLogger(__name__)

_CANDLE_RATE_LIMIT_LOCK = threading.Lock()
_CANDLE_NEXT_ALLOWED = 0.0
_REST_RATE_LIMIT_LOCK = threading.Lock()
_REST_NEXT_ALLOWED = 0.0
_REST_LAST_REFILL = 0.0
_REST_TOKENS = 0.0
_REST_RATE_LIMIT_CONFIG_LOCK = threading.Lock()
_REST_RATE_LIMIT_BACKEND_OVERRIDE: Optional[str] = None
_REST_RATE_LIMIT_DB_URL_OVERRIDE: Optional[str] = None
_REST_RATE_LIMIT_WARNED: set[str] = set()
_DB_RATE_LIMIT_LOCAL = threading.local()
_DB_RATE_LIMIT_ERROR_LOCK = threading.Lock()
_DB_RATE_LIMIT_ERROR_TS = 0.0
_DB_RATE_LIMIT_KEY = "kalshi_rest_rate_limit"


def _rest_limits() -> tuple[float, int]:
    """Return (rate_per_sec, burst) for REST token bucket."""
    try:
        rate = float(os.getenv("KALSHI_REST_RATE_PER_SEC", "0"))
    except ValueError:
        rate = 0.0
    try:
        burst = int(os.getenv("KALSHI_REST_BURST", "5"))
    except ValueError:
        burst = 5
    burst = max(burst, 1)
    return rate, burst


def configure_rest_rate_limit(
    backend: str | None = None,
    db_url: str | None = None,
) -> None:
    """Configure the REST rate limiter backend and DB URL overrides."""
    global _REST_RATE_LIMIT_BACKEND_OVERRIDE  # pylint: disable=global-statement
    global _REST_RATE_LIMIT_DB_URL_OVERRIDE  # pylint: disable=global-statement
    with _REST_RATE_LIMIT_CONFIG_LOCK:
        if backend is not None:
            backend = backend.strip().lower() if backend else ""
            _REST_RATE_LIMIT_BACKEND_OVERRIDE = backend or None
        if db_url is not None:
            db_url = db_url.strip() if db_url else ""
            _REST_RATE_LIMIT_DB_URL_OVERRIDE = db_url or None


def _rest_rate_limit_db_url() -> Optional[str]:
    if _REST_RATE_LIMIT_DB_URL_OVERRIDE is not None:
        return _REST_RATE_LIMIT_DB_URL_OVERRIDE
    db_url = os.getenv("KALSHI_REST_RATE_LIMIT_DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        return None
    return db_url.strip() or None


def _rate_limit_warn_once(key: str, message: str) -> None:
    if key in _REST_RATE_LIMIT_WARNED:
        return
    _REST_RATE_LIMIT_WARNED.add(key)
    logger.warning("%s", message)


def _rest_rate_limit_backend() -> str:
    raw = _REST_RATE_LIMIT_BACKEND_OVERRIDE
    if raw is None:
        raw = os.getenv("KALSHI_REST_RATE_LIMIT_BACKEND", "").strip().lower()
    if raw:
        if raw in {"db", "database", "postgres", "postgresql"}:
            return "db"
        if raw in {"memory", "local", "in-memory", "inprocess"}:
            return "memory"
        if raw == "redis":
            _rate_limit_warn_once(
                "backend-redis",
                (
                    "KALSHI_REST_RATE_LIMIT_BACKEND=redis is not supported; "
                    "using in-process limiter."
                ),
            )
            return "memory"
        _rate_limit_warn_once(
            f"backend-{raw}",
            f"Unknown KALSHI_REST_RATE_LIMIT_BACKEND={raw}; using in-process limiter.",
        )
        return "memory"
    return "db" if _rest_rate_limit_db_url() else "memory"


def _import_psycopg():
    try:
        return importlib.import_module("psycopg")
    except ImportError:
        return None


def _db_rate_limit_conn():
    if _rest_rate_limit_backend() != "db":
        return None
    db_url = _rest_rate_limit_db_url()
    if not db_url:
        _rate_limit_warn_once(
            "db-missing-url",
            "DB rate limiter requested but DATABASE_URL is not set; using in-process limiter.",
        )
        return None
    conn = getattr(_DB_RATE_LIMIT_LOCAL, "conn", None)
    if conn is not None and not getattr(conn, "closed", False):
        return conn
    psycopg = _import_psycopg()
    if psycopg is None:
        _rate_limit_warn_once(
            "db-missing-psycopg",
            "DB rate limiter unavailable (psycopg import failed); using in-process limiter.",
        )
        return None
    try:
        conn = psycopg.connect(db_url)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _rate_limit_warn_once(
            "db-connect-failed",
            f"DB rate limiter connection failed ({exc}); using in-process limiter.",
        )
        return None
    _DB_RATE_LIMIT_LOCAL.conn = conn
    return conn


def _db_rate_limit_error(exc: Exception) -> None:
    now = time.monotonic()
    with _DB_RATE_LIMIT_ERROR_LOCK:
        global _DB_RATE_LIMIT_ERROR_TS  # pylint: disable=global-statement
        if now - _DB_RATE_LIMIT_ERROR_TS < 60:
            return
        _DB_RATE_LIMIT_ERROR_TS = now
    logger.warning(
        "DB rate limiter failed; falling back to in-process limiter: %s",
        exc,
    )


def _reset_db_rate_limit_conn() -> None:
    conn = getattr(_DB_RATE_LIMIT_LOCAL, "conn", None)
    if conn is None:
        return
    safe_close(conn)
    _DB_RATE_LIMIT_LOCAL.conn = None


def _to_float(value: Any, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _parse_db_rate_state(raw: str | None, now: float, burst: int) -> dict[str, float]:
    state = {
        "tokens": float(burst),
        "last_refill_ts": now,
        "next_allowed_ts": 0.0,
    }
    if not raw:
        return state
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return state
    if not isinstance(payload, dict):
        return state
    tokens = _to_float(payload.get("tokens"), state["tokens"])
    last_refill = _to_float(payload.get("last_refill_ts"), state["last_refill_ts"])
    next_allowed = _to_float(payload.get("next_allowed_ts"), state["next_allowed_ts"])
    tokens = max(0.0, min(tokens, float(burst)))
    last_refill = min(last_refill, now)
    next_allowed = max(next_allowed, 0.0)
    return {
        "tokens": tokens,
        "last_refill_ts": last_refill,
        "next_allowed_ts": next_allowed,
    }


def _serialize_db_rate_state(state: dict[str, float]) -> str:
    return json.dumps(state, separators=(",", ":"), sort_keys=True)


def _db_rate_limit_fetch_state(conn, now: float, burst: int) -> dict[str, float]:
    default_state = _serialize_db_rate_state(
        {
            "tokens": float(burst),
            "last_refill_ts": now,
            "next_allowed_ts": 0.0,
        }
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_state (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO NOTHING
            """,
            (_DB_RATE_LIMIT_KEY, default_state),
        )
        cur.execute(
            "SELECT value FROM ingest_state WHERE key = %s FOR UPDATE",
            (_DB_RATE_LIMIT_KEY,),
        )
        row = cur.fetchone()
    raw = row[0] if row else None
    return _parse_db_rate_state(raw, now, burst)


def _db_rate_limit_write_state(conn, state: dict[str, float]) -> None:
    payload = _serialize_db_rate_state(state)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingest_state
            SET value = %s, updated_at = NOW()
            WHERE key = %s
            """,
            (payload, _DB_RATE_LIMIT_KEY),
        )


def _db_rest_backoff_remaining() -> Optional[float]:
    conn = _db_rate_limit_conn()
    if conn is None:
        return None
    now = time.time()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM ingest_state WHERE key = %s",
                (_DB_RATE_LIMIT_KEY,),
            )
            row = cur.fetchone()
        state = _parse_db_rate_state(row[0] if row else None, now, burst=1)
        return max(0.0, state["next_allowed_ts"] - now)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _db_rate_limit_error(exc)
        _reset_db_rate_limit_conn()
        return None


def _db_rest_apply_cooldown(cooldown_sec: float) -> bool:
    if cooldown_sec <= 0:
        return True
    conn = _db_rate_limit_conn()
    if conn is None:
        return False
    rate_per_sec, burst = _rest_limits()
    now = time.time()
    try:
        with conn.transaction():
            state = _db_rate_limit_fetch_state(conn, now, burst)
            state["next_allowed_ts"] = max(
                state["next_allowed_ts"],
                now + cooldown_sec,
            )
            if rate_per_sec <= 0:
                state["tokens"] = float(burst)
                state["last_refill_ts"] = now
            _db_rate_limit_write_state(conn, state)
        return True
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _db_rate_limit_error(exc)
        _reset_db_rate_limit_conn()
        return False


def _db_rest_wait() -> bool:
    rate_per_sec, burst = _rest_limits()
    conn = _db_rate_limit_conn()
    if conn is None:
        return False
    while True:
        now = time.time()
        wait_s = 0.0
        try:
            with conn.transaction():
                state = _db_rate_limit_fetch_state(conn, now, burst)
                backoff_s = state["next_allowed_ts"] - now
                if backoff_s > 0:
                    wait_s = backoff_s
                elif rate_per_sec <= 0:
                    state["tokens"] = float(burst)
                    state["last_refill_ts"] = now
                else:
                    elapsed = max(0.0, now - state["last_refill_ts"])
                    tokens = min(float(burst), state["tokens"] + elapsed * rate_per_sec)
                    state["last_refill_ts"] = now
                    if tokens >= 1.0:
                        state["tokens"] = tokens - 1.0
                        wait_s = 0.0
                    else:
                        state["tokens"] = tokens
                        wait_s = (1.0 - tokens) / rate_per_sec
                _db_rate_limit_write_state(conn, state)
            if wait_s <= 0:
                return True
            time.sleep(wait_s)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _db_rate_limit_error(exc)
            _reset_db_rate_limit_conn()
            return False


def _rest_backoff_remaining_local() -> float:
    with _REST_RATE_LIMIT_LOCK:
        remaining = _REST_NEXT_ALLOWED - time.monotonic()
    return max(0.0, remaining)


def _rest_apply_cooldown_local(cooldown_sec: float) -> None:
    if cooldown_sec <= 0:
        return
    now = time.monotonic()
    with _REST_RATE_LIMIT_LOCK:
        global _REST_NEXT_ALLOWED  # pylint: disable=global-statement
        _REST_NEXT_ALLOWED = max(_REST_NEXT_ALLOWED, now + cooldown_sec)


def _rest_wait_local() -> None:
    rate_per_sec, burst = _rest_limits()
    while True:
        now = time.monotonic()
        wait_s = 0.0
        with _REST_RATE_LIMIT_LOCK:
            global _REST_LAST_REFILL, _REST_TOKENS  # pylint: disable=global-statement
            if _REST_LAST_REFILL <= 0:
                _REST_LAST_REFILL = now
                _REST_TOKENS = float(burst)
            backoff_s = _REST_NEXT_ALLOWED - now
            if backoff_s > 0:
                wait_s = backoff_s
            elif rate_per_sec <= 0:
                return
            else:
                elapsed = max(0.0, now - _REST_LAST_REFILL)
                _REST_TOKENS = min(float(burst), _REST_TOKENS + elapsed * rate_per_sec)
                _REST_LAST_REFILL = now
                if _REST_TOKENS >= 1.0:
                    _REST_TOKENS -= 1.0
                    return
                wait_s = (1.0 - _REST_TOKENS) / rate_per_sec
        if wait_s <= 0:
            return
        time.sleep(wait_s)


def rest_backoff_remaining() -> float:
    """Return remaining global REST backoff seconds."""
    if _rest_rate_limit_backend() == "db":
        remaining = _db_rest_backoff_remaining()
        if remaining is not None:
            return remaining
    return _rest_backoff_remaining_local()


def rest_apply_cooldown(cooldown_sec: float) -> None:
    """Apply a shared REST cooldown window."""
    _rest_apply_cooldown_local(cooldown_sec)
    if _rest_rate_limit_backend() == "db":
        _db_rest_apply_cooldown(cooldown_sec)


def rest_wait() -> None:
    """Wait for shared REST backoff and token bucket allowance."""
    if _rest_rate_limit_backend() == "db" and _db_rest_wait():
        return
    _rest_wait_local()


def rest_register_rate_limit(
    exc: Exception | None = None,
    cooldown_sec: float | None = None,
) -> None:
    """Register a REST rate limit based on an exception or fallback."""
    register_rest_rate_limit(
        rest_apply_cooldown,
        _extract_retry_after,
        exc,
        cooldown_sec,
    )


def _candlesticks_wait() -> None:
    """Enforce a minimum interval between candlestick calls."""
    global _CANDLE_NEXT_ALLOWED  # pylint: disable=global-statement
    try:
        min_interval = float(os.getenv("KALSHI_CANDLE_MIN_INTERVAL_SECONDS", "0"))
    except ValueError:
        min_interval = 0.0
    now = time.monotonic()
    with _CANDLE_RATE_LIMIT_LOCK:
        wait_s = _CANDLE_NEXT_ALLOWED - now
        if min_interval > 0:
            next_allowed = max(_CANDLE_NEXT_ALLOWED, now) + min_interval
            _CANDLE_NEXT_ALLOWED = next_allowed
    if wait_s > 0:
        time.sleep(wait_s)


def _candlesticks_apply_cooldown(cooldown_sec: float) -> None:
    """Push out the next candlestick call based on a cooldown."""
    global _CANDLE_NEXT_ALLOWED  # pylint: disable=global-statement
    if cooldown_sec <= 0:
        return
    now = time.monotonic()
    with _CANDLE_RATE_LIMIT_LOCK:
        _CANDLE_NEXT_ALLOWED = max(_CANDLE_NEXT_ALLOWED, now + cooldown_sec)


def _extract_headers(exc: Exception) -> Any:
    """Extract headers from an SDK exception when possible."""
    headers = getattr(exc, "headers", None)
    if headers:
        return headers
    http_resp = getattr(exc, "http_resp", None)
    if http_resp is None:
        return None
    headers = getattr(http_resp, "headers", None)
    if headers:
        return headers
    getheaders = getattr(http_resp, "getheaders", None)
    if callable(getheaders):
        try:
            return getheaders()
        except Exception:  # pylint: disable=broad-exception-caught
            return None
    return None


def _header_lookup(headers: Any, name: str) -> Optional[Any]:
    """Find a header value by name using case-insensitive matching."""
    if headers is None:
        return None
    name_lc = name.lower()
    value = None
    if isinstance(headers, dict):
        for key, item in headers.items():
            if str(key).lower() == name_lc:
                value = item
                break
    elif isinstance(headers, (list, tuple)):
        for item in headers:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                continue
            if str(item[0]).lower() == name_lc:
                value = item[1]
                break
    else:
        getter = getattr(headers, "get", None)
        if callable(getter):
            try:
                value = getter(name)
            except Exception:  # pylint: disable=broad-exception-caught
                value = None
    return value


def _parse_retry_after(value: Any) -> Optional[float]:
    """Parse Retry-After or rate-limit-reset header values into seconds."""
    if value is None:
        return None
    try:
        value_f = float(value)
    except (TypeError, ValueError):
        value_f = None
    if value_f is not None:
        now = time.time()
        if value_f > 1e9 or value_f > now:
            return max(0.0, value_f - now)
        return max(0.0, value_f)
    try:
        parsed_dt = parsedate_to_datetime(str(value))
    except (TypeError, ValueError, IndexError):
        return None
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
    return max(0.0, parsed_dt.timestamp() - time.time())


def _extract_retry_after(exc: Exception) -> Optional[float]:
    """Extract retry-after seconds from an SDK exception when possible."""
    headers = _extract_headers(exc)
    retry_after = _header_lookup(headers, "Retry-After")
    if retry_after is not None:
        return _parse_retry_after(retry_after)
    reset = _header_lookup(headers, "X-Rate-Limit-Reset") or _header_lookup(
        headers, "X-RateLimit-Reset"
    )
    if reset is not None:
        return _parse_retry_after(reset)
    return None
