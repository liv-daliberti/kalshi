"""Service health checks for REST/WS/worker/RAG."""

from __future__ import annotations

import argparse
import json
import logging
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional

import psycopg  # pylint: disable=import-error
from dateutil.parser import isoparse  # pylint: disable=import-error

from src.db.db import ensure_schema_compatible
from src.core.time_utils import (
    age_seconds as _age_seconds,
    parse_epoch_seconds as _parse_epoch_seconds,
    parse_ts,
)
from src.core.env_utils import _env_bool, _env_int

logger = logging.getLogger(__name__)

_SERVICE_KEYS = {
    "rest": ["last_discovery_ts", "last_min_close_ts"],
    "ws": ["last_ws_tick_ts", "last_tick_ts"],
    "worker": ["last_worker_ts"],
    "rag": ["last_prediction_ts"],
}


def _parse_ts(value: Any) -> datetime | None:
    return parse_ts(value, parser=isoparse)


def _fetch_state_rows(
    conn: psycopg.Connection,
    keys: list[str],
) -> dict[str, dict[str, Any]]:
    if not keys:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT key, value, updated_at
            FROM ingest_state
            WHERE key = ANY(%s)
            """,
            (keys,),
        )
        rows = cur.fetchall()
    return {row[0]: {"value": row[1], "updated_at": row[2]} for row in rows if row}


def _status_from_age(
    age_seconds: float | None,
    stale_seconds: float,
    *,
    allow_missing: bool = False,
) -> tuple[bool, str]:
    if age_seconds is None:
        return (allow_missing, "missing" if not allow_missing else "starting")
    if age_seconds <= stale_seconds:
        return True, "ok"
    return False, "stale"


def _resolve_stale_thresholds(service: str) -> dict[str, int]:
    if service == "rest":
        discovery_interval = _env_int("DISCOVERY_SECONDS", 1800, minimum=60)
        backfill_interval = _env_int("BACKFILL_SECONDS", 900, minimum=60)
        discovery_default = max(discovery_interval * 2, 600)
        backfill_default = max(backfill_interval * 2, 600)
        return {
            "last_discovery_ts": _env_int(
                "HEALTH_DISCOVERY_STALE_SECONDS",
                _env_int("WEB_PORTAL_DISCOVERY_STALE_SECONDS", discovery_default, minimum=60),
                minimum=60,
            ),
            "last_min_close_ts": _env_int(
                "HEALTH_BACKFILL_STALE_SECONDS",
                _env_int("WEB_PORTAL_BACKFILL_STALE_SECONDS", backfill_default, minimum=60),
                minimum=60,
            ),
        }
    if service == "ws":
        return {
            "last_ws_tick_ts": _env_int(
                "HEALTH_WS_STALE_SECONDS",
                _env_int("WEB_PORTAL_WS_STALE_SECONDS", 120, minimum=10),
                minimum=10,
            )
        }
    if service == "worker":
        poll_default = max(_env_int("WORK_QUEUE_POLL_SECONDS", 10, minimum=1) * 2, 60)
        return {
            "last_worker_ts": _env_int(
                "HEALTH_WORKER_STALE_SECONDS",
                poll_default,
                minimum=30,
            )
        }
    if service == "rag":
        return {
            "last_prediction_ts": _env_int(
                "HEALTH_RAG_STALE_SECONDS",
                _env_int("WEB_PORTAL_RAG_STALE_SECONDS", 300, minimum=30),
                minimum=30,
            )
        }
    raise ValueError(f"Unknown service '{service}'")


def _load_service_timestamps(
    conn: psycopg.Connection,
    service: str,
) -> dict[str, datetime | None]:
    keys = _SERVICE_KEYS.get(service, [])
    state_rows = _fetch_state_rows(conn, keys)
    timestamps: dict[str, datetime | None] = {}
    for key in keys:
        row = state_rows.get(key)
        if not row:
            timestamps[key] = None
            continue
        value = row.get("value")
        updated_at = row.get("updated_at")
        if key == "last_min_close_ts":
            parsed = _parse_epoch_seconds(value)
            timestamps[key] = parsed or _parse_ts(updated_at)
        else:
            parsed = _parse_ts(value)
            timestamps[key] = parsed or _parse_ts(updated_at)

    if service == "ws":
        if timestamps.get("last_ws_tick_ts") is None and timestamps.get("last_tick_ts"):
            timestamps["last_ws_tick_ts"] = timestamps.get("last_tick_ts")
        if timestamps.get("last_ws_tick_ts") is None:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(ts) FROM market_ticks")
                row = cur.fetchone()
            timestamps["last_ws_tick_ts"] = row[0] if row else None
    return timestamps


def _build_base_payload(service: str, checked_at: datetime) -> dict[str, Any]:
    return {
        "service": service,
        "ok": False,
        "checked_at": checked_at.isoformat(),
        "checks": {},
    }


def _evaluate_checks(
    timestamps: dict[str, datetime | None],
    thresholds: dict[str, int],
    checked_at: datetime,
    *,
    allow_missing: bool,
) -> tuple[bool, dict[str, dict[str, Any]]]:
    checks: dict[str, dict[str, Any]] = {}
    overall_ok = True
    for key, stale_seconds in thresholds.items():
        ts_value = timestamps.get(key)
        age_seconds = _age_seconds(ts_value, checked_at)
        check_ok, status = _status_from_age(
            age_seconds,
            stale_seconds,
            allow_missing=allow_missing,
        )
        overall_ok = overall_ok and check_ok
        checks[key] = {
            "status": status,
            "age_seconds": age_seconds,
            "stale_seconds": stale_seconds,
            "ts": ts_value.isoformat() if ts_value else None,
        }
    return overall_ok, checks


def check_service(
    service: str,
    database_url: str,
    *,
    connect_timeout: int = 3,
    allow_missing: bool = False,
) -> dict[str, Any]:
    """Check service health by inspecting ingest_state and schema."""
    checked_at = datetime.now(timezone.utc)
    payload: dict[str, Any] = _build_base_payload(service, checked_at)
    with psycopg.connect(database_url, connect_timeout=connect_timeout) as conn:
        version = ensure_schema_compatible(conn)
        payload["schema_version"] = version
        timestamps = _load_service_timestamps(conn, service)
    thresholds = _resolve_stale_thresholds(service)
    overall_ok, checks = _evaluate_checks(
        timestamps,
        thresholds,
        checked_at,
        allow_missing=allow_missing,
    )
    payload["checks"] = checks
    payload["ok"] = overall_ok
    return payload


def _health_http_response(handler: BaseHTTPRequestHandler, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, default=str).encode("utf-8")
    handler.send_response(200 if payload.get("ok") else 503)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def start_health_server(
    service: str,
    *,
    host: Optional[str] = None,
    port: Optional[int] = None,
) -> Optional[ThreadingHTTPServer]:
    """Start a thread-backed health endpoint or return None if disabled."""
    if port is None:
        raw = os.getenv("SERVICE_HEALTH_PORT")
        if not raw:
            return None
        try:
            port = int(raw)
        except ValueError:
            logger.error("Invalid SERVICE_HEALTH_PORT=%s", raw)
            return None
    if host is None:
        host = os.getenv("SERVICE_HEALTH_HOST", "0.0.0.0")
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        logger.error("DATABASE_URL is not set; health server disabled.")
        return None
    connect_timeout = _env_int("HEALTH_CONNECT_TIMEOUT", 3, minimum=1)
    allow_missing = _env_bool("HEALTH_ALLOW_MISSING", False)

    class Handler(BaseHTTPRequestHandler):
        """Serve JSON health responses."""

        def do_GET(self) -> None:  # noqa: N802 pylint: disable=invalid-name
            """Respond to health check requests."""
            if self.path not in {"/healthz", "/health"}:
                self.send_response(404)
                self.end_headers()
                return
            try:
                payload = check_service(
                    service,
                    database_url,
                    connect_timeout=connect_timeout,
                    allow_missing=allow_missing,
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                payload = {
                    "service": service,
                    "ok": False,
                    "error": str(exc) or "Health check failed.",
                }
            _health_http_response(self, payload)

        def log_request(  # noqa: D401
            self,
            code: int | str = "-",
            size: int | str = "-",
        ) -> None:
            """Redirect HTTP request logs into the module logger."""
            logger.info(
                'healthz "%s" %s %s',
                self.requestline,
                code,
                size,
            )

    try:
        server = ThreadingHTTPServer((host, port), Handler)
    except OSError as exc:
        logger.error("Failed to start health endpoint on %s:%s (%s)", host, port, exc)
        return None

    def _run() -> None:
        try:
            server.serve_forever()
        finally:
            server.server_close()

    thread = threading.Thread(target=_run, name=f"healthz-{service}", daemon=True)
    thread.start()
    logger.info("Health endpoint started on %s:%s for service=%s", host, port, service)
    return server


def main() -> None:
    """CLI entrypoint for health checks."""
    parser = argparse.ArgumentParser(description="Service health check.")
    parser.add_argument(
        "--service",
        choices=sorted(_SERVICE_KEYS),
        required=True,
        help="Service to check.",
    )
    parser.add_argument("--database-url", help="Override DATABASE_URL.")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    args = parser.parse_args()

    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required for health checks.")
    try:
        payload = check_service(args.service, database_url)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        payload = {"service": args.service, "ok": False, "error": str(exc) or "failed"}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        status = "ok" if payload.get("ok") else "failed"
        print(f"{args.service} health={status}")
    raise SystemExit(0 if payload.get("ok") else 1)


if __name__ == "__main__":
    main()
