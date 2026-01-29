"""Streaming routes."""

from __future__ import annotations

import json
import logging
import os
import time

from flask import Blueprint, Response, jsonify, stream_with_context  # pylint: disable=import-error

from ..db import _db_connection, maybe_refresh_portal_snapshot
from ..db_timing import timed_cursor
from ..portal_utils import portal_func as _portal_func
from ..queue_stream_utils import _queue_stream_enabled

bp = Blueprint("stream", __name__)
logger = logging.getLogger(__name__)


@bp.get("/stream/queue")
def queue_stream():
    """Stream queue updates to the frontend via SSE."""
    queue_stream_enabled = _portal_func("_queue_stream_enabled", _queue_stream_enabled)
    if not queue_stream_enabled():
        return jsonify({"error": "Queue stream disabled."}), 404
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return jsonify({"error": "DATABASE_URL is not set."}), 503

    def event_stream():
        db_connection = _portal_func("_db_connection", _db_connection)
        maybe_refresh = _portal_func(
            "maybe_refresh_portal_snapshot",
            maybe_refresh_portal_snapshot,
        )
        try:
            # Use a direct connection for long-lived LISTEN to avoid pool starvation.
            with db_connection(autocommit=True, force_direct=True) as conn:
                with timed_cursor(conn) as cur:
                    cur.execute("LISTEN work_queue_update")
                last_ping = time.monotonic()
                while True:
                    for notify in conn.notifies(timeout=1.0):
                        payload = notify.payload or "{}"
                        try:
                            maybe_refresh(reason="queue", background=True)
                        except Exception:  # pylint: disable=broad-exception-caught
                            logger.exception("queue stream snapshot refresh failed")
                        yield f"event: queue\ndata: {payload}\n\n"
                    now = time.monotonic()
                    if now - last_ping >= 15:
                        yield "event: ping\ndata: {}\n\n"
                        last_ping = now
        except GeneratorExit:
            return
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.exception("queue stream failed")
            payload = json.dumps(
                {"error": str(exc) or "Queue stream failed."},
                separators=(",", ":"),
                ensure_ascii=True,
            )
            yield f"event: error\ndata: {payload}\n\n"

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
        headers=headers,
    )
