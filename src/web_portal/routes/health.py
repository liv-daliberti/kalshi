"""Health view routes."""

from __future__ import annotations

from datetime import datetime, timezone

from flask import Blueprint, jsonify, render_template  # pylint: disable=import-error

from ..auth_utils import is_authenticated
from ..formatters import fmt_ts
from ..health_utils import _build_health_cards
from ..portal_utils import portal_func as _portal_func

bp = Blueprint("health", __name__)


@bp.get("/health")
def health():
    """Render the application health view."""
    render = _portal_func("render_template", render_template)
    build_cards = _portal_func("_build_health_cards", _build_health_cards)
    auth_check = _portal_func("is_authenticated", is_authenticated)
    fmt = _portal_func("fmt_ts", fmt_ts)
    return render(
        "health.html",
        status_cards=build_cards(),
        refreshed_at=fmt(datetime.now(timezone.utc)),
        logged_in=auth_check(),
    )


@bp.get("/health/data")
def health_data():
    """Return health data as JSON for incremental updates."""
    now = datetime.now(timezone.utc)
    build_cards = _portal_func("_build_health_cards", _build_health_cards)
    auth_check = _portal_func("is_authenticated", is_authenticated)
    fmt = _portal_func("fmt_ts", fmt_ts)
    return jsonify(
        {
            "status_cards": build_cards(),
            "refreshed_at": fmt(now),
            "logged_in": auth_check(),
        }
    )
