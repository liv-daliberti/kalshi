"""Health view routes."""

from __future__ import annotations

from datetime import datetime, timezone

from flask import Blueprint, render_template  # pylint: disable=import-error

from ..auth_utils import is_authenticated
from ..formatters import fmt_ts
from ..health_utils import _build_health_cards

bp = Blueprint("health", __name__)


@bp.get("/health")
def health():
    """Render the application health view."""
    return render_template(
        "health.html",
        status_cards=_build_health_cards(),
        refreshed_at=fmt_ts(datetime.now(timezone.utc)),
        logged_in=is_authenticated(),
    )
