"""Authentication routes."""

from __future__ import annotations

import hmac

from flask import (  # pylint: disable=import-error
    Blueprint,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from ..auth_utils import require_password

bp = Blueprint("auth", __name__)


@bp.route("/login", methods=["GET", "POST"])
def login():
    """Render or handle login form."""
    error = None
    next_path = request.args.get("next") or "/"
    if not next_path.startswith("/"):
        next_path = "/"
    if request.method == "POST":
        try:
            expected = require_password()
        except RuntimeError as exc:
            error = str(exc)
        else:
            provided = request.form.get("password", "")
            if hmac.compare_digest(provided, expected):
                session["web_portal_authed"] = True
                return redirect(next_path)
            error = "Invalid password."

    return render_template(
        "login.html",
        error=error,
        next_path=next_path,
    )


@bp.get("/logout")
def logout():
    """Clear login session."""
    session.clear()
    return redirect(url_for("auth.login"))
