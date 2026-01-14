"""Authentication helpers for the web portal."""

from __future__ import annotations

import os

from flask import session  # pylint: disable=import-error


def require_password() -> str:
    """Load the portal password or raise if missing."""
    password = os.getenv("WEB_PORTAL_PASSWORD")
    if not password:
        raise RuntimeError("WEB_PORTAL_PASSWORD is not set.")
    return password


def is_authenticated() -> bool:
    """Check whether the current session is authenticated."""
    return bool(session.get("web_portal_authed"))
