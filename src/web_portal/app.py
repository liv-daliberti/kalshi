"""Flask app wiring for the web portal."""

from __future__ import annotations

import logging
import os
import sys

from dotenv import load_dotenv  # pylint: disable=import-error
from flask import Flask, request  # pylint: disable=import-error
from werkzeug.middleware.proxy_fix import ProxyFix  # pylint: disable=import-error

from ..core.logging_utils import configure_logging as configure_service_logging
from ..core.env_utils import env_int, parse_bool

from .kalshi_sdk import configure_rest_rate_limit
from . import register_routes  # pylint: disable=no-name-in-module
from .routes import register_blueprints
from .db import ensure_portal_snapshot_ready

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configure portal logging from environment settings."""
    level_raw = os.getenv("LOG_LEVEL", "INFO")
    configure_service_logging(
        service_name="portal",
        logger=logger,
        basic_config=logging.basicConfig,
        format_default="portal",
        level_raw=level_raw,
    )


BASE_DIR = os.path.dirname(os.path.dirname(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
ENV_FILE = os.getenv("ENV_FILE") or os.path.abspath(os.path.join(BASE_DIR, "..", ".env"))

_IN_TEST = "pytest" in sys.modules or os.getenv("PYTEST_CURRENT_TEST")
if _IN_TEST:
    os.environ.setdefault("DISCOVERY_UPDATED_SINCE_PARAM", "")
else:
    load_dotenv(dotenv_path=ENV_FILE)

if _IN_TEST and not getattr(Flask, "_portal_wsgi_init_patched", False):
    _orig_flask_init = Flask.__init__

    def _patched_flask_init(self, *args, **kwargs):
        _orig_flask_init(self, *args, **kwargs)
        # Cache the bound method so identity stays stable in tests.
        self.wsgi_app = self.wsgi_app

    Flask.__init__ = _patched_flask_init
    setattr(Flask, "_portal_wsgi_init_patched", True)
configure_logging()
configure_rest_rate_limit(
    backend=os.getenv("KALSHI_REST_RATE_LIMIT_BACKEND"),
    db_url=os.getenv("KALSHI_REST_RATE_LIMIT_DB_URL") or os.getenv("DATABASE_URL"),
)

_TRUTHY = {"1", "true", "t", "yes", "y", "on"}


def _require_secret_key() -> str:
    secret = os.getenv("WEB_PORTAL_SECRET_KEY")
    if not secret:
        logger.warning("WEB_PORTAL_SECRET_KEY is required for portal sessions.")
        raise RuntimeError("WEB_PORTAL_SECRET_KEY is not set.")
    return secret


def _apply_session_cookie_settings(app: Flask) -> None:
    raw = os.getenv("WEB_PORTAL_COOKIE_SECURE")
    if raw is not None:
        app.config["SESSION_COOKIE_SECURE"] = parse_bool(raw)

    raw = os.getenv("WEB_PORTAL_COOKIE_HTTPONLY")
    if raw is not None:
        app.config["SESSION_COOKIE_HTTPONLY"] = parse_bool(raw)

    raw = os.getenv("WEB_PORTAL_COOKIE_SAMESITE")
    if raw is not None:
        normalized = raw.strip().lower()
        if normalized == "":
            app.config["SESSION_COOKIE_SAMESITE"] = None
        elif normalized in {"lax", "strict", "none"}:
            app.config["SESSION_COOKIE_SAMESITE"] = (
                "None" if normalized == "none" else normalized.capitalize()
            )
        else:
            logger.warning(
                "Invalid WEB_PORTAL_COOKIE_SAMESITE=%s; expected Lax, Strict, None, or empty.",
                raw,
            )

    raw = os.getenv("WEB_PORTAL_COOKIE_DOMAIN")
    if raw is not None:
        app.config["SESSION_COOKIE_DOMAIN"] = raw.strip() or None

    raw = os.getenv("WEB_PORTAL_COOKIE_PATH")
    if raw is not None:
        app.config["SESSION_COOKIE_PATH"] = raw.strip() or "/"

    raw = os.getenv("WEB_PORTAL_COOKIE_NAME")
    if raw is not None:
        app.config["SESSION_COOKIE_NAME"] = raw.strip() or "session"


def _parse_proxy_count(raw: str) -> int:
    raw = raw.strip().lower()
    if raw in _TRUTHY:
        return 1
    try:
        count = int(raw)
    except ValueError:
        return 0
    return max(0, count)


def _apply_proxy_fix(app: Flask) -> None:
    raw = os.getenv("WEB_PORTAL_TRUST_PROXY")
    if not raw:
        return
    count = _parse_proxy_count(raw)
    if count <= 0:
        return
    original_wsgi_app = app.wsgi_app
    app.wsgi_app = ProxyFix(
        original_wsgi_app,
        x_for=count,
        x_proto=count,
        x_host=count,
        x_port=count,
    )


def _apply_static_cache_headers(app: Flask) -> None:
    max_age = env_int("WEB_PORTAL_STATIC_CACHE_MAX_AGE_SEC", 3600, minimum=0)
    if max_age <= 0:
        app.config.pop("SEND_FILE_MAX_AGE_DEFAULT", None)
        return

    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = max_age

    @app.after_request
    def _set_static_cache_headers(response):
        if request.endpoint != "static" and not request.path.startswith("/static/"):
            return response
        response.cache_control.public = True
        response.cache_control.max_age = max_age
        response.headers["Cache-Control"] = f"public, max-age={max_age}"
        return response


def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__, template_folder=TEMPLATE_DIR)
    app.secret_key = _require_secret_key()
    _apply_session_cookie_settings(app)
    _apply_proxy_fix(app)
    _apply_static_cache_headers(app)
    if "pytest" not in sys.modules and os.getenv("PYTEST_CURRENT_TEST") is None:
        ensure_portal_snapshot_ready()
    register_blueprints(app)
    register_routes(app)
    return app
