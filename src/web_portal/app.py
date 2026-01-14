"""Flask app wiring for the web portal."""

from __future__ import annotations

import logging
import os

from dotenv import load_dotenv  # pylint: disable=import-error
from flask import Flask  # pylint: disable=import-error

from src.core.logging_utils import configure_logging as configure_service_logging

from .kalshi_sdk import configure_rest_rate_limit
from .routes import register_blueprints

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

load_dotenv(dotenv_path=ENV_FILE)
configure_logging()
configure_rest_rate_limit(
    backend=os.getenv("KALSHI_REST_RATE_LIMIT_BACKEND"),
    db_url=os.getenv("KALSHI_REST_RATE_LIMIT_DB_URL") or os.getenv("DATABASE_URL"),
)


def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__, template_folder=TEMPLATE_DIR)
    app.secret_key = os.getenv("WEB_PORTAL_SECRET_KEY", os.urandom(24))
    register_blueprints(app)
    from . import register_routes
    register_routes(app)
    return app
