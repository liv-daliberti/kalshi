"""Blueprint registration for web portal routes."""

from __future__ import annotations

from flask import Flask  # pylint: disable=import-error

from .auth import bp as auth_bp
from .event import bp as event_bp
from .health import bp as health_bp
from .market import bp as market_bp
from .opportunities import bp as opportunities_bp
from .stream import bp as stream_bp


def register_blueprints(app: Flask) -> None:
    """Register Flask blueprints for portal routes."""
    app.register_blueprint(health_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(event_bp)
    app.register_blueprint(market_bp)
    app.register_blueprint(opportunities_bp)
    app.register_blueprint(stream_bp)
