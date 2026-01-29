"""App factory helpers for the web portal."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

from flask import Flask  # pylint: disable=import-error

from .config import _env_bool

_LOGGER = logging.getLogger(__name__)


def _portal_module():
    module_name = __package__ or "src.web_portal"
    module = sys.modules.get(module_name)
    if module is None and module_name != "src.web_portal":
        module = sys.modules.get("src.web_portal")
    if module is None:
        module = sys.modules.get("web_portal")
    return module


def _portal_logger():
    portal_module = _portal_module()
    if portal_module is not None:
        try:
            namespace = getattr(portal_module, "__dict__", None)
        except (AttributeError, TypeError):  # pragma: no cover - defensive fallback
            namespace = None
        logger = None
        if isinstance(namespace, dict):
            logger = namespace.get("logger")
        else:
            try:
                logger = getattr(portal_module, "logger", None)
            except (AttributeError, TypeError):  # pragma: no cover - defensive fallback
                logger = None
        if logger is not None:
            return logger
    return _LOGGER


def _create_portal_app() -> Flask:
    """Build a portal Flask app with a safe fallback for tests."""
    import importlib  # pylint: disable=import-outside-toplevel

    require_snapshot = _env_bool("WEB_PORTAL_DB_SNAPSHOT_REQUIRE", False)
    try:
        _wire_portal_routes(importlib)
        app_module = importlib.import_module(".app", __package__)
        return app_module.create_app()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if require_snapshot:
            raise
        _portal_logger().warning("Portal app fallback; create_app failed: %s", exc)
        try:
            return _build_fallback_portal_app()
        except Exception as fallback_exc:  # pylint: disable=broad-exception-caught
            _portal_logger().warning(
                "Portal app fallback failed; returning minimal app: %s",
                fallback_exc,
            )
            return _build_base_portal_app(_portal_template_dir())


def _wire_portal_routes(importlib_module: Any) -> None:
    """Wire route modules best-effort before app creation."""
    wire_routes = _resolve_wire_routes(importlib_module)
    if callable(wire_routes):
        try:
            wire_routes()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _portal_logger().warning("Portal app fallback; route wiring failed: %s", exc)


def _resolve_wire_routes(importlib_module: Any) -> Any:
    """Resolve the optional route wiring hook."""
    portal_module = _portal_module()
    wire_routes = None
    if portal_module is not None:
        wire_routes = getattr(portal_module, "_wire_route_modules", None)
    if callable(wire_routes):
        return wire_routes
    try:
        part1 = importlib_module.import_module("src.web_portal._portal_part1")
    except Exception:  # pylint: disable=broad-exception-caught
        return None
    return getattr(part1, "_wire_route_modules", None)


def _build_fallback_portal_app() -> Flask:
    """Build a fallback app when the main factory fails."""
    template_dir = _portal_template_dir()
    portal_app = _build_base_portal_app(template_dir)
    _register_portal_blueprints(portal_app)
    _register_portal_routes(portal_app)
    return portal_app


def _portal_template_dir() -> str:
    base_dir = os.path.dirname(os.path.dirname(__file__))
    return os.path.join(base_dir, "templates")


def _build_base_portal_app(template_dir: str) -> Flask:
    try:
        portal_app = Flask(__name__, template_folder=template_dir)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _portal_logger().warning("Portal app fallback; Flask init failed: %s", exc)
        portal_app = _create_stub_flask(template_dir)
    portal_app.secret_key = os.getenv("WEB_PORTAL_SECRET_KEY") or "dev"
    return portal_app


def _create_stub_flask(template_dir: str) -> Flask:
    portal_app = Flask.__new__(Flask)
    try:
        portal_app.template_folder = template_dir
    except (AttributeError, TypeError):  # pragma: no cover - best-effort for stub app
        pass
    return portal_app


def _register_portal_blueprints(portal_app: Flask) -> None:
    try:
        from .routes import register_blueprints  # pylint: disable=import-outside-toplevel

        register_blueprints(portal_app)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _portal_logger().warning(
            "Portal app fallback; blueprint registration failed: %s", exc
        )


def _register_portal_routes(portal_app: Flask) -> None:
    register_routes_fn = None
    portal_module = _portal_module()
    if portal_module is not None:
        try:
            namespace = getattr(portal_module, "__dict__", None)
        except (AttributeError, TypeError):  # pragma: no cover - defensive fallback
            namespace = None
        if isinstance(namespace, dict):
            register_routes_fn = namespace.get("register_routes")
        else:
            try:
                register_routes_fn = getattr(portal_module, "register_routes", None)
            except (AttributeError, TypeError):  # pragma: no cover - defensive fallback
                register_routes_fn = None
    if callable(register_routes_fn):
        try:
            register_routes_fn(portal_app)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _portal_logger().warning(
                "Portal app fallback; route registration failed: %s", exc
            )
