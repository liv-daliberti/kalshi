import os
import sys
import types
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from _test_utils import add_src_to_path

add_src_to_path()

import src.web_portal.portal_app_factory as app_factory

_MISSING = object()


@contextmanager
def _patched_sys_modules(updates):
    saved = {}
    for name, module in updates.items():
        saved[name] = sys.modules.get(name, _MISSING)
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module
    try:
        yield
    finally:
        for name, previous in saved.items():
            if previous is _MISSING:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous


class TestPortalAppFactoryModule(unittest.TestCase):
    def test_portal_module_falls_back_to_web_portal(self) -> None:
        dummy_portal = types.SimpleNamespace()
        original_package = app_factory.__package__
        try:
            with _patched_sys_modules(
                {
                    "alt.web_portal": None,
                    "src.web_portal": None,
                    "web_portal": dummy_portal,
                }
            ):
                app_factory.__package__ = "alt.web_portal"
                self.assertIs(app_factory._portal_module(), dummy_portal)
        finally:
            app_factory.__package__ = original_package

    def test_portal_logger_defaults_to_local_logger(self) -> None:
        with patch.object(app_factory, "_portal_module", return_value=None):
            self.assertIs(app_factory._portal_logger(), app_factory._LOGGER)


class TestPortalAppFactoryRoutes(unittest.TestCase):
    def test_wire_portal_routes_logs_warning(self) -> None:
        logger = MagicMock()

        def bad_wire():
            raise RuntimeError("boom")

        with patch.object(app_factory, "_resolve_wire_routes", return_value=bad_wire):
            with patch.object(app_factory, "_portal_logger", return_value=logger):
                app_factory._wire_portal_routes(importlib_module=object())

        logger.warning.assert_called_once()
        self.assertIn("route wiring failed", logger.warning.call_args[0][0])

    def test_resolve_wire_routes_import_error_returns_none(self) -> None:
        stub_importlib = types.SimpleNamespace(
            import_module=MagicMock(side_effect=ImportError("boom"))
        )
        with patch.object(app_factory, "_portal_module", return_value=None):
            self.assertIsNone(app_factory._resolve_wire_routes(stub_importlib))
        stub_importlib.import_module.assert_called_once_with("src.web_portal._portal_part1")

    def test_resolve_wire_routes_import_success_returns_hook(self) -> None:
        hook = lambda: None
        stub_importlib = types.SimpleNamespace(
            import_module=MagicMock(
                return_value=types.SimpleNamespace(_wire_route_modules=hook)
            )
        )
        with patch.object(app_factory, "_portal_module", return_value=None):
            self.assertIs(app_factory._resolve_wire_routes(stub_importlib), hook)
        stub_importlib.import_module.assert_called_once_with("src.web_portal._portal_part1")


class TestPortalAppFactoryFallback(unittest.TestCase):
    def test_build_base_portal_app_uses_stub_on_flask_failure(self) -> None:
        class DummyFlask:
            def __init__(self, *_args, **_kwargs):
                raise RuntimeError("boom")

        logger = MagicMock()
        with patch.object(app_factory, "Flask", DummyFlask):
            with patch.object(app_factory, "_portal_logger", return_value=logger):
                with patch.dict(os.environ, {"WEB_PORTAL_SECRET_KEY": "sek"}, clear=True):
                    app = app_factory._build_base_portal_app("/tmp/templates")

        self.assertIsInstance(app, DummyFlask)
        self.assertEqual(app.template_folder, "/tmp/templates")
        self.assertEqual(app.secret_key, "sek")
        logger.warning.assert_called_once()
        self.assertIn("Flask init failed", logger.warning.call_args[0][0])

    def test_register_portal_blueprints_logs_warning(self) -> None:
        routes_module = types.ModuleType("src.web_portal.routes")

        def bad_register(_app):
            raise RuntimeError("boom")

        routes_module.register_blueprints = bad_register
        logger = MagicMock()
        with _patched_sys_modules({"src.web_portal.routes": routes_module}):
            with patch.object(app_factory, "_portal_logger", return_value=logger):
                app_factory._register_portal_blueprints(object())

        logger.warning.assert_called_once()
        self.assertIn("blueprint registration failed", logger.warning.call_args[0][0])

    def test_register_portal_routes_logs_warning(self) -> None:
        def bad_register(_app):
            raise RuntimeError("boom")

        portal_module = types.SimpleNamespace(register_routes=bad_register)
        logger = MagicMock()
        with patch.object(app_factory, "_portal_module", return_value=portal_module):
            with patch.object(app_factory, "_portal_logger", return_value=logger):
                app_factory._register_portal_routes(object())

        logger.warning.assert_called_once()
        self.assertIn("route registration failed", logger.warning.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
