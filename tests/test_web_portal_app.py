import importlib
import os
import sys
import unittest
from unittest.mock import patch

from _test_utils import add_src_to_path

add_src_to_path()

app_module = importlib.import_module("src.web_portal.app")
from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix


class TestPortalAppCookies(unittest.TestCase):
    def test_apply_session_cookie_settings(self) -> None:
        app = Flask("test")
        with patch.dict(
            os.environ,
            {
                "WEB_PORTAL_COOKIE_SECURE": "1",
                "WEB_PORTAL_COOKIE_HTTPONLY": "0",
                "WEB_PORTAL_COOKIE_SAMESITE": "",
                "WEB_PORTAL_COOKIE_DOMAIN": " example.com ",
                "WEB_PORTAL_COOKIE_PATH": "",
                "WEB_PORTAL_COOKIE_NAME": " ",
            },
            clear=True,
        ):
            app_module._apply_session_cookie_settings(app)
        self.assertTrue(app.config["SESSION_COOKIE_SECURE"])
        self.assertFalse(app.config["SESSION_COOKIE_HTTPONLY"])
        self.assertIsNone(app.config["SESSION_COOKIE_SAMESITE"])
        self.assertEqual(app.config["SESSION_COOKIE_DOMAIN"], "example.com")
        self.assertEqual(app.config["SESSION_COOKIE_PATH"], "/")
        self.assertEqual(app.config["SESSION_COOKIE_NAME"], "session")

    def test_apply_session_cookie_settings_samesite_none(self) -> None:
        app = Flask("test")
        with patch.dict(
            os.environ,
            {"WEB_PORTAL_COOKIE_SAMESITE": "none"},
            clear=True,
        ):
            app_module._apply_session_cookie_settings(app)
        self.assertEqual(app.config["SESSION_COOKIE_SAMESITE"], "None")

    def test_apply_session_cookie_settings_invalid_samesite(self) -> None:
        app = Flask("test")
        with patch.dict(
            os.environ,
            {"WEB_PORTAL_COOKIE_SAMESITE": "not-valid"},
            clear=True,
        ):
            with patch.object(app_module.logger, "warning") as warning:
                app_module._apply_session_cookie_settings(app)
        warning.assert_called_once()


class TestPortalAppProxy(unittest.TestCase):
    def test_parse_proxy_count(self) -> None:
        self.assertEqual(app_module._parse_proxy_count("true"), 1)
        self.assertEqual(app_module._parse_proxy_count("2"), 2)
        self.assertEqual(app_module._parse_proxy_count("-1"), 0)
        self.assertEqual(app_module._parse_proxy_count("nope"), 0)

    def test_apply_proxy_fix_no_env(self) -> None:
        app = Flask("test")
        original = app.wsgi_app
        with patch.dict(os.environ, {}, clear=True):
            app_module._apply_proxy_fix(app)
        self.assertIs(app.wsgi_app, original)

    def test_apply_proxy_fix_disabled(self) -> None:
        app = Flask("test")
        original = app.wsgi_app
        with patch.dict(os.environ, {"WEB_PORTAL_TRUST_PROXY": "0"}, clear=True):
            app_module._apply_proxy_fix(app)
        self.assertIs(app.wsgi_app, original)

    def test_apply_proxy_fix_enabled(self) -> None:
        app = Flask("test")
        with patch.dict(os.environ, {"WEB_PORTAL_TRUST_PROXY": "2"}, clear=True):
            app_module._apply_proxy_fix(app)
        self.assertIsInstance(app.wsgi_app, ProxyFix)
        self.assertEqual(app.wsgi_app.x_for, 2)


class TestPortalAppStaticCache(unittest.TestCase):
    def test_apply_static_cache_headers_disabled(self) -> None:
        app = Flask("test")
        with patch.dict(
            os.environ,
            {"WEB_PORTAL_STATIC_CACHE_MAX_AGE_SEC": "0"},
            clear=True,
        ):
            app_module._apply_static_cache_headers(app)
        self.assertNotIn("SEND_FILE_MAX_AGE_DEFAULT", app.config)

    def test_apply_static_cache_headers_sets_cache(self) -> None:
        app = Flask("test")
        with patch.dict(
            os.environ,
            {"WEB_PORTAL_STATIC_CACHE_MAX_AGE_SEC": "120"},
            clear=True,
        ):
            app_module._apply_static_cache_headers(app)
        self.assertEqual(app.config.get("SEND_FILE_MAX_AGE_DEFAULT"), 120)
        with app.test_request_context("/static/foo"):
            response = app.response_class("ok")
            response = app.process_response(response)
        self.assertTrue(response.cache_control.public)
        self.assertEqual(response.cache_control.max_age, 120)
        self.assertEqual(response.headers.get("Cache-Control"), "public, max-age=120")

    def test_apply_static_cache_headers_non_static_request(self) -> None:
        app = Flask("test")
        with patch.dict(
            os.environ,
            {"WEB_PORTAL_STATIC_CACHE_MAX_AGE_SEC": "120"},
            clear=True,
        ):
            app_module._apply_static_cache_headers(app)
        with app.test_request_context("/health"):
            response = app.response_class("ok")
            response.headers["Cache-Control"] = "private"
            response = app.process_response(response)
        self.assertEqual(response.headers.get("Cache-Control"), "private")


class TestPortalAppEnvironment(unittest.TestCase):
    def test_load_dotenv_when_not_in_test(self) -> None:
        modules = sys.modules.copy()
        modules.pop("pytest", None)
        with patch.dict(os.environ, {"ENV_FILE": "/tmp/portal.env"}, clear=True):
            with patch.dict(sys.modules, modules, clear=True):
                with patch("dotenv.load_dotenv") as load_dotenv_mock, \
                     patch("src.web_portal.kalshi_sdk.configure_rest_rate_limit"), \
                     patch("src.core.logging_utils.configure_logging"):
                    importlib.reload(app_module)
        load_dotenv_mock.assert_called_once_with(dotenv_path="/tmp/portal.env")


class TestPortalAppCreate(unittest.TestCase):
    def test_create_app_calls_snapshot_ready_when_not_pytest(self) -> None:
        env = {"WEB_PORTAL_SECRET_KEY": "secret"}
        modules = sys.modules.copy()
        modules.pop("pytest", None)
        with patch.dict(os.environ, env, clear=True):
            with patch.dict(sys.modules, modules, clear=True):
                with patch.object(app_module, "ensure_portal_snapshot_ready") as ready, \
                     patch.object(app_module, "register_blueprints") as register_blueprints, \
                     patch.object(app_module, "register_routes") as register_routes:
                    app = app_module.create_app()
        ready.assert_called_once()
        register_blueprints.assert_called_once_with(app)
        register_routes.assert_called_once_with(app)


if __name__ == "__main__":
    unittest.main()
