import importlib.util
import os
import runpy
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

from _test_utils import add_src_to_path

add_src_to_path()

import src.services.rag_service as rag_service
import src.services.rest_service as rest_service


class TestRagService(unittest.TestCase):
    def test_ensure_env_preserves_defaults(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_ROLE": "custom", "DB_INIT_SCHEMA": "1"},
            clear=True,
        ):
            rag_service._ensure_env()
            self.assertEqual(os.environ["KALSHI_RUN_MODE"], "rag")
            self.assertEqual(os.environ["SERVICE_ROLE"], "custom")
            self.assertEqual(os.environ["DB_INIT_SCHEMA"], "1")

    def test_main_runs_asyncio(self) -> None:
        with patch.dict(os.environ, {}, clear=True), \
             patch("src.services.rag_service.asyncio.run") as run, \
             patch("src.services.rag_service.ingestor_main.main") as main:
            rag_service.main()
        main.assert_called_once()
        run.assert_called_once()

    def test_module_runs_as_main(self) -> None:
        module = types.ModuleType("src.services.main")
        module.main = MagicMock(return_value="coro")
        services_pkg = sys.modules.get("src.services")
        prev_attr = getattr(services_pkg, "main", None) if services_pkg else None
        prev = sys.modules.get("src.services.main")
        sys.modules["src.services.main"] = module
        if services_pkg is not None:
            services_pkg.main = module
        prev_rag = sys.modules.pop("src.services.rag_service", None)
        try:
            def fake_run(coro):
                if hasattr(coro, "close"):
                    coro.close()

            with patch("asyncio.run", side_effect=fake_run) as run:
                runpy.run_path(rag_service.__file__, run_name="__main__")
        finally:
            if prev is None:
                sys.modules.pop("src.services.main", None)
            else:
                sys.modules["src.services.main"] = prev
            if services_pkg is not None:
                if prev_attr is None:
                    delattr(services_pkg, "main")
                else:
                    services_pkg.main = prev_attr
            if prev_rag is not None:
                sys.modules["src.services.rag_service"] = prev_rag
        module.main.assert_called_once()
        run.assert_called_once()


class TestRestService(unittest.TestCase):
    def test_ensure_env_preserves_defaults(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_ROLE": "custom", "DB_INIT_SCHEMA": "1"},
            clear=True,
        ):
            rest_service._ensure_env()
            self.assertEqual(os.environ["KALSHI_RUN_MODE"], "rest")
            self.assertEqual(os.environ["SERVICE_ROLE"], "custom")
            self.assertEqual(os.environ["DB_INIT_SCHEMA"], "1")

    def test_main_runs_asyncio(self) -> None:
        with patch.dict(os.environ, {}, clear=True), \
             patch("src.services.rest_service.asyncio.run") as run, \
             patch("src.services.rest_service.ingestor_main.main") as main:
            rest_service.main()
        main.assert_called_once()
        run.assert_called_once()

    def test_module_runs_as_main(self) -> None:
        module = types.ModuleType("src.services.main")
        module.main = MagicMock(return_value="coro")
        services_pkg = sys.modules.get("src.services")
        prev_attr = getattr(services_pkg, "main", None) if services_pkg else None
        prev = sys.modules.get("src.services.main")
        sys.modules["src.services.main"] = module
        if services_pkg is not None:
            services_pkg.main = module
        prev_rest = sys.modules.pop("src.services.rest_service", None)
        try:
            def fake_run(coro):
                if hasattr(coro, "close"):
                    coro.close()

            with patch("asyncio.run", side_effect=fake_run) as run:
                runpy.run_path(rest_service.__file__, run_name="__main__")
        finally:
            if prev is None:
                sys.modules.pop("src.services.main", None)
            else:
                sys.modules["src.services.main"] = prev
            if services_pkg is not None:
                if prev_attr is None:
                    delattr(services_pkg, "main")
                else:
                    services_pkg.main = prev_attr
            if prev_rest is not None:
                sys.modules["src.services.rest_service"] = prev_rest
        module.main.assert_called_once()
        run.assert_called_once()


class TestPortalService(unittest.TestCase):
    def test_module_runs_as_main(self) -> None:
        module = types.ModuleType("src.web_portal")
        module.main = MagicMock()
        src_pkg = sys.modules.get("src")
        prev_attr = getattr(src_pkg, "web_portal", None) if src_pkg else None
        prev = sys.modules.get("src.web_portal")
        sys.modules["src.web_portal"] = module
        if src_pkg is not None:
            src_pkg.web_portal = module
        prev_portal = sys.modules.pop("src.services.portal_service", None)
        spec = importlib.util.find_spec("src.services.portal_service")
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.origin)
        try:
            runpy.run_path(spec.origin, run_name="__main__")
        finally:
            if prev is None:
                sys.modules.pop("src.web_portal", None)
            else:
                sys.modules["src.web_portal"] = prev
            if src_pkg is not None:
                if prev_attr is None:
                    delattr(src_pkg, "web_portal")
                else:
                    src_pkg.web_portal = prev_attr
            if prev_portal is not None:
                sys.modules["src.services.portal_service"] = prev_portal
        module.main.assert_called_once()
