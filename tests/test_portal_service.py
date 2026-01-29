import os
import runpy
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import importlib

from _test_utils import add_src_to_path

add_src_to_path()

portal_service = importlib.import_module("src.services.portal_service")


class TestPortalService(unittest.TestCase):
    def test_repo_root_inserted_when_run_as_script(self) -> None:
        module_path = Path(portal_service.__file__).resolve()
        repo_root = str(module_path.parents[2])
        original_sys_path = list(sys.path)
        try:
            sys.path = [entry for entry in sys.path if entry != repo_root]
            runpy.run_path(str(module_path))
            self.assertEqual(sys.path[0], repo_root)
        finally:
            sys.path = original_sys_path

    def test_ensure_env_defaults(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            portal_service._ensure_env()
            self.assertEqual(os.environ["SERVICE_ROLE"], "portal")
            self.assertEqual(os.environ["DB_INIT_SCHEMA"], "0")

    def test_ensure_env_respects_existing(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_ROLE": "rest", "DB_INIT_SCHEMA": "1"},
            clear=True,
        ):
            portal_service._ensure_env()
            self.assertEqual(os.environ["SERVICE_ROLE"], "rest")
            self.assertEqual(os.environ["DB_INIT_SCHEMA"], "1")

    def test_main_calls_web_portal(self) -> None:
        fake_portal = SimpleNamespace(main=Mock())
        with patch("src.services.portal_service._ensure_env") as ensure_env, \
             patch("src.services.portal_service._load_web_portal") as load_portal:
            load_portal.return_value = fake_portal
            portal_service.main()
        ensure_env.assert_called_once()
        load_portal.assert_called_once()
        fake_portal.main.assert_called_once()

    def test_portal_db_url_override(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://primary",
                "DATABASE_URL_PORTAL": "postgresql://replica",
                "DATABASE_URL_REST": "postgresql://rest",
            },
            clear=True,
        ):
            portal_service._ensure_env()
            self.assertEqual(os.environ["DATABASE_URL"], "postgresql://replica")
            self.assertNotIn("DATABASE_URL_PORTAL", os.environ)
            self.assertNotIn("DATABASE_URL_REST", os.environ)
