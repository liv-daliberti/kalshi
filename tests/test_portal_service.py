import os
import unittest
from unittest.mock import patch

from _test_utils import add_src_to_path

add_src_to_path()

import src.services.portal_service as portal_service


class TestPortalService(unittest.TestCase):
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
        with patch("src.services.portal_service._ensure_env") as ensure_env, \
             patch("src.services.portal_service.web_portal.main") as portal_main:
            portal_service.main()
        ensure_env.assert_called_once()
        portal_main.assert_called_once()
