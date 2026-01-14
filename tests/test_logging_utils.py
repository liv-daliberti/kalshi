import os
import unittest
from unittest.mock import patch

from _test_utils import add_src_to_path

add_src_to_path()

import src.core.logging_utils as logging_utils


class TestLoggingUtils(unittest.TestCase):
    def test_service_label_priority(self) -> None:
        with self.subTest("service_role"):
            with patch.dict(
                os.environ,
                {"SERVICE_ROLE": "Portal", "KALSHI_RUN_MODE": "rest"},
                clear=True,
            ):
                self.assertEqual(logging_utils.service_label(), "portal")
        with self.subTest("run_mode"):
            with patch.dict(
                os.environ,
                {"KALSHI_RUN_MODE": "WS", "KALSHI_SERVICE": "other"},
                clear=True,
            ):
                self.assertEqual(logging_utils.service_label(), "ws")
        with self.subTest("service_name"):
            with patch.dict(
                os.environ,
                {"KALSHI_SERVICE": "RAG"},
                clear=True,
            ):
                self.assertEqual(logging_utils.service_label(), "rag")

    def test_service_label_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(logging_utils.service_label("fallback"), "fallback")

    def test_log_format(self) -> None:
        with patch.dict(os.environ, {"SERVICE_ROLE": "WS"}, clear=True):
            fmt = logging_utils.log_format()
        self.assertIn("ws", fmt)
