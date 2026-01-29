import io
import logging
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
import importlib

from _test_utils import add_src_to_path

add_src_to_path()

logging_utils = importlib.import_module("src.core.logging_utils")


class TestLoggingUtilsBasics(unittest.TestCase):
    def test_service_label_priority(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_ROLE": " ReSt ", "KALSHI_RUN_MODE": "ws", "KALSHI_SERVICE": "svc"},
            clear=True,
        ):
            self.assertEqual(logging_utils.service_label(), "rest")
        with patch.dict(
            os.environ,
            {"KALSHI_RUN_MODE": "ws", "KALSHI_SERVICE": "svc"},
            clear=True,
        ):
            self.assertEqual(logging_utils.service_label(), "ws")
        with patch.dict(os.environ, {"KALSHI_SERVICE": "svc"}, clear=True):
            self.assertEqual(logging_utils.service_label(), "svc")
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(logging_utils.service_label(default="fallback"), "fallback")

    def test_log_format_includes_label(self) -> None:
        with patch.dict(os.environ, {"SERVICE_ROLE": "rag"}, clear=True):
            fmt = logging_utils.log_format()
        self.assertIn("rag", fmt)

    def test_parse_log_level_and_known(self) -> None:
        self.assertEqual(logging_utils.parse_log_level(None, logging.WARNING), logging.WARNING)
        self.assertEqual(logging_utils.parse_log_level("10", logging.INFO), 10)
        self.assertEqual(logging_utils.parse_log_level("debug", logging.INFO), logging.DEBUG)
        self.assertEqual(logging_utils.parse_log_level("unknown", logging.INFO), logging.INFO)
        self.assertFalse(logging_utils.is_known_log_level(None))
        self.assertTrue(logging_utils.is_known_log_level("10"))
        self.assertTrue(logging_utils.is_known_log_level("INFO"))

    def test_truthy_and_sanitize(self) -> None:
        self.assertFalse(logging_utils._truthy(None))
        self.assertTrue(logging_utils._truthy(" yes "))
        self.assertEqual(logging_utils._sanitize_filename("Bad Name!"), "Bad_Name_")
        self.assertEqual(logging_utils._sanitize_filename(""), "service")


class TestLoggingUtilsPaths(unittest.TestCase):
    def test_resolve_log_file_from_env(self) -> None:
        with patch.dict(os.environ, {"LOG_FILE": "~/test.log"}, clear=True):
            expected = os.path.expanduser("~/test.log")
            self.assertEqual(logging_utils._resolve_log_file("svc"), expected)

    def test_resolve_log_file_from_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(logging_utils, "service_label", return_value="My App"):
                with patch.dict(os.environ, {"LOG_DIR": tmp}, clear=True):
                    resolved = logging_utils._resolve_log_file("svc")
        self.assertEqual(resolved, str(Path(tmp) / "My_App.log"))

    def test_ensure_log_dir_no_parent(self) -> None:
        class DummyPath:
            parent = None

        self.assertTrue(logging_utils._ensure_log_dir(DummyPath()))

    def test_ensure_log_dir_unwritable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "nested" / "file.log"
            with patch("os.access", return_value=False), patch(
                "sys.stderr", new=io.StringIO()
            ):
                self.assertFalse(logging_utils._ensure_log_dir(target))

    def test_ensure_log_dir_mkdir_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "nested" / "file.log"
            with patch.object(Path, "mkdir", side_effect=OSError("boom")), patch(
                "sys.stderr", new=io.StringIO()
            ):
                self.assertFalse(logging_utils._ensure_log_dir(target))

    def test_ensure_log_dir_creates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "nested" / "file.log"
            self.assertTrue(logging_utils._ensure_log_dir(target))
            self.assertTrue(target.parent.exists())


class TestLoggingHandlers(unittest.TestCase):
    def test_log_handlers_file_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "service.log"
            with patch.dict(
                os.environ,
                {"LOG_FILE": str(log_path), "LOG_STDOUT": "0"},
                clear=True,
            ):
                handlers = logging_utils.log_handlers("svc")
        self.assertTrue(any(isinstance(handler, logging.FileHandler) for handler in handlers))
        self.assertFalse(
            any(
                isinstance(handler, logging.StreamHandler)
                and not isinstance(handler, logging.FileHandler)
                for handler in handlers
            )
        )

    def test_log_handlers_file_error_falls_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "service.log"
            with patch.dict(
                os.environ,
                {"LOG_FILE": str(log_path), "LOG_STDOUT": "0"},
                clear=True,
            ), patch("logging.FileHandler", side_effect=OSError("boom")), patch(
                "sys.stderr", new=io.StringIO()
            ):
                handlers = logging_utils.log_handlers("svc")
        self.assertTrue(any(isinstance(handler, logging.StreamHandler) for handler in handlers))


class TestLoggingConfiguration(unittest.TestCase):
    def test_configure_logging_default_level_env(self) -> None:
        logger = MagicMock()
        basic_config = MagicMock()
        with patch.dict(os.environ, {"LOG_LEVEL": "INFO"}, clear=True), patch.object(
            logging_utils, "log_handlers", return_value=[MagicMock()]
        ), patch.object(logging_utils, "configure_http_logging") as http_cfg, patch.object(
            logging_utils, "configure_ws_logging"
        ) as ws_cfg:
            level = logging_utils.configure_logging(
                service_name="svc",
                logger=logger,
                basic_config=basic_config,
                format_default="svc",
                level_raw=None,
            )
        self.assertEqual(level, logging.INFO)
        http_cfg.assert_called_once()
        ws_cfg.assert_called_once()

    def test_configure_http_logging_raw(self) -> None:
        fake_logger = MagicMock()
        with patch.dict(os.environ, {"LOG_HTTP_LEVEL": "ERROR"}, clear=True), patch(
            "logging.getLogger", return_value=fake_logger
        ):
            logging_utils.configure_http_logging()
        fake_logger.setLevel.assert_called_with(logging.ERROR)

    def test_configure_http_logging_debug_default(self) -> None:
        fake_logger = MagicMock()
        with patch.dict(os.environ, {}, clear=True), patch(
            "logging.getLogger", return_value=fake_logger
        ):
            logging_utils.configure_http_logging(default_level=logging.DEBUG)
        fake_logger.setLevel.assert_called_with(logging.WARNING)

    def test_configure_ws_logging_debug_default(self) -> None:
        fake_logger = MagicMock()
        with patch.dict(os.environ, {}, clear=True), patch(
            "logging.getLogger", return_value=fake_logger
        ):
            logging_utils.configure_ws_logging(default_level=logging.DEBUG)
        fake_logger.setLevel.assert_called_with(logging.WARNING)

    def test_configure_ws_logging_raw(self) -> None:
        fake_logger = MagicMock()
        with patch.dict(os.environ, {"LOG_WS_LEVEL": "ERROR"}, clear=True), patch(
            "logging.getLogger", return_value=fake_logger
        ):
            logging_utils.configure_ws_logging()
        fake_logger.setLevel.assert_called_with(logging.ERROR)
