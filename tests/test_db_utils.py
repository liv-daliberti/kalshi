import unittest
from unittest.mock import MagicMock
import importlib

from _test_utils import add_src_to_path

add_src_to_path()

db_utils = importlib.import_module("src.core.db_utils")


class TestSafeClose(unittest.TestCase):
    def test_safe_close_none(self) -> None:
        db_utils.safe_close(None)

    def test_safe_close_success(self) -> None:
        conn = MagicMock()
        db_utils.safe_close(conn)
        conn.close.assert_called_once()

    def test_safe_close_logs_default_message(self) -> None:
        conn = MagicMock()
        conn.close.side_effect = RuntimeError("boom")
        logger = MagicMock()
        db_utils.safe_close(conn, logger=logger)
        logger.warning.assert_called_once_with("DB close failed")

    def test_safe_close_logs_custom_message(self) -> None:
        conn = MagicMock()
        conn.close.side_effect = RuntimeError("boom")
        logger = MagicMock()
        db_utils.safe_close(conn, logger=logger, warn_message="custom")
        logger.warning.assert_called_once_with("custom")
