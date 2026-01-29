import unittest
from types import SimpleNamespace
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path

add_src_to_path()

db_utils = importlib.import_module("src.ingest.ws.ws_ingest_db_utils")


class TestWsIngestDbUtils(unittest.TestCase):
    def test_require_psycopg_returns_module(self) -> None:
        fake_module = SimpleNamespace(name="psycopg")
        with patch.object(db_utils, "_load_psycopg", return_value=fake_module):
            self.assertIs(db_utils._require_psycopg(), fake_module)

    def test_psycopg_error_type_fallback(self) -> None:
        with patch.object(db_utils, "_load_psycopg", side_effect=RuntimeError("missing")):
            self.assertIs(db_utils._psycopg_error_type(), db_utils.PsycopgError)

    def test_psycopg_privilege_error_type_fallback(self) -> None:
        with patch.object(db_utils, "_load_psycopg", side_effect=RuntimeError("missing")):
            self.assertIs(
                db_utils._psycopg_privilege_error_type(),
                db_utils.InsufficientPrivilegeError,
            )
