import os
import runpy
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()


class DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestMigrator(unittest.TestCase):
    def test_module_runs_as_main(self) -> None:
        module = types.ModuleType("src.db.db")
        module.init_schema = MagicMock()
        module.ensure_schema_compatible = MagicMock(return_value=1)
        prev_db = sys.modules.get("src.db.db")
        prev_migrator = sys.modules.pop("src.services.migrator", None)
        sys.modules["src.db.db"] = module
        try:
            with patch.dict(os.environ, {"DATABASE_URL": "db"}, clear=True), \
                 patch("dotenv.load_dotenv"), \
                 patch("psycopg.connect", return_value=DummyConn(), create=True):
                runpy.run_module("src.services.migrator", run_name="__main__")
        finally:
            if prev_db is None:
                sys.modules.pop("src.db.db", None)
            else:
                sys.modules["src.db.db"] = prev_db
            if prev_migrator is not None:
                sys.modules["src.services.migrator"] = prev_migrator
        module.init_schema.assert_called_once()

    def test_main_requires_database_url(self) -> None:
        import src.services.migrator as migrator
        with patch.dict(os.environ, {}, clear=True), \
             patch("src.services.migrator.load_dotenv"), \
             patch("src.services.migrator.logging.basicConfig"):
            with self.assertRaises(RuntimeError):
                migrator.main()
