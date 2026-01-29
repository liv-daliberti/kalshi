import sys
import types
import unittest
from contextlib import contextmanager
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from unittest.mock import patch

from _test_utils import add_src_to_path

add_src_to_path()

DB_ROW_PATH = Path(__file__).resolve().parents[1] / "src" / "web_portal" / "db_row.py"


@contextmanager
def _db_row_module(name: str):
    spec = spec_from_file_location(name, DB_ROW_PATH)
    assert spec and spec.loader
    module = module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    try:
        yield module
    finally:
        sys.modules.pop(name, None)


class TestDbRow(unittest.TestCase):
    def test_dict_row_available(self) -> None:
        rows_stub = types.SimpleNamespace(dict_row=object())
        psycopg_stub = types.SimpleNamespace(rows=rows_stub)
        with patch.dict(sys.modules, {"psycopg": psycopg_stub, "psycopg.rows": rows_stub}):
            with _db_row_module("db_row_available") as module:
                self.assertIs(module.DICT_ROW, rows_stub.dict_row)
                self.assertEqual(module.__all__, ["DICT_ROW"])

    def test_dict_row_missing(self) -> None:
        with patch.dict(sys.modules, {"psycopg": None, "psycopg.rows": None}):
            with _db_row_module("db_row_missing") as module:
                self.assertIsNone(module.DICT_ROW)
                self.assertEqual(module.__all__, ["DICT_ROW"])
