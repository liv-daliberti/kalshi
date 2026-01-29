import unittest
import importlib

from _test_utils import add_src_to_path

add_src_to_path()

json_utils = importlib.import_module("src.core.json_utils")


class TestJsonUtils(unittest.TestCase):
    def test_normalize_metadata_value_paths(self) -> None:
        self.assertIsNone(json_utils.normalize_metadata_value(None))
        self.assertIsNone(json_utils.normalize_metadata_value("   "))
        self.assertEqual(json_utils.normalize_metadata_value(" ok "), "ok")
        self.assertEqual(json_utils.normalize_metadata_value(5), 5)
