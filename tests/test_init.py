import unittest
import importlib

from _test_utils import add_src_to_path

add_src_to_path()

src = importlib.import_module("src")


class TestPackageInit(unittest.TestCase):
    def test_docstring_present(self) -> None:
        self.assertTrue(src.__doc__)
        self.assertIn("Kalshi ingestor", src.__doc__)
