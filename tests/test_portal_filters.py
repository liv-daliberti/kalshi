import unittest
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

portal_filters = importlib.import_module("src.web_portal.portal_filters")


class TestPortalFilters(unittest.TestCase):
    def test_parse_category_filters_single_value(self) -> None:
        args = {"category": "Sports"}
        categories = portal_filters._parse_category_filters(args)
        self.assertEqual(categories, ("Sports",))

    def test_parse_portal_filters_lowercases_status(self) -> None:
        filters = portal_filters._parse_portal_filters({"status": "OPEN"})
        self.assertEqual(filters.status, "open")
