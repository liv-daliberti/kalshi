import importlib
import unittest

from _test_utils import add_src_to_path

add_src_to_path()

filter_params = importlib.import_module("src.web_portal.filter_params")
portal_filters = importlib.import_module("src.web_portal.portal_filters")


def _make_filters(**overrides):
    data = {
        "search": None,
        "categories": (),
        "strike_period": None,
        "close_window": None,
        "status": None,
        "sort": None,
        "order": None,
    }
    data.update(overrides)
    return portal_filters.PortalFilters(**data)


class TestBuildFilterParams(unittest.TestCase):
    def test_build_filter_params_includes_truthy_page_params(self) -> None:
        filters = _make_filters(
            search="rain",
            categories=("Weather", "Sports"),
            strike_period="day",
            close_window="24h",
            status="open",
            sort="close_time",
            order="desc",
        )
        params = filter_params.build_filter_params(
            50,
            filters,
            page_params={"page": 2, "cursor": None, "empty": ""},
        )
        self.assertEqual(params["limit"], 50)
        self.assertEqual(params["search"], "rain")
        self.assertEqual(params["category"], ["Weather", "Sports"])
        self.assertEqual(params["strike_period"], "day")
        self.assertEqual(params["close_window"], "24h")
        self.assertEqual(params["status"], "open")
        self.assertEqual(params["sort"], "close_time")
        self.assertEqual(params["order"], "desc")
        self.assertEqual(params["page"], 2)
        self.assertNotIn("cursor", params)
        self.assertNotIn("empty", params)

    def test_build_filter_params_skips_category_when_excluded(self) -> None:
        filters = _make_filters(categories=("Sports",))
        params = filter_params.build_filter_params(10, filters, include_category=False)
        self.assertEqual(params, {"limit": 10})
