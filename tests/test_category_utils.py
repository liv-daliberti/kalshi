import importlib
import unittest

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

category_utils = importlib.import_module("src.web_portal.category_utils")


class TestCategoryFilters(unittest.TestCase):
    def test_build_category_filters_appends_unselected_category(self) -> None:
        calls = []

        def fake_url_for(endpoint, **params):
            calls.append((endpoint, params))
            return f"/{endpoint}"

        filters = category_utils.build_category_filters(
            active_categories=["Economy"],
            selected_categories=["Sports"],
            base_params={"foo": "bar"},
            endpoint="events",
            url_for=fake_url_for,
        )

        economy_filter = next(item for item in filters if item["label"] == "Economy")
        self.assertFalse(economy_filter["active"])
        self.assertEqual(calls[0][0], "events")
        self.assertEqual(calls[0][1]["foo"], "bar")
        self.assertEqual(calls[0][1]["category"], ["Sports", "Economy"])

    def test_build_category_choices_dedupes_case_insensitive(self) -> None:
        choices, selected = category_utils.build_category_choices(
            active_categories=["Economy", "Sports"],
            selected_categories=["economy", "Politics"],
        )
        self.assertEqual(choices, ["Economy", "Sports", "Politics"])
        self.assertEqual(selected, {"economy", "politics"})
