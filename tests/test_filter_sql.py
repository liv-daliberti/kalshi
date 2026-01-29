import importlib
import unittest

from _test_utils import add_src_to_path

add_src_to_path()

filter_sql = importlib.import_module("src.web_portal.filter_sql")
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


class TestNormalizeSearch(unittest.TestCase):
    def test_normalize_search_handles_empty(self) -> None:
        self.assertIsNone(filter_sql.normalize_search(None))
        self.assertIsNone(filter_sql.normalize_search("   "))
        self.assertEqual(filter_sql.normalize_search("  Hello   world  "), "Hello world")
        self.assertEqual(filter_sql.normalize_search(123), "123")


class TestAppendSearchClause(unittest.TestCase):
    def test_append_search_clause_no_fields(self) -> None:
        clauses: list[str] = []
        params: list[object] = []
        filter_sql.append_search_clause(clauses, params, "hello", [])
        self.assertEqual(clauses, [])
        self.assertEqual(params, [])

    def test_append_search_clause_with_fields(self) -> None:
        clauses: list[str] = []
        params: list[object] = []
        filter_sql.append_search_clause(clauses, params, "hey", ("e.title", "e.subtitle"))
        self.assertEqual(clauses, ["(e.title ILIKE %s OR e.subtitle ILIKE %s)"])
        self.assertEqual(params, ["%hey%", "%hey%"])


class TestAppendCategoryClause(unittest.TestCase):
    def test_append_category_clause_empty_iterator(self) -> None:
        clauses: list[str] = []
        params: list[object] = []
        filter_sql.append_category_clause(clauses, params, iter(()))
        self.assertEqual(clauses, [])
        self.assertEqual(params, [])

    def test_append_category_clause_with_values(self) -> None:
        clauses: list[str] = []
        params: list[object] = []
        filter_sql.append_category_clause(clauses, params, ("Sports", "News"))
        self.assertEqual(clauses, ["LOWER(e.category) IN (%s, %s)"])
        self.assertEqual(params, ["sports", "news"])


class TestBuildFilterWhere(unittest.TestCase):
    def test_build_filter_where_combines_clauses(self) -> None:
        filters = _make_filters(
            search="  big   game  ",
            categories=("Sports",),
            strike_period="DAY",
        )
        where, params = filter_sql.build_filter_where(
            filters,
            ("e.title", "e.subtitle"),
        )
        self.assertEqual(
            where,
            " AND (e.title ILIKE %s OR e.subtitle ILIKE %s)"
            " AND LOWER(e.category) IN (%s)"
            " AND LOWER(e.strike_period) = %s",
        )
        self.assertEqual(params, ["%big game%", "%big game%", "sports", "day"])

    def test_build_filter_where_respects_override(self) -> None:
        filters = _make_filters(search="ignored", categories=("Sports",))
        where, params = filter_sql.build_filter_where(
            filters,
            ("e.title",),
            include_category=False,
            search_override="override",
        )
        self.assertEqual(where, " AND (e.title ILIKE %s)")
        self.assertEqual(params, ["%override%"])
