import importlib
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

db_opportunities = importlib.import_module("src.web_portal.db_opportunities")
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


class FakeCursor:
    def __init__(self, rows=None):
        self._rows = list(rows or [])
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestOpportunityWhere(unittest.TestCase):
    def test_build_opportunity_where_normalizes_search(self) -> None:
        filters = _make_filters(
            search="  Big   Game  ",
            categories=("Sports",),
            strike_period="DAY",
        )
        where, params = db_opportunities._build_opportunity_where(filters)
        self.assertEqual(
            where,
            " AND (e.search_text ILIKE %s OR m.search_text ILIKE %s)"
            " AND LOWER(e.category) IN (%s)"
            " AND LOWER(e.strike_period) = %s",
        )
        self.assertEqual(params, ["%Big Game%", "%Big Game%", "sports", "day"])


class TestOpportunityScanLimit(unittest.TestCase):
    def test_opportunity_scan_limit_caps_at_5000(self) -> None:
        with patch.object(db_opportunities, "_env_int", return_value=6000) as env_int:
            result = db_opportunities._opportunity_scan_limit(200)
        self.assertEqual(result, 5000)
        env_int.assert_called_once_with(
            "WEB_PORTAL_OPPORTUNITY_SCAN_LIMIT",
            1000,
            minimum=200,
        )

    def test_opportunity_scan_limit_returns_env_value(self) -> None:
        with patch.object(db_opportunities, "_env_int", return_value=450) as env_int:
            result = db_opportunities._opportunity_scan_limit(100)
        self.assertEqual(result, 450)
        env_int.assert_called_once_with(
            "WEB_PORTAL_OPPORTUNITY_SCAN_LIMIT",
            500,
            minimum=100,
        )


class TestOpportunityCriteriaWhere(unittest.TestCase):
    def test_build_opportunity_criteria_where_empty(self) -> None:
        criteria = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=None,
            max_tick_age_minutes=None,
        )
        where, params = db_opportunities._build_opportunity_criteria_where(criteria)
        self.assertEqual(where, "")
        self.assertEqual(params, [])

    def test_build_opportunity_criteria_where_all_filters(self) -> None:
        criteria = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=Decimal("0.55"),
            max_prediction_age_minutes=15.0,
            max_tick_age_minutes=30.0,
        )
        where, params = db_opportunities._build_opportunity_criteria_where(criteria)
        self.assertEqual(
            where,
            " AND p.confidence >= %s"
            " AND p.created_at >= NOW() - (%s * INTERVAL '1 minute')"
            " AND t.ts >= NOW() - (%s * INTERVAL '1 minute')",
        )
        self.assertEqual(params, [Decimal("0.55"), 15.0, 30.0])


class TestOpportunityFilters(unittest.TestCase):
    def test_build_opportunity_filters_normalizes_decimals(self) -> None:
        config = db_opportunities.build_opportunity_filters(0.1, None, 15, 30)
        self.assertEqual(config.min_gap, Decimal("0.1"))
        self.assertIsNone(config.min_confidence)
        self.assertEqual(config.max_prediction_age_minutes, 15)
        self.assertEqual(config.max_tick_age_minutes, 30)


class TestAgeLabel(unittest.TestCase):
    def test_age_label_no_timestamp(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(db_opportunities._age_label(None, now, None), "N/A")
        self.assertIsNone(db_opportunities._age_label(None, now, 5))

    def test_age_label_respects_max_minutes(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        ts_value = now - timedelta(minutes=10)
        self.assertIsNone(db_opportunities._age_label(ts_value, now, 5))

    def test_age_label_within_limit(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        ts_value = now - timedelta(minutes=2)
        self.assertEqual(db_opportunities._age_label(ts_value, now, 5), "2m")


class TestDirectionLabel(unittest.TestCase):
    def test_direction_label(self) -> None:
        self.assertEqual(
            db_opportunities._direction_label(Decimal("0.6"), Decimal("0.4")),
            ("Model > Market", "up"),
        )
        self.assertEqual(
            db_opportunities._direction_label(Decimal("0.4"), Decimal("0.4")),
            ("Model < Market", "down"),
        )


class TestOpportunityPriceContext(unittest.TestCase):
    def _base_row(self, now: datetime) -> dict:
        return {
            "open_time": now - timedelta(hours=1),
            "close_time": now + timedelta(hours=1),
            "implied_yes_mid": "0.4",
            "predicted_yes_prob": "0.7",
            "prediction_confidence": "0.8",
        }

    def test_opportunity_price_context_success(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = self._base_row(now)
        filters = db_opportunities.OpportunityFilterConfig(
            min_gap=Decimal("0.1"),
            min_confidence=Decimal("0.5"),
            max_prediction_age_minutes=None,
            max_tick_age_minutes=None,
        )
        ctx = db_opportunities._opportunity_price_context(row, now, filters)
        self.assertIsNotNone(ctx)
        assert ctx is not None
        self.assertEqual(ctx.yes_price, Decimal("0.4"))
        self.assertEqual(ctx.predicted, Decimal("0.7"))
        self.assertEqual(ctx.gap, Decimal("0.3"))
        self.assertEqual(ctx.confidence, Decimal("0.8"))
        self.assertEqual(ctx.market_close_dt, row["close_time"])

    def test_opportunity_price_context_filters_gap(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = self._base_row(now)
        filters = db_opportunities.OpportunityFilterConfig(
            min_gap=Decimal("0.5"),
            min_confidence=None,
            max_prediction_age_minutes=None,
            max_tick_age_minutes=None,
        )
        self.assertIsNone(db_opportunities._opportunity_price_context(row, now, filters))

    def test_opportunity_price_context_filters_confidence(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = self._base_row(now)
        row["prediction_confidence"] = None
        filters = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=Decimal("0.6"),
            max_prediction_age_minutes=None,
            max_tick_age_minutes=None,
        )
        self.assertIsNone(db_opportunities._opportunity_price_context(row, now, filters))

    def test_opportunity_price_context_requires_valid_prediction(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = self._base_row(now)
        row["predicted_yes_prob"] = "1.2"
        filters = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=None,
            max_tick_age_minutes=None,
        )
        self.assertIsNone(db_opportunities._opportunity_price_context(row, now, filters))


class TestOpportunityAgeContext(unittest.TestCase):
    def test_opportunity_age_context_success(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = {
            "prediction_ts": now - timedelta(minutes=10),
            "last_tick_ts": now - timedelta(minutes=5),
        }
        filters = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=15,
            max_tick_age_minutes=10,
        )
        ctx = db_opportunities._opportunity_age_context(row, now, filters)
        self.assertIsNotNone(ctx)
        assert ctx is not None
        self.assertEqual(ctx.prediction_age, "10m")
        self.assertEqual(ctx.tick_age, "5m")

    def test_opportunity_age_context_filters_prediction_age(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = {
            "prediction_ts": now - timedelta(minutes=20),
            "last_tick_ts": now - timedelta(minutes=5),
        }
        filters = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=10,
            max_tick_age_minutes=10,
        )
        self.assertIsNone(db_opportunities._opportunity_age_context(row, now, filters))

    def test_opportunity_age_context_filters_tick_age(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = {
            "prediction_ts": now - timedelta(minutes=2),
            "last_tick_ts": now - timedelta(minutes=20),
        }
        filters = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=10,
            max_tick_age_minutes=5,
        )
        self.assertIsNone(db_opportunities._opportunity_age_context(row, now, filters))


class TestBuildOpportunityItem(unittest.TestCase):
    def _base_row(self, now: datetime) -> dict:
        return {
            "event_title": "Big Game",
            "event_ticker": "BIGGAME-2024",
            "event_category": "Sports",
            "strike_period": "day",
            "market_ticker": "BIGGAME-1",
            "market_title": "YES",
            "series_ticker": "BIGGAME",
            "open_time": now - timedelta(hours=1),
            "close_time": now + timedelta(hours=1),
            "implied_yes_mid": "0.45",
            "predicted_yes_prob": "0.60",
            "prediction_confidence": "0.70",
            "prediction_ts": now - timedelta(minutes=2),
            "last_tick_ts": now - timedelta(minutes=3),
            "volume": 1000,
        }

    def test_build_opportunity_item_success(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = self._base_row(now)
        filters = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=10,
            max_tick_age_minutes=10,
        )
        item = db_opportunities._build_opportunity_item(row, now, filters)
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["event_title"], "Big Game")
        self.assertEqual(item["market_label"], "YES")
        self.assertEqual(item["kalshi_percent"], "45%")
        self.assertEqual(item["gpt_percent"], "60%")
        self.assertEqual(item["gap_percent"], "15%")
        self.assertEqual(item["confidence_percent"], "70%")
        self.assertEqual(item["time_remaining"], "1h")
        self.assertEqual(item["volume"], "1,000")
        self.assertEqual(item["direction"], "Model > Market")
        self.assertEqual(item["direction_key"], "up")
        self.assertEqual(
            item["event_url"],
            "https://kalshi.com/markets/biggame/big-game/biggame-2024",
        )
        self.assertEqual(
            item["market_url"],
            "https://kalshi.com/markets/biggame/big-game/biggame-1",
        )

    def test_build_opportunity_item_defaults(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = self._base_row(now)
        row["event_title"] = None
        row["event_ticker"] = None
        row["market_ticker"] = None
        filters = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=10,
            max_tick_age_minutes=10,
        )
        item = db_opportunities._build_opportunity_item(row, now, filters)
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["event_title"], "Unknown event")
        self.assertEqual(item["event_ticker"], "N/A")
        self.assertEqual(item["market_ticker"], "N/A")
        self.assertIsNone(item["event_url"])
        self.assertIsNone(item["market_url"])

    def test_build_opportunity_item_requires_price_context(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = self._base_row(now)
        filters = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=10,
            max_tick_age_minutes=10,
        )
        with patch.object(db_opportunities, "_opportunity_price_context", return_value=None) as price_ctx, \
             patch.object(db_opportunities, "_opportunity_age_context") as age_ctx:
            item = db_opportunities._build_opportunity_item(row, now, filters)
        self.assertIsNone(item)
        price_ctx.assert_called_once_with(row, now, filters)
        age_ctx.assert_not_called()

    def test_build_opportunity_item_requires_age_context(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = self._base_row(now)
        filters = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=10,
            max_tick_age_minutes=10,
        )
        price_ctx = db_opportunities.OpportunityPriceContext(
            yes_price=Decimal("0.4"),
            predicted=Decimal("0.6"),
            gap=Decimal("0.2"),
            confidence=Decimal("0.8"),
            market_close_dt=now + timedelta(hours=1),
        )
        with patch.object(db_opportunities, "_opportunity_price_context", return_value=price_ctx) as price_ctx_mock, \
             patch.object(db_opportunities, "_opportunity_age_context", return_value=None) as age_ctx_mock:
            item = db_opportunities._build_opportunity_item(row, now, filters)
        self.assertIsNone(item)
        price_ctx_mock.assert_called_once_with(row, now, filters)
        age_ctx_mock.assert_called_once_with(row, now, filters)


class TestFetchOpportunityRows(unittest.TestCase):
    def test_fetch_opportunity_rows_builds_query(self) -> None:
        fake_cursor = FakeCursor(rows=[{"market_ticker": "M1"}])
        captured = {}

        @contextmanager
        def fake_timed_cursor(_conn, row_factory=None):
            captured["row_factory"] = row_factory
            yield fake_cursor

        filters = _make_filters()
        criteria = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=None,
            max_tick_age_minutes=None,
        )

        with patch.object(db_opportunities, "timed_cursor", side_effect=fake_timed_cursor), \
             patch.object(db_opportunities, "_build_opportunity_where", return_value=(" AND e.category = %s", ["Sports"])), \
             patch.object(db_opportunities, "_build_opportunity_criteria_where", return_value=(" AND p.confidence >= %s", [Decimal("0.5")])), \
             patch.object(db_opportunities, "event_core_columns_sql", return_value="EVENT_COLS"), \
             patch.object(db_opportunities, "market_identity_columns_sql", return_value="MARKET_COLS"), \
             patch.object(db_opportunities, "tick_columns_sql", return_value="TICK_COLS"), \
             patch.object(db_opportunities, "last_tick_lateral_sql", return_value="LAST_TICK"), \
             patch.object(db_opportunities, "last_prediction_lateral_sql", return_value="LAST_PRED"):
            rows = db_opportunities._fetch_opportunity_rows(
                object(),
                filters,
                criteria,
                scan_limit=123,
            )

        self.assertEqual(rows, [{"market_ticker": "M1"}])
        self.assertEqual(captured["row_factory"], db_opportunities.dict_row)
        self.assertEqual(len(fake_cursor.executed), 1)
        query, params = fake_cursor.executed[0]
        self.assertIn("EVENT_COLS", query)
        self.assertIn("MARKET_COLS", query)
        self.assertIn("TICK_COLS", query)
        self.assertIn("LAST_TICK", query)
        self.assertIn("LAST_PRED", query)
        self.assertIn("AND e.category = %s", query)
        self.assertIn("AND p.confidence >= %s", query)
        self.assertEqual(params, ("Sports", Decimal("0.5"), 123))


class TestBuildOpportunityList(unittest.TestCase):
    def test_build_opportunity_list_sorts_and_limits(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        rows = [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]
        items = [
            {"id": 1, "gap_value": Decimal("0.2"), "prediction_ts": now - timedelta(minutes=1)},
            None,
            {"id": 3, "gap_value": Decimal("0.2"), "prediction_ts": now},
            {"id": 4, "gap_value": Decimal("0.1"), "prediction_ts": now + timedelta(minutes=5)},
        ]
        with patch.object(db_opportunities, "_build_opportunity_item", side_effect=items) as build_item:
            result = db_opportunities._build_opportunity_list(
                rows,
                now,
                db_opportunities.OpportunityFilterConfig(
                    min_gap=None,
                    min_confidence=None,
                    max_prediction_age_minutes=None,
                    max_tick_age_minutes=None,
                ),
                limit=2,
            )
        self.assertEqual([item["id"] for item in result], [3, 1])
        self.assertEqual(build_item.call_count, 4)


class TestFetchOpportunities(unittest.TestCase):
    def test_fetch_opportunities_returns_empty_for_zero_limit(self) -> None:
        filters = _make_filters()
        criteria = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=None,
            max_tick_age_minutes=None,
        )
        with patch.object(db_opportunities, "_fetch_opportunity_rows") as fetch_rows:
            result = db_opportunities.fetch_opportunities(
                object(),
                0,
                filters,
                criteria=criteria,
            )
        self.assertEqual(result, [])
        fetch_rows.assert_not_called()

    def test_fetch_opportunities_happy_path(self) -> None:
        filters = _make_filters()
        criteria = db_opportunities.OpportunityFilterConfig(
            min_gap=None,
            min_confidence=None,
            max_prediction_age_minutes=None,
            max_tick_age_minutes=None,
        )
        captured = {}
        conn = object()

        def fake_build_list(rows, now, criteria_arg, limit):
            captured["rows"] = rows
            captured["now"] = now
            captured["criteria"] = criteria_arg
            captured["limit"] = limit
            return [{"ok": True}]

        with patch.object(db_opportunities, "_opportunity_scan_limit", return_value=123) as scan_limit, \
             patch.object(db_opportunities, "_fetch_opportunity_rows", return_value=[{"row": 1}]) as fetch_rows, \
             patch.object(db_opportunities, "_build_opportunity_list", side_effect=fake_build_list):
            result = db_opportunities.fetch_opportunities(
                conn,
                5,
                filters,
                criteria=criteria,
            )

        self.assertEqual(result, [{"ok": True}])
        scan_limit.assert_called_once_with(5)
        fetch_rows.assert_called_once_with(conn, filters, criteria, 123)
        self.assertEqual(captured["rows"], [{"row": 1}])
        self.assertEqual(captured["criteria"], criteria)
        self.assertEqual(captured["limit"], 5)
        self.assertIsInstance(captured["now"], datetime)
        self.assertEqual(captured["now"].tzinfo, timezone.utc)
