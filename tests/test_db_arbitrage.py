import importlib
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

db_arbitrage = importlib.import_module("src.web_portal.db_arbitrage")
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


def _base_row(now: datetime, **overrides) -> dict:
    row = {
        "event_ticker": "EVT-1",
        "event_title": "Election 2028",
        "event_category": "Politics",
        "strike_period": "DAY",
        "series_ticker": "EVT",
        "market_ticker": "EVT-1-A",
        "market_title": "Outcome A",
        "last_tick_ts": now,
        "yes_bid_dollars": "0.45",
        "yes_ask_dollars": "0.5",
        "price_dollars": None,
        "implied_yes_mid": None,
    }
    row.update(overrides)
    return row


class TestArbitrageWhere(unittest.TestCase):
    def test_build_arbitrage_where_normalizes_search(self) -> None:
        filters = _make_filters(
            search="  Big   Game  ",
            categories=("Sports",),
            strike_period="DAY",
        )
        where, params = db_arbitrage._build_arbitrage_where(filters)
        self.assertEqual(
            where,
            " AND (e.search_text ILIKE %s OR m.search_text ILIKE %s)"
            " AND LOWER(e.category) IN (%s)"
            " AND LOWER(e.strike_period) = %s",
        )
        self.assertEqual(params, ["%Big Game%", "%Big Game%", "sports", "day"])


class TestPercentFormatting(unittest.TestCase):
    def test_fmt_percent_any(self) -> None:
        self.assertEqual(db_arbitrage._fmt_percent_any(None), "N/A")
        self.assertEqual(db_arbitrage._fmt_percent_any(Decimal("0")), "0%")
        self.assertEqual(db_arbitrage._fmt_percent_any(Decimal("0.004")), "<1%")
        self.assertEqual(db_arbitrage._fmt_percent_any(Decimal("0.234")), "23%")
        self.assertEqual(db_arbitrage._fmt_percent_any(Decimal("-0.1")), "N/A")


class TestBuildArbitrageItem(unittest.TestCase):
    def test_build_arbitrage_item_buy_yes_basket(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        rows = [
            _base_row(now, market_ticker="EVT-1-A", market_title="Outcome A", yes_ask_dollars="0.45"),
            _base_row(now, market_ticker="EVT-1-B", market_title="Outcome B", yes_ask_dollars="0.45"),
        ]
        filters = db_arbitrage.ArbitrageFilterConfig(max_tick_age_minutes=60)
        item = db_arbitrage._build_arbitrage_item(rows, now, filters)
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["arb_action"], "Buy YES basket")
        self.assertEqual(item["arb_action_key"], "arb-yes")
        self.assertEqual(item["edge_percent"], "10%")
        self.assertEqual(item["sum_yes_ask_percent"], "90%")
        self.assertEqual(item["market_count"], 2)
        self.assertEqual(item["tick_age"], "<1m")

    def test_build_arbitrage_item_buy_no_basket(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        rows = [
            _base_row(
                now,
                market_ticker="EVT-1-A",
                market_title="Outcome A",
                yes_bid_dollars="0.6",
                yes_ask_dollars="0.6",
            ),
            _base_row(
                now,
                market_ticker="EVT-1-B",
                market_title="Outcome B",
                yes_bid_dollars="0.55",
                yes_ask_dollars="0.55",
            ),
        ]
        filters = db_arbitrage.ArbitrageFilterConfig(max_tick_age_minutes=60)
        item = db_arbitrage._build_arbitrage_item(rows, now, filters)
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["arb_action"], "Buy NO basket")
        self.assertEqual(item["arb_action_key"], "arb-no")
        self.assertEqual(item["edge_percent"], "15%")

    def test_build_arbitrage_item_filters_stale_ticks(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        old_tick = now - timedelta(minutes=6)
        rows = [
            _base_row(now, market_ticker="EVT-1-A", last_tick_ts=old_tick),
            _base_row(now, market_ticker="EVT-1-B"),
        ]
        filters = db_arbitrage.ArbitrageFilterConfig(max_tick_age_minutes=5)
        self.assertIsNone(db_arbitrage._build_arbitrage_item(rows, now, filters))

    def test_build_arbitrage_item_filters_wide_spread(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        rows = [
            _base_row(
                now,
                market_ticker="EVT-1-A",
                yes_bid_dollars="0",
                yes_ask_dollars="1",
            ),
            _base_row(now, market_ticker="EVT-1-B"),
        ]
        filters = db_arbitrage.ArbitrageFilterConfig(max_tick_age_minutes=60)
        self.assertIsNone(db_arbitrage._build_arbitrage_item(rows, now, filters))

    def test_build_arbitrage_item_requires_multiple_markets(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        rows = [_base_row(now)]
        filters = db_arbitrage.ArbitrageFilterConfig(max_tick_age_minutes=None)
        self.assertIsNone(db_arbitrage._build_arbitrage_item(rows, now, filters))


class TestFetchArbitrageOpportunities(unittest.TestCase):
    def test_fetch_arbitrage_opportunities_groups_and_limits(self) -> None:
        rows = [
            {"event_ticker": "EVT-1"},
            {"event_ticker": "EVT-1"},
            {"event_ticker": "EVT-2"},
        ]

        def _fake_build(event_rows, now, criteria):
            ticker = event_rows[0].get("event_ticker")
            if ticker == "EVT-1":
                return {"edge_value": Decimal("0.1")}
            if ticker == "EVT-2":
                return {"edge_value": Decimal("0.2")}
            return None

        with patch.object(db_arbitrage, "_fetch_arbitrage_rows", return_value=rows) as fetch_rows, \
            patch.object(db_arbitrage, "_build_arbitrage_item", side_effect=_fake_build) as build_item:
            result = db_arbitrage.fetch_arbitrage_opportunities(
                conn=object(),
                limit=1,
                filters=_make_filters(),
                criteria=db_arbitrage.build_arbitrage_filters(None),
            )
        fetch_rows.assert_called_once()
        self.assertEqual(build_item.call_count, 2)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["edge_value"], Decimal("0.2"))
