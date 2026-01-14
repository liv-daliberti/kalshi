import json
import os
import runpy
import sys
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, mock_open, patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

import src.web_portal as web_portal


class TestPortalParsing(unittest.TestCase):
    def test_parse_csv_and_fmt_hours(self) -> None:
        self.assertEqual(web_portal._parse_csv("a, b , ,c"), ("a", "b", "c"))
        self.assertEqual(web_portal._parse_csv(None), ())
        self.assertEqual(web_portal._fmt_hours("2", 1.5), "2")
        self.assertEqual(web_portal._fmt_hours("2.5", 1.5), "2.5")
        self.assertEqual(web_portal._fmt_hours("bad", 1.5), "1.5")

    def test_env_parsers(self) -> None:
        with patch.dict(os.environ, {"TEST_INT": "5", "TEST_FLOAT": "2.5", "TEST_BOOL": "Yes"}):
            self.assertEqual(web_portal._env_int("TEST_INT", 2), 5)
            self.assertEqual(web_portal._env_float("TEST_FLOAT", 1.0), 2.5)
            self.assertTrue(web_portal._env_bool("TEST_BOOL"))
        with patch.dict(os.environ, {"TEST_INT": "bad", "TEST_FLOAT": "bad"}, clear=True):
            self.assertEqual(web_portal._env_int("TEST_INT", 3), 3)
            self.assertEqual(web_portal._env_float("TEST_FLOAT", 1.0), 1.0)
        with patch.dict(os.environ, {"TEST_INT": "0"}):
            self.assertEqual(web_portal._env_int("TEST_INT", 3, minimum=1), 3)
        with patch.dict(os.environ, {"TEST_FLOAT": "0"}):
            self.assertEqual(web_portal._env_float("TEST_FLOAT", 1.0, minimum=0.1), 0.1)

    def test_filter_parsers(self) -> None:
        args = {"category": ["Sports", "sports,News", ""]}
        categories = web_portal._parse_category_filters(args)
        self.assertEqual(categories, ("Sports", "News"))
        self.assertEqual(web_portal._clean_filter_value(" all "), None)
        self.assertEqual(web_portal._parse_close_window("1h"), ("1h", 1))
        self.assertIsNone(web_portal._parse_sort_value("unknown"))
        self.assertEqual(web_portal._parse_order_value("ASC"), "asc")

    def test_filter_parsers_with_getlist(self) -> None:
        class DummyArgs:
            def getlist(self, _key):
                return [None, " A ", "a", "B,C", ""]

        categories = web_portal._parse_category_filters(DummyArgs())
        self.assertEqual(categories, ("A", "B", "C"))
        self.assertIsNone(web_portal._clean_filter_value(None))
        self.assertEqual(web_portal._parse_close_window(""), (None, None))
        self.assertEqual(web_portal._parse_close_window("bad"), (None, None))
        self.assertIsNone(web_portal._parse_sort_value(""))
        self.assertIsNone(web_portal._parse_order_value(""))

    def test_build_filters(self) -> None:
        filters = web_portal.PortalFilters(
            search="e",
            categories=("A", "B"),
            strike_period="hour",
            close_window="1h",
            close_window_hours=1.0,
            status="open",
            sort="title",
            order="desc",
        )
        where_sql, params = web_portal._build_event_where(filters)
        self.assertIn("e.title ILIKE", where_sql)
        self.assertIn("LOWER(e.category)", where_sql)
        self.assertEqual(len(params), 7)
        order_by = web_portal._build_order_by("title", "desc", "close_time", "asc")
        self.assertIn("e.title", order_by)
        params_dict = web_portal._build_filter_params(10, filters)
        self.assertEqual(params_dict["limit"], 10)
        self.assertEqual(params_dict["category"], ["A", "B"])
        parsed = web_portal._parse_portal_filters(
            {
                "search": "  hi ",
                "category": "A",
                "strike_period": "hour",
                "close_window": "1h",
                "status": "OPEN",
                "sort": "close_time",
                "order": "asc",
            }
        )
        self.assertEqual(parsed.search, "hi")
        self.assertEqual(parsed.categories, ("A",))
        self.assertEqual(parsed.status, "open")
        empty_filters = web_portal.PortalFilters(
            search=None,
            categories=(),
            strike_period=None,
            close_window=None,
            close_window_hours=None,
            status=None,
            sort=None,
            order=None,
        )
        where_sql, params = web_portal._build_event_where(empty_filters)
        self.assertEqual(where_sql, "")
        self.assertEqual(params, [])

    def test_load_backfill_config(self) -> None:
        with patch.dict(
            os.environ,
            {
                "STRIKE_PERIODS": "hour,week",
                "BACKFILL_EVENT_STATUSES": "open,closed",
                "CANDLE_MINUTES_FOR_HOUR": "5",
                "CANDLE_MINUTES_FOR_DAY": "120",
                "CANDLE_LOOKBACK_HOURS": "12",
            },
            clear=True,
        ):
            config = web_portal._load_backfill_config()
        self.assertEqual(config.strike_periods, ("hour", "week"))
        self.assertEqual(config.event_statuses, ("open", "closed"))
        self.assertEqual(config.minutes_hour, 5)
        self.assertEqual(config.minutes_day, 120)
        self.assertEqual(config.lookback_hours, 12)

    def test_human_join_and_scope(self) -> None:
        self.assertEqual(web_portal._human_join([]), "")
        self.assertEqual(web_portal._human_join(["A"]), "A")
        self.assertEqual(web_portal._human_join(["A", "B"]), "A and B")
        self.assertEqual(web_portal._human_join(["A", "B", "C"]), "A, B, and C")
        with patch.dict(
            os.environ,
            {"STRIKE_PERIODS": "hour,day", "STRIKE_HOUR_MAX_HOURS": "2", "STRIKE_DAY_MAX_HOURS": "36"},
        ):
            note = web_portal.describe_event_scope()
        self.assertIn("hourly", note)
        self.assertIn("daily", note)

    def test_describe_event_scope_empty(self) -> None:
        with patch.dict(os.environ, {"STRIKE_PERIODS": ""}):
            self.assertIsNone(web_portal.describe_event_scope())


class TestPortalFormatting(unittest.TestCase):
    def test_money_formatting(self) -> None:
        self.assertEqual(web_portal._money_quant(0), Decimal("1"))
        self.assertEqual(web_portal.fmt_money(None), "N/A")
        self.assertEqual(web_portal.fmt_money("-1.234", digits=2), "-$1.23")
        self.assertEqual(web_portal.fmt_cents("0.25"), "25¢")
        self.assertEqual(web_portal.fmt_cents(None), "--")
        self.assertEqual(web_portal.fmt_percent(0), "0%")
        self.assertEqual(web_portal.fmt_percent(1), "100%")
        self.assertEqual(web_portal.fmt_percent("0.005"), "<1%")
        self.assertEqual(web_portal.fmt_percent("0.995"), ">99%")
        self.assertEqual(web_portal.fmt_percent("0.5"), "50%")

    def test_money_and_percent_invalid(self) -> None:
        self.assertEqual(web_portal.fmt_money("bad"), "N/A")
        self.assertEqual(web_portal.fmt_cents("bad"), "N/A")
        self.assertEqual(web_portal.fmt_percent(None), "--")
        self.assertEqual(web_portal.fmt_percent("bad"), "--")
        self.assertEqual(web_portal.fmt_percent("-0.2"), "--")
        self.assertIsNone(web_portal.clamp_probability("bad"))

    def test_probability_helpers(self) -> None:
        self.assertEqual(web_portal.clamp_probability("0.2"), Decimal("0.2"))
        self.assertIsNone(web_portal.clamp_probability("2"))
        self.assertTrue(web_portal._invalid_probability("bad"))
        self.assertFalse(web_portal._invalid_probability(None))

    def test_price_helpers(self) -> None:
        row = {
            "last_tick_ts": None,
            "candle_close": None,
            "settlement_value_dollars": None,
            "settlement_value": None,
        }
        error = web_portal._pricing_error_for_row(row, None, None, None)
        self.assertIn("Pricing unavailable", error)
        self.assertIsNone(web_portal._decimal("bad"))
        self.assertEqual(web_portal._coerce_int("5.1"), 5)
        self.assertIsNone(web_portal._coerce_int(True))
        self.assertEqual(web_portal._to_cents("0.25"), 25)
        self.assertEqual(web_portal._to_cents("2.4"), 2)
        self.assertEqual(web_portal._dollars_from_cents(25), Decimal("0.25"))

    def test_price_helpers_edge_cases(self) -> None:
        self.assertEqual(web_portal._to_cents(0.75), 75)
        self.assertEqual(web_portal._to_cents(1.5), 2)
        self.assertIsNone(web_portal._to_cents("  "))
        self.assertEqual(web_portal._to_cents("10"), 10)
        self.assertIsNone(web_portal._to_cents("bad"))
        self.assertIsNone(web_portal._coerce_int("bad"))

    def test_pricing_error_details(self) -> None:
        self.assertIsNone(
            web_portal._pricing_error_for_row(
                {},
                Decimal("0.5"),
                Decimal("0.4"),
                Decimal("0.6"),
            )
        )

        row = {
            "last_tick_ts": datetime.now(timezone.utc),
            "implied_yes_mid": None,
            "price_dollars": "bad",
            "yes_bid_dollars": None,
            "yes_ask_dollars": None,
            "candle_close": "bad",
            "settlement_value_dollars": "bad",
            "settlement_value": "bad",
        }
        error = web_portal._pricing_error_for_row(row, None, None, None)
        self.assertIn("missing tick fields", error)
        self.assertIn("invalid tick fields", error)
        self.assertIn("invalid market_candles close", error)
        self.assertIn("invalid settlement value (dollars)", error)
        self.assertIn("invalid settlement value (cents)", error)

        row = {
            "last_tick_ts": datetime.now(timezone.utc),
            "implied_yes_mid": "0.5",
            "price_dollars": "0.5",
            "yes_bid_dollars": "0.4",
            "yes_ask_dollars": "0.6",
            "candle_close": "0.5",
            "settlement_value_dollars": None,
            "settlement_value": 200,
        }
        error = web_portal._pricing_error_for_row(row, None, None, None)
        self.assertIn("invalid settlement value (cents)", error)

        row = {
            "last_tick_ts": datetime.now(timezone.utc),
            "implied_yes_mid": "0.5",
            "price_dollars": "0.5",
            "yes_bid_dollars": "0.4",
            "yes_ask_dollars": "0.6",
            "candle_close": "0.5",
            "settlement_value_dollars": "0.5",
            "settlement_value": 50,
        }
        error = web_portal._pricing_error_for_row(row, None, None, None)
        self.assertIn("pricing data incomplete", error)

    def test_misc_formatters(self) -> None:
        self.assertEqual(web_portal.fmt_num(1200), "1,200")
        self.assertEqual(web_portal.fmt_num("nope"), "nope")
        self.assertEqual(web_portal.fmt_bool(None), "N/A")
        self.assertEqual(web_portal.fmt_bool(0), "No")
        expected = json.dumps({"b": 1, "a": 2}, indent=2, sort_keys=True, ensure_ascii=True)
        self.assertEqual(web_portal.fmt_json({"b": 1, "a": 2}), expected)
        self.assertEqual(web_portal.fmt_json(None), "N/A")
        self.assertEqual(web_portal.fmt_json({1}), "{1}")
        self.assertEqual(web_portal.fmt_outcome("50", None, None, None, None), "50")
        self.assertEqual(web_portal.fmt_outcome(None, "0.5", None, None, None), "0.5")
        self.assertEqual(web_portal.fmt_outcome("bad", None, None, None, None), "Pending")


class TestPortalLabels(unittest.TestCase):
    def test_outcome_and_labels(self) -> None:
        self.assertEqual(web_portal.fmt_outcome(100, None, None, None, None), "YES")
        self.assertEqual(web_portal.fmt_outcome(0, None, None, None, None), "NO")
        self.assertEqual(web_portal.fmt_outcome(None, "1", None, None, None), "YES")
        self.assertEqual(web_portal.fmt_outcome(None, None, None, None, None), "Pending")
        row = {"market_title": "Title", "market_subtitle": "Sub", "ticker": "M1"}
        self.assertEqual(web_portal._market_label(row), "Sub")
        self.assertTrue(web_portal._settlement_is_yes(100, None))
        self.assertTrue(web_portal._settlement_is_yes(None, "1.0000"))
        row = {"ticker": "M1"}
        self.assertEqual(web_portal._market_label(row), "M1")

    def test_outcome_dollars_invalid(self) -> None:
        self.assertEqual(web_portal.fmt_outcome(None, "bad", None, None, None), "Pending")
        self.assertEqual(web_portal.fmt_outcome(None, "0.0000", None, None, None), "NO")
        self.assertFalse(web_portal._settlement_is_yes("bad", None))
        self.assertFalse(web_portal._settlement_is_yes(None, "bad"))

    def test_yes_price_and_spread(self) -> None:
        row = {"yes_bid_dollars": "0.4", "yes_ask_dollars": "0.6"}
        yes_price, yes_bid, yes_ask = web_portal._derive_yes_price(row, True)
        self.assertEqual(yes_price, Decimal("0.5"))
        self.assertEqual(yes_bid, Decimal("0.4"))
        self.assertEqual(yes_ask, Decimal("0.6"))
        self.assertFalse(web_portal._is_wide_spread(row))
        wide = {"yes_bid_dollars": "0", "yes_ask_dollars": "1"}
        self.assertTrue(web_portal._is_wide_spread(wide))

    def test_yes_price_branches(self) -> None:
        row = {"yes_bid_dollars": "0", "yes_ask_dollars": "1", "implied_yes_mid": "0.5"}
        yes_price, yes_bid, yes_ask = web_portal._derive_yes_price(row, True)
        self.assertIsNone(yes_price)
        self.assertEqual(yes_bid, Decimal("0"))
        self.assertEqual(yes_ask, Decimal("1"))

        row = {
            "yes_bid_dollars": None,
            "yes_ask_dollars": "1",
            "price_dollars": None,
            "implied_yes_mid": "1",
        }
        yes_price, yes_bid, yes_ask = web_portal._derive_yes_price(row, True)
        self.assertIsNone(yes_price)
        self.assertIsNone(yes_ask)
        self.assertIsNone(yes_bid)

        row = {"yes_bid_dollars": "0.3", "yes_ask_dollars": None, "price_dollars": None}
        yes_price, _, _ = web_portal._derive_yes_price(row, True)
        self.assertEqual(yes_price, Decimal("0.3"))

        row = {"yes_bid_dollars": None, "yes_ask_dollars": "0.7", "price_dollars": None}
        yes_price, _, _ = web_portal._derive_yes_price(row, True)
        self.assertEqual(yes_price, Decimal("0.7"))

        row = {"price_dollars": "0.45"}
        yes_price, _, _ = web_portal._derive_yes_price(row, True)
        self.assertEqual(yes_price, Decimal("0.45"))

    def test_event_outcome_label(self) -> None:
        label = web_portal._format_event_outcome_label(["A"], None, False)
        self.assertEqual(label, "A")
        label = web_portal._format_event_outcome_label([], (Decimal("0.6"), "X"), None)
        self.assertIn("Leading", label)
        label = web_portal._format_event_outcome_label(["A", "B"], None, True)
        self.assertIn("Multiple outcomes settled", label)
        label = web_portal._format_event_outcome_label([], (Decimal("2"), "X"), None)
        self.assertEqual(label, "Leading: X")

    def test_compute_event_outcome_label(self) -> None:
        now = datetime.now(timezone.utc)
        rows = [
            {
                "market_title": "M1",
                "settlement_value": 100,
                "market_open_time": now - timedelta(hours=1),
                "market_close_time": now + timedelta(hours=1),
                "implied_yes_mid": "0.4",
            },
        ]
        label = web_portal._compute_event_outcome_label(rows, False)
        self.assertEqual(label, "M1")

    def test_compute_event_outcome_label_leader(self) -> None:
        now = datetime.now(timezone.utc)
        rows = [
            {
                "market_title": "Leader",
                "settlement_value": None,
                "settlement_value_dollars": None,
                "market_open_time": now - timedelta(hours=1),
                "market_close_time": now + timedelta(hours=1),
                "yes_bid_dollars": "0.6",
                "yes_ask_dollars": "0.8",
            },
            {
                "market_title": "Lag",
                "settlement_value": None,
                "settlement_value_dollars": None,
                "market_open_time": now - timedelta(hours=1),
                "market_close_time": now + timedelta(hours=1),
                "yes_bid_dollars": "0.2",
                "yes_ask_dollars": "0.3",
            },
        ]
        label = web_portal._compute_event_outcome_label(rows, False)
        self.assertIn("Leading: Leader", label)

    def test_yes_price_closed_market_fallbacks(self) -> None:
        row = {
            "yes_bid_dollars": "0",
            "yes_ask_dollars": None,
            "price_dollars": None,
            "implied_yes_mid": "0",
            "candle_close": "0.4",
        }
        yes_price, yes_bid, yes_ask = web_portal._derive_yes_price(row, False)
        self.assertEqual(yes_price, Decimal("0.4"))
        self.assertIsNone(yes_bid)
        self.assertIsNone(yes_ask)
        row = {
            "yes_bid_dollars": None,
            "yes_ask_dollars": None,
            "price_dollars": None,
            "implied_yes_mid": None,
            "settlement_value": 25,
        }
        yes_price, _, _ = web_portal._derive_yes_price(row, False)
        self.assertEqual(yes_price, Decimal("0.25"))


class TestPortalUrlsAndTime(unittest.TestCase):
    def test_slug_and_urls(self) -> None:
        self.assertEqual(web_portal.slugify("Hello World!"), "hello-world")
        self.assertEqual(web_portal.derive_series_ticker("EV-123"), "ev")
        self.assertEqual(web_portal.derive_series_ticker(None), "")
        self.assertIsNone(web_portal.get_market_url(None))
        self.assertIsNone(web_portal.get_event_url(None, None, None))
        url = web_portal.get_market_url("M1", event_ticker="EV-123", event_title=None)
        self.assertIn("/ev/ev-123/m1", url)
        with patch.dict(
            os.environ,
            {"WEB_PORTAL_KALSHI_MARKET_URL_TEMPLATE": "https://x/{ticker}/{event_slug}"},
        ):
            url = web_portal.get_market_url("M1", event_ticker="EV1", event_title="Event")
        self.assertEqual(url, "https://x/m1/event")
        with patch.dict(os.environ, {"WEB_PORTAL_KALSHI_EVENT_URL_TEMPLATE": "https://x/{event_ticker}"}):
            url = web_portal.get_event_url("EV1", "SR", "Event")
        self.assertEqual(url, "https://x/ev1")
        with patch.dict(
            os.environ,
            {"WEB_PORTAL_KALSHI_MARKET_URL_TEMPLATE": "https://x/{missing}"},
        ):
            url = web_portal.get_market_url("M1", event_ticker="EV1", event_title="Event")
        self.assertTrue(url.startswith("https://kalshi.com/markets/"))
        with patch.dict(
            os.environ,
            {"WEB_PORTAL_KALSHI_EVENT_URL_TEMPLATE": "https://x/{missing}"},
        ):
            url = web_portal.get_event_url("EV1", "SR", "Event")
        self.assertTrue(url.startswith("https://kalshi.com/markets/"))

    def test_fmt_ts_and_version(self) -> None:
        ts = datetime(2024, 1, 1, 0, 0, 0)
        self.assertIn("2024-01-01", web_portal.fmt_ts(ts))
        self.assertEqual(web_portal.fmt_ts(None), "N/A")
        self.assertEqual(web_portal._format_pg_version(150002), "15.0.2")
        self.assertIsNone(web_portal._format_pg_version(None))

    def test_time_helpers(self) -> None:
        ts = web_portal._parse_ts("2024-01-01T00:00:00")
        self.assertIsNotNone(ts)
        self.assertIsNone(web_portal._parse_ts("not-a-date"))
        now = datetime(2024, 1, 1, 0, 10, tzinfo=timezone.utc)
        age_minutes, label = web_portal._format_age_minutes(
            datetime(2024, 1, 1, 0, 9, 30, tzinfo=timezone.utc), now
        )
        self.assertEqual(age_minutes, 0)
        self.assertEqual(label, "<1m")
        age_minutes, label = web_portal._format_age_minutes(
            datetime(2024, 1, 1, 0, 8, 0, tzinfo=timezone.utc), now
        )
        self.assertEqual((age_minutes, label), (2, "2m"))
        self.assertIsNotNone(web_portal._parse_epoch_seconds("100"))
        self.assertIsNone(web_portal._parse_epoch_seconds("bad"))
        self.assertIsNone(web_portal._parse_epoch_seconds(None))
        payload = web_portal._health_time_payload(now, now)
        self.assertEqual(payload["age_text"], "<1m ago")
        now_utc = web_portal._now_utc()
        self.assertIsNotNone(now_utc.tzinfo)

    def test_infer_strike_and_time_remaining(self) -> None:
        now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        open_time = now
        close_time = now + timedelta(hours=1)
        inferred = web_portal._infer_strike_period_from_times(open_time, close_time, 2.0, 36.0)
        self.assertEqual(inferred, "hour")
        with patch("src.web_portal._now_utc", return_value=now):
            self.assertEqual(web_portal.fmt_time_remaining(close_time), "1h")

    def test_clamp_limit_and_status(self) -> None:
        self.assertEqual(web_portal.clamp_limit(None), web_portal.DEFAULT_LIMIT)
        self.assertEqual(web_portal.clamp_limit(5000), web_portal.MAX_LIMIT)
        label, status = web_portal.normalize_status("open", None, None)
        self.assertEqual((label, status), ("Open", "open"))
        now = datetime.now(timezone.utc)
        label, status = web_portal.normalize_status(None, now + timedelta(hours=1), None)
        self.assertEqual(status, "scheduled")

    def test_normalize_status_closed_and_inactive(self) -> None:
        now = datetime.now(timezone.utc)
        label, status = web_portal.normalize_status(None, None, now - timedelta(seconds=1))
        self.assertEqual((label, status), ("Closed", "closed"))
        label, status = web_portal.normalize_status(None, None, None)
        self.assertEqual((label, status), ("Inactive", "inactive"))
        self.assertEqual(web_portal.clamp_limit("bad"), web_portal.DEFAULT_LIMIT)

    def test_require_password_and_auth(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                web_portal.require_password()
        with patch.dict(os.environ, {"WEB_PORTAL_PASSWORD": "pw"}):
            self.assertEqual(web_portal.require_password(), "pw")
        with web_portal.app.test_request_context("/"):
            self.assertFalse(web_portal.is_authenticated())
            web_portal.session["web_portal_authed"] = True
            self.assertTrue(web_portal.is_authenticated())


class DummyConn:
    def __init__(self, autocommit: bool = True) -> None:
        self.autocommit = autocommit
        self.committed = 0
        self.rolled_back = 0
        self.closed = 0

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolled_back += 1

    def close(self) -> None:
        self.closed += 1


class DummyPool:
    def __init__(self, conn: DummyConn, raise_timeout: bool = False) -> None:
        self.conn = conn
        self.raise_timeout = raise_timeout
        self.connection_calls = 0

    @contextmanager
    def connection(self):
        self.connection_calls += 1
        if self.raise_timeout:
            raise web_portal.PoolTimeout("timeout")
        yield self.conn


class DummyCursor:
    def __init__(self, row):
        self.row = row

    def execute(self, *args, **kwargs) -> None:
        return None

    def fetchone(self):
        return self.row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyConnWithCursor:
    def __init__(self, row) -> None:
        self.row = row

    def cursor(self, *args, **kwargs):
        return DummyCursor(self.row)


class DummyRowsCursor:
    def __init__(self, rows):
        self.rows = rows
        self.execute_calls = []

    def execute(self, *args, **kwargs) -> None:
        self.execute_calls.append((args, kwargs))

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyExecCursor:
    def __init__(self) -> None:
        self.calls = []

    def execute(self, sql, params=None) -> None:
        self.calls.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyRowsConn:
    def __init__(self, rows):
        self.cursor_obj = DummyRowsCursor(rows)

    def cursor(self, *args, **kwargs):
        return self.cursor_obj


class DummyExecConn:
    def __init__(self, cursor: DummyExecCursor) -> None:
        self.cursor_obj = cursor

    def cursor(self, *args, **kwargs):
        return self.cursor_obj


class RecordingCursor:
    def __init__(self, row=None, rows=None) -> None:
        self.row = row
        self.rows = rows if rows is not None else []
        self.execute_calls = []

    def execute(self, sql, params=None) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class RecordingConn:
    def __init__(self, cursor: RecordingCursor) -> None:
        self.cursor_obj = cursor

    def cursor(self, *args, **kwargs):
        return self.cursor_obj


class SequencedConn:
    def __init__(self, cursors) -> None:
        self.cursors = list(cursors)

    def cursor(self, *args, **kwargs):
        if not self.cursors:
            raise AssertionError("Unexpected cursor request")
        return self.cursors.pop(0)


class SequenceCursor:
    def __init__(self, fetchone_queue=None, fetchall_queue=None):
        self.fetchone_queue = list(fetchone_queue or [])
        self.fetchall_queue = list(fetchall_queue or [])
        self.execute_calls = []

    def execute(self, sql, params=None) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self):
        if self.fetchone_queue:
            return self.fetchone_queue.pop(0)
        return None

    def fetchall(self):
        if self.fetchall_queue:
            return self.fetchall_queue.pop(0)
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class SequenceConn:
    def __init__(self, cursors):
        self.cursors = list(cursors)

    def cursor(self, *args, **kwargs):
        return self.cursors.pop(0)


class TestPortalDbPool(unittest.TestCase):
    def setUp(self) -> None:
        self._db_pool = web_portal._DB_POOL
        web_portal._DB_POOL = None

    def tearDown(self) -> None:
        web_portal._DB_POOL = self._db_pool

    def test_db_pool_sizes_and_timeout(self) -> None:
        with patch.dict(
            os.environ,
            {
                "WEB_DB_POOL_MIN": "5",
                "WEB_DB_POOL_MAX": "2",
                "WEB_DB_POOL_TIMEOUT": "0",
            },
        ):
            self.assertEqual(web_portal._db_pool_sizes(), (5, 5))
            self.assertEqual(web_portal._db_pool_timeout(), 0.1)

    def test_get_db_pool_disabled(self) -> None:
        with patch.dict(os.environ, {"WEB_DB_POOL_ENABLE": "0"}):
            self.assertIsNone(web_portal._get_db_pool("postgres://example"))

    def test_get_db_pool_builds_once(self) -> None:
        class FakePool:
            def __init__(self, dsn, min_size, max_size, timeout):
                self.dsn = dsn
                self.min_size = min_size
                self.max_size = max_size
                self.timeout = timeout

        with patch.dict(
            os.environ,
            {
                "WEB_DB_POOL_ENABLE": "1",
                "WEB_DB_POOL_MIN": "2",
                "WEB_DB_POOL_MAX": "4",
                "WEB_DB_POOL_TIMEOUT": "1.5",
            },
        ):
            with patch.object(web_portal, "ConnectionPool", FakePool):
                pool = web_portal._get_db_pool("postgres://example")
                self.assertIsInstance(pool, FakePool)
                self.assertEqual(pool.min_size, 2)
                self.assertEqual(pool.max_size, 4)
                self.assertEqual(pool.timeout, 1.5)
                self.assertIs(web_portal._get_db_pool("postgres://example"), pool)

    def test_get_db_pool_returns_existing_inside_lock(self) -> None:
        sentinel = object()

        class FakeLock:
            def __enter__(self):
                web_portal._DB_POOL = sentinel
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch.dict(os.environ, {"WEB_DB_POOL_ENABLE": "1"}):
            with patch.object(web_portal, "_DB_POOL_LOCK", FakeLock()), \
                 patch.object(web_portal, "ConnectionPool") as pool_cls:
                pool = web_portal._get_db_pool("postgres://example")
        self.assertIs(pool, sentinel)
        pool_cls.assert_not_called()

    def test_db_connection_pool_commit_and_rollback(self) -> None:
        conn = DummyConn(autocommit=True)
        pool = DummyPool(conn)
        with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
            with patch.object(web_portal, "_get_db_pool", return_value=pool):
                with web_portal._db_connection() as got:
                    self.assertIs(got, conn)
                self.assertEqual(conn.committed, 1)
                self.assertEqual(conn.rolled_back, 0)
                self.assertTrue(conn.autocommit)
                with self.assertRaises(RuntimeError):
                    with web_portal._db_connection() as _:
                        raise RuntimeError("boom")
                self.assertEqual(conn.rolled_back, 1)

    def test_db_connection_pool_timeout(self) -> None:
        conn = DummyConn()
        pool = DummyPool(conn, raise_timeout=True)
        with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
            with patch.object(web_portal, "_get_db_pool", return_value=pool):
                with self.assertRaises(RuntimeError):
                    with web_portal._db_connection():
                        pass

    def test_db_connection_direct(self) -> None:
        conn = DummyConn(autocommit=False)
        with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
            with patch.object(web_portal, "_get_db_pool", return_value=None):
                with patch("src.web_portal.psycopg.connect", return_value=conn) as mocked:
                    with web_portal._db_connection() as got:
                        self.assertIs(got, conn)
                    mocked.assert_called_with("postgres://example")
        self.assertEqual(conn.committed, 1)
        self.assertEqual(conn.closed, 1)

    def test_db_connection_direct_rollback(self) -> None:
        conn = DummyConn(autocommit=False)
        with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
            with patch.object(web_portal, "_get_db_pool", return_value=None):
                with patch("src.web_portal.psycopg.connect", return_value=conn):
                    with self.assertRaises(RuntimeError):
                        with web_portal._db_connection():
                            raise RuntimeError("boom")
        self.assertEqual(conn.rolled_back, 1)
        self.assertEqual(conn.committed, 0)
        self.assertEqual(conn.closed, 1)

    def test_get_db_pool_init_failure(self) -> None:
        class BadPool:
            def __init__(self, *args, **kwargs):
                raise RuntimeError("boom")

        with patch.dict(os.environ, {"WEB_DB_POOL_ENABLE": "1"}):
            with patch.object(web_portal, "ConnectionPool", BadPool):
                with patch.object(web_portal.logger, "warning") as warn:
                    pool = web_portal._get_db_pool("postgres://example")
        self.assertIsNone(pool)
        warn.assert_called_once()

    def test_db_connection_missing_url(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                with web_portal._db_connection():
                    pass

    def test_db_connection_force_direct_timeout(self) -> None:
        conn = DummyConn(autocommit=False)
        with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
            with patch.object(web_portal, "_get_db_pool") as get_pool:
                with patch("src.web_portal.psycopg.connect", return_value=conn) as mocked:
                    with web_portal._db_connection(connect_timeout=7, force_direct=True) as got:
                        self.assertIs(got, conn)
                    get_pool.assert_not_called()
                    mocked.assert_called_with("postgres://example", connect_timeout=7)
        self.assertEqual(conn.committed, 1)
        self.assertEqual(conn.closed, 1)

    def test_maybe_prewarm_db_pool(self) -> None:
        conn = DummyConn()
        pool = DummyPool(conn)
        with patch.dict(
            os.environ,
            {
                "WEB_DB_POOL_PREWARM": "1",
                "DATABASE_URL": "postgres://example",
            },
        ):
            with patch.object(web_portal, "_get_db_pool", return_value=pool):
                web_portal._maybe_prewarm_db_pool()
                self.assertEqual(pool.connection_calls, 1)
        with patch.dict(os.environ, {"WEB_DB_POOL_PREWARM": "0"}):
            with patch.object(web_portal, "_get_db_pool") as mocked:
                web_portal._maybe_prewarm_db_pool()
                mocked.assert_not_called()

    def test_maybe_prewarm_db_pool_failure(self) -> None:
        class BadPool:
            @contextmanager
            def connection(self):
                raise RuntimeError("boom")
                yield None

        with patch.dict(
            os.environ,
            {
                "WEB_DB_POOL_PREWARM": "1",
                "DATABASE_URL": "postgres://example",
            },
        ):
            with patch.object(web_portal, "_get_db_pool", return_value=BadPool()):
                with patch.object(web_portal.logger, "warning") as warn:
                    web_portal._maybe_prewarm_db_pool()
        warn.assert_called_once()


class TestPortalQueueStream(unittest.TestCase):
    def test_queue_stream_flags(self) -> None:
        with patch.dict(
            os.environ,
            {
                "WEB_PORTAL_QUEUE_STREAM_ENABLE": "0",
                "WEB_PORTAL_QUEUE_STREAM_MIN_RELOAD_MS": "10",
            },
        ):
            self.assertFalse(web_portal._queue_stream_enabled())
            self.assertEqual(web_portal._queue_stream_min_reload_ms(), 5000)
            payload = web_portal.inject_queue_stream_enabled()
            self.assertFalse(payload["queue_stream_enabled"])
            self.assertEqual(payload["queue_stream_min_reload_ms"], 5000)

    def test_queue_stream_min_reload_valid(self) -> None:
        with patch.dict(os.environ, {"WEB_PORTAL_QUEUE_STREAM_MIN_RELOAD_MS": "2500"}):
            self.assertEqual(web_portal._queue_stream_min_reload_ms(), 2500)


class TestPortalHttpStatus(unittest.TestCase):
    def test_extract_http_status(self) -> None:
        class StatusError(Exception):
            pass

        exc = StatusError("missing")
        exc.status = "404"
        self.assertEqual(web_portal._extract_http_status(exc), 404)
        exc2 = StatusError("bad")
        exc2.status_code = "nope"
        self.assertIsNone(web_portal._extract_http_status(exc2))
        exc3 = StatusError("wrapped")
        exc3.http_resp = type("Resp", (), {"status_code": 500})()
        self.assertEqual(web_portal._extract_http_status(exc3), 500)
        exc4 = StatusError("wrapped")
        exc4.http_resp = type("Resp", (), {"status": "503"})()
        self.assertEqual(web_portal._extract_http_status(exc4), 503)
        exc5 = StatusError("bad")
        exc5.http_resp = type("Resp", (), {"status": "bad"})()
        self.assertIsNone(web_portal._extract_http_status(exc5))


class TestPortalEventQueryHelpers(unittest.TestCase):
    def test_fetch_event_count_variants(self) -> None:
        filters = web_portal.PortalFilters(
            search=None,
            categories=(),
            strike_period=None,
            close_window="1h",
            close_window_hours=1.0,
            status="open",
            sort="close_time",
            order="asc",
        )
        conn = SequenceConn([SequenceCursor(fetchone_queue=[(3,)])])
        self.assertEqual(web_portal._fetch_event_count(conn, "active", filters), 3)
        conn = SequenceConn([SequenceCursor(fetchone_queue=[(2,)])])
        self.assertEqual(web_portal._fetch_event_count(conn, "scheduled", filters), 2)
        conn = SequenceConn([SequenceCursor(fetchone_queue=[(1,)])])
        self.assertEqual(web_portal._fetch_event_count(conn, "closed", filters), 1)

    def test_fetch_event_categories_and_strike_periods(self) -> None:
        conn = DummyRowsConn([("Sports",), (None,), ("",), ("News",)])
        self.assertEqual(web_portal.fetch_event_categories(conn), ["Sports", "News"])
        conn = DummyRowsConn([("hour",), ("",), (None,), ("day",)])
        self.assertEqual(web_portal.fetch_strike_periods(conn), ["hour", "day"])

    def test_fetch_active_event_categories(self) -> None:
        filters = web_portal.PortalFilters(
            search=None,
            categories=(),
            strike_period=None,
            close_window=None,
            close_window_hours=None,
            status="open",
            sort=None,
            order=None,
        )
        conn = DummyRowsConn([("Sports",), ("",), (None,), ("News",)])
        self.assertEqual(web_portal.fetch_active_event_categories(conn, filters), ["Sports", "News"])

    def test_fetch_event_rows(self) -> None:
        filters = web_portal.PortalFilters(
            search=None,
            categories=(),
            strike_period=None,
            close_window=None,
            close_window_hours=None,
            status=None,
            sort=None,
            order=None,
        )
        row = {
            "event_ticker": "EV1",
            "event_title": "Event 1",
            "open_time": "2024-01-01T00:00:00Z",
            "close_time": "2024-01-02T00:00:00Z",
            "market_count": 2,
        }
        conn = DummyRowsConn([row])
        with patch.object(web_portal, "fmt_time_remaining", return_value="1h"):
            active = web_portal.fetch_active_events(conn, 5, filters)
        self.assertEqual(active[0]["event_ticker"], "EV1")
        self.assertEqual(active[0]["time_remaining"], "1h")

        conn = DummyRowsConn([row])
        scheduled = web_portal.fetch_scheduled_events(conn, 5, filters)
        self.assertEqual(scheduled[0]["event_ticker"], "EV1")

        conn = DummyRowsConn([row])
        closed = web_portal.fetch_closed_events(conn, 5, filters)
        self.assertEqual(closed[0]["event_ticker"], "EV1")

    def test_fetch_event_market_rows(self) -> None:
        rows = [{"ticker": "M1"}]
        conn = DummyRowsConn(rows)
        self.assertEqual(web_portal._fetch_event_market_rows(conn, "EV1"), rows)


class TestPortalSparklineHelpers(unittest.TestCase):
    def test_sparkline_value(self) -> None:
        self.assertEqual(
            web_portal._sparkline_value({"implied_yes_mid": Decimal("0.2")}),
            0.2,
        )
        self.assertEqual(
            web_portal._sparkline_value({"price_dollars": Decimal("0.3")}),
            0.3,
        )
        self.assertEqual(
            web_portal._sparkline_value(
                {"yes_bid_dollars": Decimal("0.2"), "yes_ask_dollars": Decimal("0.4")}
            ),
            0.3,
        )
        self.assertIsNone(web_portal._sparkline_value({}))

    def test_build_event_sparklines(self) -> None:
        rows = [
            {"ticker": "T1", "implied_yes_mid": Decimal("0.25")},
            {"ticker": "T1", "yes_bid_dollars": Decimal("0.2"), "yes_ask_dollars": Decimal("0.4")},
            {"ticker": "T2", "price_dollars": Decimal("1.2")},
            {"ticker": None, "price_dollars": Decimal("0.5")},
        ]
        conn = DummyRowsConn(rows)
        with patch.dict(os.environ, {"WEB_PORTAL_EVENT_SPARKLINE_POINTS": "4"}):
            points = web_portal._build_event_sparklines(conn, ["T1", "T2", "T3"])
        self.assertEqual(points["T1"], [0.25, 0.3])
        self.assertEqual(points["T2"], [1.0])
        self.assertEqual(points["T3"], [])


class TestPortalForecastSeriesHelpers(unittest.TestCase):
    def test_build_event_forecast_series_from_candles(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        market_rows = [
            {
                "ticker": "T1",
                "title": "Outcome A",
                "market_open_time": None,
                "market_close_time": None,
            },
            {
                "ticker": "T2",
                "title": "Outcome B",
                "market_open_time": None,
                "market_close_time": None,
            },
        ]
        candle_rows = [
            {"market_ticker": "T1", "end_period_ts": now, "close": Decimal("0.55")},
            {
                "market_ticker": "T1",
                "end_period_ts": now + timedelta(minutes=5),
                "close": Decimal("0.65"),
            },
        ]
        conn = DummyRowsConn(candle_rows)

        def fake_yes_price(row, _is_open):
            if row.get("ticker") == "T1":
                return Decimal("0.7"), None, None
            return Decimal("0.2"), None, None

        with patch.dict(
            os.environ,
            {
                "WEB_PORTAL_EVENT_FORECAST_SERIES_LIMIT": "1",
                "WEB_PORTAL_EVENT_FORECAST_POINTS": "2",
            },
        ):
            with patch.object(
                web_portal,
                "_derive_yes_price",
                side_effect=fake_yes_price,
                create=True,
            ):
                series, note = web_portal._build_event_forecast_series(conn, market_rows)
        self.assertEqual(len(series), 1)
        self.assertEqual(series[0]["ticker"], "T1")
        self.assertEqual(len(series[0]["points"]), 2)
        self.assertIn("Showing top 1 of 2", note)

    def test_build_event_forecast_series_tick_fallback(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        market_rows = [
            {
                "ticker": "T1",
                "title": "Outcome A",
                "market_open_time": None,
                "market_close_time": None,
            },
            {
                "ticker": "T2",
                "title": "Outcome B",
                "market_open_time": None,
                "market_close_time": None,
            },
        ]
        conn = SequenceConn(
            [
                SequenceCursor(fetchall_queue=[[]]),
                SequenceCursor(
                    fetchall_queue=[
                        [
                            {
                                "ticker": "T1",
                                "ts": now,
                                "price_dollars": Decimal("0.25"),
                            }
                        ]
                    ]
                ),
            ]
        )
        with patch.dict(
            os.environ,
            {
                "WEB_PORTAL_EVENT_FORECAST_SERIES_LIMIT": "2",
                "WEB_PORTAL_EVENT_FORECAST_POINTS": "2",
            },
        ):
            with patch.object(
                web_portal,
                "_derive_yes_price",
                return_value=(Decimal("0.25"), None, None),
                create=True,
            ):
                series, note = web_portal._build_event_forecast_series(conn, market_rows)
        self.assertEqual(len(series), 1)
        self.assertIn("Using tick history", note)
        self.assertIn("No tick history for 1 outcome", note)


class TestPortalEventDetail(unittest.TestCase):
    def test_fetch_event_detail_missing(self) -> None:
        conn = SequenceConn([SequenceCursor(fetchone_queue=[None])])
        self.assertIsNone(web_portal.fetch_event_detail(conn, "EV1"))

    def test_fetch_event_detail_builds_payload(self) -> None:
        event_row = {
            "event_ticker": "EV1",
            "event_title": "Event 1",
            "event_sub_title": "Sub",
            "event_category": "Category",
            "series_ticker": "SER",
            "strike_date": "2024-01-01T00:00:00Z",
            "strike_period": "hour",
            "mutually_exclusive": True,
            "available_on_brokers": False,
            "product_metadata": None,
            "open_time": None,
            "close_time": None,
            "market_count": 1,
        }
        market_rows = [
            {
                "ticker": "MKT1",
                "market_title": "Market 1",
                "market_subtitle": "Outcome A",
                "yes_sub_title": "Yes",
                "no_sub_title": "No",
                "market_open_time": None,
                "market_close_time": None,
                "settlement_value": None,
                "settlement_value_dollars": None,
                "last_tick_ts": "2024-01-01T00:00:00Z",
                "tick_source": "live_snapshot",
                "implied_yes_mid": Decimal("0.4"),
                "price_dollars": None,
                "yes_bid_dollars": Decimal("0.35"),
                "yes_ask_dollars": Decimal("0.45"),
                "candle_end_ts": None,
                "candle_close": None,
                "predicted_yes_prob": Decimal("0.5"),
                "prediction_confidence": Decimal("0.9"),
                "prediction_ts": "2024-01-01T00:00:00Z",
            }
        ]
        conn = SequenceConn([SequenceCursor(fetchone_queue=[event_row])])
        with patch.object(web_portal, "_fetch_event_market_rows", return_value=market_rows):
            with patch.object(
                web_portal,
                "_build_event_sparklines",
                return_value={"MKT1": [0.1, 0.2]},
            ):
                with patch.object(
                    web_portal,
                    "_build_event_forecast_series",
                    return_value=([{"label": "Outcome A"}], "note"),
                ):
                    with patch.object(
                        web_portal,
                        "_compute_event_outcome_label",
                        return_value="Outcome",
                    ):
                        with patch.object(
                            web_portal,
                            "_derive_yes_price",
                            return_value=(
                                Decimal("0.4"),
                                Decimal("0.35"),
                                Decimal("0.45"),
                            ),
                            create=True,
                        ):
                            with patch.object(web_portal, "_is_wide_spread", return_value=False):
                                with patch.object(
                                    web_portal,
                                    "_pricing_error_for_row",
                                    return_value="bad",
                                ):
                                    payload = web_portal.fetch_event_detail(conn, "EV1")
        self.assertEqual(payload["event_ticker"], "EV1")
        self.assertEqual(payload["event_outcome_label"], "Outcome")
        self.assertTrue(payload["pricing_errors"])
        self.assertFalse(payload["ticks_missing"])
        self.assertEqual(payload["outcomes"][0]["market_ticker"], "MKT1")
        self.assertEqual(payload["forecast_note"], "note")


class TestPortalMarketDetail(unittest.TestCase):
    def test_fetch_market_detail_updates_metadata(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        row = {
            "ticker": "M1",
            "event_ticker": "EV1",
            "title": "Market 1",
            "subtitle": "Subtitle",
            "yes_sub_title": "Yes",
            "no_sub_title": "No",
            "category": "Category",
            "response_price_units": "dollars",
            "tick_size": 1,
            "risk_limit_cents": 100,
            "strike_type": "binary",
            "floor_strike": None,
            "cap_strike": None,
            "functional_strike": None,
            "settlement_value": None,
            "settlement_value_dollars": None,
            "rules_primary": "rules",
            "rules_secondary": None,
            "price_level_structure": json.dumps({"levels": []}),
            "price_ranges": None,
            "custom_strike": None,
            "mve_selected_legs": None,
            "open_time": "2024-01-01T00:00:00Z",
            "close_time": "2024-01-02T00:00:00Z",
            "expiration_time": None,
            "settlement_ts": None,
            "status_label": None,
            "status_class": None,
            "event_title": "Event 1",
            "event_sub_title": "Event sub",
            "event_category": "Cat",
            "series_ticker": "SER",
            "strike_date": "2024-01-01T00:00:00Z",
            "strike_period": "hour",
            "mutually_exclusive": True,
            "available_on_brokers": False,
            "product_metadata": None,
            "active_status": None,
            "active_last_seen": None,
            "last_tick_ts": None,
            "implied_yes_mid": Decimal("0.4"),
            "price_dollars": None,
            "yes_bid_dollars": Decimal("0.3"),
            "yes_ask_dollars": Decimal("0.5"),
            "volume": 10,
            "open_interest": 5,
            "candle_end_ts": now,
            "candle_close": Decimal("0.45"),
        }
        candle_rows = [
            {
                "end_period_ts": now,
                "open": Decimal("0.2"),
                "high": Decimal("0.4"),
                "low": Decimal("0.1"),
                "close": Decimal("0.3"),
                "volume": 10,
            }
        ]
        prediction_rows = [
            {
                "created_at": now,
                "predicted_yes_prob": Decimal("0.25"),
                "confidence": Decimal("0.7"),
                "rationale": "because",
                "agent": "agent",
                "model": "model",
            }
        ]
        conn = SequenceConn(
            [
                SequenceCursor(fetchone_queue=[row]),
                SequenceCursor(
                    fetchone_queue=[{"period_interval_minutes": 5}],
                    fetchall_queue=[candle_rows],
                ),
                SequenceCursor(fetchall_queue=[prediction_rows]),
            ]
        )
        market_data = {
            "price_ranges": [{"min": 0, "max": 1}],
            "custom_strike": {"value": 10},
            "mve_selected_legs": ["L1"],
        }
        with patch.object(web_portal, "_get_market_data", return_value=(market_data, None, None)):
            with patch.object(
                web_portal,
                "_get_event_metadata",
                return_value=({"meta": "data"}, None),
            ):
                with patch.object(web_portal, "_update_market_extras") as update_market:
                    with patch.object(web_portal, "_update_event_metadata") as update_event:
                        with patch.object(
                            web_portal,
                            "_fetch_event_market_rows",
                            return_value=[{"ticker": "M1"}],
                        ):
                            with patch.object(
                                web_portal,
                                "_compute_event_outcome_label",
                                return_value="Outcome",
                            ):
                                market = web_portal.fetch_market_detail(conn, "M1")
        self.assertEqual(market["market_ticker"], "M1")
        self.assertEqual(market["status_label"], "Unknown")
        self.assertEqual(market["status_class"], "status-unknown")
        self.assertEqual(len(market["candles"]), 1)
        self.assertEqual(market["predictions"][0]["agent"], "agent / model")
        update_market.assert_called_once()
        update_event.assert_called_once()


class TestPortalKalshiClient(unittest.TestCase):
    def setUp(self) -> None:
        self._client = web_portal._KALSHI_CLIENT
        self._error = web_portal._KALSHI_CLIENT_ERROR
        web_portal._KALSHI_CLIENT = None
        web_portal._KALSHI_CLIENT_ERROR = None

    def tearDown(self) -> None:
        web_portal._KALSHI_CLIENT = self._client
        web_portal._KALSHI_CLIENT_ERROR = self._error

    def test_load_kalshi_client_missing_creds(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            client, err = web_portal._load_kalshi_client()
        self.assertIsNone(client)
        self.assertEqual(err, "Kalshi API credentials are not configured.")

    def test_load_kalshi_client_pem_unreadable(self) -> None:
        with patch.dict(
            os.environ,
            {"KALSHI_API_KEY_ID": "id", "KALSHI_PRIVATE_KEY_PEM_PATH": "/tmp/key.pem"},
        ):
            with patch("src.web_portal.open", side_effect=OSError("nope")):
                client, err = web_portal._load_kalshi_client()
        self.assertIsNone(client)
        self.assertEqual(err, "Kalshi private key PEM could not be read.")

    def test_load_kalshi_client_make_client_error(self) -> None:
        with patch.dict(
            os.environ,
            {"KALSHI_API_KEY_ID": "id", "KALSHI_PRIVATE_KEY_PEM_PATH": "/tmp/key.pem"},
        ):
            with patch("src.web_portal.open", mock_open(read_data="pem")):
                with patch(
                    "src.web_portal.make_client",
                    side_effect=web_portal.KalshiSdkError("fail"),
                ):
                    client, err = web_portal._load_kalshi_client()
        self.assertIsNone(client)
        self.assertEqual(err, "Kalshi SDK client could not be initialized.")

    def test_load_kalshi_client_success_and_cache(self) -> None:
        sentinel = object()
        with patch.dict(
            os.environ,
            {"KALSHI_API_KEY_ID": "id", "KALSHI_PRIVATE_KEY_PEM_PATH": "/tmp/key.pem"},
        ):
            with patch("src.web_portal.open", mock_open(read_data="pem")) as mocked:
                with patch("src.web_portal.make_client", return_value=sentinel):
                    client, err = web_portal._load_kalshi_client()
                    self.assertIs(client, sentinel)
                    self.assertIsNone(err)
                    client2, err2 = web_portal._load_kalshi_client()
                    self.assertIs(client2, sentinel)
                    self.assertIsNone(err2)
        mocked.assert_called_once()


class TestPortalMarketData(unittest.TestCase):
    def test_get_market_data_load_error(self) -> None:
        with patch.object(web_portal, "_load_kalshi_client", return_value=(None, "no creds")):
            data, err, status = web_portal._get_market_data("T1")
        self.assertIsNone(data)
        self.assertEqual(err, "no creds")
        self.assertIsNone(status)

    def test_get_market_data_rate_limited(self) -> None:
        class RateError(Exception):
            def __init__(self, message: str, status: int):
                super().__init__(message)
                self.status = status

        client = MagicMock()
        client.get_market.side_effect = RateError("rate limited", 429)
        with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
            with patch("src.web_portal.rest_wait"):
                with patch("src.web_portal.rest_register_rate_limit") as register:
                    data, err, status = web_portal._get_market_data("TICK")
        self.assertIsNone(data)
        self.assertEqual(status, 429)
        self.assertIn("rate", err)
        register.assert_called_once()

    def test_get_market_data_model_dump(self) -> None:
        class Market:
            def model_dump(self, mode="json"):
                return {"ticker": "T1"}

        class Resp:
            def __init__(self, market):
                self.market = market

        client = MagicMock()
        client.get_market.return_value = Resp(Market())
        with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
            with patch("src.web_portal.rest_wait"):
                data, err, status = web_portal._get_market_data("T1")
        self.assertEqual(data["ticker"], "T1")
        self.assertIsNone(err)
        self.assertIsNone(status)

    def test_get_market_data_fallback_empty(self) -> None:
        class Market:
            pass

        class Resp:
            def __init__(self, market):
                self.market = market

        client = MagicMock()
        client.get_market.return_value = Resp(Market())
        with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
            with patch("src.web_portal.rest_wait"):
                data, err, status = web_portal._get_market_data("T1")
        self.assertEqual(data, {})
        self.assertIsNone(err)
        self.assertIsNone(status)

    def test_get_market_data_not_found(self) -> None:
        class Resp:
            def __init__(self, market):
                self.market = market

        client = MagicMock()
        client.get_market.return_value = Resp(None)
        with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
            with patch("src.web_portal.rest_wait"):
                data, err, status = web_portal._get_market_data("T1")
        self.assertIsNone(data)
        self.assertEqual(err, "Market not found.")
        self.assertIsNone(status)

    def test_get_market_data_dict_market(self) -> None:
        client = MagicMock()
        client.get_market.return_value = MagicMock(market={"ticker": "T1"})
        with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
            with patch("src.web_portal.rest_wait"):
                data, err, status = web_portal._get_market_data("T1")
        self.assertEqual(data, {"ticker": "T1"})
        self.assertIsNone(err)
        self.assertIsNone(status)

    def test_get_market_data_exception_non_rate(self) -> None:
        class Boom(Exception):
            pass

        exc = Boom("boom")
        exc.status_code = 500
        client = MagicMock()
        client.get_market.side_effect = exc
        with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
            with patch("src.web_portal.rest_wait"):
                with patch.object(web_portal.logger, "exception") as log_exc:
                    data, err, status = web_portal._get_market_data("T1")
        self.assertIsNone(data)
        self.assertEqual(err, "boom")
        self.assertEqual(status, 500)
        log_exc.assert_called_once()


class TestPortalMetadataCache(unittest.TestCase):
    def setUp(self) -> None:
        self._cache = dict(web_portal._EVENT_METADATA_CACHE)
        web_portal._EVENT_METADATA_CACHE.clear()

    def tearDown(self) -> None:
        web_portal._EVENT_METADATA_CACHE = self._cache

    def test_event_metadata_cache_ttl_and_store(self) -> None:
        with patch.dict(os.environ, {"WEB_PORTAL_EVENT_METADATA_CACHE_SEC": "0"}):
            web_portal._store_event_metadata_cache("EV1", {"a": 1}, None)
            self.assertNotIn("EV1", web_portal._EVENT_METADATA_CACHE)
            self.assertIsNone(web_portal._load_event_metadata_cache("EV1"))
        with patch.dict(os.environ, {"WEB_PORTAL_EVENT_METADATA_CACHE_SEC": "10"}):
            with patch("src.web_portal.time.monotonic", return_value=100.0):
                web_portal._store_event_metadata_cache("EV1", {"a": 1}, None)
            with patch("src.web_portal.time.monotonic", return_value=105.0):
                cached = web_portal._load_event_metadata_cache("EV1")
            self.assertEqual(cached, ({"a": 1}, None))
            with patch("src.web_portal.time.monotonic", return_value=200.0):
                expired = web_portal._load_event_metadata_cache("EV1")
            self.assertIsNone(expired)

    def test_event_metadata_cache_empty(self) -> None:
        with patch.dict(os.environ, {"WEB_PORTAL_EVENT_METADATA_CACHE_SEC": "10"}):
            self.assertIsNone(web_portal._load_event_metadata_cache("EV1"))

    def test_get_event_metadata_success_and_missing(self) -> None:
        client = MagicMock()
        client.get_event.return_value = MagicMock(
            event={"product_metadata": {"x": 1}}
        )
        with patch.object(web_portal, "_load_event_metadata_cache", return_value=None):
            with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
                with patch("src.web_portal.rest_wait"):
                    with patch.object(web_portal, "_store_event_metadata_cache") as store:
                        data, err = web_portal._get_event_metadata("EV1")
        self.assertEqual(data, {"x": 1})
        self.assertIsNone(err)
        store.assert_called_with("EV1", {"x": 1}, None)
        with patch.object(web_portal, "_load_event_metadata_cache", return_value=None):
            with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
                with patch("src.web_portal.rest_wait"):
                    with patch.object(web_portal, "_store_event_metadata_cache") as store:
                        client.get_event.return_value = MagicMock(event={})
                        data, err = web_portal._get_event_metadata("EV1")
        self.assertIsNone(data)
        self.assertEqual(err, "Event metadata missing.")
        store.assert_called_with("EV1", None, "Event metadata missing.")

    def test_get_event_metadata_load_error(self) -> None:
        with patch.object(web_portal, "_load_event_metadata_cache", return_value=None):
            with patch.object(web_portal, "_load_kalshi_client", return_value=(None, "no creds")):
                data, err = web_portal._get_event_metadata("EV1")
        self.assertIsNone(data)
        self.assertEqual(err, "no creds")

    def test_get_event_metadata_missing_client_method(self) -> None:
        client = object()
        with patch.object(web_portal, "_load_event_metadata_cache", return_value=None):
            with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
                data, err = web_portal._get_event_metadata("EV1")
        self.assertIsNone(data)
        self.assertEqual(err, "Event metadata API unavailable.")

    def test_get_event_metadata_fallback_empty(self) -> None:
        class Event:
            pass

        client = MagicMock()
        client.get_event.return_value = MagicMock(event=Event())
        with patch.object(web_portal, "_load_event_metadata_cache", return_value=None):
            with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
                with patch("src.web_portal.rest_wait"):
                    with patch.object(web_portal, "_store_event_metadata_cache") as store:
                        data, err = web_portal._get_event_metadata("EV1")
        self.assertIsNone(data)
        self.assertEqual(err, "Event metadata missing.")
        store.assert_called_with("EV1", None, "Event metadata missing.")

    def test_get_event_metadata_rate_limit(self) -> None:
        class RateError(Exception):
            def __init__(self, message: str, status: int):
                super().__init__(message)
                self.status = status

        client = MagicMock()
        client.get_event.side_effect = RateError("rate limited", 429)
        with patch.object(web_portal, "_load_event_metadata_cache", return_value=None):
            with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
                with patch("src.web_portal.rest_wait"):
                    with patch("src.web_portal.rest_register_rate_limit") as register:
                        with patch.object(web_portal, "_store_event_metadata_cache"):
                            data, err = web_portal._get_event_metadata("EV1")
        self.assertIsNone(data)
        self.assertIn("rate", err)
        register.assert_called_once()

    def test_get_event_metadata_missing_ticker(self) -> None:
        data, err = web_portal._get_event_metadata(None)
        self.assertIsNone(data)
        self.assertEqual(err, "Event ticker missing.")

    def test_get_event_metadata_cached(self) -> None:
        with patch.object(web_portal, "_load_event_metadata_cache", return_value=({"x": 1}, None)):
            with patch.object(web_portal, "_load_kalshi_client") as load_client:
                data, err = web_portal._get_event_metadata("EV1")
        self.assertEqual(data, {"x": 1})
        self.assertIsNone(err)
        load_client.assert_not_called()

    def test_get_event_metadata_api_unavailable(self) -> None:
        client = object()
        with patch.object(web_portal, "_load_event_metadata_cache", return_value=None):
            with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
                data, err = web_portal._get_event_metadata("EV1")
        self.assertIsNone(data)
        self.assertEqual(err, "Event metadata API unavailable.")

    def test_get_event_metadata_event_not_found(self) -> None:
        client = MagicMock()
        client.get_event.return_value = MagicMock(event=None)
        with patch.object(web_portal, "_load_event_metadata_cache", return_value=None):
            with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
                with patch("src.web_portal.rest_wait"):
                    with patch.object(web_portal, "_store_event_metadata_cache") as store:
                        data, err = web_portal._get_event_metadata("EV1")
        self.assertIsNone(data)
        self.assertEqual(err, "Event not found.")
        store.assert_called_with("EV1", None, "Event not found.")

    def test_get_event_metadata_model_dump(self) -> None:
        class Event:
            def model_dump(self, mode="json"):
                return {"product_metadata": {"a": 1}}

        client = MagicMock()
        client.get_event.return_value = MagicMock(event=Event())
        with patch.object(web_portal, "_load_event_metadata_cache", return_value=None):
            with patch.object(web_portal, "_load_kalshi_client", return_value=(client, None)):
                with patch("src.web_portal.rest_wait"):
                    with patch.object(web_portal, "_store_event_metadata_cache") as store:
                        data, err = web_portal._get_event_metadata("EV1")
        self.assertEqual(data, {"a": 1})
        self.assertIsNone(err)
        store.assert_called_with("EV1", {"a": 1}, None)


class TestPortalTickLoading(unittest.TestCase):
    def test_load_open_market_tickers(self) -> None:
        conn = DummyRowsConn([("M1",), ("M2",)])
        tickers = web_portal._load_open_market_tickers(conn, limit=2, min_age_sec=0)
        self.assertEqual(tickers, ["M1", "M2"])
        self.assertTrue(conn.cursor_obj.execute_calls)

    def test_load_latest_tick(self) -> None:
        row = {"ts": datetime(2024, 1, 1, tzinfo=timezone.utc), "price_dollars": Decimal("0.5")}
        conn = DummyRowsConn([row])
        tick = web_portal._load_latest_tick(conn, "T1")
        self.assertEqual(tick, row)


class TestPortalSnapshotHelpers(unittest.TestCase):
    def test_snapshot_from_tick(self) -> None:
        tick = {
            "ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "implied_yes_mid": None,
            "yes_bid_dollars": Decimal("0.4"),
            "yes_ask_dollars": Decimal("0.6"),
            "price_dollars": Decimal("0.5"),
        }
        snapshot = web_portal._snapshot_from_tick(tick)
        self.assertEqual(snapshot["yes_mid"], "$0.5000")
        self.assertEqual(snapshot["snapshot_source"], "market_ticks")

    def test_snapshot_from_tick_bid_or_ask(self) -> None:
        tick = {
            "ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "implied_yes_mid": None,
            "yes_bid_dollars": Decimal("0.3"),
            "yes_ask_dollars": None,
            "price_dollars": None,
        }
        snapshot = web_portal._snapshot_from_tick(tick)
        self.assertEqual(snapshot["yes_mid"], "$0.3000")
        tick = {
            "ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "implied_yes_mid": None,
            "yes_bid_dollars": None,
            "yes_ask_dollars": Decimal("0.7"),
            "price_dollars": None,
        }
        snapshot = web_portal._snapshot_from_tick(tick)
        self.assertEqual(snapshot["yes_mid"], "$0.7000")

    def test_prefer_tick_snapshot(self) -> None:
        now = datetime.now(timezone.utc)
        tick = {"ts": now - timedelta(seconds=120)}
        with patch.object(web_portal, "_load_latest_tick", return_value=tick):
            with patch.object(web_portal, "_snapshot_from_tick", return_value={"ok": True}):
                stale = web_portal._prefer_tick_snapshot(
                    MagicMock(), "T1", freshness_sec=60, allow_stale=False
                )
                self.assertIsNone(stale)
                allowed = web_portal._prefer_tick_snapshot(
                    MagicMock(), "T1", freshness_sec=60, allow_stale=True
                )
                self.assertEqual(allowed, {"ok": True})
        tick = {"ts": now}
        with patch.object(web_portal, "_load_latest_tick", return_value=tick):
            with patch.object(web_portal, "_snapshot_from_tick", return_value={"ok": True}):
                fresh = web_portal._prefer_tick_snapshot(
                    MagicMock(), "T1", freshness_sec=60, allow_stale=False
                )
                self.assertEqual(fresh, {"ok": True})
        with patch.object(web_portal, "_load_latest_tick", return_value=None):
            missing = web_portal._prefer_tick_snapshot(MagicMock(), "T1", freshness_sec=60)
        self.assertIsNone(missing)

    def test_market_is_closed(self) -> None:
        self.assertFalse(web_portal._market_is_closed(DummyConnWithCursor(None), "T1"))
        self.assertFalse(web_portal._market_is_closed(DummyConnWithCursor((None,)), "T1"))
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        self.assertFalse(
            web_portal._market_is_closed(DummyConnWithCursor((future,)), "T1")
        )
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        self.assertTrue(web_portal._market_is_closed(DummyConnWithCursor((past,)), "T1"))
        naive = datetime.utcnow() - timedelta(hours=1)
        self.assertTrue(web_portal._market_is_closed(DummyConnWithCursor((naive,)), "T1"))

    def test_snapshot_allow_closed_and_backoff(self) -> None:
        with web_portal.app.test_request_context("/?include_closed=1"):
            self.assertTrue(web_portal._snapshot_allows_closed())
        with web_portal.app.test_request_context("/?include_closed=0"):
            self.assertFalse(web_portal._snapshot_allows_closed())
        with web_portal.app.test_request_context("/?include_closed=maybe"):
            self.assertFalse(web_portal._snapshot_allows_closed())
        with patch.dict(os.environ, {"WEB_PORTAL_SNAPSHOT_ALLOW_CLOSED": "1"}):
            with web_portal.app.test_request_context("/"):
                self.assertTrue(web_portal._snapshot_allows_closed())
        with patch.dict(os.environ, {"WEB_PORTAL_SNAPSHOT_ALLOW_CLOSED": "0"}, clear=True):
            with web_portal.app.test_request_context("/"):
                self.assertFalse(web_portal._snapshot_allows_closed())
        with patch("src.web_portal.rest_backoff_remaining", return_value=12.5):
            self.assertEqual(web_portal._snapshot_backoff_remaining(), 12.5)
        with patch("src.web_portal.rest_apply_cooldown") as cooldown:
            web_portal._set_snapshot_backoff(30)
            cooldown.assert_called_once_with(30)

    def test_fetch_live_snapshot(self) -> None:
        with patch.object(
            web_portal, "_get_market_data", return_value=(None, "oops", 429)
        ):
            payload, tick = web_portal.fetch_live_snapshot("T1")
        self.assertIsNone(tick)
        self.assertTrue(payload.get("rate_limited"))
        market_data = {
            "ticker": "T1",
            "status": "open",
            "yes_bid_dollars": "0.4",
            "yes_ask_dollars": "0.6",
            "last_price_dollars": "0.5",
            "volume": "10",
            "open_interest": "5",
        }
        with patch.object(
            web_portal, "_get_market_data", return_value=(market_data, None, None)
        ):
            payload, tick = web_portal.fetch_live_snapshot("T1")
        self.assertIsNotNone(tick)
        self.assertEqual(tick["price"], 50)
        self.assertEqual(tick["yes_bid"], 40)
        self.assertEqual(tick["yes_ask"], 60)
        self.assertEqual(payload["yes_mid"], "$0.5000")

    def test_fetch_live_snapshot_cents(self) -> None:
        market_data = {
            "ticker": "T2",
            "status": "open",
            "yes_bid": 0,
            "yes_bid_cents": 0,
            "yes_ask": 100,
            "last_price": 40,
            "volume": "5",
            "open_interest": "2",
        }
        with patch.object(
            web_portal, "_get_market_data", return_value=(market_data, None, None)
        ):
            payload, tick = web_portal.fetch_live_snapshot("T2")
        self.assertEqual(tick["price"], 40)
        self.assertEqual(tick["yes_bid"], 0)
        self.assertEqual(tick["yes_ask"], 100)
        self.assertEqual(payload["yes_mid"], "$0.5000")

    def test_fetch_live_snapshot_yes_mid_fallbacks(self) -> None:
        cases = [
            ({"ticker": "T1", "status": "open", "yes_bid_dollars": "0.4"}, "$0.4000"),
            ({"ticker": "T2", "status": "open", "yes_ask_dollars": "0.7"}, "$0.7000"),
            ({"ticker": "T3", "status": "open", "last_price_dollars": "0.25"}, "$0.2500"),
        ]
        for market_data, expected in cases:
            with patch.object(
                web_portal, "_get_market_data", return_value=(market_data, None, None)
            ):
                payload, tick = web_portal.fetch_live_snapshot(market_data["ticker"])
            self.assertEqual(payload["yes_mid"], expected)
            self.assertIsNotNone(tick)

    def test_fetch_live_snapshot_price_fallbacks(self) -> None:
        market_data = {
            "ticker": "T4",
            "status": "open",
            "yes_bid_cents": 30,
            "yes_ask_cents": 70,
        }
        with patch.object(
            web_portal, "_get_market_data", return_value=(market_data, None, None)
        ):
            _, tick = web_portal.fetch_live_snapshot("T4")
        self.assertEqual(tick["price_dollars"], Decimal("0.5"))

        market_data = {
            "ticker": "T5",
            "status": "open",
            "yes_bid_cents": 25,
        }
        with patch.object(
            web_portal, "_get_market_data", return_value=(market_data, None, None)
        ):
            _, tick = web_portal.fetch_live_snapshot("T5")
        self.assertEqual(tick["price_dollars"], Decimal("0.25"))
        self.assertEqual(tick["implied_yes_mid"], Decimal("0.250000"))

        market_data = {
            "ticker": "T6",
            "status": "open",
            "yes_ask_cents": 80,
        }
        with patch.object(
            web_portal, "_get_market_data", return_value=(market_data, None, None)
        ):
            _, tick = web_portal.fetch_live_snapshot("T6")
        self.assertEqual(tick["price_dollars"], Decimal("0.8"))


class TestPortalMetadataHelpers(unittest.TestCase):
    def test_parse_json_and_custom_strike(self) -> None:
        self.assertEqual(web_portal._maybe_parse_json({"a": 1}), {"a": 1})
        self.assertEqual(web_portal._maybe_parse_json("[1]"), [1])
        self.assertIsNone(web_portal._maybe_parse_json("bad"))
        self.assertIsNone(web_portal._maybe_parse_json("  "))
        self.assertIsNone(web_portal._maybe_parse_json(None))
        self.assertIsNone(web_portal._maybe_parse_json(5))
        self.assertIsNone(
            web_portal._derive_custom_strike(
                {
                    "strike_type": None,
                    "floor_strike": None,
                    "cap_strike": None,
                    "functional_strike": None,
                }
            )
        )
        self.assertEqual(
            web_portal._derive_custom_strike(
                {
                    "strike_type": "custom",
                    "floor_strike": 1,
                    "cap_strike": None,
                    "functional_strike": 2,
                }
            ),
            {"strike_type": "custom", "floor_strike": 1, "functional_strike": 2},
        )

    def test_extract_event_metadata(self) -> None:
        self.assertEqual(web_portal._extract_event_metadata({"product_metadata": {"x": 1}}), {"x": 1})
        nested = {"event": {"event_metadata": {"y": 2}}}
        self.assertEqual(web_portal._extract_event_metadata(nested), {"y": 2})
        self.assertIsNone(web_portal._extract_event_metadata({}))
        self.assertIsNone(web_portal._extract_event_metadata({"event": {}}))
        self.assertIsNone(web_portal._extract_event_metadata(None))


class TestPortalUpdateHelpers(unittest.TestCase):
    def test_update_market_extras(self) -> None:
        cursor = DummyExecCursor()
        conn = DummyExecConn(cursor)
        web_portal._update_market_extras(conn, "T1")
        self.assertEqual(cursor.calls, [])
        web_portal._update_market_extras(
            conn,
            "T1",
            price_ranges={"a": 1},
            custom_strike={"b": 2},
            mve_selected_legs={"c": 3},
        )
        self.assertEqual(len(cursor.calls), 1)
        sql, params = cursor.calls[0]
        self.assertIn("price_ranges", sql)
        self.assertIn("custom_strike", sql)
        self.assertIn("mve_selected_legs", sql)
        self.assertEqual(params["ticker"], "T1")

    def test_update_event_metadata(self) -> None:
        cursor = DummyExecCursor()
        conn = DummyExecConn(cursor)
        web_portal._update_event_metadata(conn, "EV1", None)
        self.assertEqual(cursor.calls, [])
        web_portal._update_event_metadata(conn, "EV1", {"a": 1})
        self.assertEqual(len(cursor.calls), 1)
        sql, params = cursor.calls[0]
        self.assertIn("product_metadata", sql)
        self.assertEqual(params["event_ticker"], "EV1")


class DummyHealthCursor:
    def __init__(self, row):
        self.row = row
        self.execute_calls = 0

    def execute(self, *args, **kwargs) -> None:
        self.execute_calls += 1

    def fetchone(self):
        return self.row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyHealthConn:
    def __init__(self, last_tick_row, queue_row):
        self.last_tick_row = last_tick_row
        self.queue_row = queue_row

    def cursor(self, *args, **kwargs):
        if kwargs.get("row_factory") is web_portal.dict_row:
            return DummyHealthCursor(self.queue_row)
        return DummyHealthCursor(self.last_tick_row)


class TestPortalHealth(unittest.TestCase):
    def setUp(self) -> None:
        self._ws_alert = web_portal._WS_LAG_LAST_ALERT
        web_portal._WS_LAG_LAST_ALERT = 0.0

    def tearDown(self) -> None:
        web_portal._WS_LAG_LAST_ALERT = self._ws_alert

    def test_fetch_state_rows(self) -> None:
        rows = [
            {"key": "last_discovery_ts", "value": "2024-01-01T00:00:00Z"},
            {"key": None, "value": "skip"},
        ]
        conn = DummyRowsConn(rows)
        state = web_portal._fetch_state_rows(conn, ["last_discovery_ts"])
        self.assertIn("last_discovery_ts", state)
        self.assertNotIn(None, state)

    def test_fetch_state_rows_empty_keys(self) -> None:
        conn = DummyRowsConn([])
        self.assertEqual(web_portal._fetch_state_rows(conn, []), {})

    def test_load_portal_health(self) -> None:
        now = datetime.now(timezone.utc)
        state_rows = {
            "last_discovery_ts": {"value": (now - timedelta(minutes=15)).isoformat()},
            "last_min_close_ts": {"value": str(int(now.timestamp()) - 900)},
            "last_prediction_ts": {
                "value": None,
                "updated_at": (now - timedelta(minutes=20)).isoformat(),
            },
            "last_tick_ts": {"value": None},
            "last_ws_tick_ts": {"value": (now - timedelta(minutes=10)).isoformat()},
        }
        last_tick_row = (now - timedelta(minutes=5),)
        queue_row = {"pending": 2, "running": 1, "failed": 0, "workers": 1}
        conn = DummyHealthConn(last_tick_row, queue_row)
        with patch.object(web_portal, "_fetch_state_rows", return_value=state_rows):
            with patch.object(web_portal, "_fetch_latest_prediction_ts", return_value=None):
                with patch("src.web_portal.time.monotonic", return_value=100.0):
                    with patch.object(web_portal.logger, "warning") as warn:
                        with patch.dict(
                            os.environ,
                            {
                                "KALSHI_WS_ENABLE": "1",
                                "WEB_PORTAL_WS_STALE_SECONDS": "120",
                                "WEB_PORTAL_WS_LAG_ALERT_SECONDS": "300",
                                "WEB_PORTAL_WS_LAG_ALERT_COOLDOWN_SECONDS": "0",
                                "PREDICTION_ENABLE": "1",
                                "WEB_PORTAL_RAG_STALE_SECONDS": "300",
                            },
                        ):
                            payload = web_portal._load_portal_health(conn)
        self.assertEqual(payload["queue"]["pending"], 2)
        self.assertEqual(payload["ws"]["status"], "stale")
        self.assertTrue(payload["ws"]["alert"])
        self.assertEqual(payload["rag"]["status"], "stale")
        warn.assert_called()

    def test_load_portal_health_ws_disabled(self) -> None:
        conn = DummyHealthConn((None,), {"pending": 0, "running": 0, "failed": 0, "workers": 0})
        with patch.object(web_portal, "_fetch_state_rows", return_value={}):
            with patch.dict(
                os.environ,
                {
                    "KALSHI_WS_ENABLE": "0",
                    "PREDICTION_ENABLE": "0",
                },
                clear=True,
            ):
                payload = web_portal._load_portal_health(conn)
        self.assertEqual(payload["ws"]["status"], "disabled")
        self.assertEqual(payload["rag"]["status"], "disabled")


class TestPortalHealthHelpers(unittest.TestCase):
    def test_status_from_age(self) -> None:
        status, label = web_portal._status_from_age(None, 10)
        self.assertEqual((status, label), ("stale", "Missing"))
        status, label = web_portal._status_from_age(5, 10, ok_label="Good")
        self.assertEqual((status, label), ("ok", "Good"))
        status, label = web_portal._status_from_age(20, 10, stale_label="Old")
        self.assertEqual((status, label), ("stale", "Old"))


class TestPortalHealthCards(unittest.TestCase):
    def test_build_health_cards_missing_settings(self) -> None:
        prior_started = web_portal._SNAPSHOT_THREAD_STARTED
        web_portal._SNAPSHOT_THREAD_STARTED = False
        try:
            with patch.dict(os.environ, {}, clear=True):
                with patch.object(web_portal, "_load_kalshi_client", return_value=(None, "no creds")):
                    with patch.object(web_portal, "_snapshot_poll_enabled", return_value=False):
                        with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                            cards = web_portal._build_health_cards()
        finally:
            web_portal._SNAPSHOT_THREAD_STARTED = prior_started
        titles = {card["title"] for card in cards}
        self.assertIn("Portal Authentication", titles)
        self.assertIn("Database Connection", titles)
        self.assertIn("Kalshi API Client", titles)
        self.assertIn("Snapshot Poller", titles)
        auth_card = next(card for card in cards if card["title"] == "Portal Authentication")
        self.assertEqual(auth_card["level"], "error")
        db_card = next(card for card in cards if card["title"] == "Database Connection")
        self.assertEqual(db_card["label"], "Missing")
        kalshi_card = next(card for card in cards if card["title"] == "Kalshi API Client")
        self.assertEqual(kalshi_card["level"], "warn")
        poll_card = next(card for card in cards if card["title"] == "Snapshot Poller")
        self.assertEqual(poll_card["label"], "Disabled")

    def test_build_health_cards_ready(self) -> None:
        @contextmanager
        def fake_db(*_args, **_kwargs):
            class DummyCursor:
                def execute(self, *_args, **_kwargs):
                    return None

                def fetchone(self):
                    return (1,)

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            class DummyConn:
                def __init__(self):
                    self.info = SimpleNamespace(server_version=150002)

                def cursor(self):
                    return DummyCursor()

            yield DummyConn()

        prior_started = web_portal._SNAPSHOT_THREAD_STARTED
        web_portal._SNAPSHOT_THREAD_STARTED = True
        try:
            with patch.dict(
                os.environ,
                {
                    "WEB_PORTAL_PASSWORD": "pw",
                    "DATABASE_URL": "postgres://example",
                    "WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC": "15",
                    "WEB_PORTAL_SNAPSHOT_POLL_LIMIT": "10",
                },
                clear=True,
            ):
                with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                    with patch.object(web_portal, "_load_kalshi_client", return_value=(object(), None)):
                        with patch.object(web_portal, "_snapshot_poll_enabled", return_value=True):
                            with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                                cards = web_portal._build_health_cards()
        finally:
            web_portal._SNAPSHOT_THREAD_STARTED = prior_started
        db_card = next(card for card in cards if card["title"] == "Database Connection")
        self.assertEqual(db_card["level"], "ok")
        self.assertTrue(any("Server version" in detail for detail in db_card["details"]))
        poll_card = next(card for card in cards if card["title"] == "Snapshot Poller")
        self.assertEqual(poll_card["label"], "Running")


class TestPortalSnapshotPolling(unittest.TestCase):
    class StopLoop(Exception):
        pass

    def setUp(self) -> None:
        self._thread_started = web_portal._SNAPSHOT_THREAD_STARTED
        self._last_attempt_ref = web_portal._SNAPSHOT_LAST_ATTEMPT
        self._last_attempt_data = dict(web_portal._SNAPSHOT_LAST_ATTEMPT)
        web_portal._SNAPSHOT_THREAD_STARTED = False
        web_portal._SNAPSHOT_LAST_ATTEMPT.clear()

    def tearDown(self) -> None:
        web_portal._SNAPSHOT_THREAD_STARTED = self._thread_started
        web_portal._SNAPSHOT_LAST_ATTEMPT = self._last_attempt_ref
        web_portal._SNAPSHOT_LAST_ATTEMPT.clear()
        web_portal._SNAPSHOT_LAST_ATTEMPT.update(self._last_attempt_data)

    def test_snapshot_poll_enabled(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(web_portal._snapshot_poll_enabled())
        with patch.dict(os.environ, {"WEB_PORTAL_SNAPSHOT_POLL_ENABLE": "0"}):
            self.assertFalse(web_portal._snapshot_poll_enabled())
        with patch.dict(os.environ, {"WEB_PORTAL_SNAPSHOT_POLL_ENABLE": "yes"}):
            self.assertTrue(web_portal._snapshot_poll_enabled())

    def test_snapshot_poll_loop_missing_db_url(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                with patch("src.web_portal.time.sleep", side_effect=self.StopLoop):
                    with patch.object(web_portal.logger, "warning") as warn:
                        with self.assertRaises(self.StopLoop):
                            web_portal._snapshot_poll_loop()
        warn.assert_any_call("snapshot poll skipped: DATABASE_URL is not set")

    def test_snapshot_poll_loop_backoff(self) -> None:
        with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=2.5):
            with patch("src.web_portal.time.sleep", side_effect=self.StopLoop):
                with patch.object(web_portal.logger, "warning") as warn:
                    with self.assertRaises(self.StopLoop):
                        web_portal._snapshot_poll_loop()
        warn.assert_any_call("snapshot poll backing off for %.1fs", 2.5)

    def test_snapshot_poll_loop_no_tickers(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        env = {
            "DATABASE_URL": "postgres://example",
            "WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC": "10",
            "WEB_PORTAL_SNAPSHOT_POLL_LIMIT": "1",
            "WEB_PORTAL_SNAPSHOT_MIN_AGE_SEC": "0",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                with patch.object(web_portal, "_load_open_market_tickers", return_value=[]):
                    with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                        with patch("src.web_portal.time.sleep", side_effect=self.StopLoop):
                            with patch.object(web_portal.logger, "info") as log_info:
                                with self.assertRaises(self.StopLoop):
                                    web_portal._snapshot_poll_loop()
        log_info.assert_any_call("snapshot poll: no active tickers to update")

    def test_snapshot_poll_loop_min_attempt_skip(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        web_portal._SNAPSHOT_LAST_ATTEMPT["T1"] = 95.0
        env = {
            "DATABASE_URL": "postgres://example",
            "WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC": "10",
            "WEB_PORTAL_SNAPSHOT_POLL_LIMIT": "1",
            "WEB_PORTAL_SNAPSHOT_MIN_AGE_SEC": "0",
            "WEB_PORTAL_SNAPSHOT_MIN_ATTEMPT_SEC": "10",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                with patch.object(web_portal, "_load_open_market_tickers", return_value=["T1"]):
                    with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                        with patch("src.web_portal.time.monotonic", return_value=100.0):
                            with patch("src.web_portal.time.sleep", side_effect=self.StopLoop):
                                with patch.object(web_portal, "fetch_live_snapshot") as fetch:
                                    with self.assertRaises(self.StopLoop):
                                        web_portal._snapshot_poll_loop()
        fetch.assert_not_called()
        self.assertEqual(web_portal._SNAPSHOT_LAST_ATTEMPT["T1"], 95.0)

    def test_snapshot_poll_loop_updates_last_attempt(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        env = {
            "DATABASE_URL": "postgres://example",
            "WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC": "10",
            "WEB_PORTAL_SNAPSHOT_POLL_LIMIT": "1",
            "WEB_PORTAL_SNAPSHOT_MIN_AGE_SEC": "0",
            "WEB_PORTAL_SNAPSHOT_MIN_ATTEMPT_SEC": "10",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                with patch.object(web_portal, "_load_open_market_tickers", return_value=["T1"]):
                    with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                        with patch("src.web_portal.time.monotonic", return_value=100.0):
                            with patch("src.web_portal.time.sleep", side_effect=self.StopLoop):
                                with patch.object(
                                    web_portal,
                                    "fetch_live_snapshot",
                                    return_value=({"error": "boom"}, None),
                                ):
                                    with self.assertRaises(self.StopLoop):
                                        web_portal._snapshot_poll_loop()
        self.assertEqual(web_portal._SNAPSHOT_LAST_ATTEMPT["T1"], 100.0)

    def test_snapshot_poll_loop_mid_cycle_backoff(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        env = {
            "DATABASE_URL": "postgres://example",
            "WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC": "10",
            "WEB_PORTAL_SNAPSHOT_POLL_LIMIT": "1",
            "WEB_PORTAL_SNAPSHOT_MIN_AGE_SEC": "0",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                with patch.object(web_portal, "_load_open_market_tickers", return_value=["T1"]):
                    with patch.object(
                        web_portal,
                        "_snapshot_backoff_remaining",
                        side_effect=[0, 2.0],
                    ):
                        with patch("src.web_portal.time.sleep", side_effect=self.StopLoop):
                            with patch.object(web_portal.logger, "warning") as warn:
                                with self.assertRaises(self.StopLoop):
                                    web_portal._snapshot_poll_loop()
        warn.assert_any_call("snapshot poll backing off mid-cycle for %.1fs", 2.0)

    def test_snapshot_poll_loop_insert_success(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        env = {
            "DATABASE_URL": "postgres://example",
            "WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC": "10",
            "WEB_PORTAL_SNAPSHOT_POLL_LIMIT": "1",
            "WEB_PORTAL_SNAPSHOT_MIN_AGE_SEC": "0",
            "WEB_PORTAL_SNAPSHOT_POLL_DELAY_MS": "0",
            "WEB_PORTAL_SNAPSHOT_POLL_DELAY_JITTER_MS": "0",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                with patch.object(web_portal, "_load_open_market_tickers", return_value=["T1"]):
                    with patch.object(
                        web_portal,
                        "fetch_live_snapshot",
                        return_value=({"status": "open"}, {"ticker": "T1"}),
                    ):
                        with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                            with patch("src.web_portal.time.sleep", side_effect=self.StopLoop):
                                with patch.object(web_portal, "insert_market_tick") as insert:
                                    with patch.object(web_portal.logger, "info") as log_info:
                                        with self.assertRaises(self.StopLoop):
                                            web_portal._snapshot_poll_loop()
        insert.assert_called_once()
        log_info.assert_any_call("snapshot poll: updated %s/%s (errors=%s)", 1, 1, 0)

    def test_snapshot_poll_loop_exception(self) -> None:
        env = {
            "DATABASE_URL": "postgres://example",
            "WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC": "10",
            "WEB_PORTAL_SNAPSHOT_POLL_LIMIT": "1",
            "WEB_PORTAL_SNAPSHOT_MIN_AGE_SEC": "0",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(web_portal, "_db_connection", side_effect=RuntimeError("boom")):
                with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                    with patch("src.web_portal.time.sleep", side_effect=self.StopLoop):
                        with patch.object(web_portal.logger, "exception") as log_exc:
                            with self.assertRaises(self.StopLoop):
                                web_portal._snapshot_poll_loop()
        log_exc.assert_called_once()

    def test_snapshot_poll_loop_error_response(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        env = {
            "DATABASE_URL": "postgres://example",
            "WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC": "10",
            "WEB_PORTAL_SNAPSHOT_POLL_LIMIT": "1",
            "WEB_PORTAL_SNAPSHOT_MIN_AGE_SEC": "0",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                with patch.object(web_portal, "_load_open_market_tickers", return_value=["T1"]):
                    with patch.object(
                        web_portal,
                        "fetch_live_snapshot",
                        return_value=({"error": "boom"}, None),
                    ):
                        with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                            with patch("src.web_portal.time.sleep", side_effect=self.StopLoop):
                                with patch.object(web_portal.logger, "warning") as warn, \
                                     patch.object(web_portal.logger, "info") as log_info:
                                    with self.assertRaises(self.StopLoop):
                                        web_portal._snapshot_poll_loop()
        warn.assert_any_call("snapshot poll failed for %s: %s", "T1", "boom")
        log_info.assert_any_call("snapshot poll: updated %s/%s (errors=%s)", 0, 1, 1)

    def test_snapshot_poll_loop_insert_exception_and_delay(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        env = {
            "DATABASE_URL": "postgres://example",
            "WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC": "10",
            "WEB_PORTAL_SNAPSHOT_POLL_LIMIT": "1",
            "WEB_PORTAL_SNAPSHOT_MIN_AGE_SEC": "0",
            "WEB_PORTAL_SNAPSHOT_POLL_DELAY_MS": "100",
            "WEB_PORTAL_SNAPSHOT_POLL_DELAY_JITTER_MS": "50",
            "WEB_PORTAL_SNAPSHOT_POLL_JITTER_SEC": "1",
        }
        sleep_calls = []

        def fake_sleep(seconds):
            sleep_calls.append(seconds)
            if len(sleep_calls) > 1:
                raise self.StopLoop()

        with patch.dict(os.environ, env, clear=True):
            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                with patch.object(web_portal, "_load_open_market_tickers", return_value=["T1"]):
                    with patch.object(
                        web_portal,
                        "fetch_live_snapshot",
                        return_value=({"status": "open"}, {"ticker": "T1"}),
                    ):
                        with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                            with patch("src.web_portal.random.uniform", side_effect=[50.0, 0.25]):
                                with patch("src.web_portal.time.sleep", side_effect=fake_sleep):
                                    with patch.object(web_portal, "insert_market_tick", side_effect=RuntimeError("boom")):
                                        with patch.object(web_portal.logger, "exception") as log_exc:
                                            with self.assertRaises(self.StopLoop):
                                                web_portal._snapshot_poll_loop()
        self.assertGreaterEqual(len(sleep_calls), 2)
        self.assertAlmostEqual(sleep_calls[0], 0.15, places=3)
        log_exc.assert_called()

    def test_snapshot_poll_loop_rate_limited(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        env = {
            "DATABASE_URL": "postgres://example",
            "WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC": "10",
            "WEB_PORTAL_SNAPSHOT_POLL_LIMIT": "1",
            "WEB_PORTAL_SNAPSHOT_MIN_AGE_SEC": "0",
            "WEB_PORTAL_SNAPSHOT_POLL_DELAY_MS": "0",
            "WEB_PORTAL_SNAPSHOT_POLL_DELAY_JITTER_MS": "0",
            "WEB_PORTAL_SNAPSHOT_POLL_COOLDOWN_SEC": "30",
            "WEB_PORTAL_SNAPSHOT_POLL_JITTER_SEC": "0",
            "WEB_PORTAL_SNAPSHOT_MIN_ATTEMPT_SEC": "0",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                with patch.object(web_portal, "_load_open_market_tickers", return_value=["T1"]):
                    with patch.object(
                        web_portal,
                        "fetch_live_snapshot",
                        return_value=({"rate_limited": True, "error": "nope"}, None),
                    ):
                        with patch.object(web_portal, "_set_snapshot_backoff") as cooldown:
                            with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                                with patch("src.web_portal.time.sleep", side_effect=self.StopLoop):
                                    with self.assertRaises(self.StopLoop):
                                        web_portal._snapshot_poll_loop()
        cooldown.assert_called_once_with(30)

    def test_snapshot_poll_loop_missing_tick(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        env = {
            "DATABASE_URL": "postgres://example",
            "WEB_PORTAL_SNAPSHOT_POLL_INTERVAL_SEC": "10",
            "WEB_PORTAL_SNAPSHOT_POLL_LIMIT": "1",
            "WEB_PORTAL_SNAPSHOT_MIN_AGE_SEC": "0",
            "WEB_PORTAL_SNAPSHOT_POLL_DELAY_MS": "0",
            "WEB_PORTAL_SNAPSHOT_POLL_DELAY_JITTER_MS": "0",
            "WEB_PORTAL_SNAPSHOT_POLL_JITTER_SEC": "0",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                with patch.object(web_portal, "_load_open_market_tickers", return_value=["T1"]):
                    with patch.object(web_portal, "fetch_live_snapshot", return_value=({}, None)):
                        with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=0):
                            with patch("src.web_portal.time.sleep", side_effect=self.StopLoop):
                                with patch.object(web_portal.logger, "warning") as warn:
                                    with self.assertRaises(self.StopLoop):
                                        web_portal._snapshot_poll_loop()
        warn.assert_any_call("snapshot poll returned no data for %s", "T1")

    def test_start_snapshot_polling(self) -> None:
        threads = []

        class FakeThread:
            def __init__(self, target, name, daemon):
                self.target = target
                self.name = name
                self.daemon = daemon
                self.started = False

            def start(self):
                self.started = True

        def make_thread(*args, **kwargs):
            thread = FakeThread(*args, **kwargs)
            threads.append(thread)
            return thread

        with patch.object(web_portal, "_snapshot_poll_enabled", return_value=True):
            with patch("src.web_portal.threading.Thread", side_effect=make_thread):
                web_portal._start_snapshot_polling()
                web_portal._start_snapshot_polling()
        self.assertEqual(len(threads), 1)
        self.assertTrue(threads[0].started)

    def test_start_snapshot_polling_disabled(self) -> None:
        with patch.object(web_portal, "_snapshot_poll_enabled", return_value=False):
            with patch("src.web_portal.threading.Thread") as thread:
                web_portal._start_snapshot_polling()
        thread.assert_not_called()

    def test_start_snapshot_polling_already_started(self) -> None:
        web_portal._SNAPSHOT_THREAD_STARTED = True
        with patch.object(web_portal, "_snapshot_poll_enabled", return_value=True):
            with patch("src.web_portal.threading.Thread") as thread:
                web_portal._start_snapshot_polling()
        thread.assert_not_called()


class TestPortalRouteHandlers(unittest.TestCase):
    def test_enforce_login_blocks_api(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=False):
                client = web_portal.app.test_client()
                resp = client.get("/market/M1/snapshot")
        self.assertEqual(resp.status_code, 401)

    def test_enforce_login_redirects(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=False):
                client = web_portal.app.test_client()
                resp = client.get("/")
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login", resp.headers.get("Location"))

    def test_index_missing_settings(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with web_portal.app.test_request_context("/"):
                with patch.object(
                    web_portal,
                    "render_template",
                    side_effect=lambda *args, **kwargs: kwargs,
                ):
                    payload = web_portal.index()
        self.assertEqual(payload["error"], "WEB_PORTAL_PASSWORD is not set.")

    def test_index_success(self) -> None:
        filters = web_portal.PortalFilters(
            search=None,
            categories=("Sports",),
            strike_period=None,
            close_window=None,
            close_window_hours=None,
            status=None,
            sort=None,
            order=None,
        )

        @contextmanager
        def fake_db():
            yield object()

        env = {
            "WEB_PORTAL_PASSWORD": "pw",
            "DATABASE_URL": "postgres://example",
        }
        with patch.dict(os.environ, env, clear=True):
            with web_portal.app.test_request_context("/?category=Sports"):
                with patch.object(web_portal, "_parse_portal_filters", return_value=filters):
                    with patch.object(web_portal, "describe_event_scope", return_value="scope"):
                        with patch.object(web_portal, "is_authenticated", return_value=True):
                            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                                with patch.object(
                                    web_portal,
                                    "fetch_active_events",
                                    return_value=[{"event_ticker": "EV1"}],
                                ):
                                    with patch.object(
                                        web_portal,
                                        "fetch_scheduled_events",
                                        return_value=[],
                                    ):
                                        with patch.object(
                                            web_portal,
                                            "fetch_closed_events",
                                            return_value=[],
                                        ):
                                            with patch.object(
                                                web_portal,
                                                "fetch_counts",
                                                return_value=(1, 0, 0),
                                            ):
                                                with patch.object(
                                                    web_portal,
                                                    "fetch_strike_periods",
                                                    return_value=["hour"],
                                                ):
                                                    with patch.object(
                                                        web_portal,
                                                        "fetch_active_event_categories",
                                                        return_value=["Sports"],
                                                    ):
                                                        with patch.object(
                                                            web_portal,
                                                            "_load_portal_health",
                                                            return_value={"ok": True},
                                                        ):
                                                            with patch.object(
                                                                web_portal,
                                                                "render_template",
                                                                side_effect=lambda *args, **kwargs: kwargs,
                                                            ):
                                                                payload = web_portal.index()
        self.assertIsNone(payload["error"])
        self.assertEqual(payload["active_total"], 1)
        self.assertEqual(payload["category_filters"][0]["label"], "Sports")
        self.assertTrue(payload["health"]["ok"])

    def test_health_route(self) -> None:
        with web_portal.app.test_request_context("/health"):
            with patch.object(web_portal, "_build_health_cards", return_value=[{"title": "OK"}]):
                with patch.object(web_portal, "is_authenticated", return_value=False):
                    with patch.object(
                        web_portal,
                        "render_template",
                        side_effect=lambda *args, **kwargs: kwargs,
                    ):
                        payload = web_portal.health()
        self.assertEqual(payload["status_cards"][0]["title"], "OK")

    def test_queue_stream_disabled(self) -> None:
        with patch.dict(os.environ, {"WEB_PORTAL_QUEUE_STREAM_ENABLE": "0"}, clear=True):
            with web_portal.app.test_request_context("/stream/queue"):
                resp, status = web_portal.queue_stream()
        self.assertEqual(status, 404)
        self.assertEqual(resp.get_json()["error"], "Queue stream disabled.")

    def test_queue_stream_missing_db(self) -> None:
        with patch.dict(os.environ, {"WEB_PORTAL_QUEUE_STREAM_ENABLE": "1"}, clear=True):
            with web_portal.app.test_request_context("/stream/queue"):
                resp, status = web_portal.queue_stream()
        self.assertEqual(status, 503)
        self.assertEqual(resp.get_json()["error"], "DATABASE_URL is not set.")

    def test_event_snapshot_missing_db(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with web_portal.app.test_request_context("/event/EV1/snapshot"):
                resp, status = web_portal.event_snapshot("EV1")
        self.assertEqual(status, 503)
        self.assertEqual(resp.get_json()["error"], "DATABASE_URL is not set.")

    def test_event_snapshot_not_found(self) -> None:
        @contextmanager
        def fake_db():
            yield SequenceConn(
                [
                    SequenceCursor(fetchall_queue=[[]]),
                    SequenceCursor(fetchone_queue=[None]),
                ]
            )

        with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
            with web_portal.app.test_request_context("/event/EV1/snapshot"):
                with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                    with patch.object(web_portal, "_snapshot_allows_closed", return_value=True):
                        resp, status = web_portal.event_snapshot("EV1")
        self.assertEqual(status, 404)
        self.assertEqual(resp.get_json()["event_ticker"], "EV1")

    def test_market_snapshot_cached(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
            with web_portal.app.test_request_context("/market/M1/snapshot"):
                with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                    with patch.object(web_portal, "_snapshot_allows_closed", return_value=True):
                        with patch.object(web_portal, "_market_is_closed", return_value=False):
                            with patch.object(
                                web_portal,
                                "_prefer_tick_snapshot",
                                return_value={"status": "open"},
                            ):
                                resp = web_portal.market_snapshot("M1")
        payload = resp.get_json()
        self.assertFalse(payload["db_saved"])

    def test_market_snapshot_rate_limited(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
            with web_portal.app.test_request_context("/market/M1/snapshot"):
                with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                    with patch.object(web_portal, "_snapshot_allows_closed", return_value=True):
                        with patch.object(web_portal, "_market_is_closed", return_value=False):
                            with patch.object(web_portal, "_prefer_tick_snapshot", return_value=None):
                                with patch.object(
                                    web_portal,
                                    "fetch_live_snapshot",
                                    return_value=({"error": "nope", "rate_limited": True}, None),
                                ):
                                    with patch.object(web_portal, "_set_snapshot_backoff") as backoff:
                                        resp, status = web_portal.market_snapshot("M1")
        self.assertEqual(status, 429)
        self.assertEqual(resp.get_json()["error"], "nope")
        backoff.assert_called_once()

    def test_market_backfill_disabled(self) -> None:
        with patch.dict(os.environ, {"WEB_PORTAL_BACKFILL_MODE": "disabled"}, clear=True):
            with web_portal.app.test_request_context(
                "/market/M1/backfill", method="POST", json={}
            ):
                resp, status = web_portal.market_backfill("M1")
        self.assertEqual(status, 403)
        self.assertIn("Backfill endpoint is disabled", resp.get_json()["error"])

    def test_market_backfill_unsupported(self) -> None:
        with patch.dict(os.environ, {"WEB_PORTAL_BACKFILL_MODE": "manual"}, clear=True):
            with web_portal.app.test_request_context(
                "/market/M1/backfill", method="POST", json={}
            ):
                resp, status = web_portal.market_backfill("M1")
        self.assertEqual(status, 400)
        self.assertIn("Unsupported backfill mode", resp.get_json()["error"])

    def test_market_backfill_success(self) -> None:
        row = {
            "ticker": "M1",
            "open_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "close_time": datetime(2024, 1, 2, tzinfo=timezone.utc),
            "series_ticker": "SER",
            "strike_period": "hour",
        }

        @contextmanager
        def fake_db():
            yield SequenceConn([SequenceCursor(fetchone_queue=[row])])

        cfg = web_portal.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=(),
            minutes_hour=5,
            minutes_day=60,
            lookback_hours=1,
        )
        env = {
            "WEB_PORTAL_BACKFILL_MODE": "queue",
            "WORK_QUEUE_ENABLE": "1",
            "DATABASE_URL": "postgres://example",
        }
        with patch.dict(os.environ, env, clear=True):
            with web_portal.app.test_request_context(
                "/market/M1/backfill", method="POST", json={"force_full": True}
            ):
                with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                    with patch.object(web_portal, "_load_backfill_config", return_value=cfg):
                        with patch.object(web_portal, "enqueue_job", return_value="job-1"):
                            resp = web_portal.market_backfill("M1")
        payload = resp.get_json()
        self.assertTrue(payload["queued"])
        self.assertEqual(payload["job_id"], "job-1")


class TestPortalLoginFlow(unittest.TestCase):
    def test_login_and_logout(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            client = web_portal.app.test_client()
            resp = client.get("/login")
            self.assertEqual(resp.status_code, 200)
            with patch.dict(os.environ, {"WEB_PORTAL_PASSWORD": "pw"}):
                resp = client.post("/login?next=/market/T1", data={"password": "pw"})
                self.assertEqual(resp.status_code, 302)
                self.assertIn("/market/T1", resp.headers.get("Location"))
                with client.session_transaction() as sess:
                    self.assertTrue(sess.get("web_portal_authed"))
                with client.session_transaction() as sess:
                    sess["web_portal_authed"] = True
                resp = client.get("/logout")
                self.assertEqual(resp.status_code, 302)
                self.assertIn("/login", resp.headers.get("Location"))


class TestPortalRoutes(unittest.TestCase):
    def test_enforce_login_requires_auth(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            client = web_portal.app.test_client()
            resp = client.get("/market/T1/snapshot")
        self.assertEqual(resp.status_code, 401)

    def test_index_missing_password_or_db(self) -> None:
        with web_portal.app.test_request_context("/"):
            with patch.dict(os.environ, {}, clear=True):
                with patch("src.web_portal.render_template", side_effect=lambda *args, **kwargs: kwargs):
                    payload = web_portal.index()
        self.assertIn("WEB_PORTAL_PASSWORD", payload["error"])

        with web_portal.app.test_request_context("/"):
            with patch.dict(os.environ, {"WEB_PORTAL_PASSWORD": "pw"}, clear=True):
                with patch("src.web_portal.render_template", side_effect=lambda *args, **kwargs: kwargs):
                    payload = web_portal.index()
        self.assertEqual(payload["error"], "DATABASE_URL is not set.")

    def test_index_success(self) -> None:
        with web_portal.app.test_request_context("/?category=Sports&limit=10"):
            with patch.dict(
                os.environ,
                {"WEB_PORTAL_PASSWORD": "pw", "DATABASE_URL": "db"},
                clear=True,
            ):
                with patch.object(web_portal, "_db_connection") as db_conn, \
                     patch.object(web_portal, "fetch_active_events", return_value=[{"event_ticker": "EV1"}]), \
                     patch.object(web_portal, "fetch_scheduled_events", return_value=[]), \
                     patch.object(web_portal, "fetch_closed_events", return_value=[]), \
                     patch.object(web_portal, "fetch_counts", return_value=(1, 0, 0)), \
                     patch.object(web_portal, "fetch_strike_periods", return_value=["hour"]), \
                     patch.object(web_portal, "fetch_active_event_categories", return_value=["Sports"]), \
                     patch.object(web_portal, "_load_portal_health", return_value={"ok": True}), \
                     patch("src.web_portal.render_template", side_effect=lambda *args, **kwargs: kwargs):
                    @contextmanager
                    def fake_db():
                        yield object()

                    db_conn.side_effect = fake_db
                    payload = web_portal.index()
        self.assertIsNone(payload["error"])
        self.assertEqual(payload["active_total"], 1)
        self.assertEqual(payload["strike_periods"], ["hour"])
        self.assertTrue(payload["category_filters"])

    def test_health_route(self) -> None:
        with web_portal.app.test_request_context("/health"):
            with patch.object(web_portal, "_build_health_cards", return_value=[{"title": "ok"}]):
                with patch("src.web_portal.render_template", side_effect=lambda *args, **kwargs: kwargs):
                    payload = web_portal.health()
        self.assertEqual(payload["status_cards"], [{"title": "ok"}])

    def test_queue_stream_disabled_and_missing_db(self) -> None:
        with web_portal.app.test_request_context("/stream/queue"):
            with patch.object(web_portal, "_queue_stream_enabled", return_value=False):
                resp, status = web_portal.queue_stream()
        self.assertEqual(status, 404)
        self.assertIn("Queue stream disabled", resp.json["error"])

        with web_portal.app.test_request_context("/stream/queue"):
            with patch.object(web_portal, "_queue_stream_enabled", return_value=True):
                with patch.dict(os.environ, {}, clear=True):
                    resp, status = web_portal.queue_stream()
        self.assertEqual(status, 503)
        self.assertIn("DATABASE_URL", resp.json["error"])

    def test_event_and_market_detail_missing_db(self) -> None:
        with web_portal.app.test_request_context("/event/EV1"):
            with patch.dict(os.environ, {}, clear=True):
                with patch("src.web_portal.render_template", side_effect=lambda *args, **kwargs: kwargs):
                    payload = web_portal.event_detail("EV1")
        self.assertEqual(payload["error"], "DATABASE_URL is not set.")

        with web_portal.app.test_request_context("/market/M1"):
            with patch.dict(os.environ, {}, clear=True):
                with patch("src.web_portal.render_template", side_effect=lambda *args, **kwargs: kwargs):
                    payload = web_portal.market_detail("M1")
        self.assertEqual(payload["error"], "DATABASE_URL is not set.")

    def test_event_snapshot_no_db(self) -> None:
        with web_portal.app.test_request_context("/event/EV1/snapshot"):
            with patch.dict(os.environ, {}, clear=True):
                resp, status = web_portal.event_snapshot("EV1")
        self.assertEqual(status, 503)

    def test_event_snapshot_no_markets(self) -> None:
        class SnapshotCursor:
            def __init__(self, rows=None, exists=True):
                self.rows = rows or []
                self.exists = exists

            def execute(self, *args, **kwargs):
                return None

            def fetchall(self):
                return self.rows

            def fetchone(self):
                return (1,) if self.exists else None

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class SnapshotConn:
            def __init__(self, rows=None, exists=True):
                self.rows = rows or []
                self.exists = exists
                self.calls = 0

            def cursor(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    return SnapshotCursor(rows=self.rows, exists=self.exists)
                return SnapshotCursor(rows=[], exists=self.exists)

        @contextmanager
        def fake_db():
            yield SnapshotConn(rows=[], exists=True)

        with web_portal.app.test_request_context("/event/EV1/snapshot"):
            with patch.dict(os.environ, {"DATABASE_URL": "db"}, clear=True):
                with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                    with patch.object(web_portal, "_snapshot_allows_closed", return_value=False):
                        resp, status = web_portal.event_snapshot("EV1")
        self.assertEqual(status, 409)
        self.assertIn("Event is closed", resp.json["error"])

        with web_portal.app.test_request_context("/event/EV1/snapshot"):
            with patch.dict(os.environ, {"DATABASE_URL": "db"}, clear=True):
                with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                    with patch.object(web_portal, "_snapshot_allows_closed", return_value=True):
                        resp, status = web_portal.event_snapshot("EV1")
        self.assertEqual(status, 409)
        self.assertIn("No markets found", resp.json["error"])

    def test_market_snapshot_no_db(self) -> None:
        with web_portal.app.test_request_context("/market/M1/snapshot"):
            with patch.dict(os.environ, {}, clear=True):
                resp, status = web_portal.market_snapshot("M1")
        self.assertEqual(status, 503)


class TestPortalHealthCards(unittest.TestCase):
    def test_build_health_cards_missing_db(self) -> None:
        with patch.dict(os.environ, {"WEB_PORTAL_PASSWORD": "pw"}, clear=True):
            with patch.object(web_portal, "_load_kalshi_client", return_value=(None, "err")):
                cards = web_portal._build_health_cards()
        db_card = next(card for card in cards if card["title"] == "Database Connection")
        self.assertEqual(db_card["label"], "Missing")

    def test_build_health_cards_db_error(self) -> None:
        @contextmanager
        def fake_db():
            raise RuntimeError("db down")
            yield None

        with patch.dict(
            os.environ,
            {
                "WEB_PORTAL_PASSWORD": "pw",
                "DATABASE_URL": "db",
            },
            clear=True,
        ):
            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                with patch.object(web_portal, "_load_kalshi_client", return_value=(None, "no creds")):
                    cards = web_portal._build_health_cards()
        db_card = next(card for card in cards if card["title"] == "Database Connection")
        self.assertEqual(db_card["label"], "Down")


class TestPortalHttpStatusExtra(unittest.TestCase):
    def test_extract_http_status_missing(self) -> None:
        class StatusError(Exception):
            pass

        exc = StatusError("missing")
        self.assertIsNone(web_portal._extract_http_status(exc))


class TestPortalFormattingExtra(unittest.TestCase):
    def test_to_cents_decimal(self) -> None:
        self.assertEqual(web_portal._to_cents(Decimal("0.5")), 50)


class TestPortalOutcomeLabelExtra(unittest.TestCase):
    def test_event_outcome_label_multiple_and_pending(self) -> None:
        label = web_portal._format_event_outcome_label(["A", "B"], None, False)
        self.assertEqual(label, "Multiple outcomes settled")
        label = web_portal._format_event_outcome_label([], None, None)
        self.assertEqual(label, "Pending")


class TestPortalTimeExtras(unittest.TestCase):
    def test_market_url_event_ticker_fallback(self) -> None:
        url = web_portal.get_market_url("M1", event_ticker="-EV1", event_title=None)
        self.assertEqual(url, "https://kalshi.com/markets/-ev1/-ev1/m1")

    def test_fmt_ts_passthrough(self) -> None:
        self.assertEqual(web_portal.fmt_ts("2024-01-01"), "2024-01-01")

    def test_infer_strike_period_branches(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.assertIsNone(web_portal._infer_strike_period_from_times(None, now, 2.0, 36.0))
        self.assertIsNone(
            web_portal._infer_strike_period_from_times(now, now - timedelta(hours=1), 2.0, 36.0)
        )
        self.assertEqual(
            web_portal._infer_strike_period_from_times(now, now + timedelta(hours=12), 2.0, 36.0),
            "day",
        )
        self.assertIsNone(
            web_portal._infer_strike_period_from_times(now, now + timedelta(hours=40), 2.0, 36.0)
        )

    def test_fmt_time_remaining_branches(self) -> None:
        now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        with patch("src.web_portal._now_utc", return_value=now):
            self.assertEqual(web_portal.fmt_time_remaining(None), "N/A")
            self.assertEqual(web_portal.fmt_time_remaining("bad"), "N/A")
            self.assertEqual(web_portal.fmt_time_remaining(now - timedelta(minutes=1)), "Closed")
            self.assertEqual(
                web_portal.fmt_time_remaining(now + timedelta(days=2, hours=3)), "2d 3h"
            )
            self.assertEqual(
                web_portal.fmt_time_remaining(now + timedelta(hours=5, minutes=30)), "5h 30m"
            )
            self.assertEqual(web_portal.fmt_time_remaining(now + timedelta(minutes=10)), "10m")
            self.assertEqual(web_portal.fmt_time_remaining(now + timedelta(seconds=30)), "<1m")
            self.assertEqual(web_portal.fmt_time_remaining("2024-01-01T01:00:00"), "1h")

    def test_normalize_status_closed_inactive(self) -> None:
        now = datetime.now(timezone.utc)
        label, status = web_portal.normalize_status(None, None, now - timedelta(hours=1))
        self.assertEqual((label, status), ("Closed", "closed"))
        label, status = web_portal.normalize_status(None, None, None)
        self.assertEqual((label, status), ("Inactive", "inactive"))


class TestPortalSnapshotPollingExtra(unittest.TestCase):
    def test_start_snapshot_polling_lock_short_circuit(self) -> None:
        prior_started = web_portal._SNAPSHOT_THREAD_STARTED

        class FakeLock:
            def __enter__(self):
                web_portal._SNAPSHOT_THREAD_STARTED = True
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        try:
            web_portal._SNAPSHOT_THREAD_STARTED = False
            with patch.object(web_portal, "_snapshot_poll_enabled", return_value=True):
                with patch.object(web_portal, "_SNAPSHOT_THREAD_LOCK", FakeLock()):
                    with patch("src.web_portal.threading.Thread") as thread:
                        web_portal._start_snapshot_polling()
            thread.assert_not_called()
        finally:
            web_portal._SNAPSHOT_THREAD_STARTED = prior_started


class TestPortalHealthCardsExtra(unittest.TestCase):
    def test_build_health_cards_starting_with_backoff(self) -> None:
        @contextmanager
        def fake_db():
            class DummyCursor:
                def execute(self, *_args, **_kwargs):
                    return None

                def fetchone(self):
                    return (1,)

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            class DummyConn:
                def __init__(self):
                    self.info = SimpleNamespace(server_version=150002)

                def cursor(self):
                    return DummyCursor()

            yield DummyConn()

        prior_started = web_portal._SNAPSHOT_THREAD_STARTED
        web_portal._SNAPSHOT_THREAD_STARTED = False
        try:
            with patch.dict(
                os.environ,
                {
                    "WEB_PORTAL_PASSWORD": "pw",
                    "DATABASE_URL": "postgres://example",
                },
                clear=True,
            ):
                with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                    with patch.object(web_portal, "_load_kalshi_client", return_value=(object(), None)):
                        with patch.object(web_portal, "_snapshot_poll_enabled", return_value=True):
                            with patch.object(web_portal, "_snapshot_backoff_remaining", return_value=12.5):
                                cards = web_portal._build_health_cards()
        finally:
            web_portal._SNAPSHOT_THREAD_STARTED = prior_started
        db_card = next(card for card in cards if card["title"] == "Database Connection")
        self.assertTrue(any("Server version" in detail for detail in db_card["details"]))
        poll_card = next(card for card in cards if card["title"] == "Snapshot Poller")
        self.assertEqual(poll_card["label"], "Starting")
        self.assertTrue(any("Backoff remaining" in detail for detail in poll_card["details"]))


class TestPortalHealthExtra(unittest.TestCase):
    def test_load_portal_health_updated_at_ok(self) -> None:
        now = datetime.now(timezone.utc)
        state_rows = {
            "last_discovery_ts": {
                "value": None,
                "updated_at": (now - timedelta(minutes=5)).isoformat(),
            },
            "last_min_close_ts": {
                "value": None,
                "updated_at": (now - timedelta(minutes=10)).isoformat(),
            },
            "last_prediction_ts": {"value": (now - timedelta(minutes=1)).isoformat()},
            "last_tick_ts": {"value": None},
            "last_ws_tick_ts": {"value": (now - timedelta(minutes=1)).isoformat()},
        }

        class ErrorQueueConn:
            def cursor(self, *args, **kwargs):
                if kwargs.get("row_factory") is web_portal.dict_row:
                    raise RuntimeError("boom")
                return DummyCursor((now - timedelta(minutes=1),))

        with patch.object(web_portal, "_fetch_state_rows", return_value=state_rows):
            with patch.object(web_portal.logger, "exception"):
                with patch.dict(
                    os.environ,
                    {
                        "KALSHI_WS_ENABLE": "1",
                        "WEB_PORTAL_WS_STALE_SECONDS": "120",
                        "PREDICTION_ENABLE": "1",
                        "WEB_PORTAL_RAG_STALE_SECONDS": "300",
                    },
                ):
                    payload = web_portal._load_portal_health(ErrorQueueConn())
        self.assertEqual(payload["ws"]["status"], "ok")
        self.assertEqual(payload["rag"]["status"], "ok")
        self.assertIsNotNone(payload["discovery"]["ts"])
        self.assertIsNotNone(payload["backfill"]["ts"])

    def test_load_portal_health_prediction_fallback(self) -> None:
        now = datetime.now(timezone.utc)
        state_rows = {
            "last_discovery_ts": {"value": (now - timedelta(minutes=5)).isoformat()},
            "last_min_close_ts": {"value": str(int(now.timestamp()) - 900)},
            "last_prediction_ts": {"value": None},
            "last_tick_ts": {"value": None},
            "last_ws_tick_ts": {"value": (now - timedelta(minutes=2)).isoformat()},
        }
        conn = DummyHealthConn((None,), {"pending": 0, "running": 0, "failed": 0, "workers": 0})
        fallback_ts = now - timedelta(minutes=2)
        with patch.object(web_portal, "_fetch_state_rows", return_value=state_rows):
            with patch.object(
                web_portal, "_fetch_latest_prediction_ts", return_value=fallback_ts
            ):
                with patch.dict(
                    os.environ,
                    {
                        "KALSHI_WS_ENABLE": "0",
                        "PREDICTION_ENABLE": "0",
                        "WEB_PORTAL_RAG_STALE_SECONDS": "300",
                    },
                    clear=True,
                ):
                    payload = web_portal._load_portal_health(conn)
        self.assertTrue(payload["rag"]["enabled"])
        self.assertEqual(payload["rag"]["status"], "ok")
        self.assertEqual(payload["rag"]["label"], "Running")
        self.assertNotEqual(payload["rag"]["age_text"], "N/A")


class TestPortalAuthEnforcement(unittest.TestCase):
    def test_snapshot_endpoint_requires_auth(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=False):
                client = web_portal.app.test_client()
                resp = client.get("/market/T1/snapshot")
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(resp.json["error"], "Authentication required.")

    def test_non_snapshot_redirects(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=False):
                client = web_portal.app.test_client()
                resp = client.get("/")
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login", resp.headers.get("Location"))


class TestPortalEventQueries(unittest.TestCase):
    def test_fetch_event_count_buckets(self) -> None:
        filters = web_portal.PortalFilters(
            search=None,
            categories=(),
            strike_period=None,
            close_window=None,
            close_window_hours=2.0,
            status="open",
            sort=None,
            order=None,
        )
        for bucket in ("active", "scheduled", "closed"):
            cursor = RecordingCursor(row=(5,))
            conn = RecordingConn(cursor)
            self.assertEqual(web_portal._fetch_event_count(conn, bucket, filters), 5)
            self.assertTrue(cursor.execute_calls)

    def test_fetch_counts(self) -> None:
        filters = web_portal.PortalFilters(
            search=None,
            categories=(),
            strike_period=None,
            close_window=None,
            close_window_hours=None,
            status=None,
            sort=None,
            order=None,
        )
        conn = SequencedConn(
            [
                RecordingCursor(row=(1,)),
                RecordingCursor(row=(2,)),
                RecordingCursor(row=(3,)),
            ]
        )
        self.assertEqual(web_portal.fetch_counts(conn, filters), (1, 2, 3))

    def test_build_event_snapshot_defaults(self) -> None:
        snapshot = web_portal.build_event_snapshot(
            {
                "event_title": None,
                "event_ticker": None,
                "open_time": None,
                "close_time": None,
                "market_count": None,
            }
        )
        self.assertEqual(snapshot["event_title"], "Unknown event")
        self.assertEqual(snapshot["event_ticker"], "N/A")

    def test_fetch_event_lists(self) -> None:
        filters = web_portal.PortalFilters(
            search=None,
            categories=(),
            strike_period=None,
            close_window=None,
            close_window_hours=1.0,
            status="open",
            sort=None,
            order=None,
        )
        rows = [
            {
                "event_title": None,
                "event_ticker": None,
                "open_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "close_time": datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc),
                "market_count": 3,
            }
        ]
        with patch.object(web_portal, "fmt_time_remaining", return_value="1h"):
            active = web_portal.fetch_active_events(DummyRowsConn(rows), 10, filters)
        self.assertEqual(active[0]["event_title"], "Unknown event")
        self.assertEqual(active[0]["time_remaining"], "1h")
        scheduled = web_portal.fetch_scheduled_events(DummyRowsConn(rows), 10, filters)
        self.assertEqual(scheduled[0]["event_ticker"], "N/A")
        closed = web_portal.fetch_closed_events(DummyRowsConn(rows), 10, filters)
        self.assertEqual(closed[0]["event_ticker"], "N/A")

    def test_fetch_categories_and_periods(self) -> None:
        conn = DummyRowsConn([("Sports",), (None,), ("",), ("News",)])
        self.assertEqual(web_portal.fetch_event_categories(conn), ["Sports", "News"])
        conn = DummyRowsConn([("hour",), ("",), (None,), ("day",)])
        self.assertEqual(web_portal.fetch_strike_periods(conn), ["hour", "day"])

    def test_fetch_active_event_categories(self) -> None:
        filters = web_portal.PortalFilters(
            search=None,
            categories=(),
            strike_period=None,
            close_window=None,
            close_window_hours=1.0,
            status="open",
            sort=None,
            order=None,
        )
        conn = DummyRowsConn([("Sports",), (None,), ("",), ("News",)])
        self.assertEqual(web_portal.fetch_active_event_categories(conn, filters), ["Sports", "News"])


class TestPortalMarketRowsAndSparklines(unittest.TestCase):
    def test_fetch_event_market_rows(self) -> None:
        rows = [{"ticker": "T1"}]
        conn = DummyRowsConn(rows)
        self.assertEqual(web_portal._fetch_event_market_rows(conn, "EV1"), rows)
        self.assertTrue(conn.cursor_obj.execute_calls)

    def test_sparkline_value_branches(self) -> None:
        self.assertEqual(web_portal._sparkline_value({"implied_yes_mid": Decimal("0.2")}), 0.2)
        self.assertEqual(web_portal._sparkline_value({"price_dollars": Decimal("0.4")}), 0.4)
        self.assertEqual(
            web_portal._sparkline_value(
                {"yes_bid_dollars": Decimal("0.3"), "yes_ask_dollars": Decimal("0.7")}
            ),
            0.5,
        )
        self.assertEqual(web_portal._sparkline_value({"yes_bid_dollars": Decimal("0.1")}), 0.1)
        self.assertEqual(web_portal._sparkline_value({"yes_ask_dollars": Decimal("0.9")}), 0.9)
        self.assertIsNone(web_portal._sparkline_value({}))

    def test_build_event_sparklines(self) -> None:
        rows = [
            {"ticker": "A", "implied_yes_mid": Decimal("0.2")},
            {"ticker": "A", "price_dollars": Decimal("1.2")},
            {"ticker": "B", "yes_bid_dollars": Decimal("0.3"), "yes_ask_dollars": Decimal("0.7")},
        ]
        conn = DummyRowsConn(rows)
        with patch.dict(os.environ, {"WEB_PORTAL_EVENT_SPARKLINE_POINTS": "10"}):
            points = web_portal._build_event_sparklines(conn, ["A", "B", "C"])
        self.assertEqual(points["A"], [0.2, 1.0])
        self.assertEqual(points["B"], [0.5])
        self.assertEqual(points["C"], [])
        self.assertEqual(web_portal._build_event_sparklines(conn, []), {})


class TestPortalForecastSeries(unittest.TestCase):
    def test_build_event_forecast_series_candles(self) -> None:
        now = datetime.now(timezone.utc)
        market_rows = [
            {
                "ticker": "T1",
                "market_title": "Yes",
                "market_open_time": (now - timedelta(hours=2)).isoformat(),
                "market_close_time": (now + timedelta(hours=2)).isoformat(),
                "yes_bid_dollars": Decimal("0.6"),
                "yes_ask_dollars": Decimal("0.7"),
            },
            {
                "ticker": "T2",
                "market_title": "No",
                "market_open_time": (now - timedelta(hours=2)).isoformat(),
                "market_close_time": (now + timedelta(hours=2)).isoformat(),
                "predicted_yes_prob": Decimal("0.1"),
            },
        ]
        candle_rows = [
            {
                "market_ticker": "T1",
                "end_period_ts": now - timedelta(hours=1),
                "close": Decimal("0.4"),
            },
            {
                "market_ticker": "T1",
                "end_period_ts": now,
                "close": Decimal("0.6"),
            },
        ]
        conn = SequencedConn([RecordingCursor(rows=candle_rows)])
        with patch.dict(
            os.environ,
            {
                "WEB_PORTAL_EVENT_FORECAST_SERIES_LIMIT": "1",
                "WEB_PORTAL_EVENT_FORECAST_POINTS": "10",
            },
        ):
            series, note = web_portal._build_event_forecast_series(conn, market_rows)
        self.assertEqual(len(series), 1)
        self.assertIn("Showing top 1 of 2 outcomes", note)

    def test_build_event_forecast_series_tick_fallback(self) -> None:
        now = datetime.now(timezone.utc)
        market_rows = [
            {
                "ticker": "T1",
                "market_title": "Yes",
                "market_open_time": (now - timedelta(hours=2)).isoformat(),
                "market_close_time": (now + timedelta(hours=2)).isoformat(),
                "yes_bid_dollars": Decimal("0.4"),
                "yes_ask_dollars": Decimal("0.6"),
            }
        ]
        tick_rows = [
            {
                "ticker": "T1",
                "ts": now - timedelta(minutes=10),
                "implied_yes_mid": Decimal("0.5"),
                "yes_bid_dollars": Decimal("0.4"),
                "yes_ask_dollars": Decimal("0.6"),
            }
        ]
        conn = SequencedConn(
            [
                RecordingCursor(rows=[]),
                RecordingCursor(rows=tick_rows),
            ]
        )
        with patch.dict(
            os.environ,
            {
                "WEB_PORTAL_EVENT_FORECAST_SERIES_LIMIT": "2",
                "WEB_PORTAL_EVENT_FORECAST_POINTS": "10",
            },
        ):
            series, note = web_portal._build_event_forecast_series(conn, market_rows)
        self.assertEqual(len(series), 1)
        self.assertIn("Using tick history", note)


class TestPortalEventDetailExtra(unittest.TestCase):
    def test_fetch_event_detail(self) -> None:
        now = datetime.now(timezone.utc)
        event_row = {
            "event_ticker": "EV1",
            "event_title": "Event Title",
            "event_sub_title": "Event Sub",
            "event_category": "Cat",
            "series_ticker": "EV",
            "strike_date": now.date().isoformat(),
            "strike_period": "hour",
            "mutually_exclusive": False,
            "available_on_brokers": True,
            "product_metadata": None,
            "open_time": (now - timedelta(hours=1)).isoformat(),
            "close_time": (now + timedelta(hours=1)).isoformat(),
            "market_count": 2,
        }
        market_rows = [
            {
                "ticker": "M1",
                "market_title": "Yes",
                "market_subtitle": "Yes",
                "market_open_time": (now - timedelta(hours=2)).isoformat(),
                "market_close_time": (now + timedelta(hours=2)).isoformat(),
                "settlement_value": 100,
                "settlement_value_dollars": None,
                "last_tick_ts": now.isoformat(),
                "tick_source": "live_snapshot",
                "implied_yes_mid": Decimal("0.6"),
                "price_dollars": Decimal("0.6"),
                "yes_bid_dollars": Decimal("0.55"),
                "yes_ask_dollars": Decimal("0.65"),
                "candle_end_ts": None,
                "candle_close": None,
                "predicted_yes_prob": Decimal("0.7"),
                "prediction_confidence": Decimal("0.8"),
                "prediction_ts": now.isoformat(),
            },
            {
                "ticker": "M2",
                "market_title": "No",
                "yes_sub_title": "No",
                "market_open_time": (now - timedelta(hours=2)).isoformat(),
                "market_close_time": (now - timedelta(hours=1)).isoformat(),
                "settlement_value": 0,
                "settlement_value_dollars": None,
                "last_tick_ts": None,
                "tick_source": "",
                "implied_yes_mid": None,
                "price_dollars": None,
                "yes_bid_dollars": Decimal("0"),
                "yes_ask_dollars": Decimal("1"),
                "candle_end_ts": (now - timedelta(hours=1)).isoformat(),
                "candle_close": Decimal("0.4"),
                "predicted_yes_prob": None,
                "prediction_confidence": None,
                "prediction_ts": None,
            },
        ]
        conn = RecordingConn(RecordingCursor(row=event_row))
        with patch.object(web_portal, "_fetch_event_market_rows", return_value=market_rows):
            with patch.object(web_portal, "_build_event_sparklines", return_value={"M1": [0.1]}):
                with patch.object(web_portal, "_build_event_forecast_series", return_value=([], None)):
                    event = web_portal.fetch_event_detail(conn, "EV1")
        self.assertIsNotNone(event)
        self.assertEqual(event["event_ticker"], "EV1")
        self.assertEqual(event["event_outcome_label"], "Yes")
        self.assertEqual(event["outcomes"][0]["freshness_source"], "snapshot")
        self.assertEqual(event["outcomes"][1]["freshness_source"], "backfill")


class TestPortalMarketDetailExtra(unittest.TestCase):
    def test_fetch_market_detail(self) -> None:
        now = datetime.now(timezone.utc)
        row = {
            "ticker": "T1",
            "event_ticker": "EV1",
            "title": "Market Title",
            "subtitle": "Sub",
            "yes_sub_title": "Yes",
            "no_sub_title": "No",
            "category": "Cat",
            "response_price_units": "cents",
            "tick_size": 1,
            "risk_limit_cents": 100,
            "strike_type": "custom",
            "floor_strike": 1,
            "cap_strike": 2,
            "functional_strike": 1.5,
            "settlement_value": 100,
            "settlement_value_dollars": Decimal("1"),
            "rules_primary": "Rule 1",
            "rules_secondary": "Rule 2",
            "price_level_structure": '{"levels":[1,2]}',
            "price_ranges": None,
            "custom_strike": None,
            "mve_selected_legs": None,
            "open_time": (now - timedelta(hours=1)).isoformat(),
            "close_time": (now + timedelta(hours=1)).isoformat(),
            "expiration_time": None,
            "settlement_ts": None,
            "status_label": "Open",
            "status_class": "open",
            "event_title": "Event Title",
            "event_sub_title": "Event Sub",
            "event_category": "Event Cat",
            "series_ticker": "EV",
            "strike_date": now.date().isoformat(),
            "strike_period": "hour",
            "mutually_exclusive": False,
            "available_on_brokers": True,
            "product_metadata": None,
            "active_status": "open",
            "active_last_seen": now.isoformat(),
            "last_tick_ts": None,
            "implied_yes_mid": Decimal("0.5"),
            "price_dollars": None,
            "yes_bid_dollars": Decimal("0.4"),
            "yes_ask_dollars": Decimal("0.6"),
            "volume": 10,
            "open_interest": 5,
            "candle_end_ts": now - timedelta(minutes=5),
            "candle_close": Decimal("0.45"),
        }
        interval_row = {"period_interval_minutes": 60}
        candle_rows = [
            {
                "end_period_ts": now - timedelta(minutes=10),
                "open": Decimal("0.4"),
                "high": Decimal("0.5"),
                "low": Decimal("0.3"),
                "close": Decimal("0.45"),
                "volume": 100,
            }
        ]
        prediction_rows = [
            {
                "created_at": now,
                "predicted_yes_prob": Decimal("0.6"),
                "confidence": Decimal("0.7"),
                "rationale": "Because",
                "agent": "agent",
                "model": "v1",
            },
            {
                "created_at": now - timedelta(hours=1),
                "predicted_yes_prob": Decimal("0.4"),
                "confidence": Decimal("0.5"),
                "rationale": "Older",
                "agent": "agent",
                "model": None,
            },
        ]
        conn = SequencedConn(
            [
                RecordingCursor(row=row),
                RecordingCursor(row=interval_row, rows=candle_rows),
                RecordingCursor(rows=prediction_rows),
            ]
        )
        market_data = {
            "custom_strike": None,
            "mve_selected_legs": {"leg": 1},
        }
        event_market_rows = [
            {
                "market_title": "Yes",
                "settlement_value": 100,
                "market_open_time": (now - timedelta(hours=2)).isoformat(),
                "market_close_time": (now + timedelta(hours=2)).isoformat(),
                "yes_bid_dollars": Decimal("0.6"),
                "yes_ask_dollars": Decimal("0.7"),
            }
        ]
        with patch.object(web_portal, "_get_market_data", return_value=(market_data, None, None)):
            with patch.object(web_portal, "_get_event_metadata", return_value=({"meta": 1}, None)):
                with patch.object(web_portal, "_update_market_extras") as update_market:
                    with patch.object(web_portal, "_update_event_metadata") as update_event:
                        with patch.object(web_portal, "_fetch_event_market_rows", return_value=event_market_rows):
                            market = web_portal.fetch_market_detail(conn, "T1")
        self.assertIsNotNone(market)
        self.assertEqual(market["market_ticker"], "T1")
        self.assertEqual(market["status_label"], "Open")
        self.assertTrue(market["candles"])
        update_market.assert_called_once()
        update_event.assert_called_once()


class TestPortalIndexRoutes(unittest.TestCase):
    def test_index_missing_db_or_password(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {}, clear=True):
                    with web_portal.app.test_request_context("/"):
                        with patch.object(web_portal, "render_template", return_value="ok") as render:
                            result = web_portal.index()
        self.assertEqual(result, "ok")
        _, kwargs = render.call_args
        self.assertIn("WEB_PORTAL_PASSWORD is not set.", kwargs["error"])

    def test_index_success(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        rows = [{"event_ticker": "EV1", "event_title": "Event", "market_count": 1}]
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {
                        "WEB_PORTAL_PASSWORD": "pw",
                        "DATABASE_URL": "postgres://example",
                    },
                    clear=True,
                ):
                    with web_portal.app.test_request_context("/?category=Sports&category=News"):
                        with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                            with patch.object(web_portal, "fetch_active_events", return_value=rows):
                                with patch.object(web_portal, "fetch_scheduled_events", return_value=rows):
                                    with patch.object(web_portal, "fetch_closed_events", return_value=rows):
                                        with patch.object(web_portal, "fetch_counts", return_value=(1, 1, 1)):
                                            with patch.object(
                                                web_portal, "fetch_strike_periods", return_value=["hour"]
                                            ):
                                                with patch.object(
                                                    web_portal,
                                                    "fetch_active_event_categories",
                                                    return_value=["Sports", "News", "Sports"],
                                                ):
                                                    with patch.object(
                                                        web_portal, "_load_portal_health", return_value={"ok": True}
                                                    ):
                                                        with patch.object(
                                                            web_portal, "render_template", return_value="ok"
                                                        ) as render:
                                                            result = web_portal.index()
        self.assertEqual(result, "ok")
        _, kwargs = render.call_args
        self.assertEqual(kwargs["active_total"], 1)
        self.assertTrue(kwargs["category_filters"])


class TestPortalHealthRoute(unittest.TestCase):
    def test_health_route(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.object(web_portal, "_build_health_cards", return_value=[{"title": "ok"}]):
                    with patch.object(web_portal, "render_template", return_value="ok") as render:
                        client = web_portal.app.test_client()
                        resp = client.get("/health")
        self.assertEqual(resp.status_code, 200)
        render.assert_called_once()


class TestPortalQueueStreamRoute(unittest.TestCase):
    def test_queue_stream_disabled(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"WEB_PORTAL_QUEUE_STREAM_ENABLE": "0"}):
                    client = web_portal.app.test_client()
                    resp = client.get("/stream/queue")
        self.assertEqual(resp.status_code, 404)

    def test_queue_stream_event_and_ping(self) -> None:
        class NotifyConn:
            def __init__(self):
                self.cursor_obj = DummyExecCursor()

            def cursor(self, *args, **kwargs):
                return self.cursor_obj

            def notifies(self, timeout=1.0):
                return iter([SimpleNamespace(payload='{"ok":1}')])

        @contextmanager
        def fake_db(*_args, **_kwargs):
            yield NotifyConn()

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {
                        "WEB_PORTAL_QUEUE_STREAM_ENABLE": "1",
                        "DATABASE_URL": "postgres://example",
                    },
                ):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        client = web_portal.app.test_client()
                        resp = client.get("/stream/queue")
                        chunk = next(resp.response).decode("utf-8")
                        resp.close()
        self.assertIn("event: queue", chunk)

        class PingConn:
            def __init__(self):
                self.cursor_obj = DummyExecCursor()

            def cursor(self, *args, **kwargs):
                return self.cursor_obj

            def notifies(self, timeout=1.0):
                return iter([])

        @contextmanager
        def fake_db_ping(*_args, **_kwargs):
            yield PingConn()

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {
                        "WEB_PORTAL_QUEUE_STREAM_ENABLE": "1",
                        "DATABASE_URL": "postgres://example",
                    },
                ):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db_ping):
                        with patch(
                            "src.web_portal.time.monotonic", side_effect=[0.0, 20.0]
                        ):
                            client = web_portal.app.test_client()
                            resp = client.get("/stream/queue")
                            chunk = next(resp.response).decode("utf-8")
                            resp.close()
        self.assertIn("event: ping", chunk)

    def test_queue_stream_error_event(self) -> None:
        def raise_db(*_args, **_kwargs):
            raise RuntimeError("boom")

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {
                        "WEB_PORTAL_QUEUE_STREAM_ENABLE": "1",
                        "DATABASE_URL": "postgres://example",
                    },
                ):
                    with patch.object(web_portal, "_db_connection", side_effect=raise_db):
                        client = web_portal.app.test_client()
                        resp = client.get("/stream/queue")
                        chunk = next(resp.response).decode("utf-8")
                        resp.close()
        self.assertIn("event: error", chunk)


class TestPortalEventDetailRoute(unittest.TestCase):
    def test_event_detail_missing_db(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {}, clear=True):
                    with patch.object(web_portal, "render_template", return_value="ok") as render:
                        client = web_portal.app.test_client()
                        resp = client.get("/event/EV1")
        self.assertEqual(resp.status_code, 200)
        _, kwargs = render.call_args
        self.assertIn("DATABASE_URL is not set.", kwargs["error"])

    def test_event_detail_not_found(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "fetch_event_detail", return_value=None):
                            with patch.object(web_portal, "render_template", return_value="ok") as render:
                                client = web_portal.app.test_client()
                                resp = client.get("/event/EV1")
        self.assertEqual(resp.status_code, 200)
        _, kwargs = render.call_args
        self.assertEqual(kwargs["error"], "Event not found.")


class TestPortalEventSnapshotRoute(unittest.TestCase):
    def test_event_snapshot_missing_db(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {}, clear=True):
                    client = web_portal.app.test_client()
                    resp = client.get("/event/EV1/snapshot")
        self.assertEqual(resp.status_code, 503)

    def test_event_snapshot_no_markets(self) -> None:
        class SnapshotConn:
            def __init__(self, rows, event_exists=True):
                self.rows = rows
                self.event_exists = event_exists

            def cursor(self, *args, **kwargs):
                if kwargs.get("row_factory") is web_portal.dict_row:
                    return DummyRowsCursor(self.rows)
                row = (1,) if self.event_exists else None
                return DummyCursor(row)

        @contextmanager
        def fake_db():
            yield SnapshotConn([])

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_snapshot_allows_closed", return_value=False):
                            client = web_portal.app.test_client()
                            resp = client.get("/event/EV1/snapshot")
        self.assertEqual(resp.status_code, 409)
        self.assertIn("Event is closed", resp.json["error"])

        @contextmanager
        def fake_db_allow():
            yield SnapshotConn([])

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db_allow):
                        with patch.object(web_portal, "_snapshot_allows_closed", return_value=True):
                            client = web_portal.app.test_client()
                            resp = client.get("/event/EV1/snapshot")
        self.assertEqual(resp.status_code, 409)
        self.assertIn("No markets found", resp.json["error"])

    def test_event_snapshot_event_not_found(self) -> None:
        class SnapshotConn:
            def cursor(self, *args, **kwargs):
                if kwargs.get("row_factory") is web_portal.dict_row:
                    return DummyRowsCursor([])
                return DummyCursor(None)

        @contextmanager
        def fake_db():
            yield SnapshotConn()

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        client = web_portal.app.test_client()
                        resp = client.get("/event/EV1/snapshot")
        self.assertEqual(resp.status_code, 404)

    def test_event_snapshot_updates_and_errors(self) -> None:
        now = datetime.now(timezone.utc)
        rows = [
            {"ticker": "CACHED", "close_time": now + timedelta(hours=1)},
            {"ticker": "CLOSED", "close_time": now - timedelta(hours=1)},
            {"ticker": "ERR", "close_time": now + timedelta(hours=1)},
            {"ticker": "MISSING", "close_time": now + timedelta(hours=1)},
            {"ticker": "INSERT_FAIL", "close_time": now + timedelta(hours=1)},
            {"ticker": "OK", "close_time": now + timedelta(hours=1)},
        ]

        class SnapshotConn:
            def __init__(self, rows):
                self.rows = rows

            def cursor(self, *args, **kwargs):
                if kwargs.get("row_factory") is web_portal.dict_row:
                    return DummyRowsCursor(self.rows)
                return DummyCursor((1,))

        @contextmanager
        def fake_db():
            yield SnapshotConn(rows)

        def fake_cached(conn, ticker, *_args, **_kwargs):
            if ticker == "CACHED":
                return {"cached": True}
            return None

        def fake_fetch(ticker):
            if ticker == "ERR":
                return {"error": "boom"}, None
            if ticker == "MISSING":
                return {"status": "open"}, None
            if ticker == "INSERT_FAIL":
                return {"status": "open"}, {"ticker": ticker}
            if ticker == "OK":
                return {"status": "open"}, {"ticker": ticker}
            return {}, None

        def fake_insert(_conn, snapshot_tick):
            if snapshot_tick.get("ticker") == "INSERT_FAIL":
                raise RuntimeError("boom")

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {
                        "DATABASE_URL": "postgres://example",
                        "WEB_PORTAL_SNAPSHOT_POLL_DELAY_MS": "1",
                    },
                ):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_snapshot_allows_closed", return_value=False):
                            with patch.object(web_portal, "_prefer_tick_snapshot", side_effect=fake_cached):
                                with patch.object(web_portal, "fetch_live_snapshot", side_effect=fake_fetch):
                                    with patch.object(web_portal, "insert_market_tick", side_effect=fake_insert):
                                        with patch("src.web_portal.time.sleep"):
                                            client = web_portal.app.test_client()
                                            resp = client.get("/event/EV1/snapshot")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json["cached"], 1)
        self.assertEqual(resp.json["updated"], 1)
        self.assertEqual(len(resp.json["errors"]), 4)

    def test_event_snapshot_rate_limited(self) -> None:
        rows = [{"ticker": "LIMIT", "close_time": None}]

        class SnapshotConn:
            def __init__(self, rows):
                self.rows = rows

            def cursor(self, *args, **kwargs):
                if kwargs.get("row_factory") is web_portal.dict_row:
                    return DummyRowsCursor(self.rows)
                return DummyCursor((1,))

        @contextmanager
        def fake_db():
            yield SnapshotConn(rows)

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {
                        "DATABASE_URL": "postgres://example",
                        "WEB_PORTAL_SNAPSHOT_POLL_COOLDOWN_SEC": "5",
                    },
                ):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_snapshot_allows_closed", return_value=True):
                            with patch.object(
                                web_portal,
                                "_prefer_tick_snapshot",
                                return_value=None,
                            ):
                                with patch.object(
                                    web_portal,
                                    "fetch_live_snapshot",
                                    return_value=({"rate_limited": True, "error": "nope"}, None),
                                ):
                                    with patch.object(web_portal, "_set_snapshot_backoff") as cooldown:
                                        client = web_portal.app.test_client()
                                        resp = client.get("/event/EV1/snapshot")
        self.assertEqual(resp.status_code, 200)
        cooldown.assert_called_once_with(5)
        self.assertIn("Rate limited", resp.json["errors"][0]["error"])


class TestPortalMarketDetailRoute(unittest.TestCase):
    def test_market_detail_missing_db(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {}, clear=True):
                    with patch.object(web_portal, "render_template", return_value="ok") as render:
                        client = web_portal.app.test_client()
                        resp = client.get("/market/T1")
        self.assertEqual(resp.status_code, 200)
        _, kwargs = render.call_args
        self.assertIn("DATABASE_URL is not set.", kwargs["error"])

    def test_market_detail_not_found(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "fetch_market_detail", return_value=None):
                            with patch.object(web_portal, "render_template", return_value="ok") as render:
                                client = web_portal.app.test_client()
                                resp = client.get("/market/T1")
        self.assertEqual(resp.status_code, 200)
        _, kwargs = render.call_args
        self.assertEqual(kwargs["error"], "Market not found.")


class TestPortalMarketSnapshotRoute(unittest.TestCase):
    def test_market_snapshot_missing_db(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {}, clear=True):
                    client = web_portal.app.test_client()
                    resp = client.get("/market/T1/snapshot")
        self.assertEqual(resp.status_code, 503)

    def test_market_snapshot_cached(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_snapshot_allows_closed", return_value=True):
                            with patch.object(web_portal, "_market_is_closed", return_value=False):
                                with patch.object(
                                    web_portal, "_prefer_tick_snapshot", return_value={"yes_mid": "$0.5000"}
                                ):
                                    client = web_portal.app.test_client()
                                    resp = client.get("/market/T1/snapshot")
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(resp.json["db_saved"])

    def test_market_snapshot_closed_disallowed(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_snapshot_allows_closed", return_value=False):
                            with patch.object(web_portal, "_market_is_closed", return_value=True):
                                client = web_portal.app.test_client()
                                resp = client.get("/market/T1/snapshot")
        self.assertEqual(resp.status_code, 409)

    def test_market_snapshot_rate_limited(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {"DATABASE_URL": "postgres://example", "WEB_PORTAL_SNAPSHOT_POLL_COOLDOWN_SEC": "7"},
                ):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_snapshot_allows_closed", return_value=True):
                            with patch.object(web_portal, "_market_is_closed", return_value=False):
                                with patch.object(web_portal, "_prefer_tick_snapshot", return_value=None):
                                    with patch.object(
                                        web_portal,
                                        "fetch_live_snapshot",
                                        return_value=({"error": "rate", "rate_limited": True}, None),
                                    ):
                                        with patch.object(web_portal, "_set_snapshot_backoff") as backoff:
                                            client = web_portal.app.test_client()
                                            resp = client.get("/market/T1/snapshot")
        self.assertEqual(resp.status_code, 429)
        backoff.assert_called_once_with(7)

    def test_market_snapshot_missing_tick(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_snapshot_allows_closed", return_value=True):
                            with patch.object(web_portal, "_market_is_closed", return_value=False):
                                with patch.object(web_portal, "_prefer_tick_snapshot", return_value=None):
                                    with patch.object(
                                        web_portal,
                                        "fetch_live_snapshot",
                                        return_value=({"status": "open"}, None),
                                    ):
                                        client = web_portal.app.test_client()
                                        resp = client.get("/market/T1/snapshot")
        self.assertEqual(resp.status_code, 503)

    def test_market_snapshot_success(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_snapshot_allows_closed", return_value=True):
                            with patch.object(web_portal, "_market_is_closed", return_value=False):
                                with patch.object(web_portal, "_prefer_tick_snapshot", return_value=None):
                                    with patch.object(
                                        web_portal,
                                        "fetch_live_snapshot",
                                        return_value=({"status": "open"}, {"ticker": "T1"}),
                                    ):
                                        with patch.object(web_portal, "insert_market_tick") as insert:
                                            client = web_portal.app.test_client()
                                            resp = client.get("/market/T1/snapshot")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json["db_saved"])
        insert.assert_called_once()


class TestPortalBackfillRoute(unittest.TestCase):
    def test_market_backfill_disabled(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"WEB_PORTAL_BACKFILL_MODE": "disabled"}):
                    client = web_portal.app.test_client()
                    resp = client.post("/market/T1/backfill")
        self.assertEqual(resp.status_code, 403)

    def test_market_backfill_unsupported_mode(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"WEB_PORTAL_BACKFILL_MODE": "direct"}):
                    client = web_portal.app.test_client()
                    resp = client.post("/market/T1/backfill")
        self.assertEqual(resp.status_code, 400)

    def test_market_backfill_queue_disabled(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {"WEB_PORTAL_BACKFILL_MODE": "queue", "WORK_QUEUE_ENABLE": "0"},
                ):
                    client = web_portal.app.test_client()
                    resp = client.post("/market/T1/backfill")
        self.assertEqual(resp.status_code, 409)

    def test_market_backfill_missing_db(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {"WEB_PORTAL_BACKFILL_MODE": "queue", "WORK_QUEUE_ENABLE": "1"},
                    clear=True,
                ):
                    client = web_portal.app.test_client()
                    resp = client.post("/market/T1/backfill")
        self.assertEqual(resp.status_code, 503)

    def test_market_backfill_market_not_found(self) -> None:
        @contextmanager
        def fake_db():
            yield DummyRowsConn([])

        cfg = web_portal.BackfillConfig(
            strike_periods=("hour", "day"),
            event_statuses=("open",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=24,
        )
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {
                        "WEB_PORTAL_BACKFILL_MODE": "queue",
                        "WORK_QUEUE_ENABLE": "1",
                        "DATABASE_URL": "postgres://example",
                    },
                ):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_load_backfill_config", return_value=cfg):
                            client = web_portal.app.test_client()
                            resp = client.post("/market/T1/backfill")
        self.assertEqual(resp.status_code, 404)

    def test_market_backfill_series_missing(self) -> None:
        row = {"ticker": "T1", "open_time": None, "close_time": None, "series_ticker": None, "strike_period": "hour"}

        @contextmanager
        def fake_db():
            yield DummyRowsConn([row])

        cfg = web_portal.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("open",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=24,
        )
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {
                        "WEB_PORTAL_BACKFILL_MODE": "queue",
                        "WORK_QUEUE_ENABLE": "1",
                        "DATABASE_URL": "postgres://example",
                    },
                ):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_load_backfill_config", return_value=cfg):
                            client = web_portal.app.test_client()
                            resp = client.post("/market/T1/backfill")
        self.assertEqual(resp.status_code, 503)

    def test_market_backfill_strike_period_disabled(self) -> None:
        row = {
            "ticker": "T1",
            "open_time": None,
            "close_time": None,
            "series_ticker": "SR",
            "strike_period": "week",
        }

        @contextmanager
        def fake_db():
            yield DummyRowsConn([row])

        cfg = web_portal.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("open",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=24,
        )
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {
                        "WEB_PORTAL_BACKFILL_MODE": "queue",
                        "WORK_QUEUE_ENABLE": "1",
                        "DATABASE_URL": "postgres://example",
                    },
                ):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_load_backfill_config", return_value=cfg):
                            with patch.object(web_portal, "_infer_strike_period_from_times", return_value=None):
                                client = web_portal.app.test_client()
                                resp = client.post("/market/T1/backfill")
        self.assertEqual(resp.status_code, 400)

    def test_market_backfill_inferred_strike_period(self) -> None:
        row = {
            "ticker": "T1",
            "open_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "close_time": datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc),
            "series_ticker": "SR",
            "strike_period": "week",
        }

        @contextmanager
        def fake_db():
            yield DummyRowsConn([row])

        cfg = web_portal.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("open",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=24,
        )
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {
                        "WEB_PORTAL_BACKFILL_MODE": "queue",
                        "WORK_QUEUE_ENABLE": "1",
                        "DATABASE_URL": "postgres://example",
                    },
                ):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_load_backfill_config", return_value=cfg):
                            with patch.object(web_portal, "_infer_strike_period_from_times", return_value="hour"):
                                with patch.object(web_portal, "enqueue_job", return_value="job-1"):
                                    client = web_portal.app.test_client()
                                    resp = client.post("/market/T1/backfill", json={"force_full": True})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json["queued"])


class TestPortalLoginExtra(unittest.TestCase):
    def test_login_invalid_password(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.dict(os.environ, {"WEB_PORTAL_PASSWORD": "pw"}):
                client = web_portal.app.test_client()
                resp = client.post("/login", data={"password": "bad"})
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Invalid password.", resp.get_data(as_text=True))

    def test_login_missing_password(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.dict(os.environ, {}, clear=True):
                client = web_portal.app.test_client()
                resp = client.post("/login", data={"password": "pw"})
        self.assertEqual(resp.status_code, 200)
        self.assertIn("WEB_PORTAL_PASSWORD is not set.", resp.get_data(as_text=True))

    def test_login_next_path_sanitized(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            client = web_portal.app.test_client()
            resp = client.get("/login?next=http://evil.com")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("action=\"/login?next=/\"", resp.get_data(as_text=True))


class TestPortalMain(unittest.TestCase):
    def test_main_requires_db_url(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                web_portal.main()

    def test_main_runs_app(self) -> None:
        @contextmanager
        def fake_db(*_args, **_kwargs):
            yield DummyConn(autocommit=False)

        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgres://example",
                "WEB_PORTAL_HOST": "127.0.0.1",
                "WEB_PORTAL_PORT": "8001",
                "WEB_PORTAL_THREADS": "2",
            },
            clear=True,
        ):
            with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                with patch.object(web_portal, "ensure_schema_compatible") as ensure_schema:
                    with patch.object(web_portal.app, "run") as run:
                        web_portal.main()
        ensure_schema.assert_called_once()
        run.assert_called_once_with(host="127.0.0.1", port=8001, threaded=True)


class TestPortalModuleEntrypoint(unittest.TestCase):
    def test_module_entrypoint_runs_main(self) -> None:
        module_name = "src.web_portal"
        sys.modules.pop(module_name, None)
        dummy_conn = DummyConn(autocommit=False)
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgres://example",
                "WEB_DB_POOL_ENABLE": "0",
                "WEB_DB_POOL_PREWARM": "0",
            },
            clear=True,
        ):
            with patch("flask.app.Flask.run") as run:
                with patch("psycopg.connect", return_value=dummy_conn):
                    with patch("src.db.db.ensure_schema_compatible") as ensure_schema:
                        runpy.run_module(module_name, run_name="__main__")
        run.assert_called_once()
        ensure_schema.assert_called_once()


class TestPortalHealthHelpersExtra(unittest.TestCase):
    def test_status_from_age(self) -> None:
        self.assertEqual(web_portal._status_from_age(None, 10), ("stale", "Missing"))
        self.assertEqual(web_portal._status_from_age(5, 10), ("ok", "Running"))
        self.assertEqual(web_portal._status_from_age(15, 10), ("stale", "Stale"))

    def test_load_portal_health_ws_alert(self) -> None:
        now = datetime.now(timezone.utc)
        state_rows = {
            "last_discovery_ts": {"value": (now - timedelta(minutes=30)).isoformat()},
            "last_min_close_ts": {"value": "1700000000"},
            "last_prediction_ts": {"value": None},
            "last_tick_ts": {"value": (now - timedelta(minutes=10)).isoformat()},
            "last_ws_tick_ts": {"value": (now - timedelta(minutes=3)).isoformat()},
        }

        class HealthConn:
            def __init__(self, row):
                self.row = row

            def cursor(self, *args, **kwargs):
                if kwargs.get("row_factory") is web_portal.dict_row:
                    return DummyRowsCursor([self.row])
                return DummyCursor((None,))

        conn = HealthConn({"pending": 2, "running": 1, "failed": 0, "workers": 3})
        prior_alert = web_portal._WS_LAG_LAST_ALERT
        try:
            web_portal._WS_LAG_LAST_ALERT = 0.0
            with patch.object(web_portal, "_fetch_state_rows", return_value=state_rows):
                with patch.object(web_portal.logger, "warning") as warn:
                    with patch("src.web_portal.time.monotonic", return_value=100.0):
                        with patch.dict(
                            os.environ,
                            {
                                "KALSHI_WS_ENABLE": "1",
                                "WEB_PORTAL_WS_STALE_SECONDS": "30",
                                "WEB_PORTAL_WS_LAG_ALERT_SECONDS": "60",
                                "WEB_PORTAL_WS_LAG_ALERT_COOLDOWN_SECONDS": "0",
                                "PREDICTION_ENABLE": "0",
                            },
                        ):
                            payload = web_portal._load_portal_health(conn)
        finally:
            web_portal._WS_LAG_LAST_ALERT = prior_alert
        self.assertEqual(payload["queue"]["pending"], 2)
        self.assertEqual(payload["ws"]["status"], "stale")
        self.assertTrue(payload["ws"]["alert"])
        warn.assert_called_once()


class TestPortalHealthCardsPasswordMissing(unittest.TestCase):
    def test_build_health_cards_missing_password_running(self) -> None:
        @contextmanager
        def fake_db(*_args, **_kwargs):
            class DummyCursorLocal:
                def execute(self, *_args, **_kwargs):
                    return None

                def fetchone(self):
                    return (1,)

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            class DummyConnLocal:
                def __init__(self):
                    self.info = SimpleNamespace(server_version=150002)

                def cursor(self, *args, **kwargs):
                    return DummyCursorLocal()

            yield DummyConnLocal()

        prior_started = web_portal._SNAPSHOT_THREAD_STARTED
        web_portal._SNAPSHOT_THREAD_STARTED = True
        try:
            with patch.dict(
                os.environ,
                {
                    "WEB_PORTAL_PASSWORD": "",
                    "DATABASE_URL": "postgres://example",
                },
                clear=True,
            ):
                with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                    with patch.object(web_portal, "_load_kalshi_client", return_value=(object(), None)):
                        with patch.object(web_portal, "_snapshot_poll_enabled", return_value=True):
                            cards = web_portal._build_health_cards()
        finally:
            web_portal._SNAPSHOT_THREAD_STARTED = prior_started
        auth_card = next(card for card in cards if card["title"] == "Portal Authentication")
        db_card = next(card for card in cards if card["title"] == "Database Connection")
        api_card = next(card for card in cards if card["title"] == "Kalshi API Client")
        poll_card = next(card for card in cards if card["title"] == "Snapshot Poller")
        self.assertEqual(auth_card["label"], "Missing")
        self.assertEqual(db_card["label"], "Connected")
        self.assertEqual(api_card["label"], "Ready")
        self.assertEqual(poll_card["label"], "Running")


class TestPortalAuthHelpersExtra(unittest.TestCase):
    def test_require_password(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                web_portal.require_password()
        with patch.dict(os.environ, {"WEB_PORTAL_PASSWORD": "pw"}):
            self.assertEqual(web_portal.require_password(), "pw")


class TestPortalIndexErrorHandling(unittest.TestCase):
    def test_index_db_error(self) -> None:
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {"WEB_PORTAL_PASSWORD": "pw", "DATABASE_URL": "postgres://example"},
                ):
                    with web_portal.app.test_request_context("/"):
                        with patch.object(web_portal, "_db_connection", side_effect=RuntimeError("boom")):
                            with patch.object(web_portal, "render_template", return_value="ok") as render:
                                result = web_portal.index()
        self.assertEqual(result, "ok")
        _, kwargs = render.call_args
        self.assertIn("boom", kwargs["error"])


class TestPortalEventDetailMissing(unittest.TestCase):
    def test_fetch_event_detail_missing(self) -> None:
        conn = RecordingConn(RecordingCursor(row=None))
        self.assertIsNone(web_portal.fetch_event_detail(conn, "EV1"))


class TestPortalMarketSnapshotErrors(unittest.TestCase):
    def test_market_snapshot_error(self) -> None:
        @contextmanager
        def fake_db():
            yield object()

        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(os.environ, {"DATABASE_URL": "postgres://example"}):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_snapshot_allows_closed", return_value=True):
                            with patch.object(web_portal, "_market_is_closed", return_value=False):
                                with patch.object(web_portal, "_prefer_tick_snapshot", return_value=None):
                                    with patch.object(
                                        web_portal,
                                        "fetch_live_snapshot",
                                        return_value=({"error": "boom"}, None),
                                    ):
                                        client = web_portal.app.test_client()
                                        resp = client.get("/market/T1/snapshot")
        self.assertEqual(resp.status_code, 503)
        self.assertIn("boom", resp.json["error"])


class TestPortalBackfillStrikePeriodAllowed(unittest.TestCase):
    def test_market_backfill_strike_period_allowed(self) -> None:
        row = {
            "ticker": "T1",
            "open_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "close_time": datetime(2024, 1, 1, 2, 0, tzinfo=timezone.utc),
            "series_ticker": "SR",
            "strike_period": "hour",
        }

        @contextmanager
        def fake_db():
            yield DummyRowsConn([row])

        cfg = web_portal.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("open",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=24,
        )
        with patch.object(web_portal, "_start_snapshot_polling"):
            with patch.object(web_portal, "is_authenticated", return_value=True):
                with patch.dict(
                    os.environ,
                    {
                        "WEB_PORTAL_BACKFILL_MODE": "queue",
                        "WORK_QUEUE_ENABLE": "1",
                        "DATABASE_URL": "postgres://example",
                    },
                ):
                    with patch.object(web_portal, "_db_connection", side_effect=fake_db):
                        with patch.object(web_portal, "_load_backfill_config", return_value=cfg):
                            with patch.object(web_portal, "enqueue_job", return_value="job-1") as enqueue:
                                client = web_portal.app.test_client()
                                resp = client.post("/market/T1/backfill")
        self.assertEqual(resp.status_code, 200)
        _, _, payload = enqueue.call_args[0]
        self.assertEqual(payload["market"]["open_time"], "2024-01-01T00:00:00+00:00")
        self.assertEqual(payload["market"]["close_time"], "2024-01-01T02:00:00+00:00")
