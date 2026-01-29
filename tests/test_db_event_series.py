import os
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

from src.web_portal import db_event_series


class DummyCursor:
    def __init__(self, *, row=None, rows=None):
        self._row = row
        self._rows = rows if rows is not None else []
        self.execute_calls = []

    def execute(self, *args, **kwargs) -> None:
        self.execute_calls.append((args, kwargs))

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestDbEventSeries(unittest.TestCase):
    def test_row_first_value_variants(self) -> None:
        self.assertEqual(db_event_series._row_first_value({"to_regclass": "ok"}), "ok")
        self.assertEqual(db_event_series._row_first_value({"relkind": "r"}), "r")
        self.assertEqual(db_event_series._row_first_value({"only": 5}), 5)
        self.assertEqual(db_event_series._row_first_value(["first", "second"]), "first")
        self.assertIsNone(db_event_series._row_first_value([]))
        self.assertIsNone(db_event_series._row_first_value(object()))

    def test_build_event_sparklines_from_ticks_skips_missing(self) -> None:
        rows = [
            {"ticker": "T1"},
            {"ticker": "T1", "yes_bid_dollars": Decimal("0.1"), "yes_ask_dollars": Decimal("0.3")},
            {"ticker": "T1", "price_dollars": Decimal("2.0")},
            {"ticker": None, "price_dollars": Decimal("0.5")},
        ]

        @contextmanager
        def fake_cursor(_conn, *args, **kwargs):
            yield DummyCursor(rows=rows)

        with patch.object(db_event_series, "timed_cursor", side_effect=fake_cursor):
            points = db_event_series._build_event_sparklines_from_ticks(
                object(), ["T1"], max_points=5
            )
        self.assertEqual(points["T1"], [0.2, 1.0])

    def test_build_event_sparklines_table_points(self) -> None:
        rows = [
            {"ticker": "T1", "points": [0.1, None, 0.25, 1.5]},
            {"ticker": None, "points": [0.2]},
        ]
        prior_ready = db_event_series._SPARKLINE_TABLE_READY
        db_event_series._SPARKLINE_TABLE_READY = True
        try:
            @contextmanager
            def fake_cursor(_conn, *args, **kwargs):
                yield DummyCursor(rows=rows)

            with patch.object(db_event_series, "timed_cursor", side_effect=fake_cursor):
                with patch.dict(os.environ, {"WEB_PORTAL_EVENT_SPARKLINE_POINTS": "3"}):
                    points = db_event_series._build_event_sparklines(object(), ["T1", "T2"])
        finally:
            db_event_series._SPARKLINE_TABLE_READY = prior_ready
        self.assertEqual(points["T1"], [0.25, 1.0])
        self.assertEqual(points["T2"], [])

    def test_forecast_candidates_missing_ticker_and_fallback_score(self) -> None:
        market_rows = [
            {"ticker": None, "title": "Missing"},
            {
                "ticker": "T1",
                "title": "Outcome",
                "market_open_time": None,
                "market_close_time": None,
                "predicted_yes_prob": None,
            },
        ]
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with patch.object(
            db_event_series,
            "_portal_func",
            return_value=lambda *_args, **_kwargs: (None, None, None),
        ):
            candidates = db_event_series._forecast_candidates(market_rows, now)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["ticker"], "T1")
        self.assertEqual(candidates[0]["score"], Decimal("-1"))

    def test_series_from_candles_skips_rows(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        rows = [
            {"market_ticker": None, "end_period_ts": now, "close": Decimal("0.3")},
            {"market_ticker": "T1", "end_period_ts": None, "close": Decimal("0.3")},
            {"market_ticker": "T1", "end_period_ts": now, "close": None},
            {"market_ticker": "T2", "end_period_ts": now, "close": Decimal("0.5")},
        ]
        selected = [{"ticker": "T1", "label": "Outcome"}]

        @contextmanager
        def fake_cursor(_conn, *args, **kwargs):
            yield DummyCursor(rows=rows)

        with patch.object(db_event_series, "timed_cursor", side_effect=fake_cursor):
            series, note = db_event_series._series_from_candles(
                object(),
                ["T1"],
                selected,
                max_points=5,
                total_outcomes=1,
            )
        self.assertEqual(series, [])
        self.assertIsNone(note)

    def test_series_from_ticks_empty_series(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        rows = [
            {"ticker": "T1", "ts": None, "price_dollars": Decimal("0.4")},
            {"ticker": None, "ts": now, "price_dollars": Decimal("0.4")},
            {"ticker": "T1", "ts": now, "price_dollars": Decimal("0.4")},
        ]
        selected = [{"ticker": "T1", "label": "Outcome"}]

        @contextmanager
        def fake_cursor(_conn, *args, **kwargs):
            yield DummyCursor(rows=rows)

        with patch.object(db_event_series, "timed_cursor", side_effect=fake_cursor):
            with patch.object(
                db_event_series,
                "_portal_func",
                return_value=lambda *_args, **_kwargs: (None, None, None),
            ):
                series, note = db_event_series._series_from_ticks(
                    object(),
                    ["T1"],
                    selected,
                    max_points=5,
                    total_outcomes=1,
                )
        self.assertEqual(series, [])
        self.assertIsNone(note)
