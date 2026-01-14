import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from _test_utils import add_src_to_path

add_src_to_path()

import src.jobs.event_filter as event_filter


class TestEventScanStats(unittest.TestCase):
    def test_summarize_counts_empty(self) -> None:
        stats = event_filter.EventScanStats()
        self.assertEqual(stats.summarize_strike_counts(), "none")
        self.assertEqual(stats.summarize_inferred_counts(), "none")

    def test_summarize_counts_limit(self) -> None:
        stats = event_filter.EventScanStats(
            strike_counts={"hour": 3, "": 2, "day": 1},
            inferred_counts={"day": 5, "hour": 4, "week": 1},
        )
        self.assertEqual(stats.summarize_strike_counts(limit=2), "hour=3,unknown=2,...")
        self.assertEqual(stats.summarize_inferred_counts(limit=2), "day=5,hour=4,...")


class TestParseIso(unittest.TestCase):
    def test_parse_iso(self) -> None:
        self.assertIsNone(event_filter._parse_iso(None))
        dt = event_filter._parse_iso(1704067200)
        self.assertEqual(dt, datetime(2024, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(
            event_filter._parse_iso("2024-01-01T00:00:00Z"),
            datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        self.assertIsNone(event_filter._parse_iso({"bad": "type"}))
        self.assertIsNone(event_filter._parse_iso("not-a-date"))


class TestInferStrikePeriod(unittest.TestCase):
    def test_infer_strike_period_event_times(self) -> None:
        event = {"open_time": "2024-01-01T00:00:00Z", "close_time": "2024-01-01T01:00:00Z"}
        self.assertEqual(event_filter._infer_strike_period(event, 2.0, 36.0), "hour")
        event["close_time"] = "2024-01-01T10:00:00Z"
        self.assertEqual(event_filter._infer_strike_period(event, 2.0, 12.0), "day")
        event["close_time"] = "2024-01-03T00:00:00Z"
        self.assertIsNone(event_filter._infer_strike_period(event, 2.0, 36.0))
        event["close_time"] = "2023-12-31T00:00:00Z"
        self.assertIsNone(event_filter._infer_strike_period(event, 2.0, 36.0))

    def test_infer_strike_period_market_times(self) -> None:
        event = {
            "markets": [
                {"open_time": "2024-01-01T00:00:00Z", "close_time": "2024-01-01T02:00:00Z"},
                {"open_time": "2024-01-01T01:00:00Z", "close_time": "2024-01-01T03:00:00Z"},
            ]
        }
        self.assertEqual(event_filter._infer_strike_period(event, 4.0, 36.0), "hour")

    def test_infer_strike_period_naive_times(self) -> None:
        event = {"open_time": "2024-01-01T00:00:00", "close_time": "2024-01-01T01:00:00"}
        self.assertEqual(event_filter._infer_strike_period(event, 2.0, 36.0), "hour")

    def test_infer_strike_period_missing_times(self) -> None:
        self.assertIsNone(event_filter._infer_strike_period({}, 2.0, 36.0))


class TestAcceptEvent(unittest.TestCase):
    def test_accept_event_direct_strike(self) -> None:
        stats = event_filter.EventScanStats()
        event = {"event_ticker": "EV1", "strike_period": "Hour"}
        result = event_filter.accept_event(event, ("hour",), stats)
        self.assertEqual(result, "hour")
        self.assertEqual(stats.raw_events, 1)
        self.assertEqual(stats.filtered_events, 0)
        self.assertEqual(stats.dup_events, 0)
        self.assertEqual(stats.inferred_events, 0)
        self.assertIn("EV1", stats.seen_events)
        self.assertEqual(stats.strike_counts.get("hour"), 1)

    def test_accept_event_duplicate(self) -> None:
        stats = event_filter.EventScanStats()
        event = {"event_ticker": "EV1", "strike_period": "hour"}
        self.assertEqual(event_filter.accept_event(event, ("hour",), stats), "hour")
        self.assertIsNone(event_filter.accept_event(event, ("hour",), stats))
        self.assertEqual(stats.raw_events, 2)
        self.assertEqual(stats.dup_events, 1)
        self.assertEqual(stats.strike_counts.get("hour"), 1)

    def test_accept_event_inferred(self) -> None:
        stats = event_filter.EventScanStats()
        event = {
            "event_ticker": "EV2",
            "strike_period": "week",
            "open_time": "2024-01-01T00:00:00Z",
            "close_time": "2024-01-01T01:00:00Z",
        }
        with patch.dict(os.environ, {"STRIKE_HOUR_MAX_HOURS": "2", "STRIKE_DAY_MAX_HOURS": "36"}):
            result = event_filter.accept_event(event, ("hour",), stats)
        self.assertEqual(result, "hour")
        self.assertEqual(stats.inferred_events, 1)
        self.assertEqual(stats.inferred_counts.get("hour"), 1)
        self.assertEqual(stats.filtered_events, 0)

    def test_accept_event_filtered(self) -> None:
        stats = event_filter.EventScanStats()
        event = {}
        with patch.dict(os.environ, {"STRIKE_HOUR_MAX_HOURS": "2", "STRIKE_DAY_MAX_HOURS": "36"}):
            result = event_filter.accept_event(event, ("hour",), stats)
        self.assertIsNone(result)
        self.assertEqual(stats.filtered_events, 1)
        self.assertEqual(stats.raw_events, 1)
        self.assertEqual(stats.seen_events, set())
        self.assertEqual(stats.strike_counts.get(""), 1)
