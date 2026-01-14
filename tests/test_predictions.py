import asyncio
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import ModuleType
from unittest.mock import Mock, patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

import src.predictions.predictions as predictions


class FakeCursor:
    def __init__(self, rows=None, row=None):
        self.rows = rows or []
        self.row = row
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, rows=None, row=None):
        self.rows = rows
        self.row = row

    def cursor(self, row_factory=None):
        return FakeCursor(rows=self.rows, row=self.row)


def _cfg(**overrides):
    base = predictions.PredictionConfig(
        enabled=True,
        intervals=predictions.PredictionIntervals(
            poll_seconds=5,
            interval_hour_minutes=10,
            interval_day_minutes=30,
            interval_default_minutes=15,
        ),
        limits=predictions.PredictionLimits(
            max_events_per_pass=5,
            max_markets_per_event=10,
            context_doc_limit=5,
        ),
        strike=predictions.PredictionStrikeConfig(
            strike_periods=("hour", "day"),
            hour_max_hours=2.0,
            day_max_hours=36.0,
        ),
        model=predictions.PredictionModelConfig(
            handler_path=None,
            agent_name="baseline",
            model_name=None,
            prompt="prompt",
        ),
    )
    if not overrides:
        return base
    intervals_keys = {
        "poll_seconds",
        "interval_hour_minutes",
        "interval_day_minutes",
        "interval_default_minutes",
    }
    limits_keys = {"max_events_per_pass", "max_markets_per_event", "context_doc_limit"}
    strike_keys = {"strike_periods", "hour_max_hours", "day_max_hours"}
    model_keys = {"handler_path", "agent_name", "model_name", "prompt"}

    enabled = overrides.pop("enabled", base.enabled)
    interval_overrides = {k: overrides.pop(k) for k in list(overrides) if k in intervals_keys}
    limit_overrides = {k: overrides.pop(k) for k in list(overrides) if k in limits_keys}
    strike_overrides = {k: overrides.pop(k) for k in list(overrides) if k in strike_keys}
    model_overrides = {k: overrides.pop(k) for k in list(overrides) if k in model_keys}
    if overrides:
        raise ValueError(f"Unexpected overrides: {sorted(overrides)}")

    cfg = replace(base, enabled=enabled)
    if interval_overrides:
        cfg = replace(cfg, intervals=replace(cfg.intervals, **interval_overrides))
    if limit_overrides:
        cfg = replace(cfg, limits=replace(cfg.limits, **limit_overrides))
    if strike_overrides:
        cfg = replace(cfg, strike=replace(cfg.strike, **strike_overrides))
    if model_overrides:
        cfg = replace(cfg, model=replace(cfg.model, **model_overrides))
    return cfg


class TestPredictionParsing(unittest.TestCase):
    def test_parse_helpers(self) -> None:
        self.assertTrue(predictions._parse_bool("yes"))
        self.assertFalse(predictions._parse_bool("no"))
        self.assertFalse(predictions._parse_bool(None))
        self.assertEqual(predictions._parse_int("bad", 3), 3)
        self.assertEqual(predictions._parse_int(None, 3), 3)
        self.assertEqual(predictions._parse_int("-1", 3, minimum=1), 1)
        self.assertEqual(predictions._parse_float("bad", 2.5), 2.5)
        self.assertEqual(predictions._parse_float(None, 2.5), 2.5)
        self.assertEqual(predictions._parse_float("-1", 2.5, minimum=0.1), 0.1)
        self.assertEqual(predictions._parse_csv("hour, day ,"), ("hour", "day"))
        self.assertEqual(predictions._parse_csv(None), ())

    def test_load_prompt_from_env(self) -> None:
        with patch.dict(os.environ, {"PREDICTION_PROMPT": " hello "}):
            self.assertEqual(predictions._load_prompt(), "hello")

    def test_load_prompt_from_file(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as handle:
            handle.write("file prompt")
            path = handle.name
        try:
            with patch.dict(os.environ, {"PREDICTION_PROMPT_PATH": path}):
                self.assertEqual(predictions._load_prompt(), "file prompt")
        finally:
            os.unlink(path)

    def test_load_prompt_file_error(self) -> None:
        with patch.dict(os.environ, {"PREDICTION_PROMPT_PATH": "/missing"}), \
             patch.object(predictions.logger, "warning") as warn:
            self.assertEqual(predictions._load_prompt(), predictions.DEFAULT_PROMPT)
        warn.assert_called_once()

    def test_load_prediction_config(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PREDICTION_ENABLE": "1",
                "PREDICTION_POLL_SECONDS": "3",
                "PREDICTION_INTERVAL_HOUR_MINUTES": "2",
                "PREDICTION_INTERVAL_DAY_MINUTES": "5",
                "PREDICTION_INTERVAL_DEFAULT_MINUTES": "7",
                "PREDICTION_MAX_EVENTS": "4",
                "PREDICTION_MAX_MARKETS": "11",
                "PREDICTION_CONTEXT_DOCS": "0",
                "PREDICTION_STRIKE_PERIODS": "hour",
                "STRIKE_HOUR_MAX_HOURS": "1.5",
                "STRIKE_DAY_MAX_HOURS": "20",
                "PREDICTION_HANDLER": "mod:handler",
                "PREDICTION_AGENT_NAME": "agent",
                "PREDICTION_MODEL_NAME": "model",
            },
        ), patch("src.predictions.predictions._load_prompt", return_value="prompt"):
            cfg = predictions.load_prediction_config()
        self.assertTrue(cfg.enabled)
        self.assertEqual(cfg.intervals.poll_seconds, 5)
        self.assertEqual(cfg.intervals.interval_hour_minutes, 2)
        self.assertEqual(cfg.intervals.interval_day_minutes, 5)
        self.assertEqual(cfg.intervals.interval_default_minutes, 7)
        self.assertEqual(cfg.limits.max_events_per_pass, 4)
        self.assertEqual(cfg.limits.max_markets_per_event, 11)
        self.assertEqual(cfg.limits.context_doc_limit, 0)
        self.assertEqual(cfg.strike.strike_periods, ("hour",))
        self.assertEqual(cfg.strike.hour_max_hours, 1.5)
        self.assertEqual(cfg.strike.day_max_hours, 20.0)
        self.assertEqual(cfg.model.handler_path, "mod:handler")
        self.assertEqual(cfg.model.agent_name, "agent")
        self.assertEqual(cfg.model.model_name, "model")
        self.assertEqual(cfg.model.prompt, "prompt")


class TestPredictionHelpers(unittest.TestCase):
    def test_normalize_prob(self) -> None:
        self.assertIsNone(predictions._normalize_prob("bad"))
        self.assertIsNone(predictions._normalize_prob(2))
        self.assertIsNone(predictions._normalize_prob(-1))
        self.assertEqual(predictions._normalize_prob("0.1"), Decimal("0.100000"))

    def test_infer_strike_period(self) -> None:
        open_time = datetime(2024, 1, 1, 0, 0, 0)
        close_time = datetime(2024, 1, 1, 1, 0, 0)
        self.assertEqual(
            predictions._infer_strike_period(open_time, close_time, 2.0, 36.0),
            "hour",
        )
        close_time = datetime(2024, 1, 2, 0, 0, 0)
        self.assertEqual(
            predictions._infer_strike_period(open_time, close_time, 2.0, 36.0),
            "day",
        )
        close_time = datetime(2024, 1, 1, 0, 0, 0)
        self.assertIsNone(
            predictions._infer_strike_period(open_time, close_time, 2.0, 36.0),
        )
        close_time = datetime(2024, 2, 1, 0, 0, 0)
        self.assertIsNone(
            predictions._infer_strike_period(open_time, close_time, 2.0, 10.0),
        )
        self.assertIsNone(predictions._infer_strike_period(None, close_time, 2.0, 36.0))

    def test_event_interval_minutes(self) -> None:
        cfg = _cfg(strike_periods=("hour",))
        event = predictions.PredictionEvent(
            event_ticker="EV1",
            event_title=None,
            strike_period="hour",
            open_time=None,
            close_time=None,
            last_prediction_ts=None,
        )
        self.assertEqual(predictions._event_interval_minutes(event, cfg), 10)
        cfg = _cfg(strike_periods=("day",))
        self.assertIsNone(predictions._event_interval_minutes(event, cfg))

        now = datetime.now(timezone.utc)
        inferred = predictions.PredictionEvent(
            event_ticker="EV2",
            event_title=None,
            strike_period=None,
            open_time=now,
            close_time=now + timedelta(hours=1),
            last_prediction_ts=None,
        )
        cfg = _cfg(strike_periods=("hour",))
        self.assertEqual(predictions._event_interval_minutes(inferred, cfg), 10)
        day_event = predictions.PredictionEvent(
            event_ticker="EV3",
            event_title=None,
            strike_period="day",
            open_time=None,
            close_time=None,
            last_prediction_ts=None,
        )
        cfg = _cfg(strike_periods=("day",))
        self.assertEqual(predictions._event_interval_minutes(day_event, cfg), 30)
        default_event = predictions.PredictionEvent(
            event_ticker="EV4",
            event_title=None,
            strike_period="week",
            open_time=None,
            close_time=None,
            last_prediction_ts=None,
        )
        cfg = _cfg(strike_periods=())
        self.assertEqual(predictions._event_interval_minutes(default_event, cfg), 15)

    def test_fetch_candidate_events(self) -> None:
        now = datetime.now(timezone.utc)
        rows = [
            {
                "event_ticker": "EV1",
                "event_title": "Title",
                "strike_period": "hour",
                "open_time": now,
                "close_time": now,
                "last_prediction_ts": None,
            },
            {"event_ticker": None},
        ]
        conn = FakeConn(rows=rows)
        events = predictions._fetch_candidate_events(conn, limit=2)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_ticker, "EV1")

    def test_fetch_event_metadata(self) -> None:
        row = {"event_ticker": "EV1", "title": "Title"}
        conn = FakeConn(row=row)
        meta = predictions._fetch_event_metadata(conn, "EV1")
        self.assertEqual(meta["event_ticker"], "EV1")

        conn = FakeConn(row=None)
        meta = predictions._fetch_event_metadata(conn, "EV1")
        self.assertEqual(meta, {})

    def test_fetch_event_markets(self) -> None:
        conn = FakeConn(rows=[{"ticker": "M1"}, {"ticker": "M2"}])
        markets = predictions._fetch_event_markets(conn, "EV1", 2)
        self.assertEqual([m["ticker"] for m in markets], ["M1", "M2"])

    def test_fetch_rag_documents(self) -> None:
        self.assertEqual(
            predictions._fetch_rag_documents(Mock(), "EV1", ["M1"], 0),
            [],
        )
        conn = FakeConn(rows=[{"id": 1}, {"id": 2}])
        docs = predictions._fetch_rag_documents(conn, "EV1", ["M1"], 2)
        self.assertEqual([doc["id"] for doc in docs], [1, 2])

    def test_load_handler(self) -> None:
        module = ModuleType("mod")

        def handler(*_args, **_kwargs):
            return []

        module.handler = handler
        with patch.dict("sys.modules", {"mod": module}):
            loaded = predictions._load_handler("mod:handler")
        self.assertIs(loaded, handler)

    def test_load_handler_non_callable(self) -> None:
        module = ModuleType("mod2")
        module.handler = "nope"
        with patch.dict("sys.modules", {"mod2": module}):
            with self.assertRaises(TypeError):
                predictions._load_handler("mod2:handler")

    def test_default_prediction_handler(self) -> None:
        markets = [
            {"ticker": "M1", "implied_yes_mid": "0.2"},
            {"ticker": "M2", "implied_yes_mid": None, "candle_close": "0.3"},
            {"ticker": "M3", "implied_yes_mid": "bad", "candle_close": None},
            {"ticker": None, "implied_yes_mid": "0.5"},
        ]
        results = predictions.default_prediction_handler({}, markets, {}, "")
        tickers = [r["market_ticker"] for r in results]
        self.assertEqual(tickers, ["M1", "M2"])

    def test_resolve_prediction_handler_default(self) -> None:
        cfg = _cfg(handler_path=None)
        handler = predictions.resolve_prediction_handler(cfg)
        self.assertIs(handler, predictions.default_prediction_handler)

    def test_resolve_prediction_handler_fallback(self) -> None:
        cfg = _cfg(handler_path="bad:path")
        with patch("src.predictions.predictions._load_handler", side_effect=RuntimeError("boom")), \
             patch.object(predictions.logger, "exception") as log_exc:
            handler = predictions.resolve_prediction_handler(cfg)
        self.assertIs(handler, predictions.default_prediction_handler)
        log_exc.assert_called_once()

    def test_coerce_predictions(self) -> None:
        results = [
            {"market_ticker": "M1", "yes_prob": "0.2", "confidence": "0.5"},
            {"market_ticker": "M1", "yes_prob": "bad"},
            {"ticker": "M2", "predicted_yes_prob": "0.3"},
            {"ticker": "M3", "probability": "bad"},
            "skip",
        ]
        preds = predictions._coerce_predictions("EV1", 5, results, {"M1", "M2"})
        self.assertEqual(len(preds), 2)
        self.assertEqual(preds[0]["market_ticker"], "M1")
        self.assertEqual(preds[0]["predicted_yes_prob"], Decimal("0.200000"))


class TestPredictionPass(unittest.TestCase):
    def test_prediction_pass_disabled(self) -> None:
        cfg = _cfg(enabled=False)
        with patch("src.predictions.predictions.assert_service_role") as assert_role:
            self.assertEqual(predictions.prediction_pass(Mock(), cfg), 0)
        assert_role.assert_not_called()

    def test_prediction_pass_resolves_handler(self) -> None:
        now = datetime.now(timezone.utc)
        event = predictions.PredictionEvent(
            event_ticker="EV1",
            event_title="Title",
            strike_period="hour",
            open_time=now - timedelta(hours=1),
            close_time=now + timedelta(hours=1),
            last_prediction_ts=None,
        )
        cfg = _cfg()
        handler = Mock(return_value=[])
        with patch("src.predictions.predictions.assert_service_role"), \
             patch("src.predictions.predictions.resolve_prediction_handler", return_value=handler) as resolve, \
             patch("src.predictions.predictions._fetch_candidate_events", return_value=[event]), \
             patch("src.predictions.predictions._fetch_event_metadata", return_value={"event_ticker": "EV1"}), \
             patch("src.predictions.predictions._fetch_event_markets", return_value=[{"ticker": "M1", "volume": 1}]), \
             patch("src.predictions.predictions._fetch_rag_documents", return_value=[]), \
             patch("src.predictions.predictions.insert_prediction_run", return_value=1), \
             patch("src.predictions.predictions.update_prediction_run"), \
             patch("src.predictions.predictions.set_state"):
            predictions.prediction_pass(Mock(), cfg, handler=None)
        resolve.assert_called_once_with(cfg)

    def test_prediction_pass_success(self) -> None:
        now = datetime.now(timezone.utc)
        event = predictions.PredictionEvent(
            event_ticker="EV1",
            event_title="Title",
            strike_period="hour",
            open_time=now - timedelta(hours=1),
            close_time=now + timedelta(hours=1),
            last_prediction_ts=None,
        )
        cfg = _cfg()
        handler = Mock(return_value=[{"market_ticker": "M1", "yes_prob": "0.2"}])
        with patch("src.predictions.predictions.assert_service_role"), \
             patch("src.predictions.predictions._fetch_candidate_events", return_value=[event]), \
             patch("src.predictions.predictions._fetch_event_metadata", return_value={"event_ticker": "EV1"}), \
             patch("src.predictions.predictions._fetch_event_markets", return_value=[{"ticker": "M1", "volume": 1}]), \
             patch("src.predictions.predictions._fetch_rag_documents", return_value=[]), \
             patch("src.predictions.predictions.insert_prediction_run", return_value=7) as insert_run, \
             patch("src.predictions.predictions.insert_market_predictions") as insert_predictions, \
             patch("src.predictions.predictions.update_prediction_run") as update_run, \
             patch("src.predictions.predictions.set_state") as set_state:
            count = predictions.prediction_pass(Mock(), cfg, handler=handler)
        self.assertEqual(count, 1)
        insert_run.assert_called_once()
        insert_predictions.assert_called_once()
        update_run.assert_called_once()
        self.assertEqual(set_state.call_count, 2)

    def test_prediction_pass_no_markets(self) -> None:
        event = predictions.PredictionEvent(
            event_ticker="EV1",
            event_title="Title",
            strike_period="hour",
            open_time=None,
            close_time=None,
            last_prediction_ts=None,
        )
        cfg = _cfg()
        with patch("src.predictions.predictions.assert_service_role"), \
             patch("src.predictions.predictions._fetch_candidate_events", return_value=[event]), \
             patch("src.predictions.predictions._fetch_event_metadata", return_value={}), \
             patch("src.predictions.predictions._fetch_event_markets", return_value=[]), \
             patch("src.predictions.predictions.insert_prediction_run") as insert_run, \
             patch("src.predictions.predictions.set_state"):
            count = predictions.prediction_pass(Mock(), cfg, handler=Mock())
        self.assertEqual(count, 0)
        insert_run.assert_not_called()

    def test_prediction_pass_interval_none(self) -> None:
        event = predictions.PredictionEvent(
            event_ticker="EV1",
            event_title="Title",
            strike_period="week",
            open_time=None,
            close_time=None,
            last_prediction_ts=None,
        )
        cfg = _cfg(strike_periods=("hour",))
        with patch("src.predictions.predictions.assert_service_role"), \
             patch("src.predictions.predictions._fetch_candidate_events", return_value=[event]), \
             patch("src.predictions.predictions._fetch_event_metadata") as fetch_meta, \
             patch("src.predictions.predictions.set_state"):
            count = predictions.prediction_pass(Mock(), cfg, handler=Mock())
        self.assertEqual(count, 0)
        fetch_meta.assert_not_called()

    def test_prediction_pass_naive_last_ts_due(self) -> None:
        event = predictions.PredictionEvent(
            event_ticker="EV1",
            event_title="Title",
            strike_period="hour",
            open_time=None,
            close_time=None,
            last_prediction_ts=datetime(2000, 1, 1, 0, 0, 0),
        )
        cfg = _cfg()
        with patch("src.predictions.predictions.assert_service_role"), \
             patch("src.predictions.predictions._fetch_candidate_events", return_value=[event]), \
             patch("src.predictions.predictions._fetch_event_metadata", return_value={}) as fetch_meta, \
             patch("src.predictions.predictions._fetch_event_markets", return_value=[]), \
             patch("src.predictions.predictions.set_state"):
            count = predictions.prediction_pass(Mock(), cfg, handler=Mock())
        self.assertEqual(count, 0)
        fetch_meta.assert_called_once()

    def test_prediction_pass_exception_before_run(self) -> None:
        event = predictions.PredictionEvent(
            event_ticker="EV1",
            event_title="Title",
            strike_period="hour",
            open_time=None,
            close_time=None,
            last_prediction_ts=None,
        )
        cfg = _cfg()
        with patch("src.predictions.predictions.assert_service_role"), \
             patch("src.predictions.predictions._fetch_candidate_events", return_value=[event]), \
             patch("src.predictions.predictions._fetch_event_metadata", side_effect=RuntimeError("boom")), \
             patch("src.predictions.predictions.insert_prediction_run", return_value=11) as insert_run, \
             patch("src.predictions.predictions.update_prediction_run") as update_run, \
             patch("src.predictions.predictions.set_state"):
            count = predictions.prediction_pass(Mock(), cfg, handler=Mock())
        self.assertEqual(count, 0)
        insert_run.assert_called_once()
        update_run.assert_not_called()

    def test_prediction_pass_exception_after_run(self) -> None:
        event = predictions.PredictionEvent(
            event_ticker="EV1",
            event_title="Title",
            strike_period="hour",
            open_time=None,
            close_time=None,
            last_prediction_ts=None,
        )
        cfg = _cfg()
        handler = Mock(side_effect=RuntimeError("boom"))
        with patch("src.predictions.predictions.assert_service_role"), \
             patch("src.predictions.predictions._fetch_candidate_events", return_value=[event]), \
             patch("src.predictions.predictions._fetch_event_metadata", return_value={"event_ticker": "EV1"}), \
             patch("src.predictions.predictions._fetch_event_markets", return_value=[{"ticker": "M1", "volume": 1}]), \
             patch("src.predictions.predictions._fetch_rag_documents", return_value=[]), \
             patch("src.predictions.predictions.insert_prediction_run", return_value=22), \
             patch("src.predictions.predictions.update_prediction_run") as update_run, \
             patch("src.predictions.predictions.set_state"):
            count = predictions.prediction_pass(Mock(), cfg, handler=handler)
        self.assertEqual(count, 0)
        update_run.assert_called_once()

    def test_prediction_pass_set_state_failure(self) -> None:
        event = predictions.PredictionEvent(
            event_ticker="EV1",
            event_title="Title",
            strike_period="hour",
            open_time=None,
            close_time=None,
            last_prediction_ts=None,
        )
        cfg = _cfg()
        handler = Mock(return_value=[])
        with patch("src.predictions.predictions.assert_service_role"), \
             patch("src.predictions.predictions._fetch_candidate_events", return_value=[event]), \
             patch("src.predictions.predictions._fetch_event_metadata", return_value={"event_ticker": "EV1"}), \
             patch("src.predictions.predictions._fetch_event_markets", return_value=[{"ticker": "M1", "volume": 1}]), \
             patch("src.predictions.predictions._fetch_rag_documents", return_value=[]), \
             patch("src.predictions.predictions.insert_prediction_run", return_value=1), \
             patch("src.predictions.predictions.update_prediction_run"), \
             patch("src.predictions.predictions.set_state", side_effect=RuntimeError("boom")), \
             patch.object(predictions.logger, "exception") as log_exc:
            predictions.prediction_pass(Mock(), cfg, handler=handler)
        self.assertEqual(log_exc.call_count, 2)

    def test_prediction_pass_skips_markets_without_volume(self) -> None:
        event = predictions.PredictionEvent(
            event_ticker="EV1",
            event_title="Title",
            strike_period="hour",
            open_time=None,
            close_time=None,
            last_prediction_ts=None,
        )
        cfg = _cfg()
        with patch("src.predictions.predictions.assert_service_role"), \
             patch("src.predictions.predictions._fetch_candidate_events", return_value=[event]), \
             patch("src.predictions.predictions._fetch_event_metadata", return_value={"event_ticker": "EV1"}), \
             patch("src.predictions.predictions._fetch_event_markets", return_value=[{"ticker": "M1"}]), \
             patch("src.predictions.predictions._fetch_rag_documents") as fetch_docs, \
             patch("src.predictions.predictions.insert_prediction_run") as insert_run, \
             patch("src.predictions.predictions.set_state"):
            count = predictions.prediction_pass(Mock(), cfg, handler=Mock())
        self.assertEqual(count, 0)
        fetch_docs.assert_not_called()
        insert_run.assert_not_called()


class TestPeriodicPredictions(unittest.IsolatedAsyncioTestCase):
    async def test_periodic_predictions_disabled(self) -> None:
        cfg = _cfg(enabled=False)
        await predictions.periodic_predictions(Mock(), cfg)

    async def test_periodic_predictions_runs(self) -> None:
        cfg = _cfg(enabled=True, poll_seconds=1)
        with patch("src.predictions.predictions.resolve_prediction_handler", return_value="handler"), \
             patch("src.predictions.predictions.prediction_pass", return_value=2), \
             patch("src.predictions.predictions.asyncio.sleep", side_effect=asyncio.CancelledError), \
             patch.object(predictions.logger, "info") as log_info:
            with self.assertRaises(asyncio.CancelledError):
                await predictions.periodic_predictions(Mock(), cfg)
        log_info.assert_called_once()

    async def test_periodic_predictions_logs_exception(self) -> None:
        cfg = _cfg(enabled=True, poll_seconds=1)
        with patch("src.predictions.predictions.resolve_prediction_handler", return_value="handler"), \
             patch("src.predictions.predictions.prediction_pass", side_effect=RuntimeError("boom")), \
             patch("src.predictions.predictions.asyncio.sleep", side_effect=asyncio.CancelledError), \
             patch.object(predictions.logger, "exception") as log_exc:
            with self.assertRaises(asyncio.CancelledError):
                await predictions.periodic_predictions(Mock(), cfg)
        log_exc.assert_called_once()
