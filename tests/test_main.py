import asyncio
import inspect
import os
import runpy
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, mock_open, patch

from _test_utils import (
    add_src_to_path,
    ensure_cryptography_stub,
    ensure_psycopg_stub,
    ensure_websockets_stub,
)

ensure_psycopg_stub()
add_src_to_path()
ensure_websockets_stub()
ensure_cryptography_stub()

import src.services.main as main
import src.rag.rag_loop as rag_loop
import src.ingest.rest_loop as rest_loop


def _settings(**overrides):
    base = SimpleNamespace(
        strike_periods=("hour",),
        discovery_event_statuses=("open",),
        backfill_event_statuses=("closed",),
        closed_cleanup_event_statuses=("closed",),
        candle_minutes_for_hour=1,
        candle_minutes_for_day=60,
        candle_lookback_hours=2,
        closed_cleanup_grace_minutes=5,
        discovery_seconds=1,
        backfill_seconds=2,
        closed_cleanup_seconds=3,
        kalshi_host="https://host",
        kalshi_api_key_id="key",
        database_url="postgres://db",
        kalshi_private_key_pem_path="/tmp/key.pem",
        max_active_tickers=10,
        ws_batch_size=5,
        ws_sub_refresh_seconds=10,
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def _queue_cfg(enabled: bool, publish: bool = False, queue_name: str = "q"):
    rabbitmq = SimpleNamespace(
        url="amqp://",
        queue_name=queue_name,
        prefetch=1,
        publish=publish,
    )
    timing = SimpleNamespace(
        poll_seconds=1,
        retry_delay_seconds=1,
        lock_timeout_seconds=1,
        max_attempts=3,
        cleanup_done_hours=1,
    )
    return SimpleNamespace(
        enabled=enabled,
        job_types=(),
        worker_id="worker",
        rabbitmq=rabbitmq,
        timing=timing,
    )


class FakeCursor:
    def execute(self, *args, **kwargs):
        return None

    def fetchone(self):
        return (1,)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def cursor(self, *args, **kwargs):
        return FakeCursor()


class TestPeriodicLoops(unittest.TestCase):
    def test_periodic_discovery_handles_exception(self) -> None:
        settings = _settings()
        with patch("src.services.main.discovery_pass", side_effect=RuntimeError("boom")), \
             patch("src.services.main.asyncio.sleep", side_effect=asyncio.CancelledError), \
             patch.object(main.logger, "exception") as log_exc:
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(main.periodic_discovery(object(), object(), settings))
        log_exc.assert_called_once()

    def test_parse_log_level_variants(self) -> None:
        self.assertEqual(main._parse_log_level(""), main.logging.INFO)
        self.assertEqual(main._parse_log_level("10"), 10)
        self.assertEqual(main._parse_log_level("unknown"), main.logging.INFO)

    def test_configure_logging_unknown_level_warns(self) -> None:
        with patch.dict(os.environ, {"LOG_LEVEL": "mystery"}), \
             patch("src.services.main.logging.basicConfig") as basic_config, \
             patch.object(main.logger, "warning") as log_warn:
            main.configure_logging()
        basic_config.assert_called_once()
        log_warn.assert_called_once()

    def test_parse_run_mode_default(self) -> None:
        self.assertEqual(main._parse_run_mode(""), "all")
        self.assertEqual(main._parse_run_mode("rest-only"), "rest")
        self.assertEqual(main._parse_run_mode("unknown"), "all")

    def test_periodic_backfill_builds_config(self) -> None:
        settings = _settings(
            backfill_event_statuses=("settled",),
            candle_minutes_for_hour=5,
            candle_minutes_for_day=30,
            candle_lookback_hours=4,
        )
        captured = {}

        def fake_backfill(_conn, _client, cfg, queue_cfg=None, publisher=None):
            captured["cfg"] = cfg
            raise RuntimeError("boom")

        with patch("src.services.main.backfill_pass", side_effect=fake_backfill), \
             patch("src.services.main.asyncio.sleep", side_effect=asyncio.CancelledError), \
             patch.object(main.logger, "exception"):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(main.periodic_backfill(object(), object(), settings))

        cfg = captured["cfg"]
        self.assertEqual(cfg.strike_periods, settings.strike_periods)
        self.assertEqual(cfg.event_statuses, settings.backfill_event_statuses)
        self.assertEqual(cfg.minutes_hour, settings.candle_minutes_for_hour)
        self.assertEqual(cfg.minutes_day, settings.candle_minutes_for_day)
        self.assertEqual(cfg.lookback_hours, settings.candle_lookback_hours)

    def test_periodic_closed_cleanup_handles_exception(self) -> None:
        settings = _settings()
        with patch("src.services.main.closed_cleanup_pass", side_effect=RuntimeError("boom")), \
             patch("src.services.main.asyncio.sleep", side_effect=asyncio.CancelledError), \
             patch.object(main.logger, "exception") as log_exc:
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(main.periodic_closed_cleanup(object(), object(), settings))
        log_exc.assert_called_once()


class TestRestLoops(unittest.TestCase):
    def test_rest_discovery_loop(self) -> None:
        settings = _settings()
        with patch("src.ingest.rest_loop.make_client", return_value="client"), \
             patch("src.ingest.rest_loop.psycopg.connect", return_value="conn"), \
             patch("src.ingest.rest_loop.maybe_init_schema") as init_schema, \
             patch("src.ingest.rest_loop.ensure_schema_compatible"), \
             patch("src.ingest.rest_loop.discovery_pass", side_effect=RuntimeError("boom")), \
             patch("src.ingest.rest_loop.time.sleep", side_effect=SystemExit), \
             patch.object(rest_loop.logger, "exception") as log_exc:
            with self.assertRaises(SystemExit):
                rest_loop.rest_discovery_loop(settings, "pem")
        init_schema.assert_called_once()
        log_exc.assert_called_once()

    def test_rest_backfill_loop_queue_publisher_failure(self) -> None:
        settings = _settings()
        queue_cfg = _queue_cfg(True, publish=True, queue_name="q")
        with patch("src.ingest.rest_loop.make_client", return_value="client"), \
             patch("src.ingest.rest_loop.psycopg.connect", return_value=FakeConn()), \
             patch("src.ingest.rest_loop.maybe_init_schema"), \
             patch("src.ingest.rest_loop.ensure_schema_compatible"), \
             patch("src.ingest.rest_loop.QueuePublisher", side_effect=RuntimeError("boom")), \
             patch("src.ingest.rest_loop.backfill_pass", side_effect=RuntimeError("fail")) as backfill_pass, \
             patch("src.ingest.rest_loop.time.sleep", side_effect=SystemExit), \
             patch.object(rest_loop.logger, "exception") as log_exc:
            with self.assertRaises(SystemExit):
                rest_loop.rest_backfill_loop(settings, "pem", queue_cfg)
        self.assertGreaterEqual(log_exc.call_count, 2)
        _, kwargs = backfill_pass.call_args
        self.assertIsNone(kwargs["publisher"])

    def test_rest_backfill_loop_queue_publisher_success(self) -> None:
        settings = _settings()
        queue_cfg = _queue_cfg(True, publish=True, queue_name="q")
        publisher = object()
        with patch("src.ingest.rest_loop.make_client", return_value="client"), \
             patch("src.ingest.rest_loop.psycopg.connect", return_value=FakeConn()), \
             patch("src.ingest.rest_loop.maybe_init_schema"), \
             patch("src.ingest.rest_loop.ensure_schema_compatible"), \
             patch("src.ingest.rest_loop.QueuePublisher", return_value=publisher), \
             patch("src.ingest.rest_loop.backfill_pass") as backfill_pass, \
             patch("src.ingest.rest_loop.time.sleep", side_effect=SystemExit), \
             patch.object(rest_loop.logger, "info") as log_info:
            with self.assertRaises(SystemExit):
                rest_loop.rest_backfill_loop(settings, "pem", queue_cfg)
        backfill_pass.assert_called_once()
        _, kwargs = backfill_pass.call_args
        self.assertIs(kwargs["publisher"], publisher)
        log_info.assert_any_call("Queue enabled: publishing to %s", queue_cfg.rabbitmq.queue_name)

    def test_rest_closed_cleanup_loop(self) -> None:
        settings = _settings()
        with patch("src.ingest.rest_loop.make_client", return_value="client"), \
             patch("src.ingest.rest_loop.psycopg.connect", return_value="conn"), \
             patch("src.ingest.rest_loop.maybe_init_schema"), \
             patch("src.ingest.rest_loop.ensure_schema_compatible"), \
             patch("src.ingest.rest_loop.closed_cleanup_pass", side_effect=RuntimeError("boom")), \
             patch("src.ingest.rest_loop.time.sleep", side_effect=SystemExit), \
             patch.object(rest_loop.logger, "exception") as log_exc:
            with self.assertRaises(SystemExit):
                rest_loop.rest_closed_cleanup_loop(settings, "pem")
        log_exc.assert_called_once()

    def test_rest_prediction_loop_handles_exception(self) -> None:
        prediction_cfg = SimpleNamespace(intervals=SimpleNamespace(poll_seconds=1))
        with patch("src.rag.rag_loop.psycopg.connect", return_value="conn"), \
             patch("src.rag.rag_loop.maybe_init_schema") as init_schema, \
             patch("src.rag.rag_loop.ensure_schema_compatible"), \
             patch("src.rag.rag_loop.resolve_prediction_handler", return_value="handler") as resolve_handler, \
             patch("src.rag.rag_loop.prediction_pass", side_effect=RuntimeError("boom")), \
             patch("src.rag.rag_loop.time.sleep", side_effect=SystemExit), \
             patch.object(rag_loop.logger, "exception") as log_exc:
            with self.assertRaises(SystemExit):
                rag_loop.rag_prediction_loop(prediction_cfg, "postgres://db")
        init_schema.assert_called_once()
        resolve_handler.assert_called_once_with(prediction_cfg)
        log_exc.assert_called_once()


class TestMainRunModes(unittest.IsolatedAsyncioTestCase):
    async def test_main_rag_disabled(self) -> None:
        with patch.dict(os.environ, {"KALSHI_RUN_MODE": "rag"}), \
             patch("src.services.main.load_dotenv"), \
             patch("src.services.main.configure_logging"), \
             patch("src.services.main.load_prediction_config", return_value=SimpleNamespace(enabled=False)), \
             patch.object(main.logger, "error") as log_error:
            await main.main()
        log_error.assert_called_once()

    async def test_main_rag_missing_db_url(self) -> None:
        with patch.dict(os.environ, {"KALSHI_RUN_MODE": "rag"}, clear=True), \
             patch("src.services.main.load_dotenv"), \
             patch("src.services.main.configure_logging"), \
             patch("src.services.main.load_prediction_config", return_value=SimpleNamespace(enabled=True)), \
             patch.object(main.logger, "error") as log_error:
            await main.main()
        log_error.assert_called_once()

    async def test_main_rag_runs_prediction_loop(self) -> None:
        prediction_cfg = SimpleNamespace(
            enabled=True,
            intervals=SimpleNamespace(poll_seconds=1),
        )
        to_thread = AsyncMock()
        with patch.dict(os.environ, {"KALSHI_RUN_MODE": "rag", "DATABASE_URL": "postgres://db"}), \
             patch("src.services.main.load_dotenv"), \
             patch("src.services.main.configure_logging"), \
             patch("src.services.main.load_prediction_config", return_value=prediction_cfg), \
             patch("src.services.main.asyncio.to_thread", to_thread):
            await main.main()
        to_thread.assert_awaited_once_with(main.rag_prediction_loop, prediction_cfg, "postgres://db")

    async def test_main_worker_runs_queue_worker(self) -> None:
        settings = _settings()
        queue_cfg = _queue_cfg(False)
        with patch.dict(os.environ, {"KALSHI_RUN_MODE": "worker"}), \
             patch("src.services.main.load_dotenv"), \
             patch("src.services.main.configure_logging"), \
             patch("src.services.main.load_settings", return_value=settings), \
             patch("src.services.main.load_queue_config", return_value=queue_cfg), \
             patch("builtins.open", mock_open(read_data="pem")), \
             patch("src.services.main.run_worker") as run_worker:
            await main.main()
        run_worker.assert_called_once_with(
            settings=settings,
            private_key_pem="pem",
            queue_cfg=queue_cfg,
            configure_log=False,
        )

    async def test_main_all_mode_tasks(self) -> None:
        settings = _settings()
        queue_cfg = _queue_cfg(False)
        to_thread = Mock(side_effect=["t1", "t2", "t3"])
        captured = {}

        async def fake_ws_loop(*_args, **_kwargs):
            return "ws_task"

        async def fake_gather(*tasks):
            captured["tasks"] = tasks
            for task in tasks:
                if inspect.isawaitable(task):
                    await task
            return None

        with patch.dict(os.environ, {"KALSHI_RUN_MODE": "all"}), \
             patch("src.services.main.load_dotenv"), \
             patch("src.services.main.configure_logging"), \
             patch("src.services.main.load_settings", return_value=settings), \
             patch("src.services.main.load_queue_config", return_value=queue_cfg), \
             patch("builtins.open", mock_open(read_data="pem")), \
             patch("src.services.main.asyncio.to_thread", to_thread), \
             patch("src.services.main.ws_loop", new=fake_ws_loop), \
             patch("src.services.main.psycopg.connect", return_value="conn"), \
             patch("src.services.main.maybe_init_schema"), \
             patch("src.services.main.ensure_schema_compatible"), \
             patch("src.services.main.asyncio.gather", new=fake_gather):
            await main.main()
        self.assertEqual(to_thread.call_count, 3)
        self.assertEqual(len(captured.get("tasks", [])), 4)

    async def test_main_all_mode_blocked_with_guardrails(self) -> None:
        with patch.dict(os.environ, {"KALSHI_RUN_MODE": "all", "SERVICE_GUARDRAILS": "1"}), \
             patch("src.services.main.load_dotenv"), \
             patch("src.services.main.configure_logging"), \
             patch.object(main.logger, "error") as log_error:
            await main.main()
        log_error.assert_called_once()

    async def test_main_unknown_run_mode_logs_warning(self) -> None:
        settings = _settings()
        queue_cfg = _queue_cfg(False)
        async def fake_ws_loop(*_args, **_kwargs):
            return "ws_task"

        async def fake_gather(*tasks):
            for task in tasks:
                if inspect.isawaitable(task):
                    await task
            return None

        with patch.dict(os.environ, {"KALSHI_RUN_MODE": "mystery"}), \
             patch("src.services.main.load_dotenv"), \
             patch("src.services.main.configure_logging"), \
             patch("src.services.main.load_settings", return_value=settings), \
             patch("src.services.main.load_queue_config", return_value=queue_cfg), \
             patch("builtins.open", mock_open(read_data="pem")), \
             patch("src.services.main.asyncio.to_thread", return_value="task"), \
             patch("src.services.main.ws_loop", new=fake_ws_loop), \
             patch("src.services.main.psycopg.connect", return_value="conn"), \
             patch("src.services.main.maybe_init_schema"), \
             patch("src.services.main.ensure_schema_compatible"), \
             patch("src.services.main.asyncio.gather", new=fake_gather), \
             patch.object(main.logger, "warning") as log_warn:
            await main.main()
        log_warn.assert_called_once()

    async def test_main_no_tasks_logs_error(self) -> None:
        settings = _settings()
        queue_cfg = _queue_cfg(False)
        with patch.dict(os.environ, {"KALSHI_RUN_MODE": "none"}), \
             patch("src.services.main.load_dotenv"), \
             patch("src.services.main.configure_logging"), \
             patch("src.services.main._parse_run_mode", return_value="none"), \
             patch("src.services.main.load_settings", return_value=settings), \
             patch("src.services.main.load_queue_config", return_value=queue_cfg), \
             patch("builtins.open", mock_open(read_data="pem")), \
             patch.object(main.logger, "error") as log_error:
            await main.main()
        log_error.assert_called_once()


class TestMainEntrypoint(unittest.TestCase):
    def test_module_entrypoint_invokes_asyncio_run(self) -> None:
        called = {}

        def fake_run(coro):
            called["called"] = True
            coro.close()

        prev = sys.modules.pop("src.services.main", None)
        try:
            with patch("asyncio.run", side_effect=fake_run):
                runpy.run_module("src.services.main", run_name="__main__")
        finally:
            if prev is not None:
                sys.modules["src.services.main"] = prev

        self.assertTrue(called.get("called", False))
