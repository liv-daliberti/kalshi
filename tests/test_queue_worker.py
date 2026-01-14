import os
import runpy
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import mock_open, patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

import src.queue.queue_worker as queue_worker


class StopLoop(Exception):
    pass


class DummyCursor:
    def __init__(self):
        self.executes = []

    def execute(self, sql, params=None):
        self.executes.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self):
        self.rollback_called = False
        self.commits = 0
        self.cursors = []

    def cursor(self):
        cursor = DummyCursor()
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollback_called = True


class FakeChannel:
    def __init__(self, responses, raise_on_basic_get=False):
        self.responses = list(responses)
        self.acked = []
        self.closed = False
        self.raise_on_basic_get = raise_on_basic_get

    def basic_get(self, queue, auto_ack):
        if self.raise_on_basic_get:
            raise RuntimeError("read failed")
        return self.responses.pop(0)

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class TestQueueWorkerHelpers(unittest.TestCase):
    def test_parse_log_level(self) -> None:
        self.assertEqual(queue_worker._parse_log_level("DEBUG"), queue_worker.logging.DEBUG)
        self.assertEqual(queue_worker._parse_log_level("15"), 15)
        self.assertEqual(queue_worker._parse_log_level("unknown"), queue_worker.logging.INFO)
        self.assertEqual(queue_worker._parse_log_level(""), queue_worker.logging.INFO)

    def test_configure_logging_unknown_level(self) -> None:
        with patch.dict(os.environ, {"LOG_LEVEL": "mystery"}), \
             patch("src.queue.queue_worker.logger.warning") as warn, \
             patch("src.queue.queue_worker.logging.basicConfig") as basic_config:
            queue_worker.configure_logging()
        self.assertEqual(basic_config.call_count, 1)
        warn.assert_called_once()


class TestQueueWorkerProcess(unittest.TestCase):
    def test_process_item_backfill(self) -> None:
        item = queue_worker.WorkItem(
            job_id=1,
            job_type="backfill_market",
            payload={"series_ticker": "SR1", "strike_period": "hour", "market": {"ticker": "M1"}},
            attempts=0,
            max_attempts=1,
        )
        cfg = queue_worker.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
        )
        with patch("src.queue.queue_worker.backfill_market", return_value=3) as backfill_market:
            result = queue_worker._process_item(object(), object(), cfg, item)
        self.assertEqual(result, 3)
        backfill_market.assert_called_once()

    def test_process_item_invalid_payload(self) -> None:
        item = queue_worker.WorkItem(
            job_id=2,
            job_type="backfill_market",
            payload={"series_ticker": "SR1", "strike_period": "hour"},
            attempts=0,
            max_attempts=1,
        )
        cfg = queue_worker.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
        )
        with self.assertRaises(ValueError):
            queue_worker._process_item(object(), object(), cfg, item)

    def test_process_item_unknown_job(self) -> None:
        item = queue_worker.WorkItem(
            job_id=3,
            job_type="unknown",
            payload={},
            attempts=0,
            max_attempts=1,
        )
        cfg = queue_worker.BackfillConfig(
            strike_periods=("hour",),
            event_statuses=("closed",),
            minutes_hour=1,
            minutes_day=60,
            lookback_hours=2,
        )
        with self.assertRaises(ValueError):
            queue_worker._process_item(object(), object(), cfg, item)


class TestQueueWorkerLoop(unittest.TestCase):
    def _queue_cfg(self, **overrides):
        enabled = overrides.pop("enabled", True)
        job_types = overrides.pop("job_types", ("backfill_market",))
        worker_id = overrides.pop("worker_id", "worker")
        rabbitmq = SimpleNamespace(
            url=overrides.pop("rabbitmq_url", "amqp://"),
            queue_name=overrides.pop("queue_name", "q"),
            prefetch=overrides.pop("prefetch", 1),
            publish=overrides.pop("publish", True),
        )
        timing = SimpleNamespace(
            poll_seconds=overrides.pop("poll_seconds", 1),
            retry_delay_seconds=overrides.pop("retry_delay_seconds", 1),
            lock_timeout_seconds=overrides.pop("lock_timeout_seconds", 1),
            max_attempts=overrides.pop("max_attempts", 3),
            cleanup_done_hours=overrides.pop("cleanup_done_hours", 1),
        )
        values = dict(
            enabled=enabled,
            job_types=job_types,
            worker_id=worker_id,
            rabbitmq=rabbitmq,
            timing=timing,
        )
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_run_worker_loop_rabbitmq_connect_fails(self) -> None:
        conn = FakeConn()
        cfg = self._queue_cfg()
        with patch("src.queue.queue_worker.open_rabbitmq_channel", side_effect=RuntimeError("boom")), \
             patch("src.queue.queue_worker.claim_next_job", return_value=None), \
             patch("src.queue.queue_worker.time.sleep", side_effect=StopLoop), \
             patch("src.queue.queue_worker.logger.exception") as log_exc:
            with self.assertRaises(StopLoop):
                queue_worker._run_worker_loop(conn, object(), object(), cfg)
        log_exc.assert_called_once()

    def test_run_worker_loop_rabbitmq_retry_closes_connection(self) -> None:
        conn = FakeConn()
        cfg = self._queue_cfg()
        connection = FakeConnection()
        with patch("src.queue.queue_worker.open_rabbitmq_channel", side_effect=[(connection, None), RuntimeError("boom")]), \
             patch("src.queue.queue_worker.claim_next_job", side_effect=[None, StopLoop]), \
             patch("src.queue.queue_worker.time.monotonic", side_effect=[0, 0, 0, 0, 0]), \
             patch("src.queue.queue_worker.time.sleep", return_value=None), \
             patch("src.queue.queue_worker.logger.exception") as log_exc:
            with self.assertRaises(StopLoop):
                queue_worker._run_worker_loop(conn, object(), object(), cfg)
        self.assertTrue(connection.closed)
        log_exc.assert_called()

    def test_run_worker_loop_invalid_job_id_payload(self) -> None:
        conn = FakeConn()
        cfg = self._queue_cfg()
        channel = FakeChannel(
            responses=[(SimpleNamespace(delivery_tag=1), None, b"bad"), (None, None, None)]
        )
        connection = FakeConnection()
        with patch("src.queue.queue_worker.open_rabbitmq_channel", return_value=(connection, channel)), \
             patch("src.queue.queue_worker.claim_next_job", return_value=None), \
             patch("src.queue.queue_worker.time.sleep", side_effect=StopLoop), \
             patch("src.queue.queue_worker.logger.warning") as log_warn:
            with self.assertRaises(StopLoop):
                queue_worker._run_worker_loop(conn, object(), object(), cfg)
        self.assertEqual(channel.acked, [1])
        log_warn.assert_called_once()

    def test_run_worker_loop_read_failure_reconnects(self) -> None:
        conn = FakeConn()
        cfg = self._queue_cfg()
        channel = FakeChannel(responses=[], raise_on_basic_get=True)
        connection = FakeConnection()
        with patch("src.queue.queue_worker.open_rabbitmq_channel", return_value=(connection, channel)), \
             patch("src.queue.queue_worker.time.sleep", side_effect=StopLoop), \
             patch("src.queue.queue_worker.logger.exception") as log_exc:
            with self.assertRaises(StopLoop):
                queue_worker._run_worker_loop(conn, object(), object(), cfg)
        self.assertTrue(channel.closed)
        self.assertTrue(connection.closed)
        log_exc.assert_called()

    def test_run_worker_loop_read_failure_close_errors_continue(self) -> None:
        class BadConnection(FakeConnection):
            def close(self):
                raise RuntimeError("close failed")

        class BadChannel(FakeChannel):
            def close(self):
                raise RuntimeError("close failed")

        conn = FakeConn()
        cfg = self._queue_cfg()
        channel = BadChannel(responses=[], raise_on_basic_get=True)
        connection = BadConnection()
        with patch(
            "src.queue.queue_worker.open_rabbitmq_channel",
            side_effect=[(connection, channel), (FakeConnection(), None)],
        ) as open_channel, \
             patch("src.queue.queue_worker.claim_next_job", side_effect=StopLoop), \
             patch("src.queue.queue_worker.time.sleep", return_value=None), \
             patch("src.queue.queue_worker.logger.exception") as log_exc:
            with self.assertRaises(StopLoop):
                queue_worker._run_worker_loop(conn, object(), object(), cfg)
        self.assertEqual(open_channel.call_count, 2)
        log_exc.assert_called()

    def test_run_worker_loop_connect_close_error(self) -> None:
        class BadConnection(FakeConnection):
            def close(self):
                raise RuntimeError("close failed")

        conn = FakeConn()
        cfg = self._queue_cfg()
        bad_connection = BadConnection()
        with patch(
            "src.queue.queue_worker.open_rabbitmq_channel",
            side_effect=[(bad_connection, None), RuntimeError("boom")],
        ), \
             patch("src.queue.queue_worker.claim_next_job", side_effect=[None, StopLoop]), \
             patch("src.queue.queue_worker.time.monotonic", side_effect=[0, 0, 0, 0, 0, 0]), \
             patch("src.queue.queue_worker.time.sleep", return_value=None), \
             patch("src.queue.queue_worker.logger.exception") as log_exc:
            with self.assertRaises(StopLoop):
                queue_worker._run_worker_loop(conn, object(), object(), cfg)
        log_exc.assert_called()

    def test_run_worker_loop_processes_job(self) -> None:
        conn = FakeConn()
        cfg = self._queue_cfg()
        item = queue_worker.WorkItem(
            job_id=5,
            job_type="backfill_market",
            payload={"series_ticker": "SR1", "strike_period": "hour", "market": {"ticker": "M1"}},
            attempts=0,
            max_attempts=1,
        )
        channel = FakeChannel(
            responses=[(SimpleNamespace(delivery_tag=2), None, b"5"), (None, None, None)]
        )
        connection = FakeConnection()
        with patch("src.queue.queue_worker.open_rabbitmq_channel", return_value=(connection, channel)), \
             patch("src.queue.queue_worker.claim_job_by_id", return_value=item) as claim_job, \
             patch("src.queue.queue_worker.claim_next_job", side_effect=StopLoop), \
             patch("src.queue.queue_worker._process_item") as process_item, \
             patch("src.queue.queue_worker.mark_done") as mark_done:
            with self.assertRaises(StopLoop):
                queue_worker._run_worker_loop(conn, object(), object(), cfg)
        claim_job.assert_called_once_with(conn, 5, cfg.worker_id, cfg.job_types)
        process_item.assert_called_once()
        mark_done.assert_called_once_with(conn, 5)
        self.assertEqual(channel.acked, [2])

    def test_run_worker_loop_marks_failed(self) -> None:
        conn = FakeConn()
        cfg = self._queue_cfg(publish=False)
        item = queue_worker.WorkItem(
            job_id=6,
            job_type="backfill_market",
            payload={"series_ticker": "SR1", "strike_period": "hour", "market": {"ticker": "M1"}},
            attempts=0,
            max_attempts=1,
        )
        with patch("src.queue.queue_worker.claim_next_job", side_effect=[item, StopLoop]), \
             patch("src.queue.queue_worker._process_item", side_effect=RuntimeError("boom")), \
             patch("src.queue.queue_worker.mark_failed") as mark_failed:
            with self.assertRaises(StopLoop):
                queue_worker._run_worker_loop(conn, object(), object(), cfg)
        self.assertTrue(conn.rollback_called)
        mark_failed.assert_called_once()

    def test_run_worker_loop_marks_failed_rollback_error(self) -> None:
        class BadConn(FakeConn):
            def rollback(self):
                raise RuntimeError("rollback failed")

        conn = BadConn()
        cfg = self._queue_cfg(publish=False)
        item = queue_worker.WorkItem(
            job_id=7,
            job_type="backfill_market",
            payload={"series_ticker": "SR1", "strike_period": "hour", "market": {"ticker": "M1"}},
            attempts=0,
            max_attempts=1,
        )
        with patch("src.queue.queue_worker.claim_next_job", side_effect=[item, StopLoop]), \
             patch("src.queue.queue_worker._process_item", side_effect=RuntimeError("boom")), \
             patch("src.queue.queue_worker.mark_failed") as mark_failed:
            with self.assertRaises(StopLoop):
                queue_worker._run_worker_loop(conn, object(), object(), cfg)
        mark_failed.assert_called_once()

    def test_run_worker_loop_maintenance(self) -> None:
        conn = FakeConn()
        cfg = self._queue_cfg(publish=False, poll_seconds=1)
        with patch("src.queue.queue_worker.claim_next_job", return_value=None), \
             patch("src.queue.queue_worker.requeue_stale_jobs", return_value=2) as requeue, \
             patch("src.queue.queue_worker.cleanup_finished_jobs", return_value=3) as cleanup, \
             patch("src.queue.queue_worker.logger.info") as log_info, \
             patch("src.queue.queue_worker.time.monotonic", side_effect=[0, 31, 31]), \
             patch("src.queue.queue_worker.time.sleep", side_effect=StopLoop):
            with self.assertRaises(StopLoop):
                queue_worker._run_worker_loop(conn, object(), object(), cfg)
        requeue.assert_called_once()
        cleanup.assert_called_once()
        self.assertGreaterEqual(log_info.call_count, 2)

    def test_run_worker_loop_sleep_continue(self) -> None:
        conn = FakeConn()
        cfg = self._queue_cfg(publish=False, poll_seconds=1)
        with patch("src.queue.queue_worker.claim_next_job", return_value=None), \
             patch("src.queue.queue_worker.time.monotonic", side_effect=[0, 0, 0, 0, 0]), \
             patch("src.queue.queue_worker.time.sleep", side_effect=[None, StopLoop]):
            with self.assertRaises(StopLoop):
                queue_worker._run_worker_loop(conn, object(), object(), cfg)


class TestRunWorker(unittest.TestCase):
    def _settings(self):
        return SimpleNamespace(
            kalshi_host="host",
            kalshi_api_key_id="key",
            kalshi_private_key_pem_path="key.pem",
            database_url="db",
            strike_periods=("hour",),
            backfill_event_statuses=("closed",),
            candle_minutes_for_hour=1,
            candle_minutes_for_day=60,
            candle_lookback_hours=2,
        )

    def _queue_cfg(self, enabled=True):
        rabbitmq = SimpleNamespace(
            url="amqp://",
            queue_name="q",
            prefetch=1,
            publish=False,
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
            job_types=("backfill_market",),
            worker_id="worker",
            rabbitmq=rabbitmq,
            timing=timing,
        )

    def test_run_worker_queue_disabled(self) -> None:
        with patch("src.queue.queue_worker.assert_service_role"), \
             patch("src.queue.queue_worker.load_settings", return_value=self._settings()), \
             patch("src.queue.queue_worker.load_queue_config", return_value=self._queue_cfg(False)), \
             patch("src.queue.queue_worker.logger.error") as log_error:
            queue_worker.run_worker(configure_log=False)
        log_error.assert_called_once()

    def test_run_worker_configure_logging(self) -> None:
        with patch("src.queue.queue_worker.assert_service_role"), \
             patch("src.queue.queue_worker.configure_logging") as configure_logging, \
             patch("src.queue.queue_worker.load_settings", return_value=self._settings()), \
             patch("src.queue.queue_worker.load_queue_config", return_value=self._queue_cfg(False)):
            queue_worker.run_worker(configure_log=True)
        configure_logging.assert_called_once()

    def test_run_worker_reads_key_and_runs_loop(self) -> None:
        settings = self._settings()
        queue_cfg = self._queue_cfg(True)
        conn = object()
        with patch("src.queue.queue_worker.assert_service_role"), \
             patch("src.queue.queue_worker.load_settings", return_value=settings), \
             patch("src.queue.queue_worker.load_queue_config", return_value=queue_cfg), \
             patch("builtins.open", mock_open(read_data="key")), \
             patch("src.queue.queue_worker.make_client", return_value=object()) as make_client, \
             patch("src.queue.queue_worker.psycopg.connect", return_value=conn) as connect, \
             patch("src.queue.queue_worker.maybe_init_schema") as init_schema, \
             patch("src.queue.queue_worker.ensure_schema_compatible"), \
             patch("src.queue.queue_worker._run_worker_loop") as run_loop:
            queue_worker.run_worker(configure_log=False)
        make_client.assert_called_once()
        connect.assert_called_once()
        init_schema.assert_called_once()
        run_loop.assert_called_once()

    def test_run_worker_private_key_provided(self) -> None:
        settings = self._settings()
        queue_cfg = self._queue_cfg(True)
        with patch("src.queue.queue_worker.assert_service_role"), \
             patch("src.queue.queue_worker.load_settings", return_value=settings), \
             patch("src.queue.queue_worker.load_queue_config", return_value=queue_cfg), \
             patch("builtins.open") as open_file, \
             patch("src.queue.queue_worker.make_client", return_value=object()), \
             patch("src.queue.queue_worker.psycopg.connect", return_value=object()), \
             patch("src.queue.queue_worker.maybe_init_schema"), \
             patch("src.queue.queue_worker.ensure_schema_compatible"), \
             patch("src.queue.queue_worker._run_worker_loop"):
            queue_worker.run_worker(configure_log=False, private_key_pem="pem")
        open_file.assert_not_called()

    def test_run_worker_main_invokes(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "db",
                "KALSHI_API_KEY_ID": "key",
                "KALSHI_PRIVATE_KEY_PEM_PATH": "key.pem",
                "WORK_QUEUE_ENABLE": "0",
            },
            clear=True,
        ):
            prev = sys.modules.pop("src.queue.queue_worker", None)
            try:
                runpy.run_module("src.queue.queue_worker", run_name="__main__")
            finally:
                if prev is not None:
                    sys.modules["src.queue.queue_worker"] = prev
