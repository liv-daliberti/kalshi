import os
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

work_queue = importlib.import_module("src.queue.work_queue")


class FakeCursor:
    def __init__(self, conn, fail_on_notify=False):
        self.conn = conn
        self.fail_on_notify = fail_on_notify

    def execute(self, sql, params=None):
        if self.fail_on_notify and sql.strip().startswith("SELECT pg_notify"):
            raise RuntimeError("notify failed")
        self.conn.executes.append((sql, params))

    def fetchone(self):
        if self.conn.fetchone_queue:
            return self.conn.fetchone_queue.pop(0)
        return None

    def fetchall(self):
        if self.conn.fetchall_queue:
            return self.conn.fetchall_queue.pop(0)
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, fetchone_queue=None, fetchall_queue=None, fail_on_notify=False):
        self.executes = []
        self.commits = 0
        self.fetchone_queue = list(fetchone_queue or [])
        self.fetchall_queue = list(fetchall_queue or [])
        self.fail_on_notify = fail_on_notify

    def cursor(self):
        return FakeCursor(self, fail_on_notify=self.fail_on_notify)

    def commit(self):
        self.commits += 1


class FakeChannel:
    def __init__(self):
        self.declares = []
        self.qos = []
        self.publishes = []

    def queue_declare(self, queue, durable):
        self.declares.append((queue, durable))

    def basic_qos(self, prefetch_count):
        self.qos.append(prefetch_count)

    def basic_publish(self, exchange, routing_key, body, properties):
        self.publishes.append((exchange, routing_key, body, properties))


class FakeConnection:
    def __init__(self, params=None, raise_on_close=False):
        self.params = params
        self.raise_on_close = raise_on_close
        self.channel_obj = FakeChannel()
        self.is_open = True
        self.closed = False

    def channel(self):
        return self.channel_obj

    def close(self):
        if self.raise_on_close:
            raise RuntimeError("close failed")
        self.is_open = False
        self.closed = True


class FakePikaModule:
    def __init__(self, connection_factory):
        self._connection_factory = connection_factory
        self.BasicProperties = SimpleNamespace

    def URLParameters(self, url):
        return {"url": url}

    def BlockingConnection(self, params):
        return self._connection_factory(params)


class TestQueueParsing(unittest.TestCase):
    def test_job_type_where_clause_empty(self) -> None:
        clause, params = work_queue.job_type_where_clause(None)
        self.assertEqual(clause, "")
        self.assertEqual(params, ())
        clause, params = work_queue.job_type_where_clause(())
        self.assertEqual(clause, "")
        self.assertEqual(params, ())

    def test_parse_bool(self) -> None:
        self.assertFalse(work_queue._parse_bool(None))
        self.assertFalse(work_queue._parse_bool(""))
        self.assertTrue(work_queue._parse_bool(" yes "))
        self.assertFalse(work_queue._parse_bool("0"))

    def test_parse_int(self) -> None:
        self.assertEqual(work_queue._parse_int(None, 3), 3)
        self.assertEqual(work_queue._parse_int("10", 3), 10)
        self.assertEqual(work_queue._parse_int("nope", 3), 3)

    def test_parse_csv(self) -> None:
        self.assertEqual(work_queue._parse_csv(None), ())
        self.assertEqual(work_queue._parse_csv("A, b,, C "), ("a", "b", "c"))

    def test_default_worker_id(self) -> None:
        with patch("src.queue.work_queue.socket.gethostname", return_value="host"), \
             patch("src.queue.work_queue.os.getpid", return_value=123):
            self.assertEqual(work_queue._default_worker_id(), "host-123")

    def test_queue_name_for_job_type_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            name = work_queue.queue_name_for_job_type("", "queue.default")
        self.assertEqual(name, "queue.default")

    def test_default_queue_for_job_types_backfill_override(self) -> None:
        with patch.dict(
            os.environ,
            {"WORK_QUEUE_NAME_BACKFILL_MARKET": "queue.backfill"},
            clear=True,
        ):
            name = work_queue._default_queue_for_job_types(
                ("backfill_market", "discover_market")
            )
        self.assertEqual(name, "queue.backfill")

    def test_default_queue_for_job_types_backfill_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            name = work_queue._default_queue_for_job_types(
                ("backfill_market", "discover_market")
            )
        self.assertEqual(name, "kalshi.backfill")

    def test_default_queue_for_job_types_cleanup(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            name = work_queue._default_queue_for_job_types(("cleanup_market",))
        self.assertEqual(name, "kalshi.cleanup")

    def test_default_queue_for_job_types_single_override(self) -> None:
        with patch.dict(
            os.environ,
            {"WORK_QUEUE_NAME_CLEANUP_MARKET": "queue.cleanup"},
            clear=True,
        ):
            name = work_queue._default_queue_for_job_types(("cleanup_market",))
        self.assertEqual(name, "queue.cleanup")

    def test_default_queue_for_job_types_fallback(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            name = work_queue._default_queue_for_job_types(("other_job",))
        self.assertEqual(name, "kalshi.ingest")

    def test_load_queue_config_env(self) -> None:
        env = {
            "WORK_QUEUE_ENABLE": "1",
            "RABBITMQ_URL": "amqp://test",
            "WORK_QUEUE_NAME": "queue",
            "WORK_QUEUE_JOB_TYPES": "backfill_market,cleanup_market",
            "WORK_QUEUE_PREFETCH": "2",
            "WORK_QUEUE_POLL_SECONDS": "7",
            "WORK_QUEUE_RETRY_SECONDS": "9",
            "WORK_QUEUE_LOCK_TIMEOUT_SECONDS": "11",
            "WORK_QUEUE_MAX_ATTEMPTS": "4",
            "WORK_QUEUE_WORKER_ID": "worker",
            "WORK_QUEUE_CLEANUP_HOURS": "5",
            "WORK_QUEUE_PUBLISH": "0",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = work_queue.load_queue_config()
        self.assertTrue(cfg.enabled)
        self.assertEqual(cfg.rabbitmq.url, "amqp://test")
        self.assertEqual(cfg.rabbitmq.queue_name, "queue")
        self.assertEqual(cfg.job_types, ("backfill_market", "cleanup_market"))
        self.assertEqual(cfg.rabbitmq.prefetch, 2)
        self.assertEqual(cfg.timing.poll_seconds, 7)
        self.assertEqual(cfg.timing.retry_delay_seconds, 9)
        self.assertEqual(cfg.timing.lock_timeout_seconds, 11)
        self.assertEqual(cfg.timing.max_attempts, 4)
        self.assertEqual(cfg.worker_id, "worker")
        self.assertEqual(cfg.timing.cleanup_done_hours, 5)
        self.assertFalse(cfg.rabbitmq.publish)

    def test_parse_payload(self) -> None:
        self.assertEqual(work_queue._parse_payload(None), {})
        payload = {"a": 1}
        self.assertEqual(work_queue._parse_payload(payload), payload)
        self.assertEqual(work_queue._parse_payload('{"b":2}'), {"b": 2})
        self.assertEqual(work_queue._parse_payload("nope"), {})
        self.assertEqual(work_queue._parse_payload([("c", 3)]), {"c": 3})


class TestRabbitMqPublisher(unittest.TestCase):
    def _cfg(self, **overrides):
        enabled = overrides.pop("enabled", True)
        job_types = overrides.pop("job_types", ("backfill_market",))
        worker_id = overrides.pop("worker_id", "worker")
        rabbitmq = work_queue.QueueRabbitMQConfig(
            url=overrides.pop("rabbitmq_url", "amqp://"),
            queue_name=overrides.pop("queue_name", "queue"),
            prefetch=overrides.pop("prefetch", 1),
            publish=overrides.pop("publish", True),
        )
        timing = work_queue.QueueTimingConfig(
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
        return work_queue.QueueConfig(**values)

    def test_queue_publisher_requires_pika(self) -> None:
        cfg = self._cfg()
        with patch("src.queue.work_queue.PIKA", None):
            with self.assertRaises(RuntimeError):
                work_queue.QueuePublisher(cfg)

    def test_queue_publisher_publish_and_close(self) -> None:
        cfg = self._cfg()
        connection = FakeConnection()

        def connection_factory(params):
            connection.params = params
            return connection

        fake_pika = FakePikaModule(connection_factory)
        with patch("src.queue.work_queue.PIKA", fake_pika):
            publisher = work_queue.QueuePublisher(cfg)
            publisher.publish(7, job_type="backfill_market")
            publisher.disable()
            publisher.publish(8, job_type="backfill_market")
            with publisher as context:
                self.assertIs(context, publisher)
            self.assertTrue(connection.closed)

        self.assertEqual(connection.channel_obj.declares, [("queue", True)])
        self.assertEqual(len(connection.channel_obj.publishes), 1)
        publish = connection.channel_obj.publishes[0]
        self.assertEqual(publish[0], "")
        self.assertEqual(publish[1], "queue")
        self.assertEqual(publish[2], b"7")

    def test_queue_publisher_close_logs_exception(self) -> None:
        cfg = self._cfg()
        connection = FakeConnection(raise_on_close=True)

        def connection_factory(params):
            connection.params = params
            return connection

        fake_pika = FakePikaModule(connection_factory)
        with patch("src.queue.work_queue.PIKA", fake_pika), \
             patch("src.queue.work_queue.logger.exception") as logger_exception:
            publisher = work_queue.QueuePublisher(cfg)
            publisher.close()
        logger_exception.assert_called_once()

    def test_open_rabbitmq_channel(self) -> None:
        cfg = self._cfg(prefetch=0)
        connection = FakeConnection()

        def connection_factory(params):
            connection.params = params
            return connection

        fake_pika = FakePikaModule(connection_factory)
        with patch("src.queue.work_queue.PIKA", fake_pika):
            conn, channel = work_queue.open_rabbitmq_channel(cfg)
        self.assertIs(conn, connection)
        self.assertIs(channel, connection.channel_obj)
        self.assertEqual(channel.declares, [("queue", True)])
        self.assertEqual(channel.qos, [1])

    def test_open_rabbitmq_channel_requires_pika(self) -> None:
        cfg = self._cfg()
        with patch("src.queue.work_queue.PIKA", None):
            with self.assertRaises(RuntimeError):
                work_queue.open_rabbitmq_channel(cfg)

    def test_queue_name_for_job_type_override(self) -> None:
        with patch.dict(
            os.environ,
            {"WORK_QUEUE_NAME_BACKFILL_MARKET": "queue.backfill"},
            clear=True,
        ):
            name = work_queue.queue_name_for_job_type("backfill_market", "queue.default")
        self.assertEqual(name, "queue.backfill")


class TestQueueDatabaseOps(unittest.TestCase):
    def test_enqueue_job_defaults_and_notify(self) -> None:
        conn = FakeConn(fetchone_queue=[(7,)])
        job_id = work_queue.enqueue_job(conn, "job", {"a": 1})
        self.assertEqual(job_id, 7)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.executes), 2)
        insert_params = conn.executes[0][1]
        self.assertEqual(insert_params[0], "job")
        self.assertIn('"a":1', insert_params[1])
        self.assertEqual(insert_params[3], 5)
        notify_sql, notify_params = conn.executes[1]
        self.assertTrue(notify_sql.strip().startswith("SELECT pg_notify"))
        self.assertEqual(notify_params[0], "work_queue_update")
        self.assertIn('"job_id":7', notify_params[1])

    def test_enqueue_job_notify_failure(self) -> None:
        conn = FakeConn(fetchone_queue=[(2,)], fail_on_notify=True)
        job_id = work_queue.enqueue_job(conn, "job", {"a": 1})
        self.assertEqual(job_id, 2)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(len(conn.executes), 1)

    def test_claim_job_by_id(self) -> None:
        conn = FakeConn(fetchone_queue=[(5, "type", '{"a": 1}', 2, 3)])
        item = work_queue.claim_job_by_id(conn, 5, "worker")
        self.assertIsNotNone(item)
        self.assertEqual(item.job_id, 5)
        self.assertEqual(item.payload, {"a": 1})
        self.assertEqual(item.attempts, 2)
        self.assertEqual(item.max_attempts, 3)

    def test_claim_job_by_id_with_job_types(self) -> None:
        conn = FakeConn(fetchone_queue=[(5, "type", {"a": 1}, 2, 3)])
        item = work_queue.claim_job_by_id(conn, 5, "worker", job_types=("a", "b"))
        self.assertIsNotNone(item)
        sql, params = conn.executes[0]
        self.assertIn("job_type = ANY", sql)
        self.assertEqual(params[-1], ["a", "b"])

    def test_claim_job_by_id_empty(self) -> None:
        conn = FakeConn(fetchone_queue=[None])
        self.assertIsNone(work_queue.claim_job_by_id(conn, 1, "worker"))

    def test_claim_next_job(self) -> None:
        conn = FakeConn(fetchone_queue=[(3, "type", {"a": 1}, 1, 4)])
        item = work_queue.claim_next_job(conn, "worker")
        self.assertIsNotNone(item)
        self.assertEqual(item.payload, {"a": 1})

    def test_claim_next_job_with_job_types(self) -> None:
        conn = FakeConn(fetchone_queue=[(3, "type", {"a": 1}, 1, 4)])
        item = work_queue.claim_next_job(conn, "worker", job_types=("x",))
        self.assertIsNotNone(item)
        sql, params = conn.executes[0]
        self.assertIn("job_type = ANY", sql)
        self.assertEqual(params[-1], ["x"])

    def test_claim_next_job_empty(self) -> None:
        conn = FakeConn(fetchone_queue=[None])
        self.assertIsNone(work_queue.claim_next_job(conn, "worker"))
        self.assertEqual(conn.commits, 1)

    def test_mark_done(self) -> None:
        conn = FakeConn()
        work_queue.mark_done(conn, 5)
        self.assertEqual(conn.commits, 1)
        self.assertIn("UPDATE work_queue", conn.executes[0][0])

    def test_mark_failed_failed_status(self) -> None:
        conn = FakeConn()
        long_error = "x" * 2100
        item = work_queue.WorkItem(job_id=1, job_type="job", payload={}, attempts=3, max_attempts=3)
        work_queue.mark_failed(conn, item, RuntimeError(long_error), 5)
        self.assertEqual(conn.commits, 1)
        params = conn.executes[0][1]
        self.assertEqual(params[0], "failed")
        self.assertIsNone(params[1])
        self.assertTrue(params[2].endswith("..."))
        self.assertEqual(len(params[2]), 2003)

    def test_mark_failed_pending_status(self) -> None:
        conn = FakeConn()
        item = work_queue.WorkItem(job_id=1, job_type="job", payload={}, attempts=2, max_attempts=5)
        before = datetime.now(timezone.utc)
        work_queue.mark_failed(conn, item, RuntimeError("err"), 5)
        params = conn.executes[0][1]
        self.assertEqual(params[0], "pending")
        self.assertIsNotNone(params[1])
        self.assertGreater(params[1], before)
        self.assertEqual(params[2], "err")

    def test_requeue_stale_jobs_skips(self) -> None:
        conn = FakeConn()
        self.assertEqual(work_queue.requeue_stale_jobs(conn, 0), 0)
        self.assertEqual(conn.executes, [])

    def test_requeue_stale_jobs(self) -> None:
        conn = FakeConn(fetchall_queue=[[(1,), (2,)]])
        count = work_queue.requeue_stale_jobs(conn, 5)
        self.assertEqual(count, 2)
        self.assertEqual(conn.commits, 1)

    def test_cleanup_finished_jobs_skips(self) -> None:
        conn = FakeConn()
        self.assertEqual(work_queue.cleanup_finished_jobs(conn, 0), 0)
        self.assertEqual(conn.executes, [])

    def test_cleanup_finished_jobs(self) -> None:
        conn = FakeConn(fetchall_queue=[[(1,), (2,), (3,)]])
        count = work_queue.cleanup_finished_jobs(conn, 1)
        self.assertEqual(count, 3)
        self.assertEqual(conn.commits, 1)
