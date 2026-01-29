import unittest
from types import SimpleNamespace
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

rest_loop = importlib.import_module("src.ingest.rest_loop")
psycopg = importlib.import_module("psycopg")


def _settings(**overrides):
    base = SimpleNamespace(
        strike_periods=("hour",),
        discovery_event_statuses=("open",),
        discovery_seconds=1.0,
        backfill_seconds=2.0,
        closed_cleanup_seconds=3.0,
        kalshi_host="host",
        kalshi_api_key_id="key",
        database_url="postgres://db",
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


class _RollbackConn:
    def __init__(self, raises: bool = False):
        self.raises = raises
        self.calls = 0

    def rollback(self):
        self.calls += 1
        if self.raises:
            raise RuntimeError("boom")


class _CtxConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestRestLoopHelpers(unittest.TestCase):
    def test_safe_rollback_none(self) -> None:
        rest_loop._safe_rollback(None)

    def test_safe_rollback_warns_on_exception(self) -> None:
        conn = _RollbackConn(raises=True)
        with patch.object(rest_loop.logger, "warning") as log_warn:
            rest_loop._safe_rollback(conn)
        log_warn.assert_called_once()
        self.assertEqual(conn.calls, 1)

    def test_should_reconnect(self) -> None:
        self.assertTrue(rest_loop._should_reconnect(psycopg.OperationalError("boom")))
        self.assertFalse(rest_loop._should_reconnect(RuntimeError("nope")))

    def test_connect_rest_resources_success(self) -> None:
        settings = _settings()
        with patch("src.ingest.rest_loop.make_client", return_value="client") as make_client, \
            patch("src.ingest.rest_loop.psycopg.connect", return_value="conn") as connect, \
            patch("src.ingest.rest_loop.maybe_init_schema") as init_schema, \
            patch("src.ingest.rest_loop.ensure_schema_compatible") as ensure_schema:
            client, conn = rest_loop._connect_rest_resources(settings, "pem")
        self.assertEqual(client, "client")
        self.assertEqual(conn, "conn")
        make_client.assert_called_once()
        connect.assert_called_once()
        init_schema.assert_called_once()
        ensure_schema.assert_called_once()

    def test_connect_rest_resources_retry_then_success(self) -> None:
        settings = _settings()
        with patch("src.ingest.rest_loop.make_client", side_effect=[RuntimeError("boom"), "client"]), \
            patch("src.ingest.rest_loop.psycopg.connect", return_value="conn"), \
            patch("src.ingest.rest_loop.maybe_init_schema"), \
            patch("src.ingest.rest_loop.ensure_schema_compatible"), \
            patch("src.ingest.rest_loop.safe_close") as safe_close, \
            patch("src.ingest.rest_loop.random.uniform", return_value=0.0), \
            patch("src.ingest.rest_loop.time.sleep") as sleep:
            client, conn = rest_loop._connect_rest_resources(settings, "pem")
        self.assertEqual(client, "client")
        self.assertEqual(conn, "conn")
        safe_close.assert_called_once()
        sleep.assert_called_once()


class TestRestLoops(unittest.TestCase):
    def test_rest_discovery_loop_success(self) -> None:
        settings = _settings()
        with patch("src.ingest.rest_loop._connect_rest_resources", return_value=("client", "conn")), \
            patch("src.ingest.rest_loop.discovery_pass", return_value=(1, 2, 3)), \
            patch("src.ingest.rest_loop.log_metric") as log_metric, \
            patch("src.ingest.rest_loop.time.monotonic", side_effect=[10.0, 11.0, 11.5]), \
            patch("src.ingest.rest_loop.time.sleep", side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                rest_loop.rest_discovery_loop(settings, "pem")
        log_metric.assert_called_once()

    def test_rest_discovery_loop_reconnect_handle_exception_true(self) -> None:
        settings = _settings()
        with patch("src.ingest.rest_loop._connect_rest_resources", side_effect=[("client", "conn"), SystemExit]), \
            patch("src.ingest.rest_loop.discovery_pass", side_effect=psycopg.OperationalError("boom")), \
            patch("src.ingest.rest_loop.safe_close") as safe_close, \
            patch.object(rest_loop.LoopFailureContext, "handle_exception", return_value=True):
            with self.assertRaises(SystemExit):
                rest_loop.rest_discovery_loop(settings, "pem")
        safe_close.assert_called_once()

    def test_rest_discovery_loop_reconnect_handle_exception_false(self) -> None:
        settings = _settings()
        with patch("src.ingest.rest_loop._connect_rest_resources", side_effect=[("client", "conn"), SystemExit]), \
            patch("src.ingest.rest_loop.discovery_pass", side_effect=psycopg.OperationalError("boom")), \
            patch("src.ingest.rest_loop.safe_close") as safe_close, \
            patch.object(rest_loop.LoopFailureContext, "handle_exception", return_value=False):
            with self.assertRaises(SystemExit):
                rest_loop.rest_discovery_loop(settings, "pem")
        safe_close.assert_called_once()

    def test_rest_backfill_loop_reconnect_handle_exception_true(self) -> None:
        settings = _settings()
        queue_cfg = SimpleNamespace(enabled=False, rabbitmq=SimpleNamespace(publish=False))
        with patch("src.ingest.rest_loop._connect_rest_resources", side_effect=[("client", "conn"), SystemExit]), \
            patch("src.ingest.rest_loop.build_backfill_config_from_settings", return_value="cfg"), \
            patch("src.ingest.rest_loop.backfill_pass", side_effect=psycopg.OperationalError("boom")), \
            patch("src.ingest.rest_loop.safe_close") as safe_close, \
            patch.object(rest_loop.LoopFailureContext, "handle_exception", return_value=True):
            with self.assertRaises(SystemExit):
                rest_loop.rest_backfill_loop(settings, "pem", queue_cfg)
        safe_close.assert_called_once()

    def test_rest_backfill_loop_reconnect_handle_exception_false(self) -> None:
        settings = _settings()
        queue_cfg = SimpleNamespace(enabled=False, rabbitmq=SimpleNamespace(publish=False))
        with patch("src.ingest.rest_loop._connect_rest_resources", side_effect=[("client", "conn"), SystemExit]), \
            patch("src.ingest.rest_loop.build_backfill_config_from_settings", return_value="cfg"), \
            patch("src.ingest.rest_loop.backfill_pass", side_effect=psycopg.OperationalError("boom")), \
            patch("src.ingest.rest_loop.safe_close") as safe_close, \
            patch.object(rest_loop.LoopFailureContext, "handle_exception", return_value=False):
            with self.assertRaises(SystemExit):
                rest_loop.rest_backfill_loop(settings, "pem", queue_cfg)
        safe_close.assert_called_once()

    def test_maybe_archive_closed_success(self) -> None:
        settings = _settings(backup_database_url="postgres://backup")
        stats = SimpleNamespace(events=5, markets=7)
        with patch("src.ingest.rest_loop.build_archive_closed_config", return_value="cfg"), \
            patch("src.ingest.rest_loop.psycopg.connect", return_value=_CtxConn()), \
            patch("src.ingest.rest_loop.archive_closed_events", return_value=stats), \
            patch("src.ingest.rest_loop.log_metric") as log_metric:
            rest_loop._maybe_archive_closed("conn", settings)
        log_metric.assert_called_once()

    def test_maybe_archive_closed_no_backup(self) -> None:
        settings = _settings()
        with patch("src.ingest.rest_loop.build_archive_closed_config") as build_cfg:
            rest_loop._maybe_archive_closed("conn", settings)
        build_cfg.assert_not_called()

    def test_maybe_archive_closed_exception(self) -> None:
        settings = _settings(backup_database_url="postgres://backup")
        with patch("src.ingest.rest_loop.psycopg.connect", side_effect=RuntimeError("boom")), \
            patch.object(rest_loop.logger, "exception") as log_exc:
            rest_loop._maybe_archive_closed("conn", settings)
        log_exc.assert_called_once()

    def test_rest_closed_cleanup_loop_success(self) -> None:
        settings = _settings()
        with patch("src.ingest.rest_loop._connect_rest_resources", return_value=("client", "conn")), \
            patch("src.ingest.rest_loop.build_closed_cleanup_config", return_value="cfg"), \
            patch("src.ingest.rest_loop.closed_cleanup_pass", return_value=(1, 2, 3)), \
            patch("src.ingest.rest_loop.log_metric") as log_metric, \
            patch("src.ingest.rest_loop._maybe_archive_closed") as archive, \
            patch("src.ingest.rest_loop.time.monotonic", side_effect=[20.0, 21.0]), \
            patch("src.ingest.rest_loop.time.sleep", side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                rest_loop.rest_closed_cleanup_loop(settings, "pem")
        log_metric.assert_called_once()
        archive.assert_called_once()

    def test_rest_closed_cleanup_loop_reconnect_handle_exception_true(self) -> None:
        settings = _settings()
        with patch("src.ingest.rest_loop._connect_rest_resources", side_effect=[("client", "conn"), SystemExit]), \
            patch("src.ingest.rest_loop.build_closed_cleanup_config", return_value="cfg"), \
            patch("src.ingest.rest_loop.closed_cleanup_pass", side_effect=psycopg.OperationalError("boom")), \
            patch("src.ingest.rest_loop.safe_close") as safe_close, \
            patch.object(rest_loop.LoopFailureContext, "handle_exception", return_value=True):
            with self.assertRaises(SystemExit):
                rest_loop.rest_closed_cleanup_loop(settings, "pem")
        safe_close.assert_called_once()

    def test_rest_closed_cleanup_loop_reconnect_handle_exception_false(self) -> None:
        settings = _settings()
        with patch("src.ingest.rest_loop._connect_rest_resources", side_effect=[("client", "conn"), SystemExit]), \
            patch("src.ingest.rest_loop.build_closed_cleanup_config", return_value="cfg"), \
            patch("src.ingest.rest_loop.closed_cleanup_pass", side_effect=psycopg.OperationalError("boom")), \
            patch("src.ingest.rest_loop.safe_close") as safe_close, \
            patch.object(rest_loop.LoopFailureContext, "handle_exception", return_value=False):
            with self.assertRaises(SystemExit):
                rest_loop.rest_closed_cleanup_loop(settings, "pem")
        safe_close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
