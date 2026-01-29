import importlib
import unittest
from unittest.mock import Mock, patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

job_utils = importlib.import_module("src.jobs.job_utils")


class TestUpsertEventWithErrors(unittest.TestCase):
    def test_upsert_event_with_errors_returns_false(self) -> None:
        logger = Mock()
        rollback = Mock()
        event = {"event_ticker": "EV1"}

        with patch("src.db.db.upsert_event", side_effect=RuntimeError("boom")), \
             patch("src.jobs.job_utils.handle_event_upsert_error", return_value=False) as handle:
            result = job_utils.upsert_event_with_errors(
                object(),
                event,
                logger=logger,
                metric_name="metric",
                rollback=rollback,
                source="test",
            )

        self.assertFalse(result)
        handle.assert_called_once()

    def test_upsert_event_with_errors_success(self) -> None:
        logger = Mock()
        rollback = Mock()
        event = {"event_ticker": "EV3"}

        with patch("src.db.db.upsert_event") as upsert_event, \
             patch("src.jobs.job_utils.handle_event_upsert_error") as handle:
            result = job_utils.upsert_event_with_errors(
                object(),
                event,
                logger=logger,
                metric_name="metric",
                rollback=rollback,
                source="test",
            )

        self.assertTrue(result)
        upsert_event.assert_called_once()
        handle.assert_not_called()

    def test_upsert_event_with_errors_non_dict_event(self) -> None:
        logger = Mock()
        rollback = Mock()
        event = ["EV4"]
        conn = object()

        with patch("src.db.db.upsert_event") as upsert_event:
            result = job_utils.upsert_event_with_errors(
                conn,
                event,
                logger=logger,
                metric_name="metric",
                rollback=rollback,
                source="test",
            )

        self.assertTrue(result)
        upsert_event.assert_called_once_with(conn, event)

    def test_upsert_event_with_errors_reraises(self) -> None:
        logger = Mock()
        rollback = Mock()
        event = {"event_ticker": "EV2"}

        with patch("src.db.db.upsert_event", side_effect=RuntimeError("boom")), \
             patch("src.jobs.job_utils.handle_event_upsert_error", return_value=True):
            with self.assertRaises(RuntimeError):
                job_utils.upsert_event_with_errors(
                    object(),
                    event,
                    logger=logger,
                    metric_name="metric",
                    rollback=rollback,
                    source="test",
                )


class TestHandleEventUpsertError(unittest.TestCase):
    def test_handle_event_upsert_error_logs_and_returns(self) -> None:
        logger = Mock()
        rollback = Mock()
        conn = object()
        exc = RuntimeError("boom")

        with patch("src.jobs.job_utils.log_item_error_and_should_raise", return_value=False) as should_raise:
            result = job_utils.handle_event_upsert_error(
                logger=logger,
                metric_name="metric",
                event_ticker="EV4",
                rollback=rollback,
                conn=conn,
                exc=exc,
                source="source",
            )

        self.assertFalse(result)
        logger.exception.assert_called_once_with(
            "%s: event upsert failed event_ticker=%s",
            "source",
            "EV4",
        )
        args = should_raise.call_args[0]
        self.assertIs(args[0], logger)
        self.assertEqual(args[1], "metric")
        ctx = args[2]
        self.assertEqual(ctx.kind, "event_upsert")
        self.assertEqual(ctx.event_ticker, "EV4")
        self.assertIsNone(ctx.market_ticker)
        self.assertIs(ctx.rollback, rollback)
        self.assertIs(ctx.conn, conn)
        self.assertIs(args[3], exc)


class TestSafeRollback(unittest.TestCase):
    def test_safe_rollback_closed_conn(self) -> None:
        class ClosedConn:
            closed = True

            def rollback(self):
                raise RuntimeError("should not be called")

        logger = Mock()
        job_utils.safe_rollback(ClosedConn(), logger=logger, label="closed")
        logger.warning.assert_not_called()
