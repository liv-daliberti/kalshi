import os
import tempfile
import unittest
from unittest.mock import Mock, patch
import importlib

from _test_utils import add_src_to_path

add_src_to_path()

loop_utils = importlib.import_module("src.core.loop_utils")


class TestMaybeCircuitBreak(unittest.TestCase):
    def test_threshold_disabled(self) -> None:
        logger = Mock()
        with patch("src.core.loop_utils.time.sleep") as sleep:
            tripped = loop_utils.maybe_circuit_break(
                logger,
                "loop",
                failures=5,
                threshold=0,
                cooldown=1.0,
            )
        self.assertFalse(tripped)
        logger.warning.assert_not_called()
        sleep.assert_not_called()

    def test_under_threshold(self) -> None:
        logger = Mock()
        with patch("src.core.loop_utils.time.sleep") as sleep:
            tripped = loop_utils.maybe_circuit_break(
                logger,
                "loop",
                failures=1,
                threshold=2,
                cooldown=2.5,
            )
        self.assertFalse(tripped)
        logger.warning.assert_not_called()
        sleep.assert_not_called()

    def test_trips_and_sleeps(self) -> None:
        logger = Mock()
        with patch("src.core.loop_utils.time.sleep") as sleep:
            tripped = loop_utils.maybe_circuit_break(
                logger,
                "loop",
                failures=3,
                threshold=2,
                cooldown=1.5,
            )
        self.assertTrue(tripped)
        logger.warning.assert_called_once()
        sleep.assert_called_once_with(1.5)


class TestLoopFailureContext(unittest.TestCase):
    def test_record_success_logs_and_resets(self) -> None:
        logger = Mock()
        context = loop_utils.LoopFailureContext(
            name="loop",
            failure_threshold=2,
            breaker_seconds=1.0,
            error_total=5,
            consecutive_failures=2,
        )
        context.record_success(logger, "Recovered after %d failures")
        logger.info.assert_called_once_with("Recovered after %d failures", 2)
        self.assertEqual(context.consecutive_failures, 0)

    def test_record_success_no_failures(self) -> None:
        logger = Mock()
        context = loop_utils.LoopFailureContext(
            name="loop",
            failure_threshold=2,
            breaker_seconds=1.0,
        )
        context.record_success(logger, "Recovered after %d failures")
        logger.info.assert_not_called()
        self.assertEqual(context.consecutive_failures, 0)

    def test_handle_exception_resets_on_circuit_break(self) -> None:
        logger = Mock()
        context = loop_utils.LoopFailureContext(
            name="loop",
            failure_threshold=1,
            breaker_seconds=0.0,
        )
        with patch("src.core.loop_utils.time.sleep") as sleep, \
             patch("src.core.loop_utils.time.monotonic", return_value=5.0):
            should_continue = context.handle_exception(logger, start=1.0)
        self.assertTrue(should_continue)
        self.assertEqual(context.error_total, 1)
        self.assertEqual(context.consecutive_failures, 0)
        sleep.assert_called_once_with(0.0)


class TestSchemaPath(unittest.TestCase):
    def test_schema_path_finds_existing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = os.path.join(tmp, "project")
            module_dir = os.path.join(base, "src", "core")
            os.makedirs(module_dir, exist_ok=True)
            schema_dir = os.path.join(base, "sql")
            os.makedirs(schema_dir, exist_ok=True)
            schema_path = os.path.join(schema_dir, "schema.sql")
            with open(schema_path, "w", encoding="utf-8") as handle:
                handle.write("-- schema\n")
            module_file = os.path.join(module_dir, "loop.py")
            with open(module_file, "w", encoding="utf-8") as handle:
                handle.write("# module\n")
            self.assertEqual(loop_utils.schema_path(module_file), schema_path)

    def test_schema_path_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            module_dir = os.path.join(tmp, "pkg")
            os.makedirs(module_dir, exist_ok=True)
            module_file = os.path.join(module_dir, "module.py")
            with open(module_file, "w", encoding="utf-8") as handle:
                handle.write("# module\n")
            expected = os.path.join(os.path.dirname(module_file), "..", "sql", "schema.sql")
            self.assertEqual(loop_utils.schema_path(module_file), expected)


class TestLogMetric(unittest.TestCase):
    def test_log_metric_emits_message(self) -> None:
        logger = Mock()
        loop_utils.log_metric(logger, "tick", foo=1, bar="baz")
        logger.info.assert_called_once()


if __name__ == "__main__":
    unittest.main()
