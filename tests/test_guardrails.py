import os
import unittest
from unittest.mock import patch

from _test_utils import add_src_to_path

add_src_to_path()

import src.core.guardrails as guardrails


class TestGuardrailsNoRole(unittest.TestCase):
    def test_state_write_returns_when_no_role(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_GUARDRAILS": "1", "KALSHI_RUN_MODE": "unknown"},
            clear=True,
        ), patch.object(guardrails.logger, "warning") as warn:
            guardrails.assert_state_write_allowed("last_discovery_ts")
        warn.assert_not_called()

    def test_queue_op_returns_when_no_role(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_GUARDRAILS": "1", "KALSHI_RUN_MODE": "unknown"},
            clear=True,
        ), patch.object(guardrails.logger, "warning") as warn:
            guardrails.assert_queue_op_allowed("enqueue")
        warn.assert_not_called()

    def test_assert_service_role_returns_when_no_role(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_GUARDRAILS": "1", "KALSHI_RUN_MODE": "unknown"},
            clear=True,
        ):
            guardrails.assert_service_role("rest", "ctx")


class TestGuardrailsEnforcement(unittest.TestCase):
    def test_service_role_env_overrides(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_ROLE": "ReSt", "KALSHI_RUN_MODE": "ws"},
            clear=True,
        ):
            self.assertEqual(guardrails._service_role(), "rest")

    def test_service_role_from_run_mode(self) -> None:
        with patch.dict(os.environ, {"KALSHI_RUN_MODE": "ws"}, clear=True):
            self.assertEqual(guardrails._service_role(), "ws")

    def test_owners_for_state_key(self) -> None:
        self.assertEqual(guardrails._owners_for_state_key("last_discovery_ts"), {"rest"})
        self.assertEqual(
            guardrails._owners_for_state_key("backfill_last_ts:abc"),
            {"rest", "worker"},
        )
        self.assertEqual(
            guardrails._owners_for_state_key("last_min_close_ts"),
            {"rest", "worker"},
        )
        self.assertEqual(
            guardrails._owners_for_state_key("last_worker_ts"),
            {"worker"},
        )
        self.assertEqual(
            guardrails._owners_for_state_key("last_tick_ts"),
            {"ws"},
        )
        self.assertEqual(
            guardrails._owners_for_state_key("last_prediction_ts"),
            {"rag"},
        )
        self.assertIsNone(guardrails._owners_for_state_key("unknown_key"))

    def test_state_write_disallowed(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_GUARDRAILS": "1", "SERVICE_ROLE": "ws"},
            clear=True,
        ):
            with self.assertRaises(PermissionError):
                guardrails.assert_state_write_allowed("last_discovery_ts")

    def test_state_write_warns_for_unknown_key(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_GUARDRAILS": "1", "SERVICE_ROLE": "rest"},
            clear=True,
        ), patch.object(guardrails.logger, "warning") as warn:
            guardrails.assert_state_write_allowed("unknown_key")
        warn.assert_called_once()

    def test_queue_op_disallowed(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_GUARDRAILS": "1", "SERVICE_ROLE": "rest"},
            clear=True,
        ):
            with self.assertRaises(PermissionError):
                guardrails.assert_queue_op_allowed("claim")

    def test_queue_op_warns_for_unknown_op(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_GUARDRAILS": "1", "SERVICE_ROLE": "worker"},
            clear=True,
        ), patch.object(guardrails.logger, "warning") as warn:
            guardrails.assert_queue_op_allowed("mystery")
        warn.assert_called_once()

    def test_assert_service_role_mismatch(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_GUARDRAILS": "1", "SERVICE_ROLE": "rest"},
            clear=True,
        ):
            with self.assertRaises(PermissionError):
                guardrails.assert_service_role("rag", "ctx")
