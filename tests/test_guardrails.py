import os
import unittest
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path

add_src_to_path()

guardrails = importlib.import_module("src.core.guardrails")


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
            guardrails._owners_for_state_key("ws_active_cursor:foo"),
            {"ws"},
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


class TestGuardrailsEnvHelpers(unittest.TestCase):
    def test_env_truthy(self) -> None:
        self.assertFalse(guardrails._env_truthy(None))
        self.assertTrue(guardrails._env_truthy(" YES "))
        self.assertFalse(guardrails._env_truthy("0"))

    def test_env_guardrail_mode_defaults(self) -> None:
        with patch.dict(os.environ, {"SERVICE_GUARDRAILS": "1"}, clear=True):
            self.assertEqual(guardrails._env_guardrail_mode(), "warn")
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(guardrails._env_guardrail_mode(), "off")

    def test_env_guardrail_mode_values(self) -> None:
        with patch.dict(os.environ, {"SERVICE_ENV_GUARDRAILS": "strict"}, clear=True):
            self.assertEqual(guardrails._env_guardrail_mode(), "enforce")
        with patch.dict(os.environ, {"SERVICE_ENV_GUARDRAILS": "warning"}, clear=True):
            self.assertEqual(guardrails._env_guardrail_mode(), "warn")
        with patch.dict(os.environ, {"SERVICE_ENV_GUARDRAILS": "nope"}, clear=True):
            self.assertEqual(guardrails._env_guardrail_mode(), "off")

    def test_parse_csv(self) -> None:
        self.assertEqual(guardrails._parse_csv(None), ())
        self.assertEqual(guardrails._parse_csv(" a, ,b "), ("a", "b"))

    def test_assert_env_isolated_warns(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_ENV_GUARDRAILS": "warn", "BAD_KEY": "1"},
            clear=True,
        ), patch.object(guardrails.logger, "warning") as warn:
            guardrails.assert_env_isolated("svc", forbidden_keys=("bad_key",))
        warn.assert_called_once()

    def test_assert_env_isolated_allows_keys(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SERVICE_ENV_GUARDRAILS": "warn",
                "SERVICE_ENV_ALLOW_KEYS": "BAD_KEY",
                "BAD_KEY": "1",
            },
            clear=True,
        ), patch.object(guardrails.logger, "warning") as warn:
            guardrails.assert_env_isolated("svc", forbidden_keys=("BAD_KEY",))
        warn.assert_not_called()

    def test_assert_env_isolated_allows_prefixes(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SERVICE_ENV_GUARDRAILS": "warn",
                "SERVICE_ENV_ALLOW_PREFIXES": "ALLOW_",
                "ALLOW_SECRET": "1",
            },
            clear=True,
        ), patch.object(guardrails.logger, "warning") as warn:
            guardrails.assert_env_isolated("svc", forbidden_prefixes=("ALLOW_",))
        warn.assert_not_called()

    def test_assert_env_isolated_ignores_empty_values(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_ENV_GUARDRAILS": "warn", "BAD_EMPTY": ""},
            clear=True,
        ), patch.object(guardrails.logger, "warning") as warn:
            guardrails.assert_env_isolated("svc", forbidden_prefixes=("BAD_",))
        warn.assert_not_called()

    def test_assert_env_isolated_enforces(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_ENV_GUARDRAILS": "enforce", "BAD_ENV": "1"},
            clear=True,
        ):
            with self.assertRaises(RuntimeError):
                guardrails.assert_env_isolated("svc", forbidden_prefixes=("BAD_",))
