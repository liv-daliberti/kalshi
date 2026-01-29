import os
import unittest
from unittest.mock import MagicMock, patch

from _test_utils import add_src_to_path

add_src_to_path()

service_entrypoint = __import__("src.services.service_entrypoint", fromlist=["run_service"])


class TestServiceEntrypoint(unittest.TestCase):
    def test_run_service_sets_env_and_calls_dependencies(self) -> None:
        sentinel = MagicMock(name="main_coro")
        with patch.dict(os.environ, {}, clear=True), \
             patch.object(service_entrypoint, "assert_env_isolated") as assert_env, \
             patch.object(service_entrypoint, "start_health_server") as start_health, \
             patch.object(service_entrypoint.ingestor_main, "main", return_value=sentinel) as main, \
             patch.object(service_entrypoint.asyncio, "run") as run:
            service_entrypoint.run_service(
                role="rest",
                run_mode="rest",
                forbidden_prefixes=("RAG_",),
                forbidden_keys=("SECRET",),
            )
            self.assertEqual(os.environ["KALSHI_RUN_MODE"], "rest")
            self.assertEqual(os.environ["SERVICE_ROLE"], "rest")
            self.assertEqual(os.environ["DB_INIT_SCHEMA"], "0")
            assert_env.assert_called_once_with(
                "rest",
                forbidden_prefixes=("RAG_",),
                forbidden_keys=("SECRET",),
            )
            start_health.assert_called_once_with("rest")
            main.assert_called_once_with()
            run.assert_called_once_with(sentinel)

    def test_run_service_preserves_existing_env(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_ROLE": "custom", "DB_INIT_SCHEMA": "1"},
            clear=True,
        ), patch.object(service_entrypoint, "assert_env_isolated"), \
            patch.object(service_entrypoint, "start_health_server"), \
            patch.object(service_entrypoint.ingestor_main, "main", return_value=None), \
            patch.object(service_entrypoint.asyncio, "run"):
            service_entrypoint.run_service(
                role="rest",
                run_mode="rest",
                forbidden_prefixes=(),
            )
            self.assertEqual(os.environ["KALSHI_RUN_MODE"], "rest")
            self.assertEqual(os.environ["SERVICE_ROLE"], "custom")
            self.assertEqual(os.environ["DB_INIT_SCHEMA"], "1")
