import os
import runpy
import sys
import types
import unittest
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path

add_src_to_path()

if "src.services.main" not in sys.modules:
    main_stub = types.ModuleType("src.services.main")

    def _main():
        return None

    main_stub.main = _main
    sys.modules["src.services.main"] = main_stub

ws_service = importlib.import_module("src.services.ws_service")


class TestWsService(unittest.TestCase):
    def test_ensure_env_sets_defaults(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            ws_service._ensure_env()
            self.assertEqual(os.environ["KALSHI_RUN_MODE"], "ws")
            self.assertEqual(os.environ["SERVICE_ROLE"], "ws")
            self.assertEqual(os.environ["DB_INIT_SCHEMA"], "0")

    def test_ensure_env_does_not_override_defaults(self) -> None:
        with patch.dict(
            os.environ,
            {"SERVICE_ROLE": "rest", "DB_INIT_SCHEMA": "1"},
            clear=True,
        ):
            ws_service._ensure_env()
            self.assertEqual(os.environ["KALSHI_RUN_MODE"], "ws")
            self.assertEqual(os.environ["SERVICE_ROLE"], "rest")
            self.assertEqual(os.environ["DB_INIT_SCHEMA"], "1")

    def test_main_runs_ingestor_main(self) -> None:
        def fake_run(coro):
            if hasattr(coro, "close"):
                coro.close()

        with patch("src.services.ws_service._ensure_env") as ensure_env, \
             patch("src.services.ws_service.asyncio.run", side_effect=fake_run) as run:
            ws_service.main()
        ensure_env.assert_called_once()
        run.assert_called_once()

    def test_module_main_entrypoint(self) -> None:
        prev = sys.modules.pop("src.services.ws_service", None)
        try:
            def fake_run(coro):
                if hasattr(coro, "close"):
                    coro.close()

            with patch.dict(os.environ, {}, clear=True):
                with patch("asyncio.run", side_effect=fake_run) as run:
                    runpy.run_module("src.services.ws_service", run_name="__main__")
                self.assertEqual(os.environ["KALSHI_RUN_MODE"], "ws")
            run.assert_called_once()
        finally:
            if prev is not None:
                sys.modules["src.services.ws_service"] = prev
