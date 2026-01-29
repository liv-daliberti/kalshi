import importlib
import unittest
from unittest.mock import patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

portal_kalshi = importlib.import_module("src.web_portal.kalshi")


class TestPortalKalshiState(unittest.TestCase):
    def test_state_get_falls_back_to_globals(self) -> None:
        sentinel = object()
        original = portal_kalshi.__dict__.get("TEST_STATE")
        portal_kalshi.__dict__["TEST_STATE"] = sentinel
        try:
            with patch.object(portal_kalshi, "_portal_module", return_value=None):
                value = portal_kalshi._state_get("TEST_STATE", "fallback")
        finally:
            if original is None:
                portal_kalshi.__dict__.pop("TEST_STATE", None)
            else:
                portal_kalshi.__dict__["TEST_STATE"] = original
        self.assertIs(value, sentinel)
