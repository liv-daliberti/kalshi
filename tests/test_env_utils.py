import os
import unittest
from unittest.mock import patch
import importlib

from _test_utils import add_src_to_path

add_src_to_path()

env_utils = importlib.import_module("src.core.env_utils")


class TestEnvUtils(unittest.TestCase):
    def test_parse_helpers(self) -> None:
        self.assertEqual(env_utils._parse_int(None, 3), 3)
        self.assertEqual(env_utils._parse_int("5", 2), 5)
        self.assertEqual(env_utils._parse_int("bad", 2), 2)
        self.assertEqual(env_utils._parse_float(None, 1.0), 1.0)
        self.assertEqual(env_utils._parse_float("2.5", 1.0), 2.5)
        self.assertEqual(env_utils._parse_float("bad", 1.0), 1.0)

    def test_env_bool_defaults_and_values(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(env_utils._env_bool("X", True))
            self.assertFalse(env_utils._env_bool("X", False))
        with patch.dict(os.environ, {"X": " yes "}, clear=True):
            self.assertTrue(env_utils._env_bool("X", False))
        with patch.dict(os.environ, {"X": "no"}, clear=True):
            self.assertFalse(env_utils._env_bool("X", True))

    def test_env_int_minimum(self) -> None:
        with patch.dict(os.environ, {"X": "0"}, clear=True):
            self.assertEqual(env_utils._env_int("X", 5, minimum=1), 1)
        with patch.dict(os.environ, {"X": "10"}, clear=True):
            self.assertEqual(env_utils._env_int("X", 5, minimum=1), 10)
        with patch.dict(os.environ, {"X": "bad"}, clear=True):
            self.assertEqual(env_utils._env_int("X", 3, minimum=5), 5)

    def test_env_int_fallback(self) -> None:
        with patch.dict(os.environ, {"X": "0"}, clear=True):
            self.assertEqual(env_utils._env_int_fallback("X", 7, minimum=1), 7)
        with patch.dict(os.environ, {"X": "2"}, clear=True):
            self.assertEqual(env_utils._env_int_fallback("X", 7, minimum=1), 2)

    def test_env_float_minimum_and_fallback(self) -> None:
        with patch.dict(os.environ, {"X": "0.1"}, clear=True):
            self.assertEqual(env_utils._env_float("X", 2.0, minimum=0.5), 0.5)
            self.assertEqual(env_utils._env_float_fallback("X", 2.0, minimum=0.5), 2.0)
        with patch.dict(os.environ, {"X": "0.6"}, clear=True):
            self.assertEqual(env_utils._env_float_fallback("X", 2.0, minimum=0.5), 0.6)

    def test_public_helpers(self) -> None:
        with patch.dict(os.environ, {"X": "1"}, clear=True):
            self.assertTrue(env_utils.env_bool("X"))
            self.assertEqual(env_utils.env_int("X", 2), 1)
        with patch.dict(os.environ, {"X": "0.1"}, clear=True):
            self.assertEqual(env_utils.env_float_fallback("X", 2.0, minimum=0.5), 2.0)
        self.assertTrue(env_utils.parse_bool("On"))
        self.assertFalse(env_utils.parse_bool(None, default=False))
        self.assertEqual(env_utils.parse_int("0", 3, minimum=1), 1)
