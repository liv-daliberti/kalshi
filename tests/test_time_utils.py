import unittest
from unittest.mock import patch

from _test_utils import add_src_to_path

add_src_to_path()

import src.core.time_utils as time_utils


class TestTimeUtils(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_checked = time_utils._ISOPARSE_CHECKED
        self._orig_parser = time_utils._ISOPARSE

    def tearDown(self) -> None:
        time_utils._ISOPARSE_CHECKED = self._orig_checked
        time_utils._ISOPARSE = self._orig_parser

    def test_resolve_isoparse_import_error(self) -> None:
        time_utils._ISOPARSE_CHECKED = False
        time_utils._ISOPARSE = None
        with patch("src.core.time_utils.importlib.import_module", side_effect=ImportError):
            parser = time_utils._resolve_isoparse()
        self.assertIsNone(parser)
        self.assertTrue(time_utils._ISOPARSE_CHECKED)
        self.assertIsNone(time_utils._ISOPARSE)

    def test_parse_ts_no_parser_available(self) -> None:
        with patch.object(time_utils, "_resolve_isoparse", return_value=None):
            self.assertIsNone(time_utils.parse_ts("2024-01-01T00:00:00Z"))

    def test_normalize_strike_period(self) -> None:
        self.assertIsNone(time_utils.normalize_strike_period(None))
        self.assertIsNone(time_utils.normalize_strike_period("   "))
        self.assertEqual(time_utils.normalize_strike_period("HOUR"), "hour")
        self.assertEqual(time_utils.normalize_strike_period("days"), "day")
        self.assertEqual(time_utils.normalize_strike_period("15 min"), "hour")
        self.assertEqual(time_utils.normalize_strike_period("week"), "week")
