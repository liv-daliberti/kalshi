import importlib
import unittest
from unittest.mock import patch

from _test_utils import add_src_to_path

add_src_to_path()

candles = importlib.import_module("src.kalshi.kalshi_sdk_candles")


class TestCandlestickKwargs(unittest.TestCase):
    def test_build_candlestick_kwargs_signature_error_fallback(self) -> None:
        def method(**_kwargs):
            return None

        with patch(
            "src.kalshi.kalshi_sdk_candles._filter_kwargs",
            side_effect=lambda _m, kwargs: kwargs,
        ), patch("inspect.signature", side_effect=ValueError("boom")):
            kwargs = candles._build_candlestick_kwargs(
                method,
                "S",
                "M",
                {"period_interval_minutes": 15},
            )
        self.assertEqual(kwargs["period_interval"], 15)
        self.assertEqual(kwargs["series_ticker"], "S")
        self.assertEqual(kwargs["market_ticker"], "M")
