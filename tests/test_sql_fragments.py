import unittest
import importlib

from _test_utils import add_src_to_path

add_src_to_path()

sql_fragments = importlib.import_module("src.db.sql_fragments")


class TestSqlFragments(unittest.TestCase):
    def test_market_identity_columns_alias(self) -> None:
        sql = sql_fragments.market_identity_columns_sql(ticker_alias="alias_ticker")
        self.assertIn("m.ticker AS alias_ticker", sql)
