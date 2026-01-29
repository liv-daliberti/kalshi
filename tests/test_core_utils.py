import unittest
from types import SimpleNamespace
from unittest.mock import mock_open, patch
import importlib

from _test_utils import add_src_to_path, ensure_kalshi_sdk_stub, ensure_psycopg_stub

ensure_psycopg_stub()
ensure_kalshi_sdk_stub()
add_src_to_path()

number_utils = importlib.import_module("src.core.number_utils")
service_utils = importlib.import_module("src.core.service_utils")


class TestNumberUtils(unittest.TestCase):
    def test_dollars_from_cents_invalid(self) -> None:
        self.assertIsNone(number_utils.dollars_from_cents("bad"))


class TestServiceUtils(unittest.TestCase):
    def test_default_schema_path_uses_schema_path(self) -> None:
        with patch.object(service_utils, "schema_path", return_value="/tmp/schema.sql") as spy:
            result = service_utils._default_schema_path()
        self.assertEqual(result, "/tmp/schema.sql")
        spy.assert_called_once_with(service_utils.__file__)

    def test_load_private_key_reads_file(self) -> None:
        with patch("builtins.open", mock_open(read_data="pem-data")) as mopen:
            result = service_utils.load_private_key("/tmp/key.pem")
        self.assertEqual(result, "pem-data")
        mopen.assert_called_once_with("/tmp/key.pem", "r", encoding="utf-8")

    def test_open_client_and_conn_uses_private_key_and_schema_override(self) -> None:
        settings = SimpleNamespace(
            kalshi_private_key_pem_path="/tmp/key.pem",
            kalshi_host="host",
            kalshi_api_key_id="key",
            database_url="postgres://db",
        )
        with patch.object(service_utils, "load_private_key", return_value="pem") as load_key, \
             patch.object(service_utils, "make_client", return_value="client") as make_client, \
             patch.object(service_utils.psycopg, "connect", return_value="conn") as connect, \
             patch.object(service_utils, "maybe_init_schema") as init_schema, \
             patch.object(service_utils, "ensure_schema_compatible") as ensure_schema:
            client, conn = service_utils.open_client_and_conn(
                settings,
                schema_path_override="/tmp/schema.sql",
            )
        self.assertEqual(client, "client")
        self.assertEqual(conn, "conn")
        load_key.assert_called_once_with("/tmp/key.pem")
        make_client.assert_called_once_with("host", "key", "pem")
        connect.assert_called_once_with("postgres://db")
        init_schema.assert_called_once_with("conn", schema_path="/tmp/schema.sql")
        ensure_schema.assert_called_once_with("conn")

    def test_open_client_and_conn_uses_provided_key(self) -> None:
        settings = SimpleNamespace(
            kalshi_private_key_pem_path="/tmp/key.pem",
            kalshi_host="host",
            kalshi_api_key_id="key",
            database_url="postgres://db",
        )
        with patch.object(service_utils, "load_private_key") as load_key, \
             patch.object(service_utils, "make_client", return_value="client") as make_client, \
             patch.object(service_utils.psycopg, "connect", return_value="conn") as connect, \
             patch.object(service_utils, "maybe_init_schema") as init_schema, \
             patch.object(service_utils, "ensure_schema_compatible") as ensure_schema, \
             patch.object(service_utils, "_default_schema_path", return_value="/tmp/default.sql"):
            client, conn = service_utils.open_client_and_conn(
                settings,
                private_key_pem="pem-inline",
            )
        self.assertEqual(client, "client")
        self.assertEqual(conn, "conn")
        load_key.assert_not_called()
        make_client.assert_called_once_with("host", "key", "pem-inline")
        connect.assert_called_once_with("postgres://db")
        init_schema.assert_called_once_with("conn", schema_path="/tmp/default.sql")
        ensure_schema.assert_called_once_with("conn")
