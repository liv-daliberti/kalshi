from __future__ import annotations

import sys
import types
from pathlib import Path


def add_src_to_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


def ensure_psycopg_stub() -> None:
    try:
        import psycopg  # noqa: F401
    except Exception:
        class Error(Exception):
            pass

        class UndefinedTable(Error):
            pass

        class OperationalError(Error):
            pass

        class InterfaceError(Error):
            pass

        class InsufficientPrivilege(Error):
            pass

        rows_stub = types.SimpleNamespace(dict_row=object())
        errors_stub = types.SimpleNamespace(
            UndefinedTable=UndefinedTable,
            OperationalError=OperationalError,
            InterfaceError=InterfaceError,
            InsufficientPrivilege=InsufficientPrivilege,
        )
        sys.modules["psycopg.rows"] = rows_stub
        sys.modules["psycopg"] = types.SimpleNamespace(
            Connection=object,
            connect=lambda *args, **kwargs: None,
            rows=rows_stub,
            errors=errors_stub,
            Error=Error,
            OperationalError=OperationalError,
            InterfaceError=InterfaceError,
        )


def ensure_kalshi_sdk_stub() -> None:
    try:
        import src.kalshi.kalshi_sdk as kalshi_sdk  # noqa: F401
    except Exception:
        kalshi_sdk = types.ModuleType("src.kalshi.kalshi_sdk")
        sys.modules["src.kalshi.kalshi_sdk"] = kalshi_sdk
    else:
        kalshi_sdk = sys.modules["src.kalshi.kalshi_sdk"]

    if not hasattr(kalshi_sdk, "iter_events"):
        kalshi_sdk.iter_events = lambda *args, **kwargs: []
    if not hasattr(kalshi_sdk, "get_market_candlesticks"):
        kalshi_sdk.get_market_candlesticks = lambda *args, **kwargs: {}
    if not hasattr(kalshi_sdk, "make_client"):
        kalshi_sdk.make_client = lambda *args, **kwargs: None


def ensure_websockets_stub() -> None:
    try:
        import websockets  # noqa: F401
    except Exception:
        sys.modules["websockets"] = types.SimpleNamespace(
            connect=lambda *args, **kwargs: None
        )


def ensure_cryptography_stub() -> None:
    try:
        import cryptography  # noqa: F401
    except Exception:
        fake_crypto = types.ModuleType("cryptography")
        fake_hazmat = types.ModuleType("cryptography.hazmat")
        fake_primitives = types.ModuleType("cryptography.hazmat.primitives")
        fake_hashes = types.ModuleType("cryptography.hazmat.primitives.hashes")
        fake_serialization = types.ModuleType("cryptography.hazmat.primitives.serialization")
        fake_asymmetric = types.ModuleType("cryptography.hazmat.primitives.asymmetric")
        fake_padding = types.ModuleType("cryptography.hazmat.primitives.asymmetric.padding")

        class FakePSS:
            MAX_LENGTH = object()

            def __init__(self, *args, **kwargs):
                return None

        class FakeMGF1:
            def __init__(self, *args, **kwargs):
                return None

        fake_hashes.SHA256 = lambda: object()
        fake_serialization.load_pem_private_key = lambda *args, **kwargs: None
        fake_padding.PSS = FakePSS
        fake_padding.MGF1 = FakeMGF1

        fake_primitives.hashes = fake_hashes
        fake_primitives.serialization = fake_serialization
        fake_asymmetric.padding = fake_padding

        sys.modules["cryptography"] = fake_crypto
        sys.modules["cryptography.hazmat"] = fake_hazmat
        sys.modules["cryptography.hazmat.primitives"] = fake_primitives
        sys.modules["cryptography.hazmat.primitives.hashes"] = fake_hashes
        sys.modules["cryptography.hazmat.primitives.serialization"] = fake_serialization
        sys.modules["cryptography.hazmat.primitives.asymmetric"] = fake_asymmetric
        sys.modules["cryptography.hazmat.primitives.asymmetric.padding"] = fake_padding
