"""Helpers for WS protocol messages and authentication."""

from __future__ import annotations

import base64
import time
from typing import Iterable
from urllib.parse import urlparse

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


def _ws_signature_payload(timestamp_ms: int, path: str) -> bytes:
    """Build the message payload to sign for WS auth."""
    return f"{timestamp_ms}GET{path}".encode("utf-8")


def _build_ws_headers(
    api_key_id: str,
    private_key_pem: str,
    ws_url: str,
    default_path: str,
) -> dict[str, str]:
    """Build authenticated headers for the Kalshi WebSocket."""
    parsed = urlparse(ws_url)
    path = parsed.path or default_path
    timestamp_ms = int(time.time() * 1000)
    message = _ws_signature_payload(timestamp_ms, path)
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"),
        password=None,
    )
    signature = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    signature_b64 = base64.b64encode(signature).decode("ascii")
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": signature_b64,
        "KALSHI-ACCESS-TIMESTAMP": str(timestamp_ms),
    }


def _chunked(values: Iterable[str], size: int) -> list[list[str]]:
    """Chunk an iterable into lists of size <= size."""
    batch: list[str] = []
    batches: list[list[str]] = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            batches.append(batch)
            batch = []
    if batch:
        batches.append(batch)
    return batches


def _build_subscribe_message(
    request_id: int,
    channels: Iterable[str],
    market_tickers: list[str],
) -> dict:
    """Build a subscribe request for the WS API."""
    return {
        "id": request_id,
        "cmd": "subscribe",
        "params": {
            "channels": list(channels),
            "market_tickers": market_tickers,
        },
    }
