"""JSON helpers for DB payloads."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        ts_value = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return ts_value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def json_default(value: Any) -> Any:
    """Public wrapper for JSON default serialization."""
    return _json_default(value)


def to_json_value(value: Any) -> Any:
    """Serialize dict/list payloads for JSONB columns."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=_json_default)
    return value
