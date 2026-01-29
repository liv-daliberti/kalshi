"""JSON parsing helpers shared across services."""

from __future__ import annotations

import json
from typing import Any


def maybe_parse_json(value: Any) -> Any | None:
    """Parse a JSON-encoded string value if possible."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except (TypeError, ValueError):
            return None
    return None


def normalize_metadata_value(value: Any) -> Any | None:
    """Normalize metadata values for equality checks."""
    parsed = maybe_parse_json(value)
    if parsed is not None:
        return parsed
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return value
