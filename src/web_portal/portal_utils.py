"""Helpers for accessing shared portal module state."""

from __future__ import annotations

import logging
import sys
from typing import Any


def portal_module():
    """Return the loaded portal module instance if available."""
    return (
        sys.modules.get("src.web_portal")
        or sys.modules.get("src.web_portal.__init__")
        or sys.modules.get("web_portal")
    )


def portal_attr(name: str, default: Any):
    """Fetch an attribute from the portal module with a default fallback."""
    portal = portal_module()
    if portal is not None and hasattr(portal, name):
        return getattr(portal, name)
    return default


def portal_func(name: str, fallback):
    """Fetch a callable portal attribute with a fallback when missing."""
    func = portal_attr(name, None)
    if callable(func):
        return func
    return fallback


def portal_logger(name: str) -> logging.Logger:
    """Return a shared portal logger when available."""
    return portal_attr("logger", logging.getLogger(name))
