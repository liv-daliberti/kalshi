"""Helpers for accessing shared portal module state."""

from __future__ import annotations

import logging
import sys
from typing import Any

_PORTAL_MODULE_CANDIDATES = (
    "src.web_portal",
    "src.web_portal.__init__",
    "web_portal",
)


def _iter_portal_modules():
    seen = set()
    for key in _PORTAL_MODULE_CANDIDATES:
        module = sys.modules.get(key)
        if module is None:
            continue
        module_id = id(module)
        if module_id in seen:
            continue
        seen.add(module_id)
        yield module
    for module in sys.modules.values():
        if module is None:
            continue
        module_id = id(module)
        if module_id in seen or not _is_portal_module(module):
            continue
        seen.add(module_id)
        yield module


def _module_namespace(module) -> dict[str, Any] | None:
    try:
        namespace = getattr(module, "__dict__", None)
    except (AttributeError, TypeError):
        return None
    if isinstance(namespace, dict):
        return namespace
    return None


def _safe_hasattr(module, name: str) -> bool:
    namespace = _module_namespace(module)
    return bool(namespace) and name in namespace


def _safe_getattr(module, name: str, default: Any):
    namespace = _module_namespace(module)
    if not namespace:
        return default
    return namespace.get(name, default)


def _portal_module_with_attr(name: str | None):
    if name:
        for module in _iter_portal_modules():
            if _safe_hasattr(module, name):
                return module
        for key in _PORTAL_MODULE_CANDIDATES:
            module = sys.modules.get(key)
            if module is not None:
                return module
        return None
    for module in _iter_portal_modules():
        return module
    return None


def _is_portal_module(module) -> bool:
    file_path = getattr(module, "__file__", "")
    if not file_path:
        return False
    normalized = file_path.replace("\\", "/")
    return normalized.endswith("/web_portal/__init__.py") or normalized.endswith(
        "/web_portal/__init__.pyc"
    )


def portal_module():
    """Return the loaded portal module instance if available."""
    return _portal_module_with_attr(None)


def portal_attr(name: str, default: Any):
    """Fetch an attribute from the portal module with a default fallback."""
    portal = _portal_module_with_attr(name)
    if portal is not None and _safe_hasattr(portal, name):
        return _safe_getattr(portal, name, default)
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
