"""Simple web portal for browsing active and closed Kalshi markets."""

from __future__ import annotations

from importlib.machinery import SourceFileLoader
from importlib.util import spec_from_loader
from pathlib import Path
import sys


def _exec_portal_part(filename: str) -> None:
    path = Path(__file__).with_name(filename)
    module_globals = globals()
    saved = {
        "__cached__": module_globals.get("__cached__"),
        "__file__": module_globals.get("__file__"),
        "__loader__": module_globals.get("__loader__"),
        "__package__": module_globals.get("__package__"),
        "__path__": module_globals.get("__path__"),
        "__spec__": module_globals.get("__spec__"),
    }
    loader = SourceFileLoader(__name__, str(path))
    spec = spec_from_loader(__name__, loader, is_package=True)
    if saved["__path__"] is not None:
        spec.submodule_search_locations = list(saved["__path__"])
    module = sys.modules[__name__]
    module.__loader__ = loader
    module.__spec__ = spec
    package_name = saved["__package__"]
    if __name__.endswith("web_portal") and package_name != __name__:
        package_name = __name__
    module.__package__ = package_name
    loader.exec_module(module)
    for key, value in saved.items():
        if value is None:
            module_globals.pop(key, None)
        else:
            module_globals[key] = value


for _part in ("_portal_part1.py", "_portal_part2.py"):
    _exec_portal_part(_part)

del _part
del _exec_portal_part
del Path
del SourceFileLoader
del spec_from_loader
del sys
