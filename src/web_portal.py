"""Compatibility shim for the web_portal package."""

from importlib import import_module

_portal = import_module("src.web_portal.__init__")
app = _portal.app
main = _portal.main

__all__ = ["app", "main"]


if __name__ == "__main__":
    main()
