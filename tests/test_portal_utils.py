import sys
import types
import unittest
from unittest.mock import patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

from src.web_portal import portal_utils


class TestPortalUtils(unittest.TestCase):
    def test_iter_portal_modules_dedup(self) -> None:
        module = types.ModuleType("src.web_portal")
        module.__file__ = "/tmp/web_portal/__init__.py"
        original_main = sys.modules.get("src.web_portal")
        original_init = sys.modules.get("src.web_portal.__init__")
        try:
            sys.modules["src.web_portal"] = module
            sys.modules["src.web_portal.__init__"] = module
            modules = list(portal_utils._iter_portal_modules())
        finally:
            if original_main is None:
                sys.modules.pop("src.web_portal", None)
            else:
                sys.modules["src.web_portal"] = original_main
            if original_init is None:
                sys.modules.pop("src.web_portal.__init__", None)
            else:
                sys.modules["src.web_portal.__init__"] = original_init
        self.assertEqual(modules.count(module), 1)

    def test_iter_portal_modules_fallback_and_none_entries(self) -> None:
        module = types.ModuleType("custom.portal")
        module.__file__ = "/tmp/web_portal/__init__.py"
        key = "custom.portal"
        none_key = "portal_utils.none"
        original = sys.modules.get(key)
        original_none = sys.modules.get(none_key)
        try:
            sys.modules[key] = module
            sys.modules[none_key] = None
            modules = list(portal_utils._iter_portal_modules())
        finally:
            if original is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = original
            if original_none is None:
                sys.modules.pop(none_key, None)
            else:
                sys.modules[none_key] = original_none
        self.assertIn(module, modules)

    def test_module_namespace_edge_cases(self) -> None:
        class NoDict:
            def __getattribute__(self, name):
                if name == "__dict__":
                    raise AttributeError("no dict")
                return super().__getattribute__(name)

        class BadDict:
            def __getattribute__(self, name):
                if name == "__dict__":
                    raise TypeError("bad dict")
                return super().__getattribute__(name)

        class NonDict:
            def __getattribute__(self, name):
                if name == "__dict__":
                    return []
                return super().__getattribute__(name)

        no_dict = NoDict()
        bad_dict = BadDict()
        non_dict = NonDict()
        self.assertIsNone(portal_utils._module_namespace(no_dict))
        self.assertIsNone(portal_utils._module_namespace(bad_dict))
        self.assertIsNone(portal_utils._module_namespace(non_dict))
        self.assertEqual(portal_utils._safe_getattr(no_dict, "missing", "fallback"), "fallback")

    def test_portal_module_with_attr_missing_and_empty(self) -> None:
        originals = {key: sys.modules.get(key) for key in portal_utils._PORTAL_MODULE_CANDIDATES}
        try:
            for key in portal_utils._PORTAL_MODULE_CANDIDATES:
                sys.modules.pop(key, None)
            with patch.object(portal_utils, "_iter_portal_modules", return_value=iter(())):
                self.assertIsNone(portal_utils._portal_module_with_attr("missing_attr"))
                self.assertIsNone(portal_utils._portal_module_with_attr(None))
        finally:
            for key, value in originals.items():
                if value is None:
                    sys.modules.pop(key, None)
                else:
                    sys.modules[key] = value
