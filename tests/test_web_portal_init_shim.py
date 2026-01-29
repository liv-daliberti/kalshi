import importlib.machinery
import importlib.util
import runpy
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from _test_utils import add_src_to_path

add_src_to_path()


class TestWebPortalInit(unittest.TestCase):
    def test_init_exec_portal_part_adjusts_package_and_clears_path(self) -> None:
        init_path = Path(__file__).resolve().parents[1] / "src" / "web_portal" / "__init__.py"
        module_name = "testpkg.web_portal"
        spec = importlib.util.spec_from_file_location(module_name, init_path)
        self.assertIsNotNone(spec)
        module = importlib.util.module_from_spec(spec)
        module.__package__ = "testpkg"
        module.__path__ = None
        sys.modules[module_name] = module
        recorded = {}
        orig_exec_module = importlib.machinery.SourceFileLoader.exec_module

        def guarded_exec_module(self, module_to_exec):
            if self.path == str(init_path):
                return orig_exec_module(self, module_to_exec)
            recorded["package"] = module_to_exec.__package__
            return None

        try:
            with patch("importlib.machinery.SourceFileLoader.exec_module", new=guarded_exec_module):
                spec.loader.exec_module(module)
        finally:
            sys.modules.pop(module_name, None)

        self.assertEqual(recorded.get("package"), module.__name__)
        self.assertNotIn("__path__", module.__dict__)


class TestWebPortalShim(unittest.TestCase):
    def test_shim_exports_app_and_main(self) -> None:
        shim_path = Path(__file__).resolve().parents[1] / "src" / "web_portal.py"
        dummy_portal = SimpleNamespace(app=object(), main=object())
        spec = importlib.util.spec_from_file_location("web_portal_shim", shim_path)
        module = importlib.util.module_from_spec(spec)
        with patch("importlib.import_module", return_value=dummy_portal) as import_mod:
            spec.loader.exec_module(module)
        import_mod.assert_called_once_with("src.web_portal.__init__")
        self.assertIs(module.app, dummy_portal.app)
        self.assertIs(module.main, dummy_portal.main)
        self.assertEqual(module.__all__, ["app", "main"])

    def test_shim_main_executes_main(self) -> None:
        shim_path = Path(__file__).resolve().parents[1] / "src" / "web_portal.py"
        called = {"count": 0}

        def fake_main():
            called["count"] += 1

        dummy_portal = SimpleNamespace(app=object(), main=fake_main)
        with patch("importlib.import_module", return_value=dummy_portal):
            runpy.run_path(str(shim_path), run_name="__main__")
        self.assertEqual(called["count"], 1)


if __name__ == "__main__":
    unittest.main()
