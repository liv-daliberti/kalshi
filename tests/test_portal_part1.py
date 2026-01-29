import sys
import types
import unittest
from contextlib import contextmanager
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

PORTAL_PART1_PATH = (
    Path(__file__).resolve().parents[1] / "src" / "web_portal" / "_portal_part1.py"
)
_MISSING = object()


def _stub_module(name: str, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


@contextmanager
def _portal_part1_module(name: str = "src.web_portal._portal_part1"):
    saved: dict[str, object] = {}

    def _stash(module_name: str, module: object):
        saved[module_name] = sys.modules.get(module_name, _MISSING)
        sys.modules[module_name] = module

    root = Path(__file__).resolve().parents[1] / "src"
    portal_root = root / "web_portal"

    src_pkg = _stub_module("src")
    src_pkg.__path__ = [str(root)]
    _stash("src", src_pkg)

    portal_pkg = _stub_module("src.web_portal")
    portal_pkg.__path__ = [str(portal_root)]
    portal_pkg.__file__ = str(portal_root / "__init__.py")
    _stash("src.web_portal", portal_pkg)

    flask_mod = _stub_module("flask")

    class DummyFlask:
        pass

    flask_mod.Flask = DummyFlask
    flask_mod.g = SimpleNamespace()
    flask_mod.request = SimpleNamespace(method="GET", path="/", endpoint="index")

    def jsonify(payload):
        return ("json", payload)

    def redirect(url):
        return ("redirect", url)

    def url_for(endpoint, **_kwargs):
        return f"/{endpoint}"

    flask_mod.jsonify = jsonify
    flask_mod.redirect = redirect
    flask_mod.url_for = url_for
    flask_mod.session = {}
    _stash("flask", flask_mod)

    health_utils = _stub_module(
        "src.web_portal.health_utils",
        fetch_latest_prediction_ts=lambda _conn: None,
        load_portal_health=lambda _conn: {"ok": True},
        build_health_cards=lambda: [],
    )
    health_utils.logger = None
    _stash("src.web_portal.health_utils", health_utils)

    snapshot_utils = _stub_module(
        "src.web_portal.snapshot_utils",
        _set_snapshot_backoff=lambda *_a, **_k: None,
        _snapshot_backoff_remaining=lambda: 0.0,
        fetch_live_snapshot=lambda _ticker: ({}, None),
    )
    _stash("src.web_portal.snapshot_utils", snapshot_utils)

    for module_name in (
        "src.web_portal.routes.market",
        "src.web_portal.routes.event",
        "src.web_portal.routes.health",
        "src.web_portal.routes.stream",
    ):
        _stash(module_name, _stub_module(module_name))

    spec = spec_from_file_location(name, PORTAL_PART1_PATH)
    assert spec and spec.loader
    module = module_from_spec(spec)
    module.__package__ = "src.web_portal"
    sys.modules[name] = module
    spec.loader.exec_module(module)
    try:
        yield module
    finally:
        sys.modules.pop(name, None)
        for key, previous in saved.items():
            if previous is _MISSING:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = previous


class TestPortalPart1Basics(unittest.TestCase):
    def test_build_filter_params_and_order_by(self) -> None:
        with _portal_part1_module() as mod:
            filters = SimpleNamespace(
                search=None,
                categories=None,
                strike_period=None,
                close_window=None,
                status=None,
                sort=None,
                order=None,
            )
            with patch.object(mod, "build_filter_params", return_value={"ok": True}) as bfp:
                result = mod._build_filter_params(
                    5,
                    filters,
                    include_category=False,
                    page_params={"p": 1},
                )
            self.assertEqual(result, {"ok": True})
            bfp.assert_called_once_with(
                5,
                filters,
                include_category=False,
                page_params={"p": 1},
            )
            self.assertEqual(
                mod._build_order_by("title", "desc", "close_time", "asc"),
                "event_title desc NULLS LAST, event_ticker desc",
            )
            self.assertEqual(
                mod._build_order_by("bad", "bad", "close_time", "asc"),
                "close_time asc NULLS LAST, event_ticker asc",
            )

    def test_missing_handlers_and_route_lookup(self) -> None:
        with _portal_part1_module() as mod:
            with self.assertRaises(RuntimeError):
                mod._missing_portal_handler()
            self.assertIsNone(mod._start_portal_db_snapshot_refresh())

            def sample_handler():
                return "ok"

            mod.sample_handler = sample_handler
            self.assertIs(mod._portal_route_handler("sample_handler"), sample_handler)
            self.assertIs(mod._portal_route_handler("missing"), mod._missing_portal_handler)

    def test_module_prefix_variants(self) -> None:
        with _portal_part1_module("src.web_portal._portal_part1") as mod:
            self.assertEqual(mod._PORTAL_MODULE_PREFIX, "src.web_portal")
        with _portal_part1_module("portal_part1_alt") as mod:
            self.assertEqual(mod._PORTAL_MODULE_PREFIX, "src.web_portal")

    def test_getattr_fallback_errors(self) -> None:
        with _portal_part1_module() as mod:
            mod._FALLBACK_MODULES = ("stub.one",)

            def missing_import(name):
                raise ModuleNotFoundError(name=name)

            with patch.object(mod.importlib, "import_module", side_effect=missing_import):
                with self.assertRaises(AttributeError):
                    mod.__getattr__("missing")

            def wrong_name_import(name):
                raise ModuleNotFoundError(name="other")

            with patch.object(mod.importlib, "import_module", side_effect=wrong_name_import):
                with self.assertRaises(ModuleNotFoundError):
                    mod.__getattr__("missing")

            def import_with_error(name):
                if name == "stub.one":
                    raise ImportError("boom")
                module = types.ModuleType(name)
                module.value = 123
                return module

            mod._FALLBACK_MODULES = ("stub.one", "stub.two")
            with patch.object(mod.importlib, "import_module", side_effect=import_with_error):
                self.assertEqual(mod.__getattr__("value"), 123)

    def test_make_proxy_missing(self) -> None:
        with _portal_part1_module() as mod:
            proxy = mod._make_proxy("ghost")
            with self.assertRaises(AttributeError) as ctx:
                proxy()
            self.assertIn("ghost", str(ctx.exception))

    def test_wire_route_module_sets_attrs(self) -> None:
        with _portal_part1_module() as mod:
            dummy = SimpleNamespace()
            mod.EXISTING = 42
            mod._wire_route_module(dummy, ("call_me",), ("EXISTING",))
            self.assertTrue(callable(dummy.call_me))
            self.assertEqual(dummy.EXISTING, 42)

    def test_wire_route_modules_handles_import_errors(self) -> None:
        with _portal_part1_module() as mod:
            mod._PORTAL_MODULE_PREFIX = "src.web_portal"

            def import_side_effect(name):
                if name.endswith(".market"):
                    raise ModuleNotFoundError(name=name)
                if name.endswith(".event"):
                    raise ImportError("boom")
                return types.ModuleType(name)

            with patch.object(mod.importlib, "import_module", side_effect=import_side_effect):
                mod._wire_route_modules()

    def test_wire_route_modules_raises_on_unexpected_missing(self) -> None:
        with _portal_part1_module() as mod:
            mod._PORTAL_MODULE_PREFIX = "src.web_portal"

            def import_side_effect(name):
                if name.endswith(".market"):
                    raise ModuleNotFoundError(name="other.module")
                return types.ModuleType(name)

            with patch.object(mod.importlib, "import_module", side_effect=import_side_effect):
                with self.assertRaises(ModuleNotFoundError):
                    mod._wire_route_modules()


class TestPortalPart1HealthCards(unittest.TestCase):
    def test_build_health_cards_with_none_details(self) -> None:
        with _portal_part1_module() as mod:
            cards = [{"title": "Snapshot Poller", "details": None}]
            with patch.object(mod._health_utils, "build_health_cards", return_value=cards), \
                patch.object(mod, "_snapshot_backoff_remaining", return_value=2.0):
                result = mod._build_health_cards()
            self.assertEqual(result[0]["details"], ["Backoff remaining: 2.0s"])

    def test_build_health_cards_with_string_details(self) -> None:
        with _portal_part1_module() as mod:
            cards = [{"title": "Snapshot Poller", "details": "hello"}]
            with patch.object(mod._health_utils, "build_health_cards", return_value=cards), \
                patch.object(mod, "_snapshot_backoff_remaining", return_value=None):
                result = mod._build_health_cards()
            details = result[0]["details"]
            self.assertEqual(details[0], "hello")
            self.assertEqual(details[-1], "Backoff remaining: N/A")

    def test_build_health_cards_with_iterable_details(self) -> None:
        with _portal_part1_module() as mod:
            cards = [{"title": "Snapshot Poller", "details": ("a", "b")}]
            with patch.object(mod._health_utils, "build_health_cards", return_value=cards), \
                patch.object(mod, "_snapshot_backoff_remaining", return_value=None):
                result = mod._build_health_cards()
            self.assertEqual(result[0]["details"][0], "a")

    def test_build_health_cards_with_non_iterable_details(self) -> None:
        with _portal_part1_module() as mod:
            marker = object()
            cards = [{"title": "Snapshot Poller", "details": marker}]
            with patch.object(mod._health_utils, "build_health_cards", return_value=cards), \
                patch.object(mod, "_snapshot_backoff_remaining", return_value=None):
                result = mod._build_health_cards()
            self.assertEqual(result[0]["details"][0], str(marker))

    def test_build_health_cards_backoff_exception(self) -> None:
        with _portal_part1_module() as mod:
            cards = [{"title": "Snapshot Poller", "details": []}]

            def raise_backoff():
                raise RuntimeError("boom")

            with patch.object(mod._health_utils, "build_health_cards", return_value=cards), \
                patch.object(mod, "_snapshot_backoff_remaining", side_effect=raise_backoff):
                result = mod._build_health_cards()
            self.assertEqual(result[0]["details"][-1], "Backoff remaining: N/A")


class TestPortalPart1SnapshotPolling(unittest.TestCase):
    def test_log_request_timing_no_start(self) -> None:
        with _portal_part1_module() as mod:
            mod.g = SimpleNamespace()
            response = SimpleNamespace(status_code=200)
            self.assertIs(mod.log_request_timing(response), response)

    def test_snapshot_poll_tickers_breaks_on_backoff(self) -> None:
        with _portal_part1_module() as mod:
            config = SimpleNamespace(min_attempt_sec=0, delay=SimpleNamespace(delay_ms=0, jitter_ms=0))
            with patch.object(mod, "_snapshot_attempt_allowed", return_value=True), \
                patch.object(mod, "_snapshot_poll_backoff", return_value=1.0) as backoff, \
                patch.object(mod, "_snapshot_poll_result") as poll_result, \
                patch.object(mod, "_snapshot_delay") as delay:
                updated, errors = mod._snapshot_poll_tickers("conn", ["A", "B"], config)
            self.assertEqual((updated, errors), (0, 0))
            poll_result.assert_not_called()
            delay.assert_not_called()
            backoff.assert_called_with(mid_cycle=True)

    def test_snapshot_poll_cycle_returns_on_backoff(self) -> None:
        with _portal_part1_module() as mod:
            config = SimpleNamespace(limit=1, min_age_sec=0)
            with patch.object(mod, "_snapshot_poll_backoff", return_value=1.0), \
                patch.object(mod, "_db_connection") as db_conn:
                mod._snapshot_poll_cycle(config)
            db_conn.assert_not_called()


if __name__ == "__main__":
    unittest.main()
