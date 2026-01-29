import os
import sys
import types
import unittest
from contextlib import contextmanager
from dataclasses import dataclass
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from typing import Optional, Tuple
from unittest.mock import MagicMock, patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

PORTAL_PART2_PATH = (
    Path(__file__).resolve().parents[1] / "src" / "web_portal" / "_portal_part2.py"
)
_MISSING = object()


def _stub_module(name: str, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


@contextmanager
def _portal_part2_module(name: str = "src.web_portal._portal_part2"):
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
        def __init__(self, *_args, **_kwargs):
            return None

        def run(self, *_args, **_kwargs):
            return None

    flask_mod.Flask = DummyFlask
    flask_mod.request = SimpleNamespace(args={})

    def jsonify(payload):
        return {"json": payload}

    def render_template(template, **context):
        return ("render", template, context)

    def url_for(endpoint, **_kwargs):
        return f"/{endpoint}"

    flask_mod.jsonify = jsonify
    flask_mod.render_template = render_template
    flask_mod.url_for = url_for
    _stash("flask", flask_mod)

    auth_utils = _stub_module(
        "src.web_portal.auth_utils",
        is_authenticated=lambda: False,
        require_password=lambda: None,
    )
    _stash("src.web_portal.auth_utils", auth_utils)

    category_utils = _stub_module(
        "src.web_portal.category_utils", build_category_filters=lambda **_kwargs: []
    )
    _stash("src.web_portal.category_utils", category_utils)

    def _env_bool(_name, default, **_kwargs):
        return default

    def _env_int(_name, default, **_kwargs):
        return default

    def _parse_csv(raw):
        return [token for token in raw.split(",") if token]

    config_mod = _stub_module(
        "src.web_portal.config",
        CLOSE_WINDOW_OPTIONS=(),
        EVENT_ORDER_OPTIONS=(),
        EVENT_SORT_COLUMNS=("close_time", "open_time"),
        EVENT_SORT_OPTIONS=(),
        EVENT_STATUS_OPTIONS=(),
        _env_bool=_env_bool,
        _env_int=_env_int,
        _parse_csv=_parse_csv,
    )
    _stash("src.web_portal.config", config_mod)

    @dataclass
    class EventCursor:
        value: Optional[object] = None
        event_ticker: Optional[str] = None

    @contextmanager
    def _db_connection(*_args, **_kwargs):
        yield object()

    db_mod = _stub_module(
        "src.web_portal.db",
        _db_connection=_db_connection,
        _cursor_from_rows=lambda *_args, **_kwargs: None,
        _normalize_cursor_value=lambda value: value,
        build_event_snapshot=lambda row: row,
        EventCursor=EventCursor,
        fetch_active_event_categories=lambda *_args, **_kwargs: [],
        fetch_active_events=lambda *_args, **_kwargs: [],
        fetch_closed_events=lambda *_args, **_kwargs: [],
        fetch_closed_filled_count=lambda *_args, **_kwargs: 0,
        fetch_counts=lambda *_args, **_kwargs: (0, 0, 0),
        fetch_portal_snapshot=lambda *_args, **_kwargs: None,
        fetch_scheduled_events=lambda *_args, **_kwargs: [],
        fetch_strike_periods=lambda *_args, **_kwargs: [],
    )
    _stash("src.web_portal.db", db_mod)

    filter_params_mod = _stub_module(
        "src.web_portal.filter_params", build_filter_params=lambda *_args, **_kwargs: {}
    )
    _stash("src.web_portal.filter_params", filter_params_mod)

    health_utils_mod = _stub_module(
        "src.web_portal.health_utils",
        build_portal_health_from_snapshot=lambda _payload: None,
    )
    _stash("src.web_portal.health_utils", health_utils_mod)

    @dataclass(frozen=True)
    class PortalFilters:
        search: Optional[str]
        categories: Tuple[str, ...]
        strike_period: Optional[str]
        close_window: Optional[str]
        status: Optional[str]
        sort: Optional[str]
        order: Optional[str]

    portal_filters_mod = _stub_module(
        "src.web_portal.portal_filters",
        PortalFilters=PortalFilters,
        _parse_portal_filters=lambda _args: PortalFilters(
            search=None,
            categories=(),
            strike_period=None,
            close_window=None,
            status=None,
            sort=None,
            order=None,
        ),
    )
    _stash("src.web_portal.portal_filters", portal_filters_mod)

    portal_limits_mod = _stub_module(
        "src.web_portal.portal_limits",
        clamp_limit=lambda value: int(value) if value is not None else 0,
        clamp_page=lambda value: int(value) if value is not None else 0,
    )
    _stash("src.web_portal.portal_limits", portal_limits_mod)

    portal_app_factory_mod = _stub_module(
        "src.web_portal.portal_app_factory", _create_portal_app=lambda: DummyFlask()
    )
    _stash("src.web_portal.portal_app_factory", portal_app_factory_mod)

    portal_utils_mod = _stub_module(
        "src.web_portal.portal_utils", portal_func=lambda _name, default: default
    )
    _stash("src.web_portal.portal_utils", portal_utils_mod)

    formatters_mod = _stub_module(
        "src.web_portal.formatters", fmt_ts=lambda _dt: "ts"
    )
    _stash("src.web_portal.formatters", formatters_mod)

    portal_part1_mod = _stub_module(
        "src.web_portal._portal_part1",
        _PORTAL_DATA_CACHE={},
        _PORTAL_DATA_CACHE_LOCK=MagicMock(),
        _load_portal_health=lambda _conn: {"ok": True},
        describe_event_scope=lambda: "scope",
        fmt_time_remaining=lambda _ts: "soon",
        logger=MagicMock(),
    )
    _stash("src.web_portal._portal_part1", portal_part1_mod)

    db_root_mod = _stub_module("src.db")
    db_root_mod.__path__ = []
    _stash("src.db", db_root_mod)
    db_db_mod = _stub_module("src.db.db", ensure_schema_compatible=lambda _conn: None)
    _stash("src.db.db", db_db_mod)

    spec = spec_from_file_location(name, PORTAL_PART2_PATH)
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


class TestPortalPart2(unittest.TestCase):
    def test_store_portal_data_cache_and_snapshot_enabled_flag(self) -> None:
        with _portal_part2_module() as mod:
            cache_key = ("cache",)
            payload = mod._empty_portal_data()
            mod._PORTAL_DATA_CACHE.clear()
            with patch.object(mod.time, "monotonic", return_value=12.5):
                mod._store_portal_data_cache(cache_key, 5, payload)
            self.assertIn(cache_key, mod._PORTAL_DATA_CACHE)
            self.assertIs(mod._PORTAL_DATA_CACHE[cache_key][1], payload)

            mod._PORTAL_DATA_CACHE.clear()
            mod._store_portal_data_cache(cache_key, 0, payload)
            self.assertEqual(mod._PORTAL_DATA_CACHE, {})

            with patch.object(mod, "_env_bool", return_value=True) as env_bool:
                self.assertTrue(mod._portal_db_snapshot_enabled())
                env_bool.assert_called_once_with("WEB_PORTAL_DB_SNAPSHOT_ENABLE", False)

    def test_snapshot_flags_and_refresh(self) -> None:
        with _portal_part2_module() as mod:
            with patch.object(mod, "_env_bool", return_value=False) as env_bool:
                self.assertFalse(mod._portal_include_health())
                env_bool.assert_called_once_with(
                    "WEB_PORTAL_DB_SNAPSHOT_INCLUDE_HEALTH", True
                )
            with patch.object(mod, "_env_int", return_value=120) as env_int:
                self.assertEqual(mod._portal_db_snapshot_refresh_sec(), 120)
                env_int.assert_called_once_with(
                    "WEB_PORTAL_DB_SNAPSHOT_REFRESH_SEC", 60, minimum=5
                )
            mod._PORTAL_SNAPSHOT_REFRESH_DISABLED_WARNED = False
            with patch.object(mod, "_portal_db_snapshot_enabled", return_value=True):
                with patch.object(mod.logger, "info") as info:
                    mod._start_portal_db_snapshot_refresh()
                    info.assert_called_once()
            self.assertTrue(mod._PORTAL_SNAPSHOT_REFRESH_DISABLED_WARNED)

    def test_snapshot_refresh_disabled_paths(self) -> None:
        with _portal_part2_module() as mod:
            mod._PORTAL_SNAPSHOT_REFRESH_DISABLED_WARNED = False
            with patch.object(mod, "_portal_db_snapshot_enabled", return_value=False):
                with patch.object(mod.logger, "info") as info:
                    mod._start_portal_db_snapshot_refresh()
                info.assert_not_called()
            self.assertFalse(mod._PORTAL_SNAPSHOT_REFRESH_DISABLED_WARNED)

            mod._PORTAL_SNAPSHOT_REFRESH_DISABLED_WARNED = True
            with patch.object(mod, "_portal_db_snapshot_enabled", return_value=True):
                with patch.object(mod.logger, "info") as info:
                    mod._start_portal_db_snapshot_refresh()
                info.assert_not_called()

    def test_parse_portal_paging_resets_active_page(self) -> None:
        with _portal_part2_module() as mod:
            paging = mod._parse_portal_paging({"active_page": "2"}, limit=10)
            self.assertEqual(paging.active_page, 0)
            self.assertIsNone(paging.active_cursor)

    def test_parse_portal_paging_retains_closed_cursor(self) -> None:
        with _portal_part2_module() as mod:
            token = mod._encode_cursor_token(
                mod.EventCursor(value=3, event_ticker="EV1")
            )
            paging = mod._parse_portal_paging(
                {"closed_page": "2", "closed_after": token},
                limit=10,
            )
            self.assertEqual(paging.closed_page, 2)
            self.assertEqual(paging.closed_cursor, token)

    def test_portal_data_from_snapshot_closed_total_paths(self) -> None:
        with _portal_part2_module() as mod:
            rows = mod.PortalRows(active=[], scheduled=[], closed=[])
            cursors = mod.PortalCursorTokens(active=None, scheduled=None, closed=None)
            payload = {
                "active_total": 1,
                "scheduled_total": 2,
                "closed_total": 3,
                "health_raw": {"ok": True},
            }
            filters = SimpleNamespace()

            @contextmanager
            def fake_conn():
                yield object()

            with patch.object(mod, "_snapshot_rows_and_cursors", return_value=(rows, cursors)):
                with patch.object(mod, "_db_connection", return_value=fake_conn()):
                    with patch.object(mod, "fetch_closed_filled_count", return_value=7):
                        with patch.object(
                            mod,
                            "build_portal_health_from_snapshot",
                            return_value={"ok": True},
                        ):
                            data = mod._portal_data_from_snapshot(payload, filters)
            self.assertEqual(data.closed_filled_total, 7)

            payload["closed_filled_total"] = "11"
            with patch.object(mod, "_snapshot_rows_and_cursors", return_value=(rows, cursors)):
                with patch.object(mod, "build_portal_health_from_snapshot", return_value=None):
                    data = mod._portal_data_from_snapshot(payload, filters)
            self.assertEqual(data.closed_filled_total, 11)

    def test_portal_data_from_snapshot_missing_closed_filled_total(self) -> None:
        with _portal_part2_module() as mod:
            rows = mod.PortalRows(active=[], scheduled=[], closed=[])
            cursors = mod.PortalCursorTokens(active=None, scheduled=None, closed=None)
            payload = {
                "active_total": 1,
                "scheduled_total": 5,
                "closed_total": 2,
            }
            filters = SimpleNamespace()

            @contextmanager
            def fake_conn():
                yield object()

            with patch.object(mod, "_snapshot_rows_and_cursors", return_value=(rows, cursors)):
                with patch.object(mod, "_db_connection", return_value=fake_conn()):
                    with patch.object(
                        mod,
                        "fetch_closed_filled_count",
                        side_effect=RuntimeError("boom"),
                    ):
                        data = mod._portal_data_from_snapshot(payload, filters)
            self.assertEqual(data.closed_filled_total, 0)
            self.assertEqual(data.totals.scheduled, 5)

    def test_fetch_portal_snapshot_data_warns_once(self) -> None:
        with _portal_part2_module() as mod:
            mod._PORTAL_SNAPSHOT_QUERY_WARNED = False
            filters = SimpleNamespace()
            cursors = (None, None, None)

            @contextmanager
            def fake_conn():
                yield object()

            with patch.object(mod, "_db_connection", return_value=fake_conn()):
                with patch.object(
                    mod, "fetch_portal_snapshot", side_effect=RuntimeError("boom")
                ):
                    with patch.object(mod.logger, "warning") as warn:
                        self.assertIsNone(
                            mod._fetch_portal_snapshot_data(5, filters, cursors)
                        )
                        warn.assert_called_once()
            self.assertTrue(mod._PORTAL_SNAPSHOT_QUERY_WARNED)

    def test_fetch_portal_snapshot_data_success(self) -> None:
        with _portal_part2_module() as mod:
            filters = SimpleNamespace()
            cursors = (None, None, None)
            payload = {"active_rows": [], "scheduled_rows": [], "closed_rows": []}
            sentinel = object()

            @contextmanager
            def fake_conn():
                yield object()

            with patch.object(mod, "_db_connection", return_value=fake_conn()):
                with patch.object(mod, "fetch_portal_snapshot", return_value=payload):
                    with patch.object(
                        mod,
                        "_portal_data_from_snapshot",
                        return_value=sentinel,
                    ) as build:
                        result = mod._fetch_portal_snapshot_data(5, filters, cursors)
            self.assertIs(result, sentinel)
            build.assert_called_once_with(payload, filters)

    def test_fetch_portal_live_data_includes_health(self) -> None:
        with _portal_part2_module() as mod:
            rows = mod.PortalRows(active=[{"a": 1}], scheduled=[], closed=[])
            totals = mod.PortalTotals(active=1, scheduled=0, closed=0)
            cursor_tokens = mod.PortalCursorTokens(
                active="a", scheduled="b", closed="c"
            )
            filters = SimpleNamespace()

            @contextmanager
            def fake_conn():
                yield object()

            with patch.object(mod, "_db_connection", return_value=fake_conn()):
                with patch.object(
                    mod, "_fetch_portal_rows", return_value=(rows, totals, cursor_tokens)
                ):
                    with patch.object(mod, "fetch_closed_filled_count", return_value=9):
                        with patch.object(
                            mod, "fetch_strike_periods", return_value=["day"]
                        ):
                            with patch.object(
                                mod, "fetch_active_event_categories", return_value=["a"]
                            ):
                                with patch.object(mod, "_portal_include_health", return_value=True):
                                    with patch.object(
                                        mod, "_load_portal_health", return_value={"ok": True}
                                    ):
                                        data = mod._fetch_portal_live_data(
                                            5, filters, (None, None, None)
                                        )
            self.assertEqual(data.health, {"ok": True})
            self.assertEqual(data.closed_filled_total, 9)

    def test_fetch_portal_live_data_excludes_health(self) -> None:
        with _portal_part2_module() as mod:
            rows = mod.PortalRows(active=[{"a": 1}], scheduled=[], closed=[])
            totals = mod.PortalTotals(active=1, scheduled=0, closed=0)
            cursor_tokens = mod.PortalCursorTokens(
                active="a", scheduled="b", closed="c"
            )
            filters = SimpleNamespace()

            @contextmanager
            def fake_conn():
                yield object()

            with patch.object(mod, "_db_connection", return_value=fake_conn()):
                with patch.object(
                    mod, "_fetch_portal_rows", return_value=(rows, totals, cursor_tokens)
                ):
                    with patch.object(mod, "fetch_closed_filled_count", return_value=1):
                        with patch.object(
                            mod, "fetch_strike_periods", return_value=["day"]
                        ):
                            with patch.object(
                                mod, "fetch_active_event_categories", return_value=["a"]
                            ):
                                with patch.object(mod, "_portal_include_health", return_value=False):
                                    with patch.object(mod, "_load_portal_health") as load_health:
                                        data = mod._fetch_portal_live_data(
                                            5, filters, (None, None, None)
                                        )
            self.assertIsNone(data.health)
            load_health.assert_not_called()

    def test_fetch_portal_data_caches_live_payload(self) -> None:
        with _portal_part2_module() as mod:
            filters = mod.PortalFilters(
                search=None,
                categories=(),
                strike_period=None,
                close_window=None,
                status=None,
                sort=None,
                order=None,
            )
            paging = mod.PortalPaging(limit=5, active_page=0, scheduled_page=0, closed_page=0)
            data = mod.PortalData(
                rows=mod.PortalRows(active=[], scheduled=[], closed=[]),
                totals=mod.PortalTotals(active=0, scheduled=0, closed=0),
                closed_filled_total=0,
                strike_periods=[],
                active_categories=[],
                health=None,
                error=None,
                cursors=None,
            )
            with patch.object(mod, "_portal_data_cache_ttl", return_value=60):
                with patch.object(mod, "_load_portal_data_cache", return_value=None):
                    with patch.object(mod, "_portal_db_snapshot_enabled", return_value=False):
                        with patch.object(mod, "_fetch_portal_live_data", return_value=data):
                            with patch.object(mod, "_store_portal_data_cache") as store:
                                result = mod._fetch_portal_data(5, filters, paging)
            self.assertIs(result, data)
            store.assert_called_once()

    def test_fetch_portal_data_uses_snapshot_when_enabled(self) -> None:
        with _portal_part2_module() as mod:
            filters = mod.PortalFilters(
                search=None,
                categories=(),
                strike_period=None,
                close_window=None,
                status=None,
                sort=None,
                order=None,
            )
            paging = mod.PortalPaging(limit=5, active_page=0, scheduled_page=0, closed_page=0)
            data = mod._empty_portal_data()
            with patch.object(mod, "_portal_data_cache_ttl", return_value=30):
                with patch.object(mod, "_load_portal_data_cache", return_value=None):
                    with patch.object(mod, "_portal_db_snapshot_enabled", return_value=True):
                        with patch.object(
                            mod, "_fetch_portal_snapshot_data", return_value=data
                        ) as fetch_snapshot:
                            with patch.object(mod, "_fetch_portal_live_data") as fetch_live:
                                result = mod._fetch_portal_data(5, filters, paging)
            self.assertIs(result, data)
            fetch_snapshot.assert_called_once()
            fetch_live.assert_not_called()

    def test_portal_data_success_response(self) -> None:
        with _portal_part2_module() as mod:
            filters = SimpleNamespace(
                search=None,
                categories=("cat",),
                strike_period=None,
                close_window=None,
                status=None,
                sort=None,
                order=None,
            )
            paging = mod.PortalPaging(limit=5, active_page=0, scheduled_page=0, closed_page=0)
            data = mod.PortalData(
                rows=mod.PortalRows(active=[], scheduled=[], closed=[]),
                totals=mod.PortalTotals(active=0, scheduled=0, closed=0),
                closed_filled_total=0,
                strike_periods=[],
                active_categories=["cat"],
                health=None,
                error=None,
                cursors=None,
            )
            with patch.dict(os.environ, {"DATABASE_URL": "db"}):
                with patch.object(mod, "require_password", return_value=None):
                    with patch.object(mod, "clamp_limit", return_value=5):
                        with patch.object(mod, "_parse_portal_filters", return_value=filters):
                            with patch.object(mod, "_parse_portal_paging", return_value=paging):
                                with patch.object(mod, "_portal_filter_fields", return_value=[]):
                                    with patch.object(mod, "_fetch_portal_data", return_value=data):
                                        with patch.object(
                                            mod,
                                            "build_filter_params",
                                            return_value={"base": True},
                                        ):
                                            with patch.object(
                                                mod,
                                                "build_category_filters",
                                                return_value=[{"name": "cat"}],
                                            ) as build_filters:
                                                with patch.object(
                                                    mod,
                                                    "_build_load_more_links",
                                                    return_value={"active": "/next"},
                                                ):
                                                    with patch.object(
                                                        mod,
                                                        "_portal_context",
                                                        return_value={
                                                            "refreshed_at": "ts",
                                                            "error": None,
                                                            "health": None,
                                                            "active_rows": [],
                                                            "scheduled_rows": [],
                                                            "closed_rows": [],
                                                            "active_total": 0,
                                                            "scheduled_total": 0,
                                                            "closed_total": 0,
                                                            "closed_filled_total": 0,
                                                            "load_more_links": {},
                                                            "active_has_more": False,
                                                            "scheduled_has_more": False,
                                                            "closed_has_more": False,
                                                            "limit": 5,
                                                            "active_page": 0,
                                                            "scheduled_page": 0,
                                                            "closed_page": 0,
                                                            "scope_note": "scope",
                                                            "category_filters": [],
                                                        },
                                                    ):
                                                        resp = mod.portal_data()
            self.assertIn("json", resp)
            self.assertEqual(resp["json"]["scope_note"], "scope")
            build_filters.assert_called_once()

    def test_portal_data_error_response(self) -> None:
        with _portal_part2_module() as mod:
            with patch.dict(os.environ, {}, clear=True):
                with patch.object(mod, "require_password", side_effect=RuntimeError("boom")):
                    resp = mod.portal_data()
        self.assertEqual(resp["json"]["error"], "boom")
