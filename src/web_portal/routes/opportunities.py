"""Opportunities routes."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

from flask import (  # pylint: disable=import-error
    Blueprint,
    jsonify,
    render_template,
    request,
    url_for,
)

from ..auth_utils import is_authenticated
from ..category_utils import build_category_filters
from ..config import _env_float
from ..db import (
    _db_connection,
    fetch_active_event_categories,
    fetch_arbitrage_opportunities,
    fetch_opportunities,
    fetch_strike_periods,
)
from ..db_arbitrage import build_arbitrage_filters
from ..db_opportunities import build_opportunity_filters
from ..filter_params import build_filter_params
from ..formatters import fmt_ts
from ..portal_filters import _parse_portal_filters
from ..portal_limits import clamp_limit

bp = Blueprint("opportunities", __name__)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OpportunityArgs:
    """Parsed and clamped opportunity query arguments."""

    min_gap: float | None
    min_conf: float | None
    max_age: float | None
    max_tick_age: float | None


@dataclass(frozen=True)
class OpportunityData:
    """Result payload for opportunity queries."""

    rows: list[dict[str, object]]
    strike_periods: list[str]
    active_categories: list[str]
    error: str | None


def _parse_float_arg(
    raw: str | None,
    fallback: float | None,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float | None:
    if raw is None:
        value = fallback
    else:
        text = raw.strip()
        if not text:
            value = fallback
        else:
            try:
                value = float(text)
            except (TypeError, ValueError):
                value = fallback
    if value is None:
        return None
    if minimum is not None and value < minimum:
        value = minimum
    if maximum is not None and value > maximum:
        value = maximum
    return value


def _opportunity_defaults() -> OpportunityArgs:
    return OpportunityArgs(
        min_gap=_env_float("WEB_PORTAL_OPPORTUNITY_MIN_GAP", 0.10, minimum=0.0),
        min_conf=_env_float("WEB_PORTAL_OPPORTUNITY_MIN_CONF", 0.55, minimum=0.0),
        max_age=_env_float(
            "WEB_PORTAL_OPPORTUNITY_MAX_AGE_MINUTES",
            360.0,
            minimum=0.0,
        ),
        max_tick_age=_env_float(
            "WEB_PORTAL_OPPORTUNITY_MAX_TICK_AGE_MINUTES",
            60.0,
            minimum=0.0,
        ),
    )


def _parse_opportunity_view(args: dict[str, str]) -> str:
    view = (args.get("view") or "").strip().lower()
    if view in {"arbitrage", "arb", "market"}:
        return "arbitrage"
    return "model"


def _parse_opportunity_args(
    args: dict[str, str],
    defaults: OpportunityArgs,
) -> OpportunityArgs:
    return OpportunityArgs(
        min_gap=_parse_float_arg(
            args.get("min_gap"),
            defaults.min_gap,
            minimum=0.0,
            maximum=1.0,
        ),
        min_conf=_parse_float_arg(
            args.get("min_conf"),
            defaults.min_conf,
            minimum=0.0,
            maximum=1.0,
        ),
        max_age=_parse_float_arg(
            args.get("max_age"),
            defaults.max_age,
            minimum=0.0,
        ),
        max_tick_age=_parse_float_arg(
            args.get("max_tick_age"),
            defaults.max_tick_age,
            minimum=0.0,
        ),
    )


def _opportunity_base_params(
    limit: int,
    filters: "PortalFilters",
    args: OpportunityArgs,
    *,
    include_category: bool = False,
    view: str | None = None,
) -> dict[str, object]:
    base_params = build_filter_params(
        limit,
        filters,
        include_category=include_category,
    )
    if args.min_gap is not None:
        base_params["min_gap"] = f"{args.min_gap:g}"
    if args.min_conf is not None:
        base_params["min_conf"] = f"{args.min_conf:g}"
    if args.max_age is not None:
        base_params["max_age"] = f"{args.max_age:g}"
    if args.max_tick_age is not None:
        base_params["max_tick_age"] = f"{args.max_tick_age:g}"
    if view:
        base_params["view"] = view
    return base_params


def _empty_opportunity_data(error: str | None = None) -> OpportunityData:
    return OpportunityData(
        rows=[],
        strike_periods=[],
        active_categories=[],
        error=error,
    )


def _fetch_opportunity_data(
    limit: int,
    filters: "PortalFilters",
    criteria: object,
) -> OpportunityData:
    try:
        with _db_connection() as conn:
            rows = fetch_opportunities(conn, limit, filters, criteria=criteria)
            strike_periods = fetch_strike_periods(conn)
            active_categories = fetch_active_event_categories(conn, filters)
        return OpportunityData(
            rows=rows,
            strike_periods=strike_periods,
            active_categories=active_categories,
            error=None,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("opportunities query failed")
        return _empty_opportunity_data(str(exc))


def _fetch_arbitrage_data(
    limit: int,
    filters: "PortalFilters",
    criteria: object,
) -> OpportunityData:
    try:
        with _db_connection() as conn:
            rows = fetch_arbitrage_opportunities(conn, limit, filters, criteria=criteria)
            strike_periods = fetch_strike_periods(conn)
            active_categories = fetch_active_event_categories(conn, filters)
        return OpportunityData(
            rows=rows,
            strike_periods=strike_periods,
            active_categories=active_categories,
            error=None,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("arbitrage query failed")
        return _empty_opportunity_data(str(exc))


@bp.get("/opportunities")
def opportunities():
    """Render the opportunity listing page."""
    db_url = os.getenv("DATABASE_URL")
    limit = clamp_limit(
        request.args.get("limit") or os.getenv("WEB_PORTAL_OPPORTUNITY_LIMIT")
    )
    filters = _parse_portal_filters(request.args)
    now = datetime.now(timezone.utc)
    args = _parse_opportunity_args(request.args, _opportunity_defaults())
    view = _parse_opportunity_view(request.args)
    criteria = build_opportunity_filters(
        min_gap=args.min_gap,
        min_confidence=args.min_conf,
        max_prediction_age_minutes=args.max_age,
        max_tick_age_minutes=args.max_tick_age,
    )
    arb_criteria = build_arbitrage_filters(
        max_tick_age_minutes=args.max_tick_age,
    )
    selected_categories = list(filters.categories)
    tab_params = _opportunity_base_params(
        limit,
        filters,
        args,
        include_category=True,
    )
    tab_links = {
        "model": url_for("opportunities.opportunities", **{**tab_params, "view": "model"}),
        "arbitrage": url_for(
            "opportunities.opportunities",
            **{**tab_params, "view": "arbitrage"},
        ),
    }
    if not db_url:
        empty_data = _empty_opportunity_data("DATABASE_URL is not set.")
        row_count = 0
        return render_template(
            "opportunities.html",
            error=empty_data.error,
            opportunities=empty_data.rows,
            arbitrage_opportunities=[],
            refreshed_at=fmt_ts(now),
            logged_in=is_authenticated(),
            search=filters.search or "",
            selected_categories=selected_categories,
            selected_strike_period=filters.strike_period or "",
            strike_periods=empty_data.strike_periods,
            category_filters=[],
            min_gap=args.min_gap,
            min_conf=args.min_conf,
            max_age=args.max_age,
            max_tick_age=args.max_tick_age,
            limit=limit,
            active_tab=view,
            tab_links=tab_links,
            row_count=row_count,
        )

    if view == "arbitrage":
        data = _fetch_arbitrage_data(limit, filters, arb_criteria)
        opportunities = []
        arbitrage_opportunities = data.rows
    else:
        data = _fetch_opportunity_data(limit, filters, criteria)
        opportunities = data.rows
        arbitrage_opportunities = []

    base_params = _opportunity_base_params(
        limit,
        filters,
        args,
        view=view,
    )
    category_filters = build_category_filters(
        active_categories=data.active_categories,
        selected_categories=selected_categories,
        base_params=base_params,
        endpoint="opportunities.opportunities",
        url_for=url_for,
    )
    row_count = len(arbitrage_opportunities) if view == "arbitrage" else len(opportunities)
    return render_template(
        "opportunities.html",
        error=data.error,
        opportunities=opportunities,
        arbitrage_opportunities=arbitrage_opportunities,
        refreshed_at=fmt_ts(now),
        logged_in=is_authenticated(),
        search=filters.search or "",
        selected_categories=selected_categories,
        selected_strike_period=filters.strike_period or "",
        strike_periods=data.strike_periods,
        category_filters=category_filters,
        min_gap=args.min_gap,
        min_conf=args.min_conf,
        max_age=args.max_age,
        max_tick_age=args.max_tick_age,
        limit=limit,
        active_tab=view,
        tab_links=tab_links,
        row_count=row_count,
    )


@bp.get("/opportunities/data")
def opportunities_data():
    """Return opportunity data as JSON for incremental updates."""
    db_url = os.getenv("DATABASE_URL")
    limit = clamp_limit(
        request.args.get("limit") or os.getenv("WEB_PORTAL_OPPORTUNITY_LIMIT")
    )
    filters = _parse_portal_filters(request.args)
    now = datetime.now(timezone.utc)
    args = _parse_opportunity_args(request.args, _opportunity_defaults())
    view = _parse_opportunity_view(request.args)
    criteria = build_opportunity_filters(
        min_gap=args.min_gap,
        min_confidence=args.min_conf,
        max_prediction_age_minutes=args.max_age,
        max_tick_age_minutes=args.max_tick_age,
    )
    arb_criteria = build_arbitrage_filters(
        max_tick_age_minutes=args.max_tick_age,
    )
    selected_categories = list(filters.categories)
    if not db_url:
        data = _empty_opportunity_data("DATABASE_URL is not set.")
        return jsonify(
            {
                "error": data.error,
                "opportunities": [],
                "arbitrage": [],
                "count": 0,
                "view": view,
                "refreshed_at": fmt_ts(now),
                "logged_in": is_authenticated(),
                "search": filters.search or "",
                "selected_categories": selected_categories,
                "selected_strike_period": filters.strike_period or "",
                "strike_periods": data.strike_periods,
                "category_filters": [],
                "min_gap": args.min_gap,
                "min_conf": args.min_conf,
                "max_age": args.max_age,
                "max_tick_age": args.max_tick_age,
                "limit": limit,
            }
        )

    if view == "arbitrage":
        data = _fetch_arbitrage_data(limit, filters, arb_criteria)
        opportunities = []
        arbitrage = data.rows
    else:
        data = _fetch_opportunity_data(limit, filters, criteria)
        opportunities = data.rows
        arbitrage = []

    base_params = _opportunity_base_params(
        limit,
        filters,
        args,
        view=view,
    )
    category_filters = build_category_filters(
        active_categories=data.active_categories,
        selected_categories=selected_categories,
        base_params=base_params,
        endpoint="opportunities.opportunities",
        url_for=url_for,
    )
    return jsonify(
        {
            "error": data.error,
            "opportunities": opportunities,
            "arbitrage": arbitrage,
            "count": len(arbitrage) if view == "arbitrage" else len(opportunities),
            "view": view,
            "refreshed_at": fmt_ts(now),
            "logged_in": is_authenticated(),
            "search": filters.search or "",
            "selected_categories": selected_categories,
            "selected_strike_period": filters.strike_period or "",
            "strike_periods": data.strike_periods,
            "category_filters": category_filters,
            "min_gap": args.min_gap,
            "min_conf": args.min_conf,
            "max_age": args.max_age,
            "max_tick_age": args.max_tick_age,
            "limit": limit,
        }
    )
