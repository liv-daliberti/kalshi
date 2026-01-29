"""Portal data models and context containers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .portal_filters import PortalFilters


@dataclass(frozen=True)
class PortalRows:
    """Container for portal event rows."""

    active: list[dict[str, Any]]
    scheduled: list[dict[str, Any]]
    closed: list[dict[str, Any]]


@dataclass(frozen=True)
class PortalTotals:
    """Container for portal event totals."""

    active: int
    scheduled: int
    closed: int


@dataclass(frozen=True)
class PortalCursorTokens:
    """Cursor tokens for keyset pagination."""

    active: str | None
    scheduled: str | None
    closed: str | None


@dataclass(frozen=True)
class PortalDiagnostics:
    """Health/error details for portal payloads."""

    health: dict[str, Any] | None
    error: str | None


@dataclass(frozen=True, init=False)
class PortalData:
    """Container for portal query results."""

    rows: PortalRows
    totals: PortalTotals
    strike_periods: list[str]
    active_categories: list[str]
    diagnostics: PortalDiagnostics
    closed_filled_total: int = 0
    cursors: PortalCursorTokens | None = None

    def __init__(
        self,
        *,
        rows: PortalRows,
        totals: PortalTotals,
        strike_periods: list[str],
        active_categories: list[str],
        diagnostics: PortalDiagnostics | None = None,
        closed_filled_total: int = 0,
        cursors: PortalCursorTokens | None = None,
        health: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        if diagnostics is None:
            diagnostics = PortalDiagnostics(health=health, error=error)
        elif health is not None or error is not None:
            diagnostics = PortalDiagnostics(
                health=diagnostics.health if health is None else health,
                error=diagnostics.error if error is None else error,
            )
        object.__setattr__(self, "rows", rows)
        object.__setattr__(self, "totals", totals)
        object.__setattr__(self, "strike_periods", strike_periods)
        object.__setattr__(self, "active_categories", active_categories)
        object.__setattr__(self, "diagnostics", diagnostics)
        object.__setattr__(self, "closed_filled_total", closed_filled_total)
        object.__setattr__(self, "cursors", cursors)

    @property
    def health(self) -> dict[str, Any] | None:
        """Expose health for backward-compatible access."""
        return self.diagnostics.health

    @property
    def error(self) -> str | None:
        """Expose error for backward-compatible access."""
        return self.diagnostics.error


@dataclass(frozen=True)
class PortalPaging:
    """Pagination state for portal event tables."""

    limit: int
    active_page: int
    scheduled_page: int
    closed_page: int
    active_cursor: str | None = None
    scheduled_cursor: str | None = None
    closed_cursor: str | None = None

    @property
    def active_offset(self) -> int:
        """Return the row index for the active events page."""
        return self.active_page * self.limit

    @property
    def scheduled_offset(self) -> int:
        """Return the row index for the scheduled events page."""
        return self.scheduled_page * self.limit

    @property
    def closed_offset(self) -> int:
        """Return the row index for the closed events page."""
        return self.closed_page * self.limit

    def as_params(self) -> dict[str, Any]:
        """Serialize paging to URL parameters."""
        params: dict[str, Any] = {
            "active_page": self.active_page,
            "scheduled_page": self.scheduled_page,
            "closed_page": self.closed_page,
        }
        if self.active_cursor:
            params["active_after"] = self.active_cursor
        if self.scheduled_cursor:
            params["scheduled_after"] = self.scheduled_cursor
        if self.closed_cursor:
            params["closed_after"] = self.closed_cursor
        return params


@dataclass(frozen=True)
class PortalFilterContext:
    """Filter selections for portal template context."""

    selected_categories: list[str]
    category_filters: list[dict[str, Any]]
    filter_fields: list[dict[str, Any]]


@dataclass(frozen=True)
class PortalDataContext:
    """Portal data payload and pagination links."""

    data: PortalData
    load_more_links: dict[str, str]


@dataclass(frozen=True)
class PortalContextArgs:
    """Inputs for portal template context construction."""

    limit: int
    filters: "PortalFilters"
    paging: PortalPaging
    scope_note: str | None
    filter_context: PortalFilterContext
    data_context: PortalDataContext
    lazy_load: bool = False
