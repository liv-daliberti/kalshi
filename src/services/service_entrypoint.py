"""Shared helpers for service entrypoints."""

from __future__ import annotations

import asyncio
import os
from typing import Callable, Optional
from types import SimpleNamespace

def _missing_dependency(label: str, exc: Exception) -> callable:
    def _raise(*_args, **_kwargs):
        raise RuntimeError(f"service_entrypoint: unable to import {label}") from exc

    return _raise


try:  # Allow running as a script without a package context.
    from . import main as ingestor_main
except Exception as exc:  # pragma: no cover - import-time dependencies missing
    try:  # pragma: no cover - fallback for __main__ execution
        import src.services.main as ingestor_main
    except Exception as exc2:  # pragma: no cover - test fallback
        ingestor_main = SimpleNamespace(main=_missing_dependency("services.main", exc2))

try:
    from ..core.guardrails import assert_env_isolated
except Exception as exc:  # pragma: no cover - test fallback
    assert_env_isolated = _missing_dependency("core.guardrails.assert_env_isolated", exc)

try:
    from .healthcheck import start_health_server
except Exception as exc:  # pragma: no cover - test fallback
    start_health_server = _missing_dependency("services.healthcheck.start_health_server", exc)


def run_service(
    *,
    role: str,
    run_mode: str,
    forbidden_prefixes: tuple[str, ...],
    forbidden_keys: tuple[str, ...] = (),
    main_fn: Optional[Callable[[], object]] = None,
    asyncio_run: Optional[Callable[[object], object]] = None,
    start_health: Optional[Callable[[str], object]] = None,
) -> None:
    """Run a service entrypoint with shared setup."""
    if main_fn is None:
        main_fn = ingestor_main.main
    if asyncio_run is None:
        asyncio_run = asyncio.run
    if start_health is None:
        start_health = start_health_server
    os.environ["KALSHI_RUN_MODE"] = run_mode
    os.environ.setdefault("SERVICE_ROLE", role)
    os.environ.setdefault("DB_INIT_SCHEMA", "0")
    assert_env_isolated(
        role,
        forbidden_prefixes=forbidden_prefixes,
        forbidden_keys=forbidden_keys,
    )
    start_health(role)
    asyncio_run(main_fn())
