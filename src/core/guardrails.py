"""Service-level guardrails for write ownership enforcement."""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "t", "yes", "y", "on"}


def _env_truthy(value: Optional[str]) -> bool:
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY


def _guardrails_enabled() -> bool:
    return _env_truthy(os.getenv("SERVICE_GUARDRAILS"))


def _env_guardrail_mode() -> str:
    raw = os.getenv("SERVICE_ENV_GUARDRAILS")
    if raw is None:
        return "warn" if _guardrails_enabled() else "off"
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on", "strict", "enforce"}:
        return "enforce"
    if normalized in {"warn", "warning"}:
        return "warn"
    return "off"


def _parse_csv(raw: Optional[str]) -> tuple[str, ...]:
    if raw is None:
        return tuple()
    values = []
    for part in raw.split(","):
        value = part.strip()
        if value:
            values.append(value)
    return tuple(values)


def assert_env_isolated(
    service: str,
    forbidden_prefixes: tuple[str, ...] = (),
    forbidden_keys: tuple[str, ...] = (),
) -> None:
    """Warn or fail when forbidden env vars are present for a service."""
    mode = _env_guardrail_mode()
    if mode == "off":
        return
    prefixes = tuple(p.upper() for p in forbidden_prefixes if p)
    keys = {k.upper() for k in forbidden_keys if k}
    allow_prefixes = tuple(
        p.upper() for p in _parse_csv(os.getenv("SERVICE_ENV_ALLOW_PREFIXES")) if p
    )
    allow_keys = {k.upper() for k in _parse_csv(os.getenv("SERVICE_ENV_ALLOW_KEYS")) if k}
    found = set()
    for key, value in os.environ.items():
        if value is None or not str(value).strip():
            continue
        upper_key = key.upper()
        if upper_key in allow_keys:
            continue
        if allow_prefixes and any(upper_key.startswith(prefix) for prefix in allow_prefixes):
            continue
        if upper_key in keys:
            found.add(key)
            continue
        if prefixes and any(upper_key.startswith(prefix) for prefix in prefixes):
            found.add(key)
    if not found:
        return
    formatted = ", ".join(sorted(found))
    if mode == "enforce":
        raise RuntimeError(
            f"{service} env isolation violation: forbidden envs present: {formatted}"
        )
    logger.warning(
        "%s env isolation warning: forbidden envs present: %s",
        service,
        formatted,
    )


def _service_role() -> Optional[str]:
    role = os.getenv("SERVICE_ROLE")
    if role:
        return role.strip().lower()
    run_mode = os.getenv("KALSHI_RUN_MODE")
    if run_mode:
        run_mode = run_mode.strip().lower()
        if run_mode in {"rest", "ws", "worker", "rag"}:
            return run_mode
    return None


def _owners_for_state_key(key: str) -> Optional[set[str]]:
    if key.startswith("backfill_last_ts:"):
        return {"rest", "worker"}
    if key.startswith("ws_active_cursor:"):
        return {"ws"}
    owners_by_key = {
        "last_discovery_ts": {"rest"},
        "last_discovery_heartbeat_ts": {"rest"},
        "last_min_close_ts": {"rest", "worker"},
        "last_worker_ts": {"worker"},
        "last_tick_ts": {"ws"},
        "last_ws_tick_ts": {"ws"},
        "last_prediction_ts": {"rag"},
        "rag_24h_calls": {"rag"},
        "ws_heartbeat": {"ws"},
    }
    return owners_by_key.get(key)


def assert_state_write_allowed(key: str) -> None:
    """Enforce ingest_state ownership rules."""
    if not _guardrails_enabled():
        return
    role = _service_role()
    if not role:
        return
    owners = _owners_for_state_key(key)
    if owners is None:
        logger.warning("Guardrails: no owner mapping for ingest_state key=%s", key)
        return
    if role not in owners:
        raise PermissionError(
            f"Service role '{role}' cannot write ingest_state key '{key}'"
        )


def assert_queue_op_allowed(operation: str) -> None:
    """Enforce work_queue ownership rules."""
    if not _guardrails_enabled():
        return
    role = _service_role()
    if not role:
        return
    allowed: dict[str, set[str]] = {
        "enqueue": {"rest", "ws"},
        "claim": {"worker"},
        "complete": {"worker"},
        "maintenance": {"worker"},
    }
    owners = allowed.get(operation)
    if owners is None:
        logger.warning("Guardrails: no owner mapping for queue op=%s", operation)
        return
    if role not in owners:
        raise PermissionError(
            f"Service role '{role}' cannot run queue op '{operation}'"
        )


def assert_service_role(expected: str, context: str) -> None:
    """Enforce that a code path is executed by the expected service role."""
    if not _guardrails_enabled():
        return
    role = _service_role()
    if not role:
        return
    if role != expected:
        raise PermissionError(
            f"Service role '{role}' cannot execute {context} (expected '{expected}')"
        )
