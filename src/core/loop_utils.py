"""Shared helpers for service loop modules."""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Any


def log_metric(logger: logging.Logger, name: str, **fields: Any) -> None:
    """Emit a structured metric log line."""
    parts = [f"{key}={value}" for key, value in fields.items()]
    suffix = " ".join(parts)
    logger.info("metric=%s %s", name, suffix)


def maybe_circuit_break(
    logger: logging.Logger,
    name: str,
    failures: int,
    threshold: int,
    cooldown: float,
) -> bool:
    """Return True when the circuit breaker trips and sleeps for cooldown."""
    if threshold <= 0:
        return False
    if failures >= threshold:
        logger.warning(
            "%s circuit open failures=%d cooldown_s=%.1f",
            name,
            failures,
            cooldown,
        )
        time.sleep(cooldown)
        return True
    return False


@dataclass(frozen=True)
class LoopFailure:
    """Context for logging a loop failure and deciding circuit behavior."""

    name: str
    duration_s: float
    error_total: int
    consecutive_failures: int
    failure_threshold: int
    breaker_seconds: float


def handle_loop_failure(logger: logging.Logger, failure: LoopFailure) -> bool:
    """Log a failure metric and trigger the circuit breaker when needed."""
    log_metric(
        logger,
        failure.name,
        duration_s=round(failure.duration_s, 2),
        success=0,
        errors_total=failure.error_total,
        consecutive_failures=failure.consecutive_failures,
    )
    return maybe_circuit_break(
        logger,
        failure.name,
        failure.consecutive_failures,
        failure.failure_threshold,
        failure.breaker_seconds,
    )


def bump_failure_counts(
    start: float,
    error_total: int,
    consecutive_failures: int,
) -> tuple[float, int, int]:
    """Return (duration_s, updated_error_total, updated_consecutive_failures)."""
    duration = time.monotonic() - start
    return duration, error_total + 1, consecutive_failures + 1


def handle_loop_exception(
    logger: logging.Logger,
    *,
    name: str,
    start: float,
    error_total: int,
    consecutive_failures: int,
    failure_threshold: int,
    breaker_seconds: float,
) -> tuple[float, int, int, bool]:
    """Update failure counters and apply circuit breaker behavior."""
    duration, error_total, consecutive_failures = bump_failure_counts(
        start,
        error_total,
        consecutive_failures,
    )
    failure = LoopFailure(
        name=name,
        duration_s=duration,
        error_total=error_total,
        consecutive_failures=consecutive_failures,
        failure_threshold=failure_threshold,
        breaker_seconds=breaker_seconds,
    )
    should_continue = handle_loop_failure(logger, failure)
    return duration, error_total, consecutive_failures, should_continue


@dataclass
class LoopFailureContext:
    """Track failure counts for a loop and apply circuit behavior."""

    name: str
    failure_threshold: int
    breaker_seconds: float
    error_total: int = 0
    consecutive_failures: int = 0

    def record_success(self, logger: logging.Logger, recovered_message: str) -> None:
        """Log recovery and reset consecutive failure counters."""
        if self.consecutive_failures:
            logger.info(recovered_message, self.consecutive_failures)
        self.consecutive_failures = 0

    def handle_exception(self, logger: logging.Logger, start: float) -> bool:
        """Update counters after an exception and return whether to continue."""
        _, self.error_total, self.consecutive_failures, should_continue = (
            handle_loop_exception(
                logger,
                name=self.name,
                start=start,
                error_total=self.error_total,
                consecutive_failures=self.consecutive_failures,
                failure_threshold=self.failure_threshold,
                breaker_seconds=self.breaker_seconds,
            )
        )
        if should_continue:
            self.consecutive_failures = 0
        return should_continue


def schema_path(module_file: str) -> str:
    """Return the schema.sql path relative to a module file."""
    module_dir = Path(module_file).resolve().parent
    for base in (module_dir, *module_dir.parents):
        candidate = base / "sql" / "schema.sql"
        if candidate.is_file():
            return str(candidate)
    return os.path.join(os.path.dirname(module_file), "..", "sql", "schema.sql")
