# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded reader/writer fences for PTG snapshot lifecycle mutations."""

from __future__ import annotations

from typing import Any

from db.connection import db
from process.ptg_parts.snapshot_tables import _normalize_source_key


PTG2_SOURCE_POINTER_GC_LOCK_KEY = "ptg2_source_pointer_gc_v1"
PTG2_SOURCE_LIFECYCLE_LOCK_NAMESPACE = "ptg2_source_lifecycle_v2"
PTG2_LIFECYCLE_LOCK_TIMEOUT = "500ms"
PTG2_LIFECYCLE_STATEMENT_TIMEOUT = "5s"
_RETRYABLE_SQLSTATES = frozenset({"55P03", "57014"})


class PTG2LifecycleLockDeferred(RuntimeError):
    """A bounded lifecycle fence was busy and this source should retry."""

    retryable = True


async def _execute_lifecycle_statement(
    session: Any,
    statement: Any,
    parameters: dict[str, Any],
) -> Any:
    execute = getattr(session, "execute", None)
    if execute is not None:
        return await execute(statement, parameters)
    status = getattr(session, "status", None)
    if status is None:
        raise TypeError("PTG lifecycle executor cannot execute SQL")
    return await status(str(statement), **parameters)


def lifecycle_database_sqlstate(error: BaseException) -> str:
    """Return a PostgreSQL state from common driver/wrapper exception chains."""

    pending_errors = [error]
    visited_error_ids: set[int] = set()
    while pending_errors:
        candidate = pending_errors.pop()
        if id(candidate) in visited_error_ids:
            continue
        visited_error_ids.add(id(candidate))
        for field in ("sqlstate", "pgcode"):
            value = getattr(candidate, field, None)
            if value:
                return str(value)
        for field in ("orig", "__cause__", "__context__"):
            nested = getattr(candidate, field, None)
            if isinstance(nested, BaseException):
                pending_errors.append(nested)
    return ""


def is_retryable_lifecycle_database_error(error: BaseException) -> bool:
    """Return whether PostgreSQL classified the bounded wait as retryable."""

    return lifecycle_database_sqlstate(error) in _RETRYABLE_SQLSTATES


async def configure_ptg2_lifecycle_transaction(
    session: Any,
    *,
    lock_timeout: str = PTG2_LIFECYCLE_LOCK_TIMEOUT,
    statement_timeout: str = PTG2_LIFECYCLE_STATEMENT_TIMEOUT,
) -> None:
    """Bound lock and statement waits before touching lifecycle state."""

    await _execute_lifecycle_statement(
        session,
        db.text(
            "SELECT set_config('lock_timeout', :lock_timeout, true), "
            "set_config('statement_timeout', :statement_timeout, true)"
        ),
        {
            "lock_timeout": lock_timeout,
            "statement_timeout": statement_timeout,
        },
    )


async def acquire_ptg2_source_lifecycle_lock(
    session: Any,
    *,
    source_key: str,
    lock_timeout: str = PTG2_LIFECYCLE_LOCK_TIMEOUT,
    statement_timeout: str = PTG2_LIFECYCLE_STATEMENT_TIMEOUT,
) -> str:
    """Fence GC globally while serializing only one normalized source."""

    normalized_source_key = _normalize_source_key(source_key)
    if not normalized_source_key:
        raise ValueError("PTG lifecycle source key is required")
    try:
        await configure_ptg2_lifecycle_transaction(
            session,
            lock_timeout=lock_timeout,
            statement_timeout=statement_timeout,
        )
        await _execute_lifecycle_statement(
            session,
            db.text(
                "SELECT pg_advisory_xact_lock_shared("
                "hashtext(:gc_lock_key))"
            ),
            {"gc_lock_key": PTG2_SOURCE_POINTER_GC_LOCK_KEY},
        )
        await _execute_lifecycle_statement(
            session,
            db.text(
                "SELECT pg_advisory_xact_lock("
                "hashtextextended(:source_lock_key, 0))"
            ),
            {
                "source_lock_key": (
                    f"{PTG2_SOURCE_LIFECYCLE_LOCK_NAMESPACE}:"
                    f"{normalized_source_key}"
                )
            },
        )
    except Exception as exc:
        if not is_retryable_lifecycle_database_error(exc):
            raise
        raise PTG2LifecycleLockDeferred(
            f"PTG lifecycle fence is busy for {normalized_source_key}; retry"
        ) from exc
    return normalized_source_key


async def acquire_ptg2_lifecycle_lock(
    session: Any,
    *,
    lock_timeout: str = PTG2_LIFECYCLE_LOCK_TIMEOUT,
    statement_timeout: str = PTG2_LIFECYCLE_STATEMENT_TIMEOUT,
) -> None:
    """Acquire the bounded global-exclusive fence reserved for garbage collection."""

    try:
        await configure_ptg2_lifecycle_transaction(
            session,
            lock_timeout=lock_timeout,
            statement_timeout=statement_timeout,
        )
        await _execute_lifecycle_statement(
            session,
            db.text(
                "SELECT pg_advisory_xact_lock("
                "hashtext(:gc_lock_key))"
            ),
            {"gc_lock_key": PTG2_SOURCE_POINTER_GC_LOCK_KEY},
        )
    except Exception as exc:
        if not is_retryable_lifecycle_database_error(exc):
            raise
        raise PTG2LifecycleLockDeferred(
            "PTG global garbage-collection fence is busy; retry"
        ) from exc


__all__ = [
    "PTG2LifecycleLockDeferred",
    "PTG2_LIFECYCLE_LOCK_TIMEOUT",
    "PTG2_LIFECYCLE_STATEMENT_TIMEOUT",
    "PTG2_SOURCE_LIFECYCLE_LOCK_NAMESPACE",
    "PTG2_SOURCE_POINTER_GC_LOCK_KEY",
    "acquire_ptg2_lifecycle_lock",
    "acquire_ptg2_source_lifecycle_lock",
    "configure_ptg2_lifecycle_transaction",
    "is_retryable_lifecycle_database_error",
    "lifecycle_database_sqlstate",
]
