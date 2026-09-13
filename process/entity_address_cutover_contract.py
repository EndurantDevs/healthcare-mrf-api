# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded transaction helpers and callbacks for entity-address preparation and cutover."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EntityAddressCutoverCallbacks:
    """Run a local fence before cutover and a native receipt write after publish."""

    before_cutover: Callable[[], Awaitable[None]] | None = None
    after_publish: Callable[[], Awaitable[None]] | None = None


async def run_publish_validation_operations(database: Any, *operations):
    """Keep borrowed-session queries in their owning task; otherwise retain concurrency."""
    transaction_binding = getattr(database, "_transaction_binding", None)
    if callable(transaction_binding) and transaction_binding() is not None:
        return tuple([await operation() for operation in operations])
    return await asyncio.gather(*(operation() for operation in operations))


async def apply_transaction_sql_settings(database: Any, settings, quote_literal, logger) -> None:
    """Apply each local SQL setting inside a savepoint without escaping the caller transaction."""
    for name, value in settings:
        try:
            async with database.transaction():
                await database.status(f"SET LOCAL {name} = {quote_literal(value)};")
        except Exception as exc:
            if "permission denied to set parameter" not in str(exc).lower():
                raise
            logger.warning("Skipping unprivileged entity-address SQL setting %s=%s: %s", name, value, exc)


def require_caller_owned_cutover_transaction(database: Any) -> None:
    """Reject a cutover that would create and commit its own outer transaction."""

    transaction_binding = getattr(database, "_transaction_binding", None)
    if not callable(transaction_binding) or transaction_binding() is None:
        raise RuntimeError("entity-address snapshot adoption requires a caller-owned database transaction")


def postgres_sqlstate(error: BaseException) -> str | None:
    """Return a PostgreSQL error code through common wrapper exception shapes."""

    original_error = getattr(error, "orig", None)
    candidates = (
        error,
        original_error,
        getattr(error, "__cause__", None),
        getattr(original_error, "__cause__", None),
    )
    for candidate in candidates:
        if candidate is None:
            continue
        sqlstate = getattr(candidate, "sqlstate", None) or getattr(candidate, "pgcode", None)
        if sqlstate:
            return str(sqlstate)
    return None
