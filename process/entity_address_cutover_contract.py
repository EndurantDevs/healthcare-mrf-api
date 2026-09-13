# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded callback contract for a caller-owned entity-address cutover."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EntityAddressCutoverCallbacks:
    """Run a local fence before cutover and a native receipt write after publish."""

    before_cutover: Callable[[], Awaitable[None]] | None = None
    after_publish: Callable[[], Awaitable[None]] | None = None


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
