# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded transaction helpers and callbacks for entity-address preparation and cutover."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
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


async def apply_transaction_sql_settings(database: Any, settings, quote_literal, logger) -> tuple[str, ...]:
    """Apply each local SQL setting inside a savepoint without escaping the caller transaction."""
    applied_setting_names = []
    for name, value in settings:
        try:
            async with database.transaction():
                await database.status(f"SET LOCAL {name} = {quote_literal(value)};")
        except Exception as exc:
            if "permission denied to set parameter" not in str(exc).lower():
                raise
            logger.warning("Skipping unprivileged entity-address SQL setting %s=%s: %s", name, value, exc)
        else:
            applied_setting_names.append(name)
    return tuple(applied_setting_names)


@asynccontextmanager
async def preserve_transaction_sql_settings(
    database: Any,
    setting_names,
    quote_literal,
    *,
    applied_setting_names: set[str] | None = None,
):
    """Restore a borrowed transaction's settings after one nested operation."""

    previous_settings = []
    for setting_name in setting_names:
        setting_value = await database.scalar(
            "SELECT current_setting(:setting_name)",
            setting_name=setting_name,
        )
        previous_settings.append((setting_name, str(setting_value)))
    async with database.transaction():
        yield
        for setting_name, setting_value in previous_settings:
            if applied_setting_names is not None and setting_name not in applied_setting_names:
                continue
            await database.status(f"SET LOCAL {setting_name} = {quote_literal(setting_value)};")


@asynccontextmanager
async def entity_address_tuned_transaction(
    database: Any,
    settings,
    quote_literal,
    logger,
):
    """Apply statement tuning without leaking it into a borrowed transaction."""

    setting_names = [name for name, _value in settings]
    applied_setting_names: set[str] = set()
    async with preserve_transaction_sql_settings(
        database,
        setting_names,
        quote_literal,
        applied_setting_names=applied_setting_names,
    ):
        applied_settings = await apply_transaction_sql_settings(
            database,
            settings,
            quote_literal,
            logger,
        )
        applied_setting_names.update(applied_settings)
        yield


@asynccontextmanager
async def entity_address_cutover_transaction(
    database: Any,
    lock_timeout: str,
    quote_literal,
):
    """Apply a required cutover timeout and preserve a borrowed caller setting."""

    transaction_binding = getattr(database, "_transaction_binding", None)
    if not callable(transaction_binding) or transaction_binding() is None:
        async with database.transaction():
            await database.status(f"SET LOCAL lock_timeout = {quote_literal(lock_timeout)};")
            yield
        return
    async with preserve_transaction_sql_settings(
        database,
        ["lock_timeout"],
        quote_literal,
    ):
        await database.status(f"SET LOCAL lock_timeout = {quote_literal(lock_timeout)};")
        yield


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
