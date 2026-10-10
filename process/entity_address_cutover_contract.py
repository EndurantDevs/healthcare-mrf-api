# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded transaction helpers and callbacks for entity-address preparation and cutover."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any


class _ServingRelationLockTimeout(TimeoutError):
    sqlstate = "55P03"


async def lock_live_serving_relations(execute, schema, relation_names):
    """Drain current readers in their lock order, with one short total wait budget.

    Private stages and retired tables still require NOWAIT ownership checks. The
    caller must roll back its whole transaction before retrying a failed lock.
    """
    if not relation_names:
        return
    quoted_schema = schema.replace('"', '""')
    quoted_names = [name.replace('"', '""') for name in sorted(set(relation_names))]
    qualified_names = ", ".join(f'"{quoted_schema}"."{name}"' for name in quoted_names)
    try:
        async with asyncio.timeout(0.1):
            await execute(f"LOCK TABLE {qualified_names} IN ACCESS EXCLUSIVE MODE")
    except TimeoutError as error:
        raise _ServingRelationLockTimeout("serving_relation_lock_timeout") from error


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
    *,
    temp_file_limit_bytes: int | None = None,
):
    """Apply statement tuning without leaking it into a borrowed transaction."""

    if temp_file_limit_bytes is not None:
        async with _admitted_temp_transaction(database, settings, quote_literal, logger, temp_file_limit_bytes):
            yield
        return

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
async def _admitted_temp_transaction(database, settings, quote_literal, logger, limit_bytes):
    """Keep an explicit admitted cap on its owner's backend without direct SET rights."""
    from process.provider_directory_profile_temp_limit import apply_temp_file_limit, require_temp_file_limit_capability

    if (
        type(limit_bytes) is not int
        or limit_bytes <= 0
        or limit_bytes % 1024
        or [setting_value for name, setting_value in settings if name == "temp_file_limit"]
        != [f"{limit_bytes // 1024}kB"]
    ):
        raise ValueError("entity_address_admitted_temp_limit_invalid")
    transaction_binding = getattr(database, "_transaction_binding", None)
    if not callable(transaction_binding):
        # ConnectionProxy.transaction merely borrows an existing owner. It cannot
        # establish the transaction-end reset required by this entry point.
        raise RuntimeError("entity_address_admitted_temp_limit_owner_required")
    is_borrowed_transaction = transaction_binding() is not None
    previous = None
    other_settings = [(name, setting_value) for name, setting_value in settings if name != "temp_file_limit"]
    async with database.transaction():
        mode = await require_temp_file_limit_capability(database)
        if is_borrowed_transaction:
            previous = await database.scalar("SELECT pg_size_bytes(current_setting('temp_file_limit'))::bigint")
            if type(previous) is not int or (previous < 0 and mode != "direct"):
                raise RuntimeError("entity_address_admitted_temp_limit_bounded_owner_required")
        await apply_temp_file_limit(database, limit_bytes)
        async with entity_address_tuned_transaction(database, other_settings, quote_literal, logger):
            yield
        if is_borrowed_transaction:
            if previous < 0:
                await database.status("SET LOCAL temp_file_limit = '-1';")
            else:
                await apply_temp_file_limit(database, previous)


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


async def wait_for_publication_lock(error: Exception, attempt: int, *, max_attempts: int = 4) -> None:
    """Back off only after a whole failed publication transaction has rolled back."""
    if attempt >= max_attempts or postgres_sqlstate(error) != "55P03":
        raise error
    await asyncio.sleep(min(25 * (2 ** (attempt - 1)), 100) / 1000)
