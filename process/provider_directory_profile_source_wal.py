# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose issued SOURCE custody with the existing original WAL owner."""

import asyncio
from contextlib import asynccontextmanager

from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_connection

from process import provider_directory_generic_source_policy as source
from process import provider_directory_owned_wal_transaction as owned
from process.uhc_flex_practitioner_async_safety import drain_operation


async def _isolation_origin(connection):
    """Retain the authenticated checkpoint backend before any work transaction."""
    raw = await connection.get_raw_connection()
    driver, adapter = raw.driver_connection, raw.dbapi_connection
    if type(adapter) is not AsyncAdapt_asyncpg_connection or adapter.driver_connection is not driver:
        raise RuntimeError("provider_directory_source_wal_isolation_custody_changed")
    pid = owned._driver_identity(driver, outside_transaction=True)
    origin = asyncio.current_task(), source._FACTORY.get(), driver, pid, adapter
    await _require_isolation_origin(connection, origin)
    return origin


async def _require_isolation_origin(connection, origin):
    task, factory, driver, pid, adapter = origin
    if (
        asyncio.current_task() is not task
        or source._FACTORY.get() is not factory
        or connection.closed
        or connection.invalidated
        or connection.in_transaction()
        or connection.in_nested_transaction()
    ):
        raise RuntimeError("provider_directory_source_wal_isolation_custody_changed")
    raw = await connection.get_raw_connection()
    if (
        raw.driver_connection is not driver
        or raw.dbapi_connection is not adapter
        or adapter.driver_connection is not driver
    ):
        raise RuntimeError("provider_directory_source_wal_isolation_custody_changed")
    owned._driver_identity(driver, pid, outside_transaction=True)


async def _read_isolation(connection, origin):
    """Read the original adapter mode without starting an unmanaged transaction."""
    await _require_isolation_origin(connection, origin)
    cached = origin[4].isolation_level
    isolation = cached
    if cached is None:
        observed = await origin[2].fetchval("SHOW transaction_isolation")
        isolation = (
            observed.replace(" ", "_")
            if type(observed) is str and observed in {"read committed", "repeatable read", "serializable"}
            else None
        )
    await _require_isolation_origin(connection, origin)
    # Only the installed asyncpg dialect's transaction modes can be restored.
    if (
        origin[4].isolation_level != cached
        or type(isolation) is not str
        or isolation not in {"read_committed", "repeatable_read", "serializable"}
    ):
        raise RuntimeError("provider_directory_source_wal_isolation_changed")
    return isolation.replace("_", " ").upper()


async def _set_isolation(connection, origin, isolation):
    await _require_isolation_origin(connection, origin)
    if await connection.execution_options(isolation_level=isolation) is not connection:
        raise RuntimeError("provider_directory_source_wal_isolation_connection_changed")
    await _require_isolation_origin(connection, origin)
    if await _read_isolation(connection, origin) != isolation:
        raise RuntimeError("provider_directory_source_wal_isolation_changed")
    await _require_isolation_origin(connection, origin)


async def _restore_isolation(connection, origin, original, outcome):
    """Restore healthy caller custody; failed restoration retires only this backend."""
    driver = origin[2]
    if connection.closed and driver.is_closed() is True and outcome is not None and outcome.cleanup_complete:
        return
    try:
        await _set_isolation(connection, origin, original)
    except BaseException as error:
        if outcome is not None:
            owned._incomplete(outcome)
            outcome.cleanup_complete = False
            await drain_operation(
                owned._retire_preserving_failure(connection, driver, outcome, error), preserve_cancellation=True
            )
        else:
            owned._terminate_owned_driver(driver)
            if not connection.closed:
                await connection.invalidate()
                await connection.close()
        raise


@asynccontextmanager
async def _source_isolation(connection, isolation, captured_outcomes):
    """Select before native BEGIN, restore after terminal WAL cleanup, retain failures."""
    origin = await _isolation_origin(connection)
    original = await _read_isolation(connection, origin)
    await _require_isolation_origin(connection, origin)
    failure = None
    try:
        await _set_isolation(connection, origin, isolation)
        yield
    except BaseException as error:
        failure = error
        raise
    finally:
        outcome = captured_outcomes[0]
        try:
            await _restore_isolation(connection, origin, original, outcome)
        except BaseException as cleanup_error:
            failures = BaseExceptionGroup(
                "provider_directory_source_wal_isolation_cleanup_failed",
                [failure, cleanup_error] if failure is not None else [cleanup_error],
            )
            is_cancelled = isinstance(failure, asyncio.CancelledError) or isinstance(
                cleanup_error, asyncio.CancelledError
            )
            if outcome is None:
                if is_cancelled:
                    raise asyncio.CancelledError("provider_directory_source_wal_isolation_cancelled") from failures
                raise failures
            error_type = owned.OwnedWalTransactionCancelled if is_cancelled else owned.OwnedWalTransactionError
            raise error_type(outcome) from failures


@asynccontextmanager
async def source_owned_wal_transaction(directory, *, isolation="read committed"):
    """Keep one genuine SOURCE backend through body, top-level commit and sampling."""
    source._require(
        type(isolation) is str and isolation in {"read committed", "serializable"}, "owned_wal_isolation_invalid"
    )
    source.source_producer_identity()
    factory = source._FACTORY.get()
    source._require(callable(getattr(factory, "bind_existing_transaction", None)), "owned_wal_factory_required")
    async with source.source_checkpoint_guard(directory.db) as (connection, _transaction):
        captured_outcomes = [None]
        async with _source_isolation(connection, isolation.upper(), captured_outcomes):
            async with owned.registry_retained_wal_transaction(connection) as outcome:
                captured_outcomes[0] = outcome
                async with (
                    owned.bind_retained_wal_transaction(directory.db, outcome) as session,
                    factory.bind_existing_transaction(directory.db, session, isolation=isolation),
                ):
                    source._require(source._FACTORY.get() is factory, "policy_changed")
                    yield outcome
                    source._require(source._FACTORY.get() is factory, "policy_changed")
                source._require(source._FACTORY.get() is factory, "policy_changed")
