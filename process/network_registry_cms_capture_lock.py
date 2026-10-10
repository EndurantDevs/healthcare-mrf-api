# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Hold native capture custody before opening one fresh owner transaction."""

import asyncio
from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import AsyncAdaptedQueuePool

from process import provider_directory_owned_wal_transaction as owned
from process.uhc_flex_practitioner_async_safety import drain_operation


def _source_engine(source_session_factory, capture_id):
    if type(capture_id) is not UUID or capture_id.int == 0 or type(source_session_factory) is not async_sessionmaker:
        raise ValueError("registry_cms_capture_transaction_invalid")
    engine = source_session_factory.kw.get("bind")
    if (
        type(engine) is not AsyncEngine
        or source_session_factory.class_ is not AsyncSession
        or source_session_factory.kw.get("binds")
        or engine.dialect.name != "postgresql"
        or engine.dialect.driver != "asyncpg"
        or type(engine.pool) is not AsyncAdaptedQueuePool
        or engine.pool.size() != 1
        or engine.pool._max_overflow != 0
    ):
        raise ValueError("registry_cms_capture_transaction_invalid")
    if engine.pool.checkedout():
        raise ValueError("registry_cms_capture_busy")
    return engine


async def _start_connection(connection, retirement):
    if connection.engine.pool.checkedout():
        raise ValueError("registry_cms_capture_busy")
    async with asyncio.timeout(5):
        await connection.start()
        driver = (await connection.get_raw_connection()).driver_connection
        retirement.retained_driver = driver
        retirement.retained_pid = owned._driver_identity(driver, outside_transaction=True)


async def _unlock_capture(connection, key):
    if connection.in_transaction():
        raise ValueError("registry_cms_capture_transaction_invalid")
    await connection.execution_options(isolation_level="AUTOCOMMIT")
    released = await connection.scalar(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
    await connection.commit()
    if released is not True:
        raise ValueError("registry_cms_capture_unlock_failed")


async def _finish_connection(connection, key, has_failed, retirement):
    if connection.sync_connection is None:
        return
    driver = retirement.retained_driver
    if has_failed:
        await drain_operation(
            owned._retire_owned_connection(connection, driver, retirement), preserve_cancellation=False
        )
        return
    try:
        await drain_operation(_unlock_capture(connection, key), preserve_cancellation=True)
        await drain_operation(connection.close(), preserve_cancellation=True)
    except BaseException as failure:
        if not connection.closed:
            await drain_operation(
                owned._retire_preserving_failure(connection, driver, retirement, failure), preserve_cancellation=False
            )
        raise


@asynccontextmanager
async def registry_cms_capture_transaction(source_session_factory, *, capture_id, fhir=None):
    """Yield one owned RR session after taking the signed capture's native lock.

    The trusted factory must own a one-connection native publisher pool. Callers
    must leave commit/rollback to this context. This lock grants no cleanup or
    terminal-owner authority. A failed or cancelled outcome retires its physical
    connection; committed retained candidates still require a fresh probe.
    """
    engine = _source_engine(source_session_factory, capture_id)
    key = int.from_bytes(capture_id.bytes[:8], "big", signed=True)
    connection = engine.connect()
    has_failed = True
    outcome = failure = None
    retirement = owned.OwnedWalTransaction(connection=connection)
    try:
        await drain_operation(_start_connection(connection, retirement), preserve_cancellation=True)
        await connection.execution_options(isolation_level="AUTOCOMMIT")
        acquired = await connection.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": key})
        await connection.commit()
        if acquired is not True:
            raise ValueError("registry_cms_capture_busy")
        await connection.execution_options(isolation_level="REPEATABLE READ")
        if fhir is None:
            async with source_session_factory(bind=connection) as session, session.begin():
                yield session
        else:
            from process import provider_directory_cms_publication_custody as wal_custody
            from process.provider_directory_owned_wal_transaction import OwnedWalTransaction

            async with source_session_factory(bind=connection) as session:
                outcome = OwnedWalTransaction(session=session, connection=connection)
                async with wal_custody.capture_transaction(fhir, connection, session, outcome):
                    yield session
        has_failed = False
    except BaseException as error:
        failure = error
        raise
    finally:
        cleanup_error = None
        try:
            await _finish_connection(connection, key, has_failed, retirement)
        except BaseException as error:
            cleanup_error = error
        try:
            if outcome is not None:
                wal_custody.finish_capture(fhir, outcome, failure or cleanup_error)
        except BaseException as error:
            cleanup_error = cleanup_error or error
        if cleanup_error is not None:
            if failure is None:
                raise cleanup_error
            failure.add_note("registry_cms_capture_cleanup_incomplete")
