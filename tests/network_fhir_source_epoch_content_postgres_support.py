# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Tiny native serialization and cleanup proofs for the eleven-table epoch hash."""

import asyncio
import os
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process import network_fhir_source_epoch as epoch

_PROFILES = (
    ("UTC", "ISO, YMD", "iso_8601", "3", "hex"),
    ("America/New_York", "German, DMY", "postgres", "0", "escape"),
    ("Pacific/Auckland", "SQL, MDY", "postgres_verbose", "-1", "hex"),
)


async def _settings(connection):
    return tuple(
        await connection.fetchval(
            "SELECT array_agg(current_setting(name) ORDER BY ordinal) "
            "FROM unnest($1::text[]) WITH ORDINALITY AS settings(name,ordinal)",
            [name for name, _ in epoch._CONTENT_SETTINGS],
        )
    )


@pytest.fixture
async def content_database():
    """Register exact schema cleanup before creating any synthetic native rows."""
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    schema = "epoch_content_test_" + uuid4().hex
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        for table in epoch._TABLES:
            await connection.execute(
                f'''CREATE TABLE "{schema}"."{table}" AS SELECT
                1 AS id,'2026-07-14 12:34:56.123456+00'::timestamptz AS published_at,
                '2026-07-14'::date AS source_date,'1 year 2 mons 3 days 04:05:06.7'::interval AS duration,
                1.2345678901234567::float8 AS score,decode('00ff5c','hex') AS payload'''
            )
        yield SimpleNamespace(connection=connection, schema=schema)
    finally:
        try:
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        finally:
            await connection.close()


async def verify_canonical_hash_and_profiles(content_database):
    """Alternate reader profiles retain identical bytes while real row changes differ."""
    connection, schema = content_database.connection, content_database.schema
    names = [name for name, _ in epoch._CONTENT_SETTINGS]
    async with connection.transaction():
        await epoch._set_content_settings(connection, names, _PROFILES[0])
        original = await epoch._content_chunks(connection, schema)
        for profile in _PROFILES:
            await epoch._set_content_settings(connection, names, profile)
            previous = await _settings(connection)
            assert await epoch._content(connection, schema) == original
            assert await _settings(connection) == previous
        await connection.execute(
            f'UPDATE "{schema}"."{epoch._TABLES[0]}" SET published_at=published_at+interval \'1 second\''
        )
        assert await epoch._content(connection, schema) != original
        assert await _settings(connection) == previous
        return original


async def verify_sql_error_restoration(content_database, monkeypatch):
    """A failed native scan rolls back only its savepoint and leaves the caller usable."""
    connection = content_database.connection
    observed_errors = []

    async def fail(driver, *_args):
        try:
            await driver.fetchval("SELECT 1/0")
        except asyncpg.DivisionByZeroError as error:
            observed_errors.append(error)
            raise

    monkeypatch.setattr(epoch, "_content_chunks", fail)
    async with connection.transaction():
        await epoch._set_content_settings(connection, [name for name, _ in epoch._CONTENT_SETTINGS], _PROFILES[1])
        previous = await _settings(connection)
        with pytest.raises(asyncpg.DivisionByZeroError) as failure:
            await epoch._content(connection, content_database.schema)
        assert failure.value is observed_errors[0]
        assert await _settings(connection) == previous
        assert await connection.fetchval("SELECT 1") == 1


class _PausedTransaction:
    """Pause real native boundaries so repeated cancellation is deterministic."""

    def __init__(self, transaction, boundary):
        self.transaction, self.boundary = transaction, boundary
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.rolled_back = False

    async def start(self):
        await self.transaction.start()
        if self.boundary == "start":
            self.entered.set()
            await self.release.wait()

    async def rollback(self):
        if self.boundary == "rollback":
            self.entered.set()
            await self.release.wait()
        await self.transaction.rollback()
        self.rolled_back = True

    async def commit(self):
        await self.transaction.commit()


async def _lock_caller_tables(connection, schema):
    """Acquire the same pre-savepoint table locks as the production custody check."""
    await connection.execute(
        "LOCK TABLE " + ",".join(f'"{schema}"."{table}"' for table in epoch._TABLES) + " IN ACCESS SHARE MODE NOWAIT"
    )
    await connection.execute("CREATE TEMP TABLE epoch_caller_work(value integer) ON COMMIT DROP")
    await connection.execute("INSERT INTO epoch_caller_work VALUES(42)")


async def _assert_caller_preserved(connection, schema, previous, search_path):
    """A settings rollback retains outer data, settings, snapshot and custody locks."""
    assert await _settings(connection) == previous
    assert await connection.fetchval("SHOW search_path") == search_path
    assert await connection.fetchval("SELECT value FROM epoch_caller_work") == 42
    assert await connection.fetchval("SHOW transaction_isolation") == "repeatable read"
    held = await connection.fetchval(
        "SELECT array_agg(c.relname::text ORDER BY c.relname) FROM pg_locks l "
        "JOIN pg_class c ON c.oid=l.relation JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE l.pid=pg_backend_pid() AND l.granted AND l.mode='AccessShareLock' AND n.nspname=$1",
        schema,
    )
    assert held == sorted(epoch._TABLES)


async def verify_catalog_restoration(content_database, *, fail=False):
    """Catalog success and native failure leave every borrowed caller resource intact."""
    connection, schema = content_database.connection, content_database.schema

    async def failed_fetch(*_args):
        return await connection.fetch("SELECT 1/0")

    async with connection.transaction(isolation="repeatable_read"):
        await _lock_caller_tables(connection, schema)
        await connection.execute("SET LOCAL search_path TO public,pg_catalog")
        previous, search_path = await _settings(connection), await connection.fetchval("SHOW search_path")
        if fail:
            driver = SimpleNamespace(
                transaction=connection.transaction,
                execute=connection.execute,
                fetch=failed_fetch,
                terminate=connection.terminate,
            )
            with pytest.raises(asyncpg.DivisionByZeroError):
                await epoch._catalog_relations(driver, schema)
        else:
            catalog = await epoch._catalog_relations(connection, schema)
            assert [row["relname"] for row in catalog] == sorted(epoch._TABLES)
            assert catalog == await epoch._catalog_relations(connection, schema)
        await _assert_caller_preserved(connection, schema, previous, search_path)


async def verify_repeated_cancellation(content_database, monkeypatch, boundary, *, catalog=False):
    """Cancellation during startup or rollback cannot leak a savepoint or canonical GUCs."""
    connection = content_database.connection
    scanning = asyncio.Event()

    async def scan(*_args):
        scanning.set()
        await connection.fetchval("SELECT pg_sleep(30)")

    if not catalog and boundary != "success_rollback":
        monkeypatch.setattr(epoch, "_content_chunks", scan)
    async with connection.transaction(isolation="repeatable_read"):
        await _lock_caller_tables(connection, content_database.schema)
        await connection.execute("SET LOCAL search_path TO public,pg_catalog")
        await epoch._set_content_settings(connection, [name for name, _ in epoch._CONTENT_SETTINGS], _PROFILES[1])
        previous, search_path = await _settings(connection), await connection.fetchval("SHOW search_path")
        transaction = _PausedTransaction(
            connection.transaction(), "rollback" if boundary == "success_rollback" else boundary
        )
        driver = SimpleNamespace(
            fetch=scan if catalog and boundary != "success_rollback" else connection.fetch,
            fetchval=connection.fetchval,
            execute=connection.execute,
            transaction=lambda: transaction,
            terminate=connection.terminate,
        )
        operation = epoch._catalog_relations if catalog else epoch._content
        task = asyncio.create_task(operation(driver, content_database.schema))
        try:
            if boundary == "rollback":
                await asyncio.wait_for(scanning.wait(), 5)
                task.cancel("scan interrupted")
            await asyncio.wait_for(transaction.entered.wait(), 5)
            task.cancel("boundary interrupted")
            await asyncio.sleep(0)
            task.cancel("repeated interruption")
        finally:
            transaction.release.set()
            if not task.done():
                task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
        assert transaction.rolled_back
        await _assert_caller_preserved(connection, content_database.schema, previous, search_path)
