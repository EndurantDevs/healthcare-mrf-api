# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read locks bind swappable ACA names before selecting their committed edition."""

import asyncio
from uuid import uuid4

import asyncpg
import pytest

from process.registry_imported_source_inventory import lock_aca_source_tables
from tests.test_registry_fhir_catalog_store_postgres import storage_database as storage_database


async def source_heads(fixture):
    schema = "aca_head_race_" + uuid4().hex
    fixture.schemas.append(schema)
    await fixture.connection.execute(f'CREATE SCHEMA "{schema}"')
    await fixture.connection.execute(
        f'CREATE TABLE "{schema}".plan_npi_raw(npi bigint); INSERT INTO "{schema}".plan_npi_raw VALUES(1234567893)'
    )
    await fixture.connection.execute(
        f'CREATE TABLE "{schema}".mrf_publication_receipt(import_id text); INSERT INTO "{schema}".mrf_publication_receipt VALUES(\'old\')'
    )
    return schema


async def replace_heads(writer, schema):
    await writer.execute(f'ALTER TABLE "{schema}".plan_npi_raw RENAME TO retained_old')
    await writer.execute(
        f'CREATE TABLE "{schema}".plan_npi_raw(npi bigint); INSERT INTO "{schema}".plan_npi_raw VALUES(1999999984)'
    )
    await writer.execute(f"UPDATE \"{schema}\".mrf_publication_receipt SET import_id='new'")


@pytest.mark.asyncio
async def test_read_before_swap_keeps_old_named_data(storage_database):
    fixture = storage_database
    schema = await source_heads(fixture)
    reader = await asyncpg.connect(fixture.dsn.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        async with reader.transaction(readonly=True, isolation="repeatable_read"):
            await lock_aca_source_tables(reader, schema, ("plan_npi_raw",))
            assert await reader.fetchval(f'SELECT import_id FROM "{schema}".mrf_publication_receipt') == "old"
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with fixture.connection.transaction():
                    await fixture.connection.execute("SET LOCAL lock_timeout='75ms'")
                    await replace_heads(fixture.connection, schema)
            assert await reader.fetchval(f'SELECT npi FROM "{schema}".plan_npi_raw') == 1234567893
    finally:
        await reader.close()


@pytest.mark.asyncio
async def test_swap_before_read_selects_new_head_and_data(storage_database):
    fixture = storage_database
    schema = await source_heads(fixture)
    reader = await asyncpg.connect(fixture.dsn.replace("postgresql+asyncpg://", "postgresql://"))
    pending = None
    try:
        async with reader.transaction(readonly=True, isolation="repeatable_read"):
            async with fixture.connection.transaction():
                await replace_heads(fixture.connection, schema)
                pending = asyncio.create_task(lock_aca_source_tables(reader, schema, ("plan_npi_raw",)))
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(asyncio.shield(pending), 0.075)
            await asyncio.wait_for(pending, 2)
            assert await reader.fetchval(f'SELECT import_id FROM "{schema}".mrf_publication_receipt') == "new"
            assert await reader.fetchval(f'SELECT npi FROM "{schema}".plan_npi_raw') == 1999999984
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await reader.close()
