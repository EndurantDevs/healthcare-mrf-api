# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SELECT-only validation retains data access and blocks concurrent relation DDL."""

import os
from uuid import uuid4

import asyncpg
import pytest

from process.registry_source_retention import _require_source_retention_lock
from tests.test_network_membership_slices_postgres import slice_db as slice_db


@pytest.fixture
async def retention_lock_db(slice_db):
    fixture = slice_db
    fixture.reader_role = "retention_lock_reader_" + uuid4().hex
    fixture.relation = '"' + fixture.source_schema + '".retained_addresses'
    is_role_created = False
    await fixture.connection.execute("SET statement_timeout='5s'")
    await fixture.connection.execute("SET lock_timeout='2s'")
    try:
        await fixture.connection.execute(f'CREATE ROLE "{fixture.reader_role}" NOLOGIN')
        is_role_created = True
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.source_schema}" TO "{fixture.reader_role}"')
        await fixture.connection.execute(f'GRANT SELECT ON {fixture.relation} TO "{fixture.reader_role}"')
        yield fixture
    finally:
        if is_role_created:
            await fixture.connection.execute(f'REVOKE SELECT ON {fixture.relation} FROM "{fixture.reader_role}"')
            await fixture.connection.execute(
                f'REVOKE USAGE ON SCHEMA "{fixture.source_schema}" FROM "{fixture.reader_role}"'
            )
            await fixture.connection.execute(f'DROP ROLE "{fixture.reader_role}"')


@pytest.mark.asyncio
async def test_select_only_reader_validates_in_readonly_transaction(retention_lock_db):
    fixture = retention_lock_db
    async with fixture.connection.transaction(readonly=True):
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.reader_role}"')
        await _require_source_retention_lock(fixture.connection, fixture.relation)
        assert await fixture.connection.fetchval(f"SELECT count(*) FROM {fixture.relation}") == 4


@pytest.mark.asyncio
async def test_read_lock_requires_select_privilege(retention_lock_db):
    fixture = retention_lock_db
    await fixture.connection.execute(f'REVOKE SELECT ON {fixture.relation} FROM "{fixture.reader_role}"')
    async with fixture.connection.transaction(readonly=True):
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.reader_role}"')
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with fixture.connection.transaction():
                await _require_source_retention_lock(fixture.connection, fixture.relation)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO {relation}(location_key,entity_type,entity_id) VALUES('other','npi','other')",
        "UPDATE {relation} SET entity_id='other'",
        "DELETE FROM {relation}",
        "TRUNCATE {relation}",
    ],
)
async def test_read_lock_does_not_grant_data_writes(retention_lock_db, statement):
    fixture = retention_lock_db
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.reader_role}"')
        await _require_source_retention_lock(fixture.connection, fixture.relation)
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with fixture.connection.transaction():
                await fixture.connection.execute(statement.format(relation=fixture.relation))
        assert await fixture.connection.fetchval(f"SELECT count(*) FROM {fixture.relation}") == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("statement", ["DROP TABLE {relation}", "ALTER TABLE {relation} ADD COLUMN extra text"])
async def test_read_lock_bounds_concurrent_ddl(retention_lock_db, statement):
    fixture = retention_lock_db
    peer = None
    try:
        peer = await asyncpg.connect(
            os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://"),
            timeout=5,
            command_timeout=5,
        )
        async with fixture.connection.transaction(readonly=True):
            await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.reader_role}"')
            await _require_source_retention_lock(fixture.connection, fixture.relation)
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with peer.transaction():
                    await peer.execute("SET LOCAL lock_timeout='100ms'")
                    await peer.execute("SET LOCAL statement_timeout='2s'")
                    await peer.execute(statement.format(relation=fixture.relation))
            assert await fixture.connection.fetchval(f"SELECT count(*) FROM {fixture.relation}") == 4
        transaction = peer.transaction()
        await transaction.start()
        try:
            await peer.execute("SET LOCAL lock_timeout='100ms'")
            await peer.execute("SET LOCAL statement_timeout='2s'")
            await peer.execute(statement.format(relation=fixture.relation))
        finally:
            await transaction.rollback()
        assert await peer.fetchval(f"SELECT count(*) FROM {fixture.relation}") == 4
    finally:
        if peer is not None:
            await peer.close()
