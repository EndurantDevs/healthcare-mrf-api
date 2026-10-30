# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reuse only exact self-held native locks that already exclude fact writers."""

import os
from contextlib import AsyncExitStack
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
    await fixture.connection.execute(f'CREATE ROLE "{fixture.reader_role}" NOLOGIN')
    try:
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.source_schema}" TO "{fixture.reader_role}"')
        await fixture.connection.execute(f'GRANT SELECT ON {fixture.relation} TO "{fixture.reader_role}"')
        yield fixture
    finally:
        await fixture.connection.execute(f'REVOKE SELECT ON {fixture.relation} FROM "{fixture.reader_role}"')
        await fixture.connection.execute(
            f'REVOKE USAGE ON SCHEMA "{fixture.source_schema}" FROM "{fixture.reader_role}"'
        )
        await fixture.connection.execute(f'DROP ROLE "{fixture.reader_role}"')


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [None, "ACCESS SHARE", "SHARE UPDATE EXCLUSIVE"])
async def test_missing_or_weaker_self_lock_does_not_bypass_native_privileges(retention_lock_db, mode):
    fixture = retention_lock_db
    async with fixture.connection.transaction():
        if mode is not None:
            await fixture.connection.execute(f"LOCK TABLE {fixture.relation} IN {mode} MODE")
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.reader_role}"')
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with fixture.connection.transaction():
                await _require_source_retention_lock(fixture.connection, fixture.relation)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["SHARE", "SHARE ROW EXCLUSIVE", "EXCLUSIVE", "ACCESS EXCLUSIVE"])
async def test_owner_capability_lock_remains_effective_after_return_to_reader(retention_lock_db, mode):
    fixture = retention_lock_db
    async with fixture.connection.transaction():
        await fixture.connection.execute(f"LOCK TABLE {fixture.relation} IN {mode} MODE")
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.reader_role}"')
        await _require_source_retention_lock(fixture.connection, fixture.relation)
        assert await fixture.connection.fetchval("SELECT current_user") == fixture.reader_role
        assert await fixture.connection.fetchval(f"SELECT count(*) FROM {fixture.relation}") == 4


@pytest.mark.asyncio
async def test_another_backend_lock_cannot_supply_the_custodian_capability(retention_lock_db):
    fixture = retention_lock_db
    peer = await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    )
    try:
        async with AsyncExitStack() as transactions:
            await transactions.enter_async_context(peer.transaction())
            await peer.execute(f"LOCK TABLE {fixture.relation} IN SHARE MODE")
            await transactions.enter_async_context(fixture.connection.transaction())
            await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.reader_role}"')
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with fixture.connection.transaction():
                    await _require_source_retention_lock(fixture.connection, fixture.relation)
    finally:
        await peer.close()
