# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native allocation and durable-record checks on an explicitly selected test DB."""

import asyncio
import importlib.util
import os
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.network_registry_identity import allocate_network_ids


@pytest.fixture
async def registry_db():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated test database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    schema = "registry_test_" + uuid4().hex
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    path = Path(__file__).parents[1] / "alembic/versions/20261007010000_managed_network_registry.py"
    spec = importlib.util.spec_from_file_location("network_registry_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://"))
    try:
        for statement in migration._ddl(schema):
            await connection.execute(statement)
        yield connection, schema, async_sessionmaker(engine)
    finally:
        await engine.dispose()
        await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await connection.close()


@pytest.mark.asyncio
async def test_identity_replay_archive_and_transaction_rollback(registry_db):
    connection, schema, sessions = registry_db
    keys = [uuid4(), uuid4()]
    async with sessions() as session, session.begin():
        assigned = await allocate_network_ids(session, keys, schema=schema)
    async with sessions() as session, session.begin():
        assert await allocate_network_ids(session, keys[::-1], schema=schema) == assigned
    network_id = assigned[keys[0]]
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_record(network_id,display_name) VALUES ($1,$2)',
        network_id,
        "Example Preferred",
    )
    await connection.execute(
        f'UPDATE "{schema}".network_registry_record SET archived=true WHERE network_id=$1', network_id
    )
    async with sessions() as session, session.begin():
        assert (await allocate_network_ids(session, [keys[0]], schema=schema))[keys[0]] == network_id
    rolled_back = uuid4()
    with pytest.raises(RuntimeError, match="rollback"):
        async with sessions() as session, session.begin():
            await allocate_network_ids(session, [rolled_back], schema=schema)
            raise RuntimeError("rollback")
    assert not await connection.fetchval(
        f'SELECT count(*) FROM "{schema}".network_registry_identity WHERE allocation_key=$1', rolled_back
    )


@pytest.mark.asyncio
async def test_concurrent_allocation_is_consistent(registry_db):
    _, schema, sessions = registry_db
    key = uuid4()

    async def allocate():
        async with sessions() as session, session.begin():
            return await allocate_network_ids(session, [key], schema=schema)

    results = await asyncio.gather(allocate(), allocate(), allocate())
    assert results[0] == results[1] == results[2]


@pytest.mark.asyncio
async def test_native_int4_identity_and_never_cycle(registry_db):
    connection, schema, sessions = registry_db
    identity = await connection.fetchrow(
        "SELECT data_type,is_identity,identity_generation,identity_maximum,identity_cycle "
        "FROM information_schema.columns WHERE table_schema=$1 AND table_name='network_registry_identity' AND column_name='network_id'",
        schema,
    )
    assert tuple(identity) == ("integer", "YES", "ALWAYS", "2147483647", "NO")
    await connection.execute(
        f'ALTER TABLE "{schema}".network_registry_identity ALTER COLUMN network_id RESTART WITH 2147483647'
    )
    async with sessions() as session, session.begin():
        assert list((await allocate_network_ids(session, [uuid4()], schema=schema)).values()) == [2147483647]
    with pytest.raises(Exception, match="maximum value|SequenceGeneratorLimitExceeded"):
        async with sessions() as session, session.begin():
            await allocate_network_ids(session, [uuid4()], schema=schema)


@pytest.mark.asyncio
async def test_distinct_groups_manual_roles_and_scoped_aliases(registry_db):
    connection, schema, sessions = registry_db
    for kind in ["naic_group", "corporate_parent"]:
        await connection.execute(
            f'INSERT INTO "{schema}".company_group_registry(group_id,group_kind,display_name) VALUES ($1,$2,$3)',
            uuid4(),
            kind,
            "Example Group",
        )
    await connection.execute(
        f'INSERT INTO "{schema}".company_registry(company_id,display_name,roles) VALUES ($1,$2,$3)',
        uuid4(),
        "Example Benefits",
        ["employer", "network_operator"],
    )
    async with sessions() as session, session.begin():
        allocated_ids = list((await allocate_network_ids(session, [uuid4(), uuid4()], schema=schema)).values())
    legacy = str(uuid4())
    for source_id, network_id in zip(["source-one", "source-two"], allocated_ids):
        await connection.execute(
            f'INSERT INTO "{schema}".network_registry_alias(source_system,source_id,alias_type,alias_value,scope_key,network_id,evidence_id) '
            "VALUES ('fhir',$1,'legacy_uuid',$2,'medical/2026',$3,'verified-source-record')",
            source_id,
            legacy,
            network_id,
        )
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_alias') == 2
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            f'INSERT INTO "{schema}".company_registry(company_id,display_name,roles) VALUES ($1,$2,$3)',
            uuid4(),
            "Example Invalid",
            ["employer", None],
        )
