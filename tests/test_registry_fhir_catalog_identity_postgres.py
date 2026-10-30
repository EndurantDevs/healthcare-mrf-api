# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Batch identity reuse across reimports, source aliases and conflicting reviews."""

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.network_registry import NetworkRegistryAlias, NetworkRegistryIdentity
from db.models.registry_approval import RegistryApprovedRecord
from process.registry_fhir_catalog_identity import (
    FHIRNetworkIdentityInput,
    allocate_fhir_catalog_network_ids,
    resolve_fhir_catalog_network_ids,
)


@pytest.fixture
async def identity_database():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an isolated database")
    connection = await asyncpg.connect(dsn)
    schema = "fhir_identity_test_" + uuid4().hex
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        metadata = MetaData()
        for model in (NetworkRegistryIdentity, NetworkRegistryAlias, RegistryApprovedRecord):
            table = model.__table__.to_metadata(metadata, schema=schema)
            await connection.execute(str(CreateTable(table).compile(dialect=postgresql.dialect())))
        yield SimpleNamespace(connection=connection, schema=schema, dsn=dsn)
    finally:
        await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        await connection.close()


async def _allocate(fixture, rows):
    async with fixture.connection.transaction():
        return await allocate_fhir_catalog_network_ids(
            fixture.connection, rows, approved_revision=2, control_schema=fixture.schema
        )


@pytest.mark.asyncio
async def test_complete_batch_replay_and_new_source_alias_reuse_ids(identity_database):
    fixture = identity_database
    rows = tuple(
        FHIRNetworkIdentityInput(f"network-{index}", "endpoint-one", ("source-one",), "edition-one")
        for index in range(1000)
    )
    first = await _allocate(fixture, rows)
    second = await _allocate(fixture, rows)
    assert first["created_networks"] == 1000 and second["created_networks"] == 0
    assert first["rows"] == second["rows"]
    aliases = tuple(
        FHIRNetworkIdentityInput(row.organization_id, row.endpoint_id, ("source-two",), "edition-two") for row in rows
    )
    third = await _allocate(fixture, aliases)
    assert third["created_networks"] == 0 and third["rows"] == first["rows"]
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".network_registry_identity') == 1000
    )


@pytest.mark.asyncio
async def test_genuine_scoped_legacy_alias_is_adopted_without_allocation(identity_database):
    fixture = identity_database
    network_id = await fixture.connection.fetchval(
        f'INSERT INTO "{fixture.schema}".network_registry_identity(allocation_key) VALUES($1) RETURNING network_id',
        uuid4(),
    )
    legacy = uuid4()
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.schema}".network_registry_alias '
        "VALUES('fhir','source-one','legacy_fhir_uuid',$1,'reviewed-scope',$2,'original-evidence',now())",
        str(legacy),
        network_id,
    )
    row = FHIRNetworkIdentityInput(
        "network-one", "endpoint-one", ("source-one",), "edition-two", str(legacy), "reviewed-scope"
    )
    result = await _allocate(fixture, (row,))
    assert result["created_networks"] == 0 and result["rows"][0]["network_id"] == network_id
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".network_registry_identity') == 1


@pytest.mark.asyncio
async def test_ambiguous_approved_mapping_never_reassigns_or_allocates(identity_database):
    fixture = identity_database
    row = FHIRNetworkIdentityInput("network-one", "endpoint-one", ("source-one",), "edition-one")
    first = await _allocate(fixture, (row,))
    incumbent = first["rows"][0]["network_id"]
    different = await fixture.connection.fetchval(
        f'INSERT INTO "{fixture.schema}".network_registry_identity(allocation_key) VALUES($1) RETURNING network_id',
        uuid4(),
    )
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.schema}".registry_approved_record '
        "VALUES(2,'network_binding',$1,1,1,jsonb_build_object('source_system','fhir','source_id','source-one',"
        "'source_key','network-one','network_id',$2::integer,'archived',false))",
        str(uuid4()),
        different,
    )
    conflict = await _allocate(fixture, (row,))
    assert conflict["created_networks"] == 0 and conflict["rows"][0]["conflict"] is True
    assert (
        await fixture.connection.fetchval(
            f'SELECT network_id FROM "{fixture.schema}".network_registry_alias '
            "WHERE alias_type='fhir_organization' AND alias_value='network-one'"
        )
        == incumbent
    )


@pytest.mark.asyncio
async def test_resolve_needs_only_control_select_and_preserves_identities_and_aliases(identity_database):
    fixture = identity_database
    initial = await _allocate(
        fixture,
        tuple(
            FHIRNetworkIdentityInput(name, "endpoint-one", ("source-one",), "original")
            for name in ("network-known", "network-conflict")
        ),
    )
    known_id = initial["rows"][0]["network_id"]
    conflict_id = initial["rows"][1]["network_id"]
    spare = await fixture.connection.fetchval(
        f'INSERT INTO "{fixture.schema}".network_registry_identity(allocation_key) VALUES($1) RETURNING network_id',
        uuid4(),
    )
    legacy_uuid = uuid4()
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.schema}".network_registry_alias '
        "VALUES('fhir','source-one','legacy_fhir_uuid',$1,'legacy-scope',$2,'original',now())",
        str(legacy_uuid),
        spare,
    )
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.schema}".registry_approved_record '
        "VALUES(2,'network_binding',$1,1,1,jsonb_build_object('source_system','fhir','source_id','source-one',"
        "'source_key','network-conflict','network_id',$2::integer,'archived',false))",
        str(uuid4()),
        spare,
    )
    aliases = ("source-one", "source-two")
    inputs = tuple(
        FHIRNetworkIdentityInput(name, "endpoint-one", aliases, "lookup-only")
        for name in ("network-known", "network-missing", "network-conflict")
    ) + (
        FHIRNetworkIdentityInput(
            "network-legacy", "endpoint-one", aliases, "lookup-only", str(legacy_uuid), "legacy-scope"
        ),
    )
    before = await _identity_control_state(fixture)
    resolved = await _select_only_resolve(fixture, inputs)
    assert resolved == (
        {"organization_id": "network-known", "network_id": known_id, "conflict": False},
        {"organization_id": "network-missing", "network_id": None, "conflict": False},
        {"organization_id": "network-conflict", "network_id": conflict_id, "conflict": True},
        {"organization_id": "network-legacy", "network_id": spare, "conflict": False},
    )
    assert await _identity_control_state(fixture) == before
    assert (
        await fixture.connection.fetchval(
            f"SELECT count(*) FROM \"{fixture.schema}\".network_registry_alias WHERE source_id='source-two'"
        )
        == 0
    )


async def _identity_control_state(fixture):
    counts = await fixture.connection.fetchrow(
        f'SELECT (SELECT count(*) FROM "{fixture.schema}".network_registry_identity) AS identities,'
        f'(SELECT count(*) FROM "{fixture.schema}".network_registry_alias) AS aliases'
    )
    sequence = await fixture.connection.fetchrow(
        f'SELECT last_value,is_called FROM "{fixture.schema}".network_registry_identity_network_id_seq'
    )
    return dict(counts), dict(sequence)


async def _select_only_resolve(fixture, identity_inputs):
    role = "fc_identity_lookup_" + uuid4().hex
    password = uuid4().hex
    reader = None
    assert not await fixture.connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
    try:
        await fixture.connection.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.schema}" TO "{role}"')
        await fixture.connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.schema}" TO "{role}"')
        reader = await asyncpg.connect(fixture.dsn, user=role, password=password)
        assert await reader.fetchval(
            "SELECT NOT has_table_privilege(current_user,$1,'INSERT,UPDATE,DELETE') "
            "AND NOT has_table_privilege(current_user,$2,'INSERT,UPDATE,DELETE')",
            fixture.schema + ".network_registry_identity",
            fixture.schema + ".network_registry_alias",
        )
        async with reader.transaction():
            resolved = await resolve_fhir_catalog_network_ids(
                reader, identity_inputs, approved_revision=2, control_schema=fixture.schema
            )
            assert (
                await reader.fetchval(
                    "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'fhir_catalog_identity_%'"
                )
                == 0
            )
            return resolved
    finally:
        if reader is not None:
            await reader.close()
        if await fixture.connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role):
            await fixture.connection.execute(f'DROP OWNED BY "{role}"')
            await fixture.connection.execute(f'DROP ROLE "{role}"')
        assert not await fixture.connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows", [(), tuple(FHIRNetworkIdentityInput("network", "endpoint", ("source",), "evidence") for _ in range(1001))]
)
async def test_identity_resolution_rejects_unbounded_batches_before_database_writes(rows):
    connection = Mock(is_in_transaction=Mock(return_value=True), execute=AsyncMock())
    with pytest.raises(ValueError, match="identity_bounds"):
        await resolve_fhir_catalog_network_ids(connection, rows, approved_revision=2, control_schema="control")
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_reimports_reuse_one_identity_and_preserve_both_source_aliases(identity_database):
    fixture = identity_database
    other = await asyncpg.connect(fixture.dsn)
    tasks = []

    async def allocate(connection, source_alias):
        async with connection.transaction():
            return await allocate_fhir_catalog_network_ids(
                connection,
                (FHIRNetworkIdentityInput("shared-network", "endpoint", (source_alias,), "edition"),),
                approved_revision=2,
                control_schema=fixture.schema,
            )

    try:
        tasks = [
            asyncio.create_task(allocate(connection, alias))
            for connection, alias in ((fixture.connection, "source-one"), (other, "source-two"))
        ]
        allocations = await asyncio.wait_for(asyncio.gather(*tasks), 10)
        assert allocations[0]["rows"][0]["network_id"] == allocations[1]["rows"][0]["network_id"]
        assert sum(allocation["created_networks"] for allocation in allocations) == 1
        assert (
            await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".network_registry_identity') == 1
        )
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(DISTINCT source_id) FROM "{fixture.schema}".network_registry_alias'
            )
            == 2
        )
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await other.close()
