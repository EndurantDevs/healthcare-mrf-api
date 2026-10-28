# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Batch identity reuse across reimports, source aliases and conflicting reviews."""

import os
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.network_registry import NetworkRegistryAlias, NetworkRegistryIdentity
from db.models.registry_approval import RegistryApprovedRecord
from process.registry_fhir_catalog_identity import FHIRNetworkIdentityInput, allocate_fhir_catalog_network_ids


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
        yield SimpleNamespace(connection=connection, schema=schema)
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
