# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native SQL proves imported membership mappings preserve approved manual intent."""

import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from process import registry_imported_catalog_bindings as imported
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_typed_mapping import _approved_graph_keys
from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
from tests.test_registry_fhir_catalog_sources import descriptor as source_descriptor


async def _create_binding_tables(connection, control, catalog):
    await connection.execute(f"""CREATE TABLE "{control}".registry_approved_record(
      approved_revision bigint,record_kind text,record_key text,record_revision bigint,
      custom_revision bigint,record_json jsonb);
    CREATE TABLE "{control}".registry_fhir_catalog_snapshot(
      metadata_generation text,schema_name text,metadata_approved_revision bigint,
      source_vector_json jsonb,published_at timestamptz);
    CREATE TABLE "{catalog}".registry_fhir_catalog_source(
      source_generation text,descriptor_json jsonb,profile text);
    CREATE TABLE "{catalog}".registry_fhir_catalog_network(
      metadata_generation text,network_id integer,archived boolean);
    CREATE TABLE "{catalog}".registry_fhir_catalog_binding(
      metadata_generation text,network_id integer,binding_id uuid,source_id text,document_json jsonb)""")


async def _populate_bindings(connection, catalog, descriptor, coordinates, generation, scope):
    for network in (10, 20, 30, 40):
        binding = uuid4()
        document_by_field = {
            **descriptor["coordinates"],
            "binding_id": str(binding),
            "revision": 1,
            "source_key": "network-" + str(network),
            "source_scope": {"organization_id": "network-" + str(network), "source_generation": generation},
        }
        await connection.execute(
            f'INSERT INTO "{catalog}".registry_fhir_catalog_network VALUES($1,$2,false)',
            scope.metadata_generation,
            network,
        )
        await connection.execute(
            f'INSERT INTO "{catalog}".registry_fhir_catalog_binding VALUES($1,$2,$3,$4,$5::jsonb)',
            scope.metadata_generation,
            network,
            binding,
            coordinates.source_id,
            json.dumps(document_by_field),
        )


@pytest.fixture
async def imported_catalog(monkeypatch):
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    control, catalog = "imported_binding_test_" + uuid4().hex, "fhir_catalog_" + uuid4().hex
    created_schemas = []
    try:
        for schema in (control, catalog):
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            created_schemas.append(schema)
        await _create_binding_tables(connection, control, catalog)
        descriptor = source_descriptor()
        coordinates = RegistryNetworkSourceCoordinates(**descriptor["coordinates"])
        generation = fhir_catalog_source_generation(descriptor)
        scope = imported.PublishedFHIRCatalogBindingScope("d" * 64)
        approved = ApprovedMembershipSource(7, "c" * 64)
        monkeypatch.setattr(imported, "pin_approved_membership_source", AsyncMock(return_value=approved))
        await connection.execute(
            f'INSERT INTO "{control}".registry_fhir_catalog_snapshot VALUES($1,$2,5,$3::jsonb,now())',
            scope.metadata_generation,
            catalog,
            json.dumps([{"source_generation": generation, "source_id": coordinates.source_id, "status": "current"}]),
        )
        await connection.execute(
            f"INSERT INTO \"{catalog}\".registry_fhir_catalog_source VALUES($1,$2::jsonb,'published_normalized')",
            generation,
            json.dumps(descriptor),
        )
        await _populate_bindings(connection, catalog, descriptor, coordinates, generation, scope)
        case = SimpleNamespace(
            connection=connection,
            control=control,
            catalog=catalog,
            descriptor=descriptor,
            coordinates=coordinates,
            generation=generation,
            scope=scope,
            approved=approved,
        )
        await _manual(case, "network", "30", {"network_id": 30, "archived": True})
        await _manual(case, "network", "50", {"network_id": 50, "archived": False})
        await _binding_override(case, "network-20", 50, False)
        await _binding_override(case, "network-40", 40, True)
        yield case
    finally:
        for schema in reversed(created_schemas):
            await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await connection.close()


async def _manual(case, kind, key, document, *, custom_revision=6):
    await case.connection.execute(
        f'INSERT INTO "{case.control}".registry_approved_record VALUES(7,$1,$2,1,$3,$4::jsonb)',
        kind,
        key,
        custom_revision,
        json.dumps(document),
    )


async def _binding_override(case, source_key, network, archived):
    await _manual(
        case,
        "network_binding",
        str(uuid4()),
        {
            "source_system": "fhir",
            "source_id": case.descriptor["source_aliases"][-1],
            "dataset_schema": "previous_edition_schema",
            "source_key": source_key,
            "network_id": network,
            "archived": archived,
        },
    )


async def _bindings(case):
    return await imported.published_fhir_catalog_bindings_sql(
        case.connection, case.scope, case.approved, case.coordinates, case.descriptor, control_schema=case.control
    )


@pytest.mark.asyncio
async def test_native_imported_mapping_adds_unapproved_network_and_preserves_manual_intent(imported_catalog):
    case = imported_catalog
    for alias, key, network, revision in (
        ("unrelated-source", "network-10", 10, 6),
        (case.coordinates.source_id, "network-20", 20, 5),
    ):
        await _manual(
            case,
            "network_binding",
            str(uuid4()),
            {
                "source_system": "fhir",
                "source_id": alias,
                "dataset_schema": "previous_edition_schema",
                "source_key": key,
                "network_id": network,
                "archived": True,
            },
            custom_revision=revision,
        )
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        sql = await _bindings(case)
        arguments = (7, *case.coordinates.sql_parameters)
        binding_rows = await case.connection.fetch(
            "WITH " + sql + " SELECT network_id,record_json->>'source_key' AS source_key "
            "FROM approved_network_bindings ORDER BY network_id",
            *arguments,
        )
        assert [tuple(binding_row) for binding_row in binding_rows] == [(10, "network-10"), (50, "network-20")]
        assert await _approved_graph_keys(case.connection, sql, (*arguments, case.generation), allow_empty=True) == [
            "network-10",
            "network-20",
        ]
        assert await case.connection.fetchval(f'SELECT count(*) FROM "{case.control}".registry_approved_record') == 6


@pytest.mark.asyncio
async def test_native_ambiguous_manual_rebind_refuses_complete_source(imported_catalog):
    case = imported_catalog
    await _binding_override(case, "network-20", 60, False)
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(ValueError, match="binding_conflict"):
            await _bindings(case)


@pytest.mark.asyncio
async def test_published_fhir_override_can_hash_on_source_key(imported_catalog):
    case = imported_catalog
    async with case.connection.transaction(readonly=True):
        bindings = await _bindings(case)
        await case.connection.execute("SET LOCAL enable_nestloop=off; SET LOCAL enable_mergejoin=off")
        plan_lines = await case.connection.fetch(
            "EXPLAIN (COSTS OFF) WITH " + bindings + " SELECT binding_id FROM manual_bindings",
            case.approved.approved_revision,
            *case.coordinates.sql_parameters,
        )
    assert any("Hash Cond:" in line[0] and "'source_key'" in line[0] for line in plan_lines)
