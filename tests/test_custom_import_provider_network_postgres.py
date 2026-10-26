# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native pinned alias authority and exact legacy provider-office subset proof."""

from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import async_sessionmaker

from api import custom_import_provider_network as network
from api.network_address_scope import (
    current_network_address_scope,
    network_address_read_scope,
    scoped_address_parameters,
    scoped_address_relation_sql,
)
from process.custom_import.read_contracts import CustomImportReadUnavailableError
from process.network_serving_read import PinnedNetworkServingManifest
from tests.test_custom_import_provider_network import _bound_document
from tests.test_network_custom_address_source_postgres import _draft
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _create
from tests.test_registry_network_catalog_read_postgres import (
    ACA,
    ACA_SCOPE,
    _binding,
    _publish_legacy_batch_bindings,
)
from tests.test_registry_network_catalog_read_postgres import catalog_db as catalog_db

pytestmark = pytest.mark.asyncio


async def _approve_shared_aliases(fixture):
    """Resolve two genuinely approved source checksums to one canonical identity."""
    allocation = uuid4()
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".network_registry_identity(network_id,allocation_key) '
        "OVERRIDING SYSTEM VALUE VALUES(84,$1)",
        allocation,
    )
    record = await _draft(fixture, replace(_create("network"), allocation_key=allocation), fixture.seed.actor)
    bindings = []
    for source_key, checksum, previous in (
        ("alpha-scope", -17, fixture.records.alpha),
        ("beta-scope", 42, fixture.records.beta),
    ):
        binding_id = await fixture.connection.fetchval(
            f'SELECT binding_id FROM "{fixture.control_schema}".registry_network_binding WHERE source_key=$1',
            source_key,
        )
        bindings.append(
            _binding(84, ACA, source_key, {**ACA_SCOPE, "checksum_network": checksum})
            | {
                "binding_id": str(binding_id),
                "operation": "rebind",
                "expected_revision": 1,
                "expected_network_id": previous["record_id"],
            }
        )
    return await _publish_legacy_batch_bindings(fixture, bindings, record)


async def test_signed_subset_rechecks_real_approved_catalog_in_the_pinned_reader(catalog_db, monkeypatch):
    """Use genuine approval/publication/Reader SQL, without replacing the resolver."""
    fixture = catalog_db
    manifest = await _approve_shared_aliases(fixture)
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
    document = _bound_document()
    document["network_binding"].update(
        generation_id=manifest.generation_id,
        manifest_sha256=manifest.manifest_sha256,
        network_ids=[84],
        selector={"namespace": "canonical", "values": [84]},
        legacy_checksums=[42],
    )
    binding = network.parse_provider_network_binding(document)
    async with async_sessionmaker(fixture.engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles["reader"]}"'))
        async with network.provider_network_read_scope(session, binding) as pinned:
            assert pinned == manifest
            assert current_network_address_scope().legacy_checksums == (42,)
            relation = scoped_address_relation_sql("mrf.entity_address_unified")
            selected = await session.scalar(
                text(f"SELECT count(*) FROM {relation} addresses"), scoped_address_parameters({})
            )
            assert selected == 1
            reply = network.provider_network_response({"count": selected}, pinned)
            assert reply.headers["X-Network-Legacy-Checksums"] == "42"
            assert reply.headers["X-Network-Generation"] == str(manifest.generation_id)
        assert current_network_address_scope() is None
        for wrong in (
            replace(binding, network_ids=(fixture.records.alpha["record_id"],)),
            replace(binding, legacy_checksums=(999,)),
        ):
            with pytest.raises(CustomImportReadUnavailableError, match="legacy scope"):
                async with network.provider_network_read_scope(session, wrong):
                    pytest.fail("unresolved signed subset reached the native handler")
        assert current_network_address_scope() is None


async def _subset_rows(connection, manifest, legacy_checksums):
    """Execute the production scope predicate, including its typed parameters."""
    with network_address_read_scope(manifest, (84,), legacy_checksums=legacy_checksums) as scope:
        relation = scope.address_relation.element
        physical = relation.get_final_froms()[0]
        statement = (
            select(physical.c.location_key, physical.c.npi)
            .where(*relation._where_criteria)
            .order_by(physical.c.location_key)
        )
        compiled = statement.compile(dialect=postgresql.asyncpg.dialect())
        parameters = compiled.params | scoped_address_parameters({})
        return await connection.fetch(str(compiled), *(parameters[name] for name in compiled.positiontup))


async def test_native_same_canonical_identity_never_widens_original_office_or_provider_prices(serving_schema):
    """Component SQL proof complements the genuine catalog/Reader integration above."""
    connection, _, _ = serving_schema
    candidate = uuid4()
    schema = "network_candidate_" + candidate.hex
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await connection.execute(
            f'CREATE TABLE "{schema}".entity_address_unified(location_key text PRIMARY KEY, '
            "npi bigint,canonical_network_ids integer[],plans_network_array integer[])"
        )
        await connection.execute(f'CREATE TABLE "{schema}".synthetic_prices(npi bigint PRIMARY KEY,price integer)')
        await connection.execute(
            f'INSERT INTO "{schema}".entity_address_unified VALUES '
            "('office-a',1111111111,ARRAY[84],ARRAY[42]),('office-b',2222222222,ARRAY[84],ARRAY[-17])"
        )
        await connection.execute(f'INSERT INTO "{schema}".synthetic_prices VALUES(1111111111,10),(2222222222,9000)')
        manifest = PinnedNetworkServingManifest(1, str(candidate), schema, 1, {}, 0, "a" * 64, 1)
        for selected, expected in ((None, ["office-a", "office-b"]), ((42,), ["office-a"]), ((-17,), ["office-b"])):
            offices = await _subset_rows(connection, manifest, selected)
            assert [office["location_key"] for office in offices] == expected
            prices = await connection.fetch(
                f'SELECT price FROM "{schema}".synthetic_prices WHERE npi=ANY($1::bigint[]) ORDER BY price',
                [office["npi"] for office in offices],
            )
            assert [price_row["price"] for price_row in prices] == (
                {None: [10, 9000], (42,): [10], (-17,): [9000]}[selected]
            )
        await connection.execute(
            f"UPDATE \"{schema}\".entity_address_unified SET npi=1111111111 WHERE location_key='office-b'"
        )
        assert [office["location_key"] for office in await _subset_rows(connection, manifest, None)] == [
            "office-a",
            "office-b",
        ]
        assert [office["location_key"] for office in await _subset_rows(connection, manifest, (42,))] == ["office-a"]
        # Claim prices are provider-grain aggregates; this does not invent office-level prices.
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
