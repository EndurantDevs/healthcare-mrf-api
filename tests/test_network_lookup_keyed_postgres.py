# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify exact requested-office probes against a large unrelated address population."""

import json

import asyncpg
import pytest

from process import network_fhir_typed_office_lookup as lookup
from tests.test_network_fhir_typed_office_lookup_postgres import ObservedConnection, _fixture, _lookup_rows
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema

pytestmark = pytest.mark.asyncio


def _nodes(plan):
    yield plan
    for child in plan.get("Plans", ()):
        yield from _nodes(child)


async def test_sparse_office_lookup_matches_full_join_and_probes_existing_index(serving_schema):
    connection, _, _ = serving_schema
    async with connection.transaction():
        address, mappings = await _fixture(connection)
        await connection.execute("""INSERT INTO lookup_addresses
            SELECT 'unrelated-'||sequence,'npi',(3000000000+sequence)::text,3000000000+sequence,
                md5('unrelated-'||sequence)::uuid,'practice','street',NULL,
                '999 Example St',NULL,'Boston','MA','02110','US'
            FROM generate_series(1,40000) sequence""")
        await connection.execute("ANALYZE lookup_addresses")
        observed = ObservedConnection(connection)
        async with lookup.typed_office_lookup(
            observed, [selected.table_names["offices"] for selected in mappings], address
        ) as matches:
            requested = (
                'pg_temp."' + lookup.office_lookup_names(mappings[0].table_names["offices"])["office_keys"] + '"'
            )
            expected = await connection.fetch(
                f"""SELECT requested.provider_id,requested.address_key,a.location_key
                FROM lookup_addresses a JOIN {requested} requested
                    ON a.entity_id=requested.provider_id AND a.address_key=requested.address_key
                WHERE a.entity_type='npi' AND a.npi=requested.provider_id::bigint
                    AND a.type='practice' AND a.address_precision='street' AND a.inferred_npi IS NULL
                ORDER BY 1,2,3"""
            )
            assert await connection.fetch(f"SELECT * FROM {matches} ORDER BY 1,2,3") == expected
            assert len(expected) == 2 and observed.pages == [3, 0]
            await connection.execute(f"DELETE FROM {matches}")
            query, arguments = observed.statements[0]
            explained = json.loads(
                await connection.fetchval("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) " + query, *arguments)
            )
            scans = [node for node in _nodes(explained[0]["Plan"]) if node.get("Relation Name") == "lookup_addresses"]
            assert len(scans) == 1
            probe = scans[0]
            assert probe["Index Name"] == "lookup_addresses_service_key_npi"
            assert "address_key" in probe["Index Cond"] and "npi" in probe["Index Cond"]
            assert probe["Actual Loops"] == 3 and probe["Actual Rows"] <= 1
            assert await connection.fetch(f"SELECT * FROM {matches} ORDER BY 1,2,3") == expected
        assert await _lookup_rows(connection) == 0
        assert await connection.fetchval("SELECT count(*) FROM lookup_addresses") == 40005


async def test_empty_requested_offices_do_not_expose_matches(serving_schema):
    connection, _, _ = serving_schema
    async with connection.transaction():
        address, mappings = await _fixture(connection)
        for selected in mappings:
            await connection.execute("DELETE FROM " + selected.relation("offices"))
        observed = ObservedConnection(connection)
        async with lookup.typed_office_lookup(
            observed, [selected.table_names["offices"] for selected in mappings], address
        ) as matches:
            assert await connection.fetchval("SELECT count(*) FROM " + matches) == 0
        assert observed.pages == [0] and await _lookup_rows(connection) == 0


async def test_invalid_provider_identifier_rolls_back_requested_companions(serving_schema):
    connection, _, _ = serving_schema
    async with connection.transaction():
        address, mappings = await _fixture(connection)
        await connection.execute("UPDATE " + mappings[0].relation("offices") + " SET provider_id='invalid'")
        with pytest.raises(asyncpg.InvalidTextRepresentationError):
            async with lookup.typed_office_lookup(
                connection, [selected.table_names["offices"] for selected in mappings], address
            ):
                pytest.fail("invalid provider identifiers cannot expose a partial lookup")
        assert await _lookup_rows(connection) == 0
