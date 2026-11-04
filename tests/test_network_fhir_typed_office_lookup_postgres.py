# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native bounded office-lookup parity; synthetic rows do not establish Source custody."""

import asyncio
from types import SimpleNamespace
from uuid import UUID

import pytest

from process import network_fhir_typed_mapping as mapping
from process import network_fhir_typed_office_lookup as lookup
from process.network_fhir_typed_contract import TypedFHIRGraphError
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema

pytestmark = pytest.mark.asyncio
_ADDRESS = ("123 Example St", None, "Boston", "MA", "02110", "US")
_TAIL_ADDRESS = ("456 Example St", None, "Boston", "MA", "02110", "US")
_PROVIDER = "1234567893"


async def _fixture(connection):
    import ptg2_address_canon as native

    canonical = native.canonicalize_batch([_ADDRESS, _TAIL_ADDRESS])
    keys = [UUID(canonical_address["address_key"]) for canonical_address in canonical]
    await connection.execute("""CREATE TEMP TABLE lookup_addresses(
        location_key text PRIMARY KEY,entity_type text,entity_id text,npi bigint,address_key uuid,
        type text,address_precision text,inferred_npi bigint,
        first_line text,second_line text,city_name text,state_name text,postal_code text,country_code text)""")
    for index, address in enumerate((_ADDRESS, _ADDRESS, _ADDRESS, _ADDRESS, _TAIL_ADDRESS)):
        await connection.execute(
            "INSERT INTO lookup_addresses VALUES($1,'npi',$2,$3,$4,'practice','street',NULL,$5,$6,$7,$8,$9,$10)",
            str(index),
            _PROVIDER if index in (2, 4) else "unrelated-nonnumeric",
            int(_PROVIDER),
            keys[1 if index == 4 else 0],
            *address,
        )
    mappings = []
    for sites in ((1, 2), (3, 4)):
        names = await mapping.create_typed_mapping_tables(connection)
        selected = mapping.TypedGraphMapping(None, names, {}, {})
        for site in sites:
            key_index = int(site == 4)
            await connection.execute(
                "INSERT INTO " + selected.relation("offices") + " VALUES($1,$2,$3,$4,NULL)",
                "1000000004" if site == 2 else _PROVIDER,
                UUID(int=site),
                keys[key_index],
                canonical[key_index]["identity_key"],
            )
        mappings.append(selected)
    return SimpleNamespace(schema_name="pg_temp", table_name="lookup_addresses"), mappings


async def _lookup_rows(connection):
    companions = await connection.fetch(
        "SELECT relname FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relkind='r' "
        "AND (relname LIKE 'typed_map_offices_%_keys' OR relname LIKE 'typed_map_offices_%_matches')"
    )
    return sum(
        [
            await connection.fetchval("SELECT count(*) FROM pg_temp." + mapping._identifier(row["relname"]))
            for row in companions
        ]
    )


class ObservedConnection:
    def __init__(self, connection, *, cancel_page=None):
        self.connection, self.cancel_page, self.pages = connection, cancel_page, []

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def fetchrow(self, query, *arguments):
        if "AS scanned" in query:
            if len(self.pages) == self.cancel_page:
                raise asyncio.CancelledError()
            row = await self.connection.fetchrow(query, *arguments)
            self.pages.append(row["scanned"])
            return row
        return await self.connection.fetchrow(query, *arguments)


async def test_shared_lookup_keeps_missing_offices_and_empty_page_tail(serving_schema, monkeypatch):
    connection, _, _ = serving_schema
    monkeypatch.setattr(lookup, "_ADDRESS_PAGE_ROWS", 2)
    async with connection.transaction():
        address, mappings = await _fixture(connection)
        observed = ObservedConnection(connection)
        async with lookup.typed_office_lookup(
            observed, [row.table_names["offices"] for row in mappings], address
        ) as matches:
            assert await connection.fetchval("SELECT count(*) FROM " + matches) == 2
            for selected in mappings:
                await mapping._match_offices(connection, selected, address, matches)
        assert observed.pages == [2, 2, 1, 0]
        actual_offices = [
            tuple(row)
            for selected in mappings
            for row in await connection.fetch(
                "SELECT location_id,location_key FROM " + selected.relation("offices") + " ORDER BY location_id"
            )
        ]
        assert actual_offices == [(UUID(int=1), "2"), (UUID(int=2), None), (UUID(int=3), "2"), (UUID(int=4), "4")]
        assert await _lookup_rows(connection) == 0
        assert await connection.fetchval("SELECT count(*) FROM lookup_addresses") == 5


@pytest.mark.parametrize(
    "change,refusal", [("duplicate", "ambiguous_office"), ("canonical", "office_identity_differs")]
)
async def test_lookup_preserves_ambiguity_and_full_canonical_refusals(serving_schema, change, refusal):
    connection, _, _ = serving_schema
    async with connection.transaction():
        address, mappings = await _fixture(connection)
        if change == "duplicate":
            await connection.execute(
                "INSERT INTO lookup_addresses SELECT 'duplicate',entity_type,entity_id,npi,address_key,type,address_precision,inferred_npi,first_line,second_line,city_name,state_name,postal_code,country_code FROM lookup_addresses WHERE location_key='2'"
            )
        else:
            await connection.execute("UPDATE lookup_addresses SET first_line='789 Different St' WHERE location_key='2'")
        with pytest.raises(TypedFHIRGraphError, match=refusal):
            async with lookup.typed_office_lookup(connection, [mappings[0].table_names["offices"]], address) as matches:
                await mapping._match_offices(connection, mappings[0], address, matches)
        assert await _lookup_rows(connection) == 0
        assert await connection.fetchval("SELECT count(*) FROM " + mappings[0].relation("bindings")) == 0


async def test_cancelled_partial_scan_rolls_back_lookup_tables(serving_schema, monkeypatch):
    connection, _, _ = serving_schema
    monkeypatch.setattr(lookup, "_ADDRESS_PAGE_ROWS", 2)
    async with connection.transaction():
        address, mappings = await _fixture(connection)
        observed = ObservedConnection(connection, cancel_page=2)
        with pytest.raises(asyncio.CancelledError):
            async with lookup.typed_office_lookup(observed, [row.table_names["offices"] for row in mappings], address):
                pytest.fail("cancelled scan cannot expose a partial lookup")
        assert observed.pages == [2, 2] and await _lookup_rows(connection) == 0
        assert await connection.fetchval("SELECT count(*) FROM lookup_addresses") == 5


async def test_mapping_failure_rolls_back_earlier_office_updates(serving_schema):
    connection, _, _ = serving_schema
    async with connection.transaction():
        address, mappings = await _fixture(connection)
        with pytest.raises(ValueError, match="later_mapping_failed"):
            async with lookup.typed_office_lookup(
                connection, [row.table_names["offices"] for row in mappings], address
            ) as matches:
                await mapping._match_offices(connection, mappings[0], address, matches)
                assert await connection.fetchval("SELECT count(*) FROM " + mappings[0].relation("bindings")) == 1
                raise ValueError("later_mapping_failed")
        assert await connection.fetchval("SELECT count(*) FROM " + mappings[0].relation("bindings")) == 0
        assert await _lookup_rows(connection) == 0


async def test_precreated_mapping_companions_work_in_read_only_snapshot(serving_schema):
    connection, _, _ = serving_schema
    address, mappings = await _fixture(connection)
    names = await mapping.create_typed_mapping_tables(connection)
    assert set(names) == {"provider_only", "offices", "memberships", "bindings"}
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        selected = await mapping._mapping_workspace(connection, None, names)
        assert len(selected.oids) == 6
        await connection.execute(
            "INSERT INTO " + selected.relation("offices") + " SELECT * FROM " + mappings[0].relation("offices")
        )
        async with lookup.typed_office_lookup(connection, [names["offices"]], address) as matches:
            await mapping._match_offices(connection, selected, address, matches)
        assert await connection.fetchval("SHOW transaction_read_only") == "on"
        assert await connection.fetchval("SELECT count(*) FROM " + selected.relation("bindings")) == 1
        assert await _lookup_rows(connection) == 0
