# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native bounded office-lookup parity; synthetic rows do not establish Source custody."""

import asyncio
import json
from types import SimpleNamespace
from uuid import UUID

import pytest

from process import network_fhir_typed_mapping as mapping
from process import network_fhir_typed_office_lookup as lookup
from process.network_fhir_typed_sql import PAIR_DDL
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


async def _mixed_offices(connection, selected):
    import ptg2_address_canon as native

    addresses = [(line, *_ADDRESS[1:]) for line in ("789 Example St", "901 Example St", "234 Example St")]
    canonical = native.canonicalize_batch(addresses)
    for site, normalized in enumerate(canonical, 5):
        await connection.execute(
            "INSERT INTO " + selected.relation("offices") + " VALUES($1,$2,$3,$4,NULL)",
            _PROVIDER,
            UUID(int=site),
            UUID(normalized["address_key"]),
            normalized["identity_key"],
        )
    for key, index, address in (
        ("wrong-canonical", 0, _ADDRESS),
        ("uncanonicalizable", 1, (None, None, None, None, None, "US")),
        ("ambiguous-one", 2, addresses[2]),
        ("ambiguous-two", 2, addresses[2]),
    ):
        await connection.execute(
            "INSERT INTO lookup_addresses VALUES($1,'npi',$2,$3,$4,'practice','street',NULL,$5,$6,$7,$8,$9,$10)",
            key,
            _PROVIDER,
            int(_PROVIDER),
            UUID(canonical[index]["address_key"]),
            *address,
        )
    assert native.canonicalize_batch([(None, None, None, None, None, "US")])[0]["address_key"] is None


async def _pair_accounting(connection, selected):
    await connection.execute("CREATE TEMP TABLE lookup_pairs(" + PAIR_DDL + ")")
    await connection.execute(
        "INSERT INTO lookup_pairs SELECT 'network-one','npi',provider_id,location_id,address_key,canonical_identity,"
        "'PractitionerRole','role-'||location_id::text,location_id::text,'direct',3 FROM "
        + selected.relation("offices")
    )
    count = await connection.fetchval("SELECT count(*) FROM lookup_pairs")
    stage = SimpleNamespace(
        relation=lambda _kind: "pg_temp.lookup_pairs",
        document={"counts": {"pair_rows": count, "reference_rows": count * 3}},
    )
    bindings = (
        "approved_network_bindings AS (SELECT $1::integer network_id,$2::jsonb record_json,"
        "$3::text source,$4::text schema,$5::text dataset,$6::text producer,$7::text edition)"
    )
    binding_by_field = {"source_key": "network-one", "source_scope_json": {"source_generation": "generation-one"}}
    totals = await mapping._map_pair_rows(
        connection,
        stage,
        selected,
        bindings,
        (1, json.dumps(binding_by_field), None, None, None, None, None, "generation-one"),
    )
    offices = await mapping._require_mapping_counts(connection, stage, selected, totals)
    await connection.execute("DROP TABLE lookup_pairs")
    return totals, dict(offices)


async def test_mixed_lookup_omits_invalid_and_ambiguous_offices_with_exact_accounting(serving_schema, monkeypatch):
    import ptg2_address_canon as native

    connection, _, _ = serving_schema
    async with connection.transaction():
        address, mappings = await _fixture(connection)
        await _mixed_offices(connection, mappings[1])
        original, batch_sizes = native.canonicalize_batch, []

        def observed(rows):
            batch_sizes.append(len(rows))
            return original(rows)

        monkeypatch.setattr(native, "canonicalize_batch", observed)
        async with lookup.typed_office_lookup(
            connection, [selected.table_names["offices"] for selected in mappings], address
        ) as matches:
            for selected in mappings:
                await mapping._match_offices(connection, selected, address, matches)
        assert batch_sizes == [1, 4]
        actual_bindings = [
            tuple(binding_row)
            for selected in mappings
            for binding_row in await connection.fetch(
                "SELECT location_id,location_key FROM " + selected.relation("bindings") + " ORDER BY location_id"
            )
        ]
        assert actual_bindings == [(UUID(int=1), "2"), (UUID(int=3), "2"), (UUID(int=4), "4")]
        for selected, matched, missing in ((mappings[0], 1, 1), (mappings[1], 2, 3)):
            totals, offices = await _pair_accounting(connection, selected)
            assert offices == {"matched_offices": matched, "missing_offices": missing}
            assert totals == {
                "mapped_pair_rows": matched,
                "mapped_reference_rows": matched * 3,
                "unapproved_pair_rows": 0,
                "unapproved_reference_rows": 0,
                "unmatched_office_pair_rows": missing,
                "unmatched_office_reference_rows": missing * 3,
            }
            assert await connection.fetchval("SELECT count(*) FROM " + selected.relation("memberships")) == matched
        assert await _lookup_rows(connection) == 0
        assert await connection.fetchval("SELECT count(*) FROM lookup_addresses") == 9


async def test_office_acceptance_keeps_full_identity_and_native_result_shape_checks():
    import ptg2_address_canon as native

    canonical = native.canonicalize_batch([_ADDRESS])
    office_by_field = {
        "provider_id": _PROVIDER,
        "location_id": UUID(int=1),
        "matched_key": "one",
        "address_key": UUID(canonical[0]["address_key"]),
        "canonical_identity": canonical[0]["identity_key"] + "changed",
    }
    assert mapping._accepted_office_rows([office_by_field], canonical) == []
    with pytest.raises(ValueError):
        mapping._accepted_office_rows([office_by_field], [])
    with pytest.raises(ValueError):
        mapping._accepted_office_rows([office_by_field], [{**canonical[0], "address_key": "invalid"}])


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
