# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real compact reads use current qualifying postal facts and immutable prices."""

import json
from decimal import Decimal
from types import SimpleNamespace

import orjson
import pytest
from sqlalchemy import text

from api import plan_pricing_projection_contract as contract
from api import plan_pricing_projection_read as projections
from api import pricing_address_source_filter as source_filter
from api import ptg2_serving as serving
from api.plan_pricing_projection_materialize import CardStats, card_fragment
from tests.test_plan_pricing_projection_v3_differential_postgres import migrated_v3_database
from tests.test_ptg2_factorized_card_postgres import _insert_factorized_card_fixture


async def _current_offices(connection, schema, has_postal_fields):
    await connection.execute(
        text(f'''CREATE TABLE "{schema}".entity_address_unified (
        npi bigint, location_key text, zip5 text, postal_code text, type text, source_count integer,
        city_name text, state_name text, state_code text
    )''')
    )
    await connection.execute(
        text(f'''INSERT INTO "{schema}".entity_address_unified VALUES
        (101, 'first-low', '10002', '10002', 'practice', 1, 'Frozen City', 'IL', 'IL'),
        (101, 'alternate-high', '10002', '10002', 'practice', 3, :city, :state_name, :state_code)
    '''),
        {
            "city": "Qualified City" if has_postal_fields else None,
            "state_name": "Indiana" if has_postal_fields else None,
            "state_code": "IN" if has_postal_fields else None,
        },
    )


def _patch_schema(monkeypatch, schema):
    monkeypatch.setattr(contract, "SCHEMA", schema)
    monkeypatch.setattr(source_filter, "SCHEMA", schema)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_postal_fields", [True, False])
async def test_factorized_card_replaces_postal_facts_from_the_qualified_alternate_office(
    monkeypatch,
    migrated_v3_database,
    has_postal_fields,
):
    database, projection_id = migrated_v3_database, "a" * 64
    _patch_schema(monkeypatch, database.schema)
    async with database.engine.begin() as connection:
        await _insert_factorized_card_fixture(connection, database.schema, projection_id)
        await _current_offices(connection, database.schema, has_postal_fields)
        frozen = await serving._factorized_card_complete_selected_npis(
            connection,
            projection_id,
            ("CPT", "27447"),
            ["10002", "10001"],
            {},
            (101,),
            {101: Decimal(10)},
        )
        current = await serving._factorized_card_complete_selected_npis(
            connection,
            projection_id,
            ("CPT", "27447"),
            ["10002", "10001"],
            {"min_address_sources": "3"},
            (101,),
            {101: Decimal(10)},
        )
    assert frozen[0]["city"] == "Chicago" and frozen[0]["state"] == "IL"
    assert current[0]["city"] == ("Qualified City" if has_postal_fields else None)
    assert current[0]["state"] == ("IN" if has_postal_fields else None)
    for field in set(frozen[0]) - {"city", "state"}:
        assert current[0][field] == frozen[0][field]
    assert current[0]["minimum_negotiated_rate"] == 10 and current[0]["maximum_negotiated_rate"] == 100
    assert current[0]["rate_count"] == 6


async def _store_legacy_card(connection, schema, projection_id, fragment, minimum, maximum):
    await connection.execute(
        text(f'''INSERT INTO "{schema}".plan_pricing_projection_candidate (
            projection_id, contract_version, binding_manifest_digest, binding_manifest, provider_signature, state
        ) VALUES (:projection_id, 'plan_pricing_card_v2', :projection_id, '[]'::jsonb, :projection_id, 'building')'''),
        {"projection_id": projection_id},
    )
    await connection.execute(
        text(f'''INSERT INTO "{schema}".plan_pricing_card (
            projection_id, code_system, code, geo_cell, npi, minimum_negotiated_rate, maximum_negotiated_rate, rate_count, fragment
        ) VALUES (:projection_id, 'CPT', '27447', '10002', 101, :minimum, :maximum, 2, :fragment)'''),
        {"projection_id": projection_id, "fragment": fragment, "minimum": minimum, "maximum": maximum},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("has_postal_fields", [True, False])
async def test_legacy_card_keeps_frozen_rates_and_hydrates_the_same_qualified_cell(
    monkeypatch,
    migrated_v3_database,
    has_postal_fields,
):
    database, projection_id = migrated_v3_database, "b" * 64
    _patch_schema(monkeypatch, database.schema)
    frozen_provider_by_field = {
        "npi": 101,
        "provider_name": "Frozen Provider",
        "zip5": "10002",
        "city": "Frozen City",
        "state": "IL",
    }
    minimum = Decimal("9007199254740993.12345")
    maximum = Decimal("184467440737095516160000000000000000001")
    fragment = card_fragment(CardStats(frozen_provider_by_field, minimum, maximum, 2))
    request = SimpleNamespace(
        result_type="provider_cards",
        contract=contract.LEGACY_PROJECTION_CONTRACT,
        projection_id=projection_id,
        code_system="CPT",
        code="27447",
    )
    async with database.engine.begin() as connection:
        await _store_legacy_card(connection, database.schema, projection_id, fragment, minimum, maximum)
        await _current_offices(connection, database.schema, has_postal_fields)
        original, total = await projections._read_page(
            connection, request, ["10002"], {}, SimpleNamespace(limit=1, offset=0)
        )
        current, qualified_total = await projections._read_page(
            connection, request, ["10002"], {"min_address_sources": "3"}, SimpleNamespace(limit=1, offset=0)
        )
        empty, empty_total = await projections._read_page(
            connection, request, ["10002"], {"min_address_sources": "4"}, SimpleNamespace(limit=1, offset=0)
        )
    assert total == qualified_total == 1 and empty == [] and empty_total == 0
    assert orjson.dumps(original[0]) == fragment
    card_by_field = json.loads(orjson.dumps(current[0]), parse_float=Decimal, parse_int=Decimal)
    expected_by_field = json.loads(fragment, parse_float=Decimal, parse_int=Decimal) | {
        "city": "Qualified City" if has_postal_fields else None,
        "state": "IN" if has_postal_fields else None,
    }
    assert card_by_field == expected_by_field
