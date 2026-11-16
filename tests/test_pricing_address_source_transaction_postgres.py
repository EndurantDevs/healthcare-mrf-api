# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A request uses one office generation across pricing selection and hydration."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api import plan_pricing_projection_contract as projection_contract
from api import pricing_address_source_filter as source_filter
from api import ptg2_serving as serving
from tests.test_plan_pricing_projection_v3_differential_postgres import migrated_v3_database
from tests.test_ptg2_factorized_card_postgres import _insert_factorized_card_fixture


async def _insert_office_generation(connection, schema, projection_id):
    await _insert_factorized_card_fixture(connection, schema, projection_id)
    await connection.execute(
        text(f'''CREATE TABLE "{schema}".entity_address_unified (
        npi bigint, location_key text, zip5 text, postal_code text, type text, source_count integer,
            city_name text DEFAULT 'Example City', state_name text DEFAULT 'IL', state_code text DEFAULT 'IL'
    )''')
    )
    await connection.execute(
        text(f'''INSERT INTO "{schema}".entity_address_unified (npi, location_key, zip5, postal_code, type, source_count) VALUES
        (101,'qualified','10002','10002','practice',3)
    ''')
    )


async def _select_page(connection, projection_id):
    return await serving._factorized_card_candidates(
        connection, projection_id, ("CPT", "27447"), ["10002", "10001"], {"min_address_sources": "3"}, 1
    )


@pytest.mark.asyncio
async def test_office_name_switch_waits_until_request_selection_and_hydration_finish(monkeypatch, migrated_v3_database):
    database = migrated_v3_database
    projection_id = "f" * 64
    monkeypatch.setattr(projection_contract, "SCHEMA", database.schema)
    monkeypatch.setattr(source_filter, "SCHEMA", database.schema)
    async with database.engine.begin() as setup:
        await _insert_office_generation(setup, database.schema, projection_id)
    async with database.engine.begin() as request:
        selected = await _select_page(request, projection_id)
        assert tuple(selected.minimum_rate_by_npi) == (101,)
        with pytest.raises(DBAPIError) as failure:
            async with database.engine.begin() as publisher:
                await publisher.execute(text("SET LOCAL lock_timeout = '100ms'"))
                await publisher.execute(
                    text(f'ALTER TABLE "{database.schema}".entity_address_unified RENAME TO previous_offices')
                )
        assert failure.value.orig.sqlstate == "55P03"
        cards = await serving._factorized_card_complete_selected_npis(
            request,
            projection_id,
            ("CPT", "27447"),
            ["10002", "10001"],
            {"min_address_sources": "3"},
            (101,),
            selected.minimum_rate_by_npi,
        )
        assert len(cards) == 1 and cards[0]["zip5"] == "10002"
    async with database.engine.begin() as publisher:
        await publisher.execute(
            text(f'ALTER TABLE "{database.schema}".entity_address_unified RENAME TO previous_offices')
        )
        await publisher.execute(
            text(f'CREATE TABLE "{database.schema}".entity_address_unified (LIKE "{database.schema}".previous_offices)')
        )
    async with database.engine.begin() as next_request:
        selected = await _select_page(next_request, projection_id)
        assert selected.minimum_rate_by_npi == {} and selected.total_is_exact
