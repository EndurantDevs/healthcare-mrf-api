# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL proof for bounded factorized-card serving."""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import text

from api import plan_pricing_projection_contract as projection_contract
from api import ptg2_serving as serving
from tests.ptg2_factorized_card_support import provider_cell_row
from tests.test_plan_pricing_projection_v3_differential_postgres import (
    _insert_candidate,
    migrated_v3_database,
)


def _stored_cell_by_field(projection_id, npi, zip5, taxonomy_code):
    """Return one insertable frozen provider cell."""

    provider_by_field = provider_cell_row(npi, zip5)
    provider_by_field.update(
        projection_id=projection_id,
        taxonomy_codes=[taxonomy_code],
    )
    return provider_by_field


async def _insert_memberships(connection, schema, projection_id):
    """Insert the cross-binding provider memberships used by the proof."""

    await connection.execute(
        text(
            f"""INSERT INTO "{schema}".plan_pricing_provider_membership (
                projection_id, binding_ordinal, provider_set_key, npi
            ) VALUES (
                :projection_id, :binding_ordinal, :provider_set_key, :npi
            )"""
        ),
        [
            {"projection_id": projection_id, "binding_ordinal": 0,
             "provider_set_key": 10, "npi": 101},
            {"projection_id": projection_id, "binding_ordinal": 0,
             "provider_set_key": 10, "npi": 102},
            {"projection_id": projection_id, "binding_ordinal": 1,
             "provider_set_key": 20, "npi": 101},
            {"projection_id": projection_id, "binding_ordinal": 2,
             "provider_set_key": 30, "npi": 103},
        ],
    )


async def _insert_provider_cells(connection, schema, projection_id):
    """Insert frozen cells, including one taxonomy-ineligible provider."""

    await connection.execute(
        text(
            f"""INSERT INTO "{schema}".plan_pricing_provider_cell (
                projection_id, geo_cell, npi, entity_type_code,
                taxonomy_codes, fragment
            ) VALUES (
                :projection_id, :geo_cell, :npi, :entity_type_code,
                :taxonomy_codes, :fragment
            )"""
        ),
        [
            _stored_cell_by_field(
                projection_id, 101, "10001", "207X00000X"
            ),
            _stored_cell_by_field(
                projection_id, 101, "10002", "207X00000X"
            ),
            _stored_cell_by_field(
                projection_id, 102, "10001", "207X00000X"
            ),
            _stored_cell_by_field(
                projection_id, 103, "10001", "208D00000X"
            ),
        ],
    )


async def _insert_rate_profiles(connection, schema, projection_id):
    """Insert low-cost ties plus a second-binding completion profile."""

    await connection.execute(
        text(
            f"""INSERT INTO "{schema}".plan_pricing_rate_profile (
                projection_id, code_system, code, binding_ordinal,
                provider_set_key, membership_count,
                minimum_negotiated_rate, maximum_negotiated_rate,
                rate_count, negotiated_rates, rate_multiplicities
            ) VALUES (
                :projection_id, 'CPT', '27447', :binding_ordinal,
                :provider_set_key, :membership_count, :minimum_rate,
                :maximum_rate, :rate_count, :rates, :multiplicities
            )"""
        ),
        [
            {"projection_id": projection_id, "binding_ordinal": 0,
             "provider_set_key": 10, "membership_count": 2,
             "minimum_rate": 10, "maximum_rate": 20, "rate_count": 3,
             "rates": [10, 20], "multiplicities": [2, 1]},
            {"projection_id": projection_id, "binding_ordinal": 1,
             "provider_set_key": 20, "membership_count": 1,
             "minimum_rate": 100, "maximum_rate": 100, "rate_count": 3,
             "rates": [100], "multiplicities": [3]},
            {"projection_id": projection_id, "binding_ordinal": 2,
             "provider_set_key": 30, "membership_count": 1,
             "minimum_rate": 5, "maximum_rate": 5, "rate_count": 1,
             "rates": [5], "multiplicities": [1]},
        ],
    )


async def _insert_factorized_card_fixture(connection, schema, projection_id):
    """Seed one projection using the real durable v3 schema."""

    await _insert_candidate(connection, schema, projection_id)
    await _insert_memberships(connection, schema, projection_id)
    await _insert_provider_cells(connection, schema, projection_id)
    await _insert_rate_profiles(connection, schema, projection_id)


@pytest.mark.asyncio
async def test_factorized_card_queries_merge_bindings_from_frozen_rows(
    monkeypatch,
    migrated_v3_database,
):
    """Candidate and completion SQL preserve ties, scope, and exact stats."""

    database = migrated_v3_database
    projection_id = "d" * 64
    async with database.engine.begin() as connection:
        await _insert_factorized_card_fixture(
            connection, database.schema, projection_id
        )
        with monkeypatch.context() as scoped:
            scoped.setattr(projection_contract, "SCHEMA", database.schema)
            candidate_selection = await serving._factorized_card_candidates(
                connection,
                projection_id,
                ("CPT", "27447"),
                ["10002", "10001"],
                {"code_system": "CPT", "code": "27447"},
                1,
            )
            card_items = await serving._factorized_card_complete_selected_npis(
                connection,
                projection_id,
                ("CPT", "27447"),
                ["10002", "10001"],
                {"code_system": "CPT", "code": "27447"},
                (101, 102),
                candidate_selection.minimum_rate_by_npi,
            )

    assert candidate_selection.minimum_rate_by_npi == {
        101: Decimal("10"),
        102: Decimal("10"),
    }
    assert candidate_selection.total_lower_bound == 2
    assert candidate_selection.total_is_exact is True
    assert [
        provider_card_by_field["npi"]
        for provider_card_by_field in card_items
    ] == [101, 102]
    assert card_items[0]["zip5"] == "10002"
    assert card_items[0]["maximum_negotiated_rate"] == 100
    assert card_items[0]["rate_count"] == 6
