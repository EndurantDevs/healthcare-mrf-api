# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL admission-order proof for projection v3."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from api import plan_pricing_projection_v3 as projection
from api import plan_pricing_projection_v3_work as work_admission
from api.plan_pricing_aggregate_pack import AggregateZipRecord
from tests.test_plan_pricing_projection_v3_differential_postgres import (
    _aggregate_records,
    _insert_candidate,
    _insert_provider_cells,
    _membership,
    _occurrence,
    _prepare_code_work_under_limits,
    _rate,
    _stage_rows,
    _stage_three_binding_inputs,
    _stored_rate_profiles,
    migrated_v3_database,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limit_name", "limit_value", "error", "projection_id"),
    (
        ("MAX_CODE_MEMBER_CELL_WORK_ROWS", 7, "member-cell", "d" * 64),
        ("MAX_CODE_RATE_PROFILE_WORK_ROWS", 4, "rate-profile", "e" * 64),
        ("MAX_CODE_AGGREGATE_WORK_ROWS", 10, "aggregate", "f" * 64),
    ),
)
async def test_v3_sql_work_caps_reject_before_persistent_projection_rows(
    monkeypatch,
    migrated_v3_database,
    limit_name: str,
    limit_value: int,
    error: str,
    projection_id: str,
) -> None:
    database = migrated_v3_database
    async with database.engine.begin() as connection:
        await _insert_candidate(connection, database.schema, projection_id)
        await projection._create_stage_tables(connection)
        await _insert_provider_cells(connection, projection_id)
        await _stage_three_binding_inputs(connection)

        with pytest.raises(ValueError, match=error):
            await _prepare_code_work_under_limits(
                connection,
                monkeypatch,
                database.schema,
                projection_id,
                ("HCPCS", "G0439"),
                None,
                **{limit_name: limit_value},
            )

        if limit_name == "MAX_CODE_RATE_PROFILE_WORK_ROWS":
            for relation in (
                "plan_pricing_eligible_member_cell_stage",
                "plan_pricing_set_cell_stage",
                "plan_pricing_rate_frequency_stage",
            ):
                assert (
                    await connection.scalar(text(f"SELECT COUNT(*) FROM {relation}"))
                    == 0
                )

        stored_counts = await connection.execute(
            text(
                f"""SELECT
                    (SELECT COUNT(*) FROM "{database.schema}".
                        plan_pricing_provider_membership
                     WHERE projection_id = :projection_id),
                    (SELECT COUNT(*) FROM "{database.schema}".
                        plan_pricing_provider_cell
                     WHERE projection_id = :projection_id),
                    (SELECT COUNT(*) FROM "{database.schema}".
                        plan_pricing_rate_profile
                     WHERE projection_id = :projection_id),
                    (SELECT COUNT(*) FROM "{database.schema}".
                        plan_pricing_aggregate_pack
                     WHERE projection_id = :projection_id)"""
            ),
            {"projection_id": projection_id},
        )
        assert stored_counts.one() == (0, 0, 0, 0)


async def _stage_empty_provider_set(
    connection,
    binding_ordinal: int,
    provider_set_key: int,
) -> None:
    await connection.execute(
        text(
            """INSERT INTO plan_pricing_provider_set_stage
               (binding_ordinal, provider_set_key, provider_set_id,
                membership_count)
               VALUES (:binding_ordinal, :provider_set_key,
                       :provider_set_id, 0)"""
        ),
        {
            "binding_ordinal": binding_ordinal,
            "provider_set_key": provider_set_key,
            "provider_set_id": "e" * 32,
        },
    )


@pytest.mark.asyncio
async def test_v3_sql_matches_v2_empty_provider_set_semantics(
    monkeypatch,
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    projection_id = "b" * 64
    async with database.engine.begin() as connection:
        await _insert_candidate(connection, database.schema, projection_id)
        await projection._create_stage_tables(connection)
        await _insert_provider_cells(connection, projection_id)
        await _stage_rows(
            connection,
            [_membership(0, 10, 1000000001)],
            [_occurrence(0, 10, "a"), _occurrence(0, 11, "b")],
            [_rate(0, "a", "10"), _rate(0, "b", "999")],
        )
        await _stage_empty_provider_set(connection, 0, 11)
        prepared_work, _state = await _prepare_code_work_under_limits(
            connection,
            monkeypatch,
            database.schema,
            projection_id,
            ("HCPCS", "G0439"),
            None,
        )
        stored_profiles, _profile_state = await _stored_rate_profiles(
            connection,
            monkeypatch,
            database.schema,
            projection_id,
            ("HCPCS", "G0439"),
        )
        actual_records = await _aggregate_records(
            connection,
            projection_id,
            ("HCPCS", "G0439"),
        )

    assert prepared_work == work_admission._CodeWork(
        1, 2, 2, 1, 2, 1, 1, 1, 2, 1
    )
    assert len(stored_profiles) == 1
    assert stored_profiles[0]["provider_set_key"] == 10
    assert actual_records == (
        AggregateZipRecord(
            "10001", 1, 1, Decimal("10"), Decimal("10"), Decimal("10")
        ),
        AggregateZipRecord(
            "10003", 1, 1, Decimal("10"), Decimal("10"), Decimal("10")
        ),
    )


@pytest.mark.asyncio
async def test_v3_aggregate_work_counts_distinct_rates_per_cell(
    monkeypatch,
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    projection_id = "a" * 64
    async with database.engine.begin() as connection:
        await _insert_candidate(connection, database.schema, projection_id)
        await projection._create_stage_tables(connection)
        await _insert_provider_cells(connection, projection_id)
        await _stage_rows(
            connection,
            [_membership(0, 10, 1000000001)],
            [_occurrence(0, 10, "a"), _occurrence(0, 10, "b")],
            [_rate(0, "a", "10"), _rate(0, "b", "10", 5)],
        )

        prepared_work, _state = await _prepare_code_work_under_limits(
            connection,
            monkeypatch,
            database.schema,
            projection_id,
            ("HCPCS", "G0439"),
            None,
        )

    assert prepared_work == work_admission._CodeWork(
        1, 2, 2, 2, 2, 6, 6, 1, 12, 6
    )


async def _cancelled_build(
    engine: AsyncEngine,
    schema: str,
    projection_id: str,
    inserted: asyncio.Event,
) -> None:
    async with engine.begin() as connection:
        await _insert_candidate(connection, schema, projection_id)
        await connection.execute(
            text(
                f"""INSERT INTO "{schema}".plan_pricing_provider_membership
                    (projection_id, binding_ordinal, provider_set_key, npi)
                    VALUES (:projection_id, 0, 1, 1000000001)"""
            ),
            {"projection_id": projection_id},
        )
        await connection.execute(
            text(
                f"""INSERT INTO "{schema}".plan_pricing_provider_cell
                    (projection_id, geo_cell, npi, entity_type_code,
                     taxonomy_codes, fragment)
                    VALUES (:projection_id, '10001', 1000000001, 1,
                            ARRAY['207X00000X'], :fragment)"""
            ),
            {"projection_id": projection_id, "fragment": b"{}"},
        )
        await connection.execute(
            text(
                f"""INSERT INTO "{schema}".plan_pricing_rate_profile (
                    projection_id, code_system, code, binding_ordinal,
                    provider_set_key, membership_count,
                    minimum_negotiated_rate, maximum_negotiated_rate,
                    rate_count, negotiated_rates, rate_multiplicities
                ) VALUES (
                    :projection_id, 'CPT', '27447', 0, 1, 1,
                    10, 10, 1, ARRAY[10]::numeric[], ARRAY[1]::bigint[]
                )"""
            ),
            {"projection_id": projection_id},
        )
        inserted.set()
        await asyncio.Future()


@pytest.mark.asyncio
async def test_cancelled_transaction_leaves_no_candidate_or_child_rows(
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    projection_id = "c" * 64
    inserted = asyncio.Event()
    build_task = asyncio.create_task(
        _cancelled_build(database.engine, database.schema, projection_id, inserted)
    )
    await asyncio.wait_for(inserted.wait(), timeout=2)
    build_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(build_task, timeout=2)

    async with database.engine.connect() as connection:
        counts = await connection.execute(
            text(
                f"""SELECT
                  (SELECT COUNT(*) FROM "{database.schema}".
                    plan_pricing_projection_candidate WHERE projection_id = :id),
                  (SELECT COUNT(*) FROM "{database.schema}".
                    plan_pricing_provider_membership WHERE projection_id = :id),
                  (SELECT COUNT(*) FROM "{database.schema}".
                    plan_pricing_provider_cell WHERE projection_id = :id),
                  (SELECT COUNT(*) FROM "{database.schema}".
                    plan_pricing_rate_profile WHERE projection_id = :id)"""
            ),
            {"id": projection_id},
        )
        assert counts.one() == (0, 0, 0, 0)
