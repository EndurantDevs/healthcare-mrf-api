# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL safety proof for bounded factorized price staging."""

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from api import plan_pricing_projection_contract as projection_contract
from api import plan_pricing_projection_v3 as projection
from api import plan_pricing_projection_v3_code as rate_profiles
from api import plan_pricing_projection_v3_work as work_admission
from api import plan_pricing_projection_v4_occurrence as rate_occurrences
from api import ptg2_serving as serving
from api.plan_pricing_projection_source import BindingProjection
from api.plan_pricing_projection_v3_types import _BuildState
from tests.test_plan_pricing_projection_v3_differential_postgres import (
    _insert_candidate,
    _membership,
    _occurrence,
    _rate,
    _stage_rows,
    migrated_v3_database,
)


_PROJECTION_ID = "d" * 64
_PRICE_IDS = ("1" * 32, "2" * 32)


def _large_occurrence_fragment() -> str:
    description = "".join(
        hashlib.sha256(index.to_bytes(2, "big")).hexdigest()
        for index in range(90)
    )
    fragment = json.dumps(
        {
            "plan_id": "synthetic-plan",
            "reported_code_system": "CPT",
            "reported_code": "27447",
            "source_procedure_description": description,
        },
        separators=(",", ":"),
    )
    assert 4_000 < len(fragment) < 8_192
    return fragment


async def _create_occurrence_target(connection, schema: str) -> None:
    await connection.execute(
        text(
            f"""CREATE TABLE \"{schema}\".plan_pricing_rate_occurrence (
                projection_id varchar(64) NOT NULL,
                code_system varchar(16) NOT NULL,
                code varchar(64) NOT NULL,
                binding_ordinal integer NOT NULL,
                occurrence_ordinal bigint NOT NULL,
                provider_set_key bigint NOT NULL,
                provider_set_ref varchar(32) NOT NULL,
                price_key bigint NOT NULL,
                price_set_ref varchar(32) NOT NULL,
                rate_pack_ref varchar(32) NOT NULL,
                source_artifact_key bigint NOT NULL,
                provider_count integer NOT NULL,
                group_fragment jsonb NOT NULL,
                occurrence_multiplicity bigint NOT NULL
            )"""
        )
    )


async def _stage_occurrences(connection, group_fragment: str) -> None:
    await connection.execute(
        text(
            """INSERT INTO plan_pricing_provider_set_stage
               (binding_ordinal, provider_set_key, provider_set_id,
                membership_count)
               VALUES (0, 7, :empty_ref, 0), (0, 8, :member_ref, 1)"""
        ),
        {"empty_ref": "7" * 32, "member_ref": "8" * 32},
    )
    await connection.execute(
        text(
            """INSERT INTO plan_pricing_rate_occurrence_stage (
                binding_ordinal, provider_set_key, provider_set_ref,
                price_key, price_set_ref, rate_pack_ref, source_artifact_key,
                provider_count, group_fragment, occurrence_multiplicity
            ) VALUES (
                0, :provider_set_key, :provider_set_ref,
                9, :price_set_ref, :rate_pack_ref, 11,
                :provider_count, CAST(:group_fragment AS jsonb), 1
            )"""
        ),
        [
            {
                "provider_set_key": provider_set_key,
                "provider_set_ref": str(provider_set_key) * 32,
                "price_set_ref": "2" * 32,
                "rate_pack_ref": "3" * 32,
                "provider_count": membership_count,
                "group_fragment": group_fragment,
            }
            for provider_set_key, membership_count in ((7, 0), (8, 1))
        ],
    )


def _overflow_binding() -> BindingProjection:
    return BindingProjection(
        {"ordinal": 0},
        SimpleNamespace(
            network_names=(),
            price_key_block_span=512,
            shared_snapshot_key=1,
            uses_shared_blocks=True,
        ),
        {("CPT", "27447"): [{"code_key": 1, "rate_count": 2}]},
        1,
    )


async def _overflow_code_rows(_session, _binding, _code_rows):
    return [
        {
            "_ptg_provider_set_key": 7,
            "price_set_global_id_128": price_id,
        }
        for price_id in _PRICE_IDS
    ], dict(zip(_PRICE_IDS, (1, 2), strict=True))


async def _overflow_prices(_session, _tables, price_keys, **_kwargs):
    normalized_keys = tuple(price_keys)
    if normalized_keys == (1,):
        return {1: [{"negotiated_rate": "10"}]}
    raise serving.ManifestReadLimitError("atom limit")


async def _stage_partial_then_fail(connection, monkeypatch) -> None:
    stage_provider_sets = AsyncMock()
    with monkeypatch.context() as scoped:
        scoped.setattr(serving, "_declared_geo_rate_count", lambda _rows: 2)
        scoped.setattr(serving, "_ptg2_manifest_id", str)
        scoped.setattr(
            serving,
            "_version_three_bounded_prices_by_key",
            _overflow_prices,
        )
        with pytest.raises(serving.ManifestReadLimitError):
            await rate_profiles._has_staged_code_inputs(
                connection,
                _BuildState(hashlib.sha256()),
                ("CPT", "27447"),
                [_overflow_binding()],
                binding_code_rows=_overflow_code_rows,
                stage_code_provider_sets=stage_provider_sets,
                preflight_price_membership_aliases=AsyncMock(),
            )
    stage_provider_sets.assert_not_awaited()


async def _durable_projection_counts(connection, schema: str):
    table_names = (
        "plan_pricing_projection_candidate",
        "plan_pricing_provider_membership",
        "plan_pricing_provider_cell",
        "plan_pricing_rate_profile",
        "plan_pricing_aggregate_pack",
        "plan_pricing_prewarm_shape",
    )
    counts = []
    for table_name in table_names:
        counts.append(
            await connection.scalar(
                text(
                    f'SELECT COUNT(*) FROM "{schema}".{table_name} '
                    "WHERE projection_id = :projection_id"
                ),
                {"projection_id": _PROJECTION_ID},
            )
        )
    return tuple(counts)


@pytest.mark.asyncio
async def test_v4_occurrence_stage_accepts_large_fragments_and_omits_empty_sets(
    monkeypatch,
    migrated_v3_database,
) -> None:
    """Exercise both occurrence-stage fixes against real PostgreSQL."""

    database = migrated_v3_database
    group_fragment = _large_occurrence_fragment()
    state = _BuildState(hashlib.sha256())
    async with database.engine.begin() as connection:
        await _create_occurrence_target(connection, database.schema)
        await projection._create_stage_tables(connection)
        await _stage_occurrences(connection, group_fragment)
        configured_target = projection_contract.table(
            "plan_pricing_rate_occurrence"
        )
        test_target = (
            f'"{database.schema}"."plan_pricing_rate_occurrence"'
        )
        monkeypatch.setattr(
            rate_occurrences,
            "_STORE_OCCURRENCES_SQL",
            rate_occurrences._STORE_OCCURRENCES_SQL.replace(
                configured_target, test_target
            ),
        )
        monkeypatch.setattr(
            rate_occurrences,
            "_READ_OCCURRENCES_SQL",
            rate_occurrences._READ_OCCURRENCES_SQL.replace(
                configured_target, test_target
            ),
        )

        await rate_occurrences.store_rate_occurrences(
            connection,
            "f" * 64,
            ("CPT", "27447"),
            state,
        )
        stored_rows = (
            await connection.execute(
                text(
                    f"SELECT provider_set_key, group_fragment "
                    f"FROM {test_target}"
                )
            )
        ).mappings().all()

    assert [stored_row["provider_set_key"] for stored_row in stored_rows] == [8]
    assert stored_rows[0]["group_fragment"] == json.loads(group_fragment)
    assert state.rate_occurrence_count == 1


@pytest.mark.asyncio
async def test_v3_profile_rate_limit_precedes_persistent_insert(
    monkeypatch,
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    projection_id = "e" * 64
    async with database.engine.begin() as connection:
        await _insert_candidate(connection, database.schema, projection_id)
        await projection._create_stage_tables(connection)
        await _stage_rows(
            connection,
            [_membership(0, 10, 1000000001)],
            [_occurrence(0, 10, "a"), _occurrence(0, 10, "b")],
            [_rate(0, "a", "10"), _rate(0, "b", "20")],
        )
        await connection.execute(text(work_admission._RATE_FREQUENCY_INSERT_SQL))
        with monkeypatch.context() as scoped:
            scoped.setattr(projection_contract, "SCHEMA", database.schema)
            scoped.setattr(rate_profiles, "MAX_RATE_PROFILE_RATES", 1)
            with pytest.raises(ValueError, match="rate profile is too large"):
                await rate_profiles._store_rate_profiles(
                    connection,
                    projection_id,
                    ("CPT", "27447"),
                    _BuildState(hashlib.sha256()),
                )
        stored_count = await connection.scalar(
            text(
                f'SELECT COUNT(*) FROM "{database.schema}".'
                "plan_pricing_rate_profile WHERE projection_id = :projection_id"
            ),
            {"projection_id": projection_id},
        )

    assert stored_count == 0


@pytest.mark.asyncio
async def test_partial_price_stage_rolls_back_after_terminal_batch_overflow(
    monkeypatch,
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    async with database.engine.connect() as connection:
        transaction = await connection.begin()
        try:
            await _insert_candidate(connection, database.schema, _PROJECTION_ID)
            await projection._create_stage_tables(connection)
            await _stage_partial_then_fail(connection, monkeypatch)
            staged_counts = (
                await connection.execute(
                    text(
                        """
                        SELECT
                          (SELECT COUNT(*) FROM plan_pricing_price_rate_stage),
                          (SELECT COUNT(*) FROM plan_pricing_code_occurrence_stage),
                          (SELECT COUNT(*) FROM plan_pricing_provider_set_stage)
                        """
                    )
                )
            ).one()
            assert staged_counts == (1, 0, 0)
        finally:
            await transaction.rollback()
        temporary_stage_count = await connection.scalar(
            text(
                "SELECT COUNT(*) FROM pg_class "
                "WHERE relnamespace = pg_my_temp_schema() "
                "AND relname LIKE 'plan_pricing_%_stage'"
            )
        )
        assert temporary_stage_count == 0

    async with database.engine.connect() as connection:
        assert await _durable_projection_counts(
            connection, database.schema
        ) == (0, 0, 0, 0, 0, 0)
