# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL safety proof for bounded factorized price staging."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from api import plan_pricing_projection_contract as projection_contract
from api import plan_pricing_projection_v3 as projection
from api import plan_pricing_projection_v3_code as rate_profiles
from api import plan_pricing_projection_v3_work as work_admission
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
