# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for optional legacy-sweeper stages."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy.exc import DBAPIError

from process.ptg_parts import ptg2_legacy_orphan_sweeper as legacy_sweeper
from process.ptg_parts.ptg2_legacy_orphan_contract import LegacySweepLimits
from process.ptg_parts.ptg2_legacy_orphan_store import (
    require_legacy_sweep_schema,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _MRF_OPTIONAL_TABLES,
)
from process.ptg_parts.ptg2_legacy_orphan_store_mutation import (
    lock_legacy_sweep_authority as _lock_legacy_sweep_authority,
)
from process.ptg_parts.ptg2_legacy_orphan_sweeper import (
    build_legacy_orphan_sweep_plan,
    execute_legacy_orphan_sweep,
)
from tests.test_ptg2_legacy_orphan_postgres_support import (
    LIMITS,
    _PostgresCase,
    _create_root,
    _has_relation,
    _prepared_case,
    _q,
    _seed_terminal_owner,
)


async def _set_optional_stage_presence(
    case: _PostgresCase,
    present_names: frozenset[str],
) -> None:
    async with case.database.acquire() as connection:
        for table_name in _MRF_OPTIONAL_TABLES:
            if table_name not in present_names:
                await connection.status(
                    f"DROP TABLE {_q(case.mrf_schema)}.{_q(table_name)}"
                )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "present_names",
    (
        frozenset(),
        frozenset({"ptg2_price_set_stage"}),
        frozenset(_MRF_OPTIONAL_TABLES),
    ),
)
async def test_legacy_sweep_plans_with_each_optional_stage_presence(
    monkeypatch,
    present_names: frozenset[str],
) -> None:
    suffix = "b" * 32
    async with _prepared_case(monkeypatch) as case:
        await _set_optional_stage_presence(case, present_names)
        async with case.database.acquire() as connection:
            await _create_root(
                connection, case.mrf_schema, suffix, populated=False
            )
        authority = await require_legacy_sweep_schema(
            case.database,
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
        )
        plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=LIMITS,
            executor=case.database,
        )

        assert authority.present_optional_table_names == tuple(
            sorted(present_names)
        )
        assert [candidate.suffix for candidate in plan.candidates] == [suffix]


@pytest.mark.asyncio
@pytest.mark.parametrize("table_name", _MRF_OPTIONAL_TABLES)
async def test_present_optional_stage_rows_block_cleanup(
    monkeypatch,
    table_name: str,
) -> None:
    suffix = "c" * 32
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            snapshot_id = await _seed_terminal_owner(
                connection,
                case.mrf_schema,
                case.control_schema,
                suffix,
            )
            await connection.status(
                f"INSERT INTO {_q(case.mrf_schema)}.{_q(table_name)} "
                "(snapshot_id) VALUES (:snapshot_id)",
                snapshot_id=snapshot_id,
            )
        plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=LIMITS,
            executor=case.database,
        )
        reasons_by_suffix = {
            blocked.suffix: blocked.reasons for blocked in plan.blocked
        }

        assert reasons_by_suffix[suffix] == (
            "serving_or_lifecycle_reference",
        )
        assert plan.candidates == ()


async def _mutate_optional_catalog(
    connection,
    *,
    schema_name: str,
    mutation: str,
) -> None:
    table = f"{_q(schema_name)}.{_q('ptg2_price_set_stage')}"
    if mutation == "create":
        await connection.status(f"CREATE TABLE {table} (snapshot_id text)")
    elif mutation == "drop":
        await connection.status(f"DROP TABLE {table}")
    elif mutation == "oid":
        await connection.status(f"DROP TABLE {table}")
        await connection.status(f"CREATE TABLE {table} (snapshot_id text)")
    elif mutation == "shape":
        await connection.status(
            f"ALTER TABLE {table} ADD COLUMN ordinal bigint"
        )
    else:
        raise AssertionError(f"unsupported mutation: {mutation}")


async def _plan_single_empty_suffix(case: _PostgresCase, suffix: str):
    async with case.database.acquire() as connection:
        await _create_root(
            connection, case.mrf_schema, suffix, populated=False
        )
    return await build_legacy_orphan_sweep_plan(
        schema_name=case.mrf_schema,
        control_schema_name=case.control_schema,
        limits=LIMITS,
        executor=case.database,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ("create", "drop", "oid", "shape"))
async def test_optional_catalog_drift_after_plan_refuses_apply(
    monkeypatch,
    mutation: str,
) -> None:
    async with _prepared_case(monkeypatch) as case:
        if mutation == "create":
            await _set_optional_stage_presence(
                case, frozenset({"ptg2_serving_rate_stage"})
            )
        plan = await _plan_single_empty_suffix(case, "d" * 32)
        async with case.database.acquire() as connection:
            await _mutate_optional_catalog(
                connection,
                schema_name=case.mrf_schema,
                mutation=mutation,
            )

        with pytest.raises(
            RuntimeError,
            match="legacy_sweep_plan_digest_changed",
        ):
            await execute_legacy_orphan_sweep(
                expected_plan_digest=plan.plan_digest,
                actor="postgres-test",
                schema_name=case.mrf_schema,
                control_schema_name=case.control_schema,
                limits=LIMITS,
                database=case.database,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ("create", "drop", "oid", "shape"))
async def test_optional_catalog_drift_after_authority_lock_fails_closed(
    monkeypatch,
    mutation: str,
) -> None:
    async with _prepared_case(monkeypatch) as case:
        if mutation == "create":
            await _set_optional_stage_presence(
                case, frozenset({"ptg2_serving_rate_stage"})
            )
        plan = await _plan_single_empty_suffix(case, "e" * 32)

        async def mutate_after_lock(executor, **parameters) -> None:
            await _lock_legacy_sweep_authority(executor, **parameters)
            await _mutate_optional_catalog(
                executor,
                schema_name=case.mrf_schema,
                mutation=mutation,
            )

        monkeypatch.setattr(
            legacy_sweeper,
            "lock_legacy_sweep_authority",
            mutate_after_lock,
        )
        with pytest.raises(
            RuntimeError,
            match="legacy_sweep_authority_catalog_changed",
        ):
            await execute_legacy_orphan_sweep(
                expected_plan_digest=plan.plan_digest,
                actor="postgres-test",
                schema_name=case.mrf_schema,
                control_schema_name=case.control_schema,
                limits=LIMITS,
                database=case.database,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ("create", "drop"))
async def test_catalog_lock_blocks_concurrent_optional_relation_ddl(
    monkeypatch,
    mutation: str,
) -> None:
    """Fence concurrent create and drop after the final catalog snapshot."""

    async with _prepared_case(monkeypatch) as case:
        if mutation == "create":
            await _set_optional_stage_presence(
                case, frozenset({"ptg2_serving_rate_stage"})
            )
        async with case.database.acquire() as sweep_connection:
            authority = await require_legacy_sweep_schema(
                sweep_connection,
                schema_name=case.mrf_schema,
                control_schema_name=case.control_schema,
            )
            await _lock_legacy_sweep_authority(
                sweep_connection,
                schema_name=case.mrf_schema,
                control_schema_name=case.control_schema,
                lock_timeout="5s",
                present_optional_table_names=(
                    authority.present_optional_table_names
                ),
            )
            assert await require_legacy_sweep_schema(
                sweep_connection,
                schema_name=case.mrf_schema,
                control_schema_name=case.control_schema,
            ) == authority

            async with case.database.acquire() as ddl_connection:
                await ddl_connection.status(
                    "SELECT set_config('lock_timeout', '50ms', true)"
                )
                with pytest.raises(DBAPIError):
                    await asyncio.wait_for(
                        _mutate_optional_catalog(
                            ddl_connection,
                            schema_name=case.mrf_schema,
                            mutation=mutation,
                        ),
                        timeout=2,
                    )


@pytest.mark.asyncio
async def test_required_relation_absence_still_refuses_planning(
    monkeypatch,
) -> None:
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            await connection.status(
                f"DROP TABLE {_q(case.mrf_schema)}.ptg2_plan_month"
            )
        with pytest.raises(
            RuntimeError,
            match=(
                "legacy_sweep_required_relations_missing:"
                f"{case.mrf_schema}.ptg2_plan_month"
            ),
        ):
            await build_legacy_orphan_sweep_plan(
                schema_name=case.mrf_schema,
                control_schema_name=case.control_schema,
                limits=LIMITS,
                executor=case.database,
            )


@pytest.mark.asyncio
async def test_bounded_plan_removes_only_selected_exact_suffix(
    monkeypatch,
) -> None:
    """Apply one bounded family without touching another exact suffix."""

    selected_suffix = "0" * 32
    retained_suffix = "f" * 32
    limits = LegacySweepLimits(1, 200, 800, 2 * 1024 * 1024 * 1024)
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            await _create_root(
                connection,
                case.mrf_schema,
                selected_suffix,
                populated=False,
            )
            retained_snapshot = await _seed_terminal_owner(
                connection,
                case.mrf_schema,
                case.control_schema,
                retained_suffix,
            )
        plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=limits,
            executor=case.database,
        )
        assert [candidate.suffix for candidate in plan.candidates] == [
            selected_suffix
        ]

        await execute_legacy_orphan_sweep(
            expected_plan_digest=plan.plan_digest,
            actor="postgres-test",
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=limits,
            database=case.database,
        )

        async with case.database.acquire() as connection:
            assert not await _has_relation(
                connection,
                case.mrf_schema,
                f"ptg_file_{selected_suffix}",
            )
            assert await _has_relation(
                connection,
                case.mrf_schema,
                f"ptg_file_{retained_suffix}",
            )
            assert await connection.scalar(
                f"SELECT COUNT(*) FROM {_q(case.mrf_schema)}.ptg2_snapshot "
                "WHERE snapshot_id = :snapshot_id",
                snapshot_id=retained_snapshot,
            ) == 1
