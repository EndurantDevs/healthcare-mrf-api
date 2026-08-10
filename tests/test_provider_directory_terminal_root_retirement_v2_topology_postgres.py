# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL topology proofs for terminal-root retirement v2."""

from __future__ import annotations

import pytest

from db import (
    migration_provider_directory_terminal_root_retirement_guards as legacy_guards,
)
from db import migration_provider_directory_terminal_root_retirement_v2 as retirement_v2
from process.provider_directory_terminal_root_retirement_operator import (
    apply_terminal_root_retirement,
    preview_terminal_root_retirement,
)
from tests.provider_directory_terminal_root_retirement_pg_support import (
    TARGET_DATASET_ID,
    RetirementPostgres,
    retirement_postgres,
)
from tests.provider_directory_terminal_root_retirement_v2_pg_support import (
    expect_fence_rejection,
    expect_migration_rejection,
    function_signature_sql,
    load_v2_migration,
    run_v2_migration,
)
from tests.test_provider_directory_terminal_root_retirement_postgres import request


def _boolean_overload_sql(
    scenario: RetirementPostgres,
    name: str,
    arguments: str,
) -> str:
    return (
        f'CREATE FUNCTION {scenario.schema}."{name}"({arguments}) '
        "RETURNS boolean LANGUAGE sql IMMUTABLE "
        "AS $function$ SELECT FALSE $function$"
    )


def _legacy_marker_drift_sql(scenario: RetirementPostgres) -> str:
    table = f'{scenario.schema}."provider_directory_endpoint_dataset"'
    return (
        f'ALTER TABLE {table} DISABLE TRIGGER "pd_trr_dataset_row"; '
        f"UPDATE {table} SET publication_metadata_json = "
        "publication_metadata_json || "
        f"jsonb_build_object('{legacy_guards.MARKER}', '{{}}'::jsonb) "
        f"WHERE dataset_id = '{TARGET_DATASET_ID}'; "
        f'ALTER TABLE {table} ENABLE ALWAYS TRIGGER "pd_trr_dataset_row"'
    )


@pytest.mark.asyncio
async def test_function_fence_rejects_argument_name_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Authenticate named SQL parameters as part of each frozen function."""

    async with retirement_postgres(monkeypatch) as scenario:
        migration = load_v2_migration()
        function_spec = migration._legacy_function_specs(scenario.schema_name)[2]
        signature = function_signature_sql(scenario, migration, function_spec)
        mutation_sql = (
            "UPDATE pg_catalog.pg_proc SET proargnames = "
            "ARRAY['renamed_dataset_id', 'minimum_age']::text[] "
            f"WHERE oid = pg_catalog.to_regprocedure('{signature}')"
        )
        await expect_fence_rejection(
            scenario,
            mutation_sql,
            migration._shape_fence_sql(scenario.schema_name, **function_spec),
            "v2_function_changed",
        )


@pytest.mark.asyncio
async def test_upgrade_and_downgrade_reject_same_name_overloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject overloads that could win varchar call resolution."""

    async with retirement_postgres(monkeypatch) as scenario:
        migration = load_v2_migration()
        await expect_migration_rejection(
            scenario,
            migration,
            "upgrade",
            _boolean_overload_sql(
                scenario,
                legacy_guards.VALID_FUNCTION,
                "candidate_dataset_id varchar",
            ),
            "v2_function_changed",
        )
        await expect_migration_rejection(
            scenario,
            migration,
            "upgrade",
            _boolean_overload_sql(
                scenario,
                retirement_v2.ELIGIBLE_FUNCTION,
                "candidate_dataset_id varchar, minimum_age integer",
            ),
            "v2_adoption_blocked",
        )
        await run_v2_migration(scenario, migration, "upgrade")
        await expect_migration_rejection(
            scenario,
            migration,
            "downgrade",
            _boolean_overload_sql(
                scenario,
                retirement_v2.VALID_FUNCTION,
                "candidate_dataset_id varchar",
            ),
            "v2_function_changed",
        )


@pytest.mark.asyncio
async def test_migration_rejects_nonretired_legacy_marker_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the legacy marker equivalent to the retired parent status."""

    async with retirement_postgres(monkeypatch) as scenario:
        migration = load_v2_migration()
        marker_drift_sql = _legacy_marker_drift_sql(scenario)
        await expect_migration_rejection(
            scenario,
            migration,
            "upgrade",
            marker_drift_sql,
            "v2_adoption_blocked",
        )
        await run_v2_migration(scenario, migration, "upgrade")
        await expect_migration_rejection(
            scenario,
            migration,
            "downgrade",
            marker_drift_sql,
            "v2_downgrade_blocked",
        )


@pytest.mark.asyncio
async def test_dual_parent_guard_accepts_fresh_legacy_retirement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise a new v1 transition after the dual parent guard is installed."""

    async with retirement_postgres(monkeypatch) as scenario:
        migration = load_v2_migration()
        await run_v2_migration(scenario, migration, "upgrade")
        token = await preview_terminal_root_retirement(
            request(), database=scenario.database
        )
        result = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=token),
            database=scenario.database,
        )
        assert result.retired is True
        assert await scenario.connection.fetchval(
            f"SELECT {scenario.schema}.{legacy_guards.VALID_FUNCTION}($1)",
            TARGET_DATASET_ID,
        )
        assert not await scenario.connection.fetchval(
            f"SELECT {scenario.schema}.{retirement_v2.VALID_FUNCTION}($1)",
            TARGET_DATASET_ID,
        )
        await run_v2_migration(scenario, migration, "downgrade")


@pytest.mark.asyncio
async def test_trigger_prefix_fence_ignores_wildcard_lookalike(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat underscores in the owned trigger prefix as literal bytes."""

    async with retirement_postgres(monkeypatch) as scenario:
        migration = load_v2_migration()
        transaction = scenario.connection.transaction()
        await transaction.start()
        try:
            await scenario.connection.execute(
                'CREATE TRIGGER "pdXtrrYunrelated" BEFORE INSERT ON '
                f'{scenario.schema}."provider_directory_endpoint_dataset" '
                "FOR EACH ROW EXECUTE FUNCTION "
                f'{scenario.schema}."{legacy_guards.PARENT_GUARD}"()'
            )
            await scenario.connection.execute(
                migration._trigger_topology_fence_sql(scenario.schema_name)
            )
        finally:
            await transaction.rollback()
