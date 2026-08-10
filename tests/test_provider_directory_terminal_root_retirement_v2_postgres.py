# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real-PostgreSQL proof for additive terminal-root retirement v2."""

from __future__ import annotations

import json
from typing import Any

import pytest

from db import (
    migration_provider_directory_terminal_root_retirement_evidence as legacy_evidence,
)
from db import (
    migration_provider_directory_terminal_root_retirement_guards as legacy_guards,
)
from db import migration_provider_directory_terminal_root_retirement_v2 as retirement_v2
from process.provider_directory_terminal_root_retirement_contract import (
    RETIREMENT_METADATA_KEY,
)
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
    disable_trigger_sql,
    drifted_function_body_sql,
    drop_trigger_sql,
    expect_fence_rejection,
    expect_migration_rejection,
    function_signature_sql,
    load_v2_migration,
    representative_trigger_drift_sql,
    retirement_trigger_state,
    run_v2_migration,
)
from tests.test_provider_directory_terminal_root_retirement_postgres import (
    drift_replay_inputs,
    request,
    require_database_guard,
    require_runtime_error_without_writes,
    target_json,
)


async def _make_target_v4(scenario: RetirementPostgres) -> None:
    await scenario.connection.execute(
        f"""UPDATE {scenario.schema}.provider_directory_endpoint_dataset
               SET resource_count = 0,
                   publication_metadata_json =
                       publication_metadata_json ||
                       jsonb_build_object(
                           'resource_hash_contract', 'semantic_content_v4'
                       )
             WHERE dataset_id = $1""",
        TARGET_DATASET_ID,
    )


def _dataset_rows(
    snapshot: dict[str, tuple[str, ...]],
) -> dict[str, dict[str, Any]]:
    return {
        decoded["dataset_id"]: decoded
        for row in snapshot["provider_directory_endpoint_dataset"]
        for decoded in (json.loads(row),)
    }


def _verify_parent_seal(
    before_snapshot: dict[str, tuple[str, ...]],
    after_snapshot: dict[str, tuple[str, ...]],
) -> dict[str, Any]:
    for relation_name, relation_rows in before_snapshot.items():
        if relation_name != "provider_directory_endpoint_dataset":
            assert after_snapshot[relation_name] == relation_rows
    before_rows = _dataset_rows(before_snapshot)
    after_rows = _dataset_rows(after_snapshot)
    expected_target = before_rows.pop(TARGET_DATASET_ID)
    actual_target = after_rows.pop(TARGET_DATASET_ID)
    assert after_rows == before_rows
    retirement_marker_by_field = actual_target["publication_metadata_json"].pop(
        retirement_v2.MARKER
    )
    expected_target["status"] = "acquisition_retired"
    assert actual_target == expected_target
    return retirement_marker_by_field


async def _verify_v2_database_guards(scenario: RetirementPostgres) -> None:
    await require_database_guard(
        scenario,
        f"UPDATE {scenario.schema}.provider_directory_dataset_resource "
        "SET payload_json = '{\"drift\":true}' "
        f"WHERE dataset_id = '{TARGET_DATASET_ID}'",
        "child_immutable",
    )
    await require_database_guard(
        scenario,
        f"INSERT INTO {scenario.schema}.import_run "
        "(run_id, importer, status, retry_of_run_id, created_at) VALUES "
        "('run-v2-late', 'provider-directory-fhir', 'queued', "
        "'run-terminal-3', now())",
        "run_immutable",
    )


async def _verify_v2_marker(
    scenario: RetirementPostgres,
    retirement_marker_by_field: dict[str, Any],
) -> None:
    assert retirement_marker_by_field["contract_version"] == retirement_v2.CONTRACT
    evidence_by_field = retirement_marker_by_field["evidence"]
    assert evidence_by_field["parent_resource_count"] == 0
    assert evidence_by_field["actual_resource_count"] == 3
    assert evidence_by_field["proof_row_count"] == 4
    assert await scenario.connection.fetchval(
        f"SELECT {scenario.schema}.{retirement_v2.VALID_FUNCTION}($1)",
        TARGET_DATASET_ID,
    )
    assert not await scenario.connection.fetchval(
        f"SELECT {scenario.schema}.{legacy_guards.VALID_FUNCTION}($1)",
        TARGET_DATASET_ID,
    )


@pytest.mark.asyncio
async def test_v1_retirement_replays_across_v2_upgrade_and_downgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep a used v1 parent valid while adopting and removing unused v2."""

    async with retirement_postgres(monkeypatch) as scenario:
        original_trigger_state = await retirement_trigger_state(scenario)
        assert len(original_trigger_state) == 28
        token = await preview_terminal_root_retirement(
            request(), database=scenario.database
        )
        await apply_terminal_root_retirement(
            request(expected_evidence_sha256=token),
            database=scenario.database,
        )
        v2_migration = load_v2_migration()
        await run_v2_migration(scenario, v2_migration, "upgrade")
        assert await retirement_trigger_state(scenario) == original_trigger_state
        assert await scenario.connection.fetchval(
            f"SELECT {scenario.schema}.{legacy_guards.VALID_FUNCTION}($1)",
            TARGET_DATASET_ID,
        )
        replay = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=token),
            database=scenario.database,
        )
        assert replay.retired is False
        await run_v2_migration(scenario, v2_migration, "downgrade")
        assert await retirement_trigger_state(scenario) == original_trigger_state
        assert await scenario.connection.fetchval(
            f"SELECT {scenario.schema}.{legacy_guards.VALID_FUNCTION}($1)",
            TARGET_DATASET_ID,
        )
        for function_spec in v2_migration._v2_function_specs(scenario.schema_name):
            signature = function_signature_sql(scenario, v2_migration, function_spec)
            assert (
                await scenario.connection.fetchval(
                    "SELECT pg_catalog.to_regprocedure($1)", signature
                )
                is None
            )


@pytest.mark.asyncio
async def test_v4_retirement_is_parent_only_valid_and_immutable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seal independent parent/resource/proof counts under the v2 marker."""

    async with retirement_postgres(monkeypatch) as scenario:
        await _make_target_v4(scenario)
        v2_migration = load_v2_migration()
        await run_v2_migration(scenario, v2_migration, "upgrade")
        await require_database_guard(
            scenario,
            (
                "UPDATE "
                f"{scenario.schema}.provider_directory_endpoint_dataset "
                "SET status = 'acquisition_retired', "
                "publication_metadata_json = publication_metadata_json || "
                f"jsonb_build_object('{RETIREMENT_METADATA_KEY}', '{{}}'::jsonb, "
                f"'{retirement_v2.MARKER}', '{{}}'::jsonb) "
                f"WHERE dataset_id = '{TARGET_DATASET_ID}'"
            ),
            "transition_invalid",
        )
        before_snapshot = await scenario.snapshot()
        evidence_sha256 = await preview_terminal_root_retirement(
            request(), database=scenario.database
        )
        assert await scenario.snapshot() == before_snapshot
        await require_runtime_error_without_writes(
            scenario,
            "evidence_changed",
            lambda: apply_terminal_root_retirement(
                request(expected_evidence_sha256="0" * 64),
                database=scenario.database,
            ),
        )
        retirement_result = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=evidence_sha256),
            database=scenario.database,
        )
        assert retirement_result.retired is True
        retirement_marker_by_field = _verify_parent_seal(
            before_snapshot, await scenario.snapshot()
        )
        await _verify_v2_marker(scenario, retirement_marker_by_field)
        await _verify_v2_database_guards(scenario)
        with pytest.raises(Exception, match="v2_downgrade_blocked"):
            await run_v2_migration(scenario, v2_migration, "downgrade")


@pytest.mark.asyncio
async def test_v4_replay_ignores_driftable_source_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Authenticate immutable v2 evidence after permitted source drift."""

    async with retirement_postgres(monkeypatch) as scenario:
        await _make_target_v4(scenario)
        await run_v2_migration(scenario, load_v2_migration(), "upgrade")
        token = await preview_terminal_root_retirement(
            request(), database=scenario.database
        )
        await apply_terminal_root_retirement(
            request(expected_evidence_sha256=token),
            database=scenario.database,
        )
        await drift_replay_inputs(scenario)
        before = await scenario.snapshot()
        assert (
            await preview_terminal_root_retirement(
                request(), database=scenario.database
            )
            == token
        )
        replay = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=token),
            database=scenario.database,
        )
        assert replay.retired is False
        assert await scenario.snapshot() == before
        metadata = (await target_json(scenario))["publication_metadata_json"]
        assert retirement_v2.MARKER in metadata
        assert RETIREMENT_METADATA_KEY not in metadata


@pytest.mark.asyncio
async def test_upgrade_fences_every_missing_or_disabled_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject every missing or non-ALWAYS retirement trigger."""

    async with retirement_postgres(monkeypatch) as scenario:
        migration = load_v2_migration()
        original_state = await retirement_trigger_state(scenario)
        fence_sql = migration._trigger_topology_fence_sql(scenario.schema_name)
        for (
            table_name,
            trigger_name,
            _trigger_type,
            _function_name,
        ) in migration._trigger_specs(scenario.schema_name):
            for mutation_sql in (
                drop_trigger_sql(scenario, table_name, trigger_name),
                disable_trigger_sql(scenario, table_name, trigger_name),
            ):
                await expect_fence_rejection(
                    scenario, mutation_sql, fence_sql, "v2_trigger_changed"
                )
        assert await retirement_trigger_state(scenario) == original_state


@pytest.mark.asyncio
async def test_upgrade_fences_rewired_and_extra_triggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject representative event, timing, function, argument, and count drift."""

    async with retirement_postgres(monkeypatch) as scenario:
        migration = load_v2_migration()
        fence_sql = migration._trigger_topology_fence_sql(scenario.schema_name)
        extra_trigger_sql = (
            f"CREATE TRIGGER pd_trr_extra BEFORE INSERT ON {scenario.schema}."
            '"provider_directory_endpoint_dataset" FOR EACH ROW EXECUTE FUNCTION '
            f'{scenario.schema}."{legacy_guards.PARENT_GUARD}"()'
        )
        for mutation_sql in (
            *representative_trigger_drift_sql(scenario),
            extra_trigger_sql,
        ):
            await expect_fence_rejection(
                scenario, mutation_sql, fence_sql, "v2_trigger_changed"
            )


@pytest.mark.asyncio
async def test_upgrade_fences_every_legacy_function_before_adoption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Authenticate every legacy dependency before calling legacy validity."""

    async with retirement_postgres(monkeypatch) as scenario:
        migration = load_v2_migration()
        for function_spec in migration._legacy_function_specs(scenario.schema_name):
            signature = function_signature_sql(scenario, migration, function_spec)
            fence_sql = migration._shape_fence_sql(
                scenario.schema_name, **function_spec
            )
            for mutation_sql in (
                f"ALTER FUNCTION {signature} SECURITY INVOKER",
                drifted_function_body_sql(migration, function_spec),
            ):
                await expect_fence_rejection(
                    scenario,
                    mutation_sql,
                    fence_sql,
                    "v2_function_changed",
                )
        valid_function = (
            f'{scenario.schema}."{legacy_guards.VALID_FUNCTION}"'
            "(candidate_dataset_id text)"
        )
        replacement_sql = (
            f"CREATE OR REPLACE FUNCTION {valid_function} RETURNS boolean "
            "LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog "
            "AS $function$ SELECT TRUE $function$"
        )
        await expect_migration_rejection(
            scenario, migration, "upgrade", replacement_sql, "v2_function_changed"
        )


@pytest.mark.asyncio
async def test_upgrade_fences_legacy_catalog_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject configuration, volatility, ACL, and execution-cost drift."""

    async with retirement_postgres(monkeypatch) as scenario:
        migration = load_v2_migration()
        specs_by_name = {
            str(function_spec["name"]): function_spec
            for function_spec in migration._legacy_function_specs(scenario.schema_name)
        }
        mutation_by_name = {
            legacy_evidence.RELATION_EVIDENCE_FUNCTION: (
                "ALTER FUNCTION {signature} SET TimeZone TO 'Europe/Prague'"
            ),
            legacy_evidence.EVIDENCE_FUNCTION: (
                "ALTER FUNCTION {signature} SET search_path TO public"
            ),
            legacy_guards.ELIGIBLE_FUNCTION: ("ALTER FUNCTION {signature} VOLATILE"),
            legacy_guards.PARENT_GUARD: (
                "GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC"
            ),
            legacy_guards.CHILD_GUARD: "ALTER FUNCTION {signature} COST 101",
        }
        for function_name, mutation_template in mutation_by_name.items():
            function_spec = specs_by_name[function_name]
            signature = function_signature_sql(scenario, migration, function_spec)
            await expect_fence_rejection(
                scenario,
                mutation_template.format(signature=signature),
                migration._shape_fence_sql(scenario.schema_name, **function_spec),
                "v2_function_changed",
            )


@pytest.mark.asyncio
async def test_upgrade_rejects_partial_v2_state_and_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse preinstalled v2 functions or markers on any parent status."""

    async with retirement_postgres(monkeypatch) as scenario:
        migration = load_v2_migration()
        partial_function_sql = (
            f'CREATE FUNCTION {scenario.schema}."{retirement_v2.VALID_FUNCTION}"'
            "(candidate_dataset_id text) RETURNS boolean LANGUAGE sql "
            "AS $function$ SELECT FALSE $function$"
        )
        marker_sql = (
            f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset "
            "SET publication_metadata_json = publication_metadata_json || "
            f"jsonb_build_object('{retirement_v2.MARKER}', '{{}}'::jsonb) "
            f"WHERE dataset_id = '{TARGET_DATASET_ID}'"
        )
        for mutation_sql in (partial_function_sql, marker_sql):
            await expect_migration_rejection(
                scenario, migration, "upgrade", mutation_sql, "v2_adoption_blocked"
            )


@pytest.mark.asyncio
async def test_downgrade_fences_v2_functions_and_trigger_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject installed-v2 or trigger drift before downgrade state inspection."""

    async with retirement_postgres(monkeypatch) as scenario:
        migration = load_v2_migration()
        await run_v2_migration(scenario, migration, "upgrade")
        installed_specs = (
            *migration._v2_function_specs(scenario.schema_name),
            migration._parent_guard_spec(scenario.schema_name, dual=True),
        )
        for function_spec in installed_specs:
            signature = function_signature_sql(scenario, migration, function_spec)
            fence_sql = migration._shape_fence_sql(
                scenario.schema_name, **function_spec
            )
            for mutation_sql in (
                f"ALTER FUNCTION {signature} SECURITY INVOKER",
                drifted_function_body_sql(migration, function_spec),
            ):
                await expect_fence_rejection(
                    scenario,
                    mutation_sql,
                    fence_sql,
                    "v2_function_changed",
                )
        v2_valid_signature = function_signature_sql(
            scenario,
            migration,
            migration._v2_function_specs(scenario.schema_name)[-1],
        )
        await expect_migration_rejection(
            scenario,
            migration,
            "downgrade",
            f"GRANT EXECUTE ON FUNCTION {v2_valid_signature} TO PUBLIC",
            "v2_function_changed",
        )
        await expect_migration_rejection(
            scenario,
            migration,
            "downgrade",
            drop_trigger_sql(
                scenario, "provider_directory_endpoint_dataset", "pd_trr_dataset_row"
            ),
            "v2_trigger_changed",
        )
