# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for the mixed terminal disposition."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import asyncpg
import pytest

from process import provider_directory_fhir_subset_abandonment as abandonment
from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONMENT_ENABLED_ENV,
)
from process.provider_directory_fhir_subset_terminal_disposition_store import (
    sync_reviewed_subset_terminal_disposition_transaction,
)
from tests.provider_directory_fhir_subset_abandonment_pg_support import (
    authorize_operator,
    close_abandonment_scenario,
    create_abandonment_relations,
    runtime_database,
    seed_expired_root,
)
from tests.provider_directory_fhir_subset_terminal_disposition_pg_lifecycle import (
    assert_candidate_root_mismatch_rejected,
    assert_numeric_digest_rejected,
    assert_post_seal_handoff,
    assert_recent_history_shape_rejected,
    assert_swapped_resource_marker_rejected,
    assert_terminal_evidence_is_immutable,
    assert_terminal_parent,
)
from tests.provider_directory_fhir_subset_terminal_disposition_pg_tamper import (
    assert_completion_envelope_rejected,
    assert_retryable_precount_rejected,
    assert_shared_proof_identity_rejected,
    assert_source_import_envelope_rejected,
    assert_terminal_geometry_tamper_rejected,
)
from tests.provider_directory_fhir_subset_terminal_disposition_pg_support import (
    seed_mixed_terminal_root,
)
from tests.provider_directory_reviewed_root_policy_pg import (
    _install_policy_predecessors,
    _load_policy_migration,
)
from tests.provider_directory_effective_endpoint_pg_cases import (
    _load_effective_endpoint_migration,
)
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    load_activation_migration,
)
from tests.provider_directory_subset_completion_pg_concurrency import (
    create_committed_subset_schema,
)
from tests.provider_directory_subset_completion_pg_setup import (
    MigrationSqlCapture,
    load_abandonment_migration,
    load_payload_guard_repair_migration,
)
from tests.tin_npi_connector_postgres_support import TransactionalSchema


ROOT = Path(__file__).resolve().parents[1]
BOUNDED_MIGRATION_PATH = (
    ROOT
    / "alembic/versions"
    / "20260810000000_provider_directory_reviewed_subset_bounded_drift.py"
)
MIGRATION_PATH = (
    ROOT
    / "alembic/versions"
    / "20260810010000_provider_directory_reviewed_subset_terminal_disposition.py"
)


def _load(path: Path, name: str):
    module_spec = importlib.util.spec_from_file_location(name, path)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _load_bounded_migration():
    return _load(
        BOUNDED_MIGRATION_PATH,
        "provider_directory_terminal_disposition_bounded_predecessor",
    )


def _load_migration():
    return _load(
        MIGRATION_PATH,
        "provider_directory_terminal_disposition_postgres_migration",
    )


async def _run_migration(scenario, migration, action: str) -> list[str]:
    capture = MigrationSqlCapture()
    migration.op = capture
    getattr(migration, action)()
    for statement_index, statement in enumerate(capture.statements):
        try:
            await scenario.connection.execute(statement)
        except Exception as error:
            raise AssertionError(
                f"failed migration {migration.revision} {action} "
                f"statement {statement_index} at "
                f"{getattr(error, 'position', None)}: {error}"
            ) from error
    return capture.statements


async def _install_bounded_predecessor(scenario):
    await _install_policy_predecessors(scenario)
    bounded = _load_bounded_migration()
    await _run_migration(scenario, bounded, "upgrade")
    return bounded


async def _install_committed_bounded_predecessor(scenario):
    await create_abandonment_relations(scenario)
    migrations = (
        load_activation_migration(),
        load_payload_guard_repair_migration(),
        load_abandonment_migration(),
        _load_effective_endpoint_migration(),
        _load_policy_migration(),
        _load_bounded_migration(),
    )
    for migration in migrations:
        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "upgrade")
    return migrations[-1]


async def _guard_catalog(scenario, migration) -> dict:
    abandonment_migration = migration._abandonment()
    function_names = (
        abandonment_migration._VALID,
        abandonment_migration._DATASET_GUARD,
        abandonment_migration._CHECKPOINT_GUARD,
    )
    function_rows = await scenario.connection.fetch(
        """
        SELECT function_row.proname, function_row.oid,
               function_row.proacl::text AS proacl,
               function_row.proowner,
               function_row.prosecdef, function_row.proconfig,
               pg_catalog.pg_get_functiondef(function_row.oid) AS definition
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
         WHERE namespace_row.nspname = $1
           AND function_row.proname = ANY($2::text[])
        """,
        scenario.schema,
        function_names,
    )
    trigger_rows = await scenario.connection.fetch(
        """
        SELECT relation_row.relname, trigger_row.tgname,
               trigger_row.oid, trigger_row.tgfoid,
               trigger_row.tgenabled::text AS tgenabled
          FROM pg_catalog.pg_trigger AS trigger_row
          JOIN pg_catalog.pg_class AS relation_row
            ON relation_row.oid = trigger_row.tgrelid
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = relation_row.relnamespace
         WHERE namespace_row.nspname = $1
           AND trigger_row.tgname <> ALL($2::text[])
           AND trigger_row.tgisinternal IS FALSE
        """,
        scenario.schema,
        (migration._DATASET_CONSTRAINT, migration._CHECKPOINT_CONSTRAINT),
    )
    return {
        "functions": {
            function_record["proname"]: dict(function_record)
            for function_record in function_rows
        },
        "triggers": {
            (trigger_record["relname"], trigger_record["tgname"]): dict(
                trigger_record
            )
            for trigger_record in trigger_rows
        },
    }


async def _assert_terminal_downgrade_blocked(scenario, migration) -> None:
    """Assert retained terminal evidence blocks removing its validator."""
    with pytest.raises(AssertionError) as downgrade_error:
        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "downgrade")
    cause = downgrade_error.value.__cause__
    assert isinstance(cause, asyncpg.PostgresError)
    assert (
        "provider_directory_subset_terminal_disposition_downgrade_blocked"
        in str(cause)
    )


async def _assert_direct_terminal_tamper_rejections(
    scenario,
    migration,
    database,
) -> None:
    """Exercise direct SQL tampering that preserves surrounding hashes."""
    await assert_candidate_root_mismatch_rejected(scenario, migration, database)
    await assert_swapped_resource_marker_rejected(scenario, migration, database)
    await assert_numeric_digest_rejected(scenario, migration, database)
    await assert_recent_history_shape_rejected(scenario, migration, database)
    await assert_retryable_precount_rejected(scenario, migration, database)
    await assert_shared_proof_identity_rejected(scenario, migration, database)
    await assert_terminal_geometry_tamper_rejected(
        scenario,
        migration,
        database,
    )
    await assert_completion_envelope_rejected(scenario, migration, database)
    await assert_source_import_envelope_rejected(scenario, migration, database)


@pytest.mark.asyncio
async def test_clean_migration_preserves_oids_acl_and_restores_bodies(monkeypatch):
    """Preserve old object identities and restore exact guard bodies."""
    scenario = await TransactionalSchema.create(monkeypatch)
    migration = _load_migration()
    try:
        await _install_bounded_predecessor(scenario)
        before = await _guard_catalog(scenario, migration)
        assert len(before["triggers"]) >= 17
        await _run_migration(scenario, migration, "upgrade")
        upgraded = await _guard_catalog(scenario, migration)

        assert upgraded["triggers"] == before["triggers"]
        for name, before_row in before["functions"].items():
            upgraded_row = upgraded["functions"][name]
            assert upgraded_row["oid"] == before_row["oid"]
            assert upgraded_row["proacl"] == before_row["proacl"]
            assert upgraded_row["proowner"] == before_row["proowner"]
            assert upgraded_row["prosecdef"] is True
            assert upgraded_row["proconfig"] == ["search_path=pg_catalog"]
        abandonment_valid = migration._abandonment()._VALID
        assert (
            upgraded["functions"][abandonment_valid]["definition"]
            == before["functions"][abandonment_valid]["definition"]
        )

        await _run_migration(scenario, migration, "downgrade")
        restored = await _guard_catalog(scenario, migration)
        assert restored == before
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_terminal_disposition_lifecycle_tamper_idempotence_and_fence(
    monkeypatch,
):
    """Exercise swapped-marker rejection, sealing, replay, and handoff."""
    scenario = await create_committed_subset_schema(monkeypatch)
    migration = _load_migration()
    database = runtime_database()
    try:
        await _install_committed_bounded_predecessor(scenario)
        await seed_mixed_terminal_root(scenario)
        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "upgrade")
        await _assert_direct_terminal_tamper_rejections(
            scenario,
            migration,
            database,
        )

        first = await sync_reviewed_subset_terminal_disposition_transaction(
            database,
            "source-a",
        )
        second = await sync_reviewed_subset_terminal_disposition_transaction(
            database,
            "source-a",
        )
        assert first.disposed is True
        assert second.disposed is False
        assert await scenario.connection.fetchval(
            f"SELECT {scenario.quoted_schema}."
            f'"{migration._VALID}"($1)',
            "dataset-a",
        ) is True
        await assert_terminal_parent(scenario, migration)
        await assert_terminal_evidence_is_immutable(scenario)
        await assert_post_seal_handoff(scenario, migration, database)
        await _assert_terminal_downgrade_blocked(scenario, migration)
    finally:
        await database.engine.dispose()
        await close_abandonment_scenario(scenario)


@pytest.mark.asyncio
async def test_expired_cursor_v1_remains_valid_and_downgradable(monkeypatch):
    """Keep the predecessor v1 seal valid before and after clean downgrade."""
    scenario = await create_committed_subset_schema(monkeypatch)
    migration = _load_migration()
    database = runtime_database()
    try:
        await _install_committed_bounded_predecessor(scenario)
        await seed_expired_root(scenario)
        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "upgrade")
        authorize_operator(monkeypatch, ABANDONMENT_ENABLED_ENV)

        abandonment_result = await abandonment.abandon_reviewed_subset_expired_root(
            database=database
        )
        assert abandonment_result.abandoned is True
        old_valid = migration._abandonment()._VALID
        assert await scenario.connection.fetchval(
            f"SELECT {scenario.quoted_schema}.\"{old_valid}\"($1)",
            "dataset-abandoned",
        ) is True
        assert await scenario.connection.fetchval(
            f"SELECT {scenario.quoted_schema}.\"{migration._VALID}\"($1)",
            "dataset-abandoned",
        ) is False

        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "downgrade")
        assert await scenario.connection.fetchval(
            f"SELECT {scenario.quoted_schema}.\"{old_valid}\"($1)",
            "dataset-abandoned",
        ) is True
    finally:
        await database.engine.dispose()
        await close_abandonment_scenario(scenario)
