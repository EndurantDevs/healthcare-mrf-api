# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for the thin direct-v4 disposition."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from process import (
    provider_directory_fhir_subset_terminal_disposition_v4_selection
    as v4_selection,
)
from process.provider_directory_fhir_subset_terminal_disposition_store import (
    sync_reviewed_subset_terminal_disposition_transaction,
    sync_v4_terminal_disposition,
)
from tests.provider_directory_fhir_subset_abandonment_pg_support import (
    close_abandonment_scenario,
    runtime_database,
)
from tests.provider_directory_fhir_subset_terminal_disposition_pg_support import (
    seed_mixed_terminal_root,
)
from tests.provider_directory_fhir_subset_terminal_disposition_v4_pg_support import (
    seed_direct_v4_terminal_root,
)
from tests.test_provider_directory_reviewed_subset_terminal_disposition_postgres import (
    _install_committed_bounded_predecessor,
    _load_migration as load_terminal_migration,
    _load_scope_binding_migration,
    _run_migration,
)
from tests.provider_directory_subset_completion_pg_concurrency import (
    create_committed_subset_schema,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic/versions"
    / "20260810110000_provider_directory_reviewed_subset_direct_v4_disposition.py"
)
SYNTHETIC_MARKER_SHA256 = (
    "e75f1f8addca0bc3079bb164baa6dc7bf39e0e424a0c8f8c53d2a3cdeae96489"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_direct_v4_disposition_postgres_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _install_terminal_stack(scenario):
    await scenario.connection.execute(
        f"""
        CREATE TABLE {scenario.quoted_schema}.import_run (
            run_id varchar(64) PRIMARY KEY,
            retry_of_run_id varchar(64)
        )
        """
    )
    await _install_committed_bounded_predecessor(scenario)
    terminal = load_terminal_migration()
    scope_binding = _load_scope_binding_migration()
    for migration in (terminal, scope_binding):
        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "upgrade")
    return terminal, scope_binding


async def _object_identity_by_kind(scenario, terminal) -> dict[str, tuple]:
    function_names = (
        terminal._VALID,
        terminal._abandonment()._DATASET_GUARD,
        terminal._abandonment()._CHECKPOINT_GUARD,
    )
    function_rows = await scenario.connection.fetch(
        """
        SELECT function_row.proname, function_row.oid, function_row.proowner,
               function_row.proacl::text, function_row.prosecdef,
               function_row.proconfig
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
         WHERE namespace_row.nspname = $1
           AND function_row.proname = ANY($2::text[])
         ORDER BY function_row.proname
        """,
        scenario.schema,
        function_names,
    )
    trigger_rows = await scenario.connection.fetch(
        """
        SELECT trigger_row.tgname, trigger_row.oid, trigger_row.tgfoid,
               trigger_row.tgenabled::text
          FROM pg_catalog.pg_trigger AS trigger_row
          JOIN pg_catalog.pg_class AS relation_row
            ON relation_row.oid = trigger_row.tgrelid
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = relation_row.relnamespace
         WHERE namespace_row.nspname = $1
           AND trigger_row.tgname = ANY($2::text[])
         ORDER BY trigger_row.tgname
        """,
        scenario.schema,
        (terminal._DATASET_CONSTRAINT, terminal._CHECKPOINT_CONSTRAINT),
    )
    return {
        "functions": tuple(
            tuple(function_record.values()) for function_record in function_rows
        ),
        "triggers": tuple(
            tuple(trigger_record.values()) for trigger_record in trigger_rows
        ),
    }


@pytest.mark.asyncio
async def test_direct_v4_lifecycle_replay_acl_identity_and_downgrade_fence(
    monkeypatch,
):
    """Seal once, replay once, and preserve every pre-existing object identity."""

    scenario = await create_committed_subset_schema(monkeypatch)
    migration = _load_migration()
    migration._MARKER_SHA256 = SYNTHETIC_MARKER_SHA256
    monkeypatch.setattr(
        v4_selection,
        "DIRECT_V4_TERMINAL_MARKER_SHA256",
        SYNTHETIC_MARKER_SHA256,
    )
    database = runtime_database()
    try:
        terminal, _scope_binding = await _install_terminal_stack(scenario)
        before = await _object_identity_by_kind(scenario, terminal)
        await seed_direct_v4_terminal_root(scenario)
        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "upgrade")
        after = await _object_identity_by_kind(scenario, terminal)
        assert after == before

        first = await sync_v4_terminal_disposition(database, "source-a")
        second = await sync_v4_terminal_disposition(database, "source-a")
        assert first.disposed is True
        assert second.disposed is False
        assert await scenario.connection.fetchval(
            f'SELECT {scenario.quoted_schema}."{migration._VALID}"($1)',
            "dataset-a",
        ) is True
        assert await scenario.connection.fetchval(
            f'SELECT {scenario.quoted_schema}."{migration._DIRECT_VALID}"($1)',
            "dataset-a",
        ) is True
        assert await scenario.connection.fetchval(
            f"""
            SELECT count(*)
              FROM pg_catalog.pg_proc AS helper
              CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
                   helper.proacl,
                   pg_catalog.acldefault('f', helper.proowner)
              )) AS helper_acl
             WHERE helper.oid = pg_catalog.to_regprocedure($1)
               AND helper_acl.privilege_type = 'EXECUTE'
               AND helper_acl.grantee <> helper.proowner
            """,
            f'{scenario.schema}."{migration._DIRECT_VALID}"(text)',
        ) == 0
        with pytest.raises(AssertionError) as downgrade_error:
            async with scenario.connection.transaction():
                await _run_migration(scenario, migration, "downgrade")
        assert "provider_directory_subset_terminal_v4_downgrade_blocked" in str(
            downgrade_error.value.__cause__
        )
    finally:
        await database.engine.dispose()
        await close_abandonment_scenario(scenario)


@pytest.mark.asyncio
async def test_v1_evidence_survives_v2_upgrade_and_clean_downgrade(
    monkeypatch,
):
    """Keep a historical v1 marker valid across the thin v2 migration."""

    scenario = await create_committed_subset_schema(monkeypatch)
    migration = _load_migration()
    database = runtime_database()
    try:
        terminal, _scope_binding = await _install_terminal_stack(scenario)
        await seed_mixed_terminal_root(scenario)
        disposition_result = (
            await sync_reviewed_subset_terminal_disposition_transaction(
                database,
                "source-a",
            )
        )
        assert disposition_result.disposed is True
        old_valid = terminal._VALID
        assert await scenario.connection.fetchval(
            f'SELECT {scenario.quoted_schema}."{old_valid}"($1)',
            "dataset-a",
        ) is True

        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "upgrade")
        assert await scenario.connection.fetchval(
            f'SELECT {scenario.quoted_schema}."{old_valid}"($1)',
            "dataset-a",
        ) is True

        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "downgrade")
        assert await scenario.connection.fetchval(
            f'SELECT {scenario.quoted_schema}."{old_valid}"($1)',
            "dataset-a",
        ) is True
    finally:
        await database.engine.dispose()
        await close_abandonment_scenario(scenario)
