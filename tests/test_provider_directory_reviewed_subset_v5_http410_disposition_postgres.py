# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for the exact v5 HTTP-410 disposition."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from process import (
    provider_directory_fhir_subset_terminal_disposition_v4_selection
    as v4_selection,
)
from process import (
    provider_directory_fhir_subset_terminal_disposition_v5_selection
    as v5_selection,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    TERMINAL_DISPOSITION_METADATA_KEY,
)
from process.provider_directory_fhir_subset_terminal_disposition_store import (
    sync_v4_terminal_disposition,
    sync_v5_terminal_disposition,
)
from process.provider_directory_fhir_subset_terminal_disposition_v5_selection import (
    selected_direct_v5_terminal_disposition,
)
from tests.provider_directory_fhir_subset_abandonment_pg_support import (
    close_abandonment_scenario,
    runtime_database,
)
from tests.provider_directory_fhir_subset_terminal_disposition_v4_pg_support import (
    seed_direct_v4_terminal_root,
)
from tests.provider_directory_fhir_subset_terminal_disposition_v5_pg_support import (
    seed_direct_v5_terminal_root,
)
from tests.provider_directory_fhir_subset_terminal_disposition_v5_support import (
    DirectV5TerminalDatabase,
)
from tests.provider_directory_subset_completion_pg_concurrency import (
    create_committed_subset_schema,
)
from tests.test_provider_directory_reviewed_subset_direct_v4_disposition_postgres import (
    SYNTHETIC_MARKER_SHA256 as SYNTHETIC_V4_MARKER_SHA256,
    _install_terminal_stack,
    _object_identity_by_kind,
)
from tests.test_provider_directory_reviewed_subset_terminal_window_postgres import (
    DIRECT_DISPOSITION_MIGRATION_PATH,
    MIGRATION_PATH as TERMINAL_WINDOW_MIGRATION_PATH,
    _load_migration as load_profile_migration,
    _run_migration,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic/versions" / (
    "20260811120000_provider_directory_reviewed_subset_v5_http410_disposition.py"
)
SYNTHETIC_V5_MARKER_SHA256 = (
    "f419b51652df88f110e7f1f8ea61298c7342d19427e9f6209d839ce54434eb83"
)
SUCCESSOR_CAMPAIGN_ID = (
    "provider-directory-reviewed-subset-2026-08-11-v5-r2"
)


def _load_migration(path=MIGRATION_PATH, module_name="v5_http410_migration"):
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _install_v5_predecessor(scenario, *, v4_marker_sha256=None):
    await _install_terminal_stack(scenario)
    direct = load_profile_migration(
        DIRECT_DISPOSITION_MIGRATION_PATH,
        "v5_http410_direct_predecessor",
    )
    if v4_marker_sha256 is not None:
        direct._MARKER_SHA256 = v4_marker_sha256
    async with scenario.connection.transaction():
        await _run_migration(scenario, direct, "upgrade")
    terminal_window = load_profile_migration(
        TERMINAL_WINDOW_MIGRATION_PATH,
        "v5_http410_terminal_window_predecessor",
    )
    if v4_marker_sha256 is not None:
        terminal_window._direct_disposition()._MARKER_SHA256 = v4_marker_sha256
    async with scenario.connection.transaction():
        await _run_migration(scenario, terminal_window, "upgrade")
    return direct, terminal_window


async def _immutable_fingerprints(scenario) -> tuple[str, str, str]:
    resource_fingerprint = await scenario.connection.fetchval(
        f"""
        SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
                   pg_catalog.to_jsonb(resource_row)::text, '|'
                   ORDER BY resource_row.resource_type, resource_row.resource_id
               ), ''))
          FROM {scenario.quoted_schema}.provider_directory_dataset_resource
               AS resource_row
         WHERE resource_row.dataset_id = 'dataset-a'
        """
    )
    proof_fingerprint = await scenario.connection.fetchval(
        f"""
        SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
                   pg_catalog.to_jsonb(proof_row)::text, '|'
                   ORDER BY proof_row.shard_id
               ), ''))
          FROM {scenario.quoted_schema}.provider_directory_dataset_proof_shard
               AS proof_row
         WHERE proof_row.dataset_id = 'dataset-a'
        """
    )
    source_fingerprint = await scenario.connection.fetchval(
        f"""
        SELECT pg_catalog.md5(source.metadata_json::text)
          FROM {scenario.quoted_schema}.provider_directory_source AS source
         WHERE source.source_id = 'source-a'
        """
    )
    return resource_fingerprint, proof_fingerprint, source_fingerprint


async def _synthetic_v5_marker() -> dict:
    fake_database = DirectV5TerminalDatabase()
    selection, _checkpoint_rows = await selected_direct_v5_terminal_disposition(
        fake_database,
        "source-a",
    )
    return selection.marker_by_field


async def _assert_helper_is_private(scenario, migration) -> None:
    assert await scenario.connection.fetchval(
        """
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
        f'{scenario.schema}."{migration._HELPER}"(text)',
    ) == 0


async def _assert_partial_bypass_is_rejected(
    scenario,
    marker_by_field: dict,
) -> None:
    """Prove the deferred guard rolls back a marker-only transition."""

    with pytest.raises(Exception):
        async with scenario.connection.transaction():
            await scenario.connection.execute(
                f"""
                UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
                   SET status = 'acquisition_abandoned',
                       publication_metadata_json =
                           publication_metadata_json::jsonb
                           || pg_catalog.jsonb_build_object($1::text, $2::jsonb)
                 WHERE dataset_id = 'dataset-a'
                """,
                TERMINAL_DISPOSITION_METADATA_KEY,
                json.dumps(marker_by_field),
            )
            await scenario.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    status = await scenario.connection.fetchval(
        f"""
        SELECT status
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-a'
        """
    )
    abandoned_checkpoints = await scenario.connection.fetchval(
        f"""
        SELECT count(*)
          FROM {scenario.quoted_schema}.provider_directory_pagination_checkpoint
         WHERE dataset_id = 'dataset-a'
           AND state = 'acquisition_abandoned'
        """
    )
    assert status == "failed"
    assert abandoned_checkpoints == 0


async def _assert_v5_seal_and_replay(
    scenario,
    migration,
    direct,
    database,
    immutable_before: tuple[str, str, str],
) -> None:
    """Seal once, replay as a no-op, and retain immutable evidence."""

    first = await sync_v5_terminal_disposition(database, "source-a")
    second = await sync_v5_terminal_disposition(database, "source-a")
    assert first.disposed is True
    assert second.disposed is False
    assert await scenario.connection.fetchval(
        f'SELECT {scenario.quoted_schema}."{migration._HELPER}"($1)',
        "dataset-a",
    ) is True
    assert await scenario.connection.fetchval(
        f'SELECT {scenario.quoted_schema}."{direct._VALID}"($1)',
        "dataset-a",
    ) is True
    sealed_checkpoints = await scenario.connection.fetchval(
        f"""
        SELECT count(*)
          FROM {scenario.quoted_schema}.provider_directory_pagination_checkpoint
         WHERE dataset_id = 'dataset-a'
           AND state = 'acquisition_abandoned'
           AND completed_at IS NOT NULL
        """
    )
    assert sealed_checkpoints == 7
    assert await _immutable_fingerprints(scenario) == immutable_before


async def _assert_v3_downgrade_is_blocked(
    scenario,
    migration,
) -> None:
    """Prove installed v3 evidence prevents a destructive downgrade."""

    with pytest.raises(AssertionError) as downgrade_error:
        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "downgrade")
    assert "provider_directory_v5_http410_downgrade_blocked" in str(
        downgrade_error.value.__cause__
    )


async def _assert_replay_survives_source_campaign_rotation(
    scenario,
    database,
) -> None:
    """Keep sealed evidence replayable after the next source generation."""

    async with scenario.connection.transaction():
        await scenario.connection.execute(
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_source
               SET metadata_json = pg_catalog.jsonb_set(
                       metadata_json::jsonb,
                       '{{provider_directory_verification_campaign_id}}',
                       pg_catalog.to_jsonb($1::text),
                       false
                   )
             WHERE source_id = 'source-a'
            """,
            SUCCESSOR_CAMPAIGN_ID,
        )
    replay = await sync_v5_terminal_disposition(database, "source-a")
    assert replay.disposed is False


@pytest.mark.asyncio
async def test_v5_http410_guard_rollback_seal_replay_and_downgrade_fence(
    monkeypatch,
):
    """Reject partial bypass, seal once, replay, and retain exact evidence."""

    scenario = await create_committed_subset_schema(monkeypatch)
    migration = _load_migration()
    migration._MARKER_SHA256 = SYNTHETIC_V5_MARKER_SHA256
    monkeypatch.setattr(
        v5_selection,
        "DIRECT_V5_TERMINAL_MARKER_SHA256",
        SYNTHETIC_V5_MARKER_SHA256,
    )
    database = runtime_database()
    try:
        direct, _terminal_window = await _install_v5_predecessor(scenario)
        before_identity = await _object_identity_by_kind(scenario, direct)
        await seed_direct_v5_terminal_root(scenario)
        immutable_before = await _immutable_fingerprints(scenario)
        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "upgrade")
        assert await _object_identity_by_kind(scenario, direct) == before_identity
        await _assert_helper_is_private(scenario, migration)

        marker_by_field = await _synthetic_v5_marker()
        await _assert_partial_bypass_is_rejected(scenario, marker_by_field)
        await _assert_v5_seal_and_replay(
            scenario,
            migration,
            direct,
            database,
            immutable_before,
        )
        await _assert_replay_survives_source_campaign_rotation(
            scenario,
            database,
        )
        await _assert_v3_downgrade_is_blocked(scenario, migration)
        assert await _object_identity_by_kind(scenario, direct) == before_identity
    finally:
        await database.engine.dispose()
        await close_abandonment_scenario(scenario)


@pytest.mark.asyncio
async def test_v2_replay_survives_v3_upgrade_and_clean_downgrade(monkeypatch):
    """Keep one historical v2 root valid across the v3 round trip."""

    scenario = await create_committed_subset_schema(monkeypatch)
    migration = _load_migration(module_name="v5_http410_v2_round_trip")
    monkeypatch.setattr(
        v4_selection,
        "DIRECT_V4_TERMINAL_MARKER_SHA256",
        SYNTHETIC_V4_MARKER_SHA256,
    )
    database = runtime_database()
    try:
        direct, _terminal_window = await _install_v5_predecessor(
            scenario,
            v4_marker_sha256=SYNTHETIC_V4_MARKER_SHA256,
        )
        migration._direct()._MARKER_SHA256 = SYNTHETIC_V4_MARKER_SHA256
        migration._terminal_window()._direct_disposition()._MARKER_SHA256 = (
            SYNTHETIC_V4_MARKER_SHA256
        )
        await seed_direct_v4_terminal_root(scenario)
        assert (await sync_v4_terminal_disposition(database, "source-a")).disposed
        before_identity = await _object_identity_by_kind(scenario, direct)

        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "upgrade")
        assert await _object_identity_by_kind(scenario, direct) == before_identity
        assert not (await sync_v4_terminal_disposition(database, "source-a")).disposed

        async with scenario.connection.transaction():
            await _run_migration(scenario, migration, "downgrade")
        assert await _object_identity_by_kind(scenario, direct) == before_identity
        assert not (await sync_v4_terminal_disposition(database, "source-a")).disposed
    finally:
        await database.engine.dispose()
        await close_abandonment_scenario(scenario)
