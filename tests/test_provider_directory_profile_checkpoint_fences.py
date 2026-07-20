# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile as profile


importer = importlib.import_module("process.provider_directory_fhir")


def _build() -> importer._ProviderDirectoryProfileBuild:
    build_id = f"pdpb_{'a' * 32}"
    return importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation-a",
        source_ids=("source-a",),
        retained_source_ids=("source-a",),
        dataset_ids=("dataset-a",),
        profile_as_of="2026-07-20",
        evidence_stage=profile.profile_evidence_stage_table_name(build_id),
        profile_stage=profile.profile_stage_table_name(build_id),
        resume_lineage_hash="a" * 64,
        build_id=build_id,
        owner_run_id="run-a",
    )


def _checkpoint_map(
    build: importer._ProviderDirectoryProfileBuild,
) -> dict[str, object]:
    return {
        "build_id": build.build_id,
        "strategy_version": profile.PROFILE_BUILD_STRATEGY_VERSION,
        "schema_version": profile.PROFILE_SCHEMA_VERSION,
        "resume_lineage_hash": build.resume_lineage_hash,
        "profile_as_of": build.profile_as_of,
        "source_ids": list(build.source_ids),
        "retained_source_ids": list(build.retained_source_ids),
        "dataset_ids": list(build.dataset_ids),
        "evidence_stage": build.evidence_stage,
        "profile_stage": build.profile_stage,
        "evidence_stage_oid": 31,
        "profile_stage_oid": 32,
        "evidence_target_oid": None,
        "profile_target_oid": None,
        "has_existing_artifacts": False,
        "evidence_next_batch": 52,
        "evidence_total_batches": 52,
        "profile_next_batch": 120,
        "profile_total_batches": 400,
        "state": "building_profile",
    }


@pytest.mark.asyncio
async def test_profile_stage_pair_requires_checkpointed_oids(monkeypatch):
    stage_identity = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )

    assert not await importer._is_profile_stage_pair_matching(
        "mrf",
        {"evidence_stage_oid": None, "profile_stage_oid": 32},
        "evidence_stage",
        "profile_stage",
    )
    stage_identity.assert_not_awaited()


@pytest.mark.asyncio
async def test_ready_profile_checkpoint_requires_complete_logged_stage_pair(
    monkeypatch,
):
    build = _build()
    checkpoint_map = {
        **_checkpoint_map(build),
        "state": "ready",
        "profile_next_batch": 400,
    }
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=checkpoint_map),
    )
    stage_identity = AsyncMock(
        side_effect=[(31, "r", "p"), (32, "r", "p")]
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )

    await importer._assert_provider_directory_profile_checkpoint_ready(
        build,
        importer.ProviderDirectoryArtifactBuildFence(None),
        importer.ProviderDirectoryArtifactBuildFence(None),
    )
    assert stage_identity.await_count == 2


@pytest.mark.asyncio
async def test_ready_profile_checkpoint_rejects_missing_or_stale_state(
    monkeypatch,
):
    build = _build()
    first = AsyncMock(return_value=None)
    monkeypatch.setattr(importer.db, "first", first)

    with pytest.raises(RuntimeError, match="ready_checkpoint_missing"):
        await importer._assert_provider_directory_profile_checkpoint_ready(
            build,
            importer.ProviderDirectoryArtifactBuildFence(None),
            importer.ProviderDirectoryArtifactBuildFence(None),
        )

    first.return_value = _checkpoint_map(build)
    stage_identity = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    with pytest.raises(RuntimeError, match="ready_checkpoint_stale"):
        await importer._assert_provider_directory_profile_checkpoint_ready(
            build,
            importer.ProviderDirectoryArtifactBuildFence(None),
            importer.ProviderDirectoryArtifactBuildFence(None),
        )
    stage_identity.assert_not_awaited()


@pytest.mark.asyncio
async def test_ready_profile_checkpoint_rejects_replaced_stage_oid(monkeypatch):
    build = _build()
    checkpoint_map = {
        **_checkpoint_map(build),
        "state": "ready",
        "profile_next_batch": 400,
    }
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=checkpoint_map),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        AsyncMock(return_value=(999, "r", "p")),
    )

    with pytest.raises(RuntimeError, match="ready_checkpoint_stale"):
        await importer._assert_provider_directory_profile_checkpoint_ready(
            build,
            importer.ProviderDirectoryArtifactBuildFence(None),
            importer.ProviderDirectoryArtifactBuildFence(None),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity",
    [None, (0, "r", "p"), (31, "v", "p"), (31, "r", "u")],
)
async def test_profile_stage_oid_requires_logged_permanent_table(
    monkeypatch,
    identity,
):
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        AsyncMock(return_value=identity),
    )

    with pytest.raises(RuntimeError, match="stage_identity_invalid"):
        await importer._require_provider_directory_profile_stage_oid(
            "mrf",
            "profile_stage",
        )


@pytest.mark.asyncio
async def test_profile_stage_oid_returns_valid_relation_oid(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        AsyncMock(return_value=(31, "r", "p")),
    )

    assert (
        await importer._require_provider_directory_profile_stage_oid(
            "mrf",
            "profile_stage",
        )
        == 31
    )


@pytest.mark.asyncio
async def test_profile_reinitialize_rejects_name_drift_and_unowned_stage(
    monkeypatch,
):
    build = _build()
    checkpoint_map = {
        **_checkpoint_map(build),
        "profile_stage": "wrong_stage",
    }
    stage_identity = AsyncMock(return_value=(32, "r", "p"))
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )

    with pytest.raises(RuntimeError, match="reinitialize_stage_name_mismatch"):
        await importer._drop_profile_stages_for_reinitialize(
            build,
            checkpoint_map,
        )
    stage_identity.assert_not_awaited()

    with pytest.raises(RuntimeError, match="unowned_stage_exists"):
        await importer._drop_profile_stages_for_reinitialize(build, {})


@pytest.mark.asyncio
async def test_profile_reinitialize_locks_rechecks_and_drops_owned_stages(
    monkeypatch,
):
    build = _build()
    checkpoint_map = _checkpoint_map(build)
    stage_identity = AsyncMock(
        side_effect=[
            (32, "r", "p"),
            (32, "r", "p"),
            (31, "r", "p"),
            (31, "r", "p"),
        ]
    )
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    monkeypatch.setattr(importer.db, "status", status)

    await importer._drop_profile_stages_for_reinitialize(
        build,
        checkpoint_map,
    )

    assert stage_identity.await_count == 4
    assert status.await_count == 4
    assert "LOCK TABLE" in str(status.await_args_list[0].args[0])
    assert "DROP TABLE" in str(status.await_args_list[-1].args[0])


@pytest.mark.asyncio
async def test_profile_reinitialize_rejects_identity_change_after_lock(
    monkeypatch,
):
    build = _build()
    stage_identity = AsyncMock(
        side_effect=[(32, "r", "p"), (999, "r", "p")]
    )
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    monkeypatch.setattr(importer.db, "status", status)

    with pytest.raises(RuntimeError, match="stage_identity_changed"):
        await importer._drop_profile_stages_for_reinitialize(
            build,
            _checkpoint_map(build),
        )
    status.assert_awaited_once()
