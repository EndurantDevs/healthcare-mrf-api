# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from dataclasses import replace
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
        batch_plan=importer._ProviderDirectoryProfileBatchPlan(
            has_existing_artifacts=False,
            include_copy_batch=False,
            evidence_batches=(object(),),
            compact_batches=(object(),),
            fingerprint="f" * 64,
        ),
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
        "targets_json": importer._profile_build_target_coordinates(build),
        "executable_plan_hash": "f" * 64,
        "materialization_mode": "full_swap",
        "capacity_geometry_status": "legacy_unavailable",
        "capacity_geometry_hash": None,
        "capacity_geometry_json": None,
        "has_existing_artifacts": False,
        "evidence_next_batch": 1,
        "evidence_total_batches": 1,
        "profile_next_batch": 0,
        "profile_total_batches": 1,
        "state": "building_profile",
    }


@pytest.mark.asyncio
async def test_checkpoint_advance_uses_the_frozen_plan_and_complete_named_cas(monkeypatch):
    build = _build()
    execute = AsyncMock(return_value=1)
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_status", execute)
    await importer._advance_provider_directory_profile_build_checkpoint(
        build, phase="profile", expected_batch=0, total_batches=1
    )
    statement = execute.await_args.args[0]
    bindings = execute.await_args.kwargs
    assert bindings["executable_plan_hash"] == build.batch_plan.fingerprint
    assert bindings["targets_json"] == importer._stable_identity_json(importer._profile_build_target_coordinates(build))
    assert "targets_json::jsonb = CAST(:targets_json AS jsonb)" in statement
    assert "resume_lineage_hash = :resume_lineage_hash" in statement
    execute.reset_mock()
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="checkpoint_plan_missing"):
        await importer._advance_provider_directory_profile_build_checkpoint(
            replace(build, batch_plan=None), phase="profile", expected_batch=0, total_batches=1
        )
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_profile_stage_pair_requires_checkpointed_names(monkeypatch):
    stage_identity = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_layout",
        AsyncMock(side_effect=["e" * 64, "d" * 64]),
    )

    assert not await importer._is_profile_stage_pair_matching(
        "mrf",
        {"evidence_stage": None, "profile_stage": "profile_stage"},
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
        "profile_next_batch": 1,
    }
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=checkpoint_map),
    )
    stage_identity = AsyncMock(side_effect=[(31, "r", "p"), (32, "r", "p")])
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_layout",
        AsyncMock(side_effect=["e" * 64, "d" * 64]),
    )

    await importer._assert_provider_directory_profile_checkpoint_ready(
        build,
        importer.ProviderDirectoryArtifactBuildFence(),
        importer.ProviderDirectoryArtifactBuildFence(),
    )
    importer._provider_directory_profile_stage_layout.assert_awaited()
    stage_identity.assert_not_awaited()


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
            importer.ProviderDirectoryArtifactBuildFence(),
            importer.ProviderDirectoryArtifactBuildFence(),
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
            importer.ProviderDirectoryArtifactBuildFence(),
            importer.ProviderDirectoryArtifactBuildFence(),
        )
    stage_identity.assert_not_awaited()


@pytest.mark.asyncio
async def test_ready_profile_checkpoint_rejects_unsupported_stage_shape(monkeypatch):
    build = _build()
    checkpoint_map = {
        **_checkpoint_map(build),
        "state": "ready",
        "profile_next_batch": 1,
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
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_layout",
        AsyncMock(side_effect=importer.ProviderDirectoryArtifactBuildStale("unsupported_shape")),
    )

    with pytest.raises(RuntimeError, match="ready_checkpoint_stale"):
        await importer._assert_provider_directory_profile_checkpoint_ready(
            build,
            importer.ProviderDirectoryArtifactBuildFence(),
            importer.ProviderDirectoryArtifactBuildFence(),
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
    stage_identity = AsyncMock(side_effect=[(32, "r", "p"), (31, "r", "p")])
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    monkeypatch.setattr(importer, "_provider_directory_profile_stage_layout", AsyncMock())
    monkeypatch.setattr(importer.db, "status", status)

    await importer._drop_profile_stages_for_reinitialize(
        build,
        checkpoint_map,
    )

    assert stage_identity.await_count == 2
    assert status.await_count == 4
    assert "LOCK TABLE" in str(status.await_args_list[0].args[0])
    assert "DROP TABLE" in str(status.await_args_list[-1].args[0])


@pytest.mark.asyncio
async def test_profile_reinitialize_rejects_unsupported_shape_after_lock(
    monkeypatch,
):
    build = _build()
    stage_identity = AsyncMock(side_effect=[(32, "r", "p"), (999, "r", "p")])
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    monkeypatch.setattr(importer.db, "status", status)

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_layout",
        AsyncMock(side_effect=importer.ProviderDirectoryArtifactBuildStale("stage_shape_invalid")),
    )
    with pytest.raises(RuntimeError, match="stage_shape_invalid"):
        await importer._drop_profile_stages_for_reinitialize(
            build,
            _checkpoint_map(build),
        )
    status.assert_awaited_once()
