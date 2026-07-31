# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _build() -> importer._ProviderDirectoryProfileBuild:
    return importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation-a",
        source_ids=("source-a",),
        retained_source_ids=("source-a",),
        dataset_ids=("dataset-a",),
        profile_as_of="2026-07-20",
        evidence_stage="profile_evidence_stage",
        profile_stage="profile_stage",
        build_id=f"pdpb_{'a' * 32}",
        owner_run_id="run-a",
    )


def _evidence_batches():
    return tuple(
        importer._ProviderDirectoryProfileEvidenceBatch(
            kind="fact",
            source_id="source-a",
            dataset_id="dataset-a",
            fact_type=("name", "specialty", "role")[batch_number],
        )
        for batch_number in range(3)
    )


def _compact_batches():
    return tuple(
        importer._ProviderDirectoryProfileCompactBatch(
            kind="npi",
            npi_start=1_000_000_000 + (batch_number * 5_000_000),
            npi_end=1_005_000_000 + (batch_number * 5_000_000),
        )
        for batch_number in range(3)
    )


def _production_profile_build():
    evidence_batch = importer._ProviderDirectoryProfileEvidenceBatch(
        kind="fact",
        source_id="source-a",
        dataset_id="dataset-a",
        fact_type="name",
    )
    compact_batch = importer._ProviderDirectoryProfileCompactBatch(
        kind="npi",
        npi_start=1_000_000_000,
        npi_end=1_005_000_000,
    )
    batch_plan = importer._ProviderDirectoryProfileBatchPlan(
        has_existing_artifacts=True,
        include_copy_batch=False,
        evidence_batches=(evidence_batch,) * 2_185,
        compact_batches=(compact_batch,) * 400,
        fingerprint="f" * 64,
    )
    return replace(_build(), batch_plan=batch_plan)


def _patch_population_dependencies(monkeypatch) -> AsyncMock:
    progress = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_mark_provider_directory_profile_batch_progress",
        progress,
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=1))
    monkeypatch.setattr(
        importer,
        "_advance_provider_directory_profile_build_checkpoint",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_mark_profile_build_checkpoint_state",
        AsyncMock(),
    )
    return progress


def _assert_resumed_progress(progress, batches) -> None:
    progress_calls = progress.await_args_list
    assert [
        call.kwargs["completed_batches"] for call in progress_calls
    ] == [1, 2, 3]
    assert {call.kwargs["total_batches"] for call in progress_calls} == {3}
    assert {
        call.kwargs["resumed_from_batch"] for call in progress_calls
    } == {1}
    assert progress_calls[0].kwargs.get("batch") is None
    assert progress_calls[1].kwargs["batch"] is batches[1]
    assert progress_calls[2].kwargs["batch"] is batches[2]


@pytest.mark.asyncio
async def test_profile_progress_uses_existing_control_run_contract(
    monkeypatch,
):
    """Expose batch state through the existing import-run projection."""
    mark_control_run = AsyncMock()
    monkeypatch.setattr(
        importer,
        "mark_control_run",
        mark_control_run,
    )
    batch = importer._ProviderDirectoryProfileEvidenceBatch(
        kind="fact",
        source_id="source-a",
        dataset_id="dataset-a",
        fact_type="affiliation",
        role_bucket_count=32,
        role_bucket=7,
    )

    await importer._mark_provider_directory_profile_batch_progress(
        _build(),
        phase="evidence",
        completed_batches=8,
        total_batches=52,
        resumed_from_batch=4,
        batch=batch,
    )

    progress = mark_control_run.await_args.kwargs["progress"]
    assert progress["phase"] == importer._PROFILE_EVIDENCE_PROGRESS_PHASE
    assert (progress["done"], progress["total"]) == (8, 52)
    assert progress["unit"] == "batches"
    assert progress["detail"]["profile_batch_phase"] == "evidence"
    assert progress["detail"]["role_bucket"] == 7
    assert progress["detail"]["resource_bucket"] == 7
    assert progress["detail"]["bucket_schemes"] == [
        importer._PROFILE_ROLE_RESOURCE_BUCKET_SCHEME,
    ]
    assert progress["detail"]["stage_pct"] == 15.38
    assert progress["detail"]["resumed_from_batch"] == 4
    assert progress["pct"] == 58.24


async def _emit_nested_profile_progress(build) -> None:
    await importer._mark_provider_directory_progress(
        build.owner_run_id,
        phase="provider-directory publishing artifacts",
        done=4,
        total=7,
        message="published address artifacts",
    )
    await importer._mark_provider_directory_profile_batch_progress(
        build,
        phase="evidence",
        completed_batches=1,
        total_batches=2_185,
        resumed_from_batch=0,
    )
    await importer._mark_provider_directory_profile_batch_progress(
        build,
        phase="evidence",
        completed_batches=2_185,
        total_batches=2_185,
        resumed_from_batch=0,
    )
    await importer._mark_provider_directory_profile_batch_progress(
        build,
        phase="profile",
        completed_batches=0,
        total_batches=400,
        resumed_from_batch=0,
    )
    await importer._mark_provider_directory_profile_batch_progress(
        build,
        phase="profile",
        completed_batches=400,
        total_batches=400,
        resumed_from_batch=0,
    )
    await importer._mark_provider_directory_progress(
        build.owner_run_id,
        phase="provider-directory publishing artifacts",
        done=5,
        total=7,
        message="published Provider Directory Profile",
    )


@pytest.mark.asyncio
async def test_profile_progress_preserves_outer_publish_envelope(monkeypatch):
    """Never let inner batch percentages regress the outer 4/7 progress."""
    mark_control_run = AsyncMock()
    monkeypatch.setattr(importer, "mark_control_run", mark_control_run)
    await _emit_nested_profile_progress(_production_profile_build())

    percentages = [
        call.kwargs["progress"]["pct"]
        for call in mark_control_run.await_args_list
    ]
    assert percentages == [
        57.14,
        57.15,
        69.22,
        69.22,
        71.43,
        71.43,
    ]
    assert percentages == sorted(percentages)
    assert mark_control_run.await_args_list[1].kwargs["progress"]["done"] == 1
    assert mark_control_run.await_args_list[1].kwargs["progress"]["total"] == 2_185


@pytest.mark.asyncio
async def test_evidence_progress_resumes_monotonically(monkeypatch):
    """Start at the durable evidence offset, then only advance."""
    batches = _evidence_batches()
    progress = _patch_population_dependencies(monkeypatch)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_evidence_batches",
        lambda *_args, **_kwargs: batches,
    )

    await importer._populate_provider_directory_profile_evidence_stage(
        _build(),
        has_evidence_target=False,
        bounded=True,
        start_batch=1,
        checkpointed=True,
    )

    _assert_resumed_progress(progress, batches)


@pytest.mark.asyncio
async def test_compact_progress_reports_bounded_npi_ranges(monkeypatch):
    """Start at the durable compact offset and expose exact NPI ranges."""
    batches = _compact_batches()
    progress = _patch_population_dependencies(monkeypatch)
    batch_plan = importer._ProviderDirectoryProfileBatchPlan(
        has_existing_artifacts=False,
        include_copy_batch=False,
        evidence_batches=(),
        compact_batches=batches,
        fingerprint="f" * 64,
    )

    await importer._populate_provider_directory_profile_compact_stage(
        _build(),
        has_existing_artifacts=False,
        npi_batch_size=5_000_000,
        start_batch=1,
        checkpointed=True,
        batch_plan=batch_plan,
    )

    _assert_resumed_progress(progress, batches)
