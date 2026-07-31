# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for attested Profile replay and pre-build fences."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from .test_provider_directory_profile_selection_attestation import _execution
from .test_provider_directory_profile_capacity import _geometry_payload
from .test_provider_directory_profile_control_capacity import (
    _bound_control_wal_projection,
)
from .provider_directory_profile_execution_test_support import (
    _capacity_consumption_row,
    _capacity_geometry_identity,
    _profile_serving_state,
    _published_dataset_state,
    _wal_tracker_admission,
)

importer = importlib.import_module("process.provider_directory_fhir")
capacity = importlib.import_module("process.provider_directory_profile_capacity")

def test_profile_selection_result_requires_complete_profile_metrics():
    execution = _execution()
    with pytest.raises(RuntimeError, match="result_missing"):
        importer._attach_profile_selection_result(execution, {})
    with pytest.raises(RuntimeError, match="generation_missing"):
        importer._attach_profile_selection_result(execution, {"profile": {}})
    with pytest.raises(RuntimeError, match="evidence_scope_incomplete"):
        importer._attach_profile_selection_result(
            execution,
            {
                "profile": {
                    "generation_id": "profile-one",
                    "profile_as_of": "2026-07-30",
                    "profile_rows": 2,
                    "evidence_rows": 3,
                    "selected_evidence_rows": 2,
                }
            },
        )

    published_metrics_by_name = {
        "profile": {
            "generation_id": "profile-one",
            "profile_as_of": "2026-07-30",
            "profile_rows": 2,
            "evidence_rows": 3,
            "selected_evidence_rows": 3,
        }
    }
    importer._attach_profile_selection_result(
        execution,
        published_metrics_by_name,
    )
    assert importer.PROFILE_SELECTION_RESULT_METRIC in published_metrics_by_name


def _patch_committed_replay_path(
    monkeypatch,
    replayed_profile_by_field: dict[str, object],
) -> tuple[AsyncMock, AsyncMock, AsyncMock, AsyncMock]:
    """Install the exact committed-replay seams and return observed mocks."""
    fence = importer.ProviderDirectoryArtifactDatasetFence(())
    monkeypatch.setattr(
        importer,
        "assert_registered_profile_selection_current",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_attested_profile_publication_fence",
        AsyncMock(return_value=fence),
    )
    monkeypatch.setattr(
        importer,
        "_assert_profile_selection_matches_artifact_fence",
        Mock(),
    )
    serving_bootstrap = AsyncMock()
    replay = AsyncMock(return_value=replayed_profile_by_field)
    replay_publish = AsyncMock(
        return_value={"profile": replayed_profile_by_field}
    )
    build = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_bootstrap_provider_directory_profile_serving_generation",
        serving_bootstrap,
    )
    monkeypatch.setattr(
        importer, "_provider_directory_profile_committed_run_replay", replay
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_replay_publish_metrics",
        replay_publish,
    )
    monkeypatch.setattr(
        importer, "_publish_attested_provider_directory_profile_build", build
    )
    return serving_bootstrap, replay, replay_publish, build


@pytest.mark.asyncio
async def test_attested_profile_returns_committed_replay_before_build(
    monkeypatch,
):
    """Return an exact committed replay without entering the build path."""
    execution = _execution()
    replayed_profile_by_field = {
        "generation_id": "pdprofile_" + "1" * 32,
        "profile_as_of": "2026-07-30",
        "profile_rows": 2,
        "evidence_rows": 3,
        "selected_evidence_rows": 3,
    }
    serving_bootstrap, replay, replay_publish, build = (
        _patch_committed_replay_path(
            monkeypatch,
            replayed_profile_by_field,
        )
    )

    publication_result = (
        await importer._publish_attested_provider_directory_profile(
            run_id="run_" + "1" * 32,
            control_run_id="run_" + "1" * 32,
            metrics={},
            execution=execution,
        )
    )

    replay.assert_awaited_once()
    serving_bootstrap.assert_awaited_once_with(importer._schema())
    replay_publish.assert_awaited_once()
    build.assert_not_awaited()
    assert importer.PROFILE_SELECTION_RESULT_METRIC in publication_result


@pytest.mark.asyncio
async def test_attested_profile_never_rebuilds_after_replay_conflict(
    monkeypatch,
):
    execution = _execution()
    fence = importer.ProviderDirectoryArtifactDatasetFence(())
    monkeypatch.setattr(
        importer,
        "assert_registered_profile_selection_current",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_attested_profile_publication_fence",
        AsyncMock(return_value=fence),
    )
    monkeypatch.setattr(
        importer,
        "_assert_profile_selection_matches_artifact_fence",
        Mock(),
    )
    monkeypatch.setattr(
        importer,
        "_bootstrap_provider_directory_profile_serving_generation",
        AsyncMock(),
    )
    replay = AsyncMock(
        side_effect=importer.ProviderDirectoryArtifactBuildStale(
            "provider_directory_profile_replay_receipt_changed"
        )
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_committed_run_replay",
        replay,
    )
    build = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_publish_attested_provider_directory_profile_build",
        build,
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="replay_receipt_changed",
    ):
        await importer._publish_attested_provider_directory_profile(
            run_id="run_" + "1" * 32,
            control_run_id="run_" + "1" * 32,
            metrics={},
            execution=execution,
        )

    build.assert_not_awaited()


@pytest.mark.asyncio
async def test_attested_profile_refuses_npi_repair_before_capacity_admission(
    monkeypatch,
):
    execution = _execution()
    fence = importer.ProviderDirectoryArtifactDatasetFence(())
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_resource_scope_fence",
        AsyncMock(return_value=fence),
    )
    repair_preflight = AsyncMock(
        side_effect=RuntimeError(
            "provider_directory_profile_resource_id_npi_backfill_"
            "required_before_capacity_admission:Organization"
        )
    )
    monkeypatch.setattr(
        importer,
        "_assert_no_provider_directory_resource_id_npi_backfill_candidates",
        repair_preflight,
    )
    admit = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_admit_provider_directory_profile_capacity",
        admit,
    )

    with pytest.raises(
        RuntimeError,
        match="required_before_capacity_admission:Organization",
    ):
        await importer._publish_attested_provider_directory_profile_build(
            run_id="run_" + "1" * 32,
            control_run_id="run_" + "1" * 32,
            metrics={},
            execution=execution,
            fence=fence,
            source_ids=["pdfhir_payer"],
        )

    repair_preflight.assert_awaited_once()
    admit.assert_not_awaited()


@pytest.mark.asyncio
async def test_admitted_profile_publish_never_runs_canonical_npi_backfill(
    monkeypatch,
):
    preflight = AsyncMock()
    backfill = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: SimpleNamespace(),
    )
    monkeypatch.setattr(
        importer,
        "_assert_no_provider_directory_resource_id_npi_backfill_candidates",
        preflight,
    )
    monkeypatch.setattr(
        importer,
        "backfill_provider_directory_resource_id_npis",
        backfill,
    )
    monkeypatch.setattr(
        importer,
        "_mark_provider_directory_progress",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_publish_provider_directory_profile_target",
        AsyncMock(return_value=({"profile_rows": 1}, "published profile")),
    )

    metrics = await importer._publish_provider_directory_artifacts(
        run_id="run_" + "1" * 32,
        metrics={},
        source_ids=["pdfhir_payer"],
        publish_artifacts_targets={"profile"},
    )

    assert metrics["resource_id_npis_backfilled"] == {
        "skipped": True,
        "reason": "capacity_admitted_preflight_zero",
    }
    preflight.assert_awaited_once()
    backfill.assert_not_awaited()


def test_profile_source_context_must_match_active_attestation():
    execution = _execution()
    matching_context = importer._ProviderDirectoryProfileSourceContext(
        source_id="pdfhir_payer",
        endpoint_id="endpoint-1",
        canonical_api_base="https://payer.example/fhir",
        org_name="Payer",
        plan_name="Payer Plan",
    )
    execution_token = importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(
        execution
    )
    try:
        importer._assert_attested_profile_source_contexts((matching_context,))
        changed_context = importer._ProviderDirectoryProfileSourceContext(
            source_id=matching_context.source_id,
            endpoint_id=matching_context.endpoint_id,
            canonical_api_base=matching_context.canonical_api_base,
            org_name=matching_context.org_name,
            plan_name="Changed Plan",
        )
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="source_context_attestation_changed",
        ):
            importer._assert_attested_profile_source_contexts((changed_context,))
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(
            execution_token
        )
