# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for proof-bound Profile publication integration."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from .test_provider_directory_profile_selection_attestation import _execution

importer = importlib.import_module("process.provider_directory_fhir")


def _published_dataset_state() -> dict[str, object]:
    return {
        "dataset_id": "dataset-1",
        "acquisition_root_run_id": "run-root-1",
        "dataset_hash": "b" * 64,
        "status": importer.ENDPOINT_DATASET_PUBLISHED,
        "is_current": True,
        "superseded_at": None,
        "publication_metadata_json": {"source_ids": ["pdfhir_payer"]},
    }


@pytest.mark.asyncio
async def test_current_profile_dataset_map_covers_absent_and_present_rows(
    monkeypatch,
):
    first = AsyncMock(side_effect=[None, _published_dataset_state()])
    monkeypatch.setattr(importer.db, "first", first)

    assert await importer._current_profile_dataset_map("pdfhir_payer") == {}
    assert await importer._current_profile_dataset_map("pdfhir_payer") == (
        _published_dataset_state()
    )


@pytest.mark.asyncio
async def test_source_local_profile_followup_requires_exact_current_dataset(
    monkeypatch,
):
    monkeypatch.setattr(
        importer.profile_artifact,
        "configured_profile_source_ids",
        lambda: ("pdfhir_payer",),
    )
    current_dataset = AsyncMock(return_value={})
    monkeypatch.setattr(importer, "_current_profile_dataset_map", current_dataset)
    assert await importer._source_local_profile_followup_if_current(
        source_ids=[],
        expected_acquisition_root_run_id="run-root-1",
    ) is None
    assert await importer._source_local_profile_followup_if_current(
        source_ids=["pdfhir_payer"],
        expected_acquisition_root_run_id=None,
    ) is None
    assert await importer._source_local_profile_followup_if_current(
        source_ids=["not-configured"],
        expected_acquisition_root_run_id="run-root-1",
    ) is None
    assert await importer._source_local_profile_followup_if_current(
        source_ids=["pdfhir_payer"],
        expected_acquisition_root_run_id="run-root-1",
    ) is None

    current_dataset.return_value = _published_dataset_state()
    followup = await importer._source_local_profile_followup_if_current(
        source_ids=["pdfhir_payer"],
        expected_acquisition_root_run_id="run-root-1",
    )
    assert followup["dataset_id"] == "dataset-1"
    assert followup["parent_run_id"] == "run-root-1"


@pytest.mark.asyncio
async def test_attested_profile_fence_covers_purge_and_publish(monkeypatch):
    purge_execution = SimpleNamespace(
        attestation=SimpleNamespace(operation="purge")
    )
    purge_fence = await importer._attested_profile_publication_fence(
        run_id="run-purge",
        metrics={},
        execution=purge_execution,
        source_ids=[],
    )
    assert purge_fence.datasets == ()

    expected_fence = importer.ProviderDirectoryArtifactDatasetFence(())
    prepare = AsyncMock(return_value=expected_fence)
    monkeypatch.setattr(
        importer,
        "_prepare_artifact_publication_fence",
        prepare,
    )
    observed_fence = await importer._attested_profile_publication_fence(
        run_id="run-publish",
        metrics={},
        execution=_execution(),
        source_ids=["pdfhir_payer"],
    )
    assert observed_fence is expected_fence
    prepare.assert_awaited_once()


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
                    "profile_rows": 2,
                    "evidence_rows": 3,
                    "selected_evidence_rows": 2,
                }
            },
        )

    published_metrics_by_name = {
        "profile": {
            "generation_id": "profile-one",
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
