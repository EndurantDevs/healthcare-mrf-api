# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""A desired CMS release cannot select unrelated unpublished source candidates."""

import dataclasses
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_desired_fence as desired_fence

fhir = importlib.import_module("process.provider_directory_fhir")


def _pair(dataset):
    return {
        "source_id": dataset.source_id,
        "endpoint_id": dataset.endpoint_id,
        "dataset_id": dataset.dataset_id,
        "dataset_hash": dataset.dataset_hash,
        "acquisition_root_run_id": dataset.evidence_run_id,
        "publication_status": dataset.status,
        "is_current": dataset.is_current,
    }


def _fixture(*, current=False):
    cms = fhir.ProviderDirectoryArtifactDataset(
        source_id="cms-npd",
        endpoint_id="cms-endpoint",
        dataset_id="cms-new",
        evidence_run_id="cms-run",
        dataset_hash="a" * 64,
        selected_resources=tuple(sorted(desired_fence._RESOURCE_TYPES)),
        status="published" if current else "validated",
        is_current=current,
        promote_on_cutover=not current,
        expected_incumbent_dataset_id="cms-old" if not current else None,
    )
    other = fhir.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        evidence_run_id="run-a",
        dataset_hash="b" * 64,
        selected_resources=("Practitioner",),
    )
    cms_pair = _pair(cms)
    execution = SimpleNamespace(
        attestation=SimpleNamespace(
            pairs=(cms_pair, _pair(other)),
            desired_cms_dataset=cms_pair,
            expected_cms_incumbent=cms_pair if current else {**cms_pair, "dataset_id": "cms-old"},
        )
    )
    return cms, other, execution


def _backend(cms, other):
    return SimpleNamespace(
        ProviderDirectoryArtifactDatasetFence=fhir.ProviderDirectoryArtifactDatasetFence,
        _resolve_provider_directory_artifact_datasets=AsyncMock(
            side_effect=[
                fhir.ProviderDirectoryArtifactDatasetFence((other,)),
                fhir.ProviderDirectoryArtifactDatasetFence(
                    (cms,), should_select_validated_candidates=cms.promote_on_cutover
                ),
            ]
        ),
        _assert_profile_selection_matches_artifact_fence=fhir._assert_profile_selection_matches_artifact_fence,
        _verify_provider_directory_artifact_dataset_fence=AsyncMock(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("current", [False, True])
async def test_only_cms_may_select_a_candidate(current):
    cms, other, execution = _fixture(current=current)
    backend = _backend(cms, other)
    fence = await desired_fence.resolve_desired_fence(backend, execution)
    calls = backend._resolve_provider_directory_artifact_datasets.call_args_list
    assert calls[0].args == (["source-a"],)
    assert calls[0].kwargs == {"should_select_validated_candidates": False}
    assert calls[1].kwargs == {"should_select_validated_candidates": not current}
    assert fence.datasets == (cms, other)
    assert fence.promotion_datasets == ([] if current else [cms])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,error",
    [
        ({"dataset_hash": "c" * 64}, "candidate_selection_changed"),
        ({"selected_resources": ("Practitioner",)}, "candidate_selection_changed"),
        ({"expected_incumbent_dataset_id": "different"}, "incumbent_changed"),
    ],
)
async def test_desired_pin_and_complete_resource_vector_cannot_drift(change, error):
    cms, other, execution = _fixture()
    backend = _backend(dataclasses.replace(cms, **change), other)
    with pytest.raises(RuntimeError, match=error):
        await desired_fence.resolve_desired_fence(backend, execution)


@pytest.mark.asyncio
async def test_an_unrelated_candidate_cannot_enter_the_bundle():
    cms, other, execution = _fixture()
    backend = _backend(cms, dataclasses.replace(other, promote_on_cutover=True))
    with pytest.raises(RuntimeError, match="other_candidate_selected"):
        await desired_fence.resolve_desired_fence(backend, execution)


def test_same_byte_cms_refresh_still_requires_complete_profile_metrics():
    cms, _other, _execution = _fixture(current=True)
    with pytest.raises(RuntimeError, match="artifact_metric_missing:profile"):
        fhir._assert_candidate_artifact_bundle_complete(
            fhir.ProviderDirectoryArtifactDatasetFence((cms,)),
            {},
            fhir.ProviderDirectoryArtifactBundle(),
            publish_corroboration=False,
            publish_artifacts_targets={"profile"},
        )


def test_cms_cannot_skip_its_enabled_profile_projection():
    with pytest.raises(RuntimeError, match="artifact_skipped:profile"):
        fhir._assert_candidate_artifact_metrics_complete(
            {"profile"},
            {"profile": {"skipped": True, "reason": "no_profile_enabled_sources_in_scope"}},
            allow_no_profile_sources=False,
        )
