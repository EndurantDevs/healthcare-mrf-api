# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure fence binding checks; native authority is qualified separately."""

import importlib
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.provider_directory_cms_preparation import desired_fence_hash
from process.provider_directory_dataset_rehydrate_publish import _fence_scope
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError

directory = importlib.import_module("process.provider_directory_fhir")


def _inputs():
    dataset = directory.ProviderDirectoryArtifactDataset(
        "source",
        "endpoint",
        "dataset",
        "root",
        dataset_hash="a" * 64,
        resource_count=1,
        retained_resources=("Organization",),
    )
    fence = directory.ProviderDirectoryArtifactDatasetFence((dataset,))
    scope = SimpleNamespace(
        source_id="source",
        endpoint_id="endpoint",
        dataset_id="dataset",
        acquisition_root_run_id="root",
        dataset_hash="a" * 64,
        resource_count=1,
    )
    operation_by_field = dict(resource_types=["Organization"], source_fence_digest=desired_fence_hash(fence))
    return SimpleNamespace(scope=scope), operation_by_field, fence


def test_published_full_fence_preserves_other_global_sources():
    pinned, operation, fence = _inputs()
    other = replace(fence.datasets[0], source_id="other", dataset_id="other-dataset")
    fence = replace(fence, datasets=(*fence.datasets, other))
    operation["source_fence_digest"] = desired_fence_hash(fence)
    _fence_scope(directory, pinned, operation, fence)


@pytest.mark.parametrize(
    "change",
    [
        {"source_id": "other"},
        {"endpoint_id": "other"},
        {"dataset_id": "other"},
        {"evidence_run_id": "other"},
        {"dataset_hash": "b" * 64},
        {"resource_count": 2},
        {"retained_resources": ("Location",)},
        {"is_current": False},
        {"status": "validated"},
        {"promote_on_cutover": True},
        {"reconcile_source_alias_on_cutover": True},
    ],
)
def test_changed_selected_source_or_promotion_refuses(change):
    pinned, operation, fence = _inputs()
    fence = replace(fence, datasets=(replace(fence.datasets[0], **change),))
    operation["source_fence_digest"] = desired_fence_hash(fence)
    with pytest.raises(DatasetRehydrationError, match="artifact_fence_changed"):
        _fence_scope(directory, pinned, operation, fence)


def test_changed_global_fence_digest_refuses():
    pinned, operation, fence = _inputs()
    operation["source_fence_digest"] = "f" * 64
    with pytest.raises(DatasetRehydrationError, match="artifact_fence_changed"):
        _fence_scope(directory, pinned, operation, fence)


@pytest.mark.parametrize(
    "change",
    [
        {"should_select_validated_candidates": True},
        {"validated_publication_candidate": object()},
    ],
)
def test_candidate_selection_cannot_enter_retained_repair(change):
    pinned, operation, fence = _inputs()
    with pytest.raises(DatasetRehydrationError, match="artifact_fence_changed"):
        _fence_scope(directory, pinned, operation, replace(fence, **change))
