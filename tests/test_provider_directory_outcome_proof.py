# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import dataclasses
import importlib
from unittest.mock import ANY, AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _candidate() -> importer.EndpointDatasetCandidate:
    resources = ("Organization", "Practitioner")
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_1",
        dataset_id="dataset_candidate",
        acquisition_root_run_id="root_candidate",
        source_ids=("source_a", "source_b"),
        selected_resources=resources,
        expected_resources=resources,
        import_run_id="root_candidate",
        previous_dataset_id="dataset_current",
        requires_twin_root_verification=False,
    )


def _content_proof() -> importer.EndpointDatasetContentProof:
    return importer.EndpointDatasetContentProof(
        dataset_hash="a" * 64,
        resource_count=2,
        resource_hashes={
            "Organization": "b" * 64,
            "Practitioner": "c" * 64,
        },
        resource_counts={"Organization": 1, "Practitioner": 1},
    )


@pytest.mark.asyncio
async def test_non_twin_candidate_preserves_full_resource_count_proof(
    monkeypatch,
):
    candidate = _candidate()
    content_proof = _content_proof()
    proof_builder = AsyncMock(return_value=content_proof)
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_content_proof",
        proof_builder,
    )

    observed_proof = await importer._candidate_endpoint_dataset_content_proof(
        object(),
        candidate,
    )

    assert observed_proof is content_proof
    proof_builder.assert_awaited_once_with(
        ANY,
        candidate.dataset_id,
        candidate.selected_resources,
    )


def test_outcome_resource_count_proof_binds_exact_dataset_identity():
    candidate = _candidate()

    proof = importer._outcome_resource_count_proof(candidate, _content_proof())

    assert proof == {
        "complete": True,
        "version": 1,
        "dataset_id": candidate.dataset_id,
        "endpoint_id": candidate.endpoint_id,
        "acquisition_root_run_id": candidate.acquisition_root_run_id,
        "dataset_hash": "a" * 64,
        "source_ids": list(candidate.source_ids),
        "selected_resources": list(candidate.selected_resources),
        "resource_count": 2,
        "resource_counts": {"Organization": 1, "Practitioner": 1},
    }


def test_outcome_resource_count_proof_includes_linked_supported_resources():
    candidate = dataclasses.replace(
        _candidate(),
        selected_resources=("Practitioner",),
        expected_resources=("Practitioner",),
    )

    proof = importer._outcome_resource_count_proof(candidate, _content_proof())

    assert proof["selected_resources"] == ["Practitioner"]
    assert proof["resource_counts"] == {
        "Organization": 1,
        "Practitioner": 1,
    }


def test_outcome_resource_count_proof_rejects_unreconciled_counts():
    invalid_proof = dataclasses.replace(
        _content_proof(),
        resource_counts={"Organization": 1, "Practitioner": 2},
    )

    with pytest.raises(
        RuntimeError,
        match="provider_directory_outcome_resource_count_proof_invalid",
    ):
        importer._outcome_resource_count_proof(_candidate(), invalid_proof)
