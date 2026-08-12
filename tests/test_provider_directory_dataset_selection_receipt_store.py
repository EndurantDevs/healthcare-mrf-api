# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Atomic write contract for compact Provider Directory selection receipts."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_dataset_selection_receipt_db import (
    _ZERO_SUMMARY_COUNTS,
    _large_metadata_with_normalized_receipt,
    _receipt_candidate_and_proof,
    importer,
)


def test_validation_metadata_builds_exact_selection_receipt():
    candidate, content_proof = _receipt_candidate_and_proof()
    metadata = _large_metadata_with_normalized_receipt()
    summary = metadata[importer.SOURCE_SUMMARY_METADATA_KEY]

    assert summary["resource_hashes"] == content_proof.resource_hashes
    assert summary["resource_counts"] == content_proof.resource_counts
    assert metadata[
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
    ] == importer._outcome_resource_count_proof(candidate, content_proof)
    receipt = importer._artifact_selection_receipt(metadata)
    assert receipt is not None
    assert receipt[importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY] == {
        "complete": True,
        "contract_id": content_proof.proof_metadata["contract_id"],
        "proof_sha256": content_proof.proof_metadata["proof_sha256"],
    }
    assert len(json.dumps(receipt).encode()) < 64 * 1024


def test_validation_metadata_rejects_mismatched_selection_receipt():
    candidate, content_proof = _receipt_candidate_and_proof()
    mismatched_proof = importer.EndpointDatasetContentProof(
        dataset_hash=content_proof.dataset_hash,
        resource_count=content_proof.resource_count,
        resource_hashes={"Location": "0" * 64},
        resource_counts=content_proof.resource_counts,
    )
    summary = importer._build_endpoint_dataset_source_summary(
        candidate, mismatched_proof, _ZERO_SUMMARY_COUNTS, "root-candidate"
    )

    with pytest.raises(importer.ProviderDirectorySourceSummaryError):
        importer._dataset_validation_metadata(
            candidate,
            {},
            content_proof,
            {},
            {},
            {},
            {importer.SOURCE_SUMMARY_METADATA_KEY: summary},
        )


def test_selection_receipt_rejects_oversized_bounded_metadata():
    metadata = _large_metadata_with_normalized_receipt()
    metadata["source_authority_id"] = "x" * (
        importer.PROVIDER_DIRECTORY_ARTIFACT_SELECTION_RECEIPT_MAX_BYTES
    )

    with pytest.raises(
        RuntimeError,
        match="artifact_selection_receipt_too_large",
    ):
        importer._artifact_selection_receipt(metadata)


def test_selection_receipt_rejects_invalid_proof_identity():
    metadata = _large_metadata_with_normalized_receipt()
    metadata[importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY] = {}

    with pytest.raises(RuntimeError, match="artifact_selection_receipt_invalid"):
        importer._artifact_selection_receipt(metadata)


@pytest.mark.asyncio
async def test_validated_store_writes_receipt_and_seal_atomically():
    candidate, content_proof = _receipt_candidate_and_proof()
    metadata = _large_metadata_with_normalized_receipt()
    connection = AsyncMock()
    connection.all.return_value = [
        {"source_id": source_id} for source_id in sorted(candidate.source_ids)
    ]
    connection.status.return_value = "UPDATE 1"

    await importer._store_validated_endpoint_dataset(
        connection,
        candidate,
        candidate.previous_dataset_id,
        content_proof.dataset_hash,
        content_proof.resource_count,
        metadata,
        status=importer.ENDPOINT_DATASET_VALIDATED,
    )

    query = connection.status.await_args.args[0]
    parameters = connection.status.await_args.kwargs
    stored_receipt = json.loads(parameters["artifact_selection_receipt_json"])
    assert "publication_metadata_json" in query
    assert "artifact_selection_receipt_json" in query
    assert "publication_metadata_summary_json" in query
    assert "publication_metadata_sha256" in query
    assert "content_proof_admission_version" in query
    assert "content_proof_admission_kind" in query
    assert "content_proof_admission_sha256" in query
    assert "content_proof_resource_types" in query
    assert stored_receipt == importer._artifact_selection_receipt(metadata)
    assert json.loads(parameters["publication_metadata_summary_json"])
    assert parameters["publication_metadata_sha256"]
    assert parameters["content_proof_admission_version"] == (
        importer.ADMISSION_SEAL_VERSION
    )
    assert parameters["content_proof_admission_kind"] == (
        importer.ADMISSION_KIND_GENERIC
    )
    assert parameters["content_proof_admission_sha256"] == (
        content_proof.proof_metadata["proof_sha256"]
    )
    assert parameters["content_proof_resource_types"] == ["Location"]
    assert "shards" not in stored_receipt[
        importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
    ]
