# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded admission summaries for completed reviewed subsets."""

from __future__ import annotations

import copy
from dataclasses import replace
import importlib
import json
from unittest.mock import AsyncMock

import pytest

from process.provider_directory_admission_seal import (
    ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    AdmissionSealError,
    admission_seal_from_validated_metadata,
)
from process.provider_directory_fhir_census_execution import (
    current_version_census_completed_proof,
    current_version_census_initial_proof,
)
from process.provider_directory_fhir_root_policy import ReviewedRootPolicy
from process.provider_directory_fhir_subset_canonical import canonical_payload_json
from tests.provider_directory_fhir_subset_completion_support import (
    PAGE_COUNT,
    build_finalization_inputs,
)
from tests.test_provider_directory_dataset_selection_bounded_db import (
    _large_metadata_by_field,
)


importer = importlib.import_module("process.provider_directory_fhir")
LARGE_PAGE_COUNT = 3_150


def _large_completed_proof(contract, resource_type: str) -> dict:
    """Return one valid 3,150-page terminal proof without page-by-page work."""

    pages = LARGE_PAGE_COUNT
    checkpointed_pages = pages - 1
    proof = current_version_census_initial_proof(
        contract,
        resource_type,
        2,
        expected_page_count=PAGE_COUNT,
    )
    proof["page_geometry"] = {
        "version": 2,
        "page_count": PAGE_COUNT,
        "checkpointed_pages": checkpointed_pages,
        "checkpointed_rows": 1,
        "logical_next_offset": checkpointed_pages * PAGE_COUNT,
        "sparse_pages": checkpointed_pages,
        "empty_pages": checkpointed_pages - 1,
    }
    proof["page_entry_counts"] = [1] + [0] * (checkpointed_pages - 1)
    proof["continuation_hop_sha256"] = ["1" * 64] * checkpointed_pages
    proof["continuation_shape_sha256"] = ["a" * 64] * checkpointed_pages
    return current_version_census_completed_proof(
        proof,
        post_count=2,
        processed_rows=1,
        unique_candidate_rows=1,
        pages_processed=pages,
        expected_page_count=PAGE_COUNT,
        terminal_page_entry_count=0,
    )


def _large_subset_inputs():
    """Build a production-shaped single-root proof above the legacy cap."""

    contract, candidate, diagnostics, content_proof = build_finalization_inputs()
    candidate = replace(
        candidate,
        requires_twin_root_verification=False,
        reviewed_root_policy=ReviewedRootPolicy(1),
        verification_role=None,
    )
    for resource_type in tuple(contract.resources)[:2]:
        proof = _large_completed_proof(contract, resource_type)
        diagnostics[resource_type] = {
            "fetch_mode": importer.SERVER_ISSUED_SUBSET_FETCH_MODE,
            "rows_fetched": 1,
            "pages_fetched": LARGE_PAGE_COUNT,
            "server_issued_subset_completeness": (
                importer._sanitized_server_issued_subset_execution_proof(proof)
            ),
            importer._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY: (
                importer._server_issued_subset_internal_replay_evidence(proof)
            ),
        }
    completed_content = importer._content_proof_with_subset_completion(
        candidate,
        diagnostics,
        content_proof,
    )
    metadata_by_key = importer._dataset_validation_metadata(
        candidate,
        diagnostics,
        completed_content,
        {},
        {},
        {},
        {},
    )
    metadata_by_key[importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY] = (
        _large_metadata_by_field(1)[
            importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
        ]
    )
    return contract, candidate, completed_content, metadata_by_key


def _assert_projection_rebuilds_exactly(
    metadata_by_key: dict,
    completion_pair: tuple,
    resource_type: str,
) -> None:
    """Reject null, forged, or stale compact projections before sealing."""

    null_projection = copy.deepcopy(metadata_by_key)
    null_projection[importer.PROVIDER_DIRECTORY_SUBSET_ADMISSION_SUMMARY_KEY] = None
    forged_projection = copy.deepcopy(metadata_by_key)
    forged_projection[
        importer.PROVIDER_DIRECTORY_SUBSET_ADMISSION_SUMMARY_KEY
    ]["completion_proof"]["dataset"]["count"] += 1
    stale_projection = copy.deepcopy(metadata_by_key)
    stale_projection["resource_diagnostics"][resource_type]["rows_fetched"] += 1
    for rejected_metadata in (
        null_projection,
        forged_projection,
        stale_projection,
    ):
        with pytest.raises(RuntimeError, match="admission_summary_invalid"):
            importer._subset_admission_seal_metadata(
                rejected_metadata,
                completion_pair,
            )


async def _assert_writer_preserves_raw_metadata(
    candidate,
    completed_content,
    metadata_by_key: dict,
    completion_pair: tuple,
) -> None:
    """Store the bounded receipt while retaining complete raw evidence."""

    connection = AsyncMock()
    connection.all.return_value = [
        {"source_id": source_id} for source_id in candidate.source_ids
    ]
    connection.status.return_value = "UPDATE 1"
    await importer._store_validated_endpoint_dataset(
        connection,
        candidate,
        candidate.previous_dataset_id,
        completed_content.dataset_hash,
        completed_content.resource_count,
        metadata_by_key,
        status=importer.ENDPOINT_DATASET_VALIDATED,
        completion_proof_pair=completion_pair,
    )
    stored_fields = connection.status.await_args.kwargs
    stored_raw = json.loads(stored_fields["publication_metadata_json"])
    stored_summary = json.loads(
        stored_fields["publication_metadata_summary_json"]
    )
    assert "resource_diagnostics" in stored_raw
    assert importer.SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_KEY in stored_raw
    assert "resource_diagnostics" not in stored_summary
    assert importer.SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_KEY not in stored_summary


@pytest.mark.asyncio
async def test_large_single_root_subset_uses_bounded_admission_summary():
    """Seal a large exact proof without dropping its raw retained evidence."""

    contract, candidate, completed_content, metadata_by_key = (
        _large_subset_inputs()
    )
    proof_keys = {
        importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
        importer.UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
    }
    raw_summary_by_key = {
        key: metadata_value
        for key, metadata_value in metadata_by_key.items()
        if key not in proof_keys
    }
    assert len(canonical_payload_json(raw_summary_by_key).encode()) > (
        ADMISSION_METADATA_SUMMARY_MAX_BYTES
    )
    with pytest.raises(AdmissionSealError, match="metadata_summary_unbounded"):
        admission_seal_from_validated_metadata(metadata_by_key)

    completion_pair = (
        completed_content.completion_proof,
        completed_content.completion_proof_sha256,
    )
    _assert_projection_rebuilds_exactly(
        metadata_by_key,
        completion_pair,
        contract.resources[0],
    )
    seal_input = importer._subset_admission_seal_metadata(
        metadata_by_key,
        completion_pair,
    )
    receipt = admission_seal_from_validated_metadata(seal_input)
    assert receipt is not None
    assert len(canonical_payload_json(receipt.metadata_summary).encode()) < (
        ADMISSION_METADATA_SUMMARY_MAX_BYTES
    )
    projection = receipt.metadata_summary[
        importer.PROVIDER_DIRECTORY_SUBSET_ADMISSION_SUMMARY_KEY
    ]
    assert "resources" not in projection["completion_proof"]
    assert set(projection["replay_evidence"]) == {"proof_sha256"}
    await _assert_writer_preserves_raw_metadata(
        candidate,
        completed_content,
        metadata_by_key,
        completion_pair,
    )
