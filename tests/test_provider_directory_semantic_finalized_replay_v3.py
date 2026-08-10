# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Finalized replay binding for ordinary semantic-proof datasets."""

from __future__ import annotations

import dataclasses
import importlib

import pytest

from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
)
from tests.test_provider_directory_proof_store import (
    DATASET_ID,
    ENDPOINT_ID,
    ROOT_RUN_ID,
    SOURCE_IDS,
)
from tests.test_provider_directory_proof_store_contract_shape import (
    _sealed_semantic_proof_metadata,
)
from tests.test_provider_directory_twin_root_replay import (
    _candidate,
    _content_proof,
    _matched_dataset_map,
    _terminal_dataset_map,
)


importer = importlib.import_module("process.provider_directory_fhir")
PROJECTION_AS_OF = "2026-08-10"
PROOF_RESOURCE_SCOPE = ("Organization", "Practitioner")


def _semantic_candidate(status: str):
    verification_role = (
        importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE
        if status == importer.ENDPOINT_DATASET_VERIFICATION_BASELINE
        else importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE
    )
    return dataclasses.replace(
        _candidate("dataset_semantic", "root_semantic", verification_role),
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=PROJECTION_AS_OF,
        proof_resource_scope=PROOF_RESOURCE_SCOPE,
    )


def _embedded_twin_proof(dataset_map):
    metadata = dataset_map["publication_metadata_json"]
    return metadata[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY]["proof"]


def _build_finalized_row(monkeypatch, status):
    candidate = _semantic_candidate(status)
    if status == importer.ENDPOINT_DATASET_VERIFICATION_BASELINE:
        return candidate, _terminal_dataset_map(candidate, status)
    with monkeypatch.context() as builder_patch:
        builder_patch.setattr(
            importer,
            "_twin_root_baseline_proof",
            lambda baseline: _embedded_twin_proof(baseline),
        )
        if status == importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH:
            return candidate, _terminal_dataset_map(candidate, status)
        return candidate, _matched_dataset_map(
            candidate,
            status,
            status == importer.ENDPOINT_DATASET_PUBLISHED,
        )[0]


def _select_finalized_row(candidate, dataset_map, status):
    selection_options_by_name = {
        "requires_twin_root_verification": True,
        "verification_campaign_id": candidate.verification_campaign_id,
        "verification_source_scope_hash": (
            candidate.verification_source_scope_hash
        ),
    }
    if status in {
        importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH,
    }:
        return importer._verification_terminal_endpoint_dataset_selection(
            dataset_map,
            candidate.dataset_id,
            candidate.endpoint_id,
            candidate.acquisition_root_run_id,
            **selection_options_by_name,
        )
    selector = (
        importer._validated_endpoint_dataset_selection
        if status == importer.ENDPOINT_DATASET_VALIDATED
        else importer._published_endpoint_dataset_selection
    )
    return selector(
        dataset_map,
        candidate.dataset_id,
        candidate.endpoint_id,
        candidate.acquisition_root_run_id,
        **selection_options_by_name,
    )


@pytest.mark.parametrize(
    "status",
    (
        importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH,
        importer.ENDPOINT_DATASET_VALIDATED,
        importer.ENDPOINT_DATASET_PUBLISHED,
    ),
)
def test_v3_finalized_replay_rejects_malformed_sealed_proof(
    monkeypatch,
    status,
):
    """Reject every finalized state when its mandatory seal is malformed."""

    candidate, dataset_map = _build_finalized_row(monkeypatch, status)
    dataset_map["publication_metadata_json"][
        PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
    ] = {"malformed": True}

    with pytest.raises(RuntimeError, match="verification_.*_invalid"):
        _select_finalized_row(candidate, dataset_map, status)


def test_v3_finalized_replay_binds_twin_and_outcome_proofs(monkeypatch):
    """Accept one identity and reject typed outcome or twin-proof drift."""

    status = importer.ENDPOINT_DATASET_VERIFICATION_BASELINE
    candidate, dataset_map = _build_finalized_row(monkeypatch, status)
    content_proof = _content_proof()
    metadata = dataset_map["publication_metadata_json"]
    metadata[
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
    ] = importer._outcome_resource_count_proof(candidate, content_proof)
    stored_proof_by_field = {
        "dataset_hash": content_proof.dataset_hash,
        "resource_count": content_proof.resource_count,
        "resource_hashes": content_proof.resource_hashes,
        "resource_counts": content_proof.resource_counts,
    }
    monkeypatch.setattr(
        importer,
        "_validated_finalized_stored_proof",
        lambda _dataset_map, _metadata: stored_proof_by_field,
    )

    assert _select_finalized_row(candidate, dataset_map, status) is not None
    outcome_proof = metadata[
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
    ]
    outcome_proof["version"] = True
    with pytest.raises(RuntimeError, match="verification_proof_invalid"):
        importer._validate_finalized_content_proof(dataset_map, metadata)
    outcome_proof["version"] = 1
    outcome_proof["resource_counts"]["Organization"] = True
    with pytest.raises(RuntimeError, match="verification_proof_invalid"):
        importer._validate_finalized_content_proof(dataset_map, metadata)
    outcome_proof["resource_counts"] = dict(content_proof.resource_counts)

    embedded_proof = _embedded_twin_proof(dataset_map)
    embedded_proof["resource_count"] = True
    with pytest.raises(RuntimeError, match="verification_proof_invalid"):
        importer._validate_finalized_content_proof(dataset_map, metadata)
    embedded_proof["resource_count"] = content_proof.resource_count
    embedded_proof["resource_hashes"] = {
        "Organization": "f" * 64,
        "Practitioner": "c" * 64,
    }
    with pytest.raises(RuntimeError, match="verification_proof_invalid"):
        importer._validate_finalized_content_proof(dataset_map, metadata)


def _artifact_metadata_with_sealed_proof(stored_proof_by_field):
    """Build one exact semantic artifact proof identity."""

    return {
        "source_ids": SOURCE_IDS,
        "selected_resources": ["Practitioner"],
        "expected_resources": ["Practitioner"],
        "resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "semantic_projection_as_of": "2026-08-09",
        "proof_resource_scope": ["Practitioner"],
        PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY: stored_proof_by_field,
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY: {
            "complete": True,
            "version": 1,
            "dataset_id": DATASET_ID,
            "endpoint_id": ENDPOINT_ID,
            "acquisition_root_run_id": ROOT_RUN_ID,
            "source_ids": SOURCE_IDS,
            "selected_resources": ["Practitioner"],
            "dataset_hash": stored_proof_by_field["dataset_hash"],
            "resource_count": stored_proof_by_field["resource_count"],
            "resource_counts": stored_proof_by_field["resource_counts"],
        },
    }


def _artifact_row_with_sealed_proof(stored_proof_by_field):
    """Build one finalized artifact row bound to the supplied seal."""

    return {
        "source_id": SOURCE_IDS[0],
        "endpoint_id": ENDPOINT_ID,
        "source_record_json": {
            "source_id": SOURCE_IDS[0],
            "endpoint_id": ENDPOINT_ID,
            "metadata_json": {
                "provider_directory_supported_resources": ["Practitioner"],
                "provider_directory_fully_enumerable_resources": [
                    "Practitioner"
                ],
            },
        },
        "dataset_id": DATASET_ID,
        "evidence_run_id": ROOT_RUN_ID,
        "selected_resources": ["Practitioner"],
        "recorded_expected_resources": ["Practitioner"],
        "status": importer.ENDPOINT_DATASET_PUBLISHED,
        "is_current": True,
        "dataset_hash": stored_proof_by_field["dataset_hash"],
        "resource_count": stored_proof_by_field["resource_count"],
        "publication_metadata_json": _artifact_metadata_with_sealed_proof(
            stored_proof_by_field
        ),
    }


@pytest.mark.asyncio
async def test_v3_artifact_selection_binds_sealed_proof_to_row():
    """Reject a coherent row and outcome mutation outside the sealed proof."""

    stored_proof_by_field = await _sealed_semantic_proof_metadata()
    artifact_row_by_field = _artifact_row_with_sealed_proof(
        stored_proof_by_field
    )
    assert importer._provider_directory_artifact_dataset_from_row(
        artifact_row_by_field
    ) is not None

    artifact_row_by_field["dataset_hash"] = "f" * 64
    artifact_row_by_field["publication_metadata_json"][
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
    ]["dataset_hash"] = "f" * 64
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        importer._provider_directory_artifact_dataset_from_row(
            artifact_row_by_field
        )
