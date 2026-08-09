# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic pure inputs for reviewed subset activation tests."""

from __future__ import annotations

from process import provider_directory_fhir_subset_activation as activation
from tests.provider_directory_subset_completion_pg_support import (
    CUTOFF,
    RESOURCE_TYPES,
    VALID_SOURCE_SCOPE_SHA256,
    terminal_metadata,
    valid_evidence_pairs,
    valid_source_record,
)


def _dataset_row(
    proof,
    proof_sha256,
    dataset_id,
    root_run_id,
    status,
    metadata_by_field,
):
    return {
        "dataset_id": dataset_id,
        "endpoint_id": "endpoint-a",
        "acquisition_root_run_id": root_run_id,
        "status": status,
        "is_current": False,
        "dataset_hash": proof["dataset"]["hash"],
        "resource_count": len(RESOURCE_TYPES),
        "publication_metadata_json": metadata_by_field,
        "completion_proof_required_version": 3,
        "completion_proof_json": proof,
        "completion_proof_sha256": proof_sha256,
        "validated_at": (
            "2026-08-09T00:01:00Z" if status == "validated" else None
        ),
        "published_at": None,
        "superseded_at": None,
    }


def activation_inputs():
    """Return one exact pending source and matched sealed root pair."""

    source_record = valid_source_record(activation.PENDING_STATUS)
    proof, proof_sha256, replay, replay_sha256 = valid_evidence_pairs()
    baseline_metadata = terminal_metadata(
        proof, proof_sha256, replay, replay_sha256, "root-baseline"
    )
    candidate_metadata = terminal_metadata(
        proof,
        proof_sha256,
        replay,
        replay_sha256,
        "root-candidate",
        baseline_dataset_id="dataset-baseline",
        baseline_root_run_id="root-baseline",
    )

    dataset_rows = [
        _dataset_row(
            proof,
            proof_sha256,
            "dataset-baseline",
            "root-baseline",
            "verification_baseline",
            baseline_metadata,
        ),
        _dataset_row(
            proof,
            proof_sha256,
            "dataset-candidate",
            "root-candidate",
            "validated",
            candidate_metadata,
        ),
    ]
    evidence = activation.ReviewedSubsetActivationEvidence(
        source_contract_sha256=(
            activation.reviewed_subset_source_contract_sha256(source_record)
        ),
        cutoff=CUTOFF,
        verification_source_scope_sha256=VALID_SOURCE_SCOPE_SHA256,
        completion_proof_sha256=proof_sha256,
    )
    return source_record, dataset_rows, evidence
