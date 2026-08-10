# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic pure inputs for reviewed subset activation tests."""

from __future__ import annotations

from copy import deepcopy

from process import provider_directory_fhir_subset_activation as activation
from process import provider_directory_proof_store as proof_store
from process.provider_directory_fhir_root_policy import ReviewedRootPolicy
from process.provider_directory_fhir_subset_canonical import canonical_sha256
from process.provider_directory_fhir_subset_identity import (
    server_issued_subset_source_scope_payload,
)
from tests.provider_directory_subset_completion_pg_support import (
    CUTOFF,
    RESOURCE_TYPES,
    VALID_RESOURCE_ROWS,
    VALID_SOURCE_SCOPE_SHA256,
    _resource_diagnostics_from_proof,
    coverage_from_proof,
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
    candidate_metadata_by_field = terminal_metadata(
        proof,
        proof_sha256,
        replay,
        replay_sha256,
        "root-candidate",
        baseline_dataset_id="dataset-baseline",
        baseline_root_run_id="root-baseline",
    )
    baseline_metadata[
        proof_store.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
    ] = _single_root_content_proof(
        proof,
        dataset_id="dataset-baseline",
        root_run_id="root-baseline",
    )
    candidate_metadata_by_field[
        proof_store.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
    ] = _single_root_content_proof(
        proof,
        dataset_id="dataset-candidate",
        root_run_id="root-candidate",
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
            candidate_metadata_by_field,
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


def _single_root_content_proof(
    proof,
    *,
    dataset_id: str,
    root_run_id: str,
):
    descriptors = []
    for resource_row in VALID_RESOURCE_ROWS:
        descriptor, _payload = proof_store.build_dataset_proof_shard(
            [resource_row],
            dataset_id=dataset_id,
            endpoint_id="endpoint-a",
            acquisition_root_run_id=root_run_id,
            source_ids=("synthetic-source",),
        )
        descriptors.append(descriptor)
    descriptors.sort(key=lambda descriptor: descriptor["shard_id"])
    completion_dataset = proof["dataset"]
    metadata = {
        "contract_id": (
            proof_store.PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
        ),
        "complete": True,
        "dataset_id": dataset_id,
        "endpoint_id": "endpoint-a",
        "acquisition_root_run_id": root_run_id,
        "source_ids": ["synthetic-source"],
        "selected_resources": list(RESOURCE_TYPES),
        "dataset_hash": completion_dataset["hash"],
        "resource_count": completion_dataset["count"],
        "resource_hashes": completion_dataset["resource_hashes"],
        "resource_counts": completion_dataset["resource_counts"],
        "source_metrics": {
            "address_records": 0,
            "addressed_locations": 0,
            "distinct_npis": 0,
            "geocoded_locations": 0,
        },
        "npi_set_sha256": "0" * 64,
        "shard_count": len(descriptors),
        "shard_set_sha256": proof_store._line_hash(
            proof_store._stable_json(descriptor).encode()
            for descriptor in descriptors
        ),
        "shards": descriptors,
    }
    metadata["proof_sha256"] = proof_store._json_hash(metadata)
    return proof_store.validate_stored_dataset_proof_metadata(
        metadata,
        dataset_id=dataset_id,
        endpoint_id="endpoint-a",
        acquisition_root_run_id=root_run_id,
        source_ids=("synthetic-source",),
        selected_resources=RESOURCE_TYPES,
    )


def _single_root_candidate_metadata(
    proof,
    proof_sha256,
    replay,
    replay_sha256,
    scope_sha256,
    root_policy,
):
    coverage = coverage_from_proof(proof, proof_sha256, "not_required")
    for resource_coverage in coverage["resources"].values():
        resource_coverage["twin_state"] = "not_required"
    return {
        "acquisition_root_run_id": "root-candidate",
        "requires_twin_root_verification": False,
        "verification_campaign_id": proof["campaign_id"],
        "verification_source_scope_hash": scope_sha256,
        "source_ids": ["synthetic-source"],
        "selected_resources": list(RESOURCE_TYPES),
        "expected_resources": list(RESOURCE_TYPES),
        "provider_directory_reviewed_root_policy_v1": (
            root_policy.document()
        ),
        "server_issued_subset_replay_evidence": replay,
        "server_issued_subset_replay_evidence_sha256": replay_sha256,
        "server_issued_subset_coverage": coverage,
        "provider_directory_content_proof_v1": (
            _single_root_content_proof(
                proof,
                dataset_id="dataset-candidate",
                root_run_id="root-candidate",
            )
        ),
        "resource_diagnostics": _resource_diagnostics_from_proof(proof),
    }


def single_root_activation_inputs():
    """Return one policy-bearing source and one complete sealed root."""

    root_policy = ReviewedRootPolicy(1)
    source_record = valid_source_record("pending_reviewed_subset_acquisition")
    source_record["metadata_json"][
        "provider_directory_reviewed_root_policy_v1"
    ] = root_policy.document()
    proof, proof_sha256, replay, replay_sha256 = valid_evidence_pairs()
    scope_sha256 = canonical_sha256(
        server_issued_subset_source_scope_payload(
            source_record,
            (source_record["source_id"],),
            CUTOFF,
            source_record["canonical_api_base"],
        )
    )
    candidate_metadata_by_field = _single_root_candidate_metadata(
        proof,
        proof_sha256,
        replay,
        replay_sha256,
        scope_sha256,
        root_policy,
    )
    dataset_rows = [
        _dataset_row(
            proof,
            proof_sha256,
            "dataset-candidate",
            "root-candidate",
            "validated",
            candidate_metadata_by_field,
        )
    ]
    evidence = activation.ReviewedSubsetActivationEvidence(
        source_contract_sha256=(
            activation.reviewed_subset_source_contract_sha256(source_record)
        ),
        cutoff=CUTOFF,
        verification_source_scope_sha256=scope_sha256,
        completion_proof_sha256=proof_sha256,
        root_policy=root_policy,
    )
    return deepcopy(source_record), deepcopy(dataset_rows), evidence
