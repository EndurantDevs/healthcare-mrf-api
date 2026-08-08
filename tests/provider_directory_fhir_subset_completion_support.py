# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic fixtures for reviewed FHIR subset completion contracts."""

from __future__ import annotations

import importlib
from typing import Any

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
    SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_RESOURCE_TYPES,
    SERVER_ISSUED_SUBSET_SEMANTICS,
    SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY,
    SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
    SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
)
from process.provider_directory_fhir_census_execution import (
    SERVER_ISSUED_SUBSET_FETCH_MODE,
    current_version_census_checkpoint_proof,
    current_version_census_completed_proof,
    current_version_census_initial_proof,
)
from process.provider_directory_fhir_subset_completion import (
    build_subset_completion_proof,
    canonical_sha256,
)


importer = importlib.import_module("process.provider_directory_fhir")

CUTOFF = "2026-08-01T12:00:00.000000Z"
PAGE_COUNT = 250


def build_subset_contract(**overrides: Any) -> CurrentVersionCensusContract:
    resource_types = SERVER_ISSUED_SUBSET_RESOURCE_TYPES
    contract_value_by_field = {
        "source_id": "synthetic-source",
        "cutoff": CUTOFF,
        "resources": resource_types,
        "expected_nonempty_resources": resource_types,
        "start_urls": tuple(
            (
                resource_type,
                f"https://directory.example.test/fhir/{resource_type}",
            )
            for resource_type in resource_types
        ),
        "continuation_strategy": (
            SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY
        ),
        "strategy_version": SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
        "contract_version": 3,
        "semantics": SERVER_ISSUED_SUBSET_SEMANTICS,
        "page_count": PAGE_COUNT,
        "traversal_version": SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
        "canonicalization_version": (
            SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION
        ),
        "completion_scopes": SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
        "campaign_id": "synthetic-reviewed-subset-v3",
    }
    contract_value_by_field.update(overrides)
    return CurrentVersionCensusContract(**contract_value_by_field)


def build_execution_proof(
    *,
    hop_prefix: str = "1",
    shape_prefixes: tuple[str, str] = ("a", "b"),
) -> dict[str, Any]:
    alternate_hop_prefix = "2" if hop_prefix != "2" else "3"
    hop_hashes = [hop_prefix * 64, alternate_hop_prefix * 64]
    shape_hashes = [prefix * 64 for prefix in shape_prefixes]
    return {
        "verified": True,
        "advertised_pre": 2,
        "advertised_post": 2,
        "returned_unique": 1,
        "deficit": 1,
        "terminal_reason": "source_no_next",
        "page_entry_counts": [1, 0, 0],
        "continuation_hop_sha256": hop_hashes,
        "continuation_shape_sha256": shape_hashes,
        "terminal_page_geometry": {
            "version": 2,
            "page_count": PAGE_COUNT,
            "pages_processed": 3,
            "processed_rows": 1,
            "terminal_page_start_offset": 500,
            "logical_window_end_offset": 750,
            "terminal_page_entries": 0,
            "sparse_pages": 3,
            "empty_pages": 2,
        },
    }


def build_proof_pair(
    *,
    hop_prefix: str = "1",
) -> tuple[dict[str, Any], str, dict[str, dict[str, Any]]]:
    execution_proof_by_type = {
        resource_type: build_execution_proof(hop_prefix=hop_prefix)
        for resource_type in SERVER_ISSUED_SUBSET_RESOURCE_TYPES
    }
    resource_hash_by_type = dict.fromkeys(
        SERVER_ISSUED_SUBSET_RESOURCE_TYPES,
        "c" * 64,
    )
    acquired_hash_by_type = dict.fromkeys(
        SERVER_ISSUED_SUBSET_RESOURCE_TYPES,
        "d" * 64,
    )
    resource_count_by_type = dict.fromkeys(
        SERVER_ISSUED_SUBSET_RESOURCE_TYPES,
        1,
    )
    completion_proof, proof_sha256 = build_subset_completion_proof(
        contract=build_subset_contract(),
        resource_proof_by_type=execution_proof_by_type,
        dataset_hash="e" * 64,
        resource_count=len(SERVER_ISSUED_SUBSET_RESOURCE_TYPES),
        resource_hash_by_type=resource_hash_by_type,
        acquired_resource_hash_by_type=acquired_hash_by_type,
        resource_count_by_type=resource_count_by_type,
    )
    return completion_proof, proof_sha256, execution_proof_by_type


def build_completed_execution_proof(
    contract: CurrentVersionCensusContract,
    resource_type: str,
) -> dict[str, Any]:
    execution_proof = current_version_census_initial_proof(
        contract,
        resource_type,
        2,
        expected_page_count=PAGE_COUNT,
    )
    execution_proof = current_version_census_checkpoint_proof(
        execution_proof,
        pages_processed=1,
        rows_processed=1,
        page_entry_count=1,
        expected_page_count=PAGE_COUNT,
        continuation_identity_sha256="1" * 64,
        continuation_shape_sha256="a" * 64,
    )
    execution_proof = current_version_census_checkpoint_proof(
        execution_proof,
        pages_processed=2,
        rows_processed=1,
        page_entry_count=0,
        expected_page_count=PAGE_COUNT,
        continuation_identity_sha256="2" * 64,
        continuation_shape_sha256="b" * 64,
    )
    return current_version_census_completed_proof(
        execution_proof,
        post_count=2,
        processed_rows=1,
        unique_candidate_rows=1,
        pages_processed=3,
        expected_page_count=PAGE_COUNT,
        terminal_page_entry_count=0,
    )


def build_dataset_candidate(
    contract: CurrentVersionCensusContract,
    *,
    root_run_id: str = "synthetic-root",
) -> importer.EndpointDatasetCandidate:
    return importer.EndpointDatasetCandidate(
        endpoint_id="synthetic-endpoint",
        dataset_id=f"synthetic-dataset-{root_run_id}",
        acquisition_root_run_id=root_run_id,
        source_ids=("synthetic-source",),
        selected_resources=tuple(sorted(contract.resources)),
        expected_resources=tuple(sorted(contract.resources)),
        import_run_id="synthetic-run",
        previous_dataset_id=None,
        requires_twin_root_verification=True,
        verification_campaign_id=contract.campaign_id,
        verification_source_scope_hash="f" * 64,
        verification_role=importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE,
        completion_proof_required_version=3,
        subset_contract=contract,
    )


def build_transport_coordinate_rows() -> tuple[Any, dict, dict, dict]:
    raw_resource_by_field = {
        "resourceType": "Organization",
        "id": "organization-a",
        "name": "Synthetic Network",
    }
    model, parsed_row_by_field = importer.parse_fhir_resource(
        "synthetic-source",
        raw_resource_by_field,
    )
    acquired_sha256 = canonical_sha256(raw_resource_by_field)
    first_page_row_by_field = {
        **parsed_row_by_field,
        "resource_url": "https://directory.example.test/fhir/Organization/a",
        "fhir_self_url": "https://directory.example.test/fhir/Organization/a",
        "fhir_fetch_url": "https://directory.example.test/fhir/Organization",
        "fhir_fetch_mode": "rest_bundle",
        "_acquired_resource_sha256": acquired_sha256,
    }
    continuation_row_by_field = {
        **first_page_row_by_field,
        "resource_url": "https://directory.example.test/fhir/a",
        "fhir_self_url": "https://directory.example.test/fhir/a",
        "fhir_fetch_url": "https://directory.example.test/fhir",
    }
    changed_row_by_field = {
        **continuation_row_by_field,
        "name": "Changed Synthetic Network",
        "_acquired_resource_sha256": canonical_sha256(
            {**raw_resource_by_field, "name": "Changed Synthetic Network"}
        ),
    }
    return (
        model,
        first_page_row_by_field,
        continuation_row_by_field,
        changed_row_by_field,
    )


def _completed_resource_diagnostic(
    contract: CurrentVersionCensusContract,
    resource_type: str,
) -> dict[str, Any]:
    execution_proof = build_completed_execution_proof(contract, resource_type)
    return {
        "fetch_mode": SERVER_ISSUED_SUBSET_FETCH_MODE,
        "rows_fetched": 1,
        "pages_fetched": 3,
        "server_issued_subset_completeness": (
            importer._sanitized_server_issued_subset_execution_proof(
                execution_proof
            )
        ),
        importer._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY: (
            importer._server_issued_subset_internal_replay_evidence(
                execution_proof
            )
        ),
    }


def build_finalization_inputs() -> tuple[Any, Any, dict, Any]:
    contract = build_subset_contract()
    candidate = build_dataset_candidate(contract)
    diagnostic_by_type = {
        resource_type: _completed_resource_diagnostic(contract, resource_type)
        for resource_type in contract.resources
    }
    content_proof = importer.EndpointDatasetContentProof(
        dataset_hash="e" * 64,
        resource_count=len(contract.resources),
        resource_hashes=dict.fromkeys(contract.resources, "c" * 64),
        resource_counts=dict.fromkeys(contract.resources, 1),
        acquired_resource_hashes=dict.fromkeys(contract.resources, "d" * 64),
    )
    return contract, candidate, diagnostic_by_type, content_proof


def build_coverage_inputs() -> tuple[Any, ...]:
    completion_proof, proof_sha256, execution_proof_by_type = build_proof_pair()
    contract = build_subset_contract()
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="synthetic-endpoint",
        dataset_id="synthetic-dataset",
        acquisition_root_run_id="synthetic-root",
        source_ids=("synthetic-source",),
        selected_resources=tuple(sorted(contract.resources)),
        expected_resources=tuple(sorted(contract.resources)),
        import_run_id="synthetic-run",
        previous_dataset_id=None,
        completion_proof_required_version=3,
        subset_contract=contract,
    )
    diagnostic_by_type = {
        resource_type: {
            "server_issued_subset_completeness": {
                **execution_proof_by_type[resource_type],
                "cutoff": CUTOFF,
            }
        }
        for resource_type in contract.resources
    }
    content_proof = importer.EndpointDatasetContentProof(
        dataset_hash=completion_proof["dataset"]["hash"],
        resource_count=completion_proof["dataset"]["count"],
        resource_hashes=completion_proof["dataset"]["resource_hashes"],
        resource_counts=completion_proof["dataset"]["resource_counts"],
        completion_proof=completion_proof,
        completion_proof_sha256=proof_sha256,
    )
    verification_metadata_by_field = {
        importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
            "result": "baseline_recorded"
        }
    }
    relation_proof_by_type = {
        importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY: {
            "unresolved_reference_count": 2
        },
        importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY: {
            "unresolved_reference_count": 3
        },
    }
    return (
        completion_proof,
        proof_sha256,
        candidate,
        diagnostic_by_type,
        content_proof,
        relation_proof_by_type,
        verification_metadata_by_field,
    )
