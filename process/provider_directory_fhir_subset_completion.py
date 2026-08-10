# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical completion and sealed replay proofs for reviewed FHIR subsets."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
    SERVER_ISSUED_SUBSET_SEMANTICS,
    SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
)
from process.provider_directory_fhir_subset_canonical import (
    ALLOWED_SUBSET_RESOURCE_TYPES,
    SERVER_ISSUED_SUBSET_COMPLETION_PROOF_VERSION,
    SERVER_ISSUED_SUBSET_REQUIRED_VERSION,
    SERVER_ISSUED_SUBSET_TERMINAL_REASON,
    _is_nonnegative_int,
    _is_sha256,
    canonical_payload_sha256,
    canonical_sha256,
    validate_subset_completion_proof_pair,
)
from process.provider_directory_fhir_subset_replay import (
    SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_VERSION,
    build_subset_replay_evidence,
    validate_subset_replay_evidence_pair,
)


__all__ = (
    "ALLOWED_SUBSET_RESOURCE_TYPES",
    "SERVER_ISSUED_SUBSET_COMPLETION_PROOF_VERSION",
    "SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_VERSION",
    "SERVER_ISSUED_SUBSET_REQUIRED_VERSION",
    "SERVER_ISSUED_SUBSET_TERMINAL_REASON",
    "build_subset_completion_proof",
    "build_subset_replay_evidence",
    "canonical_payload_sha256",
    "canonical_sha256",
    "validate_subset_completion_proof_pair",
    "validate_subset_replay_evidence_pair",
)


def _geometry_from_execution_proof(
    resource_proof: Mapping[str, Any],
) -> dict[str, Any]:
    geometry = resource_proof.get("terminal_page_geometry")
    required_fields = {
        "pages_processed",
        "version",
        "page_count",
        "processed_rows",
        "terminal_page_start_offset",
        "logical_window_end_offset",
        "terminal_page_entries",
        "sparse_pages",
        "empty_pages",
    }
    if not isinstance(geometry, Mapping) or not required_fields.issubset(geometry):
        raise ValueError("provider_directory_subset_completion_geometry_invalid")
    geometry_by_field = {
        field_name: geometry[field_name]
        for field_name in sorted(required_fields)
    }
    if any(
        not _is_nonnegative_int(geometry_field_value)
        for geometry_field_value in geometry_by_field.values()
    ):
        raise ValueError("provider_directory_subset_completion_geometry_invalid")
    page_entry_counts = resource_proof.get("page_entry_counts")
    if (
        geometry_by_field["pages_processed"] <= 0
        or geometry_by_field["page_count"] <= 0
        or type(page_entry_counts) is not list
        or not page_entry_counts
        or len(page_entry_counts) != geometry_by_field["pages_processed"]
        or any(
            type(entry_count) is not int
            or not 0 <= entry_count <= geometry_by_field["page_count"]
            for entry_count in page_entry_counts
        )
        or sum(page_entry_counts) != geometry_by_field["processed_rows"]
        or page_entry_counts[-1] != geometry_by_field["terminal_page_entries"]
    ):
        raise ValueError("provider_directory_subset_completion_geometry_invalid")
    geometry_by_field["page_entry_counts"] = list(page_entry_counts)
    return geometry_by_field


def _validated_resource_metrics(
    resource_proof: Mapping[str, Any],
    geometry_by_field: Mapping[str, Any],
    content_sha256: str,
    acquired_content_sha256: str,
    max_advertised_count_decrease: int,
) -> tuple[int, int, int, int, list[str]]:
    advertised_pre = resource_proof.get("advertised_pre")
    advertised_post = resource_proof.get("advertised_post")
    returned_unique = resource_proof.get("returned_unique")
    deficit = resource_proof.get("deficit")
    continuation_shape_hashes = resource_proof.get(
        "continuation_shape_sha256"
    )
    advertised_count_decrease = (
        advertised_pre - advertised_post
        if _is_nonnegative_int(advertised_pre)
        and _is_nonnegative_int(advertised_post)
        else None
    )
    if (
        any(
            not _is_nonnegative_int(metric)
            for metric in (
                advertised_pre,
                advertised_post,
                returned_unique,
                deficit,
            )
        )
        or advertised_count_decrease is None
        or not 0 <= advertised_count_decrease <= max_advertised_count_decrease
        or returned_unique > advertised_post
        or deficit != advertised_pre - returned_unique
        or resource_proof.get("terminal_reason")
        != SERVER_ISSUED_SUBSET_TERMINAL_REASON
        or resource_proof.get("verified") is not True
        or not _is_sha256(content_sha256)
        or not _is_sha256(acquired_content_sha256)
        or type(continuation_shape_hashes) is not list
        or len(continuation_shape_hashes)
        != geometry_by_field["pages_processed"] - 1
        or any(
            not _is_sha256(shape_digest)
            for shape_digest in continuation_shape_hashes
        )
    ):
        raise ValueError("provider_directory_subset_completion_counts_invalid")
    return (
        advertised_pre,
        advertised_post,
        returned_unique,
        deficit,
        list(continuation_shape_hashes),
    )


def _canonical_resource_proof(
    resource_proof: Mapping[str, Any],
    *,
    content_sha256: str,
    acquired_content_sha256: str,
    max_advertised_count_decrease: int,
) -> dict[str, Any]:
    geometry_by_field = _geometry_from_execution_proof(resource_proof)
    (
        advertised_pre,
        advertised_post,
        returned_unique,
        deficit,
        continuation_shape_hashes,
    ) = _validated_resource_metrics(
        resource_proof,
        geometry_by_field,
        content_sha256,
        acquired_content_sha256,
        max_advertised_count_decrease,
    )
    return {
        "advertised_pre": advertised_pre,
        "advertised_post": advertised_post,
        "returned_unique": returned_unique,
        "deficit": deficit,
        "geometry_version": geometry_by_field["version"],
        "page_count": geometry_by_field["page_count"],
        "pages": geometry_by_field["pages_processed"],
        "processed_rows": geometry_by_field["processed_rows"],
        "page_entry_counts": geometry_by_field["page_entry_counts"],
        "continuation_shape_sha256": continuation_shape_hashes,
        "continuation_shape_chain_sha256": canonical_sha256(
            continuation_shape_hashes
        ),
        "logical_terminal_offset": geometry_by_field["terminal_page_start_offset"],
        "logical_window_end_offset": geometry_by_field["logical_window_end_offset"],
        "terminal_entries": geometry_by_field["terminal_page_entries"],
        "sparse_pages": geometry_by_field["sparse_pages"],
        "empty_pages": geometry_by_field["empty_pages"],
        "geometry_sha256": canonical_sha256(geometry_by_field),
        "terminal_reason": SERVER_ISSUED_SUBSET_TERMINAL_REASON,
        "content_sha256": content_sha256,
        "acquired_content_sha256": acquired_content_sha256,
    }


def _validate_completion_inputs(
    contract: CurrentVersionCensusContract,
    resource_proof_by_type: Mapping[str, Mapping[str, Any]],
    dataset_hash: str,
    resource_count: int,
    resource_hash_by_type: Mapping[str, str],
    acquired_resource_hash_by_type: Mapping[str, str],
    resource_count_by_type: Mapping[str, int],
) -> set[str]:
    resource_types = set(resource_proof_by_type)
    if (
        not contract.is_server_issued_subset_v3
        or resource_types != set(contract.resources)
        or resource_types != set(resource_hash_by_type)
        or resource_types != set(resource_count_by_type)
        or resource_types != set(acquired_resource_hash_by_type)
        or resource_types != ALLOWED_SUBSET_RESOURCE_TYPES
        or not _is_sha256(dataset_hash)
        or not _is_nonnegative_int(resource_count)
        or any(
            not _is_nonnegative_int(count)
            for count in resource_count_by_type.values()
        )
        or sum(resource_count_by_type.values()) != resource_count
    ):
        raise ValueError("provider_directory_subset_completion_resources_invalid")
    return resource_types


def _completion_proof_by_field(
    contract: CurrentVersionCensusContract,
    canonical_resource_by_type: Mapping[str, Mapping[str, Any]],
    dataset_hash: str,
    resource_count: int,
    resource_hash_by_type: Mapping[str, str],
    acquired_resource_hash_by_type: Mapping[str, str],
    resource_count_by_type: Mapping[str, int],
) -> dict[str, Any]:
    return {
        "proof_version": SERVER_ISSUED_SUBSET_COMPLETION_PROOF_VERSION,
        "contract_version": SERVER_ISSUED_SUBSET_REQUIRED_VERSION,
        "semantics": SERVER_ISSUED_SUBSET_SEMANTICS,
        "strategy_version": contract.strategy_version,
        "traversal_version": SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
        "canonicalization_version": SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
        "completion_scopes": list(contract.completion_scopes),
        "campaign_id": contract.campaign_id,
        "cutoff": contract.cutoff,
        "page_count": contract.page_count,
        "resources": dict(canonical_resource_by_type),
        "dataset": {
            "hash": dataset_hash,
            "count": resource_count,
            "resource_hashes": dict(sorted(resource_hash_by_type.items())),
            "resource_counts": dict(sorted(resource_count_by_type.items())),
            "acquired_resource_hashes": dict(
                sorted(acquired_resource_hash_by_type.items())
            ),
        },
    }


def build_subset_completion_proof(
    *,
    contract: CurrentVersionCensusContract,
    resource_proof_by_type: Mapping[str, Mapping[str, Any]],
    dataset_hash: str,
    resource_count: int,
    resource_hash_by_type: Mapping[str, str],
    acquired_resource_hash_by_type: Mapping[str, str],
    resource_count_by_type: Mapping[str, int],
) -> tuple[dict[str, Any], str]:
    """Build and self-validate one root-neutral subset completion proof."""

    resource_types = _validate_completion_inputs(
        contract,
        resource_proof_by_type,
        dataset_hash,
        resource_count,
        resource_hash_by_type,
        acquired_resource_hash_by_type,
        resource_count_by_type,
    )
    max_advertised_count_decrease = contract.max_advertised_count_decrease
    canonical_resource_by_type = {
        resource_type: _canonical_resource_proof(
            resource_proof_by_type[resource_type],
            content_sha256=resource_hash_by_type[resource_type],
            acquired_content_sha256=acquired_resource_hash_by_type[resource_type],
            max_advertised_count_decrease=max_advertised_count_decrease,
        )
        for resource_type in sorted(resource_types)
    }
    if any(
        canonical_resource_by_type[resource_type]["returned_unique"]
        != resource_count_by_type[resource_type]
        for resource_type in resource_types
    ):
        raise ValueError("provider_directory_subset_completion_dataset_invalid")
    completion_proof_by_field = _completion_proof_by_field(
        contract,
        canonical_resource_by_type,
        dataset_hash,
        resource_count,
        resource_hash_by_type,
        acquired_resource_hash_by_type,
        resource_count_by_type,
    )
    completion_sha256 = canonical_sha256(completion_proof_by_field)
    validate_subset_completion_proof_pair(
        completion_proof_by_field,
        completion_sha256,
    )
    return completion_proof_by_field, completion_sha256
