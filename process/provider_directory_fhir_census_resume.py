# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Small resume-state identity helpers for reviewed FHIR traversal."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)


def validated_initial_resume_url(
    contract: CurrentVersionCensusContract,
    resource_type: str,
    start_url: str,
    next_url: str | None,
    *,
    expected_page_count: int,
    pages_processed: int,
) -> str | None:
    """Validate the reviewed start and return it for an initial checkpoint."""

    if start_url != contract.start_url(resource_type, expected_page_count):
        raise ValueError(
            "provider_directory_current_version_census_resume_start_url_invalid"
        )
    if pages_processed != 0:
        return None
    if next_url != start_url:
        raise ValueError(
            "provider_directory_current_version_census_resume_url_invalid"
        )
    return start_url


def resume_prior_page_entry_count(
    contract: CurrentVersionCensusContract,
    proof_by_field: Mapping[str, Any],
) -> int:
    """Return the last persisted page count after v3 shape validation."""

    page_entry_counts = proof_by_field.get("page_entry_counts")
    if contract.is_server_issued_subset_v3 and (
        type(page_entry_counts) is not list or not page_entry_counts
    ):
        raise ValueError(
            "provider_directory_current_version_census_resume_identity_invalid"
        )
    return (
        page_entry_counts[-1]
        if type(page_entry_counts) is list and page_entry_counts
        else 0
    )


def validate_resume_identity_evidence(
    contract: CurrentVersionCensusContract,
    proof_by_field: Mapping[str, Any],
    continuation_identity: str,
    continuation_shape_identity: str,
) -> None:
    """Bind a resumed URL to its token-specific and neutral commitments."""

    hop_hashes = proof_by_field.get("continuation_hop_sha256")
    shape_hashes = proof_by_field.get("continuation_shape_sha256")
    if contract.is_server_issued_subset_v3 and (
        type(hop_hashes) is not list
        or not hop_hashes
        or hop_hashes[-1] != continuation_identity
        or type(shape_hashes) is not list
        or not shape_hashes
        or shape_hashes[-1] != continuation_shape_identity
    ):
        raise ValueError(
            "provider_directory_current_version_census_resume_identity_invalid"
        )
