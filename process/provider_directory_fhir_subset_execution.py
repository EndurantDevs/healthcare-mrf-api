# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Ordered terminal execution fields for server-issued subset traversal."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_fhir_subset_identity import (
    reviewed_subset_max_advertised_count_decrease,
)


def reviewed_subset_count_decrease_from_proof(
    initial_proof: Mapping[str, Any],
) -> int:
    """Return the exact allowlisted count-decrease bound in a reviewed proof."""

    raw_completion_scopes = initial_proof.get("completion_scopes")
    maximum = reviewed_subset_max_advertised_count_decrease(
        initial_proof.get("strategy_version"),
        (
            tuple(raw_completion_scopes)
            if type(raw_completion_scopes) is list
            else None
        ),
    )
    if maximum is None:
        raise ValueError(
            "provider_directory_current_version_census_profile_invalid"
        )
    return maximum


def has_valid_reviewed_subset_counts(
    count_by_name: Mapping[str, Any],
    max_advertised_count_decrease: int,
) -> bool:
    """Validate one allowlisted subset profile's terminal counts."""

    required_count_by_name = {
        field_name: count_by_name.get(field_name)
        for field_name in (
            "pre_count",
            "post_count",
            "processed_rows",
            "unique_candidate_rows",
        )
    }
    if not all(
        type(count) is int and count >= 0
        for count in required_count_by_name.values()
    ):
        return False
    advertised_count_decrease = (
        required_count_by_name["pre_count"]
        - required_count_by_name["post_count"]
    )
    return bool(
        0 <= advertised_count_decrease <= max_advertised_count_decrease
        and count_by_name["processed_rows"]
        == count_by_name["unique_candidate_rows"]
        and count_by_name["unique_candidate_rows"] <= count_by_name["post_count"]
    )


def subset_completed_fields(
    initial_proof: Mapping[str, Any],
    *,
    pre_count: int,
    post_count: int,
    unique_candidate_rows: int,
    pages_processed: int,
    terminal_page_entry_count: int,
) -> dict[str, Any]:
    """Return exact v3 counters and append the terminal page geometry."""

    prior_entry_counts = initial_proof.get("page_entry_counts")
    if (
        type(prior_entry_counts) is not list
        or len(prior_entry_counts) != pages_processed - 1
    ):
        raise ValueError(
            "provider_directory_current_version_census_page_geometry_invalid"
        )
    return {
        "advertised_pre": pre_count,
        "advertised_post": post_count,
        "returned_unique": unique_candidate_rows,
        "deficit": max(pre_count - unique_candidate_rows, 0),
        "terminal_reason": "source_no_next",
        "page_entry_counts": [
            *prior_entry_counts,
            terminal_page_entry_count,
        ],
    }


def _is_valid_sha_vector(sha_values: Any, expected_count: int) -> bool:
    return bool(
        type(sha_values) is list
        and len(sha_values) == expected_count
        and all(
            type(sha_value) is str
            and len(sha_value) == 64
            and all(
                character in "0123456789abcdef"
                for character in sha_value
            )
            for sha_value in sha_values
        )
    )


def _is_valid_sha_value(sha_value: Any) -> bool:
    return bool(
        type(sha_value) is str
        and len(sha_value) == 64
        and all(character in "0123456789abcdef" for character in sha_value)
    )


def _is_valid_page_sequence(
    page_entry_counts: Any,
    terminal_geometry: Mapping[str, Any],
    page_count: int,
    processed_rows: int,
) -> bool:
    terminal_pages = terminal_geometry.get("pages_processed")
    terminal_entries = terminal_geometry.get("terminal_page_entries")
    sparse_pages = terminal_geometry.get("sparse_pages")
    empty_pages = terminal_geometry.get("empty_pages")
    return bool(
        type(page_entry_counts) is list
        and page_entry_counts
        and type(terminal_pages) is int
        and terminal_pages > 0
        and type(terminal_entries) is int
        and type(sparse_pages) is int
        and type(empty_pages) is int
        and len(page_entry_counts) == terminal_pages
        and all(
            type(entry_count) is int and 0 <= entry_count <= page_count
            for entry_count in page_entry_counts
        )
        and sum(page_entry_counts) == processed_rows
        and page_entry_counts[-1] == terminal_entries
        and sum(entry_count < page_count for entry_count in page_entry_counts)
        == sparse_pages
        and page_entry_counts.count(0) == empty_pages
    )


def has_valid_subset_completed_fields(
    completeness: Mapping[str, Any],
    count_by_name: Mapping[str, Any],
    page_count: int,
) -> bool:
    """Validate ordered v3 terminal counters and both continuation chains."""

    terminal_geometry = completeness.get("terminal_page_geometry")
    if not isinstance(terminal_geometry, Mapping):
        return False
    terminal_pages = terminal_geometry.get("pages_processed")
    if type(terminal_pages) is not int or terminal_pages <= 0:
        return False
    return bool(
        completeness.get("advertised_pre") == count_by_name["pre_count"]
        and completeness.get("advertised_post") == count_by_name["post_count"]
        and completeness.get("returned_unique")
        == count_by_name["unique_candidate_rows"]
        and completeness.get("deficit")
        == count_by_name["pre_count"]
        - count_by_name["unique_candidate_rows"]
        and completeness.get("terminal_reason") == "source_no_next"
        and _is_valid_page_sequence(
            completeness.get("page_entry_counts"),
            terminal_geometry,
            page_count,
            count_by_name["processed_rows"],
        )
        and _is_valid_sha_vector(
            completeness.get("continuation_hop_sha256"),
            terminal_pages - 1,
        )
        and _is_valid_sha_vector(
            completeness.get("continuation_shape_sha256"),
            terminal_pages - 1,
        )
    )


def _has_valid_checkpoint_entries(
    page_entry_counts: Any,
    geometry_by_field: Mapping[str, int],
    *,
    pages_processed: int,
    rows_processed: int,
    expected_page_count: int,
    includes_terminal: bool,
) -> bool:
    expected_length = pages_processed + int(includes_terminal)
    checkpoint_counts = (
        page_entry_counts[:pages_processed]
        if type(page_entry_counts) is list
        else []
    )
    return bool(
        type(page_entry_counts) is list
        and len(page_entry_counts) == expected_length
        and all(
            type(entry_count) is int
            and 0 <= entry_count <= expected_page_count
            for entry_count in page_entry_counts
        )
        and sum(checkpoint_counts) == rows_processed
        and sum(
            entry_count < expected_page_count
            for entry_count in checkpoint_counts
        )
        == geometry_by_field["sparse_pages"]
        and checkpoint_counts.count(0) == geometry_by_field["empty_pages"]
    )


def validate_subset_checkpoint_sequences(
    proof_by_field: Mapping[str, Any],
    geometry_by_field: Mapping[str, int],
    *,
    pages_processed: int,
    rows_processed: int,
    expected_page_count: int,
) -> None:
    """Bind ordered page and source-issued hop evidence before transport."""

    if proof_by_field.get("contract_version") != 3:
        return
    terminal_geometry = proof_by_field.get("terminal_page_geometry")
    includes_terminal = bool(
        isinstance(terminal_geometry, Mapping)
        and terminal_geometry.get("pages_processed") == pages_processed + 1
    )
    is_valid = bool(
        _has_valid_checkpoint_entries(
            proof_by_field.get("page_entry_counts"),
            geometry_by_field,
            pages_processed=pages_processed,
            rows_processed=rows_processed,
            expected_page_count=expected_page_count,
            includes_terminal=includes_terminal,
        )
        and _is_valid_sha_vector(
            proof_by_field.get("continuation_hop_sha256"),
            pages_processed,
        )
        and _is_valid_sha_vector(
            proof_by_field.get("continuation_shape_sha256"),
            pages_processed,
        )
    )
    if not is_valid:
        raise ValueError(
            "provider_directory_current_version_census_page_geometry_invalid"
        )


def append_subset_checkpoint_evidence(
    proof_by_field: Mapping[str, Any],
    updated_proof_by_field: dict[str, Any],
    *,
    pages_processed: int,
    page_entry_count: int,
    expected_page_count: int,
    continuation_identity_sha256: str | None,
    continuation_shape_sha256: str | None,
) -> dict[str, Any]:
    """Append one page count and both validated continuation commitments."""

    if proof_by_field.get("contract_version") != 3:
        return updated_proof_by_field
    prior_entry_counts = proof_by_field.get("page_entry_counts")
    prior_hop_hashes = proof_by_field.get("continuation_hop_sha256")
    prior_shape_hashes = proof_by_field.get("continuation_shape_sha256")
    has_valid_prior = bool(
        type(prior_entry_counts) is list
        and len(prior_entry_counts) == pages_processed - 1
        and all(
            type(entry_count) is int
            and 0 <= entry_count <= expected_page_count
            for entry_count in prior_entry_counts
        )
        and _is_valid_sha_vector(prior_hop_hashes, pages_processed - 1)
        and _is_valid_sha_value(continuation_identity_sha256)
        and _is_valid_sha_vector(prior_shape_hashes, pages_processed - 1)
        and _is_valid_sha_value(continuation_shape_sha256)
    )
    if not has_valid_prior:
        raise ValueError(
            "provider_directory_current_version_census_page_geometry_invalid"
        )
    updated_proof_by_field["page_entry_counts"] = [
        *prior_entry_counts,
        page_entry_count,
    ]
    updated_proof_by_field["continuation_hop_sha256"] = [
        *prior_hop_hashes,
        continuation_identity_sha256,
    ]
    updated_proof_by_field["continuation_shape_sha256"] = [
        *prior_shape_hashes,
        continuation_shape_sha256,
    ]
    return updated_proof_by_field
