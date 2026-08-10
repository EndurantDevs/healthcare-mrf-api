# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Resource proof checks for the reviewed mixed-terminal disposition."""

from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_BLOCKED_ERROR,
    CURRENT_VERSION_CENSUS_RETRYABLE_ERROR,
    SERVER_ISSUED_SUBSET_FETCH_MODE,
)
from process.provider_directory_fhir_census_page_geometry import (
    current_version_census_terminal_page_geometry,
    validate_current_version_census_checkpoint_geometry,
)
from process.provider_directory_fhir_subset_execution import (
    has_valid_reviewed_subset_counts,
    has_valid_subset_completed_fields,
)
from process.provider_directory_fhir_subset_identity import (
    SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
    SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
    SERVER_ISSUED_SUBSET_SEMANTICS,
    SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    COUNT_DRIFT_DISPOSITION,
    EXPECTED_RESOURCE_TYPES,
    RETRYABLE_HTTP_500_DISPOSITION,
    STABLE_COMPLETE_DISPOSITION,
    ReviewedSubsetTerminalDispositionError,
    canonical_evidence_sha256,
)
from process.provider_directory_fhir_subset_terminal_disposition_shapes import (
    validate_disposition_diagnostic_shape,
    validate_disposition_proof_shapes,
)
from process.provider_directory_fhir_subset_terminal_disposition_util import (
    clean_text,
    json_object,
)


_COMPLETED_COUNT_FIELDS = (
    "pre_count",
    "post_count",
    "processed_rows",
    "unique_candidate_rows",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _proof_identity(
    proof: Mapping[str, Any],
    resource_type: str,
) -> tuple[Any, ...]:
    page_count = proof.get("page_count")
    identity = (
        proof.get("cutoff"),
        proof.get("contract_identity"),
        page_count,
        proof.get("campaign_id"),
    )
    if (
        proof.get("contract_version") != 3
        or proof.get("strategy_version")
        != SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION
        or proof.get("semantics") != SERVER_ISSUED_SUBSET_SEMANTICS
        or proof.get("traversal_version")
        != SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION
        or proof.get("canonicalization_version")
        != SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION
        or proof.get("completion_scopes")
        != list(SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES)
        or proof.get("resource_type") != resource_type
        or clean_text(identity[0]) is None
        or clean_text(identity[1]) is None
        or _SHA256.fullmatch(identity[1]) is None
        or type(page_count) is not int
        or not 1 <= page_count <= 1000
        or clean_text(identity[3]) is None
        or type(proof.get("pre_count")) is not int
        or proof["pre_count"] < 0
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return identity


def _safe_checkpoint_proof(checkpoint_proof: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field_name: field_value
        for field_name, field_value in checkpoint_proof.items()
        if field_name != "continuation_hop_sha256"
    }


def expected_subset_coverage(proof_by_field: Mapping[str, Any]) -> dict[str, Any]:
    """Rebuild the exact safe coverage projection stored by the importer."""

    geometry = proof_by_field.get("terminal_page_geometry")
    continuation_shape_sha256 = proof_by_field.get(
        "continuation_shape_sha256"
    )
    return {
        "cutoff": proof_by_field.get("cutoff"),
        "scope": "server_issued_traversal_subset",
        "advertised_pre": proof_by_field.get("advertised_pre"),
        "advertised_post": proof_by_field.get("advertised_post"),
        "returned_unique": proof_by_field.get("returned_unique"),
        "deficit": proof_by_field.get("deficit"),
        "geometry": (
            {
                "pages": geometry.get("pages_processed"),
                "logical_terminal_offset": geometry.get(
                    "terminal_page_start_offset"
                ),
                "sparse_pages": geometry.get("sparse_pages"),
                "empty_pages": geometry.get("empty_pages"),
                "page_entry_counts_sha256": canonical_evidence_sha256(
                    proof_by_field.get("page_entry_counts")
                ),
                "geometry_sha256": canonical_evidence_sha256(
                    {
                        **dict(geometry),
                        "page_entry_counts": proof_by_field.get(
                            "page_entry_counts"
                        ),
                    }
                ),
            }
            if isinstance(geometry, Mapping)
            else None
        ),
        "continuation": (
            {
                "validated_hops": len(continuation_shape_sha256),
                "chain_sha256": canonical_evidence_sha256(
                    continuation_shape_sha256
                ),
            }
            if type(continuation_shape_sha256) is list
            else None
        ),
        "twin_state": "pending_matching_reviewed_root",
        "proof_state": (
            "resource_terminal_verified"
            if proof_by_field.get("verified") is True
            else "not_verified"
        ),
        "unresolved_reference_count": None,
        "absence_semantics": "unknown_under_subset",
    }


def _checkpoint_hash_commitments(
    checkpoint: Mapping[str, Any],
    diagnostic: Mapping[str, Any],
    expected_start_url_sha256: str,
) -> tuple[str, str]:
    start_url_sha256 = checkpoint.get("start_url_hash")
    recent_cursor_hashes = checkpoint.get("recent_cursor_hashes")
    checkpoint_pages = checkpoint.get("pages_processed")
    stable_terminal_page_count = int(diagnostic.get("complete") is True)
    history_length = (
        checkpoint_pages - stable_terminal_page_count
        if type(checkpoint_pages) is int
        else -1
    )
    expected_recent_count = min(history_length, 64)
    if (
        type(start_url_sha256) is not str
        or _SHA256.fullmatch(start_url_sha256) is None
        or start_url_sha256 != expected_start_url_sha256
        or type(recent_cursor_hashes) is not list
        or history_length < 0
        or len(recent_cursor_hashes) != expected_recent_count
        or any(
            type(cursor_sha256) is not str
            or _SHA256.fullmatch(cursor_sha256) is None
            for cursor_sha256 in recent_cursor_hashes
        )
        or len(set(recent_cursor_hashes)) != len(recent_cursor_hashes)
        or (
            0 < history_length <= 64
            and recent_cursor_hashes[0] != start_url_sha256
        )
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return (
        start_url_sha256,
        canonical_evidence_sha256(recent_cursor_hashes),
    )


def _completed_counts(proof: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field_name: proof.get(field_name)
        for field_name in _COMPLETED_COUNT_FIELDS
    }


def _validate_terminal_sequence(
    proof: Mapping[str, Any],
    checkpoint_pages: int,
    checkpoint_rows: int,
    *,
    terminal_checkpointed: bool,
) -> None:
    terminal_geometry = proof.get("terminal_page_geometry")
    page_entry_counts = proof.get("page_entry_counts")
    expected_terminal_pages = checkpoint_pages + int(not terminal_checkpointed)
    if (
        not isinstance(terminal_geometry, Mapping)
        or terminal_geometry.get("pages_processed") != expected_terminal_pages
        or type(page_entry_counts) is not list
        or not page_entry_counts
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    terminal_pages = terminal_geometry["pages_processed"]
    terminal_page_entries = page_entry_counts[-1]
    prior_rows = sum(page_entry_counts[:-1])
    expected_prior_rows = (
        checkpoint_rows - page_entry_counts[-1]
        if terminal_checkpointed
        else checkpoint_rows
    )
    if prior_rows != expected_prior_rows:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    try:
        expected_terminal_geometry = current_version_census_terminal_page_geometry(
            proof,
            pages_processed=terminal_pages,
            processed_rows=proof["processed_rows"],
            expected_page_count=proof["page_count"],
            terminal_page_entry_count=terminal_page_entries,
        )
        validate_current_version_census_checkpoint_geometry(
            proof,
            pages_processed=terminal_pages - 1,
            rows_processed=prior_rows,
            expected_page_count=proof["page_count"],
        )
    except ValueError:
        raise ReviewedSubsetTerminalDispositionError("evidence") from None
    if expected_terminal_geometry != dict(terminal_geometry):
        raise ReviewedSubsetTerminalDispositionError("evidence")


def _completed_or_drift_disposition(
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    proof: Mapping[str, Any],
) -> str:
    counts = _completed_counts(proof)
    is_complete = diagnostic.get("complete") is True
    maximum_decrease = 0 if is_complete else 1
    if (
        not has_valid_reviewed_subset_counts(counts, maximum_decrease)
        or not has_valid_subset_completed_fields(
            proof,
            counts,
            proof["page_count"],
        )
        or proof.get("processed_rows") != checkpoint.get("rows_processed")
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    if is_complete:
        is_valid = bool(
            diagnostic.get("error") is None
            and diagnostic.get("traversal_complete") is True
            and diagnostic.get("source_continuation_exhausted") is True
            and diagnostic.get("next_url_remaining") is False
            and checkpoint.get("state") == "complete"
            and checkpoint.get("next_url") is None
            and proof.get("verified") is True
            and proof.get("failure") is None
            and proof["pre_count"] == proof["post_count"]
            and diagnostic["pages_fetched"]
            == checkpoint.get("pages_processed")
        )
        disposition = STABLE_COMPLETE_DISPOSITION
    else:
        is_valid = bool(
            diagnostic.get("error")
            == f"{CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:census_drift"
            and diagnostic.get("traversal_complete") is False
            and diagnostic.get("source_continuation_exhausted") is False
            and diagnostic.get("next_url_remaining") is False
            and checkpoint.get("state") == "active"
            and clean_text(checkpoint.get("next_url")) is not None
            and proof.get("verified") is False
            and proof.get("failure") == "census_drift"
            and proof["pre_count"] - proof["post_count"] == 1
            and diagnostic["pages_fetched"]
            == checkpoint.get("pages_processed") + 1
        )
        disposition = COUNT_DRIFT_DISPOSITION
    if not is_valid:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    _validate_terminal_sequence(
        proof,
        checkpoint["pages_processed"],
        checkpoint["rows_processed"],
        terminal_checkpointed=is_complete,
    )
    return disposition


def _retryable_disposition(
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    proof: Mapping[str, Any],
) -> str:
    forbidden_terminal_fields = set(_COMPLETED_COUNT_FIELDS[1:]) | {
        "failure",
        "terminal_page_geometry",
        "advertised_pre",
        "advertised_post",
        "returned_unique",
        "deficit",
        "terminal_reason",
    }
    is_valid = bool(
        diagnostic.get("complete") is False
        and diagnostic.get("error")
        == f"{CURRENT_VERSION_CENSUS_RETRYABLE_ERROR}:http_500"
        and diagnostic.get("traversal_complete") is False
        and diagnostic.get("source_continuation_exhausted") is False
        and diagnostic.get("next_url_remaining") is True
        and checkpoint.get("state") == "active"
        and clean_text(checkpoint.get("next_url")) is not None
        and proof.get("verified") is False
        and proof.get("pre_count") > 0
        and checkpoint.get("rows_processed") <= proof["pre_count"]
        and not any(field_name in proof for field_name in forbidden_terminal_fields)
        and diagnostic["pages_fetched"] == checkpoint.get("pages_processed")
        and diagnostic["rows_fetched"] == checkpoint.get("rows_processed")
    )
    if not is_valid:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    try:
        validate_current_version_census_checkpoint_geometry(
            proof,
            pages_processed=checkpoint["pages_processed"],
            rows_processed=checkpoint["rows_processed"],
            expected_page_count=proof["page_count"],
        )
    except ValueError:
        raise ReviewedSubsetTerminalDispositionError("evidence") from None
    return RETRYABLE_HTTP_500_DISPOSITION


def _resource_disposition(
    resource_type: str,
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    expected_start_url_sha256: str,
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    validate_disposition_diagnostic_shape(diagnostic)
    checkpoint_proof = json_object(checkpoint.get("completeness_json"))
    diagnostic_proof = json_object(
        diagnostic.get("server_issued_subset_completeness")
    )
    validate_disposition_proof_shapes(
        diagnostic,
        checkpoint_proof,
        diagnostic_proof,
    )
    if diagnostic.get("server_issued_subset_coverage") != (
        expected_subset_coverage(diagnostic_proof)
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    start_url_sha256, recent_cursor_hashes_sha256 = (
        _checkpoint_hash_commitments(
            checkpoint,
            diagnostic,
            expected_start_url_sha256,
        )
    )
    if (
        _safe_checkpoint_proof(checkpoint_proof) != diagnostic_proof
        or diagnostic.get("rows_fetched") != checkpoint.get("rows_processed")
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    identity = _proof_identity(checkpoint_proof, resource_type)
    disposition = (
        _retryable_disposition(diagnostic, checkpoint, checkpoint_proof)
        if diagnostic.get("error")
        == f"{CURRENT_VERSION_CENSUS_RETRYABLE_ERROR}:http_500"
        else _completed_or_drift_disposition(
            diagnostic,
            checkpoint,
            checkpoint_proof,
        )
    )
    return identity, _resource_marker(
        diagnostic,
        checkpoint,
        checkpoint_proof,
        disposition,
        start_url_sha256,
        recent_cursor_hashes_sha256,
    )


def _resource_marker(
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    checkpoint_proof: Mapping[str, Any],
    disposition: str,
    start_url_sha256: str,
    recent_cursor_hashes_sha256: str,
) -> dict[str, Any]:
    return {
        "disposition": disposition,
        "checkpoint_state": checkpoint["state"],
        "checkpoint_pages": checkpoint["pages_processed"],
        "diagnostic_pages": diagnostic["pages_fetched"],
        "page_delta": (
            diagnostic["pages_fetched"] - checkpoint["pages_processed"]
        ),
        "retained_rows": checkpoint["rows_processed"],
        "advertised_pre": checkpoint_proof.get("pre_count"),
        "advertised_post": checkpoint_proof.get("post_count"),
        "returned_unique": checkpoint_proof.get("unique_candidate_rows"),
        "deficit": checkpoint_proof.get("unreturned_count"),
        "diagnostic_sha256": canonical_evidence_sha256(diagnostic),
        "checkpoint_proof_sha256": canonical_evidence_sha256(checkpoint_proof),
        "start_url_sha256": start_url_sha256,
        "recent_cursor_hashes_sha256": recent_cursor_hashes_sha256,
    }


def validated_resource_dispositions(
    diagnostics: Mapping[str, Any],
    checkpoint_rows: Sequence[Mapping[str, Any]],
    candidate_metadata: Mapping[str, Any],
    *,
    expected_start_hash_by_type: Mapping[str, str],
) -> dict[str, dict[str, Any]]:
    """Validate the 2/1/4 resource partition and return marker evidence."""

    checkpoint_by_resource_type = {
        clean_text(checkpoint.get("resource_type")): checkpoint
        for checkpoint in checkpoint_rows
    }
    if (
        set(checkpoint_by_resource_type) != set(EXPECTED_RESOURCE_TYPES)
        or set(expected_start_hash_by_type) != set(EXPECTED_RESOURCE_TYPES)
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    identities = set()
    resource_by_type = {}
    for resource_type in EXPECTED_RESOURCE_TYPES:
        identity, resource = _resource_disposition(
            resource_type,
            json_object(diagnostics[resource_type]),
            checkpoint_by_resource_type[resource_type],
            expected_start_hash_by_type[resource_type],
        )
        identities.add(identity)
        resource_by_type[resource_type] = resource
    if (
        len(identities) != 1
        or next(iter(identities))[3]
        != candidate_metadata.get("verification_campaign_id")
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return resource_by_type


__all__ = ("validated_resource_dispositions",)
