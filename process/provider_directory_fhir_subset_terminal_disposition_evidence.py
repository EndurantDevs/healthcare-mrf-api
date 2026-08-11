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
    validate_current_version_census_checkpoint_geometry,
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
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V4_CAMPAIGN_ID,
    DIRECT_V4_COMPLETION_SCOPES,
    DIRECT_V4_DISPOSITION_BY_RESOURCE_TYPE,
    DIRECT_V4_PAGE_COUNT,
    DIRECT_V4_STRATEGY_VERSION,
)
from process.provider_directory_fhir_subset_terminal_disposition_shapes import (
    completed_or_drift_disposition as _completed_or_drift_disposition,
    expected_subset_coverage,
    validate_disposition_diagnostic_shape,
    validate_disposition_proof_shapes,
    validate_terminal_sequence as _validate_terminal_sequence,
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
    *,
    direct_v4: bool = False,
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
        != (
            DIRECT_V4_STRATEGY_VERSION
            if direct_v4
            else SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION
        )
        or proof.get("semantics") != SERVER_ISSUED_SUBSET_SEMANTICS
        or proof.get("traversal_version")
        != SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION
        or proof.get("canonicalization_version")
        != SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION
        or proof.get("completion_scopes")
        != list(
            DIRECT_V4_COMPLETION_SCOPES
            if direct_v4
            else SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES
        )
        or proof.get("resource_type") != resource_type
        or clean_text(identity[0]) is None
        or clean_text(identity[1]) is None
        or _SHA256.fullmatch(identity[1]) is None
        or type(page_count) is not int
        or not 1 <= page_count <= 1000
        or (direct_v4 and page_count != DIRECT_V4_PAGE_COUNT)
        or (direct_v4 and proof.get("campaign_id") != DIRECT_V4_CAMPAIGN_ID)
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


def validated_terminal_resource_proof(
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    expected_start_url_sha256: str,
) -> tuple[dict[str, Any], str, str]:
    """Validate one retained proof and return its cursor commitments."""

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
    if (
        diagnostic.get("server_issued_subset_coverage")
        != expected_subset_coverage(diagnostic_proof)
        or _safe_checkpoint_proof(checkpoint_proof) != diagnostic_proof
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    start_url_sha256, recent_cursor_hashes_sha256 = (
        _checkpoint_hash_commitments(
            checkpoint,
            diagnostic,
            expected_start_url_sha256,
        )
    )
    return (
        checkpoint_proof,
        start_url_sha256,
        recent_cursor_hashes_sha256,
    )


def _resource_disposition(
    resource_type: str,
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    expected_start_url_sha256: str,
    *,
    direct_v4: bool = False,
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    (
        checkpoint_proof,
        start_url_sha256,
        recent_cursor_hashes_sha256,
    ) = validated_terminal_resource_proof(
        diagnostic,
        checkpoint,
        expected_start_url_sha256,
    )
    identity = _proof_identity(
        checkpoint_proof,
        resource_type,
        direct_v4=direct_v4,
    )
    expected_disposition = (
        DIRECT_V4_DISPOSITION_BY_RESOURCE_TYPE[resource_type]
        if direct_v4
        else None
    )
    disposition = (
        _retryable_disposition(diagnostic, checkpoint, checkpoint_proof)
        if not direct_v4
        and diagnostic.get("error")
        == f"{CURRENT_VERSION_CENSUS_RETRYABLE_ERROR}:http_500"
        else _completed_or_drift_disposition(
            diagnostic,
            checkpoint,
            checkpoint_proof,
            expected_disposition
            or (
                STABLE_COMPLETE_DISPOSITION
                if diagnostic.get("complete") is True
                else COUNT_DRIFT_DISPOSITION
            ),
            is_direct_v4=direct_v4,
        )
    )
    return identity, terminal_resource_marker(
        diagnostic,
        checkpoint,
        checkpoint_proof,
        disposition,
        start_url_sha256,
        recent_cursor_hashes_sha256,
        direct_v4=direct_v4,
    )


def terminal_resource_marker(
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    checkpoint_proof: Mapping[str, Any],
    disposition: str,
    start_url_sha256: str,
    recent_cursor_hashes_sha256: str,
    *,
    direct_v4: bool,
) -> dict[str, Any]:
    """Project one validated resource into the durable marker shape."""

    marker_by_field = {
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
    if direct_v4:
        marker_by_field["terminal_page_entry_count"] = checkpoint_proof[
            "page_entry_counts"
        ][-1]
    return marker_by_field


def validated_resource_dispositions(
    diagnostics: Mapping[str, Any],
    checkpoint_rows: Sequence[Mapping[str, Any]],
    candidate_metadata: Mapping[str, Any],
    *,
    expected_start_hash_by_type: Mapping[str, str],
    direct_v4: bool = False,
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
            direct_v4=direct_v4,
        )
        identities.add(identity)
        resource_by_type[resource_type] = resource
    if (
        len(identities) != 1
        or next(iter(identities))[3]
        != (
            DIRECT_V4_CAMPAIGN_ID
            if direct_v4
            else candidate_metadata.get("verification_campaign_id")
        )
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return resource_by_type


__all__ = (
    "terminal_resource_marker",
    "validated_resource_dispositions",
    "validated_terminal_resource_proof",
)
