# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed diagnostic and proof shapes for terminal disposition evidence."""

from __future__ import annotations

import re
from typing import Any, Mapping

from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_BLOCKED_ERROR,
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
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    COUNT_DRIFT_DISPOSITION,
    STABLE_COMPLETE_DISPOSITION,
    ReviewedSubsetTerminalDispositionError,
    canonical_evidence_sha256,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    ACTIVE_PROOF_FIELDS,
    COUNT_DRIFT_PROOF_FIELDS,
    DIAGNOSTIC_FIELDS,
    DIRECT_V4_MAX_VERIFIED_DECREASE,
    STABLE_COMPLETE_PROOF_FIELDS,
    TERMINAL_CENSUS_DRIFT_DISPOSITION,
    VERIFIED_COMPLETE_DISPOSITION,
)
from process.provider_directory_fhir_subset_terminal_disposition_util import (
    clean_text,
)


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_FALSE_DIAGNOSTIC_FIELDS = (
    "bounded",
    "collection_complete",
    "row_limit_reached",
    "page_limit_reached",
    "hard_page_limit_reached",
    "deadline_reached",
)
_INTEGER_DIAGNOSTIC_FIELDS = (
    "rows_written",
    "source_fetch_elapsed_ms",
    "page_prefetch_started",
    "page_prefetch_consumed",
    "page_prefetch_discarded",
    "pagination_cooldown_retries",
)
_NUMBER_DIAGNOSTIC_FIELDS = (
    "stream_write_elapsed_seconds",
    "checkpoint_persist_elapsed_seconds",
    "page_prefetch_wait_seconds",
    "pagination_cooldown_wait_seconds",
)
_BOOLEAN_DIAGNOSTIC_FIELDS = (
    "page_prefetch_eligible",
    "pagination_cooldown_recovered",
    "pagination_cooldown_exhausted",
    "pagination_cooldown_deadline_blocked",
)
_SERIAL_CONCURRENCY_FIELDS = (
    "resource_scan_concurrency_requested",
    "resource_scan_concurrency_effective",
)


def _has_valid_diagnostic_metrics(diagnostic: Mapping[str, Any]) -> bool:
    integers_are_valid = all(
        type(diagnostic.get(field_name)) is int
        and diagnostic[field_name] >= 0
        for field_name in _INTEGER_DIAGNOSTIC_FIELDS
    )
    numbers_are_valid = all(
        type(diagnostic.get(field_name)) in (int, float)
        and diagnostic[field_name] >= 0
        for field_name in _NUMBER_DIAGNOSTIC_FIELDS
    )
    booleans_are_exact = all(
        type(diagnostic.get(field_name)) is bool
        for field_name in _BOOLEAN_DIAGNOSTIC_FIELDS
    )
    concurrency_is_serial = all(
        type(diagnostic.get(field_name)) is int
        and diagnostic[field_name] == 1
        for field_name in _SERIAL_CONCURRENCY_FIELDS
    )
    return bool(
        integers_are_valid
        and numbers_are_valid
        and booleans_are_exact
        and concurrency_is_serial
    )


def validate_disposition_diagnostic_shape(
    diagnostic: Mapping[str, Any],
) -> None:
    """Require the exact stored importer diagnostic field and type contract."""

    retry_not_before = diagnostic.get("retry_not_before")
    has_exact_false_fields = all(
        diagnostic.get(field_name) is False
        for field_name in _FALSE_DIAGNOSTIC_FIELDS
    )
    if (
        set(diagnostic) != DIAGNOSTIC_FIELDS
        or diagnostic.get("fetch_mode") != SERVER_ISSUED_SUBSET_FETCH_MODE
        or not has_exact_false_fields
        or diagnostic.get("absence_semantics") != "unknown_under_subset"
        or diagnostic.get("plan_graph_complete") is not False
        or diagnostic.get("source_fetch") is not None
        or diagnostic.get("last_updated_completeness") is not None
        or diagnostic.get("caresource_opaque_cursor_completeness") is not None
        or diagnostic.get("current_version_census_completeness") is not None
        or not _has_valid_diagnostic_metrics(diagnostic)
        or not (
            retry_not_before is None
            or (type(retry_not_before) is str and retry_not_before)
        )
        or type(diagnostic.get("pages_fetched")) is not int
        or diagnostic["pages_fetched"] < 0
        or type(diagnostic.get("rows_fetched")) is not int
        or diagnostic["rows_fetched"] < 0
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")


def _expected_proof_fields(
    diagnostic: Mapping[str, Any],
) -> frozenset[str]:
    if diagnostic.get("complete") is True:
        return STABLE_COMPLETE_PROOF_FIELDS
    if diagnostic.get("error") == (
        f"{CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:census_drift"
    ):
        return COUNT_DRIFT_PROOF_FIELDS
    return ACTIVE_PROOF_FIELDS


def validate_disposition_proof_shapes(
    diagnostic: Mapping[str, Any],
    checkpoint_proof: Mapping[str, Any],
    diagnostic_proof: Mapping[str, Any],
) -> None:
    """Require exact persisted and sanitized proof shapes and SHA vectors."""

    expected_fields = _expected_proof_fields(diagnostic)
    expected_safe_fields = expected_fields - {"continuation_hop_sha256"}
    hash_vectors = (
        checkpoint_proof.get("continuation_hop_sha256"),
        checkpoint_proof.get("continuation_shape_sha256"),
    )
    has_invalid_hash_vector = any(
        type(hash_vector) is not list
        or any(
            type(digest) is not str or _SHA256.fullmatch(digest) is None
            for digest in hash_vector
        )
        for hash_vector in hash_vectors
    )
    if (
        set(checkpoint_proof) != expected_fields
        or set(diagnostic_proof) != expected_safe_fields
        or has_invalid_hash_vector
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")


def expected_subset_coverage(proof_by_field: Mapping[str, Any]) -> dict[str, Any]:
    """Rebuild the exact safe coverage projection stored by the importer."""

    geometry = proof_by_field.get("terminal_page_geometry")
    continuation_shape_sha256 = proof_by_field.get(
        "continuation_shape_sha256"
    )
    geometry_projection_by_field = None
    if isinstance(geometry, Mapping):
        geometry_projection_by_field = {
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
    continuation_projection_by_field = None
    if type(continuation_shape_sha256) is list:
        continuation_projection_by_field = {
            "validated_hops": len(continuation_shape_sha256),
            "chain_sha256": canonical_evidence_sha256(
                continuation_shape_sha256
            ),
        }
    return {
        "cutoff": proof_by_field.get("cutoff"),
        "scope": "server_issued_traversal_subset",
        "advertised_pre": proof_by_field.get("advertised_pre"),
        "advertised_post": proof_by_field.get("advertised_post"),
        "returned_unique": proof_by_field.get("returned_unique"),
        "deficit": proof_by_field.get("deficit"),
        "geometry": geometry_projection_by_field,
        "continuation": continuation_projection_by_field,
        "twin_state": "pending_matching_reviewed_root",
        "proof_state": (
            "resource_terminal_verified"
            if proof_by_field.get("verified") is True
            else "not_verified"
        ),
        "unresolved_reference_count": None,
        "absence_semantics": "unknown_under_subset",
    }


def validate_terminal_sequence(
    proof: Mapping[str, Any],
    checkpoint_pages: int,
    checkpoint_rows: int,
    *,
    terminal_checkpointed: bool,
) -> None:
    """Bind terminal geometry to the last durable checkpoint coordinates."""

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
        checkpoint_rows - terminal_page_entries
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


def _completed_counts(proof: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field_name: proof.get(field_name)
        for field_name in (
            "pre_count",
            "post_count",
            "processed_rows",
            "unique_candidate_rows",
        )
    }


def _has_valid_completed_counts(
    counts_by_field: Mapping[str, Any],
    *,
    is_complete: bool,
    is_direct_v4: bool,
) -> bool:
    maximum_decrease = (
        DIRECT_V4_MAX_VERIFIED_DECREASE
        if is_direct_v4 or not is_complete
        else 0
    )
    if is_direct_v4 and not is_complete:
        return bool(
            all(
                type(value) is int and value >= 0
                for value in counts_by_field.values()
            )
            and counts_by_field["post_count"] <= counts_by_field["pre_count"]
            and counts_by_field["processed_rows"]
            == counts_by_field["unique_candidate_rows"]
            and counts_by_field["unique_candidate_rows"]
            <= counts_by_field["post_count"]
        )
    return has_valid_reviewed_subset_counts(
        counts_by_field,
        maximum_decrease,
    )


def _is_completed_state_valid(
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    proof: Mapping[str, Any],
    advertised_decrease: int,
    maximum_decrease: int,
) -> bool:
    return bool(
        diagnostic.get("complete") is True
        and diagnostic.get("error") is None
        and diagnostic.get("traversal_complete") is True
        and diagnostic.get("source_continuation_exhausted") is True
        and diagnostic.get("next_url_remaining") is False
        and checkpoint.get("state") == "complete"
        and checkpoint.get("next_url") is None
        and proof.get("verified") is True
        and proof.get("failure") is None
        and advertised_decrease <= maximum_decrease
        and proof.get("processed_rows") == checkpoint.get("rows_processed")
        and diagnostic.get("rows_fetched") == checkpoint.get("rows_processed")
        and diagnostic.get("pages_fetched") == checkpoint.get("pages_processed")
    )


def _is_drift_state_valid(
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    proof: Mapping[str, Any],
    advertised_decrease: int,
    *,
    is_direct_v4: bool,
) -> bool:
    terminal_entries = proof["page_entry_counts"][-1]
    expected_decrease = (
        advertised_decrease > DIRECT_V4_MAX_VERIFIED_DECREASE
        if is_direct_v4
        else advertised_decrease == 1
    )
    return bool(
        diagnostic.get("complete") is False
        and diagnostic.get("error")
        == f"{CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:census_drift"
        and diagnostic.get("traversal_complete") is False
        and diagnostic.get("source_continuation_exhausted") is False
        and diagnostic.get("next_url_remaining") is False
        and checkpoint.get("state") == "active"
        and clean_text(checkpoint.get("next_url")) is not None
        and proof.get("verified") is False
        and proof.get("failure") == "census_drift"
        and expected_decrease
        and proof.get("processed_rows")
        == checkpoint.get("rows_processed") + terminal_entries
        and diagnostic.get("rows_fetched") == proof.get("processed_rows")
        and diagnostic.get("pages_fetched")
        == checkpoint.get("pages_processed") + 1
    )


def _validated_completed_context(
    diagnostic: Mapping[str, Any],
    proof: Mapping[str, Any],
    expected_disposition: str | None,
    *,
    is_direct_v4: bool,
) -> tuple[str, bool, int, int]:
    selected_disposition = expected_disposition or (
        STABLE_COMPLETE_DISPOSITION
        if diagnostic.get("complete") is True
        else COUNT_DRIFT_DISPOSITION
    )
    is_complete = selected_disposition in {
        STABLE_COMPLETE_DISPOSITION,
        VERIFIED_COMPLETE_DISPOSITION,
    }
    counts_by_field = _completed_counts(proof)
    if not _has_valid_completed_counts(
        counts_by_field,
        is_complete=is_complete,
        is_direct_v4=is_direct_v4,
    ) or not has_valid_subset_completed_fields(
        proof,
        counts_by_field,
        proof["page_count"],
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    advertised_decrease = (
        counts_by_field["pre_count"] - counts_by_field["post_count"]
    )
    maximum_decrease = (
        DIRECT_V4_MAX_VERIFIED_DECREASE
        if is_direct_v4 or not is_complete
        else 0
    )
    return (
        selected_disposition,
        is_complete,
        advertised_decrease,
        maximum_decrease,
    )


def completed_or_drift_disposition(
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    proof: Mapping[str, Any],
    expected_disposition: str | None = None,
    *,
    is_direct_v4: bool = False,
) -> str:
    """Validate one completed or census-drift terminal proof."""

    (
        selected_disposition,
        is_complete,
        advertised_decrease,
        maximum_decrease,
    ) = _validated_completed_context(
        diagnostic,
        proof,
        expected_disposition,
        is_direct_v4=is_direct_v4,
    )
    is_valid = (
        _is_completed_state_valid(
            diagnostic,
            checkpoint,
            proof,
            advertised_decrease,
            maximum_decrease,
        )
        if is_complete
        else _is_drift_state_valid(
            diagnostic,
            checkpoint,
            proof,
            advertised_decrease,
            is_direct_v4=is_direct_v4,
        )
    )
    if not is_valid or (
        not is_direct_v4
        and (
            proof.get("processed_rows") != checkpoint.get("rows_processed")
            or diagnostic.get("rows_fetched")
            != checkpoint.get("rows_processed")
        )
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    validate_terminal_sequence(
        proof,
        checkpoint["pages_processed"],
        checkpoint["rows_processed"],
        terminal_checkpointed=is_complete,
    )
    return selected_disposition


__all__ = (
    "completed_or_drift_disposition",
    "expected_subset_coverage",
    "validate_disposition_diagnostic_shape",
    "validate_disposition_proof_shapes",
    "validate_terminal_sequence",
)
