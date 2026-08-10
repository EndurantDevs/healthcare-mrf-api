# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed diagnostic and proof shapes for terminal disposition evidence."""

from __future__ import annotations

import re
from typing import Any, Mapping

from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_BLOCKED_ERROR,
    SERVER_ISSUED_SUBSET_FETCH_MODE,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    ReviewedSubsetTerminalDispositionError,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    ACTIVE_PROOF_FIELDS,
    COUNT_DRIFT_PROOF_FIELDS,
    DIAGNOSTIC_FIELDS,
    STABLE_COMPLETE_PROOF_FIELDS,
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
    return integers_are_valid and numbers_are_valid and booleans_are_exact


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


__all__ = (
    "validate_disposition_diagnostic_shape",
    "validate_disposition_proof_shapes",
)
