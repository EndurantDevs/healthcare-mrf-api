# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Logical-window geometry for reviewed FHIR traversal checkpoints."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_fhir_subset_execution import (
    append_subset_checkpoint_evidence,
    validate_subset_checkpoint_sequences,
)


CURRENT_VERSION_CENSUS_PAGE_GEOMETRY_VERSION = 2
CURRENT_VERSION_CENSUS_PAGE_GEOMETRY_FIELD = "page_geometry"
_PAGE_GEOMETRY_FIELDS = frozenset(
    {
        "version",
        "page_count",
        "checkpointed_pages",
        "checkpointed_rows",
        "logical_next_offset",
        "sparse_pages",
        "empty_pages",
    }
)


def validate_census_page_entries(
    page_entry_count: int,
    expected_page_count: int,
) -> int:
    """Validate one returned entry count against its logical window size."""

    if (
        isinstance(page_entry_count, bool)
        or not isinstance(page_entry_count, int)
        or page_entry_count < 0
        or isinstance(expected_page_count, bool)
        or not isinstance(expected_page_count, int)
        or expected_page_count <= 0
        or page_entry_count > expected_page_count
    ):
        raise ValueError(
            "provider_directory_current_version_census_page_state_invalid"
        )
    return page_entry_count


def current_version_census_initial_page_geometry(
    expected_page_count: int,
) -> dict[str, int]:
    """Create the zero-position geometry stored with a new pre-count proof."""

    validate_census_page_entries(0, expected_page_count)
    return {
        "version": CURRENT_VERSION_CENSUS_PAGE_GEOMETRY_VERSION,
        "page_count": expected_page_count,
        "checkpointed_pages": 0,
        "checkpointed_rows": 0,
        "logical_next_offset": 0,
        "sparse_pages": 0,
        "empty_pages": 0,
    }


def validate_current_version_census_resume_state(
    pages_processed: int,
    rows_processed: int,
    expected_page_count: int,
    pre_total: int,
    *,
    allow_sparse_logical_offsets: bool = False,
) -> None:
    """Bind resumable rows to bounded fixed-width logical windows."""

    has_invalid_integer = any(
        isinstance(metric, bool) or not isinstance(metric, int) or metric < 0
        for metric in (pages_processed, rows_processed, pre_total)
    )
    validate_census_page_entries(0, expected_page_count)
    if has_invalid_integer:
        raise ValueError(
            "provider_directory_current_version_census_resume_state_invalid"
        )
    if pages_processed == 0 and rows_processed == 0:
        return
    logical_next_offset = pages_processed * expected_page_count
    if (
        pages_processed <= 0
        or (
            not allow_sparse_logical_offsets
            and logical_next_offset >= pre_total
        )
        or rows_processed > min(pre_total, logical_next_offset)
    ):
        raise ValueError(
            "provider_directory_current_version_census_resume_state_invalid"
        )


def _validated_checkpoint_geometry_metrics(
    proof_by_field: Mapping[str, Any],
) -> dict[str, int]:
    geometry_by_field = proof_by_field.get(
        CURRENT_VERSION_CENSUS_PAGE_GEOMETRY_FIELD
    )
    if (
        not isinstance(geometry_by_field, Mapping)
        or set(geometry_by_field) != _PAGE_GEOMETRY_FIELDS
    ):
        raise ValueError(
            "provider_directory_current_version_census_page_geometry_invalid"
        )
    normalized_geometry_by_field = dict(geometry_by_field)
    has_invalid_metric = any(
        isinstance(metric, bool) or not isinstance(metric, int) or metric < 0
        for metric in normalized_geometry_by_field.values()
    )
    if has_invalid_metric:
        raise ValueError(
            "provider_directory_current_version_census_page_geometry_invalid"
        )
    return normalized_geometry_by_field


def _checkpoint_geometry_row_bounds(
    geometry_by_field: Mapping[str, int],
) -> tuple[int, int]:
    page_count = geometry_by_field["page_count"]
    checkpointed_pages = geometry_by_field["checkpointed_pages"]
    sparse_pages = geometry_by_field["sparse_pages"]
    empty_pages = geometry_by_field["empty_pages"]
    full_pages = checkpointed_pages - sparse_pages
    nonempty_sparse_pages = sparse_pages - empty_pages
    minimum_rows = full_pages * page_count + nonempty_sparse_pages
    maximum_rows = (
        full_pages * page_count
        + nonempty_sparse_pages * max(page_count - 1, 0)
    )
    return minimum_rows, maximum_rows


def _has_checkpoint_geometry_coordinate_mismatch(
    geometry_by_field: Mapping[str, int],
    *,
    pages_processed: int,
    rows_processed: int,
    expected_page_count: int,
) -> bool:
    minimum_rows, maximum_rows = _checkpoint_geometry_row_bounds(
        geometry_by_field
    )
    return bool(
        geometry_by_field["version"]
        != CURRENT_VERSION_CENSUS_PAGE_GEOMETRY_VERSION
        or geometry_by_field["page_count"] != expected_page_count
        or geometry_by_field["checkpointed_pages"] != pages_processed
        or geometry_by_field["checkpointed_rows"] != rows_processed
        or geometry_by_field["logical_next_offset"]
        != pages_processed * expected_page_count
        or geometry_by_field["sparse_pages"] > pages_processed
        or geometry_by_field["empty_pages"]
        > geometry_by_field["sparse_pages"]
        or rows_processed > pages_processed * expected_page_count
        or rows_processed < minimum_rows
        or rows_processed > maximum_rows
    )


def validate_current_version_census_checkpoint_geometry(
    proof_by_field: Mapping[str, Any],
    *,
    pages_processed: int,
    rows_processed: int,
    expected_page_count: int,
) -> dict[str, int]:
    """Require persisted measurement to match its checkpoint coordinates."""

    normalized_geometry_by_field = _validated_checkpoint_geometry_metrics(
        proof_by_field
    )
    if _has_checkpoint_geometry_coordinate_mismatch(
        normalized_geometry_by_field,
        pages_processed=pages_processed,
        rows_processed=rows_processed,
        expected_page_count=expected_page_count,
    ):
        raise ValueError(
            "provider_directory_current_version_census_page_geometry_invalid"
        )
    validate_subset_checkpoint_sequences(
        proof_by_field,
        normalized_geometry_by_field,
        pages_processed=pages_processed,
        rows_processed=rows_processed,
        expected_page_count=expected_page_count,
    )
    return normalized_geometry_by_field


def _previous_checkpoint_geometry(
    proof_by_field: Mapping[str, Any],
    *,
    pages_processed: int,
    expected_page_count: int,
) -> tuple[dict[str, int], int]:
    geometry_by_field = proof_by_field.get(
        CURRENT_VERSION_CENSUS_PAGE_GEOMETRY_FIELD
    )
    previous_rows = (
        geometry_by_field.get("checkpointed_rows")
        if isinstance(geometry_by_field, Mapping)
        else None
    )
    if isinstance(previous_rows, bool) or not isinstance(previous_rows, int):
        raise ValueError(
            "provider_directory_current_version_census_page_geometry_invalid"
        )
    previous_geometry_by_field = (
        validate_current_version_census_checkpoint_geometry(
            proof_by_field,
            pages_processed=pages_processed - 1,
            rows_processed=previous_rows,
            expected_page_count=expected_page_count,
        )
    )
    return previous_geometry_by_field, previous_rows


def _validate_checkpoint_advance(
    proof_by_field: Mapping[str, Any],
    *,
    rows_processed: int,
    previous_rows: int,
    page_entry_count: int,
    logical_next_offset: int,
) -> None:
    pre_count = proof_by_field.get("pre_count")
    has_invalid_advance = bool(
        isinstance(rows_processed, bool)
        or not isinstance(rows_processed, int)
        or rows_processed < 0
        or rows_processed - previous_rows != page_entry_count
        or isinstance(pre_count, bool)
        or not isinstance(pre_count, int)
        or rows_processed > pre_count
        or (
            proof_by_field.get("contract_version") != 3
            and logical_next_offset >= pre_count
        )
    )
    if has_invalid_advance:
        raise ValueError(
            "provider_directory_current_version_census_page_geometry_invalid"
        )


def current_version_census_checkpoint_proof(
    proof_by_field: Mapping[str, Any],
    *,
    pages_processed: int,
    rows_processed: int,
    page_entry_count: int,
    expected_page_count: int,
    continuation_identity_sha256: str | None = None,
    continuation_shape_sha256: str | None = None,
) -> dict[str, Any]:
    """Advance measurement for one validated nonterminal logical window."""

    validate_census_page_entries(
        page_entry_count,
        expected_page_count,
    )
    if (
        isinstance(pages_processed, bool)
        or not isinstance(pages_processed, int)
        or pages_processed <= 0
    ):
        raise ValueError(
            "provider_directory_current_version_census_page_geometry_invalid"
        )
    previous_geometry_by_field, previous_rows = _previous_checkpoint_geometry(
        proof_by_field,
        pages_processed=pages_processed,
        expected_page_count=expected_page_count,
    )
    logical_next_offset = pages_processed * expected_page_count
    _validate_checkpoint_advance(
        proof_by_field,
        rows_processed=rows_processed,
        previous_rows=previous_rows,
        page_entry_count=page_entry_count,
        logical_next_offset=logical_next_offset,
    )
    updated_proof_by_field = _active_checkpoint_proof(proof_by_field)
    updated_proof_by_field[CURRENT_VERSION_CENSUS_PAGE_GEOMETRY_FIELD] = {
        **previous_geometry_by_field,
        "checkpointed_pages": pages_processed,
        "checkpointed_rows": rows_processed,
        "logical_next_offset": logical_next_offset,
        "sparse_pages": previous_geometry_by_field["sparse_pages"]
        + int(page_entry_count < expected_page_count),
        "empty_pages": previous_geometry_by_field["empty_pages"]
        + int(page_entry_count == 0),
    }
    return append_subset_checkpoint_evidence(
        proof_by_field,
        updated_proof_by_field,
        pages_processed=pages_processed,
        page_entry_count=page_entry_count,
        expected_page_count=expected_page_count,
        continuation_identity_sha256=continuation_identity_sha256,
        continuation_shape_sha256=continuation_shape_sha256,
    )


def _active_checkpoint_proof(
    proof_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    stale_fields = {
        "failure",
        "last_terminal_page_geometry",
        "post_count",
        "processed_rows",
        "terminal_page_geometry",
        "unique_candidate_rows",
        "unreturned_count",
        "advertised_pre",
        "advertised_post",
        "returned_unique",
        "deficit",
        "terminal_reason",
    }
    active_proof_by_field = {
        field_name: field_value
        for field_name, field_value in proof_by_field.items()
        if field_name not in stale_fields
    }
    active_proof_by_field["verified"] = False
    return active_proof_by_field


def _terminal_page_context(
    proof_by_field: Mapping[str, Any],
    *,
    pages_processed: int,
    processed_rows: int,
    expected_page_count: int,
    terminal_page_entry_count: int,
) -> tuple[dict[str, int], int]:
    validate_census_page_entries(
        terminal_page_entry_count,
        expected_page_count,
    )
    if (
        isinstance(pages_processed, bool)
        or not isinstance(pages_processed, int)
        or pages_processed <= 0
    ):
        raise ValueError(
            "provider_directory_current_version_census_terminal_geometry_invalid"
        )
    try:
        previous_geometry_by_field, previous_rows = (
            _previous_checkpoint_geometry(
                proof_by_field,
                pages_processed=pages_processed,
                expected_page_count=expected_page_count,
            )
        )
    except ValueError as exc:
        raise ValueError(
            "provider_directory_current_version_census_terminal_geometry_invalid"
        ) from exc
    pre_count = proof_by_field.get("pre_count")
    terminal_page_start_offset = (pages_processed - 1) * expected_page_count
    has_invalid_terminal = bool(
        isinstance(processed_rows, bool)
        or not isinstance(processed_rows, int)
        or processed_rows < 0
        or processed_rows - previous_rows != terminal_page_entry_count
        or isinstance(pre_count, bool)
        or not isinstance(pre_count, int)
        or (
            proof_by_field.get("contract_version") != 3
            and (
                (pre_count > 0 and terminal_page_start_offset >= pre_count)
                or (pre_count == 0 and terminal_page_start_offset != 0)
            )
        )
    )
    if has_invalid_terminal:
        raise ValueError(
            "provider_directory_current_version_census_terminal_geometry_invalid"
        )
    return previous_geometry_by_field, terminal_page_start_offset


def current_version_census_terminal_page_geometry(
    proof_by_field: Mapping[str, Any],
    *,
    pages_processed: int,
    processed_rows: int,
    expected_page_count: int,
    terminal_page_entry_count: int,
) -> dict[str, int]:
    """Measure the terminal window without advancing the resumable cursor."""

    previous_geometry_by_field, terminal_page_start_offset = (
        _terminal_page_context(
            proof_by_field,
            pages_processed=pages_processed,
            processed_rows=processed_rows,
            expected_page_count=expected_page_count,
            terminal_page_entry_count=terminal_page_entry_count,
        )
    )
    return {
        "version": CURRENT_VERSION_CENSUS_PAGE_GEOMETRY_VERSION,
        "page_count": expected_page_count,
        "pages_processed": pages_processed,
        "processed_rows": processed_rows,
        "terminal_page_start_offset": terminal_page_start_offset,
        "logical_window_end_offset": pages_processed * expected_page_count,
        "terminal_page_entries": terminal_page_entry_count,
        "sparse_pages": previous_geometry_by_field["sparse_pages"]
        + int(terminal_page_entry_count < expected_page_count),
        "empty_pages": previous_geometry_by_field["empty_pages"]
        + int(terminal_page_entry_count == 0),
    }


def current_version_census_terminal_attempt_proof(
    proof_by_field: Mapping[str, Any],
    *,
    pages_processed: int,
    processed_rows: int,
    expected_page_count: int,
    terminal_page_entry_count: int,
) -> dict[str, Any]:
    """Record a terminal observation without advancing its resumable cursor."""

    return {
        **dict(proof_by_field),
        "verified": False,
        "processed_rows": processed_rows,
        "last_terminal_page_geometry": (
            current_version_census_terminal_page_geometry(
                proof_by_field,
                pages_processed=pages_processed,
                processed_rows=processed_rows,
                expected_page_count=expected_page_count,
                terminal_page_entry_count=terminal_page_entry_count,
            )
        ),
    }
