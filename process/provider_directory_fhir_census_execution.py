# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Execution proofs for a reviewed current-version Provider Directory census."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_cursor import (
    CurrentVersionCensusContinuation,
    CurrentVersionCensusResumeState,
    resolved_current_version_census_next_url,
    validate_census_resume_url as _validate_resume_url,
)
from process.provider_directory_fhir_census_page_geometry import (
    CURRENT_VERSION_CENSUS_PAGE_GEOMETRY_FIELD,
    CURRENT_VERSION_CENSUS_PAGE_GEOMETRY_VERSION,
    current_version_census_checkpoint_proof,
    current_version_census_initial_page_geometry,
    current_version_census_terminal_attempt_proof,
    current_version_census_terminal_page_geometry,
    validate_census_page_entries,
)


CURRENT_VERSION_CENSUS_FETCH_MODE = "current_version_exact_census"
CURRENT_VERSION_CENSUS_BLOCKED_ERROR = (
    "provider_directory_current_version_census_completeness_blocked"
)
CURRENT_VERSION_CENSUS_RETRYABLE_ERROR = (
    "provider_directory_current_version_census_completeness_retryable"
)
_COMPLETED_COUNT_FIELDS = (
    "pre_count",
    "post_count",
    "processed_rows",
    "unique_candidate_rows",
)


def current_version_census_proof_identity(
    contract: CurrentVersionCensusContract,
) -> str:
    """Hash the complete admitted acquisition contract."""

    identity_payload = json.dumps(
        contract.identity(),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(identity_payload.encode("utf-8")).hexdigest()


def validated_current_version_census_total(payload: Any) -> int:
    """Return one exact FHIR Bundle total without bool coercion."""

    if not isinstance(payload, Mapping):
        raise ValueError(
            "provider_directory_current_version_census_count_payload_invalid"
        )
    if payload.get("resourceType") != "Bundle":
        raise ValueError(
            "provider_directory_current_version_census_count_bundle_required"
        )
    if payload.get("type") != "searchset":
        raise ValueError(
            "provider_directory_current_version_census_count_searchset_required"
        )
    total = payload.get("total")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise ValueError(
            "provider_directory_current_version_census_count_total_invalid"
        )
    return total


def current_version_census_initial_proof(
    contract: CurrentVersionCensusContract,
    resource_type: str,
    pre_count: int,
    *,
    expected_page_count: int,
) -> dict[str, Any]:
    """Create the durable pre-census proof bound to the admitted contract."""

    if resource_type not in contract.resources:
        raise ValueError(
            "provider_directory_current_version_census_resource_not_bound"
        )
    if isinstance(pre_count, bool) or not isinstance(pre_count, int) or pre_count < 0:
        raise ValueError(
            "provider_directory_current_version_census_pre_count_invalid"
        )
    return {
        "strategy_version": contract.strategy_version,
        "contract_identity": current_version_census_proof_identity(contract),
        "cutoff": contract.cutoff,
        "resource_type": resource_type,
        "pre_count": pre_count,
        "verified": False,
        CURRENT_VERSION_CENSUS_PAGE_GEOMETRY_FIELD: (
            current_version_census_initial_page_geometry(expected_page_count)
        ),
    }


def current_version_census_persisted_pre_count(
    completeness: Mapping[str, Any],
    contract: CurrentVersionCensusContract,
    resource_type: str,
) -> int | None:
    """Load a compatible pre-count or reject a drifted checkpoint."""

    if not completeness:
        return None
    expected_by_field = {
        "strategy_version": contract.strategy_version,
        "contract_identity": current_version_census_proof_identity(contract),
        "cutoff": contract.cutoff,
        "resource_type": resource_type,
    }
    if any(
        completeness.get(field_name) != expected_value
        for field_name, expected_value in expected_by_field.items()
    ):
        raise ValueError(
            "provider_directory_current_version_census_checkpoint_identity_mismatch"
        )
    pre_count = completeness.get("pre_count")
    if isinstance(pre_count, bool) or not isinstance(pre_count, int) or pre_count < 0:
        raise ValueError(
            "provider_directory_current_version_census_checkpoint_pre_count_invalid"
        )
    return pre_count


def _current_version_census_failure(
    *,
    pre_count: int,
    post_count: int,
    processed_rows: int,
    unique_candidate_rows: int,
) -> str | None:
    if post_count != pre_count:
        return "census_drift"
    if processed_rows < pre_count:
        return "cursor_loss"
    if processed_rows != unique_candidate_rows:
        return "duplicate_resource_ids"
    if processed_rows != pre_count:
        return "processed_count_mismatch"
    return None


def current_version_census_completed_proof(
    initial_proof: Mapping[str, Any],
    *,
    post_count: int,
    processed_rows: int,
    unique_candidate_rows: int,
    pages_processed: int,
    expected_page_count: int,
    terminal_page_entry_count: int,
) -> dict[str, Any]:
    """Require advertised-total equality before marking a resource verified."""

    count_by_name = {
        "pre_count": initial_proof.get("pre_count"),
        "post_count": post_count,
        "processed_rows": processed_rows,
        "unique_candidate_rows": unique_candidate_rows,
    }
    if any(
        isinstance(count, bool) or not isinstance(count, int) or count < 0
        for count in count_by_name.values()
    ):
        raise ValueError(
            "provider_directory_current_version_census_proof_count_invalid"
        )
    pre_count = count_by_name["pre_count"]
    terminal_page_geometry = current_version_census_terminal_page_geometry(
        initial_proof,
        pages_processed=pages_processed,
        processed_rows=processed_rows,
        expected_page_count=expected_page_count,
        terminal_page_entry_count=terminal_page_entry_count,
    )
    failure = _current_version_census_failure(
        pre_count=pre_count,
        post_count=post_count,
        processed_rows=processed_rows,
        unique_candidate_rows=unique_candidate_rows,
    )
    reusable_proof_by_field = {
        field_name: field_value
        for field_name, field_value in initial_proof.items()
        if field_name not in {"failure", "last_terminal_page_geometry"}
    }
    completed_proof_by_field = {
        **reusable_proof_by_field,
        "post_count": post_count,
        "processed_rows": processed_rows,
        "unique_candidate_rows": unique_candidate_rows,
        "unreturned_count": max(pre_count - unique_candidate_rows, 0),
        "terminal_page_geometry": terminal_page_geometry,
        "verified": failure is None,
    }
    if failure is not None:
        completed_proof_by_field["failure"] = failure
    return completed_proof_by_field


def _completed_count_map(
    completeness: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        field_name: completeness.get(field_name)
        for field_name in _COMPLETED_COUNT_FIELDS
    }


def _has_valid_completed_counts(count_by_name: Mapping[str, Any]) -> bool:
    if any(
        isinstance(count, bool) or not isinstance(count, int) or count < 0
        for count in count_by_name.values()
    ):
        return False
    return len(set(count_by_name.values())) == 1


def _has_valid_terminal_geometry(
    completeness: Mapping[str, Any],
    count_by_name: Mapping[str, Any],
    pages_processed: int | None,
) -> bool:
    terminal_geometry_by_field = completeness.get("terminal_page_geometry")
    if not isinstance(terminal_geometry_by_field, Mapping):
        return False
    terminal_pages = terminal_geometry_by_field.get("pages_processed")
    terminal_page_count = terminal_geometry_by_field.get("page_count")
    terminal_entries = terminal_geometry_by_field.get("terminal_page_entries")
    try:
        expected_geometry_by_field = (
            current_version_census_terminal_page_geometry(
                completeness,
                pages_processed=terminal_pages,
                processed_rows=count_by_name["processed_rows"],
                expected_page_count=terminal_page_count,
                terminal_page_entry_count=terminal_entries,
            )
        )
    except ValueError:
        return False
    return bool(
        expected_geometry_by_field == dict(terminal_geometry_by_field)
        and (
            pages_processed is None
            or terminal_pages == pages_processed
        )
    )


def _has_valid_unreturned_count(
    completeness: Mapping[str, Any],
    count_by_name: Mapping[str, Any],
) -> bool:
    unreturned_count = completeness.get("unreturned_count")
    return bool(
        not isinstance(unreturned_count, bool)
        and isinstance(unreturned_count, int)
        and unreturned_count >= 0
        and unreturned_count
        == max(
            count_by_name["pre_count"]
            - count_by_name["unique_candidate_rows"],
            0,
        )
    )


def validated_current_version_census_completed_proof(
    completeness: Mapping[str, Any],
    contract: CurrentVersionCensusContract,
    resource_type: str,
    *,
    rows_processed: int | None = None,
    pages_processed: int | None = None,
) -> dict[str, Any]:
    """Require a complete, identity-bound four-way census proof."""

    current_version_census_persisted_pre_count(
        completeness,
        contract,
        resource_type,
    )
    count_by_name = _completed_count_map(completeness)
    has_valid_proof = bool(
        _has_valid_completed_counts(count_by_name)
        and _has_valid_terminal_geometry(
            completeness,
            count_by_name,
            pages_processed,
        )
        and _has_valid_unreturned_count(completeness, count_by_name)
        and completeness.get("verified") is True
        and completeness.get("failure") is None
        and (
            rows_processed is None
            or count_by_name["processed_rows"] == rows_processed
        )
    )
    if not has_valid_proof:
        raise ValueError(
            "provider_directory_current_version_census_completed_proof_invalid"
        )
    return dict(completeness)


def validated_current_version_census_resume_url(
    contract: CurrentVersionCensusContract,
    resource_type: str,
    start_url: str,
    next_url: str | None,
    *,
    pages_processed: int,
    rows_processed: int,
    expected_page_count: int,
    proof: Mapping[str, Any],
) -> str:
    """Validate an identity-bound persisted cursor before resumed transport."""

    pre_total = current_version_census_persisted_pre_count(
        proof,
        contract,
        resource_type,
    )
    assert pre_total is not None
    return _validate_resume_url(
        contract,
        resource_type,
        start_url,
        next_url,
        CurrentVersionCensusResumeState(
            pages_processed=pages_processed,
            rows_processed=rows_processed,
            expected_page_count=expected_page_count,
            pre_total=pre_total,
            proof_by_field=proof,
        ),
    )
