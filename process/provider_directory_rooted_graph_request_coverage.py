# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed, source-lineage request coverage without invented resource totals."""

from __future__ import annotations

from process.fhir_request_failure_policy import (
    FHIR_REQUEST_FAILURE_POLICY_ID,
    FHIR_REQUEST_FAILURE_RESOURCE_COVERAGE,
    can_tolerate_fhir_request_failures,
)


_COUNT_FIELDS = (
    "total_requests", "failed_requests", "rooted_total_requests", "rooted_failed_requests"
)
_FIELDS = frozenset((*_COUNT_FIELDS, "policy_id", "resource_coverage"))


def validate_rooted_request_coverage(raw: object) -> dict[str, object]:
    """Validate a partial proof; retries and undiscovered resources are not counts."""

    if (
        type(raw) is not dict
        or set(raw) != _FIELDS
        or raw["policy_id"] != FHIR_REQUEST_FAILURE_POLICY_ID
        or raw["resource_coverage"] != FHIR_REQUEST_FAILURE_RESOURCE_COVERAGE
        or any(
            type(raw[name]) is not int or not 0 <= raw[name] <= 9_007_199_254_740_991
            for name in _COUNT_FIELDS
        )
    ):
        raise ValueError("provider_directory_rooted_request_coverage_invalid")
    total = raw["total_requests"]
    failed = raw["failed_requests"]
    rooted_total = raw["rooted_total_requests"]
    rooted_failed = raw["rooted_failed_requests"]
    if (
        not 0 < rooted_total < total
        or not 0 < failed <= total
        or rooted_failed > rooted_total
        or rooted_failed > failed
        or failed - rooted_failed > total - rooted_total
        or not can_tolerate_fhir_request_failures(
            total_count=total,
            completed_count=total - failed,
            error_count_by_code={"transport_timeout": failed},
        )
    ):
        raise ValueError("provider_directory_rooted_request_coverage_invalid")
    return dict(raw)


def has_matching_rooted_request_coverage(
    raw: object, *, completed_count: int, error_count: int
) -> bool:
    """Bind the source proof to this acquisition's conserved logical work census."""

    if raw is None:
        return type(error_count) is int and error_count == 0
    try:
        coverage = validate_rooted_request_coverage(raw)
    except ValueError:
        return False
    return bool(
        type(completed_count) is int
        and completed_count >= 0
        and type(error_count) is int
        and error_count >= 0
        and coverage["rooted_failed_requests"] == error_count
        and coverage["rooted_total_requests"] == completed_count + error_count
    )


def has_matching_rooted_publication_coverage(
    raw: object, *, retry_exhausted_count: int, rooted_graph_complete: bool
) -> bool:
    """Keep inherited Flex omissions distinct from this rooted crawl's failures."""

    if raw is None:
        return rooted_graph_complete is True
    try:
        coverage = validate_rooted_request_coverage(raw)
    except ValueError:
        return False
    return bool(
        type(retry_exhausted_count) is int
        and retry_exhausted_count >= 0
        and coverage["failed_requests"] - coverage["rooted_failed_requests"]
        == retry_exhausted_count
        and rooted_graph_complete is (coverage["rooted_failed_requests"] == 0)
    )
