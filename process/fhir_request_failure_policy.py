# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-scoped terminal failure budget for distinct logical FHIR requests."""

from __future__ import annotations

from collections.abc import Mapping


FHIR_REQUEST_FAILURE_POLICY_ID = "healthporta.fhir.request-failure-budget.v1"
FHIR_REQUEST_FAILURE_BUDGET_MULTIPLIER = 50
FHIR_REQUEST_FAILURE_COVERAGE_BASIS = "logical_requests"
# Partial request coverage cannot quantify hidden resources or descendants.
FHIR_REQUEST_FAILURE_RESOURCE_COVERAGE = "unknown"
FHIR_REQUEST_FAILURE_ALLOWED_CODES = frozenset(
    {"transport_timeout", "retry_exhausted_transport"}
)


def can_tolerate_fhir_request_failures(
    *,
    total_count: int,
    completed_count: int,
    error_count_by_code: Mapping[str, int],
    pending_count: int = 0,
    leased_count: int = 0,
) -> bool:
    """Require a terminal, conserved source census with strictly <2% failures.

    Callers count unique logical work keys across one exact source lineage,
    including inherited phases. Retries, pages, and page-size fallbacks do not
    add work keys. This decision does not assert resource or graph completeness.
    """

    counts = (total_count, completed_count, pending_count, leased_count)
    if (
        any(type(count) is not int or count < 0 for count in counts)
        or total_count == 0
        or pending_count != 0
        or leased_count != 0
        or not isinstance(error_count_by_code, Mapping)
        or any(
            type(code) is not str
            or code not in FHIR_REQUEST_FAILURE_ALLOWED_CODES
            or type(count) is not int
            or count < 0
            for code, count in error_count_by_code.items()
        )
    ):
        return False
    failed_count = sum(error_count_by_code.values())
    return bool(
        completed_count + failed_count == total_count
        and FHIR_REQUEST_FAILURE_BUDGET_MULTIPLIER * failed_count < total_count
    )
