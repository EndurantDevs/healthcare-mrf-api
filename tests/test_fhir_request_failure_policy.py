# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict source-level logical-request failure-budget boundaries."""

import pytest

from process.fhir_request_failure_policy import (
    FHIR_REQUEST_FAILURE_COVERAGE_BASIS,
    FHIR_REQUEST_FAILURE_POLICY_ID,
    FHIR_REQUEST_FAILURE_RESOURCE_COVERAGE,
    can_tolerate_fhir_request_failures,
)


@pytest.mark.parametrize(
    ("total", "failed", "allowed"),
    [(0, 0, False), (1, 0, True), (1, 1, False), (50, 1, False),
     (51, 1, True), (100, 1, True), (100, 2, False), (100, 3, False),
     (1_012_289, 8_827, True), (2**63 - 1, (2**63 - 1) // 50, True)],
)
def test_strict_failure_budget(total, failed, allowed):
    assert can_tolerate_fhir_request_failures(
        total_count=total,
        completed_count=total - failed,
        error_count_by_code={"transport_timeout": failed},
    ) is allowed


@pytest.mark.parametrize(
    "changes",
    [{"total_count": True}, {"total_count": 100.0}, {"total_count": -1},
     {"completed_count": True}, {"completed_count": -1},
     {"completed_count": 98}, {"completed_count": 100},
     {"pending_count": 1}, {"pending_count": False}, {"pending_count": -1},
     {"leased_count": 1}, {"leased_count": False}, {"leased_count": -1},
     {"error_count_by_code": None},
     {"error_count_by_code": {"transport_timeout": True}},
     {"error_count_by_code": {"transport_timeout": -1}},
     {"error_count_by_code": {"transport_timeout": 1.0}},
     {"error_count_by_code": {"response_invalid": 1}},
     {"error_count_by_code": {"http_401": 1}},
     {"error_count_by_code": {"http_403": 1}},
     {"error_count_by_code": {"http_404": 1}},
     {"error_count_by_code": {"http_410": 1}},
     {"error_count_by_code": {"request_invalid": 1}},
     {"error_count_by_code": {"retry_exhausted": 1}},
     {"error_count_by_code": {"transport_timeout": 1, "unknown": 0}},
     {"error_count_by_code": {False: 1}}],
)
def test_budget_rejects_unfinished_unclassified_or_inconsistent_census(changes):
    census_by_field = {
        "total_count": 100,
        "completed_count": 99,
        "error_count_by_code": {"transport_timeout": 1},
    }
    assert can_tolerate_fhir_request_failures(**(census_by_field | changes)) is False


def test_aggregate_lineage_counts_logical_work_once_and_not_retries():
    # Distinct phase work keys are aggregated; retry attempts never add successes.
    assert can_tolerate_fhir_request_failures(
        total_count=101,
        completed_count=99,
        error_count_by_code={"transport_timeout": 1, "retry_exhausted_transport": 1},
    )
    assert not can_tolerate_fhir_request_failures(
        total_count=101 + 8,
        completed_count=99,
        error_count_by_code={"transport_timeout": 1, "retry_exhausted_transport": 1},
    )
    assert FHIR_REQUEST_FAILURE_POLICY_ID == "healthporta.fhir.request-failure-budget.v1"
    assert FHIR_REQUEST_FAILURE_COVERAGE_BASIS == "logical_requests"
    assert FHIR_REQUEST_FAILURE_RESOURCE_COVERAGE == "unknown"
