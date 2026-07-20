# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict validation helpers for partitioned candidate audit reports."""

from __future__ import annotations

import math
from typing import Any, Mapping

from process.ptg_parts.ptg2_batch_candidate_audit_report_schema import (
    report_digest_hex,
    strict_nonnegative_report_int,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_contract import (
    PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_IN_FLIGHT,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUESTS_PER_SECOND,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT,
)
from process.ptg_parts.ptg2_candidate_audit_contract import (
    PTG2_FAST_AUDIT_DEADLINE_SECONDS,
)


def validate_endpoint_duration(batch_by_field: Mapping[str, Any]) -> None:
    """Validate the slowest endpoint duration against the release deadline."""

    endpoint_duration_ms = batch_by_field.get("endpoint_duration_ms")
    if (
        not _is_valid_nonnegative_number(endpoint_duration_ms)
        or float(endpoint_duration_ms)
        > PTG2_FAST_AUDIT_DEADLINE_SECONDS * 1_000
    ):
        raise ValueError("batch audit report endpoint timing is invalid")


def validate_partitioned_batch_binding(
    batch_by_field: Mapping[str, Any],
    *,
    occurrence_count: int,
    persisted_audit_sample_count: int,
    unique_source_condition_count: int,
) -> None:
    """Validate aggregate counts and digests for the partitioned request family."""

    request_digest = report_digest_hex(
        batch_by_field.get("request_digest"),
        field_name="batch.request_digest",
    )
    matched_digest = report_digest_hex(
        batch_by_field.get("matched_challenge_digest"),
        field_name="batch.matched_challenge_digest",
    )
    unique_count = strict_nonnegative_report_int(
        batch_by_field.get("unique_challenge_count"),
        field_name="batch.unique_challenge_count",
    )
    if (
        batch_by_field.get("response_contract")
        != PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT
        or batch_by_field.get("challenge_count") != occurrence_count
        or batch_by_field.get("matched_challenge_count") != occurrence_count
        or batch_by_field.get("persisted_audit_occurrence_count")
        != persisted_audit_sample_count
        or batch_by_field.get("validated_persisted_audit_occurrence_count")
        != persisted_audit_sample_count
        or unique_count != unique_source_condition_count
        or unique_count not in range(1, occurrence_count + 1)
        or request_digest == matched_digest
    ):
        raise ValueError("batch audit report response binding is invalid")


def _is_valid_nonnegative_number(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and float(value) >= 0
    )


def validate_partitioned_http_metrics(
    http_metrics_by_name: Mapping[str, Any],
    *,
    expected_request_count: int,
) -> None:
    """Validate exact request counts, concurrency, and measured start rate."""

    maximum_concurrency = http_metrics_by_name.get("max_concurrency")
    start_rate = http_metrics_by_name.get(
        "request_start_rate_actual_per_second"
    )
    start_span = http_metrics_by_name.get("request_start_span_seconds")
    expected_metrics_by_name = {
        "batch_api_planned_http_requests": expected_request_count,
        "batch_api_actual_http_requests": expected_request_count,
        "batch_api_completed_http_requests": expected_request_count,
        "batch_api_failed_http_requests": 0,
        "retry_count": 0,
        "max_concurrency": maximum_concurrency,
        "request_start_rate_limit_per_second": (
            PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUESTS_PER_SECOND
        ),
        "request_start_rate_actual_per_second": start_rate,
        "request_start_span_seconds": start_span,
        "method": "POST",
        "redirect_count": 0,
    }
    maximum_allowed_concurrency = min(
        expected_request_count,
        PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_IN_FLIGHT,
    )
    minimum_start_span = (expected_request_count - 1) / 2.0 - 0.1
    if (
        http_metrics_by_name != expected_metrics_by_name
        or type(maximum_concurrency) is not int
        or maximum_concurrency not in range(1, maximum_allowed_concurrency + 1)
        or not _is_valid_nonnegative_number(start_rate)
        or float(start_rate) > 2.1
        or not _is_valid_nonnegative_number(start_span)
        or (
            expected_request_count > 1
            and float(start_span) < minimum_start_span
        )
    ):
        raise ValueError("batch audit report HTTP accounting is invalid")


__all__ = [
    "validate_endpoint_duration",
    "validate_partitioned_batch_binding",
    "validate_partitioned_http_metrics",
]
