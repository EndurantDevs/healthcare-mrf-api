# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict response construction and aggregation for candidate-audit partitions."""

from __future__ import annotations

import math
from dataclasses import replace
from typing import Any, Iterable, Mapping, Sequence

from process.ptg_parts.ptg2_partitioned_candidate_audit_types import (
    BLOCK_IO_FIELDS,
    CANDIDATE_IO_FIELDS,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT,
    RESULT_FIELDS,
    PartitionedCandidateAuditAggregate,
    PartitionedCandidateAuditPlan,
    PartitionedCandidateAuditRequest,
    PartitionedCandidateAuditResult,
    canonical_digest,
    lower_hex,
    nonnegative_integer,
)


def _validated_ledger(
    raw_ledger: Any,
    fields: frozenset[str],
    name: str,
) -> dict[str, int]:
    if not isinstance(raw_ledger, Mapping) or set(raw_ledger) != fields:
        raise ValueError(f"partitioned_audit_{name}_fields_invalid")
    return {
        key: nonnegative_integer(
            raw_ledger[key],
            field_name=f"partitioned_audit_{name}_{key}",
        )
        for key in sorted(fields)
    }


def _validate_result_ledgers(
    block_io: Mapping[str, int],
    candidate_io: Mapping[str, int],
) -> None:
    unique_blocks = block_io["unique_physical_blocks"]
    if (
        unique_blocks < 1
        or block_io["physical_block_reads"] != unique_blocks
        or block_io["physical_block_decodes"] != unique_blocks
        or block_io["physical_payload_preparations"] != unique_blocks
        or block_io["repeated_physical_reads"] != 0
        or block_io["repeated_physical_decodes"] != 0
        or block_io["repeated_physical_preparations"] != 0
        or block_io["repeated_logical_payload_processes"] != 0
        or candidate_io["repeated_candidate_projection_builds"] != 0
    ):
        raise ValueError("partitioned_audit_result_repeated_work")


def build_partitioned_candidate_audit_result(
    *,
    request: PartitionedCandidateAuditRequest,
    matched_source_occurrence_count: int,
    validated_persisted_occurrence_count: int,
    duration_ms: float,
    block_io: Mapping[str, int],
    candidate_processing_io: Mapping[str, int],
) -> PartitionedCandidateAuditResult:
    """Build one strict successful response bound to its exact request."""

    if (
        isinstance(duration_ms, bool)
        or not isinstance(duration_ms, (int, float))
        or not math.isfinite(float(duration_ms))
        or float(duration_ms) < 0
    ):
        raise ValueError("partitioned_audit_result_duration_invalid")
    audit_result = PartitionedCandidateAuditResult(
        plan_digest=request.plan_digest,
        partition_index=request.partition_index,
        partition_count=request.partition_count,
        request_digest=request.request_digest,
        source_challenge_count=len(request.source_challenges),
        source_occurrence_count=request.source_occurrence_count,
        persisted_occurrence_count=len(request.persisted_occurrences),
        item_count=request.item_count,
        matched_source_occurrence_count=nonnegative_integer(
            matched_source_occurrence_count,
            field_name="partitioned_audit_matched_count",
        ),
        validated_persisted_occurrence_count=nonnegative_integer(
            validated_persisted_occurrence_count,
            field_name="partitioned_audit_validated_persisted_count",
        ),
        duration_ms=float(duration_ms),
        block_io=_validated_ledger(block_io, BLOCK_IO_FIELDS, "block_io"),
        candidate_processing_io=_validated_ledger(
            candidate_processing_io,
            CANDIDATE_IO_FIELDS,
            "candidate_io",
        ),
        result_digest="0" * 64,
    )
    if (
        audit_result.matched_source_occurrence_count
        != request.source_occurrence_count
        or audit_result.validated_persisted_occurrence_count
        != len(request.persisted_occurrences)
    ):
        raise ValueError("partitioned_audit_result_incomplete")
    _validate_result_ledgers(
        audit_result.block_io,
        audit_result.candidate_processing_io,
    )
    return replace(
        audit_result,
        result_digest=canonical_digest(audit_result.unsigned_payload),
    )


def _result_identity_by_field(raw_result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "plan_digest": lower_hex(
            raw_result.get("plan_digest"),
            field_name="partitioned_audit_plan_digest",
        ),
        "partition_index": nonnegative_integer(
            raw_result.get("partition_index"),
            field_name="partitioned_audit_partition_index",
        ),
        "partition_count": nonnegative_integer(
            raw_result.get("partition_count"),
            field_name="partitioned_audit_partition_count",
            minimum=1,
        ),
        "request_digest": lower_hex(
            raw_result.get("request_digest"),
            field_name="partitioned_audit_request_digest",
        ),
        "result_digest": lower_hex(
            raw_result.get("result_digest"),
            field_name="partitioned_audit_result_digest",
        ),
    }


def _result_counts_by_field(raw_result: Mapping[str, Any]) -> dict[str, int]:
    error_name_by_field = {
        "source_challenge_count": "partitioned_audit_source_challenge_count",
        "source_occurrence_count": "partitioned_audit_source_occurrence_count",
        "persisted_occurrence_count": (
            "partitioned_audit_persisted_occurrence_count"
        ),
        "item_count": "partitioned_audit_item_count",
        "matched_source_occurrence_count": "partitioned_audit_matched_count",
        "validated_persisted_occurrence_count": (
            "partitioned_audit_validated_persisted_count"
        ),
    }
    return {
        field_name: nonnegative_integer(
            raw_result.get(field_name),
            field_name=error_name,
            minimum=1 if field_name == "item_count" else 0,
        )
        for field_name, error_name in error_name_by_field.items()
    }


def _parsed_result(
    raw_result: Mapping[str, Any],
    duration_ms: float,
) -> PartitionedCandidateAuditResult:
    return PartitionedCandidateAuditResult(
        **_result_identity_by_field(raw_result),
        **_result_counts_by_field(raw_result),
        duration_ms=float(duration_ms),
        block_io=_validated_ledger(
            raw_result.get("block_io"),
            BLOCK_IO_FIELDS,
            "block_io",
        ),
        candidate_processing_io=_validated_ledger(
            raw_result.get("candidate_processing_io"),
            CANDIDATE_IO_FIELDS,
            "candidate_io",
        ),
    )


def parse_partitioned_candidate_audit_result(
    raw_result: Any,
    *,
    request: PartitionedCandidateAuditRequest,
) -> PartitionedCandidateAuditResult:
    """Parse and verify one response against its originating request."""

    if not isinstance(raw_result, Mapping) or set(raw_result) != RESULT_FIELDS:
        raise ValueError("partitioned_audit_result_fields_invalid")
    if (
        raw_result.get("contract")
        != PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT
    ):
        raise ValueError("partitioned_audit_result_contract_invalid")
    duration_ms = raw_result.get("duration_ms")
    if isinstance(duration_ms, bool) or not isinstance(
        duration_ms,
        (int, float),
    ):
        raise ValueError("partitioned_audit_result_duration_invalid")
    parsed_result = _parsed_result(raw_result, duration_ms)
    expected_result = build_partitioned_candidate_audit_result(
        request=request,
        matched_source_occurrence_count=(
            parsed_result.matched_source_occurrence_count
        ),
        validated_persisted_occurrence_count=(
            parsed_result.validated_persisted_occurrence_count
        ),
        duration_ms=parsed_result.duration_ms,
        block_io=parsed_result.block_io,
        candidate_processing_io=parsed_result.candidate_processing_io,
    )
    if parsed_result != expected_result:
        raise ValueError("partitioned_audit_result_binding_invalid")
    return parsed_result


def _aggregate_ledger(
    ledgers: Iterable[Mapping[str, int]],
    fields: frozenset[str],
) -> dict[str, int]:
    ledger_list = list(ledgers)
    return {
        field: (
            max((ledger[field] for ledger in ledger_list), default=0)
            if field == "peak_raw_bytes"
            else sum(ledger[field] for ledger in ledger_list)
        )
        for field in sorted(fields)
    }


def _ordered_complete_results(
    plan: PartitionedCandidateAuditPlan,
    response_results: Sequence[PartitionedCandidateAuditResult],
) -> tuple[PartitionedCandidateAuditResult, ...]:
    results_by_index: dict[int, PartitionedCandidateAuditResult] = {}
    for audit_result in response_results:
        if audit_result.partition_index in results_by_index:
            raise ValueError("partitioned_audit_duplicate_result")
        results_by_index[audit_result.partition_index] = audit_result
    if set(results_by_index) != set(range(plan.request_count)):
        raise ValueError("partitioned_audit_incomplete_results")
    ordered_results = tuple(
        results_by_index[index] for index in range(plan.request_count)
    )
    for request, partition_result in zip(plan.requests, ordered_results):
        if (
            partition_result.plan_digest != plan.plan_digest
            or partition_result.partition_count != plan.request_count
            or partition_result.request_digest != request.request_digest
            or partition_result.source_challenge_count
            != len(request.source_challenges)
            or partition_result.source_occurrence_count
            != request.source_occurrence_count
            or partition_result.persisted_occurrence_count
            != len(request.persisted_occurrences)
            or partition_result.item_count != request.item_count
        ):
            raise ValueError("partitioned_audit_result_plan_mismatch")
    return ordered_results


def _validated_aggregate_counts(
    plan: PartitionedCandidateAuditPlan,
    ordered_results: Sequence[PartitionedCandidateAuditResult],
) -> tuple[int, int, int]:
    source_challenge_count = sum(
        audit_result.source_challenge_count
        for audit_result in ordered_results
    )
    source_occurrence_count = sum(
        audit_result.matched_source_occurrence_count
        for audit_result in ordered_results
    )
    persisted_count = sum(
        audit_result.validated_persisted_occurrence_count
        for audit_result in ordered_results
    )
    if (
        source_challenge_count != plan.source_challenge_count
        or source_occurrence_count != plan.source_occurrence_count
        or persisted_count != plan.persisted_occurrence_count
        or len({request.request_digest for request in plan.requests})
        != plan.request_count
    ):
        raise ValueError("partitioned_audit_aggregate_count_mismatch")
    return source_challenge_count, source_occurrence_count, persisted_count


def _candidate_audit_aggregate(
    plan: PartitionedCandidateAuditPlan,
    ordered_results: tuple[PartitionedCandidateAuditResult, ...],
    aggregate_counts: tuple[int, int, int],
) -> PartitionedCandidateAuditAggregate:
    source_challenge_count, source_occurrence_count, persisted_count = (
        aggregate_counts
    )
    return PartitionedCandidateAuditAggregate(
        results=ordered_results,
        source_challenge_count=source_challenge_count,
        source_occurrence_count=source_occurrence_count,
        persisted_occurrence_count=persisted_count,
        request_count=plan.request_count,
        maximum_duration_ms=max(
            audit_result.duration_ms for audit_result in ordered_results
        ),
        block_io=_aggregate_ledger(
            (audit_result.block_io for audit_result in ordered_results),
            BLOCK_IO_FIELDS,
        ),
        candidate_processing_io=_aggregate_ledger(
            (
                audit_result.candidate_processing_io
                for audit_result in ordered_results
            ),
            CANDIDATE_IO_FIELDS,
        ),
        aggregate_digest=canonical_digest(
            {
                "plan_digest": plan.plan_digest,
                "result_digests": [
                    audit_result.result_digest
                    for audit_result in ordered_results
                ],
            }
        ),
    )


def validate_partitioned_candidate_audit_results(
    plan: PartitionedCandidateAuditPlan,
    response_results: Sequence[PartitionedCandidateAuditResult],
) -> PartitionedCandidateAuditAggregate:
    """Fail closed unless every planned request produced exactly one result."""

    ordered_results = _ordered_complete_results(plan, response_results)
    aggregate_counts = _validated_aggregate_counts(plan, ordered_results)
    return _candidate_audit_aggregate(
        plan,
        ordered_results,
        aggregate_counts,
    )


__all__ = [
    "build_partitioned_candidate_audit_result",
    "parse_partitioned_candidate_audit_result",
    "validate_partitioned_candidate_audit_results",
]
