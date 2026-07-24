# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict durable validation for redacted V4 candidate batch reports."""

from __future__ import annotations

import datetime
import hashlib
from dataclasses import dataclass
from typing import Any, Mapping

from process.ptg_parts.ptg2_batch_candidate_audit_report_schema import (
    PTG2_BATCH_AUDIT_ATTESTATION_CONTRACT,
    PTG2_BATCH_AUDIT_TOOL,
    AUDIT_SAMPLE_FIELDS,
    BATCH_FIELDS,
    CHECK_FIELDS,
    COVERAGE_FIELDS,
    REPORT_FIELDS,
    canonical_report_bytes,
    report_digest_bytes,
    report_digest_hex,
    strict_nonnegative_report_int,
    strict_report_mapping,
)
from process.ptg_parts.ptg2_batch_candidate_audit_report_sections import (
    CandidateReportCoordinates,
    ValidatedSourceEvidence,
    validate_report_header,
    validate_report_redaction,
    validate_report_target,
    validate_report_timing,
    validated_report_source,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchRequest,
    PTG2_AUDIT_BATCH_API_PATH,
    PTG2_AUDIT_BATCH_REQUEST_CONTRACT,
    PTG2_AUDIT_BATCH_RESPONSE_CONTRACT,
    matched_audit_batch_digest,
    parse_audit_batch_request,
    validate_audit_batch_once_ledgers,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_contract import (
    PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_report_validation import (
    validate_endpoint_duration,
    validate_partitioned_batch_binding,
    validate_partitioned_http_metrics,
)
from process.ptg_parts.ptg2_shared_blocks import PTG2_V3_SHARED_GENERATION
from process.ptg_parts.ptg2_candidate_audit_contract import (
    validated_public_audit_sample_projection,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_V3_SOURCE_WITNESS_SELECTION,
)


@dataclass(frozen=True)
class BatchBindingContext:
    """Immutable values needed to reconstruct and validate one batch request."""

    coordinates: CandidateReportCoordinates
    witness: Mapping[str, Any]
    audit_sample: Mapping[str, Any]
    occurrence_count: int
    unique_source_condition_count: int
    persisted_audit_sample_count: int


@dataclass(frozen=True)
class ValidatedReportParts:
    """Validated report components used to build durable attestation evidence."""

    completed_at: datetime.datetime
    tool_version: str
    source: ValidatedSourceEvidence
    audit_sample: Mapping[str, Any]
    checks: Mapping[str, int]
    ordered_source_ordinal_digest: str


def _validated_coverage_checks(
    report_by_field: Mapping[str, Any],
    *,
    witness: Mapping[str, Any],
    audit_sample: Mapping[str, Any],
) -> dict[str, int]:
    """Validate coverage and execution counters against sealed evidence."""

    coverage_by_field = strict_report_mapping(
        report_by_field.get("coverage"),
        field_name="coverage",
        expected_fields=COVERAGE_FIELDS,
    )
    batch_by_field = strict_report_mapping(
        report_by_field.get("batch"),
        field_name="batch",
        expected_fields=BATCH_FIELDS,
    )
    is_partitioned = (
        batch_by_field.get("request_contract")
        == PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT
    )
    expected_coverage_by_field = _expected_coverage(
        coverage_by_field,
        witness=witness,
        audit_sample=audit_sample,
        partitioned=is_partitioned,
    )
    if coverage_by_field != expected_coverage_by_field:
        raise ValueError("batch audit report bounded coverage is invalid")
    return _validated_checks(
        report_by_field,
        witness=witness,
        audit_sample=audit_sample,
        coverage=coverage_by_field,
        partitioned=is_partitioned,
    )


def _expected_coverage(
    coverage: Mapping[str, Any],
    *,
    witness: Mapping[str, Any],
    audit_sample: Mapping[str, Any],
    partitioned: bool,
) -> dict[str, Any]:
    """Return the only coverage section accepted for the supplied evidence."""

    return {
        "selection_method": PTG2_V3_SOURCE_WITNESS_SELECTION,
        "queryable_occurrence_population_count": witness[
            "queryable_occurrence_population_count"
        ],
        "emitted_rate_row_count": witness["emitted_rate_row_count"],
        "unqueryable_rate_row_count": witness["unqueryable_rate_row_count"],
        "unqueryable_rate_policy": witness["unqueryable_rate_policy"],
        "occurrence_sample_count": witness["occurrence_witness_count"],
        "provider_sample_count": witness["provider_witness_count"],
        "unique_source_condition_count": strict_nonnegative_report_int(
            coverage.get("unique_source_condition_count"),
            field_name="coverage.unique_source_condition_count",
        ),
        "persisted_audit_sample_count": audit_sample["sample_count"],
        "execution_mode": (
            "worker_validated_partitioned_batch_v1"
            if partitioned
            else "server_derived_one_request_batch"
        ),
    }


def _validated_checks(
    report_by_field: Mapping[str, Any],
    *,
    witness: Mapping[str, Any],
    audit_sample: Mapping[str, Any],
    coverage: Mapping[str, Any],
    partitioned: bool,
) -> dict[str, int]:
    """Validate exact executed-item and request counters."""

    checks_by_name = strict_report_mapping(
        report_by_field.get("checks"),
        field_name="checks",
        expected_fields=CHECK_FIELDS,
    )
    batch_request_count = strict_nonnegative_report_int(
        checks_by_name.get("batch_requests_executed"),
        field_name="checks.batch_requests_executed",
    )
    expected_checks_by_name = {
        "source_witnesses": int(witness["record_count"]),
        "source_occurrence_witnesses_matched": int(
            witness["occurrence_witness_count"]
        ),
        "unique_source_conditions_executed": int(
            coverage["unique_source_condition_count"]
        ),
        "provider_witnesses_validated": int(witness["provider_witness_count"]),
        "persisted_audit_occurrences_validated": int(audit_sample["sample_count"]),
        "batch_requests_executed": (
            batch_request_count if partitioned else 1
        ),
    }
    if (
        checks_by_name != expected_checks_by_name
        or (partitioned and batch_request_count < 1)
    ):
        raise ValueError("batch audit report checks are invalid")
    return {
        field_name: int(counter_value)
        for field_name, counter_value in checks_by_name.items()
    }


def _parsed_batch_request(
    batch_by_field: Mapping[str, Any],
    context: BatchBindingContext,
) -> AuditBatchRequest:
    """Reconstruct the exact request covered by the durable batch digest."""

    coordinates = context.coordinates
    try:
        return parse_audit_batch_request(
            {
                "contract": batch_by_field.get("request_contract"),
                "snapshot_id": coordinates.snapshot_id,
                "source_key": coordinates.source_key,
                "plan_id": coordinates.plan_id,
                "plan_market_type": coordinates.plan_market_type,
                "audit_sample_digest": context.audit_sample.get("sample_digest"),
                "source_witness_sample_digest": context.witness.get("sample_digest"),
                "source_witness_payload_sha256": context.witness.get("payload_sha256"),
                "ordered_source_ordinal_digest": batch_by_field.get(
                    "ordered_source_ordinal_digest"
                ),
                "challenge_count": batch_by_field.get("challenge_count"),
                "request_digest": batch_by_field.get("request_digest"),
            }
        )
    except ValueError as exc:
        raise ValueError(
            "batch audit report request binding is invalid"
        ) from exc


def _validate_batch_response_binding(
    batch_by_field: Mapping[str, Any],
    request: AuditBatchRequest,
    context: BatchBindingContext,
) -> None:
    matched_digest = report_digest_hex(
        batch_by_field.get("matched_challenge_digest"),
        field_name="batch.matched_challenge_digest",
    )
    occurrence_count = context.occurrence_count
    if (
        batch_by_field.get("request_contract")
        != PTG2_AUDIT_BATCH_REQUEST_CONTRACT
        or batch_by_field.get("response_contract")
        != PTG2_AUDIT_BATCH_RESPONSE_CONTRACT
        or request.challenge_count != occurrence_count
        or batch_by_field.get("challenge_count") != occurrence_count
        or batch_by_field.get("matched_challenge_count") != occurrence_count
        or batch_by_field.get("persisted_audit_occurrence_count")
        != context.persisted_audit_sample_count
        or batch_by_field.get("validated_persisted_audit_occurrence_count")
        != context.persisted_audit_sample_count
        or strict_nonnegative_report_int(
            batch_by_field.get("unique_challenge_count"),
            field_name="batch.unique_challenge_count",
        )
        != context.unique_source_condition_count
        or context.unique_source_condition_count
        not in range(1, occurrence_count + 1)
        or matched_digest
        != matched_audit_batch_digest(request.request_digest, occurrence_count)
    ):
        raise ValueError("batch audit report response binding is invalid")


def _validated_batch_ordinal_digest(
    report_by_field: Mapping[str, Any],
    context: BatchBindingContext,
) -> str:
    """Validate one batch section and return its source-ordinal binding."""

    batch_by_field = strict_report_mapping(
        report_by_field.get("batch"),
        field_name="batch",
        expected_fields=BATCH_FIELDS,
    )
    if (
        batch_by_field.get("request_contract")
        == PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT
    ):
        validate_partitioned_batch_binding(
            batch_by_field,
            occurrence_count=context.occurrence_count,
            persisted_audit_sample_count=(
                context.persisted_audit_sample_count
            ),
            unique_source_condition_count=(
                context.unique_source_condition_count
            ),
        )
        ordinal_digest = report_digest_hex(
            batch_by_field.get("ordered_source_ordinal_digest"),
            field_name="batch.ordered_source_ordinal_digest",
        )
    else:
        request = _parsed_batch_request(batch_by_field, context)
        _validate_batch_response_binding(batch_by_field, request, context)
        ordinal_digest = request.ordered_source_ordinal_digest
    validate_endpoint_duration(batch_by_field)
    return ordinal_digest


def _validate_http_and_io(
    report_by_field: Mapping[str, Any],
    *,
    witness: Mapping[str, Any],
    checks: Mapping[str, int],
    partitioned: bool,
) -> None:
    """Validate request accounting and read/process-once ledgers."""

    http_metrics_by_name = strict_report_mapping(
        report_by_field.get("http"),
        field_name="http",
    )
    expected_request_count = checks["batch_requests_executed"]
    if partitioned:
        validate_partitioned_http_metrics(
            http_metrics_by_name,
            expected_request_count=expected_request_count,
        )
    elif http_metrics_by_name != {
        "batch_api_actual_http_requests": expected_request_count,
        "retry_count": 0,
        "max_concurrency": 1,
        "method": "POST",
        "redirect_count": 0,
    }:
        raise ValueError("batch audit report HTTP accounting is invalid")
    io_ledgers_by_kind = strict_report_mapping(
        report_by_field.get("io"),
        field_name="io",
    )
    if set(io_ledgers_by_kind) != {
        "block_io",
        "witness_io",
        "candidate_processing_io",
    }:
        raise ValueError("batch audit report I/O fields are invalid")
    _, witness_io, _ = validate_audit_batch_once_ledgers(
        block_io=io_ledgers_by_kind["block_io"],
        witness_io=io_ledgers_by_kind["witness_io"],
        candidate_processing_io=io_ledgers_by_kind[
            "candidate_processing_io"
        ],
    )
    if (
        witness_io["record_decodes"] != int(witness["record_count"])
        or witness_io["unique_evidence_entries"]
        != int(witness["evidence_dictionary_count"])
    ):
        raise ValueError("batch audit report witness I/O is invalid")


def _validated_audit_sample(
    report_by_field: Mapping[str, Any],
    *,
    source_count: int,
) -> dict[str, Any]:
    audit_sample_by_field = strict_report_mapping(
        report_by_field.get("api_audit_sample"),
        field_name="api_audit_sample",
        expected_fields=AUDIT_SAMPLE_FIELDS,
    )
    if (
        audit_sample_by_field.get("sample_digest_validated") is not True
        or audit_sample_by_field.get("source_set_validated") is not True
    ):
        raise ValueError("batch audit report sample binding is invalid")
    return validated_public_audit_sample_projection(
        audit_sample_by_field,
        expected_source_count=source_count,
    )


def _validated_report_parts(
    report_by_field: Mapping[str, Any],
    coordinates: CandidateReportCoordinates,
    evaluation_time: datetime.datetime,
) -> ValidatedReportParts:
    """Validate and collect all report sections needed for attestation."""

    tool_version = validate_report_header(report_by_field)
    completed_at = validate_report_timing(report_by_field, evaluated_at=evaluation_time)
    validate_report_target(report_by_field, coordinates)
    validate_report_redaction(report_by_field)
    source_evidence = validated_report_source(report_by_field)
    audit_sample = _validated_audit_sample(
        report_by_field,
        source_count=int(source_evidence.witness["source_count"]),
    )
    checks = _validated_coverage_checks(
        report_by_field,
        witness=source_evidence.witness,
        audit_sample=audit_sample,
    )
    batch_context = BatchBindingContext(
        coordinates=coordinates,
        witness=source_evidence.witness,
        audit_sample=audit_sample,
        occurrence_count=int(
            source_evidence.witness["occurrence_witness_count"]
        ),
        unique_source_condition_count=int(
            checks["unique_source_conditions_executed"]
        ),
        persisted_audit_sample_count=int(audit_sample["sample_count"]),
    )
    ordinal_digest = _validated_batch_ordinal_digest(report_by_field, batch_context)
    batch_by_field = strict_report_mapping(
        report_by_field.get("batch"),
        field_name="batch",
        expected_fields=BATCH_FIELDS,
    )
    _validate_http_and_io(
        report_by_field,
        witness=source_evidence.witness,
        checks=checks,
        partitioned=(
            batch_by_field.get("request_contract")
            == PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT
        ),
    )
    return ValidatedReportParts(
        completed_at=completed_at,
        tool_version=tool_version,
        source=source_evidence,
        audit_sample=audit_sample,
        checks=checks,
        ordered_source_ordinal_digest=ordinal_digest,
    )


def _attestation_evidence(
    report_by_field: Mapping[str, Any],
    parts: ValidatedReportParts,
) -> dict[str, Any]:
    report_bytes = canonical_report_bytes(report_by_field)
    return {
        "contract": PTG2_BATCH_AUDIT_ATTESTATION_CONTRACT,
        "tool_name": PTG2_BATCH_AUDIT_TOOL,
        "tool_version": parts.tool_version,
        "report_digest": hashlib.sha256(report_bytes).digest(),
        "report_json": report_bytes.decode("utf-8"),
        "completed_at": parts.completed_at,
        "audit_sample_digest": report_digest_bytes(
            parts.audit_sample.get("sample_digest"),
            field_name="api_audit_sample.sample_digest",
        ),
        "source_set_digest": parts.source.source_set_digest,
        "source_witness_digest": parts.source.source_witness_digest,
        "source_witness_manifest": dict(parts.source.witness),
        "audit_sample_public": dict(parts.audit_sample),
        "provider_identifier_quarantine_evidence": (
            parts.source.quarantine_evidence
        ),
        "ordered_source_ordinal_digest": parts.ordered_source_ordinal_digest,
        "checks": dict(parts.checks),
        "batch_api_actual_http_requests": parts.checks[
            "batch_requests_executed"
        ],
    }


def validate_batch_candidate_release_audit_report(
    report: Mapping[str, Any],
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
    plan_market_type: str,
    storage_generation: str = PTG2_V3_SHARED_GENERATION,
    evaluated_at: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Validate and digest one strict V4 bounded release report."""

    report_by_field = dict(report)
    if set(report_by_field) != REPORT_FIELDS:
        raise ValueError("batch audit report fields are invalid")
    evaluation_time = evaluated_at or datetime.datetime.now(datetime.timezone.utc)
    if evaluation_time.tzinfo is None or evaluation_time.utcoffset() is None:
        evaluation_time = evaluation_time.replace(tzinfo=datetime.timezone.utc)
    coordinates = CandidateReportCoordinates(
        snapshot_id=snapshot_id,
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=plan_market_type,
        storage_generation=storage_generation,
    )
    parts = _validated_report_parts(report_by_field, coordinates, evaluation_time)
    return _attestation_evidence(report_by_field, parts)


__all__ = ["validate_batch_candidate_release_audit_report"]
