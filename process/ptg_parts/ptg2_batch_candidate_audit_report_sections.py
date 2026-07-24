# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate, timing, redaction, and source validation for V4 reports."""

from __future__ import annotations

import datetime
import math
from dataclasses import dataclass
from typing import Any, Mapping

from process.ptg_parts.ptg2_batch_candidate_audit_report_schema import (
    PTG2_BATCH_AUDIT_REPORT_CONTRACT,
    PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION,
    PTG2_BATCH_AUDIT_TOOL,
    PTG2_BATCH_AUDIT_TOOL_VERSION,
    REDACTION_EXCLUSIONS,
    REDACTION_FIELDS,
    REPORT_FUTURE_SKEW_SECONDS,
    SOURCE_FIELDS,
    TARGET_FIELDS,
    report_digest_bytes,
    report_digest_hex,
    report_max_age,
    report_timestamp,
    sha256_text,
    strict_nonnegative_report_int,
    strict_report_mapping,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    PTG2_AUDIT_BATCH_API_PATH,
    PTG2_AUDIT_BATCH_RESPONSE_CONTRACT,
)
from process.ptg_parts.ptg2_candidate_audit_contract import (
    PTG2_FAST_AUDIT_DEADLINE_SECONDS,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_contract import (
    PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_IN_FLIGHT,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUESTS_PER_SECOND,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    validate_provider_identifier_quarantine_evidence,
)
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_SHARED_DENSE_WRITE_GENERATIONS,
    PTG2_V3_SHARED_GENERATION,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_SOURCE_WITNESS_MANIFEST_FIELDS,
    source_witness_manifest_projection,
)


@dataclass(frozen=True)
class CandidateReportCoordinates:
    """Public candidate coordinates covered by the durable request digest."""

    snapshot_id: str
    source_key: str
    plan_id: str
    plan_market_type: str
    storage_generation: str = PTG2_V3_SHARED_GENERATION


@dataclass(frozen=True)
class ValidatedSourceEvidence:
    """Canonical source evidence extracted from one redacted report."""

    witness: Mapping[str, Any]
    source_set_digest: bytes
    source_witness_digest: bytes
    quarantine_evidence: Mapping[str, Any]


def _partitioned_request_start_span(
    report_by_field: Mapping[str, Any],
    *,
    duration_seconds: float,
) -> float:
    """Return a bounded, measured request-start span."""

    checks_by_field = report_by_field.get("checks")
    request_count = (
        checks_by_field.get("batch_requests_executed")
        if isinstance(checks_by_field, Mapping)
        else None
    )
    http_by_field = report_by_field.get("http")
    request_start_span_seconds = (
        http_by_field.get("request_start_span_seconds")
        if isinstance(http_by_field, Mapping)
        else None
    )
    if (
        type(request_count) is not int
        or request_count < 1
        or isinstance(request_start_span_seconds, bool)
        or not isinstance(request_start_span_seconds, (int, float))
        or not math.isfinite(float(request_start_span_seconds))
        or float(request_start_span_seconds) < 0
    ):
        raise ValueError("batch audit report timing is invalid")
    remaining_start_count = request_count - 1
    full_request_waves, trailing_start_count = divmod(
        remaining_start_count,
        PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_IN_FLIGHT,
    )
    maximum_contract_start_span = (
        full_request_waves * PTG2_FAST_AUDIT_DEADLINE_SECONDS
        + trailing_start_count
        / PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUESTS_PER_SECOND
        + 5.0
    )
    normalized_start_span = float(request_start_span_seconds)
    if (
        normalized_start_span > duration_seconds + 1.0
        or normalized_start_span > maximum_contract_start_span
    ):
        raise ValueError("batch audit report timing is invalid")
    return normalized_start_span


def validate_report_timing(
    report_by_field: Mapping[str, Any],
    *,
    evaluated_at: datetime.datetime,
) -> datetime.datetime:
    """Validate duration and freshness, returning the completion timestamp."""

    started_at = report_timestamp(
        report_by_field.get("started_at"),
        field_name="started_at",
    )
    completed_at = report_timestamp(
        report_by_field.get("completed_at"),
        field_name="completed_at",
    )
    duration_seconds = report_by_field.get("duration_seconds")
    batch_by_field = report_by_field.get("batch")
    partitioned = (
        isinstance(batch_by_field, Mapping)
        and batch_by_field.get("request_contract")
        == PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT
    )
    if (
        isinstance(duration_seconds, bool)
        or not isinstance(duration_seconds, (int, float))
        or not math.isfinite(float(duration_seconds))
        or float(duration_seconds) < 0
    ):
        raise ValueError("batch audit report timing is invalid")
    normalized_duration_seconds = float(duration_seconds)
    maximum_duration_seconds = PTG2_FAST_AUDIT_DEADLINE_SECONDS
    if partitioned:
        maximum_duration_seconds += (
            _partitioned_request_start_span(
                report_by_field,
                duration_seconds=normalized_duration_seconds,
            )
            + 5.0
        )
    if (
        normalized_duration_seconds > maximum_duration_seconds
        or started_at > completed_at
        or abs(
            (completed_at - started_at).total_seconds()
            - normalized_duration_seconds
        )
        > 1.0
    ):
        raise ValueError("batch audit report timing is invalid")
    evaluation_time = evaluated_at.astimezone(datetime.timezone.utc)
    if completed_at > evaluation_time + datetime.timedelta(
        seconds=REPORT_FUTURE_SKEW_SECONDS
    ):
        raise ValueError("batch audit report completion time is in the future")
    if completed_at <= evaluation_time - report_max_age():
        raise ValueError("batch audit report is too old for candidate activation")
    return completed_at


def validate_report_header(report_by_field: Mapping[str, Any]) -> str:
    """Validate the V4 tool/runtime header and return its version."""

    harness_by_field = strict_report_mapping(
        report_by_field.get("harness"),
        field_name="harness",
    )
    runtime_by_field = strict_report_mapping(
        report_by_field.get("runtime"),
        field_name="runtime",
    )
    expected_harness_by_field = {
        "name": PTG2_BATCH_AUDIT_TOOL,
        "version": PTG2_BATCH_AUDIT_TOOL_VERSION,
        "contract": PTG2_BATCH_AUDIT_REPORT_CONTRACT,
    }
    if (
        report_by_field.get("schema_version")
        != PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION
        or report_by_field.get("status") != "pass"
        or report_by_field.get("profile") != "release"
        or report_by_field.get("release_profile_enforced") is not True
        or report_by_field.get("release_gate_eligible") is not True
        or harness_by_field != expected_harness_by_field
        or runtime_by_field != {"http_client": "aiohttp", "event_loop": "uvloop"}
    ):
        raise ValueError("batch audit report release contract is invalid")
    return str(harness_by_field["version"])


def validate_report_target(
    report_by_field: Mapping[str, Any],
    coordinates: CandidateReportCoordinates,
) -> None:
    """Validate target hashes and the authenticated transport contract."""

    if coordinates.storage_generation not in PTG2_SHARED_DENSE_WRITE_GENERATIONS:
        raise ValueError("candidate storage generation is unsupported")
    target_by_field = strict_report_mapping(
        report_by_field.get("target"),
        field_name="target",
        expected_fields=TARGET_FIELDS,
    )
    batch_by_field = report_by_field.get("batch")
    partitioned = (
        isinstance(batch_by_field, Mapping)
        and batch_by_field.get("request_contract")
        == PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT
    )
    expected_by_field = {
        "expected_architecture": "postgres_binary_v3",
        "expected_storage_generation": coordinates.storage_generation,
        "expected_database_backend": "postgresql",
        "expected_snapshot_lifecycle": "validated",
        "architecture_assertion": "required_postgresql_session_evidence",
        "api_path_sha256": sha256_text(PTG2_AUDIT_BATCH_API_PATH),
        "endpoint_contract": (
            PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT
            if partitioned
            else PTG2_AUDIT_BATCH_RESPONSE_CONTRACT
        ),
        "snapshot_id_sha256": sha256_text(coordinates.snapshot_id),
        "source_key_sha256": sha256_text(coordinates.source_key),
        "plan_id_sha256": sha256_text(coordinates.plan_id),
        "market_type_sha256": sha256_text(coordinates.plan_market_type),
    }
    if any(
        target_by_field.get(field_name) != expected_value
        for field_name, expected_value in expected_by_field.items()
    ):
        raise ValueError("batch audit report target does not match the candidate")
    report_digest_hex(
        target_by_field.get("api_origin_sha256"),
        field_name="api_origin_sha256",
    )
    transport_contract = target_by_field.get("transport_contract")
    is_tls_verified = target_by_field.get("tls_verified")
    if not (
        (transport_contract == "verified_https_v1" and is_tls_verified is True)
        or (
            transport_contract == "authenticated_cluster_service_v1"
            and is_tls_verified is False
        )
    ):
        raise ValueError("batch audit report transport contract is invalid")


def validate_report_redaction(report_by_field: Mapping[str, Any]) -> None:
    """Require the exact declared sensitive-field exclusion policy."""

    redaction_by_field = strict_report_mapping(
        report_by_field.get("redaction"),
        field_name="redaction",
        expected_fields=REDACTION_FIELDS,
    )
    if (
        redaction_by_field.get("policy") != "sensitive_identifiers_excluded"
        or tuple(redaction_by_field.get("excluded") or ())
        != REDACTION_EXCLUSIONS
    ):
        raise ValueError("batch audit report redaction contract is invalid")


def validated_report_source(
    report_by_field: Mapping[str, Any],
) -> ValidatedSourceEvidence:
    """Validate and return canonical redacted source evidence."""

    source_by_field = strict_report_mapping(
        report_by_field.get("source"),
        field_name="source",
        expected_fields=SOURCE_FIELDS,
    )
    source_count = strict_nonnegative_report_int(
        source_by_field.get("source_count"),
        field_name="source.source_count",
    )
    if source_count < 1:
        raise ValueError("batch audit report source count is invalid")
    source_set_digest_hex = report_digest_hex(
        source_by_field.get("source_set_digest"),
        field_name="source.source_set_digest",
    )
    try:
        witness_by_field = strict_report_mapping(
            source_by_field.get("witness"),
            field_name="source.witness",
            expected_fields=frozenset(PTG2_SOURCE_WITNESS_MANIFEST_FIELDS),
        )
        witness = source_witness_manifest_projection(
            witness_by_field,
            expected_source_count=source_count,
        )
    except ValueError as exc:
        raise ValueError("batch audit report source witness is invalid") from exc
    if witness.get("source_set_digest") != source_set_digest_hex:
        raise ValueError("batch audit report source set is invalid")
    quarantine_evidence = validate_provider_identifier_quarantine_evidence(
        source_by_field.get("provider_identifier_quarantine")
    )
    return ValidatedSourceEvidence(
        witness=witness,
        source_set_digest=bytes.fromhex(source_set_digest_hex),
        source_witness_digest=report_digest_bytes(
            witness.get("payload_sha256"),
            field_name="source.witness.payload_sha256",
        ),
        quarantine_evidence=quarantine_evidence,
    )


__all__ = [
    "CandidateReportCoordinates",
    "ValidatedSourceEvidence",
    "validate_report_header",
    "validate_report_redaction",
    "validate_report_target",
    "validate_report_timing",
    "validated_report_source",
]
