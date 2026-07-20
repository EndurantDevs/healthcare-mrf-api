# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Truthful redacted V4 attestation reports for bounded batch audits."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlsplit

from process.ptg_parts.ptg2_batch_candidate_audit_report_schema import (
    PTG2_BATCH_AUDIT_ATTESTATION_CONTRACT,
    PTG2_BATCH_AUDIT_REPORT_CONTRACT,
    PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION,
    PTG2_BATCH_AUDIT_TOOL,
    PTG2_BATCH_AUDIT_TOOL_VERSION,
    REDACTION_EXCLUSIONS,
    sha256_text,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchRequest,
    AuditBatchResponse,
    PTG2_AUDIT_BATCH_API_PATH,
    PTG2_AUDIT_BATCH_REQUEST_CONTRACT,
    PTG2_AUDIT_BATCH_RESPONSE_CONTRACT,
)
from process.ptg_parts.ptg2_candidate_audit_contract import (
    FastAuditHttpConfig,
    validated_public_audit_sample_projection,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_evidence,
)
from process.ptg_parts.ptg2_source_witness import source_set_digest
from process.ptg_parts.ptg2_source_witness_contract import (
    source_witness_manifest_projection,
)

@dataclass(frozen=True)
class BatchAuditReportTarget:
    """Sealed candidate metadata retained without raw source identities."""

    snapshot_id: str
    source_key: str
    plan_id: str
    plan_market_type: str
    raw_container_sha256: tuple[str, ...]
    source_witness: Mapping[str, Any]
    audit_sample: Mapping[str, Any]
    provider_identifier_quarantine: Mapping[str, Any]


@dataclass(frozen=True)
class BatchAuditReportInput:
    """Validated request, response, runtime, and sealed candidate bindings."""

    target: BatchAuditReportTarget
    request: AuditBatchRequest
    response: AuditBatchResponse
    http_config: FastAuditHttpConfig
    event_loop_contract: str
    started_at: datetime.datetime
    completed_at: datetime.datetime


def _target_section(report_input: BatchAuditReportInput) -> dict[str, Any]:
    target = report_input.target
    http_config = report_input.http_config
    parsed_origin = urlsplit(http_config.api_base_url)
    api_origin = f"{parsed_origin.scheme}://{parsed_origin.netloc}"
    return {
        "expected_architecture": "postgres_binary_v3",
        "expected_storage_generation": "shared_blocks_v3",
        "expected_database_backend": "postgresql",
        "expected_snapshot_lifecycle": "validated",
        "architecture_assertion": "required_postgresql_session_evidence",
        "api_origin_sha256": sha256_text(api_origin),
        "api_path_sha256": sha256_text(PTG2_AUDIT_BATCH_API_PATH),
        "endpoint_contract": PTG2_AUDIT_BATCH_RESPONSE_CONTRACT,
        "snapshot_id_sha256": sha256_text(target.snapshot_id),
        "source_key_sha256": sha256_text(target.source_key),
        "plan_id_sha256": sha256_text(target.plan_id),
        "market_type_sha256": sha256_text(target.plan_market_type),
        "transport_contract": http_config.transport_contract,
        "tls_verified": http_config.verify_tls,
    }


def _source_section(report_input: BatchAuditReportInput) -> dict[str, Any]:
    target = report_input.target
    source_count = len(target.raw_container_sha256)
    return {
        "source_count": source_count,
        "source_set_digest": source_set_digest(target.raw_container_sha256),
        "witness": source_witness_manifest_projection(
            target.source_witness,
            expected_source_count=source_count,
        ),
        "provider_identifier_quarantine": provider_identifier_quarantine_evidence(
            target.provider_identifier_quarantine
        ),
    }


def _coverage_section(report_input: BatchAuditReportInput) -> dict[str, Any]:
    witness_by_field = report_input.target.source_witness
    return {
        "selection_method": witness_by_field["selection_method"],
        "queryable_occurrence_population_count": witness_by_field[
            "queryable_occurrence_population_count"
        ],
        "emitted_rate_row_count": witness_by_field["emitted_rate_row_count"],
        "unqueryable_rate_row_count": witness_by_field[
            "unqueryable_rate_row_count"
        ],
        "unqueryable_rate_policy": witness_by_field[
            "unqueryable_rate_policy"
        ],
        "occurrence_sample_count": witness_by_field[
            "occurrence_witness_count"
        ],
        "provider_sample_count": witness_by_field["provider_witness_count"],
        "unique_source_condition_count": (
            report_input.response.unique_challenge_count
        ),
        "persisted_audit_sample_count": (
            report_input.response.persisted_audit_occurrence_count
        ),
        "execution_mode": "server_derived_one_request_batch",
    }


def _checks_section(report_input: BatchAuditReportInput) -> dict[str, int]:
    witness_by_field = report_input.target.source_witness
    response = report_input.response
    return {
        "source_witnesses": int(witness_by_field["record_count"]),
        "source_occurrence_witnesses_matched": (
            response.matched_challenge_count
        ),
        "unique_source_conditions_executed": response.unique_challenge_count,
        "provider_witnesses_validated": int(
            witness_by_field["provider_witness_count"]
        ),
        "persisted_audit_occurrences_validated": (
            response.validated_persisted_audit_occurrence_count
        ),
        "batch_requests_executed": 1,
    }


def _batch_section(report_input: BatchAuditReportInput) -> dict[str, Any]:
    request = report_input.request
    response = report_input.response
    return {
        "request_contract": PTG2_AUDIT_BATCH_REQUEST_CONTRACT,
        "response_contract": PTG2_AUDIT_BATCH_RESPONSE_CONTRACT,
        "request_digest": request.request_digest,
        "ordered_source_ordinal_digest": (
            request.ordered_source_ordinal_digest
        ),
        "matched_challenge_digest": response.matched_challenge_digest,
        "challenge_count": response.challenge_count,
        "unique_challenge_count": response.unique_challenge_count,
        "matched_challenge_count": response.matched_challenge_count,
        "persisted_audit_occurrence_count": (
            response.persisted_audit_occurrence_count
        ),
        "validated_persisted_audit_occurrence_count": (
            response.validated_persisted_audit_occurrence_count
        ),
        "endpoint_duration_ms": response.duration_ms,
    }


def build_batch_audit_report(report_input: BatchAuditReportInput) -> dict[str, Any]:
    """Build one pass-only report from a strictly validated batch response."""

    duration_seconds = (
        report_input.completed_at - report_input.started_at
    ).total_seconds()
    return {
        "schema_version": PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION,
        "harness": {
            "name": PTG2_BATCH_AUDIT_TOOL,
            "version": PTG2_BATCH_AUDIT_TOOL_VERSION,
            "contract": PTG2_BATCH_AUDIT_REPORT_CONTRACT,
        },
        "runtime": {
            "http_client": "aiohttp",
            "event_loop": report_input.event_loop_contract,
        },
        "profile": "release",
        "release_profile_enforced": True,
        "status": "pass",
        "release_gate_eligible": True,
        "started_at": report_input.started_at.isoformat(),
        "completed_at": report_input.completed_at.isoformat(),
        "duration_seconds": round(duration_seconds, 6),
        "target": _target_section(report_input),
        "source": _source_section(report_input),
        "coverage": _coverage_section(report_input),
        "checks": _checks_section(report_input),
        "api_audit_sample": {
            **validated_public_audit_sample_projection(
                report_input.target.audit_sample,
                expected_source_count=len(
                    report_input.target.raw_container_sha256
                ),
            ),
            "sample_digest_validated": True,
            "source_set_validated": True,
        },
        "batch": _batch_section(report_input),
        "http": {
            "batch_api_actual_http_requests": 1,
            "retry_count": 0,
            "max_concurrency": 1,
            "method": "POST",
            "redirect_count": 0,
        },
        "io": {
            "block_io": dict(report_input.response.block_io),
            "witness_io": dict(report_input.response.witness_io),
            "candidate_processing_io": dict(
                report_input.response.candidate_processing_io
            ),
        },
        "redaction": {
            "policy": "sensitive_identifiers_excluded",
            "excluded": list(REDACTION_EXCLUSIONS),
        },
    }


def validate_batch_candidate_release_audit_report(
    report: Mapping[str, Any],
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
    plan_market_type: str,
    evaluated_at: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Validate one V4 report through the bounded companion validator."""

    from process.ptg_parts.ptg2_batch_candidate_audit_report_validation import (
        validate_batch_candidate_release_audit_report as validate_report,
    )

    return validate_report(
        report,
        snapshot_id=snapshot_id,
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=plan_market_type,
        evaluated_at=evaluated_at,
    )


__all__ = [
    "BatchAuditReportInput",
    "BatchAuditReportTarget",
    "PTG2_BATCH_AUDIT_ATTESTATION_CONTRACT",
    "PTG2_BATCH_AUDIT_REPORT_CONTRACT",
    "PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION",
    "PTG2_BATCH_AUDIT_TOOL",
    "PTG2_BATCH_AUDIT_TOOL_VERSION",
    "build_batch_audit_report",
    "validate_batch_candidate_release_audit_report",
]
