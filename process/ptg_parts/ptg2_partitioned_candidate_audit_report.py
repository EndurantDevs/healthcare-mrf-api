# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Redacted report and request metrics for partitioned candidate audits."""

from __future__ import annotations

import datetime
import time
from dataclasses import dataclass, field
from typing import Any, Mapping
from urllib.parse import urlsplit

from process.ptg_parts.ptg2_batch_candidate_audit_report import (
    BatchAuditReportTarget,
)
from process.ptg_parts.ptg2_batch_candidate_audit_report_schema import (
    PTG2_BATCH_AUDIT_REPORT_CONTRACT,
    PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION,
    PTG2_BATCH_AUDIT_TOOL,
    PTG2_BATCH_AUDIT_TOOL_VERSION,
    REDACTION_EXCLUSIONS,
    sha256_text,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    PTG2_AUDIT_BATCH_API_PATH,
)
from process.ptg_parts.ptg2_candidate_audit_contract import (
    FastAuditHttpConfig,
    validated_public_audit_sample_projection,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_contract import (
    PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUESTS_PER_SECOND,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT,
    PartitionedCandidateAuditAggregate,
    PartitionedCandidateAuditPlan,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_evidence,
)
from process.ptg_parts.ptg2_source_witness import source_set_digest
from process.ptg_parts.ptg2_source_witness_contract import (
    source_witness_manifest_projection,
)


@dataclass
class PartitionedAuditHttpMetrics:
    """Truthful request accounting for one successful plan execution."""

    planned_request_count: int
    started_request_count: int = 0
    completed_request_count: int = 0
    failed_request_count: int = 0
    in_flight: int = 0
    peak_in_flight: int = 0
    start_times: list[float] = field(default_factory=list)

    def started(self) -> None:
        """Record one request start and its resulting concurrency."""

        self.started_request_count += 1
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        self.start_times.append(time.monotonic())

    def completed(self) -> None:
        """Record one successful terminal response."""

        self.completed_request_count += 1
        self.in_flight -= 1

    def failed(self) -> None:
        """Record one failed terminal response."""

        self.failed_request_count += 1
        self.in_flight -= 1

    @property
    def start_span_seconds(self) -> float:
        """Return elapsed time between first and last request starts."""

        if len(self.start_times) < 2:
            return 0.0
        return self.start_times[-1] - self.start_times[0]

    @property
    def actual_start_rate_per_second(self) -> float:
        """Return the measured inter-start throughput."""

        span = self.start_span_seconds
        if span <= 0:
            return 0.0
        return (len(self.start_times) - 1) / span


@dataclass(frozen=True, slots=True)
class PartitionedAuditReportInput:
    """Complete successful evidence used to construct one redacted report."""

    audit_target: BatchAuditReportTarget
    plan: PartitionedCandidateAuditPlan
    aggregate: PartitionedCandidateAuditAggregate
    metrics: PartitionedAuditHttpMetrics
    http_config: FastAuditHttpConfig
    witness_io: Mapping[str, int]
    event_loop_contract: str
    started_at: datetime.datetime
    completed_at: datetime.datetime


def _target_section(report_input: PartitionedAuditReportInput) -> dict[str, Any]:
    parsed_origin = urlsplit(report_input.http_config.api_base_url)
    api_origin = f"{parsed_origin.scheme}://{parsed_origin.netloc}"
    audit_target = report_input.audit_target
    return {
        "expected_architecture": "postgres_binary_v3",
        "expected_storage_generation": audit_target.storage_generation,
        "expected_database_backend": "postgresql",
        "expected_snapshot_lifecycle": "validated",
        "architecture_assertion": "required_postgresql_session_evidence",
        "api_origin_sha256": sha256_text(api_origin),
        "api_path_sha256": sha256_text(PTG2_AUDIT_BATCH_API_PATH),
        "endpoint_contract": PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT,
        "snapshot_id_sha256": sha256_text(audit_target.snapshot_id),
        "source_key_sha256": sha256_text(audit_target.source_key),
        "plan_id_sha256": sha256_text(audit_target.plan_id),
        "market_type_sha256": sha256_text(audit_target.plan_market_type),
        "transport_contract": report_input.http_config.transport_contract,
        "tls_verified": report_input.http_config.verify_tls,
    }


def _source_section(report_input: PartitionedAuditReportInput) -> dict[str, Any]:
    audit_target = report_input.audit_target
    source_count = len(audit_target.raw_container_sha256)
    return {
        "source_count": source_count,
        "source_set_digest": source_set_digest(
            audit_target.raw_container_sha256
        ),
        "witness": source_witness_manifest_projection(
            audit_target.source_witness,
            expected_source_count=source_count,
        ),
        "provider_identifier_quarantine": (
            provider_identifier_quarantine_evidence(
                audit_target.provider_identifier_quarantine
            )
        ),
    }


def _coverage_section(report_input: PartitionedAuditReportInput) -> dict[str, Any]:
    audit_target = report_input.audit_target
    witness = audit_target.source_witness
    return {
        "selection_method": witness["selection_method"],
        "queryable_occurrence_population_count": witness[
            "queryable_occurrence_population_count"
        ],
        "emitted_rate_row_count": witness["emitted_rate_row_count"],
        "unqueryable_rate_row_count": witness["unqueryable_rate_row_count"],
        "unqueryable_rate_policy": witness["unqueryable_rate_policy"],
        "occurrence_sample_count": witness["occurrence_witness_count"],
        "provider_sample_count": witness["provider_witness_count"],
        "unique_source_condition_count": (
            report_input.plan.source_challenge_count
        ),
        "persisted_audit_sample_count": (
            report_input.plan.persisted_occurrence_count
        ),
        "execution_mode": "worker_validated_partitioned_batch_v1",
    }


def _checks_section(report_input: PartitionedAuditReportInput) -> dict[str, int]:
    witness = report_input.audit_target.source_witness
    aggregate = report_input.aggregate
    return {
        "source_witnesses": int(witness["record_count"]),
        "source_occurrence_witnesses_matched": (
            aggregate.source_occurrence_count
        ),
        "unique_source_conditions_executed": aggregate.source_challenge_count,
        "provider_witnesses_validated": int(witness["provider_witness_count"]),
        "persisted_audit_occurrences_validated": (
            aggregate.persisted_occurrence_count
        ),
        "batch_requests_executed": aggregate.request_count,
    }


def _batch_section(report_input: PartitionedAuditReportInput) -> dict[str, Any]:
    plan = report_input.plan
    aggregate = report_input.aggregate
    return {
        "request_contract": PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT,
        "response_contract": PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT,
        "request_digest": plan.plan_digest,
        "ordered_source_ordinal_digest": (
            plan.binding.ordered_source_ordinal_digest
        ),
        "matched_challenge_digest": aggregate.aggregate_digest,
        "challenge_count": aggregate.source_occurrence_count,
        "unique_challenge_count": aggregate.source_challenge_count,
        "matched_challenge_count": aggregate.source_occurrence_count,
        "persisted_audit_occurrence_count": (
            aggregate.persisted_occurrence_count
        ),
        "validated_persisted_audit_occurrence_count": (
            aggregate.persisted_occurrence_count
        ),
        "endpoint_duration_ms": aggregate.maximum_duration_ms,
    }


def _http_section(report_input: PartitionedAuditReportInput) -> dict[str, Any]:
    metrics = report_input.metrics
    return {
        "batch_api_planned_http_requests": metrics.planned_request_count,
        "batch_api_actual_http_requests": metrics.started_request_count,
        "batch_api_completed_http_requests": metrics.completed_request_count,
        "batch_api_failed_http_requests": metrics.failed_request_count,
        "retry_count": 0,
        "max_concurrency": metrics.peak_in_flight,
        "request_start_rate_limit_per_second": (
            PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUESTS_PER_SECOND
        ),
        "request_start_rate_actual_per_second": round(
            metrics.actual_start_rate_per_second,
            6,
        ),
        "request_start_span_seconds": round(
            metrics.start_span_seconds,
            6,
        ),
        "method": "POST",
        "redirect_count": 0,
    }


def _audit_sample_section(
    report_input: PartitionedAuditReportInput,
) -> dict[str, Any]:
    audit_target = report_input.audit_target
    source_count = len(audit_target.raw_container_sha256)
    return {
        **validated_public_audit_sample_projection(
            audit_target.audit_sample,
            expected_source_count=source_count,
        ),
        "sample_digest_validated": True,
        "source_set_validated": True,
    }


def build_partitioned_audit_report(
    report_input: PartitionedAuditReportInput,
) -> dict[str, Any]:
    """Build one complete redacted report with request-throughput evidence."""

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
        "api_audit_sample": _audit_sample_section(report_input),
        "batch": _batch_section(report_input),
        "http": _http_section(report_input),
        "io": {
            "block_io": dict(report_input.aggregate.block_io),
            "witness_io": dict(report_input.witness_io),
            "candidate_processing_io": dict(
                report_input.aggregate.candidate_processing_io
            ),
        },
        "redaction": {
            "policy": "sensitive_identifiers_excluded",
            "excluded": list(REDACTION_EXCLUSIONS),
        },
    }


__all__ = [
    "PartitionedAuditHttpMetrics",
    "PartitionedAuditReportInput",
    "build_partitioned_audit_report",
]
