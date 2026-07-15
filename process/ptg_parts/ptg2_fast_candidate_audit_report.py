# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Redacted release report assembly for bounded PTG V3 candidate audits."""

from __future__ import annotations

import datetime
import hashlib
import math
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

from process.ptg_parts.ptg2_candidate_audit_contract import (
    PTG2_FAST_AUDIT_CONTRACT,
    PTG2_FAST_AUDIT_TOOL,
    PTG2_FAST_AUDIT_TOOL_VERSION,
    FastAuditHttpConfig,
    FastAuditTarget,
)
from process.ptg_parts.ptg2_source_witness import LoadedSourceWitness
from scripts.validation import ptg2_v3_source_api_audit as source_audit


_REDACTION_EXCLUSIONS = (
    "source_paths",
    "source_file_names",
    "raw_source_hashes",
    "source_trace_URLs",
    "plan_and_snapshot_values",
    "auth_values",
    "HTTP_bodies",
    "network_names",
    "arbitrary_source_and_API_strings",
)


@dataclass
class FastAuditHttpMetrics:
    """Bounded request counters retained for the redacted audit report."""

    request_count: int = 0
    retry_count: int = 0
    latencies_ms: list[float] = field(default_factory=list)


@dataclass(frozen=True)
class FastAuditReportInput:
    """All validated values required to render one audit report."""

    witness: LoadedSourceWitness
    audit_target: FastAuditTarget
    http_config: FastAuditHttpConfig
    audit_sample: Mapping[str, Any]
    http_metrics: FastAuditHttpMetrics
    challenge_count: int
    provider_count: int
    concurrency: int
    event_loop_contract: str
    started_at: datetime.datetime
    completed_at: datetime.datetime


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _percentile(metric_values: Sequence[float], percentile: float) -> float:
    if not metric_values:
        return 0.0
    ordered_values = sorted(float(metric_value) for metric_value in metric_values)
    rank = max(math.ceil(percentile * len(ordered_values)) - 1, 0)
    return round(ordered_values[min(rank, len(ordered_values) - 1)], 3)


def _target_section(report_input: FastAuditReportInput) -> dict[str, Any]:
    audit_target = report_input.audit_target
    http_config = report_input.http_config
    origin = urlsplit(http_config.api_base_url)
    api_origin = f"{origin.scheme}://{origin.netloc}"
    return {
        "expected_architecture": source_audit.EXPECTED_ARCHITECTURE,
        "expected_storage_generation": source_audit.EXPECTED_STORAGE_GENERATION,
        "expected_database_backend": source_audit.EXPECTED_DATABASE_BACKEND,
        "expected_snapshot_lifecycle": "validated",
        "architecture_assertion": "required_postgresql_session_evidence",
        "api_origin_sha256": _sha256_text(api_origin),
        "api_path_sha256": _sha256_text(source_audit.DEFAULT_CANDIDATE_API_PATH),
        "api_audit_path_sha256": _sha256_text(source_audit.DEFAULT_API_AUDIT_PATH),
        "endpoint_contract": "pricing.providers.search_by_procedure",
        "audit_endpoint_contract": source_audit.AUDIT_SAMPLE_CONTRACT,
        "snapshot_id_sha256": _sha256_text(audit_target.snapshot_id),
        "source_key_sha256": _sha256_text(audit_target.source_key),
        "plan_id_sha256": _sha256_text(audit_target.plan_id),
        "market_type_sha256": _sha256_text(audit_target.plan_market_type),
        "transport_contract": http_config.transport_contract,
        "tls_verified": http_config.verify_tls,
    }


def _source_and_coverage_sections(
    report_input: FastAuditReportInput,
) -> dict[str, dict[str, Any]]:
    witness_metadata = report_input.witness.metadata
    audit_target = report_input.audit_target
    return {
        "source": {
            "source_count": audit_target.source_count,
            "source_set_digest": audit_target.source_set_digest,
            "witness": dict(witness_metadata),
            "provider_identifier_quarantine": dict(
                audit_target.provider_identifier_quarantine
            ),
        },
        "coverage": {
            "failures": [],
            "selection_method": witness_metadata["selection_method"],
            "queryable_occurrence_population_count": witness_metadata[
                "queryable_occurrence_population_count"
            ],
            "emitted_rate_row_count": witness_metadata["emitted_rate_row_count"],
            "unqueryable_rate_row_count": witness_metadata[
                "unqueryable_rate_row_count"
            ],
            "unqueryable_rate_policy": witness_metadata["unqueryable_rate_policy"],
            "occurrence_sample_count": report_input.challenge_count,
            "provider_sample_count": report_input.provider_count,
        },
    }


def _request_sections(report_input: FastAuditReportInput) -> dict[str, dict[str, Any]]:
    http_metrics = report_input.http_metrics
    challenge_count = report_input.challenge_count
    provider_count = report_input.provider_count
    return {
        "checks": {
            "source_witnesses": challenge_count + provider_count,
            "api_witnesses_matched": challenge_count,
            "api_challenges_executed": challenge_count,
            "provider_witnesses_validated": provider_count,
            "api_audit_occurrences_validated": 1,
        },
        "http": {
            "standard_api_actual_http_requests": http_metrics.request_count,
            "retry_count": http_metrics.retry_count,
            "max_concurrency": report_input.concurrency,
        },
        "latency": {
            "request_p50_ms": _percentile(http_metrics.latencies_ms, 0.50),
            "request_p95_ms": _percentile(http_metrics.latencies_ms, 0.95),
            "request_max_ms": round(max(http_metrics.latencies_ms, default=0.0), 3),
        },
        "random_api_requests": {
            "requested": challenge_count,
            "executed": challenge_count,
        },
    }


def build_fast_audit_report(report_input: FastAuditReportInput) -> dict[str, Any]:
    """Render the complete redacted report from already validated evidence."""

    duration_seconds = (
        report_input.completed_at - report_input.started_at
    ).total_seconds()
    report_by_section: dict[str, Any] = {
        "schema_version": 3,
        "harness": {
            "name": PTG2_FAST_AUDIT_TOOL,
            "version": PTG2_FAST_AUDIT_TOOL_VERSION,
            "contract": PTG2_FAST_AUDIT_CONTRACT,
        },
        "runtime": {
            "http_client": "aiohttp",
            "event_loop": report_input.event_loop_contract,
        },
        "profile": "release",
        "status": "pass",
        "release_profile_enforced": True,
        "release_gate_eligible": True,
        "started_at": report_input.started_at.isoformat(),
        "completed_at": report_input.completed_at.isoformat(),
        "duration_seconds": round(duration_seconds, 6),
        "target": _target_section(report_input),
        "api_audit_sample": {
            **dict(report_input.audit_sample),
            "sample_digest_validated": True,
            "source_set_validated": True,
        },
        "failures": {"counts": {}, "examples": []},
        "reproducibility": {
            "selection_method": report_input.witness.metadata["selection_method"],
            "source_witness_sample_digest": report_input.witness.metadata[
                "sample_digest"
            ],
        },
        "redaction": {
            "policy": "sensitive_identifiers_excluded",
            "excluded": list(_REDACTION_EXCLUSIONS),
        },
        "limitations": [
            "bounded_deterministic_source_sample_not_complete_source_reparse",
            "network_names_from_unselected_provider_references_are_subset_checked",
        ],
    }
    report_by_section.update(_source_and_coverage_sections(report_input))
    report_by_section.update(_request_sections(report_input))
    return report_by_section


__all__ = [
    "FastAuditHttpMetrics",
    "FastAuditReportInput",
    "build_fast_audit_report",
]
