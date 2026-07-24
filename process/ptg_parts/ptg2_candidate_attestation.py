# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable release-audit attestations for strict V3 candidate activation."""

from __future__ import annotations

import datetime
import hashlib
import json
import math
import os
from dataclasses import dataclass
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.domain import PTG2_CANDIDATE_ACTIVATION_CONTRACT
from process.ptg_parts.ptg2_candidate_audit_contract import (
    PTG2_FAST_AUDIT_CONTRACT,
    PTG2_FAST_AUDIT_DEADLINE_SECONDS,
    PTG2_FAST_AUDIT_REQUEST_P95_CEILING_MS,
    PTG2_FAST_AUDIT_TOOL,
    PTG2_FAST_AUDIT_TOOL_VERSION,
    validated_public_audit_sample_projection,
)
from process.ptg_parts.ptg2_batch_candidate_audit_report import (
    PTG2_BATCH_AUDIT_ATTESTATION_CONTRACT,
    PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION,
    PTG2_BATCH_AUDIT_TOOL,
    validate_batch_candidate_release_audit_report,
)
from process.ptg_parts.ptg2_shared_reuse import (
    PTG2_V3_SOURCE_SET_CONTRACT,
    shared_source_set_metadata,
)
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_SHARED_DENSE_WRITE_GENERATIONS,
    PTG2_V3_SHARED_GENERATION,
)
from process.ptg_parts.ptg2_shared_source_set import (
    ordered_source_ordinal_digest,
)
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_evidence,
    validate_provider_identifier_quarantine,
    validate_provider_identifier_quarantine_evidence,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    source_witness_manifest_projection,
    validate_source_witness_manifest,
)


PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3 = (
    "ptg2_v3_release_audit_attestation_v3"
)
PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4 = (
    PTG2_BATCH_AUDIT_ATTESTATION_CONTRACT
)
PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT = (
    PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
)
PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS = (
    PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4,
    PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3,
)
# Compatibility alias for callers that record the current writer contract.
# Readers continue accepting every explicitly SUPPORTED rolling contract.
PTG2_CANDIDATE_ATTESTATION_CONTRACT = (
    PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT
)
PTG2_CANDIDATE_AUDIT_TOOL = PTG2_FAST_AUDIT_TOOL
PTG2_CANDIDATE_AUDIT_TOOL_VERSION = PTG2_FAST_AUDIT_TOOL_VERSION
PTG2_VERIFIED_HTTPS_TRANSPORT = "verified_https_v1"
PTG2_TRUSTED_CLUSTER_HTTP_TRANSPORT = "authenticated_cluster_service_v1"
PTG2_CANDIDATE_API_PATH = "/api/v1/pricing/providers/audit-search-by-procedure"
PTG2_CANDIDATE_OCCURRENCE_API_PATH = "/api/v1/pricing/providers/audit-occurrences"
PTG2_CANDIDATE_ATTESTATION_TTL_HOURS_ENV = (
    "HLTHPRT_PTG2_CANDIDATE_ATTESTATION_TTL_HOURS"
)
PTG2_CANDIDATE_ATTESTATION_TTL_HOURS_DEFAULT = 24
PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES_ENV = (
    "HLTHPRT_PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES"
)


class CandidateAttestationWriterContractError(ValueError):
    """Report is readable, but this rollout must not persist its contract."""

    control_error_code = "candidate_attestation_writer_contract_mismatch"
    retryable = False


PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES_DEFAULT = 120
PTG2_CANDIDATE_AUDIT_REPORT_FUTURE_SKEW_SECONDS = 300

_REQUIRED_REPORT_TOP_LEVEL_KEYS = {
    "api_audit_sample",
    "checks",
    "completed_at",
    "coverage",
    "duration_seconds",
    "failures",
    "harness",
    "http",
    "latency",
    "limitations",
    "profile",
    "random_api_requests",
    "redaction",
    "release_gate_eligible",
    "release_profile_enforced",
    "reproducibility",
    "runtime",
    "schema_version",
    "source",
    "started_at",
    "status",
    "target",
}
_REQUIRED_REDACTION_EXCLUSIONS = (
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


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, Mapping):
        return dict(row)
    return dict(getattr(row, "_mapping", row))


def _canonical_report_bytes(report: Mapping[str, Any]) -> bytes:
    try:
        serialized = json.dumps(
            report,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("audit report is not canonical JSON") from exc
    return serialized.encode("utf-8")


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_digest(value: Any, *, field: str) -> bytes:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64:
        raise ValueError(f"audit report field {field} is invalid")
    try:
        digest = bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError(f"audit report field {field} is invalid") from exc
    if len(digest) != 32:
        raise ValueError(f"audit report field {field} is invalid")
    return digest


def _sha256_hex(value: Any, *, field: str) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        normalized = bytes(value).hex()
    else:
        normalized = str(value or "").strip().lower()
    _sha256_digest(normalized, field=field)
    return normalized


def _report_timestamp(value: Any, *, field: str) -> datetime.datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"audit report field {field} is invalid")
    normalized = value.strip().replace("Z", "+00:00")
    try:
        timestamp = datetime.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"audit report field {field} is invalid") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError(f"audit report field {field} must include a timezone")
    return timestamp.astimezone(datetime.timezone.utc)


def _audit_report_max_age() -> datetime.timedelta:
    raw = os.getenv(PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES_ENV)
    try:
        minutes = int(str(raw).strip()) if raw is not None else 0
    except ValueError:
        minutes = 0
    if minutes <= 0:
        minutes = PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES_DEFAULT
    return datetime.timedelta(minutes=min(minutes, 1_440))


def _required_count(mapping: Mapping[str, Any], key: str, minimum: int) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"audit report check {key} is invalid")
    if value < minimum:
        raise ValueError(f"audit report check {key} is below its release minimum")
    return value


def _required_report_mapping(
    report: Mapping[str, Any],
    key: str,
) -> dict[str, Any]:
    value = report.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"audit report field {key} is invalid")
    return dict(value)


@dataclass(frozen=True)
class _V3ReportSections:
    report_by_field: Mapping[str, Any]
    harness_by_field: Mapping[str, Any]
    runtime_by_field: Mapping[str, Any]
    target_by_field: Mapping[str, Any]
    failures_by_field: Mapping[str, Any]
    failure_counts_by_name: Mapping[str, Any]
    coverage_by_field: Mapping[str, Any]
    checks_by_name: Mapping[str, Any]
    http_by_field: Mapping[str, Any]
    latency_by_field: Mapping[str, Any]
    random_requests_by_field: Mapping[str, Any]
    audit_sample_by_field: Mapping[str, Any]
    redaction_by_field: Mapping[str, Any]
    source_by_field: Mapping[str, Any]


@dataclass(frozen=True)
class _V3WitnessEvidence:
    witness_by_field: Mapping[str, Any]
    expected_challenge_count: int
    provider_witness_count: int
    checks_by_name: Mapping[str, int]
    source_set_digest: bytes
    source_witness_digest: bytes
    quarantine_by_field: Mapping[str, Any]


def _v3_report_sections(report: Mapping[str, Any]) -> _V3ReportSections:
    report_by_field = dict(report)
    report_keys = set(report_by_field)
    if report_keys != _REQUIRED_REPORT_TOP_LEVEL_KEYS:
        missing_fields = sorted(_REQUIRED_REPORT_TOP_LEVEL_KEYS - report_keys)
        unsupported_fields = sorted(report_keys - _REQUIRED_REPORT_TOP_LEVEL_KEYS)
        detail = "missing=" + ",".join(missing_fields) if missing_fields else ""
        if unsupported_fields:
            detail += ("; " if detail else "") + "unsupported=" + ",".join(
                unsupported_fields
            )
        raise ValueError(f"audit report fields are invalid ({detail})")
    failures_by_field = _required_report_mapping(report_by_field, "failures")
    return _V3ReportSections(
        report_by_field=report_by_field,
        harness_by_field=_required_report_mapping(report_by_field, "harness"),
        runtime_by_field=_required_report_mapping(report_by_field, "runtime"),
        target_by_field=_required_report_mapping(report_by_field, "target"),
        failures_by_field=failures_by_field,
        failure_counts_by_name=_required_report_mapping(
            failures_by_field,
            "counts",
        ),
        coverage_by_field=_required_report_mapping(report_by_field, "coverage"),
        checks_by_name=_required_report_mapping(report_by_field, "checks"),
        http_by_field=_required_report_mapping(report_by_field, "http"),
        latency_by_field=_required_report_mapping(report_by_field, "latency"),
        random_requests_by_field=_required_report_mapping(
            report_by_field,
            "random_api_requests",
        ),
        audit_sample_by_field=_required_report_mapping(
            report_by_field,
            "api_audit_sample",
        ),
        redaction_by_field=_required_report_mapping(report_by_field, "redaction"),
        source_by_field=_required_report_mapping(report_by_field, "source"),
    )


def _validated_v3_report_time(
    report_by_field: Mapping[str, Any],
    evaluated_at: datetime.datetime | None,
) -> datetime.datetime:
    evaluation_time = evaluated_at or datetime.datetime.now(datetime.timezone.utc)
    if evaluation_time.tzinfo is None or evaluation_time.utcoffset() is None:
        evaluation_time = evaluation_time.replace(tzinfo=datetime.timezone.utc)
    evaluation_time = evaluation_time.astimezone(datetime.timezone.utc)
    started_at = _report_timestamp(
        report_by_field.get("started_at"),
        field="started_at",
    )
    completed_at = _report_timestamp(
        report_by_field.get("completed_at"),
        field="completed_at",
    )
    duration_seconds = report_by_field.get("duration_seconds")
    if (
        isinstance(duration_seconds, bool)
        or not isinstance(duration_seconds, (int, float))
        or not math.isfinite(float(duration_seconds))
        or float(duration_seconds) < 0
        or float(duration_seconds) > PTG2_FAST_AUDIT_DEADLINE_SECONDS
        or started_at > completed_at
        or abs(
            (completed_at - started_at).total_seconds()
            - float(duration_seconds)
        )
        > 1.0
    ):
        raise ValueError("audit report timing is invalid")
    if completed_at > evaluation_time + datetime.timedelta(
        seconds=PTG2_CANDIDATE_AUDIT_REPORT_FUTURE_SKEW_SECONDS
    ):
        raise ValueError("audit report completion time is in the future")
    if completed_at <= evaluation_time - _audit_report_max_age():
        raise ValueError("audit report is too old for candidate activation")
    return completed_at


def _validated_v3_tool_version(sections: _V3ReportSections) -> str:
    report_by_field = sections.report_by_field
    harness_by_field = sections.harness_by_field
    if report_by_field.get("schema_version") != 3:
        raise ValueError("audit report schema is unsupported")
    if harness_by_field.get("name") != PTG2_CANDIDATE_AUDIT_TOOL:
        raise ValueError("audit report tool is unsupported")
    if harness_by_field.get("contract") != PTG2_FAST_AUDIT_CONTRACT:
        raise ValueError("audit report contract is unsupported")
    tool_version = str(harness_by_field.get("version") or "").strip()
    if tool_version != PTG2_CANDIDATE_AUDIT_TOOL_VERSION:
        raise ValueError("audit report tool version is invalid")
    if sections.runtime_by_field != {
        "http_client": "aiohttp",
        "event_loop": "uvloop",
    }:
        raise ValueError("audit report async runtime is invalid")
    if (
        report_by_field.get("status") != "pass"
        or report_by_field.get("profile") != "release"
        or report_by_field.get("release_profile_enforced") is not True
        or report_by_field.get("release_gate_eligible") is not True
        or sections.failure_counts_by_name
        or not isinstance(sections.failures_by_field.get("examples"), list)
        or sections.failures_by_field["examples"]
        or not isinstance(sections.coverage_by_field.get("failures"), list)
        or sections.coverage_by_field["failures"]
    ):
        raise ValueError("audit report did not pass the release gate")
    request_p95_ms = sections.latency_by_field.get("request_p95_ms")
    request_p95_ceiling_ms = sections.latency_by_field.get(
        "request_p95_ceiling_ms"
    )
    if (
        isinstance(request_p95_ms, bool)
        or not isinstance(request_p95_ms, (int, float))
        or not math.isfinite(float(request_p95_ms))
        or float(request_p95_ms) < 0
        or float(request_p95_ms) > PTG2_FAST_AUDIT_REQUEST_P95_CEILING_MS
        or request_p95_ceiling_ms != PTG2_FAST_AUDIT_REQUEST_P95_CEILING_MS
        or sections.latency_by_field.get("request_p95_within_ceiling") is not True
    ):
        raise ValueError("audit report latency did not pass the release gate")
    return tool_version


def _validate_v3_target(
    target_by_field: Mapping[str, Any],
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
    plan_market_type: str,
) -> None:
    expected_target_by_field = {
        "expected_architecture": "postgres_binary_v3",
        "expected_storage_generation": "shared_blocks_v3",
        "expected_database_backend": "postgresql",
        "expected_snapshot_lifecycle": "validated",
        "architecture_assertion": "required_postgresql_session_evidence",
        "api_path_sha256": _sha256_text(PTG2_CANDIDATE_API_PATH),
        "api_audit_path_sha256": _sha256_text(PTG2_CANDIDATE_OCCURRENCE_API_PATH),
        "endpoint_contract": "pricing.providers.search_by_procedure",
        "audit_endpoint_contract": "persisted_served_occurrence_sample_v2",
        "snapshot_id_sha256": _sha256_text(snapshot_id),
        "source_key_sha256": _sha256_text(source_key),
        "plan_id_sha256": _sha256_text(plan_id),
        "market_type_sha256": _sha256_text(plan_market_type),
    }
    for field_name, expected_value in expected_target_by_field.items():
        if target_by_field.get(field_name) != expected_value:
            raise ValueError(
                f"audit report target {field_name} does not match the candidate"
            )
    transport_contract = target_by_field.get("transport_contract")
    is_tls_verified = target_by_field.get("tls_verified")
    if not (
        (
            transport_contract == PTG2_VERIFIED_HTTPS_TRANSPORT
            and is_tls_verified is True
        )
        or (
            transport_contract == PTG2_TRUSTED_CLUSTER_HTTP_TRANSPORT
            and is_tls_verified is False
        )
    ):
        raise ValueError("audit report transport contract is invalid")


def _validate_v3_redaction(redaction_by_field: Mapping[str, Any]) -> None:
    if (
        redaction_by_field.get("policy") != "sensitive_identifiers_excluded"
        or not isinstance(redaction_by_field.get("excluded"), list)
        or tuple(redaction_by_field["excluded"])
        != _REQUIRED_REDACTION_EXCLUSIONS
    ):
        raise ValueError("audit report redaction contract is invalid")


def _validated_v3_checks(
    checks_by_name: Mapping[str, Any],
    witness_by_field: Mapping[str, Any],
) -> dict[str, int]:
    expected_challenge_count = int(witness_by_field["occurrence_witness_count"])
    provider_witness_count = int(witness_by_field["provider_witness_count"])
    expected_checks_by_name = {
        "source_witnesses": int(witness_by_field["record_count"]),
        "api_witnesses_matched": expected_challenge_count,
        "api_challenges_executed": expected_challenge_count,
        "provider_witnesses_validated": provider_witness_count,
        "api_audit_occurrences_validated": 1,
    }
    observed_checks_by_name = {
        check_name: _required_count(
            checks_by_name,
            check_name,
            expected_count,
        )
        for check_name, expected_count in expected_checks_by_name.items()
    }
    if observed_checks_by_name != expected_checks_by_name:
        raise ValueError("audit report source-witness coverage is invalid")
    return observed_checks_by_name


def _validated_v3_witness(
    sections: _V3ReportSections,
) -> _V3WitnessEvidence:
    source_count = _required_count(sections.source_by_field, "source_count", 1)
    try:
        witness_by_field = validate_source_witness_manifest(
            sections.source_by_field.get("witness"),
            expected_source_count=source_count,
        )
    except ValueError as exc:
        raise ValueError("audit report source-witness coverage is invalid") from exc
    source_set_digest = _sha256_digest(
        sections.source_by_field.get("source_set_digest"),
        field="source.source_set_digest",
    )
    if witness_by_field["source_set_digest"] != sections.source_by_field.get(
        "source_set_digest"
    ):
        raise ValueError("audit report source-witness source set is invalid")
    source_witness_digest = _sha256_digest(
        witness_by_field.get("payload_sha256"),
        field="source.witness.payload_sha256",
    )
    _sha256_digest(
        witness_by_field.get("sample_digest"),
        field="source.witness.sample_digest",
    )
    return _V3WitnessEvidence(
        witness_by_field=witness_by_field,
        expected_challenge_count=int(
            witness_by_field["occurrence_witness_count"]
        ),
        provider_witness_count=int(witness_by_field["provider_witness_count"]),
        checks_by_name=_validated_v3_checks(
            sections.checks_by_name,
            witness_by_field,
        ),
        source_set_digest=source_set_digest,
        source_witness_digest=source_witness_digest,
        quarantine_by_field=validate_provider_identifier_quarantine(
            sections.source_by_field.get("provider_identifier_quarantine")
        ),
    )


def _validated_v3_http_requests(
    sections: _V3ReportSections,
    witness_evidence: _V3WitnessEvidence,
) -> int:
    expected_challenge_count = witness_evidence.expected_challenge_count
    witness_by_field = witness_evidence.witness_by_field
    standard_http_requests = _required_count(
        sections.http_by_field,
        "standard_api_actual_http_requests",
        expected_challenge_count + 1,
    )
    if (
        sections.coverage_by_field.get("selection_method")
        != PTG2_V3_SOURCE_WITNESS_SELECTION
        or sections.coverage_by_field.get(
            "queryable_occurrence_population_count"
        )
        != witness_by_field["queryable_occurrence_population_count"]
        or sections.coverage_by_field.get("emitted_rate_row_count")
        != witness_by_field["emitted_rate_row_count"]
        or sections.coverage_by_field.get("unqueryable_rate_row_count")
        != witness_by_field["unqueryable_rate_row_count"]
        or sections.coverage_by_field.get("unqueryable_rate_policy")
        != witness_by_field["unqueryable_rate_policy"]
        or sections.coverage_by_field.get("occurrence_sample_count")
        != expected_challenge_count
        or sections.coverage_by_field.get("provider_sample_count")
        != witness_evidence.provider_witness_count
        or sections.random_requests_by_field.get("requested")
        != expected_challenge_count
        or sections.random_requests_by_field.get("executed")
        != expected_challenge_count
        or sections.http_by_field.get("max_concurrency") != 32
        or _required_count(sections.http_by_field, "retry_count", 0)
        > standard_http_requests
        or standard_http_requests > expected_challenge_count * 16 + 2
        or sections.audit_sample_by_field.get("sample_digest_validated") is not True
        or sections.audit_sample_by_field.get("source_set_validated") is not True
    ):
        raise ValueError("audit report bounded coverage is invalid")
    return standard_http_requests


def _validate_v3_release_report(
    report: Mapping[str, Any],
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
    plan_market_type: str,
    evaluated_at: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Validate and digest one redacted release report for an exact candidate."""

    sections = _v3_report_sections(report)
    completed_at = _validated_v3_report_time(
        sections.report_by_field,
        evaluated_at,
    )
    tool_version = _validated_v3_tool_version(sections)
    _validate_v3_target(
        sections.target_by_field,
        snapshot_id=snapshot_id,
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=plan_market_type,
    )
    _validate_v3_redaction(sections.redaction_by_field)
    witness_evidence = _validated_v3_witness(sections)
    standard_http_requests = _validated_v3_http_requests(
        sections,
        witness_evidence,
    )
    audit_sample_digest = _sha256_digest(
        sections.audit_sample_by_field.get("sample_digest"),
        field="api_audit_sample.sample_digest",
    )
    report_bytes = _canonical_report_bytes(sections.report_by_field)
    return {
        "contract": PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3,
        "tool_name": PTG2_CANDIDATE_AUDIT_TOOL,
        "tool_version": tool_version,
        "report_digest": hashlib.sha256(report_bytes).digest(),
        "report_json": report_bytes.decode("utf-8"),
        "completed_at": completed_at,
        "audit_sample_digest": audit_sample_digest,
        "source_set_digest": witness_evidence.source_set_digest,
        "source_witness_digest": witness_evidence.source_witness_digest,
        "provider_identifier_quarantine": witness_evidence.quarantine_by_field,
        "checks": witness_evidence.checks_by_name,
        "standard_api_actual_http_requests": standard_http_requests,
    }


def validate_candidate_release_audit_report(
    report: Mapping[str, Any],
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
    plan_market_type: str,
    storage_generation: str = PTG2_V3_SHARED_GENERATION,
    evaluated_at: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Dispatch strict V3 and V4 release reports without rewriting history."""

    if report.get("schema_version") == PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION:
        return validate_batch_candidate_release_audit_report(
            report,
            snapshot_id=snapshot_id,
            source_key=source_key,
            plan_id=plan_id,
            plan_market_type=plan_market_type,
            storage_generation=storage_generation,
            evaluated_at=evaluated_at,
        )
    if storage_generation != PTG2_V3_SHARED_GENERATION:
        raise ValueError(
            "legacy candidate audit reports require shared_blocks_v3"
        )
    return _validate_v3_release_report(
        report,
        snapshot_id=snapshot_id,
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=plan_market_type,
        evaluated_at=evaluated_at,
    )


def _require_current_candidate_attestation_writer(
    evidence: Mapping[str, Any],
) -> None:
    """Reject writes from contracts that this rollout only knows how to read."""

    report_contract = str(evidence.get("contract") or "").strip()
    if report_contract != PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT:
        raise CandidateAttestationWriterContractError(
            "candidate audit report contract is not enabled for writes"
        )


def _attestation_ttl_hours() -> int:
    raw = os.getenv(PTG2_CANDIDATE_ATTESTATION_TTL_HOURS_ENV)
    try:
        value = int(str(raw).strip()) if raw is not None else 0
    except ValueError:
        value = 0
    if value <= 0:
        value = PTG2_CANDIDATE_ATTESTATION_TTL_HOURS_DEFAULT
    return min(value, 168)


@dataclass(frozen=True)
class _CandidatePhysicalIdentity:
    raw_container_hashes: tuple[str, ...]
    source_count: int
    source_set_digest: bytes
    coverage_scope_id: bytes


def _validated_candidate_physical_identity(
    database_row: Mapping[str, Any],
    serving_index_by_field: Mapping[str, Any],
    layout_serving_index_by_field: Mapping[str, Any],
) -> _CandidatePhysicalIdentity:
    source_set_by_field = _mapping(serving_index_by_field.get("source_set"))
    raw_container_hashes = tuple(
        _sha256_hex(raw_digest, field="candidate raw container digest")
        for raw_digest in list(database_row.get("raw_container_sha256_values") or [])
    )
    observed_source_set_by_field = shared_source_set_metadata(
        raw_container_hashes
    )
    source_set_digest_hex = str(
        source_set_by_field.get("raw_container_sha256_digest") or ""
    ).strip().lower()
    coverage_scope_hex = str(
        serving_index_by_field.get("coverage_scope_id") or ""
    ).strip().lower()
    if len(source_set_digest_hex) != 64 or len(coverage_scope_hex) != 64:
        raise ValueError("candidate manifest is missing immutable audit identity")
    try:
        source_set_digest = bytes.fromhex(source_set_digest_hex)
        coverage_scope_id = bytes.fromhex(coverage_scope_hex)
    except ValueError as exc:
        raise ValueError("candidate manifest audit identity is malformed") from exc
    source_count = int(observed_source_set_by_field["source_count"])
    if (
        source_set_by_field.get("contract") != PTG2_V3_SOURCE_SET_CONTRACT
        or int(source_set_by_field.get("source_count") or -1) != source_count
        or source_set_digest
        != bytes.fromhex(
            observed_source_set_by_field["raw_container_sha256_digest"]
        )
        or coverage_scope_id
        != bytes(database_row.get("coverage_scope_id") or b"")
    ):
        raise ValueError("candidate manifest disagrees with its PostgreSQL bindings")
    if (
        str(layout_serving_index_by_field.get("coverage_scope_id") or "")
        .strip()
        .lower()
        != coverage_scope_hex
        or int(layout_serving_index_by_field.get("source_count") or -1)
        != source_count
    ):
        raise ValueError("sealed layout disagrees with the candidate physical scope")
    return _CandidatePhysicalIdentity(
        raw_container_hashes=raw_container_hashes,
        source_count=source_count,
        source_set_digest=source_set_digest,
        coverage_scope_id=coverage_scope_id,
    )


def _validated_candidate_audit_sample(
    snapshot_sample_by_field: Mapping[str, Any],
    layout_sample_by_field: Mapping[str, Any],
    source_count: int,
) -> tuple[dict[str, Any], bytes]:
    try:
        snapshot_public_sample_by_field = (
            validated_public_audit_sample_projection(
                snapshot_sample_by_field,
                expected_source_count=source_count,
            )
        )
        layout_public_sample_by_field = validated_public_audit_sample_projection(
            layout_sample_by_field,
            expected_source_count=source_count,
        )
    except ValueError as exc:
        raise ValueError("candidate audit sample is incompatible") from exc
    snapshot_sample_digest = _sha256_digest(
        snapshot_public_sample_by_field.get("sample_digest"),
        field="candidate snapshot audit sample digest",
    )
    layout_sample_digest = _sha256_digest(
        layout_public_sample_by_field.get("sample_digest"),
        field="candidate layout audit sample digest",
    )
    if (
        snapshot_public_sample_by_field != layout_public_sample_by_field
        or snapshot_sample_digest != layout_sample_digest
    ):
        raise ValueError("candidate audit sample changed after layout sealing")
    return snapshot_public_sample_by_field, layout_sample_digest


def _validated_candidate_source_witness(
    snapshot_witness_by_field: Mapping[str, Any],
    layout_witness_by_field: Mapping[str, Any],
    *,
    source_count: int,
    source_set_digest_hex: str,
) -> tuple[dict[str, Any], bytes]:
    if snapshot_witness_by_field != layout_witness_by_field:
        raise ValueError("candidate source witness changed after layout sealing")
    try:
        validated_witness_by_field = validate_source_witness_manifest(
            snapshot_witness_by_field,
            expected_source_count=source_count,
        )
    except ValueError as exc:
        raise ValueError("candidate source witness is incompatible") from exc
    if validated_witness_by_field.get("source_set_digest") != source_set_digest_hex:
        raise ValueError("candidate source witness changed after layout sealing")
    source_witness_digest = _sha256_digest(
        validated_witness_by_field.get("payload_sha256"),
        field="candidate source witness payload digest",
    )
    return (
        source_witness_manifest_projection(
            validated_witness_by_field,
            expected_source_count=source_count,
        ),
        source_witness_digest,
    )


def _validated_candidate_quarantine(
    serving_index_by_field: Mapping[str, Any],
    layout_serving_index_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    snapshot_quarantine_by_field = validate_provider_identifier_quarantine(
        serving_index_by_field.get("provider_identifier_quarantine")
    )
    layout_quarantine_by_field = validate_provider_identifier_quarantine(
        layout_serving_index_by_field.get("provider_identifier_quarantine")
    )
    if snapshot_quarantine_by_field != layout_quarantine_by_field:
        raise ValueError(
            "candidate provider identifier quarantine changed after layout sealing"
        )
    return layout_quarantine_by_field


def _candidate_identity(database_row: Mapping[str, Any]) -> dict[str, Any]:
    """Validate candidate bindings and return their immutable audit identity."""

    snapshot_by_field = _mapping(database_row.get("manifest"))
    activation_by_field = _mapping(snapshot_by_field.get("activation"))
    layout_manifest_by_field = _mapping(database_row.get("layout_manifest"))
    serving_index_by_field = _mapping(snapshot_by_field.get("serving_index"))
    if (
        str(database_row.get("status") or "") != "validated"
        or activation_by_field.get("contract")
        != PTG2_CANDIDATE_ACTIVATION_CONTRACT
        or activation_by_field.get("state") != "validated"
    ):
        raise ValueError("snapshot is not a strict V3 validated candidate")
    layout_serving_index_by_field = _mapping(
        layout_manifest_by_field.get("serving_index")
    )
    storage_generation = str(
        database_row.get("storage_generation")
        or serving_index_by_field.get("storage_generation")
        or layout_serving_index_by_field.get("storage_generation")
        or PTG2_V3_SHARED_GENERATION
    ).strip()
    if (
        storage_generation not in PTG2_SHARED_DENSE_WRITE_GENERATIONS
        or serving_index_by_field.get(
            "storage_generation",
            storage_generation,
        )
        != storage_generation
        or layout_serving_index_by_field.get(
            "storage_generation",
            storage_generation,
        )
        != storage_generation
    ):
        raise ValueError(
            "candidate storage generation changed after layout sealing"
        )
    physical_identity = _validated_candidate_physical_identity(
        database_row,
        serving_index_by_field,
        layout_serving_index_by_field,
    )
    quarantine_by_field = _validated_candidate_quarantine(
        serving_index_by_field,
        layout_serving_index_by_field,
    )
    audit_sample_by_field, audit_sample_digest = (
        _validated_candidate_audit_sample(
            _mapping(serving_index_by_field.get("audit_sample")),
            _mapping(layout_serving_index_by_field.get("audit_sample")),
            physical_identity.source_count,
        )
    )
    source_witness_by_field, source_witness_digest = (
        _validated_candidate_source_witness(
            _mapping(serving_index_by_field.get("source_witness")),
            _mapping(layout_serving_index_by_field.get("source_witness")),
            source_count=physical_identity.source_count,
            source_set_digest_hex=physical_identity.source_set_digest.hex(),
        )
    )
    return {
        "snapshot_key": int(database_row["snapshot_key"]),
        "source_key": str(
            activation_by_field.get("source_key") or ""
        ).strip().lower(),
        "plan_id": str(database_row.get("plan_id") or "").strip(),
        "plan_market_type": str(database_row.get("plan_market_type") or "").strip().lower(),
        "coverage_scope_id": physical_identity.coverage_scope_id,
        "storage_generation": storage_generation,
        "source_set_digest": physical_identity.source_set_digest,
        "ordered_source_ordinal_digest": ordered_source_ordinal_digest(
            physical_identity.raw_container_hashes
        ),
        "source_witness_manifest": source_witness_by_field,
        "audit_sample_public": audit_sample_by_field,
        "source_witness_digest": source_witness_digest,
        "audit_sample_digest": audit_sample_digest,
        "provider_identifier_quarantine": quarantine_by_field,
        "provider_identifier_quarantine_evidence": (
            provider_identifier_quarantine_evidence(quarantine_by_field)
        ),
    }


async def _locked_candidate_identity(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
) -> dict[str, Any]:
    query_result = await session.execute(
        db.text(
            f"""
            SELECT snapshot.status,
                   snapshot.manifest,
                   binding.snapshot_key,
                   scope.plan_id,
                   scope.plan_market_type,
                   scope.coverage_scope_id,
                   layout.generation AS storage_generation,
                   layout.layout_manifest,
                   ARRAY(
                       SELECT source.raw_container_sha256
                         FROM {_quote_ident(schema_name)}.ptg2_v3_snapshot_source source
                        WHERE source.snapshot_id = snapshot.snapshot_id
                        ORDER BY source.source_key
                   ) AS raw_container_sha256_values
              FROM {_quote_ident(schema_name)}.ptg2_snapshot snapshot
              JOIN {_quote_ident(schema_name)}.ptg2_v3_snapshot_binding binding
                ON binding.snapshot_id = snapshot.snapshot_id
              JOIN {_quote_ident(schema_name)}.ptg2_v3_snapshot_scope scope
                ON scope.snapshot_id = snapshot.snapshot_id
              JOIN {_quote_ident(schema_name)}.ptg2_v3_snapshot_layout layout
                ON layout.snapshot_key = binding.snapshot_key
             WHERE snapshot.snapshot_id = :snapshot_id
               AND layout.state = 'sealed'
               AND layout.generation = ANY(
                   CAST(:storage_generations AS text[])
               )
             FOR UPDATE OF snapshot
            """
        ),
        {
            "snapshot_id": snapshot_id,
            "storage_generations": sorted(
                PTG2_SHARED_DENSE_WRITE_GENERATIONS
            ),
        },
    )
    candidate_row = query_result.one_or_none()
    if candidate_row is None:
        raise ValueError("validated candidate is unavailable")
    return _candidate_identity(_row_mapping(candidate_row))


async def _database_timestamp(session: Any) -> datetime.datetime:
    result = await session.execute(db.text("SELECT clock_timestamp()"))
    timestamp = result.scalar_one()
    if not isinstance(timestamp, datetime.datetime):
        raise RuntimeError("PostgreSQL did not return an attestation timestamp")
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
    return timestamp.astimezone(datetime.timezone.utc)


async def record_candidate_audit_attestation(
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
    plan_market_type: str,
    storage_generation: str = PTG2_V3_SHARED_GENERATION,
    report: Mapping[str, Any],
) -> dict[str, Any]:
    """Persist a passing release report against the candidate's immutable identity."""

    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_source_key = str(source_key or "").strip().lower()
    normalized_plan_id = str(plan_id or "").strip()
    normalized_market_type = str(plan_market_type or "").strip().lower()
    normalized_storage_generation = str(
        storage_generation or ""
    ).strip().lower()
    if not all(
        (
            normalized_snapshot_id,
            normalized_source_key,
            normalized_plan_id,
            normalized_market_type,
        )
    ):
        raise ValueError("snapshot, source, plan, and market are required")
    if (
        normalized_storage_generation
        not in PTG2_SHARED_DENSE_WRITE_GENERATIONS
    ):
        raise ValueError("candidate storage generation is unsupported")
    preflight_now = datetime.datetime.now(datetime.timezone.utc)
    evidence = validate_candidate_release_audit_report(
        report,
        snapshot_id=normalized_snapshot_id,
        source_key=normalized_source_key,
        plan_id=normalized_plan_id,
        plan_market_type=normalized_market_type,
        storage_generation=normalized_storage_generation,
        evaluated_at=preflight_now,
    )
    _require_current_candidate_attestation_writer(evidence)
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    async with db.transaction() as session:
        await acquire_ptg2_lifecycle_lock(session)
        now = await _database_timestamp(session)
        evidence = validate_candidate_release_audit_report(
            report,
            snapshot_id=normalized_snapshot_id,
            source_key=normalized_source_key,
            plan_id=normalized_plan_id,
            plan_market_type=normalized_market_type,
            storage_generation=normalized_storage_generation,
            evaluated_at=now,
        )
        _require_current_candidate_attestation_writer(evidence)
        expires_at = min(
            now + datetime.timedelta(hours=_attestation_ttl_hours()),
            evidence["completed_at"] + _audit_report_max_age(),
        )
        if expires_at <= now:
            raise ValueError("audit report is too old for candidate activation")
        identity = await _locked_candidate_identity(
            session,
            schema_name=schema_name,
            snapshot_id=normalized_snapshot_id,
        )
        if (
            identity["source_key"] != normalized_source_key
            or identity["plan_id"] != normalized_plan_id
            or identity["plan_market_type"] != normalized_market_type
            or identity.get(
                "storage_generation",
                PTG2_V3_SHARED_GENERATION,
            )
            != normalized_storage_generation
        ):
            raise ValueError("audit target does not match the candidate bindings")
        if evidence["audit_sample_digest"] != identity["audit_sample_digest"]:
            raise ValueError("audit report does not match the sealed candidate sample")
        if evidence["source_set_digest"] != identity["source_set_digest"]:
            raise ValueError("audit report does not match the sealed candidate sources")
        if (
            evidence["contract"] == PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
            and evidence.get("ordered_source_ordinal_digest")
            != identity["ordered_source_ordinal_digest"]
        ):
            raise ValueError(
                "audit report does not match the sealed candidate source ordinals"
            )
        if evidence["contract"] == PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4 and (
            evidence.get("source_witness_manifest")
            != identity["source_witness_manifest"]
            or evidence.get("audit_sample_public")
            != identity["audit_sample_public"]
        ):
            raise ValueError(
                "audit report metadata does not match the sealed candidate"
            )
        if evidence["source_witness_digest"] != identity["source_witness_digest"]:
            raise ValueError("audit report does not match the sealed source witnesses")
        if evidence["contract"] == PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4:
            is_quarantine_match = (
                evidence.get("provider_identifier_quarantine_evidence")
                == identity["provider_identifier_quarantine_evidence"]
            )
        else:
            is_quarantine_match = (
                evidence["provider_identifier_quarantine"]
                == identity["provider_identifier_quarantine"]
            )
        if not is_quarantine_match:
            raise ValueError(
                "audit report provider identifier quarantine does not match the sealed candidate"
            )
        insert_result = await session.execute(
            db.text(
                f"""
                INSERT INTO {_quote_ident(schema_name)}.ptg2_v3_candidate_audit_attestation AS attestation
                    (snapshot_id, snapshot_key, source_key, plan_id,
                     plan_market_type, coverage_scope_id, source_set_digest,
                     audit_sample_digest, source_witness_digest,
                     contract, tool_name, tool_version, report_digest, report,
                     attested_at, expires_at, activated_at)
                VALUES
                    (:snapshot_id, :snapshot_key, :source_key, :plan_id,
                     :plan_market_type, :coverage_scope_id, :source_set_digest,
                     :audit_sample_digest, :source_witness_digest,
                     :contract, :tool_name, :tool_version, :report_digest,
                     CAST(:report_json AS jsonb), :attested_at, :expires_at, NULL)
                ON CONFLICT (snapshot_id) DO UPDATE SET
                    contract = EXCLUDED.contract,
                    tool_name = EXCLUDED.tool_name,
                    tool_version = EXCLUDED.tool_version,
                    report_digest = EXCLUDED.report_digest,
                    report = EXCLUDED.report,
                    attested_at = EXCLUDED.attested_at,
                    expires_at = EXCLUDED.expires_at
                WHERE attestation.snapshot_key = EXCLUDED.snapshot_key
                  AND attestation.source_key = EXCLUDED.source_key
                  AND attestation.plan_id = EXCLUDED.plan_id
                  AND attestation.plan_market_type = EXCLUDED.plan_market_type
                  AND attestation.coverage_scope_id = EXCLUDED.coverage_scope_id
                  AND attestation.source_set_digest = EXCLUDED.source_set_digest
                  AND attestation.audit_sample_digest = EXCLUDED.audit_sample_digest
                  AND attestation.source_witness_digest = EXCLUDED.source_witness_digest
                  AND attestation.activated_at IS NULL
                  AND (
                      (
                          attestation.contract = EXCLUDED.contract
                          AND attestation.tool_name = EXCLUDED.tool_name
                      )
                      OR (
                          attestation.contract = :v3_contract
                          AND attestation.tool_name = :v3_tool_name
                          AND EXCLUDED.contract = :v4_contract
                          AND EXCLUDED.tool_name = :v4_tool_name
                      )
                  )
                RETURNING report_digest
                """
            ),
            {
                "snapshot_id": normalized_snapshot_id,
                **identity,
                "contract": evidence["contract"],
                "tool_name": evidence["tool_name"],
                "tool_version": evidence["tool_version"],
                "report_digest": evidence["report_digest"],
                "report_json": evidence["report_json"],
                "audit_sample_digest": evidence["audit_sample_digest"],
                "source_witness_digest": evidence["source_witness_digest"],
                "attested_at": now,
                "expires_at": expires_at,
                "v3_contract": PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3,
                "v3_tool_name": PTG2_FAST_AUDIT_TOOL,
                "v4_contract": PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4,
                "v4_tool_name": PTG2_BATCH_AUDIT_TOOL,
            },
        )
        if insert_result.first() is None:
            raise ValueError("candidate audit attestation conflicts with existing evidence")
    attestation_result_by_field = {
        "status": "attested",
        "snapshot_id": normalized_snapshot_id,
        "contract": evidence["contract"],
        "tool_version": evidence["tool_version"],
        "report_digest": evidence["report_digest"].hex(),
        "expires_at": expires_at.isoformat(),
        "checks": evidence["checks"],
    }
    for request_metric_name in (
        "standard_api_actual_http_requests",
        "batch_api_actual_http_requests",
    ):
        if request_metric_name in evidence:
            attestation_result_by_field[request_metric_name] = evidence[
                request_metric_name
            ]
    return attestation_result_by_field


async def verify_candidate_audit_attestation_in_transaction(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    snapshot_key: int,
    source_key: str,
    plan_id: str,
    plan_market_type: str,
    coverage_scope_id: bytes,
) -> bytes:
    """Lock and verify unexpired evidence before atomic pointer activation."""

    identity = await _locked_candidate_identity(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
    )
    expected_identity_by_field = {
        "snapshot_key": int(snapshot_key),
        "source_key": str(source_key).strip().lower(),
        "plan_id": str(plan_id).strip(),
        "plan_market_type": str(plan_market_type).strip().lower(),
        "coverage_scope_id": bytes(coverage_scope_id),
    }
    if any(
        identity[key] != expected_value
        for key, expected_value in expected_identity_by_field.items()
    ):
        raise ValueError("candidate identity changed after its release audit")
    query_result = await session.execute(
        db.text(
            f"""
            SELECT report_digest, report
              FROM {_quote_ident(schema_name)}.ptg2_v3_candidate_audit_attestation
             WHERE snapshot_id = :snapshot_id
               AND snapshot_key = :snapshot_key
               AND source_key = :source_key
               AND plan_id = :plan_id
               AND plan_market_type = :plan_market_type
               AND coverage_scope_id = :coverage_scope_id
               AND source_set_digest = :source_set_digest
               AND audit_sample_digest = :audit_sample_digest
               AND source_witness_digest = :source_witness_digest
               AND contract = ANY(CAST(:supported_contracts AS text[]))
               AND activated_at IS NULL
               AND expires_at > clock_timestamp()
             FOR UPDATE
            """
        ),
        {
            "snapshot_id": snapshot_id,
            "snapshot_key": int(snapshot_key),
            "source_key": str(source_key).strip().lower(),
            "plan_id": str(plan_id).strip(),
            "plan_market_type": str(plan_market_type).strip().lower(),
            "coverage_scope_id": bytes(coverage_scope_id),
            "source_set_digest": identity["source_set_digest"],
            "audit_sample_digest": identity["audit_sample_digest"],
            "source_witness_digest": identity["source_witness_digest"],
            "supported_contracts": list(
                PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
            ),
        },
    )
    attestation_row = query_result.first()
    if attestation_row is None:
        raise ValueError("candidate has no current passing release audit attestation")
    report_digest = bytes(attestation_row[0])
    if len(report_digest) != 32:
        raise ValueError("candidate audit attestation digest is invalid")
    stored_report = _mapping(attestation_row[1])
    observed_report_digest = hashlib.sha256(
        _canonical_report_bytes(stored_report)
    ).digest()
    if observed_report_digest != report_digest:
        raise ValueError("candidate audit attestation report changed after validation")
    report_source = _required_report_mapping(stored_report, "source")
    is_v4_report = (
        stored_report.get("schema_version")
        == PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION
    )
    if is_v4_report:
        is_report_quarantine_match = (
            validate_provider_identifier_quarantine_evidence(
                report_source.get("provider_identifier_quarantine")
            )
            == identity["provider_identifier_quarantine_evidence"]
        )
    else:
        is_report_quarantine_match = (
            validate_provider_identifier_quarantine(
                report_source.get("provider_identifier_quarantine")
            )
            == identity["provider_identifier_quarantine"]
        )
    if not is_report_quarantine_match:
        raise ValueError(
            "candidate provider identifier quarantine changed after its release audit"
        )
    report_witness = _required_report_mapping(report_source, "witness")
    if _sha256_digest(
        report_witness.get("payload_sha256"),
        field="source.witness.payload_sha256",
    ) != identity["source_witness_digest"]:
        raise ValueError(
            "candidate source witness changed after its release audit"
        )
    if is_v4_report:
        report_batch = _required_report_mapping(stored_report, "batch")
        if report_batch.get("ordered_source_ordinal_digest") != identity.get(
            "ordered_source_ordinal_digest"
        ):
            raise ValueError(
                "candidate source ordinals changed after its release audit"
            )
        report_audit_sample = validated_public_audit_sample_projection(
            _required_report_mapping(stored_report, "api_audit_sample"),
            expected_source_count=int(
                identity["source_witness_manifest"]["source_count"]
            ),
        )
        if (
            report_witness != identity.get("source_witness_manifest")
            or report_audit_sample != identity.get("audit_sample_public")
        ):
            raise ValueError(
                "candidate audit metadata changed after its release audit"
            )
    return report_digest


async def consume_candidate_audit_attestation_in_transaction(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    report_digest: bytes,
    activated_at: datetime.datetime,
) -> None:
    """Mark the exact locked attestation consumed by the successful activation."""

    if activated_at.tzinfo is None:
        activated_at = activated_at.replace(tzinfo=datetime.timezone.utc)

    update_result = await session.execute(
        db.text(
            f"""
            UPDATE {_quote_ident(schema_name)}.ptg2_v3_candidate_audit_attestation
               SET activated_at = :activated_at
             WHERE snapshot_id = :snapshot_id
               AND report_digest = :report_digest
               AND activated_at IS NULL
               AND expires_at > clock_timestamp()
            RETURNING snapshot_id
            """
        ),
        {
            "snapshot_id": snapshot_id,
            "report_digest": bytes(report_digest),
            "activated_at": activated_at,
        },
    )
    if update_result.first() is None:
        raise RuntimeError("candidate audit attestation changed during activation")
