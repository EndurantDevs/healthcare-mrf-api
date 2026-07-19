# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict field sets and scalar helpers for V4 batch-audit reports."""

from __future__ import annotations

import datetime
import hashlib
import json
import os
from typing import Any, Mapping

from process.ptg_parts.ptg2_candidate_audit_contract import (
    PTG2_PUBLIC_AUDIT_SAMPLE_FIELDS,
)


PTG2_BATCH_AUDIT_ATTESTATION_CONTRACT = (
    "ptg2_v3_release_audit_attestation_v4"
)
PTG2_BATCH_AUDIT_REPORT_CONTRACT = "ptg2_v3_batch_source_witness_audit_v4"
PTG2_BATCH_AUDIT_TOOL = "ptg2_v3_batch_source_witness_audit"
PTG2_BATCH_AUDIT_TOOL_VERSION = "4.0.0"
PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION = 4
REPORT_MAX_AGE_MINUTES_ENV = (
    "HLTHPRT_PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES"
)
REPORT_MAX_AGE_MINUTES_DEFAULT = 120
REPORT_FUTURE_SKEW_SECONDS = 300
REPORT_FIELDS = frozenset(
    {
        "api_audit_sample",
        "batch",
        "checks",
        "completed_at",
        "coverage",
        "duration_seconds",
        "harness",
        "http",
        "io",
        "profile",
        "redaction",
        "release_gate_eligible",
        "release_profile_enforced",
        "runtime",
        "schema_version",
        "source",
        "started_at",
        "status",
        "target",
    }
)
BATCH_FIELDS = frozenset(
    {
        "request_contract",
        "response_contract",
        "request_digest",
        "ordered_source_ordinal_digest",
        "matched_challenge_digest",
        "challenge_count",
        "unique_challenge_count",
        "matched_challenge_count",
        "persisted_audit_occurrence_count",
        "validated_persisted_audit_occurrence_count",
        "endpoint_duration_ms",
    }
)
AUDIT_SAMPLE_FIELDS = frozenset(
    {
        *PTG2_PUBLIC_AUDIT_SAMPLE_FIELDS,
        "sample_digest_validated",
        "source_set_validated",
    }
)
SOURCE_FIELDS = frozenset(
    {
        "source_count",
        "source_set_digest",
        "witness",
        "provider_identifier_quarantine",
    }
)
TARGET_FIELDS = frozenset(
    {
        "expected_architecture",
        "expected_storage_generation",
        "expected_database_backend",
        "expected_snapshot_lifecycle",
        "architecture_assertion",
        "api_origin_sha256",
        "api_path_sha256",
        "endpoint_contract",
        "snapshot_id_sha256",
        "source_key_sha256",
        "plan_id_sha256",
        "market_type_sha256",
        "transport_contract",
        "tls_verified",
    }
)
REDACTION_FIELDS = frozenset({"policy", "excluded"})
COVERAGE_FIELDS = frozenset(
    {
        "selection_method",
        "queryable_occurrence_population_count",
        "emitted_rate_row_count",
        "unqueryable_rate_row_count",
        "unqueryable_rate_policy",
        "occurrence_sample_count",
        "provider_sample_count",
        "unique_source_condition_count",
        "persisted_audit_sample_count",
        "execution_mode",
    }
)
CHECK_FIELDS = frozenset(
    {
        "source_witnesses",
        "source_occurrence_witnesses_matched",
        "unique_source_conditions_executed",
        "provider_witnesses_validated",
        "persisted_audit_occurrences_validated",
        "batch_requests_executed",
    }
)
REDACTION_EXCLUSIONS = (
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


def sha256_text(value: str) -> str:
    """Return the lowercase SHA-256 digest for one public contract value."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_report_bytes(report: Mapping[str, Any]) -> bytes:
    """Serialize a report into its deterministic digest representation."""

    try:
        serialized_report = json.dumps(
            dict(report),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("batch audit report is not canonical JSON") from exc
    return serialized_report.encode("utf-8")


def report_digest_hex(raw_digest: Any, *, field_name: str) -> str:
    """Return one strict lowercase SHA-256 digest."""

    normalized_digest = str(raw_digest or "").strip().lower()
    if len(normalized_digest) != 64:
        raise ValueError(f"batch audit report field {field_name} is invalid")
    try:
        digest_bytes = bytes.fromhex(normalized_digest)
    except ValueError as exc:
        raise ValueError(
            f"batch audit report field {field_name} is invalid"
        ) from exc
    if len(digest_bytes) != 32:
        raise ValueError(f"batch audit report field {field_name} is invalid")
    return normalized_digest


def report_digest_bytes(raw_digest: Any, *, field_name: str) -> bytes:
    """Return strict SHA-256 bytes for one report field."""

    return bytes.fromhex(report_digest_hex(raw_digest, field_name=field_name))


def strict_report_mapping(
    raw_mapping: Any,
    *,
    field_name: str,
    expected_fields: frozenset[str] | None = None,
) -> dict[str, Any]:
    """Return a mapping only when its shape matches the report contract."""

    if not isinstance(raw_mapping, Mapping):
        raise ValueError(f"batch audit report field {field_name} is invalid")
    mapping_by_field = dict(raw_mapping)
    if expected_fields is not None and set(mapping_by_field) != expected_fields:
        raise ValueError(f"batch audit report field {field_name} is invalid")
    return mapping_by_field


def strict_nonnegative_report_int(raw_number: Any, *, field_name: str) -> int:
    """Return a strict nonnegative report counter."""

    if type(raw_number) is not int or raw_number < 0:
        raise ValueError(f"batch audit report field {field_name} is invalid")
    return raw_number


def report_timestamp(
    raw_timestamp: Any,
    *,
    field_name: str,
) -> datetime.datetime:
    """Return one timezone-aware report timestamp in UTC."""

    if not isinstance(raw_timestamp, str) or not raw_timestamp.strip():
        raise ValueError(f"batch audit report field {field_name} is invalid")
    try:
        timestamp = datetime.datetime.fromisoformat(
            raw_timestamp.strip().replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError(
            f"batch audit report field {field_name} is invalid"
        ) from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError(
            f"batch audit report field {field_name} must include a timezone"
        )
    return timestamp.astimezone(datetime.timezone.utc)


def report_max_age() -> datetime.timedelta:
    """Return the configured bounded report freshness window."""

    raw_minutes = os.getenv(REPORT_MAX_AGE_MINUTES_ENV)
    try:
        minutes = int(str(raw_minutes).strip()) if raw_minutes is not None else 0
    except ValueError:
        minutes = 0
    if minutes <= 0:
        minutes = REPORT_MAX_AGE_MINUTES_DEFAULT
    return datetime.timedelta(minutes=min(minutes, 1_440))


__all__ = [
    "AUDIT_SAMPLE_FIELDS",
    "BATCH_FIELDS",
    "CHECK_FIELDS",
    "COVERAGE_FIELDS",
    "PTG2_BATCH_AUDIT_ATTESTATION_CONTRACT",
    "PTG2_BATCH_AUDIT_REPORT_CONTRACT",
    "PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION",
    "PTG2_BATCH_AUDIT_TOOL",
    "PTG2_BATCH_AUDIT_TOOL_VERSION",
    "REDACTION_EXCLUSIONS",
    "REDACTION_FIELDS",
    "REPORT_FIELDS",
    "REPORT_FUTURE_SKEW_SECONDS",
    "SOURCE_FIELDS",
    "TARGET_FIELDS",
    "canonical_report_bytes",
    "report_digest_bytes",
    "report_digest_hex",
    "report_max_age",
    "report_timestamp",
    "sha256_text",
    "strict_nonnegative_report_int",
    "strict_report_mapping",
]
