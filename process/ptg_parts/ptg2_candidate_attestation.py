# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable release-audit attestations for strict V3 candidate activation."""

from __future__ import annotations

import datetime
import hashlib
import json
import math
import os
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
)
from process.ptg_parts.ptg2_shared_reuse import (
    PTG2_V3_SOURCE_SET_CONTRACT,
    shared_source_set_metadata,
)
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock
from process.ptg_parts.ptg2_provider_quarantine import (
    validate_provider_identifier_quarantine,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    validate_source_witness_manifest,
)


PTG2_CANDIDATE_ATTESTATION_CONTRACT = "ptg2_v3_release_audit_attestation_v2"
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


def validate_candidate_release_audit_report(
    report: Mapping[str, Any],
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
    plan_market_type: str,
    evaluated_at: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Validate and digest one redacted release report for an exact candidate."""

    report_map = dict(report)
    report_keys = set(report_map)
    if report_keys != _REQUIRED_REPORT_TOP_LEVEL_KEYS:
        missing = sorted(_REQUIRED_REPORT_TOP_LEVEL_KEYS - report_keys)
        unsupported = sorted(report_keys - _REQUIRED_REPORT_TOP_LEVEL_KEYS)
        detail = "missing=" + ",".join(missing) if missing else ""
        if unsupported:
            detail += ("; " if detail else "") + "unsupported=" + ",".join(
                unsupported
            )
        raise ValueError(f"audit report fields are invalid ({detail})")
    harness = _required_report_mapping(report_map, "harness")
    runtime = _required_report_mapping(report_map, "runtime")
    target_mapping = _required_report_mapping(report_map, "target")
    failures = _required_report_mapping(report_map, "failures")
    failure_counts = _required_report_mapping(failures, "counts")
    coverage = _required_report_mapping(report_map, "coverage")
    checks = _required_report_mapping(report_map, "checks")
    http = _required_report_mapping(report_map, "http")
    latency = _required_report_mapping(report_map, "latency")
    random_api_requests = _required_report_mapping(
        report_map,
        "random_api_requests",
    )
    audit_sample = _required_report_mapping(report_map, "api_audit_sample")
    redaction = _required_report_mapping(report_map, "redaction")
    source_mapping = _required_report_mapping(report_map, "source")
    provider_identifier_quarantine = validate_provider_identifier_quarantine(
        source_mapping.get("provider_identifier_quarantine")
    )
    evaluation_time = evaluated_at or datetime.datetime.now(datetime.timezone.utc)
    if evaluation_time.tzinfo is None or evaluation_time.utcoffset() is None:
        evaluation_time = evaluation_time.replace(tzinfo=datetime.timezone.utc)
    evaluation_time = evaluation_time.astimezone(datetime.timezone.utc)
    started_at = _report_timestamp(report_map.get("started_at"), field="started_at")
    completed_at = _report_timestamp(
        report_map.get("completed_at"), field="completed_at"
    )
    duration_seconds = report_map.get("duration_seconds")
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
    if report_map.get("schema_version") != 3:
        raise ValueError("audit report schema is unsupported")
    if harness.get("name") != PTG2_CANDIDATE_AUDIT_TOOL:
        raise ValueError("audit report tool is unsupported")
    if harness.get("contract") != PTG2_FAST_AUDIT_CONTRACT:
        raise ValueError("audit report contract is unsupported")
    tool_version = str(harness.get("version") or "").strip()
    if tool_version != PTG2_CANDIDATE_AUDIT_TOOL_VERSION:
        raise ValueError("audit report tool version is invalid")
    if runtime != {"http_client": "aiohttp", "event_loop": "uvloop"}:
        raise ValueError("audit report async runtime is invalid")
    if (
        report_map.get("status") != "pass"
        or report_map.get("profile") != "release"
        or report_map.get("release_profile_enforced") is not True
        or report_map.get("release_gate_eligible") is not True
        or failure_counts
        or not isinstance(failures.get("examples"), list)
        or failures["examples"]
        or not isinstance(coverage.get("failures"), list)
        or coverage["failures"]
    ):
        raise ValueError("audit report did not pass the release gate")
    request_p95_ms = latency.get("request_p95_ms")
    request_p95_ceiling_ms = latency.get("request_p95_ceiling_ms")
    if (
        isinstance(request_p95_ms, bool)
        or not isinstance(request_p95_ms, (int, float))
        or not math.isfinite(float(request_p95_ms))
        or float(request_p95_ms) < 0
        or float(request_p95_ms) > PTG2_FAST_AUDIT_REQUEST_P95_CEILING_MS
        or request_p95_ceiling_ms != PTG2_FAST_AUDIT_REQUEST_P95_CEILING_MS
        or latency.get("request_p95_within_ceiling") is not True
    ):
        raise ValueError("audit report latency did not pass the release gate")
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
    for key, expected_value in expected_target_by_field.items():
        if target_mapping.get(key) != expected_value:
            raise ValueError(f"audit report target {key} does not match the candidate")
    transport_contract = target_mapping.get("transport_contract")
    tls_verified = target_mapping.get("tls_verified")
    if not (
        (
            transport_contract == PTG2_VERIFIED_HTTPS_TRANSPORT
            and tls_verified is True
        )
        or (
            transport_contract == PTG2_TRUSTED_CLUSTER_HTTP_TRANSPORT
            and tls_verified is False
        )
    ):
        raise ValueError("audit report transport contract is invalid")
    if (
        redaction.get("policy") != "sensitive_identifiers_excluded"
        or not isinstance(redaction.get("excluded"), list)
        or tuple(redaction["excluded"]) != _REQUIRED_REDACTION_EXCLUSIONS
    ):
        raise ValueError("audit report redaction contract is invalid")
    source_count = _required_count(source_mapping, "source_count", 1)
    try:
        source_witness = validate_source_witness_manifest(
            source_mapping.get("witness"),
            expected_source_count=source_count,
        )
    except ValueError as exc:
        raise ValueError("audit report source-witness coverage is invalid") from exc
    expected_challenge_count = int(source_witness["occurrence_witness_count"])
    provider_witness_count = int(source_witness["provider_witness_count"])
    total_witness_count = int(source_witness["record_count"])
    observed_check_by_name = {
        "source_witnesses": _required_count(
            checks,
            "source_witnesses",
            total_witness_count,
        ),
        "api_witnesses_matched": _required_count(
            checks,
            "api_witnesses_matched",
            expected_challenge_count,
        ),
        "api_challenges_executed": _required_count(
            checks,
            "api_challenges_executed",
            expected_challenge_count,
        ),
        "provider_witnesses_validated": _required_count(
            checks,
            "provider_witnesses_validated",
            provider_witness_count,
        ),
        "api_audit_occurrences_validated": _required_count(
            checks,
            "api_audit_occurrences_validated",
            1,
        ),
    }
    expected_check_by_name = {
        "source_witnesses": total_witness_count,
        "api_witnesses_matched": expected_challenge_count,
        "api_challenges_executed": expected_challenge_count,
        "provider_witnesses_validated": provider_witness_count,
        "api_audit_occurrences_validated": 1,
    }
    if observed_check_by_name != expected_check_by_name:
        raise ValueError("audit report source-witness coverage is invalid")
    source_set_digest = _sha256_digest(
        source_mapping.get("source_set_digest"),
        field="source.source_set_digest",
    )
    if source_witness["source_set_digest"] != source_mapping.get("source_set_digest"):
        raise ValueError("audit report source-witness source set is invalid")
    source_witness_digest = _sha256_digest(
        source_witness.get("payload_sha256"),
        field="source.witness.payload_sha256",
    )
    _sha256_digest(
        source_witness.get("sample_digest"),
        field="source.witness.sample_digest",
    )
    standard_http_requests = _required_count(
        http,
        "standard_api_actual_http_requests",
        expected_challenge_count + 1,
    )
    if (
        coverage.get("selection_method") != PTG2_V3_SOURCE_WITNESS_SELECTION
        or coverage.get("queryable_occurrence_population_count")
        != source_witness["queryable_occurrence_population_count"]
        or coverage.get("emitted_rate_row_count")
        != source_witness["emitted_rate_row_count"]
        or coverage.get("unqueryable_rate_row_count")
        != source_witness["unqueryable_rate_row_count"]
        or coverage.get("unqueryable_rate_policy")
        != source_witness["unqueryable_rate_policy"]
        or coverage.get("occurrence_sample_count") != expected_challenge_count
        or coverage.get("provider_sample_count") != provider_witness_count
        or random_api_requests.get("requested") != expected_challenge_count
        or random_api_requests.get("executed") != expected_challenge_count
        or http.get("max_concurrency") != 32
        or _required_count(http, "retry_count", 0) > standard_http_requests
        or standard_http_requests > expected_challenge_count * 16 + 2
        or audit_sample.get("sample_digest_validated") is not True
        or audit_sample.get("source_set_validated") is not True
    ):
        raise ValueError("audit report bounded coverage is invalid")
    audit_sample_digest = _sha256_digest(
        audit_sample.get("sample_digest"),
        field="api_audit_sample.sample_digest",
    )
    report_bytes = _canonical_report_bytes(report_map)
    return {
        "contract": PTG2_CANDIDATE_ATTESTATION_CONTRACT,
        "tool_name": PTG2_CANDIDATE_AUDIT_TOOL,
        "tool_version": tool_version,
        "report_digest": hashlib.sha256(report_bytes).digest(),
        "report_json": report_bytes.decode("utf-8"),
        "completed_at": completed_at,
        "audit_sample_digest": audit_sample_digest,
        "source_set_digest": source_set_digest,
        "source_witness_digest": source_witness_digest,
        "provider_identifier_quarantine": provider_identifier_quarantine,
        "checks": observed_check_by_name,
        "standard_api_actual_http_requests": standard_http_requests,
    }


def _attestation_ttl_hours() -> int:
    raw = os.getenv(PTG2_CANDIDATE_ATTESTATION_TTL_HOURS_ENV)
    try:
        value = int(str(raw).strip()) if raw is not None else 0
    except ValueError:
        value = 0
    if value <= 0:
        value = PTG2_CANDIDATE_ATTESTATION_TTL_HOURS_DEFAULT
    return min(value, 168)


def _candidate_identity(database_row: Mapping[str, Any]) -> dict[str, Any]:
    """Validate candidate bindings and return their immutable audit identity."""

    snapshot = _mapping(database_row.get("manifest"))
    activation = _mapping(snapshot.get("activation"))
    layout_manifest = _mapping(database_row.get("layout_manifest"))
    serving_index = _mapping(snapshot.get("serving_index"))
    if (
        str(database_row.get("status") or "") != "validated"
        or activation.get("contract") != PTG2_CANDIDATE_ACTIVATION_CONTRACT
        or activation.get("state") != "validated"
    ):
        raise ValueError("snapshot is not a strict V3 validated candidate")
    source_set = _mapping(serving_index.get("source_set"))
    snapshot_audit_sample = _mapping(serving_index.get("audit_sample"))
    snapshot_source_witness = _mapping(serving_index.get("source_witness"))
    layout_serving_index = _mapping(layout_manifest.get("serving_index"))
    layout_audit_sample = _mapping(layout_serving_index.get("audit_sample"))
    layout_source_witness = _mapping(
        layout_serving_index.get("source_witness")
    )
    snapshot_quarantine = validate_provider_identifier_quarantine(
        serving_index.get("provider_identifier_quarantine")
    )
    layout_quarantine = validate_provider_identifier_quarantine(
        layout_serving_index.get("provider_identifier_quarantine")
    )
    if snapshot_quarantine != layout_quarantine:
        raise ValueError(
            "candidate provider identifier quarantine changed after layout sealing"
        )
    raw_container_hashes = [
        _sha256_hex(raw_digest, field="candidate raw container digest")
        for raw_digest in list(database_row.get("raw_container_sha256_values") or [])
    ]
    observed_source_set = shared_source_set_metadata(raw_container_hashes)
    source_set_digest_hex = str(
        source_set.get("raw_container_sha256_digest") or ""
    ).strip().lower()
    coverage_scope_hex = str(serving_index.get("coverage_scope_id") or "").strip().lower()
    if len(source_set_digest_hex) != 64 or len(coverage_scope_hex) != 64:
        raise ValueError("candidate manifest is missing immutable audit identity")
    try:
        source_set_digest = bytes.fromhex(source_set_digest_hex)
        coverage_scope_id = bytes.fromhex(coverage_scope_hex)
    except ValueError as exc:
        raise ValueError("candidate manifest audit identity is malformed") from exc
    if (
        source_set.get("contract") != PTG2_V3_SOURCE_SET_CONTRACT
        or int(source_set.get("source_count") or -1)
        != int(observed_source_set["source_count"])
        or source_set_digest
        != bytes.fromhex(observed_source_set["raw_container_sha256_digest"])
        or coverage_scope_id != bytes(database_row.get("coverage_scope_id") or b"")
    ):
        raise ValueError("candidate manifest disagrees with its PostgreSQL bindings")
    layout_coverage_scope_hex = str(
        layout_serving_index.get("coverage_scope_id") or ""
    ).strip().lower()
    if (
        layout_coverage_scope_hex != coverage_scope_hex
        or int(layout_serving_index.get("source_count") or -1)
        != int(observed_source_set["source_count"])
    ):
        raise ValueError("sealed layout disagrees with the candidate physical scope")
    snapshot_audit_sample_digest = _sha256_digest(
        snapshot_audit_sample.get("sample_digest"),
        field="candidate snapshot audit sample digest",
    )
    layout_audit_sample_digest = _sha256_digest(
        layout_audit_sample.get("sample_digest"),
        field="candidate layout audit sample digest",
    )
    if snapshot_audit_sample_digest != layout_audit_sample_digest:
        raise ValueError("candidate audit sample changed after layout sealing")
    if snapshot_source_witness != layout_source_witness:
        raise ValueError("candidate source witness changed after layout sealing")
    try:
        snapshot_source_witness = validate_source_witness_manifest(
            snapshot_source_witness,
            expected_source_count=int(observed_source_set["source_count"]),
        )
    except ValueError as exc:
        raise ValueError("candidate source witness is incompatible") from exc
    if snapshot_source_witness.get("source_set_digest") != source_set_digest_hex:
        raise ValueError("candidate source witness changed after layout sealing")
    source_witness_digest = _sha256_digest(
        snapshot_source_witness.get("payload_sha256"),
        field="candidate source witness payload digest",
    )
    return {
        "snapshot_key": int(database_row["snapshot_key"]),
        "source_key": str(activation.get("source_key") or "").strip().lower(),
        "plan_id": str(database_row.get("plan_id") or "").strip(),
        "plan_market_type": str(database_row.get("plan_market_type") or "").strip().lower(),
        "coverage_scope_id": coverage_scope_id,
        "source_set_digest": source_set_digest,
        "source_witness_digest": source_witness_digest,
        "audit_sample_digest": layout_audit_sample_digest,
        "provider_identifier_quarantine": layout_quarantine,
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
               AND layout.generation = 'shared_blocks_v3'
             FOR UPDATE OF snapshot
            """
        ),
        {"snapshot_id": snapshot_id},
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
    report: Mapping[str, Any],
) -> dict[str, Any]:
    """Persist a passing release report against the candidate's immutable identity."""

    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_source_key = str(source_key or "").strip().lower()
    normalized_plan_id = str(plan_id or "").strip()
    normalized_market_type = str(plan_market_type or "").strip().lower()
    if not all(
        (
            normalized_snapshot_id,
            normalized_source_key,
            normalized_plan_id,
            normalized_market_type,
        )
    ):
        raise ValueError("snapshot, source, plan, and market are required")
    preflight_now = datetime.datetime.now(datetime.timezone.utc)
    evidence = validate_candidate_release_audit_report(
        report,
        snapshot_id=normalized_snapshot_id,
        source_key=normalized_source_key,
        plan_id=normalized_plan_id,
        plan_market_type=normalized_market_type,
        evaluated_at=preflight_now,
    )
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
            evaluated_at=now,
        )
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
        ):
            raise ValueError("audit target does not match the candidate bindings")
        if evidence["audit_sample_digest"] != identity["audit_sample_digest"]:
            raise ValueError("audit report does not match the sealed candidate sample")
        if evidence["source_set_digest"] != identity["source_set_digest"]:
            raise ValueError("audit report does not match the sealed candidate sources")
        if evidence["source_witness_digest"] != identity["source_witness_digest"]:
            raise ValueError("audit report does not match the sealed source witnesses")
        if (
            evidence["provider_identifier_quarantine"]
            != identity["provider_identifier_quarantine"]
        ):
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
                  AND attestation.contract = EXCLUDED.contract
                  AND attestation.tool_name = EXCLUDED.tool_name
                  AND attestation.activated_at IS NULL
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
            },
        )
        if insert_result.first() is None:
            raise ValueError("candidate audit attestation conflicts with existing evidence")
    return {
        "status": "attested",
        "snapshot_id": normalized_snapshot_id,
        "contract": evidence["contract"],
        "tool_version": evidence["tool_version"],
        "report_digest": evidence["report_digest"].hex(),
        "expires_at": expires_at.isoformat(),
        "checks": evidence["checks"],
        "standard_api_actual_http_requests": evidence[
            "standard_api_actual_http_requests"
        ],
    }


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
               AND contract = :contract
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
            "contract": PTG2_CANDIDATE_ATTESTATION_CONTRACT,
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
    report_quarantine = validate_provider_identifier_quarantine(
        report_source.get("provider_identifier_quarantine")
    )
    if report_quarantine != identity["provider_identifier_quarantine"]:
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
