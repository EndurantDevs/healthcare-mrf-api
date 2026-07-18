#!/usr/bin/env python3
"""Independently verifiable capacity release gate for strict PTG2 V3.

Release evidence requires a short-lived Ed25519 collector receipt tied to a
fixed, protected verifier trust store. Redacted raw rows are embedded for every
release-critical import, audit, and HTTP assertion; the gate recomputes their
commitments and aggregates before evaluating capacity. Collector-signed HTTP run
counts also reconcile planned, attempted, succeeded, failed, and retried physical
requests with those rows. Reports never echo input paths, unknown keys, arbitrary
JSON values, or exception text. Exit codes are stable: 0 passes, 1 is a completed
gate failure or non-release example, and 2 is invalid or unavailable evidence.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import secrets
import stat
import sys
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Mapping, Sequence

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

if __package__:
    from . import ptg2_v3_capacity_resources as _capacity_resources
else:
    import ptg2_v3_capacity_resources as _capacity_resources

RawResourceSummaries = _capacity_resources.ResourceSummaries
RawResourceTelemetry = _capacity_resources.ResourceTelemetry
ResourceTelemetryError = _capacity_resources.ResourceTelemetryError
parse_and_derive_resource_telemetry = (
    _capacity_resources.parse_and_derive_resource_telemetry
)


SCRIPT_VERSION = "7.0.0"
SCHEMA_VERSION = 7
TRUST_CONFIG_VERSION = 2
RECEIPT_VERSION = 2
RECEIPT_PURPOSE = "ptg2_v3_monthly_capacity_release"
RECEIPT_ALGORITHM = "ed25519"
MAX_RECEIPT_AGE = timedelta(minutes=120)
MAX_RECEIPT_LIFETIME = timedelta(minutes=120)
MAX_COLLECTOR_DELAY = timedelta(minutes=15)
MAX_CLOCK_SKEW = timedelta(minutes=5)
MAX_EVIDENCE_AGE = timedelta(days=8)
MAX_CONTENTION_RESULT_AGE = timedelta(minutes=15)
TRUST_CONFIG_PATH = Path("/etc/healthporta/ptg2-capacity-trust.json")
TRUST_CONFIG_OWNER_UID = 0
MAX_TRUST_CONFIG_BYTES = 16 * 1024
MAX_CANONICAL_NUMBER_DIGITS = 64
MAX_CANONICAL_NUMBER_EXPONENT = 64
ED25519_PUBLIC_KEY_BYTES = 32
ED25519_SIGNATURE_BYTES = 64
MIN_RELEASE_TARGET_LOGICAL_IMPORTS_PER_MONTH = 2_000
MONTH_DAYS = 30
MONTH_HOURS = Decimal(MONTH_DAYS * 24)
MONTH_MINUTES = MONTH_HOURS * Decimal(60)
MAX_UNIQUE_BUILD_MINUTES = Decimal("15")
MAX_LANE_UTILIZATION = Decimal("0.70")
MIN_RESOURCE_HEADROOM_FRACTION = Decimal("0.20")
MIN_QUALIFYING_UNIQUE_BUILDS = 30
MIN_REUSE_ONLY_SAMPLES = 30
MIN_CANDIDATE_AUDIT_SAMPLES = 30
MIN_PEAK_SAMPLE_WINDOWS = 30
MIN_PEAK_WINDOW_MINUTES = Decimal("30")
MIN_PEAK_OBSERVATION_MINUTES = Decimal(7 * 24 * 60)
MAX_PEAK_GAP_MINUTES = Decimal("0")
MIN_PEAK_COVERAGE_RATIO = Decimal("1")
MAX_IMPORT_QUEUE_DELAY_MINUTES = Decimal("30")
MAX_CANDIDATE_AUDIT_QUEUE_AGE_MINUTES = Decimal("30")
MIN_CONTENTION_SECONDS = Decimal(30 * 60)
MAX_CONTENTION_GAP_SECONDS = Decimal("5")
MIN_CONTENTION_COVERAGE_RATIO = Decimal("0.99")
MIN_API_REQUESTS_PER_SECOND = Decimal("1")
API_REQUEST_RATE_BUCKET_SECONDS = Decimal("5")
MAX_COLD_FIRST_PAGE_P95_MS = Decimal("40")
MAX_API_ERROR_RATE = Decimal("0")
MIN_API_REQUESTS = 3_000
MIN_MATCHED_POSITIVE_COLD_SAMPLES = 100
MIN_NEGATIVE_COLD_SAMPLES = 250
MIN_RANDOM_COLD_SAMPLES = 2_500
MIN_DISTINCT_MATCHED_KEYS = 100
MIN_DISTINCT_NEGATIVE_KEYS = 250
MIN_DISTINCT_RANDOM_KEYS = 2_500
MIN_HTTP_REQUESTS_PER_ACTIVATION = 3_000
MIN_POOL_WAIT_OBSERVATIONS = 3_000
MAX_POOL_WAIT_P95_MS = Decimal("10")
MAX_POOL_WAIT_MS = Decimal("100")
MIN_CHECKPOINTS = 2
MAX_AUTOVACUUM_PENDING_MINUTES = Decimal("60")
MIN_GC_WINDOW_HOURS = Decimal("24")
MIN_GC_CYCLES = 24
MIN_GC_LAYOUTS = 30
MAX_INPUT_BYTES = 16 * 1024 * 1024
EVIDENCE_PROFILE = "deployed_release_production_like"
REUSE_EVIDENCE_TYPE = "observed_complete_fingerprint"

EXIT_PASS = 0
EXIT_GATE_FAILURE = 1
EXIT_INVALID_EVIDENCE = 2

_ROOT_FIELDS = (
    "schema_version",
    "evidence_profile",
    "release_evidence",
    "objective",
    "receipt",
    "raw_samples",
    "resource_telemetry",
    "resource_observation",
    "end_to_end",
    "unique_build",
    "reuse",
    "retry",
    "lanes",
    "candidate_audit",
    "peak_arrival",
    "scratch",
    "postgresql",
    "storage",
    "gc",
    "api",
)

_DIGEST_RE = re.compile(r"[0-9a-f]{64}\Z")
_SIGNATURE_RE = re.compile(r"[0-9a-f]{128}\Z")
_IDENTIFIER_RE = re.compile(r"[a-z0-9][a-z0-9._-]{2,63}\Z")
_API_KEY_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
_BASE64URL_RE = re.compile(r"[A-Za-z0-9_-]+\Z")
_UTC_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

_API_EVIDENCE_VERSION = "3"
_API_SIGNATURE_VERSION = "1"
_API_SIGNATURE_ALGORITHM = "Ed25519"
_API_SIGNATURE_DOMAIN = "healthporta.ptg2.capacity-http-evidence.v3"
_API_QUERY_CONTRACT_ID = "healthporta.ptg2.standard-search.v1"
_API_QUERY_PATH = "/api/v1/pricing/providers/search-by-procedure"
_API_PAGE_LIMIT = 100
_API_OBSERVATION_ORDINAL = 0
_API_MAX_PROCESS_AGE_SECONDS = Decimal(300)
_API_MAX_RECEIVE_SKEW_SECONDS = Decimal(5)
_API_MAX_DURATION_NS = 300_000_000_000
_API_CONTRACT_DIGEST_DOMAIN = "healthporta.ptg2.capacity-contract.v1"
_API_SIGNATURE_ENVELOPE = b"HealthPorta PTG2 capacity evidence signature\x00v1\x00"
EXAMPLE_API_EVIDENCE_KEY_ID = "capacity-api-example-key"
EXAMPLE_API_EVIDENCE_PUBLIC_KEY = bytes.fromhex(
    "adc14011f82d1c56d956aa4f9d73d8858361a606048525e0d08c638dc75dd8c7"
)
EXAMPLE_RELEASE_DIGEST = hashlib.sha256(b"synthetic-release").hexdigest()
EXAMPLE_ENVIRONMENT_ID = hashlib.sha256(b"synthetic-environment").hexdigest()
_EXAMPLE_API_EVIDENCE_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(
    bytes(range(65, 97))
)
_API_QUERY_CONTRACT = {
    "contract_id": _API_QUERY_CONTRACT_ID,
    "fixed_parameters": {
        "mode": "exact_source",
        "include_providers": "true",
        "include_details": "true",
        "include_sources": "true",
        "include_allowed_amounts": "false",
        "include_unverified_addresses": "true",
        "order_by": "npi",
        "order": "asc",
        "limit": str(_API_PAGE_LIMIT),
        "offset": "0",
    },
    "method": "GET",
    "path": _API_QUERY_PATH,
    "scope_parameters": ["plan_id", "snapshot_id"],
    "semantic_parameters": ["code_system", "code", "npi"],
    "version": 1,
}
_COMMITMENT_NAMES = (
    "import_lifecycle_samples",
    "audit_result_samples",
    "peak_import_events",
    "peak_audit_events",
    "peak_windows",
    "resource_telemetry",
    "cold_matched_positive_samples",
    "cold_negative_samples",
    "cold_random_samples",
)
_RECEIPT_FIELDS = (
    "version",
    "purpose",
    "algorithm",
    "key_id",
    "collector_id",
    "collector_version",
    "release_digest",
    "environment_id",
    "issued_at",
    "expires_at",
    "observation_started_at",
    "observation_ended_at",
    "contention_run_id",
    "measurement_sha256",
    "commitments",
    "signature",
)
_IMPORT_SAMPLE_FIELDS = (
    "import_id_sha256",
    "contention_run_id",
    "kind",
    "status",
    "enqueued_at",
    "build_started_at",
    "build_completed_at",
    "source_audit_started_at",
    "source_audit_completed_at",
    "candidate_attested_at",
    "activated_at",
    "fresh_fingerprint",
    "complete_source_coverage",
    "durable_publication",
    "persisted_audit",
    "release_scanner",
    "representative_large_build",
    "complete_fingerprint_verified",
    "logical_publication_verified",
    "production_like_observed",
    "audited_activation_verified",
    "failed_attempts",
    "failed_attempt_worker_seconds",
)
_AUDIT_SAMPLE_FIELDS = (
    "audit_id_sha256",
    "import_id_sha256",
    "contention_run_id",
    "status",
    "queued_at",
    "started_at",
    "completed_at",
    "candidate_attested_at",
    "first_request_at",
    "last_request_at",
    "http_requests",
    "http_successes",
    "http_errors",
)
_PEAK_IMPORT_EVENT_FIELDS = (
    "import_id_sha256",
    "kind",
    "enqueued_at",
    "build_started_at",
)
_PEAK_AUDIT_EVENT_FIELDS = (
    "audit_id_sha256",
    "import_id_sha256",
    "queued_at",
    "started_at",
)
_API_SIGNED_PAYLOAD_FIELDS = (
    "evidence_version",
    "signature_version",
    "signature_domain",
    "signature_algorithm",
    "api_evidence_key_id",
    "release_digest",
    "environment_id",
    "method",
    "path",
    "query_contract",
    "query_contract_digest",
    "page_limit",
    "challenge_digest",
    "run_digest",
    "semantic_query_digest",
    "scope_digest",
    "process_instance_digest",
    "process_started_at",
    "server_received_at",
    "server_observed_at",
    "server_duration_ns",
    "isolated",
    "observation_ordinal",
    "contention_run_id",
    "semantic_class",
    "selection_method",
    "selection_ordinal",
    "cold",
    "first_observation",
    "response_status",
    "response_body_sha256",
    "result_count",
)
_HTTP_SAMPLE_FIELDS = (
    *_API_SIGNED_PAYLOAD_FIELDS,
    "api_evidence_signature",
    "collector_received_at",
)
_HTTP_SELECTION_METHODS = {
    "matched_positive": "known_match_v1",
    "negative": "known_miss_v1",
    "random": "sha256_seeded_v1",
}
_CANDIDATE_AUDIT_FIELDS = (
    "sample_count",
    "successful_audits",
    "duration_p95_minutes",
    "duration_max_minutes",
    "queue_age_p95_minutes",
    "queue_age_max_minutes",
    "activation_p95_minutes",
    "activation_max_minutes",
    "lane_count",
    "availability_factor",
    "http_requests_per_activation",
    "activations_with_http_floor",
    "observed_http_requests",
    "observed_request_seconds",
    "errors",
)
_RESOURCE_OBSERVATION_FIELDS = (
    "contention_run_id",
    "contention_started_at",
    "contention_ended_at",
    "storage_measured_at",
    "gc_started_at",
    "gc_ended_at",
)


class EvidenceError(ValueError):
    """A report-safe validation error with no input-derived message."""

    def __init__(self, code: str, field: str | None = None):
        super().__init__(code)
        self.code = code
        self.field = field


class _DuplicateFieldError(ValueError):
    pass


def _field(parent: str, name: str) -> str:
    return f"{parent}.{name}" if parent else name


def _object(
    value: Any,
    path: str,
    required: Sequence[str],
    optional: Sequence[str] = (),
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EvidenceError("invalid_type", path or "root")
    for name in required:
        if name not in value:
            raise EvidenceError("missing_field", _field(path, name))
    allowed = set(required) | set(optional)
    if any(name not in allowed for name in value):
        # Never echo an unknown key: it may itself contain sensitive text.
        raise EvidenceError("unexpected_field", path or "root")
    return value


def _integer(
    obj: Mapping[str, Any],
    name: str,
    path: str,
    *,
    minimum: int | None = None,
) -> int:
    value = obj[name]
    field = _field(path, name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise EvidenceError("invalid_type", field)
    if minimum is not None and value < minimum:
        raise EvidenceError("invalid_value", field)
    return value


def _decimal(
    obj: Mapping[str, Any],
    name: str,
    path: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
    minimum_inclusive: bool = True,
) -> Decimal:
    raw = obj[name]
    field = _field(path, name)
    if isinstance(raw, bool) or not isinstance(raw, (int, float, Decimal)):
        raise EvidenceError("invalid_type", field)
    try:
        value = raw if isinstance(raw, Decimal) else Decimal(str(raw))
    except Exception:
        raise EvidenceError("invalid_value", field)
    if not value.is_finite():
        raise EvidenceError("invalid_value", field)
    if minimum is not None:
        below = value < minimum if minimum_inclusive else value <= minimum
        if below:
            raise EvidenceError("invalid_value", field)
    if maximum is not None and value > maximum:
        raise EvidenceError("invalid_value", field)
    return value


def _boolean(obj: Mapping[str, Any], name: str, path: str) -> bool:
    value = obj[name]
    if not isinstance(value, bool):
        raise EvidenceError("invalid_type", _field(path, name))
    return value


def _fixed_string(
    obj: Mapping[str, Any],
    name: str,
    path: str,
    expected: str,
) -> str:
    value = obj[name]
    field = _field(path, name)
    if not isinstance(value, str):
        raise EvidenceError("invalid_type", field)
    if value != expected:
        raise EvidenceError("invalid_enum", field)
    return value


def _enum_string(
    obj: Mapping[str, Any],
    name: str,
    path: str,
    allowed: Sequence[str],
) -> str:
    value = obj[name]
    field = _field(path, name)
    if not isinstance(value, str):
        raise EvidenceError("invalid_type", field)
    if value not in allowed:
        raise EvidenceError("invalid_enum", field)
    return value


def _at_most(value: int, maximum: int, field: str) -> None:
    if value > maximum:
        raise EvidenceError("inconsistent_evidence", field)


@dataclass(frozen=True)
class ReceiptTrust:
    """Out-of-band trust policy controlled by the release environment."""

    key_id: str
    public_key: bytes
    api_evidence_key_id: str
    api_evidence_public_key: bytes
    release_digest: str
    environment_id: str
    collector_id: str
    collector_version: str


@dataclass(frozen=True)
class CommitmentEvidence:
    """Authenticated commitment to a redacted collector sample set."""

    sha256: str
    sample_count: int
    distinct_query_keys: int | None = None
    contention_samples: int | None = None


@dataclass(frozen=True)
class VerifiedReceipt:
    """Receipt fields retained after identity, freshness, and signature checks."""

    issued_at: datetime
    expires_at: datetime
    observation_started_at: datetime
    observation_ended_at: datetime
    contention_run_id: str
    measurement_sha256: str
    release_digest: str
    environment_id: str
    commitments: Mapping[str, CommitmentEvidence]


@dataclass(frozen=True)
class ImportLifecycleSample:
    import_id_sha256: str
    contention_run_id: str
    kind: str
    status: str
    enqueued_at: datetime
    build_started_at: datetime
    build_completed_at: datetime
    source_audit_started_at: datetime
    source_audit_completed_at: datetime
    candidate_attested_at: datetime
    activated_at: datetime
    fresh_fingerprint: bool
    complete_source_coverage: bool
    durable_publication: bool
    persisted_audit: bool
    release_scanner: bool
    representative_large_build: bool
    complete_fingerprint_verified: bool
    logical_publication_verified: bool
    production_like_observed: bool
    audited_activation_verified: bool
    failed_attempts: int
    failed_attempt_worker_seconds: Decimal


@dataclass(frozen=True)
class AuditResultSample:
    audit_id_sha256: str
    import_id_sha256: str
    contention_run_id: str
    status: str
    queued_at: datetime
    started_at: datetime
    completed_at: datetime
    candidate_attested_at: datetime
    first_request_at: datetime
    last_request_at: datetime
    http_requests: int
    http_successes: int
    http_errors: int


@dataclass(frozen=True)
class PeakImportEvent:
    import_id_sha256: str
    kind: str
    enqueued_at: datetime
    build_started_at: datetime


@dataclass(frozen=True)
class PeakAuditEvent:
    audit_id_sha256: str
    import_id_sha256: str
    queued_at: datetime
    started_at: datetime


@dataclass(frozen=True)
class HttpSample:
    challenge_digest: str
    run_digest: str
    semantic_query_digest: str
    scope_digest: str
    process_instance_digest: str
    process_started_at: datetime
    server_received_at: datetime
    server_observed_at: datetime
    server_duration_ns: int
    collector_received_at: datetime
    page_limit: int
    contention_run_id: str
    semantic_class: str
    selection_method: str
    selection_ordinal: int
    cold: bool
    first_observation: bool
    response_status: int
    response_body_sha256: str
    result_count: int
    first_page_ms: Decimal


@dataclass(frozen=True)
class ThresholdCoverageEvidence:
    intervals: tuple[tuple[datetime, datetime], ...]
    coverage_ratio: Decimal
    max_gap_seconds: Decimal


@dataclass(frozen=True)
class HttpRateBucketEvidence:
    bucket_seconds: Decimal
    bucket_count: int
    minimum_observations: int
    minimum_requests_per_second: Decimal
    underfilled_buckets: int


@dataclass(frozen=True)
class RawSamplesEvidence:
    imports: tuple[ImportLifecycleSample, ...]
    audits: tuple[AuditResultSample, ...]
    peak_import_events: tuple[PeakImportEvent, ...]
    peak_audit_events: tuple[PeakAuditEvent, ...]
    matched_positive: tuple[HttpSample, ...]
    negative: tuple[HttpSample, ...]
    random: tuple[HttpSample, ...]
    build_threshold: ThresholdCoverageEvidence
    audit_threshold: ThresholdCoverageEvidence
    full_lane_threshold: ThresholdCoverageEvidence
    http_observation_coverage_ratio: Decimal
    http_observation_max_gap_seconds: Decimal
    http_rate_buckets: HttpRateBucketEvidence
    concurrent_unique_builds: int
    concurrent_candidate_audits: int

    @property
    def contention_coverage_ratio(self) -> Decimal:
        """Return overlap coverage for full lanes and HTTP observations."""

        return min(
            self.full_lane_threshold.coverage_ratio,
            self.http_observation_coverage_ratio,
        )

    @property
    def contention_max_gap_seconds(self) -> Decimal:
        """Return the largest full-lane or HTTP-observation gap."""

        return max(
            self.full_lane_threshold.max_gap_seconds,
            self.http_observation_max_gap_seconds,
        )


@dataclass(frozen=True)
class _ValidatedRawSampleGroups:
    imports: tuple[ImportLifecycleSample, ...]
    audits: tuple[AuditResultSample, ...]
    peak_import_events: tuple[PeakImportEvent, ...]
    peak_audit_events: tuple[PeakAuditEvent, ...]
    matched_positive: tuple[HttpSample, ...]
    negative: tuple[HttpSample, ...]
    random_samples: tuple[HttpSample, ...]


@dataclass(frozen=True)
class _RawSampleCoverage:
    build_threshold: ThresholdCoverageEvidence
    audit_threshold: ThresholdCoverageEvidence
    full_lane_threshold: ThresholdCoverageEvidence
    http_observation_coverage_ratio: Decimal
    http_observation_max_gap_seconds: Decimal
    http_rate_buckets: HttpRateBucketEvidence
    concurrent_unique_builds: int
    concurrent_candidate_audits: int


@dataclass(frozen=True)
class ResourceObservationEvidence:
    contention_run_id: str
    contention_started_at: datetime
    contention_ended_at: datetime
    storage_measured_at: datetime
    gc_started_at: datetime
    gc_ended_at: datetime


@dataclass(frozen=True)
class EndToEndEvidence:
    """Per-logical-import end-to-end timing summary."""

    sample_count: int
    unique_build_samples: int
    reuse_samples: int
    complete_stage_samples: int
    within_15_minutes: int
    p95_minutes: Decimal
    max_minutes: Decimal
    errors: int


@dataclass(frozen=True)
class UniqueBuildEvidence:
    sample_count: int
    p95_minutes: Decimal
    max_minutes: Decimal
    fresh_fingerprint_builds: int
    complete_source_coverage_builds: int
    durable_publication_builds: int
    persisted_audit_builds: int
    release_scanner_builds: int
    representative_large_builds: int


@dataclass(frozen=True)
class ReuseEvidence:
    observed_logical_imports: int
    observed_unique_builds: int
    observed_reuse_hits: int
    complete_fingerprint_verified_hits: int
    logical_publication_verified_hits: int
    production_like_observed_hits: int
    audited_activation_verified_hits: int
    reuse_only_sample_count: int
    reuse_only_p95_minutes: Decimal
    reuse_only_max_minutes: Decimal

    @property
    def qualified_hits(self) -> int:
        """Return the reuse-hit count satisfying every qualification bound."""

        return min(
            self.complete_fingerprint_verified_hits,
            self.logical_publication_verified_hits,
            self.production_like_observed_hits,
            self.audited_activation_verified_hits,
        )


@dataclass(frozen=True)
class RetryEvidence:
    successful_unique_builds: int
    failed_attempts: int
    failed_attempt_worker_minutes: Decimal


@dataclass(frozen=True)
class LaneEvidence:
    count: int
    availability_factor: Decimal


@dataclass(frozen=True)
class CandidateAuditEvidence:
    sample_count: int
    successful_audits: int
    duration_p95_minutes: Decimal
    duration_max_minutes: Decimal
    queue_age_p95_minutes: Decimal
    queue_age_max_minutes: Decimal
    activation_p95_minutes: Decimal
    activation_max_minutes: Decimal
    lane_count: int
    availability_factor: Decimal
    http_requests_per_activation: int
    activations_with_http_floor: int
    observed_http_requests: int
    observed_request_seconds: Decimal
    errors: int


@dataclass(frozen=True)
class PeakWindowEvidence:
    """One redacted aggregate arrival window."""

    started_at: datetime
    ended_at: datetime
    logical_imports: int
    unique_builds: int
    candidate_audits: int
    max_queue_delay_minutes: Decimal
    max_audit_queue_age_minutes: Decimal


@dataclass(frozen=True)
class PeakArrivalEvidence:
    sample_windows: int
    observation_minutes: Decimal
    window_minutes: Decimal
    queue_delay_p95_minutes: Decimal
    max_queue_delay_minutes: Decimal
    audit_queue_age_p95_minutes: Decimal
    max_audit_queue_age_minutes: Decimal
    observed_peak_logical_imports: int
    observed_peak_unique_builds: int
    observed_peak_candidate_audits: int
    coverage_ratio: Decimal
    max_gap_minutes: Decimal
    windows: tuple[PeakWindowEvidence, ...]


@dataclass(frozen=True)
class ScratchEvidence:
    configured_concurrent_unique_builds: int
    measured_concurrent_unique_builds: int
    capacity_bytes: int
    baseline_used_bytes: int
    reserve_bytes: int
    measured_peak_incremental_bytes: int
    cleanup_cycles: int
    cleanup_failures: int


@dataclass(frozen=True)
class PostgresLoadEvidence:
    contention_run_id: str
    started_at: datetime
    ended_at: datetime
    concurrent_unique_builds: int
    concurrent_candidate_audits: int
    api_requests_per_second: Decimal
    candidate_audit_requests_per_second: Decimal
    sample_seconds: Decimal


@dataclass(frozen=True)
class PostgresConnectionEvidence:
    max_connections: int
    reserved_connections: int
    peak_api_connections: int
    peak_import_connections: int
    peak_other_connections: int
    required_headroom_connections: int


@dataclass(frozen=True)
class PostgresRateEvidence:
    import_bytes_per_second: Decimal
    api_bytes_per_second: Decimal
    other_bytes_per_second: Decimal
    sustainable_bytes_per_second: Decimal


@dataclass(frozen=True)
class PostgresPoolWaitEvidence:
    observations: int
    waited_acquisitions: int
    p95_ms: Decimal
    max_ms: Decimal
    timeout_errors: int


@dataclass(frozen=True)
class PostgresCheckpointEvidence:
    sample_seconds: Decimal
    completed: int
    requested: int
    write_seconds: Decimal
    sync_seconds: Decimal


@dataclass(frozen=True)
class PostgresTempEvidence:
    capacity_bytes: int
    baseline_used_bytes: int
    reserve_bytes: int
    measured_peak_incremental_bytes: int


@dataclass(frozen=True)
class PostgresAutovacuumEvidence:
    sample_seconds: Decimal
    max_workers: int
    peak_workers: int
    required_headroom_workers: int
    completed_cycles: int
    cancelled_cycles: int
    oldest_pending_minutes: Decimal


@dataclass(frozen=True)
class PostgresEvidence:
    load: PostgresLoadEvidence
    connections: PostgresConnectionEvidence
    write: PostgresRateEvidence
    wal: PostgresRateEvidence
    pool_wait: PostgresPoolWaitEvidence
    checkpoint: PostgresCheckpointEvidence
    temp: PostgresTempEvidence
    autovacuum: PostgresAutovacuumEvidence


@dataclass(frozen=True)
class StorageEvidence:
    physical_unique_builds_observed: int
    physical_net_new_bytes_observed: int
    physical_max_bytes_per_unique_build: int
    logical_imports_observed: int
    logical_net_new_bytes_observed: int
    logical_max_bytes_per_import: int
    current_used_bytes: int
    capacity_bytes: int
    reserve_bytes: int
    physical_retention_months: Decimal
    logical_retention_months: Decimal


@dataclass(frozen=True)
class GcEvidence:
    window_hours: Decimal
    cycles_observed: int
    executed_cycles: int
    overlap_count: int
    reference_recheck_failures: int
    starting_backlog_bytes: int
    starting_backlog_layouts: int
    newly_eligible_bytes: int
    deleted_bytes: int
    ending_backlog_bytes: int
    ending_backlog_layouts: int
    max_backlog_bytes: int
    max_clearance_hours: Decimal
    eligible_layouts: int
    deleted_layouts: int


@dataclass(frozen=True)
class ApiClassEvidence:
    samples: int
    distinct_query_keys: int
    contention_samples: int
    contention_distinct_query_keys: int
    errors: int
    cold_first_page_p95_ms: Decimal
    sample_started_at: datetime
    sample_ended_at: datetime


@dataclass(frozen=True)
class ApiEvidence:
    contention_run_id: str
    concurrent_unique_builds: int
    concurrent_candidate_audits: int
    contention_seconds: Decimal
    contention_started_at: datetime
    contention_ended_at: datetime
    fresh_processes: bool
    planned_requests: int
    attempted_requests: int
    succeeded_requests: int
    failed_requests: int
    retried_requests: int
    requests: int
    requests_while_imports_running: int
    candidate_audit_requests: int
    candidate_audit_requests_while_imports_running: int
    errors: int
    matched_positive: ApiClassEvidence
    negative: ApiClassEvidence
    random: ApiClassEvidence


@dataclass(frozen=True)
class CapacityEvidence:
    target_logical_imports_per_month: int
    raw_samples: RawSamplesEvidence
    resource_telemetry: RawResourceTelemetry
    resource_summaries: RawResourceSummaries
    resource_observation: ResourceObservationEvidence
    end_to_end: EndToEndEvidence
    unique_build: UniqueBuildEvidence
    reuse: ReuseEvidence
    retry: RetryEvidence
    lanes: LaneEvidence
    candidate_audit: CandidateAuditEvidence
    peak_arrival: PeakArrivalEvidence
    scratch: ScratchEvidence
    postgresql: PostgresEvidence
    storage: StorageEvidence
    gc: GcEvidence
    api: ApiEvidence


def _canonical_number(value: Decimal | float) -> str:
    number = value if isinstance(value, Decimal) else Decimal(str(value))
    if not number.is_finite():
        raise EvidenceError("invalid_value", "canonical_number")
    number_tuple = number.as_tuple()
    if (
        len(number_tuple.digits) > MAX_CANONICAL_NUMBER_DIGITS
        or abs(number_tuple.exponent) > MAX_CANONICAL_NUMBER_EXPONENT
        or abs(number.adjusted()) > MAX_CANONICAL_NUMBER_EXPONENT
    ):
        raise EvidenceError("numeric_range_exceeded", "canonical_number")
    if number == 0:
        return "0"
    rendered = format(number, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _canonical_json_text(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (Decimal, float)):
        return _canonical_number(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise EvidenceError("invalid_type", "canonical_object")
        fields = (
            f"{_canonical_json_text(key)}:{_canonical_json_text(value[key])}"
            for key in sorted(value)
        )
        return "{" + ",".join(fields) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_canonical_json_text(item) for item in value) + "]"
    raise EvidenceError("invalid_type", "canonical_value")


def canonical_json_bytes(value: Any) -> bytes:
    """Return the collector/gate canonical JSON representation."""

    return _canonical_json_text(value).encode("ascii")


def _api_domain_digest(domain: str, payload: bytes) -> str:
    return hashlib.sha256(domain.encode("ascii") + b"\x00" + payload).hexdigest()


_API_QUERY_CONTRACT_DIGEST = _api_domain_digest(
    _API_CONTRACT_DIGEST_DOMAIN,
    canonical_json_bytes(_API_QUERY_CONTRACT),
)


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _base64url_decode_signature(value: Any, field: str) -> bytes:
    if not isinstance(value, str) or not value or not _BASE64URL_RE.fullmatch(value):
        raise EvidenceError("invalid_api_evidence_signature", field)
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (TypeError, ValueError):
        raise EvidenceError("invalid_api_evidence_signature", field)
    if len(decoded) != ED25519_SIGNATURE_BYTES or _base64url_encode(decoded) != value:
        raise EvidenceError("invalid_api_evidence_signature", field)
    return decoded


def _api_signature_message(payload: Mapping[str, Any]) -> bytes:
    return _API_SIGNATURE_ENVELOPE + canonical_json_bytes(payload)


def measurement_sha256(record: Mapping[str, Any]) -> str:
    """Hash every signed measurement field while excluding the receipt itself."""

    measurement_dict = {key: value for key, value in record.items() if key != "receipt"}
    return hashlib.sha256(canonical_json_bytes(measurement_dict)).hexdigest()


def receipt_signing_payload(
    record: Mapping[str, Any], receipt: Mapping[str, Any]
) -> bytes:
    """Return the exact bytes an authenticated collector must sign."""

    unsigned_receipt_dict = {
        key: value for key, value in receipt.items() if key != "signature"
    }
    measurement_dict = {key: value for key, value in record.items() if key != "receipt"}
    return canonical_json_bytes(
        {"measurement": measurement_dict, "receipt": unsigned_receipt_dict}
    )


def sample_set_sha256(value: Any) -> str:
    """Hash one redacted sample set using the receipt canonicalization."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _timestamp_value(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceError("invalid_type", field)
    try:
        parsed = datetime.strptime(value, _UTC_TIMESTAMP_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        raise EvidenceError("invalid_timestamp", field)
    return parsed


def _timestamp(obj: Mapping[str, Any], name: str, path: str) -> datetime:
    return _timestamp_value(obj[name], _field(path, name))


def _duration_seconds(started_at: datetime, ended_at: datetime) -> Decimal:
    return Decimal(int((ended_at - started_at).total_seconds()))


def _duration_minutes(started_at: datetime, ended_at: datetime) -> Decimal:
    return _duration_seconds(started_at, ended_at) / Decimal(60)


def _nearest_rank_p95(values: Sequence[Decimal]) -> Decimal:
    if not values:
        raise EvidenceError("missing_samples", "raw_samples")
    ordered_values = sorted(values)
    rank = _ceil_ratio(95 * len(ordered_values), 100)
    return ordered_values[rank - 1]


def _require_equal(actual: Any, expected: Any, field: str) -> None:
    if actual != expected:
        raise EvidenceError("raw_aggregate_mismatch", field)


def _sample_list(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list) or not value:
        raise EvidenceError("invalid_type", path)
    return value


def _digest(obj: Mapping[str, Any], name: str, path: str) -> str:
    value = obj[name]
    field = _field(path, name)
    if not isinstance(value, str):
        raise EvidenceError("invalid_type", field)
    if not _DIGEST_RE.fullmatch(value) or value == "0" * 64:
        raise EvidenceError("invalid_digest", field)
    return value


def _signature(obj: Mapping[str, Any], name: str, path: str) -> str:
    value = obj[name]
    field = _field(path, name)
    if not isinstance(value, str):
        raise EvidenceError("invalid_type", field)
    if not _SIGNATURE_RE.fullmatch(value) or value == "0" * 128:
        raise EvidenceError("invalid_signature", field)
    return value


def _identifier(obj: Mapping[str, Any], name: str, path: str) -> str:
    value = obj[name]
    field = _field(path, name)
    if not isinstance(value, str):
        raise EvidenceError("invalid_type", field)
    if not _IDENTIFIER_RE.fullmatch(value):
        raise EvidenceError("invalid_identifier", field)
    return value


def _validate_trust(trust: ReceiptTrust | None) -> ReceiptTrust:
    if trust is None:
        raise EvidenceError("receipt_trust_unavailable")
    if not _IDENTIFIER_RE.fullmatch(trust.key_id):
        raise EvidenceError("invalid_trust_configuration", "receipt.key_id")
    if not _IDENTIFIER_RE.fullmatch(trust.collector_id):
        raise EvidenceError("invalid_trust_configuration", "receipt.collector_id")
    if not _IDENTIFIER_RE.fullmatch(trust.collector_version):
        raise EvidenceError("invalid_trust_configuration", "receipt.collector_version")
    if not _API_KEY_ID_RE.fullmatch(trust.api_evidence_key_id):
        raise EvidenceError("invalid_trust_configuration", "api_evidence.key_id")
    if not _DIGEST_RE.fullmatch(trust.release_digest):
        raise EvidenceError("invalid_trust_configuration", "receipt.release_digest")
    if not _DIGEST_RE.fullmatch(trust.environment_id):
        raise EvidenceError("invalid_trust_configuration", "receipt.environment_id")
    if (
        not isinstance(trust.public_key, bytes)
        or len(trust.public_key) != ED25519_PUBLIC_KEY_BYTES
        or trust.public_key == bytes(ED25519_PUBLIC_KEY_BYTES)
    ):
        raise EvidenceError("invalid_trust_configuration", "receipt.public_key")
    try:
        Ed25519PublicKey.from_public_bytes(trust.public_key)
    except ValueError:
        raise EvidenceError("invalid_trust_configuration", "receipt.public_key")
    if (
        not isinstance(trust.api_evidence_public_key, bytes)
        or len(trust.api_evidence_public_key) != ED25519_PUBLIC_KEY_BYTES
        or trust.api_evidence_public_key == bytes(ED25519_PUBLIC_KEY_BYTES)
    ):
        raise EvidenceError("invalid_trust_configuration", "api_evidence.public_key")
    try:
        Ed25519PublicKey.from_public_bytes(trust.api_evidence_public_key)
    except ValueError:
        raise EvidenceError("invalid_trust_configuration", "api_evidence.public_key")
    if secrets.compare_digest(
        trust.key_id, trust.api_evidence_key_id
    ) or secrets.compare_digest(trust.public_key, trust.api_evidence_public_key):
        raise EvidenceError(
            "invalid_trust_configuration", "api_evidence.trust_separation"
        )
    return trust


def _validate_commitment(
    commitments: Mapping[str, Any],
    name: str,
    *,
    cold_class: bool,
) -> CommitmentEvidence:
    path = f"receipt.commitments.{name}"
    fields = ["sha256", "sample_count"]
    if cold_class:
        fields.extend(("distinct_query_keys", "contention_samples"))
    obj = _object(commitments[name], path, tuple(fields))
    return CommitmentEvidence(
        sha256=_digest(obj, "sha256", path),
        sample_count=_integer(obj, "sample_count", path, minimum=1),
        distinct_query_keys=(
            _integer(obj, "distinct_query_keys", path, minimum=1)
            if cold_class
            else None
        ),
        contention_samples=(
            _integer(obj, "contention_samples", path, minimum=1) if cold_class else None
        ),
    )


def _validate_receipt_identity(
    receipt: Mapping[str, Any],
    trusted: ReceiptTrust,
    path: str,
) -> None:
    identity_by_name = {
        "key_id": _identifier(receipt, "key_id", path),
        "collector_id": _identifier(receipt, "collector_id", path),
        "collector_version": _identifier(receipt, "collector_version", path),
        "release_digest": _digest(receipt, "release_digest", path),
        "environment_id": _digest(receipt, "environment_id", path),
    }
    expected_by_name = {
        "key_id": trusted.key_id,
        "collector_id": trusted.collector_id,
        "collector_version": trusted.collector_version,
        "release_digest": trusted.release_digest,
        "environment_id": trusted.environment_id,
    }
    for name, identity_value in identity_by_name.items():
        if not secrets.compare_digest(identity_value, expected_by_name[name]):
            raise EvidenceError("receipt_identity_mismatch", f"{path}.{name}")


def _validate_receipt_window(
    receipt: Mapping[str, Any],
    path: str,
    now: datetime | None,
) -> tuple[datetime, datetime, datetime, datetime]:
    issued_at = _timestamp(receipt, "issued_at", path)
    expires_at = _timestamp(receipt, "expires_at", path)
    observed_from = _timestamp(receipt, "observation_started_at", path)
    observed_to = _timestamp(receipt, "observation_ended_at", path)
    current = now or datetime.now(UTC)
    if current.tzinfo is None or current.utcoffset() is None:
        raise EvidenceError("invalid_trust_configuration", "current_time")
    current = current.astimezone(UTC)
    if not observed_from < observed_to <= issued_at:
        raise EvidenceError("invalid_receipt_window", f"{path}.observation_ended_at")
    if issued_at - observed_to > MAX_COLLECTOR_DELAY:
        raise EvidenceError("stale_observation", f"{path}.observation_ended_at")
    if issued_at - observed_from > MAX_EVIDENCE_AGE:
        raise EvidenceError("stale_evidence", f"{path}.observation_started_at")
    if not issued_at < expires_at <= issued_at + MAX_RECEIPT_LIFETIME:
        raise EvidenceError("invalid_receipt_window", f"{path}.expires_at")
    if issued_at > current + MAX_CLOCK_SKEW:
        raise EvidenceError("receipt_from_future", f"{path}.issued_at")
    if current - issued_at > MAX_RECEIPT_AGE or current > expires_at:
        raise EvidenceError("expired_receipt", f"{path}.expires_at")
    return issued_at, expires_at, observed_from, observed_to


def _receipt_commitment_map(
    receipt: Mapping[str, Any], path: str
) -> dict[str, CommitmentEvidence]:
    commitment_dict = _object(
        receipt["commitments"],
        f"{path}.commitments",
        _COMMITMENT_NAMES,
    )
    return {
        name: _validate_commitment(
            commitment_dict,
            name,
            cold_class=name.startswith("cold_"),
        )
        for name in _COMMITMENT_NAMES
    }


def verify_receipt(
    measurement_record: Any,
    *,
    trust: ReceiptTrust | None,
    now: datetime | None = None,
) -> VerifiedReceipt:
    """Authenticate one release receipt against protected trust configuration."""

    measurement_fields = tuple(name for name in _ROOT_FIELDS if name != "receipt")
    root = _object(measurement_record, "", measurement_fields, ("receipt",))
    if not _boolean(root, "release_evidence", ""):
        raise EvidenceError("non_release_example", "release_evidence")
    if "receipt" not in root:
        raise EvidenceError("missing_field", "receipt")
    trusted = _validate_trust(trust)
    path = "receipt"
    receipt = _object(root[path], path, _RECEIPT_FIELDS)
    if _integer(receipt, "version", path, minimum=1) != RECEIPT_VERSION:
        raise EvidenceError("unsupported_receipt_version", f"{path}.version")
    _fixed_string(receipt, "purpose", path, RECEIPT_PURPOSE)
    _fixed_string(receipt, "algorithm", path, RECEIPT_ALGORITHM)
    _validate_receipt_identity(receipt, trusted, path)
    issued_at, expires_at, observed_from, observed_to = _validate_receipt_window(
        receipt, path, now
    )
    claimed_measurement_hash = _digest(receipt, "measurement_sha256", path)
    actual_measurement_hash = measurement_sha256(root)
    if not secrets.compare_digest(claimed_measurement_hash, actual_measurement_hash):
        raise EvidenceError("measurement_digest_mismatch", f"{path}.measurement_sha256")
    commitment_by_name = _receipt_commitment_map(receipt, path)
    contention_run_id = _digest(receipt, "contention_run_id", path)
    signature = _signature(receipt, "signature", path)
    try:
        Ed25519PublicKey.from_public_bytes(trusted.public_key).verify(
            bytes.fromhex(signature),
            receipt_signing_payload(root, receipt),
        )
    except InvalidSignature:
        raise EvidenceError("invalid_receipt_signature", f"{path}.signature")
    return VerifiedReceipt(
        issued_at=issued_at,
        expires_at=expires_at,
        observation_started_at=observed_from,
        observation_ended_at=observed_to,
        contention_run_id=contention_run_id,
        measurement_sha256=claimed_measurement_hash,
        release_digest=trusted.release_digest,
        environment_id=trusted.environment_id,
        commitments=commitment_by_name,
    )


def _validate_end_to_end(root: Mapping[str, Any]) -> EndToEndEvidence:
    path = "end_to_end"
    fields = (
        "sample_count",
        "unique_build_samples",
        "reuse_samples",
        "complete_stage_samples",
        "within_15_minutes",
        "p95_minutes",
        "max_minutes",
        "errors",
    )
    evidence_dict = _object(root[path], path, fields)
    sample_count = _integer(evidence_dict, "sample_count", path, minimum=1)
    unique = _integer(evidence_dict, "unique_build_samples", path, minimum=0)
    reuse = _integer(evidence_dict, "reuse_samples", path, minimum=0)
    if unique + reuse != sample_count:
        raise EvidenceError("inconsistent_evidence", f"{path}.sample_count")
    complete = _integer(evidence_dict, "complete_stage_samples", path, minimum=0)
    within = _integer(evidence_dict, "within_15_minutes", path, minimum=0)
    errors = _integer(evidence_dict, "errors", path, minimum=0)
    for name, stage_count in (
        ("complete_stage_samples", complete),
        ("within_15_minutes", within),
        ("errors", errors),
    ):
        _at_most(stage_count, sample_count, f"{path}.{name}")
    p95 = _decimal(evidence_dict, "p95_minutes", path, minimum=Decimal(0))
    maximum = _decimal(evidence_dict, "max_minutes", path, minimum=Decimal(0))
    if p95 > maximum:
        raise EvidenceError("inconsistent_evidence", f"{path}.p95_minutes")
    return EndToEndEvidence(
        sample_count=sample_count,
        unique_build_samples=unique,
        reuse_samples=reuse,
        complete_stage_samples=complete,
        within_15_minutes=within,
        p95_minutes=p95,
        max_minutes=maximum,
        errors=errors,
    )


def _validate_unique_build(root: Mapping[str, Any]) -> UniqueBuildEvidence:
    path = "unique_build"
    fields = (
        "sample_count",
        "p95_minutes",
        "max_minutes",
        "fresh_fingerprint_builds",
        "complete_source_coverage_builds",
        "durable_publication_builds",
        "persisted_audit_builds",
        "release_scanner_builds",
        "representative_large_builds",
    )
    build_dict = _object(root[path], path, fields)
    sample_count = _integer(build_dict, "sample_count", path, minimum=1)
    p95_minutes = _decimal(
        build_dict,
        "p95_minutes",
        path,
        minimum=Decimal(0),
        minimum_inclusive=False,
    )
    max_minutes = _decimal(
        build_dict,
        "max_minutes",
        path,
        minimum=Decimal(0),
        minimum_inclusive=False,
    )
    if p95_minutes > max_minutes:
        raise EvidenceError("inconsistent_evidence", f"{path}.p95_minutes")
    count_by_field = {
        name: _integer(build_dict, name, path, minimum=0) for name in fields[3:]
    }
    for name, build_count in count_by_field.items():
        _at_most(build_count, sample_count, f"{path}.{name}")
    return UniqueBuildEvidence(
        sample_count=sample_count,
        p95_minutes=p95_minutes,
        max_minutes=max_minutes,
        **count_by_field,
    )


_REUSE_FIELDS = (
    "evidence_type",
    "observed_logical_imports",
    "observed_unique_builds",
    "observed_reuse_hits",
    "complete_fingerprint_verified_hits",
    "logical_publication_verified_hits",
    "production_like_observed_hits",
    "audited_activation_verified_hits",
    "reuse_only_sample_count",
    "reuse_only_p95_minutes",
    "reuse_only_max_minutes",
)


def _reuse_verification_counts(
    reuse_dict: Mapping[str, Any], path: str, reuse_hits: int
) -> dict[str, int]:
    verified_by_field = {
        name: _integer(reuse_dict, name, path, minimum=0) for name in _REUSE_FIELDS[4:8]
    }
    for name, verified_count in verified_by_field.items():
        _at_most(verified_count, reuse_hits, f"{path}.{name}")
    return verified_by_field


def _validate_reuse(root: Mapping[str, Any]) -> ReuseEvidence:
    """Validate reuse observations and all qualification counters."""

    path = "reuse"
    reuse_dict = _object(root[path], path, _REUSE_FIELDS)
    _fixed_string(reuse_dict, "evidence_type", path, REUSE_EVIDENCE_TYPE)
    logical = _integer(reuse_dict, "observed_logical_imports", path, minimum=1)
    unique = _integer(reuse_dict, "observed_unique_builds", path, minimum=0)
    hits = _integer(reuse_dict, "observed_reuse_hits", path, minimum=0)
    if unique + hits != logical:
        raise EvidenceError("inconsistent_evidence", f"{path}.observed_reuse_hits")
    verified_by_field = _reuse_verification_counts(reuse_dict, path, hits)
    reuse_sample_count = _integer(
        reuse_dict,
        "reuse_only_sample_count",
        path,
        minimum=1,
    )
    _at_most(reuse_sample_count, hits, f"{path}.reuse_only_sample_count")
    p95_minutes = _decimal(
        reuse_dict,
        "reuse_only_p95_minutes",
        path,
        minimum=Decimal(0),
        minimum_inclusive=False,
    )
    max_minutes = _decimal(
        reuse_dict,
        "reuse_only_max_minutes",
        path,
        minimum=Decimal(0),
        minimum_inclusive=False,
    )
    if p95_minutes > max_minutes:
        raise EvidenceError(
            "inconsistent_evidence",
            f"{path}.reuse_only_p95_minutes",
        )
    return ReuseEvidence(
        observed_logical_imports=logical,
        observed_unique_builds=unique,
        observed_reuse_hits=hits,
        **verified_by_field,
        reuse_only_sample_count=reuse_sample_count,
        reuse_only_p95_minutes=p95_minutes,
        reuse_only_max_minutes=max_minutes,
    )


def _validate_retry(root: Mapping[str, Any]) -> RetryEvidence:
    path = "retry"
    fields = (
        "successful_unique_builds",
        "failed_attempts",
        "failed_attempt_worker_minutes",
    )
    obj = _object(root[path], path, fields)
    successful = _integer(obj, "successful_unique_builds", path, minimum=1)
    failed = _integer(obj, "failed_attempts", path, minimum=0)
    failed_minutes = _decimal(
        obj,
        "failed_attempt_worker_minutes",
        path,
        minimum=Decimal(0),
    )
    if failed == 0 and failed_minutes != 0:
        raise EvidenceError(
            "inconsistent_evidence",
            f"{path}.failed_attempt_worker_minutes",
        )
    return RetryEvidence(successful, failed, failed_minutes)


def _validate_lanes(root: Mapping[str, Any]) -> LaneEvidence:
    path = "lanes"
    obj = _object(root[path], path, ("count", "availability_factor"))
    return LaneEvidence(
        count=_integer(obj, "count", path, minimum=1),
        availability_factor=_decimal(
            obj,
            "availability_factor",
            path,
            minimum=Decimal(0),
            maximum=Decimal(1),
            minimum_inclusive=False,
        ),
    )


def _candidate_audit_durations(
    audit_dict: Mapping[str, Any], path: str
) -> dict[str, Decimal]:
    duration_by_name: dict[str, Decimal] = {}
    for prefix in ("duration", "queue_age", "activation"):
        p95_name = f"{prefix}_p95_minutes"
        max_name = f"{prefix}_max_minutes"
        p95_value = _decimal(
            audit_dict,
            p95_name,
            path,
            minimum=Decimal(0),
        )
        max_value = _decimal(
            audit_dict,
            max_name,
            path,
            minimum=Decimal(0),
        )
        if p95_value > max_value:
            raise EvidenceError("inconsistent_evidence", f"{path}.{p95_name}")
        duration_by_name[p95_name] = p95_value
        duration_by_name[max_name] = max_value
    if duration_by_name["duration_max_minutes"] <= 0:
        raise EvidenceError("invalid_value", f"{path}.duration_max_minutes")
    if duration_by_name["activation_max_minutes"] <= 0:
        raise EvidenceError("invalid_value", f"{path}.activation_max_minutes")
    return duration_by_name


def _candidate_audit_workload(
    audit_dict: Mapping[str, Any], path: str, successful: int
) -> tuple[int, Decimal, int]:
    observed_http_requests = _integer(
        audit_dict, "observed_http_requests", path, minimum=1
    )
    observed_request_seconds = _decimal(
        audit_dict,
        "observed_request_seconds",
        path,
        minimum=Decimal(0),
        minimum_inclusive=False,
    )
    activations_with_http_floor = _integer(
        audit_dict, "activations_with_http_floor", path, minimum=0
    )
    _at_most(
        activations_with_http_floor,
        successful,
        f"{path}.activations_with_http_floor",
    )
    return observed_http_requests, observed_request_seconds, activations_with_http_floor


def _validate_candidate_audit(root: Mapping[str, Any]) -> CandidateAuditEvidence:
    """Validate candidate-audit counts, timings, and workload summaries."""

    path = "candidate_audit"
    audit_dict = _object(root[path], path, _CANDIDATE_AUDIT_FIELDS)
    sample_count = _integer(audit_dict, "sample_count", path, minimum=1)
    successful = _integer(audit_dict, "successful_audits", path, minimum=0)
    errors = _integer(audit_dict, "errors", path, minimum=0)
    _at_most(successful, sample_count, f"{path}.successful_audits")
    _at_most(errors, sample_count, f"{path}.errors")
    if successful + errors > sample_count:
        raise EvidenceError("inconsistent_evidence", f"{path}.errors")
    duration_by_name = _candidate_audit_durations(audit_dict, path)
    workload = _candidate_audit_workload(audit_dict, path, successful)

    return CandidateAuditEvidence(
        sample_count=sample_count,
        successful_audits=successful,
        duration_p95_minutes=duration_by_name["duration_p95_minutes"],
        duration_max_minutes=duration_by_name["duration_max_minutes"],
        queue_age_p95_minutes=duration_by_name["queue_age_p95_minutes"],
        queue_age_max_minutes=duration_by_name["queue_age_max_minutes"],
        activation_p95_minutes=duration_by_name["activation_p95_minutes"],
        activation_max_minutes=duration_by_name["activation_max_minutes"],
        lane_count=_integer(audit_dict, "lane_count", path, minimum=1),
        availability_factor=_decimal(
            audit_dict,
            "availability_factor",
            path,
            minimum=Decimal(0),
            maximum=Decimal(1),
            minimum_inclusive=False,
        ),
        http_requests_per_activation=_integer(
            audit_dict,
            "http_requests_per_activation",
            path,
            minimum=1,
        ),
        activations_with_http_floor=workload[2],
        observed_http_requests=workload[0],
        observed_request_seconds=workload[1],
        errors=errors,
    )


_PEAK_FIELDS = (
    "sample_windows",
    "observation_minutes",
    "window_minutes",
    "queue_delay_p95_minutes",
    "max_queue_delay_minutes",
    "audit_queue_age_p95_minutes",
    "max_audit_queue_age_minutes",
    "observed_peak_logical_imports",
    "observed_peak_unique_builds",
    "observed_peak_candidate_audits",
    "windows",
)
_PEAK_WINDOW_FIELDS = (
    "started_at",
    "ended_at",
    "logical_imports",
    "unique_builds",
    "candidate_audits",
    "max_queue_delay_minutes",
    "max_audit_queue_age_minutes",
)


def _peak_window_evidence(window_value: Any, window_path: str) -> PeakWindowEvidence:
    window_dict = _object(window_value, window_path, _PEAK_WINDOW_FIELDS)
    started_at = _timestamp(window_dict, "started_at", window_path)
    ended_at = _timestamp(window_dict, "ended_at", window_path)
    if started_at >= ended_at:
        raise EvidenceError("invalid_window", f"{window_path}.ended_at")
    logical_imports = _integer(window_dict, "logical_imports", window_path, minimum=0)
    unique_builds = _integer(window_dict, "unique_builds", window_path, minimum=0)
    candidate_audits = _integer(window_dict, "candidate_audits", window_path, minimum=0)
    _at_most(unique_builds, logical_imports, f"{window_path}.unique_builds")
    return PeakWindowEvidence(
        started_at=started_at,
        ended_at=ended_at,
        logical_imports=logical_imports,
        unique_builds=unique_builds,
        candidate_audits=candidate_audits,
        max_queue_delay_minutes=_decimal(
            window_dict, "max_queue_delay_minutes", window_path, minimum=Decimal(0)
        ),
        max_audit_queue_age_minutes=_decimal(
            window_dict,
            "max_audit_queue_age_minutes",
            window_path,
            minimum=Decimal(0),
        ),
    )


def _peak_windows(
    peak_dict: Mapping[str, Any], path: str
) -> tuple[PeakWindowEvidence, ...]:
    window_values = peak_dict["windows"]
    if not isinstance(window_values, list) or not window_values:
        raise EvidenceError("invalid_type", f"{path}.windows")
    return tuple(
        _peak_window_evidence(window_value, f"{path}.windows.{index}")
        for index, window_value in enumerate(window_values)
    )


def _peak_coverage(
    windows: tuple[PeakWindowEvidence, ...],
    window_minutes: Decimal,
    path: str,
) -> tuple[Decimal, Decimal]:
    previous_end: datetime | None = None
    maximum_gap = Decimal(0)
    covered_minutes = Decimal(0)
    for index, window in enumerate(windows):
        if previous_end is not None:
            gap_minutes = _duration_minutes(previous_end, window.started_at)
            if gap_minutes < 0:
                raise EvidenceError(
                    "overlapping_windows", f"{path}.windows.{index}.started_at"
                )
            maximum_gap = max(maximum_gap, gap_minutes)
        duration_minutes = _duration_minutes(window.started_at, window.ended_at)
        if duration_minutes != window_minutes:
            raise EvidenceError(
                "inconsistent_evidence", f"{path}.windows.{index}.ended_at"
            )
        covered_minutes += duration_minutes
        previous_end = window.ended_at
    return covered_minutes, maximum_gap


def _peak_counts(
    peak_dict: Mapping[str, Any],
    windows: tuple[PeakWindowEvidence, ...],
    path: str,
) -> tuple[int, Decimal, int, int, int]:
    sample_windows = _integer(peak_dict, "sample_windows", path, minimum=1)
    if sample_windows != len(windows):
        raise EvidenceError("inconsistent_evidence", f"{path}.sample_windows")
    observation_minutes = _decimal(
        peak_dict,
        "observation_minutes",
        path,
        minimum=Decimal(0),
        minimum_inclusive=False,
    )
    observed_span = _duration_minutes(windows[0].started_at, windows[-1].ended_at)
    if observation_minutes != observed_span:
        raise EvidenceError("inconsistent_evidence", f"{path}.observation_minutes")
    logical = _integer(peak_dict, "observed_peak_logical_imports", path, minimum=1)
    unique = _integer(peak_dict, "observed_peak_unique_builds", path, minimum=0)
    candidate_audits = _integer(
        peak_dict, "observed_peak_candidate_audits", path, minimum=0
    )
    _at_most(unique, logical, f"{path}.observed_peak_unique_builds")
    if logical != max(window.logical_imports for window in windows):
        raise EvidenceError(
            "inconsistent_evidence", f"{path}.observed_peak_logical_imports"
        )
    if unique != max(window.unique_builds for window in windows):
        raise EvidenceError(
            "inconsistent_evidence", f"{path}.observed_peak_unique_builds"
        )
    if candidate_audits != max(window.candidate_audits for window in windows):
        raise EvidenceError(
            "inconsistent_evidence", f"{path}.observed_peak_candidate_audits"
        )
    return sample_windows, observation_minutes, logical, unique, candidate_audits


def _peak_queue_metrics(
    peak_dict: Mapping[str, Any],
    windows: tuple[PeakWindowEvidence, ...],
    path: str,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    p95_queue = _decimal(peak_dict, "queue_delay_p95_minutes", path, minimum=Decimal(0))
    max_queue = _decimal(peak_dict, "max_queue_delay_minutes", path, minimum=Decimal(0))
    if p95_queue > max_queue:
        raise EvidenceError("inconsistent_evidence", f"{path}.queue_delay_p95_minutes")
    expected_p95 = _nearest_rank_p95(
        [window.max_queue_delay_minutes for window in windows]
    )
    if p95_queue != expected_p95:
        raise EvidenceError("raw_aggregate_mismatch", f"{path}.queue_delay_p95_minutes")
    if max_queue != max(window.max_queue_delay_minutes for window in windows):
        raise EvidenceError("inconsistent_evidence", f"{path}.max_queue_delay_minutes")
    audit_p95 = _decimal(
        peak_dict, "audit_queue_age_p95_minutes", path, minimum=Decimal(0)
    )
    audit_max = _decimal(
        peak_dict, "max_audit_queue_age_minutes", path, minimum=Decimal(0)
    )
    expected_audit_p95 = _nearest_rank_p95(
        [window.max_audit_queue_age_minutes for window in windows]
    )
    if audit_p95 != expected_audit_p95:
        raise EvidenceError(
            "raw_aggregate_mismatch", f"{path}.audit_queue_age_p95_minutes"
        )
    if audit_max != max(window.max_audit_queue_age_minutes for window in windows):
        raise EvidenceError(
            "inconsistent_evidence", f"{path}.max_audit_queue_age_minutes"
        )
    return p95_queue, max_queue, audit_p95, audit_max


def _validate_peak(root: Mapping[str, Any]) -> PeakArrivalEvidence:
    """Validate contiguous peak windows and recompute peak summaries."""

    path = "peak_arrival"
    peak_dict = _object(root[path], path, _PEAK_FIELDS)
    windows = _peak_windows(peak_dict, path)
    window_minutes = _decimal(
        peak_dict,
        "window_minutes",
        path,
        minimum=Decimal(0),
        minimum_inclusive=False,
    )
    covered_minutes, maximum_gap = _peak_coverage(windows, window_minutes, path)
    sample_windows, observation_minutes, logical, unique, candidate_audits = (
        _peak_counts(peak_dict, windows, path)
    )
    p95_queue, max_queue, audit_p95, audit_max = _peak_queue_metrics(
        peak_dict, windows, path
    )
    return PeakArrivalEvidence(
        sample_windows=sample_windows,
        observation_minutes=observation_minutes,
        window_minutes=window_minutes,
        queue_delay_p95_minutes=p95_queue,
        max_queue_delay_minutes=max_queue,
        audit_queue_age_p95_minutes=audit_p95,
        max_audit_queue_age_minutes=audit_max,
        observed_peak_logical_imports=logical,
        observed_peak_unique_builds=unique,
        observed_peak_candidate_audits=candidate_audits,
        coverage_ratio=covered_minutes / observation_minutes,
        max_gap_minutes=maximum_gap,
        windows=windows,
    )


def _validate_scratch(root: Mapping[str, Any]) -> ScratchEvidence:
    path = "scratch"
    fields = (
        "configured_concurrent_unique_builds",
        "measured_concurrent_unique_builds",
        "capacity_bytes",
        "baseline_used_bytes",
        "reserve_bytes",
        "measured_peak_incremental_bytes",
        "cleanup_cycles",
        "cleanup_failures",
    )
    scratch_dict = _object(root[path], path, fields)
    cycles = _integer(scratch_dict, "cleanup_cycles", path, minimum=1)
    failures = _integer(scratch_dict, "cleanup_failures", path, minimum=0)
    _at_most(failures, cycles, f"{path}.cleanup_failures")
    return ScratchEvidence(
        configured_concurrent_unique_builds=_integer(
            scratch_dict,
            "configured_concurrent_unique_builds",
            path,
            minimum=1,
        ),
        measured_concurrent_unique_builds=_integer(
            scratch_dict,
            "measured_concurrent_unique_builds",
            path,
            minimum=1,
        ),
        capacity_bytes=_integer(scratch_dict, "capacity_bytes", path, minimum=1),
        baseline_used_bytes=_integer(
            scratch_dict, "baseline_used_bytes", path, minimum=0
        ),
        reserve_bytes=_integer(scratch_dict, "reserve_bytes", path, minimum=0),
        measured_peak_incremental_bytes=_integer(
            scratch_dict,
            "measured_peak_incremental_bytes",
            path,
            minimum=1,
        ),
        cleanup_cycles=cycles,
        cleanup_failures=failures,
    )


def _validate_rate(rate_value: Mapping[str, Any], path: str) -> PostgresRateEvidence:
    fields = (
        "import_bytes_per_second",
        "api_bytes_per_second",
        "other_bytes_per_second",
        "sustainable_bytes_per_second",
    )
    rate_dict = _object(rate_value, path, fields)
    return PostgresRateEvidence(
        import_bytes_per_second=_decimal(
            rate_dict,
            "import_bytes_per_second",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        api_bytes_per_second=_decimal(
            rate_dict,
            "api_bytes_per_second",
            path,
            minimum=Decimal(0),
        ),
        other_bytes_per_second=_decimal(
            rate_dict,
            "other_bytes_per_second",
            path,
            minimum=Decimal(0),
        ),
        sustainable_bytes_per_second=_decimal(
            rate_dict,
            "sustainable_bytes_per_second",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
    )


def _validate_postgres_pool_wait(
    postgres_dict: Mapping[str, Any], path: str
) -> PostgresPoolWaitEvidence:
    pool_path = f"{path}.pool_wait"
    pool_dict = _object(
        postgres_dict["pool_wait"],
        pool_path,
        ("observations", "waited_acquisitions", "p95_ms", "max_ms", "timeout_errors"),
    )
    observations = _integer(pool_dict, "observations", pool_path, minimum=1)
    waited = _integer(pool_dict, "waited_acquisitions", pool_path, minimum=0)
    timeout_errors = _integer(pool_dict, "timeout_errors", pool_path, minimum=0)
    _at_most(waited, observations, f"{pool_path}.waited_acquisitions")
    _at_most(timeout_errors, observations, f"{pool_path}.timeout_errors")
    p95_ms = _decimal(pool_dict, "p95_ms", pool_path, minimum=Decimal(0))
    max_ms = _decimal(pool_dict, "max_ms", pool_path, minimum=Decimal(0))
    if p95_ms > max_ms:
        raise EvidenceError("inconsistent_evidence", f"{pool_path}.p95_ms")
    return PostgresPoolWaitEvidence(
        observations=observations,
        waited_acquisitions=waited,
        p95_ms=p95_ms,
        max_ms=max_ms,
        timeout_errors=timeout_errors,
    )


def _validate_postgres_checkpoint(
    postgres_dict: Mapping[str, Any], path: str
) -> PostgresCheckpointEvidence:
    checkpoint_path = f"{path}.checkpoint"
    checkpoint_dict = _object(
        postgres_dict["checkpoint"],
        checkpoint_path,
        ("sample_seconds", "completed", "requested", "write_seconds", "sync_seconds"),
    )
    sample_seconds = _decimal(
        checkpoint_dict,
        "sample_seconds",
        checkpoint_path,
        minimum=Decimal(0),
        minimum_inclusive=False,
    )
    write_seconds = _decimal(
        checkpoint_dict, "write_seconds", checkpoint_path, minimum=Decimal(0)
    )
    sync_seconds = _decimal(
        checkpoint_dict, "sync_seconds", checkpoint_path, minimum=Decimal(0)
    )
    if write_seconds + sync_seconds > sample_seconds:
        raise EvidenceError("inconsistent_evidence", f"{checkpoint_path}.write_seconds")
    return PostgresCheckpointEvidence(
        sample_seconds=sample_seconds,
        completed=_integer(checkpoint_dict, "completed", checkpoint_path, minimum=0),
        requested=_integer(checkpoint_dict, "requested", checkpoint_path, minimum=0),
        write_seconds=write_seconds,
        sync_seconds=sync_seconds,
    )


def _validate_postgresql(root: Mapping[str, Any]) -> PostgresEvidence:
    """Validate and materialize the PostgreSQL load evidence section."""

    path = "postgresql"
    postgres_dict = _object(
        root[path],
        path,
        (
            "load",
            "connections",
            "write",
            "wal",
            "pool_wait",
            "checkpoint",
            "temp",
            "autovacuum",
        ),
    )
    load_path = f"{path}.load"
    load_obj = _object(
        postgres_dict["load"],
        load_path,
        (
            "contention_run_id",
            "started_at",
            "ended_at",
            "concurrent_unique_builds",
            "concurrent_candidate_audits",
            "api_requests_per_second",
            "candidate_audit_requests_per_second",
            "sample_seconds",
        ),
    )
    connection_path = f"{path}.connections"
    connection_fields = (
        "max_connections",
        "reserved_connections",
        "peak_api_connections",
        "peak_import_connections",
        "peak_other_connections",
        "required_headroom_connections",
    )
    connection_obj = _object(
        postgres_dict["connections"], connection_path, connection_fields
    )
    load = PostgresLoadEvidence(
        contention_run_id=_digest(load_obj, "contention_run_id", load_path),
        started_at=_timestamp(load_obj, "started_at", load_path),
        ended_at=_timestamp(load_obj, "ended_at", load_path),
        concurrent_unique_builds=_integer(
            load_obj,
            "concurrent_unique_builds",
            load_path,
            minimum=1,
        ),
        concurrent_candidate_audits=_integer(
            load_obj,
            "concurrent_candidate_audits",
            load_path,
            minimum=1,
        ),
        api_requests_per_second=_decimal(
            load_obj,
            "api_requests_per_second",
            load_path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        candidate_audit_requests_per_second=_decimal(
            load_obj,
            "candidate_audit_requests_per_second",
            load_path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        sample_seconds=_decimal(
            load_obj,
            "sample_seconds",
            load_path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
    )
    if load.started_at >= load.ended_at:
        raise EvidenceError("invalid_window", f"{load_path}.ended_at")
    if _duration_seconds(load.started_at, load.ended_at) != load.sample_seconds:
        raise EvidenceError("inconsistent_evidence", f"{load_path}.sample_seconds")
    connections = PostgresConnectionEvidence(
        max_connections=_integer(
            connection_obj,
            "max_connections",
            connection_path,
            minimum=1,
        ),
        reserved_connections=_integer(
            connection_obj,
            "reserved_connections",
            connection_path,
            minimum=0,
        ),
        peak_api_connections=_integer(
            connection_obj,
            "peak_api_connections",
            connection_path,
            minimum=1,
        ),
        peak_import_connections=_integer(
            connection_obj,
            "peak_import_connections",
            connection_path,
            minimum=1,
        ),
        peak_other_connections=_integer(
            connection_obj,
            "peak_other_connections",
            connection_path,
            minimum=0,
        ),
        required_headroom_connections=_integer(
            connection_obj,
            "required_headroom_connections",
            connection_path,
            minimum=1,
        ),
    )

    pool_wait = _validate_postgres_pool_wait(postgres_dict, path)
    checkpoint = _validate_postgres_checkpoint(postgres_dict, path)

    temp_path = f"{path}.temp"
    temp_obj = _object(
        postgres_dict["temp"],
        temp_path,
        (
            "capacity_bytes",
            "baseline_used_bytes",
            "reserve_bytes",
            "measured_peak_incremental_bytes",
        ),
    )
    temp = PostgresTempEvidence(
        capacity_bytes=_integer(temp_obj, "capacity_bytes", temp_path, minimum=1),
        baseline_used_bytes=_integer(
            temp_obj,
            "baseline_used_bytes",
            temp_path,
            minimum=0,
        ),
        reserve_bytes=_integer(temp_obj, "reserve_bytes", temp_path, minimum=0),
        measured_peak_incremental_bytes=_integer(
            temp_obj,
            "measured_peak_incremental_bytes",
            temp_path,
            minimum=1,
        ),
    )

    vacuum_path = f"{path}.autovacuum"
    vacuum_obj = _object(
        postgres_dict["autovacuum"],
        vacuum_path,
        (
            "sample_seconds",
            "max_workers",
            "peak_workers",
            "required_headroom_workers",
            "completed_cycles",
            "cancelled_cycles",
            "oldest_pending_minutes",
        ),
    )
    max_workers = _integer(vacuum_obj, "max_workers", vacuum_path, minimum=1)
    peak_workers = _integer(vacuum_obj, "peak_workers", vacuum_path, minimum=0)
    required_workers = _integer(
        vacuum_obj,
        "required_headroom_workers",
        vacuum_path,
        minimum=1,
    )
    _at_most(peak_workers, max_workers, f"{vacuum_path}.peak_workers")
    _at_most(
        required_workers,
        max_workers,
        f"{vacuum_path}.required_headroom_workers",
    )
    autovacuum = PostgresAutovacuumEvidence(
        sample_seconds=_decimal(
            vacuum_obj,
            "sample_seconds",
            vacuum_path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        max_workers=max_workers,
        peak_workers=peak_workers,
        required_headroom_workers=required_workers,
        completed_cycles=_integer(
            vacuum_obj,
            "completed_cycles",
            vacuum_path,
            minimum=0,
        ),
        cancelled_cycles=_integer(
            vacuum_obj,
            "cancelled_cycles",
            vacuum_path,
            minimum=0,
        ),
        oldest_pending_minutes=_decimal(
            vacuum_obj,
            "oldest_pending_minutes",
            vacuum_path,
            minimum=Decimal(0),
        ),
    )
    return PostgresEvidence(
        load=load,
        connections=connections,
        write=_validate_rate(postgres_dict["write"], f"{path}.write"),
        wal=_validate_rate(postgres_dict["wal"], f"{path}.wal"),
        pool_wait=pool_wait,
        checkpoint=checkpoint,
        temp=temp,
        autovacuum=autovacuum,
    )


def _validate_storage(root: Mapping[str, Any]) -> StorageEvidence:
    """Validate storage evidence, including observed count and byte consistency."""

    path = "storage"
    fields = (
        "physical_unique_builds_observed",
        "physical_net_new_bytes_observed",
        "physical_max_bytes_per_unique_build",
        "logical_imports_observed",
        "logical_net_new_bytes_observed",
        "logical_max_bytes_per_import",
        "current_used_bytes",
        "capacity_bytes",
        "reserve_bytes",
        "physical_retention_months",
        "logical_retention_months",
    )
    storage_dict = _object(root[path], path, fields)
    physical_count = _integer(
        storage_dict,
        "physical_unique_builds_observed",
        path,
        minimum=1,
    )
    physical_total = _integer(
        storage_dict,
        "physical_net_new_bytes_observed",
        path,
        minimum=0,
    )
    physical_max = _integer(
        storage_dict,
        "physical_max_bytes_per_unique_build",
        path,
        minimum=1,
    )
    logical_count = _integer(storage_dict, "logical_imports_observed", path, minimum=1)
    logical_total = _integer(
        storage_dict,
        "logical_net_new_bytes_observed",
        path,
        minimum=0,
    )
    logical_max = _integer(
        storage_dict,
        "logical_max_bytes_per_import",
        path,
        minimum=1,
    )
    if physical_max * physical_count < physical_total:
        raise EvidenceError(
            "inconsistent_evidence",
            f"{path}.physical_max_bytes_per_unique_build",
        )
    if logical_max * logical_count < logical_total:
        raise EvidenceError(
            "inconsistent_evidence",
            f"{path}.logical_max_bytes_per_import",
        )
    return StorageEvidence(
        physical_unique_builds_observed=physical_count,
        physical_net_new_bytes_observed=physical_total,
        physical_max_bytes_per_unique_build=physical_max,
        logical_imports_observed=logical_count,
        logical_net_new_bytes_observed=logical_total,
        logical_max_bytes_per_import=logical_max,
        current_used_bytes=_integer(
            storage_dict, "current_used_bytes", path, minimum=0
        ),
        capacity_bytes=_integer(storage_dict, "capacity_bytes", path, minimum=1),
        reserve_bytes=_integer(storage_dict, "reserve_bytes", path, minimum=0),
        physical_retention_months=_decimal(
            storage_dict,
            "physical_retention_months",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        logical_retention_months=_decimal(
            storage_dict,
            "logical_retention_months",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
    )


_GC_FIELDS = (
    "window_hours",
    "cycles_observed",
    "executed_cycles",
    "overlap_count",
    "reference_recheck_failures",
    "starting_backlog_bytes",
    "starting_backlog_layouts",
    "newly_eligible_bytes",
    "deleted_bytes",
    "ending_backlog_bytes",
    "ending_backlog_layouts",
    "max_backlog_bytes",
    "max_clearance_hours",
    "eligible_layouts",
    "deleted_layouts",
)


def _gc_backlog_counts(
    gc_dict: Mapping[str, Any], path: str
) -> tuple[int, int, int, int]:
    starting_bytes = _integer(gc_dict, "starting_backlog_bytes", path, minimum=0)
    eligible_bytes = _integer(gc_dict, "newly_eligible_bytes", path, minimum=0)
    deleted_bytes = _integer(gc_dict, "deleted_bytes", path, minimum=0)
    ending_bytes = _integer(gc_dict, "ending_backlog_bytes", path, minimum=0)
    if (
        deleted_bytes > starting_bytes + eligible_bytes
        or ending_bytes != starting_bytes + eligible_bytes - deleted_bytes
    ):
        raise EvidenceError("inconsistent_evidence", f"{path}.ending_backlog_bytes")
    return starting_bytes, eligible_bytes, deleted_bytes, ending_bytes


def _gc_backlog_layout_counts(
    gc_dict: Mapping[str, Any], path: str
) -> tuple[int, int, int, int]:
    starting_layouts = _integer(gc_dict, "starting_backlog_layouts", path, minimum=0)
    eligible_layouts = _integer(gc_dict, "eligible_layouts", path, minimum=1)
    deleted_layouts = _integer(gc_dict, "deleted_layouts", path, minimum=0)
    ending_layouts = _integer(gc_dict, "ending_backlog_layouts", path, minimum=0)
    if (
        deleted_layouts > starting_layouts + eligible_layouts
        or ending_layouts != starting_layouts + eligible_layouts - deleted_layouts
    ):
        raise EvidenceError("inconsistent_evidence", f"{path}.ending_backlog_layouts")
    return starting_layouts, eligible_layouts, deleted_layouts, ending_layouts


def _validate_gc(root: Mapping[str, Any]) -> GcEvidence:
    """Validate GC window accounting and backlog conservation."""

    path = "gc"
    gc_dict = _object(root[path], path, _GC_FIELDS)
    cycles = _integer(gc_dict, "cycles_observed", path, minimum=1)
    executed = _integer(gc_dict, "executed_cycles", path, minimum=0)
    _at_most(executed, cycles, f"{path}.executed_cycles")
    start, eligible, deleted, end = _gc_backlog_counts(gc_dict, path)
    starting_layouts, eligible_layouts, deleted_layouts, ending_layouts = (
        _gc_backlog_layout_counts(gc_dict, path)
    )
    return GcEvidence(
        window_hours=_decimal(
            gc_dict,
            "window_hours",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        cycles_observed=cycles,
        executed_cycles=executed,
        overlap_count=_integer(gc_dict, "overlap_count", path, minimum=0),
        reference_recheck_failures=_integer(
            gc_dict,
            "reference_recheck_failures",
            path,
            minimum=0,
        ),
        starting_backlog_bytes=start,
        starting_backlog_layouts=starting_layouts,
        newly_eligible_bytes=eligible,
        deleted_bytes=deleted,
        ending_backlog_bytes=end,
        ending_backlog_layouts=ending_layouts,
        max_backlog_bytes=_integer(gc_dict, "max_backlog_bytes", path, minimum=0),
        max_clearance_hours=_decimal(
            gc_dict,
            "max_clearance_hours",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        eligible_layouts=eligible_layouts,
        deleted_layouts=deleted_layouts,
    )


def _validate_api_class(class_value: Any, path: str) -> ApiClassEvidence:
    fields = (
        "samples",
        "distinct_query_keys",
        "contention_samples",
        "contention_distinct_query_keys",
        "errors",
        "cold_first_page_p95_ms",
        "sample_started_at",
        "sample_ended_at",
    )
    class_obj = _object(class_value, path, fields)
    samples = _integer(class_obj, "samples", path, minimum=1)
    distinct = _integer(class_obj, "distinct_query_keys", path, minimum=1)
    contention_samples = _integer(
        class_obj,
        "contention_samples",
        path,
        minimum=1,
    )
    contention_distinct = _integer(
        class_obj,
        "contention_distinct_query_keys",
        path,
        minimum=1,
    )
    errors = _integer(class_obj, "errors", path, minimum=0)
    _at_most(distinct, samples, f"{path}.distinct_query_keys")
    _at_most(contention_samples, samples, f"{path}.contention_samples")
    _at_most(
        contention_distinct,
        contention_samples,
        f"{path}.contention_distinct_query_keys",
    )
    _at_most(errors, samples, f"{path}.errors")
    sample_started_at = _timestamp(class_obj, "sample_started_at", path)
    sample_ended_at = _timestamp(class_obj, "sample_ended_at", path)
    if sample_started_at >= sample_ended_at:
        raise EvidenceError("invalid_window", f"{path}.sample_ended_at")
    return ApiClassEvidence(
        samples=samples,
        distinct_query_keys=distinct,
        contention_samples=contention_samples,
        contention_distinct_query_keys=contention_distinct,
        errors=errors,
        cold_first_page_p95_ms=_decimal(
            class_obj,
            "cold_first_page_p95_ms",
            path,
            minimum=Decimal(0),
        ),
        sample_started_at=sample_started_at,
        sample_ended_at=sample_ended_at,
    )


_API_FIELDS = (
    "contention_run_id",
    "concurrent_unique_builds",
    "concurrent_candidate_audits",
    "contention_seconds",
    "contention_started_at",
    "contention_ended_at",
    "fresh_processes",
    "planned_requests",
    "attempted_requests",
    "succeeded_requests",
    "failed_requests",
    "retried_requests",
    "requests",
    "requests_while_imports_running",
    "candidate_audit_requests",
    "candidate_audit_requests_while_imports_running",
    "errors",
    "matched_positive",
    "negative",
    "random",
)


def _api_request_counts(
    api_dict: Mapping[str, Any], path: str
) -> Mapping[str, int]:
    count_map = {
        "planned_requests": _integer(
            api_dict, "planned_requests", path, minimum=1
        ),
        "attempted_requests": _integer(
            api_dict, "attempted_requests", path, minimum=1
        ),
        "succeeded_requests": _integer(
            api_dict, "succeeded_requests", path, minimum=0
        ),
        "failed_requests": _integer(
            api_dict, "failed_requests", path, minimum=0
        ),
        "retried_requests": _integer(
            api_dict, "retried_requests", path, minimum=0
        ),
        "requests": _integer(api_dict, "requests", path, minimum=1),
        "requests_while_imports_running": _integer(
            api_dict, "requests_while_imports_running", path, minimum=0
        ),
        "candidate_audit_requests": _integer(
            api_dict, "candidate_audit_requests", path, minimum=1
        ),
        "candidate_audit_requests_while_imports_running": _integer(
            api_dict,
            "candidate_audit_requests_while_imports_running",
            path,
            minimum=0,
        ),
        "errors": _integer(api_dict, "errors", path, minimum=0),
    }
    if (
        count_map["succeeded_requests"] + count_map["failed_requests"]
        != count_map["attempted_requests"]
    ):
        raise EvidenceError("inconsistent_evidence", f"{path}.attempted_requests")
    if (
        count_map["planned_requests"] + count_map["retried_requests"]
        != count_map["attempted_requests"]
    ):
        raise EvidenceError("inconsistent_evidence", f"{path}.retried_requests")
    if count_map["requests"] > count_map["attempted_requests"]:
        raise EvidenceError("inconsistent_evidence", f"{path}.requests")
    if count_map["errors"] != count_map["failed_requests"]:
        raise EvidenceError("inconsistent_evidence", f"{path}.errors")
    _at_most(
        count_map["requests_while_imports_running"],
        count_map["requests"],
        f"{path}.requests_while_imports_running",
    )
    _at_most(
        count_map["candidate_audit_requests_while_imports_running"],
        count_map["candidate_audit_requests"],
        f"{path}.candidate_audit_requests_while_imports_running",
    )
    return count_map


def _api_class_evidence(
    api_dict: Mapping[str, Any], path: str, requests: int, errors: int
) -> tuple[ApiClassEvidence, ApiClassEvidence, ApiClassEvidence]:
    matched_positive = _validate_api_class(
        api_dict["matched_positive"],
        f"{path}.matched_positive",
    )
    negative = _validate_api_class(api_dict["negative"], f"{path}.negative")
    random = _validate_api_class(api_dict["random"], f"{path}.random")
    classes = (matched_positive, negative, random)
    if sum(class_evidence.samples for class_evidence in classes) > requests:
        raise EvidenceError("inconsistent_evidence", f"{path}.requests")
    if sum(class_evidence.errors for class_evidence in classes) > errors:
        raise EvidenceError("inconsistent_evidence", f"{path}.errors")
    return classes


def _api_contention_window(
    api_dict: Mapping[str, Any],
    path: str,
    classes: tuple[ApiClassEvidence, ApiClassEvidence, ApiClassEvidence],
) -> tuple[datetime, datetime, Decimal]:
    contention_started_at = _timestamp(api_dict, "contention_started_at", path)
    contention_ended_at = _timestamp(api_dict, "contention_ended_at", path)
    if contention_started_at >= contention_ended_at:
        raise EvidenceError("invalid_window", f"{path}.contention_ended_at")
    contention_seconds = _decimal(
        api_dict,
        "contention_seconds",
        path,
        minimum=Decimal(0),
        minimum_inclusive=False,
    )
    timestamp_seconds = Decimal(
        str((contention_ended_at - contention_started_at).total_seconds())
    )
    if contention_seconds != timestamp_seconds:
        raise EvidenceError("inconsistent_evidence", f"{path}.contention_seconds")
    for class_name, class_evidence in zip(
        ("matched_positive", "negative", "random"),
        classes,
        strict=True,
    ):
        if not (
            contention_started_at <= class_evidence.sample_started_at
            and class_evidence.sample_ended_at <= contention_ended_at
        ):
            raise EvidenceError(
                "sample_outside_contention",
                f"{path}.{class_name}.sample_started_at",
            )
    return contention_started_at, contention_ended_at, contention_seconds


def _validate_api(root: Mapping[str, Any]) -> ApiEvidence:
    """Validate contention identity, duration, and HTTP class summaries."""

    path = "api"
    api_dict = _object(root[path], path, _API_FIELDS)
    counts = _api_request_counts(api_dict, path)
    classes = _api_class_evidence(
        api_dict, path, counts["requests"], counts["errors"]
    )
    contention_started_at, contention_ended_at, contention_seconds = (
        _api_contention_window(api_dict, path, classes)
    )
    return ApiEvidence(
        contention_run_id=_digest(api_dict, "contention_run_id", path),
        concurrent_unique_builds=_integer(
            api_dict,
            "concurrent_unique_builds",
            path,
            minimum=1,
        ),
        concurrent_candidate_audits=_integer(
            api_dict,
            "concurrent_candidate_audits",
            path,
            minimum=1,
        ),
        contention_seconds=contention_seconds,
        contention_started_at=contention_started_at,
        contention_ended_at=contention_ended_at,
        fresh_processes=_boolean(api_dict, "fresh_processes", path),
        planned_requests=counts["planned_requests"],
        attempted_requests=counts["attempted_requests"],
        succeeded_requests=counts["succeeded_requests"],
        failed_requests=counts["failed_requests"],
        retried_requests=counts["retried_requests"],
        requests=counts["requests"],
        requests_while_imports_running=counts["requests_while_imports_running"],
        candidate_audit_requests=counts["candidate_audit_requests"],
        candidate_audit_requests_while_imports_running=counts[
            "candidate_audit_requests_while_imports_running"
        ],
        errors=counts["errors"],
        matched_positive=classes[0],
        negative=classes[1],
        random=classes[2],
    )


def _import_timestamp_fields(
    sample_object: Mapping[str, Any], path: str
) -> dict[str, datetime]:
    names = (
        "enqueued_at",
        "build_started_at",
        "build_completed_at",
        "source_audit_started_at",
        "source_audit_completed_at",
        "candidate_attested_at",
        "activated_at",
    )
    timestamp_by_stage = {name: _timestamp(sample_object, name, path) for name in names}
    if any(
        timestamp_by_stage[left] > timestamp_by_stage[right]
        for left, right in zip(names, names[1:])
    ):
        raise EvidenceError("invalid_stage_order", f"{path}.activated_at")
    return timestamp_by_stage


def _import_boolean_fields(
    sample_object: Mapping[str, Any], path: str
) -> dict[str, bool]:
    names = _IMPORT_SAMPLE_FIELDS[11:21]
    return {name: _boolean(sample_object, name, path) for name in names}


def _validate_import_sample(value: Any, path: str) -> ImportLifecycleSample:
    sample_object = _object(value, path, _IMPORT_SAMPLE_FIELDS)
    timestamp_by_stage = _import_timestamp_fields(sample_object, path)
    failed_attempts = _integer(sample_object, "failed_attempts", path, minimum=0)
    failed_attempt_worker_seconds = _decimal(
        sample_object,
        "failed_attempt_worker_seconds",
        path,
        minimum=Decimal(0),
    )
    if (failed_attempts == 0) != (failed_attempt_worker_seconds == 0):
        raise EvidenceError(
            "inconsistent_evidence", f"{path}.failed_attempt_worker_seconds"
        )
    return ImportLifecycleSample(
        import_id_sha256=_digest(sample_object, "import_id_sha256", path),
        contention_run_id=_digest(sample_object, "contention_run_id", path),
        kind=_enum_string(sample_object, "kind", path, ("unique_build", "reuse")),
        status=_enum_string(sample_object, "status", path, ("succeeded", "failed")),
        **timestamp_by_stage,
        **_import_boolean_fields(sample_object, path),
        failed_attempts=failed_attempts,
        failed_attempt_worker_seconds=failed_attempt_worker_seconds,
    )


def _validate_import_sample_collection(
    values: Any, contention_run_id: str
) -> tuple[ImportLifecycleSample, ...]:
    samples = tuple(
        _validate_import_sample(value, f"raw_samples.import_lifecycle.{index}")
        for index, value in enumerate(
            _sample_list(values, "raw_samples.import_lifecycle")
        )
    )
    identifiers = [sample.import_id_sha256 for sample in samples]
    if len(set(identifiers)) != len(identifiers):
        raise EvidenceError("duplicate_sample_id", "raw_samples.import_lifecycle")
    if any(sample.contention_run_id != contention_run_id for sample in samples):
        raise EvidenceError("contention_run_mismatch", "raw_samples.import_lifecycle")
    return samples


def _audit_timestamp_fields(
    sample_object: Mapping[str, Any], path: str
) -> dict[str, datetime]:
    names = (
        "queued_at",
        "started_at",
        "first_request_at",
        "last_request_at",
        "completed_at",
        "candidate_attested_at",
    )
    timestamp_by_stage = {name: _timestamp(sample_object, name, path) for name in names}
    if any(
        timestamp_by_stage[left] > timestamp_by_stage[right]
        for left, right in zip(names, names[1:])
    ):
        raise EvidenceError("invalid_stage_order", f"{path}.candidate_attested_at")
    return timestamp_by_stage


def _validate_audit_sample(value: Any, path: str) -> AuditResultSample:
    sample_object = _object(value, path, _AUDIT_SAMPLE_FIELDS)
    request_count = _integer(sample_object, "http_requests", path, minimum=1)
    success_count = _integer(sample_object, "http_successes", path, minimum=0)
    error_count = _integer(sample_object, "http_errors", path, minimum=0)
    if success_count + error_count != request_count:
        raise EvidenceError("inconsistent_evidence", f"{path}.http_requests")
    return AuditResultSample(
        audit_id_sha256=_digest(sample_object, "audit_id_sha256", path),
        import_id_sha256=_digest(sample_object, "import_id_sha256", path),
        contention_run_id=_digest(sample_object, "contention_run_id", path),
        status=_enum_string(sample_object, "status", path, ("passed", "failed")),
        **_audit_timestamp_fields(sample_object, path),
        http_requests=request_count,
        http_successes=success_count,
        http_errors=error_count,
    )


def _require_audit_import_join(
    audit_sample: AuditResultSample,
    import_sample: ImportLifecycleSample,
    path: str,
) -> None:
    expected_timestamps = (
        (audit_sample.queued_at, import_sample.build_completed_at, "queued_at"),
        (audit_sample.started_at, import_sample.source_audit_started_at, "started_at"),
        (
            audit_sample.completed_at,
            import_sample.source_audit_completed_at,
            "completed_at",
        ),
        (
            audit_sample.candidate_attested_at,
            import_sample.candidate_attested_at,
            "candidate_attested_at",
        ),
    )
    for actual, expected, field_name in expected_timestamps:
        _require_equal(actual, expected, f"{path}.{field_name}")


def _validate_audit_sample_collection(
    values: Any,
    imports: tuple[ImportLifecycleSample, ...],
    contention_run_id: str,
) -> tuple[AuditResultSample, ...]:
    audit_values = _sample_list(values, "raw_samples.audit_results")
    audits = tuple(
        _validate_audit_sample(value, f"raw_samples.audit_results.{index}")
        for index, value in enumerate(audit_values)
    )
    import_by_id = {sample.import_id_sha256: sample for sample in imports}
    audit_ids = [sample.audit_id_sha256 for sample in audits]
    joined_import_ids = [sample.import_id_sha256 for sample in audits]
    if len(set(audit_ids)) != len(audit_ids):
        raise EvidenceError("duplicate_sample_id", "raw_samples.audit_results")
    if set(joined_import_ids) != set(import_by_id) or len(joined_import_ids) != len(
        imports
    ):
        raise EvidenceError("import_audit_join_mismatch", "raw_samples.audit_results")
    for index, audit_sample in enumerate(audits):
        if audit_sample.contention_run_id != contention_run_id:
            raise EvidenceError("contention_run_mismatch", "raw_samples.audit_results")
        _require_audit_import_join(
            audit_sample,
            import_by_id[audit_sample.import_id_sha256],
            f"raw_samples.audit_results.{index}",
        )
    return audits


def _validate_peak_import_event(value: Any, path: str) -> PeakImportEvent:
    event_object = _object(value, path, _PEAK_IMPORT_EVENT_FIELDS)
    enqueued_at = _timestamp(event_object, "enqueued_at", path)
    build_started_at = _timestamp(event_object, "build_started_at", path)
    if enqueued_at > build_started_at:
        raise EvidenceError("invalid_stage_order", f"{path}.build_started_at")
    return PeakImportEvent(
        import_id_sha256=_digest(event_object, "import_id_sha256", path),
        kind=_enum_string(event_object, "kind", path, ("unique_build", "reuse")),
        enqueued_at=enqueued_at,
        build_started_at=build_started_at,
    )


def _validate_peak_import_event_collection(values: Any) -> tuple[PeakImportEvent, ...]:
    path = "raw_samples.peak_import_events"
    events = tuple(
        _validate_peak_import_event(value, f"{path}.{index}")
        for index, value in enumerate(_sample_list(values, path))
    )
    identifiers = [event.import_id_sha256 for event in events]
    if len(set(identifiers)) != len(identifiers):
        raise EvidenceError("duplicate_sample_id", path)
    return events


def _validate_peak_audit_event(value: Any, path: str) -> PeakAuditEvent:
    event_object = _object(value, path, _PEAK_AUDIT_EVENT_FIELDS)
    queued_at = _timestamp(event_object, "queued_at", path)
    started_at = _timestamp(event_object, "started_at", path)
    if queued_at > started_at:
        raise EvidenceError("invalid_stage_order", f"{path}.started_at")
    return PeakAuditEvent(
        audit_id_sha256=_digest(event_object, "audit_id_sha256", path),
        import_id_sha256=_digest(event_object, "import_id_sha256", path),
        queued_at=queued_at,
        started_at=started_at,
    )


def _validate_peak_audit_event_collection(values: Any) -> tuple[PeakAuditEvent, ...]:
    path = "raw_samples.peak_audit_events"
    events = tuple(
        _validate_peak_audit_event(value, f"{path}.{index}")
        for index, value in enumerate(_sample_list(values, path))
    )
    audit_ids = [event.audit_id_sha256 for event in events]
    import_ids = [event.import_id_sha256 for event in events]
    if len(set(audit_ids)) != len(audit_ids) or len(set(import_ids)) != len(import_ids):
        raise EvidenceError("duplicate_sample_id", path)
    return events


def _validate_http_semantics(sample: HttpSample, path: str) -> None:
    if sample.response_status != 200:
        return
    if sample.semantic_class == "matched_positive" and sample.result_count < 1:
        raise EvidenceError("semantic_class_mismatch", f"{path}.result_count")
    if sample.semantic_class == "negative" and sample.result_count != 0:
        raise EvidenceError("semantic_class_mismatch", f"{path}.result_count")


def _require_api_payload_value(
    payload: Mapping[str, Any],
    name: str,
    expected: Any,
    path: str,
    error_code: str,
) -> None:
    actual = payload[name]
    if isinstance(expected, str):
        if not isinstance(actual, str) or not secrets.compare_digest(actual, expected):
            raise EvidenceError(error_code, f"{path}.{name}")
        return
    if type(actual) is not type(expected) or actual != expected:
        raise EvidenceError(error_code, f"{path}.{name}")


def _verify_api_protocol_contract(
    signed_payload: Mapping[str, Any], path: str
) -> None:
    protocol_fields_by_name = {
        "evidence_version": _API_EVIDENCE_VERSION,
        "signature_version": _API_SIGNATURE_VERSION,
        "signature_domain": _API_SIGNATURE_DOMAIN,
        "signature_algorithm": _API_SIGNATURE_ALGORITHM,
        "method": "GET",
        "path": _API_QUERY_PATH,
        "query_contract": _API_QUERY_CONTRACT_ID,
        "query_contract_digest": _API_QUERY_CONTRACT_DIGEST,
        "page_limit": _API_PAGE_LIMIT,
        "isolated": True,
        "observation_ordinal": _API_OBSERVATION_ORDINAL,
    }
    for name, expected in protocol_fields_by_name.items():
        _require_api_payload_value(
            signed_payload, name, expected, path, "api_evidence_protocol_mismatch"
        )


def _verify_api_identity_bindings(
    signed_payload: Mapping[str, Any],
    path: str,
    trust: ReceiptTrust,
    receipt: VerifiedReceipt,
) -> None:
    _require_api_payload_value(
        signed_payload,
        "api_evidence_key_id",
        trust.api_evidence_key_id,
        path,
        "api_evidence_key_mismatch",
    )
    for name, trusted_value, receipt_value in (
        ("release_digest", trust.release_digest, receipt.release_digest),
        ("environment_id", trust.environment_id, receipt.environment_id),
    ):
        _require_api_payload_value(
            signed_payload, name, trusted_value, path, f"api_evidence_{name}_mismatch"
        )
        if not secrets.compare_digest(trusted_value, receipt_value):
            raise EvidenceError(f"api_evidence_{name}_mismatch", f"{path}.{name}")


def _verify_api_signed_payload(
    sample_object: Mapping[str, Any],
    path: str,
    trust: ReceiptTrust,
    receipt: VerifiedReceipt,
) -> Mapping[str, Any]:
    """Verify one signed API evidence payload and its release bindings."""

    signed_payload_by_field = {
        name: sample_object[name] for name in _API_SIGNED_PAYLOAD_FIELDS
    }
    signature_field = f"{path}.api_evidence_signature"
    signature = _base64url_decode_signature(
        sample_object["api_evidence_signature"], signature_field
    )
    try:
        Ed25519PublicKey.from_public_bytes(trust.api_evidence_public_key).verify(
            signature,
            _api_signature_message(signed_payload_by_field),
        )
    except (InvalidSignature, ValueError):
        raise EvidenceError("invalid_api_evidence_signature", signature_field)

    _verify_api_protocol_contract(signed_payload_by_field, path)
    _verify_api_identity_bindings(signed_payload_by_field, path, trust, receipt)
    return signed_payload_by_field


def _http_sample_timestamps(
    sample_object: Mapping[str, Any], path: str
) -> tuple[datetime, datetime, datetime, datetime]:
    process_started_at = _timestamp(sample_object, "process_started_at", path)
    server_received_at = _timestamp(sample_object, "server_received_at", path)
    server_observed_at = _timestamp(sample_object, "server_observed_at", path)
    collector_received_at = _timestamp(sample_object, "collector_received_at", path)
    process_age = _duration_seconds(process_started_at, server_received_at)
    receive_skew = _duration_seconds(server_observed_at, collector_received_at)
    if process_age < 0 or process_age > _API_MAX_PROCESS_AGE_SECONDS:
        raise EvidenceError("stale_process", f"{path}.process_started_at")
    if receive_skew < 0 or receive_skew > _API_MAX_RECEIVE_SKEW_SECONDS:
        raise EvidenceError("collector_receive_skew", f"{path}.collector_received_at")
    if not process_started_at <= server_received_at <= server_observed_at:
        raise EvidenceError("invalid_timestamp", f"{path}.server_observed_at")
    return (
        process_started_at,
        server_received_at,
        server_observed_at,
        collector_received_at,
    )


def _validated_server_duration_ns(
    sample_object: Mapping[str, Any],
    path: str,
    server_received_at: datetime,
    server_observed_at: datetime,
) -> int:
    duration_ns = _integer(sample_object, "server_duration_ns", path, minimum=0)
    if duration_ns > _API_MAX_DURATION_NS:
        raise EvidenceError("invalid_value", f"{path}.server_duration_ns")
    wall_duration_seconds = _duration_seconds(
        server_received_at, server_observed_at
    )
    monotonic_duration_seconds = Decimal(duration_ns) / Decimal(1_000_000_000)
    if abs(monotonic_duration_seconds - wall_duration_seconds) > 1:
        raise EvidenceError("duration_timestamp_mismatch", f"{path}.server_duration_ns")
    return duration_ns


def _validate_http_sample_context(
    sample: HttpSample, expected_class: str, api: ApiEvidence, path: str
) -> None:
    if sample.semantic_class != expected_class:
        raise EvidenceError("semantic_class_mismatch", f"{path}.semantic_class")
    if sample.selection_method != _HTTP_SELECTION_METHODS[expected_class]:
        raise EvidenceError("semantic_class_mismatch", f"{path}.selection_method")
    if not sample.cold:
        raise EvidenceError("cold_observation_required", f"{path}.cold")
    if not sample.first_observation:
        raise EvidenceError("first_observation_required", f"{path}.first_observation")
    if sample.contention_run_id != api.contention_run_id:
        raise EvidenceError("contention_run_mismatch", f"{path}.contention_run_id")
    if not (
        api.contention_started_at
        <= sample.server_received_at
        <= sample.server_observed_at
        < api.contention_ended_at
    ):
        raise EvidenceError("sample_outside_contention", f"{path}.server_observed_at")
    _validate_http_semantics(sample, path)


def _validated_http_response_status(
    sample_object: Mapping[str, Any], path: str
) -> int:
    response_status = _integer(
        sample_object, "response_status", path, minimum=100
    )
    if response_status > 599:
        raise EvidenceError("invalid_value", f"{path}.response_status")
    return response_status


def _validate_http_sample(
    sample_value: Any,
    path: str,
    expected_class: str,
    api: ApiEvidence,
    trust: ReceiptTrust,
    receipt: VerifiedReceipt,
) -> HttpSample:
    """Validate one signed HTTP sample against its contention context."""

    sample_object = _object(sample_value, path, _HTTP_SAMPLE_FIELDS)
    _verify_api_signed_payload(sample_object, path, trust, receipt)
    (
        process_started_at,
        server_received_at,
        server_observed_at,
        collector_received_at,
    ) = _http_sample_timestamps(sample_object, path)
    server_duration_ns = _validated_server_duration_ns(
        sample_object,
        path,
        server_received_at,
        server_observed_at,
    )
    response_status = _validated_http_response_status(sample_object, path)
    sample = HttpSample(
        challenge_digest=_digest(sample_object, "challenge_digest", path),
        run_digest=_digest(sample_object, "run_digest", path),
        semantic_query_digest=_digest(sample_object, "semantic_query_digest", path),
        scope_digest=_digest(sample_object, "scope_digest", path),
        process_instance_digest=_digest(sample_object, "process_instance_digest", path),
        process_started_at=process_started_at,
        server_received_at=server_received_at,
        server_observed_at=server_observed_at,
        server_duration_ns=server_duration_ns,
        collector_received_at=collector_received_at,
        page_limit=_integer(sample_object, "page_limit", path, minimum=1),
        contention_run_id=_digest(sample_object, "contention_run_id", path),
        semantic_class=_enum_string(
            sample_object, "semantic_class", path, tuple(_HTTP_SELECTION_METHODS)
        ),
        selection_method=_enum_string(
            sample_object,
            "selection_method",
            path,
            tuple(_HTTP_SELECTION_METHODS.values()),
        ),
        selection_ordinal=_integer(sample_object, "selection_ordinal", path, minimum=0),
        cold=_boolean(sample_object, "cold", path),
        first_observation=_boolean(sample_object, "first_observation", path),
        response_status=response_status,
        response_body_sha256=_digest(sample_object, "response_body_sha256", path),
        result_count=_integer(sample_object, "result_count", path, minimum=0),
        first_page_ms=Decimal(server_duration_ns) / Decimal(1_000_000),
    )
    _validate_http_sample_context(sample, expected_class, api, path)
    return sample


def _validate_http_class_samples(
    values: Any,
    class_name: str,
    api: ApiEvidence,
    trust: ReceiptTrust,
    receipt: VerifiedReceipt,
) -> tuple[HttpSample, ...]:
    path = f"raw_samples.http_{class_name}"
    samples = tuple(
        _validate_http_sample(value, f"{path}.{index}", class_name, api, trust, receipt)
        for index, value in enumerate(_sample_list(values, path))
    )
    request_ids = [sample.challenge_digest for sample in samples]
    query_keys = [sample.semantic_query_digest for sample in samples]
    ordinals = sorted(sample.selection_ordinal for sample in samples)
    if len(set(request_ids)) != len(request_ids):
        raise EvidenceError("duplicate_sample_id", path)
    if len(set(query_keys)) != len(query_keys):
        raise EvidenceError("duplicate_query_key", path)
    process_keys = [sample.process_instance_digest for sample in samples]
    if len(set(process_keys)) != len(process_keys):
        raise EvidenceError("reused_api_process", path)
    if ordinals != list(range(len(samples))):
        raise EvidenceError("invalid_selection_ordinals", path)
    return samples


def _interval_coverage_metrics(
    intervals: Sequence[tuple[datetime, datetime]],
    started_at: datetime,
    ended_at: datetime,
) -> tuple[Decimal, Decimal]:
    clipped = sorted(
        (max(start, started_at), min(end, ended_at))
        for start, end in intervals
        if start < ended_at and end > started_at
    )
    if not clipped:
        return Decimal(0), _duration_seconds(started_at, ended_at)
    cursor = started_at
    covered_seconds = Decimal(0)
    maximum_gap = Decimal(0)
    for interval_start, interval_end in clipped:
        maximum_gap = max(maximum_gap, _duration_seconds(cursor, interval_start))
        effective_start = max(cursor, interval_start)
        if interval_end > effective_start:
            covered_seconds += _duration_seconds(effective_start, interval_end)
            cursor = interval_end
    maximum_gap = max(maximum_gap, _duration_seconds(cursor, ended_at))
    total_seconds = _duration_seconds(started_at, ended_at)
    return covered_seconds / total_seconds, maximum_gap


def _observation_coverage_metrics(
    observed_at_values: Sequence[datetime],
    started_at: datetime,
    ended_at: datetime,
) -> tuple[Decimal, Decimal]:
    """Measure signed point-observation span and maximum boundary gap."""

    observed = sorted(
        timestamp
        for timestamp in observed_at_values
        if started_at <= timestamp < ended_at
    )
    if not observed:
        return Decimal(0), _duration_seconds(started_at, ended_at)
    boundaries = (started_at, *observed, ended_at)
    maximum_gap = max(
        _duration_seconds(left, right)
        for left, right in zip(boundaries, boundaries[1:])
    )
    covered_seconds = _duration_seconds(observed[0], observed[-1])
    total_seconds = _duration_seconds(started_at, ended_at)
    return covered_seconds / total_seconds, maximum_gap


def _maximum_concurrency(
    intervals: Sequence[tuple[datetime, datetime]],
    started_at: datetime,
    ended_at: datetime,
) -> int:
    events: list[tuple[datetime, int]] = []
    for interval_start, interval_end in intervals:
        clipped_start = max(interval_start, started_at)
        clipped_end = min(interval_end, ended_at)
        if clipped_start < clipped_end:
            events.extend(((clipped_start, 1), (clipped_end, -1)))
    active = 0
    maximum = 0
    for _event_time, delta in sorted(events, key=lambda item: (item[0], item[1])):
        active += delta
        maximum = max(maximum, active)
    return maximum


def _threshold_concurrency_intervals(
    intervals: Sequence[tuple[datetime, datetime]],
    started_at: datetime,
    ended_at: datetime,
    threshold: int,
) -> tuple[tuple[datetime, datetime], ...]:
    """Return half-open intervals where concurrency meets ``threshold``."""

    event_delta_by_time: dict[datetime, int] = {}
    for interval_start, interval_end in intervals:
        clipped_start = max(interval_start, started_at)
        clipped_end = min(interval_end, ended_at)
        if clipped_start >= clipped_end:
            continue
        event_delta_by_time[clipped_start] = (
            event_delta_by_time.get(clipped_start, 0) + 1
        )
        event_delta_by_time[clipped_end] = (
            event_delta_by_time.get(clipped_end, 0) - 1
        )

    active = 0
    cursor = started_at
    threshold_intervals: list[tuple[datetime, datetime]] = []
    for event_at in sorted(event_delta_by_time):
        if cursor < event_at and active >= threshold:
            if threshold_intervals and threshold_intervals[-1][1] == cursor:
                threshold_intervals[-1] = (threshold_intervals[-1][0], event_at)
            else:
                threshold_intervals.append((cursor, event_at))
        active += event_delta_by_time[event_at]
        cursor = event_at
    if cursor < ended_at and active >= threshold:
        if threshold_intervals and threshold_intervals[-1][1] == cursor:
            threshold_intervals[-1] = (threshold_intervals[-1][0], ended_at)
        else:
            threshold_intervals.append((cursor, ended_at))
    return tuple(threshold_intervals)


def _threshold_coverage_evidence(
    intervals: Sequence[tuple[datetime, datetime]],
    started_at: datetime,
    ended_at: datetime,
    threshold: int,
) -> ThresholdCoverageEvidence:
    threshold_intervals = _threshold_concurrency_intervals(
        intervals, started_at, ended_at, threshold
    )
    coverage_ratio, max_gap_seconds = _interval_coverage_metrics(
        threshold_intervals, started_at, ended_at
    )
    return ThresholdCoverageEvidence(
        intervals=threshold_intervals,
        coverage_ratio=coverage_ratio,
        max_gap_seconds=max_gap_seconds,
    )


def _intersect_intervals(
    left: Sequence[tuple[datetime, datetime]],
    right: Sequence[tuple[datetime, datetime]],
) -> tuple[tuple[datetime, datetime], ...]:
    intersections: list[tuple[datetime, datetime]] = []
    left_index = 0
    right_index = 0
    while left_index < len(left) and right_index < len(right):
        left_start, left_end = left[left_index]
        right_start, right_end = right[right_index]
        intersection_start = max(left_start, right_start)
        intersection_end = min(left_end, right_end)
        if intersection_start < intersection_end:
            intersections.append((intersection_start, intersection_end))
        if left_end <= right_end:
            left_index += 1
        if right_end <= left_end:
            right_index += 1
    return tuple(intersections)


def _http_rate_bucket_evidence(
    observed_at_values: Sequence[datetime],
    started_at: datetime,
    ended_at: datetime,
) -> HttpRateBucketEvidence:
    total_seconds = _duration_seconds(started_at, ended_at)
    bucket_count = int(
        (total_seconds / API_REQUEST_RATE_BUCKET_SECONDS).to_integral_value(
            rounding=ROUND_CEILING
        )
    )
    observations_by_bucket = [0] * bucket_count
    for observed_at in observed_at_values:
        if not started_at <= observed_at < ended_at:
            continue
        bucket_index = int(
            _duration_seconds(started_at, observed_at)
            // API_REQUEST_RATE_BUCKET_SECONDS
        )
        observations_by_bucket[bucket_index] += 1

    bucket_rates: list[Decimal] = []
    underfilled_buckets = 0
    for bucket_index, observations in enumerate(observations_by_bucket):
        bucket_start_offset = API_REQUEST_RATE_BUCKET_SECONDS * bucket_index
        bucket_duration = min(
            API_REQUEST_RATE_BUCKET_SECONDS,
            total_seconds - bucket_start_offset,
        )
        required_observations = int(
            (MIN_API_REQUESTS_PER_SECOND * bucket_duration).to_integral_value(
                rounding=ROUND_CEILING
            )
        )
        bucket_rates.append(Decimal(observations) / bucket_duration)
        underfilled_buckets += observations < required_observations

    return HttpRateBucketEvidence(
        bucket_seconds=API_REQUEST_RATE_BUCKET_SECONDS,
        bucket_count=bucket_count,
        minimum_observations=min(observations_by_bucket),
        minimum_requests_per_second=min(bucket_rates),
        underfilled_buckets=underfilled_buckets,
    )


def _http_samples_within_intervals(
    samples: Sequence[HttpSample],
    intervals: Sequence[tuple[datetime, datetime]],
) -> tuple[HttpSample, ...]:
    return tuple(
        sample
        for sample in samples
        if any(
            interval_start <= sample.server_received_at
            and sample.server_observed_at <= interval_end
            for interval_start, interval_end in intervals
        )
    )


def _fully_contended_http_samples(
    evidence: CapacityEvidence,
    samples: Sequence[HttpSample],
) -> tuple[HttpSample, ...]:
    return _http_samples_within_intervals(
        samples,
        evidence.raw_samples.full_lane_threshold.intervals,
    )


def _fully_contended_class_metrics(
    evidence: CapacityEvidence,
    samples: Sequence[HttpSample],
) -> dict[str, Any]:
    contended_samples = _fully_contended_http_samples(evidence, samples)
    return {
        "samples": len(contended_samples),
        "distinct_query_keys": len(
            {sample.semantic_query_digest for sample in contended_samples}
        ),
        "distinct_processes": len(
            {sample.process_instance_digest for sample in contended_samples}
        ),
        "errors": sum(sample.response_status != 200 for sample in contended_samples),
        "cold_first_page_p95_ms": (
            _nearest_rank_p95([sample.first_page_ms for sample in contended_samples])
            if contended_samples
            else None
        ),
    }


def _validate_http_sample_identity(
    matched: tuple[HttpSample, ...],
    negative: tuple[HttpSample, ...],
    random_samples: tuple[HttpSample, ...],
) -> None:
    query_sets = [
        {sample.semantic_query_digest for sample in class_samples}
        for class_samples in (matched, negative, random_samples)
    ]
    class_pairs = ((0, 1), (0, 2), (1, 2))
    if any(query_sets[left] & query_sets[right] for left, right in class_pairs):
        raise EvidenceError("cross_class_query_overlap", "raw_samples")
    request_ids = [
        sample.challenge_digest for sample in (*matched, *negative, *random_samples)
    ]
    if len(set(request_ids)) != len(request_ids):
        raise EvidenceError("duplicate_sample_id", "raw_samples")
    all_samples = (*matched, *negative, *random_samples)
    if len({sample.run_digest for sample in all_samples}) != 1:
        raise EvidenceError("run_digest_mismatch", "raw_samples")
    process_identities = [
        (sample.process_instance_digest, sample.process_started_at)
        for sample in all_samples
    ]
    if len(set(process_identities)) != len(process_identities):
        raise EvidenceError("reused_api_process", "raw_samples")


def _validate_raw_http_samples(
    raw_object: Mapping[str, Any],
    api: ApiEvidence,
    trust: ReceiptTrust,
    receipt: VerifiedReceipt,
) -> tuple[tuple[HttpSample, ...], tuple[HttpSample, ...], tuple[HttpSample, ...]]:
    matched = _validate_http_class_samples(
        raw_object["http_matched_positive"], "matched_positive", api, trust, receipt
    )
    negative = _validate_http_class_samples(
        raw_object["http_negative"], "negative", api, trust, receipt
    )
    random_samples = _validate_http_class_samples(
        raw_object["http_random"], "random", api, trust, receipt
    )
    _validate_http_sample_identity(matched, negative, random_samples)
    return matched, negative, random_samples


def _validate_raw_sample_groups(
    raw_object: Mapping[str, Any],
    api: ApiEvidence,
    trust: ReceiptTrust,
    receipt: VerifiedReceipt,
) -> _ValidatedRawSampleGroups:
    imports = _validate_import_sample_collection(
        raw_object["import_lifecycle"], api.contention_run_id
    )
    audits = _validate_audit_sample_collection(
        raw_object["audit_results"], imports, api.contention_run_id
    )
    peak_import_events = _validate_peak_import_event_collection(
        raw_object["peak_import_events"]
    )
    peak_audit_events = _validate_peak_audit_event_collection(raw_object["peak_audit_events"])
    matched, negative, random_samples = _validate_raw_http_samples(
        raw_object, api, trust, receipt
    )
    return _ValidatedRawSampleGroups(
        imports=imports,
        audits=audits,
        peak_import_events=peak_import_events,
        peak_audit_events=peak_audit_events,
        matched_positive=matched,
        negative=negative,
        random_samples=random_samples,
    )


def _full_lane_threshold_evidence(
    build_threshold: ThresholdCoverageEvidence,
    audit_threshold: ThresholdCoverageEvidence,
    api: ApiEvidence,
) -> ThresholdCoverageEvidence:
    full_lane_intervals = _intersect_intervals(
        build_threshold.intervals, audit_threshold.intervals
    )
    coverage_ratio, max_gap_seconds = _interval_coverage_metrics(
        full_lane_intervals, api.contention_started_at, api.contention_ended_at
    )
    return ThresholdCoverageEvidence(
        intervals=full_lane_intervals,
        coverage_ratio=coverage_ratio,
        max_gap_seconds=max_gap_seconds,
    )


def _derive_raw_sample_coverage(
    sample_groups: _ValidatedRawSampleGroups,
    api: ApiEvidence,
    required_unique_builds: int,
    required_candidate_audits: int,
) -> _RawSampleCoverage:
    unique_intervals = tuple(
        (sample.build_started_at, sample.build_completed_at)
        for sample in sample_groups.imports
        if sample.kind == "unique_build"
    )
    audit_intervals = tuple(
        (sample.started_at, sample.completed_at) for sample in sample_groups.audits
    )
    build_threshold = _threshold_coverage_evidence(
        unique_intervals,
        api.contention_started_at,
        api.contention_ended_at,
        required_unique_builds,
    )
    audit_threshold = _threshold_coverage_evidence(
        audit_intervals,
        api.contention_started_at,
        api.contention_ended_at,
        required_candidate_audits,
    )
    full_lane_threshold = _full_lane_threshold_evidence(
        build_threshold, audit_threshold, api
    )
    all_http_samples = (
        *sample_groups.matched_positive,
        *sample_groups.negative,
        *sample_groups.random_samples,
    )
    fully_contended_http = _http_samples_within_intervals(
        all_http_samples, full_lane_threshold.intervals
    )
    observation_times = tuple(
        sample.server_received_at for sample in fully_contended_http
    )
    observation_coverage, observation_max_gap = _observation_coverage_metrics(
        observation_times, api.contention_started_at, api.contention_ended_at
    )
    return _RawSampleCoverage(
        build_threshold=build_threshold,
        audit_threshold=audit_threshold,
        full_lane_threshold=full_lane_threshold,
        http_observation_coverage_ratio=observation_coverage,
        http_observation_max_gap_seconds=observation_max_gap,
        http_rate_buckets=_http_rate_bucket_evidence(
            observation_times, api.contention_started_at, api.contention_ended_at
        ),
        concurrent_unique_builds=_maximum_concurrency(
            unique_intervals, api.contention_started_at, api.contention_ended_at
        ),
        concurrent_candidate_audits=_maximum_concurrency(
            audit_intervals, api.contention_started_at, api.contention_ended_at
        ),
    )


def _validate_raw_samples(
    root: Mapping[str, Any],
    api: ApiEvidence,
    trust: ReceiptTrust,
    receipt: VerifiedReceipt,
    required_unique_builds: int,
    required_candidate_audits: int,
) -> RawSamplesEvidence:
    """Validate the raw release measurements used to recompute aggregates."""

    raw_object = _object(
        root["raw_samples"],
        "raw_samples",
        (
            "import_lifecycle",
            "audit_results",
            "peak_import_events",
            "peak_audit_events",
            "http_matched_positive",
            "http_negative",
            "http_random",
        ),
    )
    sample_groups = _validate_raw_sample_groups(raw_object, api, trust, receipt)
    coverage = _derive_raw_sample_coverage(
        sample_groups,
        api,
        required_unique_builds,
        required_candidate_audits,
    )
    return RawSamplesEvidence(
        imports=sample_groups.imports,
        audits=sample_groups.audits,
        peak_import_events=sample_groups.peak_import_events,
        peak_audit_events=sample_groups.peak_audit_events,
        matched_positive=sample_groups.matched_positive,
        negative=sample_groups.negative,
        random=sample_groups.random_samples,
        build_threshold=coverage.build_threshold,
        audit_threshold=coverage.audit_threshold,
        full_lane_threshold=coverage.full_lane_threshold,
        http_observation_coverage_ratio=coverage.http_observation_coverage_ratio,
        http_observation_max_gap_seconds=(
            coverage.http_observation_max_gap_seconds
        ),
        http_rate_buckets=coverage.http_rate_buckets,
        concurrent_unique_builds=coverage.concurrent_unique_builds,
        concurrent_candidate_audits=coverage.concurrent_candidate_audits,
    )


def _validate_resource_observation(
    root: Mapping[str, Any],
) -> ResourceObservationEvidence:
    path = "resource_observation"
    observation_object = _object(root[path], path, _RESOURCE_OBSERVATION_FIELDS)
    contention_started_at = _timestamp(
        observation_object, "contention_started_at", path
    )
    contention_ended_at = _timestamp(observation_object, "contention_ended_at", path)
    storage_measured_at = _timestamp(observation_object, "storage_measured_at", path)
    gc_started_at = _timestamp(observation_object, "gc_started_at", path)
    gc_ended_at = _timestamp(observation_object, "gc_ended_at", path)
    if not contention_started_at < contention_ended_at:
        raise EvidenceError("invalid_window", f"{path}.contention_ended_at")
    if not contention_started_at <= storage_measured_at <= contention_ended_at:
        raise EvidenceError("sample_outside_contention", f"{path}.storage_measured_at")
    if not gc_started_at < gc_ended_at <= contention_started_at:
        raise EvidenceError("invalid_window", f"{path}.gc_ended_at")
    return ResourceObservationEvidence(
        contention_run_id=_digest(observation_object, "contention_run_id", path),
        contention_started_at=contention_started_at,
        contention_ended_at=contention_ended_at,
        storage_measured_at=storage_measured_at,
        gc_started_at=gc_started_at,
        gc_ended_at=gc_ended_at,
    )


def _validate_resource_telemetry(
    root: Mapping[str, Any],
) -> tuple[RawResourceTelemetry, RawResourceSummaries]:
    try:
        parsed = parse_and_derive_resource_telemetry(root["resource_telemetry"])
        telemetry = parsed.telemetry
        summaries = parsed.summaries
    except ResourceTelemetryError as exc:
        field = "resource_telemetry"
        if exc.field not in ("", "root"):
            field = f"{field}.{exc.field}"
        raise EvidenceError(exc.code, field)
    return telemetry, summaries


def _reconcile_attributes(
    aggregate: Any,
    derived: Any,
    path: str,
    names: Sequence[str],
) -> None:
    for name in names:
        _require_equal(
            getattr(aggregate, name),
            getattr(derived, name),
            f"{path}.{name}",
        )


def _reconcile_resource_observation(
    evidence: CapacityEvidence,
    telemetry: RawResourceTelemetry,
) -> None:
    observation = evidence.resource_observation
    _require_equal(
        observation.contention_run_id,
        telemetry.contention_run_id,
        "resource_observation.contention_run_id",
    )
    _require_equal(
        observation.contention_started_at,
        telemetry.contention_started_at,
        "resource_observation.contention_started_at",
    )
    _require_equal(
        observation.contention_ended_at,
        telemetry.contention_ended_at,
        "resource_observation.contention_ended_at",
    )
    _require_equal(
        observation.storage_measured_at,
        telemetry.storage_endpoint.measured_at,
        "resource_observation.storage_measured_at",
    )
    _require_equal(
        observation.gc_started_at,
        telemetry.gc_cycles[0].started_at,
        "resource_observation.gc_started_at",
    )
    _require_equal(
        observation.gc_ended_at,
        telemetry.gc_cycles[-1].ended_at,
        "resource_observation.gc_ended_at",
    )


def _reconcile_postgresql_telemetry(
    evidence: CapacityEvidence,
    derived: RawResourceSummaries,
) -> None:
    _require_equal(
        evidence.postgresql.load.sample_seconds,
        derived.sample_seconds,
        "postgresql.load.sample_seconds",
    )
    _reconcile_attributes(
        evidence.postgresql.connections,
        derived.postgresql.connections,
        "postgresql.connections",
        (
            "max_connections", "reserved_connections", "peak_api_connections",
            "peak_import_connections", "peak_other_connections",
            "required_headroom_connections",
        ),
    )
    rate_fields = (
        "import_bytes_per_second", "api_bytes_per_second", "other_bytes_per_second",
        "sustainable_bytes_per_second",
    )
    _reconcile_attributes(evidence.postgresql.write, derived.postgresql.write, "postgresql.write", rate_fields)
    _reconcile_attributes(evidence.postgresql.wal, derived.postgresql.wal, "postgresql.wal", rate_fields)
    _reconcile_attributes(
        evidence.postgresql.pool_wait, derived.postgresql.pool_wait, "postgresql.pool_wait",
        ("observations", "waited_acquisitions", "p95_ms", "max_ms", "timeout_errors"),
    )
    _reconcile_attributes(
        evidence.postgresql.checkpoint, derived.postgresql.checkpoint, "postgresql.checkpoint",
        ("sample_seconds", "completed", "requested", "write_seconds", "sync_seconds"),
    )
    _reconcile_attributes(
        evidence.postgresql.temp, derived.postgresql.temp, "postgresql.temp",
        ("capacity_bytes", "baseline_used_bytes", "reserve_bytes", "measured_peak_incremental_bytes"),
    )
    _reconcile_attributes(
        evidence.postgresql.autovacuum, derived.postgresql.autovacuum, "postgresql.autovacuum",
        (
            "sample_seconds", "max_workers", "peak_workers", "required_headroom_workers",
            "completed_cycles", "cancelled_cycles", "oldest_pending_minutes",
        ),
    )


def _reconcile_storage_and_gc_telemetry(
    evidence: CapacityEvidence,
    derived: RawResourceSummaries,
) -> None:
    _reconcile_attributes(
        evidence.storage, derived.storage, "storage",
        (
            "physical_unique_builds_observed", "physical_net_new_bytes_observed",
            "physical_max_bytes_per_unique_build", "logical_imports_observed",
            "logical_net_new_bytes_observed", "logical_max_bytes_per_import",
            "current_used_bytes", "capacity_bytes", "reserve_bytes",
            "physical_retention_months", "logical_retention_months",
        ),
    )
    _reconcile_attributes(
        evidence.gc, derived.gc, "gc",
        (
            "window_hours", "cycles_observed", "executed_cycles", "overlap_count",
            "reference_recheck_failures", "starting_backlog_bytes", "starting_backlog_layouts",
            "newly_eligible_bytes", "deleted_bytes", "ending_backlog_bytes",
            "ending_backlog_layouts", "max_backlog_bytes", "max_clearance_hours",
            "eligible_layouts", "deleted_layouts",
        ),
    )


def _reconcile_resource_telemetry(evidence: CapacityEvidence) -> None:
    """Require parsed resource telemetry to match every gate aggregate."""

    telemetry = evidence.resource_telemetry
    derived = evidence.resource_summaries
    _reconcile_resource_observation(evidence, telemetry)
    observed_kind_by_import_id = {
        storage_delta.import_id_sha256: storage_delta.kind
        for storage_delta in telemetry.storage_deltas
    }
    expected_kind_by_import_id = {
        import_sample.import_id_sha256: import_sample.kind
        for import_sample in evidence.raw_samples.imports
    }
    _require_equal(
        observed_kind_by_import_id,
        expected_kind_by_import_id,
        "resource_telemetry.storage_deltas",
    )

    _reconcile_attributes(
        evidence.scratch,
        derived.scratch,
        "scratch",
        (
            "capacity_bytes",
            "baseline_used_bytes",
            "reserve_bytes",
            "measured_peak_incremental_bytes",
            "cleanup_cycles",
            "cleanup_failures",
        ),
    )
    _reconcile_postgresql_telemetry(evidence, derived)
    _reconcile_storage_and_gc_telemetry(evidence, derived)


def _reconcile_end_to_end(evidence: CapacityEvidence) -> None:
    imports = evidence.raw_samples.imports
    durations = [
        _duration_minutes(sample.enqueued_at, sample.activated_at) for sample in imports
    ]
    expected_by_field = {
        "sample_count": len(imports),
        "unique_build_samples": sum(
            sample.kind == "unique_build" for sample in imports
        ),
        "reuse_samples": sum(sample.kind == "reuse" for sample in imports),
        "complete_stage_samples": len(imports),
        "within_15_minutes": sum(
            duration <= MAX_UNIQUE_BUILD_MINUTES for duration in durations
        ),
        "p95_minutes": _nearest_rank_p95(durations),
        "max_minutes": max(durations),
        "errors": sum(sample.status != "succeeded" for sample in imports),
    }
    for field_name, expected_value in expected_by_field.items():
        _require_equal(
            getattr(evidence.end_to_end, field_name),
            expected_value,
            f"end_to_end.{field_name}",
        )


def _reconcile_unique_builds(evidence: CapacityEvidence) -> None:
    samples = tuple(
        sample
        for sample in evidence.raw_samples.imports
        if sample.kind == "unique_build"
    )
    durations = [
        _duration_minutes(sample.build_started_at, sample.build_completed_at)
        for sample in samples
    ]
    expected_by_field = {
        "sample_count": len(samples),
        "p95_minutes": _nearest_rank_p95(durations),
        "max_minutes": max(durations),
        "fresh_fingerprint_builds": sum(sample.fresh_fingerprint for sample in samples),
        "complete_source_coverage_builds": sum(
            sample.complete_source_coverage for sample in samples
        ),
        "durable_publication_builds": sum(
            sample.durable_publication for sample in samples
        ),
        "persisted_audit_builds": sum(sample.persisted_audit for sample in samples),
        "release_scanner_builds": sum(sample.release_scanner for sample in samples),
        "representative_large_builds": sum(
            sample.representative_large_build for sample in samples
        ),
    }
    for field_name, expected_value in expected_by_field.items():
        _require_equal(
            getattr(evidence.unique_build, field_name),
            expected_value,
            f"unique_build.{field_name}",
        )


def _reconcile_reuse(evidence: CapacityEvidence) -> None:
    imports = evidence.raw_samples.imports
    reuse_samples = tuple(sample for sample in imports if sample.kind == "reuse")
    durations = [
        _duration_minutes(sample.enqueued_at, sample.activated_at)
        for sample in reuse_samples
    ]
    expected_by_field = {
        "observed_logical_imports": len(imports),
        "observed_unique_builds": len(imports) - len(reuse_samples),
        "observed_reuse_hits": len(reuse_samples),
        "complete_fingerprint_verified_hits": sum(
            sample.complete_fingerprint_verified for sample in reuse_samples
        ),
        "logical_publication_verified_hits": sum(
            sample.logical_publication_verified for sample in reuse_samples
        ),
        "production_like_observed_hits": sum(
            sample.production_like_observed for sample in reuse_samples
        ),
        "audited_activation_verified_hits": sum(
            sample.audited_activation_verified for sample in reuse_samples
        ),
        "reuse_only_sample_count": len(reuse_samples),
        "reuse_only_p95_minutes": _nearest_rank_p95(durations),
        "reuse_only_max_minutes": max(durations),
    }
    for field_name, expected_value in expected_by_field.items():
        _require_equal(
            getattr(evidence.reuse, field_name),
            expected_value,
            f"reuse.{field_name}",
        )


def _reconcile_retry(evidence: CapacityEvidence) -> None:
    imports = evidence.raw_samples.imports
    unique_samples = tuple(
        sample for sample in imports if sample.kind == "unique_build"
    )
    expected_by_field = {
        "successful_unique_builds": sum(
            sample.status == "succeeded" for sample in unique_samples
        ),
        "failed_attempts": sum(sample.failed_attempts for sample in imports),
        "failed_attempt_worker_minutes": sum(
            (sample.failed_attempt_worker_seconds for sample in imports),
            Decimal(0),
        )
        / Decimal(60),
    }
    for field_name, expected_value in expected_by_field.items():
        _require_equal(
            getattr(evidence.retry, field_name),
            expected_value,
            f"retry.{field_name}",
        )


def _audit_duration_sets(
    evidence: CapacityEvidence,
) -> tuple[list[Decimal], list[Decimal], list[Decimal]]:
    import_by_id = {
        sample.import_id_sha256: sample for sample in evidence.raw_samples.imports
    }
    audit_durations = [
        _duration_minutes(sample.started_at, sample.completed_at)
        for sample in evidence.raw_samples.audits
    ]
    queue_durations = [
        _duration_minutes(sample.queued_at, sample.started_at)
        for sample in evidence.raw_samples.audits
    ]
    activation_durations = [
        _duration_minutes(
            sample.candidate_attested_at,
            import_by_id[sample.import_id_sha256].activated_at,
        )
        for sample in evidence.raw_samples.audits
    ]
    return audit_durations, queue_durations, activation_durations


def _reconcile_candidate_audits(evidence: CapacityEvidence) -> None:
    audits = evidence.raw_samples.audits
    duration_sets = _audit_duration_sets(evidence)
    contention_seconds = evidence.api.contention_seconds
    expected_by_field = {
        "sample_count": len(audits),
        "successful_audits": sum(sample.status == "passed" for sample in audits),
        "duration_p95_minutes": _nearest_rank_p95(duration_sets[0]),
        "duration_max_minutes": max(duration_sets[0]),
        "queue_age_p95_minutes": _nearest_rank_p95(duration_sets[1]),
        "queue_age_max_minutes": max(duration_sets[1]),
        "activation_p95_minutes": _nearest_rank_p95(duration_sets[2]),
        "activation_max_minutes": max(duration_sets[2]),
        "http_requests_per_activation": min(sample.http_requests for sample in audits),
        "activations_with_http_floor": sum(
            sample.http_requests >= MIN_HTTP_REQUESTS_PER_ACTIVATION
            for sample in audits
        ),
        "observed_http_requests": sum(sample.http_requests for sample in audits),
        "observed_request_seconds": contention_seconds,
        "errors": sum(
            sample.status != "passed" or sample.http_errors > 0 for sample in audits
        ),
    }
    for field_name, expected_value in expected_by_field.items():
        _require_equal(
            getattr(evidence.candidate_audit, field_name),
            expected_value,
            f"candidate_audit.{field_name}",
        )


def _peak_event_window_index(
    windows: tuple[PeakWindowEvidence, ...],
    observed_at: datetime,
    field: str,
) -> int:
    matching_indexes = [
        index
        for index, window in enumerate(windows)
        if window.started_at <= observed_at < window.ended_at
    ]
    if len(matching_indexes) != 1:
        raise EvidenceError("peak_event_window_mismatch", field)
    return matching_indexes[0]


def _reconcile_peak_events(evidence: CapacityEvidence) -> None:
    windows = evidence.peak_arrival.windows
    import_events = evidence.raw_samples.peak_import_events
    audit_events = evidence.raw_samples.peak_audit_events
    import_buckets: list[list[PeakImportEvent]] = [[] for _window in windows]
    audit_buckets: list[list[PeakAuditEvent]] = [[] for _window in windows]
    for event in import_events:
        window_index = _peak_event_window_index(
            windows, event.enqueued_at, "raw_samples.peak_import_events"
        )
        import_buckets[window_index].append(event)
    for event in audit_events:
        window_index = _peak_event_window_index(
            windows, event.queued_at, "raw_samples.peak_audit_events"
        )
        audit_buckets[window_index].append(event)

    import_ids = {event.import_id_sha256 for event in import_events}
    audited_import_ids = {event.import_id_sha256 for event in audit_events}
    if import_ids != audited_import_ids:
        raise EvidenceError(
            "import_audit_join_mismatch", "raw_samples.peak_audit_events"
        )
    if len(import_events) != sum(window.logical_imports for window in windows):
        raise EvidenceError(
            "raw_aggregate_mismatch", "peak_arrival.windows.logical_imports"
        )
    if len(audit_events) != sum(window.candidate_audits for window in windows):
        raise EvidenceError(
            "raw_aggregate_mismatch", "peak_arrival.windows.candidate_audits"
        )
    for index, (window, import_bucket, audit_bucket) in enumerate(
        zip(windows, import_buckets, audit_buckets)
    ):
        import_queue_delays = [
            _duration_minutes(event.enqueued_at, event.build_started_at)
            for event in import_bucket
        ]
        audit_queue_delays = [
            _duration_minutes(event.queued_at, event.started_at)
            for event in audit_bucket
        ]
        expected_by_field = {
            "logical_imports": len(import_bucket),
            "unique_builds": sum(
                event.kind == "unique_build" for event in import_bucket
            ),
            "candidate_audits": len(audit_bucket),
            "max_queue_delay_minutes": max(import_queue_delays, default=Decimal(0)),
            "max_audit_queue_age_minutes": max(audit_queue_delays, default=Decimal(0)),
        }
        for field_name, expected_value in expected_by_field.items():
            _require_equal(
                getattr(window, field_name),
                expected_value,
                f"peak_arrival.windows.{index}.{field_name}",
            )


def _reconcile_api_class(
    summary: ApiClassEvidence,
    samples: tuple[HttpSample, ...],
    path: str,
) -> None:
    received_times = [sample.server_received_at for sample in samples]
    completed_times = [sample.server_observed_at for sample in samples]
    expected_by_field = {
        "samples": len(samples),
        "distinct_query_keys": len(
            {sample.semantic_query_digest for sample in samples}
        ),
        "contention_samples": len(samples),
        "contention_distinct_query_keys": len(
            {sample.semantic_query_digest for sample in samples}
        ),
        "errors": sum(sample.response_status != 200 for sample in samples),
        "cold_first_page_p95_ms": _nearest_rank_p95(
            [sample.first_page_ms for sample in samples]
        ),
        "sample_started_at": min(received_times),
        "sample_ended_at": max(completed_times),
    }
    for field_name, expected_value in expected_by_field.items():
        _require_equal(
            getattr(summary, field_name), expected_value, f"{path}.{field_name}"
        )


def _reconcile_api(evidence: CapacityEvidence) -> None:
    raw = evidence.raw_samples
    api = evidence.api
    http_samples = (*raw.matched_positive, *raw.negative, *raw.random)
    observed_successes = sum(sample.response_status == 200 for sample in http_samples)
    observed_failures = len(http_samples) - observed_successes
    audit_requests = sum(sample.http_requests for sample in raw.audits)
    expected_by_field = {
        "concurrent_unique_builds": raw.concurrent_unique_builds,
        "concurrent_candidate_audits": raw.concurrent_candidate_audits,
        "requests": len(http_samples),
        "requests_while_imports_running": len(http_samples),
        "candidate_audit_requests": audit_requests,
        "candidate_audit_requests_while_imports_running": audit_requests,
        "fresh_processes": all(
            sample.cold and sample.first_observation for sample in http_samples
        ),
    }
    for field_name, expected_value in expected_by_field.items():
        _require_equal(getattr(api, field_name), expected_value, f"api.{field_name}")
    if observed_successes > api.succeeded_requests:
        raise EvidenceError("raw_aggregate_mismatch", "api.succeeded_requests")
    if observed_failures > api.failed_requests:
        raise EvidenceError("raw_aggregate_mismatch", "api.failed_requests")
    omitted_requests = api.attempted_requests - len(http_samples)
    omitted_from_outcomes = (
        api.succeeded_requests
        - observed_successes
        + api.failed_requests
        - observed_failures
    )
    _require_equal(
        omitted_requests,
        omitted_from_outcomes,
        "api.attempted_requests",
    )
    _reconcile_api_class(
        api.matched_positive, raw.matched_positive, "api.matched_positive"
    )
    _reconcile_api_class(api.negative, raw.negative, "api.negative")
    _reconcile_api_class(api.random, raw.random, "api.random")


def _reconcile_contention(evidence: CapacityEvidence) -> None:
    api = evidence.api
    postgres_load = evidence.postgresql.load
    _require_equal(
        postgres_load.contention_run_id, api.contention_run_id, "postgresql.load"
    )
    _require_equal(
        postgres_load.started_at,
        api.contention_started_at,
        "postgresql.load.started_at",
    )
    _require_equal(
        postgres_load.ended_at, api.contention_ended_at, "postgresql.load.ended_at"
    )
    _require_equal(
        postgres_load.concurrent_unique_builds,
        evidence.raw_samples.concurrent_unique_builds,
        "postgresql.load.concurrent_unique_builds",
    )
    _require_equal(
        postgres_load.concurrent_candidate_audits,
        evidence.raw_samples.concurrent_candidate_audits,
        "postgresql.load.concurrent_candidate_audits",
    )
    _require_equal(
        postgres_load.api_requests_per_second,
        Decimal(api.requests) / api.contention_seconds,
        "postgresql.load.api_requests_per_second",
    )
    _require_equal(
        postgres_load.candidate_audit_requests_per_second,
        Decimal(api.candidate_audit_requests) / api.contention_seconds,
        "postgresql.load.candidate_audit_requests_per_second",
    )
    _require_equal(
        evidence.scratch.measured_concurrent_unique_builds,
        evidence.raw_samples.concurrent_unique_builds,
        "scratch.measured_concurrent_unique_builds",
    )


def _reconcile_raw_aggregates(evidence: CapacityEvidence) -> None:
    _reconcile_end_to_end(evidence)
    _reconcile_unique_builds(evidence)
    _reconcile_reuse(evidence)
    _reconcile_retry(evidence)
    _reconcile_candidate_audits(evidence)
    _reconcile_peak_events(evidence)
    _reconcile_api(evidence)
    _reconcile_contention(evidence)
    _reconcile_resource_telemetry(evidence)


def _measurement_target(root: Mapping[str, Any], target_override: int | None) -> int:
    objective_dict = _object(
        root["objective"],
        "objective",
        ("target_logical_imports_per_month",),
    )
    objective_target = _integer(
        objective_dict,
        "target_logical_imports_per_month",
        "objective",
        minimum=1,
    )
    if target_override is not None:
        if isinstance(target_override, bool) or not isinstance(target_override, int):
            raise EvidenceError("invalid_type", "target_override")
        if target_override < 1:
            raise EvidenceError("invalid_value", "target_override")
        if target_override != objective_target:
            raise EvidenceError("target_override_mismatch", "target_override")
    return objective_target


def _validate_cross_section_consistency(evidence: CapacityEvidence) -> None:
    if (
        evidence.end_to_end.unique_build_samples != evidence.unique_build.sample_count
        or evidence.end_to_end.reuse_samples != evidence.reuse.reuse_only_sample_count
    ):
        raise EvidenceError("inconsistent_evidence", "end_to_end.sample_count")
    if evidence.candidate_audit.sample_count != evidence.end_to_end.sample_count:
        raise EvidenceError("inconsistent_evidence", "candidate_audit.sample_count")
    if evidence.scratch.configured_concurrent_unique_builds > evidence.lanes.count:
        raise EvidenceError(
            "inconsistent_evidence", "scratch.configured_concurrent_unique_builds"
        )
    observed_window_minutes = (
        Decimal(evidence.peak_arrival.sample_windows)
        * evidence.peak_arrival.window_minutes
    )
    if observed_window_minutes > evidence.peak_arrival.observation_minutes:
        raise EvidenceError("inconsistent_evidence", "peak_arrival.observation_minutes")

    resource = evidence.resource_observation
    api = evidence.api
    _require_equal(
        resource.contention_run_id,
        api.contention_run_id,
        "resource_observation.contention_run_id",
    )
    _require_equal(
        resource.contention_started_at,
        api.contention_started_at,
        "resource_observation.contention_started_at",
    )
    _require_equal(
        resource.contention_ended_at,
        api.contention_ended_at,
        "resource_observation.contention_ended_at",
    )
    _require_equal(
        resource.storage_measured_at,
        api.contention_ended_at,
        "resource_observation.storage_measured_at",
    )
    _require_equal(
        resource.gc_ended_at,
        api.contention_started_at,
        "resource_observation.gc_ended_at",
    )
    _require_equal(
        _duration_minutes(resource.gc_started_at, resource.gc_ended_at),
        evidence.gc.window_hours * Decimal(60),
        "resource_observation.gc_started_at",
    )


def validate_measurement(
    measurement_record: Any,
    *,
    trust: ReceiptTrust,
    receipt: VerifiedReceipt,
    target_override: int | None = None,
) -> CapacityEvidence:
    """Validate and normalize one aggregate measurement record."""

    root = _object(measurement_record, "", _ROOT_FIELDS)
    if _integer(root, "schema_version", "", minimum=1) != SCHEMA_VERSION:
        raise EvidenceError("unsupported_schema_version", "schema_version")
    _fixed_string(root, "evidence_profile", "", EVIDENCE_PROFILE)
    if not _boolean(root, "release_evidence", ""):
        raise EvidenceError("non_release_example", "release_evidence")

    api_evidence = _validate_api(root)
    candidate_audit_evidence = _validate_candidate_audit(root)
    scratch_evidence = _validate_scratch(root)
    raw_samples = _validate_raw_samples(
        root,
        api_evidence,
        trust,
        receipt,
        scratch_evidence.configured_concurrent_unique_builds,
        candidate_audit_evidence.lane_count,
    )
    resource_telemetry, resource_summaries = _validate_resource_telemetry(root)
    evidence = CapacityEvidence(
        target_logical_imports_per_month=_measurement_target(root, target_override),
        raw_samples=raw_samples,
        resource_telemetry=resource_telemetry,
        resource_summaries=resource_summaries,
        resource_observation=_validate_resource_observation(root),
        end_to_end=_validate_end_to_end(root),
        unique_build=_validate_unique_build(root),
        reuse=_validate_reuse(root),
        retry=_validate_retry(root),
        lanes=_validate_lanes(root),
        candidate_audit=candidate_audit_evidence,
        peak_arrival=_validate_peak(root),
        scratch=scratch_evidence,
        postgresql=_validate_postgresql(root),
        storage=_validate_storage(root),
        gc=_validate_gc(root),
        api=api_evidence,
    )
    _reconcile_raw_aggregates(evidence)
    _validate_cross_section_consistency(evidence)
    return evidence


def _validate_one_commitment(
    commitment: CommitmentEvidence,
    name: str,
    raw_values: Any,
    sample_count: int,
) -> None:
    path = f"receipt.commitments.{name}"
    if commitment.sample_count != sample_count:
        raise EvidenceError("commitment_count_mismatch", f"{path}.sample_count")
    expected_digest = sample_set_sha256(raw_values)
    if not secrets.compare_digest(commitment.sha256, expected_digest):
        raise EvidenceError("commitment_digest_mismatch", f"{path}.sha256")


def _validate_http_commitment_metadata(
    commitment: CommitmentEvidence,
    api_class: ApiClassEvidence,
    name: str,
) -> None:
    if (
        commitment.distinct_query_keys != api_class.distinct_query_keys
        or commitment.contention_samples != api_class.contention_samples
    ):
        raise EvidenceError(
            "commitment_count_mismatch",
            f"receipt.commitments.{name}.distinct_query_keys",
        )


def _raw_observation_bounds(
    evidence: CapacityEvidence,
) -> tuple[datetime, datetime]:
    timestamps: list[datetime] = []
    for sample in evidence.raw_samples.imports:
        timestamps.extend((sample.enqueued_at, sample.activated_at))
    for sample in evidence.raw_samples.audits:
        timestamps.extend((sample.queued_at, sample.candidate_attested_at))
    for event in evidence.raw_samples.peak_import_events:
        timestamps.extend((event.enqueued_at, event.build_started_at))
    for event in evidence.raw_samples.peak_audit_events:
        timestamps.extend((event.queued_at, event.started_at))
    for samples in (
        evidence.raw_samples.matched_positive,
        evidence.raw_samples.negative,
        evidence.raw_samples.random,
    ):
        for sample in samples:
            timestamps.extend((sample.server_observed_at, sample.collector_received_at))
    resource = evidence.resource_observation
    timestamps.extend(
        (
            resource.contention_started_at,
            resource.contention_ended_at,
            resource.storage_measured_at,
            resource.gc_started_at,
            resource.gc_ended_at,
        )
    )
    return min(timestamps), max(timestamps)


def _validate_observation_bindings(
    evidence: CapacityEvidence,
    receipt: VerifiedReceipt,
) -> None:
    peak_first = evidence.peak_arrival.windows[0].started_at
    peak_last = evidence.peak_arrival.windows[-1].ended_at
    raw_first, raw_last = _raw_observation_bounds(evidence)
    api = evidence.api
    if receipt.contention_run_id != api.contention_run_id:
        raise EvidenceError("contention_run_mismatch", "receipt.contention_run_id")
    if peak_last != api.contention_started_at:
        raise EvidenceError("peak_contention_gap", "peak_arrival.windows")
    if not (
        receipt.observation_started_at <= min(peak_first, raw_first)
        and max(raw_last, api.contention_ended_at) <= receipt.observation_ended_at
    ):
        raise EvidenceError(
            "sample_outside_observation", "receipt.observation_started_at"
        )
    contention_age = _duration_seconds(api.contention_ended_at, receipt.issued_at)
    if contention_age < 0 or contention_age > Decimal(
        MAX_CONTENTION_RESULT_AGE.total_seconds()
    ):
        raise EvidenceError("stale_contention", "api.contention_ended_at")


def _http_commitment_sources(
    raw_object: Mapping[str, Any], api: ApiEvidence
) -> tuple[tuple[str, Any, int, ApiClassEvidence], ...]:
    return (
        ("cold_matched_positive_samples", raw_object["http_matched_positive"], api.matched_positive.samples, api.matched_positive),
        ("cold_negative_samples", raw_object["http_negative"], api.negative.samples, api.negative),
        ("cold_random_samples", raw_object["http_random"], api.random.samples, api.random),
    )


def _commitment_sources(
    measurement_record: Mapping[str, Any],
    evidence: CapacityEvidence,
) -> tuple[tuple[str, Any, int, ApiClassEvidence | None], ...]:
    """Return raw measurement sources paired with their validated aggregates."""

    raw_object = measurement_record["raw_samples"]
    return (
        (
            "import_lifecycle_samples",
            raw_object["import_lifecycle"],
            evidence.end_to_end.sample_count,
            None,
        ),
        (
            "audit_result_samples",
            raw_object["audit_results"],
            evidence.candidate_audit.sample_count,
            None,
        ),
        (
            "peak_import_events",
            raw_object["peak_import_events"],
            len(evidence.raw_samples.peak_import_events),
            None,
        ),
        (
            "peak_audit_events",
            raw_object["peak_audit_events"],
            len(evidence.raw_samples.peak_audit_events),
            None,
        ),
        (
            "peak_windows",
            measurement_record["peak_arrival"]["windows"],
            evidence.peak_arrival.sample_windows,
            None,
        ),
        (
            "resource_telemetry",
            measurement_record["resource_telemetry"],
            len(evidence.resource_telemetry.intervals),
            None,
        ),
        *_http_commitment_sources(raw_object, evidence.api),
    )


def _validate_commitment_bindings(
    measurement_record: Mapping[str, Any],
    evidence: CapacityEvidence,
    receipt: VerifiedReceipt,
) -> None:
    commitment_by_name = receipt.commitments
    commitment_sources = _commitment_sources(measurement_record, evidence)
    for name, raw_values, sample_count, api_class in commitment_sources:
        commitment = commitment_by_name[name]
        _validate_one_commitment(commitment, name, raw_values, sample_count)
        if api_class is not None:
            _validate_http_commitment_metadata(commitment, api_class, name)
    commitment_hashes = [
        bound_commitment.sha256 for bound_commitment in commitment_by_name.values()
    ]
    if len(set(commitment_hashes)) != len(commitment_hashes):
        raise EvidenceError("duplicate_commitment", "receipt.commitments")
    _validate_observation_bindings(evidence, receipt)


def _json_number(value: Decimal | int | None) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if value == value.to_integral_value():
        return int(value)
    return float(value.quantize(Decimal("0.000001")))


def _ceil_decimal(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_CEILING))


def _floor_decimal(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_FLOOR))


def _ceil_ratio(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def _safe_fraction(
    numerator: Decimal | int, denominator: Decimal | int
) -> Decimal | None:
    denominator = Decimal(denominator)
    if denominator <= 0:
        return None
    return Decimal(numerator) / denominator


def _rate_metrics(rate: PostgresRateEvidence) -> dict[str, Any]:
    total = (
        rate.import_bytes_per_second
        + rate.api_bytes_per_second
        + rate.other_bytes_per_second
    )
    headroom = rate.sustainable_bytes_per_second - total
    return {
        "combined_bytes_per_second": _json_number(total),
        "sustainable_bytes_per_second": _json_number(rate.sustainable_bytes_per_second),
        "headroom_bytes_per_second": _json_number(headroom),
        "headroom_fraction": _json_number(headroom / rate.sustainable_bytes_per_second),
        "utilization_fraction": _json_number(total / rate.sustainable_bytes_per_second),
    }


def _cohort_retry_overhead_minutes(
    samples: Sequence[ImportLifecycleSample],
) -> Decimal:
    if not samples:
        return Decimal(0)
    failed_seconds = sum(
        (sample.failed_attempt_worker_seconds for sample in samples), Decimal(0)
    )
    return failed_seconds / Decimal(60 * len(samples))


def _gate(gate_id: str, passed: bool) -> dict[str, Any]:
    return {"id": gate_id, "passed": bool(passed)}


def evaluate_evidence(
    evidence: CapacityEvidence,
    *,
    receipt: VerifiedReceipt,
) -> dict[str, Any]:
    """Calculate all aggregate metrics and return a deterministic gate report."""

    logical_import_target = evidence.target_logical_imports_per_month
    end_to_end = evidence.end_to_end
    unique = evidence.unique_build
    reuse = evidence.reuse
    retry = evidence.retry
    lanes = evidence.lanes
    audit = evidence.candidate_audit
    scratch = evidence.scratch
    active_lane_count = min(
        lanes.count,
        scratch.configured_concurrent_unique_builds,
    )

    qualified_reuse_hits = reuse.qualified_hits
    reuse_adjusted_unique_builds = _ceil_ratio(
        logical_import_target * (reuse.observed_logical_imports - qualified_reuse_hits),
        reuse.observed_logical_imports,
    )
    reuse_adjusted_reuse_hits = logical_import_target - reuse_adjusted_unique_builds
    unique_import_samples = tuple(
        sample
        for sample in evidence.raw_samples.imports
        if sample.kind == "unique_build"
    )
    reuse_import_samples = tuple(
        sample for sample in evidence.raw_samples.imports if sample.kind == "reuse"
    )
    unique_retry_overhead_minutes = _cohort_retry_overhead_minutes(
        unique_import_samples
    )
    reuse_retry_overhead_minutes = _cohort_retry_overhead_minutes(reuse_import_samples)
    logical_retry_overhead_minutes = retry.failed_attempt_worker_minutes / Decimal(
        end_to_end.sample_count
    )
    retry_adjusted_unique_minutes = unique.max_minutes + unique_retry_overhead_minutes
    retry_adjusted_reuse_minutes = (
        reuse.reuse_only_max_minutes + reuse_retry_overhead_minutes
    )
    worst_worker_hours = (
        Decimal(logical_import_target) * retry_adjusted_unique_minutes / Decimal(60)
    )
    reuse_worker_hours = (
        Decimal(reuse_adjusted_unique_builds) * retry_adjusted_unique_minutes
        + Decimal(reuse_adjusted_reuse_hits) * retry_adjusted_reuse_minutes
    ) / Decimal(60)
    available_lane_hours = (
        Decimal(active_lane_count) * MONTH_HOURS * lanes.availability_factor
    )
    worst_lane_utilization = worst_worker_hours / available_lane_hours
    reuse_lane_utilization = reuse_worker_hours / available_lane_hours

    audit_service_minutes = audit.duration_max_minutes + audit.activation_max_minutes
    audit_worker_hours = (
        Decimal(logical_import_target) * audit_service_minutes / Decimal(60)
    )
    available_audit_lane_hours = (
        Decimal(audit.lane_count) * MONTH_HOURS * audit.availability_factor
    )
    audit_lane_utilization = audit_worker_hours / available_audit_lane_hours
    monthly_audit_http_requests = (
        logical_import_target * audit.http_requests_per_activation
    )
    observed_audit_http_requests_per_second = (
        Decimal(audit.observed_http_requests) / audit.observed_request_seconds
    )
    peak = evidence.peak_arrival
    peak_import_service_minutes = (
        Decimal(active_lane_count)
        * lanes.availability_factor
        * (peak.window_minutes + MAX_IMPORT_QUEUE_DELAY_MINUTES)
    )
    peak_import_demand_minutes = max(
        Decimal(window.unique_builds) * retry_adjusted_unique_minutes
        + Decimal(window.logical_imports - window.unique_builds)
        * retry_adjusted_reuse_minutes
        for window in peak.windows
    )
    peak_audit_service_minutes = (
        Decimal(audit.lane_count)
        * audit.availability_factor
        * (peak.window_minutes + MAX_CANDIDATE_AUDIT_QUEUE_AGE_MINUTES)
    )
    peak_audit_demand_minutes = max(
        Decimal(window.candidate_audits) * audit_service_minutes
        for window in peak.windows
    )
    minimum_representative_peak_arrivals = max(
        2,
        _ceil_decimal(
            Decimal(logical_import_target) * peak.window_minutes / MONTH_MINUTES
        ),
    )

    scratch_usable = (
        scratch.capacity_bytes - scratch.baseline_used_bytes - scratch.reserve_bytes
    )
    scratch_headroom = scratch_usable - scratch.measured_peak_incremental_bytes
    scratch_headroom_fraction = _safe_fraction(
        scratch_headroom,
        scratch_usable,
    )
    scratch_capacity_per_build = (
        _floor_decimal(
            Decimal(scratch_usable) / scratch.configured_concurrent_unique_builds
        )
        if scratch_usable > 0
        else 0
    )

    postgres = evidence.postgresql
    connections = postgres.connections
    usable_connections = connections.max_connections - connections.reserved_connections
    peak_connections = (
        connections.peak_api_connections
        + connections.peak_import_connections
        + connections.peak_other_connections
    )
    connection_headroom = usable_connections - peak_connections
    connection_headroom_fraction = _safe_fraction(
        connection_headroom,
        usable_connections,
    )
    write_metrics = _rate_metrics(postgres.write)
    wal_metrics = _rate_metrics(postgres.wal)
    write_total = (
        postgres.write.import_bytes_per_second
        + postgres.write.api_bytes_per_second
        + postgres.write.other_bytes_per_second
    )
    wal_total = (
        postgres.wal.import_bytes_per_second
        + postgres.wal.api_bytes_per_second
        + postgres.wal.other_bytes_per_second
    )
    write_headroom_fraction = (
        postgres.write.sustainable_bytes_per_second - write_total
    ) / postgres.write.sustainable_bytes_per_second
    wal_headroom_fraction = (
        postgres.wal.sustainable_bytes_per_second - wal_total
    ) / postgres.wal.sustainable_bytes_per_second
    pool_wait = postgres.pool_wait
    checkpoint = postgres.checkpoint
    checkpoint_busy_seconds = checkpoint.write_seconds + checkpoint.sync_seconds
    checkpoint_headroom_fraction = (
        checkpoint.sample_seconds - checkpoint_busy_seconds
    ) / checkpoint.sample_seconds
    temp = postgres.temp
    temp_usable = temp.capacity_bytes - temp.baseline_used_bytes - temp.reserve_bytes
    temp_headroom = temp_usable - temp.measured_peak_incremental_bytes
    temp_headroom_fraction = _safe_fraction(temp_headroom, temp_usable)
    autovacuum = postgres.autovacuum
    autovacuum_headroom_workers = autovacuum.max_workers - autovacuum.peak_workers
    autovacuum_headroom_fraction = _safe_fraction(
        autovacuum_headroom_workers,
        autovacuum.max_workers,
    )
    api = evidence.api
    fully_contended_matched = _fully_contended_class_metrics(
        evidence, evidence.raw_samples.matched_positive
    )
    fully_contended_negative = _fully_contended_class_metrics(
        evidence, evidence.raw_samples.negative
    )
    fully_contended_random = _fully_contended_class_metrics(
        evidence, evidence.raw_samples.random
    )
    observed_api_requests_per_second = Decimal(api.requests) / api.contention_seconds
    observed_candidate_audit_requests_per_second = (
        Decimal(api.candidate_audit_requests) / api.contention_seconds
    )
    required_candidate_audit_requests_per_second = Decimal(
        audit.lane_count * audit.http_requests_per_activation
    ) / (audit.duration_max_minutes * Decimal(60))
    required_pool_wait_observations = _ceil_decimal(
        (
            postgres.load.api_requests_per_second
            + postgres.load.candidate_audit_requests_per_second
        )
        * api.contention_seconds
    )

    storage = evidence.storage
    monthly_physical_worst = (
        logical_import_target * storage.physical_max_bytes_per_unique_build
    )
    monthly_physical_reuse = (
        reuse_adjusted_unique_builds * storage.physical_max_bytes_per_unique_build
    )
    monthly_logical = logical_import_target * storage.logical_max_bytes_per_import
    retained_physical_worst = _ceil_decimal(
        Decimal(monthly_physical_worst) * storage.physical_retention_months
    )
    retained_physical_reuse = _ceil_decimal(
        Decimal(monthly_physical_reuse) * storage.physical_retention_months
    )
    retained_logical = _ceil_decimal(
        Decimal(monthly_logical) * storage.logical_retention_months
    )
    projected_storage_worst = (
        storage.current_used_bytes + retained_physical_worst + retained_logical
    )
    projected_storage_reuse = (
        storage.current_used_bytes + retained_physical_reuse + retained_logical
    )
    storage_headroom_worst = (
        storage.capacity_bytes - storage.reserve_bytes - projected_storage_worst
    )
    storage_headroom_reuse = (
        storage.capacity_bytes - storage.reserve_bytes - projected_storage_reuse
    )

    gc = evidence.gc
    gc_delete_rate = Decimal(gc.deleted_bytes) / gc.window_hours
    gc_eligible_rate = Decimal(gc.newly_eligible_bytes) / gc.window_hours
    gc_net_drain_rate = gc_delete_rate - gc_eligible_rate
    gc_headroom_fraction = _safe_fraction(
        gc.deleted_bytes - gc.newly_eligible_bytes,
        gc.deleted_bytes,
    )
    if gc.ending_backlog_bytes == 0:
        gc_clearance_hours: Decimal | None = Decimal(0)
    elif gc_net_drain_rate > 0:
        gc_clearance_hours = Decimal(gc.ending_backlog_bytes) / gc_net_drain_rate
    else:
        gc_clearance_hours = None
    monthly_gc_growth = _ceil_decimal(
        max(Decimal(0), gc_eligible_rate - gc_delete_rate) * MONTH_HOURS
    )
    projected_gc_backlog = gc.ending_backlog_bytes + monthly_gc_growth

    omitted_api_requests = api.attempted_requests - api.requests
    observed_api_successes = sum(
        sample.response_status == 200
        for sample in (
            *evidence.raw_samples.matched_positive,
            *evidence.raw_samples.negative,
            *evidence.raw_samples.random,
        )
    )
    observed_api_failures = api.requests - observed_api_successes
    api_error_rate = Decimal(api.failed_requests) / api.attempted_requests

    qualifying_unique_builds = min(
        unique.fresh_fingerprint_builds,
        unique.complete_source_coverage_builds,
        unique.durable_publication_builds,
        unique.persisted_audit_builds,
        unique.release_scanner_builds,
        unique.representative_large_builds,
    )
    gates = [
        _gate(
            "logical_import_target",
            logical_import_target >= MIN_RELEASE_TARGET_LOGICAL_IMPORTS_PER_MONTH,
        ),
        _gate(
            "unique_build_sample_floor",
            unique.sample_count >= MIN_QUALIFYING_UNIQUE_BUILDS,
        ),
        _gate(
            "unique_build_qualifying_evidence",
            qualifying_unique_builds == unique.sample_count,
        ),
        _gate(
            "logical_import_end_to_end_sample_floor",
            end_to_end.sample_count
            >= MIN_QUALIFYING_UNIQUE_BUILDS + MIN_REUSE_ONLY_SAMPLES,
        ),
        _gate(
            "logical_import_end_to_end_duration",
            end_to_end.complete_stage_samples == end_to_end.sample_count
            and end_to_end.within_15_minutes == end_to_end.sample_count
            and end_to_end.errors == 0
            and end_to_end.max_minutes <= MAX_UNIQUE_BUILD_MINUTES,
        ),
        _gate(
            "reuse_only_sample_floor",
            reuse.reuse_only_sample_count >= MIN_REUSE_ONLY_SAMPLES,
        ),
        _gate(
            "reuse_complete_fingerprint_evidence",
            qualified_reuse_hits == reuse.observed_reuse_hits,
        ),
        _gate(
            "candidate_audit_sample_floor",
            audit.sample_count >= MIN_CANDIDATE_AUDIT_SAMPLES
            and audit.successful_audits == audit.sample_count,
        ),
        _gate(
            "candidate_audit_queue_slo",
            audit.queue_age_max_minutes <= MAX_CANDIDATE_AUDIT_QUEUE_AGE_MINUTES,
        ),
        _gate(
            "candidate_audit_http_cost",
            audit.http_requests_per_activation >= MIN_HTTP_REQUESTS_PER_ACTIVATION,
        ),
        _gate(
            "candidate_audit_http_observation",
            audit.activations_with_http_floor == audit.successful_audits
            and audit.observed_http_requests
            >= audit.successful_audits * audit.http_requests_per_activation
            and audit.successful_audits == end_to_end.sample_count
            and observed_audit_http_requests_per_second
            >= required_candidate_audit_requests_per_second,
        ),
        _gate(
            "candidate_audit_monthly_workload",
            monthly_audit_http_requests
            >= MIN_RELEASE_TARGET_LOGICAL_IMPORTS_PER_MONTH
            * MIN_HTTP_REQUESTS_PER_ACTIVATION,
        ),
        _gate("candidate_audit_errors", audit.errors == 0),
        _gate(
            "candidate_audit_lane_utilization",
            audit_lane_utilization <= MAX_LANE_UTILIZATION,
        ),
        _gate(
            "worst_case_lane_utilization",
            worst_lane_utilization <= MAX_LANE_UTILIZATION,
        ),
        _gate(
            "reuse_adjusted_lane_utilization",
            reuse_lane_utilization <= MAX_LANE_UTILIZATION,
        ),
        _gate(
            "peak_arrival_evidence",
            peak.sample_windows >= MIN_PEAK_SAMPLE_WINDOWS
            and peak.window_minutes >= MIN_PEAK_WINDOW_MINUTES
            and peak.observation_minutes >= MIN_PEAK_OBSERVATION_MINUTES
            and peak.max_gap_minutes <= MAX_PEAK_GAP_MINUTES
            and peak.coverage_ratio >= MIN_PEAK_COVERAGE_RATIO
            and peak.observed_peak_logical_imports
            >= minimum_representative_peak_arrivals
            and peak.observed_peak_unique_builds >= 1
            and peak.observed_peak_candidate_audits >= 1,
        ),
        _gate(
            "import_queue_delay_slo",
            peak.max_queue_delay_minutes <= MAX_IMPORT_QUEUE_DELAY_MINUTES,
        ),
        _gate(
            "worst_case_peak_arrival",
            peak_import_demand_minutes <= peak_import_service_minutes,
        ),
        _gate(
            "candidate_audit_peak_arrival",
            peak_audit_demand_minutes <= peak_audit_service_minutes
            and peak.max_audit_queue_age_minutes
            <= MAX_CANDIDATE_AUDIT_QUEUE_AGE_MINUTES,
        ),
        _gate(
            "scratch_concurrency_coverage",
            scratch.measured_concurrent_unique_builds
            >= scratch.configured_concurrent_unique_builds,
        ),
        _gate(
            "scratch_capacity",
            scratch_headroom >= 0
            and scratch_headroom_fraction is not None
            and scratch_headroom_fraction >= MIN_RESOURCE_HEADROOM_FRACTION,
        ),
        _gate(
            "scratch_cleanup",
            scratch.cleanup_failures == 0
            and scratch.cleanup_cycles >= unique.sample_count,
        ),
        _gate(
            "postgres_load_coverage",
            postgres.load.concurrent_unique_builds
            >= scratch.configured_concurrent_unique_builds
            and postgres.load.concurrent_candidate_audits >= audit.lane_count
            and postgres.load.sample_seconds >= MIN_CONTENTION_SECONDS
            and postgres.load.sample_seconds == api.contention_seconds
            and postgres.load.api_requests_per_second
            == observed_api_requests_per_second
            and postgres.load.api_requests_per_second >= MIN_API_REQUESTS_PER_SECOND
            and postgres.load.candidate_audit_requests_per_second
            == observed_candidate_audit_requests_per_second
            and observed_candidate_audit_requests_per_second
            >= required_candidate_audit_requests_per_second,
        ),
        _gate(
            "postgres_connection_headroom",
            connection_headroom >= connections.required_headroom_connections
            and connection_headroom_fraction is not None
            and connection_headroom_fraction >= MIN_RESOURCE_HEADROOM_FRACTION,
        ),
        _gate(
            "postgres_write_headroom",
            write_headroom_fraction >= MIN_RESOURCE_HEADROOM_FRACTION,
        ),
        _gate(
            "postgres_wal_headroom",
            wal_headroom_fraction >= MIN_RESOURCE_HEADROOM_FRACTION,
        ),
        _gate(
            "postgres_pool_wait_coverage",
            pool_wait.observations >= MIN_POOL_WAIT_OBSERVATIONS
            and pool_wait.observations >= required_pool_wait_observations,
        ),
        _gate(
            "postgres_pool_wait_slo",
            pool_wait.p95_ms <= MAX_POOL_WAIT_P95_MS
            and pool_wait.max_ms <= MAX_POOL_WAIT_MS
            and pool_wait.timeout_errors == 0,
        ),
        _gate(
            "postgres_checkpoint_coverage",
            checkpoint.sample_seconds >= MIN_CONTENTION_SECONDS
            and checkpoint.sample_seconds >= api.contention_seconds
            and checkpoint.completed >= MIN_CHECKPOINTS
            and checkpoint.requested == 0,
        ),
        _gate(
            "postgres_checkpoint_headroom",
            checkpoint_headroom_fraction >= MIN_RESOURCE_HEADROOM_FRACTION,
        ),
        _gate(
            "postgres_temp_headroom",
            temp_headroom >= 0
            and temp_headroom_fraction is not None
            and temp_headroom_fraction >= MIN_RESOURCE_HEADROOM_FRACTION,
        ),
        _gate(
            "postgres_autovacuum_coverage",
            autovacuum.sample_seconds >= MIN_CONTENTION_SECONDS
            and autovacuum.sample_seconds >= api.contention_seconds
            and autovacuum.completed_cycles >= 1
            and autovacuum.cancelled_cycles == 0
            and autovacuum.oldest_pending_minutes <= MAX_AUTOVACUUM_PENDING_MINUTES,
        ),
        _gate(
            "postgres_autovacuum_headroom",
            autovacuum_headroom_workers >= autovacuum.required_headroom_workers
            and autovacuum_headroom_fraction is not None
            and autovacuum_headroom_fraction >= MIN_RESOURCE_HEADROOM_FRACTION,
        ),
        _gate(
            "storage_measurement_volume",
            storage.physical_unique_builds_observed >= MIN_QUALIFYING_UNIQUE_BUILDS
            and storage.logical_imports_observed >= reuse.observed_logical_imports,
        ),
        _gate("storage_retention_capacity", storage_headroom_worst >= 0),
        _gate(
            "gc_measurement_volume",
            gc.window_hours >= MIN_GC_WINDOW_HOURS
            and gc.cycles_observed >= MIN_GC_CYCLES
            and gc.eligible_layouts >= MIN_GC_LAYOUTS
            and gc.deleted_layouts >= MIN_GC_LAYOUTS
            and gc.newly_eligible_bytes > 0
            and gc.deleted_bytes > 0,
        ),
        _gate(
            "gc_execution",
            gc.executed_cycles == gc.cycles_observed
            and gc.overlap_count == 0
            and gc.reference_recheck_failures == 0,
        ),
        _gate(
            "gc_throughput",
            gc_headroom_fraction is not None
            and gc_headroom_fraction >= MIN_RESOURCE_HEADROOM_FRACTION,
        ),
        _gate(
            "gc_backlog",
            gc.ending_backlog_bytes <= gc.max_backlog_bytes
            and projected_gc_backlog <= gc.max_backlog_bytes
            and gc_clearance_hours is not None
            and gc_clearance_hours <= gc.max_clearance_hours,
        ),
        _gate(
            "api_import_overlap",
            api.requests_while_imports_running == api.requests
            and api.candidate_audit_requests_while_imports_running
            == api.candidate_audit_requests
            and api.concurrent_unique_builds
            >= scratch.configured_concurrent_unique_builds
            and api.concurrent_candidate_audits >= audit.lane_count
            and api.contention_seconds >= MIN_CONTENTION_SECONDS,
        ),
        _gate(
            "api_physical_request_accounting",
            api.planned_requests == api.requests
            and api.attempted_requests == api.requests
            and api.succeeded_requests == api.requests
            and api.failed_requests == 0
            and api.retried_requests == 0
            and omitted_api_requests == 0,
        ),
        _gate(
            "api_continuous_contention_coverage",
            evidence.raw_samples.build_threshold.max_gap_seconds
            <= MAX_CONTENTION_GAP_SECONDS
            and evidence.raw_samples.build_threshold.coverage_ratio
            >= MIN_CONTENTION_COVERAGE_RATIO
            and evidence.raw_samples.audit_threshold.max_gap_seconds
            <= MAX_CONTENTION_GAP_SECONDS
            and evidence.raw_samples.audit_threshold.coverage_ratio
            >= MIN_CONTENTION_COVERAGE_RATIO
            and evidence.raw_samples.full_lane_threshold.max_gap_seconds
            <= MAX_CONTENTION_GAP_SECONDS
            and evidence.raw_samples.full_lane_threshold.coverage_ratio
            >= MIN_CONTENTION_COVERAGE_RATIO
            and evidence.raw_samples.http_observation_max_gap_seconds
            <= MAX_CONTENTION_GAP_SECONDS
            and evidence.raw_samples.http_observation_coverage_ratio
            >= MIN_CONTENTION_COVERAGE_RATIO,
        ),
        _gate(
            "api_measurement_volume",
            api.requests >= MIN_API_REQUESTS
            and observed_api_requests_per_second >= MIN_API_REQUESTS_PER_SECOND,
        ),
        _gate(
            "api_sustained_request_rate",
            evidence.raw_samples.http_rate_buckets.underfilled_buckets == 0
            and evidence.raw_samples.http_rate_buckets.minimum_requests_per_second
            >= MIN_API_REQUESTS_PER_SECOND,
        ),
        _gate(
            "api_cold_sample_coverage",
            fully_contended_matched["samples"] >= MIN_MATCHED_POSITIVE_COLD_SAMPLES
            and fully_contended_negative["samples"] >= MIN_NEGATIVE_COLD_SAMPLES
            and fully_contended_random["samples"] >= MIN_RANDOM_COLD_SAMPLES
            and api.matched_positive.contention_samples == api.matched_positive.samples
            and api.negative.contention_samples == api.negative.samples
            and api.random.contention_samples == api.random.samples,
        ),
        _gate(
            "api_distinct_matched_keys",
            fully_contended_matched["distinct_query_keys"] >= MIN_DISTINCT_MATCHED_KEYS,
        ),
        _gate(
            "api_distinct_negative_keys",
            fully_contended_negative["distinct_query_keys"]
            >= MIN_DISTINCT_NEGATIVE_KEYS,
        ),
        _gate(
            "api_distinct_random_keys",
            fully_contended_random["distinct_query_keys"] >= MIN_DISTINCT_RANDOM_KEYS,
        ),
        _gate("api_fresh_processes", api.fresh_processes),
        _gate(
            "api_matched_positive_cold_p95",
            fully_contended_matched["cold_first_page_p95_ms"] is not None
            and fully_contended_matched["cold_first_page_p95_ms"]
            <= MAX_COLD_FIRST_PAGE_P95_MS,
        ),
        _gate(
            "api_negative_cold_p95",
            fully_contended_negative["cold_first_page_p95_ms"] is not None
            and fully_contended_negative["cold_first_page_p95_ms"]
            <= MAX_COLD_FIRST_PAGE_P95_MS,
        ),
        _gate(
            "api_random_cold_p95",
            fully_contended_random["cold_first_page_p95_ms"] is not None
            and fully_contended_random["cold_first_page_p95_ms"]
            <= MAX_COLD_FIRST_PAGE_P95_MS,
        ),
        _gate(
            "api_error_rate",
            api_error_rate <= MAX_API_ERROR_RATE
            and api.matched_positive.errors == 0
            and api.negative.errors == 0
            and api.random.errors == 0,
        ),
    ]
    failed_gate_ids = [gate["id"] for gate in gates if not gate["passed"]]
    status = "pass" if not failed_gate_ids else "fail"
    exit_code = EXIT_PASS if status == "pass" else EXIT_GATE_FAILURE

    return {
        "schema_version": SCHEMA_VERSION,
        "gate": {"name": "ptg2_v3_capacity", "version": SCRIPT_VERSION},
        "status": status,
        "release": status == "pass",
        "exit_code": exit_code,
        "evidence_receipt": {
            "authenticated": True,
            "contention_run_id": receipt.contention_run_id,
            "measurement_sha256": receipt.measurement_sha256,
            "issued_at": receipt.issued_at.strftime(_UTC_TIMESTAMP_FORMAT),
            "expires_at": receipt.expires_at.strftime(_UTC_TIMESTAMP_FORMAT),
            "observation_started_at": receipt.observation_started_at.strftime(
                _UTC_TIMESTAMP_FORMAT
            ),
            "observation_ended_at": receipt.observation_ended_at.strftime(
                _UTC_TIMESTAMP_FORMAT
            ),
            "commitments": {
                name: {
                    "sha256": commitment.sha256,
                    "sample_count": commitment.sample_count,
                }
                for name, commitment in receipt.commitments.items()
            },
        },
        "objective": {
            "target_logical_imports_per_month": logical_import_target,
            "month_days": MONTH_DAYS,
            "month_hours": _json_number(MONTH_HOURS),
        },
        "limits": {
            "minimum_target_logical_imports_per_month": (
                MIN_RELEASE_TARGET_LOGICAL_IMPORTS_PER_MONTH
            ),
            "unique_build_max_minutes": _json_number(MAX_UNIQUE_BUILD_MINUTES),
            "minimum_qualifying_unique_builds": MIN_QUALIFYING_UNIQUE_BUILDS,
            "minimum_reuse_only_samples": MIN_REUSE_ONLY_SAMPLES,
            "minimum_candidate_audit_samples": MIN_CANDIDATE_AUDIT_SAMPLES,
            "lane_utilization_max_fraction": _json_number(MAX_LANE_UTILIZATION),
            "minimum_resource_headroom_fraction": _json_number(
                MIN_RESOURCE_HEADROOM_FRACTION
            ),
            "minimum_peak_sample_windows": MIN_PEAK_SAMPLE_WINDOWS,
            "minimum_peak_window_minutes": _json_number(MIN_PEAK_WINDOW_MINUTES),
            "minimum_peak_observation_minutes": _json_number(
                MIN_PEAK_OBSERVATION_MINUTES
            ),
            "maximum_peak_gap_minutes": _json_number(MAX_PEAK_GAP_MINUTES),
            "minimum_peak_coverage_ratio": _json_number(MIN_PEAK_COVERAGE_RATIO),
            "import_queue_delay_max_minutes": _json_number(
                MAX_IMPORT_QUEUE_DELAY_MINUTES
            ),
            "candidate_audit_queue_age_max_minutes": _json_number(
                MAX_CANDIDATE_AUDIT_QUEUE_AGE_MINUTES
            ),
            "minimum_contention_seconds": _json_number(MIN_CONTENTION_SECONDS),
            "maximum_contention_gap_seconds": _json_number(MAX_CONTENTION_GAP_SECONDS),
            "minimum_contention_coverage_ratio": _json_number(
                MIN_CONTENTION_COVERAGE_RATIO
            ),
            "maximum_evidence_age_hours": int(MAX_EVIDENCE_AGE.total_seconds() // 3600),
            "minimum_api_requests_per_second": _json_number(
                MIN_API_REQUESTS_PER_SECOND
            ),
            "api_request_rate_bucket_seconds": _json_number(
                API_REQUEST_RATE_BUCKET_SECONDS
            ),
            "minimum_http_requests_per_activation": (MIN_HTTP_REQUESTS_PER_ACTIVATION),
            "minimum_pool_wait_observations": MIN_POOL_WAIT_OBSERVATIONS,
            "pool_wait_p95_max_ms": _json_number(MAX_POOL_WAIT_P95_MS),
            "pool_wait_max_ms": _json_number(MAX_POOL_WAIT_MS),
            "minimum_checkpoints": MIN_CHECKPOINTS,
            "autovacuum_pending_max_minutes": _json_number(
                MAX_AUTOVACUUM_PENDING_MINUTES
            ),
            "minimum_gc_window_hours": _json_number(MIN_GC_WINDOW_HOURS),
            "minimum_gc_cycles": MIN_GC_CYCLES,
            "minimum_gc_layouts": MIN_GC_LAYOUTS,
            "api_cold_first_page_p95_max_ms": _json_number(MAX_COLD_FIRST_PAGE_P95_MS),
            "api_error_rate_max_fraction": _json_number(MAX_API_ERROR_RATE),
            "api_minimum_requests": MIN_API_REQUESTS,
            "api_minimum_matched_positive_samples": (MIN_MATCHED_POSITIVE_COLD_SAMPLES),
            "api_minimum_negative_samples": MIN_NEGATIVE_COLD_SAMPLES,
            "api_minimum_random_samples": MIN_RANDOM_COLD_SAMPLES,
            "api_minimum_distinct_matched_keys": MIN_DISTINCT_MATCHED_KEYS,
            "api_minimum_distinct_negative_keys": MIN_DISTINCT_NEGATIVE_KEYS,
            "api_minimum_distinct_random_keys": MIN_DISTINCT_RANDOM_KEYS,
        },
        "metrics": {
            "end_to_end": {
                "sample_count": end_to_end.sample_count,
                "unique_build_samples": end_to_end.unique_build_samples,
                "reuse_samples": end_to_end.reuse_samples,
                "complete_stage_samples": end_to_end.complete_stage_samples,
                "within_15_minutes": end_to_end.within_15_minutes,
                "p95_minutes": _json_number(end_to_end.p95_minutes),
                "max_minutes": _json_number(end_to_end.max_minutes),
                "errors": end_to_end.errors,
            },
            "unique_build": {
                "sample_count": unique.sample_count,
                "qualifying_builds": qualifying_unique_builds,
                "p95_minutes": _json_number(unique.p95_minutes),
                "worst_case_minutes": _json_number(unique.max_minutes),
            },
            "reuse": {
                "sample_count": reuse.reuse_only_sample_count,
                "p95_minutes": _json_number(reuse.reuse_only_p95_minutes),
                "worst_case_minutes": _json_number(reuse.reuse_only_max_minutes),
            },
            "candidate_audit": {
                "sample_count": audit.sample_count,
                "successful_audits": audit.successful_audits,
                "duration_p95_minutes": _json_number(audit.duration_p95_minutes),
                "duration_max_minutes": _json_number(audit.duration_max_minutes),
                "queue_age_p95_minutes": _json_number(audit.queue_age_p95_minutes),
                "queue_age_max_minutes": _json_number(audit.queue_age_max_minutes),
                "activation_p95_minutes": _json_number(audit.activation_p95_minutes),
                "activation_max_minutes": _json_number(audit.activation_max_minutes),
                "lane_count": audit.lane_count,
                "availability_factor": _json_number(audit.availability_factor),
                "worker_hours_per_month": _json_number(audit_worker_hours),
                "available_lane_hours": _json_number(available_audit_lane_hours),
                "lane_utilization_fraction": _json_number(audit_lane_utilization),
                "http_requests_per_activation": (audit.http_requests_per_activation),
                "http_requests_per_month": monthly_audit_http_requests,
                "observed_http_requests": audit.observed_http_requests,
                "observed_request_seconds": _json_number(
                    audit.observed_request_seconds
                ),
                "observed_requests_per_second": _json_number(
                    observed_audit_http_requests_per_second
                ),
                "errors": audit.errors,
            },
            "monthly_capacity": {
                "worst_case_unique_builds": logical_import_target,
                "reuse_adjusted_unique_builds": reuse_adjusted_unique_builds,
                "reuse_adjusted_reuse_hits": reuse_adjusted_reuse_hits,
                "observed_reuse_hits": reuse.observed_reuse_hits,
                "qualified_reuse_hits": qualified_reuse_hits,
                "qualified_reuse_fraction": _json_number(
                    Decimal(qualified_reuse_hits) / reuse.observed_logical_imports
                ),
                "retry_attempt_rate": _json_number(
                    Decimal(retry.failed_attempts)
                    / (end_to_end.sample_count + retry.failed_attempts)
                ),
                "retry_overhead_minutes_per_unique_build": _json_number(
                    unique_retry_overhead_minutes
                ),
                "retry_overhead_minutes_per_reuse": _json_number(
                    reuse_retry_overhead_minutes
                ),
                "retry_overhead_minutes_per_logical_import": _json_number(
                    logical_retry_overhead_minutes
                ),
                "retry_adjusted_minutes_per_unique_build": _json_number(
                    retry_adjusted_unique_minutes
                ),
                "retry_adjusted_minutes_per_reuse": _json_number(
                    retry_adjusted_reuse_minutes
                ),
                "reuse_only_minutes_per_hit": _json_number(
                    reuse.reuse_only_max_minutes
                ),
                "worst_case_worker_hours": _json_number(worst_worker_hours),
                "reuse_adjusted_worker_hours": _json_number(reuse_worker_hours),
                "available_lane_count": lanes.count,
                "configured_unique_build_lane_count": active_lane_count,
                "availability_factor": _json_number(lanes.availability_factor),
                "available_lane_hours": _json_number(available_lane_hours),
                "worst_case_lane_utilization_fraction": _json_number(
                    worst_lane_utilization
                ),
                "reuse_adjusted_lane_utilization_fraction": _json_number(
                    reuse_lane_utilization
                ),
            },
            "peak_arrival": {
                "sample_windows": peak.sample_windows,
                "observation_minutes": _json_number(peak.observation_minutes),
                "window_minutes": _json_number(peak.window_minutes),
                "coverage_ratio": _json_number(peak.coverage_ratio),
                "max_gap_minutes": _json_number(peak.max_gap_minutes),
                "queue_delay_p95_minutes": _json_number(peak.queue_delay_p95_minutes),
                "max_queue_delay_minutes": _json_number(peak.max_queue_delay_minutes),
                "audit_queue_age_p95_minutes": _json_number(
                    peak.audit_queue_age_p95_minutes
                ),
                "max_audit_queue_age_minutes": _json_number(
                    peak.max_audit_queue_age_minutes
                ),
                "observed_peak_logical_imports": peak.observed_peak_logical_imports,
                "observed_peak_unique_builds": peak.observed_peak_unique_builds,
                "observed_peak_candidate_audits": (peak.observed_peak_candidate_audits),
                "minimum_representative_peak_logical_imports": (
                    minimum_representative_peak_arrivals
                ),
                "import_service_capacity_minutes": _json_number(
                    peak_import_service_minutes
                ),
                "import_service_demand_minutes": _json_number(
                    peak_import_demand_minutes
                ),
                "candidate_audit_service_capacity_minutes": _json_number(
                    peak_audit_service_minutes
                ),
                "candidate_audit_service_demand_minutes": _json_number(
                    peak_audit_demand_minutes
                ),
            },
            "scratch": {
                "configured_concurrent_unique_builds": (
                    scratch.configured_concurrent_unique_builds
                ),
                "measured_concurrent_unique_builds": (
                    scratch.measured_concurrent_unique_builds
                ),
                "usable_bytes": scratch_usable,
                "capacity_per_configured_build_bytes": scratch_capacity_per_build,
                "measured_peak_incremental_bytes": (
                    scratch.measured_peak_incremental_bytes
                ),
                "headroom_bytes": scratch_headroom,
                "headroom_fraction": _json_number(scratch_headroom_fraction),
                "cleanup_cycles": scratch.cleanup_cycles,
                "cleanup_failures": scratch.cleanup_failures,
            },
            "postgresql": {
                "load": {
                    "concurrent_unique_builds": postgres.load.concurrent_unique_builds,
                    "concurrent_candidate_audits": (
                        postgres.load.concurrent_candidate_audits
                    ),
                    "api_requests_per_second": _json_number(
                        postgres.load.api_requests_per_second
                    ),
                    "observed_api_requests_per_second": _json_number(
                        observed_api_requests_per_second
                    ),
                    "candidate_audit_requests_per_second": _json_number(
                        postgres.load.candidate_audit_requests_per_second
                    ),
                    "required_candidate_audit_requests_per_second": _json_number(
                        required_candidate_audit_requests_per_second
                    ),
                    "sample_seconds": _json_number(postgres.load.sample_seconds),
                },
                "connections": {
                    "usable_connections": usable_connections,
                    "peak_connections": peak_connections,
                    "headroom_connections": connection_headroom,
                    "required_headroom_connections": (
                        connections.required_headroom_connections
                    ),
                    "headroom_fraction": _json_number(connection_headroom_fraction),
                },
                "write": write_metrics,
                "wal": wal_metrics,
                "pool_wait": {
                    "observations": pool_wait.observations,
                    "required_observations": required_pool_wait_observations,
                    "waited_acquisitions": pool_wait.waited_acquisitions,
                    "p95_ms": _json_number(pool_wait.p95_ms),
                    "max_ms": _json_number(pool_wait.max_ms),
                    "timeout_errors": pool_wait.timeout_errors,
                },
                "checkpoint": {
                    "sample_seconds": _json_number(checkpoint.sample_seconds),
                    "completed": checkpoint.completed,
                    "requested": checkpoint.requested,
                    "busy_seconds": _json_number(checkpoint_busy_seconds),
                    "headroom_fraction": _json_number(checkpoint_headroom_fraction),
                },
                "temp": {
                    "usable_bytes": temp_usable,
                    "measured_peak_incremental_bytes": (
                        temp.measured_peak_incremental_bytes
                    ),
                    "headroom_bytes": temp_headroom,
                    "headroom_fraction": _json_number(temp_headroom_fraction),
                },
                "autovacuum": {
                    "sample_seconds": _json_number(autovacuum.sample_seconds),
                    "max_workers": autovacuum.max_workers,
                    "peak_workers": autovacuum.peak_workers,
                    "headroom_workers": autovacuum_headroom_workers,
                    "required_headroom_workers": (autovacuum.required_headroom_workers),
                    "headroom_fraction": _json_number(autovacuum_headroom_fraction),
                    "completed_cycles": autovacuum.completed_cycles,
                    "cancelled_cycles": autovacuum.cancelled_cycles,
                    "oldest_pending_minutes": _json_number(
                        autovacuum.oldest_pending_minutes
                    ),
                },
            },
            "storage": {
                "monthly_physical_bytes_worst_case": monthly_physical_worst,
                "monthly_physical_bytes_reuse_adjusted": monthly_physical_reuse,
                "monthly_logical_bytes": monthly_logical,
                "physical_retention_months": _json_number(
                    storage.physical_retention_months
                ),
                "logical_retention_months": _json_number(
                    storage.logical_retention_months
                ),
                "projected_retained_bytes_worst_case": projected_storage_worst,
                "projected_retained_bytes_reuse_adjusted": projected_storage_reuse,
                "headroom_bytes_worst_case": storage_headroom_worst,
                "headroom_bytes_reuse_adjusted": storage_headroom_reuse,
            },
            "gc": {
                "window_hours": _json_number(gc.window_hours),
                "cycles_observed": gc.cycles_observed,
                "executed_cycles": gc.executed_cycles,
                "eligible_layouts": gc.eligible_layouts,
                "deleted_layouts": gc.deleted_layouts,
                "eligible_bytes_per_hour": _json_number(gc_eligible_rate),
                "deleted_bytes_per_hour": _json_number(gc_delete_rate),
                "net_drain_bytes_per_hour": _json_number(gc_net_drain_rate),
                "headroom_fraction": _json_number(gc_headroom_fraction),
                "ending_backlog_bytes": gc.ending_backlog_bytes,
                "projected_30_day_backlog_bytes": projected_gc_backlog,
                "max_backlog_bytes": gc.max_backlog_bytes,
                "clearance_hours": _json_number(gc_clearance_hours),
                "max_clearance_hours": _json_number(gc.max_clearance_hours),
            },
            "api": {
                "concurrent_unique_builds": api.concurrent_unique_builds,
                "concurrent_candidate_audits": (api.concurrent_candidate_audits),
                "contention_seconds": _json_number(api.contention_seconds),
                "threshold_concurrency": {
                    "build": {
                        "required": scratch.configured_concurrent_unique_builds,
                        "observed_peak": api.concurrent_unique_builds,
                        "interval_count": len(
                            evidence.raw_samples.build_threshold.intervals
                        ),
                        "coverage_ratio": _json_number(
                            evidence.raw_samples.build_threshold.coverage_ratio
                        ),
                        "max_gap_seconds": _json_number(
                            evidence.raw_samples.build_threshold.max_gap_seconds
                        ),
                    },
                    "audit": {
                        "required": audit.lane_count,
                        "observed_peak": api.concurrent_candidate_audits,
                        "interval_count": len(
                            evidence.raw_samples.audit_threshold.intervals
                        ),
                        "coverage_ratio": _json_number(
                            evidence.raw_samples.audit_threshold.coverage_ratio
                        ),
                        "max_gap_seconds": _json_number(
                            evidence.raw_samples.audit_threshold.max_gap_seconds
                        ),
                    },
                    "full_lane": {
                        "interval_count": len(
                            evidence.raw_samples.full_lane_threshold.intervals
                        ),
                        "coverage_ratio": _json_number(
                            evidence.raw_samples.full_lane_threshold.coverage_ratio
                        ),
                        "max_gap_seconds": _json_number(
                            evidence.raw_samples.full_lane_threshold.max_gap_seconds
                        ),
                    },
                },
                "http_observation_span": {
                    "scope": "signed_requests_fully_inside_full_lane_intervals",
                    "coverage_ratio": _json_number(
                        evidence.raw_samples.http_observation_coverage_ratio
                    ),
                    "max_gap_seconds": _json_number(
                        evidence.raw_samples.http_observation_max_gap_seconds
                    ),
                },
                "request_rate_buckets": {
                    "scope": "signed_requests_fully_inside_full_lane_intervals",
                    "bucket_seconds": _json_number(
                        evidence.raw_samples.http_rate_buckets.bucket_seconds
                    ),
                    "bucket_count": (
                        evidence.raw_samples.http_rate_buckets.bucket_count
                    ),
                    "minimum_observations": (
                        evidence.raw_samples.http_rate_buckets.minimum_observations
                    ),
                    "minimum_requests_per_second": _json_number(
                        evidence.raw_samples.http_rate_buckets.minimum_requests_per_second
                    ),
                    "underfilled_buckets": (
                        evidence.raw_samples.http_rate_buckets.underfilled_buckets
                    ),
                },
                "contention_coverage_ratio": _json_number(
                    evidence.raw_samples.contention_coverage_ratio
                ),
                "contention_max_gap_seconds": _json_number(
                    evidence.raw_samples.contention_max_gap_seconds
                ),
                "requests": api.requests,
                "planned_requests": api.planned_requests,
                "attempted_requests": api.attempted_requests,
                "succeeded_requests": api.succeeded_requests,
                "failed_requests": api.failed_requests,
                "retried_requests": api.retried_requests,
                "omitted_requests": omitted_api_requests,
                "observed_successful_requests": observed_api_successes,
                "observed_failed_requests": observed_api_failures,
                "requests_while_imports_running": (api.requests_while_imports_running),
                "requests_per_second": _json_number(observed_api_requests_per_second),
                "candidate_audit_requests": api.candidate_audit_requests,
                "candidate_audit_requests_while_imports_running": (
                    api.candidate_audit_requests_while_imports_running
                ),
                "candidate_audit_requests_per_second": _json_number(
                    observed_candidate_audit_requests_per_second
                ),
                "errors": api.errors,
                "error_rate_fraction": _json_number(api_error_rate),
                "fresh_processes": api.fresh_processes,
                "matched_positive": {
                    "samples": api.matched_positive.samples,
                    "fully_contended_samples": fully_contended_matched["samples"],
                    "distinct_query_keys": (api.matched_positive.distinct_query_keys),
                    "fully_contended_distinct_query_keys": fully_contended_matched[
                        "distinct_query_keys"
                    ],
                    "fully_contended_distinct_processes": fully_contended_matched[
                        "distinct_processes"
                    ],
                    "contention_samples": api.matched_positive.contention_samples,
                    "contention_distinct_query_keys": (
                        api.matched_positive.contention_distinct_query_keys
                    ),
                    "all_sample_cold_first_page_p95_ms": _json_number(
                        api.matched_positive.cold_first_page_p95_ms
                    ),
                    "cold_first_page_p95_ms": _json_number(
                        fully_contended_matched["cold_first_page_p95_ms"]
                    ),
                    "errors": api.matched_positive.errors,
                },
                "negative": {
                    "samples": api.negative.samples,
                    "fully_contended_samples": fully_contended_negative["samples"],
                    "distinct_query_keys": api.negative.distinct_query_keys,
                    "fully_contended_distinct_query_keys": fully_contended_negative[
                        "distinct_query_keys"
                    ],
                    "fully_contended_distinct_processes": fully_contended_negative[
                        "distinct_processes"
                    ],
                    "contention_samples": api.negative.contention_samples,
                    "contention_distinct_query_keys": (
                        api.negative.contention_distinct_query_keys
                    ),
                    "all_sample_cold_first_page_p95_ms": _json_number(
                        api.negative.cold_first_page_p95_ms
                    ),
                    "cold_first_page_p95_ms": _json_number(
                        fully_contended_negative["cold_first_page_p95_ms"]
                    ),
                    "errors": api.negative.errors,
                },
                "random": {
                    "samples": api.random.samples,
                    "fully_contended_samples": fully_contended_random["samples"],
                    "distinct_query_keys": api.random.distinct_query_keys,
                    "fully_contended_distinct_query_keys": fully_contended_random[
                        "distinct_query_keys"
                    ],
                    "fully_contended_distinct_processes": fully_contended_random[
                        "distinct_processes"
                    ],
                    "contention_samples": api.random.contention_samples,
                    "contention_distinct_query_keys": (
                        api.random.contention_distinct_query_keys
                    ),
                    "all_sample_cold_first_page_p95_ms": _json_number(
                        api.random.cold_first_page_p95_ms
                    ),
                    "cold_first_page_p95_ms": _json_number(
                        fully_contended_random["cold_first_page_p95_ms"]
                    ),
                    "errors": api.random.errors,
                },
            },
        },
        "gates": gates,
        "failures": failed_gate_ids,
        "redaction": {
            "policy": "signed_redacted_raw_samples_with_recomputed_aggregates",
            "input_values_echoed": False,
            "sensitive_identifiers_emitted": False,
        },
    }


def _evaluate_measurement_with_trust(
    record: Any,
    *,
    trust: ReceiptTrust,
    target_override: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Evaluate one record using an already loaded protected trust policy."""

    receipt = verify_receipt(record, trust=trust, now=now)
    evidence = validate_measurement(
        record,
        trust=trust,
        receipt=receipt,
        target_override=target_override,
    )
    _validate_commitment_bindings(record, evidence, receipt)
    return evaluate_evidence(evidence, receipt=receipt)


def evaluate_measurement(
    record: Any,
    *,
    target_override: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Evaluate release evidence against the fixed protected trust store."""

    return _evaluate_measurement_with_trust(
        record,
        trust=_load_release_trust(),
        target_override=target_override,
        now=now,
    )


def invalid_report(exc: EvidenceError) -> dict[str, Any]:
    """Return a redacted fail-closed report for invalid measurement evidence."""

    error_dict: dict[str, Any] = {"code": exc.code}
    if exc.field is not None:
        error_dict["field"] = exc.field
    return {
        "schema_version": SCHEMA_VERSION,
        "gate": {"name": "ptg2_v3_capacity", "version": SCRIPT_VERSION},
        "status": "invalid",
        "release": False,
        "exit_code": EXIT_INVALID_EVIDENCE,
        "errors": [error_dict],
        "redaction": {
            "policy": "signed_redacted_raw_samples_with_recomputed_aggregates",
            "input_values_echoed": False,
            "sensitive_identifiers_emitted": False,
        },
    }


def _synthetic_digest(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _timestamp_text(value: datetime) -> str:
    return value.strftime(_UTC_TIMESTAMP_FORMAT)


def _example_peak_windows(contention_started_at: datetime) -> list[dict[str, Any]]:
    peak_started_at = contention_started_at - timedelta(days=7)
    windows = []
    for index in range(7 * 24):
        started_at = peak_started_at + timedelta(hours=index)
        windows.append(
            {
                "started_at": _timestamp_text(started_at),
                "ended_at": _timestamp_text(started_at + timedelta(hours=1)),
                "logical_imports": 8,
                "unique_builds": 7,
                "candidate_audits": 8,
                "max_queue_delay_minutes": 10,
                "max_audit_queue_age_minutes": 5,
            }
        )
    return windows


def _example_peak_import_events(
    contention_started_at: datetime,
) -> list[dict[str, Any]]:
    peak_started_at = contention_started_at - timedelta(days=7)
    events: list[dict[str, Any]] = []
    for window_index in range(7 * 24):
        window_started_at = peak_started_at + timedelta(hours=window_index)
        for event_index in range(8):
            enqueued_at = window_started_at + timedelta(minutes=event_index * 5)
            queue_minutes = 10 if event_index == 0 else 5
            events.append(
                {
                    "import_id_sha256": _synthetic_digest(
                        f"synthetic-peak-import-{window_index}-{event_index}"
                    ),
                    "kind": "unique_build" if event_index < 7 else "reuse",
                    "enqueued_at": _timestamp_text(enqueued_at),
                    "build_started_at": _timestamp_text(
                        enqueued_at + timedelta(minutes=queue_minutes)
                    ),
                }
            )
    return events


def _example_peak_audit_events(
    contention_started_at: datetime,
) -> list[dict[str, Any]]:
    peak_started_at = contention_started_at - timedelta(days=7)
    events: list[dict[str, Any]] = []
    for window_index in range(7 * 24):
        window_started_at = peak_started_at + timedelta(hours=window_index)
        for event_index in range(8):
            queued_at = window_started_at + timedelta(minutes=event_index * 5)
            events.append(
                {
                    "audit_id_sha256": _synthetic_digest(
                        f"synthetic-peak-audit-{window_index}-{event_index}"
                    ),
                    "import_id_sha256": _synthetic_digest(
                        f"synthetic-peak-import-{window_index}-{event_index}"
                    ),
                    "queued_at": _timestamp_text(queued_at),
                    "started_at": _timestamp_text(queued_at + timedelta(minutes=5)),
                }
            )
    return events


def _example_import_row(
    index: int,
    contention_run_id: str,
    kind: str,
    enqueued_at: datetime,
    audit_started_at: datetime,
    audit_completed_at: datetime,
    attested_at: datetime,
    activated_at: datetime,
) -> dict[str, Any]:
    import_id = _synthetic_digest(f"synthetic-import-{index}")
    return {
        "import_id_sha256": import_id,
        "contention_run_id": contention_run_id,
        "kind": kind,
        "status": "succeeded",
        "enqueued_at": _timestamp_text(enqueued_at),
        "build_started_at": _timestamp_text(enqueued_at),
        "build_completed_at": _timestamp_text(audit_started_at),
        "source_audit_started_at": _timestamp_text(audit_started_at),
        "source_audit_completed_at": _timestamp_text(audit_completed_at),
        "candidate_attested_at": _timestamp_text(attested_at),
        "activated_at": _timestamp_text(activated_at),
        "fresh_fingerprint": kind == "unique_build",
        "complete_source_coverage": kind == "unique_build",
        "durable_publication": kind == "unique_build",
        "persisted_audit": kind == "unique_build",
        "release_scanner": kind == "unique_build",
        "representative_large_build": kind == "unique_build",
        "complete_fingerprint_verified": True,
        "logical_publication_verified": True,
        "production_like_observed": True,
        "audited_activation_verified": True,
        "failed_attempts": 0,
        "failed_attempt_worker_seconds": 0,
    }


def _example_audit_row(
    index: int,
    import_id: str,
    contention_run_id: str,
    audit_started_at: datetime,
    audit_completed_at: datetime,
    attested_at: datetime,
) -> dict[str, Any]:
    return {
        "audit_id_sha256": _synthetic_digest(f"synthetic-audit-{index}"),
        "import_id_sha256": import_id,
        "contention_run_id": contention_run_id,
        "status": "passed",
        "queued_at": _timestamp_text(audit_started_at),
        "started_at": _timestamp_text(audit_started_at),
        "completed_at": _timestamp_text(audit_completed_at),
        "candidate_attested_at": _timestamp_text(attested_at),
        "first_request_at": _timestamp_text(audit_started_at),
        "last_request_at": _timestamp_text(audit_completed_at),
        "http_requests": 3_000,
        "http_successes": 3_000,
        "http_errors": 0,
    }


def _example_lifecycle_rows(
    contention_started_at: datetime,
    contention_run_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int]:
    """Create joined synthetic import and audit rows plus observed concurrency."""

    imports: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    import_intervals: list[tuple[datetime, datetime]] = []
    audit_intervals: list[tuple[datetime, datetime]] = []
    for index in range(60):
        wave_index = index // 4
        lane_index = index % 4
        kind = "unique_build" if lane_index < 2 else "reuse"
        enqueued_at = contention_started_at + timedelta(minutes=wave_index * 4)
        audit_started_at = (
            enqueued_at + timedelta(minutes=4)
            if kind == "unique_build"
            else enqueued_at
        )
        audit_completed_at = audit_started_at + timedelta(minutes=4)
        attested_at = audit_completed_at + timedelta(minutes=1)
        activated_at = attested_at + timedelta(minutes=1)
        import_id = _synthetic_digest(f"synthetic-import-{index}")
        imports.append(
            _example_import_row(
                index,
                contention_run_id,
                kind,
                enqueued_at,
                audit_started_at,
                audit_completed_at,
                attested_at,
                activated_at,
            )
        )
        audits.append(
            _example_audit_row(
                index,
                import_id,
                contention_run_id,
                audit_started_at,
                audit_completed_at,
                attested_at,
            )
        )
        if kind == "unique_build":
            import_intervals.append((enqueued_at, audit_started_at))
        audit_intervals.append((audit_started_at, audit_completed_at))
    contention_ended_at = contention_started_at + timedelta(hours=1)
    return (
        imports,
        audits,
        _maximum_concurrency(
            import_intervals, contention_started_at, contention_ended_at
        ),
        _maximum_concurrency(
            audit_intervals, contention_started_at, contention_ended_at
        ),
    )


def _example_signed_api_evidence_payload(
    class_name: str,
    ordinal: int,
    process_started_at: datetime,
    server_observed_at: datetime,
    process_instance_digest: str,
    run_digest: str,
    scope_digest: str,
    contention_run_id: str,
    latency_ms: int,
) -> dict[str, Any]:
    return {
        "api_evidence_key_id": EXAMPLE_API_EVIDENCE_KEY_ID,
        "challenge_digest": _synthetic_digest(f"synthetic-{class_name}-challenge-{ordinal}"),
        "environment_id": EXAMPLE_ENVIRONMENT_ID,
        "evidence_version": _API_EVIDENCE_VERSION,
        "isolated": True,
        "method": "GET",
        "observation_ordinal": _API_OBSERVATION_ORDINAL,
        "page_limit": _API_PAGE_LIMIT,
        "path": _API_QUERY_PATH,
        "process_instance_digest": process_instance_digest,
        "process_started_at": _timestamp_text(process_started_at),
        "query_contract": _API_QUERY_CONTRACT_ID,
        "query_contract_digest": _API_QUERY_CONTRACT_DIGEST,
        "release_digest": EXAMPLE_RELEASE_DIGEST,
        "response_body_sha256": _synthetic_digest(f"synthetic-{class_name}-response-{ordinal}"),
        "response_status": 200,
        "run_digest": run_digest,
        "scope_digest": scope_digest,
        "semantic_query_digest": _synthetic_digest(f"synthetic-{class_name}-query-{ordinal}"),
        "server_received_at": _timestamp_text(server_observed_at),
        "server_observed_at": _timestamp_text(server_observed_at),
        "server_duration_ns": latency_ms * 1_000_000,
        "contention_run_id": contention_run_id,
        "semantic_class": class_name,
        "selection_method": _HTTP_SELECTION_METHODS[class_name],
        "selection_ordinal": ordinal,
        "cold": True,
        "first_observation": True,
        "result_count": 0 if class_name == "negative" else 1,
        "signature_algorithm": _API_SIGNATURE_ALGORITHM,
        "signature_domain": _API_SIGNATURE_DOMAIN,
        "signature_version": _API_SIGNATURE_VERSION,
    }


def _example_http_class_rows(
    class_name: str,
    observation_offsets: Sequence[int],
    latency_ms: int,
    contention_started_at: datetime,
    contention_ended_at: datetime,
    contention_run_id: str,
) -> list[dict[str, Any]]:
    """Create signed synthetic raw HTTP observations for one semantic class."""

    sample_rows = []
    run_digest = _synthetic_digest("synthetic-api-evidence-run")
    scope_digest = _synthetic_digest("synthetic-api-scope")
    contention_seconds = int(
        (contention_ended_at - contention_started_at).total_seconds()
    )
    for ordinal, observation_offset in enumerate(observation_offsets):
        if not 0 <= observation_offset < contention_seconds:
            raise ValueError("example_observation_outside_contention")
        server_observed_at = contention_started_at + timedelta(
            seconds=observation_offset
        )
        process_started_at = server_observed_at
        process_instance_digest = _synthetic_digest(
            f"synthetic-{class_name}-process-{ordinal}"
        )
        signed_payload = _example_signed_api_evidence_payload(
            class_name,
            ordinal,
            process_started_at,
            server_observed_at,
            process_instance_digest,
            run_digest,
            scope_digest,
            contention_run_id,
            latency_ms,
        )
        signature = _EXAMPLE_API_EVIDENCE_PRIVATE_KEY.sign(
            _api_signature_message(signed_payload)
        )
        sample_rows.append(
            {
                **signed_payload,
                "api_evidence_signature": _base64url_encode(signature),
                "collector_received_at": _timestamp_text(server_observed_at),
            }
        )
    return sample_rows


def _example_http_rows(
    contention_started_at: datetime,
    contention_run_id: str,
) -> dict[str, list[dict[str, Any]]]:
    contention_ended_at = contention_started_at + timedelta(hours=1)
    observation_offsets_by_class: dict[str, list[int]] = {
        "matched_positive": [],
        "negative": [],
        "random": [],
    }
    matched_positions = {0, 7, 14, 21, 28}
    negative_positions = {3, 10, 17, 24, 31}
    for observation_offset in range(60 * 60):
        block_position = observation_offset % 36
        if block_position in matched_positions:
            class_name = "matched_positive"
        elif block_position in negative_positions:
            class_name = "negative"
        else:
            class_name = "random"
        observation_offsets_by_class[class_name].append(observation_offset)
    return {
        "http_matched_positive": _example_http_class_rows(
            "matched_positive",
            observation_offsets_by_class["matched_positive"],
            32,
            contention_started_at,
            contention_ended_at,
            contention_run_id,
        ),
        "http_negative": _example_http_class_rows(
            "negative",
            observation_offsets_by_class["negative"],
            30,
            contention_started_at,
            contention_ended_at,
            contention_run_id,
        ),
        "http_random": _example_http_class_rows(
            "random",
            observation_offsets_by_class["random"],
            35,
            contention_started_at,
            contention_ended_at,
            contention_run_id,
        ),
    }


def _example_api_class(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "samples": len(rows),
        "distinct_query_keys": len(rows),
        "contention_samples": len(rows),
        "contention_distinct_query_keys": len(rows),
        "errors": 0,
        "cold_first_page_p95_ms": rows[0]["server_duration_ns"] // 1_000_000,
        "sample_started_at": rows[0]["server_received_at"],
        "sample_ended_at": rows[-1]["server_observed_at"],
    }


def _distributed_total(total: int, count: int, index: int) -> int:
    quotient, remainder = divmod(total, count)
    return quotient + int(index < remainder)


def _example_category_counters(
    epoch_sha256: str,
    import_bytes: int,
    api_bytes: int,
    other_bytes: int,
) -> dict[str, Any]:
    return {
        "epoch_sha256": epoch_sha256,
        "total_bytes": import_bytes + api_bytes + other_bytes,
        "import_bytes": import_bytes,
        "api_bytes": api_bytes,
        "other_bytes": other_bytes,
    }


def _example_resource_config(
    observed_at: datetime,
    gigabyte: int,
    config_revision_sha256: str,
) -> dict[str, Any]:
    return {
        "observed_at": _timestamp_text(observed_at),
        "config_revision_sha256": config_revision_sha256,
        "max_connections": 200,
        "reserved_connections": 10,
        "autovacuum_max_workers": 5,
        "write_enforced_limit_bytes_per_second": 500_000_000,
        "wal_enforced_limit_bytes_per_second": 300_000_000,
        "scratch_capacity_bytes": 1_000 * gigabyte,
        "scratch_reserve_bytes": 100 * gigabyte,
        "temp_capacity_bytes": 200 * gigabyte,
        "temp_reserve_bytes": 20 * gigabyte,
        "storage_capacity_bytes": 1_000_000 * gigabyte,
        "storage_reserve_bytes": 100_000 * gigabyte,
        "physical_retention_months": 3,
        "logical_retention_months": 12,
        "required_connection_headroom": 50,
        "required_autovacuum_headroom": 1,
        "gc_max_backlog_bytes": 500 * gigabyte,
        "gc_max_clearance_hours": 48,
    }


def _example_gc_cycles(
    contention_started_at: datetime,
    gigabyte: int,
    config_revision_sha256: str,
) -> list[dict[str, Any]]:
    cycle_count = 24
    cycle_started_at = contention_started_at - timedelta(hours=cycle_count)
    backlog = 100 * gigabyte
    backlog_layouts = 24
    cycles = []
    for index in range(cycle_count):
        newly_eligible = _distributed_total(400 * gigabyte, cycle_count, index)
        backlog_reduction = _distributed_total(100 * gigabyte, cycle_count, index)
        deleted = newly_eligible + backlog_reduction
        ending_backlog = backlog - backlog_reduction
        eligible_layouts = 2 if index < 6 else 1
        backlog_layout_reduction = 1
        deleted_layouts = eligible_layouts + backlog_layout_reduction
        ending_backlog_layouts = backlog_layouts - backlog_layout_reduction
        cycles.append(
            {
                "started_at": _timestamp_text(cycle_started_at),
                "ended_at": _timestamp_text(cycle_started_at + timedelta(hours=1)),
                "config_revision_sha256": config_revision_sha256,
                "executed": True,
                "overlap_detected": False,
                "reference_recheck_failures": 0,
                "starting_backlog_bytes": backlog,
                "starting_backlog_layouts": backlog_layouts,
                "newly_eligible_bytes": newly_eligible,
                "deleted_bytes": deleted,
                "ending_backlog_bytes": ending_backlog,
                "eligible_layouts": eligible_layouts,
                "deleted_layouts": deleted_layouts,
                "ending_backlog_layouts": ending_backlog_layouts,
            }
        )
        cycle_started_at += timedelta(hours=1)
        backlog = ending_backlog
        backlog_layouts = ending_backlog_layouts
    return cycles


def _example_resource_intervals(
    contention_started_at: datetime,
    gigabyte: int,
    config_revision_sha256: str,
    write_epoch_sha256: str,
    wal_epoch_sha256: str,
) -> list[dict[str, Any]]:
    interval_rows = []
    for index in range(720):
        interval_started_at = contention_started_at + timedelta(seconds=index * 5)
        step = index + 1
        interval_rows.append({
            "started_at": _timestamp_text(interval_started_at),
            "ended_at": _timestamp_text(interval_started_at + timedelta(seconds=5)),
            "config_revision_sha256": config_revision_sha256,
            "scratch_peak_used_bytes": 400 * gigabyte,
            "scratch_min_available_bytes": 600 * gigabyte,
            "temp_peak_used_bytes": 100 * gigabyte,
            "temp_min_available_bytes": 100 * gigabyte,
            "write_counters": _example_category_counters(write_epoch_sha256, step * 1_000_000_000, step * 25_000_000, step * 75_000_000),
            "wal_counters": _example_category_counters(wal_epoch_sha256, step * 500_000_000, step * 10_000_000, step * 90_000_000),
            "connections": {"api_connections": 30, "import_connections": 12, "other_connections": 20},
            "pool_wait": {"bucket_counts": [_distributed_total(200_000, 720, index), 0, 0, 0, 0, 0, 0], "overflow_count": 0, "max_ms": 0, "timeout_errors": 0},
            "autovacuum": {"workers": 3, "oldest_pending_seconds": 900},
        })
    return interval_rows


def _example_resource_events(
    contention_started_at: datetime,
    imports: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    unique_imports = [import_row for import_row in imports if import_row["kind"] == "unique_build"]
    cleanup_events = [
        {"import_id_sha256": import_row["import_id_sha256"], "started_at": _timestamp_text(contention_started_at + timedelta(minutes=index + 1)), "ended_at": _timestamp_text(contention_started_at + timedelta(minutes=index + 1, seconds=10)), "outcome": "completed"}
        for index, import_row in enumerate(unique_imports)
    ]
    checkpoint_events = [
        {"started_at": _timestamp_text(contention_started_at + timedelta(minutes=minute)), "ended_at": _timestamp_text(contention_started_at + timedelta(minutes=minute, seconds=60)), "trigger": "scheduled", "write_seconds": 50, "sync_seconds": 5}
        for minute in (5, 15, 25, 35, 45, 55)
    ]
    autovacuum_events = [
        {"relation_id_sha256": _synthetic_digest(f"synthetic-autovacuum-relation-{index}"), "started_at": _timestamp_text(contention_started_at + timedelta(minutes=2 + index * 7)), "ended_at": _timestamp_text(contention_started_at + timedelta(minutes=3 + index * 7)), "outcome": "completed"}
        for index in range(8)
    ]
    return cleanup_events, checkpoint_events, autovacuum_events


def _example_storage_deltas(
    imports: Sequence[Mapping[str, Any]], gigabyte: int
) -> list[dict[str, Any]]:
    return [
        {
            "import_id_sha256": import_row["import_id_sha256"],
            "kind": import_row["kind"],
            "physical_layout_id_sha256": _synthetic_digest(f"synthetic-layout-{import_row['import_id_sha256']}") if import_row["kind"] == "unique_build" else _synthetic_digest("synthetic-preexisting-layout"),
            "physical_net_new_bytes": 20 * gigabyte if import_row["kind"] == "unique_build" else 0,
            "logical_net_new_bytes": 10_000_000,
        }
        for import_row in imports
    ]


def _example_resource_baseline(
    observed_at: datetime,
    gigabyte: int,
    write_epoch_sha256: str,
    wal_epoch_sha256: str,
) -> dict[str, Any]:
    return {
        "observed_at": _timestamp_text(observed_at),
        "scratch_used_bytes": 100 * gigabyte,
        "scratch_available_bytes": 900 * gigabyte,
        "temp_used_bytes": 20 * gigabyte,
        "temp_available_bytes": 180 * gigabyte,
        "storage_used_bytes": 100_000 * gigabyte,
        "storage_available_bytes": 900_000 * gigabyte,
        "physical_layout_bytes": 80_000 * gigabyte,
        "physical_layout_count": 1_000,
        "logical_mapping_bytes": 20_000 * gigabyte,
        "logical_mapping_count": 5_000,
        "write_counters": _example_category_counters(write_epoch_sha256, 0, 0, 0),
        "wal_counters": _example_category_counters(wal_epoch_sha256, 0, 0, 0),
    }


def _example_event_counters(
    observed_at: datetime,
    checkpoint_epoch_sha256: str,
    checkpoint_completed: int,
    autovacuum_epoch_sha256: str,
    autovacuum_completed: int,
) -> dict[str, Any]:
    return {
        "observed_at": _timestamp_text(observed_at),
        "checkpoint_epoch_sha256": checkpoint_epoch_sha256,
        "checkpoint_completed": checkpoint_completed,
        "checkpoint_requested": 10,
        "autovacuum_epoch_sha256": autovacuum_epoch_sha256,
        "autovacuum_completed": autovacuum_completed,
        "autovacuum_cancelled": 5,
    }


def _example_storage_endpoint(measured_at: datetime, gigabyte: int) -> dict[str, Any]:
    return {
        "measured_at": _timestamp_text(measured_at),
        "used_bytes": 100_600 * gigabyte + 600_000_000,
        "available_bytes": 899_400 * gigabyte - 600_000_000,
        "physical_layout_bytes": 80_600 * gigabyte,
        "physical_layout_count": 1_030,
        "logical_mapping_bytes": 20_000 * gigabyte + 600_000_000,
        "logical_mapping_count": 5_060,
    }


def _example_resource_telemetry(
    contention_started_at: datetime,
    contention_run_id: str,
    imports: Sequence[Mapping[str, Any]],
    gigabyte: int,
) -> dict[str, Any]:
    """Create a complete internally consistent synthetic resource observation."""

    contention_ended_at = contention_started_at + timedelta(hours=1)
    config_revision_sha256 = _synthetic_digest("synthetic-resource-config")
    write_epoch_sha256 = _synthetic_digest("synthetic-write-counter-epoch")
    wal_epoch_sha256 = _synthetic_digest("synthetic-wal-counter-epoch")
    checkpoint_epoch_sha256 = _synthetic_digest("synthetic-checkpoint-counter-epoch")
    autovacuum_epoch_sha256 = _synthetic_digest("synthetic-autovacuum-counter-epoch")
    cleanup_events, checkpoint_events, autovacuum_events = _example_resource_events(contention_started_at, imports)
    return {
        "contention_run_id": contention_run_id,
        "contention_started_at": _timestamp_text(contention_started_at),
        "contention_ended_at": _timestamp_text(contention_ended_at),
        "config_start": _example_resource_config(contention_started_at, gigabyte, config_revision_sha256),
        "config_end": _example_resource_config(contention_ended_at, gigabyte, config_revision_sha256),
        "baseline": _example_resource_baseline(
            contention_started_at, gigabyte, write_epoch_sha256, wal_epoch_sha256
        ),
        "intervals": _example_resource_intervals(contention_started_at, gigabyte, config_revision_sha256, write_epoch_sha256, wal_epoch_sha256),
        "scratch_cleanup_events": cleanup_events,
        "checkpoint_events": checkpoint_events,
        "autovacuum_events": autovacuum_events,
        "event_counters_start": _example_event_counters(
            contention_started_at, checkpoint_epoch_sha256, 100,
            autovacuum_epoch_sha256, 200,
        ),
        "event_counters_end": _example_event_counters(
            contention_ended_at, checkpoint_epoch_sha256, 106,
            autovacuum_epoch_sha256, 208,
        ),
        "storage_endpoint": _example_storage_endpoint(contention_ended_at, gigabyte),
        "preexisting_layouts": [
            {
                "physical_layout_id_sha256": _synthetic_digest(
                    "synthetic-preexisting-layout"
                )
            }
        ],
        "storage_deltas": _example_storage_deltas(imports, gigabyte),
        "gc_cycles": _example_gc_cycles(
            contention_started_at,
            gigabyte,
            config_revision_sha256,
        ),
    }


def _example_postgres_resources(gigabyte: int) -> dict[str, Any]:
    return {
        "connections": {
            "max_connections": 200,
            "reserved_connections": 10,
            "peak_api_connections": 30,
            "peak_import_connections": 12,
            "peak_other_connections": 20,
            "required_headroom_connections": 50,
        },
        "write": {
            "import_bytes_per_second": 200_000_000,
            "api_bytes_per_second": 5_000_000,
            "other_bytes_per_second": 15_000_000,
            "sustainable_bytes_per_second": 500_000_000,
        },
        "wal": {
            "import_bytes_per_second": 100_000_000,
            "api_bytes_per_second": 2_000_000,
            "other_bytes_per_second": 18_000_000,
            "sustainable_bytes_per_second": 300_000_000,
        },
        "pool_wait": {
            "observations": 200_000,
            "waited_acquisitions": 0,
            "p95_ms": 0,
            "max_ms": 0,
            "timeout_errors": 0,
        },
        "checkpoint": {
            "sample_seconds": 3_600,
            "completed": 6,
            "requested": 0,
            "write_seconds": 300,
            "sync_seconds": 30,
        },
        "temp": {
            "capacity_bytes": 200 * gigabyte,
            "baseline_used_bytes": 20 * gigabyte,
            "reserve_bytes": 20 * gigabyte,
            "measured_peak_incremental_bytes": 80 * gigabyte,
        },
        "autovacuum": {
            "sample_seconds": 3_600,
            "max_workers": 5,
            "peak_workers": 3,
            "required_headroom_workers": 1,
            "completed_cycles": 8,
            "cancelled_cycles": 0,
            "oldest_pending_minutes": 15,
        },
    }


def _example_postgresql(
    gigabyte: int,
    contention_run_id: str,
    contention_started_at: datetime,
    concurrent_unique_builds: int,
    concurrent_audits: int,
) -> dict[str, Any]:
    """Create generic PostgreSQL evidence for the synthetic contention run."""

    contention_ended_at = contention_started_at + timedelta(hours=1)
    return {
        "load": {
            "contention_run_id": contention_run_id,
            "started_at": _timestamp_text(contention_started_at),
            "ended_at": _timestamp_text(contention_ended_at),
            "concurrent_unique_builds": concurrent_unique_builds,
            "concurrent_candidate_audits": concurrent_audits,
            "api_requests_per_second": 1,
            "candidate_audit_requests_per_second": 50,
            "sample_seconds": 3_600,
        },
        **_example_postgres_resources(gigabyte),
    }


def example_measurement() -> dict[str, Any]:
    """Return an unsigned, non-release record with synthetic redacted raw rows."""

    gigabyte = 1_000_000_000
    contention_started_at = datetime(2026, 7, 13, 9, tzinfo=UTC)
    contention_ended_at = contention_started_at + timedelta(hours=1)
    contention_run_id = _synthetic_digest("synthetic-contention-run")
    peak_windows = _example_peak_windows(contention_started_at)
    peak_import_events = _example_peak_import_events(contention_started_at)
    peak_audit_events = _example_peak_audit_events(contention_started_at)
    imports, audits, concurrent_builds, concurrent_audits = _example_lifecycle_rows(
        contention_started_at, contention_run_id
    )
    http_rows = _example_http_rows(contention_started_at, contention_run_id)
    resource_telemetry = _example_resource_telemetry(
        contention_started_at,
        contention_run_id,
        imports,
        gigabyte,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "evidence_profile": EVIDENCE_PROFILE,
        "release_evidence": False,
        "objective": {"target_logical_imports_per_month": 2_000},
        "raw_samples": {
            "import_lifecycle": imports,
            "audit_results": audits,
            "peak_import_events": peak_import_events,
            "peak_audit_events": peak_audit_events,
            **http_rows,
        },
        "resource_telemetry": resource_telemetry,
        "resource_observation": {
            "contention_run_id": contention_run_id,
            "contention_started_at": _timestamp_text(contention_started_at),
            "contention_ended_at": _timestamp_text(contention_ended_at),
            "storage_measured_at": _timestamp_text(contention_ended_at),
            "gc_started_at": _timestamp_text(
                contention_started_at - timedelta(hours=24)
            ),
            "gc_ended_at": _timestamp_text(contention_started_at),
        },
        "end_to_end": {
            "sample_count": 60,
            "unique_build_samples": 30,
            "reuse_samples": 30,
            "complete_stage_samples": 60,
            "within_15_minutes": 60,
            "p95_minutes": 10,
            "max_minutes": 10,
            "errors": 0,
        },
        "unique_build": {
            "sample_count": 30,
            "p95_minutes": 4,
            "max_minutes": 4,
            "fresh_fingerprint_builds": 30,
            "complete_source_coverage_builds": 30,
            "durable_publication_builds": 30,
            "persisted_audit_builds": 30,
            "release_scanner_builds": 30,
            "representative_large_builds": 30,
        },
        "reuse": {
            "evidence_type": REUSE_EVIDENCE_TYPE,
            "observed_logical_imports": 60,
            "observed_unique_builds": 30,
            "observed_reuse_hits": 30,
            "complete_fingerprint_verified_hits": 30,
            "logical_publication_verified_hits": 30,
            "production_like_observed_hits": 30,
            "audited_activation_verified_hits": 30,
            "reuse_only_sample_count": 30,
            "reuse_only_p95_minutes": 6,
            "reuse_only_max_minutes": 6,
        },
        "retry": {
            "successful_unique_builds": 30,
            "failed_attempts": 0,
            "failed_attempt_worker_minutes": 0,
        },
        "lanes": {"count": 2, "availability_factor": 0.9},
        "candidate_audit": {
            "sample_count": 60,
            "successful_audits": 60,
            "duration_p95_minutes": 4,
            "duration_max_minutes": 4,
            "queue_age_p95_minutes": 0,
            "queue_age_max_minutes": 0,
            "activation_p95_minutes": 1,
            "activation_max_minutes": 1,
            "lane_count": 2,
            "availability_factor": 0.9,
            "http_requests_per_activation": 3_000,
            "activations_with_http_floor": 60,
            "observed_http_requests": 180_000,
            "observed_request_seconds": 3_600,
            "errors": 0,
        },
        "peak_arrival": {
            "sample_windows": len(peak_windows),
            "observation_minutes": 7 * 24 * 60,
            "window_minutes": 60,
            "queue_delay_p95_minutes": 10,
            "max_queue_delay_minutes": 10,
            "audit_queue_age_p95_minutes": 5,
            "max_audit_queue_age_minutes": 5,
            "observed_peak_logical_imports": 8,
            "observed_peak_unique_builds": 7,
            "observed_peak_candidate_audits": 8,
            "windows": peak_windows,
        },
        "scratch": {
            "configured_concurrent_unique_builds": 2,
            "measured_concurrent_unique_builds": concurrent_builds,
            "capacity_bytes": 1_000 * gigabyte,
            "baseline_used_bytes": 100 * gigabyte,
            "reserve_bytes": 100 * gigabyte,
            "measured_peak_incremental_bytes": 300 * gigabyte,
            "cleanup_cycles": 30,
            "cleanup_failures": 0,
        },
        "postgresql": _example_postgresql(
            gigabyte,
            contention_run_id,
            contention_started_at,
            concurrent_builds,
            concurrent_audits,
        ),
        "storage": {
            "physical_unique_builds_observed": 30,
            "physical_net_new_bytes_observed": 600 * gigabyte,
            "physical_max_bytes_per_unique_build": 20 * gigabyte,
            "logical_imports_observed": 60,
            "logical_net_new_bytes_observed": 600_000_000,
            "logical_max_bytes_per_import": 10_000_000,
            "current_used_bytes": 100_600 * gigabyte + 600_000_000,
            "capacity_bytes": 1_000_000 * gigabyte,
            "reserve_bytes": 100_000 * gigabyte,
            "physical_retention_months": 3,
            "logical_retention_months": 12,
        },
        "gc": {
            "window_hours": 24,
            "cycles_observed": 24,
            "executed_cycles": 24,
            "overlap_count": 0,
            "reference_recheck_failures": 0,
            "starting_backlog_bytes": 100 * gigabyte,
            "starting_backlog_layouts": 24,
            "newly_eligible_bytes": 400 * gigabyte,
            "deleted_bytes": 500 * gigabyte,
            "ending_backlog_bytes": 0,
            "ending_backlog_layouts": 0,
            "max_backlog_bytes": 500 * gigabyte,
            "max_clearance_hours": 48,
            "eligible_layouts": 30,
            "deleted_layouts": 54,
        },
        "api": {
            "contention_run_id": contention_run_id,
            "concurrent_unique_builds": concurrent_builds,
            "concurrent_candidate_audits": concurrent_audits,
            "contention_seconds": 3_600,
            "contention_started_at": _timestamp_text(contention_started_at),
            "contention_ended_at": _timestamp_text(contention_ended_at),
            "fresh_processes": True,
            "planned_requests": 3_600,
            "attempted_requests": 3_600,
            "succeeded_requests": 3_600,
            "failed_requests": 0,
            "retried_requests": 0,
            "requests": 3_600,
            "requests_while_imports_running": 3_600,
            "candidate_audit_requests": 180_000,
            "candidate_audit_requests_while_imports_running": 180_000,
            "errors": 0,
            "matched_positive": _example_api_class(http_rows["http_matched_positive"]),
            "negative": _example_api_class(http_rows["http_negative"]),
            "random": _example_api_class(http_rows["http_random"]),
        },
    }


def _reject_constant(_value: str) -> None:
    raise ValueError("non_finite_number")


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result_dict: dict[str, Any] = {}
    for key, value in pairs:
        if key in result_dict:
            raise _DuplicateFieldError()
        result_dict[key] = value
    return result_dict


def parse_measurement_bytes(payload: bytes) -> Any:
    """Parse bounded UTF-8 JSON while rejecting duplicates and non-finite values."""

    if len(payload) > MAX_INPUT_BYTES:
        raise EvidenceError("input_too_large")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise EvidenceError("invalid_encoding")
    try:
        return json.loads(
            text,
            parse_float=Decimal,
            parse_int=int,
            parse_constant=_reject_constant,
            object_pairs_hook=_strict_object,
        )
    except _DuplicateFieldError:
        raise EvidenceError("duplicate_field")
    except (ValueError, RecursionError):
        raise EvidenceError("invalid_json")


def load_measurement(path: str) -> Any:
    """Read one bounded measurement from a file or stdin and parse it strictly."""

    try:
        if path == "-":
            payload = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
        else:
            with Path(path).open("rb") as input_file:
                payload = input_file.read(MAX_INPUT_BYTES + 1)
    except OSError:
        raise EvidenceError("input_unreadable")
    return parse_measurement_bytes(payload)


def _serialized(value: Mapping[str, Any], *, pretty: bool) -> str:
    return (
        json.dumps(
            value,
            sort_keys=True,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )


def _write_json(value: Mapping[str, Any], destination: str, *, pretty: bool) -> None:
    serialized = _serialized(value, pretty=pretty)
    if destination == "-":
        sys.stdout.write(serialized)
        return
    path = Path(destination)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(serialized, encoding="utf-8")
        os.replace(temporary, path)
    except OSError:
        with suppress(OSError):
            temporary.unlink()
        raise EvidenceError("output_unwritable")


class RedactedArgumentParser(argparse.ArgumentParser):
    """Suppress argparse messages that can echo sensitive argument values."""

    def error(self, _message: str) -> None:
        """Exit with a redacted invalid-arguments report instead of echoing input."""

        report = invalid_report(EvidenceError("invalid_arguments"))
        self.exit(EXIT_INVALID_EVIDENCE, _serialized(report, pretty=False))


def _positive_target(value: str) -> int:
    try:
        target = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("invalid")
    if target < 1:
        raise argparse.ArgumentTypeError("invalid")
    return target


def _read_trust_config_bytes(path: Path, expected_owner_uid: int) -> bytes:
    descriptor: int | None = None
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        metadata = os.fstat(descriptor)
        insecure = (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != expected_owner_uid
            or bool(stat.S_IMODE(metadata.st_mode) & 0o022)
        )
        if insecure:
            raise EvidenceError("insecure_trust_config")
        chunks: list[bytes] = []
        remaining = MAX_TRUST_CONFIG_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        trust_config_bytes = b"".join(chunks)
    except EvidenceError:
        raise
    except OSError:
        raise EvidenceError("receipt_trust_unavailable")
    finally:
        if descriptor is not None:
            with suppress(OSError):
                os.close(descriptor)
    if len(trust_config_bytes) > MAX_TRUST_CONFIG_BYTES:
        raise EvidenceError("trust_config_too_large")
    return trust_config_bytes


def _trust_from_config_object(config_value: Any) -> ReceiptTrust:
    path = "trust_config"
    fields = (
        "version",
        "algorithm",
        "key_id",
        "public_key_hex",
        "api_evidence_key_id",
        "api_evidence_public_key_hex",
        "release_digest",
        "environment_id",
        "collector_id",
        "collector_version",
    )
    config = _object(config_value, path, fields)
    if _integer(config, "version", path, minimum=1) != TRUST_CONFIG_VERSION:
        raise EvidenceError("unsupported_trust_config_version", f"{path}.version")
    _fixed_string(config, "algorithm", path, RECEIPT_ALGORITHM)
    public_key_hex = config["public_key_hex"]
    if (
        not isinstance(public_key_hex, str)
        or not _DIGEST_RE.fullmatch(public_key_hex)
        or public_key_hex == "0" * (ED25519_PUBLIC_KEY_BYTES * 2)
    ):
        raise EvidenceError("invalid_trust_configuration", f"{path}.public_key_hex")
    api_public_key_hex = config["api_evidence_public_key_hex"]
    if (
        not isinstance(api_public_key_hex, str)
        or not _DIGEST_RE.fullmatch(api_public_key_hex)
        or api_public_key_hex == "0" * (ED25519_PUBLIC_KEY_BYTES * 2)
    ):
        raise EvidenceError(
            "invalid_trust_configuration",
            f"{path}.api_evidence_public_key_hex",
        )
    api_evidence_key_id = config["api_evidence_key_id"]
    if not isinstance(api_evidence_key_id, str) or not _API_KEY_ID_RE.fullmatch(
        api_evidence_key_id
    ):
        raise EvidenceError(
            "invalid_trust_configuration", f"{path}.api_evidence_key_id"
        )
    return _validate_trust(
        ReceiptTrust(
            key_id=_identifier(config, "key_id", path),
            public_key=bytes.fromhex(public_key_hex),
            api_evidence_key_id=api_evidence_key_id,
            api_evidence_public_key=bytes.fromhex(api_public_key_hex),
            release_digest=_digest(config, "release_digest", path),
            environment_id=_digest(config, "environment_id", path),
            collector_id=_identifier(config, "collector_id", path),
            collector_version=_identifier(config, "collector_version", path),
        )
    )


def _load_trust_config(path: Path, expected_owner_uid: int) -> ReceiptTrust:
    payload = _read_trust_config_bytes(path, expected_owner_uid)
    try:
        parsed = parse_measurement_bytes(payload)
    except EvidenceError as exc:
        raise EvidenceError("invalid_trust_configuration", exc.field)
    return _trust_from_config_object(parsed)


def _load_release_trust() -> ReceiptTrust:
    """Load the non-overridable release verifier trust policy."""

    return _load_trust_config(TRUST_CONFIG_PATH, TRUST_CONFIG_OWNER_UID)


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the redacting command-line parser for the capacity gate."""

    parser = RedactedArgumentParser(
        description="Evaluate independently verifiable PTG2 V3 capacity evidence.",
    )
    parser.add_argument(
        "measurement", nargs="?", help="JSON record path, or - for stdin"
    )
    parser.add_argument(
        "--input", dest="input_path", help="JSON record path, or - for stdin"
    )
    parser.add_argument("--output", default="-", help="Report path, or - for stdout")
    parser.add_argument(
        "--target-logical-imports-per-month",
        type=_positive_target,
        default=None,
    )
    parser.add_argument(
        "--write-example",
        nargs="?",
        const="-",
        metavar="PATH",
        help="Write an unsigned non-release schema example to PATH or stdout",
    )
    return parser


def run_cli(argv: Sequence[str] | None = None) -> int:
    """Execute the capacity gate CLI and return its documented exit code."""

    args = build_argument_parser().parse_args(argv)
    if args.write_example is not None:
        if args.measurement is not None or args.input_path is not None:
            report = invalid_report(EvidenceError("conflicting_arguments"))
            _write_json(report, args.output, pretty=False)
            return EXIT_INVALID_EVIDENCE
        try:
            _write_json(example_measurement(), args.write_example, pretty=True)
        except EvidenceError as exc:
            sys.stderr.write(_serialized(invalid_report(exc), pretty=False))
            return EXIT_INVALID_EVIDENCE
        return EXIT_GATE_FAILURE

    if (args.measurement is None) == (args.input_path is None):
        report = invalid_report(EvidenceError("measurement_input_required"))
        try:
            _write_json(report, args.output, pretty=False)
        except EvidenceError as exc:
            sys.stderr.write(_serialized(invalid_report(exc), pretty=False))
        return EXIT_INVALID_EVIDENCE

    input_path = args.input_path if args.input_path is not None else args.measurement
    try:
        measurement_record = load_measurement(input_path)
        report = evaluate_measurement(
            measurement_record,
            target_override=args.target_logical_imports_per_month,
        )
    except EvidenceError as exc:
        report = invalid_report(exc)

    try:
        _write_json(report, args.output, pretty=False)
    except EvidenceError as exc:
        sys.stderr.write(_serialized(invalid_report(exc), pretty=False))
        return EXIT_INVALID_EVIDENCE
    return int(report["exit_code"])


def main(argv: Sequence[str] | None = None) -> int:
    """Run the capacity-gate command-line entry point."""

    return run_cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())
