#!/usr/bin/env python3
"""Independent source-to-API release audit for strict PTG2 v3 serving.

The source side deliberately does not import importer or serving code.  It reads
plain or gzip TiC JSON files as an ijson event stream and stores only normalized
relationships in a temporary SQLite database.  Two source passes make
top-level ``provider_references`` order-independent.

The API-to-source side independently bottom-k samples a bounded, persisted
publish-time occurrence sample.  It never asks the API to enumerate the full
served occurrence population.

Canonical comparison rules (also embedded in every report):

* scalar strings are stripped; empty strings and JSON null become null;
* code systems, billing codes, arrangements, and modifiers are upper-cased;
* known code-system aliases and RC/POS/MS-DRG widths match the public API;
* money is compared as a finite base-10 value with insignificant zeroes removed;
* expiration dates must be ISO ``YYYY-MM-DD`` and compare by ISO value;
* service-code and modifier lists are stripped, canonicalized, sorted, and
  de-duplicated because the strict serving contract exposes canonical sets;
  payer-packed comma-separated modifier entries are split into individual codes;
* all other strings (type, class, setting, and additional information) preserve
  case after trimming; and
* tuple multiplicity is retained.  A count mismatch is a duplicate-count
  failure even when the canonical tuple itself exists on both sides.

The JSON report never includes source paths, plan/snapshot values, auth values,
HTTP bodies, organization names, reporting-entity names, or arbitrary source text.
Sensitive identifiers needed for reproducibility are represented by SHA-256.
"""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import contextlib
import dataclasses
import datetime as dt
import gzip
import hashlib
import heapq
import json
import math
import os
import platform
import secrets
import sqlite3
import statistics
import struct
import sys
import tempfile
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Generic, Protocol, TypeVar
from urllib.parse import urlsplit

import httpx
import ijson

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from api import ptg2_capacity_evidence as capacity_evidence


SCRIPT_VERSION = "2.12.0"
EXPECTED_ARCHITECTURE = "postgres_binary_v3"
EXPECTED_STORAGE_GENERATION = "shared_blocks_v3"
EXPECTED_DATABASE_BACKEND = "postgresql"
DATABASE_EVIDENCE_CONTRACT = "postgresql_session_v1"
EXPECTED_QUERY_MODE = "exact_source"
EXPECTED_PRICING_SCOPE = "plan_scoped_ptg"
DEFAULT_API_PATH = "/api/v1/pricing/providers/search-by-procedure"
DEFAULT_CANDIDATE_API_PATH = "/api/v1/pricing/providers/audit-search-by-procedure"
DEFAULT_API_AUDIT_PATH = "/api/v1/pricing/providers/audit-occurrences"
CANDIDATE_AUDIT_HEADER = "X-HealthPorta-PTG-Candidate-Audit"
VERIFIED_HTTPS_TRANSPORT = "verified_https_v1"
TRUSTED_CLUSTER_HTTP_TRANSPORT = "authenticated_cluster_service_v1"
AUDIT_SAMPLE_CONTRACT = "persisted_served_occurrence_sample_v2"
AUDIT_SAMPLE_METHOD = "publish_time_stratified_v1"
AUDIT_SAMPLE_FORMAT_VERSION = 2
AUDIT_SAMPLE_OCCURRENCE_IDENTITY = "sha256_candidate_ordinal_source_key_v2"
AUDIT_SAMPLE_MULTIPLICITY = "source_multiset_v1"
SOURCE_SET_CONTRACT = "sorted_raw_container_sha256_bytes_v1"
PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT = (
    "ptg2_provider_identifier_quarantine_v1"
)
PROVIDER_IDENTIFIER_QUARANTINE_HASH_DOMAIN = (
    b"PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V1\0"
)
AUDIT_SAMPLE_DIGEST_DOMAIN = b"PTG2V3AUDITROWS\x02"
AUDIT_SAMPLE_DIGEST_COORDINATE_FIELDS = (
    "code_key",
    "provider_set_key",
    "price_key",
    "source_artifact_key",
    "npi",
    "atom_ordinal",
    "atom_key",
)
_AUDIT_SAMPLE_DIGEST_COORDINATES = struct.Struct(">IIIQQQQ")
DEFAULT_API_AUDIT_PAGE_SIZE = 100
DEFAULT_SEED = "ptg2-v3-source-api-audit-v1"
RELEASE_FIXED_SEED_CONTRACT = "fixed_versioned_audit_seed_v1"
RELEASE_CAPACITY_SEED_CONTRACT = "pinned_capacity_trust_derived_seed_v1"
DIAGNOSTIC_SEED_CONTRACT = "diagnostic_caller_selected_seed_v1"
RELEASE_CAPACITY_SEED_DOMAIN = b"PTG2V3SOURCEAUDITSEED\x01"
DEFAULT_SOURCE_OCCURRENCE_SAMPLES = 2_500
DEFAULT_API_OCCURRENCE_SAMPLES = 2_500
DEFAULT_NEGATIVE_SAMPLES = 500
DEFAULT_RANDOM_API_CALLS = 2_500
DEFAULT_RANDOM_API_MAX_LIMIT = 100
RELEASE_MIN_SOURCE_OCCURRENCES = 2_500
RELEASE_MIN_API_OCCURRENCES = 2_500
RELEASE_MIN_NEGATIVE_QUERIES = 250
RELEASE_MIN_RANDOM_API_CALLS = 2_500
RELEASE_MIN_TOTAL_API_CALLS = 3_000
CAPACITY_MIN_MATCHED_POSITIVE_ELIGIBLE_HTTP_REQUESTS = 100
CAPACITY_MIN_NEGATIVE_ELIGIBLE_HTTP_REQUESTS = 250
CAPACITY_MIN_RANDOM_ELIGIBLE_HTTP_REQUESTS = 2_500
CAPACITY_MIN_DISTINCT_RANDOM_QUERIES = 2_500
CAPACITY_MIN_ELIGIBLE_HTTP_REQUESTS = 3_000
CAPACITY_CLIENT_LATENCY_SOURCE = (
    "verified_eligible_client_end_to_end_http_duration_ms"
)
CAPACITY_SIGNED_LATENCY_SOURCE = "api_signed_server_duration_ns_diagnostic_only"
CAPACITY_UNQUALIFIED_CLIENT_LATENCY_SOURCE = (
    "unqualified_client_perf_counter_diagnostic_only"
)
CAPACITY_RANDOM_COHORT = "random_unique_without_replacement_v1"
CAPACITY_POSITIVE_COHORT = "matched_positive_first_observation_v1"
CAPACITY_NEGATIVE_COHORT = "negative_first_observation_v1"
CAPACITY_COHORTS = frozenset(
    {
        CAPACITY_RANDOM_COHORT,
        CAPACITY_POSITIVE_COHORT,
        CAPACITY_NEGATIVE_COHORT,
    }
)
_CAPACITY_SEMANTIC_CLASS_BY_COHORT = {
    CAPACITY_POSITIVE_COHORT: "matched_positive",
    CAPACITY_NEGATIVE_COHORT: "negative",
    CAPACITY_RANDOM_COHORT: "random",
}
RELEASE_MIN_DISTINCT_MATCHED_QUERY_KEYS = 100
RELEASE_MAX_FIRST_PAGE_P95_MS = 40.0
RELEASE_MAX_LOGICAL_QUERY_P95_MS = 1_000.0
MAX_CANONICAL_CHARS = 131_072
SCALAR_EVENTS = {"string", "number", "boolean", "null"}
PRICE_FIELDS = (
    "negotiated_type",
    "negotiated_rate",
    "expiration_date",
    "service_code",
    "billing_class",
    "setting",
    "billing_code_modifier",
    "additional_information",
)
SOURCE_METADATA_FIELDS = (
    "billing_code_type_version",
    "name",
    "description",
    "network_names",
)
TUPLE_FIELDS = (
    "code_system",
    "code",
    "npi",
    "negotiation_arrangement",
    *SOURCE_METADATA_FIELDS,
    *PRICE_FIELDS,
    "raw_container_sha256",
)
CODE_SYSTEM_ALIASES = {
    "CLM_REV_CNTR_CD": "RC",
    "PLACE_OF_SERVICE": "POS",
    "REVENUE_CENTER": "RC",
    "REVENUE_CODE": "RC",
    "REV_CNTR": "RC",
    "SERVICE_CODE": "POS",
    "BILLING_CODE_MODIFIER": "MODIFIER",
    "CPT_MODIFIER": "MODIFIER",
    "HCPCS_MODIFIER": "MODIFIER",
    "MOD": "MODIFIER",
    "ICD-10-CM": "ICD10CM",
    "ICD10": "ICD10CM",
    "ICD-10-PCS": "ICD10PCS",
    "MS-DRG": "MS_DRG",
    "MSDRG": "MS_DRG",
    "DRG": "MS_DRG",
    "RXCUI": "RXNORM",
    "SNOMED": "SNOMEDCT_US",
    "SNOMEDCT": "SNOMEDCT_US",
}
CANONICALIZATION = {
    "version": 5,
    "nulls": "price-field JSON null and stripped empty scalar strings canonicalize to null",
    "strings": "trim Unicode surrounding whitespace; otherwise preserve case and content",
    "source_metadata": (
        "billing code version, name, and description preserve trimmed empty string "
        "distinctly from null"
    ),
    "network_names": "rate and referenced-provider names form one trimmed, sorted, unique list; blank names are omitted",
    "codes": "trim and uppercase; apply documented API aliases and catalog widths",
    "npi": "canonical integral decimal in inclusive range 1000000000..9999999999",
    "provider_tin": (
        "a provider-group TIN object or nested TIN field marks its enclosing reference or rate; "
        "valid NPIs do not clear the marker, and rates are counted once"
    ),
    "decimal": "finite base-10 value; expand exponent; remove leading/trailing insignificant zeroes; -0 becomes 0",
    "date": "strict ISO calendar date YYYY-MM-DD, emitted with date.isoformat()",
    "service_code": "scalar-or-list; POS canonicalization; sorted unique list",
    "billing_code_modifier": (
        "scalar-or-list; split payer-packed comma values; uppercase; sorted unique list"
    ),
    "arrangement": "trim and uppercase; null remains null",
    "setting": "trim only; null remains null; exact case-sensitive comparison",
    "multiplicity": "count every source negotiated-price occurrence after provider membership union",
    "occurrence_identity": "canonical price tuple plus exact original raw-container SHA-256",
    "source_types": "TiC identities and strings retain their JSON schema types; negotiated_rate is numeric",
    "price_container_types": "recognized scalar fields reject objects/arrays; list fields require one flat string array",
    "api_types": "identities and pagination integers are JSON integers; text and list fields are not coerced",
    "source_keys": "source_artifact_key is the required nonnegative dense ordinal; source_key is an optional logical string",
    "audit_sample_digest": "SHA-256 over the V2 domain, occurrence ID, and big-endian IIIQQQQ persisted coordinates in occurrence-ID order",
    "source_set": "count plus SHA-256 over concatenated raw 32-byte container digests sorted by lowercase hex",
}


class AuditError(RuntimeError):
    """Base exception with a stable, report-safe error code."""

    code = "audit_error"


class ConfigurationError(AuditError):
    code = "configuration"


class SourceFormatError(AuditError):
    code = "source_format"


class SourceCoverageError(AuditError):
    code = "source_coverage"


class ApiError(AuditError):
    code = "http"

    def __init__(self, reason: str, *, status_code: int | None = None, body_sha256: str | None = None):
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code
        self.body_sha256 = body_sha256


class ApiSchemaError(AuditError):
    code = "api_schema"


@dataclass(frozen=True)
class CapacityEvidenceTrust:
    """Pinned public trust used to verify isolated API-process observations."""

    api_evidence_key_id: str
    api_evidence_public_key: bytes = dataclasses.field(repr=False)
    release_digest: str
    environment_id: str

    def __post_init__(self) -> None:
        key_id_characters = set(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
        )
        if (
            not self.api_evidence_key_id
            or len(self.api_evidence_key_id) > 64
            or not self.api_evidence_key_id[0].isascii()
            or not self.api_evidence_key_id[0].isalnum()
            or any(
                character not in key_id_characters
                for character in self.api_evidence_key_id
            )
        ):
            raise ConfigurationError("capacity_api_evidence_key_id_invalid")
        if (
            not isinstance(self.api_evidence_public_key, bytes)
            or len(self.api_evidence_public_key) != 32
        ):
            raise ConfigurationError("capacity_api_evidence_public_key_invalid")
        for field_name, digest in (
            ("release_digest", self.release_digest),
            ("environment_id", self.environment_id),
        ):
            if (
                len(digest) != 64
                or digest == "0" * 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ConfigurationError(f"capacity_{field_name}_invalid")

    @property
    def report_value(self) -> dict[str, Any]:
        """Return only public or one-way trust coordinates."""

        return {
            "api_evidence_key_id": self.api_evidence_key_id,
            "api_evidence_public_key_sha256": hashlib.sha256(
                self.api_evidence_public_key
            ).hexdigest(),
            "release_digest": self.release_digest,
            "environment_id": self.environment_id,
        }


@dataclass(frozen=True)
class CapacityObservationIntent:
    """Non-sensitive instruction to challenge one first-page HTTP request."""

    cohort: str
    first_for_query: bool

    def __post_init__(self) -> None:
        if self.cohort not in CAPACITY_COHORTS:
            raise ConfigurationError("capacity_evidence_cohort_invalid")


@dataclass(frozen=True)
class CapacityHttpObservation:
    """One physical attempt and its optional closed signed observation."""

    record_id: int
    plan_id: int
    cohort: str
    first_for_query: bool
    attempt_index: int
    latency_ms: float
    response_status_code: int | None
    response_redirect_count: int
    observation: Mapping[str, Any] | None
    verification_error: str | None


@dataclass(frozen=True)
class _CapacityObservationPlan:
    plan_id: int
    intent: CapacityObservationIntent
    selection_ordinal: int


@dataclass(frozen=True)
class _CapacityResponseDetails:
    challenge: str
    query_parameters: Mapping[str, Any]
    response_headers: Mapping[str, Any]
    response_status_code: int
    response_body: bytes
    response_redirect_count: int
    collector_received_at: dt.datetime
    attempt_index: int
    latency_ms: float


class CapacityEvidenceCollector:
    """Thread-safe signed-observation collector for one audit run."""

    def __init__(
        self,
        trust: CapacityEvidenceTrust,
        *,
        contention_run_id: str,
        run_nonce: str | None = None,
        state: capacity_evidence.CapacityEvidenceState | None = None,
        sampling_seed: str = DEFAULT_SEED,
        sampling_profile: str = "diagnostic",
    ):
        self.trust = trust
        self._contention_run_id = str(contention_run_id)
        if (
            len(self._contention_run_id) != 64
            or self._contention_run_id == "0" * 64
            or any(
                character not in "0123456789abcdef"
                for character in self._contention_run_id
            )
        ):
            raise ConfigurationError("capacity_contention_run_id_invalid")
        self._run_nonce = run_nonce or secrets.token_hex(32)
        if (
            len(self._run_nonce) != 64
            or self._run_nonce == "0" * 64
            or any(character not in "0123456789abcdef" for character in self._run_nonce)
        ):
            raise ConfigurationError("capacity_run_nonce_invalid")
        self._state = state or capacity_evidence.CapacityEvidenceState()
        self._sampling_seed_evidence = sampling_seed_evidence(
            sampling_profile,
            sampling_seed,
            trust,
        )
        self._lock = threading.Lock()
        self._challenges: set[str] = set()
        self._next_plan_id = 1
        self._next_record_id = 1
        self._next_selection_ordinal_by_cohort: collections.Counter[str] = (
            collections.Counter()
        )
        self._plans: dict[int, CapacityObservationIntent] = {}
        self._records: dict[int, CapacityHttpObservation] = {}
        self._semantic_success: dict[int, bool] = {}

    def plan(self, intent: CapacityObservationIntent) -> _CapacityObservationPlan:
        """Register one logical challenged request before any physical attempt."""

        with self._lock:
            plan_id = self._next_plan_id
            self._next_plan_id += 1
            selection_ordinal = self._next_selection_ordinal_by_cohort[intent.cohort]
            self._next_selection_ordinal_by_cohort[intent.cohort] += 1
            self._plans[plan_id] = intent
        return _CapacityObservationPlan(
            plan_id=plan_id,
            intent=intent,
            selection_ordinal=selection_ordinal,
        )

    def fresh_challenge(self) -> str:
        """Return a unique lower-hex 32-byte challenge under concurrency."""

        while True:
            challenge = secrets.token_hex(32)
            if challenge == self._run_nonce or challenge == "0" * 64:
                continue
            with self._lock:
                if challenge in self._challenges:
                    continue
                self._challenges.add(challenge)
                return challenge

    def request_headers(
        self, plan: _CapacityObservationPlan, challenge: str
    ) -> dict[str, str]:
        """Build request-local headers without exposing the run nonce elsewhere."""

        return {
            capacity_evidence.CAPACITY_CHALLENGE_HEADER: challenge,
            capacity_evidence.CAPACITY_RUN_NONCE_HEADER: self._run_nonce,
            capacity_evidence.CAPACITY_CONTENTION_RUN_ID_HEADER: (
                self._contention_run_id
            ),
            capacity_evidence.CAPACITY_SEMANTIC_CLASS_HEADER: (
                _CAPACITY_SEMANTIC_CLASS_BY_COHORT[plan.intent.cohort]
            ),
            capacity_evidence.CAPACITY_SELECTION_ORDINAL_HEADER: str(
                plan.selection_ordinal
            ),
        }

    def _store_attempt(
        self,
        plan: _CapacityObservationPlan,
        *,
        attempt_index: int,
        latency_ms: float,
        response_status_code: int | None,
        response_redirect_count: int,
        observation: Mapping[str, Any] | None,
        verification_error: str | None,
    ) -> CapacityHttpObservation:
        with self._lock:
            record_id = self._next_record_id
            self._next_record_id += 1
            record = CapacityHttpObservation(
                record_id=record_id,
                plan_id=plan.plan_id,
                cohort=plan.intent.cohort,
                first_for_query=plan.intent.first_for_query,
                attempt_index=attempt_index,
                latency_ms=latency_ms,
                response_status_code=response_status_code,
                response_redirect_count=response_redirect_count,
                observation=(dict(observation) if observation is not None else None),
                verification_error=verification_error,
            )
            self._records[record_id] = record
            return record

    def record_transport_failure(
        self,
        plan: _CapacityObservationPlan,
        *,
        attempt_index: int,
        latency_ms: float,
        reason: str = "transport_failure",
    ) -> CapacityHttpObservation:
        """Record a physical request that produced no HTTP response."""

        if reason not in {"request_timeout", "transport_failure"}:
            raise ConfigurationError("capacity_transport_failure_reason_invalid")

        return self._store_attempt(
            plan,
            attempt_index=attempt_index,
            latency_ms=latency_ms,
            response_status_code=None,
            response_redirect_count=0,
            observation=None,
            verification_error=reason,
        )

    def record_unverified_response(
        self,
        plan: _CapacityObservationPlan,
        *,
        attempt_index: int,
        latency_ms: float,
        response_status_code: int,
        response_redirect_count: int,
        reason: str,
    ) -> CapacityHttpObservation:
        """Record a response whose complete exact body could not be verified."""

        return self._store_attempt(
            plan,
            attempt_index=attempt_index,
            latency_ms=latency_ms,
            response_status_code=response_status_code,
            response_redirect_count=response_redirect_count,
            observation=None,
            verification_error=reason,
        )

    def collect_response(
        self,
        plan: _CapacityObservationPlan,
        response: _CapacityResponseDetails | None = None,
        **response_details_by_field: Any,
    ) -> CapacityHttpObservation:
        """Verify and retain one exact response without leaking request values."""

        if response is None:
            response = _CapacityResponseDetails(**response_details_by_field)
        elif response_details_by_field:
            raise TypeError("response_details_must_not_be_mixed")
        observation: Mapping[str, Any] | None = None
        verification_error: str | None = None
        try:
            observation = capacity_evidence.collect_capacity_http_observation(
                response.response_headers,
                challenge=response.challenge,
                run_nonce=self._run_nonce,
                query_parameters=response.query_parameters,
                response_status_code=response.response_status_code,
                response_body=response.response_body,
                response_redirect_count=response.response_redirect_count,
                collector_received_at=response.collector_received_at,
                expected_api_evidence_key_id=self.trust.api_evidence_key_id,
                expected_api_evidence_public_key=self.trust.api_evidence_public_key,
                expected_release_digest=self.trust.release_digest,
                expected_environment_id=self.trust.environment_id,
                expected_contention_run_id=self._contention_run_id,
                expected_semantic_class=(
                    _CAPACITY_SEMANTIC_CLASS_BY_COHORT[plan.intent.cohort]
                ),
                expected_selection_ordinal=plan.selection_ordinal,
                state=self._state,
            )
        except capacity_evidence.CapacityEvidenceError as exc:
            verification_error = exc.code
        return self._store_attempt(
            plan,
            attempt_index=response.attempt_index,
            latency_ms=response.latency_ms,
            response_status_code=response.response_status_code,
            response_redirect_count=response.response_redirect_count,
            observation=observation,
            verification_error=verification_error,
        )

    def mark_semantic_outcome(
        self,
        observations: Sequence[CapacityHttpObservation],
        *,
        successful: bool,
    ) -> None:
        """Bind physical evidence to the completed semantic API audit result."""

        with self._lock:
            for observation in observations:
                if observation.record_id not in self._records:
                    raise ConfigurationError("capacity_observation_not_owned")
                self._semantic_success[observation.record_id] = successful

    @staticmethod
    def _commitment(observations: Sequence[Mapping[str, Any]]) -> str:
        digests = sorted(
            sha256_text(canonical_json(observation)) for observation in observations
        )
        return sha256_text(canonical_json(digests))

    def report(
        self,
        *,
        complete_request_http_requests: int | None = None,
        complete_request_retries: int | None = None,
    ) -> dict[str, Any]:
        """Return a redacted deterministic aggregate over all physical attempts."""

        with self._lock:
            plan_by_id = dict(self._plans)
            attempt_records = tuple(self._records.values())
            semantic_success_by_record_id = dict(self._semantic_success)
        report = _capacity_report_payload(
            self.trust,
            plan_by_id,
            attempt_records,
            semantic_success_by_record_id,
            self._commitment,
            self._sampling_seed_evidence,
        )
        if (
            complete_request_http_requests is None
            and complete_request_retries is None
        ):
            return report
        for accounting_count in (
            complete_request_http_requests,
            complete_request_retries,
        ):
            if (
                isinstance(accounting_count, bool)
                or not isinstance(accounting_count, int)
                or accounting_count < 0
            ):
                raise ConfigurationError("complete_request_accounting_invalid")
        report["complete_request_accounting"] = {
            "contract": "all_standard_http_attempts_v1",
            "actual_http_requests": complete_request_http_requests,
            "retries": complete_request_retries,
            "release_clean": complete_request_retries == 0,
        }
        return report


@dataclass(frozen=True)
class _CapacityReportMeasurements:
    planned_by_cohort: collections.Counter[str]
    physical_by_cohort: collections.Counter[str]
    verified_by_cohort: collections.Counter[str]
    eligible_by_cohort: collections.Counter[str]
    rejected_by_cohort: collections.Counter[str]
    status_counts: collections.Counter[str]
    rejection_counts: collections.Counter[str]
    verified_attempts: tuple[CapacityHttpObservation, ...]
    eligible_attempts: tuple[CapacityHttpObservation, ...]
    rejected_attempts: tuple[CapacityHttpObservation, ...]
    client_physical_latencies: tuple[float, ...]
    eligible_client_latencies: tuple[float, ...]
    eligible_client_latencies_by_cohort: Mapping[str, tuple[float, ...]]
    verified_server_latencies: tuple[float, ...]
    eligible_server_latencies: tuple[float, ...]
    eligible_server_latencies_by_cohort: Mapping[str, tuple[float, ...]]
    distinct_query_digests_by_cohort: Mapping[str, set[str]]
    reconciliation: Mapping[str, Any]


@dataclass
class _CapacityAttemptAccumulator:
    physical_by_cohort: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    verified_by_cohort: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    eligible_by_cohort: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    rejected_by_cohort: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    status_counts: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    rejection_counts: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    structural_failure_counts: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    verified_attempts: list[CapacityHttpObservation] = dataclasses.field(
        default_factory=list
    )
    eligible_attempts: list[CapacityHttpObservation] = dataclasses.field(
        default_factory=list
    )
    rejected_attempts: list[CapacityHttpObservation] = dataclasses.field(
        default_factory=list
    )
    client_physical_latencies: list[float] = dataclasses.field(default_factory=list)
    eligible_client_latencies: list[float] = dataclasses.field(default_factory=list)
    eligible_client_latencies_by_cohort: dict[str, list[float]] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(list)
    )
    verified_server_latencies: list[float] = dataclasses.field(default_factory=list)
    eligible_server_latencies: list[float] = dataclasses.field(default_factory=list)
    eligible_server_latencies_by_cohort: dict[str, list[float]] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(list)
    )
    distinct_query_digests_by_cohort: dict[str, set[str]] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(set)
    )
    observed_plan_ids: set[int] = dataclasses.field(default_factory=set)
    seen_record_ids: set[int] = dataclasses.field(default_factory=set)
    seen_plan_attempts: set[tuple[int, int]] = dataclasses.field(default_factory=set)
    attempt_indexes_by_plan: dict[int, list[int]] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(list)
    )
    initial_attempt_count: int = 0
    retry_attempt_count: int = 0
    invalid_attempt_index_count: int = 0
    timeout_count: int = 0
    transport_error_count: int = 0
    http_error_count: int = 0
    verification_error_count: int = 0


@dataclass(frozen=True)
class _CapacityAttemptTotals:
    planned_count: int
    physical_count: int
    verified_count: int
    eligible_count: int
    rejected_count: int
    verified_rejected_count: int
    unclassified_count: int
    missing_plan_count: int
    semantic_outcome_without_physical: int


def _signed_server_latency_ms(observation: Mapping[str, Any]) -> float:
    duration_ns = observation.get("server_duration_ns")
    if (
        isinstance(duration_ns, bool)
        or not isinstance(duration_ns, int)
        or duration_ns < 0
    ):
        raise ConfigurationError("capacity_signed_duration_missing")
    return duration_ns / 1_000_000.0


def _latency_gate_summary(values: Sequence[float]) -> dict[str, Any]:
    """Summarize latency with the strict capacity gate's nearest-rank p95."""

    report = latency_summary(values)
    if values:
        ordered = sorted(float(value) for value in values)
        rank_index = max(math.ceil(len(ordered) * 0.95) - 1, 0)
        report["gate_p95_ms"] = round(
            ordered[rank_index],
            6,
        )
    return report


def _record_capacity_attempt_status(
    attempt_record: CapacityHttpObservation,
    attempt_accumulator: _CapacityAttemptAccumulator,
) -> None:
    attempt_accumulator.physical_by_cohort[attempt_record.cohort] += 1
    attempt_accumulator.client_physical_latencies.append(attempt_record.latency_ms)
    if attempt_record.response_status_code is None:
        attempt_accumulator.transport_error_count += 1
        status_key = (
            "timeout"
            if attempt_record.verification_error == "request_timeout"
            else "transport_error"
        )
    else:
        status_key = str(attempt_record.response_status_code)
        if attempt_record.response_status_code != 200:
            attempt_accumulator.http_error_count += 1
    attempt_accumulator.status_counts[status_key] += 1
    if attempt_record.verification_error is not None:
        attempt_accumulator.verification_error_count += 1
    if attempt_record.verification_error == "request_timeout":
        attempt_accumulator.timeout_count += 1


def _capacity_attempt_structural_reasons(
    attempt_record: CapacityHttpObservation,
    plan_by_id: Mapping[int, CapacityObservationIntent],
    attempt_accumulator: _CapacityAttemptAccumulator,
) -> list[str]:
    structural_reasons: list[str] = []
    plan_intent = plan_by_id.get(attempt_record.plan_id)
    if plan_intent is None:
        structural_reasons.append("unknown_plan_id")
    else:
        attempt_accumulator.observed_plan_ids.add(attempt_record.plan_id)
        if (
            plan_intent.cohort != attempt_record.cohort
            or plan_intent.first_for_query != attempt_record.first_for_query
        ):
            structural_reasons.append("plan_intent_mismatch")
    if (
        isinstance(attempt_record.record_id, bool)
        or not isinstance(attempt_record.record_id, int)
        or attempt_record.record_id < 1
    ):
        structural_reasons.append("invalid_record_id")
    elif attempt_record.record_id in attempt_accumulator.seen_record_ids:
        structural_reasons.append("duplicate_record_id")
    else:
        attempt_accumulator.seen_record_ids.add(attempt_record.record_id)
    if (
        isinstance(attempt_record.attempt_index, bool)
        or not isinstance(attempt_record.attempt_index, int)
        or attempt_record.attempt_index < 0
    ):
        attempt_accumulator.invalid_attempt_index_count += 1
        structural_reasons.append("invalid_attempt_index")
    else:
        if attempt_record.attempt_index == 0:
            attempt_accumulator.initial_attempt_count += 1
        else:
            attempt_accumulator.retry_attempt_count += 1
        if plan_intent is not None:
            attempt_accumulator.attempt_indexes_by_plan[
                attempt_record.plan_id
            ].append(attempt_record.attempt_index)
            plan_attempt_coordinates = (
                attempt_record.plan_id,
                attempt_record.attempt_index,
            )
            if plan_attempt_coordinates in attempt_accumulator.seen_plan_attempts:
                structural_reasons.append("duplicate_plan_attempt")
            else:
                attempt_accumulator.seen_plan_attempts.add(plan_attempt_coordinates)
    return structural_reasons


def _record_capacity_attempt_outcome(
    attempt_record: CapacityHttpObservation,
    semantic_success_by_record_id: Mapping[int, bool],
    structural_reasons: Sequence[str],
    attempt_accumulator: _CapacityAttemptAccumulator,
) -> None:
    rejection_reasons = _capacity_rejection_reasons(
        attempt_record,
        semantic_success_by_record_id,
    )
    rejection_reasons.extend(structural_reasons)
    for structural_reason in set(structural_reasons):
        attempt_accumulator.structural_failure_counts[structural_reason] += 1
    if isinstance(attempt_record.observation, Mapping):
        attempt_accumulator.verified_attempts.append(attempt_record)
        attempt_accumulator.verified_by_cohort[attempt_record.cohort] += 1
        attempt_accumulator.verified_server_latencies.append(
            _signed_server_latency_ms(attempt_record.observation)
        )
    if not rejection_reasons and isinstance(attempt_record.observation, Mapping):
        attempt_accumulator.eligible_attempts.append(attempt_record)
        attempt_accumulator.eligible_by_cohort[attempt_record.cohort] += 1
        attempt_accumulator.eligible_client_latencies.append(attempt_record.latency_ms)
        attempt_accumulator.eligible_client_latencies_by_cohort[
            attempt_record.cohort
        ].append(attempt_record.latency_ms)
        signed_server_latency_ms = _signed_server_latency_ms(
            attempt_record.observation
        )
        attempt_accumulator.eligible_server_latencies.append(
            signed_server_latency_ms
        )
        attempt_accumulator.eligible_server_latencies_by_cohort[
            attempt_record.cohort
        ].append(signed_server_latency_ms)
        attempt_accumulator.distinct_query_digests_by_cohort[
            attempt_record.cohort
        ].add(str(attempt_record.observation["semantic_query_digest"]))
    else:
        attempt_accumulator.rejected_attempts.append(attempt_record)
        attempt_accumulator.rejected_by_cohort[attempt_record.cohort] += 1
    for rejection_reason in set(rejection_reasons):
        attempt_accumulator.rejection_counts[rejection_reason] += 1


def _record_capacity_attempt(
    attempt_record: CapacityHttpObservation,
    plan_by_id: Mapping[int, CapacityObservationIntent],
    semantic_success_by_record_id: Mapping[int, bool],
    attempt_accumulator: _CapacityAttemptAccumulator,
) -> None:
    _record_capacity_attempt_status(attempt_record, attempt_accumulator)
    structural_reasons = _capacity_attempt_structural_reasons(
        attempt_record, plan_by_id, attempt_accumulator
    )
    _record_capacity_attempt_outcome(
        attempt_record,
        semantic_success_by_record_id,
        structural_reasons,
        attempt_accumulator,
    )


def _record_capacity_plan_gaps(
    plan_by_id: Mapping[int, CapacityObservationIntent],
    semantic_success_by_record_id: Mapping[int, bool],
    attempt_accumulator: _CapacityAttemptAccumulator,
) -> tuple[int, int]:
    for attempt_indexes in attempt_accumulator.attempt_indexes_by_plan.values():
        distinct_indexes = set(attempt_indexes)
        if 0 not in distinct_indexes:
            attempt_accumulator.structural_failure_counts[
                "plan_missing_initial_attempt"
            ] += 1
        if distinct_indexes and distinct_indexes != set(
            range(max(distinct_indexes) + 1)
        ):
            attempt_accumulator.structural_failure_counts[
                "attempt_sequence_gap"
            ] += 1
    missing_plan_count = len(
        set(plan_by_id) - attempt_accumulator.observed_plan_ids
    )
    if missing_plan_count:
        attempt_accumulator.rejection_counts[
            "planned_without_physical_attempt"
        ] += missing_plan_count
        attempt_accumulator.structural_failure_counts[
            "planned_without_physical_attempt"
        ] += missing_plan_count
    semantic_outcome_without_physical = len(
        set(semantic_success_by_record_id) - attempt_accumulator.seen_record_ids
    )
    if semantic_outcome_without_physical:
        attempt_accumulator.structural_failure_counts[
            "semantic_outcome_without_physical_attempt"
        ] += semantic_outcome_without_physical
    return missing_plan_count, semantic_outcome_without_physical


def _capacity_attempt_totals(
    plan_by_id: Mapping[int, CapacityObservationIntent],
    attempt_records: Sequence[CapacityHttpObservation],
    attempt_accumulator: _CapacityAttemptAccumulator,
    missing_plan_count: int,
    semantic_outcome_without_physical: int,
) -> _CapacityAttemptTotals:
    physical_count = len(attempt_records)
    eligible_count = len(attempt_accumulator.eligible_attempts)
    rejected_count = len(attempt_accumulator.rejected_attempts)
    return _CapacityAttemptTotals(
        planned_count=len(plan_by_id),
        physical_count=physical_count,
        verified_count=len(attempt_accumulator.verified_attempts),
        eligible_count=eligible_count,
        rejected_count=rejected_count,
        verified_rejected_count=sum(
            isinstance(attempt.observation, Mapping)
            for attempt in attempt_accumulator.rejected_attempts
        ),
        unclassified_count=physical_count - eligible_count - rejected_count,
        missing_plan_count=missing_plan_count,
        semantic_outcome_without_physical=semantic_outcome_without_physical,
    )


def _capacity_reconciliation_status(
    attempt_accumulator: _CapacityAttemptAccumulator,
    attempt_totals: _CapacityAttemptTotals,
) -> tuple[bool, bool]:
    accounting_checks = (
        attempt_totals.planned_count
        == len(attempt_accumulator.observed_plan_ids)
        + attempt_totals.missing_plan_count,
        attempt_totals.physical_count
        == attempt_accumulator.initial_attempt_count
        + attempt_accumulator.retry_attempt_count
        + attempt_accumulator.invalid_attempt_index_count,
        attempt_totals.physical_count
        == attempt_totals.eligible_count + attempt_totals.rejected_count,
        attempt_totals.verified_count
        == attempt_totals.eligible_count + attempt_totals.verified_rejected_count,
        sum(attempt_accumulator.status_counts.values())
        == attempt_totals.physical_count,
        sum(attempt_accumulator.physical_by_cohort.values())
        == attempt_totals.physical_count,
        sum(attempt_accumulator.eligible_by_cohort.values())
        == attempt_totals.eligible_count,
        attempt_totals.unclassified_count == 0,
    )
    is_accounted = all(accounting_checks)
    is_release_clean = (
        is_accounted
        and not attempt_accumulator.structural_failure_counts
        and attempt_totals.planned_count
        == attempt_totals.physical_count
        == attempt_totals.verified_count
        == attempt_totals.eligible_count
        == attempt_accumulator.initial_attempt_count
        and attempt_totals.rejected_count == 0
        and attempt_accumulator.retry_attempt_count == 0
        and attempt_accumulator.timeout_count == 0
        and attempt_accumulator.transport_error_count == 0
        and attempt_accumulator.http_error_count == 0
        and attempt_accumulator.verification_error_count == 0
    )
    return is_accounted, is_release_clean


def _capacity_reconciliation(
    attempt_accumulator: _CapacityAttemptAccumulator,
    attempt_totals: _CapacityAttemptTotals,
) -> dict[str, Any]:
    is_accounted, is_release_clean = _capacity_reconciliation_status(
        attempt_accumulator, attempt_totals
    )
    return {
        "contract": "planned_first_attempt_outcomes_v1",
        "accounted": is_accounted,
        "release_clean": is_release_clean,
        "planned_first_physical_attempts": attempt_totals.planned_count,
        "plans_with_physical_attempt": len(attempt_accumulator.observed_plan_ids),
        "planned_without_physical_attempt": attempt_totals.missing_plan_count,
        "initial_physical_attempts": attempt_accumulator.initial_attempt_count,
        "retry_physical_attempts": attempt_accumulator.retry_attempt_count,
        "invalid_attempt_index_records": (
            attempt_accumulator.invalid_attempt_index_count
        ),
        "eligible_physical_attempts": attempt_totals.eligible_count,
        "rejected_physical_attempts": attempt_totals.rejected_count,
        "unclassified_physical_attempts": attempt_totals.unclassified_count,
        "verified_rejected_physical_attempts": (
            attempt_totals.verified_rejected_count
        ),
        "semantic_outcomes_without_physical_attempt": (
            attempt_totals.semantic_outcome_without_physical
        ),
        "structural_failures": dict(
            sorted(attempt_accumulator.structural_failure_counts.items())
        ),
    }


def _freeze_capacity_measurements(
    planned_by_cohort: collections.Counter[str],
    attempt_accumulator: _CapacityAttemptAccumulator,
    reconciliation_map: Mapping[str, Any],
) -> _CapacityReportMeasurements:
    return _CapacityReportMeasurements(
        planned_by_cohort=planned_by_cohort,
        physical_by_cohort=attempt_accumulator.physical_by_cohort,
        verified_by_cohort=attempt_accumulator.verified_by_cohort,
        eligible_by_cohort=attempt_accumulator.eligible_by_cohort,
        rejected_by_cohort=attempt_accumulator.rejected_by_cohort,
        status_counts=attempt_accumulator.status_counts,
        rejection_counts=attempt_accumulator.rejection_counts,
        verified_attempts=tuple(attempt_accumulator.verified_attempts),
        eligible_attempts=tuple(attempt_accumulator.eligible_attempts),
        rejected_attempts=tuple(attempt_accumulator.rejected_attempts),
        client_physical_latencies=tuple(
            attempt_accumulator.client_physical_latencies
        ),
        eligible_client_latencies=tuple(
            attempt_accumulator.eligible_client_latencies
        ),
        eligible_client_latencies_by_cohort={
            cohort: tuple(latencies)
            for cohort, latencies in (
                attempt_accumulator.eligible_client_latencies_by_cohort.items()
            )
        },
        verified_server_latencies=tuple(
            attempt_accumulator.verified_server_latencies
        ),
        eligible_server_latencies=tuple(
            attempt_accumulator.eligible_server_latencies
        ),
        eligible_server_latencies_by_cohort={
            cohort: tuple(latencies)
            for cohort, latencies in (
                attempt_accumulator.eligible_server_latencies_by_cohort.items()
            )
        },
        distinct_query_digests_by_cohort=(
            attempt_accumulator.distinct_query_digests_by_cohort
        ),
        reconciliation=reconciliation_map,
    )


def _capacity_report_measurements(
    plan_by_id: Mapping[int, CapacityObservationIntent],
    attempt_records: Sequence[CapacityHttpObservation],
    semantic_success_by_record_id: Mapping[int, bool],
) -> _CapacityReportMeasurements:
    """Reconcile every planned request with its physical attempt outcomes."""

    planned_by_cohort = collections.Counter(
        intent.cohort for intent in plan_by_id.values()
    )
    attempt_accumulator = _CapacityAttemptAccumulator()
    for attempt_record in attempt_records:
        _record_capacity_attempt(
            attempt_record,
            plan_by_id,
            semantic_success_by_record_id,
            attempt_accumulator,
        )
    missing_plan_count, semantic_outcome_without_physical = (
        _record_capacity_plan_gaps(
            plan_by_id,
            semantic_success_by_record_id,
            attempt_accumulator,
        )
    )
    attempt_totals = _capacity_attempt_totals(
        plan_by_id,
        attempt_records,
        attempt_accumulator,
        missing_plan_count,
        semantic_outcome_without_physical,
    )
    reconciliation_map = _capacity_reconciliation(
        attempt_accumulator,
        attempt_totals,
    )
    return _freeze_capacity_measurements(
        planned_by_cohort,
        attempt_accumulator,
        reconciliation_map,
    )


def _capacity_rejection_reasons(
    attempt_record: CapacityHttpObservation,
    semantic_success_by_record_id: Mapping[int, bool],
) -> list[str]:
    reasons = []
    if attempt_record.verification_error is not None:
        reasons.append(attempt_record.verification_error)
    signed_observation = attempt_record.observation
    if not isinstance(signed_observation, Mapping):
        reasons.append("api_signed_observation_missing")
    else:
        if signed_observation.get("cold") is not True:
            reasons.append("api_signed_cold_not_true")
        observation_ordinal = signed_observation.get("observation_ordinal")
        if (
            isinstance(observation_ordinal, bool)
            or not isinstance(observation_ordinal, int)
            or observation_ordinal != 0
        ):
            reasons.append("api_signed_observation_ordinal_not_zero")
    if not attempt_record.first_for_query:
        reasons.append("not_first_for_query")
    if attempt_record.attempt_index != 0:
        reasons.append("retry_attempt")
    if attempt_record.response_status_code != 200:
        reasons.append("status_not_200")
    if attempt_record.response_redirect_count != 0:
        reasons.append("redirect_observed")
    if semantic_success_by_record_id.get(attempt_record.record_id) is not True:
        reasons.append("semantic_audit_not_successful")
    return reasons


def _capacity_report_cohorts(
    measurements: _CapacityReportMeasurements,
) -> dict[str, dict[str, int]]:
    return {
        cohort: {
            "planned": measurements.planned_by_cohort[cohort],
            "physical": measurements.physical_by_cohort[cohort],
            "verified": measurements.verified_by_cohort[cohort],
            "eligible": measurements.eligible_by_cohort[cohort],
            "rejected": measurements.rejected_by_cohort[cohort],
            "distinct_semantic_queries": len(
                measurements.distinct_query_digests_by_cohort[cohort]
            ),
        }
        for cohort in sorted(CAPACITY_COHORTS)
    }


def _capacity_report_counters(
    plan_by_id: Mapping[int, CapacityObservationIntent],
    attempt_records: Sequence[CapacityHttpObservation],
    measurements: _CapacityReportMeasurements,
    cohort_report_by_name: Mapping[str, Mapping[str, int]],
) -> dict[str, Any]:
    distinct_query_digests = set().union(
        *measurements.distinct_query_digests_by_cohort.values()
    )
    return {
        "planned": len(plan_by_id),
        "physical": len(attempt_records),
        "verified": len(measurements.verified_attempts),
        "eligible": len(measurements.eligible_attempts),
        "rejected": len(measurements.rejected_attempts),
        "distinct_semantic_queries": len(distinct_query_digests),
        "retries": measurements.reconciliation["retry_physical_attempts"],
        "timeouts": sum(
            attempt.verification_error == "request_timeout"
            for attempt in attempt_records
        ),
        "transport_errors": sum(
            attempt.response_status_code is None for attempt in attempt_records
        ),
        "http_errors": sum(
            attempt.response_status_code is not None
            and attempt.response_status_code != 200
            for attempt in attempt_records
        ),
        "verification_errors": sum(
            attempt.verification_error is not None for attempt in attempt_records
        ),
        "redirects": sum(
            attempt.response_redirect_count for attempt in attempt_records
        ),
        "status": dict(sorted(measurements.status_counts.items())),
        "rejections": dict(sorted(measurements.rejection_counts.items())),
        "cohorts": dict(cohort_report_by_name),
        "reconciliation": dict(measurements.reconciliation),
    }


def _capacity_report_latency(
    measurements: _CapacityReportMeasurements,
) -> dict[str, Any]:
    eligible_client_latency_by_cohort = {
        cohort: _latency_gate_summary(
            measurements.eligible_client_latencies_by_cohort.get(cohort, ())
        )
        for cohort in sorted(CAPACITY_COHORTS)
    }
    eligible_server_latency_by_cohort = {
        cohort: _latency_gate_summary(
            measurements.eligible_server_latencies_by_cohort.get(cohort, ())
        )
        for cohort in sorted(CAPACITY_COHORTS)
    }
    return {
        "gate_source": CAPACITY_CLIENT_LATENCY_SOURCE,
        "client_physical_attempt_ms": latency_summary(
            measurements.client_physical_latencies
        ),
        "eligible_verified_client_http_duration_ms": latency_summary(
            measurements.eligible_client_latencies
        ),
        "eligible_verified_client_http_duration_ms_by_cohort": (
            eligible_client_latency_by_cohort
        ),
        "verified_signed_server_duration_ms": latency_summary(
            measurements.verified_server_latencies
        ),
        "eligible_signed_server_duration_ms": latency_summary(
            measurements.eligible_server_latencies
        ),
        "eligible_signed_server_duration_ms_by_cohort": (
            eligible_server_latency_by_cohort
        ),
        "signed_server_measurement_source": CAPACITY_SIGNED_LATENCY_SOURCE,
        "signed_server_duration_diagnostic_only": True,
    }


def _capacity_report_commitments(
    measurements: _CapacityReportMeasurements,
    commitment: Any,
) -> dict[str, Any]:
    verified_observations = [
        attempt.observation
        for attempt in measurements.verified_attempts
        if attempt.observation is not None
    ]
    eligible_observations = [
        attempt.observation
        for attempt in measurements.eligible_attempts
        if attempt.observation is not None
    ]
    run_digests = sorted(
        {str(observation["run_digest"]) for observation in verified_observations}
    )
    distinct_query_digests = set().union(
        *measurements.distinct_query_digests_by_cohort.values()
    )
    return {
        "run_digest": run_digests[0] if len(run_digests) == 1 else None,
        "run_digest_count": len(run_digests),
        "verified_observations_sha256": commitment(verified_observations),
        "eligible_observations_sha256": commitment(eligible_observations),
        "eligible_semantic_queries_sha256": sha256_text(
            canonical_json(sorted(distinct_query_digests))
        ),
    }


def _capacity_report_requirements() -> dict[str, int]:
    return {
        "min_matched_positive_eligible_signed_http_requests": (
            CAPACITY_MIN_MATCHED_POSITIVE_ELIGIBLE_HTTP_REQUESTS
        ),
        "min_negative_eligible_signed_http_requests": (
            CAPACITY_MIN_NEGATIVE_ELIGIBLE_HTTP_REQUESTS
        ),
        "min_random_eligible_signed_http_requests": (
            CAPACITY_MIN_RANDOM_ELIGIBLE_HTTP_REQUESTS
        ),
        "min_distinct_random_semantic_queries": (
            CAPACITY_MIN_DISTINCT_RANDOM_QUERIES
        ),
        "min_eligible_signed_http_requests": CAPACITY_MIN_ELIGIBLE_HTTP_REQUESTS,
    }


def _capacity_report_payload(
    trust: CapacityEvidenceTrust,
    plan_by_id: Mapping[int, CapacityObservationIntent],
    attempt_records: Sequence[CapacityHttpObservation],
    semantic_success_by_record_id: Mapping[int, bool],
    commitment: Any,
    sampling_seed_contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the redacted signed-capacity section of an audit report."""

    measurements = _capacity_report_measurements(
        plan_by_id,
        attempt_records,
        semantic_success_by_record_id,
    )
    cohort_report_by_name = _capacity_report_cohorts(measurements)
    return {
        "enabled": True,
        "contract": "signed_isolated_standard_http_capacity_v3",
        "trust": trust.report_value,
        "selection": dict(sampling_seed_contract),
        "page_contract": {
            "method": "GET",
            "path": capacity_evidence.CAPACITY_QUERY_PATH,
            "first_page_only": True,
            "offset": 0,
            "limit": capacity_evidence.CAPACITY_PAGE_LIMIT,
            "first_physical_attempt_only": True,
            "api_signed_cold": True,
            "api_signed_observation_ordinal": 0,
            "status": 200,
            "redirects": 0,
            "latency_gate": CAPACITY_CLIENT_LATENCY_SOURCE,
        },
        "counters": _capacity_report_counters(
            plan_by_id,
            attempt_records,
            measurements,
            cohort_report_by_name,
        ),
        "latency": _capacity_report_latency(measurements),
        "commitments": _capacity_report_commitments(measurements, commitment),
        "requirements": _capacity_report_requirements(),
    }


def disabled_capacity_evidence_report() -> dict[str, Any]:
    """Return the stable report shape for audits without signed evidence."""

    return {
        "enabled": False,
        "contract": "signed_isolated_standard_http_capacity_v3",
    }


def validate_capacity_evidence_preflight(
    *,
    distinct_random_queries: int,
    negative_queries: int,
    positive_queries: int,
) -> None:
    """Fail before standard HTTP when a run cannot meet evidence floors."""

    counts = (distinct_random_queries, negative_queries, positive_queries)
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in counts
    ):
        raise ConfigurationError("capacity_preflight_count_invalid")
    if positive_queries < CAPACITY_MIN_MATCHED_POSITIVE_ELIGIBLE_HTTP_REQUESTS:
        raise SourceCoverageError(
            "capacity_matched_positive_query_population_below_minimum"
        )
    if negative_queries < CAPACITY_MIN_NEGATIVE_ELIGIBLE_HTTP_REQUESTS:
        raise SourceCoverageError(
            "capacity_negative_query_population_below_minimum"
        )
    if distinct_random_queries < CAPACITY_MIN_DISTINCT_RANDOM_QUERIES:
        raise SourceCoverageError(
            "capacity_distinct_random_query_population_below_minimum"
        )
    if sum(counts) < CAPACITY_MIN_ELIGIBLE_HTTP_REQUESTS:
        raise SourceCoverageError("capacity_planned_http_requests_below_minimum")


def _capacity_client_latency_release_failures(
    report: Mapping[str, Any],
    counters: Mapping[str, Any],
    cohorts: Mapping[str, Any],
) -> list[str]:
    """Validate the API-qualified client end-to-end latency evidence shape."""

    latency = report.get("latency")
    page_contract = report.get("page_contract")
    if (
        not isinstance(latency, Mapping)
        or latency.get("gate_source") != CAPACITY_CLIENT_LATENCY_SOURCE
        or not isinstance(page_contract, Mapping)
        or page_contract.get("latency_gate") != CAPACITY_CLIENT_LATENCY_SOURCE
    ):
        return ["capacity_client_latency_gate_missing"]
    overall = latency.get("eligible_verified_client_http_duration_ms")
    by_cohort = latency.get(
        "eligible_verified_client_http_duration_ms_by_cohort"
    )
    if not isinstance(overall, Mapping) or not isinstance(by_cohort, Mapping):
        return ["capacity_client_latency_gate_missing"]
    if overall.get("count") != counters.get("eligible"):
        return ["capacity_client_latency_count_mismatch"]
    for cohort in CAPACITY_COHORTS:
        cohort_counts = cohorts.get(cohort)
        cohort_latency = by_cohort.get(cohort)
        gate_p95_ms = (
            cohort_latency.get("gate_p95_ms")
            if isinstance(cohort_latency, Mapping)
            else None
        )
        if (
            not isinstance(cohort_counts, Mapping)
            or not isinstance(cohort_latency, Mapping)
            or cohort_latency.get("count") != cohort_counts.get("eligible")
            or isinstance(gate_p95_ms, bool)
            or not isinstance(gate_p95_ms, (int, float))
            or not math.isfinite(float(gate_p95_ms))
            or float(gate_p95_ms) < 0
        ):
            return ["capacity_client_latency_cohort_invalid"]
    return []


def _is_complete_request_accounting_invalid(
    report: Mapping[str, Any], counters: Mapping[str, Any]
) -> bool:
    complete_accounting = report.get("complete_request_accounting")
    physical_attempt_count = counters.get("physical")
    return (
        not isinstance(complete_accounting, Mapping)
        or complete_accounting.get("contract")
        != "all_standard_http_attempts_v1"
        or isinstance(complete_accounting.get("actual_http_requests"), bool)
        or not isinstance(complete_accounting.get("actual_http_requests"), int)
        or isinstance(physical_attempt_count, bool)
        or not isinstance(physical_attempt_count, int)
        or complete_accounting.get("actual_http_requests", -1)
        < physical_attempt_count
        or complete_accounting.get("retries") != 0
        or complete_accounting.get("release_clean") is not True
    )


def _capacity_cohort_floor_failures(
    cohorts: Mapping[str, Any],
) -> list[str]:
    cohort_requirements = (
        (
            CAPACITY_POSITIVE_COHORT,
            CAPACITY_MIN_MATCHED_POSITIVE_ELIGIBLE_HTTP_REQUESTS,
            "capacity_matched_positive_eligible_requests_below_minimum",
        ),
        (
            CAPACITY_NEGATIVE_COHORT,
            CAPACITY_MIN_NEGATIVE_ELIGIBLE_HTTP_REQUESTS,
            "capacity_negative_eligible_requests_below_minimum",
        ),
        (
            CAPACITY_RANDOM_COHORT,
            CAPACITY_MIN_RANDOM_ELIGIBLE_HTTP_REQUESTS,
            "capacity_random_eligible_requests_below_minimum",
        ),
    )
    failures: list[str] = []
    for cohort, minimum, reason in cohort_requirements:
        cohort_report = cohorts.get(cohort)
        cohort_eligible = (
            cohort_report.get("eligible")
            if isinstance(cohort_report, Mapping)
            else None
        )
        if (
            isinstance(cohort_eligible, bool)
            or not isinstance(cohort_eligible, int)
            or cohort_eligible < minimum
        ):
            failures.append(reason)
    return failures


def _capacity_volume_release_failures(
    counters: Mapping[str, Any], cohorts: Mapping[str, Any]
) -> list[str]:
    failures: list[str] = []
    eligible_count = counters.get("eligible")
    if (
        isinstance(eligible_count, bool)
        or not isinstance(eligible_count, int)
        or eligible_count < CAPACITY_MIN_ELIGIBLE_HTTP_REQUESTS
    ):
        failures.append("capacity_eligible_http_requests_below_minimum")
    failures.extend(_capacity_cohort_floor_failures(cohorts))
    random_cohort = cohorts.get(CAPACITY_RANDOM_COHORT)
    distinct_random_queries = (
        random_cohort.get("distinct_semantic_queries")
        if isinstance(random_cohort, Mapping)
        else None
    )
    if (
        isinstance(distinct_random_queries, bool)
        or not isinstance(distinct_random_queries, int)
        or distinct_random_queries < CAPACITY_MIN_DISTINCT_RANDOM_QUERIES
    ):
        failures.append("capacity_distinct_random_queries_below_minimum")
    return failures


def _validated_capacity_counter_map(
    counters: Mapping[str, Any],
) -> dict[str, int] | None:
    strict_counter_names = (
        "planned",
        "physical",
        "verified",
        "eligible",
        "rejected",
        "retries",
        "timeouts",
        "transport_errors",
        "http_errors",
        "verification_errors",
        "redirects",
    )
    strict_count_map = {
        name: counters.get(name) for name in strict_counter_names
    }
    if any(
        isinstance(count, bool) or not isinstance(count, int) or count < 0
        for count in strict_count_map.values()
    ):
        return None
    return strict_count_map


def _has_valid_capacity_reconciliation(
    reconciliation_map: Any,
    strict_count_map: Mapping[str, int],
) -> bool:
    if not (
        isinstance(reconciliation_map, Mapping)
        and reconciliation_map.get("contract")
        == "planned_first_attempt_outcomes_v1"
        and reconciliation_map.get("accounted") is True
        and reconciliation_map.get("release_clean") is True
        and reconciliation_map.get("structural_failures") == {}
    ):
        return False
    return all(
        (
            reconciliation_map.get("planned_first_physical_attempts")
            == strict_count_map["planned"],
            reconciliation_map.get("plans_with_physical_attempt")
            == strict_count_map["planned"],
            reconciliation_map.get("initial_physical_attempts")
            == strict_count_map["planned"],
            reconciliation_map.get("retry_physical_attempts")
            == strict_count_map["retries"],
            reconciliation_map.get("eligible_physical_attempts")
            == strict_count_map["eligible"],
            reconciliation_map.get("rejected_physical_attempts")
            == strict_count_map["rejected"],
            reconciliation_map.get("planned_without_physical_attempt") == 0,
            reconciliation_map.get("invalid_attempt_index_records") == 0,
            reconciliation_map.get("unclassified_physical_attempts") == 0,
            reconciliation_map.get("verified_rejected_physical_attempts") == 0,
            reconciliation_map.get("semantic_outcomes_without_physical_attempt")
            == 0,
        )
    )


def _has_clean_capacity_top_level_counts(
    counters: Mapping[str, Any], strict_count_map: Mapping[str, int]
) -> bool:
    status_counts = counters.get("status")
    rejection_counts = counters.get("rejections")
    return (
        strict_count_map["planned"]
        == strict_count_map["physical"]
        == strict_count_map["verified"]
        == strict_count_map["eligible"]
        and strict_count_map["rejected"] == 0
        and strict_count_map["retries"] == 0
        and strict_count_map["timeouts"] == 0
        and strict_count_map["transport_errors"] == 0
        and strict_count_map["http_errors"] == 0
        and strict_count_map["verification_errors"] == 0
        and strict_count_map["redirects"] == 0
        and isinstance(status_counts, Mapping)
        and dict(status_counts) == {"200": strict_count_map["physical"]}
        and isinstance(rejection_counts, Mapping)
        and not rejection_counts
    )


def _has_clean_capacity_cohort_counts(
    cohorts: Mapping[str, Any], strict_count_map: Mapping[str, int]
) -> bool:
    cohort_totals = collections.Counter()
    for cohort in CAPACITY_COHORTS:
        cohort_report = cohorts.get(cohort)
        if not isinstance(cohort_report, Mapping):
            return False
        cohort_counts = [
            cohort_report.get(name)
            for name in ("planned", "physical", "verified", "eligible", "rejected")
        ]
        if (
            any(
                isinstance(cohort_count, bool)
                or not isinstance(cohort_count, int)
                or cohort_count < 0
                for cohort_count in cohort_counts
            )
            or not (
                cohort_counts[0]
                == cohort_counts[1]
                == cohort_counts[2]
                == cohort_counts[3]
            )
            or cohort_counts[4] != 0
        ):
            return False
        for name, count in zip(
            ("planned", "physical", "verified", "eligible", "rejected"),
            cohort_counts,
            strict=True,
        ):
            cohort_totals[name] += count
    return all(
        cohort_totals[name] == strict_count_map[name]
        for name in ("planned", "physical", "verified", "eligible", "rejected")
    )


def _has_clean_capacity_reconciliation(
    counters: Mapping[str, Any], cohorts: Mapping[str, Any]
) -> bool:
    strict_count_map = _validated_capacity_counter_map(counters)
    if strict_count_map is None:
        return False
    return (
        _has_valid_capacity_reconciliation(
            counters.get("reconciliation"), strict_count_map
        )
        and _has_clean_capacity_top_level_counts(counters, strict_count_map)
        and _has_clean_capacity_cohort_counts(cohorts, strict_count_map)
    )


def _capacity_release_seed_failure(
    report: Mapping[str, Any], require_release_seed: bool
) -> str | None:
    if not require_release_seed:
        return None
    try:
        expected_selection = _release_seed_evidence_from_trust_report(
            report.get("trust")
        )
    except (AuditError, TypeError, ValueError):
        expected_selection = None
    selection = report.get("selection")
    if (
        expected_selection is None
        or not isinstance(selection, Mapping)
        or dict(selection) != expected_selection
    ):
        return "capacity_release_seed_commitment_invalid"
    return None


def _capacity_run_commitment_failure(report: Mapping[str, Any]) -> str | None:
    commitments = report.get("commitments")
    run_digest = (
        commitments.get("run_digest")
        if isinstance(commitments, Mapping)
        else None
    )
    run_digest_count = (
        commitments.get("run_digest_count")
        if isinstance(commitments, Mapping)
        else None
    )
    if (
        isinstance(run_digest_count, bool)
        or run_digest_count != 1
        or not isinstance(run_digest, str)
        or len(run_digest) != 64
        or run_digest == "0" * 64
        or any(character not in "0123456789abcdef" for character in run_digest)
    ):
        return "capacity_run_commitment_invalid"
    return None


def capacity_evidence_release_failures(
    report: Mapping[str, Any],
    *,
    require_release_seed: bool = True,
) -> tuple[str, ...]:
    """Return fail-closed release reasons for one aggregate evidence report."""

    if report.get("enabled") is not True:
        return ("capacity_evidence_disabled",)
    counters = report.get("counters")
    if not isinstance(counters, Mapping):
        return ("capacity_evidence_counters_missing",)
    cohorts_value = counters.get("cohorts")
    cohorts = cohorts_value if isinstance(cohorts_value, Mapping) else {}
    failures: list[str] = []
    if report.get("contract") != "signed_isolated_standard_http_capacity_v3":
        failures.append("capacity_evidence_contract_invalid")
    failures.extend(
        _capacity_client_latency_release_failures(report, counters, cohorts)
    )
    if _is_complete_request_accounting_invalid(report, counters):
        failures.append("capacity_complete_request_accounting_failed")
    failures.extend(_capacity_volume_release_failures(counters, cohorts))

    if not _has_clean_capacity_reconciliation(counters, cohorts):
        failures.append("capacity_request_reconciliation_failed")

    seed_failure = _capacity_release_seed_failure(report, require_release_seed)
    if seed_failure is not None:
        failures.append(seed_failure)
    commitment_failure = _capacity_run_commitment_failure(report)
    if commitment_failure is not None:
        failures.append(commitment_failure)
    return tuple(failures)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return canonical_decimal(value)
    if isinstance(value, Path):
        raise TypeError("paths must not be serialized into audit reports")
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    """Serialize a value with the audit's deterministic JSON representation."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=_json_default,
    )


def sha256_text(value: str) -> str:
    """Return the lowercase SHA-256 digest for UTF-8 text."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _is_nonzero_lower_hex_digest(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and value != "0" * 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _capacity_seed_authority_payload(
    trust_report: Mapping[str, Any],
) -> dict[str, str]:
    """Return the closed pinned-trust coordinates that authorize a release seed."""

    required_fields = {
        "api_evidence_key_id",
        "api_evidence_public_key_sha256",
        "release_digest",
        "environment_id",
    }
    if not isinstance(trust_report, Mapping) or set(trust_report) != required_fields:
        raise ConfigurationError("capacity_seed_authority_invalid")
    key_id = trust_report.get("api_evidence_key_id")
    if (
        not isinstance(key_id, str)
        or not key_id
        or len(key_id) > 64
        or not key_id[0].isascii()
        or not key_id[0].isalnum()
        or any(
            character
            not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
            for character in key_id
        )
    ):
        raise ConfigurationError("capacity_seed_authority_invalid")
    for field_name in required_fields - {"api_evidence_key_id"}:
        if not _is_nonzero_lower_hex_digest(trust_report.get(field_name)):
            raise ConfigurationError("capacity_seed_authority_invalid")
    return {
        field_name: str(trust_report[field_name])
        for field_name in sorted(required_fields)
    }


def _capacity_authoritative_release_seed(
    trust_report: Mapping[str, Any],
) -> str:
    authority_payload = _capacity_seed_authority_payload(trust_report)
    return hashlib.sha256(
        RELEASE_CAPACITY_SEED_DOMAIN
        + canonical_json(authority_payload).encode("ascii")
    ).hexdigest()


def authoritative_release_seed(
    trust: CapacityEvidenceTrust | None,
) -> str:
    """Resolve the only accepted seed for one release authority."""

    if trust is None:
        return DEFAULT_SEED
    return _capacity_authoritative_release_seed(trust.report_value)


def _release_seed_evidence_from_trust_report(
    trust_report: Mapping[str, Any],
) -> dict[str, Any]:
    authority_payload = _capacity_seed_authority_payload(trust_report)
    seed = _capacity_authoritative_release_seed(authority_payload)
    return {
        "contract": RELEASE_CAPACITY_SEED_CONTRACT,
        "precommitted": True,
        "authority": "pinned_capacity_evidence_trust",
        "authority_sha256": sha256_text(canonical_json(authority_payload)),
        "seed_sha256": sha256_text(seed),
    }


def sampling_seed_evidence(
    profile: str,
    seed: str,
    trust: CapacityEvidenceTrust | None,
) -> dict[str, Any]:
    """Describe the deterministic seed contract without exposing the seed."""

    if not isinstance(seed, str) or not seed or seed != seed.strip():
        raise ConfigurationError("seed_must_be_nonempty")
    if profile == "release":
        expected_seed = authoritative_release_seed(trust)
        if seed != expected_seed:
            raise ConfigurationError("release_seed_must_match_authority")
        if trust is not None:
            return _release_seed_evidence_from_trust_report(trust.report_value)
        authority_payload_map = {
            "contract": RELEASE_FIXED_SEED_CONTRACT,
            "fixed_seed_sha256": sha256_text(DEFAULT_SEED),
        }
        return {
            "contract": RELEASE_FIXED_SEED_CONTRACT,
            "precommitted": True,
            "authority": "versioned_audit_harness_constant",
            "authority_sha256": sha256_text(
                canonical_json(authority_payload_map)
            ),
            "seed_sha256": sha256_text(seed),
        }
    if profile != "diagnostic":
        raise ConfigurationError("unknown_audit_profile")
    return {
        "contract": DIAGNOSTIC_SEED_CONTRACT,
        "precommitted": False,
        "authority": "diagnostic_caller",
        "authority_sha256": None,
        "seed_sha256": sha256_text(seed),
    }


def fingerprint(value: str, *, length: int = 24) -> str:
    """Return a shortened deterministic digest suitable for redacted reports."""

    return sha256_text(value)[:length]


def canonical_scalar(value: Any) -> str | None:
    """Normalize a supported scalar to stripped text or null."""

    if value is None:
        return None
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, (str, int, Decimal)):
        text = str(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            return None
        text = str(value)
    else:
        return None
    text = text.strip()
    if not text:
        return None
    if len(text) > MAX_CANONICAL_CHARS:
        raise SourceCoverageError("scalar_exceeds_canonical_limit")
    return text


def canonical_source_metadata_text(value: Any) -> str | None:
    """Trim source metadata while preserving the distinction between null and empty."""

    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("source_metadata_must_be_string_or_null")
    text = value.strip()
    if len(text) > MAX_CANONICAL_CHARS:
        raise SourceCoverageError("source_metadata_exceeds_canonical_limit")
    return text


def canonical_code(value: Any) -> str | None:
    """Normalize a scalar code to uppercase text or null."""

    text = canonical_scalar(value)
    return text.upper() if text is not None else None


def canonical_code_system(value: Any) -> str | None:
    """Normalize a code-system name and apply serving-compatible aliases."""

    code = canonical_code(value)
    return CODE_SYSTEM_ALIASES.get(code, code) if code else None


def canonical_catalog_code(code_system: str | None, value: Any) -> str | None:
    """Normalize a catalog code using the width rules for its code system."""

    code = canonical_code(value)
    if not code:
        return None
    digits = "".join(character for character in code if character.isdigit())
    if code_system == "RC" and digits:
        return digits.zfill(4)
    if code_system == "POS" and digits:
        return digits.zfill(2)
    if code_system == "MS_DRG" and digits:
        return digits.zfill(3)
    if code_system in {"ICD10CM", "ICD10PCS"}:
        return code.replace(".", "")
    return code


def canonical_decimal(raw_decimal: Any) -> str:
    """Normalize a finite decimal without exponent or insignificant zeroes."""

    if raw_decimal is None or isinstance(raw_decimal, bool):
        raise ValueError("missing_or_boolean_decimal")
    text = canonical_scalar(raw_decimal)
    if text is None:
        raise ValueError("missing_decimal")
    try:
        number = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError("invalid_decimal") from exc
    if not number.is_finite():
        raise ValueError("non_finite_decimal")
    if number.is_zero():
        return "0"
    sign, digits, exponent = number.as_tuple()
    digit_count = max(len(digits), 1)
    if exponent >= 0:
        output_length = digit_count + exponent + sign
    else:
        integer_digits = digit_count + exponent
        output_length = digit_count + 2 + max(-integer_digits, 0) + sign
    if output_length > MAX_CANONICAL_CHARS:
        raise ValueError("decimal_exceeds_canonical_limit")
    expanded = format(number, "f")
    if "." in expanded:
        expanded = expanded.rstrip("0").rstrip(".")
    is_negative = expanded.startswith("-")
    unsigned = expanded[1:] if is_negative else expanded
    integer, dot, fraction = unsigned.partition(".")
    integer = integer.lstrip("0") or "0"
    expanded = integer + (dot + fraction if dot else "")
    return "-" + expanded if is_negative and expanded != "0" else expanded


def canonical_date(value: Any) -> str | None:
    """Validate and return a canonical ISO calendar date or null."""

    text = canonical_scalar(value)
    if text is None:
        return None
    try:
        parsed = dt.date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("invalid_iso_date") from exc
    if parsed.isoformat() != text:
        raise ValueError("noncanonical_iso_date")
    return parsed.isoformat()


def canonical_npi(value: Any) -> int | None:
    """Return a canonical ten-digit NPI, or null when the value is invalid."""

    if value is None or isinstance(value, bool):
        return None
    text = canonical_scalar(value)
    if text is None:
        return None
    try:
        decimal_text = canonical_decimal(text)
    except ValueError:
        return None
    if "." in decimal_text:
        return None
    try:
        npi = int(decimal_text)
    except ValueError:
        return None
    return npi if 1_000_000_000 <= npi <= 9_999_999_999 else None


def strict_source_integer(event: str, value: Any) -> int | None:
    """Accept only integral JSON number events from the source event stream."""

    if event != "number" or isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        return None
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not number.is_finite() or number != number.to_integral_value():
        return None
    return int(number)


def strict_source_npi(event: str, value: Any) -> int | None:
    """Accept only ten-digit NPIs encoded as source JSON number events."""

    integer = strict_source_integer(event, value)
    if integer is None:
        return None
    return integer if 1_000_000_000 <= integer <= 9_999_999_999 else None


def provider_identifier_quarantine_payload(
    counts: Mapping[int, int],
) -> dict[str, Any]:
    """Return redacted exact evidence for malformed integer NPI occurrences."""

    if len(counts) > 1_024:
        raise ValueError("provider identifier quarantine exceeds 1024 distinct values")
    digest = hashlib.sha256(PROVIDER_IDENTIFIER_QUARANTINE_HASH_DOMAIN)
    entries: list[dict[str, Any]] = []
    occurrence_count = 0
    for value, count in sorted(counts.items()):
        if value == 0 or 1_000_000_000 <= value <= 9_999_999_999:
            raise ValueError("provider identifier quarantine contains a non-malformed value")
        if count <= 0 or count >= 2**64:
            raise ValueError("provider identifier quarantine count is invalid")
        occurrence_count += count
        if occurrence_count >= 2**64:
            raise ValueError("provider identifier quarantine count overflows uint64")
        digest.update(str(value).encode("ascii"))
        digest.update(b"\0")
        digest.update(int(count).to_bytes(8, "big"))
        entries.append({"value": str(value), "occurrence_count": count})
    return {
        "contract": PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT,
        "occurrence_count": occurrence_count,
        "distinct_value_count": len(entries),
        "entries": entries,
        "sha256": digest.hexdigest(),
    }


def canonical_list(
    values: Any,
    *,
    uppercase: bool = False,
    code_system: str | None = None,
) -> list[str]:
    """Normalize a scalar-or-list field into a sorted unique string list."""

    if values is None:
        raw_values: list[Any] = []
    elif isinstance(values, (list, tuple, set)):
        raw_values = list(values)
    else:
        raw_values = [values]
    canonical_values: set[str] = set()
    for raw_value in raw_values:
        value = (
            canonical_catalog_code(code_system, raw_value)
            if code_system
            else canonical_code(raw_value)
            if uppercase
            else canonical_scalar(raw_value)
        )
        if value:
            canonical_values.add(value)
    return sorted(canonical_values)


def canonical_modifier_list(values: Any) -> list[str]:
    """Normalize modifier arrays, including payer-packed comma-separated entries."""

    if values is None:
        raw_values: list[Any] = []
    elif isinstance(values, (list, tuple, set)):
        raw_values = list(values)
    else:
        raw_values = [values]
    split_values: list[Any] = []
    for raw_value in raw_values:
        if isinstance(raw_value, str):
            split_values.extend(raw_value.split(","))
        else:
            split_values.append(raw_value)
    return canonical_list(split_values, uppercase=True)


def canonical_price_payload(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Return the canonical serving comparison fields for one source price."""

    return {
        "negotiated_type": canonical_scalar(raw.get("negotiated_type")),
        "negotiated_rate": canonical_decimal(raw.get("negotiated_rate")),
        "expiration_date": canonical_date(raw.get("expiration_date")),
        "service_code": canonical_list(raw.get("service_code"), code_system="POS"),
        "billing_class": canonical_scalar(raw.get("billing_class")),
        "setting": canonical_scalar(raw.get("setting")),
        "billing_code_modifier": canonical_modifier_list(raw.get("billing_code_modifier")),
        "additional_information": canonical_scalar(raw.get("additional_information")),
    }


@dataclass(frozen=True, order=True)
class QueryKey:
    code_system: str
    code: str
    npi: int

    @property
    def stable_key(self) -> str:
        """Return the deterministic key used for query grouping and sampling."""

        return canonical_json(dataclasses.asdict(self))

    @property
    def report_value(self) -> dict[str, Any]:
        """Return report-safe fingerprints for this query identity."""

        return {
            "fingerprint": fingerprint(self.stable_key),
            "code_system_sha256": sha256_text(self.code_system),
            "code_sha256": sha256_text(self.code),
            "npi_sha256": sha256_text(str(self.npi)),
        }


@dataclass(frozen=True)
class CanonicalTuple:
    code_system: str
    code: str
    npi: int
    negotiation_arrangement: str | None
    billing_code_type_version: str | None
    name: str | None
    description: str | None
    network_names: tuple[str, ...]
    negotiated_type: str | None
    negotiated_rate: str
    expiration_date: str | None
    service_code: tuple[str, ...]
    billing_class: str | None
    setting: str | None
    billing_code_modifier: tuple[str, ...]
    additional_information: str | None

    @classmethod
    def from_parts(
        cls,
        query: QueryKey,
        arrangement: Any,
        price: Mapping[str, Any],
        *,
        billing_code_type_version: Any = None,
        name: Any = None,
        description: Any = None,
        network_names: Any = None,
    ) -> "CanonicalTuple":
        """Build a canonical tuple from a query, arrangement, and price."""

        normalized = canonical_price_payload(price)
        return cls(
            code_system=query.code_system,
            code=query.code,
            npi=query.npi,
            negotiation_arrangement=canonical_code(arrangement),
            billing_code_type_version=canonical_source_metadata_text(
                billing_code_type_version
            ),
            name=canonical_source_metadata_text(name),
            description=canonical_source_metadata_text(description),
            network_names=tuple(canonical_list(network_names)),
            negotiated_type=normalized["negotiated_type"],
            negotiated_rate=normalized["negotiated_rate"],
            expiration_date=normalized["expiration_date"],
            service_code=tuple(normalized["service_code"]),
            billing_class=normalized["billing_class"],
            setting=normalized["setting"],
            billing_code_modifier=tuple(normalized["billing_code_modifier"]),
            additional_information=normalized["additional_information"],
        )

    @property
    def payload(self) -> dict[str, Any]:
        """Return the JSON-compatible canonical tuple payload."""

        value = dataclasses.asdict(self)
        value["network_names"] = list(self.network_names)
        value["service_code"] = list(self.service_code)
        value["billing_code_modifier"] = list(self.billing_code_modifier)
        return value

    @property
    def stable_key(self) -> str:
        """Return the deterministic JSON key for this canonical tuple."""

        return canonical_json(self.payload)

    @property
    def query(self) -> QueryKey:
        """Return the code-and-provider identity represented by this tuple."""

        return QueryKey(self.code_system, self.code, self.npi)


def canonical_occurrence_key(
    canonical_tuple: CanonicalTuple,
    raw_container_sha256: str,
) -> str:
    """Bind a canonical tuple to its exact raw source container digest."""

    payload = canonical_tuple.payload
    payload["raw_container_sha256"] = raw_container_sha256
    return canonical_json(payload)


@dataclass(frozen=True)
class SourceIdentity:
    source_artifact_key: int
    source_key: str | None
    source_type: str
    identity_kind: str
    identity_sha256: str
    raw_container_sha256: str
    logical_json_sha256: str | None
    logical_hash_deferred: bool
    source_trace_set_hash: str
    source_trace_key: str

    @property
    def mapping_key(self) -> str:
        """Return the identity mapping key while excluding its optional label."""

        value = dataclasses.asdict(self)
        del value["source_key"]
        return canonical_json(value)


class SourceIdentityRegistry:
    """Validate exact, report-wide source attribution without exposing metadata."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._identity_by_artifact_key: dict[int, str] = {}
        self._logical_source_key_by_artifact_key: dict[int, str] = {}
        self._artifact_key_by_raw_sha256: dict[str, int] = {}
        self._artifact_key_by_trace_set_hash: dict[str, int] = {}
        self._artifact_key_by_trace_key: dict[str, int] = {}

    def register(self, identity: SourceIdentity) -> None:
        """Register one identity and reject any report-wide mapping conflict."""

        with self._lock:
            artifact_key = identity.source_artifact_key
            existing_identity = self._identity_by_artifact_key.get(artifact_key)
            if existing_identity is not None and existing_identity != identity.mapping_key:
                raise ApiSchemaError("source_artifact_key_mapping_changed_within_report")
            if identity.source_key is not None:
                existing_logical_key = self._logical_source_key_by_artifact_key.get(
                    artifact_key
                )
                if (
                    existing_logical_key is not None
                    and existing_logical_key != identity.source_key
                ):
                    raise ApiSchemaError(
                        "source_artifact_key_logical_source_key_changed_within_report"
                    )
            for identity_index, mapping_value, reason in (
                (
                    self._artifact_key_by_raw_sha256,
                    identity.raw_container_sha256,
                    "raw_container_mapped_to_multiple_source_artifact_keys",
                ),
                (
                    self._artifact_key_by_trace_set_hash,
                    identity.source_trace_set_hash,
                    "source_trace_set_mapped_to_multiple_source_artifact_keys",
                ),
                (
                    self._artifact_key_by_trace_key,
                    identity.source_trace_key,
                    "source_trace_union_mapped_to_multiple_source_artifact_keys",
                ),
            ):
                existing_artifact_key = identity_index.get(mapping_value)
                if existing_artifact_key is not None and existing_artifact_key != artifact_key:
                    raise ApiSchemaError(reason)
            self._identity_by_artifact_key[artifact_key] = identity.mapping_key
            if identity.source_key is not None:
                self._logical_source_key_by_artifact_key[artifact_key] = identity.source_key
            self._artifact_key_by_raw_sha256[identity.raw_container_sha256] = artifact_key
            self._artifact_key_by_trace_set_hash[identity.source_trace_set_hash] = artifact_key
            self._artifact_key_by_trace_key[identity.source_trace_key] = artifact_key


T = TypeVar("T")


class BottomKSampler(Generic[T]):
    """Order-independent deterministic bottom-k sampler with bounded memory."""

    def __init__(self, capacity: int, *, seed: str, namespace: str):
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.seed = seed
        self.namespace = namespace
        self._selected: dict[str, tuple[int, T]] = {}
        self._max_heap: list[tuple[int, str]] = []

    def _rank(self, stable_key: str) -> int:
        digest = hashlib.sha256(
            self.seed.encode("utf-8")
            + b"\x00"
            + self.namespace.encode("utf-8")
            + b"\x00"
            + stable_key.encode("utf-8")
        ).digest()
        return int.from_bytes(digest, "big")

    def offer(self, stable_key: str, value: T) -> None:
        """Consider a uniquely keyed value for the deterministic bottom-k set."""

        if stable_key in self._selected:
            return
        rank = self._rank(stable_key)
        if len(self._selected) >= self.capacity:
            worst_rank = -self._max_heap[0][0]
            if rank >= worst_rank:
                return
            _negative_rank, evicted_key = heapq.heappop(self._max_heap)
            del self._selected[evicted_key]
        self._selected[stable_key] = (rank, value)
        heapq.heappush(self._max_heap, (-rank, stable_key))

    def values(self) -> list[T]:
        """Return selected values ordered by deterministic sample rank."""

        return [value for _rank, value in sorted(self._selected.values(), key=lambda item: item[0])]

    def __len__(self) -> int:
        return len(self._selected)


@dataclass(frozen=True)
class SourceSpec:
    path: Path
    content_sha256: str
    stored_bytes: int
    encoding: str
    file_id: int

    @property
    def source_id(self) -> str:
        """Return the content-derived source identity."""

        return self.content_sha256


@dataclass(frozen=True)
class SourceSetEvidence:
    source_count: int
    raw_container_sha256_digest: str
    contract: str = SOURCE_SET_CONTRACT

    @property
    def payload(self) -> dict[str, Any]:
        """Return the serializable source-set seal payload."""

        return {
            "contract": self.contract,
            "source_count": self.source_count,
            "raw_container_sha256_digest": self.raw_container_sha256_digest,
        }


def source_set_evidence(raw_container_sha256_values: Sequence[str]) -> SourceSetEvidence:
    """Return the canonical composite identity for an exact raw source set."""

    raw_hashes = sorted(raw_container_sha256_values)
    if not raw_hashes:
        raise SourceCoverageError("source_set_is_empty")
    if len(raw_hashes) != len(set(raw_hashes)):
        raise SourceCoverageError("duplicate_source_content")
    if any(
        len(raw_hash) != 64
        or any(character not in "0123456789abcdef" for character in raw_hash)
        for raw_hash in raw_hashes
    ):
        raise SourceCoverageError("source_set_contains_invalid_sha256")
    digest = hashlib.sha256()
    for raw_hash in raw_hashes:
        digest.update(bytes.fromhex(raw_hash))
    return SourceSetEvidence(len(raw_hashes), digest.hexdigest())


def source_set_evidence_from_specs(specs: Sequence[SourceSpec]) -> SourceSetEvidence:
    """Derive canonical source-set evidence from source specifications."""

    return source_set_evidence([spec.content_sha256 for spec in specs])


@dataclass(frozen=True)
class SourceOccurrence:
    occurrence_id: str
    query: QueryKey
    tuple_key: str

    @property
    def stable_key(self) -> str:
        """Return the persisted occurrence identity used for sampling."""

        return self.occurrence_id


def source_specs(paths: Sequence[Path]) -> list[SourceSpec]:
    """Inspect source files and assign stable IDs in content-digest order."""

    if not paths:
        raise ConfigurationError("at_least_one_source_required")
    hashed_sources: list[tuple[str, int, str, Path]] = []
    for path in paths:
        if not path.is_file():
            raise ConfigurationError("source_not_regular_file")
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as source_stream:
            magic = source_stream.read(2)
            digest.update(magic)
            size += len(magic)
            for chunk in iter(lambda: source_stream.read(1024 * 1024), b""):
                digest.update(chunk)
                size += len(chunk)
        encoding = "gzip" if magic == b"\x1f\x8b" else "json"
        hashed_sources.append((digest.hexdigest(), size, encoding, path))
    hashed_sources.sort(key=lambda source_entry: source_entry[0])
    digests = [source_entry[0] for source_entry in hashed_sources]
    if len(digests) != len(set(digests)):
        raise SourceCoverageError("duplicate_source_content")
    return [
        SourceSpec(
            path=path,
            content_sha256=digest,
            stored_bytes=size,
            encoding=encoding,
            file_id=index + 1,
        )
        for index, (digest, size, encoding, path) in enumerate(hashed_sources)
    ]


def verify_sources_unchanged(specs: Sequence[SourceSpec]) -> None:
    """Reject source files whose content, size, or encoding changed mid-audit."""

    current_by_path = {spec.path: spec for spec in source_specs([spec.path for spec in specs])}
    for original in specs:
        current = current_by_path.get(original.path)
        if (
            current is None
            or current.content_sha256 != original.content_sha256
            or current.stored_bytes != original.stored_bytes
            or current.encoding != original.encoding
        ):
            raise SourceCoverageError("source_changed_during_indexing")


@contextlib.contextmanager
def open_source_stream(spec: SourceSpec) -> Iterator[Any]:
    """Open a source according to its detected magic bytes, not its suffix."""

    if spec.encoding == "gzip":
        with gzip.open(spec.path, "rb") as source:
            yield source
        return
    with spec.path.open("rb") as source:
        yield source


SOURCE_INDEX_SCHEMA = """
CREATE TABLE provider_ref_pending (
    file_id INTEGER NOT NULL,
    ref_ordinal INTEGER NOT NULL,
    npi INTEGER NOT NULL,
    PRIMARY KEY (file_id, ref_ordinal, npi)
) WITHOUT ROWID;
CREATE TABLE provider_ref_tin_pending (
    file_id INTEGER NOT NULL,
    ref_ordinal INTEGER NOT NULL,
    PRIMARY KEY (file_id, ref_ordinal)
) WITHOUT ROWID;
CREATE TABLE provider_ref_network_name_pending (
    file_id INTEGER NOT NULL,
    ref_ordinal INTEGER NOT NULL,
    network_name TEXT NOT NULL,
    PRIMARY KEY (file_id, ref_ordinal, network_name)
) WITHOUT ROWID;
CREATE TABLE provider_ref_npi (
    file_id INTEGER NOT NULL,
    ref_id TEXT NOT NULL,
    npi INTEGER NOT NULL,
    PRIMARY KEY (file_id, ref_id, npi)
) WITHOUT ROWID;
CREATE TABLE provider_ref_network_name (
    file_id INTEGER NOT NULL,
    ref_id TEXT NOT NULL,
    network_name TEXT NOT NULL,
    PRIMARY KEY (file_id, ref_id, network_name)
) WITHOUT ROWID;
CREATE TABLE provider_ref_identity (
    file_id INTEGER NOT NULL,
    ref_id TEXT NOT NULL,
    PRIMARY KEY (file_id, ref_id)
) WITHOUT ROWID;
CREATE TABLE provider_ref_tin_marker (
    file_id INTEGER NOT NULL,
    ref_id TEXT NOT NULL,
    PRIMARY KEY (file_id, ref_id)
) WITHOUT ROWID;
CREATE TABLE source_rate (
    rate_id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL,
    item_ordinal INTEGER NOT NULL,
    rate_ordinal INTEGER NOT NULL,
    source_token TEXT NOT NULL UNIQUE,
    code_system TEXT,
    code TEXT,
    arrangement TEXT,
    network_names_json TEXT NOT NULL DEFAULT '[]',
    eligible INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX source_rate_code_idx ON source_rate (code_system, code, eligible);
CREATE TABLE source_item_metadata (
    file_id INTEGER NOT NULL,
    item_ordinal INTEGER NOT NULL,
    billing_code_type_version TEXT,
    name TEXT,
    description TEXT,
    PRIMARY KEY (file_id, item_ordinal)
) WITHOUT ROWID;
CREATE TABLE rate_provider_ref (
    rate_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    ref_id TEXT NOT NULL,
    PRIMARY KEY (rate_id, ref_id)
) WITHOUT ROWID;
CREATE INDEX rate_provider_reverse_idx
    ON rate_provider_ref (file_id, ref_id, rate_id);
CREATE TABLE rate_inline_npi (
    rate_id INTEGER NOT NULL,
    npi INTEGER NOT NULL,
    PRIMARY KEY (rate_id, npi)
) WITHOUT ROWID;
CREATE TABLE rate_inline_tin_marker (
    rate_id INTEGER PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE rate_network_name (
    rate_id INTEGER NOT NULL,
    network_name TEXT NOT NULL,
    PRIMARY KEY (rate_id, network_name)
) WITHOUT ROWID;
CREATE INDEX rate_inline_reverse_idx ON rate_inline_npi (npi, rate_id);
CREATE TABLE source_price (
    price_id INTEGER PRIMARY KEY,
    rate_id INTEGER NOT NULL,
    price_ordinal INTEGER NOT NULL,
    price_json TEXT NOT NULL
);
CREATE INDEX source_price_rate_idx ON source_price (rate_id, price_ordinal);
"""


@dataclass
class ProviderReferenceState:
    """Mutable event-stream state for one provider-reference pass."""

    ordinal: int = 0
    current_ordinal: int | None = None
    reference_id: str | None = None
    network_name_values_seen: int = 0
    npi_values_seen: int = 0
    has_zero_npi_marker: bool = False


@dataclass
class InNetworkState:
    """Mutable event-stream state for one in-network source pass."""

    item_ordinal: int = 0
    is_item_open: bool = False
    code_system: Any = None
    code: Any = None
    arrangement: Any = None
    billing_code_type_version: Any = None
    name: Any = None
    description: Any = None
    has_valid_item_types: bool = True
    rate_ordinal: int = 0
    rate_id: int | None = None
    price_ordinal: int = 0
    price_fields: dict[str, Any] | None = None
    has_valid_price_types: bool = True
    rate_network_name_values_seen: int = 0
    npi_values_seen: int = 0
    has_zero_npi_marker: bool = False


IN_NETWORK_ITEM_PREFIX = "in_network.item"
NEGOTIATED_RATE_PREFIX = f"{IN_NETWORK_ITEM_PREFIX}.negotiated_rates.item"
NEGOTIATED_PRICE_PREFIX = f"{NEGOTIATED_RATE_PREFIX}.negotiated_prices.item"


class SourceIndex:
    """Disk-backed, streaming source model used by the independent auditor."""

    def __init__(
        self,
        database_path: Path,
        *,
        seed: str,
        source_occurrence_sample_target: int,
        capacity_query_sample_target: int = 0,
        sqlite_cache_mb: int = 64,
        max_list_values: int = 10_000,
        max_tuples_per_query: int = 10_000,
    ):
        self.database_path = database_path
        self.seed = seed
        self.max_list_values = max_list_values
        self.max_tuples_per_query = max_tuples_per_query
        self.occurrence_sampler: BottomKSampler[SourceOccurrence] = BottomKSampler(
            source_occurrence_sample_target,
            seed=seed,
            namespace="source-occurrence-id",
        )
        self.capacity_query_sampler: BottomKSampler[SourceOccurrence] | None = (
            BottomKSampler(
                capacity_query_sample_target,
                seed=seed,
                namespace="capacity-distinct-semantic-query",
            )
            if capacity_query_sample_target > 0
            else None
        )
        self.connection = sqlite3.connect(str(database_path))
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA temp_store=FILE")
        self.connection.execute(f"PRAGMA cache_size={-max(sqlite_cache_mb, 1) * 1024}")
        self._writes_since_commit = 0
        self.metrics: dict[str, int] = collections.defaultdict(int)
        self.quarantined_provider_identifiers: collections.Counter[int] = (
            collections.Counter()
        )
        self.file_metrics: dict[int, dict[str, Any]] = {}
        self.raw_container_sha256_by_file_id: dict[int, str] = {}
        self._occurrence_sample_prepared = False
        self._create_schema()

    def close(self) -> None:
        """Close the temporary SQLite source index."""

        self.connection.close()

    def __enter__(self) -> "SourceIndex":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _create_schema(self) -> None:
        self.connection.executescript(SOURCE_INDEX_SCHEMA)
        self.connection.commit()

    def _mark_write(self, count: int = 1) -> None:
        self._writes_since_commit += count
        if self._writes_since_commit >= 50_000:
            self.connection.commit()
            self._writes_since_commit = 0

    @staticmethod
    def _is_provider_npi_prefix(prefix: str, base: str) -> bool:
        return prefix == f"{base}.npi" or prefix == f"{base}.npi.item"

    @staticmethod
    def _consume_zero_npi_marker_event(
        state: ProviderReferenceState | InNetworkState,
        *,
        prefix: str,
        provider_group_prefix: str,
        event: str,
        raw_npi: Any,
    ) -> bool:
        """Accept `[0]` only as a singleton non-member marker."""

        npi_array_prefix = f"{provider_group_prefix}.npi"
        if prefix == npi_array_prefix and event == "start_array":
            state.npi_values_seen = 0
            state.has_zero_npi_marker = False
            return True
        if prefix == npi_array_prefix and event == "end_array":
            state.npi_values_seen = 0
            state.has_zero_npi_marker = False
            return True
        if prefix != f"{npi_array_prefix}.item" or event not in SCALAR_EVENTS:
            return False
        integer_npi = strict_source_integer(event, raw_npi)
        if integer_npi == 0:
            if state.npi_values_seen:
                raise SourceFormatError("zero_npi_marker_must_be_singleton")
            state.npi_values_seen = 1
            state.has_zero_npi_marker = True
            return True
        if state.has_zero_npi_marker:
            raise SourceFormatError("zero_npi_marker_must_be_singleton")
        state.npi_values_seen += 1
        return False

    @staticmethod
    def _is_provider_tin_event(prefix: str, base: str, event: str) -> bool:
        """Recognize a TIN object or any nested TIN field event."""

        tin_prefix = f"{base}.tin"
        if prefix == tin_prefix:
            return event == "start_map"
        return prefix.startswith(f"{tin_prefix}.") and event in (
            SCALAR_EVENTS | {"start_map", "start_array"}
        )

    def index(self, specs: Sequence[SourceSpec]) -> None:
        """Index all source identities before streaming their in-network rates."""

        for spec in specs:
            self.raw_container_sha256_by_file_id[spec.file_id] = spec.content_sha256
            self.file_metrics[spec.file_id] = {
                "source_fingerprint": fingerprint(spec.source_id),
                "stored_bytes": spec.stored_bytes,
                "encoding": spec.encoding,
                "provider_reference_records": 0,
                "in_network_items": 0,
                "negotiated_rates": 0,
                "valid_prices": 0,
                "eligible_rates": 0,
            }
            self._index_provider_references(spec)
        self.connection.commit()
        for spec in specs:
            self._index_in_network(spec)
            self.connection.commit()
        self.connection.execute("ANALYZE")
        self.connection.commit()

    def _start_provider_reference(
        self,
        spec: SourceSpec,
        state: ProviderReferenceState,
    ) -> None:
        state.ordinal += 1
        state.current_ordinal = state.ordinal
        state.reference_id = None
        state.network_name_values_seen = 0
        self.metrics["provider_reference_records"] += 1
        self.file_metrics[spec.file_id]["provider_reference_records"] += 1

    def _capture_provider_reference_id(
        self,
        state: ProviderReferenceState,
        event: str,
        raw_identifier: Any,
    ) -> None:
        reference_id = strict_source_integer(event, raw_identifier)
        if reference_id is None:
            self.metrics["invalid_field_types"] += 1
            state.reference_id = None
            return
        state.reference_id = str(reference_id)

    def _capture_provider_reference_npi(
        self,
        spec: SourceSpec,
        state: ProviderReferenceState,
        event: str,
        raw_npi: Any,
    ) -> None:
        assert state.current_ordinal is not None
        npi = strict_source_npi(event, raw_npi)
        if npi is None:
            invalid_integer = strict_source_integer(event, raw_npi)
            if invalid_integer == 0:
                return
            self.metrics["invalid_provider_npis"] += 1
            if invalid_integer is not None:
                self.quarantined_provider_identifiers[invalid_integer] += 1
            if event != "number":
                self.metrics["invalid_field_types"] += 1
            return
        cursor = self.connection.execute(
            """INSERT OR IGNORE INTO provider_ref_pending(file_id, ref_ordinal, npi)
               VALUES (?, ?, ?)""",
            (spec.file_id, state.current_ordinal, npi),
        )
        if cursor.rowcount:
            self._mark_write()

    def _mark_provider_reference_tin(
        self,
        spec: SourceSpec,
        state: ProviderReferenceState,
    ) -> None:
        """Associate one or more TIN events with their provider reference once."""

        assert state.current_ordinal is not None
        cursor = self.connection.execute(
            """INSERT OR IGNORE INTO provider_ref_tin_pending(file_id, ref_ordinal)
               VALUES (?, ?)""",
            (spec.file_id, state.current_ordinal),
        )
        if cursor.rowcount:
            self.metrics["provider_reference_tin_markers"] += 1
            self._mark_write()

    def _capture_provider_reference_network_name(
        self,
        spec: SourceSpec,
        state: ProviderReferenceState,
        raw_name: Any,
    ) -> None:
        """Stage one canonical provider-reference network name by source ordinal."""

        assert state.current_ordinal is not None
        state.network_name_values_seen += 1
        if state.network_name_values_seen > self.max_list_values:
            raise SourceCoverageError("provider_reference_network_names_exceed_configured_limit")
        network_name = canonical_scalar(raw_name)
        if network_name is None:
            return
        cursor = self.connection.execute(
            """INSERT OR IGNORE INTO provider_ref_network_name_pending(
                   file_id, ref_ordinal, network_name
               ) VALUES (?, ?, ?)""",
            (spec.file_id, state.current_ordinal, network_name),
        )
        if cursor.rowcount:
            self._mark_write()

    def _persist_provider_reference(
        self,
        spec: SourceSpec,
        state: ProviderReferenceState,
    ) -> None:
        """Persist one provider reference and its staged TIN, NPI, and name data."""

        assert state.current_ordinal is not None
        assert state.reference_id is not None
        try:
            self.connection.execute(
                "INSERT INTO provider_ref_identity(file_id, ref_id) VALUES (?, ?)",
                (spec.file_id, state.reference_id),
            )
        except sqlite3.IntegrityError as exc:
            raise SourceFormatError("provider_reference_id_is_duplicated") from exc
        self._mark_write()
        has_tin_marker = bool(
            self.connection.execute(
                """SELECT 1 FROM provider_ref_tin_pending
                   WHERE file_id = ? AND ref_ordinal = ?""",
                (spec.file_id, state.current_ordinal),
            ).fetchone()
        )
        if has_tin_marker:
            self.connection.execute(
                """INSERT INTO provider_ref_tin_marker(file_id, ref_id)
                   VALUES (?, ?)""",
                (spec.file_id, state.reference_id),
            )
            self._mark_write()
        pending_npis = self.connection.execute(
            """SELECT npi FROM provider_ref_pending
               WHERE file_id = ? AND ref_ordinal = ? ORDER BY npi""",
            (spec.file_id, state.current_ordinal),
        )
        inserted_count = 0
        for pending_npi in pending_npis:
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO provider_ref_npi(file_id, ref_id, npi)
                   VALUES (?, ?, ?)""",
                (spec.file_id, state.reference_id, int(pending_npi["npi"])),
            )
            inserted_count += max(cursor.rowcount, 0)
            self._mark_write()
        self.metrics["provider_reference_npis"] += inserted_count
        pending_network_names = self.connection.execute(
            """SELECT network_name FROM provider_ref_network_name_pending
               WHERE file_id = ? AND ref_ordinal = ? ORDER BY network_name""",
            (spec.file_id, state.current_ordinal),
        )
        network_name_count = 0
        for pending_network_name in pending_network_names:
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO provider_ref_network_name(
                       file_id, ref_id, network_name
                   ) VALUES (?, ?, ?)""",
                (
                    spec.file_id,
                    state.reference_id,
                    str(pending_network_name["network_name"]),
                ),
            )
            network_name_count += max(cursor.rowcount, 0)
            self._mark_write()
        self.metrics["provider_reference_network_names"] += network_name_count
        if inserted_count == 0 and not has_tin_marker:
            self.metrics["provider_references_without_valid_npi"] += 1

    def _finish_provider_reference(
        self,
        spec: SourceSpec,
        state: ProviderReferenceState,
    ) -> None:
        assert state.current_ordinal is not None
        if state.reference_id is None:
            self.metrics["provider_references_without_id"] += 1
        else:
            self._persist_provider_reference(spec, state)
        for table_name in (
            "provider_ref_pending",
            "provider_ref_tin_pending",
            "provider_ref_network_name_pending",
        ):
            self.connection.execute(
                f"DELETE FROM {table_name} WHERE file_id = ? AND ref_ordinal = ?",
                (spec.file_id, state.current_ordinal),
            )
        state.current_ordinal = None
        state.reference_id = None

    def _index_provider_references(self, spec: SourceSpec) -> None:
        """First-pass provider references so in-network order is irrelevant."""

        base_prefix = "provider_references.item"
        state = ProviderReferenceState()
        try:
            with open_source_stream(spec) as source_stream:
                for prefix, event, raw_value in ijson.parse(source_stream):
                    self._consume_provider_reference_event(
                        spec,
                        state,
                        base_prefix,
                        prefix,
                        event,
                        raw_value,
                    )
        except (OSError, EOFError, ValueError, ijson.JSONError) as exc:
            raise SourceFormatError(f"provider_reference_parse:{type(exc).__name__}") from exc

    def _consume_provider_reference_event(
        self,
        spec: SourceSpec,
        state: ProviderReferenceState,
        base_prefix: str,
        prefix: str,
        event: str,
        raw_value: Any,
    ) -> None:
        if prefix == base_prefix and event == "start_map":
            self._start_provider_reference(spec, state)
            return
        if state.current_ordinal is None:
            return
        if prefix == f"{base_prefix}.provider_group_id" and event in SCALAR_EVENTS:
            self._capture_provider_reference_id(state, event, raw_value)
            return
        provider_group_prefix = f"{base_prefix}.provider_groups.item"
        if self._consume_zero_npi_marker_event(
            state,
            prefix=prefix,
            provider_group_prefix=provider_group_prefix,
            event=event,
            raw_npi=raw_value,
        ):
            return
        if self._is_provider_tin_event(prefix, provider_group_prefix, event):
            self._mark_provider_reference_tin(spec, state)
            return
        if (
            self._is_provider_npi_prefix(prefix, provider_group_prefix)
            and event in SCALAR_EVENTS
        ):
            self._capture_provider_reference_npi(spec, state, event, raw_value)
            return
        network_name_prefix = f"{base_prefix}.network_name"
        if prefix == network_name_prefix:
            if event in {"start_array", "end_array"}:
                return
            if event in SCALAR_EVENTS or event in {"start_map"}:
                self.metrics["invalid_field_types"] += 1
                return
        if prefix == f"{network_name_prefix}.item":
            if event == "string":
                self._capture_provider_reference_network_name(
                    spec,
                    state,
                    raw_value,
                )
            elif event in SCALAR_EVENTS or event in {"start_map", "start_array"}:
                self.metrics["invalid_field_types"] += 1
            return
        if prefix == base_prefix and event == "end_map":
            self._finish_provider_reference(spec, state)

    def _new_rate(self, spec: SourceSpec, item_ordinal: int, rate_ordinal: int) -> int:
        source_token = sha256_text(f"{spec.source_id}:{item_ordinal}:{rate_ordinal}")
        cursor = self.connection.execute(
            """
            INSERT INTO source_rate(file_id, item_ordinal, rate_ordinal, source_token)
            VALUES (?, ?, ?, ?)
            """,
            (spec.file_id, item_ordinal, rate_ordinal, source_token),
        )
        self._mark_write()
        return int(cursor.lastrowid)

    def _insert_inline_npi(
        self,
        rate_id: int,
        event: str,
        value: Any,
    ) -> None:
        npi = strict_source_npi(event, value)
        if npi is None:
            invalid_integer = strict_source_integer(event, value)
            if invalid_integer == 0:
                return
            self.metrics["invalid_inline_npis"] += 1
            if invalid_integer is not None:
                self.quarantined_provider_identifiers[invalid_integer] += 1
            if event != "number":
                self.metrics["invalid_field_types"] += 1
            return
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO rate_inline_npi(rate_id, npi) VALUES (?, ?)",
            (rate_id, npi),
        )
        if cursor.rowcount:
            self.metrics["inline_npis"] += 1
            self._mark_write()

    def _mark_inline_rate_tin(self, rate_id: int) -> None:
        """Associate all inline provider-group TIN events with their rate once."""

        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO rate_inline_tin_marker(rate_id) VALUES (?)",
            (rate_id,),
        )
        if cursor.rowcount:
            self.metrics["inline_rate_tin_markers"] += 1
            self._mark_write()

    def _resolved_reference_count(self, rate_id: int) -> int:
        provider_references = self.connection.execute(
            "SELECT file_id, ref_id FROM rate_provider_ref WHERE rate_id = ? ORDER BY ref_id",
            (rate_id,),
        )
        resolved_count = 0
        for provider_reference in provider_references:
            is_resolved = bool(
                self.connection.execute(
                    """
                    SELECT 1 FROM provider_ref_npi WHERE file_id = ? AND ref_id = ?
                    UNION ALL
                    SELECT 1 FROM provider_ref_tin_marker WHERE file_id = ? AND ref_id = ?
                    LIMIT 1
                    """,
                    (
                        provider_reference["file_id"],
                        provider_reference["ref_id"],
                        provider_reference["file_id"],
                        provider_reference["ref_id"],
                    ),
                ).fetchone()
            )
            if is_resolved:
                resolved_count += 1
            else:
                self.metrics["unresolved_provider_references"] += 1
        return resolved_count

    def _has_rate_provider_membership(self, rate_id: int) -> bool:
        return bool(
            self.connection.execute(
                """
                SELECT 1
                FROM rate_inline_npi inline_npi
                WHERE inline_npi.rate_id = ?
                UNION ALL
                SELECT 1
                FROM rate_provider_ref rate_ref
                JOIN provider_ref_npi ref_npi
                  ON ref_npi.file_id = rate_ref.file_id AND ref_npi.ref_id = rate_ref.ref_id
                WHERE rate_ref.rate_id = ?
                LIMIT 1
                """,
                (rate_id, rate_id),
            ).fetchone()
        )

    def _has_rate_provider_structure(self, rate_id: int) -> bool:
        return bool(
            self.connection.execute(
                """
                SELECT 1 FROM rate_inline_npi WHERE rate_id = ?
                UNION ALL
                SELECT 1 FROM rate_inline_tin_marker WHERE rate_id = ?
                UNION ALL
                SELECT 1
                FROM rate_provider_ref rate_ref
                JOIN provider_ref_npi ref_npi
                  ON ref_npi.file_id = rate_ref.file_id
                 AND ref_npi.ref_id = rate_ref.ref_id
                WHERE rate_ref.rate_id = ?
                UNION ALL
                SELECT 1
                FROM rate_provider_ref rate_ref
                JOIN provider_ref_tin_marker tin_marker
                  ON tin_marker.file_id = rate_ref.file_id
                 AND tin_marker.ref_id = rate_ref.ref_id
                WHERE rate_ref.rate_id = ?
                LIMIT 1
                """,
                (rate_id, rate_id, rate_id, rate_id),
            ).fetchone()
        )

    def _has_rate_tin_marker(self, rate_id: int) -> bool:
        return bool(
            self.connection.execute(
                """
                SELECT 1 FROM rate_inline_tin_marker WHERE rate_id = ?
                UNION ALL
                SELECT 1
                FROM rate_provider_ref rate_ref
                JOIN provider_ref_tin_marker tin_marker
                  ON tin_marker.file_id = rate_ref.file_id
                 AND tin_marker.ref_id = rate_ref.ref_id
                WHERE rate_ref.rate_id = ?
                LIMIT 1
                """,
                (rate_id, rate_id),
            ).fetchone()
        )

    def _has_rate_price(self, rate_id: int) -> bool:
        return bool(
            self.connection.execute(
                "SELECT 1 FROM source_price WHERE rate_id = ? LIMIT 1",
                (rate_id,),
            ).fetchone()
        )

    def _finalize_rate_eligibility(self, rate_id: int) -> None:
        """Resolve provider evidence, record coverage, and persist eligibility."""

        resolved_count = self._resolved_reference_count(rate_id)
        self.metrics["resolved_provider_references"] += resolved_count
        has_provider = self._has_rate_provider_membership(rate_id)
        has_provider_structure = self._has_rate_provider_structure(rate_id)
        has_tin_marker = self._has_rate_tin_marker(rate_id)
        has_price = self._has_rate_price(rate_id)
        if has_price:
            self.metrics["rates_with_valid_prices"] += 1
        if has_provider:
            self.metrics["rates_with_provider_membership"] += 1
        if has_provider_structure:
            self.metrics["rates_with_provider_structure"] += 1
        if has_tin_marker:
            self.metrics["rates_with_tin_markers"] += 1
        if has_provider_structure and not has_provider:
            self.metrics["tin_only_rates"] += 1
        eligible = has_provider and has_price
        network_name_rows = self.connection.execute(
            """
            SELECT network_name FROM rate_network_name WHERE rate_id = ?
            UNION
            SELECT ref_name.network_name
            FROM rate_provider_ref rate_ref
            JOIN provider_ref_network_name ref_name
              ON ref_name.file_id = rate_ref.file_id
             AND ref_name.ref_id = rate_ref.ref_id
            WHERE rate_ref.rate_id = ?
            ORDER BY network_name
            """,
            (rate_id, rate_id),
        )
        network_names = [
            str(network_name_row["network_name"])
            for network_name_row in network_name_rows
        ]
        if len(network_names) > self.max_list_values:
            raise SourceCoverageError("rate_network_names_exceed_configured_limit")
        self.connection.execute(
            """UPDATE source_rate
               SET eligible = ?, network_names_json = ?
               WHERE rate_id = ?""",
            (1 if eligible else 0, canonical_json(network_names), rate_id),
        )
        self._mark_write()

    def _append_price_list_value(self, price: dict[str, Any], field: str, value: Any) -> None:
        values = price.setdefault(field, [])
        if len(values) >= self.max_list_values:
            raise SourceCoverageError("price_list_exceeds_configured_limit")
        values.append(value)

    def _start_in_network_item(self, spec: SourceSpec, state: InNetworkState) -> None:
        state.item_ordinal += 1
        state.is_item_open = True
        state.code_system = None
        state.code = None
        state.arrangement = None
        state.billing_code_type_version = None
        state.name = None
        state.description = None
        state.has_valid_item_types = True
        state.rate_ordinal = 0
        self.metrics["in_network_items"] += 1
        self.file_metrics[spec.file_id]["in_network_items"] += 1

    def _capture_item_field(
        self,
        state: InNetworkState,
        field_name: str,
        event: str,
        raw_value: Any,
    ) -> None:
        if event != "string":
            self.metrics["invalid_field_types"] += 1
            state.has_valid_item_types = False
            return
        setattr(state, field_name, raw_value)

    def _capture_optional_item_field(
        self,
        state: InNetworkState,
        field_name: str,
        event: str,
        raw_value: Any,
    ) -> None:
        if event not in {"string", "null"}:
            self.metrics["invalid_field_types"] += 1
            state.has_valid_item_types = False
            return
        setattr(state, field_name, raw_value)

    def _start_negotiated_rate(self, spec: SourceSpec, state: InNetworkState) -> None:
        state.rate_ordinal += 1
        state.rate_id = self._new_rate(spec, state.item_ordinal, state.rate_ordinal)
        state.price_ordinal = 0
        state.rate_network_name_values_seen = 0
        self.metrics["negotiated_rates"] += 1
        self.file_metrics[spec.file_id]["negotiated_rates"] += 1

    def _capture_rate_reference(
        self,
        spec: SourceSpec,
        state: InNetworkState,
        event: str,
        raw_reference: Any,
    ) -> None:
        assert state.rate_id is not None
        reference_id = strict_source_integer(event, raw_reference)
        if reference_id is None:
            self.metrics["invalid_rate_provider_references"] += 1
            if event != "number":
                self.metrics["invalid_field_types"] += 1
            return
        cursor = self.connection.execute(
            """INSERT OR IGNORE INTO rate_provider_ref(rate_id, file_id, ref_id)
               VALUES (?, ?, ?)""",
            (state.rate_id, spec.file_id, str(reference_id)),
        )
        if cursor.rowcount:
            self.metrics["rate_provider_references"] += 1
            self._mark_write()

    def _capture_rate_network_name(
        self,
        state: InNetworkState,
        raw_name: Any,
    ) -> None:
        """Persist one bounded canonical network name attached directly to a rate."""

        assert state.rate_id is not None
        state.rate_network_name_values_seen += 1
        if state.rate_network_name_values_seen > self.max_list_values:
            raise SourceCoverageError("rate_network_names_exceed_configured_limit")
        network_name = canonical_scalar(raw_name)
        if network_name is None:
            return
        cursor = self.connection.execute(
            """INSERT OR IGNORE INTO rate_network_name(rate_id, network_name)
               VALUES (?, ?)""",
            (state.rate_id, network_name),
        )
        if cursor.rowcount:
            self.metrics["rate_network_names"] += 1
            self._mark_write()

    @staticmethod
    def _start_negotiated_price(state: InNetworkState) -> None:
        state.price_ordinal += 1
        state.price_fields = {
            "negotiated_type": None,
            "negotiated_rate": None,
            "expiration_date": None,
            "service_code": [],
            "billing_class": None,
            "setting": None,
            "billing_code_modifier": [],
            "additional_information": None,
        }
        state.has_valid_price_types = True

    def _capture_price_field(
        self,
        state: InNetworkState,
        prefix: str,
        event: str,
        raw_value: Any,
    ) -> None:
        assert state.price_fields is not None
        string_fields = (
            "negotiated_type",
            "expiration_date",
            "billing_class",
            "setting",
            "additional_information",
        )
        for field_name in string_fields:
            if (
                prefix == f"{NEGOTIATED_PRICE_PREFIX}.{field_name}"
                and event in SCALAR_EVENTS | {"start_map", "start_array"}
            ):
                if event not in {"string", "null"}:
                    self.metrics["invalid_field_types"] += 1
                    state.has_valid_price_types = False
                else:
                    state.price_fields[field_name] = raw_value
                return
        if (
            prefix == f"{NEGOTIATED_PRICE_PREFIX}.negotiated_rate"
            and event in SCALAR_EVENTS | {"start_map", "start_array"}
        ):
            if event != "number" or isinstance(raw_value, bool):
                self.metrics["invalid_field_types"] += 1
                state.has_valid_price_types = False
            else:
                state.price_fields["negotiated_rate"] = raw_value
            return
        self._capture_price_list_field(state, prefix, event, raw_value)

    def _capture_price_list_field(
        self,
        state: InNetworkState,
        prefix: str,
        event: str,
        raw_value: Any,
    ) -> None:
        assert state.price_fields is not None
        for field_name in ("service_code", "billing_code_modifier"):
            field_prefix = f"{NEGOTIATED_PRICE_PREFIX}.{field_name}"
            if prefix == field_prefix:
                if event not in {"start_array", "end_array"}:
                    self.metrics["invalid_field_types"] += 1
                    state.has_valid_price_types = False
                return
            if (
                prefix == f"{field_prefix}.item"
                and event in SCALAR_EVENTS | {"start_map", "start_array"}
            ):
                if event != "string":
                    self.metrics["invalid_field_types"] += 1
                    state.has_valid_price_types = False
                else:
                    self._append_price_list_value(state.price_fields, field_name, raw_value)
                return

    def _finish_negotiated_price(self, spec: SourceSpec, state: InNetworkState) -> None:
        assert state.price_fields is not None
        try:
            if not state.has_valid_price_types:
                raise ValueError("invalid_field_types")
            canonical_price = canonical_price_payload(state.price_fields)
        except ValueError:
            self.metrics["invalid_prices"] += 1
        else:
            assert state.rate_id is not None
            self.connection.execute(
                "INSERT INTO source_price(rate_id, price_ordinal, price_json) VALUES (?, ?, ?)",
                (state.rate_id, state.price_ordinal, canonical_json(canonical_price)),
            )
            self.metrics["valid_prices"] += 1
            self.file_metrics[spec.file_id]["valid_prices"] += 1
            self._mark_write()
        state.price_fields = None

    def _finish_in_network_item(self, spec: SourceSpec, state: InNetworkState) -> None:
        code_system = canonical_code_system(state.code_system)
        code = canonical_catalog_code(code_system, state.code)
        arrangement = canonical_code(state.arrangement)
        if not state.has_valid_item_types or not code_system or not code:
            self.metrics["items_without_valid_code"] += 1
            self.connection.execute(
                "UPDATE source_rate SET eligible = 0 WHERE file_id = ? AND item_ordinal = ?",
                (spec.file_id, state.item_ordinal),
            )
        else:
            self.connection.execute(
                """UPDATE source_rate
                   SET code_system = ?, code = ?, arrangement = ?
                   WHERE file_id = ? AND item_ordinal = ?""",
                (
                    code_system,
                    code,
                    arrangement,
                    spec.file_id,
                    state.item_ordinal,
                ),
            )
            self.connection.execute(
                """INSERT INTO source_item_metadata(
                       file_id,
                       item_ordinal,
                       billing_code_type_version,
                       name,
                       description
                   ) VALUES (?, ?, ?, ?, ?)""",
                (
                    spec.file_id,
                    state.item_ordinal,
                    canonical_source_metadata_text(state.billing_code_type_version),
                    canonical_source_metadata_text(state.name),
                    canonical_source_metadata_text(state.description),
                ),
            )
            eligible_rate_rows = self.connection.execute(
                """SELECT rate_id FROM source_rate
                   WHERE file_id = ? AND item_ordinal = ? AND eligible = 1
                   ORDER BY rate_ordinal""",
                (spec.file_id, state.item_ordinal),
            )
            for _eligible_rate in eligible_rate_rows:
                self.metrics["eligible_rates"] += 1
                self.file_metrics[spec.file_id]["eligible_rates"] += 1
        self._mark_write()
        state.is_item_open = False

    def _consume_in_network_event(
        self,
        spec: SourceSpec,
        state: InNetworkState,
        prefix: str,
        event: str,
        raw_value: Any,
    ) -> None:
        """Apply one streaming JSON event to the in-network index state."""

        if prefix == IN_NETWORK_ITEM_PREFIX and event == "start_map":
            self._start_in_network_item(spec, state)
            return
        if not state.is_item_open:
            return
        item_field_map = {
            f"{IN_NETWORK_ITEM_PREFIX}.billing_code_type": "code_system",
            f"{IN_NETWORK_ITEM_PREFIX}.billing_code": "code",
            f"{IN_NETWORK_ITEM_PREFIX}.negotiation_arrangement": "arrangement",
        }
        if prefix in item_field_map and event in SCALAR_EVENTS | {"start_map", "start_array"}:
            self._capture_item_field(state, item_field_map[prefix], event, raw_value)
            return
        optional_item_field_map = {
            f"{IN_NETWORK_ITEM_PREFIX}.billing_code_type_version": (
                "billing_code_type_version"
            ),
            f"{IN_NETWORK_ITEM_PREFIX}.name": "name",
            f"{IN_NETWORK_ITEM_PREFIX}.description": "description",
        }
        if (
            prefix in optional_item_field_map
            and event in SCALAR_EVENTS | {"start_map", "start_array"}
        ):
            self._capture_optional_item_field(
                state,
                optional_item_field_map[prefix],
                event,
                raw_value,
            )
            return
        if prefix == NEGOTIATED_RATE_PREFIX and event == "start_map":
            self._start_negotiated_rate(spec, state)
            return
        if state.rate_id is not None and self._is_rate_event_consumed(
            spec,
            state,
            prefix,
            event,
            raw_value,
        ):
            return
        if state.price_fields is not None:
            self._capture_price_field(state, prefix, event, raw_value)
            if prefix == NEGOTIATED_PRICE_PREFIX and event == "end_map":
                self._finish_negotiated_price(spec, state)
                return
        if prefix == NEGOTIATED_RATE_PREFIX and event == "end_map" and state.rate_id is not None:
            self._finalize_rate_eligibility(state.rate_id)
            state.rate_id = None
            state.price_fields = None
            return
        if prefix == IN_NETWORK_ITEM_PREFIX and event == "end_map":
            self._finish_in_network_item(spec, state)

    def _is_rate_event_consumed(
        self,
        spec: SourceSpec,
        state: InNetworkState,
        prefix: str,
        event: str,
        raw_value: Any,
    ) -> bool:
        assert state.rate_id is not None
        if prefix == f"{NEGOTIATED_RATE_PREFIX}.provider_references.item" and event in SCALAR_EVENTS:
            self._capture_rate_reference(spec, state, event, raw_value)
            return True
        for field_name in ("network_name", "network_names"):
            field_prefix = f"{NEGOTIATED_RATE_PREFIX}.{field_name}"
            if prefix == field_prefix:
                if event in {"start_array", "end_array"}:
                    return True
                if event == "string":
                    self._capture_rate_network_name(state, raw_value)
                elif event == "null":
                    return True
                elif event in SCALAR_EVENTS or event == "start_map":
                    self.metrics["invalid_field_types"] += 1
                return True
            if prefix == f"{field_prefix}.item":
                if event == "string":
                    self._capture_rate_network_name(state, raw_value)
                elif event in SCALAR_EVENTS or event in {"start_map", "start_array"}:
                    self.metrics["invalid_field_types"] += 1
                return True
        inline_prefix = f"{NEGOTIATED_RATE_PREFIX}.provider_groups.item"
        if self._consume_zero_npi_marker_event(
            state,
            prefix=prefix,
            provider_group_prefix=inline_prefix,
            event=event,
            raw_npi=raw_value,
        ):
            return True
        if self._is_provider_tin_event(prefix, inline_prefix, event):
            self._mark_inline_rate_tin(state.rate_id)
            return True
        if self._is_provider_npi_prefix(prefix, inline_prefix) and event in SCALAR_EVENTS:
            self._insert_inline_npi(state.rate_id, event, raw_value)
            return True
        if prefix == NEGOTIATED_PRICE_PREFIX and event == "start_map":
            self._start_negotiated_price(state)
            return True
        return False

    def _index_in_network(self, spec: SourceSpec) -> None:
        """Second-pass in-network items into canonical disk-backed relationships."""

        state = InNetworkState()
        try:
            with open_source_stream(spec) as source_stream:
                for prefix, event, raw_value in ijson.parse(source_stream):
                    self._consume_in_network_event(spec, state, prefix, event, raw_value)
        except (OSError, EOFError, ValueError, ijson.JSONError) as exc:
            raise SourceFormatError(f"in_network_parse:{type(exc).__name__}") from exc

    def _sample_source_occurrences(self) -> None:
        """Populate the bounded deterministic sample of eligible occurrences once."""

        if self._occurrence_sample_prepared:
            return
        occurrence_rows = self.connection.execute(
            """
            WITH rate_member AS (
                SELECT rate_id, npi FROM rate_inline_npi
                UNION
                SELECT rate_ref.rate_id, ref_npi.npi
                FROM rate_provider_ref rate_ref
                JOIN provider_ref_npi ref_npi
                  ON ref_npi.file_id = rate_ref.file_id AND ref_npi.ref_id = rate_ref.ref_id
            )
            SELECT rate.source_token,
                   rate.file_id,
                   rate.code_system,
                   rate.code,
                   rate.arrangement,
                   item_metadata.billing_code_type_version,
                   item_metadata.name,
                   item_metadata.description,
                   rate.network_names_json,
                   price.price_ordinal,
                   price.price_json,
                   member.npi
            FROM source_rate rate
            JOIN source_item_metadata item_metadata
              ON item_metadata.file_id = rate.file_id
             AND item_metadata.item_ordinal = rate.item_ordinal
            JOIN source_price price ON price.rate_id = rate.rate_id
            JOIN rate_member member ON member.rate_id = rate.rate_id
            WHERE rate.eligible = 1
            ORDER BY rate.rate_id, price.price_ordinal, member.npi
            """
        )
        occurrence_count = 0
        for occurrence_row in occurrence_rows:
            query = QueryKey(
                str(occurrence_row["code_system"]),
                str(occurrence_row["code"]),
                int(occurrence_row["npi"]),
            )
            canonical_tuple = CanonicalTuple.from_parts(
                query,
                occurrence_row["arrangement"],
                json.loads(occurrence_row["price_json"]),
                billing_code_type_version=occurrence_row[
                    "billing_code_type_version"
                ],
                name=occurrence_row["name"],
                description=occurrence_row["description"],
                network_names=json.loads(occurrence_row["network_names_json"]),
            )
            occurrence_id = sha256_text(
                f"{occurrence_row['source_token']}:"
                f"{int(occurrence_row['price_ordinal'])}:{query.npi}"
            )
            occurrence = SourceOccurrence(
                occurrence_id=occurrence_id,
                query=query,
                tuple_key=canonical_occurrence_key(
                    canonical_tuple,
                    self.raw_container_sha256_by_file_id[
                        int(occurrence_row["file_id"])
                    ],
                ),
            )
            self.occurrence_sampler.offer(occurrence.stable_key, occurrence)
            if self.capacity_query_sampler is not None:
                self.capacity_query_sampler.offer(query.stable_key, occurrence)
            occurrence_count += 1
        self.metrics["source_occurrences"] = occurrence_count
        self.metrics["sampled_source_occurrences"] = len(self.occurrence_sampler)
        if self.capacity_query_sampler is not None:
            self.metrics["sampled_capacity_distinct_queries"] = len(
                self.capacity_query_sampler
            )
        self._occurrence_sample_prepared = True

    def prepare_occurrence_sample(self) -> None:
        """Build the bounded source sample after the remote source-set preflight."""

        self._sample_source_occurrences()

    def source_occurrences(self, target: int) -> list[SourceOccurrence]:
        """Return up to the requested number of sampled source occurrences."""

        self.prepare_occurrence_sample()
        if target > self.occurrence_sampler.capacity:
            raise ConfigurationError("source_occurrence_target_exceeds_sampler_capacity")
        return self.occurrence_sampler.values()[:target]

    def capacity_query_occurrences(self, target: int) -> list[SourceOccurrence]:
        """Return a deterministic sample containing at most one row per query."""

        self.prepare_occurrence_sample()
        if self.capacity_query_sampler is None:
            raise ConfigurationError("capacity_query_sampler_disabled")
        if target > self.capacity_query_sampler.capacity:
            raise ConfigurationError("capacity_query_target_exceeds_sampler_capacity")
        occurrences = self.capacity_query_sampler.values()[:target]
        if len(occurrences) < target:
            raise SourceCoverageError(
                "capacity_distinct_random_query_population_below_minimum"
            )
        return occurrences

    def has_query(self, query: QueryKey) -> bool:
        """Return whether any eligible source rate serves the exact query."""

        row = self.connection.execute(
            """
            SELECT 1
            FROM source_rate rate
            WHERE rate.eligible = 1
              AND rate.code_system = ?
              AND rate.code = ?
              AND (
                    EXISTS (
                        SELECT 1 FROM rate_inline_npi inline_npi
                        WHERE inline_npi.rate_id = rate.rate_id AND inline_npi.npi = ?
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM rate_provider_ref rate_ref
                        JOIN provider_ref_npi ref_npi
                          ON ref_npi.file_id = rate_ref.file_id AND ref_npi.ref_id = rate_ref.ref_id
                        WHERE rate_ref.rate_id = rate.rate_id AND ref_npi.npi = ?
                    )
              )
            LIMIT 1
            """,
            (query.code_system, query.code, query.npi, query.npi),
        ).fetchone()
        return row is not None

    def expected_tuples(self, query: QueryKey) -> collections.Counter[str]:
        """Return the exact source multiset for one code-and-provider query."""

        self.metrics["expected_tuple_query_scans"] += 1
        tuple_rows = self.connection.execute(
            """
            SELECT rate.file_id,
                   rate.arrangement,
                   item_metadata.billing_code_type_version,
                   item_metadata.name,
                   item_metadata.description,
                   rate.network_names_json,
                   price.price_json,
                   COUNT(*) AS occurrence_count
            FROM source_rate rate
            JOIN source_item_metadata item_metadata
              ON item_metadata.file_id = rate.file_id
             AND item_metadata.item_ordinal = rate.item_ordinal
            JOIN source_price price ON price.rate_id = rate.rate_id
            WHERE rate.eligible = 1
              AND rate.code_system = ?
              AND rate.code = ?
              AND (
                    EXISTS (
                        SELECT 1 FROM rate_inline_npi inline_npi
                        WHERE inline_npi.rate_id = rate.rate_id AND inline_npi.npi = ?
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM rate_provider_ref rate_ref
                        JOIN provider_ref_npi ref_npi
                          ON ref_npi.file_id = rate_ref.file_id AND ref_npi.ref_id = rate_ref.ref_id
                        WHERE rate_ref.rate_id = rate.rate_id AND ref_npi.npi = ?
                    )
              )
            GROUP BY rate.file_id,
                     rate.arrangement,
                     item_metadata.billing_code_type_version,
                     item_metadata.name,
                     item_metadata.description,
                     rate.network_names_json,
                     price.price_json
            ORDER BY rate.file_id,
                     rate.arrangement,
                     item_metadata.billing_code_type_version,
                     item_metadata.name,
                     item_metadata.description,
                     rate.network_names_json,
                     price.price_json
            """,
            (query.code_system, query.code, query.npi, query.npi),
        )
        counter: collections.Counter[str] = collections.Counter()
        for tuple_row in tuple_rows:
            canonical_price = json.loads(tuple_row["price_json"])
            canonical_tuple = CanonicalTuple.from_parts(
                query,
                tuple_row["arrangement"],
                canonical_price,
                billing_code_type_version=tuple_row["billing_code_type_version"],
                name=tuple_row["name"],
                description=tuple_row["description"],
                network_names=json.loads(tuple_row["network_names_json"]),
            )
            occurrence_key = canonical_occurrence_key(
                canonical_tuple,
                self.raw_container_sha256_by_file_id[int(tuple_row["file_id"])],
            )
            if occurrence_key not in counter and len(counter) >= self.max_tuples_per_query:
                raise SourceCoverageError("source_query_exceeds_canonical_tuple_limit")
            counter[occurrence_key] += int(tuple_row["occurrence_count"])
        return counter

    def negative_queries(
        self,
        positives: Sequence[QueryKey],
        sample_target: int,
    ) -> list[QueryKey]:
        """Build deterministic absent code/NPI recombinations from positive keys."""

        codes = sorted({(query.code_system, query.code) for query in positives})
        npis = sorted({query.npi for query in positives})
        if not codes or not npis or sample_target < 1:
            return []
        positive_keys = {query.stable_key for query in positives}
        sampler: BottomKSampler[QueryKey] = BottomKSampler(
            sample_target,
            seed=self.seed,
            namespace="negative-code-npi-pair",
        )
        tested_keys: set[str] = set()
        attempt_count = max(sample_target * 50, len(codes) * min(len(npis), 10))
        for attempt in range(attempt_count):
            code_rank = int.from_bytes(
                hashlib.sha256(f"{self.seed}:negative-code:{attempt}".encode()).digest()[:8], "big"
            )
            npi_rank = int.from_bytes(
                hashlib.sha256(f"{self.seed}:negative-npi:{attempt}".encode()).digest()[:8], "big"
            )
            code_system, code = codes[code_rank % len(codes)]
            query = QueryKey(code_system, code, npis[npi_rank % len(npis)])
            if query.stable_key in positive_keys or query.stable_key in tested_keys:
                continue
            tested_keys.add(query.stable_key)
            self.metrics["negative_membership_probes"] += 1
            if self.has_query(query):
                continue
            sampler.offer(query.stable_key, query)
        return sampler.values()

    def source_report(self) -> dict[str, Any]:
        """Return redacted source coverage and temporary-index measurements."""

        sqlite_bytes = 0
        for suffix in ("", "-wal", "-shm"):
            storage_path = Path(f"{self.database_path}{suffix}")
            if storage_path.exists():
                sqlite_bytes += storage_path.stat().st_size
        return {
            "files": [self.file_metrics[file_id] for file_id in sorted(self.file_metrics)],
            "totals": dict(sorted(self.metrics.items())),
            "source_set": source_set_evidence(
                list(self.raw_container_sha256_by_file_id.values())
            ).payload,
            "provider_identifier_quarantine": (
                provider_identifier_quarantine_payload(
                    self.quarantined_provider_identifiers
                )
            ),
            "sqlite_storage_bytes": sqlite_bytes,
            "max_canonical_tuples_per_query": self.max_tuples_per_query,
            "source_occurrence_sampler_capacity": self.occurrence_sampler.capacity,
            "capacity_query_sampler_capacity": (
                self.capacity_query_sampler.capacity
                if self.capacity_query_sampler is not None
                else 0
            ),
            "memory_model": "ijson_event_stream_plus_disk_backed_sqlite_plus_fixed_bottom_k_occurrence_sampler",
        }


@dataclass(frozen=True)
class PageContract:
    result_state: str
    pricing_scope: str
    resolved_snapshot_id: str
    query_snapshot_id: str
    query_plan_id: str
    query_mode: str
    provenance_arch_version: str
    provenance_storage_generation: str
    provenance_database_backend: str
    provenance_database_evidence_contract: str
    provenance_postgres_server_version_num: int
    provenance_database_selected: bool
    provenance_backend_session_active: bool
    provenance_transaction_snapshot_observed: bool
    provenance_plan_id: str
    provenance_snapshot_id: str
    provenance_mode: str
    provenance_pricing_scope: str
    query_source_key: str | None = None
    provenance_source_key: str | None = None


@dataclass(frozen=True)
class FetchResult:
    items: tuple[Mapping[str, Any], ...]
    contracts: tuple[PageContract, ...]
    page_latencies_ms: tuple[float, ...]
    total_latency_ms: float
    pages: int
    retries: int
    response_fingerprint: str
    capacity_observations: tuple[CapacityHttpObservation, ...] = ()


@dataclass(frozen=True)
class ApiOccurrence:
    occurrence_id: str
    canonical_tuple: CanonicalTuple
    source_identity: SourceIdentity

    @property
    def query(self) -> QueryKey:
        """Return the exact query identity represented by this occurrence."""

        return self.canonical_tuple.query

    @property
    def tuple_key(self) -> str:
        """Return the canonical tuple key bound to source-container identity."""

        return canonical_occurrence_key(
            self.canonical_tuple,
            self.source_identity.raw_container_sha256,
        )


@dataclass(frozen=True)
class ApiOccurrenceSample:
    occurrences: tuple[ApiOccurrence, ...]
    sample_count: int
    pages: int
    retries: int
    sample_digest: str
    contract: str = AUDIT_SAMPLE_CONTRACT
    method: str = AUDIT_SAMPLE_METHOD
    complete_population: bool = False
    sample_digest_validated: bool = False
    source_set_validated: bool = False


class ApiOccurrenceSource(Protocol):
    def validate_source_set(self) -> bool:
        """Bind supplied raw sources before either side selects occurrences."""

    def sample_occurrences(self, *, sample_target: int, seed: str) -> ApiOccurrenceSample:
        """Enumerate the persisted API audit sample and bottom-k sample occurrence IDs."""


class ApiFetcher(Protocol):
    request_count: int

    def fetch_all(
        self,
        params: Mapping[str, Any],
        *,
        phase: str,
        page_size: int | None = None,
        capacity_intent: CapacityObservationIntent | None = None,
    ) -> FetchResult:
        """Fetch every result page for one query."""


def _strict_int(value: Any, *, field: str) -> int:
    if type(value) is not int:
        raise ApiSchemaError(f"{field}_must_be_integer")
    return value


def _strict_string(
    value: Any,
    *,
    field: str,
    nullable: bool = False,
    empty: bool = False,
) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise ApiSchemaError(f"{field}_must_be_string")
    if not empty and not value.strip():
        raise ApiSchemaError(f"{field}_must_be_nonempty_string")
    if len(value) > MAX_CANONICAL_CHARS:
        raise ApiSchemaError(f"{field}_exceeds_string_limit")
    return value


def _required_mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ApiSchemaError(f"{field}_must_be_object")
    return value


def _strict_sha256(value: Any, *, field: str) -> str:
    digest = _strict_string(value, field=field)
    assert digest is not None
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ApiSchemaError(f"{field}_must_be_lowercase_sha256")
    return digest


def extract_source_set_evidence(response_payload: Mapping[str, Any]) -> SourceSetEvidence:
    """Validate the redacted complete-source seal returned by the audit endpoint."""

    raw_source_set = _required_mapping(
        response_payload.get("source_set"),
        field="source_set",
    )
    if set(raw_source_set) != {
        "contract",
        "source_count",
        "raw_container_sha256_digest",
    }:
        raise ApiSchemaError("source_set_has_unexpected_fields")
    contract = _strict_string(
        raw_source_set.get("contract"),
        field="source_set_contract",
    )
    if contract != SOURCE_SET_CONTRACT:
        raise ApiSchemaError("source_set_contract_mismatch")
    source_count = _strict_int(
        raw_source_set.get("source_count"),
        field="source_set_source_count",
    )
    if source_count < 1:
        raise ApiSchemaError("source_set_source_count_out_of_range")
    return SourceSetEvidence(
        source_count=source_count,
        raw_container_sha256_digest=_strict_sha256(
            raw_source_set.get("raw_container_sha256_digest"),
            field="source_set_raw_container_sha256_digest",
        ),
        contract=str(contract),
    )


def _validate_identity_evidence(
    *,
    identity_kind: str,
    identity_sha256: str,
    raw_sha256: str,
    logical_sha256: str | None,
    is_logical_hash_deferred: bool,
    field_prefix: str,
) -> None:
    if identity_kind == "logical_json_sha256_v1":
        if (
            is_logical_hash_deferred
            or logical_sha256 is None
            or logical_sha256 != identity_sha256
        ):
            raise ApiSchemaError(f"{field_prefix}_logical_identity_evidence_inconsistent")
        return
    if identity_kind == "raw_container_sha256_v1":
        if (
            not is_logical_hash_deferred
            or logical_sha256 is not None
            or raw_sha256 != identity_sha256
        ):
            raise ApiSchemaError(f"{field_prefix}_raw_identity_evidence_inconsistent")
        return
    raise ApiSchemaError(f"{field_prefix}_identity_kind_unsupported")


def _extract_source_trace_key(
    raw_identity: Mapping[str, Any],
    *,
    field_prefix: str,
) -> tuple[str, str]:
    trace_set_hash = _strict_sha256(
        raw_identity.get("source_trace_set_hash"),
        field=f"{field_prefix}_source_trace_set_hash",
    )
    source_trace = raw_identity.get("source_trace")
    if not isinstance(source_trace, list) or not source_trace:
        raise ApiSchemaError(f"{field_prefix}_source_trace_must_be_nonempty_array")
    for trace_index, trace_entry in enumerate(source_trace):
        if not isinstance(trace_entry, Mapping):
            raise ApiSchemaError(f"{field_prefix}_source_trace_item_must_be_object")
        _strict_string(
            trace_entry.get("source_file_version_id"),
            field=f"{field_prefix}_source_trace_{trace_index}_source_file_version_id",
        )
    return trace_set_hash, canonical_json(source_trace)


@dataclass(frozen=True)
class _SourceHashEvidence:
    identity_kind: str
    identity_sha256: str
    raw_sha256: str
    logical_sha256: str | None
    is_logical_hash_deferred: bool


def _extract_source_hash_evidence(
    raw_identity: Mapping[str, Any],
    *,
    field_prefix: str,
) -> _SourceHashEvidence:
    identity_kind = str(
        _strict_string(
            raw_identity.get("identity_kind"),
            field=f"{field_prefix}_identity_kind",
        )
    )
    identity_sha256 = _strict_sha256(
        raw_identity.get("identity_sha256"),
        field=f"{field_prefix}_identity_sha256",
    )
    raw_sha256 = _strict_sha256(
        raw_identity.get("raw_container_sha256"),
        field=f"{field_prefix}_raw_container_sha256",
    )
    is_logical_hash_deferred = raw_identity.get("logical_hash_deferred")
    if type(is_logical_hash_deferred) is not bool:
        raise ApiSchemaError(f"{field_prefix}_logical_hash_deferred_must_be_boolean")
    raw_logical_sha256 = raw_identity.get("logical_json_sha256")
    logical_sha256 = (
        None
        if raw_logical_sha256 is None
        else _strict_sha256(
            raw_logical_sha256,
            field=f"{field_prefix}_logical_json_sha256",
        )
    )
    _validate_identity_evidence(
        identity_kind=identity_kind,
        identity_sha256=identity_sha256,
        raw_sha256=raw_sha256,
        logical_sha256=logical_sha256,
        is_logical_hash_deferred=is_logical_hash_deferred,
        field_prefix=field_prefix,
    )
    return _SourceHashEvidence(
        identity_kind,
        identity_sha256,
        raw_sha256,
        logical_sha256,
        is_logical_hash_deferred,
    )


def extract_source_identity(
    raw: Mapping[str, Any],
    *,
    field_prefix: str,
    registry: SourceIdentityRegistry | None = None,
) -> SourceIdentity:
    """Validate and register exact source-attribution evidence from an API row."""

    source_artifact_key = _strict_int(
        raw.get("source_artifact_key"),
        field=f"{field_prefix}_source_artifact_key",
    )
    if source_artifact_key < 0:
        raise ApiSchemaError(f"{field_prefix}_source_artifact_key_out_of_range")
    source_key = (
        _strict_string(raw["source_key"], field=f"{field_prefix}_source_key")
        if "source_key" in raw
        else None
    )
    source_type = _strict_string(raw.get("source_type"), field=f"{field_prefix}_source_type")
    if source_type != "in_network":
        raise ApiSchemaError(f"{field_prefix}_source_type_must_be_in_network")
    hash_evidence = _extract_source_hash_evidence(raw, field_prefix=field_prefix)
    trace_set_hash, source_trace_key = _extract_source_trace_key(
        raw,
        field_prefix=field_prefix,
    )
    identity = SourceIdentity(
        source_artifact_key=source_artifact_key,
        source_key=source_key,
        source_type=source_type,
        identity_kind=hash_evidence.identity_kind,
        identity_sha256=hash_evidence.identity_sha256,
        raw_container_sha256=hash_evidence.raw_sha256,
        logical_json_sha256=hash_evidence.logical_sha256,
        logical_hash_deferred=hash_evidence.is_logical_hash_deferred,
        source_trace_set_hash=trace_set_hash,
        source_trace_key=source_trace_key,
    )
    if registry is not None:
        registry.register(identity)
    return identity


@dataclass(frozen=True)
class HttpApiFetcherOptions:
    """Connection, retry, and pagination bounds for one HTTP API surface."""

    base_url: str
    api_path: str
    headers: Mapping[str, str]
    page_size: int
    max_pages: int
    timeout_seconds: float
    retries: int
    retry_backoff_seconds: float
    max_response_bytes: int
    verify_tls: bool


@dataclass
class _FetchState:
    response_items: list[Mapping[str, Any]] = dataclasses.field(default_factory=list)
    contracts: list[PageContract] = dataclasses.field(default_factory=list)
    page_latencies_ms: list[float] = dataclasses.field(default_factory=list)
    seen_page_fingerprints: set[str] = dataclasses.field(default_factory=set)
    declared_total: int | None = None
    offset: int = 0
    retries: int = 0
    capacity_observations: list[CapacityHttpObservation] = dataclasses.field(
        default_factory=list
    )


class _ExactResponseHeaders(Mapping[str, str]):
    """Mapping adapter that preserves duplicate HTTP response fields."""

    def __init__(self, pairs: Sequence[tuple[str, str]]):
        self._pairs = tuple(pairs)
        self._by_name: dict[str, str] = {}
        for name, value in self._pairs:
            self._by_name.setdefault(name.lower(), value)

    def __getitem__(self, name: str) -> str:
        return self._by_name[name.lower()]

    def __iter__(self) -> Iterator[str]:
        return iter(self._by_name)

    def __len__(self) -> int:
        return len(self._by_name)

    def items(self, multi: bool = False) -> Any:
        """Return original duplicate-preserving pairs when requested."""

        if multi:
            return list(self._pairs)
        return [(name, self._by_name[name]) for name in self._by_name]


class HttpApiFetcher:
    """Bounded paginated HTTP client with deterministic retry behavior."""

    RETRYABLE_STATUS = {429, 502, 503, 504}

    def __init__(
        self,
        options: HttpApiFetcherOptions,
        *,
        capacity_collector: CapacityEvidenceCollector | None = None,
    ):
        parsed = urlsplit(options.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ConfigurationError("api_base_url_must_be_http_origin")
        self.url = options.base_url.rstrip("/") + "/" + options.api_path.lstrip("/")
        self.page_size = options.page_size
        self.max_pages = options.max_pages
        self.retries = options.retries
        self.retry_backoff_seconds = options.retry_backoff_seconds
        self.max_response_bytes = options.max_response_bytes
        self.capacity_collector = capacity_collector
        if capacity_collector is not None and any(
            name.lower()
            in {
                capacity_evidence.CAPACITY_CHALLENGE_HEADER.lower(),
                capacity_evidence.CAPACITY_RUN_NONCE_HEADER.lower(),
            }
            for name in options.headers
        ):
            raise ConfigurationError("capacity_headers_must_be_request_local")
        self.client = httpx.Client(
            headers=dict(options.headers),
            timeout=httpx.Timeout(options.timeout_seconds),
            verify=options.verify_tls,
            follow_redirects=False,
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
        )
        self._stats_lock = threading.Lock()
        self.request_count = 0
        self.retry_count = 0

    def close(self) -> None:
        """Close the underlying bounded HTTP client."""

        self.client.close()

    def __enter__(self) -> "HttpApiFetcher":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _read_bounded(self, response: httpx.Response) -> bytes:
        body = bytearray()
        for chunk in response.iter_bytes():
            if len(body) + len(chunk) > self.max_response_bytes:
                raise ApiError("response_exceeds_configured_byte_limit", status_code=response.status_code)
            body.extend(chunk)
        return bytes(body)

    def _sleep_before_retry(self, attempt: int, retry_after: str | None) -> None:
        delay = self.retry_backoff_seconds * (2**attempt)
        if retry_after:
            with contextlib.suppress(ValueError):
                delay = max(delay, min(float(retry_after), 30.0))
        if delay > 0:
            time.sleep(delay)

    def _request_headers_for_capacity_plan(
        self, capacity_plan: _CapacityObservationPlan | None
    ) -> tuple[str | None, dict[str, str] | None]:
        if capacity_plan is None:
            return None, None
        if self.capacity_collector is None:
            raise ConfigurationError("capacity_collector_missing")
        challenge = self.capacity_collector.fresh_challenge()
        return challenge, self.capacity_collector.request_headers(
            capacity_plan, challenge
        )

    def _execute_page_request(
        self,
        params: Mapping[str, Any],
        capacity_plan: _CapacityObservationPlan | None,
        attempt: int,
        attempt_started: float,
    ) -> tuple[int, str | None, int, bytes, tuple[CapacityHttpObservation, ...]]:
        challenge, request_headers = self._request_headers_for_capacity_plan(capacity_plan)
        capacity_observations: list[CapacityHttpObservation] = []
        with self.client.stream("GET", self.url, params=dict(params), headers=request_headers) as response:
            status_code = response.status_code
            retry_after = response.headers.get("retry-after")
            response_redirect_count = len(response.history) + int(response.is_redirect)
            try:
                body = self._read_bounded(response)
            except ApiError:
                if capacity_plan is not None:
                    assert self.capacity_collector is not None
                    capacity_observations.append(self.capacity_collector.record_unverified_response(capacity_plan, attempt_index=attempt, latency_ms=(time.perf_counter() - attempt_started) * 1000.0, response_status_code=status_code, response_redirect_count=response_redirect_count, reason="response_body_incomplete"))
                raise
            if capacity_plan is not None:
                assert self.capacity_collector is not None and challenge is not None
                capacity_observations.append(self.capacity_collector.collect_response(capacity_plan, _CapacityResponseDetails(challenge=challenge, query_parameters=params, response_headers=_ExactResponseHeaders(response.headers.multi_items()), response_status_code=status_code, response_body=body, response_redirect_count=response_redirect_count, collector_received_at=dt.datetime.now(dt.timezone.utc), attempt_index=attempt, latency_ms=(time.perf_counter() - attempt_started) * 1000.0)))
        return status_code, retry_after, response_redirect_count, body, tuple(capacity_observations)

    def _capacity_transport_failure(
        self,
        capacity_plan: _CapacityObservationPlan | None,
        attempt: int,
        attempt_started: float,
        reason: str,
    ) -> tuple[CapacityHttpObservation, ...]:
        if capacity_plan is None:
            return ()
        assert self.capacity_collector is not None
        return (
            self.capacity_collector.record_transport_failure(
                capacity_plan,
                attempt_index=attempt,
                latency_ms=(time.perf_counter() - attempt_started) * 1000.0,
                reason=reason,
            ),
        )

    @staticmethod
    def _parse_success_payload(body: bytes) -> dict[str, Any]:
        try:
            response_payload = json.loads(body.decode("utf-8"), parse_float=Decimal, parse_int=int)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiSchemaError("response_is_not_valid_utf8_json") from exc
        if not isinstance(response_payload, dict):
            raise ApiSchemaError("response_root_must_be_object")
        return response_payload

    def _request_page(
        self,
        params: Mapping[str, Any],
        *,
        capacity_plan: _CapacityObservationPlan | None = None,
    ) -> tuple[
        dict[str, Any],
        float,
        int,
        tuple[CapacityHttpObservation, ...],
    ]:
        started = time.perf_counter()
        used_retries = 0
        last_transport_error: Exception | None = None
        capacity_observations: list[CapacityHttpObservation] = []
        for attempt in range(self.retries + 1):
            with self._stats_lock:
                self.request_count += 1
            attempt_started = time.perf_counter()
            try:
                status_code, retry_after, response_redirect_count, body, attempt_observations = self._execute_page_request(params, capacity_plan, attempt, attempt_started)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_transport_error = exc
                capacity_observations.extend(
                    self._capacity_transport_failure(
                        capacity_plan,
                        attempt,
                        attempt_started,
                        (
                            "request_timeout"
                            if isinstance(exc, httpx.TimeoutException)
                            else "transport_failure"
                        ),
                    )
                )
                if attempt >= self.retries:
                    break
                used_retries += 1
                with self._stats_lock:
                    self.retry_count += 1
                self._sleep_before_retry(attempt, None)
                continue
            capacity_observations.extend(attempt_observations)
            if status_code in self.RETRYABLE_STATUS and attempt < self.retries:
                used_retries += 1
                with self._stats_lock:
                    self.retry_count += 1
                self._sleep_before_retry(attempt, retry_after)
                continue
            if status_code < 200 or status_code >= 300:
                raise ApiError(
                    "non_success_status",
                    status_code=status_code,
                    body_sha256=hashlib.sha256(body).hexdigest(),
                )
            response_payload = self._parse_success_payload(body)
            latency_ms = (time.perf_counter() - started) * 1000.0
            return response_payload, latency_ms, used_retries, tuple(capacity_observations)
        raise ApiError("transport_failure") from last_transport_error

    @staticmethod
    def _query_contract_fields(query: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "query_snapshot_id": str(
                _strict_string(query.get("snapshot_id"), field="query_snapshot_id")
            ),
            "query_plan_id": str(
                _strict_string(query.get("plan_id"), field="query_plan_id")
            ),
            "query_mode": str(_strict_string(query.get("mode"), field="query_mode")),
            "query_source_key": (
                None
                if query.get("source_key") in (None, "")
                else str(_strict_string(query.get("source_key"), field="query_source_key"))
            ),
        }

    @staticmethod
    def _provenance_contract_fields(provenance: Mapping[str, Any]) -> dict[str, Any]:
        """Validate and flatten required V3 provenance and database evidence fields."""

        database_evidence = _required_mapping(
            provenance.get("database_evidence"),
            field="provenance_database_evidence",
        )
        server_version_num = _strict_int(
            database_evidence.get("server_version_num"),
            field="provenance_postgres_server_version_num",
        )
        for field_name in (
            "database_selected",
            "backend_session_active",
            "transaction_snapshot_observed",
        ):
            if type(database_evidence.get(field_name)) is not bool:
                raise ApiSchemaError(
                    f"provenance_{field_name}_must_be_boolean"
                )
        return {
            "provenance_arch_version": str(
                _strict_string(provenance.get("arch_version"), field="provenance_arch_version")
            ),
            "provenance_storage_generation": str(
                _strict_string(
                    provenance.get("storage_generation"),
                    field="provenance_storage_generation",
                )
            ),
            "provenance_database_backend": str(
                _strict_string(
                    provenance.get("database_backend"),
                    field="provenance_database_backend",
                )
            ),
            "provenance_database_evidence_contract": str(
                _strict_string(
                    database_evidence.get("contract"),
                    field="provenance_database_evidence_contract",
                )
            ),
            "provenance_postgres_server_version_num": server_version_num,
            "provenance_database_selected": database_evidence[
                "database_selected"
            ],
            "provenance_backend_session_active": database_evidence[
                "backend_session_active"
            ],
            "provenance_transaction_snapshot_observed": database_evidence[
                "transaction_snapshot_observed"
            ],
            "provenance_plan_id": str(
                _strict_string(provenance.get("plan_id"), field="provenance_plan_id")
            ),
            "provenance_snapshot_id": str(
                _strict_string(provenance.get("snapshot_id"), field="provenance_snapshot_id")
            ),
            "provenance_mode": str(
                _strict_string(provenance.get("mode"), field="provenance_mode")
            ),
            "provenance_pricing_scope": str(
                _strict_string(
                    provenance.get("pricing_scope"),
                    field="provenance_pricing_scope",
                )
            ),
            "provenance_source_key": (
                None
                if provenance.get("source_key") in (None, "")
                else str(
                    _strict_string(
                        provenance.get("source_key"),
                        field="provenance_source_key",
                    )
                )
            ),
        }

    @staticmethod
    def _contract(response_payload: Mapping[str, Any]) -> PageContract:
        """Extract strict query and provenance assertions from one API page."""

        query = _required_mapping(response_payload.get("query"), field="query")
        provenance = _required_mapping(
            response_payload.get("provenance"),
            field="provenance",
        )
        return PageContract(
            result_state=str(
                _strict_string(response_payload.get("result_state"), field="result_state")
            ),
            pricing_scope=str(
                _strict_string(response_payload.get("pricing_scope"), field="pricing_scope")
            ),
            resolved_snapshot_id=str(
                _strict_string(
                    response_payload.get("resolved_snapshot_id"),
                    field="resolved_snapshot_id",
                )
            ),
            **HttpApiFetcher._query_contract_fields(query),
            **HttpApiFetcher._provenance_contract_fields(provenance),
        )

    def _validate_standard_page(
        self,
        response_payload: Mapping[str, Any],
        requested_page_size: int,
        state: _FetchState,
    ) -> tuple[list[Mapping[str, Any]], bool, int, str]:
        page_items = response_payload.get("items")
        if not isinstance(page_items, list):
            raise ApiSchemaError("items_must_be_array")
        if len(page_items) > requested_page_size:
            raise ApiSchemaError("page_exceeds_requested_limit")
        if any(not isinstance(api_item, Mapping) for api_item in page_items):
            raise ApiSchemaError("every_item_must_be_object")
        pagination = _required_mapping(response_payload.get("pagination"), field="pagination")
        returned_offset = _strict_int(pagination.get("offset"), field="pagination_offset")
        returned_limit = _strict_int(pagination.get("limit"), field="pagination_limit")
        total = _strict_int(pagination.get("total"), field="pagination_total")
        if returned_offset != state.offset:
            raise ApiSchemaError("pagination_offset_does_not_match_request")
        if returned_limit != requested_page_size:
            raise ApiSchemaError("pagination_limit_does_not_match_request")
        if returned_offset < 0 or returned_limit < 1 or total < 0:
            raise ApiSchemaError("pagination_values_out_of_range")
        has_more_value = pagination.get("has_more")
        if has_more_value is not None and not isinstance(has_more_value, bool):
            raise ApiSchemaError("pagination_has_more_must_be_boolean")
        has_more = state.offset + len(page_items) < total
        if has_more_value is not None and has_more_value != has_more:
            raise ApiSchemaError("pagination_has_more_disagrees_with_total")
        if total < state.offset + len(page_items):
            raise ApiSchemaError("pagination_total_smaller_than_returned_rows")
        if state.declared_total is not None and total != state.declared_total:
            raise ApiSchemaError("pagination_total_changed_between_pages")
        page_fingerprint = sha256_text(canonical_json(page_items))
        if has_more and not page_items:
            raise ApiSchemaError("pagination_claims_more_after_empty_page")
        if has_more and page_fingerprint in state.seen_page_fingerprints:
            raise ApiSchemaError("pagination_repeated_page")
        return page_items, has_more, total, page_fingerprint

    @staticmethod
    def _completed_fetch(
        state: _FetchState,
        *,
        page_number: int,
        started: float,
    ) -> FetchResult:
        return FetchResult(
            items=tuple(state.response_items),
            contracts=tuple(state.contracts),
            page_latencies_ms=tuple(state.page_latencies_ms),
            total_latency_ms=(time.perf_counter() - started) * 1000.0,
            pages=page_number,
            retries=state.retries,
            response_fingerprint=sha256_text(canonical_json(state.response_items)),
            capacity_observations=tuple(state.capacity_observations),
        )

    def _page_request_parameters(
        self,
        params: Mapping[str, Any],
        requested_page_size: int,
        state: _FetchState,
        page_number: int,
        capacity_plan: _CapacityObservationPlan | None,
    ) -> tuple[dict[str, Any], _CapacityObservationPlan | None]:
        request_param_map = dict(params)
        request_param_map.update({"limit": requested_page_size, "offset": state.offset})
        page_capacity_plan = capacity_plan if page_number == 1 and state.offset == 0 else None
        if page_capacity_plan is not None:
            request_param_map = {
                str(name): str(parameter_value)
                for name, parameter_value in request_param_map.items()
            }
        return request_param_map, page_capacity_plan

    def fetch_all(
        self,
        params: Mapping[str, Any],
        *,
        phase: str,
        page_size: int | None = None,
        capacity_intent: CapacityObservationIntent | None = None,
    ) -> FetchResult:
        """Fetch and validate every page for one exact source API query."""

        del phase
        requested_page_size = self.page_size if page_size is None else page_size
        if requested_page_size < 1:
            raise ConfigurationError("invalid_api_page_size")
        if capacity_intent is not None and requested_page_size != capacity_evidence.CAPACITY_PAGE_LIMIT:
            raise ConfigurationError("capacity_page_limit_must_be_100")
        capacity_plan = self.capacity_collector.plan(capacity_intent) if capacity_intent is not None and self.capacity_collector is not None else None
        if capacity_intent is not None and capacity_plan is None:
            raise ConfigurationError("capacity_collector_missing")
        state = _FetchState()
        started = time.perf_counter()
        for page_number in range(1, self.max_pages + 1):
            request_param_map, page_capacity_plan = self._page_request_parameters(
                params, requested_page_size, state, page_number, capacity_plan
            )
            (
                response_payload,
                latency_ms,
                page_retries,
                page_capacity_observations,
            ) = self._request_page(
                request_param_map,
                capacity_plan=page_capacity_plan,
            )
            page_items, has_more, total, page_fingerprint = self._validate_standard_page(
                response_payload,
                requested_page_size,
                state,
            )
            state.retries += page_retries
            state.page_latencies_ms.append(latency_ms)
            state.capacity_observations.extend(page_capacity_observations)
            state.contracts.append(self._contract(response_payload))
            state.declared_total = total
            state.seen_page_fingerprints.add(page_fingerprint)
            state.response_items.extend(page_items)
            if not has_more:
                if len(state.response_items) != total:
                    raise ApiSchemaError("pagination_total_does_not_match_returned_rows")
                return self._completed_fetch(state, page_number=page_number, started=started)
            state.offset += len(page_items)
        raise ApiSchemaError("pagination_exceeded_max_pages")


@dataclass(frozen=True)
class ExtractedTuples:
    counter: collections.Counter[str]
    tuples: Mapping[str, CanonicalTuple]
    item_count: int
    schema_errors: tuple[str, ...]


def _strict_api_price(raw: Mapping[str, Any], *, field_prefix: str) -> dict[str, Any]:
    normalized_price_by_field: dict[str, Any] = {}
    for field in (
        "negotiated_type",
        "expiration_date",
        "billing_class",
        "setting",
        "additional_information",
    ):
        if field not in raw:
            raise ApiSchemaError(f"{field_prefix}_{field}_missing")
        normalized_price_by_field[field] = _strict_string(
            raw[field],
            field=f"{field_prefix}_{field}",
            nullable=True,
        )
    if "negotiated_rate" not in raw:
        raise ApiSchemaError(f"{field_prefix}_negotiated_rate_missing")
    rate = raw["negotiated_rate"]
    if isinstance(rate, bool) or not isinstance(rate, (int, Decimal)):
        raise ApiSchemaError(f"{field_prefix}_negotiated_rate_must_be_number")
    normalized_price_by_field["negotiated_rate"] = rate
    for field in ("service_code", "billing_code_modifier"):
        if field not in raw or not isinstance(raw[field], list):
            raise ApiSchemaError(f"{field_prefix}_{field}_must_be_array")
        field_values: list[str] = []
        for field_value in raw[field]:
            field_values.append(
                str(
                    _strict_string(
                        field_value,
                        field=f"{field_prefix}_{field}_item",
                    )
                )
            )
        normalized_price_by_field[field] = field_values
    return normalized_price_by_field


def _strict_api_identity(
    raw: Mapping[str, Any],
    *,
    code_system_field: str,
    code_field: str,
    field_prefix: str,
) -> QueryKey:
    code_system_raw = _strict_string(
        raw.get(code_system_field),
        field=f"{field_prefix}_{code_system_field}",
    )
    code_raw = _strict_string(raw.get(code_field), field=f"{field_prefix}_{code_field}")
    npi = _strict_int(raw.get("npi"), field=f"{field_prefix}_npi")
    code_system = canonical_code_system(code_system_raw)
    code = canonical_catalog_code(code_system, code_raw)
    if not code_system or not code or not 1_000_000_000 <= npi <= 9_999_999_999:
        raise ApiSchemaError(f"{field_prefix}_identity_not_canonicalizable")
    return QueryKey(code_system, code, npi)


def _strict_api_source_metadata(
    raw: Mapping[str, Any],
    *,
    field_prefix: str,
    name_field: str,
    description_field: str,
) -> dict[str, Any]:
    """Validate source-visible procedure and per-rate network metadata."""

    field_mapping = {
        "billing_code_type_version": "billing_code_type_version",
        "name": name_field,
        "description": description_field,
    }
    normalized_metadata_map: dict[str, Any] = {}
    for canonical_field, response_field in field_mapping.items():
        if response_field not in raw:
            raise ApiSchemaError(f"{field_prefix}_{response_field}_missing")
        normalized_metadata_map[canonical_field] = _strict_string(
            raw[response_field],
            field=f"{field_prefix}_{response_field}",
            nullable=True,
            empty=True,
        )
    if "network_names" not in raw or not isinstance(raw["network_names"], list):
        raise ApiSchemaError(f"{field_prefix}_network_names_must_be_array")
    network_names: list[str] = []
    for raw_network_name in raw["network_names"]:
        network_name = _strict_string(
            raw_network_name,
            field=f"{field_prefix}_network_names_item",
            empty=True,
        )
        assert network_name is not None
        network_names.append(network_name)
    normalized_metadata_map["network_names"] = network_names
    return normalized_metadata_map


def _canonical_api_price_tuple(
    api_item: Mapping[str, Any],
    raw_price: Mapping[str, Any],
    query: QueryKey,
    *,
    item_index: int,
    price_index: int,
) -> CanonicalTuple:
    field_prefix = f"item_{item_index}_price_{price_index}"
    if "negotiation_arrangement" in raw_price:
        raw_arrangement = raw_price["negotiation_arrangement"]
    elif "negotiation_arrangement" in api_item:
        raw_arrangement = api_item["negotiation_arrangement"]
    else:
        raise ApiSchemaError(f"{field_prefix}_negotiation_arrangement_missing")
    arrangement = _strict_string(
        raw_arrangement,
        field=f"{field_prefix}_negotiation_arrangement",
        nullable=True,
    )
    strict_price = _strict_api_price(raw_price, field_prefix=field_prefix)
    metadata = _strict_api_source_metadata(
        api_item,
        field_prefix=f"item_{item_index}",
        name_field="procedure_name",
        description_field="procedure_description",
    )
    return CanonicalTuple.from_parts(
        query,
        arrangement,
        strict_price,
        **metadata,
    )


def canonical_api_price_tuple(
    api_item: Mapping[str, Any],
    raw_price: Mapping[str, Any],
    query: QueryKey,
) -> CanonicalTuple:
    """Apply the public exact-source response schema and tuple projection."""

    observed_query = _strict_api_identity(
        api_item,
        code_system_field="reported_code_system",
        code_field="reported_code",
        field_prefix="item_0",
    )
    if observed_query != query:
        raise ApiSchemaError("item_0_identity_disagrees_with_query")
    return _canonical_api_price_tuple(
        api_item,
        raw_price,
        query,
        item_index=0,
        price_index=0,
    )


def _api_item_prices(
    api_item: Mapping[str, Any],
    *,
    item_index: int,
) -> tuple[list[Any] | None, str | None]:
    raw_prices = api_item.get("prices")
    if not isinstance(raw_prices, list):
        return None, f"item_{item_index}_prices_not_array"
    if not raw_prices:
        return None, f"item_{item_index}_prices_empty"
    return raw_prices, None


def _extract_api_item_tuples(
    api_item: Mapping[str, Any],
    *,
    item_index: int,
    registry: SourceIdentityRegistry | None,
) -> ExtractedTuples:
    tuple_counts: collections.Counter[str] = collections.Counter()
    canonical_tuples_by_key: dict[str, CanonicalTuple] = {}
    schema_errors: list[str] = []
    try:
        query = _strict_api_identity(
            api_item,
            code_system_field="reported_code_system",
            code_field="reported_code",
            field_prefix=f"item_{item_index}",
        )
        source_identity = extract_source_identity(
            api_item,
            field_prefix=f"item_{item_index}",
            registry=registry,
        )
    except ApiSchemaError as exc:
        return ExtractedTuples(tuple_counts, canonical_tuples_by_key, 1, (str(exc),))
    raw_prices, price_error = _api_item_prices(api_item, item_index=item_index)
    if price_error is not None:
        return ExtractedTuples(tuple_counts, canonical_tuples_by_key, 1, (price_error,))
    assert raw_prices is not None
    for price_index, raw_price in enumerate(raw_prices):
        if not isinstance(raw_price, Mapping):
            schema_errors.append(f"item_{item_index}_price_{price_index}_not_object")
            continue
        try:
            canonical_tuple = _canonical_api_price_tuple(
                api_item,
                raw_price,
                query,
                item_index=item_index,
                price_index=price_index,
            )
        except (ApiSchemaError, SourceCoverageError, ValueError) as exc:
            schema_errors.append(
                str(exc)
                if isinstance(exc, ApiSchemaError)
                else f"item_{item_index}_price_{price_index}_not_canonical"
            )
            continue
        occurrence_key = canonical_occurrence_key(
            canonical_tuple,
            source_identity.raw_container_sha256,
        )
        tuple_counts[occurrence_key] += 1
        canonical_tuples_by_key[occurrence_key] = canonical_tuple
    return ExtractedTuples(tuple_counts, canonical_tuples_by_key, 1, tuple(schema_errors))


def extract_api_tuples(
    api_items: Sequence[Mapping[str, Any]],
    *,
    registry: SourceIdentityRegistry | None = None,
) -> ExtractedTuples:
    """Extract the exact canonical tuple multiset from standard API items."""

    tuple_counts: collections.Counter[str] = collections.Counter()
    canonical_tuples_by_key: dict[str, CanonicalTuple] = {}
    schema_errors: list[str] = []
    for item_index, api_item in enumerate(api_items):
        extracted_item = _extract_api_item_tuples(
            api_item,
            item_index=item_index,
            registry=registry,
        )
        tuple_counts.update(extracted_item.counter)
        canonical_tuples_by_key.update(extracted_item.tuples)
        schema_errors.extend(extracted_item.schema_errors)
    return ExtractedTuples(
        counter=tuple_counts,
        tuples=canonical_tuples_by_key,
        item_count=len(api_items),
        schema_errors=tuple(schema_errors),
    )


def extract_api_occurrence(
    occurrence_payload: Mapping[str, Any],
    *,
    registry: SourceIdentityRegistry | None = None,
) -> ApiOccurrence:
    """Validate one persisted audit-sample occurrence and its source identity."""

    occurrence_id = _strict_string(
        occurrence_payload.get("occurrence_id"),
        field="occurrence_id",
    )
    assert occurrence_id is not None
    if len(occurrence_id) != 64 or any(character not in "0123456789abcdef" for character in occurrence_id):
        raise ApiSchemaError("occurrence_id_must_be_lowercase_sha256")
    raw_tuple = _required_mapping(
        occurrence_payload.get("tuple"),
        field="occurrence_tuple",
    )
    query = _strict_api_identity(
        raw_tuple,
        code_system_field="code_system",
        code_field="code",
        field_prefix="occurrence_tuple",
    )
    if "negotiation_arrangement" not in raw_tuple:
        raise ApiSchemaError("occurrence_tuple_negotiation_arrangement_missing")
    arrangement = _strict_string(
        raw_tuple["negotiation_arrangement"],
        field="occurrence_tuple_negotiation_arrangement",
        nullable=True,
    )
    price = _strict_api_price(raw_tuple, field_prefix="occurrence_tuple")
    metadata = _strict_api_source_metadata(
        raw_tuple,
        field_prefix="occurrence_tuple",
        name_field="name",
        description_field="description",
    )
    try:
        canonical_tuple = CanonicalTuple.from_parts(
            query,
            arrangement,
            price,
            **metadata,
        )
    except (SourceCoverageError, ValueError) as exc:
        raise ApiSchemaError("occurrence_tuple_not_canonical") from exc
    source_identity = extract_source_identity(
        occurrence_payload,
        field_prefix="occurrence",
        registry=registry,
    )
    return ApiOccurrence(
        occurrence_id=occurrence_id,
        canonical_tuple=canonical_tuple,
        source_identity=source_identity,
    )


def _tuple_payload(stable_key: str) -> dict[str, Any]:
    payload = json.loads(stable_key)
    if not isinstance(payload, dict):
        raise AssertionError("canonical tuple key must contain an object")
    return payload


def _mismatched_fields(expected_key: str, actual_key: str) -> list[str]:
    expected = _tuple_payload(expected_key)
    actual = _tuple_payload(actual_key)
    return [field for field in TUPLE_FIELDS if expected.get(field) != actual.get(field)]


@dataclass(frozen=True)
class CounterComparison:
    exact_matches: int
    failure_counts: Mapping[str, int]
    examples: tuple[dict[str, Any], ...]


@dataclass
class _CounterComparisonState:
    exact_matches: int = 0
    failure_counts: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    examples: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    expected_only: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    actual_only: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )


def _partition_tuple_counts(
    query: QueryKey,
    expected_counts: collections.Counter[str],
    actual_counts: collections.Counter[str],
    state: _CounterComparisonState,
    *,
    example_limit: int,
) -> None:
    for tuple_key in sorted(set(expected_counts) | set(actual_counts)):
        expected_count = expected_counts.get(tuple_key, 0)
        actual_count = actual_counts.get(tuple_key, 0)
        state.exact_matches += min(expected_count, actual_count)
        if expected_count and actual_count:
            if expected_count != actual_count:
                state.failure_counts["duplicate_count"] += abs(
                    expected_count - actual_count
                )
                if len(state.examples) < example_limit:
                    state.examples.append(
                        {
                            "category": "duplicate_count",
                            "query": query.report_value,
                            "tuple_fingerprint": fingerprint(tuple_key),
                            "source_count": expected_count,
                            "api_count": actual_count,
                        }
                    )
            continue
        if expected_count:
            state.expected_only[tuple_key] = expected_count
        if actual_count:
            state.actual_only[tuple_key] = actual_count


def _nearest_alteration(
    expected_only: collections.Counter[str],
    actual_key: str,
) -> tuple[str, list[str]] | None:
    actual_payload = _tuple_payload(actual_key)
    candidates: list[tuple[int, str, list[str]]] = []
    for expected_key in expected_only:
        expected_payload = _tuple_payload(expected_key)
        has_different_identity = any(
            expected_payload.get(field) != actual_payload.get(field)
            for field in ("code_system", "code", "npi")
        )
        if has_different_identity:
            continue
        mismatched_fields = _mismatched_fields(expected_key, actual_key)
        if 1 <= len(mismatched_fields) <= 3:
            candidates.append((len(mismatched_fields), expected_key, mismatched_fields))
    if not candidates:
        return None
    _distance, expected_key, mismatched_fields = min(
        candidates,
        key=lambda candidate: (candidate[0], fingerprint(candidate[1])),
    )
    return expected_key, mismatched_fields


def _pair_altered_counts(
    query: QueryKey,
    state: _CounterComparisonState,
    *,
    example_limit: int,
) -> None:
    for actual_key in sorted(list(state.actual_only), key=fingerprint):
        while state.actual_only[actual_key] > 0 and state.expected_only:
            alteration = _nearest_alteration(state.expected_only, actual_key)
            if alteration is None:
                break
            expected_key, mismatched_fields = alteration
            paired_count = min(
                state.actual_only[actual_key],
                state.expected_only[expected_key],
            )
            state.failure_counts["altered"] += paired_count
            state.actual_only[actual_key] -= paired_count
            state.expected_only[expected_key] -= paired_count
            if state.expected_only[expected_key] == 0:
                del state.expected_only[expected_key]
            if len(state.examples) < example_limit:
                state.examples.append(
                    {
                        "category": "altered",
                        "query": query.report_value,
                        "source_tuple_fingerprint": fingerprint(expected_key),
                        "api_tuple_fingerprint": fingerprint(actual_key),
                        "count": paired_count,
                        "mismatched_fields": mismatched_fields,
                    }
                )


def _record_unpaired_counts(
    query: QueryKey,
    category: str,
    unpaired_counts: collections.Counter[str],
    state: _CounterComparisonState,
    *,
    example_limit: int,
) -> None:
    for tuple_key, count in sorted(
        unpaired_counts.items(),
        key=lambda count_entry: fingerprint(count_entry[0]),
    ):
        if count <= 0:
            continue
        state.failure_counts[category] += count
        if len(state.examples) < example_limit:
            state.examples.append(
                {
                    "category": category,
                    "query": query.report_value,
                    "tuple_fingerprint": fingerprint(tuple_key),
                    "count": count,
                }
            )


def compare_tuple_counters(
    query: QueryKey,
    expected_counts: collections.Counter[str],
    actual_counts: collections.Counter[str],
    *,
    example_limit: int = 20,
) -> CounterComparison:
    """Classify exact, duplicate, altered, missing, and extra tuple counts."""

    state = _CounterComparisonState()
    _partition_tuple_counts(
        query,
        expected_counts,
        actual_counts,
        state,
        example_limit=example_limit,
    )
    _pair_altered_counts(query, state, example_limit=example_limit)
    _record_unpaired_counts(
        query,
        "missing",
        state.expected_only,
        state,
        example_limit=example_limit,
    )
    _record_unpaired_counts(
        query,
        "extra_fake",
        state.actual_only,
        state,
        example_limit=example_limit,
    )
    return CounterComparison(
        state.exact_matches,
        dict(state.failure_counts),
        tuple(state.examples),
    )


def _is_https_api_origin(api_base_url: str) -> bool:
    try:
        parsed_origin = urlsplit(api_base_url)
    except ValueError:
        return False
    return (
        parsed_origin.scheme == "https"
        and bool(parsed_origin.netloc)
        and parsed_origin.path in {"", "/"}
        and not parsed_origin.query
        and not parsed_origin.fragment
        and parsed_origin.username is None
        and parsed_origin.password is None
    )


def _is_cluster_http_api_origin(api_base_url: str) -> bool:
    """Accept only Kubernetes service DNS names for explicit cluster HTTP."""

    try:
        parsed_origin = urlsplit(api_base_url)
    except ValueError:
        return False
    hostname = str(parsed_origin.hostname or "").rstrip(".").lower()
    has_origin_shape = (
        parsed_origin.scheme == "http"
        and bool(parsed_origin.netloc)
        and parsed_origin.path in {"", "/"}
        and not parsed_origin.query
        and not parsed_origin.fragment
        and parsed_origin.username is None
        and parsed_origin.password is None
    )
    if not has_origin_shape or not hostname:
        return False
    labels = hostname.split(".")
    if len(labels) == 1:
        return _is_dns_label(labels[0])
    if hostname.endswith(".svc") or hostname.endswith(".svc.cluster.local"):
        return all(_is_dns_label(label) for label in labels)
    return False


def _is_dns_label(value: str) -> bool:
    return (
        0 < len(value) <= 63
        and value.isascii()
        and value[0].isalnum()
        and value[-1].isalnum()
        and all(character.isalnum() or character == "-" for character in value)
    )


@dataclass(frozen=True)
class AuditConfig:
    profile: str
    api_base_url: str
    api_path: str
    api_audit_path: str
    plan_id: str
    snapshot_id: str
    plan_market_type: str | None
    source_key: str | None
    seed: str
    source_occurrence_samples: int
    api_occurrence_samples: int
    negative_samples: int
    random_api_calls: int
    random_api_max_limit: int
    min_source_occurrence_checks: int
    min_api_occurrence_checks: int
    min_negative_checks: int
    min_random_api_calls: int
    min_resolved_rate_fraction: float
    max_unresolved_provider_references: int
    max_invalid_prices: int
    max_invalid_npis: int
    max_invalid_field_types: int
    page_size: int
    max_pages: int
    api_audit_page_size: int
    api_audit_max_pages: int
    warm_repeats: int
    concurrency: int
    failure_example_limit: int
    verify_tls: bool
    validated_candidate: bool = False
    trusted_cluster_http: bool = False
    max_first_page_first_observation_p95_ms: float = RELEASE_MAX_FIRST_PAGE_P95_MS
    max_logical_query_p95_ms: float = RELEASE_MAX_LOGICAL_QUERY_P95_MS
    capacity_evidence_trust: CapacityEvidenceTrust | None = None
    capacity_contention_run_id: str | None = None

    def __post_init__(self) -> None:
        """Reject invalid settings and enforce immutable release-profile floors."""

        if self.profile not in {"release", "diagnostic"}:
            raise ConfigurationError("unknown_audit_profile")
        self._validate_numeric_bounds()
        self._validate_sample_targets()
        self._validate_capacity_evidence_contract()
        sampling_seed_evidence(
            self.profile,
            self.seed,
            self.capacity_evidence_trust,
        )
        if self.profile != "release":
            return
        for reason, is_violated in self._release_rules_by_reason().items():
            if is_violated:
                raise ConfigurationError(reason)

    def _validate_numeric_bounds(self) -> None:
        positive_integer_values = (
            self.source_occurrence_samples,
            self.api_occurrence_samples,
            self.negative_samples,
            self.random_api_calls,
            self.random_api_max_limit,
            self.page_size,
            self.max_pages,
            self.api_audit_page_size,
            self.api_audit_max_pages,
            self.warm_repeats,
            self.concurrency,
        )
        if any(configured_value < 1 for configured_value in positive_integer_values):
            raise ConfigurationError("positive_configuration_value_required")
        if self.random_api_max_limit > self.page_size:
            raise ConfigurationError("random_api_max_limit_exceeds_page_size")
        nonnegative_values = (
            self.min_source_occurrence_checks,
            self.min_api_occurrence_checks,
            self.min_negative_checks,
            self.min_random_api_calls,
            self.max_unresolved_provider_references,
            self.max_invalid_prices,
            self.max_invalid_npis,
            self.max_invalid_field_types,
            self.failure_example_limit,
        )
        if any(configured_value < 0 for configured_value in nonnegative_values):
            raise ConfigurationError("nonnegative_configuration_value_required")

    def _validate_sample_targets(self) -> None:
        if (
            self.source_occurrence_samples < self.min_source_occurrence_checks
            or self.api_occurrence_samples < self.min_api_occurrence_checks
            or self.negative_samples < self.min_negative_checks
            or self.random_api_calls < self.min_random_api_calls
        ):
            raise ConfigurationError("sample_target_below_required_checks")
        if (
            not math.isfinite(self.max_first_page_first_observation_p95_ms)
            or not math.isfinite(self.max_logical_query_p95_ms)
            or self.max_first_page_first_observation_p95_ms <= 0
            or self.max_logical_query_p95_ms <= 0
        ):
            raise ConfigurationError("latency_limits_must_be_positive")
        if not math.isfinite(self.min_resolved_rate_fraction) or not (
            0.0 <= self.min_resolved_rate_fraction <= 1.0
        ):
            raise ConfigurationError("resolved_rate_fraction_out_of_range")

    def _validate_capacity_evidence_contract(self) -> None:
        if self.capacity_evidence_trust is None:
            if self.capacity_contention_run_id is not None:
                raise ConfigurationError(
                    "capacity_contention_run_requires_evidence_trust"
                )
            return
        contention_run_id = self.capacity_contention_run_id
        if (
            not isinstance(contention_run_id, str)
            or len(contention_run_id) != 64
            or contention_run_id == "0" * 64
            or any(
                character not in "0123456789abcdef"
                for character in contention_run_id
            )
        ):
            raise ConfigurationError("capacity_contention_run_id_invalid")
        if self.validated_candidate:
            raise ConfigurationError(
                "capacity_evidence_requires_published_standard_route"
            )
        if self.api_path != DEFAULT_API_PATH:
            raise ConfigurationError("capacity_evidence_requires_standard_api_path")
        if self.plan_market_type is not None or self.source_key is not None:
            raise ConfigurationError(
                "capacity_evidence_rejects_optional_scope_parameters"
            )
        if self.page_size != capacity_evidence.CAPACITY_PAGE_LIMIT:
            raise ConfigurationError("capacity_page_limit_must_be_100")
        if self.random_api_max_limit != capacity_evidence.CAPACITY_PAGE_LIMIT:
            raise ConfigurationError("capacity_random_page_limit_must_be_100")
        if self.random_api_calls < CAPACITY_MIN_DISTINCT_RANDOM_QUERIES:
            raise ConfigurationError(
                "capacity_random_query_target_below_minimum"
            )

    def _release_rules_by_reason(self) -> dict[str, bool]:
        """Return release constraints keyed by their stable failure reason."""

        origin_rules_by_reason = self._release_origin_rules_by_reason()
        return {
            **origin_rules_by_reason,
            "release_published_capacity_evidence_required": (
                not self.validated_candidate
                and self.capacity_evidence_trust is None
            ),
            "release_tls_verification_required": not self.verify_tls,
            "release_standard_api_path_required": self.api_path
            != (
                DEFAULT_CANDIDATE_API_PATH
                if self.validated_candidate
                else DEFAULT_API_PATH
            ),
            "release_audit_api_path_required": (
                self.api_audit_path != DEFAULT_API_AUDIT_PATH
            ),
            "release_source_occurrence_sample_floor": (
                self.source_occurrence_samples < RELEASE_MIN_SOURCE_OCCURRENCES
                or self.min_source_occurrence_checks < RELEASE_MIN_SOURCE_OCCURRENCES
            ),
            "release_api_occurrence_sample_floor": (
                self.api_occurrence_samples < RELEASE_MIN_API_OCCURRENCES
                or self.min_api_occurrence_checks < RELEASE_MIN_API_OCCURRENCES
            ),
            "release_negative_query_floor": self.min_negative_checks < RELEASE_MIN_NEGATIVE_QUERIES,
            "release_random_api_call_floor": (
                self.random_api_calls < RELEASE_MIN_RANDOM_API_CALLS
                or self.min_random_api_calls < RELEASE_MIN_RANDOM_API_CALLS
            ),
            "release_total_api_call_floor": (
                self.random_api_calls + self.negative_samples < RELEASE_MIN_TOTAL_API_CALLS
            ),
            "release_first_page_latency_ceiling": (
                self.max_first_page_first_observation_p95_ms > RELEASE_MAX_FIRST_PAGE_P95_MS
            ),
            "release_logical_query_latency_ceiling": (
                self.max_logical_query_p95_ms > RELEASE_MAX_LOGICAL_QUERY_P95_MS
            ),
            "release_resolved_rate_floor": self.min_resolved_rate_fraction != 1.0,
            "release_source_integrity_ceiling": any(
                configured_value != 0
                for configured_value in (
                    self.max_unresolved_provider_references,
                    self.max_invalid_prices,
                    self.max_invalid_field_types,
                )
            )
            or (not self.validated_candidate and self.max_invalid_npis != 0),
        }

    def _release_origin_rules_by_reason(self) -> dict[str, bool]:
        """Keep public HTTPS strict while allowing an explicit cluster service."""

        is_https_origin = _is_https_api_origin(self.api_base_url)
        is_cluster_http_origin = _is_cluster_http_api_origin(self.api_base_url)
        is_trusted_cluster_origin = (
            self.validated_candidate
            and self.trusted_cluster_http
            and is_cluster_http_origin
        )
        return {
            "release_api_base_url_must_be_https_origin": not (
                is_https_origin or is_trusted_cluster_origin
            ),
            "release_cluster_http_requires_validated_candidate": (
                self.trusted_cluster_http and not self.validated_candidate
            ),
            "release_cluster_http_origin_required": (
                self.trusted_cluster_http and not is_cluster_http_origin
            ),
        }

    def api_params(self, query: QueryKey) -> dict[str, Any]:
        """Return strict standard-API parameters for one exact source query."""

        query_param_map: dict[str, Any] = {
            "plan_id": self.plan_id,
            "snapshot_id": self.snapshot_id,
            "mode": EXPECTED_QUERY_MODE,
            "code_system": query.code_system,
            "code": query.code,
            "npi": query.npi,
            "include_providers": "true",
            "include_details": "true",
            "include_sources": "true",
            "include_allowed_amounts": "false",
            "include_unverified_addresses": "true",
            "order": "asc",
            "order_by": "npi",
        }
        if self.plan_market_type:
            query_param_map["plan_market_type"] = self.plan_market_type
        if self.source_key:
            query_param_map["source_key"] = self.source_key
        return query_param_map

    def api_audit_params(self) -> dict[str, Any]:
        """Return parameters that enumerate the persisted occurrence sample."""

        audit_param_map: dict[str, Any] = {
            "plan_id": self.plan_id,
            "snapshot_id": self.snapshot_id,
            "mode": EXPECTED_QUERY_MODE,
            "order_by": "occurrence_id",
            "order": "asc",
        }
        if self.plan_market_type:
            audit_param_map["plan_market_type"] = self.plan_market_type
        if self.source_key:
            audit_param_map["source_key"] = self.source_key
        return audit_param_map

    def redacted_target(self) -> dict[str, Any]:
        """Return target metadata with every sensitive identifier hashed."""

        parsed = urlsplit(self.api_base_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        is_https_origin = _is_https_api_origin(self.api_base_url)
        is_trusted_cluster_origin = (
            self.validated_candidate
            and self.trusted_cluster_http
            and _is_cluster_http_api_origin(self.api_base_url)
        )
        return {
            "expected_architecture": EXPECTED_ARCHITECTURE,
            "expected_storage_generation": EXPECTED_STORAGE_GENERATION,
            "expected_database_backend": EXPECTED_DATABASE_BACKEND,
            "architecture_assertion": "required_postgresql_session_evidence",
            "api_origin_sha256": sha256_text(origin),
            "api_path_sha256": sha256_text(self.api_path),
            "api_audit_path_sha256": sha256_text(self.api_audit_path),
            "endpoint_contract": "pricing.providers.search_by_procedure",
            "audit_endpoint_contract": AUDIT_SAMPLE_CONTRACT,
            "plan_id_sha256": sha256_text(self.plan_id),
            "snapshot_id_sha256": sha256_text(self.snapshot_id),
            "market_type_sha256": sha256_text(self.plan_market_type) if self.plan_market_type else None,
            "source_key_sha256": sha256_text(self.source_key) if self.source_key else None,
            "expected_snapshot_lifecycle": (
                "validated" if self.validated_candidate else "published"
            ),
            "tls_verified": self.verify_tls and is_https_origin,
            "transport_contract": (
                VERIFIED_HTTPS_TRANSPORT
                if is_https_origin
                else TRUSTED_CLUSTER_HTTP_TRANSPORT
                if is_trusted_cluster_origin
                else None
            ),
            "capacity_evidence_enabled": self.capacity_evidence_trust is not None,
        }


@dataclass(frozen=True)
class QueryAuditResult:
    query: QueryKey
    expected: collections.Counter[str]
    actual: collections.Counter[str]
    actual_tuples: Mapping[str, CanonicalTuple]
    item_count: int
    comparison: CounterComparison | None
    failure_counts: Mapping[str, int]
    examples: tuple[dict[str, Any], ...]
    schema_errors: tuple[str, ...]
    matched_initial_traversal: bool
    matched_first_observation: bool
    first_page_first_observation_ms: tuple[float, ...]
    cold_page_ms: tuple[float, ...]
    cold_query_ms: tuple[float, ...]
    warm_page_ms: tuple[float, ...]
    warm_query_ms: tuple[float, ...]
    retries: int
    capacity_observations: tuple[CapacityHttpObservation, ...] = ()


def _safe_http_example(query: QueryKey, exc: AuditError) -> dict[str, Any]:
    safe_example_by_field: dict[str, Any] = {
        "category": exc.code,
        "query": query.report_value,
        "reason": exc.reason if isinstance(exc, ApiError) else exc.code,
    }
    if isinstance(exc, ApiError):
        safe_example_by_field["status_code"] = exc.status_code
        safe_example_by_field["body_sha256"] = exc.body_sha256
    return safe_example_by_field


def _validate_contracts(
    contracts: Sequence[PageContract],
    config: AuditConfig,
    *,
    positive: bool,
) -> list[str]:
    errors: list[str] = []
    if not contracts:
        return ["response_has_no_page_contract"]
    for contract in contracts:
        if contract.pricing_scope != EXPECTED_PRICING_SCOPE:
            errors.append("unexpected_pricing_scope")
        if contract.query_mode != EXPECTED_QUERY_MODE:
            errors.append("mode_not_exact_source")
        if contract.query_snapshot_id != config.snapshot_id:
            errors.append("query_snapshot_mismatch")
        if contract.query_plan_id != config.plan_id:
            errors.append("query_plan_mismatch")
        if contract.resolved_snapshot_id != config.snapshot_id:
            errors.append("response_not_pinned_to_snapshot")
        if contract.provenance_arch_version != EXPECTED_ARCHITECTURE:
            errors.append("provenance_arch_version_mismatch")
        if contract.provenance_storage_generation != EXPECTED_STORAGE_GENERATION:
            errors.append("provenance_storage_generation_mismatch")
        if contract.provenance_database_backend != EXPECTED_DATABASE_BACKEND:
            errors.append("provenance_database_backend_mismatch")
        if (
            contract.provenance_database_evidence_contract
            != DATABASE_EVIDENCE_CONTRACT
            or contract.provenance_postgres_server_version_num < 10000
            or not contract.provenance_database_selected
            or not contract.provenance_backend_session_active
            or not contract.provenance_transaction_snapshot_observed
        ):
            errors.append("provenance_database_evidence_mismatch")
        if contract.provenance_plan_id != config.plan_id:
            errors.append("provenance_plan_mismatch")
        if contract.provenance_snapshot_id != config.snapshot_id:
            errors.append("provenance_snapshot_mismatch")
        if contract.provenance_mode != EXPECTED_QUERY_MODE:
            errors.append("provenance_mode_mismatch")
        if contract.provenance_pricing_scope != EXPECTED_PRICING_SCOPE:
            errors.append("provenance_pricing_scope_mismatch")
        if config.source_key:
            if contract.query_source_key != config.source_key:
                errors.append("query_source_key_mismatch")
            if contract.provenance_source_key != config.source_key:
                errors.append("provenance_source_key_mismatch")
        if positive and contract.result_state != "matched":
            errors.append("positive_response_not_matched")
        if not positive and contract.result_state not in {"no_match_in_radius", "no_matching_rates"}:
            errors.append("negative_response_has_unexpected_result_state")
    return errors


def _new_audit_sample_digest() -> Any:
    digest = hashlib.sha256()
    digest.update(AUDIT_SAMPLE_DIGEST_DOMAIN)
    return digest


def _audit_digest_coordinate_bytes(
    raw_occurrence: Mapping[str, Any],
    occurrence: ApiOccurrence,
) -> bytes:
    coordinates = _required_mapping(
        raw_occurrence.get("digest_coordinates"),
        field="audit_digest_coordinates",
    )
    if set(coordinates) != set(AUDIT_SAMPLE_DIGEST_COORDINATE_FIELDS):
        raise ApiSchemaError("audit_digest_coordinates_have_unexpected_fields")
    coordinate_values = tuple(
        _strict_int(
            coordinates.get(field_name),
            field=f"audit_digest_{field_name}",
        )
        for field_name in AUDIT_SAMPLE_DIGEST_COORDINATE_FIELDS
    )
    coordinate_bits = (32, 32, 32, 64, 64, 64, 64)
    for field_name, coordinate_value, bit_count in zip(
        AUDIT_SAMPLE_DIGEST_COORDINATE_FIELDS,
        coordinate_values,
        coordinate_bits,
        strict=True,
    ):
        if coordinate_value < 0 or coordinate_value >= 1 << bit_count:
            raise ApiSchemaError(f"audit_digest_{field_name}_out_of_range")
    if coordinate_values[3] != occurrence.source_identity.source_artifact_key:
        raise ApiSchemaError("audit_digest_source_artifact_key_mismatch")
    if coordinate_values[4] != occurrence.canonical_tuple.npi:
        raise ApiSchemaError("audit_digest_npi_mismatch")
    return _AUDIT_SAMPLE_DIGEST_COORDINATES.pack(*coordinate_values)


@dataclass(frozen=True)
class _AuditSampleMetadata:
    contract: str
    format_version: int
    method: str
    sample_count: int
    sample_digest: str
    occurrence_identity: str
    multiplicity: str
    is_complete_population: bool


@dataclass
class _ApiSampleState:
    sampler: BottomKSampler[ApiOccurrence]
    identity_registry: SourceIdentityRegistry = dataclasses.field(
        default_factory=SourceIdentityRegistry
    )
    offset: int = 0
    retries: int = 0
    declared_total: int | None = None
    declared_metadata: _AuditSampleMetadata | None = None
    declared_source_set: SourceSetEvidence | None = None
    last_occurrence_id: str | None = None
    observed_count: int = 0
    observed_digest: Any = dataclasses.field(default_factory=_new_audit_sample_digest)


class HttpApiOccurrenceSource:
    """Enumerate a persisted publish-time API audit sample with bounded sampling."""

    def __init__(
        self,
        fetcher: HttpApiFetcher,
        config: AuditConfig,
        expected_source_set: SourceSetEvidence,
    ):
        self.fetcher = fetcher
        self.config = config
        self.expected_source_set = expected_source_set
        self._source_set_validated = False
        self._preflight_retries = 0

    def _validated_source_set(
        self,
        response_payload: Mapping[str, Any],
    ) -> SourceSetEvidence:
        observed = extract_source_set_evidence(response_payload)
        if observed != self.expected_source_set:
            raise ApiSchemaError(
                "snapshot_source_set_does_not_match_supplied_sources"
            )
        self._source_set_validated = True
        return observed

    def validate_source_set(self) -> bool:
        """Validate exact source coverage before local or API occurrence sampling."""

        if self._source_set_validated:
            return True
        request_params = self.config.api_audit_params()
        request_params.update({"limit": 1, "offset": 0})
        response_payload, _latency_ms, retries, _capacity_observations = (
            self.fetcher._request_page(request_params)
        )
        self._preflight_retries += retries
        preflight_items = response_payload.get("items")
        if not isinstance(preflight_items, list) or len(preflight_items) > 1:
            raise ApiSchemaError("audit_source_set_preflight_items_invalid")
        pagination = _required_mapping(
            response_payload.get("pagination"),
            field="audit_source_set_preflight_pagination",
        )
        if (
            _strict_int(
                pagination.get("offset"),
                field="audit_source_set_preflight_offset",
            )
            != 0
            or _strict_int(
                pagination.get("limit"),
                field="audit_source_set_preflight_limit",
            )
            != 1
        ):
            raise ApiSchemaError("audit_source_set_preflight_pagination_mismatch")
        total = _strict_int(
            pagination.get("total"),
            field="audit_source_set_preflight_total",
        )
        if total < len(preflight_items):
            raise ApiSchemaError("audit_source_set_preflight_total_out_of_range")
        metadata = self._extract_sample_metadata(response_payload)
        self._validate_sample_metadata(metadata, total)
        self._validate_audit_contract(response_payload, total)
        self._validated_source_set(response_payload)
        return True

    def _validate_audit_pagination(
        self,
        response_payload: Mapping[str, Any],
        state: _ApiSampleState,
    ) -> tuple[list[Any], Mapping[str, Any], int]:
        page_items = response_payload.get("items")
        if not isinstance(page_items, list):
            raise ApiSchemaError("audit_items_must_be_array")
        if len(page_items) > self.config.api_audit_page_size:
            raise ApiSchemaError("audit_page_exceeds_requested_limit")
        pagination = _required_mapping(
            response_payload.get("pagination"),
            field="audit_pagination",
        )
        returned_offset = _strict_int(
            pagination.get("offset"),
            field="audit_pagination_offset",
        )
        returned_limit = _strict_int(
            pagination.get("limit"),
            field="audit_pagination_limit",
        )
        total = _strict_int(pagination.get("total"), field="audit_pagination_total")
        if returned_offset != state.offset:
            raise ApiSchemaError("audit_pagination_offset_does_not_match_request")
        if returned_limit != self.config.api_audit_page_size:
            raise ApiSchemaError("audit_pagination_limit_does_not_match_request")
        if total < state.offset + len(page_items):
            raise ApiSchemaError("audit_pagination_total_smaller_than_returned_rows")
        if state.declared_total is not None and total != state.declared_total:
            raise ApiSchemaError("audit_pagination_total_changed_between_pages")
        return page_items, pagination, total

    @staticmethod
    def _extract_sample_metadata(
        response_payload: Mapping[str, Any],
    ) -> _AuditSampleMetadata:
        audit_sample = _required_mapping(response_payload.get("audit_sample"), field="audit_sample")
        is_complete_population = audit_sample.get("complete_population")
        if type(is_complete_population) is not bool:
            raise ApiSchemaError("audit_sample_complete_population_must_be_boolean")
        return _AuditSampleMetadata(
            contract=str(_strict_string(audit_sample.get("contract"), field="audit_sample_contract")),
            format_version=_strict_int(
                audit_sample.get("format_version"),
                field="audit_sample_format_version",
            ),
            method=str(_strict_string(audit_sample.get("method"), field="audit_sample_method")),
            sample_count=_strict_int(
                audit_sample.get("sample_count"),
                field="audit_sample_count",
            ),
            sample_digest=_strict_sha256(
                audit_sample.get("sample_digest"),
                field="audit_sample_digest",
            ),
            occurrence_identity=str(
                _strict_string(
                    audit_sample.get("occurrence_identity"),
                    field="audit_sample_occurrence_identity",
                )
            ),
            multiplicity=str(
                _strict_string(
                    audit_sample.get("serving_multiplicity_semantics"),
                    field="audit_sample_serving_multiplicity_semantics",
                )
            ),
            is_complete_population=is_complete_population,
        )

    @staticmethod
    def _validate_sample_metadata(metadata: _AuditSampleMetadata, total: int) -> None:
        expected_values = (
            (metadata.contract, AUDIT_SAMPLE_CONTRACT, "audit_sample_contract_mismatch"),
            (metadata.method, AUDIT_SAMPLE_METHOD, "audit_sample_method_mismatch"),
            (
                metadata.format_version,
                AUDIT_SAMPLE_FORMAT_VERSION,
                "audit_sample_format_version_mismatch",
            ),
            (
                metadata.occurrence_identity,
                AUDIT_SAMPLE_OCCURRENCE_IDENTITY,
                "audit_sample_occurrence_identity_mismatch",
            ),
            (
                metadata.multiplicity,
                AUDIT_SAMPLE_MULTIPLICITY,
                "audit_sample_multiplicity_semantics_mismatch",
            ),
        )
        for observed_value, expected_value, error_code in expected_values:
            if observed_value != expected_value:
                raise ApiSchemaError(error_code)
        if metadata.is_complete_population:
            raise ApiSchemaError("audit_sample_must_not_claim_complete_population")
        if metadata.sample_count < 0:
            raise ApiSchemaError("audit_sample_count_out_of_range")
        if metadata.sample_count != total:
            raise ApiSchemaError("audit_sample_count_does_not_match_pagination_total")

    def _has_more_audit_items(
        self,
        pagination: Mapping[str, Any],
        state: _ApiSampleState,
        page_items: Sequence[Any],
        total: int,
    ) -> bool:
        has_more_value = pagination.get("has_more")
        if has_more_value is not None and not isinstance(has_more_value, bool):
            raise ApiSchemaError("audit_pagination_has_more_must_be_boolean")
        has_more = state.offset + len(page_items) < total
        if has_more_value is not None and has_more_value != has_more:
            raise ApiSchemaError("audit_pagination_has_more_disagrees_with_total")
        if has_more and not page_items:
            raise ApiSchemaError("audit_pagination_claims_more_after_empty_page")
        return has_more

    def _validate_audit_contract(
        self,
        response_payload: Mapping[str, Any],
        total: int,
    ) -> None:
        contract_errors = _validate_contracts(
            (self.fetcher._contract(response_payload),),
            self.config,
            positive=total > 0,
        )
        if contract_errors:
            raise ApiSchemaError(contract_errors[0])

    @staticmethod
    def _consume_audit_items(page_items: Sequence[Any], state: _ApiSampleState) -> None:
        for raw_occurrence in page_items:
            if not isinstance(raw_occurrence, Mapping):
                raise ApiSchemaError("every_audit_item_must_be_object")
            occurrence = extract_api_occurrence(
                raw_occurrence,
                registry=state.identity_registry,
            )
            if (
                state.last_occurrence_id is not None
                and occurrence.occurrence_id <= state.last_occurrence_id
            ):
                raise ApiSchemaError("audit_occurrence_ids_not_strictly_increasing")
            digest_coordinate_bytes = _audit_digest_coordinate_bytes(
                raw_occurrence,
                occurrence,
            )
            state.last_occurrence_id = occurrence.occurrence_id
            state.observed_digest.update(bytes.fromhex(occurrence.occurrence_id))
            state.observed_digest.update(digest_coordinate_bytes)
            state.sampler.offer(occurrence.occurrence_id, occurrence)
            state.observed_count += 1

    def sample_occurrences(
        self,
        *,
        sample_target: int,
        seed: str,
    ) -> ApiOccurrenceSample:
        """Validate the persisted sample and select deterministic occurrences."""

        state = _ApiSampleState(
            BottomKSampler(sample_target, seed=seed, namespace="api-occurrence-id")
        )
        base_params = self.config.api_audit_params()
        for page_number in range(1, self.config.api_audit_max_pages + 1):
            request_param_map = dict(base_params)
            request_param_map.update(
                {"limit": self.config.api_audit_page_size, "offset": state.offset}
            )
            (
                response_payload,
                _latency_ms,
                page_retries,
                _capacity_observations,
            ) = self.fetcher._request_page(request_param_map)
            state.retries += page_retries
            page_items, pagination, total = self._validate_audit_pagination(
                response_payload,
                state,
            )
            source_set = self._validated_source_set(response_payload)
            if (
                state.declared_source_set is not None
                and source_set != state.declared_source_set
            ):
                raise ApiSchemaError("source_set_changed_between_pages")
            state.declared_source_set = source_set
            metadata = self._extract_sample_metadata(response_payload)
            self._validate_sample_metadata(metadata, total)
            if state.declared_metadata is not None and metadata != state.declared_metadata:
                raise ApiSchemaError("audit_sample_metadata_changed_between_pages")
            state.declared_total = total
            state.declared_metadata = metadata
            has_more = self._has_more_audit_items(pagination, state, page_items, total)
            self._validate_audit_contract(response_payload, total)
            self._consume_audit_items(page_items, state)
            if not has_more:
                if state.observed_count != total:
                    raise ApiSchemaError("audit_pagination_total_does_not_match_returned_rows")
                if state.observed_digest.hexdigest() != metadata.sample_digest:
                    raise ApiSchemaError(
                        "audit_sample_digest_does_not_match_returned_rows"
                    )
                return ApiOccurrenceSample(
                    occurrences=tuple(state.sampler.values()),
                    sample_count=state.observed_count,
                    pages=page_number,
                    retries=state.retries + self._preflight_retries,
                    sample_digest=metadata.sample_digest,
                    contract=metadata.contract,
                    method=metadata.method,
                    complete_population=metadata.is_complete_population,
                    sample_digest_validated=True,
                    source_set_validated=self._source_set_validated,
                )
            state.offset += len(page_items)
        raise ApiSchemaError("audit_pagination_exceeded_max_pages")


@dataclass
class _QueryAuditState:
    failure_counts: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    examples: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    schema_errors: list[str] = dataclasses.field(default_factory=list)
    first_page_first_observation_ms: list[float] = dataclasses.field(default_factory=list)
    cold_page_ms: list[float] = dataclasses.field(default_factory=list)
    cold_query_ms: list[float] = dataclasses.field(default_factory=list)
    warm_page_ms: list[float] = dataclasses.field(default_factory=list)
    warm_query_ms: list[float] = dataclasses.field(default_factory=list)
    retries: int = 0
    matched_initial_traversal: bool = False
    matched_first_observation: bool = False
    capacity_observations: list[CapacityHttpObservation] = dataclasses.field(
        default_factory=list
    )


def _fetch_all_with_capacity_intent(
    fetcher: ApiFetcher,
    params: Mapping[str, Any],
    *,
    phase: str,
    page_size: int | None = None,
    capacity_intent: CapacityObservationIntent | None = None,
) -> FetchResult:
    """Keep the optional extension invisible to legacy and test fetchers."""

    if capacity_intent is None:
        return fetcher.fetch_all(params, phase=phase, page_size=page_size)
    return fetcher.fetch_all(
        params,
        phase=phase,
        page_size=page_size,
        capacity_intent=capacity_intent,
    )


def _mark_capacity_semantic_outcome(
    fetcher: ApiFetcher,
    observations: Sequence[CapacityHttpObservation],
    *,
    successful: bool,
) -> None:
    collector = getattr(fetcher, "capacity_collector", None)
    if not observations:
        return
    if not isinstance(collector, CapacityEvidenceCollector):
        raise ConfigurationError("capacity_collector_missing")
    collector.mark_semantic_outcome(observations, successful=successful)


def _record_query_fetch(
    state: _QueryAuditState,
    fetch_result: FetchResult,
    *,
    is_cold: bool,
) -> None:
    state.retries += fetch_result.retries
    state.capacity_observations.extend(fetch_result.capacity_observations)
    if is_cold:
        if fetch_result.page_latencies_ms:
            state.first_page_first_observation_ms.append(
                fetch_result.page_latencies_ms[0]
            )
        state.cold_page_ms.extend(fetch_result.page_latencies_ms)
        state.cold_query_ms.append(fetch_result.total_latency_ms)
        return
    state.warm_page_ms.extend(fetch_result.page_latencies_ms)
    state.warm_query_ms.append(fetch_result.total_latency_ms)


def _query_audit_result(
    query: QueryKey,
    expected_counts: collections.Counter[str],
    extracted: ExtractedTuples,
    comparison: CounterComparison | None,
    state: _QueryAuditState,
    *,
    example_limit: int,
) -> QueryAuditResult:
    return QueryAuditResult(
        query=query,
        expected=expected_counts,
        actual=extracted.counter,
        actual_tuples=extracted.tuples,
        item_count=extracted.item_count,
        comparison=comparison,
        failure_counts=dict(state.failure_counts),
        examples=tuple(state.examples[:example_limit]),
        schema_errors=tuple(state.schema_errors),
        matched_initial_traversal=state.matched_initial_traversal,
        matched_first_observation=state.matched_first_observation,
        first_page_first_observation_ms=tuple(state.first_page_first_observation_ms),
        cold_page_ms=tuple(state.cold_page_ms),
        cold_query_ms=tuple(state.cold_query_ms),
        warm_page_ms=tuple(state.warm_page_ms),
        warm_query_ms=tuple(state.warm_query_ms),
        retries=state.retries,
        capacity_observations=tuple(state.capacity_observations),
    )


def _failed_query_audit_result(
    query: QueryKey,
    expected_counts: collections.Counter[str],
    state: _QueryAuditState,
) -> QueryAuditResult:
    return _query_audit_result(
        query,
        expected_counts,
        ExtractedTuples(collections.Counter(), {}, 0, ()),
        None,
        state,
        example_limit=len(state.examples),
    )


def _audit_positive_warm_repeats(
    query: QueryKey,
    initial: FetchResult,
    initial_extracted: ExtractedTuples,
    state: _QueryAuditState,
    *,
    fetcher: ApiFetcher,
    config: AuditConfig,
    identity_registry: SourceIdentityRegistry | None,
) -> None:
    for _repeat in range(config.warm_repeats):
        try:
            warm = _fetch_all_with_capacity_intent(
                fetcher,
                config.api_params(query),
                phase="warm",
            )
        except AuditError as exc:
            state.failure_counts[exc.code] += 1
            state.examples.append(_safe_http_example(query, exc))
            continue
        _record_query_fetch(state, warm, is_cold=False)
        state.schema_errors.extend(_validate_contracts(warm.contracts, config, positive=True))
        warm_extracted = extract_api_tuples(warm.items, registry=identity_registry)
        state.schema_errors.extend(warm_extracted.schema_errors)
        if warm_extracted.counter != initial_extracted.counter:
            state.failure_counts["altered"] += 1
            if len(state.examples) < config.failure_example_limit:
                state.examples.append(
                    {
                        "category": "altered",
                        "query": query.report_value,
                        "reason": "initial_repeat_tuple_counter_changed",
                        "initial_response_fingerprint": initial.response_fingerprint,
                        "repeat_response_fingerprint": warm.response_fingerprint,
                    }
                )


def _record_positive_schema_errors(
    query: QueryKey,
    state: _QueryAuditState,
    *,
    example_limit: int,
) -> None:
    if not state.schema_errors:
        return
    state.failure_counts["api_schema"] += len(state.schema_errors)
    for schema_error in state.schema_errors:
        if len(state.examples) >= example_limit:
            break
        state.examples.append(
            {
                "category": "api_schema",
                "query": query.report_value,
                "reason": schema_error,
            }
        )


def _audit_positive_query(
    query: QueryKey,
    expected_counts: collections.Counter[str],
    *,
    fetcher: ApiFetcher,
    config: AuditConfig,
    identity_registry: SourceIdentityRegistry | None = None,
    initial_phase: str = "cold",
) -> QueryAuditResult:
    """Audit one source-positive query across an initial and repeated traversal."""

    if initial_phase not in {"cold", "warm"}:
        raise ConfigurationError("positive_initial_phase_invalid")
    state = _QueryAuditState()
    try:
        initial = _fetch_all_with_capacity_intent(
            fetcher,
            config.api_params(query),
            phase=initial_phase,
            capacity_intent=(
                CapacityObservationIntent(
                    cohort=CAPACITY_POSITIVE_COHORT,
                    first_for_query=True,
                )
                if config.capacity_evidence_trust is not None
                and initial_phase == "cold"
                else None
            ),
        )
    except AuditError as exc:
        state.failure_counts[exc.code] += 1
        state.examples.append(_safe_http_example(query, exc))
        return _failed_query_audit_result(query, expected_counts, state)
    _record_query_fetch(state, initial, is_cold=initial_phase == "cold")
    state.matched_initial_traversal = bool(initial.contracts) and all(
        contract.result_state == "matched" for contract in initial.contracts
    )
    state.matched_first_observation = (
        initial_phase == "cold" and state.matched_initial_traversal
    )
    state.schema_errors.extend(
        _validate_contracts(initial.contracts, config, positive=True)
    )
    initial_extracted = extract_api_tuples(
        initial.items,
        registry=identity_registry,
    )
    state.schema_errors.extend(initial_extracted.schema_errors)
    comparison = compare_tuple_counters(
        query,
        expected_counts,
        initial_extracted.counter,
        example_limit=config.failure_example_limit,
    )
    state.failure_counts.update(comparison.failure_counts)
    state.examples.extend(comparison.examples)
    _mark_capacity_semantic_outcome(
        fetcher,
        initial.capacity_observations,
        successful=(
            state.matched_initial_traversal
            and not state.schema_errors
            and not comparison.failure_counts
        ),
    )
    _audit_positive_warm_repeats(
        query,
        initial,
        initial_extracted,
        state,
        fetcher=fetcher,
        config=config,
        identity_registry=identity_registry,
    )
    _record_positive_schema_errors(
        query,
        state,
        example_limit=config.failure_example_limit,
    )
    return _query_audit_result(
        query,
        expected_counts,
        initial_extracted,
        comparison,
        state,
        example_limit=config.failure_example_limit,
    )


def _audit_negative_warm_repeats(
    query: QueryKey,
    cold: FetchResult,
    state: _QueryAuditState,
    *,
    fetcher: ApiFetcher,
    config: AuditConfig,
) -> None:
    for _repeat in range(config.warm_repeats):
        try:
            warm = _fetch_all_with_capacity_intent(
                fetcher,
                config.api_params(query),
                phase="warm",
            )
        except AuditError as exc:
            state.failure_counts[exc.code] += 1
            state.examples.append(_safe_http_example(query, exc))
            continue
        _record_query_fetch(state, warm, is_cold=False)
        state.schema_errors.extend(
            _validate_contracts(warm.contracts, config, positive=False)
        )
        if warm.items:
            state.failure_counts["extra_fake"] += max(len(warm.items), 1)
        if warm.response_fingerprint != cold.response_fingerprint:
            state.failure_counts["altered"] += 1


def _fetch_negative_cold_response(
    query: QueryKey,
    fetcher: ApiFetcher,
    config: AuditConfig,
) -> FetchResult:
    capacity_intent = (
        CapacityObservationIntent(cohort=CAPACITY_NEGATIVE_COHORT, first_for_query=True)
        if config.capacity_evidence_trust is not None
        else None
    )
    return _fetch_all_with_capacity_intent(
        fetcher,
        config.api_params(query),
        phase="cold",
        capacity_intent=capacity_intent,
    )


def _audit_negative_query(
    query: QueryKey,
    *,
    fetcher: ApiFetcher,
    config: AuditConfig,
    identity_registry: SourceIdentityRegistry | None = None,
) -> QueryAuditResult:
    """Audit one source-negative query across cold and warm traversals."""

    state = _QueryAuditState()
    empty_counts: collections.Counter[str] = collections.Counter()
    extracted = ExtractedTuples(empty_counts, {}, 0, ())
    try:
        cold = _fetch_negative_cold_response(query, fetcher, config)
    except AuditError as exc:
        state.failure_counts[exc.code] += 1
        state.examples.append(_safe_http_example(query, exc))
        return _failed_query_audit_result(query, empty_counts, state)
    _record_query_fetch(state, cold, is_cold=True)
    state.schema_errors.extend(_validate_contracts(cold.contracts, config, positive=False))
    extracted = (
        extract_api_tuples(cold.items, registry=identity_registry)
        if cold.items
        else extracted
    )
    state.schema_errors.extend(extracted.schema_errors)
    if cold.items:
        fake_count = max(sum(extracted.counter.values()), len(cold.items), 1)
        state.failure_counts["extra_fake"] += fake_count
        state.examples.append(
            {
                "category": "extra_fake",
                "query": query.report_value,
                "count": fake_count,
                "reason": "negative_code_npi_combination_returned_items",
            }
        )
    _mark_capacity_semantic_outcome(
        fetcher,
        cold.capacity_observations,
        successful=not state.schema_errors and not state.failure_counts,
    )
    _audit_negative_warm_repeats(
        query,
        cold,
        state,
        fetcher=fetcher,
        config=config,
    )
    if state.schema_errors:
        state.failure_counts["api_schema"] += len(state.schema_errors)
    return _query_audit_result(
        query,
        empty_counts,
        extracted,
        None,
        state,
        example_limit=config.failure_example_limit,
    )


def _percentile(sorted_values: Sequence[float], percentile: float) -> float:
    if not sorted_values:
        return 0.0
    rank = (len(sorted_values) - 1) * percentile
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return sorted_values[lower]
    weight = rank - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def latency_summary(values: Sequence[float]) -> dict[str, Any]:
    """Return rounded deterministic latency distribution statistics."""

    ordered = sorted(float(value) for value in values)
    if not ordered:
        return {"count": 0}
    return {
        "count": len(ordered),
        "min_ms": round(ordered[0], 3),
        "mean_ms": round(statistics.fmean(ordered), 3),
        "p50_ms": round(_percentile(ordered, 0.50), 3),
        "p95_ms": round(_percentile(ordered, 0.95), 3),
        "p99_ms": round(_percentile(ordered, 0.99), 3),
        "max_ms": round(ordered[-1], 3),
    }


@dataclass(frozen=True)
class RandomApiRequest:
    index: int
    query: QueryKey
    source_occurrence_id: str
    page_size: int
    phase: str

    @property
    def stable_key(self) -> str:
        """Return the deterministic identity for one planned random request."""

        return canonical_json(
            {
                "query": self.query.stable_key,
                "source_occurrence_id": self.source_occurrence_id,
                "page_size": self.page_size,
                "phase": self.phase,
            }
        )


@dataclass(frozen=True)
class RandomApiResult:
    request: RandomApiRequest
    response_occurrences: int
    latency_ms: float | None
    first_page_latency_ms: float | None
    retries: int
    http_requests: int
    failure_counts: Mapping[str, int]
    examples: tuple[dict[str, Any], ...]
    response_fingerprint: str
    capacity_observations: tuple[CapacityHttpObservation, ...] = ()


def _seeded_index(seed: str, namespace: str, index: int, modulus: int) -> int:
    if modulus < 1:
        raise ConfigurationError("random_api_sampling_population_empty")
    digest = hashlib.sha256(
        seed.encode("utf-8")
        + b"\x00"
        + namespace.encode("utf-8")
        + b"\x00"
        + str(index).encode("ascii")
    ).digest()
    return int.from_bytes(digest[:8], "big") % modulus


def build_random_api_requests(
    occurrences: Sequence[SourceOccurrence],
    *,
    count: int,
    max_limit: int,
    seed: str,
) -> list[RandomApiRequest]:
    """Plan deterministic positive requests sampled with replacement."""

    if not occurrences:
        raise SourceCoverageError("random_api_source_population_empty")
    requests: list[RandomApiRequest] = []
    seen_queries: set[str] = set()
    for index in range(count):
        occurrence = occurrences[
            _seeded_index(seed, "random-api-occurrence", index, len(occurrences))
        ]
        query_key = occurrence.query.stable_key
        phase = "cold" if query_key not in seen_queries else "warm"
        seen_queries.add(query_key)
        requests.append(
            RandomApiRequest(
                index=index,
                query=occurrence.query,
                source_occurrence_id=occurrence.occurrence_id,
                page_size=1 + _seeded_index(seed, "random-api-page-size", index, max_limit),
                phase=phase,
            )
        )
    return requests


def build_capacity_random_api_requests(
    occurrences: Sequence[SourceOccurrence],
    *,
    count: int,
) -> list[RandomApiRequest]:
    """Plan fixed-page capacity requests without semantic-query replacement."""

    unique_by_query: dict[str, SourceOccurrence] = {}
    for occurrence in occurrences:
        unique_by_query.setdefault(occurrence.query.stable_key, occurrence)
    if len(unique_by_query) < count:
        raise SourceCoverageError(
            "capacity_distinct_random_query_population_below_minimum"
        )
    return [
        RandomApiRequest(
            index=index,
            query=occurrence.query,
            source_occurrence_id=occurrence.occurrence_id,
            page_size=capacity_evidence.CAPACITY_PAGE_LIMIT,
            phase="cold",
        )
        for index, occurrence in enumerate(list(unique_by_query.values())[:count])
    ]


def _record_random_schema_errors(
    query: QueryKey,
    schema_errors: Sequence[str],
    failure_counts: collections.Counter[str],
    examples: list[dict[str, Any]],
    *,
    example_limit: int,
) -> None:
    for schema_error in schema_errors:
        failure_counts["api_schema"] += 1
        if len(examples) < example_limit:
            examples.append(
                {
                    "category": "api_schema",
                    "query": query.report_value,
                    "reason": schema_error,
                }
            )


def _failed_random_audit_result(
    request: RandomApiRequest,
    exc: AuditError,
    failure_counts: collections.Counter[str],
    examples: Sequence[dict[str, Any]],
) -> RandomApiResult:
    return RandomApiResult(
        request=request,
        response_occurrences=0,
        latency_ms=None,
        first_page_latency_ms=None,
        retries=0,
        http_requests=0,
        failure_counts=dict(failure_counts),
        examples=tuple(examples),
        response_fingerprint=sha256_text(f"error:{exc.code}"),
    )


def _audit_random_api_request(
    request: RandomApiRequest,
    expected_full: collections.Counter[str],
    *,
    fetcher: ApiFetcher,
    config: AuditConfig,
    identity_registry: SourceIdentityRegistry | None = None,
) -> RandomApiResult:
    """Audit one deterministic random standard-API traversal."""

    failures: collections.Counter[str] = collections.Counter()
    examples: list[dict[str, Any]] = []
    try:
        response = _fetch_all_with_capacity_intent(
            fetcher,
            config.api_params(request.query),
            phase=request.phase,
            page_size=request.page_size,
            capacity_intent=(
                CapacityObservationIntent(
                    cohort=CAPACITY_RANDOM_COHORT,
                    first_for_query=request.phase == "cold",
                )
                if config.capacity_evidence_trust is not None
                else None
            ),
        )
    except AuditError as exc:
        failures[exc.code] += 1
        examples.append(_safe_http_example(request.query, exc))
        return _failed_random_audit_result(request, exc, failures, examples)
    contract_errors = _validate_contracts(response.contracts, config, positive=True)
    _record_random_schema_errors(
        request.query,
        contract_errors,
        failures,
        examples,
        example_limit=config.failure_example_limit,
    )
    extracted = extract_api_tuples(response.items, registry=identity_registry)
    _record_random_schema_errors(
        request.query,
        extracted.schema_errors,
        failures,
        examples,
        example_limit=config.failure_example_limit,
    )
    comparison = compare_tuple_counters(
        request.query,
        expected_full,
        extracted.counter,
        example_limit=config.failure_example_limit,
    )
    failures.update(comparison.failure_counts)
    examples.extend(comparison.examples)
    _mark_capacity_semantic_outcome(
        fetcher,
        response.capacity_observations,
        successful=not failures,
    )
    return RandomApiResult(
        request=request,
        response_occurrences=sum(extracted.counter.values()),
        latency_ms=response.total_latency_ms,
        first_page_latency_ms=(
            response.page_latencies_ms[0]
            if response.page_latencies_ms
            else None
        ),
        retries=response.retries,
        http_requests=response.pages + response.retries,
        failure_counts=dict(failures),
        examples=tuple(examples[: config.failure_example_limit]),
        response_fingerprint=response.response_fingerprint,
        capacity_observations=response.capacity_observations,
    )


def _fetcher_request_count(fetcher: ApiFetcher) -> int | None:
    value = getattr(fetcher, "request_count", None)
    return value if type(value) is int and value >= 0 else None


@dataclass(frozen=True)
class _AuditSamples:
    source_occurrences: list[SourceOccurrence]
    random_requests: list[RandomApiRequest]
    api_sample: ApiOccurrenceSample
    api_occurrences: list[ApiOccurrence]
    source_occurrences_by_query: Mapping[str, list[SourceOccurrence]]
    api_occurrences_by_query: Mapping[str, list[ApiOccurrence]]
    positive_queries: list[QueryKey]
    positive_first_observation_queries: list[QueryKey]
    positive_reserved_queries: list[QueryKey]
    negative_jobs: list[tuple[QueryKey, collections.Counter[str]]]
    random_query_keys: set[str]
    random_request_shape_keys: set[str]
    random_plan_sha256: str


@dataclass
class _AuditProgress:
    failure_counts: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    failure_examples: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    coverage_reasons: list[str] = dataclasses.field(default_factory=list)
    checks: collections.Counter[str] = dataclasses.field(default_factory=collections.Counter)
    first_page_first_observation_ms: list[float] = dataclasses.field(default_factory=list)
    cold_page_ms: list[float] = dataclasses.field(default_factory=list)
    cold_query_ms: list[float] = dataclasses.field(default_factory=list)
    positive_cold_query_ms: list[float] = dataclasses.field(default_factory=list)
    negative_first_page_ms: list[float] = dataclasses.field(default_factory=list)
    negative_cold_query_ms: list[float] = dataclasses.field(default_factory=list)
    warm_page_ms: list[float] = dataclasses.field(default_factory=list)
    warm_query_ms: list[float] = dataclasses.field(default_factory=list)
    retries: int = 0
    random_failure_counts: collections.Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    random_cold_latency_ms: list[float] = dataclasses.field(default_factory=list)
    random_cold_first_page_ms: list[float] = dataclasses.field(default_factory=list)
    random_warm_latency_ms: list[float] = dataclasses.field(default_factory=list)
    random_response_occurrences: int = 0
    random_retries: int = 0
    random_http_requests_fallback: int = 0
    random_response_fingerprint: Any = dataclasses.field(default_factory=hashlib.sha256)


@dataclass(frozen=True)
class _LatencyReports:
    first_page: Mapping[str, Any]
    cold_pages: Mapping[str, Any]
    logical_query: Mapping[str, Any]


@dataclass(frozen=True)
class _AuditReportContext:
    started_at: dt.datetime
    completed_at: dt.datetime
    resolved_fraction: float
    random_http_requests: int
    standard_http_requests: int
    latency_reports: _LatencyReports
    capacity_evidence: Mapping[str, Any]


AUDIT_LIMITATIONS = (
    (
        "First observation means the first standard-pricing request for a query key "
        "by this process, not forced database or HTTP cache eviction."
    ),
    (
        "The source-set seal proves the supplied raw containers exactly match the "
        "published snapshot set; it cannot prove upstream discovery selected every intended file."
    ),
    (
        "Persisted audit-sample enumeration uses a separate endpoint before standard-pricing "
        "latency measurement so API-selected query keys remain independent of source-selected keys."
    ),
    (
        "The API audit endpoint exposes a bounded persisted publish-time sample, "
        "not the complete served occurrence population."
    ),
)
REDACTION_EXCLUDED_FIELDS = (
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


def _latency_gate_semantics(uses_signed_capacity_gate: bool) -> dict[str, str]:
    if uses_signed_capacity_gate:
        return {
            "first_page_first_observation": (
                "client end-to-end HTTP duration for the first physical first-page "
                "attempt whose cold request and response were API-signed and verified, "
                "gated separately for matched positive, negative, and independently "
                "random query classes"
            ),
            "logical_query": (
                "client perf_counter wall time for all pages in the first traversal; "
                "diagnostic only and not release evidence"
            ),
            "warm": (
                "client perf_counter timing for repeated traversals and reserved positive "
                "exactness checks; diagnostic only and not release evidence"
            ),
        }
    return {
        "first_page_first_observation": (
            "page zero from the first traversal of each distinct matched positive "
            "standard-pricing query outside the random reserved cohort; negative and "
            "random requests are reported in separate classes"
        ),
        "logical_query": (
            "wall time for all pages in the first traversal, gated separately for "
            "disjoint matched positive, negative, and random positive query classes"
        ),
        "warm": (
            "immediate repeated traversal of the same sampled query, plus positive "
            "exactness traversals for query keys first observed by the random cohort"
        ),
    }


class AuditRunner:
    def __init__(
        self,
        source_index: SourceIndex,
        fetcher: ApiFetcher,
        api_occurrence_source: ApiOccurrenceSource,
        config: AuditConfig,
        capacity_collector: CapacityEvidenceCollector | None = None,
    ):
        self.source_index = source_index
        self.fetcher = fetcher
        self.api_occurrence_source = api_occurrence_source
        self.config = config
        self.capacity_collector = capacity_collector
        if config.capacity_evidence_trust is not None and capacity_collector is None:
            raise ConfigurationError("capacity_collector_missing")
        self.identity_registry = SourceIdentityRegistry()

    @staticmethod
    def _group_occurrences(
        source_occurrences: Sequence[SourceOccurrence],
        api_occurrences: Sequence[ApiOccurrence],
    ) -> tuple[
        dict[str, list[SourceOccurrence]],
        dict[str, list[ApiOccurrence]],
        list[QueryKey],
        list[QueryKey],
    ]:
        source_occurrences_by_query: dict[str, list[SourceOccurrence]] = (
            collections.defaultdict(list)
        )
        api_occurrences_by_query: dict[str, list[ApiOccurrence]] = collections.defaultdict(list)
        queries_by_key: dict[str, QueryKey] = {}
        for occurrence in source_occurrences:
            query_key = occurrence.query.stable_key
            source_occurrences_by_query[query_key].append(occurrence)
            queries_by_key[query_key] = occurrence.query
        for occurrence in api_occurrences:
            query_key = occurrence.query.stable_key
            api_occurrences_by_query[query_key].append(occurrence)
            queries_by_key[query_key] = occurrence.query
        positive_queries = [queries_by_key[key] for key in sorted(queries_by_key)]
        source_positive_queries = [
            queries_by_key[key] for key in sorted(source_occurrences_by_query)
        ]
        return (
            source_occurrences_by_query,
            api_occurrences_by_query,
            positive_queries,
            source_positive_queries,
        )

    @staticmethod
    def _random_plan_details(
        random_requests: Sequence[RandomApiRequest],
    ) -> tuple[set[str], set[str], str]:
        random_query_keys = {request.query.stable_key for request in random_requests}
        random_request_shape_keys = {
            canonical_json(
                {
                    "query": request.query.stable_key,
                    "page_size": request.page_size,
                }
            )
            for request in random_requests
        }
        plan_fingerprint = hashlib.sha256()
        for request in random_requests:
            plan_fingerprint.update(
                f"{request.index}:{request.stable_key}\n".encode("utf-8")
            )
        return random_query_keys, random_request_shape_keys, plan_fingerprint.hexdigest()

    def _prepare_samples(self) -> _AuditSamples:
        """Validate source seals and prepare bounded audit samples and request plans."""

        if self.api_occurrence_source.validate_source_set() is not True:
            raise ApiSchemaError("snapshot_source_set_was_not_validated")
        self.source_index.prepare_occurrence_sample()
        source_occurrences = self.source_index.source_occurrences(
            self.config.source_occurrence_samples
        )
        if self.config.capacity_evidence_trust is not None:
            random_requests = build_capacity_random_api_requests(
                self.source_index.capacity_query_occurrences(
                    self.config.random_api_calls
                ),
                count=self.config.random_api_calls,
            )
        else:
            random_requests = build_random_api_requests(
                source_occurrences,
                count=self.config.random_api_calls,
                max_limit=self.config.random_api_max_limit,
                seed=self.config.seed,
            )
        api_sample = self.api_occurrence_source.sample_occurrences(
            sample_target=self.config.api_occurrence_samples,
            seed=self.config.seed,
        )
        if not api_sample.sample_digest_validated:
            raise ApiSchemaError("api_occurrence_sample_digest_was_not_validated")
        if not api_sample.source_set_validated:
            raise ApiSchemaError("api_occurrence_source_set_was_not_validated")
        api_occurrences = list(api_sample.occurrences)
        for occurrence in api_occurrences:
            self.identity_registry.register(occurrence.source_identity)
        api_occurrence_ids = [occurrence.occurrence_id for occurrence in api_occurrences]
        if len(api_occurrence_ids) != len(set(api_occurrence_ids)):
            raise ApiSchemaError("api_occurrence_sample_contains_duplicate_ids")
        grouped = self._group_occurrences(source_occurrences, api_occurrences)
        source_by_query, api_by_query, positive_queries, source_queries = grouped
        negative_queries = self.source_index.negative_queries(
            source_queries,
            self.config.negative_samples,
        )
        random_details = self._random_plan_details(random_requests)
        random_query_keys = random_details[0]
        positive_first_observation_queries = [
            query
            for query in positive_queries
            if query.stable_key not in random_query_keys
        ]
        positive_reserved_queries = [
            query
            for query in positive_queries
            if query.stable_key in random_query_keys
        ]
        if self.config.capacity_evidence_trust is not None:
            validate_capacity_evidence_preflight(
                distinct_random_queries=len(random_query_keys),
                negative_queries=len(negative_queries),
                positive_queries=len(positive_first_observation_queries),
            )
        return _AuditSamples(
            source_occurrences=source_occurrences,
            random_requests=random_requests,
            api_sample=api_sample,
            api_occurrences=api_occurrences,
            source_occurrences_by_query=source_by_query,
            api_occurrences_by_query=api_by_query,
            positive_queries=positive_queries,
            positive_first_observation_queries=positive_first_observation_queries,
            positive_reserved_queries=positive_reserved_queries,
            negative_jobs=[
                (query, collections.Counter()) for query in negative_queries
            ],
            random_query_keys=random_query_keys,
            random_request_shape_keys=random_details[1],
            random_plan_sha256=random_details[2],
        )

    def _initial_progress(self, samples: _AuditSamples) -> _AuditProgress:
        progress = _AuditProgress()
        checks = progress.checks
        checks["source_occurrence_population"] = self.source_index.metrics.get(
            "source_occurrences", 0
        )
        checks["persisted_api_audit_sample_count"] = samples.api_sample.sample_count
        checks["source_occurrence_ids"] = len(samples.source_occurrences)
        checks["api_occurrence_ids"] = len(samples.api_occurrences)
        checks["source_to_api"] = len(samples.source_occurrences)
        checks["api_to_source"] = len(samples.api_occurrences)
        checks["source_sample_query_keys"] = len(samples.source_occurrences_by_query)
        checks["api_sample_query_keys"] = len(samples.api_occurrences_by_query)
        checks["positive_queries"] = len(samples.positive_queries)
        checks["positive_first_observation_query_keys"] = len(
            samples.positive_first_observation_queries
        )
        checks["positive_reserved_for_random_query_keys"] = len(
            samples.positive_reserved_queries
        )
        checks["positive_queries_executed"] = 0
        checks["positive_matched_query_keys"] = 0
        checks["positive_matched_first_observation_query_keys"] = 0
        checks["random_api_logical_requests_target"] = len(samples.random_requests)
        checks["random_api_requests_executed"] = 0
        checks["random_api_distinct_query_keys"] = len(samples.random_query_keys)
        checks["random_api_repeated_query_requests"] = len(
            samples.random_requests
        ) - len(samples.random_query_keys)
        return progress

    def _append_failure_examples(
        self,
        progress: _AuditProgress,
        examples: Sequence[dict[str, Any]],
    ) -> None:
        for failure_example in examples:
            if len(progress.failure_examples) >= self.config.failure_example_limit:
                return
            progress.failure_examples.append(failure_example)

    def _record_query_audit(
        self,
        progress: _AuditProgress,
        audit_result: QueryAuditResult,
        *,
        positive: bool,
    ) -> None:
        progress.failure_counts.update(audit_result.failure_counts)
        progress.retries += audit_result.retries
        if positive and audit_result.matched_initial_traversal:
            progress.checks["positive_matched_query_keys"] += 1
        if positive and audit_result.matched_first_observation:
            progress.first_page_first_observation_ms.extend(
                audit_result.first_page_first_observation_ms
            )
            progress.checks["positive_matched_first_observation_query_keys"] += 1
        if positive:
            progress.positive_cold_query_ms.extend(audit_result.cold_query_ms)
        else:
            if audit_result.cold_page_ms:
                progress.negative_first_page_ms.append(
                    audit_result.cold_page_ms[0]
                )
            progress.negative_cold_query_ms.extend(audit_result.cold_query_ms)
        progress.cold_page_ms.extend(audit_result.cold_page_ms)
        progress.cold_query_ms.extend(audit_result.cold_query_ms)
        progress.warm_page_ms.extend(audit_result.warm_page_ms)
        progress.warm_query_ms.extend(audit_result.warm_query_ms)
        self._append_failure_examples(progress, audit_result.examples)

    def _positive_jobs(
        self,
        query_batch: Sequence[QueryKey],
        progress: _AuditProgress,
    ) -> list[tuple[QueryKey, collections.Counter[str]]]:
        positive_jobs: list[tuple[QueryKey, collections.Counter[str]]] = []
        for query in query_batch:
            try:
                expected_counts = self.source_index.expected_tuples(query)
            except SourceCoverageError:
                progress.coverage_reasons.append(
                    "source_query_exceeds_canonical_tuple_limit"
                )
                self._append_failure_examples(
                    progress,
                    [
                        {
                            "category": "coverage",
                            "query": query.report_value,
                            "reason": "source_query_exceeds_canonical_tuple_limit",
                        }
                    ],
                )
                continue
            progress.checks["positive_queries_executed"] += 1
            positive_jobs.append((query, expected_counts))
        return positive_jobs

    @staticmethod
    def _reconcile_source_sample(
        samples: _AuditSamples,
        progress: _AuditProgress,
        audit_result: QueryAuditResult,
    ) -> None:
        query_key = audit_result.query.stable_key
        source_sample_counts = collections.Counter(
            occurrence.tuple_key
            for occurrence in samples.source_occurrences_by_query.get(query_key, ())
        )
        for tuple_key, sampled_count in source_sample_counts.items():
            expected_count = audit_result.expected.get(tuple_key, 0)
            actual_count = audit_result.actual.get(tuple_key, 0)
            if expected_count <= 0:
                progress.coverage_reasons.append(
                    "source_occurrence_missing_from_source_counter"
                )
            elif actual_count == expected_count:
                progress.checks["source_to_api_exact_matches"] += sampled_count

    def _reconcile_api_sample(
        self,
        samples: _AuditSamples,
        progress: _AuditProgress,
        audit_result: QueryAuditResult,
    ) -> None:
        query_key = audit_result.query.stable_key
        api_sample_counts = collections.Counter(
            occurrence.tuple_key
            for occurrence in samples.api_occurrences_by_query.get(query_key, ())
        )
        for tuple_key, sampled_count in api_sample_counts.items():
            source_count = audit_result.expected.get(tuple_key, 0)
            actual_count = audit_result.actual.get(tuple_key, 0)
            if source_count > 0 and actual_count == source_count:
                progress.checks["api_to_source_exact_matches"] += sampled_count
            if source_count == 0 and actual_count == 0:
                progress.failure_counts["extra_fake"] += sampled_count
                self._append_failure_examples(
                    progress,
                    [
                        {
                            "category": "extra_fake",
                            "query": audit_result.query.report_value,
                            "tuple_fingerprint": fingerprint(tuple_key),
                            "count": sampled_count,
                            "reason": "persisted_api_sample_occurrence_absent_from_source",
                        }
                    ],
                )
            if sampled_count > actual_count:
                progress.failure_counts["api_sample_inconsistent"] += (
                    sampled_count - actual_count
                )
                self._append_failure_examples(
                    progress,
                    [
                        {
                            "category": "api_sample_inconsistent",
                            "query": audit_result.query.report_value,
                            "tuple_fingerprint": fingerprint(tuple_key),
                            "sampled_count": sampled_count,
                            "query_response_count": actual_count,
                        }
                    ],
                )

    def _run_positive_checks(
        self,
        samples: _AuditSamples,
        progress: _AuditProgress,
        queries: Sequence[QueryKey],
        *,
        initial_phase: str,
    ) -> None:
        batch_size = max(self.config.concurrency * 2, 1)
        for batch_start in range(0, len(queries), batch_size):
            query_batch = queries[batch_start : batch_start + batch_size]
            positive_jobs = self._positive_jobs(query_batch, progress)
            for audit_result in self._run_jobs(
                positive_jobs,
                positive=True,
                positive_initial_phase=initial_phase,
            ):
                self._record_query_audit(
                    progress,
                    audit_result,
                    positive=True,
                )
                self._reconcile_source_sample(samples, progress, audit_result)
                self._reconcile_api_sample(samples, progress, audit_result)

    def _random_jobs(
        self,
        request_batch: Sequence[RandomApiRequest],
        progress: _AuditProgress,
    ) -> list[tuple[RandomApiRequest, collections.Counter[str]]]:
        expected_counts_by_query: dict[str, collections.Counter[str]] = {}
        random_jobs: list[tuple[RandomApiRequest, collections.Counter[str]]] = []
        for request in request_batch:
            query_key = request.query.stable_key
            if query_key not in expected_counts_by_query:
                try:
                    expected_counts_by_query[query_key] = (
                        self.source_index.expected_tuples(request.query)
                    )
                except SourceCoverageError:
                    progress.coverage_reasons.append(
                        "random_api_query_exceeds_canonical_tuple_limit"
                    )
                    expected_counts_by_query[query_key] = collections.Counter()
            random_jobs.append((request, expected_counts_by_query[query_key]))
        return random_jobs

    def _record_random_audit(
        self,
        progress: _AuditProgress,
        audit_result: RandomApiResult,
    ) -> None:
        progress.checks["random_api_requests_executed"] += 1
        progress.random_failure_counts.update(audit_result.failure_counts)
        progress.random_response_occurrences += audit_result.response_occurrences
        progress.random_retries += audit_result.retries
        progress.random_http_requests_fallback += audit_result.http_requests
        progress.random_response_fingerprint.update(
            f"{audit_result.request.index}:{audit_result.response_fingerprint}\n".encode(
                "ascii"
            )
        )
        if audit_result.latency_ms is not None:
            latency_samples = (
                progress.random_cold_latency_ms
                if audit_result.request.phase == "cold"
                else progress.random_warm_latency_ms
            )
            latency_samples.append(audit_result.latency_ms)
        if (
            audit_result.request.phase == "cold"
            and audit_result.first_page_latency_ms is not None
        ):
            progress.random_cold_first_page_ms.append(
                audit_result.first_page_latency_ms
            )
        self._append_failure_examples(progress, audit_result.examples)

    def _run_random_checks(
        self,
        samples: _AuditSamples,
        progress: _AuditProgress,
    ) -> int:
        request_count_start = _fetcher_request_count(self.fetcher)
        batch_size = max(self.config.concurrency * 2, 1)
        audit_results: list[RandomApiResult] = []
        planned_jobs = self._random_jobs(samples.random_requests, progress)
        for phase in ("cold", "warm"):
            phase_jobs = [
                job
                for job in planned_jobs
                if job[0].phase == phase
            ]
            for batch_start in range(0, len(phase_jobs), batch_size):
                random_jobs = phase_jobs[
                    batch_start : batch_start + batch_size
                ]
                audit_results.extend(self._run_random_jobs(random_jobs))
        for audit_result in sorted(
            audit_results,
            key=lambda result: result.request.index,
        ):
            self._record_random_audit(progress, audit_result)
        progress.failure_counts.update(progress.random_failure_counts)
        progress.retries += progress.random_retries
        request_count_end = _fetcher_request_count(self.fetcher)
        if request_count_start is not None and request_count_end is not None:
            return request_count_end - request_count_start
        return progress.random_http_requests_fallback

    def _run_negative_checks(
        self,
        samples: _AuditSamples,
        progress: _AuditProgress,
    ) -> None:
        progress.checks["negative_queries"] = len(samples.negative_jobs)
        for audit_result in self._run_jobs(samples.negative_jobs, positive=False):
            self._record_query_audit(
                progress,
                audit_result,
                positive=False,
            )

    def _run_jobs(
        self,
        jobs: Sequence[tuple[QueryKey, collections.Counter[str]]],
        *,
        positive: bool,
        positive_initial_phase: str = "cold",
    ) -> Iterator[QueryAuditResult]:
        def _execute_query_job(
            job: tuple[QueryKey, collections.Counter[str]],
        ) -> QueryAuditResult:
            query, expected = job
            if positive:
                return _audit_positive_query(
                    query,
                    expected,
                    fetcher=self.fetcher,
                    config=self.config,
                    identity_registry=self.identity_registry,
                    initial_phase=positive_initial_phase,
                )
            return _audit_negative_query(
                query,
                fetcher=self.fetcher,
                config=self.config,
                identity_registry=self.identity_registry,
            )

        if self.config.concurrency == 1:
            for job in jobs:
                yield _execute_query_job(job)
            return
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.concurrency) as executor:
            yield from executor.map(_execute_query_job, jobs)

    def _run_random_jobs(
        self,
        jobs: Sequence[tuple[RandomApiRequest, collections.Counter[str]]],
    ) -> Iterator[RandomApiResult]:
        def _execute_random_job(
            job: tuple[RandomApiRequest, collections.Counter[str]],
        ) -> RandomApiResult:
            request, expected = job
            return _audit_random_api_request(
                request,
                expected,
                fetcher=self.fetcher,
                config=self.config,
                identity_registry=self.identity_registry,
            )

        if self.config.concurrency == 1:
            for job in jobs:
                yield _execute_random_job(job)
            return
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.concurrency) as executor:
            yield from executor.map(_execute_random_job, jobs)

    def _resolved_rate_fraction(self) -> float:
        negotiated_rates = self.source_index.metrics.get("negotiated_rates", 0)
        rates_with_provider_structure = self.source_index.metrics.get(
            "rates_with_provider_structure",
            0,
        )
        return (
            rates_with_provider_structure / negotiated_rates
            if negotiated_rates
            else 0.0
        )

    def _standard_http_request_count(
        self,
        request_count_start: int | None,
        progress: _AuditProgress,
    ) -> int:
        request_count_end = _fetcher_request_count(self.fetcher)
        if request_count_start is not None and request_count_end is not None:
            return request_count_end - request_count_start
        return (
            len(progress.cold_page_ms)
            + len(progress.warm_page_ms)
            + (progress.retries - progress.random_retries)
            + progress.random_http_requests_fallback
        )

    def _check_sample_minimums(self, progress: _AuditProgress) -> None:
        checks = progress.checks
        if checks["source_occurrence_ids"] < self.config.min_source_occurrence_checks:
            progress.coverage_reasons.append("source_occurrence_checks_below_minimum")
        if checks["api_occurrence_ids"] < self.config.min_api_occurrence_checks:
            progress.coverage_reasons.append("api_occurrence_checks_below_minimum")
        if checks["negative_queries"] < self.config.min_negative_checks:
            progress.coverage_reasons.append("negative_checks_below_minimum")
        if checks["random_api_requests_executed"] < self.config.min_random_api_calls:
            progress.coverage_reasons.append("random_api_calls_below_minimum")
        if (
            self.config.profile == "release"
            and checks["positive_matched_query_keys"]
            < RELEASE_MIN_DISTINCT_MATCHED_QUERY_KEYS
        ):
            progress.coverage_reasons.append(
                "distinct_matched_positive_query_keys_below_release_minimum"
            )

    def _check_source_integrity(
        self,
        progress: _AuditProgress,
        resolved_fraction: float,
    ) -> None:
        source_totals = self.source_index.metrics
        if source_totals.get("unresolved_provider_references", 0) > self.config.max_unresolved_provider_references:
            progress.coverage_reasons.append(
                "unresolved_provider_references_above_maximum"
            )
        if source_totals.get("invalid_prices", 0) > self.config.max_invalid_prices:
            progress.coverage_reasons.append("invalid_prices_above_maximum")
        invalid_npis = source_totals.get("invalid_provider_npis", 0) + source_totals.get(
            "invalid_inline_npis", 0
        )
        if invalid_npis > self.config.max_invalid_npis:
            progress.coverage_reasons.append("invalid_npis_above_maximum")
        if source_totals.get("invalid_field_types", 0) > self.config.max_invalid_field_types:
            progress.coverage_reasons.append("invalid_field_types_above_maximum")
        if source_totals.get("invalid_rate_provider_references", 0):
            progress.coverage_reasons.append("invalid_rate_provider_references")
        if self.config.profile == "release" and source_totals.get("tin_only_rates", 0):
            progress.coverage_reasons.append(
                "tin_only_rates_not_npi_verifiable"
            )
        if source_totals.get("items_without_valid_code", 0):
            progress.coverage_reasons.append("source_items_without_valid_code")
        if resolved_fraction < self.config.min_resolved_rate_fraction:
            progress.coverage_reasons.append("resolved_rate_fraction_below_minimum")

    def _check_source_files(self, progress: _AuditProgress) -> None:
        for file_metrics in self.source_index.file_metrics.values():
            if not file_metrics["in_network_items"]:
                progress.coverage_reasons.append("source_file_has_no_in_network_items")
            if not file_metrics["eligible_rates"]:
                progress.coverage_reasons.append("source_file_has_no_eligible_rates")

    def _apply_coverage_checks(
        self,
        progress: _AuditProgress,
        *,
        resolved_fraction: float,
        standard_http_requests: int,
        capacity_report: Mapping[str, Any],
    ) -> None:
        self._check_sample_minimums(progress)
        if (
            self.config.profile == "release"
            and standard_http_requests < RELEASE_MIN_TOTAL_API_CALLS
        ):
            progress.coverage_reasons.append(
                "actual_http_requests_below_release_minimum"
            )
        if self.config.profile == "release" and progress.retries:
            progress.coverage_reasons.append(
                "standard_http_retries_observed"
            )
        if self.config.capacity_evidence_trust is not None:
            progress.coverage_reasons.extend(
                capacity_evidence_release_failures(
                    capacity_report,
                    require_release_seed=self.config.profile == "release",
                )
            )
        self._check_source_integrity(progress, resolved_fraction)
        self._check_source_files(progress)
        if not progress.coverage_reasons:
            return
        progress.failure_counts["coverage"] += len(progress.coverage_reasons)
        self._append_failure_examples(
            progress,
            [
                {"category": "coverage", "reason": coverage_reason}
                for coverage_reason in progress.coverage_reasons
            ],
        )

    def _record_latency_failure(
        self,
        progress: _AuditProgress,
        *,
        category: str,
        reason: str,
        observed_p95_ms: Any,
        maximum_p95_ms: float,
    ) -> None:
        if observed_p95_ms is not None and float(observed_p95_ms) <= maximum_p95_ms:
            return
        progress.failure_counts[category] += 1
        self._append_failure_examples(
            progress,
            [
                {
                    "category": category,
                    "reason": reason,
                    "observed_p95_ms": observed_p95_ms,
                    "maximum_p95_ms": maximum_p95_ms,
                }
            ],
        )

    def _build_latency_reports(
        self,
        progress: _AuditProgress,
        capacity_report: Mapping[str, Any] | None = None,
    ) -> _LatencyReports:
        """Summarize latency classes and record any p95 gate failures."""

        def client_gated_report(
            samples: Sequence[float],
            *,
            category: str,
            reason: str,
            maximum_p95_ms: float,
        ) -> dict[str, Any]:
            """Summarize one latency class and record its p95 gate result."""

            report = latency_summary(samples)
            p95_ms = (
                _percentile(sorted(samples), 0.95)
                if samples
                else None
            )
            if p95_ms is not None:
                report["gate_p95_ms"] = round(p95_ms, 6)
                self._record_latency_failure(
                    progress,
                    category=category,
                    reason=reason,
                    observed_p95_ms=p95_ms,
                    maximum_p95_ms=maximum_p95_ms,
                )
            return report

        def client_diagnostic_report(samples: Sequence[float]) -> dict[str, Any]:
            """Return client timing as explicitly nongating diagnostics."""

            report = latency_summary(samples)
            report["measurement_source"] = (
                CAPACITY_UNQUALIFIED_CLIENT_LATENCY_SOURCE
            )
            report["diagnostic_only"] = True
            return report

        def verified_client_gated_report(
            cohort: str,
            *,
            category: str,
            reason: str,
        ) -> dict[str, Any]:
            """Gate client HTTP timing only for API-verified cold responses."""

            latency_section = (
                capacity_report.get("latency")
                if isinstance(capacity_report, Mapping)
                else None
            )
            latency_by_cohort = (
                latency_section.get(
                    "eligible_verified_client_http_duration_ms_by_cohort"
                )
                if isinstance(latency_section, Mapping)
                and latency_section.get("gate_source")
                == CAPACITY_CLIENT_LATENCY_SOURCE
                else None
            )
            cohort_report = (
                latency_by_cohort.get(cohort)
                if isinstance(latency_by_cohort, Mapping)
                else None
            )
            report = (
                dict(cohort_report)
                if isinstance(cohort_report, Mapping)
                else {"count": 0}
            )
            raw_p95_ms = report.get("gate_p95_ms")
            gate_p95_ms = (
                float(raw_p95_ms)
                if not isinstance(raw_p95_ms, bool)
                and isinstance(raw_p95_ms, (int, float))
                and math.isfinite(float(raw_p95_ms))
                and float(raw_p95_ms) >= 0
                else None
            )
            report["measurement_source"] = CAPACITY_CLIENT_LATENCY_SOURCE
            report["maximum_p95_ms"] = (
                self.config.max_first_page_first_observation_p95_ms
            )
            self._record_latency_failure(
                progress,
                category=category,
                reason=reason,
                observed_p95_ms=gate_p95_ms,
                maximum_p95_ms=(
                    self.config.max_first_page_first_observation_p95_ms
                ),
            )
            return report

        uses_signed_capacity_gate = self.config.capacity_evidence_trust is not None
        if uses_signed_capacity_gate:
            positive_first_page = verified_client_gated_report(
                CAPACITY_POSITIVE_COHORT,
                category="first_page_latency",
                reason=(
                    "matched_positive_first_observation_first_page_p95_above_limit"
                ),
            )
            negative_first_page = verified_client_gated_report(
                CAPACITY_NEGATIVE_COHORT,
                category="negative_first_page_latency",
                reason="negative_first_observation_first_page_p95_above_limit",
            )
            random_first_page = verified_client_gated_report(
                CAPACITY_RANDOM_COHORT,
                category="random_first_page_latency",
                reason="random_first_observation_first_page_p95_above_limit",
            )
        else:
            positive_first_page = client_gated_report(
                progress.first_page_first_observation_ms,
                category="first_page_latency",
                reason=(
                    "matched_positive_first_observation_first_page_p95_above_limit"
                ),
                maximum_p95_ms=(
                    self.config.max_first_page_first_observation_p95_ms
                ),
            )
            negative_first_page = client_gated_report(
                progress.negative_first_page_ms,
                category="negative_first_page_latency",
                reason="negative_first_observation_first_page_p95_above_limit",
                maximum_p95_ms=(
                    self.config.max_first_page_first_observation_p95_ms
                ),
            )
            random_first_page = client_gated_report(
                progress.random_cold_first_page_ms,
                category="random_first_page_latency",
                reason="random_first_observation_first_page_p95_above_limit",
                maximum_p95_ms=(
                    self.config.max_first_page_first_observation_p95_ms
                ),
            )
        first_page_report_map = dict(positive_first_page)
        first_page_report_map["by_class"] = {
            "matched_positive": positive_first_page,
            "negative": negative_first_page,
            "random_positive": random_first_page,
        }
        if uses_signed_capacity_gate:
            first_page_report_map["measurement_source"] = (
                CAPACITY_CLIENT_LATENCY_SOURCE
            )
            first_page_report_map["client_diagnostics"] = {
                "measurement_source": CAPACITY_UNQUALIFIED_CLIENT_LATENCY_SOURCE,
                "by_class": {
                    "matched_positive": client_diagnostic_report(
                        progress.first_page_first_observation_ms
                    ),
                    "negative": client_diagnostic_report(
                        progress.negative_first_page_ms
                    ),
                    "random_positive": client_diagnostic_report(
                        progress.random_cold_first_page_ms
                    ),
                },
            }

        if uses_signed_capacity_gate:
            positive_logical_query = client_diagnostic_report(
                progress.positive_cold_query_ms
            )
            negative_logical_query = client_diagnostic_report(
                progress.negative_cold_query_ms
            )
            random_logical_query = client_diagnostic_report(
                progress.random_cold_latency_ms
            )
        else:
            positive_logical_query = client_gated_report(
                progress.positive_cold_query_ms,
                category="logical_query_latency",
                reason="matched_positive_logical_query_p95_above_limit",
                maximum_p95_ms=self.config.max_logical_query_p95_ms,
            )
            negative_logical_query = client_gated_report(
                progress.negative_cold_query_ms,
                category="negative_logical_query_latency",
                reason="negative_logical_query_p95_above_limit",
                maximum_p95_ms=self.config.max_logical_query_p95_ms,
            )
            random_logical_query = client_gated_report(
                progress.random_cold_latency_ms,
                category="random_logical_query_latency",
                reason="random_positive_logical_query_p95_above_limit",
                maximum_p95_ms=self.config.max_logical_query_p95_ms,
            )
        logical_query_report_map = dict(positive_logical_query)
        logical_query_report_map["by_class"] = {
            "matched_positive": positive_logical_query,
            "negative": negative_logical_query,
            "random_positive": random_logical_query,
        }
        cold_page_report = latency_summary(progress.cold_page_ms)
        if uses_signed_capacity_gate:
            logical_query_report_map["measurement_source"] = (
                CAPACITY_UNQUALIFIED_CLIENT_LATENCY_SOURCE
            )
            logical_query_report_map["diagnostic_only"] = True
            cold_page_report["measurement_source"] = (
                CAPACITY_UNQUALIFIED_CLIENT_LATENCY_SOURCE
            )
            cold_page_report["diagnostic_only"] = True
        return _LatencyReports(
            first_page=first_page_report_map,
            cold_pages=cold_page_report,
            logical_query=logical_query_report_map,
        )

    def _sampling_report(self, samples: _AuditSamples) -> dict[str, Any]:
        return {
            "method": "seeded_sha256_bottom_k",
            "source_occurrence_sampler_capacity": self.source_index.occurrence_sampler.capacity,
            "source_occurrence_target": self.config.source_occurrence_samples,
            "api_occurrence_target": self.config.api_occurrence_samples,
            "negative_target": self.config.negative_samples,
            "random_api_call_target": self.config.random_api_calls,
            "source_population": "immutable_source_rate_price_provider_occurrence_ids",
            "api_sample_contract": samples.api_sample.contract,
            "api_sample_method": samples.api_sample.method,
            "api_sample_count": samples.api_sample.sample_count,
            "api_sample_complete_population": samples.api_sample.complete_population,
            "api_sample_digest_validated": samples.api_sample.sample_digest_validated,
            "source_set_validated_before_sampling": (
                samples.api_sample.source_set_validated
            ),
            "api_sample_source": "persisted_publish_time_served_occurrences",
            "independence": "API sampling receives no source-selected query keys",
            "negative_strategy": "seeded_recombination_of_individually_positive_codes_and_npis",
            "random_api_strategy": (
                "seeded_unique_without_replacement_fixed_page_capacity_cohort"
                if self.config.capacity_evidence_trust is not None
                else (
                    "seeded_with_replacement_source_occurrence_draws_with_reserved_query_keys_"
                    "and_cold_before_repeat_full_logical_query_traversal"
                )
            ),
        }

    def _reproducibility_report(self, samples: _AuditSamples) -> dict[str, Any]:
        seed_contract = sampling_seed_evidence(
            self.config.profile,
            self.config.seed,
            self.config.capacity_evidence_trust,
        )
        return {
            "seed_profile": "default_v1" if self.config.seed == DEFAULT_SEED else "custom",
            "seed_sha256": sha256_text(self.config.seed),
            "seed_contract": seed_contract,
            "python_version": platform.python_version(),
            "source_order": "ascending_content_sha256",
            "sampling": self._sampling_report(samples),
            "canonicalization": CANONICALIZATION,
        }

    def _coverage_report(
        self,
        progress: _AuditProgress,
        resolved_fraction: float,
    ) -> dict[str, Any]:
        return {
            "resolved_rate_fraction": round(resolved_fraction, 9),
            "requirements": {
                "min_source_occurrence_checks": self.config.min_source_occurrence_checks,
                "min_api_occurrence_checks": self.config.min_api_occurrence_checks,
                "min_negative_checks": self.config.min_negative_checks,
                "min_random_api_calls": self.config.min_random_api_calls,
                "min_distinct_matched_positive_query_keys": (
                    RELEASE_MIN_DISTINCT_MATCHED_QUERY_KEYS
                    if self.config.profile == "release"
                    else 0
                ),
                "min_resolved_rate_fraction": self.config.min_resolved_rate_fraction,
                "max_unresolved_provider_references": self.config.max_unresolved_provider_references,
                "max_invalid_prices": self.config.max_invalid_prices,
                "max_invalid_npis": self.config.max_invalid_npis,
                "max_invalid_field_types": self.config.max_invalid_field_types,
                "max_first_page_first_observation_p95_ms": (
                    self.config.max_first_page_first_observation_p95_ms
                ),
                "max_logical_query_p95_ms": self.config.max_logical_query_p95_ms,
            },
            "failures": sorted(set(progress.coverage_reasons)),
        }

    def _random_api_report(
        self,
        samples: _AuditSamples,
        progress: _AuditProgress,
        random_http_requests: int,
    ) -> dict[str, Any]:
        random_cold_latency = latency_summary(progress.random_cold_latency_ms)
        random_warm_latency = latency_summary(progress.random_warm_latency_ms)
        if self.config.capacity_evidence_trust is not None:
            for diagnostic_report in (
                random_cold_latency,
                random_warm_latency,
            ):
                diagnostic_report["measurement_source"] = (
                    CAPACITY_UNQUALIFIED_CLIENT_LATENCY_SOURCE
                )
                diagnostic_report["diagnostic_only"] = True
        return {
            "planned_logical_request_count": progress.checks["random_api_requests_executed"],
            "actual_http_request_count": random_http_requests,
            "distinct_query_count": len(samples.random_query_keys),
            "first_observation_query_count": sum(
                request.phase == "cold" for request in samples.random_requests
            ),
            "reserved_from_positive_first_observation_count": len(
                samples.positive_reserved_queries
            ),
            "repeated_query_request_count": len(samples.random_requests)
            - len(samples.random_query_keys),
            "distinct_request_shape_count": len(samples.random_request_shape_keys),
            "repeated_request_shape_count": len(samples.random_requests)
            - len(samples.random_request_shape_keys),
            "response_occurrence_count": progress.random_response_occurrences,
            "failures": dict(sorted(progress.random_failure_counts.items())),
            "latency": {
                "cold": random_cold_latency,
                "warm": random_warm_latency,
                "semantics": (
                    "cold is the first standard-pricing request in this run for each reserved "
                    "query key; every cold draw completes before repeated warm draws begin"
                ),
            },
            "retries_observed": progress.random_retries,
            "max_page_size": self.config.random_api_max_limit,
            "fingerprints": {
                "request_plan_sha256": samples.random_plan_sha256,
                "ordered_responses_sha256": progress.random_response_fingerprint.hexdigest(),
                "distinct_query_keys_sha256": sha256_text(
                    canonical_json(
                        sorted(sha256_text(key) for key in samples.random_query_keys)
                    )
                ),
            },
        }

    def _http_report(
        self,
        samples: _AuditSamples,
        progress: _AuditProgress,
        standard_http_requests: int,
        capacity_report: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "page_size": self.config.page_size,
            "max_pages": self.config.max_pages,
            "api_audit_page_size": self.config.api_audit_page_size,
            "api_audit_max_pages": self.config.api_audit_max_pages,
            "api_audit_pages_observed": samples.api_sample.pages,
            "warm_repeats": self.config.warm_repeats,
            "concurrency": self.config.concurrency,
            "standard_api_actual_http_requests": standard_http_requests,
            "release_min_actual_http_requests": RELEASE_MIN_TOTAL_API_CALLS,
            "retries_observed": progress.retries + samples.api_sample.retries,
            "capacity_evidence": dict(capacity_report),
        }

    @staticmethod
    def _latency_report(
        progress: _AuditProgress,
        latency_reports: _LatencyReports,
    ) -> dict[str, Any]:
        """Describe signed cold gates and nongating warm diagnostics."""

        uses_signed_capacity_gate = (
            latency_reports.first_page.get("measurement_source")
            == CAPACITY_CLIENT_LATENCY_SOURCE
        )
        warm_page_report = latency_summary(progress.warm_page_ms)
        warm_query_report = latency_summary(progress.warm_query_ms)
        if uses_signed_capacity_gate:
            for diagnostic_report in (warm_page_report, warm_query_report):
                diagnostic_report["measurement_source"] = (
                    CAPACITY_UNQUALIFIED_CLIENT_LATENCY_SOURCE
                )
                diagnostic_report["diagnostic_only"] = True
        latency_semantics = _latency_gate_semantics(uses_signed_capacity_gate)
        return {
            "gate_source": (
                CAPACITY_CLIENT_LATENCY_SOURCE
                if uses_signed_capacity_gate
                else "client_perf_counter"
            ),
            "cold": {
                "first_page_first_observation": latency_reports.first_page,
                "all_pages": latency_reports.cold_pages,
                "logical_query": latency_reports.logical_query,
            },
            "warm": {
                "all_pages": warm_page_report,
                "logical_query": warm_query_report,
            },
            "semantics": {
                **latency_semantics,
                "query_key_cohorts": (
                    "random query keys are reserved from positive first-observation traversal; "
                    "their positive exactness checks run afterward without contributing to cold gates"
                ),
                "measurement_order": (
                    "source-set preflight, persisted API audit-sample enumeration, then "
                    "non-random positive first observations, random cold draws, random repeats, "
                    "reserved positive exactness traversals, and negative queries"
                ),
            },
        }

    def _build_report(
        self,
        samples: _AuditSamples,
        progress: _AuditProgress,
        context: _AuditReportContext,
    ) -> dict[str, Any]:
        return {
            "schema_version": 2,
            "harness": {"name": "ptg2_v3_source_api_audit", "version": SCRIPT_VERSION},
            "status": "pass" if not progress.failure_counts else "fail",
            "profile": self.config.profile,
            "release_profile_enforced": self.config.profile == "release",
            "release_gate_eligible": (
                self.config.profile == "release" and not progress.failure_counts
            ),
            "started_at": context.started_at.isoformat(),
            "completed_at": context.completed_at.isoformat(),
            "duration_seconds": round(
                (context.completed_at - context.started_at).total_seconds(), 3
            ),
            "target": self.config.redacted_target(),
            "reproducibility": self._reproducibility_report(samples),
            "source": self.source_index.source_report(),
            "api_audit_sample": {
                "contract": samples.api_sample.contract,
                "method": samples.api_sample.method,
                "sample_count": samples.api_sample.sample_count,
                "complete_population": samples.api_sample.complete_population,
                "sample_digest_validated": samples.api_sample.sample_digest_validated,
                "sample_digest": samples.api_sample.sample_digest,
                "source_set_validated": samples.api_sample.source_set_validated,
                "selected_occurrences": len(samples.api_occurrences),
            },
            "coverage": self._coverage_report(progress, context.resolved_fraction),
            "checks": dict(sorted(progress.checks.items())),
            "random_api_requests": self._random_api_report(
                samples, progress, context.random_http_requests
            ),
            "http": self._http_report(
                samples,
                progress,
                context.standard_http_requests,
                context.capacity_evidence,
            ),
            "latency": self._latency_report(progress, context.latency_reports),
            "failures": {
                "counts": dict(sorted(progress.failure_counts.items())),
                "examples": progress.failure_examples,
                "example_limit": self.config.failure_example_limit,
            },
            "limitations": list(AUDIT_LIMITATIONS),
            "redaction": {
                "policy": "sensitive_identifiers_excluded",
                "excluded": list(REDACTION_EXCLUDED_FIELDS),
            },
        }

    def execute_audit(self, *, started_at: dt.datetime) -> dict[str, Any]:
        """Execute every release audit phase and return the redacted report."""

        standard_request_count_start = _fetcher_request_count(self.fetcher)
        samples = self._prepare_samples()
        progress = self._initial_progress(samples)
        self._run_positive_checks(
            samples,
            progress,
            samples.positive_first_observation_queries,
            initial_phase="cold",
        )
        random_http_requests = self._run_random_checks(samples, progress)
        self._run_positive_checks(
            samples,
            progress,
            samples.positive_reserved_queries,
            initial_phase="warm",
        )
        self._run_negative_checks(samples, progress)
        resolved_fraction = self._resolved_rate_fraction()
        standard_http_requests = self._standard_http_request_count(
            standard_request_count_start,
            progress,
        )
        progress.checks["random_api_actual_http_requests"] = random_http_requests
        progress.checks["standard_api_actual_http_requests"] = standard_http_requests
        capacity_report = (
            self.capacity_collector.report(
                complete_request_http_requests=standard_http_requests,
                complete_request_retries=progress.retries,
            )
            if self.capacity_collector is not None
            else disabled_capacity_evidence_report()
        )
        self._record_capacity_evidence_checks(progress, capacity_report)
        self._apply_coverage_checks(
            progress,
            resolved_fraction=resolved_fraction,
            standard_http_requests=standard_http_requests,
            capacity_report=capacity_report,
        )
        latency_reports = self._build_latency_reports(progress, capacity_report)
        context = _AuditReportContext(
            started_at=started_at,
            completed_at=dt.datetime.now(dt.timezone.utc),
            resolved_fraction=resolved_fraction,
            random_http_requests=random_http_requests,
            standard_http_requests=standard_http_requests,
            latency_reports=latency_reports,
            capacity_evidence=capacity_report,
        )
        return self._build_report(samples, progress, context)

    @staticmethod
    def _record_capacity_evidence_checks(
        progress: _AuditProgress,
        capacity_report: Mapping[str, Any],
    ) -> None:
        capacity_counters = capacity_report.get("counters")
        if not isinstance(capacity_counters, Mapping):
            return
        progress.checks["capacity_evidence_eligible_http_requests"] = int(
            capacity_counters.get("eligible", 0)
        )
        cohorts_by_name = capacity_counters.get("cohorts")
        if isinstance(cohorts_by_name, Mapping):
            for check_name, cohort in (
                ("matched_positive", CAPACITY_POSITIVE_COHORT),
                ("negative", CAPACITY_NEGATIVE_COHORT),
                ("random", CAPACITY_RANDOM_COHORT),
            ):
                cohort_report = cohorts_by_name.get(cohort)
                if isinstance(cohort_report, Mapping):
                    progress.checks[
                        f"capacity_evidence_{check_name}_eligible_http_requests"
                    ] = int(cohort_report.get("eligible", 0))
        random_cohort = (
            cohorts_by_name.get(CAPACITY_RANDOM_COHORT)
            if isinstance(cohorts_by_name, Mapping)
            else None
        )
        if isinstance(random_cohort, Mapping):
            progress.checks["capacity_evidence_distinct_random_query_keys"] = int(
                random_cohort.get("distinct_semantic_queries", 0)
            )


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be at least 0")
    return parsed


def _fraction(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or not 0.0 <= parsed <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def _header(value: str) -> tuple[str, str]:
    name, separator, header_value = value.partition("=")
    name = name.strip()
    if not separator or not name or not header_value:
        raise argparse.ArgumentTypeError("header must be NAME=VALUE")
    if any(character in name + header_value for character in "\r\n"):
        raise argparse.ArgumentTypeError("header must not contain line breaks")
    return name, header_value


def _add_capacity_evidence_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--capacity-api-evidence-key-id",
        default=os.getenv("PTG_AUDIT_CAPACITY_API_EVIDENCE_KEY_ID"),
    )
    parser.add_argument(
        "--capacity-api-evidence-public-key-hex",
        default=os.getenv("PTG_AUDIT_CAPACITY_API_EVIDENCE_PUBLIC_KEY_HEX"),
    )
    parser.add_argument(
        "--capacity-release-digest",
        default=os.getenv("PTG_AUDIT_CAPACITY_RELEASE_DIGEST"),
    )
    parser.add_argument(
        "--capacity-environment-id",
        default=os.getenv("PTG_AUDIT_CAPACITY_ENVIRONMENT_ID"),
    )
    parser.add_argument(
        "--capacity-contention-run-id",
        default=os.getenv("PTG_AUDIT_CAPACITY_CONTENTION_RUN_ID"),
    )


def _add_target_arguments(parser: argparse.ArgumentParser) -> None:
    """Add source, API target, auth, and capacity-evidence arguments."""

    parser.add_argument("sources", nargs="+", type=Path, help="Original TiC in-network JSON or gzip file(s)")
    parser.add_argument("--report", type=Path, required=True, help="Machine-readable JSON report path")
    parser.add_argument("--profile", choices=("release", "diagnostic"), default="release")
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("PTG_AUDIT_API_BASE_URL"),
        required=os.getenv("PTG_AUDIT_API_BASE_URL") is None,
    )
    parser.add_argument("--api-path", default=DEFAULT_API_PATH)
    parser.add_argument(
        "--api-audit-path",
        default=os.getenv("PTG_AUDIT_API_AUDIT_PATH", DEFAULT_API_AUDIT_PATH),
    )
    parser.add_argument(
        "--plan-id",
        default=os.getenv("PTG_AUDIT_PLAN_ID"),
        required=os.getenv("PTG_AUDIT_PLAN_ID") is None,
    )
    parser.add_argument(
        "--snapshot-id",
        default=os.getenv("PTG_AUDIT_SNAPSHOT_ID"),
        required=os.getenv("PTG_AUDIT_SNAPSHOT_ID") is None,
    )
    parser.add_argument("--plan-market-type", default=os.getenv("PTG_AUDIT_PLAN_MARKET_TYPE"))
    parser.add_argument("--source-key", default=os.getenv("PTG_AUDIT_SOURCE_KEY"))
    parser.add_argument("--auth-token", default=os.getenv("PTG_AUDIT_AUTH_TOKEN"))
    parser.add_argument("--auth-header", default="Authorization")
    parser.add_argument("--auth-scheme", default="Bearer")
    parser.add_argument("--header", action="append", type=_header, default=[], metavar="NAME=VALUE")
    parser.add_argument(
        "--validated-candidate",
        action="store_true",
        help=(
            "Use control-token-authenticated request-scoped access to one validated "
            "candidate; requires source and market selectors"
        ),
    )
    parser.add_argument(
        "--trusted-cluster-http",
        action="store_true",
        help=(
            "Allow authenticated candidate audit traffic over a Kubernetes "
            "service origin; never valid for published/public release audits"
        ),
    )
    parser.add_argument(
        "--seed",
        default=None,
        help="Diagnostic-only sampling seed; release profiles use authoritative precommitment",
    )
    _add_capacity_evidence_arguments(parser)


def _add_sample_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source-occurrence-samples",
        "--positive-samples",
        dest="source_occurrence_samples",
        type=_positive_int,
        default=DEFAULT_SOURCE_OCCURRENCE_SAMPLES,
    )
    parser.add_argument(
        "--api-occurrence-samples",
        type=_positive_int,
        default=DEFAULT_API_OCCURRENCE_SAMPLES,
    )
    parser.add_argument("--negative-samples", type=_positive_int, default=DEFAULT_NEGATIVE_SAMPLES)
    parser.add_argument(
        "--random-api-calls",
        type=_positive_int,
        default=DEFAULT_RANDOM_API_CALLS,
        help="Deterministic positive standard-pricing API window requests",
    )
    parser.add_argument(
        "--random-api-max-limit",
        type=_positive_int,
        default=DEFAULT_RANDOM_API_MAX_LIMIT,
    )
    parser.add_argument(
        "--min-source-occurrence-checks",
        "--min-positive-checks",
        dest="min_source_occurrence_checks",
        type=_nonnegative_int,
        default=RELEASE_MIN_SOURCE_OCCURRENCES,
    )
    parser.add_argument(
        "--min-api-occurrence-checks",
        "--min-api-to-source-checks",
        dest="min_api_occurrence_checks",
        type=_nonnegative_int,
        default=RELEASE_MIN_API_OCCURRENCES,
    )
    parser.add_argument(
        "--min-negative-checks",
        type=_nonnegative_int,
        default=RELEASE_MIN_NEGATIVE_QUERIES,
    )
    parser.add_argument(
        "--min-random-api-calls",
        type=_nonnegative_int,
        default=RELEASE_MIN_RANDOM_API_CALLS,
    )


def _add_release_limit_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--min-resolved-rate-fraction", type=_fraction, default=1.0)
    parser.add_argument("--max-unresolved-provider-references", type=_nonnegative_int, default=0)
    parser.add_argument("--max-invalid-prices", type=_nonnegative_int, default=0)
    parser.add_argument("--max-invalid-npis", type=_nonnegative_int, default=0)
    parser.add_argument("--max-invalid-field-types", type=_nonnegative_int, default=0)
    parser.add_argument(
        "--max-first-page-first-observation-p95-ms",
        "--max-cold-page-p95-ms",
        dest="max_first_page_first_observation_p95_ms",
        type=_positive_float,
        default=RELEASE_MAX_FIRST_PAGE_P95_MS,
    )
    parser.add_argument(
        "--max-logical-query-p95-ms",
        type=_positive_float,
        default=RELEASE_MAX_LOGICAL_QUERY_P95_MS,
    )


def _add_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--page-size", type=_positive_int, default=100)
    parser.add_argument("--max-pages", type=_positive_int, default=100)
    parser.add_argument(
        "--api-audit-page-size",
        type=_positive_int,
        default=DEFAULT_API_AUDIT_PAGE_SIZE,
    )
    parser.add_argument("--api-audit-max-pages", type=_positive_int, default=100_000)
    parser.add_argument("--warm-repeats", type=_positive_int, default=1)
    parser.add_argument("--concurrency", type=_positive_int, default=8)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--http-retries", type=_nonnegative_int, default=2)
    parser.add_argument("--retry-backoff-seconds", type=float, default=0.25)
    parser.add_argument("--max-response-bytes", type=_positive_int, default=16 * 1024 * 1024)
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification (reported and discouraged)")
    parser.add_argument("--sqlite-cache-mb", type=_positive_int, default=64)
    parser.add_argument("--max-list-values", type=_positive_int, default=10_000)
    parser.add_argument("--max-tuples-per-query", type=_positive_int, default=10_000)
    parser.add_argument("--work-dir", type=Path, help="Parent directory for the temporary SQLite index")
    parser.add_argument("--failure-example-limit", type=_nonnegative_int, default=50)


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the CLI parser without relaxing release-profile defaults."""

    parser = argparse.ArgumentParser(
        description=(
            "Independently audit original TiC JSON sources against a pinned strict "
            "postgres_binary_v3 pricing API snapshot."
        )
    )
    _add_target_arguments(parser)
    _add_sample_arguments(parser)
    _add_release_limit_arguments(parser)
    _add_runtime_arguments(parser)
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        raise ConfigurationError("timeout_must_be_positive")
    if not math.isfinite(args.retry_backoff_seconds) or args.retry_backoff_seconds < 0:
        raise ConfigurationError("retry_backoff_must_be_nonnegative")
    if not canonical_scalar(args.plan_id) or not canonical_scalar(args.snapshot_id):
        raise ConfigurationError("plan_and_snapshot_must_be_nonempty")
    if args.seed is not None and not canonical_scalar(args.seed):
        raise ConfigurationError("seed_must_be_nonempty")
    if args.profile == "release" and args.seed is not None:
        raise ConfigurationError("release_seed_caller_override_forbidden")
    if args.work_dir is not None and not args.work_dir.is_dir():
        raise ConfigurationError("work_dir_must_exist")
    if args.validated_candidate and (
        not canonical_scalar(args.auth_token)
        or not canonical_scalar(args.source_key)
        or not canonical_scalar(args.plan_market_type)
    ):
        raise ConfigurationError(
            "validated_candidate_requires_auth_source_and_market"
        )
    if args.trusted_cluster_http and not args.validated_candidate:
        raise ConfigurationError(
            "trusted_cluster_http_requires_validated_candidate"
        )


def _capacity_evidence_trust(
    args: argparse.Namespace,
) -> CapacityEvidenceTrust | None:
    raw_values = (
        args.capacity_api_evidence_key_id,
        args.capacity_api_evidence_public_key_hex,
        args.capacity_release_digest,
        args.capacity_environment_id,
    )
    if not any(value not in (None, "") for value in raw_values):
        return None
    if any(value in (None, "") for value in raw_values):
        raise ConfigurationError("capacity_evidence_trust_incomplete")
    public_key_hex = str(args.capacity_api_evidence_public_key_hex)
    if (
        len(public_key_hex) != 64
        or any(character not in "0123456789abcdef" for character in public_key_hex)
    ):
        raise ConfigurationError("capacity_api_evidence_public_key_invalid")
    return CapacityEvidenceTrust(
        api_evidence_key_id=str(args.capacity_api_evidence_key_id),
        api_evidence_public_key=bytes.fromhex(public_key_hex),
        release_digest=str(args.capacity_release_digest),
        environment_id=str(args.capacity_environment_id),
    )


def _request_headers(args: argparse.Namespace) -> dict[str, str]:
    header_map = {name: value for name, value in args.header}
    if args.auth_token:
        token_value = str(args.auth_token)
        scheme = str(args.auth_scheme or "").strip()
        header_map[str(args.auth_header)] = f"{scheme} {token_value}".strip()
    if args.validated_candidate:
        header_map[CANDIDATE_AUDIT_HEADER] = str(args.snapshot_id).strip()
    header_map.setdefault("Accept", "application/json")
    header_map.setdefault("User-Agent", f"ptg2-v3-source-api-audit/{SCRIPT_VERSION}")
    return header_map


def _audit_config(args: argparse.Namespace) -> AuditConfig:
    capacity_trust = _capacity_evidence_trust(args)
    seed = (
        authoritative_release_seed(capacity_trust)
        if args.profile == "release"
        else str(args.seed if args.seed is not None else DEFAULT_SEED)
    )
    return AuditConfig(
        profile=args.profile,
        api_base_url=str(args.api_base_url),
        api_path=str(args.api_path),
        api_audit_path=str(args.api_audit_path),
        plan_id=str(args.plan_id).strip(),
        snapshot_id=str(args.snapshot_id).strip(),
        plan_market_type=canonical_scalar(args.plan_market_type),
        source_key=canonical_scalar(args.source_key),
        seed=seed,
        source_occurrence_samples=args.source_occurrence_samples,
        api_occurrence_samples=args.api_occurrence_samples,
        negative_samples=args.negative_samples,
        random_api_calls=args.random_api_calls,
        random_api_max_limit=args.random_api_max_limit,
        min_source_occurrence_checks=args.min_source_occurrence_checks,
        min_api_occurrence_checks=args.min_api_occurrence_checks,
        min_negative_checks=args.min_negative_checks,
        min_random_api_calls=args.min_random_api_calls,
        min_resolved_rate_fraction=args.min_resolved_rate_fraction,
        max_unresolved_provider_references=args.max_unresolved_provider_references,
        max_invalid_prices=args.max_invalid_prices,
        max_invalid_npis=args.max_invalid_npis,
        max_invalid_field_types=args.max_invalid_field_types,
        page_size=args.page_size,
        max_pages=args.max_pages,
        api_audit_page_size=args.api_audit_page_size,
        api_audit_max_pages=args.api_audit_max_pages,
        warm_repeats=args.warm_repeats,
        concurrency=args.concurrency,
        failure_example_limit=args.failure_example_limit,
        verify_tls=not args.insecure,
        validated_candidate=bool(args.validated_candidate),
        trusted_cluster_http=bool(args.trusted_cluster_http),
        max_first_page_first_observation_p95_ms=(
            args.max_first_page_first_observation_p95_ms
        ),
        max_logical_query_p95_ms=args.max_logical_query_p95_ms,
        capacity_evidence_trust=capacity_trust,
        capacity_contention_run_id=(
            str(args.capacity_contention_run_id)
            if args.capacity_contention_run_id not in (None, "")
            else None
        ),
    )


def write_report_atomic(path: Path, report: Mapping[str, Any]) -> None:
    """Atomically replace the JSON report without exposing partial output."""

    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(report, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(serialized, encoding="utf-8")
    os.replace(temporary, path)


def fatal_report(
    *,
    started_at: dt.datetime,
    exc: BaseException,
    config: AuditConfig | None,
    specs: Sequence[SourceSpec],
) -> dict[str, Any]:
    """Build a redacted error report for a fatal audit failure."""

    completed_at = dt.datetime.now(dt.timezone.utc)
    code = exc.code if isinstance(exc, AuditError) else "internal_error"
    failure_example_by_field = {
        "category": code,
        "exception_type": type(exc).__name__,
    }
    if isinstance(exc, ConfigurationError):
        failure_example_by_field["reason"] = str(exc)
    return {
        "schema_version": 2,
        "harness": {"name": "ptg2_v3_source_api_audit", "version": SCRIPT_VERSION},
        "status": "error",
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": round((completed_at - started_at).total_seconds(), 3),
        "target": config.redacted_target() if config else {"expected_architecture": EXPECTED_ARCHITECTURE},
        "reproducibility": {
            "seed_profile": (
                "default_v1" if config and config.seed == DEFAULT_SEED else "custom" if config else None
            ),
            "seed_sha256": sha256_text(config.seed) if config else None,
            "seed_contract": (
                sampling_seed_evidence(
                    config.profile,
                    config.seed,
                    config.capacity_evidence_trust,
                )
                if config
                else None
            ),
            "canonicalization": CANONICALIZATION,
            "sources": [
                {
                    "source_fingerprint": fingerprint(spec.source_id),
                    "stored_bytes": spec.stored_bytes,
                    "encoding": spec.encoding,
                }
                for spec in specs
            ],
        },
        "failures": {
            "counts": {code: 1},
            "examples": [failure_example_by_field],
        },
        "redaction": {"policy": "sensitive_identifiers_excluded"},
    }


def _http_fetcher_options(
    args: argparse.Namespace,
    config: AuditConfig,
    *,
    api_path: str,
    page_size: int,
    max_pages: int,
) -> HttpApiFetcherOptions:
    return HttpApiFetcherOptions(
        base_url=config.api_base_url,
        api_path=api_path,
        headers=_request_headers(args),
        page_size=page_size,
        max_pages=max_pages,
        timeout_seconds=args.timeout_seconds,
        retries=args.http_retries,
        retry_backoff_seconds=args.retry_backoff_seconds,
        max_response_bytes=args.max_response_bytes,
        verify_tls=config.verify_tls,
    )


def _execute_indexed_audit(
    args: argparse.Namespace,
    config: AuditConfig,
    specs: Sequence[SourceSpec],
    source_index: SourceIndex,
    *,
    started_at: dt.datetime,
) -> dict[str, Any]:
    source_index.index(specs)
    verify_sources_unchanged(specs)
    standard_options = _http_fetcher_options(args, config, api_path=config.api_path, page_size=config.page_size, max_pages=config.max_pages)
    audit_options = _http_fetcher_options(args, config, api_path=config.api_audit_path, page_size=config.api_audit_page_size, max_pages=config.api_audit_max_pages)
    capacity_collector = (
        CapacityEvidenceCollector(
            config.capacity_evidence_trust,
            contention_run_id=str(config.capacity_contention_run_id),
            sampling_seed=config.seed,
            sampling_profile=config.profile,
        )
        if config.capacity_evidence_trust is not None
        else None
    )
    with HttpApiFetcher(standard_options, capacity_collector=capacity_collector) as fetcher, HttpApiFetcher(audit_options) as audit_fetcher:
        occurrence_source = HttpApiOccurrenceSource(audit_fetcher, config, source_set_evidence_from_specs(specs))
        return AuditRunner(source_index, fetcher, occurrence_source, config, capacity_collector=capacity_collector).execute_audit(started_at=started_at)


def _execute_configured_audit(
    args: argparse.Namespace,
    config: AuditConfig,
    specs: Sequence[SourceSpec],
    *,
    started_at: dt.datetime,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(
        prefix="ptg2-v3-source-audit-",
        dir=args.work_dir,
    ) as temp_dir:
        database_path = Path(temp_dir) / "source-index.sqlite3"
        with SourceIndex(
            database_path,
            seed=config.seed,
            source_occurrence_sample_target=config.source_occurrence_samples,
            capacity_query_sample_target=(
                config.random_api_calls
                if config.capacity_evidence_trust is not None
                else 0
            ),
            sqlite_cache_mb=args.sqlite_cache_mb,
            max_list_values=args.max_list_values,
            max_tuples_per_query=args.max_tuples_per_query,
        ) as source_index:
            return _execute_indexed_audit(
                args, config, specs, source_index, started_at=started_at
            )


def _print_audit_summary(
    report_path: Path,
    audit_report_by_section: Mapping[str, Any],
) -> None:
    print(
        canonical_json(
            {
                "status": audit_report_by_section["status"],
                "report": str(report_path),
                "checks": audit_report_by_section["checks"],
                "failure_counts": audit_report_by_section["failures"]["counts"],
            }
        )
    )


def _write_fatal_cli_result(
    args: argparse.Namespace,
    *,
    started_at: dt.datetime,
    exc: BaseException,
    config: AuditConfig | None,
    specs: Sequence[SourceSpec],
) -> int:
    error_report_by_section = fatal_report(
        started_at=started_at,
        exc=exc,
        config=config,
        specs=specs,
    )
    with contextlib.suppress(OSError):
        write_report_atomic(args.report, error_report_by_section)
    print(
        canonical_json(
            {
                "status": "error",
                "category": next(iter(error_report_by_section["failures"]["counts"])),
            }
        ),
        file=sys.stderr,
    )
    return 2


def run_audit_cli(argv: Sequence[str] | None = None) -> int:
    """Parse CLI input, execute the independent audit, and write its report."""

    parser = build_argument_parser()
    args = parser.parse_args(argv)
    started_at = dt.datetime.now(dt.timezone.utc)
    specs: list[SourceSpec] = []
    config: AuditConfig | None = None
    try:
        _validate_args(args)
        config = _audit_config(args)
        specs = source_specs(args.sources)
        audit_report_by_section = _execute_configured_audit(
            args,
            config,
            specs,
            started_at=started_at,
        )
        write_report_atomic(args.report, audit_report_by_section)
        _print_audit_summary(args.report, audit_report_by_section)
        return 0 if audit_report_by_section["status"] == "pass" else 1
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return _write_fatal_cli_result(
            args,
            started_at=started_at,
            exc=exc,
            config=config,
            specs=specs,
        )


def main(argv: Sequence[str] | None = None) -> int:
    """Return the process exit code for the PTG2 v3 source/API audit CLI."""

    return run_audit_cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())
