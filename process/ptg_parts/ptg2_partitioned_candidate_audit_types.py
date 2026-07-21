# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Types and canonical primitives for partitioned candidate audits."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping


PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT = (
    "ptg2_v3_partitioned_candidate_audit_request_v1"
)
PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT = (
    "ptg2_v3_partitioned_candidate_audit_result_v1"
)
PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_ITEMS = 100
# Keep the public parser ceiling at 100 for compatibility, but emit smaller
# partitions so dense provider graphs stay within the 512 MiB read-once budget.
PTG2_PARTITIONED_CANDIDATE_AUDIT_TARGET_ITEMS = 50
PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_NETWORK_DIGESTS = 64
PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_IN_FLIGHT = 8
PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUESTS_PER_SECOND = 2.0

REQUEST_FIELDS = frozenset(
    {
        "contract",
        "snapshot_id",
        "source_key",
        "plan_id",
        "plan_market_type",
        "audit_sample_digest",
        "source_witness_sample_digest",
        "source_witness_payload_sha256",
        "ordered_source_ordinal_digest",
        "plan_source_challenge_count",
        "plan_source_occurrence_count",
        "plan_persisted_occurrence_count",
        "plan_digest",
        "partition_index",
        "partition_count",
        "source_challenge_count",
        "source_occurrence_count",
        "persisted_occurrence_count",
        "item_count",
        "source_challenges",
        "persisted_occurrences",
        "partition_digest",
        "request_digest",
    }
)
SOURCE_FIELDS = frozenset(
    {
        "ordinal",
        "code_system",
        "code",
        "npi",
        "source_artifact_key",
        "tuple_digest",
        "network_name_digests",
        "multiplicity",
    }
)
PERSISTED_FIELDS = frozenset(
    {
        "ordinal",
        "occurrence_id",
        "code_system",
        "code",
        "code_key",
        "provider_set_key",
        "price_key",
        "source_artifact_key",
        "npi",
        "atom_ordinal",
        "atom_key",
    }
)
RESULT_FIELDS = frozenset(
    {
        "contract",
        "plan_digest",
        "partition_index",
        "partition_count",
        "request_digest",
        "source_challenge_count",
        "source_occurrence_count",
        "persisted_occurrence_count",
        "item_count",
        "matched_source_occurrence_count",
        "validated_persisted_occurrence_count",
        "duration_ms",
        "block_io",
        "candidate_processing_io",
        "result_digest",
    }
)
BLOCK_IO_FIELDS = frozenset(
    {
        "logical_block_deliveries",
        "physical_mapping_references",
        "physical_mapping_aliases",
        "unique_physical_blocks",
        "physical_block_reads",
        "physical_block_decodes",
        "physical_payload_preparations",
        "expected_logical_payload_processes",
        "logical_payload_processes",
        "logical_payload_fragment_references",
        "logical_payload_fragment_aliases",
        "repeated_physical_reads",
        "repeated_physical_decodes",
        "repeated_physical_preparations",
        "repeated_logical_payload_processes",
        "peak_raw_bytes",
    }
)
CANDIDATE_IO_FIELDS = frozenset(
    {
        "candidate_occurrence_deliveries",
        "unique_candidate_projections",
        "candidate_projection_builds",
        "candidate_projection_reuse_deliveries",
        "repeated_candidate_projection_builds",
        "availability_condition_count",
        "duplicate_availability_deliveries",
    }
)


def canonical_digest(payload: Any) -> str:
    """Return a stable SHA-256 for one JSON-compatible payload."""

    serialized = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(serialized.encode("ascii")).hexdigest()


def lower_hex(raw_value: Any, *, field_name: str) -> str:
    """Validate and return one lowercase SHA-256 value."""

    digest = str(raw_value or "")
    if (
        len(digest) != 64
        or digest != digest.lower()
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise ValueError(f"{field_name}_invalid")
    return digest


def bounded_text(
    raw_value: Any,
    *,
    field_name: str,
    maximum: int = 512,
) -> str:
    """Validate and return one bounded single-line string."""

    if not isinstance(raw_value, str):
        raise ValueError(f"{field_name}_invalid")
    normalized_text = raw_value.strip()
    if (
        not normalized_text
        or len(normalized_text) > maximum
        or any(char in normalized_text for char in "\r\n")
    ):
        raise ValueError(f"{field_name}_invalid")
    return normalized_text


def nonnegative_integer(
    raw_value: Any,
    *,
    field_name: str,
    minimum: int = 0,
) -> int:
    """Validate and return one strict integer at or above its minimum."""

    if type(raw_value) is not int or raw_value < minimum:
        raise ValueError(f"{field_name}_invalid")
    return raw_value


def valid_npi(raw_value: Any) -> int:
    """Validate and return one ten-digit NPI."""

    npi = nonnegative_integer(
        raw_value,
        field_name="partitioned_audit_npi",
    )
    if not 1_000_000_000 <= npi <= 9_999_999_999:
        raise ValueError("partitioned_audit_npi_invalid")
    return npi


@dataclass(frozen=True, slots=True)
class PartitionedCandidateAuditBinding:
    """Immutable candidate and sealed evidence totals shared by every request."""

    snapshot_id: str
    source_key: str
    plan_id: str
    plan_market_type: str
    audit_sample_digest: str
    source_witness_sample_digest: str
    source_witness_payload_sha256: str
    ordered_source_ordinal_digest: str
    source_occurrence_count: int
    persisted_occurrence_count: int

    @property
    def payload(self) -> dict[str, Any]:
        """Return the shared request binding fields."""

        return {
            "snapshot_id": self.snapshot_id,
            "source_key": self.source_key,
            "plan_id": self.plan_id,
            "plan_market_type": self.plan_market_type,
            "audit_sample_digest": self.audit_sample_digest,
            "source_witness_sample_digest": self.source_witness_sample_digest,
            "source_witness_payload_sha256": self.source_witness_payload_sha256,
            "ordered_source_ordinal_digest": self.ordered_source_ordinal_digest,
            "plan_source_occurrence_count": self.source_occurrence_count,
            "plan_persisted_occurrence_count": self.persisted_occurrence_count,
        }


@dataclass(frozen=True, slots=True, order=True)
class PartitionedSourceChallenge:
    """One exact source-witness condition assigned to one partition."""

    ordinal: int
    code_system: str
    code: str
    npi: int
    source_artifact_key: int
    tuple_digest: str
    network_name_digests: tuple[str, ...]
    multiplicity: int

    @property
    def payload(self) -> dict[str, Any]:
        """Return the canonical source-challenge fields."""

        return {
            "ordinal": self.ordinal,
            "code_system": self.code_system,
            "code": self.code,
            "npi": self.npi,
            "source_artifact_key": self.source_artifact_key,
            "tuple_digest": self.tuple_digest,
            "network_name_digests": list(self.network_name_digests),
            "multiplicity": self.multiplicity,
        }


@dataclass(frozen=True, slots=True, order=True)
class PartitionedPersistedOccurrence:
    """One exact persisted audit coordinate assigned to one partition."""

    ordinal: int
    occurrence_id: bytes
    code_system: str
    code: str
    code_key: int
    provider_set_key: int
    price_key: int
    source_artifact_key: int
    npi: int
    atom_ordinal: int
    atom_key: int

    @property
    def payload(self) -> dict[str, Any]:
        """Return the canonical persisted-coordinate fields."""

        return {
            "ordinal": self.ordinal,
            "occurrence_id": self.occurrence_id.hex(),
            "code_system": self.code_system,
            "code": self.code,
            "code_key": self.code_key,
            "provider_set_key": self.provider_set_key,
            "price_key": self.price_key,
            "source_artifact_key": self.source_artifact_key,
            "npi": self.npi,
            "atom_ordinal": self.atom_ordinal,
            "atom_key": self.atom_key,
        }


@dataclass(frozen=True, slots=True)
class PartitionedCandidateAuditRequest:
    """One authenticated, bounded, non-retrying partition request."""

    binding: PartitionedCandidateAuditBinding
    plan_source_challenge_count: int
    plan_digest: str
    partition_index: int
    partition_count: int
    source_challenges: tuple[PartitionedSourceChallenge, ...]
    persisted_occurrences: tuple[PartitionedPersistedOccurrence, ...]
    partition_digest: str
    request_digest: str

    @property
    def source_occurrence_count(self) -> int:
        """Return source occurrences represented by grouped challenges."""

        return sum(
            challenge.multiplicity for challenge in self.source_challenges
        )

    @property
    def item_count(self) -> int:
        """Return the total bounded coordinate count."""

        return len(self.source_challenges) + len(self.persisted_occurrences)

    @property
    def unsigned_payload(self) -> dict[str, Any]:
        """Return every request field covered by its request digest."""

        return {
            "contract": PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT,
            **self.binding.payload,
            "plan_source_challenge_count": self.plan_source_challenge_count,
            "plan_digest": self.plan_digest,
            "partition_index": self.partition_index,
            "partition_count": self.partition_count,
            "source_challenge_count": len(self.source_challenges),
            "source_occurrence_count": self.source_occurrence_count,
            "persisted_occurrence_count": len(self.persisted_occurrences),
            "item_count": self.item_count,
            "source_challenges": [
                challenge.payload for challenge in self.source_challenges
            ],
            "persisted_occurrences": [
                occurrence.payload
                for occurrence in self.persisted_occurrences
            ],
            "partition_digest": self.partition_digest,
        }

    @property
    def payload(self) -> dict[str, Any]:
        """Return the complete authenticated request payload."""

        return {**self.unsigned_payload, "request_digest": self.request_digest}


@dataclass(frozen=True, slots=True)
class PartitionedCandidateAuditPlan:
    """Complete exact-once request plan for one sealed candidate."""

    binding: PartitionedCandidateAuditBinding
    requests: tuple[PartitionedCandidateAuditRequest, ...]
    plan_digest: str
    source_challenge_count: int
    source_occurrence_count: int
    persisted_occurrence_count: int

    @property
    def request_count(self) -> int:
        """Return the number of deterministic partitions."""

        return len(self.requests)


@dataclass(frozen=True, slots=True)
class PartitionedCandidateAuditResult:
    """One strictly bound successful partition result."""

    plan_digest: str
    partition_index: int
    partition_count: int
    request_digest: str
    source_challenge_count: int
    source_occurrence_count: int
    persisted_occurrence_count: int
    item_count: int
    matched_source_occurrence_count: int
    validated_persisted_occurrence_count: int
    duration_ms: float
    block_io: Mapping[str, int]
    candidate_processing_io: Mapping[str, int]
    result_digest: str

    @property
    def unsigned_payload(self) -> dict[str, Any]:
        """Return every response field covered by its result digest."""

        return {
            "contract": PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT,
            "plan_digest": self.plan_digest,
            "partition_index": self.partition_index,
            "partition_count": self.partition_count,
            "request_digest": self.request_digest,
            "source_challenge_count": self.source_challenge_count,
            "source_occurrence_count": self.source_occurrence_count,
            "persisted_occurrence_count": self.persisted_occurrence_count,
            "item_count": self.item_count,
            "matched_source_occurrence_count": (
                self.matched_source_occurrence_count
            ),
            "validated_persisted_occurrence_count": (
                self.validated_persisted_occurrence_count
            ),
            "duration_ms": self.duration_ms,
            "block_io": dict(self.block_io),
            "candidate_processing_io": dict(self.candidate_processing_io),
        }

    @property
    def payload(self) -> dict[str, Any]:
        """Return the complete bound response payload."""

        return {**self.unsigned_payload, "result_digest": self.result_digest}


@dataclass(frozen=True, slots=True)
class PartitionedCandidateAuditAggregate:
    """Exact complete aggregate of every successful partition."""

    results: tuple[PartitionedCandidateAuditResult, ...]
    source_challenge_count: int
    source_occurrence_count: int
    persisted_occurrence_count: int
    request_count: int
    maximum_duration_ms: float
    block_io: Mapping[str, int]
    candidate_processing_io: Mapping[str, int]
    aggregate_digest: str


__all__ = [
    "BLOCK_IO_FIELDS",
    "CANDIDATE_IO_FIELDS",
    "PERSISTED_FIELDS",
    "PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_IN_FLIGHT",
    "PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_ITEMS",
    "PTG2_PARTITIONED_CANDIDATE_AUDIT_TARGET_ITEMS",
    "PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_NETWORK_DIGESTS",
    "PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT",
    "PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUESTS_PER_SECOND",
    "PTG2_PARTITIONED_CANDIDATE_AUDIT_RESULT_CONTRACT",
    "PartitionedCandidateAuditAggregate",
    "PartitionedCandidateAuditBinding",
    "PartitionedCandidateAuditPlan",
    "PartitionedCandidateAuditRequest",
    "PartitionedCandidateAuditResult",
    "PartitionedPersistedOccurrence",
    "PartitionedSourceChallenge",
    "REQUEST_FIELDS",
    "RESULT_FIELDS",
    "SOURCE_FIELDS",
    "bounded_text",
    "canonical_digest",
    "lower_hex",
    "nonnegative_integer",
    "valid_npi",
]
