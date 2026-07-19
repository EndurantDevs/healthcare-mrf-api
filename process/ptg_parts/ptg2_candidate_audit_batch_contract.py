# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pseudonymized one-pass request contract for exhaustive PTG candidate audits."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from process.ptg_parts.ptg2_candidate_audit_evidence import (
    SourceAuditCondition,
    canonical_network_name_digests,
    canonical_tuple_digest_without_networks,
)
from process.ptg_parts.ptg2_shared_source_set import (
    ordered_source_ordinal_digest,
)


PTG2_AUDIT_BATCH_REQUEST_CONTRACT = "ptg2_v3_source_witness_batch_request_v1"
PTG2_AUDIT_BATCH_RESPONSE_CONTRACT = "ptg2_v3_source_witness_batch_response_v1"
PTG2_AUDIT_BATCH_MAX_CHALLENGES = 10_000
_REQUEST_FIELDS = frozenset(
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
        "challenge_count",
        "request_digest",
    }
)


@dataclass(frozen=True, order=True)
class AuditBatchChallenge:
    """One grouped matching condition without source text or raw identities."""

    code_system: str
    code: str
    npi: int
    source_artifact_key: int
    tuple_digest: str
    network_name_digests: tuple[str, ...]
    multiplicity: int

    @property
    def condition_key(self) -> tuple[str, str, int, int, str, tuple[str, ...]]:
        """Return the matching identity independent of grouped multiplicity."""

        return (
            self.code_system,
            self.code,
            self.npi,
            self.source_artifact_key,
            self.tuple_digest,
            self.network_name_digests,
        )

    @property
    def payload(self) -> dict[str, Any]:
        """Return the canonical JSON-compatible challenge fields."""

        return {
            "code_system": self.code_system,
            "code": self.code,
            "npi": self.npi,
            "source_artifact_key": self.source_artifact_key,
            "tuple_digest": self.tuple_digest,
            "network_name_digests": list(self.network_name_digests),
            "multiplicity": self.multiplicity,
        }


@dataclass(frozen=True)
class AuditBatchRequest:
    """Canonical exhaustive batch bound to one validated candidate."""

    snapshot_id: str
    source_key: str
    plan_id: str
    plan_market_type: str
    audit_sample_digest: str
    source_witness_sample_digest: str
    source_witness_payload_sha256: str
    ordered_source_ordinal_digest: str
    challenge_count: int
    request_digest: str

    @property
    def unsigned_payload(self) -> dict[str, Any]:
        """Return the fields covered by the request digest."""

        return {
            "contract": PTG2_AUDIT_BATCH_REQUEST_CONTRACT,
            "snapshot_id": self.snapshot_id,
            "source_key": self.source_key,
            "plan_id": self.plan_id,
            "plan_market_type": self.plan_market_type,
            "audit_sample_digest": self.audit_sample_digest,
            "source_witness_sample_digest": self.source_witness_sample_digest,
            "source_witness_payload_sha256": self.source_witness_payload_sha256,
            "ordered_source_ordinal_digest": self.ordered_source_ordinal_digest,
            "challenge_count": self.challenge_count,
        }

    @property
    def payload(self) -> dict[str, Any]:
        """Return the complete authenticated request body."""

        return {**self.unsigned_payload, "request_digest": self.request_digest}


@dataclass(frozen=True)
class AuditBatchWitnessBinding:
    """Immutable sealed-witness fields used to build one batch request."""

    audit_sample_digest: str
    source_witness_sample_digest: str
    source_witness_payload_sha256: str
    raw_container_sha256: tuple[str, ...]
    source_witness_occurrence_count: int


def _lower_hex_digest(raw_digest: Any, *, field_name: str) -> str:
    normalized_digest = str(raw_digest or "")
    if len(normalized_digest) != 64 or any(
        character not in "0123456789abcdef" for character in normalized_digest
    ):
        raise ValueError(f"{field_name}_invalid")
    return normalized_digest


def _strict_positive_int(raw_number: Any, *, field_name: str) -> int:
    if type(raw_number) is not int or raw_number < 1:
        raise ValueError(f"{field_name}_invalid")
    return raw_number


def _request_digest(unsigned_payload: Mapping[str, Any]) -> str:
    canonical_json = json.dumps(
        dict(unsigned_payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def matched_audit_batch_digest(request_digest: str, matched_count: int) -> str:
    """Bind a successful aggregate response to its exact request and count."""

    normalized_request_digest = _lower_hex_digest(
        request_digest,
        field_name="audit_batch_request_digest",
    )
    normalized_count = _strict_positive_int(
        matched_count,
        field_name="audit_batch_matched_count",
    )
    return hashlib.sha256(
        f"{normalized_request_digest}:{normalized_count}".encode("ascii")
    ).hexdigest()


def _normalized_coordinates(
    *,
    snapshot_id: Any,
    source_key: Any,
    plan_id: Any,
    plan_market_type: Any,
) -> tuple[str, str, str, str]:
    coordinates = (
        str(snapshot_id or "").strip(),
        str(source_key or "").strip().lower(),
        str(plan_id or "").strip(),
        str(plan_market_type or "").strip().lower(),
    )
    if any(not coordinate for coordinate in coordinates):
        raise ValueError("audit_batch_candidate_coordinates_invalid")
    return coordinates


def _validated_challenge_count(raw_request: Mapping[str, Any]) -> int:
    """Read one bounded count whose exact conditions are derived server-side."""

    challenge_count = _strict_positive_int(
        raw_request.get("challenge_count"),
        field_name="audit_batch_challenge_count",
    )
    if challenge_count > PTG2_AUDIT_BATCH_MAX_CHALLENGES:
        raise ValueError("audit_batch_challenge_counts_invalid")
    return challenge_count


def parse_audit_batch_request(raw_request: Any) -> AuditBatchRequest:
    """Validate strict fields, grouping, ordering, counts, and request digest."""

    if not isinstance(raw_request, Mapping) or set(raw_request) != _REQUEST_FIELDS:
        raise ValueError("audit_batch_request_fields_invalid")
    if raw_request.get("contract") != PTG2_AUDIT_BATCH_REQUEST_CONTRACT:
        raise ValueError("audit_batch_request_contract_invalid")
    coordinates = _normalized_coordinates(
        snapshot_id=raw_request.get("snapshot_id"),
        source_key=raw_request.get("source_key"),
        plan_id=raw_request.get("plan_id"),
        plan_market_type=raw_request.get("plan_market_type"),
    )
    challenge_count = _validated_challenge_count(raw_request)
    audit_sample_digest = _lower_hex_digest(
        raw_request.get("audit_sample_digest"),
        field_name="audit_batch_sample_digest",
    )
    source_witness_sample_digest = _lower_hex_digest(
        raw_request.get("source_witness_sample_digest"),
        field_name="audit_batch_source_witness_sample_digest",
    )
    source_witness_payload_sha256 = _lower_hex_digest(
        raw_request.get("source_witness_payload_sha256"),
        field_name="audit_batch_source_witness_payload_sha256",
    )
    source_ordinal_digest = _lower_hex_digest(
        raw_request.get("ordered_source_ordinal_digest"),
        field_name="audit_batch_source_ordinal_digest",
    )
    request_digest = _lower_hex_digest(
        raw_request.get("request_digest"),
        field_name="audit_batch_request_digest",
    )
    parsed_request = AuditBatchRequest(
        snapshot_id=coordinates[0],
        source_key=coordinates[1],
        plan_id=coordinates[2],
        plan_market_type=coordinates[3],
        audit_sample_digest=audit_sample_digest,
        source_witness_sample_digest=source_witness_sample_digest,
        source_witness_payload_sha256=source_witness_payload_sha256,
        ordered_source_ordinal_digest=source_ordinal_digest,
        challenge_count=challenge_count,
        request_digest=request_digest,
    )
    if request_digest != _request_digest(parsed_request.unsigned_payload):
        raise ValueError("audit_batch_request_digest_mismatch")
    return parsed_request


def group_audit_batch_challenges(
    raw_container_sha256: Sequence[str],
    challenges: Sequence[SourceAuditCondition],
) -> tuple[AuditBatchChallenge, ...]:
    """Group conditions derived from one already validated sealed witness."""

    source_key_by_digest = {
        _lower_hex_digest(raw_digest, field_name="audit_batch_raw_source_digest"): source_key
        for source_key, raw_digest in enumerate(raw_container_sha256)
    }
    if not source_key_by_digest or len(source_key_by_digest) != len(raw_container_sha256):
        raise ValueError("audit_batch_raw_source_scope_invalid")
    grouped_conditions: Counter[
        tuple[str, str, int, int, str, tuple[str, ...]]
    ] = Counter()
    for challenge in challenges:
        source_artifact_key = source_key_by_digest.get(challenge.raw_source_sha256)
        if source_artifact_key is None:
            raise ValueError("audit_batch_challenge_source_unknown")
        grouped_conditions[
            (
                challenge.query.code_system,
                challenge.query.code,
                challenge.query.npi,
                source_artifact_key,
                canonical_tuple_digest_without_networks(challenge.expected_tuple),
                tuple(sorted(canonical_network_name_digests(challenge.required_network_names))),
            )
        ] += 1
    return tuple(
        sorted(
            AuditBatchChallenge(*condition, multiplicity=multiplicity)
            for condition, multiplicity in grouped_conditions.items()
        )
    )


def _normalized_witness_fields(
    witness_binding: AuditBatchWitnessBinding,
) -> tuple[int, str, str, str, str]:
    challenge_count = _strict_positive_int(
        witness_binding.source_witness_occurrence_count,
        field_name="audit_batch_challenge_count",
    )
    if challenge_count > PTG2_AUDIT_BATCH_MAX_CHALLENGES:
        raise ValueError("audit_batch_challenge_counts_invalid")
    return (
        challenge_count,
        _lower_hex_digest(
            witness_binding.audit_sample_digest,
            field_name="audit_batch_sample_digest",
        ),
        _lower_hex_digest(
            witness_binding.source_witness_sample_digest,
            field_name="audit_batch_source_witness_sample_digest",
        ),
        _lower_hex_digest(
            witness_binding.source_witness_payload_sha256,
            field_name="audit_batch_source_witness_payload_sha256",
        ),
        ordered_source_ordinal_digest(witness_binding.raw_container_sha256),
    )


def build_audit_batch_request(
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
    plan_market_type: str,
    witness_binding: AuditBatchWitnessBinding,
) -> AuditBatchRequest:
    """Bind a small request to the server-derived sealed witness conditions."""

    coordinates = _normalized_coordinates(
        snapshot_id=snapshot_id,
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=plan_market_type,
    )
    (
        challenge_count,
        normalized_sample_digest,
        normalized_witness_sample_digest,
        normalized_witness_payload_sha256,
        source_ordinal_digest,
    ) = _normalized_witness_fields(witness_binding)
    unsigned_fields_by_name = {
        "contract": PTG2_AUDIT_BATCH_REQUEST_CONTRACT,
        "snapshot_id": coordinates[0],
        "source_key": coordinates[1],
        "plan_id": coordinates[2],
        "plan_market_type": coordinates[3],
        "audit_sample_digest": normalized_sample_digest,
        "source_witness_sample_digest": normalized_witness_sample_digest,
        "source_witness_payload_sha256": normalized_witness_payload_sha256,
        "ordered_source_ordinal_digest": source_ordinal_digest,
        "challenge_count": challenge_count,
    }
    return AuditBatchRequest(
        snapshot_id=coordinates[0],
        source_key=coordinates[1],
        plan_id=coordinates[2],
        plan_market_type=coordinates[3],
        audit_sample_digest=normalized_sample_digest,
        source_witness_sample_digest=normalized_witness_sample_digest,
        source_witness_payload_sha256=normalized_witness_payload_sha256,
        ordered_source_ordinal_digest=source_ordinal_digest,
        challenge_count=challenge_count,
        request_digest=_request_digest(unsigned_fields_by_name),
    )


__all__ = [
    "AuditBatchChallenge",
    "AuditBatchRequest",
    "AuditBatchWitnessBinding",
    "PTG2_AUDIT_BATCH_MAX_CHALLENGES",
    "PTG2_AUDIT_BATCH_REQUEST_CONTRACT",
    "PTG2_AUDIT_BATCH_RESPONSE_CONTRACT",
    "build_audit_batch_request",
    "group_audit_batch_challenges",
    "matched_audit_batch_digest",
    "parse_audit_batch_request",
]
