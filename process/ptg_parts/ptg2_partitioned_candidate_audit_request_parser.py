# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict parser for one bounded partitioned candidate-audit request."""

from __future__ import annotations

from typing import Any, Mapping

from process.ptg_parts.ptg2_partitioned_candidate_audit_request_contract import (
    _validated_binding,
    _validated_persisted_occurrence,
    _validated_source_challenge,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_types import (
    PERSISTED_FIELDS,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_ITEMS,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT,
    REQUEST_FIELDS,
    SOURCE_FIELDS,
    PartitionedCandidateAuditBinding,
    PartitionedCandidateAuditRequest,
    PartitionedPersistedOccurrence,
    PartitionedSourceChallenge,
    canonical_digest,
    lower_hex,
    nonnegative_integer,
)


def _parse_source_challenge(raw_challenge: Any) -> PartitionedSourceChallenge:
    if not isinstance(raw_challenge, Mapping) or set(raw_challenge) != SOURCE_FIELDS:
        raise ValueError("partitioned_audit_source_fields_invalid")
    raw_digests = raw_challenge.get("network_name_digests")
    if not isinstance(raw_digests, list):
        raise ValueError("partitioned_audit_network_digests_invalid")
    return _validated_source_challenge(
        PartitionedSourceChallenge(
            ordinal=raw_challenge.get("ordinal"),
            code_system=raw_challenge.get("code_system"),
            code=raw_challenge.get("code"),
            npi=raw_challenge.get("npi"),
            source_artifact_key=raw_challenge.get("source_artifact_key"),
            tuple_digest=raw_challenge.get("tuple_digest"),
            network_name_digests=tuple(raw_digests),
            multiplicity=raw_challenge.get("multiplicity"),
        )
    )


def _parse_persisted_occurrence(
    raw_occurrence: Any,
) -> PartitionedPersistedOccurrence:
    if (
        not isinstance(raw_occurrence, Mapping)
        or set(raw_occurrence) != PERSISTED_FIELDS
    ):
        raise ValueError("partitioned_audit_persisted_fields_invalid")
    occurrence_hex = lower_hex(
        raw_occurrence.get("occurrence_id"),
        field_name="partitioned_audit_occurrence_id",
    )
    return _validated_persisted_occurrence(
        PartitionedPersistedOccurrence(
            ordinal=raw_occurrence.get("ordinal"),
            occurrence_id=bytes.fromhex(occurrence_hex),
            code_system=raw_occurrence.get("code_system"),
            code=raw_occurrence.get("code"),
            code_key=raw_occurrence.get("code_key"),
            provider_set_key=raw_occurrence.get("provider_set_key"),
            price_key=raw_occurrence.get("price_key"),
            source_artifact_key=raw_occurrence.get("source_artifact_key"),
            npi=raw_occurrence.get("npi"),
            atom_ordinal=raw_occurrence.get("atom_ordinal"),
            atom_key=raw_occurrence.get("atom_key"),
        )
    )


def _binding_from_request(
    raw_request: Mapping[str, Any],
) -> PartitionedCandidateAuditBinding:
    return _validated_binding(
        PartitionedCandidateAuditBinding(
            snapshot_id=raw_request.get("snapshot_id"),
            source_key=raw_request.get("source_key"),
            plan_id=raw_request.get("plan_id"),
            plan_market_type=raw_request.get("plan_market_type"),
            audit_sample_digest=raw_request.get("audit_sample_digest"),
            source_witness_sample_digest=raw_request.get(
                "source_witness_sample_digest"
            ),
            source_witness_payload_sha256=raw_request.get(
                "source_witness_payload_sha256"
            ),
            ordered_source_ordinal_digest=raw_request.get(
                "ordered_source_ordinal_digest"
            ),
            source_occurrence_count=raw_request.get(
                "plan_source_occurrence_count"
            ),
            persisted_occurrence_count=raw_request.get(
                "plan_persisted_occurrence_count"
            ),
        )
    )


def _request_partition_digest(
    audit_request: PartitionedCandidateAuditRequest,
) -> str:
    ordered_items = sorted(
        (
            *audit_request.source_challenges,
            *audit_request.persisted_occurrences,
        ),
        key=lambda audit_item: audit_item.ordinal,
    )
    return canonical_digest(
        {
            "plan_digest": audit_request.plan_digest,
            "partition_index": audit_request.partition_index,
            "partition_count": audit_request.partition_count,
            "items": [
                {
                    "kind": (
                        "source"
                        if isinstance(audit_item, PartitionedSourceChallenge)
                        else "persisted"
                    ),
                    **audit_item.payload,
                }
                for audit_item in ordered_items
            ],
        }
    )


def _validate_parsed_request(
    raw_request: Mapping[str, Any],
    audit_request: PartitionedCandidateAuditRequest,
) -> None:
    ordered_items = tuple(
        sorted(
            (
                *audit_request.source_challenges,
                *audit_request.persisted_occurrences,
            ),
            key=lambda audit_item: audit_item.ordinal,
        )
    )
    ordinals = tuple(audit_item.ordinal for audit_item in ordered_items)
    if (
        audit_request.partition_index >= audit_request.partition_count
        or raw_request.get("source_challenge_count")
        != len(audit_request.source_challenges)
        or raw_request.get("source_occurrence_count")
        != audit_request.source_occurrence_count
        or raw_request.get("persisted_occurrence_count")
        != len(audit_request.persisted_occurrences)
        or raw_request.get("item_count") != audit_request.item_count
        or audit_request.partition_digest
        != _request_partition_digest(audit_request)
        or audit_request.request_digest
        != canonical_digest(audit_request.unsigned_payload)
    ):
        raise ValueError("partitioned_audit_request_binding_invalid")
    if (
        len(ordinals) != len(set(ordinals))
        or ordinals != tuple(range(ordinals[0], ordinals[0] + len(ordinals)))
    ):
        raise ValueError("partitioned_audit_request_duplicate_ordinal")


def _request_from_fields(
    raw_request: Mapping[str, Any],
    source_challenges: tuple[PartitionedSourceChallenge, ...],
    persisted_occurrences: tuple[PartitionedPersistedOccurrence, ...],
) -> PartitionedCandidateAuditRequest:
    return PartitionedCandidateAuditRequest(
        binding=_binding_from_request(raw_request),
        plan_source_challenge_count=nonnegative_integer(
            raw_request.get("plan_source_challenge_count"),
            field_name="partitioned_audit_plan_source_challenge_count",
            minimum=1,
        ),
        plan_digest=lower_hex(
            raw_request.get("plan_digest"),
            field_name="partitioned_audit_plan_digest",
        ),
        partition_index=nonnegative_integer(
            raw_request.get("partition_index"),
            field_name="partitioned_audit_partition_index",
        ),
        partition_count=nonnegative_integer(
            raw_request.get("partition_count"),
            field_name="partitioned_audit_partition_count",
            minimum=1,
        ),
        source_challenges=source_challenges,
        persisted_occurrences=persisted_occurrences,
        partition_digest=lower_hex(
            raw_request.get("partition_digest"),
            field_name="partitioned_audit_partition_digest",
        ),
        request_digest=lower_hex(
            raw_request.get("request_digest"),
            field_name="partitioned_audit_request_digest",
        ),
    )


def parse_partitioned_candidate_audit_request(
    raw_request: Any,
) -> PartitionedCandidateAuditRequest:
    """Parse one strict request; full-plan exactness is checked by the client."""

    if not isinstance(raw_request, Mapping) or set(raw_request) != REQUEST_FIELDS:
        raise ValueError("partitioned_audit_request_fields_invalid")
    if (
        raw_request.get("contract")
        != PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT
    ):
        raise ValueError("partitioned_audit_request_contract_invalid")
    raw_challenges = raw_request.get("source_challenges")
    raw_occurrences = raw_request.get("persisted_occurrences")
    if not isinstance(raw_challenges, list) or not isinstance(
        raw_occurrences,
        list,
    ):
        raise ValueError("partitioned_audit_request_items_invalid")
    source_challenges = tuple(
        _parse_source_challenge(raw_challenge)
        for raw_challenge in raw_challenges
    )
    persisted_occurrences = tuple(
        _parse_persisted_occurrence(raw_occurrence)
        for raw_occurrence in raw_occurrences
    )
    item_count = len(source_challenges) + len(persisted_occurrences)
    if not 1 <= item_count <= PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_ITEMS:
        raise ValueError("partitioned_audit_request_item_count_invalid")
    audit_request = _request_from_fields(
        raw_request,
        source_challenges,
        persisted_occurrences,
    )
    _validate_parsed_request(raw_request, audit_request)
    return audit_request


__all__ = ["parse_partitioned_candidate_audit_request"]
