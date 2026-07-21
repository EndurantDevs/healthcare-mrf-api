# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deterministic request planning and parsing for candidate-audit partitions."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Sequence

from process.ptg_parts.ptg2_partitioned_candidate_audit_types import (
    PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_ITEMS,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_TARGET_ITEMS,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_NETWORK_DIGESTS,
    PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT,
    PartitionedCandidateAuditBinding,
    PartitionedCandidateAuditPlan,
    PartitionedCandidateAuditRequest,
    PartitionedPersistedOccurrence,
    PartitionedSourceChallenge,
    bounded_text,
    canonical_digest,
    lower_hex,
    nonnegative_integer,
    valid_npi,
)


AuditPlanItem = PartitionedSourceChallenge | PartitionedPersistedOccurrence


def _validated_binding(
    binding: PartitionedCandidateAuditBinding,
) -> PartitionedCandidateAuditBinding:
    return PartitionedCandidateAuditBinding(
        snapshot_id=bounded_text(
            binding.snapshot_id,
            field_name="partitioned_audit_snapshot_id",
        ),
        source_key=bounded_text(
            binding.source_key,
            field_name="partitioned_audit_source_key",
        ).lower(),
        plan_id=bounded_text(
            binding.plan_id,
            field_name="partitioned_audit_plan_id",
        ),
        plan_market_type=bounded_text(
            binding.plan_market_type,
            field_name="partitioned_audit_plan_market_type",
        ).lower(),
        audit_sample_digest=lower_hex(
            binding.audit_sample_digest,
            field_name="partitioned_audit_sample_digest",
        ),
        source_witness_sample_digest=lower_hex(
            binding.source_witness_sample_digest,
            field_name="partitioned_audit_witness_sample_digest",
        ),
        source_witness_payload_sha256=lower_hex(
            binding.source_witness_payload_sha256,
            field_name="partitioned_audit_witness_payload_digest",
        ),
        ordered_source_ordinal_digest=lower_hex(
            binding.ordered_source_ordinal_digest,
            field_name="partitioned_audit_source_ordinal_digest",
        ),
        source_occurrence_count=nonnegative_integer(
            binding.source_occurrence_count,
            field_name="partitioned_audit_source_occurrence_count",
            minimum=1,
        ),
        persisted_occurrence_count=nonnegative_integer(
            binding.persisted_occurrence_count,
            field_name="partitioned_audit_persisted_occurrence_count",
            minimum=1,
        ),
    )


def _validated_source_challenge(
    challenge: PartitionedSourceChallenge,
) -> PartitionedSourceChallenge:
    network_digests = tuple(
        lower_hex(
            digest,
            field_name="partitioned_audit_network_digest",
        )
        for digest in challenge.network_name_digests
    )
    if (
        len(network_digests)
        > PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_NETWORK_DIGESTS
        or network_digests != tuple(sorted(set(network_digests)))
    ):
        raise ValueError("partitioned_audit_network_digests_invalid")
    return PartitionedSourceChallenge(
        ordinal=nonnegative_integer(
            challenge.ordinal,
            field_name="partitioned_audit_ordinal",
        ),
        code_system=bounded_text(
            challenge.code_system,
            field_name="partitioned_audit_code_system",
            maximum=64,
        ),
        code=bounded_text(
            challenge.code,
            field_name="partitioned_audit_code",
            maximum=256,
        ),
        npi=valid_npi(challenge.npi),
        source_artifact_key=nonnegative_integer(
            challenge.source_artifact_key,
            field_name="partitioned_audit_source_artifact_key",
        ),
        tuple_digest=lower_hex(
            challenge.tuple_digest,
            field_name="partitioned_audit_tuple_digest",
        ),
        network_name_digests=network_digests,
        multiplicity=nonnegative_integer(
            challenge.multiplicity,
            field_name="partitioned_audit_multiplicity",
            minimum=1,
        ),
    )


def _validated_persisted_occurrence(
    occurrence: PartitionedPersistedOccurrence,
) -> PartitionedPersistedOccurrence:
    if not isinstance(
        occurrence.occurrence_id,
        (bytes, bytearray, memoryview),
    ):
        raise ValueError("partitioned_audit_occurrence_id_invalid")
    occurrence_id = bytes(occurrence.occurrence_id)
    if len(occurrence_id) != 32:
        raise ValueError("partitioned_audit_occurrence_id_invalid")
    return PartitionedPersistedOccurrence(
        ordinal=nonnegative_integer(
            occurrence.ordinal,
            field_name="partitioned_audit_ordinal",
        ),
        occurrence_id=occurrence_id,
        code_system=bounded_text(
            occurrence.code_system,
            field_name="partitioned_audit_code_system",
            maximum=64,
        ),
        code=bounded_text(
            occurrence.code,
            field_name="partitioned_audit_code",
            maximum=256,
        ),
        code_key=nonnegative_integer(
            occurrence.code_key,
            field_name="partitioned_audit_code_key",
        ),
        provider_set_key=nonnegative_integer(
            occurrence.provider_set_key,
            field_name="partitioned_audit_provider_set_key",
        ),
        price_key=nonnegative_integer(
            occurrence.price_key,
            field_name="partitioned_audit_price_key",
        ),
        source_artifact_key=nonnegative_integer(
            occurrence.source_artifact_key,
            field_name="partitioned_audit_source_artifact_key",
        ),
        npi=valid_npi(occurrence.npi),
        atom_ordinal=nonnegative_integer(
            occurrence.atom_ordinal,
            field_name="partitioned_audit_atom_ordinal",
        ),
        atom_key=nonnegative_integer(
            occurrence.atom_key,
            field_name="partitioned_audit_atom_key",
        ),
    )


def _item_sort_key(audit_item: AuditPlanItem) -> tuple[Any, ...]:
    if isinstance(audit_item, PartitionedSourceChallenge):
        return (
            audit_item.code_system,
            audit_item.code,
            0,
            audit_item.npi,
            audit_item.source_artifact_key,
            audit_item.tuple_digest,
            audit_item.network_name_digests,
        )
    return (
        audit_item.code_system,
        audit_item.code,
        1,
        audit_item.code_key,
        audit_item.provider_set_key,
        audit_item.source_artifact_key,
        audit_item.occurrence_id,
    )


def _partition_items(
    audit_items: Sequence[AuditPlanItem],
) -> tuple[tuple[AuditPlanItem, ...], ...]:
    item_groups: list[list[AuditPlanItem]] = []
    current_group_key: tuple[str, str] | None = None
    for audit_item in audit_items:
        group_key = (audit_item.code_system, audit_item.code)
        if group_key != current_group_key:
            item_groups.append([])
            current_group_key = group_key
        item_groups[-1].append(audit_item)
    partitions: list[tuple[AuditPlanItem, ...]] = []
    current_items: list[AuditPlanItem] = []
    for item_group in item_groups:
        remaining_items = list(item_group)
        available_count = (
            PTG2_PARTITIONED_CANDIDATE_AUDIT_TARGET_ITEMS
            - len(current_items)
        )
        if current_items and len(remaining_items) <= available_count:
            current_items.extend(remaining_items)
            continue
        if current_items:
            partitions.append(tuple(current_items))
            current_items = []
        while (
            len(remaining_items)
            >= PTG2_PARTITIONED_CANDIDATE_AUDIT_TARGET_ITEMS
        ):
            partitions.append(
                tuple(
                    remaining_items[
                        :PTG2_PARTITIONED_CANDIDATE_AUDIT_TARGET_ITEMS
                    ]
                )
            )
            del remaining_items[
                :PTG2_PARTITIONED_CANDIDATE_AUDIT_TARGET_ITEMS
            ]
        current_items = remaining_items
    if current_items:
        partitions.append(tuple(current_items))
    return tuple(partitions)


def _plan_payload(
    binding: PartitionedCandidateAuditBinding,
    source_count: int,
    audit_items: Sequence[AuditPlanItem],
) -> dict[str, Any]:
    return {
        "contract": PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT,
        **binding.payload,
        "plan_source_challenge_count": source_count,
        "items": [
            {
                "kind": (
                    "source"
                    if isinstance(audit_item, PartitionedSourceChallenge)
                    else "persisted"
                ),
                **audit_item.payload,
            }
            for audit_item in audit_items
        ],
    }


def _validated_plan_populations(
    source_challenges: Sequence[PartitionedSourceChallenge],
    persisted_occurrences: Sequence[PartitionedPersistedOccurrence],
) -> tuple[
    tuple[PartitionedSourceChallenge, ...],
    tuple[PartitionedPersistedOccurrence, ...],
]:
    validated_challenges = tuple(
        _validated_source_challenge(challenge)
        for challenge in source_challenges
    )
    validated_occurrences = tuple(
        _validated_persisted_occurrence(occurrence)
        for occurrence in persisted_occurrences
    )
    if not validated_challenges or not validated_occurrences:
        raise ValueError("partitioned_audit_plan_population_empty")
    source_identities = {
        (
            challenge.code_system,
            challenge.code,
            challenge.npi,
            challenge.source_artifact_key,
            challenge.tuple_digest,
            challenge.network_name_digests,
        )
        for challenge in validated_challenges
    }
    persisted_ids = {
        occurrence.occurrence_id for occurrence in validated_occurrences
    }
    if (
        len(source_identities) != len(validated_challenges)
        or len(persisted_ids) != len(validated_occurrences)
    ):
        raise ValueError("partitioned_audit_plan_duplicate_item")
    return validated_challenges, validated_occurrences


def _ordered_plan_items(
    source_challenges: Sequence[PartitionedSourceChallenge],
    persisted_occurrences: Sequence[PartitionedPersistedOccurrence],
) -> tuple[AuditPlanItem, ...]:
    sorted_items = sorted(
        (*source_challenges, *persisted_occurrences),
        key=_item_sort_key,
    )
    return tuple(
        replace(audit_item, ordinal=ordinal)
        for ordinal, audit_item in enumerate(sorted_items)
    )


def _validated_plan_counts(
    binding: PartitionedCandidateAuditBinding,
    audit_items: Sequence[AuditPlanItem],
) -> tuple[int, int, int]:
    source_challenge_count = sum(
        isinstance(audit_item, PartitionedSourceChallenge)
        for audit_item in audit_items
    )
    source_occurrence_count = sum(
        audit_item.multiplicity
        for audit_item in audit_items
        if isinstance(audit_item, PartitionedSourceChallenge)
    )
    persisted_occurrence_count = sum(
        isinstance(audit_item, PartitionedPersistedOccurrence)
        for audit_item in audit_items
    )
    if (
        source_occurrence_count != binding.source_occurrence_count
        or persisted_occurrence_count != binding.persisted_occurrence_count
    ):
        raise ValueError("partitioned_audit_plan_sealed_count_mismatch")
    return (
        source_challenge_count,
        source_occurrence_count,
        persisted_occurrence_count,
    )


def _partition_request(
    *,
    binding: PartitionedCandidateAuditBinding,
    source_challenge_count: int,
    plan_digest: str,
    partition_index: int,
    partition_count: int,
    partition_items: Sequence[AuditPlanItem],
) -> PartitionedCandidateAuditRequest:
    source_challenges = tuple(
        audit_item
        for audit_item in partition_items
        if isinstance(audit_item, PartitionedSourceChallenge)
    )
    persisted_occurrences = tuple(
        audit_item
        for audit_item in partition_items
        if isinstance(audit_item, PartitionedPersistedOccurrence)
    )
    partition_digest = canonical_digest(
        {
            "plan_digest": plan_digest,
            "partition_index": partition_index,
            "partition_count": partition_count,
            "items": [
                {
                    "kind": (
                        "source"
                        if isinstance(audit_item, PartitionedSourceChallenge)
                        else "persisted"
                    ),
                    **audit_item.payload,
                }
                for audit_item in partition_items
            ],
        }
    )
    provisional_request = PartitionedCandidateAuditRequest(
        binding=binding,
        plan_source_challenge_count=source_challenge_count,
        plan_digest=plan_digest,
        partition_index=partition_index,
        partition_count=partition_count,
        source_challenges=source_challenges,
        persisted_occurrences=persisted_occurrences,
        partition_digest=partition_digest,
        request_digest="0" * 64,
    )
    return replace(
        provisional_request,
        request_digest=canonical_digest(provisional_request.unsigned_payload),
    )


def build_partitioned_candidate_audit_plan(
    *,
    binding: PartitionedCandidateAuditBinding,
    source_challenges: Sequence[PartitionedSourceChallenge],
    persisted_occurrences: Sequence[PartitionedPersistedOccurrence],
) -> PartitionedCandidateAuditPlan:
    """Build a code-aware, max-50, exact-once request plan."""

    validated_binding = _validated_binding(binding)
    validated_challenges, validated_occurrences = (
        _validated_plan_populations(
            source_challenges,
            persisted_occurrences,
        )
    )
    audit_items = _ordered_plan_items(
        validated_challenges,
        validated_occurrences,
    )
    source_count, source_occurrence_count, persisted_count = (
        _validated_plan_counts(validated_binding, audit_items)
    )
    plan_digest = canonical_digest(
        _plan_payload(validated_binding, source_count, audit_items)
    )
    partitions = _partition_items(audit_items)
    requests = tuple(
        _partition_request(
            binding=validated_binding,
            source_challenge_count=source_count,
            plan_digest=plan_digest,
            partition_index=partition_index,
            partition_count=len(partitions),
            partition_items=partition,
        )
        for partition_index, partition in enumerate(partitions)
    )
    return PartitionedCandidateAuditPlan(
        binding=validated_binding,
        requests=requests,
        plan_digest=plan_digest,
        source_challenge_count=source_count,
        source_occurrence_count=source_occurrence_count,
        persisted_occurrence_count=persisted_count,
    )


__all__ = ["build_partitioned_candidate_audit_plan"]
