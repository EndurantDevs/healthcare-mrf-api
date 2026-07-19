# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve exact candidate coordinates before price hydration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)


OccurrenceKey = tuple[int, int, int]


@dataclass(frozen=True)
class CandidateForwardSelection:
    """Exact forward rows and cardinality proof from one broad in-memory index."""

    price_keys_by_occurrence: Mapping[OccurrenceKey, tuple[int, ...]]
    selection_io: Mapping[str, int]


def required_candidate_occurrence_keys(
    challenges: Sequence[AuditBatchChallenge],
    code_records_by_pair: Mapping[
        tuple[str, str], Sequence[Mapping[str, Any]]
    ],
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence] = (),
) -> frozenset[OccurrenceKey]:
    """Return exact code/provider/source triples without a Cartesian product."""

    requested_coordinates = {
        (
            challenge.code_system,
            challenge.code,
            challenge.npi,
            challenge.source_artifact_key,
        )
        for challenge in challenges
    }
    required_keys = {
        (
            int(code_record["code_key"]),
            int(provider_set_key),
            int(source_artifact_key),
        )
        for code_system, code, npi, source_artifact_key in requested_coordinates
        for code_record in code_records_by_pair[(code_system, code)]
        for provider_set_key in provider_set_keys_by_npi.get(npi, ())
    }
    required_keys.update(
        (
            occurrence.code_key,
            occurrence.provider_set_key,
            occurrence.source_artifact_key,
        )
        for occurrence in persisted_audit_occurrences
    )
    return frozenset(required_keys)


def select_candidate_forward_prices(
    broad_price_keys_by_occurrence: Mapping[
        OccurrenceKey, tuple[int, ...]
    ],
    required_occurrence_keys: frozenset[OccurrenceKey],
) -> CandidateForwardSelection:
    """Prune broad forward rows and report cardinalities without another read."""

    selected_price_keys_by_occurrence = {}
    broad_price_key_deliveries = 0
    selected_price_key_deliveries = 0
    for occurrence_key, price_keys in broad_price_keys_by_occurrence.items():
        price_key_count = len(price_keys)
        broad_price_key_deliveries += price_key_count
        if occurrence_key not in required_occurrence_keys:
            continue
        selected_price_keys_by_occurrence[occurrence_key] = price_keys
        selected_price_key_deliveries += price_key_count
    return CandidateForwardSelection(
        price_keys_by_occurrence=selected_price_keys_by_occurrence,
        selection_io={
            "exact_candidate_occurrence_coordinates": len(
                required_occurrence_keys
            ),
            "forward_occurrence_coordinates_before_exact_filter": len(
                broad_price_keys_by_occurrence
            ),
            "forward_occurrence_coordinates_after_exact_filter": len(
                selected_price_keys_by_occurrence
            ),
            "forward_price_key_deliveries_before_exact_filter": (
                broad_price_key_deliveries
            ),
            "forward_price_key_deliveries_after_exact_filter": (
                selected_price_key_deliveries
            ),
            "discarded_forward_price_key_deliveries": (
                broad_price_key_deliveries - selected_price_key_deliveries
            ),
        },
    )


__all__ = [
    "CandidateForwardSelection",
    "OccurrenceKey",
    "required_candidate_occurrence_keys",
    "select_candidate_forward_prices",
]
