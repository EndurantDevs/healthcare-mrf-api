# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve exact candidate coordinates before price hydration."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)


OccurrenceKey = tuple[int, int, int]


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


__all__ = [
    "OccurrenceKey",
    "required_candidate_occurrence_keys",
]
