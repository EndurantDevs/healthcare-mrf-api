# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound exact candidate occurrence coordinates before forward reads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from api.ptg2_candidate_audit_capacity import CandidateAuditDecodedRetentionBudget
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)


OccurrenceKey = tuple[int, int, int]
_OCCURRENCE_SET_BYTES = 256
_REQUESTED_COORDINATE_BYTES = 512
_REQUIRED_OCCURRENCE_BYTES = 320
_REQUIRED_RESULT_BYTES = 216
_REQUIRED_RESULT_MEMBERSHIP_BYTES = 112


@dataclass
class _RetentionClaims:
    budget: CandidateAuditDecodedRetentionBudget | None
    temporary_bytes: int = 0
    result_bytes: int = 0

    def retain_temporary(self, byte_count: int, *, category: str) -> None:
        """Claim bytes that can be released after freezing the result."""

        if self.budget is None:
            return
        self.budget.claim(byte_count, category=category)
        self.temporary_bytes += byte_count

    def retain_result(self, byte_count: int, *, category: str) -> None:
        """Claim bytes retained by the returned immutable result."""

        if self.budget is None:
            return
        self.budget.claim(byte_count, category=category)
        self.result_bytes += byte_count

    def release_temporaries(self) -> None:
        """Release all temporary claims after successful conversion."""

        if self.budget is not None:
            self.budget.release(self.temporary_bytes)
        self.temporary_bytes = 0

    def release_all(self) -> None:
        """Release temporary and result claims after any failed conversion."""

        if self.budget is not None:
            self.budget.release(self.temporary_bytes + self.result_bytes)
        self.temporary_bytes = 0
        self.result_bytes = 0


def _requested_coordinates(
    challenges: Sequence[AuditBatchChallenge],
    claims: _RetentionClaims,
) -> set[tuple[str, str, int, int]]:
    coordinates: set[tuple[str, str, int, int]] = set()
    for challenge in challenges:
        coordinate = (
            challenge.code_system,
            challenge.code,
            challenge.npi,
            challenge.source_artifact_key,
        )
        if coordinate in coordinates:
            continue
        claims.retain_temporary(
            _REQUESTED_COORDINATE_BYTES,
            category="a requested candidate occurrence coordinate",
        )
        coordinates.add(coordinate)
    return coordinates


def _retain_occurrence_key(
    required_keys: set[OccurrenceKey],
    occurrence_key: OccurrenceKey,
    claims: _RetentionClaims,
    *,
    category: str,
) -> None:
    if occurrence_key in required_keys:
        return
    claims.retain_temporary(_REQUIRED_OCCURRENCE_BYTES, category=category)
    required_keys.add(occurrence_key)


def _graph_occurrence_keys(
    requested_coordinates: set[tuple[str, str, int, int]],
    code_records_by_pair: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
    provider_sets_by_npi_code: Mapping[tuple[int, int], tuple[int, ...]],
    claims: _RetentionClaims,
) -> set[OccurrenceKey]:
    required_keys: set[OccurrenceKey] = set()
    for code_system, code, npi, source_key in requested_coordinates:
        for code_record in code_records_by_pair[(code_system, code)]:
            code_key = int(code_record["code_key"])
            for provider_set_key in provider_sets_by_npi_code.get(
                (npi, code_key),
                (),
            ):
                _retain_occurrence_key(
                    required_keys,
                    (code_key, int(provider_set_key), int(source_key)),
                    claims,
                    category="a required candidate occurrence",
                )
    return required_keys


def _add_persisted_occurrences(
    required_keys: set[OccurrenceKey],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    claims: _RetentionClaims,
) -> None:
    for occurrence in persisted_audit_occurrences:
        _retain_occurrence_key(
            required_keys,
            (
                occurrence.code_key,
                occurrence.provider_set_key,
                occurrence.source_artifact_key,
            ),
            claims,
            category="a required persisted occurrence",
        )


def required_candidate_occurrence_keys(
    challenges: Sequence[AuditBatchChallenge],
    code_records_by_pair: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
    provider_sets_by_npi_code: Mapping[tuple[int, int], tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence] = (),
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> frozenset[OccurrenceKey]:
    """Return exact code/provider/source triples without a Cartesian product."""

    claims = _RetentionClaims(retention_budget)
    try:
        claims.retain_temporary(
            2 * _OCCURRENCE_SET_BYTES,
            category="the requested and required occurrence sets",
        )
        requested_coordinates = _requested_coordinates(challenges, claims)
        required_keys = _graph_occurrence_keys(
            requested_coordinates,
            code_records_by_pair,
            provider_sets_by_npi_code,
            claims,
        )
        _add_persisted_occurrences(
            required_keys,
            persisted_audit_occurrences,
            claims,
        )
        claims.retain_result(
            _REQUIRED_RESULT_BYTES
            + len(required_keys) * _REQUIRED_RESULT_MEMBERSHIP_BYTES,
            category="the frozen required occurrence set",
        )
        frozen_required_keys = frozenset(required_keys)
    except BaseException:
        claims.release_all()
        raise
    claims.release_temporaries()
    return frozen_required_keys


__all__ = ["OccurrenceKey", "required_candidate_occurrence_keys"]
