# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Normalize graph NPI scopes under one candidate retention budget."""

from __future__ import annotations

from itertools import chain
from typing import Sequence

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    retain_unique_integer_keys,
)
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)


def candidate_graph_npis(
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """Return challenge-only and union NPI keys with conservative claims."""

    challenge_npis, _challenge_bytes = retain_unique_integer_keys(
        (challenge.npi for challenge in challenges),
        retention_budget,
        category="challenge NPI",
    )
    candidate_npis, _candidate_bytes = retain_unique_integer_keys(
        chain(
            (challenge.npi for challenge in challenges),
            (occurrence.npi for occurrence in persisted_audit_occurrences),
        ),
        retention_budget,
        category="candidate NPI",
    )
    return challenge_npis, candidate_npis


__all__ = ["candidate_graph_npis"]
