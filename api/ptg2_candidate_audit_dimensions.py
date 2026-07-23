# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Choose bounded provider/code dimensions before candidate intersection."""

from __future__ import annotations

from typing import Iterable, Mapping, Sequence

from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)


def provider_code_request_upper_bound(
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    *,
    maximum_memberships: int,
) -> int:
    """Estimate pair fanout without materializing the provider/code product."""

    membership_upper_bound = 0
    for challenge in challenges:
        provider_count = len(provider_set_keys_by_npi.get(challenge.npi, ()))
        for _code_record in code_index.by_pair[
            (challenge.code_system, challenge.code)
        ]:
            membership_upper_bound += provider_count
            if membership_upper_bound > maximum_memberships:
                return membership_upper_bound
    for _occurrence in persisted_audit_occurrences:
        membership_upper_bound += 1
        if membership_upper_bound > maximum_memberships:
            return membership_upper_bound
    return membership_upper_bound


def candidate_provider_set_keys(
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
) -> Iterable[int]:
    """Yield provider keys for one bounded dimension-first intersection."""

    for provider_set_keys in provider_set_keys_by_npi.values():
        yield from provider_set_keys
    for occurrence in persisted_audit_occurrences:
        yield occurrence.provider_set_key


def candidate_requested_code_keys(
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
) -> Iterable[int]:
    """Yield requested codes without pairing them with every provider."""

    for challenge in challenges:
        for code_record in code_index.by_pair[
            (challenge.code_system, challenge.code)
        ]:
            yield int(code_record["code_key"])
    for occurrence in persisted_audit_occurrences:
        yield occurrence.code_key


__all__ = [
    "candidate_provider_set_keys",
    "candidate_requested_code_keys",
    "provider_code_request_upper_bound",
]
