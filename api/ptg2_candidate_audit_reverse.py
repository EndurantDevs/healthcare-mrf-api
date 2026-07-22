# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Code-first reverse provider proof for oversized candidate graph scopes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Sequence

from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_graph import (
    ChallengeGraphScopeTooLarge,
    prove_provider_candidates_by_npi,
)
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_db_sidecars import (
    lookup_forward_price_index_from_db,
    lookup_shared_graph_members_from_db,
)
from api.ptg2_serving import (
    PTG2_SCHEMA,
    _required_shared_snapshot_key,
    _required_source_count,
    _version_three_forward_lookup_hints,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)


@dataclass(frozen=True)
class ReverseCandidateScope:
    """Bounded provider proof plus the forward rows already read for it."""

    provider_set_keys_by_npi: dict[int, tuple[int, ...]]
    price_keys_by_occurrence: dict[tuple[int, int, int], tuple[int, ...]]


@dataclass(frozen=True)
class CandidateProviderScope:
    """Provider memberships and optional forward rows read during proof."""

    provider_set_keys_by_npi: dict[int, tuple[int, ...]]
    price_keys_by_occurrence: Mapping[
        tuple[int, int, int], tuple[int, ...]
    ] | None


def _source_keys_by_code_key(
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
) -> dict[int, tuple[int, ...]]:
    """Bind each requested code to only its sealed source coordinates."""

    source_keys_by_code: dict[int, set[int]] = {}
    for challenge in challenges:
        for code_record in code_index.by_pair[
            (challenge.code_system, challenge.code)
        ]:
            source_keys_by_code.setdefault(int(code_record["code_key"]), set()).add(
                challenge.source_artifact_key
            )
    for occurrence in persisted_audit_occurrences:
        source_keys_by_code.setdefault(occurrence.code_key, set()).add(
            occurrence.source_artifact_key
        )
    return {
        code_key: tuple(sorted(source_keys))
        for code_key, source_keys in source_keys_by_code.items()
    }


def _provider_candidates_by_npi(
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
    price_keys_by_occurrence: Mapping[tuple[int, int, int], tuple[int, ...]],
) -> dict[int, set[int]]:
    """Project the source-filtered forward rows into exact NPI candidates."""

    provider_keys_by_code_source: dict[tuple[int, int], set[int]] = {}
    for code_key, provider_set_key, source_key in price_keys_by_occurrence:
        provider_keys_by_code_source.setdefault((code_key, source_key), set()).add(
            provider_set_key
        )
    candidate_keys_by_npi: dict[int, set[int]] = {}
    for challenge in challenges:
        candidate_keys = candidate_keys_by_npi.setdefault(challenge.npi, set())
        for code_record in code_index.by_pair[
            (challenge.code_system, challenge.code)
        ]:
            candidate_keys.update(
                provider_keys_by_code_source.get(
                    (int(code_record["code_key"]), challenge.source_artifact_key),
                    (),
                )
            )
    for occurrence in persisted_audit_occurrences:
        candidate_keys_by_npi.setdefault(occurrence.npi, set()).add(
            occurrence.provider_set_key
        )
    return candidate_keys_by_npi


async def load_reverse_source_candidate_scope(
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
    group_keys_by_npi: Mapping[int, tuple[int, ...]],
) -> ReverseCandidateScope:
    """Replace an oversized group fan-out with code-first reverse proof."""

    source_keys_by_code = _source_keys_by_code_key(
        challenges,
        persisted_audit_occurrences,
        code_index,
    )
    price_keys_by_occurrence = await lookup_forward_price_index_from_db(
        session,
        source_keys_by_code,
        source_keys_by_code=source_keys_by_code,
        shared_snapshot_key=_required_shared_snapshot_key(serving_tables),
        source_count=_required_source_count(serving_tables),
        schema_name=PTG2_SCHEMA,
        **_version_three_forward_lookup_hints(serving_tables),
    )
    candidate_keys_by_npi = _provider_candidates_by_npi(
        challenges,
        persisted_audit_occurrences,
        code_index,
        price_keys_by_occurrence,
    )
    provider_set_keys_by_npi = await prove_provider_candidates_by_npi(
        lookup_shared_graph_members_from_db,
        session,
        _required_shared_snapshot_key(serving_tables),
        PTG2_SCHEMA,
        candidate_keys_by_npi,
        group_keys_by_npi,
    )
    return ReverseCandidateScope(
        provider_set_keys_by_npi=provider_set_keys_by_npi,
        price_keys_by_occurrence=price_keys_by_occurrence,
    )


async def load_candidate_provider_scope(
    broad_scope_lookup: Callable[..., Awaitable[dict[int, tuple[int, ...]]]],
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
) -> CandidateProviderScope:
    """Use broad graph expansion unless its read-once scope is too large."""

    try:
        provider_keys_by_npi = await broad_scope_lookup(
            session,
            serving_tables,
            challenges,
            persisted_audit_occurrences,
        )
    except ChallengeGraphScopeTooLarge as exc:
        reverse_scope = await load_reverse_source_candidate_scope(
            session,
            serving_tables,
            challenges,
            persisted_audit_occurrences,
            code_index,
            exc.group_keys_by_npi,
        )
        return CandidateProviderScope(
            provider_set_keys_by_npi=reverse_scope.provider_set_keys_by_npi,
            price_keys_by_occurrence=reverse_scope.price_keys_by_occurrence,
        )
    return CandidateProviderScope(
        provider_set_keys_by_npi=provider_keys_by_npi,
        price_keys_by_occurrence=None,
    )


__all__ = [
    "CandidateProviderScope",
    "ReverseCandidateScope",
    "load_candidate_provider_scope",
    "load_reverse_source_candidate_scope",
]
