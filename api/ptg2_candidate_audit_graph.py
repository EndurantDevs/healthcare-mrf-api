# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded provider-graph resolution for candidate audit coordinates."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence

from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_shared_blocks import (
    PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
    PTG2_V3_GRAPH_NPI_TO_GROUP,
    PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)


GraphLookup = Callable[..., Awaitable[dict[int, tuple[int, ...]]]]


def _challenge_group_keys(
    challenge_npis: Iterable[int],
    group_keys_by_npi: Mapping[int, tuple[int, ...]],
) -> tuple[int, ...]:
    """Return only groups that need broad witness expansion."""

    return tuple(
        sorted(
            {
                group_key
                for npi in challenge_npis
                for group_key in group_keys_by_npi.get(npi, ())
            }
        )
    )


def _provider_keys_for_challenges(
    npis: Iterable[int],
    group_keys_by_npi: Mapping[int, tuple[int, ...]],
    provider_keys_by_group: Mapping[int, tuple[int, ...]],
) -> dict[int, set[int]]:
    """Build the broad provider scope required by witness challenges."""

    return {
        npi: {
            provider_set_key
            for group_key in group_keys_by_npi.get(npi, ())
            for provider_set_key in provider_keys_by_group.get(group_key, ())
        }
        for npi in npis
    }


def _unresolved_persisted_memberships(
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    provider_keys_by_npi: Mapping[int, set[int]],
) -> tuple[tuple[int, int], ...]:
    """Return exact persisted coordinates absent from the broad scope."""

    return tuple(
        sorted(
            {
                (occurrence.npi, occurrence.provider_set_key)
                for occurrence in persisted_audit_occurrences
                if occurrence.provider_set_key
                not in provider_keys_by_npi[occurrence.npi]
            }
        )
    )


async def _add_reverse_persisted_memberships(
    lookup_graph_members: GraphLookup,
    session: Any,
    shared_snapshot_key: int,
    schema_name: str,
    unresolved_memberships: tuple[tuple[int, int], ...],
    group_keys_by_npi: Mapping[int, tuple[int, ...]],
    provider_keys_by_npi: dict[int, set[int]],
) -> None:
    """Prove exact persisted memberships without broad group expansion."""

    if not unresolved_memberships:
        return
    groups_by_provider_key = await lookup_graph_members(
        session,
        shared_snapshot_key,
        PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
        (provider_set_key for _npi, provider_set_key in unresolved_memberships),
        schema_name=schema_name,
    )
    unresolved_npis = {npi for npi, _provider_set_key in unresolved_memberships}
    group_key_sets_by_npi = {
        npi: frozenset(group_keys_by_npi.get(npi, ()))
        for npi in unresolved_npis
    }
    for npi, provider_set_key in unresolved_memberships:
        if group_key_sets_by_npi[npi].intersection(
            groups_by_provider_key.get(provider_set_key, ())
        ):
            provider_keys_by_npi[npi].add(provider_set_key)


async def provider_set_keys_by_npi(
    lookup_graph_members: GraphLookup,
    session: Any,
    shared_snapshot_key: int,
    schema_name: str,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
) -> dict[int, tuple[int, ...]]:
    """Resolve broad witness scopes and exact persisted memberships."""

    challenge_npis = {challenge.npi for challenge in challenges}
    npis = tuple(
        sorted(
            challenge_npis
            | {occurrence.npi for occurrence in persisted_audit_occurrences}
        )
    )
    group_keys_by_npi = await lookup_graph_members(
        session,
        shared_snapshot_key,
        PTG2_V3_GRAPH_NPI_TO_GROUP,
        npis,
        schema_name=schema_name,
    )
    group_keys = _challenge_group_keys(challenge_npis, group_keys_by_npi)
    provider_keys_by_group = (
        await lookup_graph_members(
            session,
            shared_snapshot_key,
            PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
            group_keys,
            schema_name=schema_name,
        )
        if group_keys
        else {}
    )
    provider_keys_by_npi = _provider_keys_for_challenges(
        npis,
        group_keys_by_npi,
        provider_keys_by_group,
    )
    await _add_reverse_persisted_memberships(
        lookup_graph_members,
        session,
        shared_snapshot_key,
        schema_name,
        _unresolved_persisted_memberships(
            persisted_audit_occurrences,
            provider_keys_by_npi,
        ),
        group_keys_by_npi,
        provider_keys_by_npi,
    )
    return {
        npi: tuple(sorted(provider_set_keys))
        for npi, provider_set_keys in provider_keys_by_npi.items()
    }


__all__ = ["provider_set_keys_by_npi"]
