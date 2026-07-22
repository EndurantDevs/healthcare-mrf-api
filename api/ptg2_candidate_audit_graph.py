# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded provider-graph resolution for candidate audit coordinates."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
    retain_unique_integer_keys,
)
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_candidate_audit_npis import (
    candidate_graph_npis as _candidate_graph_npis,
)
from api.ptg2_shared_blocks import (
    PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
    PTG2_V3_GRAPH_NPI_TO_GROUP,
    PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_manifest_artifacts import (
    ManifestReadLimitError,
)


GraphLookup = Callable[..., Awaitable[dict[int, tuple[int, ...]]]]
_PROVIDER_KEY_SET_BYTES = 256
# A 64-bit CPython set is 216 bytes empty and grows to 728, 2,264, and
# 8,408 bytes at 5, 19, and 77 members. 112 bytes per member covers each
# resize boundary before a key is inserted.
_PROVIDER_KEY_SET_MEMBERSHIP_BYTES = 112
_PROVIDER_KEY_ORDERING_BYTES = 112
_PROVIDER_KEY_ORDERING_MEMBERSHIP_BYTES = 16
_PROVIDER_KEY_TUPLE_BYTES = 56
_PROVIDER_KEY_TUPLE_MEMBERSHIP_BYTES = 8
# At tuple construction the result dict, 216-byte set, 56-byte sorted list,
# and 40-byte tuple coexist. Each already-owned integer also needs a set-table
# share plus one list and one tuple pointer: 112 + 8 + 8 bytes.
_PROVEN_PROVIDER_TUPLE_BYTES = 384
_PROVEN_PROVIDER_MEMBERSHIP_BYTES = 128
_NPI_PROVIDER_MAP_BYTES = 224
_NPI_PROVIDER_MAP_ENTRY_BYTES = 64
_NPI_PROVIDER_SET_BYTES = 216
_NPI_PROVIDER_SET_MEMBERSHIP_BYTES = 112


class ChallengeGraphScopeTooLarge(ManifestReadLimitError):
    """Preserve the loaded NPI groups for a bounded reverse fallback."""

    def __init__(
        self,
        group_keys_by_npi: Mapping[int, tuple[int, ...]],
        message: str = "shared PTG challenge graph exceeds the read-once byte limit",
    ) -> None:
        super().__init__(message)
        self.group_keys_by_npi = group_keys_by_npi


def _challenge_group_keys(
    challenge_npis: Iterable[int],
    group_keys_by_npi: Mapping[int, tuple[int, ...]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> tuple[int, ...]:
    """Return only groups that need broad witness expansion."""

    return _budgeted_unique_integer_tuple(
        (
            group_key
            for npi in challenge_npis
            for group_key in group_keys_by_npi.get(npi, ())
        ),
        retention_budget,
        category="challenge graph group",
    )


def _budgeted_unique_integer_tuple(
    integer_keys: Iterable[int],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
    *,
    category: str,
) -> tuple[int, ...]:
    """Normalize integers while claiming the set, ordering, and tuple peak."""

    normalized_keys, _retained_bytes = retain_unique_integer_keys(
        integer_keys,
        retention_budget,
        category=category,
    )
    return normalized_keys


def _provider_keys_for_challenges(
    npis: Iterable[int],
    group_keys_by_npi: Mapping[int, tuple[int, ...]],
    provider_keys_by_group: Mapping[int, tuple[int, ...]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> dict[int, set[int]]:
    """Build the broad provider scope required by witness challenges."""

    if retention_budget is not None:
        retention_budget.claim(
            _NPI_PROVIDER_MAP_BYTES,
            category="the broad NPI provider map",
        )
    provider_keys_by_npi: dict[int, set[int]] = {}
    for raw_npi in npis:
        npi = int(raw_npi)
        provider_keys = _provider_bucket_for_npi(
            provider_keys_by_npi,
            npi,
            retention_budget,
        )
        for group_key in group_keys_by_npi.get(npi, ()):
            for provider_set_key in provider_keys_by_group.get(group_key, ()):
                _retain_provider_key(
                    provider_keys,
                    int(provider_set_key),
                    retention_budget,
                )
    return provider_keys_by_npi


def _provider_bucket_for_npi(
    provider_keys_by_npi: dict[int, set[int]],
    npi: int,
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> set[int]:
    provider_keys = provider_keys_by_npi.get(npi)
    if provider_keys is not None:
        return provider_keys
    if retention_budget is not None:
        retention_budget.claim(
            _NPI_PROVIDER_MAP_ENTRY_BYTES + _NPI_PROVIDER_SET_BYTES,
            category="a broad NPI provider bucket",
        )
    provider_keys = set()
    provider_keys_by_npi[npi] = provider_keys
    return provider_keys


def _retain_provider_key(
    provider_keys: set[int],
    provider_set_key: int,
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> None:
    if provider_set_key in provider_keys:
        return
    if retention_budget is not None:
        retention_budget.claim(
            _NPI_PROVIDER_SET_MEMBERSHIP_BYTES,
            category="a broad NPI provider membership",
        )
    provider_keys.add(provider_set_key)


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
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
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
        retention_budget=retention_budget,
    )
    for npi, provider_set_key in unresolved_memberships:
        if _has_shared_graph_group(
            group_keys_by_npi.get(npi, ()),
            groups_by_provider_key.get(provider_set_key, ()),
        ):
            _retain_provider_key(
                provider_keys_by_npi[npi],
                provider_set_key,
                retention_budget,
            )


def _reverse_provider_set_keys(
    candidate_provider_set_keys_by_npi: Mapping[int, Iterable[int]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> tuple[int, ...]:
    """Build one budgeted provider-key tuple for the reverse graph read."""

    retained_set_bytes = _PROVIDER_KEY_SET_BYTES
    if retention_budget is not None:
        retention_budget.claim(
            retained_set_bytes,
            category="the reverse provider lookup set",
        )
    provider_set_key_set: set[int] = set()
    for candidate_keys in candidate_provider_set_keys_by_npi.values():
        for raw_provider_set_key in candidate_keys:
            provider_set_key = int(raw_provider_set_key)
            if provider_set_key in provider_set_key_set:
                continue
            if retention_budget is not None:
                retention_budget.claim(
                    _PROVIDER_KEY_SET_MEMBERSHIP_BYTES,
                    category="a reverse provider lookup key",
                )
                retained_set_bytes += _PROVIDER_KEY_SET_MEMBERSHIP_BYTES
            provider_set_key_set.add(provider_set_key)
    provider_count = len(provider_set_key_set)
    ordering_bytes = (
        _PROVIDER_KEY_ORDERING_BYTES
        + provider_count * _PROVIDER_KEY_ORDERING_MEMBERSHIP_BYTES
    )
    if retention_budget is not None:
        retention_budget.claim(
            ordering_bytes,
            category="the ordered reverse provider lookup",
        )
    provider_set_keys = tuple(sorted(provider_set_key_set))
    del provider_set_key_set
    if retention_budget is not None:
        retained_tuple_bytes = (
            _PROVIDER_KEY_TUPLE_BYTES
            + provider_count * _PROVIDER_KEY_TUPLE_MEMBERSHIP_BYTES
        )
        retention_budget.release(
            retained_set_bytes + ordering_bytes - retained_tuple_bytes
        )
    return provider_set_keys


def _has_shared_graph_group(
    npi_group_keys: tuple[int, ...],
    provider_group_keys: tuple[int, ...],
) -> bool:
    """Test group overlap without duplicating either decoded tuple."""

    return any(group_key in npi_group_keys for group_key in provider_group_keys)


def _proven_provider_keys_by_npi(
    candidate_provider_set_keys_by_npi: Mapping[int, Iterable[int]],
    group_keys_by_npi: Mapping[int, tuple[int, ...]],
    groups_by_provider_key: Mapping[int, tuple[int, ...]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> dict[int, tuple[int, ...]]:
    """Intersect each NPI candidate set with its proven graph groups."""

    proven_keys_by_npi: dict[int, tuple[int, ...]] = {}
    for raw_npi, candidate_keys in candidate_provider_set_keys_by_npi.items():
        npi = int(raw_npi)
        npi_group_keys = group_keys_by_npi.get(npi, ())
        if retention_budget is not None:
            retention_budget.claim(
                _PROVEN_PROVIDER_TUPLE_BYTES,
                category="a proven NPI provider tuple",
            )
        proven_key_set: set[int] = set()
        for raw_provider_set_key in candidate_keys:
            provider_set_key = int(raw_provider_set_key)
            if provider_set_key in proven_key_set or not _has_shared_graph_group(
                npi_group_keys,
                groups_by_provider_key.get(provider_set_key, ()),
            ):
                continue
            if retention_budget is not None:
                retention_budget.claim(
                    _PROVEN_PROVIDER_MEMBERSHIP_BYTES,
                    category="a proven NPI provider membership",
                )
            proven_key_set.add(provider_set_key)
        proven_keys = tuple(sorted(proven_key_set))
        del proven_key_set
        proven_keys_by_npi[npi] = proven_keys
    return proven_keys_by_npi


async def prove_provider_candidates_by_npi(
    lookup_graph_members: GraphLookup,
    session: Any,
    shared_snapshot_key: int,
    schema_name: str,
    candidate_provider_set_keys_by_npi: Mapping[int, Iterable[int]],
    group_keys_by_npi: Mapping[int, tuple[int, ...]],
    *,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> dict[int, tuple[int, ...]]:
    """Prove bounded provider candidates through the reverse graph."""

    provider_set_keys = _reverse_provider_set_keys(
        candidate_provider_set_keys_by_npi,
        retention_budget,
    )
    groups_by_provider_key = (
        await lookup_graph_members(
            session,
            shared_snapshot_key,
            PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
            provider_set_keys,
            schema_name=schema_name,
            retention_budget=retention_budget,
        )
        if provider_set_keys
        else {}
    )
    return _proven_provider_keys_by_npi(
        candidate_provider_set_keys_by_npi,
        group_keys_by_npi,
        groups_by_provider_key,
        retention_budget,
    )


async def _challenge_provider_keys_by_group(
    lookup_graph_members: GraphLookup,
    session: Any,
    shared_snapshot_key: int,
    schema_name: str,
    group_keys: tuple[int, ...],
    group_keys_by_npi: Mapping[int, tuple[int, ...]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> dict[int, tuple[int, ...]]:
    """Load the broad graph or preserve its NPI side for reverse proof."""

    if not group_keys:
        return {}
    try:
        return await lookup_graph_members(
            session,
            shared_snapshot_key,
            PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
            group_keys,
            schema_name=schema_name,
            retention_budget=retention_budget,
        )
    except (ManifestReadLimitError, CandidateAuditDecodedRetentionError) as exc:
        raise ChallengeGraphScopeTooLarge(
            group_keys_by_npi,
            str(exc),
        ) from exc


def _freeze_provider_keys_by_npi(
    provider_key_sets_by_npi: Mapping[int, set[int]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> dict[int, tuple[int, ...]]:
    """Replace broad sets with tuples while accounting their shared peak."""

    if retention_budget is None:
        return {
            npi: tuple(sorted(provider_keys))
            for npi, provider_keys in provider_key_sets_by_npi.items()
        }
    retained_result_bytes = _NPI_PROVIDER_MAP_BYTES
    retention_budget.claim(
        retained_result_bytes,
        category="the frozen NPI provider map",
    )
    frozen_provider_keys_by_npi: dict[int, tuple[int, ...]] = {}
    try:
        for npi, provider_keys in provider_key_sets_by_npi.items():
            provider_count = len(provider_keys)
            ordering_bytes = (
                _PROVIDER_KEY_ORDERING_BYTES
                + provider_count * _PROVIDER_KEY_ORDERING_MEMBERSHIP_BYTES
            )
            entry_peak_bytes = _NPI_PROVIDER_MAP_ENTRY_BYTES + ordering_bytes
            retention_budget.claim(
                entry_peak_bytes,
                category="a frozen NPI provider tuple",
            )
            retained_result_bytes += entry_peak_bytes
            frozen_keys = tuple(sorted(provider_keys))
            tuple_bytes = (
                _PROVIDER_KEY_TUPLE_BYTES
                + provider_count * _PROVIDER_KEY_TUPLE_MEMBERSHIP_BYTES
            )
            retention_budget.release(ordering_bytes - tuple_bytes)
            retained_result_bytes -= ordering_bytes - tuple_bytes
            frozen_provider_keys_by_npi[npi] = frozen_keys
    except BaseException:
        retention_budget.release(retained_result_bytes)
        raise
    retained_source_bytes = _NPI_PROVIDER_MAP_BYTES + sum(
        _NPI_PROVIDER_MAP_ENTRY_BYTES
        + _NPI_PROVIDER_SET_BYTES
        + len(provider_keys) * _NPI_PROVIDER_SET_MEMBERSHIP_BYTES
        for provider_keys in provider_key_sets_by_npi.values()
    )
    retention_budget.release(retained_source_bytes)
    return frozen_provider_keys_by_npi


async def provider_set_keys_by_npi(
    lookup_graph_members: GraphLookup,
    session: Any,
    shared_snapshot_key: int,
    schema_name: str,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    *,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> dict[int, tuple[int, ...]]:
    """Resolve broad witness scopes and exact persisted memberships."""

    challenge_npis, npis = _candidate_graph_npis(
        challenges,
        persisted_audit_occurrences,
        retention_budget,
    )
    group_keys_by_npi = await lookup_graph_members(
        session,
        shared_snapshot_key,
        PTG2_V3_GRAPH_NPI_TO_GROUP,
        npis,
        schema_name=schema_name,
        retention_budget=retention_budget,
    )
    group_keys = _challenge_group_keys(
        challenge_npis, group_keys_by_npi, retention_budget
    )
    provider_keys_by_group = await _challenge_provider_keys_by_group(
        lookup_graph_members,
        session,
        shared_snapshot_key,
        schema_name,
        group_keys,
        group_keys_by_npi,
        retention_budget,
    )
    provider_keys_by_npi = _provider_keys_for_challenges(
        npis,
        group_keys_by_npi,
        provider_keys_by_group,
        retention_budget,
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
        retention_budget,
    )
    return _freeze_provider_keys_by_npi(
        provider_keys_by_npi,
        retention_budget,
    )


__all__ = [
    "ChallengeGraphScopeTooLarge",
    "provider_set_keys_by_npi",
    "prove_provider_candidates_by_npi",
]
