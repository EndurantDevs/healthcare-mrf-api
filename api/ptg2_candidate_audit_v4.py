# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded code-first provider proof for V4 candidate audits."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
    retain_unique_integer_key_set,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_db_sidecars import lookup_forward_price_index_from_db
from api.ptg2_serving import (
    PTG2_SCHEMA,
    _required_shared_snapshot_key,
    _required_source_count,
    _v4_sets_by_npi,
    _version_three_forward_lookup_hints,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)


_NPI_PROVIDER_MAP_BYTES = 224
_NPI_PROVIDER_MAP_ENTRY_BYTES = 64
_NPI_PROVIDER_BUCKET_BYTES = 216
_NPI_PROVIDER_MEMBERSHIP_BYTES = 112
_V4_RESULT_MAP_BYTES = 224
_V4_RESULT_BUCKET_BYTES = 384
_V4_RESULT_MEMBERSHIP_BYTES = 128
# Covers simultaneous V4 locator/member projections, the raw graph result,
# and the exact per-NPI filtered result retained during candidate proof.
_V4_GRAPH_PEAK_MEMBER_BYTES = 512
_V4_GRAPH_TRANSIENT_MAP_BYTES = 4 * 224
_V4_GRAPH_TRANSIENT_OWNER_BYTES = 384


@dataclass(frozen=True)
class V4CandidateScope:
    """Exact provider memberships and forward rows read during their proof."""

    provider_set_keys_by_npi: dict[int, tuple[int, ...]]
    price_keys_by_occurrence: dict[tuple[int, int, int], tuple[int, ...]]


@dataclass(frozen=True)
class V4CandidateBuilders:
    """Callbacks shared with the established V3 code/source projection."""

    source_keys: Callable[..., Any]
    provider_candidates: Callable[..., dict[int, set[int]]]


@dataclass(frozen=True)
class _V4GraphReservation:
    maximum_graph_members: int
    reservation_bytes: int
    final_fixed_bytes: int


def _candidate_map_retained_bytes(
    candidate_keys_by_npi: Mapping[int, set[int]],
) -> int:
    """Return the exact claims owned by the mutable candidate projection."""

    return _NPI_PROVIDER_MAP_BYTES + sum(
        _NPI_PROVIDER_MAP_ENTRY_BYTES
        + _NPI_PROVIDER_BUCKET_BYTES
        + len(candidate_keys) * _NPI_PROVIDER_MEMBERSHIP_BYTES
        for candidate_keys in candidate_keys_by_npi.values()
    )


def _reserve_v4_graph_projection(
    candidate_keys_by_npi: Mapping[int, set[int]],
    allowed_provider_set_keys: set[int],
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> _V4GraphReservation:
    """Reserve a conservative peak before any V4 relation is decoded."""

    npi_count = len(candidate_keys_by_npi)
    final_fixed_bytes = (
        _V4_RESULT_MAP_BYTES + npi_count * _V4_RESULT_BUCKET_BYTES
    )
    transient_fixed_bytes = (
        _V4_GRAPH_TRANSIENT_MAP_BYTES
        + (4 * npi_count + len(allowed_provider_set_keys))
        * _V4_GRAPH_TRANSIENT_OWNER_BYTES
    )
    fixed_bytes = final_fixed_bytes + transient_fixed_bytes
    available_bytes = (
        retention_budget.maximum_bytes - retention_budget.retained_bytes
    )
    maximum_graph_members = (
        available_bytes - fixed_bytes
    ) // _V4_GRAPH_PEAK_MEMBER_BYTES
    if maximum_graph_members < 1:
        raise CandidateAuditDecodedRetentionError(
            "PTG2 candidate audit decoded retention exceeds the byte limit "
            "while retaining the bounded V4 candidate graph projection"
        )
    reservation_bytes = (
        fixed_bytes
        + maximum_graph_members * _V4_GRAPH_PEAK_MEMBER_BYTES
    )
    retention_budget.claim(
        reservation_bytes,
        category="the bounded V4 candidate graph projection",
    )
    return _V4GraphReservation(
        maximum_graph_members=maximum_graph_members,
        reservation_bytes=reservation_bytes,
        final_fixed_bytes=final_fixed_bytes,
    )


def _filtered_v4_provider_sets(
    raw_provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    candidate_keys_by_npi: Mapping[int, set[int]],
) -> dict[int, tuple[int, ...]]:
    """Discard graph matches outside each NPI's code/source candidates."""

    return {
        npi: tuple(
            provider_set_key
            for provider_set_key in raw_provider_set_keys_by_npi.get(npi, ())
            if provider_set_key in candidate_keys
        )
        for npi, candidate_keys in candidate_keys_by_npi.items()
    }


def _retained_v4_result_bytes(
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    final_fixed_bytes: int,
) -> int:
    return final_fixed_bytes + sum(
        len(provider_set_keys)
        for provider_set_keys in provider_set_keys_by_npi.values()
    ) * _V4_RESULT_MEMBERSHIP_BYTES


def _empty_v4_provider_sets(
    candidate_keys_by_npi: Mapping[int, set[int]],
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> dict[int, tuple[int, ...]]:
    retained_bytes = (
        _V4_RESULT_MAP_BYTES
        + len(candidate_keys_by_npi) * _V4_RESULT_BUCKET_BYTES
    )
    retention_budget.claim(
        retained_bytes,
        category="the empty V4 candidate provider result",
    )
    return {npi: () for npi in candidate_keys_by_npi}


async def _load_proven_v4_provider_sets(
    session: Any,
    serving_tables: PTG2ServingTables,
    candidate_keys_by_npi: dict[int, set[int]],
    allowed_provider_set_keys: set[int],
    retention_budget: CandidateAuditDecodedRetentionBudget,
    *,
    schema_name: str,
) -> dict[int, tuple[int, ...]]:
    """Load, filter, and release one reserved V4 graph projection."""

    reservation = _reserve_v4_graph_projection(
        candidate_keys_by_npi,
        allowed_provider_set_keys,
        retention_budget,
    )
    try:
        raw_provider_set_keys_by_npi = await _v4_sets_by_npi(
            session,
            serving_tables,
            candidate_keys_by_npi,
            allowed_provider_set_keys=allowed_provider_set_keys,
            schema_name=schema_name,
            max_members=reservation.maximum_graph_members,
        )
        provider_set_keys_by_npi = _filtered_v4_provider_sets(
            raw_provider_set_keys_by_npi,
            candidate_keys_by_npi,
        )
        retained_result_bytes = _retained_v4_result_bytes(
            provider_set_keys_by_npi,
            reservation.final_fixed_bytes,
        )
        retention_budget.release(
            reservation.reservation_bytes - retained_result_bytes
        )
    except BaseException:
        retention_budget.release(reservation.reservation_bytes)
        raise
    return provider_set_keys_by_npi


async def prove_v4_candidate_sets(
    session: Any,
    serving_tables: PTG2ServingTables,
    candidate_keys_by_npi: dict[int, set[int]],
    retention_budget: CandidateAuditDecodedRetentionBudget,
    *,
    schema_name: str,
) -> dict[int, tuple[int, ...]]:
    """Prove only code/source candidates through one bounded V4 graph walk."""

    candidate_source_bytes = _candidate_map_retained_bytes(
        candidate_keys_by_npi
    )
    allowed_provider_set_keys, allowed_set_bytes = retain_unique_integer_key_set(
        (
            provider_set_key
            for candidate_keys in candidate_keys_by_npi.values()
            for provider_set_key in candidate_keys
        ),
        retention_budget,
        category="V4 candidate provider",
    )
    try:
        if not allowed_provider_set_keys:
            provider_set_keys_by_npi = _empty_v4_provider_sets(
                candidate_keys_by_npi,
                retention_budget,
            )
        else:
            provider_set_keys_by_npi = await _load_proven_v4_provider_sets(
                session,
                serving_tables,
                candidate_keys_by_npi,
                allowed_provider_set_keys,
                retention_budget,
                schema_name=schema_name,
            )
    finally:
        retention_budget.release(allowed_set_bytes)
    retention_budget.release(candidate_source_bytes)
    return provider_set_keys_by_npi


async def load_v4_candidate_scope(
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
    *,
    builders: V4CandidateBuilders,
    schema_name: str = PTG2_SCHEMA,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> V4CandidateScope:
    """Load exact forward rows, then prove their V4 provider memberships."""

    if retention_budget is None:
        retention_budget = CandidateAuditDecodedRetentionBudget()
    budgeted_source_keys = builders.source_keys(
        challenges,
        persisted_audit_occurrences,
        code_index,
        retention_budget,
    )
    try:
        price_keys_by_occurrence = await lookup_forward_price_index_from_db(
            session,
            budgeted_source_keys.source_keys_by_code,
            source_keys_by_code=budgeted_source_keys.source_keys_by_code,
            shared_snapshot_key=_required_shared_snapshot_key(serving_tables),
            source_count=_required_source_count(serving_tables),
            schema_name=schema_name,
            retention_budget=retention_budget,
            **_version_three_forward_lookup_hints(serving_tables),
        )
    finally:
        retention_budget.release(budgeted_source_keys.retained_bytes)
    candidate_keys_by_npi = builders.provider_candidates(
        challenges,
        persisted_audit_occurrences,
        code_index,
        price_keys_by_occurrence,
        retention_budget,
    )
    provider_set_keys_by_npi = await prove_v4_candidate_sets(
        session,
        serving_tables,
        candidate_keys_by_npi,
        retention_budget,
        schema_name=schema_name,
    )
    return V4CandidateScope(
        provider_set_keys_by_npi=provider_set_keys_by_npi,
        price_keys_by_occurrence=price_keys_by_occurrence,
    )


__all__ = [
    "V4CandidateScope",
    "V4CandidateBuilders",
    "load_v4_candidate_scope",
    "prove_v4_candidate_sets",
]
