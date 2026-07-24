# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Code-first reverse provider proof for oversized candidate graph scopes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Sequence

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_graph import (
    ChallengeGraphScopeTooLarge,
    prove_provider_candidates_by_npi,
)
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_candidate_audit_v4 import (
    V4CandidateBuilders,
    load_v4_candidate_scope,
)
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


_SOURCE_KEY_SET_MAP_BYTES = 224
_SOURCE_KEY_SET_BUCKET_BYTES = 280
_SOURCE_KEY_SET_MEMBERSHIP_BYTES = 112
_SOURCE_KEY_RESULT_MAP_BYTES = 224
_SOURCE_KEY_RESULT_BUCKET_BYTES = 160
_SOURCE_KEY_RESULT_MEMBERSHIP_BYTES = 16
_CODE_SOURCE_NPI_MAP_BYTES = 224
_CODE_SOURCE_NPI_KEY_BYTES = 64
_CODE_SOURCE_NPI_BUCKET_BYTES = 216
_CODE_SOURCE_NPI_MEMBERSHIP_BYTES = 112
_NPI_PROVIDER_MAP_BYTES = 224
# Covers retained integer keys plus CPython dictionary resize slack.
_NPI_PROVIDER_MAP_ENTRY_BYTES = 64
_NPI_PROVIDER_BUCKET_BYTES = 216
_NPI_PROVIDER_MEMBERSHIP_BYTES = 112
_PRELOADED_NPI_GROUP_MAP_BYTES = 256
_PRELOADED_NPI_GROUP_BUCKET_BYTES = 160
_PRELOADED_NPI_GROUP_MEMBERSHIP_BYTES = 48


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


@dataclass(frozen=True)
class _BudgetedSourceKeys:
    source_keys_by_code: dict[int, tuple[int, ...]]
    retained_bytes: int


@dataclass(frozen=True)
class _BudgetedCodeSourceNpis:
    npis_by_code_source: dict[tuple[int, int], set[int]]
    retained_bytes: int


def _retain_source_key(
    source_key_sets_by_code: dict[int, set[int]],
    code_key: int,
    source_key: int,
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> int:
    """Retain one source key and return its newly claimed byte count."""

    claimed_bytes = 0
    source_keys = source_key_sets_by_code.get(code_key)
    if source_keys is None:
        retention_budget.claim(
            _SOURCE_KEY_SET_BUCKET_BYTES,
            category="a source-key set bucket",
        )
        claimed_bytes += _SOURCE_KEY_SET_BUCKET_BYTES
        source_keys = set()
        source_key_sets_by_code[code_key] = source_keys
    if source_key not in source_keys:
        retention_budget.claim(
            _SOURCE_KEY_SET_MEMBERSHIP_BYTES,
            category="a source-key set membership",
        )
        claimed_bytes += _SOURCE_KEY_SET_MEMBERSHIP_BYTES
        source_keys.add(source_key)
    return claimed_bytes


def _ordered_source_keys(
    source_key_sets_by_code: Mapping[int, set[int]],
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> _BudgetedSourceKeys:
    """Convert source sets with the set, list, tuple, and map peak claimed."""

    retained_result_bytes = _SOURCE_KEY_RESULT_MAP_BYTES
    retention_budget.claim(
        retained_result_bytes,
        category="the ordered source-key map",
    )
    source_keys_by_code: dict[int, tuple[int, ...]] = {}
    for code_key, source_keys in source_key_sets_by_code.items():
        ordered_bucket_bytes = (
            _SOURCE_KEY_RESULT_BUCKET_BYTES
            + len(source_keys) * _SOURCE_KEY_RESULT_MEMBERSHIP_BYTES
        )
        retention_budget.claim(
            ordered_bucket_bytes,
            category="an ordered source-key bucket",
        )
        retained_result_bytes += ordered_bucket_bytes
        source_keys_by_code[code_key] = tuple(sorted(source_keys))
    return _BudgetedSourceKeys(
        source_keys_by_code=source_keys_by_code,
        retained_bytes=retained_result_bytes,
    )


def _source_keys_by_code_key(
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> _BudgetedSourceKeys:
    """Bind each requested code to only its sealed source coordinates."""

    retained_set_bytes = _SOURCE_KEY_SET_MAP_BYTES
    retention_budget.claim(
        retained_set_bytes,
        category="the source-key set map",
    )
    source_key_sets_by_code: dict[int, set[int]] = {}
    for challenge in challenges:
        for code_record in code_index.by_pair[
            (challenge.code_system, challenge.code)
        ]:
            retained_set_bytes += _retain_source_key(
                source_key_sets_by_code,
                int(code_record["code_key"]),
                challenge.source_artifact_key,
                retention_budget,
            )
    for occurrence in persisted_audit_occurrences:
        retained_set_bytes += _retain_source_key(
            source_key_sets_by_code,
            occurrence.code_key,
            occurrence.source_artifact_key,
            retention_budget,
        )
    budgeted_source_keys = _ordered_source_keys(
        source_key_sets_by_code,
        retention_budget,
    )
    del source_key_sets_by_code
    retention_budget.release(retained_set_bytes)
    return budgeted_source_keys


def _claim_preloaded_npi_group_retention(
    group_keys_by_npi: Mapping[int, tuple[int, ...]],
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> None:
    """Account the already-decoded NPI graph without building another index."""

    retention_budget.claim(
        _PRELOADED_NPI_GROUP_MAP_BYTES,
        category="the preloaded NPI group map",
    )
    for group_keys in group_keys_by_npi.values():
        retention_budget.claim(
            _PRELOADED_NPI_GROUP_BUCKET_BYTES
            + len(group_keys) * _PRELOADED_NPI_GROUP_MEMBERSHIP_BYTES,
            category="preloaded NPI group memberships",
        )


def _code_source_npis(
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> _BudgetedCodeSourceNpis:
    """Build a resize-safe temporary code/source-to-NPI projection."""

    retained_bytes = _CODE_SOURCE_NPI_MAP_BYTES
    retention_budget.claim(
        retained_bytes,
        category="the code/source NPI map",
    )
    npis_by_code_source: dict[tuple[int, int], set[int]] = {}
    for challenge in challenges:
        for code_record in code_index.by_pair[
            (challenge.code_system, challenge.code)
        ]:
            retention_budget.claim(
                _CODE_SOURCE_NPI_KEY_BYTES,
                category="a code/source NPI key",
            )
            code_source_key = (
                int(code_record["code_key"]),
                challenge.source_artifact_key,
            )
            npis = npis_by_code_source.get(code_source_key)
            if npis is None:
                retention_budget.claim(
                    _CODE_SOURCE_NPI_BUCKET_BYTES,
                    category="a code/source NPI bucket",
                )
                retained_bytes += (
                    _CODE_SOURCE_NPI_KEY_BYTES
                    + _CODE_SOURCE_NPI_BUCKET_BYTES
                )
                npis = set()
                npis_by_code_source[code_source_key] = npis
            else:
                retention_budget.release(_CODE_SOURCE_NPI_KEY_BYTES)
            if challenge.npi not in npis:
                retention_budget.claim(
                    _CODE_SOURCE_NPI_MEMBERSHIP_BYTES,
                    category="a code/source NPI membership",
                )
                retained_bytes += _CODE_SOURCE_NPI_MEMBERSHIP_BYTES
                npis.add(challenge.npi)
    return _BudgetedCodeSourceNpis(
        npis_by_code_source=npis_by_code_source,
        retained_bytes=retained_bytes,
    )


def _provider_candidates_by_npi(
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
    price_keys_by_occurrence: Mapping[tuple[int, int, int], tuple[int, ...]],
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> dict[int, set[int]]:
    """Project the source-filtered forward rows into exact NPI candidates."""

    code_source_npis = _code_source_npis(
        challenges,
        code_index,
        retention_budget,
    )

    retention_budget.claim(
        _NPI_PROVIDER_MAP_BYTES,
        category="the NPI provider map",
    )
    candidate_keys_by_npi: dict[int, set[int]] = {}
    for challenge in challenges:
        _candidate_keys_for_npi(
            candidate_keys_by_npi,
            challenge.npi,
            retention_budget,
        )
    for code_key, provider_set_key, source_key in price_keys_by_occurrence:
        for npi in code_source_npis.npis_by_code_source.get(
            (code_key, source_key),
            (),
        ):
            _retain_npi_provider_membership(
                candidate_keys_by_npi,
                npi,
                provider_set_key,
                retention_budget,
            )
    for occurrence in persisted_audit_occurrences:
        _retain_npi_provider_membership(
            candidate_keys_by_npi,
            occurrence.npi,
            occurrence.provider_set_key,
            retention_budget,
        )
    retention_budget.release(code_source_npis.retained_bytes)
    del code_source_npis
    return candidate_keys_by_npi


def _retain_npi_provider_membership(
    candidate_keys_by_npi: dict[int, set[int]],
    npi: int,
    provider_set_key: int,
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> None:
    """Retain one unique NPI/provider edge within the decoded-memory budget."""

    candidate_keys = _candidate_keys_for_npi(
        candidate_keys_by_npi,
        npi,
        retention_budget,
    )
    if provider_set_key not in candidate_keys:
        retention_budget.claim(
            _NPI_PROVIDER_MEMBERSHIP_BYTES,
            category="an NPI provider membership",
        )
        candidate_keys.add(provider_set_key)


def _candidate_keys_for_npi(
    candidate_keys_by_npi: dict[int, set[int]],
    npi: int,
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> set[int]:
    """Return one retained candidate bucket, claiming its decoded overhead."""

    candidate_keys = candidate_keys_by_npi.get(npi)
    if candidate_keys is not None:
        return candidate_keys
    retention_budget.claim(
        _NPI_PROVIDER_MAP_ENTRY_BYTES + _NPI_PROVIDER_BUCKET_BYTES,
        category="an NPI provider map entry and bucket",
    )
    candidate_keys = set()
    candidate_keys_by_npi[npi] = candidate_keys
    return candidate_keys


async def load_reverse_source_candidate_scope(
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
    group_keys_by_npi: Mapping[int, tuple[int, ...]],
    *,
    schema_name: str = PTG2_SCHEMA,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> ReverseCandidateScope:
    """Replace an oversized group fan-out with code-first reverse proof."""

    if retention_budget is None:
        retention_budget = CandidateAuditDecodedRetentionBudget()
        _claim_preloaded_npi_group_retention(
            group_keys_by_npi,
            retention_budget,
        )
    budgeted_source_keys = _source_keys_by_code_key(
        challenges,
        persisted_audit_occurrences,
        code_index,
        retention_budget,
    )
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
    retention_budget.release(budgeted_source_keys.retained_bytes)
    del budgeted_source_keys
    candidate_keys_by_npi = _provider_candidates_by_npi(
        challenges,
        persisted_audit_occurrences,
        code_index,
        price_keys_by_occurrence,
        retention_budget,
    )
    provider_set_keys_by_npi = await prove_provider_candidates_by_npi(
        lookup_shared_graph_members_from_db,
        session,
        _required_shared_snapshot_key(serving_tables),
        schema_name,
        candidate_keys_by_npi,
        group_keys_by_npi,
        retention_budget=retention_budget,
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
    *,
    schema_name: str = PTG2_SCHEMA,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> CandidateProviderScope:
    """Select V4 code-first proof or the bounded V3 broad/reverse path."""

    if retention_budget is None:
        retention_budget = CandidateAuditDecodedRetentionBudget()
    if serving_tables.uses_v4_graph:
        v4_scope = await load_v4_candidate_scope(
            session,
            serving_tables,
            challenges,
            persisted_audit_occurrences,
            code_index,
            builders=V4CandidateBuilders(
                source_keys=_source_keys_by_code_key,
                provider_candidates=_provider_candidates_by_npi,
            ),
            schema_name=schema_name,
            retention_budget=retention_budget,
        )
        return CandidateProviderScope(
            provider_set_keys_by_npi=v4_scope.provider_set_keys_by_npi,
            price_keys_by_occurrence=v4_scope.price_keys_by_occurrence,
        )
    try:
        provider_keys_by_npi = await broad_scope_lookup(
            session,
            serving_tables,
            challenges,
            persisted_audit_occurrences,
            retention_budget=retention_budget,
        )
    except ChallengeGraphScopeTooLarge as exc:
        reverse_scope = await load_reverse_source_candidate_scope(
            session,
            serving_tables,
            challenges,
            persisted_audit_occurrences,
            code_index,
            exc.group_keys_by_npi,
            schema_name=schema_name,
            retention_budget=retention_budget,
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
