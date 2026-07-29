# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Representation-aware dispatch for bounded candidate provider proof."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Sequence

from api import ptg2_candidate_audit_reverse as reverse_scope
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_graph import ChallengeGraphScopeTooLarge
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_candidate_audit_v4 import (
    V4CandidateBuilders,
    load_v4_candidate_scope,
    load_v4_pattern_provider_scope,
)
from api.ptg2_serving import PTG2_SCHEMA, _required_shared_snapshot_key
from api.ptg2_types import PTG2ServingTables
from api.ptg2_v4_graph import load_v4_graph_root
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)


async def _load_v4_scope(
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    persisted_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
    *,
    schema_name: str,
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> reverse_scope.CandidateProviderScope:
    """Select pattern graph-first or direct code-first V4 proof."""

    graph_root = await load_v4_graph_root(
        session,
        _required_shared_snapshot_key(serving_tables),
        schema_name=schema_name,
    )
    if graph_root.representation == "pattern_v1":
        provider_sets_by_npi = await load_v4_pattern_provider_scope(
            session,
            serving_tables,
            challenges,
            persisted_occurrences,
            schema_name=schema_name,
            retention_budget=retention_budget,
        )
        return reverse_scope.CandidateProviderScope(
            provider_set_keys_by_npi=provider_sets_by_npi,
            price_keys_by_occurrence=None,
        )
    direct_scope = await load_v4_candidate_scope(
        session,
        serving_tables,
        challenges,
        persisted_occurrences,
        code_index,
        builders=V4CandidateBuilders(
            source_keys=reverse_scope._source_keys_by_code_key,
            provider_candidates=reverse_scope._provider_candidates_by_npi,
        ),
        schema_name=schema_name,
        retention_budget=retention_budget,
    )
    return reverse_scope.CandidateProviderScope(
        provider_set_keys_by_npi=direct_scope.provider_set_keys_by_npi,
        price_keys_by_occurrence=direct_scope.price_keys_by_occurrence,
    )


async def _load_v3_scope(
    broad_scope_lookup: Callable[..., Awaitable[dict[int, tuple[int, ...]]]],
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    persisted_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
    *,
    schema_name: str,
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> reverse_scope.CandidateProviderScope:
    """Select bounded V3 broad proof with code-first fallback."""

    try:
        provider_keys_by_npi = await broad_scope_lookup(
            session,
            serving_tables,
            challenges,
            persisted_occurrences,
            retention_budget=retention_budget,
        )
    except ChallengeGraphScopeTooLarge as exc:
        reverse_result = await reverse_scope.load_reverse_source_candidate_scope(
            session,
            serving_tables,
            challenges,
            persisted_occurrences,
            code_index,
            exc.group_keys_by_npi,
            schema_name=schema_name,
            retention_budget=retention_budget,
        )
        return reverse_scope.CandidateProviderScope(
            provider_set_keys_by_npi=(
                reverse_result.provider_set_keys_by_npi
            ),
            price_keys_by_occurrence=reverse_result.price_keys_by_occurrence,
        )
    return reverse_scope.CandidateProviderScope(
        provider_set_keys_by_npi=provider_keys_by_npi,
        price_keys_by_occurrence=None,
    )


async def load_candidate_provider_scope(
    broad_scope_lookup: Callable[..., Awaitable[dict[int, tuple[int, ...]]]],
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    persisted_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
    *,
    schema_name: str = PTG2_SCHEMA,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> reverse_scope.CandidateProviderScope:
    """Select the representation-aware bounded provider proof."""

    if retention_budget is None:
        retention_budget = CandidateAuditDecodedRetentionBudget()
    if serving_tables.uses_v4_graph:
        return await _load_v4_scope(
            session,
            serving_tables,
            challenges,
            persisted_occurrences,
            code_index,
            schema_name=schema_name,
            retention_budget=retention_budget,
        )
    return await _load_v3_scope(
        broad_scope_lookup,
        session,
        serving_tables,
        challenges,
        persisted_occurrences,
        code_index,
        schema_name=schema_name,
        retention_budget=retention_budget,
    )


__all__ = ["load_candidate_provider_scope"]
