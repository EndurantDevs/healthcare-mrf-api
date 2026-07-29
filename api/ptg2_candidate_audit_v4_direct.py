# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Graph-first fallback for dense direct-layout candidate partitions."""

from __future__ import annotations

from itertools import chain
from typing import Any, Callable, Mapping, Sequence

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
    retain_unique_integer_keys,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_db_sidecars import forward_price_row_retention_upper_bound
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_shared_blocks import PTG2SharedBlockError
from api.ptg2_candidate_audit_v4 import (
    _NPI_PROVIDER_MAP_BYTES,
    _NPI_PROVIDER_MAP_ENTRY_BYTES,
    _load_proven_v4_provider_sets,
)
from api.ptg2_serving import PTG2_SCHEMA
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


_DIRECT_GRAPH_MEMBER_LIMIT_MESSAGES = frozenset(
    {
        "PTG V4 graph selection exceeds max_members",
        "PTG2 V4 graph selection exceeds max_members",
    }
)


def _declared_rate_count(code_record: Mapping[str, Any]) -> int:
    """Require one authenticated non-negative rate cardinality."""

    if not isinstance(code_record, Mapping):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate code metadata is invalid"
        )
    raw_rate_count = code_record.get("rate_count")
    if type(raw_rate_count) is not int:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate code is missing its declared rate count"
        )
    if raw_rate_count < 0:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate code has an invalid declared rate count"
        )
    return raw_rate_count


def should_load_direct_graph_first(
    code_index: CandidateCodeIndex,
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> bool:
    """Choose graph-first before broad forward I/O cannot fit the budget."""

    declared_rate_count = sum(
        _declared_rate_count(code_record)
        for code_record in code_index.by_key.values()
    )
    required_bytes = forward_price_row_retention_upper_bound(
        declared_rate_count
    )
    available_bytes = (
        retention_budget.maximum_bytes - retention_budget.retained_bytes
    )
    return required_bytes >= available_bytes


def is_direct_graph_capacity_failure(error: Exception) -> bool:
    """Identify only capacity failures that permit one code-first attempt."""

    if isinstance(error, CandidateAuditDecodedRetentionError):
        return True
    return isinstance(
        error,
        (PTG2SharedBlockError, PTG2ManifestArtifactError),
    ) and str(error) in _DIRECT_GRAPH_MEMBER_LIMIT_MESSAGES


def _direct_npi_scope(
    requested_npis: tuple[int, ...],
    retention_budget: CandidateAuditDecodedRetentionBudget,
) -> tuple[dict[int, None], int]:
    """Build one claimed NPI-only map for sequential direct graph proof."""

    retained_bytes = (
        _NPI_PROVIDER_MAP_BYTES
        + len(requested_npis) * _NPI_PROVIDER_MAP_ENTRY_BYTES
    )
    retention_budget.claim(
        retained_bytes,
        category="the direct graph-first NPI scope",
    )
    try:
        candidate_keys_by_npi = dict.fromkeys(requested_npis)
    except BaseException:
        retention_budget.release(retained_bytes)
        raise
    return candidate_keys_by_npi, retained_bytes


async def load_v4_direct_provider_scope(
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    *,
    schema_name: str = PTG2_SCHEMA,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
    coordinate_observer: Callable[[int], None] | None = None,
) -> dict[int, tuple[int, ...]]:
    """Resolve direct-layout NPIs one at a time before the exact forward read."""

    if retention_budget is None:
        retention_budget = CandidateAuditDecodedRetentionBudget()
    requested_npis, retained_npi_bytes = retain_unique_integer_keys(
        chain(
            (challenge.npi for challenge in challenges),
            (
                occurrence.npi
                for occurrence in persisted_audit_occurrences
            ),
        ),
        retention_budget,
        category="direct graph-first NPI",
    )
    retained_scope_bytes = 0
    try:
        candidate_keys_by_npi, retained_scope_bytes = _direct_npi_scope(
            requested_npis,
            retention_budget,
        )
        return await _load_proven_v4_provider_sets(
            session,
            serving_tables,
            candidate_keys_by_npi,
            retention_budget,
            schema_name=schema_name,
            coordinate_observer=coordinate_observer,
        )
    finally:
        retention_budget.release(retained_npi_bytes + retained_scope_bytes)


__all__ = [
    "is_direct_graph_capacity_failure",
    "load_v4_direct_provider_scope",
    "should_load_direct_graph_first",
]
