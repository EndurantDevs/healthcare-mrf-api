# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Graph-first fallback for dense direct-layout candidate partitions."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import chain
from typing import Any, Callable, Mapping, Sequence

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
    retain_unique_integer_keys,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_candidate_audit_reverse import (
    provider_candidate_projection_retention_upper_bound,
    source_key_projection_retention_upper_bound,
)
from api.ptg2_db_sidecars import forward_price_index_retention_upper_bound
from api.ptg2_shared_blocks import PTG2SharedBlockError
from api.ptg2_candidate_audit_v4 import (
    _NPI_PROVIDER_MAP_BYTES,
    _NPI_PROVIDER_MAP_ENTRY_BYTES,
    _load_proven_v4_provider_sets,
    v4_candidate_proof_memory_bound,
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


@dataclass(frozen=True)
class _DirectCodeFirstCardinality:
    code_count: int
    rate_count: int
    source_membership_count: int
    code_source_pair_count: int
    code_source_npi_membership_count: int
    npi_count: int
    candidate_membership_count: int


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


def _challenge_npis_by_code_source(
    code_index: CandidateCodeIndex,
    challenges: Sequence[AuditBatchChallenge],
) -> dict[tuple[int, int], set[int]]:
    """Resolve exact distinct NPI fanout for each requested code/source."""

    npis_by_code_source: dict[tuple[int, int], set[int]] = {}
    for challenge in challenges:
        for code_record in code_index.by_pair[
            (challenge.code_system, challenge.code)
        ]:
            code_source_key = (
                int(code_record["code_key"]),
                challenge.source_artifact_key,
            )
            npis_by_code_source.setdefault(code_source_key, set()).add(
                challenge.npi
            )
    return npis_by_code_source


def _direct_code_first_cardinality(
    code_index: CandidateCodeIndex,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
) -> _DirectCodeFirstCardinality:
    """Derive conservative fanout from authenticated code and request scope."""

    rate_counts_by_code = {
        int(code_key): _declared_rate_count(code_record)
        for code_key, code_record in code_index.by_key.items()
    }
    npis_by_code_source = _challenge_npis_by_code_source(
        code_index,
        challenges,
    )
    maximum_npis_by_code: dict[int, int] = {}
    for (code_key, _source_key), npis in npis_by_code_source.items():
        maximum_npis_by_code[code_key] = max(
            maximum_npis_by_code.get(code_key, 0),
            len(npis),
        )
    candidate_membership_count = sum(
        rate_counts_by_code[code_key] * npi_count
        for code_key, npi_count in maximum_npis_by_code.items()
    ) + len(persisted_audit_occurrences)
    requested_npis = {
        challenge.npi for challenge in challenges
    } | {
        occurrence.npi for occurrence in persisted_audit_occurrences
    }
    source_membership_count = sum(
        len(
            code_index.by_pair[
                (challenge.code_system, challenge.code)
            ]
        )
        for challenge in challenges
    ) + len(persisted_audit_occurrences)
    return _DirectCodeFirstCardinality(
        code_count=len(code_index.by_key),
        rate_count=sum(rate_counts_by_code.values()),
        source_membership_count=source_membership_count,
        code_source_pair_count=len(npis_by_code_source),
        code_source_npi_membership_count=sum(
            len(npis) for npis in npis_by_code_source.values()
        ),
        npi_count=len(requested_npis),
        candidate_membership_count=candidate_membership_count,
    )


def direct_code_first_retention_upper_bound(
    code_index: CandidateCodeIndex,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
) -> int:
    """Bound source projection plus the complete broad forward peak."""

    cardinality = _direct_code_first_cardinality(
        code_index,
        challenges,
        persisted_audit_occurrences,
    )
    return source_key_projection_retention_upper_bound(
        cardinality.code_count,
        cardinality.source_membership_count,
    ) + forward_price_index_retention_upper_bound(
        cardinality.rate_count,
        cardinality.code_count,
        filter_coordinate_count=cardinality.source_membership_count,
    ) + provider_candidate_projection_retention_upper_bound(
        cardinality.code_source_pair_count,
        cardinality.code_source_npi_membership_count,
        cardinality.npi_count,
        cardinality.candidate_membership_count,
    ) + v4_candidate_proof_memory_bound(
        cardinality.npi_count,
        cardinality.candidate_membership_count,
    )


def should_load_direct_graph_first(
    code_index: CandidateCodeIndex,
    retention_budget: CandidateAuditDecodedRetentionBudget,
    *,
    challenges: Sequence[AuditBatchChallenge] = (),
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence] = (),
) -> bool:
    """Choose graph-first before broad forward I/O cannot fit the budget."""

    required_bytes = direct_code_first_retention_upper_bound(
        code_index,
        challenges,
        persisted_audit_occurrences,
    )
    available_bytes = (
        retention_budget.maximum_bytes - retention_budget.retained_bytes
    )
    return required_bytes > available_bytes


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
    "direct_code_first_retention_upper_bound",
    "is_direct_graph_capacity_failure",
    "load_v4_direct_provider_scope",
    "should_load_direct_graph_first",
]
