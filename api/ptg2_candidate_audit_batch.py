# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Set-based exhaustive source-witness verification for one PTG candidate."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text

from api.ptg2_candidate_audit import PTG2CandidateAuditAccess
from api.ptg2_candidate_audit_capacity import CandidateAuditDecodedRetentionBudget
from api.ptg2_candidate_audit_codes import (
    CandidateCodeIndex,
    candidate_code_records_by_pair,
)
from api.ptg2_candidate_audit_coordinates import (
    validate_persisted_audit_graph_scope,
    validate_persisted_audit_price_scope,
)
from api.ptg2_candidate_audit_graph import provider_set_keys_by_npi
from api.ptg2_candidate_audit_forward import (
    CandidatePriceLoaders,
    load_candidate_price_data,
)
from api.ptg2_candidate_audit_integrity import (
    PersistedAuditOccurrence,
    validate_candidate_source_scope,
)
from api.ptg2_candidate_audit_networks import (
    _network_record_fields as _record_fields,
    provider_network_digests_by_key as _network_digests_by_key,
    provider_network_names_by_key as _network_names_by_key,
)
from api.ptg2_candidate_audit_price_load import (
    CandidatePriceLoad as _CandidatePriceLoad,
)
from api.ptg2_candidate_audit_reverse import load_candidate_provider_scope
from api.ptg2_db_sidecars import (
    lookup_forward_price_index_from_db,
    lookup_shared_graph_members_from_db,
)
from api.ptg2_shared_blocks import bind_shared_block_decoded_retention_budget
from api.ptg2_candidate_audit_projection import (
    CandidatePriceData,
    _build_canonical_candidate_tuple,
    candidate_availability_index,
)
from api.ptg2_candidate_audit_selection import (
    load_candidate_provider_indexes as _load_candidate_provider_indexes,
    required_candidate_occurrence_keys as _required_candidate_occurrence_keys,
)
from api.ptg2_serving import (
    PTG2_SCHEMA,
    _required_shared_snapshot_key,
    _required_source_count,
    _version_three_forward_lookup_hints,
    _version_three_price_hydration,
)
from api.ptg2_tables import snapshot_serving_tables
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
    AuditBatchRequest,
)
from process.ptg_parts.ptg2_candidate_audit_evidence import (
    canonical_network_name_digests,
    is_network_digest_subset_match,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


@dataclass(frozen=True)
class _CandidateAuditData:
    challenges: tuple[AuditBatchChallenge, ...]
    witness_io: Mapping[str, int]
    network_digest_sets_by_condition: Mapping[
        tuple[str, str, int, int, str],
        tuple[frozenset[str], ...],
    ]
    candidate_processing_io: Mapping[str, int]
    persisted_audit_occurrence_count: int = 0


@dataclass(frozen=True)
class CandidateAuditBatchResult:
    """Report aggregate matching counts without returning witness evidence."""

    matched_challenge_count: int
    unique_challenge_count: int
    witness_io: Mapping[str, int]
    candidate_processing_io: Mapping[str, int]
    persisted_audit_occurrence_count: int = 0
    validated_persisted_audit_occurrence_count: int = 0


@dataclass(frozen=True)
class _CandidateScopeIndexes:
    """Code, provider, and network indexes shared by both audit populations."""

    code_index: CandidateCodeIndex
    provider_sets_by_npi_code: Mapping[tuple[int, int], tuple[int, ...]]
    provider_filters_by_code_key: Mapping[int, tuple[int, ...]]
    network_digests_by_key: Mapping[int, frozenset[str]]
    preloaded_price_keys_by_occurrence: Mapping[
        tuple[int, int, int], tuple[int, ...]
    ] | None = None


async def _provider_set_keys_by_npi(
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence] = (),
    *,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> dict[int, tuple[int, ...]]:
    """Resolve candidate provider scopes through bounded graph reads."""

    return await provider_set_keys_by_npi(
        lookup_shared_graph_members_from_db,
        session,
        _required_shared_snapshot_key(serving_tables),
        PTG2_SCHEMA,
        challenges,
        persisted_audit_occurrences,
        retention_budget=retention_budget,
    )


async def _provider_network_names_by_key(
    session: Any,
    serving_tables: PTG2ServingTables,
    provider_set_keys: Iterable[int],
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> dict[int, tuple[str, ...]]:
    """Preserve the batch schema seam while using the bounded loader."""

    return await _network_names_by_key(
        session,
        serving_tables,
        provider_set_keys,
        retention_budget,
        schema_name=PTG2_SCHEMA,
    )


async def _candidate_forward_price_keys(
    session: Any,
    serving_tables: PTG2ServingTables,
    provider_filters_by_code_key: Mapping[int, tuple[int, ...]],
    required_occurrence_keys: frozenset[tuple[int, int, int]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> dict[tuple[int, int, int], tuple[int, ...]]:
    if not provider_filters_by_code_key:
        return {}
    return await lookup_forward_price_index_from_db(
        session,
        provider_filters_by_code_key,
        provider_set_keys_by_code=provider_filters_by_code_key,
        occurrence_keys=required_occurrence_keys,
        shared_snapshot_key=_required_shared_snapshot_key(serving_tables),
        source_count=_required_source_count(serving_tables),
        schema_name=PTG2_SCHEMA,
        retention_budget=retention_budget,
        **_version_three_forward_lookup_hints(serving_tables),
    )


async def _candidate_audit_data(
    session: Any,
    audit_request: AuditBatchRequest,
    access: PTG2CandidateAuditAccess,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> _CandidateAuditData:
    """Load the sealed V1 witness, then evaluate its exact coordinate scope."""

    serving_tables = await snapshot_serving_tables(
        session,
        audit_request.snapshot_id,
        candidate_audit_access=access,
    )
    witness_scope = await validate_candidate_source_scope(
        session,
        serving_tables,
        audit_request,
    )
    return await _candidate_data_for_conditions(
        session,
        serving_tables,
        access,
        challenges=witness_scope.challenges,
        persisted_audit_occurrences=(witness_scope.persisted_audit_occurrences),
        witness_io=witness_scope.ledger,
        retention_budget=retention_budget,
    )


async def _candidate_data_for_conditions(
    session: Any,
    serving_tables: PTG2ServingTables,
    access: PTG2CandidateAuditAccess,
    *,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    witness_io: Mapping[str, int],
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> _CandidateAuditData:
    """Evaluate one already validated, bounded coordinate partition."""

    scope_indexes = await _candidate_scope_indexes(
        session,
        serving_tables,
        access,
        challenges,
        persisted_audit_occurrences,
        retention_budget,
    )
    price_load = await _load_candidate_price_data(
        session,
        serving_tables,
        challenges,
        scope_indexes.code_index.by_pair,
        scope_indexes.provider_sets_by_npi_code,
        scope_indexes.provider_filters_by_code_key,
        persisted_audit_occurrences,
        preloaded_price_keys_by_occurrence=(
            scope_indexes.preloaded_price_keys_by_occurrence
        ),
        retention_budget=retention_budget,
    )
    validate_persisted_audit_price_scope(
        persisted_audit_occurrences,
        price_load.data,
    )
    availability_index, processing_ledger = candidate_availability_index(
        challenges,
        scope_indexes.code_index.by_pair,
        scope_indexes.provider_sets_by_npi_code,
        scope_indexes.network_digests_by_key,
        price_load.data,
        retention_budget=retention_budget,
    )
    return _CandidateAuditData(
        challenges=tuple(challenges),
        witness_io=dict(witness_io),
        network_digest_sets_by_condition=availability_index,
        candidate_processing_io=processing_ledger,
        persisted_audit_occurrence_count=len(persisted_audit_occurrences),
    )


async def _candidate_scope_indexes(
    session: Any,
    serving_tables: PTG2ServingTables,
    access: PTG2CandidateAuditAccess,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> _CandidateScopeIndexes:
    """Resolve one union of witness and persisted code/provider coordinates."""

    code_index = await candidate_code_records_by_pair(
        session,
        serving_tables,
        access,
        challenges,
        persisted_code_keys=(
            occurrence.code_key for occurrence in persisted_audit_occurrences
        ),
        retention_budget=retention_budget,
    )
    provider_scope = await load_candidate_provider_scope(
        _provider_set_keys_by_npi,
        session,
        serving_tables,
        challenges,
        persisted_audit_occurrences,
        code_index,
        schema_name=PTG2_SCHEMA,
        retention_budget=retention_budget,
    )
    provider_set_keys_by_npi = provider_scope.provider_set_keys_by_npi
    validate_persisted_audit_graph_scope(
        persisted_audit_occurrences,
        code_index,
        provider_set_keys_by_npi,
    )
    provider_sets_by_npi_code, provider_filters = await _load_candidate_provider_indexes(
        session,
        _required_shared_snapshot_key(serving_tables),
        challenges,
        code_index,
        provider_set_keys_by_npi,
        persisted_audit_occurrences,
        schema_name=PTG2_SCHEMA,
        retention_budget=retention_budget,
    )
    network_digests_by_key = await _provider_network_digests_by_key(
        session,
        serving_tables,
        provider_filters,
        retention_budget,
    )
    return _CandidateScopeIndexes(
        code_index=code_index,
        provider_sets_by_npi_code=provider_sets_by_npi_code,
        provider_filters_by_code_key=provider_filters,
        network_digests_by_key=network_digests_by_key,
        preloaded_price_keys_by_occurrence=provider_scope.price_keys_by_occurrence,
    )


async def _provider_network_digests_by_key(
    session: Any,
    serving_tables: PTG2ServingTables,
    provider_filters_by_code_key: Mapping[int, tuple[int, ...]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> dict[int, frozenset[str]]:
    return await _network_digests_by_key(
        session,
        serving_tables,
        provider_filters_by_code_key,
        retention_budget,
        _provider_network_names_by_key,
    )


async def _load_candidate_price_data(
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    code_records_by_pair: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
    *scope_arguments: Any,
    preloaded_price_keys_by_occurrence: (
        Mapping[tuple[int, int, int], tuple[int, ...]] | None
    ) = None,
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
) -> _CandidatePriceLoad:
    """Retain exact forward rows, then hydrate each retained price once."""

    return await load_candidate_price_data(
        session,
        serving_tables,
        challenges,
        code_records_by_pair,
        scope_arguments,
        preloaded_price_keys_by_occurrence,
        retention_budget,
        CandidatePriceLoaders(
            forward_price_keys=_candidate_forward_price_keys,
            hydrate_prices=_version_three_price_hydration,
        ),
    )


def _is_challenge_match(
    challenge: AuditBatchChallenge,
    audit_data: _CandidateAuditData,
) -> bool:
    condition_key = (
        challenge.code_system,
        challenge.code,
        challenge.npi,
        challenge.source_artifact_key,
        challenge.tuple_digest,
    )
    required_network_digests = frozenset(challenge.network_name_digests)
    return any(
        is_network_digest_subset_match(
            required_network_digests,
            candidate_network_digests,
        )
        for candidate_network_digests in (
            audit_data.network_digest_sets_by_condition.get(condition_key, ())
        )
    )


async def audit_candidate_source_witness_batch(
    session: Any,
    audit_request: AuditBatchRequest,
    access: PTG2CandidateAuditAccess,
) -> CandidateAuditBatchResult:
    """Match every grouped source condition through one read-only snapshot."""

    if not access.is_match(
        snapshot_id=audit_request.snapshot_id,
        source_key=audit_request.source_key,
        plan_id=audit_request.plan_id,
        plan_market_type=audit_request.plan_market_type,
    ):
        raise PTG2ManifestArtifactError("PTG2 candidate audit access mismatch")
    retention_budget = CandidateAuditDecodedRetentionBudget()
    bind_shared_block_decoded_retention_budget(retention_budget)
    await session.execute(
        text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    )
    audit_data = await _candidate_audit_data(
        session,
        audit_request,
        access,
        retention_budget,
    )
    unmatched_conditions = [
        challenge
        for challenge in audit_data.challenges
        if not _is_challenge_match(challenge, audit_data)
    ]
    if unmatched_conditions:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate source witness is missing from the sealed serving layout"
        )
    return CandidateAuditBatchResult(
        matched_challenge_count=sum(
            challenge.multiplicity for challenge in audit_data.challenges
        ),
        unique_challenge_count=len(audit_data.challenges),
        witness_io=audit_data.witness_io,
        candidate_processing_io=audit_data.candidate_processing_io,
        persisted_audit_occurrence_count=(audit_data.persisted_audit_occurrence_count),
        validated_persisted_audit_occurrence_count=(
            audit_data.persisted_audit_occurrence_count
        ),
    )


__all__ = [
    "CandidateAuditBatchResult",
    "CandidatePriceData",
    "_build_canonical_candidate_tuple",
    "audit_candidate_source_witness_batch",
]
