# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Set-based exhaustive source-witness verification for one PTG candidate."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text

from api.ptg2_candidate_audit import PTG2CandidateAuditAccess
from api.ptg2_candidate_audit_codes import (
    CandidateCodeIndex,
    candidate_code_records_by_pair,
)
from api.ptg2_candidate_audit_coordinates import (
    validate_persisted_audit_graph_scope,
    validate_persisted_audit_price_scope,
)
from api.ptg2_candidate_audit_integrity import (
    PersistedAuditOccurrence,
    validate_candidate_source_scope,
)
from api.ptg2_db_sidecars import (
    lookup_forward_price_index_from_db,
    lookup_shared_graph_members_from_db,
)
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
from api.ptg2_shared_blocks import (
    PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
    PTG2_V3_GRAPH_NPI_TO_GROUP,
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
from scripts.validation import ptg2_v3_source_api_audit as source_audit


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
class _CandidatePriceLoad:
    """Candidate prices plus proof that only exact forward rows were retained."""

    data: CandidatePriceData
    selection_io: Mapping[str, int]


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


def _record_fields(database_record: Any) -> dict[str, Any]:
    record_mapping = getattr(database_record, "_mapping", None)
    if record_mapping is not None:
        return dict(record_mapping)
    if isinstance(database_record, Mapping):
        return dict(database_record)
    return dict(database_record or {})


async def _provider_set_keys_by_npi(
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence] = (),
) -> dict[int, tuple[int, ...]]:
    shared_snapshot_key = _required_shared_snapshot_key(serving_tables)
    npis = tuple(
        sorted(
            {challenge.npi for challenge in challenges}
            | {occurrence.npi for occurrence in persisted_audit_occurrences}
        )
    )
    group_keys_by_npi = await lookup_shared_graph_members_from_db(
        session,
        shared_snapshot_key,
        PTG2_V3_GRAPH_NPI_TO_GROUP,
        npis,
        schema_name=PTG2_SCHEMA,
    )
    group_keys = tuple(
        sorted(
            {
                group_key
                for npi_group_keys in group_keys_by_npi.values()
                for group_key in npi_group_keys
            }
        )
    )
    provider_keys_by_group = await lookup_shared_graph_members_from_db(
        session,
        shared_snapshot_key,
        PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
        group_keys,
        schema_name=PTG2_SCHEMA,
    )
    return {
        npi: tuple(
            sorted(
                {
                    provider_set_key
                    for group_key in group_keys_by_npi.get(npi, ())
                    for provider_set_key in provider_keys_by_group.get(group_key, ())
                }
            )
        )
        for npi in npis
    }


async def _provider_network_names_by_key(
    session: Any,
    serving_tables: PTG2ServingTables,
    provider_set_keys: Iterable[int],
) -> dict[int, tuple[str, ...]]:
    requested_keys = tuple(sorted(set(provider_set_keys)))
    if not requested_keys:
        return {}
    network_query = await session.execute(
        text(
            f"""
            SELECT provider_set_key, network_names
              FROM {PTG2_SCHEMA}.ptg2_v3_provider_set
             WHERE snapshot_key = :shared_snapshot_key
               AND provider_set_key = ANY(CAST(:provider_set_keys AS integer[]))
             ORDER BY provider_set_key
            """
        ),
        {
            "shared_snapshot_key": _required_shared_snapshot_key(serving_tables),
            "provider_set_keys": requested_keys,
        },
    )
    network_names_by_key = {}
    for raw_network_record in network_query:
        network_record = _record_fields(raw_network_record)
        network_names_by_key[int(network_record["provider_set_key"])] = tuple(
            source_audit.canonical_list(network_record["network_names"])
        )
    if set(network_names_by_key) != set(requested_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate provider-set network metadata is incomplete"
        )
    return network_names_by_key


async def _candidate_forward_price_keys(
    session: Any,
    serving_tables: PTG2ServingTables,
    provider_filters_by_code_key: Mapping[int, tuple[int, ...]],
    required_occurrence_keys: frozenset[tuple[int, int, int]],
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
        **_version_three_forward_lookup_hints(serving_tables),
    )


async def _candidate_audit_data(
    session: Any,
    audit_request: AuditBatchRequest,
    access: PTG2CandidateAuditAccess,
) -> _CandidateAuditData:
    """Load each candidate coordinate family once into bounded indexes."""

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
    challenges = witness_scope.challenges
    persisted_audit_occurrences = witness_scope.persisted_audit_occurrences
    scope_indexes = await _candidate_scope_indexes(
        session,
        serving_tables,
        access,
        challenges,
        persisted_audit_occurrences,
    )
    price_load = await _load_candidate_price_data(
        session,
        serving_tables,
        challenges,
        scope_indexes.code_index.by_pair,
        scope_indexes.provider_sets_by_npi_code,
        scope_indexes.provider_filters_by_code_key,
        persisted_audit_occurrences,
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
    )
    return _CandidateAuditData(
        challenges=challenges,
        witness_io=witness_scope.ledger,
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
) -> _CandidateScopeIndexes:
    """Resolve one union of witness and persisted code/provider coordinates."""

    code_index = await candidate_code_records_by_pair(
        session,
        serving_tables,
        access,
        challenges,
        persisted_code_keys=(
            occurrence.code_key
            for occurrence in persisted_audit_occurrences
        ),
    )
    provider_set_keys_by_npi = await _provider_set_keys_by_npi(
        session,
        serving_tables,
        challenges,
        persisted_audit_occurrences,
    )
    validate_persisted_audit_graph_scope(
        persisted_audit_occurrences,
        code_index,
        provider_set_keys_by_npi,
    )
    provider_sets_by_npi_code, provider_filters = (
        await _load_candidate_provider_indexes(
            session,
            _required_shared_snapshot_key(serving_tables),
            challenges,
            code_index,
            provider_set_keys_by_npi,
            persisted_audit_occurrences,
            schema_name=PTG2_SCHEMA,
        )
    )
    network_digests_by_key = await _provider_network_digests_by_key(
        session,
        serving_tables,
        provider_filters,
    )
    return _CandidateScopeIndexes(
        code_index=code_index,
        provider_sets_by_npi_code=provider_sets_by_npi_code,
        provider_filters_by_code_key=provider_filters,
        network_digests_by_key=network_digests_by_key,
    )
async def _provider_network_digests_by_key(
    session: Any,
    serving_tables: PTG2ServingTables,
    provider_filters_by_code_key: Mapping[int, tuple[int, ...]],
) -> dict[int, frozenset[str]]:
    provider_set_keys = (
        provider_key
        for provider_keys in provider_filters_by_code_key.values()
        for provider_key in provider_keys
    )
    network_names_by_key = await _provider_network_names_by_key(
        session,
        serving_tables,
        provider_set_keys,
    )
    return {
        provider_set_key: frozenset(canonical_network_name_digests(network_names))
        for provider_set_key, network_names in network_names_by_key.items()
    }


async def _load_candidate_price_data(
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    code_records_by_pair: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
    provider_sets_by_npi_code: Mapping[tuple[int, int], tuple[int, ...]],
    provider_filters_by_code_key: Mapping[int, tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence] = (),
) -> _CandidatePriceLoad:
    """Retain exact forward rows, then hydrate each retained price once."""

    required_occurrence_keys = _required_candidate_occurrence_keys(
        challenges,
        code_records_by_pair,
        provider_sets_by_npi_code,
        persisted_audit_occurrences,
    )
    price_keys_by_occurrence = await _candidate_forward_price_keys(
        session,
        serving_tables,
        provider_filters_by_code_key,
        required_occurrence_keys,
    )
    if set(price_keys_by_occurrence).difference(required_occurrence_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate forward read escaped its exact occurrence scope"
        )
    retained_price_keys = {
        price_key
        for occurrence_price_keys in price_keys_by_occurrence.values()
        for price_key in occurrence_price_keys
    }
    hydration = await _version_three_price_hydration(
        session,
        serving_tables,
        retained_price_keys,
        copy_payloads=False,
    )
    return _CandidatePriceLoad(
        data=CandidatePriceData(
            price_keys_by_occurrence,
            hydration.atom_keys_by_price_key,
            hydration.prices_by_key,
        ),
        selection_io={
            "exact_candidate_occurrence_coordinates": len(
                required_occurrence_keys
            ),
            "exact_forward_occurrence_coordinates_returned": len(
                price_keys_by_occurrence
            ),
            "exact_forward_price_key_deliveries_returned": sum(
                len(price_keys)
                for price_keys in price_keys_by_occurrence.values()
            ),
        },
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

    if not access.matches(
        snapshot_id=audit_request.snapshot_id,
        source_key=audit_request.source_key,
        plan_id=audit_request.plan_id,
        plan_market_type=audit_request.plan_market_type,
    ):
        raise PTG2ManifestArtifactError("PTG2 candidate audit access mismatch")
    await session.execute(
        text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    )
    audit_data = await _candidate_audit_data(session, audit_request, access)
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
        persisted_audit_occurrence_count=(
            audit_data.persisted_audit_occurrence_count
        ),
        validated_persisted_audit_occurrence_count=(
            audit_data.persisted_audit_occurrence_count
        ),
    )


__all__ = ["CandidateAuditBatchResult", "audit_candidate_source_witness_batch"]
