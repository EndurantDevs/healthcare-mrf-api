# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve one explicit bounded candidate-audit partition."""

from __future__ import annotations

from typing import Any, Mapping

from sqlalchemy import text

from api import ptg2_candidate_audit_batch as candidate_batch
from api.ptg2_candidate_audit import PTG2CandidateAuditAccess
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    fetch_snapshot_source_set_identity,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_partitioned_candidate_audit_contract import (
    PartitionedCandidateAuditRequest,
)


async def _validate_partition_binding(
    session: Any,
    serving_tables: PTG2ServingTables,
    audit_request: PartitionedCandidateAuditRequest,
) -> None:
    """Bind one explicit partition to immutable candidate metadata."""

    binding = audit_request.binding
    source_witness = serving_tables.source_witness
    audit_sample = serving_tables.audit_sample
    source_count = candidate_batch._required_source_count(serving_tables)
    if not isinstance(source_witness, Mapping) or not isinstance(
        audit_sample,
        Mapping,
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate partition requires sealed audit metadata"
        )
    if (
        int(source_witness.get("occurrence_witness_count") or -1)
        != binding.source_occurrence_count
        or str(source_witness.get("sample_digest") or "")
        != binding.source_witness_sample_digest
        or str(source_witness.get("payload_sha256") or "")
        != binding.source_witness_payload_sha256
        or int(audit_sample.get("sample_count") or -1)
        != binding.persisted_occurrence_count
        or str(audit_sample.get("sample_digest") or "")
        != binding.audit_sample_digest
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate partition disagrees with sealed audit metadata"
        )
    try:
        observed_source_set, observed_ordinal_digest, _raw_source_sha256 = (
            await fetch_snapshot_source_set_identity(
                session,
                schema_name=candidate_batch.PTG2_SCHEMA,
                logical_snapshot_id=binding.snapshot_id,
                expected_source_count=source_count,
            )
        )
    except PTG2SharedBlockError as exc:
        raise PTG2ManifestArtifactError(str(exc)) from exc
    has_invalid_source_key = any(
        partition_item.source_artifact_key >= source_count
        for partition_item in (
            *audit_request.source_challenges,
            *audit_request.persisted_occurrences,
        )
    )
    if (
        observed_source_set != serving_tables.source_set
        or observed_ordinal_digest != binding.ordered_source_ordinal_digest
        or has_invalid_source_key
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate partition source scope is invalid"
        )


def _partition_challenges(
    audit_request: PartitionedCandidateAuditRequest,
) -> tuple[AuditBatchChallenge, ...]:
    return tuple(
        AuditBatchChallenge(
            code_system=challenge.code_system,
            code=challenge.code,
            npi=challenge.npi,
            source_artifact_key=challenge.source_artifact_key,
            tuple_digest=challenge.tuple_digest,
            network_name_digests=challenge.network_name_digests,
            multiplicity=challenge.multiplicity,
        )
        for challenge in audit_request.source_challenges
    )


def _partition_persisted_occurrences(
    audit_request: PartitionedCandidateAuditRequest,
) -> tuple[PersistedAuditOccurrence, ...]:
    return tuple(
        PersistedAuditOccurrence(
            occurrence_id=occurrence.occurrence_id,
            code_key=occurrence.code_key,
            provider_set_key=occurrence.provider_set_key,
            price_key=occurrence.price_key,
            source_artifact_key=occurrence.source_artifact_key,
            npi=occurrence.npi,
            atom_ordinal=occurrence.atom_ordinal,
            atom_key=occurrence.atom_key,
        )
        for occurrence in audit_request.persisted_occurrences
    )


async def audit_candidate_partition(
    session: Any,
    audit_request: PartitionedCandidateAuditRequest,
    access: PTG2CandidateAuditAccess,
) -> candidate_batch.CandidateAuditBatchResult:
    """Evaluate only the exact coordinates assigned to one bounded request."""

    binding = audit_request.binding
    if not access.matches(
        snapshot_id=binding.snapshot_id,
        source_key=binding.source_key,
        plan_id=binding.plan_id,
        plan_market_type=binding.plan_market_type,
    ):
        raise PTG2ManifestArtifactError("PTG2 candidate audit access mismatch")
    await session.execute(
        text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    )
    serving_tables = await candidate_batch.snapshot_serving_tables(
        session,
        binding.snapshot_id,
        candidate_audit_access=access,
    )
    await _validate_partition_binding(session, serving_tables, audit_request)
    audit_data = await candidate_batch._candidate_data_for_conditions(
        session,
        serving_tables,
        access,
        challenges=_partition_challenges(audit_request),
        persisted_audit_occurrences=_partition_persisted_occurrences(
            audit_request
        ),
        witness_io={},
    )
    if any(
        not candidate_batch._is_challenge_match(challenge, audit_data)
        for challenge in audit_data.challenges
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate source witness is missing from the sealed serving layout"
        )
    return candidate_batch.CandidateAuditBatchResult(
        matched_challenge_count=sum(
            challenge.multiplicity for challenge in audit_data.challenges
        ),
        unique_challenge_count=len(audit_data.challenges),
        witness_io={},
        candidate_processing_io=audit_data.candidate_processing_io,
        persisted_audit_occurrence_count=(
            audit_data.persisted_audit_occurrence_count
        ),
        validated_persisted_audit_occurrence_count=(
            audit_data.persisted_audit_occurrence_count
        ),
    )


__all__ = ["audit_candidate_partition"]
