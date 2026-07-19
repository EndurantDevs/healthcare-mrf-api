# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate candidate source ordinals and the sealed persisted audit sample."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from api.ptg2_serving import (
    PTG2_SCHEMA,
    _required_shared_snapshot_key,
    _required_source_count,
)
from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    fetch_snapshot_source_set_identity,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
    AuditBatchRequest,
    group_audit_batch_challenges,
)
from process.ptg_parts.ptg2_candidate_audit_evidence import (
    source_audit_condition,
    validate_provider_witness,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_shared_audit import persisted_audit_sample_digest
from process.ptg_parts.ptg2_source_witness import decode_persisted_source_witness


@dataclass(frozen=True)
class CandidateWitnessScope:
    """Carry server-derived conditions and proof of one-pass witness decoding."""

    challenges: tuple[AuditBatchChallenge, ...]
    record_count: int
    unique_evidence_count: int
    evidence_reference_count: int
    persisted_audit_occurrences: tuple["PersistedAuditOccurrence", ...] = ()

    @property
    def ledger(self) -> dict[str, int]:
        """Return aggregate counters without exposing witness evidence."""

        return {
            "payload_reads": 1,
            "payload_decodes": 1,
            "record_decodes": self.record_count,
            "unique_evidence_entries": self.unique_evidence_count,
            "evidence_decompressions": self.unique_evidence_count,
            "evidence_sha256_hashes": self.unique_evidence_count,
            "evidence_json_parses": self.unique_evidence_count,
            "evidence_reuse_deliveries": (
                self.evidence_reference_count - self.unique_evidence_count
            ),
            "repeated_evidence_decompressions": 0,
            "repeated_evidence_sha256_hashes": 0,
            "repeated_evidence_json_parses": 0,
        }


@dataclass(frozen=True)
class PersistedAuditOccurrence:
    """One sealed audit coordinate retained for set-based serving validation."""

    occurrence_id: bytes
    code_key: int
    provider_set_key: int
    price_key: int
    source_artifact_key: int
    npi: int
    atom_ordinal: int
    atom_key: int


def _record_fields(database_record: Any) -> dict[str, Any]:
    record_mapping = getattr(database_record, "_mapping", None)
    if record_mapping is not None:
        return dict(record_mapping)
    if isinstance(database_record, Mapping):
        return dict(database_record)
    return dict(database_record or {})


async def validate_candidate_source_scope(
    session: Any,
    serving_tables: PTG2ServingTables,
    audit_request: AuditBatchRequest,
) -> CandidateWitnessScope:
    """Derive exact grouped challenges from one sealed witness payload read."""

    source_count = _validated_request_source_count(serving_tables, audit_request)
    persisted_audit_occurrences = await validate_persisted_audit_sample(
        session,
        serving_tables,
    )
    try:
        (
            observed_source_set,
            observed_source_ordinal_digest,
            raw_source_sha256,
        ) = (
            await fetch_snapshot_source_set_identity(
                session,
                schema_name=PTG2_SCHEMA,
                logical_snapshot_id=audit_request.snapshot_id,
                expected_source_count=source_count,
            )
        )
    except PTG2SharedBlockError as exc:
        raise PTG2ManifestArtifactError(str(exc)) from exc
    if observed_source_set != serving_tables.source_set:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate source rows disagree with the sealed source set"
        )
    if observed_source_ordinal_digest != audit_request.ordered_source_ordinal_digest:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate source ordinals disagree with the witness request"
        )
    witness_scope = await _sealed_witness_challenges(
        session,
        serving_tables,
        raw_source_sha256,
    )
    if (
        sum(challenge.multiplicity for challenge in witness_scope.challenges)
        != audit_request.challenge_count
        or any(
            not 0 <= challenge.source_artifact_key < source_count
            for challenge in witness_scope.challenges
        )
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate source witness challenge scope is invalid"
        )
    return CandidateWitnessScope(
        challenges=witness_scope.challenges,
        record_count=witness_scope.record_count,
        unique_evidence_count=witness_scope.unique_evidence_count,
        evidence_reference_count=witness_scope.evidence_reference_count,
        persisted_audit_occurrences=persisted_audit_occurrences,
    )


def _validated_request_source_count(
    serving_tables: PTG2ServingTables,
    audit_request: AuditBatchRequest,
) -> int:
    """Bind request digests and counts to the sealed candidate descriptor."""

    source_count = _required_source_count(serving_tables)
    audit_sample = serving_tables.audit_sample
    if not isinstance(audit_sample, dict):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate audit requires a sealed persisted audit sample"
        )
    source_witness = serving_tables.source_witness
    if not isinstance(source_witness, dict):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate audit requires a sealed source witness"
        )
    if str(audit_sample.get("sample_digest") or "") != (
        audit_request.audit_sample_digest
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate witness request disagrees with the sealed audit sample"
        )
    if (
        int(source_witness.get("occurrence_witness_count") or -1)
        != audit_request.challenge_count
        or str(source_witness.get("sample_digest") or "")
        != audit_request.source_witness_sample_digest
        or str(source_witness.get("payload_sha256") or "")
        != audit_request.source_witness_payload_sha256
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate request disagrees with the sealed source witness"
        )
    return source_count


async def _sealed_witness_challenges(
    session: Any,
    serving_tables: PTG2ServingTables,
    raw_source_sha256: tuple[str, ...],
) -> CandidateWitnessScope:
    """Read, decode, validate, and group each sealed witness record once."""

    payload_result = await session.execute(
        text(
            f"""
            SELECT payload
              FROM {PTG2_SCHEMA}.ptg2_v3_source_audit_witness
             WHERE snapshot_key = :shared_snapshot_key
            """
        ),
        {"shared_snapshot_key": _required_shared_snapshot_key(serving_tables)},
    )
    if hasattr(payload_result, "one_or_none"):
        payload_record = payload_result.one_or_none()
    else:
        payload_rows = list(payload_result)
        payload_record = payload_rows[0] if len(payload_rows) == 1 else None
    payload_fields = _record_fields(payload_record) if payload_record is not None else {}
    source_witness = serving_tables.source_witness
    try:
        loaded_witness = decode_persisted_source_witness(
            bytes(payload_fields.get("payload") or b""),
            expected_raw_source_sha256=raw_source_sha256,
            expected_metadata=source_witness,
        )
        for provider_record in loaded_witness.provider_records:
            validate_provider_witness(
                provider_record,
                parsed_evidence_by_sha256=loaded_witness.evidence_by_sha256,
            )
        source_conditions = tuple(
            source_audit_condition(
                occurrence_record,
                parsed_evidence_by_sha256=loaded_witness.evidence_by_sha256,
            )
            for occurrence_record in loaded_witness.occurrence_records
        )
        evidence_reference_count = sum(
            1 + int(witness_record.linked_provider_sha256 is not None)
            for witness_record in loaded_witness.records
        )
        return CandidateWitnessScope(
            challenges=group_audit_batch_challenges(
                raw_source_sha256,
                source_conditions,
            ),
            record_count=len(loaded_witness.records),
            unique_evidence_count=len(loaded_witness.evidence_by_sha256),
            evidence_reference_count=evidence_reference_count,
        )
    except (RuntimeError, ValueError) as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate persisted source witness is invalid"
        ) from exc


async def validate_persisted_audit_sample(
    session: Any,
    serving_tables: PTG2ServingTables,
) -> tuple[PersistedAuditOccurrence, ...]:
    """Read and validate every persisted sample coordinate exactly once."""

    audit_sample = serving_tables.audit_sample
    if not isinstance(audit_sample, dict):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate audit requires a sealed persisted audit sample"
        )
    sealed_sample_count = int(audit_sample.get("sample_count") or -1)
    if sealed_sample_count < 0:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate audit sample count is invalid"
        )
    digest_result = await session.execute(
        text(
            f"""
            SELECT occurrence_id, code_key, provider_set_key, price_key,
                   source_key, npi, atom_ordinal, atom_key
              FROM {PTG2_SCHEMA}.ptg2_v3_audit_occurrence
             WHERE snapshot_key = :shared_snapshot_key
             ORDER BY occurrence_id
            """
        ),
        {"shared_snapshot_key": _required_shared_snapshot_key(serving_tables)},
    )
    persisted_rows = [_record_fields(database_row) for database_row in digest_result]
    if len(persisted_rows) != sealed_sample_count:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate persisted audit rows disagree with the sealed sample count"
        )
    source_count = _required_source_count(serving_tables)
    validated_occurrences = tuple(
        _persisted_audit_occurrence(database_row, source_count)
        for database_row in persisted_rows
    )
    if (
        len(
            {
                validated_occurrence.occurrence_id
                for validated_occurrence in validated_occurrences
            }
        )
        != sealed_sample_count
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate persisted audit rows contain duplicate occurrence ids"
        )
    if persisted_audit_sample_digest(persisted_rows) != str(
        audit_sample.get("sample_digest") or ""
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate persisted audit rows disagree with the sealed sample digest"
        )
    return validated_occurrences


def _persisted_audit_occurrence(
    database_row: Mapping[str, Any],
    source_count: int,
) -> PersistedAuditOccurrence:
    """Validate and freeze one row from the already ordered sample query."""

    occurrence_id = database_row.get("occurrence_id")
    if not isinstance(occurrence_id, (bytes, bytearray, memoryview)) or len(
        occurrence_id
    ) != 32:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate persisted audit occurrence id is invalid"
        )
    integer_by_field = {
        field_name: database_row.get(field_name)
        for field_name in (
            "code_key",
            "provider_set_key",
            "price_key",
            "source_key",
            "npi",
            "atom_ordinal",
            "atom_key",
        )
    }
    if any(
        isinstance(raw_value, bool)
        or not isinstance(raw_value, int)
        or raw_value < 0
        for raw_value in integer_by_field.values()
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate persisted audit coordinate is invalid"
        )
    source_artifact_key = int(integer_by_field["source_key"])
    npi = int(integer_by_field["npi"])
    if source_artifact_key >= source_count:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate persisted audit source key is invalid"
        )
    if not 1_000_000_000 <= npi <= 9_999_999_999:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate persisted audit NPI is invalid"
        )
    return PersistedAuditOccurrence(
        occurrence_id=bytes(occurrence_id),
        code_key=int(integer_by_field["code_key"]),
        provider_set_key=int(integer_by_field["provider_set_key"]),
        price_key=int(integer_by_field["price_key"]),
        source_artifact_key=source_artifact_key,
        npi=npi,
        atom_ordinal=int(integer_by_field["atom_ordinal"]),
        atom_key=int(integer_by_field["atom_key"]),
    )


__all__ = [
    "CandidateWitnessScope",
    "PersistedAuditOccurrence",
    "validate_candidate_source_scope",
    "validate_persisted_audit_sample",
]
