# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cache-free loading of sealed PTG candidate-audit plan coordinates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from api.code_systems import canonical_catalog_code, normalize_code_system
from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_audit import (
    PTG2_V3_AUDIT_MAX_SAMPLE_ROWS,
    persisted_audit_sample_digest,
)


_ROW_FIELDS = frozenset(
    {
        "occurrence_id",
        "code_key",
        "provider_set_key",
        "price_key",
        "source_key",
        "npi",
        "atom_ordinal",
        "atom_key",
        "reported_code_system",
        "reported_code",
    }
)
_COORDINATE_FIELDS = (
    "code_key",
    "provider_set_key",
    "price_key",
    "source_key",
    "npi",
    "atom_ordinal",
    "atom_key",
)


class CandidateAuditPlanStoreError(RuntimeError):
    """Raised when persisted audit-plan rows disagree with sealed metadata."""


@dataclass(frozen=True, slots=True)
class PersistedAuditSampleRecord:
    """One immutable, code-aware coordinate for a later audit partition."""

    occurrence_id: bytes
    code_key: int
    code_system: str
    code: str
    provider_set_key: int
    price_key: int
    source_artifact_key: int
    npi: int
    atom_ordinal: int
    atom_key: int

    def digest_mapping(self) -> Mapping[str, Any]:
        """Return the persisted field names covered by the sealed sample digest."""

        return {
            "occurrence_id": self.occurrence_id,
            "code_key": self.code_key,
            "provider_set_key": self.provider_set_key,
            "price_key": self.price_key,
            "source_key": self.source_artifact_key,
            "npi": self.npi,
            "atom_ordinal": self.atom_ordinal,
            "atom_key": self.atom_key,
        }


@dataclass(frozen=True, slots=True)
class PersistedAuditSample:
    """One fully validated sealed sample, held only for the current audit run."""

    snapshot_key: int
    sample_digest: str
    records: tuple[PersistedAuditSampleRecord, ...]

    @property
    def sample_count(self) -> int:
        """Return the number of sealed persisted coordinates."""

        return len(self.records)


def _strict_nonnegative_integer(raw_value: Any, *, field_name: str) -> int:
    if type(raw_value) is not int or raw_value < 0:
        raise CandidateAuditPlanStoreError(
            f"persisted audit sample {field_name} is invalid"
        )
    return raw_value


def _expected_sample_contract(
    expected_metadata: Mapping[str, Any],
) -> tuple[int, str, int]:
    if not isinstance(expected_metadata, Mapping):
        raise CandidateAuditPlanStoreError(
            "persisted audit sample metadata is invalid"
        )
    sample_count = _strict_nonnegative_integer(
        expected_metadata.get("sample_count"),
        field_name="count",
    )
    if sample_count < 1 or sample_count > PTG2_V3_AUDIT_MAX_SAMPLE_ROWS:
        raise CandidateAuditPlanStoreError(
            "persisted audit sample count exceeds its sealed maximum"
        )
    source_count = _strict_nonnegative_integer(
        expected_metadata.get("source_count"),
        field_name="source count",
    )
    if source_count < 1:
        raise CandidateAuditPlanStoreError(
            "persisted audit sample source count is invalid"
        )
    sample_digest = expected_metadata.get("sample_digest")
    if (
        not isinstance(sample_digest, str)
        or len(sample_digest) != 64
        or sample_digest != sample_digest.lower()
        or any(character not in "0123456789abcdef" for character in sample_digest)
    ):
        raise CandidateAuditPlanStoreError(
            "persisted audit sample digest metadata is invalid"
        )
    return sample_count, sample_digest, source_count


def _row_mapping(database_row: Any) -> dict[str, Any]:
    raw_mapping = getattr(database_row, "_mapping", database_row)
    if not isinstance(raw_mapping, Mapping):
        raise CandidateAuditPlanStoreError(
            "persisted audit sample row is invalid"
        )
    row_mapping = dict(raw_mapping)
    if set(row_mapping) != _ROW_FIELDS:
        raise CandidateAuditPlanStoreError(
            "persisted audit sample row fields are invalid"
        )
    return row_mapping


def _code_identity(database_row: Mapping[str, Any]) -> tuple[str, str]:
    raw_code_system = database_row.get("reported_code_system")
    raw_code = database_row.get("reported_code")
    if not isinstance(raw_code_system, str) or not isinstance(raw_code, str):
        raise CandidateAuditPlanStoreError(
            "persisted audit sample code identity is absent or ambiguous"
        )
    code_system = normalize_code_system(raw_code_system)
    code = canonical_catalog_code(code_system, raw_code)
    if not code_system or not code:
        raise CandidateAuditPlanStoreError(
            "persisted audit sample code identity is absent or ambiguous"
        )
    return code_system, code


def _validated_record(
    database_row: Mapping[str, Any],
    *,
    source_count: int,
) -> PersistedAuditSampleRecord:
    occurrence_id = database_row.get("occurrence_id")
    if not isinstance(occurrence_id, (bytes, bytearray, memoryview)) or len(
        occurrence_id
    ) != 32:
        raise CandidateAuditPlanStoreError(
            "persisted audit sample occurrence id is invalid"
        )
    integer_by_field = {
        field_name: _strict_nonnegative_integer(
            database_row.get(field_name),
            field_name="coordinate",
        )
        for field_name in _COORDINATE_FIELDS
    }
    source_artifact_key = integer_by_field["source_key"]
    if source_artifact_key >= source_count:
        raise CandidateAuditPlanStoreError(
            "persisted audit sample source key is outside the sealed source set"
        )
    npi = integer_by_field["npi"]
    if not 1_000_000_000 <= npi <= 9_999_999_999:
        raise CandidateAuditPlanStoreError(
            "persisted audit sample NPI is invalid"
        )
    code_system, code = _code_identity(database_row)
    return PersistedAuditSampleRecord(
        occurrence_id=bytes(occurrence_id),
        code_key=integer_by_field["code_key"],
        code_system=code_system,
        code=code,
        provider_set_key=integer_by_field["provider_set_key"],
        price_key=integer_by_field["price_key"],
        source_artifact_key=source_artifact_key,
        npi=npi,
        atom_ordinal=integer_by_field["atom_ordinal"],
        atom_key=integer_by_field["atom_key"],
    )


def _validated_sample_records(
    database_rows: Sequence[Any],
    *,
    source_count: int,
    expected_count: int,
    expected_digest: str,
) -> tuple[PersistedAuditSampleRecord, ...]:
    sample_records = tuple(
        _validated_record(
            _row_mapping(database_row),
            source_count=source_count,
        )
        for database_row in database_rows
    )
    occurrence_ids = tuple(
        sample_record.occurrence_id for sample_record in sample_records
    )
    if len(occurrence_ids) != len(set(occurrence_ids)):
        raise CandidateAuditPlanStoreError(
            "persisted audit sample contains duplicate or ambiguous occurrences"
        )
    if len(sample_records) != expected_count:
        raise CandidateAuditPlanStoreError(
            "persisted audit sample row count disagrees with sealed metadata"
        )
    observed_digest = persisted_audit_sample_digest(
        tuple(
            sample_record.digest_mapping()
            for sample_record in sample_records
        )
    )
    if observed_digest != expected_digest:
        raise CandidateAuditPlanStoreError(
            "persisted audit sample digest disagrees with sealed metadata"
        )
    return sample_records


async def load_persisted_audit_sample(
    *,
    schema_name: str,
    snapshot_key: int,
    expected_metadata: Mapping[str, Any],
) -> PersistedAuditSample:
    """Read a sealed sample once and fail closed on any contract drift."""

    if not isinstance(schema_name, str) or not schema_name:
        raise CandidateAuditPlanStoreError(
            "persisted audit sample schema name is invalid"
        )
    validated_snapshot_key = _strict_nonnegative_integer(
        snapshot_key,
        field_name="snapshot key",
    )
    if validated_snapshot_key < 1:
        raise CandidateAuditPlanStoreError(
            "persisted audit sample snapshot key is invalid"
        )
    expected_count, expected_digest, source_count = _expected_sample_contract(
        expected_metadata
    )
    schema = _quote_ident(schema_name)
    database_rows = await db.all(
        f"""
        SELECT audit.occurrence_id, audit.code_key, audit.provider_set_key,
               audit.price_key, audit.source_key, audit.npi,
               audit.atom_ordinal, audit.atom_key,
               code.reported_code_system, code.reported_code
          FROM {schema}.ptg2_v3_audit_occurrence audit
          LEFT JOIN {schema}.ptg2_v3_code code
            ON code.snapshot_key = audit.snapshot_key
           AND code.code_key = audit.code_key
         WHERE audit.snapshot_key = :snapshot_key
         ORDER BY audit.occurrence_id
         LIMIT :row_limit
        """,
        snapshot_key=validated_snapshot_key,
        row_limit=expected_count + 1,
    )
    sample_records = _validated_sample_records(
        database_rows,
        source_count=source_count,
        expected_count=expected_count,
        expected_digest=expected_digest,
    )
    return PersistedAuditSample(
        snapshot_key=validated_snapshot_key,
        sample_digest=expected_digest,
        records=sample_records,
    )


__all__ = [
    "CandidateAuditPlanStoreError",
    "PersistedAuditSample",
    "PersistedAuditSampleRecord",
    "load_persisted_audit_sample",
]
