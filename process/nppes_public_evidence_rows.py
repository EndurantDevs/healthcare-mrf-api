# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact asyncpg row codecs for NPPES public-evidence admission."""

from __future__ import annotations

from datetime import UTC, date, datetime

from public_evidence.evidence_record_primitives import EvidenceSourceRecordReference
from public_evidence.nppes_registry_candidate_encoder import (
    NppesRegistryPersistenceRows,
)
from public_evidence.nppes_registry_storage_contract import (
    NppesRegistryAdmissionRow,
    NppesRegistryMemberRow,
)
from public_evidence.source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
    validate_public_evidence_source_release,
)


SOURCE_IDENTITY_COLUMNS = (
    "identity_ref",
    "identity_kind",
    "content_identity_kind",
    "content_sha256",
)
SOURCE_RELEASE_COLUMNS = (
    "source_release_ref",
    "contract_sha256",
    "contract",
    "foundation_scope",
    "source_kind",
    "authority_classification",
    "trust_classification",
    "semantic_limits",
    "artifact_identity_ref",
    "artifact_identity_kind",
    "artifact_content_identity_kind",
    "artifact_content_sha256",
    "completeness_mode",
    "completeness_evidence_contract_id",
    "completeness_count_unit",
    "completeness_subject_sha256",
    "expected_record_count",
    "observed_record_count",
    "evidence_root_sha256",
    "rights_classification",
    "rights_proof_sha256",
    "source_binding_contract_id",
    "source_artifact_source_type",
    "source_artifact_identity_kind",
    "source_artifact_sha256",
    "source_binding_sha256",
    "shadow_bundle_binding_sha256",
    "observed_start_at",
    "observed_end_at",
    "effective_start_at",
    "effective_end_at",
    "import_run_ref",
    "lifecycle_state",
    "serving_authority",
    "current_pointer_authority",
)
SOURCE_RECORD_COLUMNS = (
    "source_record_ref",
    "source_release_ref",
    "source_release_contract_sha256",
    "source_kind",
    "record_kind",
    "identity_contract_id",
    "record_hmac_sha256",
    "payload_sha256",
    "nppes_admission_ref",
)
MEMBER_COLUMNS = tuple(NppesRegistryMemberRow._fields)
COMMON_COLUMNS = (
    "evidence_ref",
    "record_contract",
    "record_contract_sha256",
    "foundation_scope",
    "source_release_ref",
    "source_release_contract_sha256",
    "source_kind",
    "observed_at",
    "effective_start_at",
    "effective_end_at",
    "record_type",
    "relationship_class",
    "source_record_count",
    "source_link_ordering_contract_id",
    "source_link_vector_sha256",
    "typed_row_sha256",
    "authority_state_sha256",
    "lifecycle_state",
    "positive_evidence_only",
    "serving_authority",
    "current_pointer_authority",
    "database_io_authority",
    "publication_enabled",
    "row_sha256",
    "nppes_admission_ref",
)
SOURCE_LINK_COLUMNS = (
    "evidence_ref",
    "source_release_ref",
    "source_release_contract_sha256",
    "source_kind",
    "source_record_ordinal",
    "source_record_ref",
    "record_kind",
    "row_sha256",
    "nppes_admission_ref",
)
NPI_ENUMERATION_COLUMNS = (
    "evidence_ref",
    "source_release_ref",
    "source_release_contract_sha256",
    "source_kind",
    "record_type",
    "relationship_class",
    "npi",
    "npi_entity_type",
    "enumeration_state",
    "row_sha256",
    "nppes_admission_ref",
)
ADMISSION_COLUMNS = tuple(NppesRegistryAdmissionRow._fields)


_DIGEST_FIELDS = frozenset(
    {
        "contract_sha256",
        "source_release_contract_sha256",
        "content_sha256",
        "artifact_sha256",
        "artifact_content_sha256",
        "completeness_subject_sha256",
        "evidence_root_sha256",
        "rights_proof_sha256",
        "source_artifact_sha256",
        "source_binding_sha256",
        "shadow_bundle_binding_sha256",
        "record_hmac_sha256",
        "payload_sha256",
        "leaf_sha256",
        "row_sha256",
        "record_contract_sha256",
        "source_link_vector_sha256",
        "typed_row_sha256",
        "authority_state_sha256",
        "zip_member_census_sha256",
        "header_sha256",
        "manifest_sha256",
    }
)
_TIMESTAMP_FIELDS = frozenset(
    {
        "observed_start_at",
        "observed_end_at",
        "effective_start_at",
        "effective_end_at",
        "observed_at",
        "minimum_effective_start_at",
        "snapshot_at",
    }
)
_DATE_FIELDS = frozenset(
    {
        "provider_enumeration_date",
        "last_update_date",
        "npi_deactivation_date",
        "npi_reactivation_date",
    }
)


def _timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def _database_value(field_name: str, value: object) -> object:
    if field_name in _DIGEST_FIELDS and value is not None:
        return bytes.fromhex(value)
    if field_name in _TIMESTAMP_FIELDS:
        return _timestamp(value)
    if field_name in _DATE_FIELDS:
        return date.fromisoformat(value) if value is not None else None
    return value


def _row_values(columns: tuple[str, ...], values: object) -> tuple[object, ...]:
    return tuple(
        _database_value(field_name, getattr(values, field_name))
        for field_name in columns
    )


def _row_values_with_admission(
    columns: tuple[str, ...],
    values: object,
    admission_ref: str,
) -> tuple[object, ...]:
    return _row_values(columns[:-1], values) + (admission_ref,)


def source_identity_values(
    release: object,
) -> tuple[object, ...]:
    """Encode the exact immutable artifact owner row."""

    fixed = validate_public_evidence_source_release(release)
    artifact = fixed.artifact_identity
    return (
        artifact.identity_ref,
        artifact.identity_kind,
        artifact.content_identity_kind,
        bytes.fromhex(artifact.content_sha256),
    )


def _release_values_by_column(
    fixed: PublicEvidenceSourceReleaseDescriptor,
) -> dict[str, object]:
    artifact = fixed.artifact_identity
    attestation = fixed.completeness_attestation
    binding = fixed.source_binding
    return {
        "source_release_ref": fixed.source_release_ref,
        "contract_sha256": fixed.contract_sha256,
        "contract": fixed.contract,
        "foundation_scope": fixed.foundation_scope,
        "source_kind": fixed.source_kind,
        "authority_classification": fixed.authority_classification,
        "trust_classification": fixed.trust_classification,
        "semantic_limits": list(fixed.semantic_limits),
        "artifact_identity_ref": artifact.identity_ref,
        "artifact_identity_kind": artifact.identity_kind,
        "artifact_content_identity_kind": artifact.content_identity_kind,
        "artifact_content_sha256": artifact.content_sha256,
        "completeness_mode": attestation.mode,
        "completeness_evidence_contract_id": attestation.evidence_contract_id,
        "completeness_count_unit": attestation.count_unit,
        "completeness_subject_sha256": attestation.subject_sha256,
        "expected_record_count": attestation.expected_record_count,
        "observed_record_count": attestation.observed_record_count,
        "evidence_root_sha256": attestation.evidence_root_sha256,
        "rights_classification": fixed.rights_classification,
        "rights_proof_sha256": fixed.rights_proof_sha256,
        "source_binding_contract_id": None if binding is None else binding.contract_id,
        "source_artifact_source_type": (
            None if binding is None else binding.source_artifact_source_type
        ),
        "source_artifact_identity_kind": (
            None if binding is None else binding.source_artifact_identity_kind
        ),
        "source_artifact_sha256": (
            None if binding is None else binding.source_artifact_sha256
        ),
        "source_binding_sha256": (
            None if binding is None else binding.source_binding_sha256
        ),
        "shadow_bundle_binding_sha256": (
            None if binding is None else binding.shadow_bundle_binding_sha256
        ),
        "observed_start_at": fixed.observed_interval.start_at,
        "observed_end_at": fixed.observed_interval.end_at,
        "effective_start_at": fixed.effective_interval.start_at,
        "effective_end_at": fixed.effective_interval.end_at,
        "import_run_ref": fixed.import_run_ref,
        "lifecycle_state": fixed.lifecycle_state,
        "serving_authority": fixed.serving_authority,
        "current_pointer_authority": fixed.current_pointer_authority,
    }


def source_release_values(
    release: object,
) -> tuple[object, ...]:
    """Encode every non-generated release column in catalog order."""

    fixed: PublicEvidenceSourceReleaseDescriptor = (
        validate_public_evidence_source_release(release)
    )
    release_values_by_column = _release_values_by_column(fixed)
    return tuple(
        _database_value(field_name, release_values_by_column[field_name])
        for field_name in SOURCE_RELEASE_COLUMNS
    )


def source_record_values(
    release: object,
    source_record: EvidenceSourceRecordReference,
    admission: NppesRegistryAdmissionRow,
) -> tuple[object, ...]:
    """Encode one source-record root owned by the exact release."""

    fixed = validate_public_evidence_source_release(release)
    source_record_values_by_column = {
        **source_record._asdict(),
        "source_release_contract_sha256": fixed.contract_sha256,
        "source_kind": fixed.source_kind,
        "nppes_admission_ref": admission.admission_ref,
    }
    return tuple(
        _database_value(field_name, source_record_values_by_column[field_name])
        for field_name in SOURCE_RECORD_COLUMNS
    )


class NppesRegistryDatabaseRowEncoder:
    """Encode many replay rows under one prevalidated source release."""

    __slots__ = ("_admission_ref", "_contract_sha256", "_source_kind")

    def __init__(
        self,
        release: object,
        admission: NppesRegistryAdmissionRow,
    ) -> None:
        fixed = validate_public_evidence_source_release(release)
        if (
            type(admission) is not NppesRegistryAdmissionRow
            or admission.source_release_ref != fixed.source_release_ref
            or admission.source_release_contract_sha256 != fixed.contract_sha256
            or admission.source_kind != fixed.source_kind
        ):
            raise ValueError("NPPES admission owner mismatch")
        self._contract_sha256 = bytes.fromhex(fixed.contract_sha256)
        self._source_kind = fixed.source_kind
        self._admission_ref = admission.admission_ref

    def source_record(
        self,
        source_record: EvidenceSourceRecordReference,
    ) -> tuple[object, ...]:
        """Encode one exact release-owned source-record root."""

        return (
            source_record.source_record_ref,
            source_record.source_release_ref,
            self._contract_sha256,
            self._source_kind,
            source_record.record_kind,
            source_record.identity_contract_id,
            bytes.fromhex(source_record.record_hmac_sha256),
            bytes.fromhex(source_record.payload_sha256),
            self._admission_ref,
        )

    @staticmethod
    def member(row: NppesRegistryMemberRow) -> tuple[object, ...]:
        """Encode one source-order member row."""

        return member_values(row)

    def projected(
        self,
        rows: NppesRegistryPersistenceRows,
    ) -> tuple[tuple[object, ...], tuple[object, ...], tuple[object, ...]] | None:
        """Encode the projected common, link, and typed rows when eligible."""

        return projected_values(rows, self._admission_ref)


def member_values(row: NppesRegistryMemberRow) -> tuple[object, ...]:
    """Encode one exact source member in catalog order."""

    return _row_values(MEMBER_COLUMNS, row)


def projected_values(
    rows: NppesRegistryPersistenceRows,
    admission_ref: str,
) -> tuple[tuple[object, ...], tuple[object, ...], tuple[object, ...]] | None:
    """Encode common/link/typed rows, or return None for an excluded member."""

    if rows.common_row is None:
        return None
    return (
        _row_values_with_admission(COMMON_COLUMNS, rows.common_row, admission_ref),
        _row_values_with_admission(
            SOURCE_LINK_COLUMNS,
            rows.source_link_row,
            admission_ref,
        ),
        _row_values_with_admission(
            NPI_ENUMERATION_COLUMNS,
            rows.typed_row,
            admission_ref,
        ),
    )


def admission_values(row: NppesRegistryAdmissionRow) -> tuple[object, ...]:
    """Encode one release admission seal in catalog order."""

    return _row_values(ADMISSION_COLUMNS, row)


__all__ = (
    "ADMISSION_COLUMNS",
    "COMMON_COLUMNS",
    "MEMBER_COLUMNS",
    "NPI_ENUMERATION_COLUMNS",
    "NppesRegistryDatabaseRowEncoder",
    "SOURCE_IDENTITY_COLUMNS",
    "SOURCE_LINK_COLUMNS",
    "SOURCE_RECORD_COLUMNS",
    "SOURCE_RELEASE_COLUMNS",
    "admission_values",
    "member_values",
    "projected_values",
    "source_identity_values",
    "source_record_values",
    "source_release_values",
)
