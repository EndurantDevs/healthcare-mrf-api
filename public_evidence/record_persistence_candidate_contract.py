# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deterministic, no-I/O row candidates for normalized public evidence."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
import hashlib
import hmac
from types import MappingProxyType
from typing import Any, Callable, Mapping

from public_evidence.evidence_record_contract import (
    EntityAddressEvidence,
    NpiEnumerationEvidence,
    ProviderDirectoryNetworkLocationEvidence,
    PublicEvidenceRecord,
    TaxIdentityNameEvidence,
    TaxIdentityRelationshipEvidence,
    validate_public_evidence_record,
)
from public_evidence.evidence_record_policies import NETWORK_RECORD_FIELDS
from public_evidence.evidence_record_primitives import (
    MAX_PUBLIC_EVIDENCE_SOURCE_RECORDS,
    _canonical_sha256,
    _derived_ref,
)
from public_evidence.record_persistence_candidate_primitives import (
    PUBLIC_EVIDENCE_RECORD_PERSISTENCE_CANDIDATE_CONTRACT,
    PUBLIC_EVIDENCE_RECORD_PERSISTENCE_CANDIDATE_REF_PREFIX,
    SOURCE_LINK_ORDERING_CONTRACT,
    SOURCE_REPORTED_NAME_BYTES_CONTRACT,
    EntityAddressRow,
    NpiEnumerationRow,
    ProviderDirectoryNetworkLocationRow,
    PublicEvidenceRecordCommonRow,
    PublicEvidenceRecordPersistenceCandidate,
    PublicEvidenceRecordSourceLinkRow,
    PublicEvidenceTypedRow,
    TaxIdentityNameRow,
    TaxIdentityRelationshipRow,
    fixed_persistence_candidate_authority,
    persistence_candidate_error,
)
from public_evidence.source_release_contract import PUBLIC_EVIDENCE_FOUNDATION_SCOPE


_ROW_TYPES = (
    PublicEvidenceRecordCommonRow,
    PublicEvidenceRecordSourceLinkRow,
    TaxIdentityRelationshipRow,
    TaxIdentityNameRow,
    NpiEnumerationRow,
    EntityAddressRow,
    ProviderDirectoryNetworkLocationRow,
)


def _candidate_sha256(purpose: str, payload: object) -> str:
    return _canonical_sha256(f"persistence_candidate_{purpose}", payload)


def _row_payload(row: object) -> dict[str, object]:
    if type(row) not in _ROW_TYPES:
        raise persistence_candidate_error()
    payload = dict(row._asdict())
    payload.pop("row_sha256")
    if type(row) is TaxIdentityNameRow:
        payload.pop("source_reported_name")
    return payload


def _finished_row(row: Any) -> Any:
    digest = _candidate_sha256("typed_row", _row_payload(row))
    if type(row) is PublicEvidenceRecordSourceLinkRow:
        digest = _candidate_sha256("source_link_row", _row_payload(row))
    elif type(row) is PublicEvidenceRecordCommonRow:
        digest = _candidate_sha256("common_row", _row_payload(row))
    return row._replace(row_sha256=digest)


def _typed_owner(record: PublicEvidenceRecord) -> dict[str, str]:
    release = record.release
    return {
        "evidence_ref": record.evidence_ref,
        "source_release_ref": release.source_release_ref,
        "source_release_contract_sha256": release.contract_sha256,
        "source_kind": release.source_kind,
        "record_type": record.record_type,
    }


def _optional_reference(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    try:
        reference = getattr(value, field_name)
    except Exception:
        normalized_error = persistence_candidate_error()
    else:
        if type(reference) is str:
            return reference
        normalized_error = persistence_candidate_error()
    raise normalized_error


def _address_fields(address: object) -> dict[str, object]:
    field_names = (
        "address_key",
        "address_site_key",
        "canonicalization_contract_id",
        "purpose",
        "zip5",
        "geo_derivation_contract_id",
        "geo_quality",
        "freshness_state",
        "freshness_rule_version",
        "freshness_as_of",
        "selection_rule_version",
        "selection_eligible",
    )
    return {field_name: getattr(address, field_name) for field_name in field_names}


def _relationship_row(record: PublicEvidenceRecord) -> TaxIdentityRelationshipRow:
    evidence = record.evidence
    if type(evidence) is not TaxIdentityRelationshipEvidence:
        raise persistence_candidate_error()
    row = TaxIdentityRelationshipRow(
        **_typed_owner(record),
        relationship_class=evidence.relationship_class,
        tax_identity_ref=evidence.tax_identity.tax_identity_ref,
        provider_group_ref=_optional_reference(
            evidence.provider_group, "provider_group_ref"
        ),
        related_npi=evidence.related_npi,
        source_entity_ref=_optional_reference(evidence.source_entity, "source_entity_ref"),
        membership_state=evidence.membership_state,
        candidate_only=evidence.candidate_only,
        row_sha256="",
    )
    return _finished_row(row)


def _name_row(record: PublicEvidenceRecord) -> TaxIdentityNameRow:
    evidence = record.evidence
    if type(evidence) is not TaxIdentityNameEvidence:
        raise persistence_candidate_error()
    reported_name_digest = hashlib.sha256(
        evidence.source_reported_name.encode("utf-8")
    ).hexdigest()
    row = TaxIdentityNameRow(
        **_typed_owner(record),
        relationship_class=evidence.relationship_class,
        tax_identity_ref=evidence.tax_identity.tax_identity_ref,
        provider_group_ref=_optional_reference(
            evidence.provider_group, "provider_group_ref"
        ),
        source_entity_ref=_optional_reference(evidence.source_entity, "source_entity_ref"),
        source_reported_name=evidence.source_reported_name,
        source_reported_name_bytes_contract_id=SOURCE_REPORTED_NAME_BYTES_CONTRACT,
        source_reported_name_utf8_sha256=reported_name_digest,
        name_kind=evidence.name_kind,
        name_normalization_contract_id=evidence.name_normalization_contract_id,
        normalized_name_sha256=evidence.normalized_name_sha256,
        candidate_only=evidence.candidate_only,
        row_sha256="",
    )
    return _finished_row(row)


def _enumeration_row(record: PublicEvidenceRecord) -> NpiEnumerationRow:
    evidence = record.evidence
    if type(evidence) is not NpiEnumerationEvidence:
        raise persistence_candidate_error()
    row = NpiEnumerationRow(
        **_typed_owner(record),
        relationship_class=evidence.relationship_class,
        npi=evidence.npi,
        npi_entity_type=evidence.npi_entity_type,
        enumeration_state=evidence.enumeration_state,
        row_sha256="",
    )
    return _finished_row(row)


def _entity_address_row(record: PublicEvidenceRecord) -> EntityAddressRow:
    evidence = record.evidence
    if type(evidence) is not EntityAddressEvidence:
        raise persistence_candidate_error()
    row = EntityAddressRow(
        **_typed_owner(record),
        relationship_class=evidence.relationship_class,
        subject_npi=evidence.subject_npi,
        source_entity_ref=_optional_reference(evidence.source_entity, "source_entity_ref"),
        **_address_fields(evidence.address),
        candidate_only=evidence.candidate_only,
        row_sha256="",
    )
    return _finished_row(row)


def _network_context_fields(evidence: object) -> dict[str, object]:
    context = evidence.network_context
    reference_by_field = {
        f"{field_name}_ref": getattr(context, field_name).source_record_ref
        for field_name, _record_kind in NETWORK_RECORD_FIELDS
    }
    return {
        **reference_by_field,
        "role_active": context.role_active,
        "pricing_bridge_state": context.pricing_bridge_state,
    }


def _network_row(record: PublicEvidenceRecord) -> ProviderDirectoryNetworkLocationRow:
    evidence = record.evidence
    if type(evidence) is not ProviderDirectoryNetworkLocationEvidence:
        raise persistence_candidate_error()
    row = ProviderDirectoryNetworkLocationRow(
        **_typed_owner(record),
        relationship_class=evidence.relationship_class,
        npi=evidence.npi,
        **_address_fields(evidence.address),
        **_network_context_fields(evidence),
        row_sha256="",
    )
    return _finished_row(row)


_TYPED_ROW_BUILDERS: Mapping[str, Callable[[PublicEvidenceRecord], PublicEvidenceTypedRow]] = (
    MappingProxyType(
        {
            "tax_identity_relationship": _relationship_row,
            "tax_identity_name": _name_row,
            "npi_enumeration": _enumeration_row,
            "entity_address": _entity_address_row,
            "provider_directory_network_location": _network_row,
        }
    )
)


def _typed_row(record: PublicEvidenceRecord) -> PublicEvidenceTypedRow:
    builder = _TYPED_ROW_BUILDERS.get(record.record_type)
    if builder is None:
        raise persistence_candidate_error()
    return builder(record)


def _source_link_rows(
    evidence_record: PublicEvidenceRecord,
) -> tuple[tuple[PublicEvidenceRecordSourceLinkRow, ...], str]:
    references = tuple(
        source_record.source_record_ref
        for source_record in evidence_record.source_records
    )
    ordered_references = tuple(
        sorted(references, key=lambda reference: reference.encode("ascii"))
    )
    if references != ordered_references or len(set(references)) != len(references):
        raise persistence_candidate_error()
    release = evidence_record.release
    source_link_rows = tuple(
        _finished_row(
            PublicEvidenceRecordSourceLinkRow(
                evidence_ref=evidence_record.evidence_ref,
                source_release_ref=release.source_release_ref,
                source_release_contract_sha256=release.contract_sha256,
                source_kind=release.source_kind,
                source_record_ordinal=ordinal,
                source_record_ref=source_record.source_record_ref,
                record_kind=source_record.record_kind,
                row_sha256="",
            )
        )
        for ordinal, source_record in enumerate(evidence_record.source_records)
    )
    if not 1 <= len(source_link_rows) <= MAX_PUBLIC_EVIDENCE_SOURCE_RECORDS:
        raise persistence_candidate_error()
    vector_payload_by_field = {
        "ordering_contract_id": SOURCE_LINK_ORDERING_CONTRACT,
        "source_record_count": len(source_link_rows),
        "links": [
            {
                "source_record_ordinal": source_link.source_record_ordinal,
                "source_record_ref": source_link.source_record_ref,
                "row_sha256": source_link.row_sha256,
            }
            for source_link in source_link_rows
        ],
    }
    return source_link_rows, _candidate_sha256(
        "source_link_vector", vector_payload_by_field
    )


def _record_authority_sha256(record: PublicEvidenceRecord) -> str:
    return _candidate_sha256("record_authority_state", record.authority_state._asdict())


def _common_row(
    evidence_record: PublicEvidenceRecord,
    source_link_vector_sha256: str,
    typed_row_sha256: str,
) -> PublicEvidenceRecordCommonRow:
    release = evidence_record.release
    authority = evidence_record.authority_state
    relationship = getattr(evidence_record.evidence, "relationship_class", None)
    if type(relationship) is not str:
        raise persistence_candidate_error()
    common_row_candidate = PublicEvidenceRecordCommonRow(
        evidence_ref=evidence_record.evidence_ref,
        record_contract=evidence_record.contract,
        record_contract_sha256=evidence_record.contract_sha256,
        foundation_scope=evidence_record.foundation_scope,
        source_release_ref=release.source_release_ref,
        source_release_contract_sha256=release.contract_sha256,
        source_kind=release.source_kind,
        observed_at=evidence_record.observed_at,
        effective_start_at=evidence_record.effective_interval.start_at,
        effective_end_at=evidence_record.effective_interval.end_at,
        record_type=evidence_record.record_type,
        relationship_class=relationship,
        source_record_count=len(evidence_record.source_records),
        source_link_ordering_contract_id=SOURCE_LINK_ORDERING_CONTRACT,
        source_link_vector_sha256=source_link_vector_sha256,
        typed_row_sha256=typed_row_sha256,
        authority_state_sha256=_record_authority_sha256(evidence_record),
        lifecycle_state=authority.lifecycle_state,
        positive_evidence_only=authority.positive_evidence_only,
        serving_authority=authority.serving_authority,
        current_pointer_authority=authority.current_pointer_authority,
        database_io_authority=authority.database_io_authority,
        publication_enabled=authority.publication_enabled,
        row_sha256="",
    )
    return _finished_row(common_row_candidate)


def _candidate_payload(
    record: PublicEvidenceRecord,
    common_row: PublicEvidenceRecordCommonRow,
    authority: object,
) -> dict[str, object]:
    return {
        "contract": PUBLIC_EVIDENCE_RECORD_PERSISTENCE_CANDIDATE_CONTRACT,
        "foundation_scope": PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        "evidence_ref": record.evidence_ref,
        "record_contract_sha256": record.contract_sha256,
        "common_row_sha256": common_row.row_sha256,
        "source_link_vector_sha256": common_row.source_link_vector_sha256,
        "typed_row_sha256": common_row.typed_row_sha256,
        "authority_state": authority._asdict(),
    }


def _build_candidate(record: object) -> PublicEvidenceRecordPersistenceCandidate:
    fixed_record = validate_public_evidence_record(record)
    typed_row = _typed_row(fixed_record)
    source_links, source_vector_sha256 = _source_link_rows(fixed_record)
    common_row = _common_row(
        fixed_record,
        source_vector_sha256,
        typed_row.row_sha256,
    )
    authority = fixed_persistence_candidate_authority()
    payload = _candidate_payload(fixed_record, common_row, authority)
    return PublicEvidenceRecordPersistenceCandidate(
        contract=PUBLIC_EVIDENCE_RECORD_PERSISTENCE_CANDIDATE_CONTRACT,
        foundation_scope=PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        record=fixed_record,
        common_row=common_row,
        source_link_rows=source_links,
        typed_row=typed_row,
        candidate_ref=_derived_ref(
            PUBLIC_EVIDENCE_RECORD_PERSISTENCE_CANDIDATE_REF_PREFIX,
            "persistence_candidate",
            payload,
        ),
        contract_sha256=_candidate_sha256("contract", payload),
        authority_state=authority,
    )


def build_public_evidence_record_persistence_candidate(
    record: PublicEvidenceRecord,
) -> PublicEvidenceRecordPersistenceCandidate:
    """Freeze one prospective row set without granting persistence authority."""

    try:
        candidate = _build_candidate(record)
    except Exception:
        normalized_error = persistence_candidate_error()
    else:
        return candidate
    raise normalized_error


def _is_digest_or_reference(field_name: str | None) -> bool:
    return field_name is not None and field_name.endswith(("_sha256", "_ref"))


def _is_exact_value_match(
    candidate: object,
    expected: object,
    field_name: str | None = None,
) -> bool:
    if type(candidate) is not type(expected):
        return False
    if type(expected) is str and _is_digest_or_reference(field_name):
        return hmac.compare_digest(candidate, expected)
    if type(expected) is tuple:
        return len(candidate) == len(expected) and all(
            _is_exact_value_match(left, right)
            for left, right in zip(candidate, expected, strict=True)
        )
    named_fields = getattr(type(expected), "_fields", None)
    if named_fields is not None:
        return all(
            _is_exact_value_match(
                getattr(candidate, nested_name),
                getattr(expected, nested_name),
                nested_name,
            )
            for nested_name in named_fields
        )
    if is_dataclass(expected) and not isinstance(expected, type):
        return all(
            _is_exact_value_match(
                getattr(candidate, field.name),
                getattr(expected, field.name),
                field.name,
            )
            for field in fields(expected)
        )
    return candidate == expected


def validate_public_evidence_record_persistence_candidate(
    candidate: object,
) -> PublicEvidenceRecordPersistenceCandidate:
    """Rebuild an exact row candidate and reject every authority escalation."""

    try:
        if type(candidate) is not PublicEvidenceRecordPersistenceCandidate:
            raise persistence_candidate_error()
        rebuilt = _build_candidate(candidate.record)
        if not _is_exact_value_match(candidate, rebuilt):
            raise persistence_candidate_error()
    except Exception:
        normalized_error = persistence_candidate_error()
    else:
        return rebuilt
    raise normalized_error
