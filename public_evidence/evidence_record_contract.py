# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dormant normalized records for phase-one public evidence."""

from __future__ import annotations

import hmac
from types import MappingProxyType
from typing import Any, Mapping, NamedTuple, TypeAlias
import unicodedata

from public_evidence.evidence_record_policies import (
    ENTITY_ADDRESS_POLICIES,
    EvidenceRecordAuthorityState,
    NPI_ENTITY_TYPES,
    NPI_ENUMERATION_RELATIONSHIP,
    NPI_ENUMERATION_SOURCE_KIND,
    NPI_ENUMERATION_STATES,
    PROVIDER_DIRECTORY_NETWORK_RELATIONSHIP,
    PROVIDER_DIRECTORY_NETWORK_SOURCE_KIND,
    RECORD_VARIANT_FIELDS,
    TAX_IDENTITY_NAME_POLICIES,
    TAX_IDENTITY_RELATIONSHIP_POLICIES,
    _fixed_authority_state,
    _normalized_record_input,
    _required_optional_reference,
    _validate_source_shape,
    _validated_authority_state,
    _validated_network_context,
    build_opaque_source_entity_reference,
    build_provider_directory_network_context,
    build_provider_group_reference,
)
from public_evidence.evidence_record_primitives import (
    PUBLIC_EVIDENCE_RECORD_CONTRACT,
    PUBLIC_EVIDENCE_RECORD_REF_PREFIX,
    CanonicalAddressEvidence,
    EvidenceSourceRecordReference,
    OpaqueSourceEntityReference,
    OpaqueTaxIdentityReference,
    ProviderDirectoryNetworkContext,
    ProviderGroupReference,
    PublicEvidenceRecordError,
    _canonical_sha256,
    _derived_ref,
    _fail,
    _strict_sha256,
    _strict_npi,
    _validate_derived_ref,
    validate_canonical_address_evidence,
)
from public_evidence.evidence_record_token_policy import validate_opaque_tax_identity
from public_evidence.source_release_contract import (
    PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
    PublicEvidenceSourceReleaseDescriptor,
)
from public_evidence.source_release_primitives import CanonicalUtcInterval

_NAME_NORMALIZATION_CONTRACT = "unicode_16_0_0_nfkc_whitespace_casefold_sha256_v1"


def _strict_name(value: object) -> tuple[str, str]:
    if type(value) is not str or not 1 <= len(value) <= 256:
        raise _fail()
    if unicodedata.unidata_version != "16.0.0":
        raise _fail()
    normalized = " ".join(unicodedata.normalize("NFKC", value).split())
    if not normalized or len(normalized.encode("utf-8")) > 512:
        raise _fail()
    if any(unicodedata.category(character).startswith("C") for character in normalized):
        raise _fail()
    name_sha256 = _canonical_sha256(
        "normalized_business_name",
        {
            "normalization": _NAME_NORMALIZATION_CONTRACT,
            "name": normalized.casefold(),
        },
    )
    return value, name_sha256


class TaxIdentityRelationshipEvidence(NamedTuple):
    relationship_class: str
    tax_identity: OpaqueTaxIdentityReference
    provider_group: ProviderGroupReference | None
    related_npi: str | None
    source_entity: OpaqueSourceEntityReference | None
    membership_state: str | None
    candidate_only: bool

    def __repr__(self) -> str:
        return "<tax-identity-relationship-evidence>"


class TaxIdentityNameEvidence(NamedTuple):
    relationship_class: str
    tax_identity: OpaqueTaxIdentityReference
    provider_group: ProviderGroupReference | None
    source_entity: OpaqueSourceEntityReference | None
    source_reported_name: str
    name_kind: str
    name_normalization_contract_id: str
    normalized_name_sha256: str
    candidate_only: bool

    def __repr__(self) -> str:
        return "<tax-identity-name-evidence>"


class NpiEnumerationEvidence(NamedTuple):
    relationship_class: str
    npi: str
    npi_entity_type: str
    enumeration_state: str

    def __repr__(self) -> str:
        return "<npi-enumeration-evidence>"


class EntityAddressEvidence(NamedTuple):
    relationship_class: str
    subject_npi: str | None
    source_entity: OpaqueSourceEntityReference | None
    address: CanonicalAddressEvidence
    candidate_only: bool

    def __repr__(self) -> str:
        return "<entity-address-evidence>"


class ProviderDirectoryNetworkLocationEvidence(NamedTuple):
    relationship_class: str
    npi: str
    address: CanonicalAddressEvidence
    network_context: ProviderDirectoryNetworkContext

    def __repr__(self) -> str:
        return "<provider-directory-network-location-evidence>"


PublicEvidence: TypeAlias = (
    TaxIdentityRelationshipEvidence
    | TaxIdentityNameEvidence
    | NpiEnumerationEvidence
    | EntityAddressEvidence
    | ProviderDirectoryNetworkLocationEvidence
)


class PublicEvidenceRecord(NamedTuple):
    contract: str
    foundation_scope: str
    release: PublicEvidenceSourceReleaseDescriptor
    source_records: tuple[EvidenceSourceRecordReference, ...]
    observed_at: str
    effective_interval: CanonicalUtcInterval
    record_type: str
    evidence: PublicEvidence
    evidence_ref: str
    contract_sha256: str
    authority_state: EvidenceRecordAuthorityState

    def __repr__(self) -> str:
        return f"<public-evidence-record type={self.record_type!r}>"


def _relationship_evidence(
    release: PublicEvidenceSourceReleaseDescriptor,
    record_fields: Mapping[str, object],
) -> TaxIdentityRelationshipEvidence:
    relationship = record_fields["relationship_class"]
    if type(relationship) is not str or relationship not in (
        TAX_IDENTITY_RELATIONSHIP_POLICIES
    ):
        raise _fail()
    policy = TAX_IDENTITY_RELATIONSHIP_POLICIES[relationship]
    identity = validate_opaque_tax_identity(record_fields["tax_identity"])
    membership = record_fields["membership_state"]
    if membership is not None and type(membership) is not str:
        raise _fail()
    if (
        release.source_kind != policy.source_kind
        or identity.tin_type not in policy.tin_types
        or membership not in policy.membership_states
    ):
        raise _fail()
    provider_group = _required_optional_reference(
        release,
        record_fields["provider_group"],
        required=policy.provider_group_required,
        entity=False,
    )
    source_entity = _required_optional_reference(
        release,
        record_fields["source_entity"],
        required=policy.source_entity_required,
        entity=True,
    )
    related_npi = record_fields["related_npi"]
    if policy.related_npi_required:
        related_npi = _strict_npi(related_npi)
    elif related_npi is not None:
        raise _fail()
    return TaxIdentityRelationshipEvidence(
        relationship,
        identity,
        provider_group,
        related_npi,
        source_entity,
        membership,
        policy.candidate_only,
    )


def _name_evidence(
    release: PublicEvidenceSourceReleaseDescriptor,
    record_fields: Mapping[str, object],
) -> TaxIdentityNameEvidence:
    relationship = record_fields["relationship_class"]
    if type(relationship) is not str or relationship not in TAX_IDENTITY_NAME_POLICIES:
        raise _fail()
    policy = TAX_IDENTITY_NAME_POLICIES[relationship]
    identity = validate_opaque_tax_identity(record_fields["tax_identity"])
    if release.source_kind != policy.source_kind or identity.tin_type not in (
        policy.tin_types
    ):
        raise _fail()
    group = _required_optional_reference(
        release,
        record_fields["provider_group"],
        required=policy.provider_group_required,
        entity=False,
    )
    entity = _required_optional_reference(
        release,
        record_fields["source_entity"],
        required=policy.source_entity_required,
        entity=True,
    )
    reported_name, name_sha256 = _strict_name(record_fields["source_reported_name"])
    return TaxIdentityNameEvidence(
        relationship,
        identity,
        group,
        entity,
        reported_name,
        policy.name_kind,
        _NAME_NORMALIZATION_CONTRACT,
        name_sha256,
        policy.candidate_only,
    )


def _enumeration_evidence(
    release: PublicEvidenceSourceReleaseDescriptor, values: Mapping[str, object]
) -> NpiEnumerationEvidence:
    relationship = values["relationship_class"]
    entity_type = values["npi_entity_type"]
    state = values["enumeration_state"]
    if (
        type(relationship) is not str
        or relationship != NPI_ENUMERATION_RELATIONSHIP
        or release.source_kind != NPI_ENUMERATION_SOURCE_KIND
        or type(entity_type) is not str
        or entity_type not in NPI_ENTITY_TYPES
        or type(state) is not str
        or state not in NPI_ENUMERATION_STATES
    ):
        raise _fail()
    return NpiEnumerationEvidence(
        relationship, _strict_npi(values["npi"]), entity_type, state
    )


def _address_evidence(
    release: PublicEvidenceSourceReleaseDescriptor, values: Mapping[str, object]
) -> EntityAddressEvidence:
    relationship = values["relationship_class"]
    if type(relationship) is not str or relationship not in ENTITY_ADDRESS_POLICIES:
        raise _fail()
    policy = ENTITY_ADDRESS_POLICIES[relationship]
    address = validate_canonical_address_evidence(values["address"])
    if (
        release.source_kind != policy.source_kind
        or address.purpose not in policy.purposes
    ):
        raise _fail()
    subject_npi = values["subject_npi"]
    source_entity = values["source_entity"]
    if policy.subject_kind == "npi":
        subject_npi = _strict_npi(subject_npi)
        if source_entity is not None:
            raise _fail()
        source_entity = None
    else:
        if subject_npi is not None:
            raise _fail()
        source_entity = _required_optional_reference(
            release, source_entity, required=True, entity=True
        )
    return EntityAddressEvidence(
        relationship, subject_npi, source_entity, address, policy.candidate_only
    )


def _network_evidence(
    release: PublicEvidenceSourceReleaseDescriptor, values: Mapping[str, object]
) -> ProviderDirectoryNetworkLocationEvidence:
    relationship = values["relationship_class"]
    address = validate_canonical_address_evidence(values["address"])
    if (
        type(relationship) is not str
        or relationship != PROVIDER_DIRECTORY_NETWORK_RELATIONSHIP
        or release.source_kind != PROVIDER_DIRECTORY_NETWORK_SOURCE_KIND
        or address.purpose != "provider_directory_location"
    ):
        raise _fail()
    return ProviderDirectoryNetworkLocationEvidence(
        relationship,
        _strict_npi(values["npi"]),
        address,
        _validated_network_context(release, values["network_context"]),
    )


_NORMALIZERS = MappingProxyType(
    {
        "tax_identity_relationship": _relationship_evidence,
        "tax_identity_name": _name_evidence,
        "npi_enumeration": _enumeration_evidence,
        "entity_address": _address_evidence,
        "provider_directory_network_location": _network_evidence,
    }
)


def _json_value(value: object) -> Any:
    if type(value) is CanonicalUtcInterval:
        return {"start_at": value.start_at, "end_at": value.end_at}
    if type(value) in {
        CanonicalAddressEvidence,
        EvidenceRecordAuthorityState,
        EvidenceSourceRecordReference,
        OpaqueSourceEntityReference,
        OpaqueTaxIdentityReference,
        ProviderDirectoryNetworkContext,
        ProviderGroupReference,
        TaxIdentityRelationshipEvidence,
        TaxIdentityNameEvidence,
        NpiEnumerationEvidence,
        EntityAddressEvidence,
        ProviderDirectoryNetworkLocationEvidence,
    }:
        return {field: _json_value(getattr(value, field)) for field in value._fields}
    if type(value) is tuple:
        return [_json_value(item) for item in value]
    if value is None or type(value) in {str, bool, int}:
        return value
    raise _fail()


def _record_payload(
    release: PublicEvidenceSourceReleaseDescriptor,
    records: tuple[EvidenceSourceRecordReference, ...],
    observed_at: str,
    effective: CanonicalUtcInterval,
    record_type: str,
    evidence: PublicEvidence,
    authority: EvidenceRecordAuthorityState,
) -> dict[str, object]:
    return {
        "contract": PUBLIC_EVIDENCE_RECORD_CONTRACT,
        "foundation_scope": PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        "source_kind": release.source_kind,
        "source_release_ref": release.source_release_ref,
        "source_release_contract_sha256": release.contract_sha256,
        "source_records": _json_value(records),
        "observed_at": observed_at,
        "effective_interval": _json_value(effective),
        "record_type": record_type,
        "evidence": _json_value(evidence),
        "authority_state": _json_value(authority),
    }


def build_public_evidence_record(
    release: PublicEvidenceSourceReleaseDescriptor, raw: Mapping[str, object]
) -> PublicEvidenceRecord:
    """Validate and freeze one capability-free normalized evidence record."""
    try:
        normalized_input = _normalized_record_input(release, raw)
        evidence = _NORMALIZERS[normalized_input.record_type](
            normalized_input.release,
            normalized_input.record_fields,
        )
        if (
            type(evidence) is NpiEnumerationEvidence
            and evidence.enumeration_state == "deactivated"
            and normalized_input.effective_interval.end_at is None
        ):
            raise _fail()
        _validate_source_shape(
            normalized_input.source_records,
            normalized_input.record_type,
            evidence,
        )
        authority = _fixed_authority_state()
        contract_payload = _record_payload(
            normalized_input.release,
            normalized_input.source_records,
            normalized_input.observed_at,
            normalized_input.effective_interval,
            normalized_input.record_type,
            evidence,
            authority,
        )
        normalized_record = PublicEvidenceRecord(
            PUBLIC_EVIDENCE_RECORD_CONTRACT,
            PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
            normalized_input.release,
            normalized_input.source_records,
            normalized_input.observed_at,
            normalized_input.effective_interval,
            normalized_input.record_type,
            evidence,
            _derived_ref(
                PUBLIC_EVIDENCE_RECORD_REF_PREFIX,
                "evidence_record",
                contract_payload,
            ),
            _canonical_sha256("evidence_record_contract", contract_payload),
            authority,
        )
    except Exception:
        normalized_error = _fail()
    else:
        return normalized_record
    raise normalized_error


def _raw_from_record(record: PublicEvidenceRecord) -> dict[str, object]:
    evidence = record.evidence
    variant_fields = RECORD_VARIANT_FIELDS.get(record.record_type)
    if variant_fields is None:
        raise _fail()
    raw_field_map = {
        "record_type": record.record_type,
        "source_records": record.source_records,
        "observed_at": record.observed_at,
        "effective_interval": record.effective_interval,
    }
    for field_name in variant_fields:
        raw_field_map[field_name] = getattr(evidence, field_name)
    return raw_field_map


def _validate_supplied_semantics(
    supplied: PublicEvidenceRecord,
    rebuilt: PublicEvidenceRecord,
) -> None:
    if type(supplied.evidence) is not type(rebuilt.evidence):
        raise _fail()
    semantic_pairs = (
        (supplied.source_records, rebuilt.source_records),
        (supplied.effective_interval, rebuilt.effective_interval),
        (supplied.evidence, rebuilt.evidence),
        (supplied.authority_state, rebuilt.authority_state),
    )
    if any(_json_value(left) != _json_value(right) for left, right in semantic_pairs):
        raise _fail()


def validate_public_evidence_record(candidate: object) -> PublicEvidenceRecord:
    """Rebuild an exact record; validation grants no serving authority."""
    try:
        if type(candidate) is not PublicEvidenceRecord:
            raise _fail()
        if (
            type(candidate.contract) is not str
            or candidate.contract != PUBLIC_EVIDENCE_RECORD_CONTRACT
            or type(candidate.foundation_scope) is not str
            or candidate.foundation_scope != PUBLIC_EVIDENCE_FOUNDATION_SCOPE
        ):
            raise _fail()
        _validated_authority_state(candidate.authority_state)
        record_input = _raw_from_record(candidate)
        rebuilt = build_public_evidence_record(candidate.release, record_input)
        _validate_supplied_semantics(candidate, rebuilt)
        _validate_derived_ref(
            candidate.evidence_ref,
            PUBLIC_EVIDENCE_RECORD_REF_PREFIX,
            rebuilt.evidence_ref,
        )
        digest = _strict_sha256(candidate.contract_sha256)
        if not hmac.compare_digest(digest, rebuilt.contract_sha256):
            raise _fail()
    except Exception:
        normalized_error = _fail()
    else:
        return rebuilt
    raise normalized_error
