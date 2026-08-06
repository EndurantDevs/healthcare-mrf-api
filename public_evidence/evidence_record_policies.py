# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed source and semantic policy matrices for public evidence records."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Mapping, NamedTuple

from public_evidence.evidence_record_primitives import (
    PUBLIC_EVIDENCE_PROVIDER_GROUP_REF_PREFIX,
    PUBLIC_EVIDENCE_SOURCE_ENTITY_REF_PREFIX,
    EvidenceSourceRecordReference,
    OpaqueSourceEntityReference,
    ProviderDirectoryNetworkContext,
    ProviderGroupReference,
    _derived_ref,
    _exact_dict,
    _fail,
    _normalized_source_records,
    _strict_kind,
    _strict_protocol,
    _strict_sha256,
    _validate_address_freshness,
    _validate_derived_ref,
    _validated_release,
    _validated_temporal_scope,
    validate_evidence_source_record_reference,
)
from public_evidence.source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
)
from public_evidence.source_release_primitives import CanonicalUtcInterval

COMMON_RECORD_FIELDS = frozenset(
    {"record_type", "source_records", "observed_at", "effective_interval"}
)
RECORD_VARIANT_FIELDS = MappingProxyType(
    {
        "tax_identity_relationship": frozenset(
            "relationship_class tax_identity provider_group related_npi "
            "source_entity membership_state".split()
        ),
        "tax_identity_name": frozenset(
            "relationship_class tax_identity provider_group source_entity "
            "source_reported_name".split()
        ),
        "npi_enumeration": frozenset(
            "relationship_class npi npi_entity_type enumeration_state".split()
        ),
        "entity_address": frozenset(
            "relationship_class subject_npi source_entity address".split()
        ),
        "provider_directory_network_location": frozenset(
            "relationship_class npi address network_context".split()
        ),
    }
)


class _NormalizedRecordInput(NamedTuple):
    release: PublicEvidenceSourceReleaseDescriptor
    record_type: str
    record_fields: Mapping[str, object]
    source_records: tuple[EvidenceSourceRecordReference, ...]
    observed_at: str
    effective_interval: CanonicalUtcInterval


def _normalized_record_input(
    release: PublicEvidenceSourceReleaseDescriptor,
    raw: Mapping[str, object],
) -> _NormalizedRecordInput:
    fixed_release = _validated_release(release)
    if type(raw) is not dict or not 1 <= len(raw) <= 10:
        raise _fail()
    if any(type(key) is not str for key in raw):
        raise _fail()
    record_type = raw.get("record_type")
    if type(record_type) is not str or record_type not in RECORD_VARIANT_FIELDS:
        raise _fail()
    fields = COMMON_RECORD_FIELDS | RECORD_VARIANT_FIELDS[record_type]
    record_fields = _exact_dict(raw, fields)
    source_records = _normalized_source_records(
        fixed_release, record_fields["source_records"]
    )
    observed_at, effective_interval = _validated_temporal_scope(
        fixed_release,
        record_fields["observed_at"],
        record_fields["effective_interval"],
    )
    if "address" in record_fields:
        _validate_address_freshness(record_fields["address"], observed_at)
    return _NormalizedRecordInput(
        fixed_release,
        record_type,
        record_fields,
        source_records,
        observed_at,
        effective_interval,
    )


class TaxIdentityRelationshipPolicy(NamedTuple):
    source_kind: str
    tin_types: tuple[str, ...]
    provider_group_required: bool
    related_npi_required: bool
    source_entity_required: bool
    membership_states: tuple[str | None, ...]
    candidate_only: bool


class TaxIdentityNamePolicy(NamedTuple):
    source_kind: str
    tin_types: tuple[str, ...]
    provider_group_required: bool
    source_entity_required: bool
    name_kind: str
    candidate_only: bool


class EntityAddressPolicy(NamedTuple):
    source_kind: str
    subject_kind: str
    purposes: tuple[str, ...]
    candidate_only: bool


class EvidenceRecordAuthorityState(NamedTuple):
    lifecycle_state: str
    positive_evidence_only: bool
    serving_authority: str
    current_pointer_authority: str
    executor_authority: str
    adapter_execution_authority: str
    database_io_authority: str
    address_selection_authority: str
    legal_ownership_claimed: bool
    employment_claimed: bool
    facility_ownership_claimed: bool
    exact_rate_site_claimed: bool
    payer_confirmed_site_claimed: bool
    site_match_claimed: bool
    confidence_claimed: bool
    independence_claimed: bool
    publication_enabled: bool
    replacement_enabled: bool
    deletion_enabled: bool
    retirement_enabled: bool
    supersession_enabled: bool

    def __repr__(self) -> str:
        return "<evidence-record-authority-state normalized_record_only>"


def _fixed_authority_state() -> EvidenceRecordAuthorityState:
    return EvidenceRecordAuthorityState(
        "normalized_record_only",
        True,
        *("none",) * 6,
        *(False,) * 13,
    )


def _validated_authority_state(value: object) -> EvidenceRecordAuthorityState:
    if type(value) is not EvidenceRecordAuthorityState:
        raise _fail()
    expected = _fixed_authority_state()
    for index, expected_value in enumerate(expected):
        candidate = value[index]
        if type(candidate) is not type(expected_value) or candidate != expected_value:
            raise _fail()
    return expected


TAX_IDENTITY_RELATIONSHIP_POLICIES = MappingProxyType(
    {
        "tic_billing_identity_provider_group": TaxIdentityRelationshipPolicy(
            source_kind="tic",
            tin_types=("ein", "npi"),
            provider_group_required=True,
            related_npi_required=False,
            source_entity_required=False,
            membership_states=("members_present", "tin_only"),
            candidate_only=False,
        ),
        "tic_provider_group_member": TaxIdentityRelationshipPolicy(
            source_kind="tic",
            tin_types=("ein", "npi"),
            provider_group_required=True,
            related_npi_required=True,
            source_entity_required=False,
            membership_states=("members_present",),
            candidate_only=False,
        ),
        "fhir_same_organization_identifier": TaxIdentityRelationshipPolicy(
            source_kind="public_provider_directory_fhir",
            tin_types=("ein",),
            provider_group_required=False,
            related_npi_required=True,
            source_entity_required=True,
            membership_states=(None,),
            candidate_only=False,
        ),
        "hpt_hospital_tax_identity_entity_candidate": TaxIdentityRelationshipPolicy(
            source_kind="public_hpt",
            tin_types=("ein",),
            provider_group_required=False,
            related_npi_required=False,
            source_entity_required=True,
            membership_states=(None,),
            candidate_only=True,
        ),
        "hpt_hospital_tax_identity_npi_candidate": TaxIdentityRelationshipPolicy(
            source_kind="public_hpt",
            tin_types=("ein",),
            provider_group_required=False,
            related_npi_required=True,
            source_entity_required=True,
            membership_states=(None,),
            candidate_only=True,
        ),
    }
)


TAX_IDENTITY_NAME_POLICIES = MappingProxyType(
    {
        "tic_source_reported_business_name": TaxIdentityNamePolicy(
            source_kind="tic",
            tin_types=("ein", "npi"),
            provider_group_required=True,
            source_entity_required=False,
            name_kind="business_name",
            candidate_only=False,
        ),
        "fhir_same_organization_reported_name": TaxIdentityNamePolicy(
            source_kind="public_provider_directory_fhir",
            tin_types=("ein",),
            provider_group_required=False,
            source_entity_required=True,
            name_kind="organization_name",
            candidate_only=False,
        ),
        "hpt_source_reported_hospital_name_candidate": TaxIdentityNamePolicy(
            source_kind="public_hpt",
            tin_types=("ein",),
            provider_group_required=False,
            source_entity_required=True,
            name_kind="hospital_name_candidate",
            candidate_only=True,
        ),
    }
)


ENTITY_ADDRESS_POLICIES = MappingProxyType(
    {
        "nppes_npi_practice_location": EntityAddressPolicy(
            source_kind="nppes_entity_address",
            subject_kind="npi",
            purposes=("nppes_practice_location",),
            candidate_only=False,
        ),
        "nppes_npi_mailing_address": EntityAddressPolicy(
            source_kind="nppes_entity_address",
            subject_kind="npi",
            purposes=("nppes_mailing",),
            candidate_only=False,
        ),
        "fhir_npi_directory_address": EntityAddressPolicy(
            source_kind="public_provider_directory_fhir",
            subject_kind="npi",
            purposes=("provider_directory_location",),
            candidate_only=False,
        ),
        "fhir_entity_directory_address": EntityAddressPolicy(
            source_kind="public_provider_directory_fhir",
            subject_kind="source_entity",
            purposes=("provider_directory_location",),
            candidate_only=False,
        ),
        "hpt_entity_location_candidate": EntityAddressPolicy(
            source_kind="public_hpt",
            subject_kind="source_entity",
            purposes=("hpt_hospital_location_candidate",),
            candidate_only=True,
        ),
    }
)


NPI_ENUMERATION_RELATIONSHIP = "nppes_npi_enumeration"
NPI_ENUMERATION_SOURCE_KIND = "nppes_entity_address"
NPI_ENTITY_TYPES = ("individual_type_1", "organization_type_2")
NPI_ENUMERATION_STATES = ("active", "deactivated")
PROVIDER_DIRECTORY_NETWORK_RELATIONSHIP = "fhir_provider_directory_network_location"
PROVIDER_DIRECTORY_NETWORK_SOURCE_KIND = "public_provider_directory_fhir"

NETWORK_RECORD_FIELDS = (
    ("npi_source_record", "fhir_npi_resource"),
    ("practitioner_role_source_record", "fhir_practitioner_role"),
    ("location_source_record", "fhir_location"),
    ("network_source_record", "fhir_network"),
    ("insurance_plan_source_record", "fhir_insurance_plan"),
)


def _source_kinds(record_type: str, evidence: Any) -> tuple[str, ...]:
    if record_type == "provider_directory_network_location":
        return tuple(kind for _field, kind in NETWORK_RECORD_FIELDS)
    relationship = evidence.relationship_class
    if relationship.startswith("tic_"):
        return ("tic_provider_group_occurrence",)
    if record_type == "npi_enumeration" or relationship.startswith("nppes_"):
        return ("nppes_registry_record",)
    if relationship.startswith("hpt_"):
        return ("hpt_hospital_record",)
    if record_type == "entity_address":
        subject_kind = (
            "fhir_npi_resource" if evidence.subject_npi else "fhir_organization"
        )
        return (subject_kind, "fhir_location")
    return ("fhir_organization",)


def _validate_source_shape(
    source_records: tuple[EvidenceSourceRecordReference, ...],
    record_type: str,
    evidence: Any,
) -> None:
    expected_kinds = tuple(sorted(_source_kinds(record_type, evidence)))
    observed_kinds = tuple(
        sorted(source_record.record_kind for source_record in source_records)
    )
    if observed_kinds != expected_kinds:
        raise _fail()
    if record_type == "provider_directory_network_location":
        context_refs = {
            getattr(evidence.network_context, field_name).source_record_ref
            for field_name, _kind in NETWORK_RECORD_FIELDS
        }
        if context_refs != {
            source_record.source_record_ref for source_record in source_records
        }:
            raise _fail()


_SOURCE_ENTITY_KIND = MappingProxyType(
    {
        "public_hpt": "hpt_hospital_entity",
        "public_provider_directory_fhir": "fhir_organization",
    }
)


def _build_release_reference(
    release: PublicEvidenceSourceReleaseDescriptor,
    raw: Mapping[str, object],
    *,
    entity: bool,
) -> OpaqueSourceEntityReference | ProviderGroupReference:
    fixed_release = _validated_release(release)
    kind_field = "entity_kind" if entity else None
    fields = {"identity_contract_id", "identity_sha256"}
    if kind_field is not None:
        fields.add(kind_field)
    reference_field_map = _exact_dict(raw, frozenset(fields))
    reference_payload_map = {
        "source_release_ref": fixed_release.source_release_ref,
        "identity_contract_id": _strict_protocol(
            reference_field_map["identity_contract_id"]
        ),
        "identity_sha256": _strict_sha256(reference_field_map["identity_sha256"]),
    }
    if entity:
        reference_payload_map["entity_kind"] = _strict_kind(
            reference_field_map["entity_kind"]
        )
        return OpaqueSourceEntityReference(
            reference_payload_map["source_release_ref"],
            reference_payload_map["entity_kind"],
            reference_payload_map["identity_contract_id"],
            reference_payload_map["identity_sha256"],
            _derived_ref(
                PUBLIC_EVIDENCE_SOURCE_ENTITY_REF_PREFIX,
                "source_entity",
                reference_payload_map,
            ),
        )
    return ProviderGroupReference(
        reference_payload_map["source_release_ref"],
        reference_payload_map["identity_contract_id"],
        reference_payload_map["identity_sha256"],
        _derived_ref(
            PUBLIC_EVIDENCE_PROVIDER_GROUP_REF_PREFIX,
            "provider_group",
            reference_payload_map,
        ),
    )


def build_opaque_source_entity_reference(
    release: PublicEvidenceSourceReleaseDescriptor, raw: Mapping[str, object]
) -> OpaqueSourceEntityReference:
    """Bind an opaque source entity to one validated release."""
    return _build_release_reference(release, raw, entity=True)


def build_provider_group_reference(
    release: PublicEvidenceSourceReleaseDescriptor, raw: Mapping[str, object]
) -> ProviderGroupReference:
    """Bind an opaque provider group to one validated release."""
    return _build_release_reference(release, raw, entity=False)


def _validate_release_reference(
    release: PublicEvidenceSourceReleaseDescriptor, value: object, *, entity: bool
) -> OpaqueSourceEntityReference | ProviderGroupReference:
    expected_type = OpaqueSourceEntityReference if entity else ProviderGroupReference
    if type(value) is not expected_type:
        raise _fail()
    reference_field_map = {
        "identity_contract_id": value.identity_contract_id,
        "identity_sha256": value.identity_sha256,
    }
    if entity:
        reference_field_map["entity_kind"] = value.entity_kind
    rebuilt = _build_release_reference(release, reference_field_map, entity=entity)
    supplied_ref = value.source_entity_ref if entity else value.provider_group_ref
    expected_ref = rebuilt.source_entity_ref if entity else rebuilt.provider_group_ref
    if (
        type(value.source_release_ref) is not str
        or value.source_release_ref != rebuilt.source_release_ref
    ):
        raise _fail()
    prefix = (
        PUBLIC_EVIDENCE_SOURCE_ENTITY_REF_PREFIX
        if entity
        else PUBLIC_EVIDENCE_PROVIDER_GROUP_REF_PREFIX
    )
    _validate_derived_ref(supplied_ref, prefix, expected_ref)
    return rebuilt


def _required_optional_reference(
    release: PublicEvidenceSourceReleaseDescriptor,
    value: object,
    *,
    required: bool,
    entity: bool,
) -> OpaqueSourceEntityReference | ProviderGroupReference | None:
    if not required:
        if value is not None:
            raise _fail()
        return None
    if value is None:
        raise _fail()
    rebuilt = _validate_release_reference(release, value, entity=entity)
    if entity and rebuilt.entity_kind != _SOURCE_ENTITY_KIND.get(release.source_kind):
        raise _fail()
    return rebuilt


def build_provider_directory_network_context(
    release: PublicEvidenceSourceReleaseDescriptor, raw: Mapping[str, object]
) -> ProviderDirectoryNetworkContext:
    """Freeze exact active FHIR witnesses for one network location."""
    fields = frozenset(name for name, _kind in NETWORK_RECORD_FIELDS) | {"role_active"}
    values = _exact_dict(raw, fields)
    if values["role_active"] is not True:
        raise _fail()
    references = []
    for field_name, expected_kind in NETWORK_RECORD_FIELDS:
        reference = validate_evidence_source_record_reference(
            release, values[field_name]
        )
        if reference.record_kind != expected_kind:
            raise _fail()
        references.append(reference)
    return ProviderDirectoryNetworkContext(
        *references, role_active=True, pricing_bridge_state="not_evaluated"
    )


def _validated_network_context(
    release: PublicEvidenceSourceReleaseDescriptor, value: object
) -> ProviderDirectoryNetworkContext:
    if type(value) is not ProviderDirectoryNetworkContext:
        raise _fail()
    if type(value.pricing_bridge_state) is not str or value.pricing_bridge_state != (
        "not_evaluated"
    ):
        raise _fail()
    context_field_map = {
        name: getattr(value, name) for name, _kind in NETWORK_RECORD_FIELDS
    }
    context_field_map["role_active"] = value.role_active
    return build_provider_directory_network_context(release, context_field_map)
