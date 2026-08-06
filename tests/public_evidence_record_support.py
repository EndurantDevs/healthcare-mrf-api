# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic builders shared by normalized public-evidence record tests."""

from __future__ import annotations

from public_evidence import evidence_record_contract as record
from public_evidence import evidence_record_primitives as primitive
from public_evidence import evidence_record_token_policy as token_policy
from public_evidence import source_release_contract as release
from tests.public_evidence_source_release_support import release_input

SYNTHETIC_TYPE_1_NPI = "1234567893"
SYNTHETIC_TYPE_2_NPI = "1000000004"


def source_release(source_kind: str) -> release.PublicEvidenceSourceReleaseDescriptor:
    return release.build_public_evidence_source_release(release_input(source_kind))


def opaque_tax_identity(
    tin_type: str = "ein", *, seed: str = "1"
) -> primitive.OpaqueTaxIdentityReference:
    full_hmac = seed * 64
    contract_id = (
        token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT
        if tin_type == "npi"
        else token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT
    )
    policy_id = (
        "healthporta-tax-identity-hmac-sha256-v1:synthetic"
        if tin_type == "npi"
        else "ptg-tin-hmac-sha256-v1:synthetic"
    )
    return token_policy.build_opaque_tax_identity(
        {
            "tin_type": tin_type,
            "token_policy_contract_id": contract_id,
            "token_policy_id": policy_id,
            "token_policy_descriptor_sha256": (
                token_policy.token_policy_descriptor_sha256(contract_id, policy_id)
            ),
            "locator_128": full_hmac[:32],
            "full_hmac_sha256": full_hmac,
        }
    )


def source_record(
    source: release.PublicEvidenceSourceReleaseDescriptor,
    record_kind: str,
    *,
    seed: str = "2",
) -> primitive.EvidenceSourceRecordReference:
    return primitive.build_evidence_source_record_reference(
        source,
        {
            "record_kind": record_kind,
            "identity_contract_id": "synthetic_record_hmac_v1",
            "record_hmac_sha256": seed * 64,
            "payload_sha256": "b" * 64,
        },
    )


def source_entity(
    source: release.PublicEvidenceSourceReleaseDescriptor,
    *,
    entity_kind: str | None = None,
) -> primitive.OpaqueSourceEntityReference:
    if entity_kind is None:
        entity_kind = {
            "public_hpt": "hpt_hospital_entity",
            "public_provider_directory_fhir": "fhir_organization",
        }.get(source.source_kind, "synthetic_organization")
    return record.build_opaque_source_entity_reference(
        source,
        {
            "entity_kind": entity_kind,
            "identity_contract_id": "synthetic_entity_digest_v1",
            "identity_sha256": "c" * 64,
        },
    )


def provider_group(
    source: release.PublicEvidenceSourceReleaseDescriptor,
) -> primitive.ProviderGroupReference:
    return record.build_provider_group_reference(
        source,
        {
            "identity_contract_id": "synthetic_provider_group_digest_v1",
            "identity_sha256": "d" * 64,
        },
    )


def canonical_address(
    *,
    purpose: str,
    selection_eligible: bool = True,
) -> primitive.CanonicalAddressEvidence:
    return primitive.build_canonical_address_evidence(
        {
            "address_key": "00000000-0000-4000-8000-000000000001",
            "address_site_key": "00000000-0000-4000-8000-000000000002",
            "canonicalization_contract_id": "address_canonical_uuid_v1",
            "purpose": purpose,
            "zip5": "12345",
            "geo_derivation_contract_id": "synthetic_geocode_v1",
            "geo_quality": "rooftop",
            "freshness_state": "current",
            "freshness_rule_version": "freshness_policy_v1",
            "freshness_as_of": "2026-07-01T12:00:00Z",
            "selection_rule_version": "address_selection_v1",
            "selection_eligible": selection_eligible,
        }
    )


def _common_input(
    record_type: str,
    records: tuple[primitive.EvidenceSourceRecordReference, ...],
) -> dict[str, object]:
    return {
        "record_type": record_type,
        "source_records": records,
        "observed_at": "2026-07-01T12:00:00Z",
        "effective_interval": release.CanonicalUtcInterval(
            "2026-07-01T00:00:00Z", None
        ),
    }


def relationship_input(
    source_release_descriptor: release.PublicEvidenceSourceReleaseDescriptor,
    relationship: str,
    *,
    tin_type: str = "ein",
    membership_state: str | None = None,
) -> dict[str, object]:
    is_tic = relationship.startswith("tic_")
    is_fhir = relationship.startswith("fhir_")
    is_npi_link = relationship in {
        "tic_provider_group_member",
        "fhir_same_organization_identifier",
        "hpt_hospital_tax_identity_npi_candidate",
    }
    kind = (
        "tic_provider_group_occurrence"
        if is_tic
        else "fhir_organization" if is_fhir else "hpt_hospital_record"
    )
    raw = _common_input(
        "tax_identity_relationship",
        (source_record(source_release_descriptor, kind),),
    )
    raw.update(
        {
            "relationship_class": relationship,
            "tax_identity": opaque_tax_identity(tin_type),
            "provider_group": (
                provider_group(source_release_descriptor) if is_tic else None
            ),
            "related_npi": SYNTHETIC_TYPE_1_NPI if is_npi_link else None,
            "source_entity": (
                source_entity(source_release_descriptor) if not is_tic else None
            ),
            "membership_state": (membership_state if is_tic else None),
        }
    )
    return raw


def name_input(
    source: release.PublicEvidenceSourceReleaseDescriptor,
    relationship: str,
    *,
    source_reported_name: str = "Synthetic Health Group",
) -> dict[str, object]:
    is_tic = relationship.startswith("tic_")
    is_fhir = relationship.startswith("fhir_")
    kind = (
        "tic_provider_group_occurrence"
        if is_tic
        else "fhir_organization" if is_fhir else "hpt_hospital_record"
    )
    raw = _common_input("tax_identity_name", (source_record(source, kind),))
    raw.update(
        {
            "relationship_class": relationship,
            "tax_identity": opaque_tax_identity(),
            "provider_group": provider_group(source) if is_tic else None,
            "source_entity": source_entity(source) if not is_tic else None,
            "source_reported_name": source_reported_name,
        }
    )
    return raw


def enumeration_input(
    source: release.PublicEvidenceSourceReleaseDescriptor,
    *,
    npi_entity_type: str = "individual_type_1",
    enumeration_state: str = "active",
) -> dict[str, object]:
    raw = _common_input(
        "npi_enumeration",
        (source_record(source, "nppes_registry_record"),),
    )
    raw.update(
        {
            "relationship_class": "nppes_npi_enumeration",
            "npi": (
                SYNTHETIC_TYPE_1_NPI
                if npi_entity_type == "individual_type_1"
                else SYNTHETIC_TYPE_2_NPI
            ),
            "npi_entity_type": npi_entity_type,
            "enumeration_state": enumeration_state,
        }
    )
    if enumeration_state == "deactivated":
        raw["effective_interval"] = release.CanonicalUtcInterval(
            "2026-07-01T00:00:00Z", "2026-07-01T18:00:00Z"
        )
    return raw


def address_input(
    source_release_descriptor: release.PublicEvidenceSourceReleaseDescriptor,
    relationship: str,
) -> dict[str, object]:
    if relationship.startswith("nppes_"):
        kinds = ("nppes_registry_record",)
        purpose = (
            "nppes_mailing"
            if relationship.endswith("mailing_address")
            else "nppes_practice_location"
        )
        subject_npi = SYNTHETIC_TYPE_1_NPI
        entity = None
    elif relationship == "hpt_entity_location_candidate":
        kinds = ("hpt_hospital_record",)
        purpose = "hpt_hospital_location_candidate"
        subject_npi = None
        entity = source_entity(source_release_descriptor)
    else:
        is_npi = relationship == "fhir_npi_directory_address"
        kinds = (
            ("fhir_npi_resource", "fhir_location")
            if is_npi
            else ("fhir_organization", "fhir_location")
        )
        purpose = "provider_directory_location"
        subject_npi = SYNTHETIC_TYPE_1_NPI if is_npi else None
        entity = None if is_npi else source_entity(source_release_descriptor)
    source_records = tuple(
        source_record(source_release_descriptor, kind, seed=str(index + 1))
        for index, kind in enumerate(kinds)
    )
    raw = _common_input("entity_address", source_records)
    raw.update(
        {
            "relationship_class": relationship,
            "subject_npi": subject_npi,
            "source_entity": entity,
            "address": canonical_address(purpose=purpose),
        }
    )
    return raw


def network_input(
    source_release_descriptor: release.PublicEvidenceSourceReleaseDescriptor,
) -> dict[str, object]:
    kinds = (
        "fhir_npi_resource",
        "fhir_practitioner_role",
        "fhir_location",
        "fhir_network",
        "fhir_insurance_plan",
    )
    source_records = tuple(
        source_record(source_release_descriptor, kind, seed=str(index + 1))
        for index, kind in enumerate(kinds)
    )
    context = record.build_provider_directory_network_context(
        source_release_descriptor,
        {
            "npi_source_record": source_records[0],
            "practitioner_role_source_record": source_records[1],
            "location_source_record": source_records[2],
            "network_source_record": source_records[3],
            "insurance_plan_source_record": source_records[4],
            "role_active": True,
        },
    )
    raw = _common_input("provider_directory_network_location", source_records)
    raw.update(
        {
            "relationship_class": "fhir_provider_directory_network_location",
            "npi": SYNTHETIC_TYPE_1_NPI,
            "address": canonical_address(purpose="provider_directory_location"),
            "network_context": context,
        }
    )
    return raw
