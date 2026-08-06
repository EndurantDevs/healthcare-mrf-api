# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Correctness and determinism for normalized public-evidence records."""

from __future__ import annotations

from copy import copy

import pytest

from public_evidence import evidence_record_contract as record
from public_evidence import evidence_record_primitives as primitive
from public_evidence import source_release_contract as release
from tests.public_evidence_record_support import (
    SYNTHETIC_TYPE_1_NPI,
    address_input,
    canonical_address,
    enumeration_input,
    name_input,
    network_input,
    opaque_tax_identity,
    relationship_input,
    source_record,
    source_release,
)

RELATIONSHIP_CASES = (
    ("tic", "tic_billing_identity_provider_group", "ein", "members_present"),
    ("tic", "tic_billing_identity_provider_group", "npi", "tin_only"),
    ("tic", "tic_provider_group_member", "ein", "members_present"),
    (
        "public_provider_directory_fhir",
        "fhir_same_organization_identifier",
        "ein",
        None,
    ),
    (
        "public_hpt",
        "hpt_hospital_tax_identity_entity_candidate",
        "ein",
        None,
    ),
    (
        "public_hpt",
        "hpt_hospital_tax_identity_npi_candidate",
        "ein",
        None,
    ),
)
NAME_CASES = (
    ("tic", "tic_source_reported_business_name"),
    (
        "public_provider_directory_fhir",
        "fhir_same_organization_reported_name",
    ),
    ("public_hpt", "hpt_source_reported_hospital_name_candidate"),
)
ADDRESS_CASES = (
    ("nppes_entity_address", "nppes_npi_practice_location"),
    ("nppes_entity_address", "nppes_npi_mailing_address"),
    ("public_provider_directory_fhir", "fhir_npi_directory_address"),
    ("public_provider_directory_fhir", "fhir_entity_directory_address"),
    ("public_hpt", "hpt_entity_location_candidate"),
)


@pytest.mark.parametrize(
    ("source_kind", "relationship", "tin_type", "membership_state"),
    RELATIONSHIP_CASES,
)
def test_valid_tax_identity_relationship_matrix(
    source_kind: str,
    relationship: str,
    tin_type: str,
    membership_state: str | None,
) -> None:
    source = source_release(source_kind)
    normalized = record.build_public_evidence_record(
        source,
        relationship_input(
            source,
            relationship,
            tin_type=tin_type,
            membership_state=membership_state,
        ),
    )

    assert normalized.record_type == "tax_identity_relationship"
    assert normalized.evidence.relationship_class == relationship
    assert normalized.evidence.tax_identity.tin_type == tin_type
    assert normalized.evidence.membership_state == membership_state
    assert normalized.evidence.candidate_only is relationship.startswith("hpt_")
    assert normalized.authority_state.legal_ownership_claimed is False


def test_tin_only_group_is_preserved_without_fabricating_an_npi() -> None:
    source = source_release("tic")
    normalized = record.build_public_evidence_record(
        source,
        relationship_input(
            source,
            "tic_billing_identity_provider_group",
            tin_type="npi",
            membership_state="tin_only",
        ),
    )

    assert normalized.evidence.provider_group is not None
    assert normalized.evidence.related_npi is None
    assert normalized.evidence.membership_state == "tin_only"
    assert not hasattr(normalized.evidence, "billing_identifier_npi")


@pytest.mark.parametrize(("source_kind", "relationship"), NAME_CASES)
def test_valid_separate_tax_identity_name_matrix(
    source_kind: str, relationship: str
) -> None:
    source = source_release(source_kind)
    normalized = record.build_public_evidence_record(
        source, name_input(source, relationship)
    )

    assert normalized.record_type == "tax_identity_name"
    assert normalized.evidence.relationship_class == relationship
    assert normalized.evidence.source_reported_name == "Synthetic Health Group"
    assert len(normalized.evidence.normalized_name_sha256) == 64


def test_business_name_is_excluded_from_tax_identity_derivation() -> None:
    source = source_release("tic")
    first = record.build_public_evidence_record(
        source,
        name_input(
            source,
            "tic_source_reported_business_name",
            source_reported_name="Synthetic Health Group",
        ),
    )
    second = record.build_public_evidence_record(
        source,
        name_input(
            source,
            "tic_source_reported_business_name",
            source_reported_name="Synthetic Medical Collective",
        ),
    )

    assert first.evidence.tax_identity.tax_identity_ref == (
        second.evidence.tax_identity.tax_identity_ref
    )
    assert first.evidence_ref != second.evidence_ref
    assert first.evidence.normalized_name_sha256 != (
        second.evidence.normalized_name_sha256
    )


@pytest.mark.parametrize(
    "npi_entity_type", ("individual_type_1", "organization_type_2")
)
@pytest.mark.parametrize("enumeration_state", ("active", "deactivated"))
def test_nppes_enumeration_keeps_type_and_status_separate(
    npi_entity_type: str, enumeration_state: str
) -> None:
    source = source_release("nppes_entity_address")
    normalized = record.build_public_evidence_record(
        source,
        enumeration_input(
            source,
            npi_entity_type=npi_entity_type,
            enumeration_state=enumeration_state,
        ),
    )

    assert normalized.evidence.npi_entity_type == npi_entity_type
    assert normalized.evidence.enumeration_state == enumeration_state
    assert normalized.evidence.relationship_class == "nppes_npi_enumeration"


@pytest.mark.parametrize(("source_kind", "relationship"), ADDRESS_CASES)
def test_valid_entity_address_matrix(source_kind: str, relationship: str) -> None:
    source = source_release(source_kind)
    normalized = record.build_public_evidence_record(
        source, address_input(source, relationship)
    )

    assert normalized.record_type == "entity_address"
    assert normalized.evidence.relationship_class == relationship
    assert normalized.evidence.address.address_key.endswith("0001")
    assert normalized.evidence.address.address_site_key.endswith("0002")
    assert normalized.evidence.address.geo_quality == "rooftop"
    assert normalized.evidence.address.freshness_state == "current"
    assert normalized.authority_state.address_selection_authority == "none"


def test_hpt_address_attaches_to_source_entity_not_tax_identity() -> None:
    source = source_release("public_hpt")
    normalized = record.build_public_evidence_record(
        source, address_input(source, "hpt_entity_location_candidate")
    )

    assert normalized.evidence.source_entity is not None
    assert normalized.evidence.subject_npi is None
    assert normalized.evidence.candidate_only is True
    assert not hasattr(normalized.evidence, "tax_identity")


def test_provider_directory_network_location_keeps_bridge_unassessed() -> None:
    source = source_release("public_provider_directory_fhir")
    normalized = record.build_public_evidence_record(source, network_input(source))

    assert normalized.record_type == "provider_directory_network_location"
    assert len(normalized.source_records) == 5
    assert normalized.evidence.npi == SYNTHETIC_TYPE_1_NPI
    assert normalized.evidence.network_context.role_active is True
    assert normalized.evidence.network_context.pricing_bridge_state == "not_evaluated"
    assert normalized.authority_state.payer_confirmed_site_claimed is False


def test_hpt_npi_link_remains_candidate_without_type2_promotion() -> None:
    source = source_release("public_hpt")
    normalized = record.build_public_evidence_record(
        source,
        relationship_input(
            source,
            "hpt_hospital_tax_identity_npi_candidate",
        ),
    )

    assert normalized.evidence.related_npi == SYNTHETIC_TYPE_1_NPI
    assert normalized.evidence.candidate_only is True
    assert not hasattr(normalized.evidence, "npi_entity_type")


def test_source_record_order_and_mapping_order_do_not_change_identity() -> None:
    source = source_release("public_provider_directory_fhir")
    first_input = network_input(source)
    first = record.build_public_evidence_record(source, first_input)
    reordered_field_map = dict(reversed(tuple(first_input.items())))
    reordered_field_map["source_records"] = tuple(
        reversed(first_input["source_records"])
    )
    second = record.build_public_evidence_record(source, reordered_field_map)

    assert first == second
    assert first.evidence_ref == second.evidence_ref
    assert first.contract_sha256 == second.contract_sha256


def test_distinct_source_occurrences_remain_distinct_records() -> None:
    source = source_release("tic")
    first_input = relationship_input(
        source,
        "tic_provider_group_member",
        membership_state="members_present",
    )
    second_input = copy(first_input)
    second_input["source_records"] = (
        source_record(source, "tic_provider_group_occurrence", seed="3"),
    )

    first = record.build_public_evidence_record(source, first_input)
    second = record.build_public_evidence_record(source, second_input)
    assert first.evidence == second.evidence
    assert first.evidence_ref != second.evidence_ref
    assert first.contract_sha256 != second.contract_sha256


def test_identity_and_address_field_changes_affect_record_digest() -> None:
    source = source_release("tic")
    first_input = relationship_input(
        source,
        "tic_provider_group_member",
        membership_state="members_present",
    )
    second_input = copy(first_input)
    second_input["tax_identity"] = opaque_tax_identity(seed="2")
    first = record.build_public_evidence_record(source, first_input)
    second = record.build_public_evidence_record(source, second_input)
    assert first.evidence_ref != second.evidence_ref

    nppes = source_release("nppes_entity_address")
    address_raw = address_input(nppes, "nppes_npi_practice_location")
    changed_raw = copy(address_raw)
    changed_address = canonical_address(purpose="nppes_practice_location")
    changed_raw["address"] = changed_address._replace(zip5="54321")
    assert record.build_public_evidence_record(nppes, address_raw).evidence_ref != (
        record.build_public_evidence_record(nppes, changed_raw).evidence_ref
    )


def test_temporal_scope_is_contained_by_source_release() -> None:
    source = source_release("nppes_entity_address")
    raw = enumeration_input(source)
    raw["observed_at"] = "2026-07-03T00:00:00Z"
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)

    raw = enumeration_input(source)
    raw["effective_interval"] = release.CanonicalUtcInterval(
        "2026-06-30T23:59:59Z", None
    )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)

    raw = enumeration_input(source, enumeration_state="deactivated")
    raw["effective_interval"] = release.CanonicalUtcInterval(
        "2026-07-01T00:00:00Z", None
    )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)


def test_record_revalidation_defensively_rebuilds_nested_state() -> None:
    source = source_release("tic")
    original = record.build_public_evidence_record(
        source,
        relationship_input(
            source,
            "tic_provider_group_member",
            membership_state="members_present",
        ),
    )
    rebuilt = record.validate_public_evidence_record(original)

    assert rebuilt == original
    assert rebuilt is not original
    assert rebuilt.release is not original.release
    assert rebuilt.source_records is not original.source_records


def test_every_descriptor_representation_is_redacted() -> None:
    tic = source_release("tic")
    relationship = record.build_public_evidence_record(
        tic,
        relationship_input(
            tic,
            "tic_provider_group_member",
            membership_state="members_present",
        ),
    )
    name = record.build_public_evidence_record(
        tic,
        name_input(
            tic,
            "tic_source_reported_business_name",
            source_reported_name="Synthetic Confidential Display Name",
        ),
    )
    nppes = source_release("nppes_entity_address")
    enumeration = record.build_public_evidence_record(nppes, enumeration_input(nppes))
    hpt = source_release("public_hpt")
    address = record.build_public_evidence_record(
        hpt, address_input(hpt, "hpt_entity_location_candidate")
    )
    fhir = source_release("public_provider_directory_fhir")
    network = record.build_public_evidence_record(fhir, network_input(fhir))
    rendered = " ".join(
        repr(descriptor)
        for descriptor in (
            relationship,
            relationship.evidence,
            relationship.evidence.tax_identity,
            relationship.evidence.provider_group,
            relationship.source_records[0],
            name.evidence,
            enumeration.evidence,
            address.evidence,
            address.evidence.source_entity,
            address.evidence.address,
            network.evidence,
            network.evidence.network_context,
            network.authority_state,
        )
    )
    assert "Synthetic Confidential Display Name" not in rendered
    assert relationship.evidence.tax_identity.full_hmac_sha256 not in rendered
    assert relationship.source_records[0].record_hmac_sha256 not in rendered


def test_frozen_cross_adapter_record_identity_vectors() -> None:
    tic = source_release("tic")
    nppes = source_release("nppes_entity_address")
    fhir = source_release("public_provider_directory_fhir")
    case_by_name = {
        "relationship": record.build_public_evidence_record(
            tic,
            relationship_input(
                tic,
                "tic_provider_group_member",
                membership_state="members_present",
            ),
        ),
        "name": record.build_public_evidence_record(
            tic, name_input(tic, "tic_source_reported_business_name")
        ),
        "enumeration": record.build_public_evidence_record(
            nppes, enumeration_input(nppes)
        ),
        "address": record.build_public_evidence_record(
            nppes, address_input(nppes, "nppes_npi_practice_location")
        ),
        "network": record.build_public_evidence_record(fhir, network_input(fhir)),
    }
    expected_by_name = {
        "relationship": "peev1_ny9gGNNxLe-ckkMt-hkeHgYfqvJONypUS7rqq8jv6gA",
        "name": "peev1_eJ_nv4iELWMqDK_umCoGElm_61VBO86nCdDfWL6eVl8",
        "enumeration": "peev1_Mj5oeZlNfveFjKpat1G0-VLMfnf47wSFEK50O9zpxJs",
        "address": "peev1_YU0M9gA2cJucWKoP5m0kT9HagdLFX7w1gxfNsxJWrOc",
        "network": "peev1_U33OmMUA2SqVtJhwv2d_PTkAwtAS6dvzaYep4YwlERM",
    }
    assert {
        name: evidence_record.evidence_ref
        for name, evidence_record in case_by_name.items()
    } == expected_by_name
    assert opaque_tax_identity().tax_identity_ref == (
        "petax1_CgB-kPEidKeYTZEcvud0I0tsZiR0EjRTsIGmZwrtaSY"
    )
