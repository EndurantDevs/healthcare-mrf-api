# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed source-specific rules for dormant public-evidence projections."""

from __future__ import annotations

from types import MappingProxyType
from typing import NamedTuple

from public_evidence.source_record_inclusion_primitives import _canonical_sha256


class AdapterProjectionRule(NamedTuple):
    planned_adapter_contract_id: str
    projection_rule_id: str
    source_kind: str
    record_type: str
    relationship_class: str
    source_record_kinds: tuple[str, ...]
    semantic_limits: tuple[str, ...]


_COMMON_LIMITS = (
    "declared_inventory_membership_not_source_authenticity",
    "authenticated_source_replay_required_not_executed",
    "payload_to_fact_derivation_required_not_executed",
    "record_identity_and_payload_canonicalization_protocols_not_pinned",
    "adapter_contract_is_planned_descriptor_only",
    "positive_evidence_only_not_whole_source_completeness",
    "no_legal_ownership_employment_or_facility_ownership_claim",
    "no_exact_rate_site_payer_confirmed_site_or_site_match_claim",
    "no_independence_confidence_serving_or_publication_authority",
)
_TIC_LIMITS = (
    "tic_group_membership_is_billing_network_association_not_ownership",
    "tic_tin_only_is_preserved_without_fabricated_provider_member",
    "tic_business_name_is_evidence_not_tax_identity_material",
    *_COMMON_LIMITS,
)
_FHIR_LIMITS = (
    "fhir_same_organization_requires_same_authenticated_resource_replay",
    "fhir_location_requires_explicit_reference_edge_replay",
    "fhir_network_location_requires_all_five_resource_edges_replayed",
    "fhir_pricing_bridge_remains_not_evaluated",
    *_COMMON_LIMITS,
)
_NPPES_LIMITS = (
    "nppes_is_npi_enumeration_and_address_evidence_not_tin_crosswalk",
    "nppes_address_is_not_affiliation_or_exact_rate_site_evidence",
    *_COMMON_LIMITS,
)
_HPT_LIMITS = (
    "hpt_is_positive_candidate_evidence_only",
    "hpt_npi_candidate_is_not_nppes_type2_corroboration",
    "hpt_location_is_not_exact_rate_site_evidence",
    *_COMMON_LIMITS,
)

_PLANNED_ADAPTER_IDS = {
    "tic": "healthporta_public_evidence_tic_adapter_v1",
    "public_provider_directory_fhir": ("healthporta_public_evidence_fhir_adapter_v1"),
    "nppes_entity_address": "healthporta_public_evidence_nppes_adapter_v1",
    "public_hpt": "healthporta_public_evidence_hpt_adapter_v1",
}


def _rule(
    source_kind: str,
    record_type: str,
    relationship_class: str,
    source_record_kinds: tuple[str, ...],
    projection_rule_id: str,
    semantic_limits: tuple[str, ...],
) -> AdapterProjectionRule:
    return AdapterProjectionRule(
        planned_adapter_contract_id=_PLANNED_ADAPTER_IDS[source_kind],
        projection_rule_id=projection_rule_id,
        source_kind=source_kind,
        record_type=record_type,
        relationship_class=relationship_class,
        source_record_kinds=tuple(sorted(source_record_kinds)),
        semantic_limits=semantic_limits,
    )


_RULE_VALUES = (
    _rule(
        "tic",
        "tax_identity_relationship",
        "tic_billing_identity_provider_group",
        ("tic_provider_group_occurrence",),
        "tic_billing_group_projection_v1",
        _TIC_LIMITS,
    ),
    _rule(
        "tic",
        "tax_identity_relationship",
        "tic_provider_group_member",
        ("tic_provider_group_occurrence",),
        "tic_group_member_projection_v1",
        _TIC_LIMITS,
    ),
    _rule(
        "tic",
        "tax_identity_name",
        "tic_source_reported_business_name",
        ("tic_provider_group_occurrence",),
        "tic_business_name_projection_v1",
        _TIC_LIMITS,
    ),
    _rule(
        "public_provider_directory_fhir",
        "tax_identity_relationship",
        "fhir_same_organization_identifier",
        ("fhir_organization",),
        "fhir_same_org_identifier_projection_v1",
        _FHIR_LIMITS,
    ),
    _rule(
        "public_provider_directory_fhir",
        "tax_identity_name",
        "fhir_same_organization_reported_name",
        ("fhir_organization",),
        "fhir_same_org_name_projection_v1",
        _FHIR_LIMITS,
    ),
    _rule(
        "nppes_entity_address",
        "npi_enumeration",
        "nppes_npi_enumeration",
        ("nppes_registry_record",),
        "nppes_enumeration_projection_v1",
        _NPPES_LIMITS,
    ),
    _rule(
        "nppes_entity_address",
        "entity_address",
        "nppes_npi_practice_location",
        ("nppes_registry_record",),
        "nppes_practice_address_projection_v1",
        _NPPES_LIMITS,
    ),
    _rule(
        "nppes_entity_address",
        "entity_address",
        "nppes_npi_mailing_address",
        ("nppes_registry_record",),
        "nppes_mailing_address_projection_v1",
        _NPPES_LIMITS,
    ),
    _rule(
        "public_provider_directory_fhir",
        "entity_address",
        "fhir_npi_directory_address",
        ("fhir_npi_resource", "fhir_location"),
        "fhir_npi_address_projection_v1",
        _FHIR_LIMITS,
    ),
    _rule(
        "public_provider_directory_fhir",
        "entity_address",
        "fhir_entity_directory_address",
        ("fhir_organization", "fhir_location"),
        "fhir_entity_address_projection_v1",
        _FHIR_LIMITS,
    ),
    _rule(
        "public_provider_directory_fhir",
        "provider_directory_network_location",
        "fhir_provider_directory_network_location",
        (
            "fhir_npi_resource",
            "fhir_practitioner_role",
            "fhir_location",
            "fhir_network",
            "fhir_insurance_plan",
        ),
        "fhir_network_location_projection_v1",
        _FHIR_LIMITS,
    ),
    _rule(
        "public_hpt",
        "tax_identity_relationship",
        "hpt_hospital_tax_identity_entity_candidate",
        ("hpt_hospital_record",),
        "hpt_entity_candidate_projection_v1",
        _HPT_LIMITS,
    ),
    _rule(
        "public_hpt",
        "tax_identity_relationship",
        "hpt_hospital_tax_identity_npi_candidate",
        ("hpt_hospital_record",),
        "hpt_npi_candidate_projection_v1",
        _HPT_LIMITS,
    ),
    _rule(
        "public_hpt",
        "tax_identity_name",
        "hpt_source_reported_hospital_name_candidate",
        ("hpt_hospital_record",),
        "hpt_name_candidate_projection_v1",
        _HPT_LIMITS,
    ),
    _rule(
        "public_hpt",
        "entity_address",
        "hpt_entity_location_candidate",
        ("hpt_hospital_record",),
        "hpt_location_candidate_projection_v1",
        _HPT_LIMITS,
    ),
)

ADAPTER_PROJECTION_RULES = MappingProxyType(
    {(rule.record_type, rule.relationship_class): rule for rule in _RULE_VALUES}
)


def adapter_projection_rule_descriptor_sha256(
    rule: AdapterProjectionRule,
) -> str:
    """Freeze the exact closed rule semantics into every projection."""
    if type(rule) is not AdapterProjectionRule:
        raise TypeError("adapter_projection_rule_invalid")
    return _canonical_sha256(
        "adapter_projection_rule",
        {
            "planned_adapter_contract_id": rule.planned_adapter_contract_id,
            "projection_rule_id": rule.projection_rule_id,
            "source_kind": rule.source_kind,
            "record_type": rule.record_type,
            "relationship_class": rule.relationship_class,
            "source_record_kinds": list(rule.source_record_kinds),
            "semantic_limits": list(rule.semantic_limits),
        },
    )
