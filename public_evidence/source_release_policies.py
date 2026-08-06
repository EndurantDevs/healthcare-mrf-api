# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed public-source policies for the publication-disabled release contract."""

from __future__ import annotations

from types import MappingProxyType
from typing import NamedTuple


class SourcePolicy(NamedTuple):
    """Deeply immutable fixed policy consumed by release validation."""

    identity_kind: str
    content_identity_kinds: tuple[str, ...]
    authority: str
    trust: str
    rights: str
    attestation_mode: str
    evidence_contract_id: str
    count_unit: str
    semantic_limits: tuple[str, ...]
    source_binding_source_types: tuple[str, ...]
    source_binding_required: bool


_COMMON_SEMANTIC_LIMITS = (
    "attestation_not_independent_source_closure_proof",
    "contract_digest_not_authenticity_or_source_authority_proof",
    "release_descriptor_not_replacement_deletion_or_current_pointer_authority",
)
_JSON_OR_CONTAINER_IDENTITIES = (
    "logical_json_sha256_v1",
    "raw_container_sha256_v1",
)

SOURCE_POLICIES = MappingProxyType({
    "tic": SourcePolicy(
        identity_kind="immutable_artifact",
        content_identity_kinds=_JSON_OR_CONTAINER_IDENTITIES,
        authority="payer_transparency_in_coverage",
        trust="authoritative_tic_rate_group_association",
        rights="tic_public_access_processing_retention_reviewed",
        attestation_mode="declared_complete_artifact",
        evidence_contract_id="tic_artifact_record_attestation_v1",
        count_unit="tic_negotiated_rate_record",
        semantic_limits=(
            "provider_group_membership_not_legal_ownership",
            "tic_rate_not_bound_to_exact_provider_site",
            "tic_provider_rate_association_not_service_capability_or_utilization",
            "tic_shadow_binding_requires_source_coordinate_revalidation",
            *_COMMON_SEMANTIC_LIMITS,
        ),
        source_binding_source_types=("in_network",),
        source_binding_required=True,
    ),
    "public_provider_directory_fhir": SourcePolicy(
        identity_kind="immutable_dataset",
        content_identity_kinds=_JSON_OR_CONTAINER_IDENTITIES,
        authority="public_payer_provider_directory_fhir",
        trust="public_provider_directory_source_evidence",
        rights="provider_directory_public_access_processing_retention_reviewed",
        attestation_mode="declared_complete_dataset",
        evidence_contract_id="provider_directory_fhir_resource_attestation_v1",
        count_unit="fhir_resource",
        semantic_limits=(
            "directory_relationship_not_legal_ownership",
            "directory_location_not_exact_rate_site",
            "location_corroboration_requires_exact_npi_active_role_location_plan_network_bridge",
            *_COMMON_SEMANTIC_LIMITS,
        ),
        source_binding_source_types=(),
        source_binding_required=False,
    ),
    "nppes_entity_address": SourcePolicy(
        identity_kind="immutable_dataset",
        content_identity_kinds=("raw_container_sha256_v1",),
        authority="cms_nppes_npi_registry",
        trust="authoritative_npi_enumeration_and_registry_record_status",
        rights="nppes_public_access_processing_retention_reviewed",
        attestation_mode="declared_complete_dataset",
        evidence_contract_id="nppes_registry_record_attestation_v1",
        count_unit="nppes_registry_record",
        semantic_limits=(
            "non_system_fields_provider_or_authorized_official_reported",
            "nppes_not_payer_confirmed",
            "nppes_has_no_plan_network_binding",
            "nppes_not_tin_address_proof",
            "nppes_not_affiliation_or_ownership_proof",
            "nppes_not_credentialing_proof",
            "nppes_not_current_service_site_proof",
            "nppes_not_universal_ein_npi_crosswalk",
            "registry_address_not_exact_rate_site",
            *_COMMON_SEMANTIC_LIMITS,
        ),
        source_binding_source_types=(),
        source_binding_required=False,
    ),
    "public_hpt": SourcePolicy(
        identity_kind="immutable_artifact",
        content_identity_kinds=_JSON_OR_CONTAINER_IDENTITIES,
        authority="hospital_published_hpt_machine_readable_artifact",
        trust="public_hospital_entity_location_candidate",
        rights="hpt_public_access_processing_retention_reviewed",
        attestation_mode="positive_evidence_only",
        evidence_contract_id="public_hpt_observation_attestation_v1",
        count_unit="hpt_candidate_record",
        semantic_limits=(
            "cms_hpt_rule_schema_is_regulatory_context_not_artifact_authorship",
            "hospital_evidence_not_universal_ein_npi_crosswalk",
            "hospital_location_not_exact_rate_site",
            *_COMMON_SEMANTIC_LIMITS,
        ),
        source_binding_source_types=(),
        source_binding_required=False,
    ),
})
