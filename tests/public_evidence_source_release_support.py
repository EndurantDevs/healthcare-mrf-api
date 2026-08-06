# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic fixtures shared by public evidence source-release tests."""

from __future__ import annotations

from public_evidence import source_release_contract as release

COMMON_LIMITS = (
    "attestation_not_independent_source_closure_proof",
    "contract_digest_not_authenticity_or_source_authority_proof",
    "release_descriptor_not_replacement_deletion_or_current_pointer_authority",
)
JSON_OR_CONTAINER_IDENTITIES = (
    "logical_json_sha256_v1",
    "raw_container_sha256_v1",
)
POLICY_MATRIX = {
    "tic": {
        "identity_kind": "immutable_artifact",
        "content_identity_kinds": JSON_OR_CONTAINER_IDENTITIES,
        "authority": "payer_transparency_in_coverage",
        "trust": "authoritative_tic_rate_group_association",
        "rights": "tic_public_access_processing_retention_reviewed",
        "mode": "declared_complete_artifact",
        "evidence_contract_id": "tic_artifact_record_attestation_v1",
        "count_unit": "tic_negotiated_rate_record",
        "source_binding_source_types": ("in_network",),
        "limits": (
            "provider_group_membership_not_legal_ownership",
            "tic_rate_not_bound_to_exact_provider_site",
            "tic_provider_rate_association_not_service_capability_or_utilization",
            "tic_shadow_binding_requires_source_coordinate_revalidation",
            *COMMON_LIMITS,
        ),
    },
    "public_provider_directory_fhir": {
        "identity_kind": "immutable_dataset",
        "content_identity_kinds": JSON_OR_CONTAINER_IDENTITIES,
        "authority": "public_payer_provider_directory_fhir",
        "trust": "public_provider_directory_source_evidence",
        "rights": "provider_directory_public_access_processing_retention_reviewed",
        "mode": "declared_complete_dataset",
        "evidence_contract_id": "provider_directory_fhir_resource_attestation_v1",
        "count_unit": "fhir_resource",
        "source_binding_source_types": (),
        "limits": (
            "directory_relationship_not_legal_ownership",
            "directory_location_not_exact_rate_site",
            "location_corroboration_requires_exact_npi_active_role_location_plan_network_bridge",
            *COMMON_LIMITS,
        ),
    },
    "nppes_entity_address": {
        "identity_kind": "immutable_dataset",
        "content_identity_kinds": ("raw_container_sha256_v1",),
        "authority": "cms_nppes_npi_registry",
        "trust": "authoritative_npi_enumeration_and_registry_record_status",
        "rights": "nppes_public_access_processing_retention_reviewed",
        "mode": "declared_complete_dataset",
        "evidence_contract_id": "nppes_registry_record_attestation_v1",
        "count_unit": "nppes_registry_record",
        "source_binding_source_types": (),
        "limits": (
            "non_system_fields_provider_or_authorized_official_reported",
            "nppes_not_payer_confirmed",
            "nppes_has_no_plan_network_binding",
            "nppes_not_tin_address_proof",
            "nppes_not_affiliation_or_ownership_proof",
            "nppes_not_credentialing_proof",
            "nppes_not_current_service_site_proof",
            "nppes_not_universal_ein_npi_crosswalk",
            "registry_address_not_exact_rate_site",
            *COMMON_LIMITS,
        ),
    },
    "public_hpt": {
        "identity_kind": "immutable_artifact",
        "content_identity_kinds": JSON_OR_CONTAINER_IDENTITIES,
        "authority": "hospital_published_hpt_machine_readable_artifact",
        "trust": "public_hospital_entity_location_candidate",
        "rights": "hpt_public_access_processing_retention_reviewed",
        "mode": "positive_evidence_only",
        "evidence_contract_id": "public_hpt_observation_attestation_v1",
        "count_unit": "hpt_candidate_record",
        "source_binding_source_types": (),
        "limits": (
            "cms_hpt_rule_schema_is_regulatory_context_not_artifact_authorship",
            "hospital_evidence_not_universal_ein_npi_crosswalk",
            "hospital_location_not_exact_rate_site",
            *COMMON_LIMITS,
        ),
    },
}


def sha256_text(character: str) -> str:
    return character * 64


def _source_binding(
    source_kind: str,
    artifact: release.ImmutablePublicSourceIdentity,
) -> release.OpaqueSourceBindingReference | None:
    if source_kind != "tic":
        return None
    return release.OpaqueSourceBindingReference(
        release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
        "in_network",
        artifact.content_identity_kind,
        artifact.content_sha256,
        sha256_text("d"),
        sha256_text("e"),
    )


def release_input(source_kind: str = "tic") -> dict[str, object]:
    policy = POLICY_MATRIX[source_kind]
    is_positive_only = policy["mode"] == "positive_evidence_only"
    content_identity_kind = policy["content_identity_kinds"][0]
    content_sha256 = sha256_text("a")
    artifact = release.ImmutablePublicSourceIdentity(
        policy["identity_kind"],
        content_identity_kind,
        release.derive_public_evidence_identity_ref(
            policy["identity_kind"],
            content_identity_kind,
            content_sha256,
        ),
        content_sha256,
    )
    attestation = release.PublicEvidenceCompletenessAttestation(
        policy["mode"],
        policy["evidence_contract_id"],
        policy["count_unit"],
        artifact.content_sha256,
        None if is_positive_only else 7,
        0 if is_positive_only else 7,
        sha256_text("b"),
    )
    return {
        "source_kind": source_kind,
        "authority_classification": policy["authority"],
        "trust_classification": policy["trust"],
        "semantic_limits": policy["limits"],
        "artifact_identity": artifact,
        "completeness_attestation": attestation,
        "rights_classification": policy["rights"],
        "rights_proof_sha256": sha256_text("c"),
        "source_binding": _source_binding(source_kind, artifact),
        "observed_interval": release.CanonicalUtcInterval(
            "2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z"
        ),
        "effective_interval": release.CanonicalUtcInterval(
            "2026-07-01T00:00:00Z", None
        ),
        "artifact_bytes_verified": True,
        "public_access_verified": True,
        "processing_retention_rights_verified": True,
        "semantic_limits_verified": True,
        "completeness_attestation_verified": True,
        "legal_ownership_claimed": False,
        "exact_rate_site_claimed": False,
        "whole_source_complete": False,
        "redistribution_enabled": False,
        "export_enabled": False,
        "publication_enabled": False,
        "replacement_enabled": False,
        "deletion_enabled": False,
        "retirement_enabled": False,
        "supersession_enabled": False,
    }
