# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contract and lifecycle tests for public evidence source releases."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from public_evidence import source_release_contract as release
from tests.public_evidence_source_release_support import (
    POLICY_MATRIX,
    release_input,
    sha256_text,
)

GOLDEN_CONTRACT_SHA256 = {
    "tic": "f6c66be8cf2bcc89c336b2d33ca1ae74ada68c1f8e5608c7fcfc6c091d7fc0b5",
    "public_provider_directory_fhir": (
        "31a7d4a81f88f3d015087f40aa70e13840e5cbc45c00c4bad9448e5ac0b2e106"
    ),
    "nppes_entity_address": (
        "cfac98c8fea2a7c97a50d6c4f38ace1055af9fb14861fbfcdcd0605e5d4c8d4d"
    ),
    "public_hpt": (
        "c89ef2ac788ec3847c189b54165e2914e82ca17c09e82a8b120ef66592ee3e7e"
    ),
}
GOLDEN_REFERENCES = {
    "tic": (
        "peid1_WgQLDrEmib6loGwrGJweoOlVE4SWXZtP0P4VHAWea_Y",
        "perun1_ckVyLu_LY1jTeCmMAypZT1uTDKdWbEM1kxPExNEr6D4",
        "perel1_4woLYE2NfqyS2M2494kSOU9OatZ71OdHO2_OGFq0VZI",
    ),
    "public_provider_directory_fhir": (
        "peid1_XEMdDTV84TGY_jbCqC-gwQlB1k908CPvYlDlYHOuJMM",
        "perun1_bQ1jEtDtQeH9LShtQlDZvgvak_BQekTteeRseViPdTM",
        "perel1_qPRO7HHyZ4MQ4IWt6jZwzbR-FWrkmSpEEEQ3GfdBuRo",
    ),
    "nppes_entity_address": (
        "peid1_jyE84RE0lKS2LcvaX0F-qPWrQMUxKJOHgCQwDboHMBY",
        "perun1_VGm1F5EXE8oEN8OIIKm5BVj2Lleu_CsXao_pp_j7-tI",
        "perel1_9S7qV6eHCtBgXXISERTACCcmutu5ox1oUbqpWWyFwo8",
    ),
    "public_hpt": (
        "peid1_WgQLDrEmib6loGwrGJweoOlVE4SWXZtP0P4VHAWea_Y",
        "perun1_YXnJsVkLTA3sBGJQ7DFF-jEES88cNBkD14LQRoO_jsk",
        "perel1_TL4NeZiM97qScshyrywQXjb2cm5GR8S_v-rS5jYIE98",
    ),
}


@pytest.mark.parametrize("source_kind", POLICY_MATRIX)
def test_accepts_exact_public_source_matrix(source_kind: str) -> None:
    descriptor = release.build_public_evidence_source_release(
        release_input(source_kind)
    )
    policy = POLICY_MATRIX[source_kind]

    assert descriptor.artifact_identity.identity_kind == policy["identity_kind"]
    assert (
        descriptor.artifact_identity.content_identity_kind
        in policy["content_identity_kinds"]
    )
    assert descriptor.authority_classification == policy["authority"]
    assert descriptor.trust_classification == policy["trust"]
    assert descriptor.rights_classification == policy["rights"]
    assert descriptor.completeness_attestation.mode == policy["mode"]
    assert descriptor.completeness_attestation.count_unit == policy["count_unit"]
    assert descriptor.semantic_limits == policy["limits"]
    assert (
        release.SOURCE_POLICIES[source_kind].source_binding_source_types
        == policy["source_binding_source_types"]
    )
    assert descriptor.lifecycle_state == "verified_disabled"
    assert descriptor.current_pointer_authority == "none"
    assert descriptor.whole_source_complete is False
    assert descriptor.publication_enabled is False
    assert descriptor.replacement_enabled is False
    assert descriptor.deletion_enabled is False
    assert descriptor.retirement_enabled is False
    assert descriptor.supersession_enabled is False


def test_positive_evidence_is_not_whole_source_completeness() -> None:
    descriptor = release.build_public_evidence_source_release(
        release_input("public_hpt")
    )

    assert descriptor.completeness_attestation.mode == "positive_evidence_only"
    assert descriptor.completeness_attestation.expected_record_count is None
    assert descriptor.completeness_attestation.observed_record_count == 0
    assert descriptor.completeness_attestation_verified is True
    assert descriptor.whole_source_complete is False
    assert descriptor.replacement_enabled is False


def test_canonical_digest_is_order_independent_for_every_source_policy() -> None:
    for source_kind in POLICY_MATRIX:
        raw = release_input(source_kind)
        release_by_field = dict(reversed(tuple(raw.items())))
        first = release.build_public_evidence_source_release(raw)
        second = release.build_public_evidence_source_release(release_by_field)

        assert first.contract_sha256 == second.contract_sha256
        assert first.contract_sha256 == GOLDEN_CONTRACT_SHA256[source_kind]
        assert (
            first.artifact_identity.identity_ref,
            first.import_run_ref,
            first.source_release_ref,
        ) == GOLDEN_REFERENCES[source_kind]


def test_digest_binds_tic_composite_bundle_and_rights_proof() -> None:
    baseline = release.build_public_evidence_source_release(release_input())
    changed_source_binding = release_input()
    changed_source_binding["source_binding"] = release.OpaqueSourceBindingReference(
        release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
        "in_network",
        "logical_json_sha256_v1",
        sha256_text("a"),
        sha256_text("f"),
        sha256_text("e"),
    )
    changed_shadow_bundle_binding = release_input()
    changed_shadow_bundle_binding["source_binding"] = release.OpaqueSourceBindingReference(
        release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
        "in_network",
        "logical_json_sha256_v1",
        sha256_text("a"),
        sha256_text("d"),
        sha256_text("f"),
    )
    changed_rights = release_input()
    changed_rights["rights_proof_sha256"] = sha256_text("f")

    changed_inputs = (
        changed_source_binding,
        changed_shadow_bundle_binding,
        changed_rights,
    )
    changed_descriptors = tuple(
        release.build_public_evidence_source_release(raw) for raw in changed_inputs
    )
    assert all(
        descriptor.contract_sha256 != baseline.contract_sha256
        and descriptor.import_run_ref != baseline.import_run_ref
        and descriptor.source_release_ref != baseline.source_release_ref
        for descriptor in changed_descriptors
    )


def test_tic_source_binding_matches_typed_artifact_subject() -> None:
    descriptor = release.build_public_evidence_source_release(release_input())
    binding = descriptor.source_binding

    assert binding is not None
    assert binding.source_artifact_source_type == "in_network"
    assert (
        binding.source_artifact_identity_kind
        == descriptor.artifact_identity.content_identity_kind
    )
    assert binding.source_artifact_sha256 == descriptor.artifact_identity.content_sha256


def test_descriptor_constructor_rejects_wrong_contract_digest() -> None:
    descriptor = release.build_public_evidence_source_release(release_input())

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        replace(descriptor, contract_sha256=sha256_text("f"))


def test_descriptor_and_nested_objects_are_frozen_and_redacted() -> None:
    descriptor = release.build_public_evidence_source_release(release_input())
    sensitive_values = (
        descriptor.source_release_ref,
        descriptor.artifact_identity.identity_ref,
        descriptor.artifact_identity.content_sha256,
        descriptor.rights_proof_sha256,
    )

    assert all(value not in repr(descriptor) for value in sensitive_values)
    assert all(
        value not in repr(descriptor.artifact_identity) for value in sensitive_values
    )
    with pytest.raises(FrozenInstanceError):
        setattr(descriptor, "source_release_ref", "changed")


@pytest.mark.parametrize(
    "source_kind",
    "restricted_claims restricted_roster restricted_directory licensed_private_feed".split(),
)
def test_rejects_restricted_or_unsupported_source_kinds(source_kind: str) -> None:
    raw = release_input()
    raw["source_kind"] = source_kind

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


@pytest.mark.parametrize(
    "field_name",
    "path source_url credentials raw_tax_identity private_identity".split(),
)
def test_rejects_unknown_sensitive_or_locator_fields(field_name: str) -> None:
    raw = release_input()
    raw[field_name] = "not-retained"

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


def test_rejects_missing_fields_and_non_exact_mapping_type() -> None:
    raw = release_input()
    del raw["rights_proof_sha256"]
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)

    class InputDictionary(dict[str, object]):
        pass

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(InputDictionary(release_input()))


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("authority_classification", "cms_nppes_npi_registry"),
        ("trust_classification", "unknown"),
        ("rights_classification", "publicly_reachable_only"),
        ("semantic_limits", ("ownership_confirmed",)),
        ("semantic_limits", list(POLICY_MATRIX["tic"]["limits"])),
    ],
)
def test_rejects_source_policy_and_semantic_drift(
    field_name: str,
    invalid_value: object,
) -> None:
    raw = release_input()
    raw[field_name] = invalid_value

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


@pytest.mark.parametrize(
    "field_name",
    "artifact_bytes_verified public_access_verified "
    "processing_retention_rights_verified semantic_limits_verified "
    "completeness_attestation_verified".split(),
)
def test_requires_exact_true_external_verification(field_name: str) -> None:
    raw = release_input()
    raw[field_name] = 1

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


@pytest.mark.parametrize(
    "field_name",
    "legal_ownership_claimed exact_rate_site_claimed whole_source_complete "
    "redistribution_enabled export_enabled publication_enabled replacement_enabled "
    "deletion_enabled retirement_enabled supersession_enabled".split(),
)
def test_rejects_overclaim_or_authority_enablement(field_name: str) -> None:
    raw = release_input()
    raw[field_name] = True

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


@pytest.mark.parametrize("source_kind", POLICY_MATRIX)
def test_rejects_wrong_identity_or_attestation_mode(source_kind: str) -> None:
    raw = release_input(source_kind)
    identity = raw["artifact_identity"]
    wrong_kind = (
        "immutable_dataset"
        if identity.identity_kind == "immutable_artifact"
        else "immutable_artifact"
    )
    raw["artifact_identity"] = release.ImmutablePublicSourceIdentity(
        wrong_kind,
        identity.content_identity_kind,
        release.derive_public_evidence_identity_ref(
            wrong_kind,
            identity.content_identity_kind,
            identity.content_sha256,
        ),
        identity.content_sha256,
    )
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)

    raw = release_input(source_kind)
    identity = raw["artifact_identity"]
    unsupported_identity_kind = "canonical_dataset_sha256_v1"
    raw["artifact_identity"] = release.ImmutablePublicSourceIdentity(
        identity.identity_kind,
        unsupported_identity_kind,
        release.derive_public_evidence_identity_ref(
            identity.identity_kind,
            unsupported_identity_kind,
            identity.content_sha256,
        ),
        identity.content_sha256,
    )
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)

    raw = release_input(source_kind)
    attestation = raw["completeness_attestation"]
    wrong_mode = (
        "declared_complete_dataset"
        if attestation.mode != "declared_complete_dataset"
        else "declared_complete_artifact"
    )
    raw["completeness_attestation"] = release.PublicEvidenceCompletenessAttestation(
        wrong_mode,
        attestation.evidence_contract_id,
        attestation.count_unit,
        attestation.subject_sha256,
        7,
        7,
        attestation.evidence_root_sha256,
    )
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


def test_attestation_is_bound_to_policy_count_unit_and_immutable_subject() -> None:
    for changed_field, changed_value in (
        ("evidence_contract_id", "alternate_record_attestation_v1"),
        ("count_unit", "alternate_record"),
        ("subject_sha256", sha256_text("f")),
    ):
        raw = release_input()
        attestation = raw["completeness_attestation"]
        attestation_by_field = {
            field_name: getattr(attestation, field_name)
            for field_name in attestation.__slots__
        }
        attestation_by_field[changed_field] = changed_value
        raw["completeness_attestation"] = (
            release.PublicEvidenceCompletenessAttestation(**attestation_by_field)
        )
        with pytest.raises(release.PublicEvidenceSourceReleaseError):
            release.build_public_evidence_source_release(raw)


def test_tic_requires_two_subject_binding_and_other_sources_forbid_it() -> None:
    tic = release_input()
    tic["source_binding"] = None
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(tic)

    public_registry = release_input("nppes_entity_address")
    public_registry["source_binding"] = release_input()["source_binding"]
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(public_registry)


def test_wraps_unexpected_builder_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(release, "_normalized_release", lambda _raw: 1 / 0)

    with pytest.raises(
        release.PublicEvidenceSourceReleaseError,
        match="^public_evidence_source_release_invalid$",
    ):
        release.build_public_evidence_source_release(release_input())
