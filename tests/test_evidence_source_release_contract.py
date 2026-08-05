from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from process import evidence_source_release_contract as release

POLICY_MATRIX = {
    "tic": (
        *"immutable_artifact payer_transparency_in_coverage "
        "authoritative_tic_rate_group_association "
        "tic_public_access_processing_retention_reviewed complete_artifact".split(),
        (
            "provider_group_membership_not_legal_ownership",
            "tic_rate_not_bound_to_exact_provider_site",
        ),
    ),
    "public_provider_directory_fhir": (
        *"immutable_dataset public_payer_provider_directory_fhir "
        "public_provider_directory_source_evidence "
        "provider_directory_public_access_processing_retention_reviewed "
        "complete_dataset".split(),
        (
            "directory_relationship_not_legal_ownership",
            "directory_location_not_exact_rate_site",
            "location_corroboration_requires_exact_npi_active_role_location_plan_network_bridge",
        ),
    ),
    "nppes_entity_address": (
        *"immutable_dataset cms_nppes_npi_registry "
        "authoritative_npi_enumeration_and_registry_record_status "
        "nppes_public_access_processing_retention_reviewed complete_dataset".split(),
        (
            "non_system_fields_provider_or_authorized_official_reported",
            "nppes_not_payer_confirmed",
            "nppes_has_no_plan_network_binding",
            "nppes_not_tin_address_proof",
            "nppes_not_affiliation_or_ownership_proof",
            "nppes_not_credentialing_proof",
            "nppes_not_current_service_site_proof",
            "nppes_not_universal_ein_npi_crosswalk",
            "registry_address_not_exact_rate_site",
        ),
    ),
    "public_hpt": (
        *"immutable_artifact hospital_published_hpt_machine_readable_artifact "
        "public_hospital_entity_location_candidate "
        "hpt_public_access_processing_retention_reviewed positive_evidence_only".split(),
        (
            "cms_hpt_rule_schema_is_regulatory_context_not_artifact_authorship",
            "hospital_evidence_not_universal_ein_npi_crosswalk",
            "hospital_location_not_exact_rate_site",
        ),
    ),
}
PUBLIC_ID_FIELDS = tuple("artifact_identity import_run_id source_release_id".split())


def _sha(character: str) -> str:
    return character * 64


def _release_input(source_kind: str = "tic") -> dict[str, object]:
    identity_kind, authority, trust, rights, mode, limits = POLICY_MATRIX[source_kind]
    is_positive_only = mode == "positive_evidence_only"
    return {
        "source_kind": source_kind,
        "authority_classification": authority,
        "trust_classification": trust,
        "semantic_limits": limits,
        "artifact_identity": release.ImmutablePublicSourceIdentity(
            identity_kind,
            f"public-{source_kind.replace('_', '-')}-202608",
            _sha("a"),
        ),
        "completeness_proof": release.PublicEvidenceCompletenessProof(
            mode,
            None if is_positive_only else 7,
            0 if is_positive_only else 7,
            _sha("b"),
        ),
        "rights_classification": rights,
        "rights_proof_sha256": _sha("c"),
        "source_binding": (
            release.OpaqueSourceBindingReference(
                release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, _sha("d")
            )
            if source_kind == "tic"
            else None
        ),
        "observed_interval": release.CanonicalUtcInterval(
            "2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z"
        ),
        "effective_interval": release.CanonicalUtcInterval(
            "2026-07-01T00:00:00Z", None
        ),
        "import_run_id": "import-run-202608",
        "source_release_id": f"release-{source_kind.replace('_', '-')}-202608",
        "artifact_bytes_verified": True,
        "public_access_verified": True,
        "processing_retention_rights_verified": True,
        "semantic_limits_verified": True,
        "completeness_verified": True,
        "legal_ownership_claimed": False,
        "exact_rate_site_claimed": False,
        "redistribution_enabled": False,
        "export_enabled": False,
        "publication_enabled": False,
        "replacement_enabled": False,
    }


@pytest.mark.parametrize("source_kind", POLICY_MATRIX)
def test_accepts_exact_public_source_matrix(source_kind: str) -> None:
    descriptor = release.build_public_evidence_source_release(
        _release_input(source_kind)
    )
    policy = POLICY_MATRIX[source_kind]

    assert descriptor.artifact_identity.identity_kind == policy[0]
    assert descriptor.authority_classification == policy[1]
    assert descriptor.trust_classification == policy[2]
    assert descriptor.rights_classification == policy[3]
    assert descriptor.completeness_proof.mode == policy[4]
    assert descriptor.semantic_limits == policy[5]
    assert descriptor.publication_enabled is False
    assert descriptor.replacement_enabled is False
    assert descriptor.redistribution_enabled is False
    assert descriptor.export_enabled is False
    if source_kind == "public_hpt":
        assert descriptor.completeness_proof.observed_record_count == 0


def test_canonical_digest_is_deterministic_order_independent_and_frozen() -> None:
    raw = _release_input("public_hpt")
    release_by_field = dict(reversed(tuple(raw.items())))

    first = release.build_public_evidence_source_release(raw)
    second = release.build_public_evidence_source_release(release_by_field)

    assert first.contract_sha256 == second.contract_sha256
    assert first.contract_sha256 == (
        "e2b342c933ede1d4d1b1a578ed6f14ca1e222f41b64c07914b1cc541bfcd33b3"
    )


def test_digest_binds_source_binding_and_rights_proof() -> None:
    baseline = release.build_public_evidence_source_release(_release_input())
    changed_binding = _release_input()
    changed_binding["source_binding"] = release.OpaqueSourceBindingReference(
        release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
        _sha("e"),
    )
    changed_rights = _release_input()
    changed_rights["rights_proof_sha256"] = _sha("f")

    assert (
        release.build_public_evidence_source_release(changed_binding).contract_sha256
        != baseline.contract_sha256
    )
    assert (
        release.build_public_evidence_source_release(changed_rights).contract_sha256
        != baseline.contract_sha256
    )


def test_descriptor_and_nested_objects_are_frozen_and_redacted() -> None:
    descriptor = release.build_public_evidence_source_release(_release_input())
    sensitive_values = (
        descriptor.source_release_id,
        descriptor.artifact_identity.content_sha256,
        descriptor.rights_proof_sha256,
    )

    assert all(value not in repr(descriptor) for value in sensitive_values)
    assert descriptor.artifact_identity.identity_id not in repr(
        descriptor.artifact_identity
    )
    with pytest.raises(FrozenInstanceError):
        setattr(descriptor, "source_release_id", "changed")
    ordinary_ids = "public-protein-release public-martin-release public-monkey-release"
    for ordinary_id in ordinary_ids.split():
        raw = _release_input()
        object.__setattr__(raw["artifact_identity"], "identity_id", ordinary_id)
        raw["import_run_id"] = raw["source_release_id"] = ordinary_id
        validated = release.build_public_evidence_source_release(raw)
        assert validated.source_release_id == ordinary_id


@pytest.mark.parametrize(
    "source_kind",
    "claims_837 payer_roster caqh licensed_directory".split(),
)
def test_rejects_restricted_or_unsupported_source_kinds(source_kind: str) -> None:
    raw = _release_input()
    raw["source_kind"] = source_kind

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


@pytest.mark.parametrize(
    "field_name",
    "path source_url credentials raw_tax_identity private_identity".split(),
)
def test_rejects_unknown_sensitive_or_locator_fields(field_name: str) -> None:
    raw = _release_input()
    raw[field_name] = "not-retained"

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


def test_rejects_missing_fields_and_non_exact_mapping_type() -> None:
    raw = _release_input()
    del raw["source_release_id"]

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)

    class InputDictionary(dict[str, object]):
        pass

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(InputDictionary(_release_input()))


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("authority_classification", "cms_nppes_npi_registry"),
        ("trust_classification", "unknown"),
        ("rights_classification", "publicly_reachable_only"),
        ("semantic_limits", ("ownership_confirmed",)),
        ("semantic_limits", list(POLICY_MATRIX["tic"][5])),
    ],
)
def test_rejects_source_policy_and_semantic_drift(
    field_name: str,
    invalid_value: object,
) -> None:
    raw = _release_input()
    raw[field_name] = invalid_value

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


@pytest.mark.parametrize(
    "field_name",
    "artifact_bytes_verified public_access_verified "
    "processing_retention_rights_verified semantic_limits_verified "
    "completeness_verified".split(),
)
def test_requires_exact_true_external_verification(field_name: str) -> None:
    raw = _release_input()
    raw[field_name] = 1

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


@pytest.mark.parametrize(
    "field_name",
    "legal_ownership_claimed exact_rate_site_claimed redistribution_enabled "
    "export_enabled publication_enabled replacement_enabled".split(),
)
def test_rejects_overclaim_or_authority_enablement(field_name: str) -> None:
    raw = _release_input()
    raw[field_name] = True

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


@pytest.mark.parametrize("source_kind", POLICY_MATRIX)
def test_rejects_wrong_identity_or_completeness_mode(source_kind: str) -> None:
    raw = _release_input(source_kind)
    identity = raw["artifact_identity"]
    wrong_kind = (
        "immutable_dataset"
        if identity.identity_kind == "immutable_artifact"
        else "immutable_artifact"
    )
    raw["artifact_identity"] = release.ImmutablePublicSourceIdentity(
        wrong_kind,
        identity.identity_id,
        identity.content_sha256,
    )
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)

    raw = _release_input(source_kind)
    wrong_mode = (
        "complete_dataset"
        if POLICY_MATRIX[source_kind][4] != "complete_dataset"
        else "complete_artifact"
    )
    raw["completeness_proof"] = release.PublicEvidenceCompletenessProof(
        wrong_mode, 7, 7, _sha("b")
    )
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


def test_tic_requires_binding_and_other_sources_forbid_it() -> None:
    tic = _release_input()
    tic["source_binding"] = None
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(tic)

    nppes = _release_input("nppes_entity_address")
    nppes["source_binding"] = _release_input()["source_binding"]
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(nppes)


@pytest.mark.parametrize(
    ("identity_kind", "identity_id", "digest"),
    [
        ("mutable_file", "public-source", _sha("a")),
        ("immutable_artifact", "https://public.invalid/source", _sha("a")),
        ("immutable_artifact", "public\u202esource", _sha("a")),
        ("immutable_artifact", "public-source", "A" * 64),
    ],
)
def test_rejects_invalid_or_sensitive_artifact_identity(
    identity_kind: str,
    identity_id: str,
    digest: str,
) -> None:
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.ImmutablePublicSourceIdentity(identity_kind, identity_id, digest)


@pytest.mark.parametrize(
    ("mode", "expected", "observed", "proof"),
    [
        ("unknown", 1, 1, _sha("b")),
        ("complete_dataset", 1, True, _sha("b")),
        ("complete_dataset", 1, -1, _sha("b")),
        ("complete_dataset", 1, 2**63, _sha("b")),
        ("complete_dataset", True, 1, _sha("b")),
        ("complete_dataset", -1, 1, _sha("b")),
        ("complete_dataset", 2, 1, _sha("b")),
        ("positive_evidence_only", 1, 1, _sha("b")),
        ("complete_dataset", 1, 1, "not-a-hash"),
    ],
)
def test_rejects_invalid_completeness_proofs(
    mode: str,
    expected: object,
    observed: object,
    proof: str,
) -> None:
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.PublicEvidenceCompletenessProof(mode, expected, observed, proof)


@pytest.mark.parametrize(
    ("start_at", "end_at"),
    [
        ("2026-07-01T00:00:00+00:00", None),
        ("2026-07-01T00:00:00.000Z", None),
        ("2026-07-01T00:00:00Z ", None),
        ("2026-02-30T00:00:00Z", None),
        ("2026-07-01T00:00:00Z\u0000", None),
        ("2026-07-01T00:00:00Z\u202e", None),
        ("2026-07-02T00:00:00Z", "2026-07-01T00:00:00Z"),
    ],
)
def test_rejects_noncanonical_or_reversed_utc_intervals(
    start_at: str,
    end_at: str | None,
) -> None:
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.CanonicalUtcInterval(start_at, end_at)


def test_observed_interval_must_be_closed_but_effective_may_be_open() -> None:
    raw = _release_input()
    raw["observed_interval"] = release.CanonicalUtcInterval(
        "2026-07-01T00:00:00Z", None
    )

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)
    assert _release_input()["effective_interval"].end_at is None


@pytest.mark.parametrize(
    ("contract_id", "binding_sha256"),
    [
        ("other-contract", _sha("d")),
        (release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, "bad"),
    ],
)
def test_rejects_invalid_opaque_source_binding(
    contract_id: str,
    binding_sha256: str,
) -> None:
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.OpaqueSourceBindingReference(contract_id, binding_sha256)


def test_rejects_wrong_or_tampered_exact_nested_types() -> None:
    raw = _release_input()
    raw["artifact_identity"] = object()
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)

    raw = _release_input()
    artifact = raw["artifact_identity"]
    object.__setattr__(artifact, "content_sha256", "A" * 64)
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)

    raw = _release_input()
    artifact = raw["artifact_identity"]
    object.__delattr__(artifact, "identity_id")
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


@pytest.mark.parametrize("field_name", PUBLIC_ID_FIELDS)
@pytest.mark.parametrize(
    "raw_identity",
    [
        *"release-00123456789 release-01234567890 release-0.1.2.3.4.5.6.7.8 "
        "release-secret-token release-secretvalue release-passwordvalue release-apikeyvalue release-sourceurl release-privatepath".split(),
        *(
            f"release-{prefix}{'a' * 24}"
            for prefix in ("sk-", "ghp_", "github_pat_", "sk_live_", "rk_live_")
        ),
    ],
)
def test_rejects_sensitive_or_tax_identity_shaped_release_ids(
    field_name: str,
    raw_identity: str,
) -> None:
    raw = _release_input()
    if field_name == "artifact_identity":
        object.__setattr__(raw[field_name], "identity_id", raw_identity)
    else:
        raw[field_name] = raw_identity
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


def test_revalidates_descriptor_and_rejects_tampering() -> None:
    descriptor = release.build_public_evidence_source_release(_release_input())
    rebuilt = release.validate_public_evidence_source_release(descriptor)
    assert rebuilt == descriptor
    assert rebuilt is not descriptor

    object.__setattr__(descriptor, "source_release_id", "release-other-202608")
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(descriptor)


def test_revalidation_rejects_wrong_type_and_fixed_state_tampering() -> None:
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(object())

    class Hostile:
        @property
        def contract(self) -> str:
            raise AssertionError("foreign property was evaluated")

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(Hostile())

    descriptor = release.build_public_evidence_source_release(_release_input())
    object.__setattr__(descriptor, "contract", "other-contract")
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(descriptor)

    descriptor = release.build_public_evidence_source_release(_release_input())
    object.__delattr__(descriptor, "source_kind")
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(descriptor)


def test_wraps_unexpected_builder_and_revalidation_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(release, "_normalized_release", lambda _raw: 1 / 0)
    with pytest.raises(
        release.PublicEvidenceSourceReleaseError,
        match="^public_evidence_source_release_invalid$",
    ):
        release.build_public_evidence_source_release(_release_input())

    monkeypatch.undo()
    descriptor = release.build_public_evidence_source_release(_release_input())
    monkeypatch.setattr(release, "replace", lambda _descriptor: 1 / 0)
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(descriptor)
