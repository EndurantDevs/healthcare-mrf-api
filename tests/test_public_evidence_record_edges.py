# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed source, shape, and temporal edges for evidence records."""

from __future__ import annotations

import operator
from types import SimpleNamespace

import pytest

from public_evidence import evidence_record_contract as record
from public_evidence import evidence_record_policies as policies
from public_evidence import evidence_record_primitives as primitive
from public_evidence import evidence_record_token_policy as token_policy
from public_evidence import source_release_contract as release
from tests.public_evidence_record_support import (
    SYNTHETIC_TYPE_1_NPI,
    address_input,
    canonical_address,
    enumeration_input,
    name_input,
    network_input,
    opaque_tax_identity,
    provider_group,
    relationship_input,
    source_entity,
    source_record,
    source_release,
)
from tests.public_evidence_source_release_support import release_input


def _token_input(
    *,
    contract_id: str,
    policy_id: str,
    tin_type: str,
    full_hmac: str,
) -> dict[str, object]:
    return {
        "tin_type": tin_type,
        "token_policy_contract_id": contract_id,
        "token_policy_id": policy_id,
        "token_policy_descriptor_sha256": (
            token_policy.token_policy_descriptor_sha256(contract_id, policy_id)
        ),
        "locator_128": full_hmac[:32],
        "full_hmac_sha256": full_hmac,
    }


def test_invalid_variant_and_source_policy_combinations_fail_closed() -> None:
    tic = source_release("tic")
    raw = name_input(tic, "tic_source_reported_business_name")
    raw["relationship_class"] = "future_name_relationship"
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(tic, raw)

    fhir = source_release("public_provider_directory_fhir")
    raw = name_input(fhir, "fhir_same_organization_reported_name")
    raw["tax_identity"] = opaque_tax_identity("npi")
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(fhir, raw)

    nppes = source_release("nppes_entity_address")
    for field_name, replacement in (
        ("relationship_class", "future_enumeration"),
        ("npi_entity_type", "unknown_type"),
        ("enumeration_state", "unknown_state"),
    ):
        raw = enumeration_input(nppes)
        raw[field_name] = replacement
        with pytest.raises(primitive.PublicEvidenceRecordError):
            record.build_public_evidence_record(nppes, raw)

    raw = address_input(nppes, "nppes_npi_practice_location")
    raw["relationship_class"] = "future_address_relationship"
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(nppes, raw)

    raw = address_input(nppes, "nppes_npi_practice_location")
    raw["address"] = canonical_address(purpose="provider_directory_location")
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(nppes, raw)


def test_address_subjects_and_network_context_cannot_cross_product() -> None:
    nppes = source_release("nppes_entity_address")
    raw = address_input(nppes, "nppes_npi_practice_location")
    raw["source_entity"] = record.build_opaque_source_entity_reference(
        nppes,
        {
            "entity_kind": "synthetic_organization",
            "identity_contract_id": "synthetic_entity_digest_v1",
            "identity_sha256": "e" * 64,
        },
    )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(nppes, raw)

    hpt = source_release("public_hpt")
    raw = address_input(hpt, "hpt_entity_location_candidate")
    raw["subject_npi"] = SYNTHETIC_TYPE_1_NPI
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(hpt, raw)

    fhir = source_release("public_provider_directory_fhir")
    for mutation in ("relationship", "purpose", "context"):
        raw = network_input(fhir)
        if mutation == "relationship":
            raw["relationship_class"] = "future_network_relationship"
        elif mutation == "purpose":
            raw["address"] = canonical_address(purpose="nppes_practice_location")
        else:
            raw["network_context"] = object()
        with pytest.raises(primitive.PublicEvidenceRecordError):
            record.build_public_evidence_record(fhir, raw)


def test_closed_semantic_registries_are_deeply_immutable() -> None:
    for vocabulary in (
        primitive._ADDRESS_PURPOSES,
        primitive._GEO_QUALITIES,
        primitive._FRESHNESS_STATES,
    ):
        assert type(vocabulary) is frozenset
    for registry in (policies._SOURCE_ENTITY_KIND, record._NORMALIZERS):
        with pytest.raises(TypeError):
            operator.setitem(registry, "future", object())


@pytest.mark.parametrize(
    ("source_kind", "relationship", "wrong_entity_kind"),
    (
        (
            "public_provider_directory_fhir",
            "fhir_same_organization_identifier",
            "hpt_hospital_entity",
        ),
        (
            "public_hpt",
            "hpt_hospital_tax_identity_npi_candidate",
            "fhir_organization",
        ),
    ),
)
def test_source_entity_kind_is_bound_to_source_kind(
    source_kind: str,
    relationship: str,
    wrong_entity_kind: str,
) -> None:
    source = source_release(source_kind)
    raw = relationship_input(source, relationship)
    raw["source_entity"] = source_entity(
        source,
        entity_kind=wrong_entity_kind,
    )

    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)


def test_source_shape_and_context_set_are_exact() -> None:
    tic = source_release("tic")
    raw = relationship_input(
        tic,
        "tic_provider_group_member",
        membership_state="members_present",
    )
    raw["source_records"] = (source_record(tic, "hpt_hospital_record"),)
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(tic, raw)

    fhir = source_release("public_provider_directory_fhir")
    raw = network_input(fhir)
    records = list(raw["source_records"])
    records[3] = source_record(fhir, "fhir_network", seed="a")
    raw["source_records"] = tuple(records)
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(fhir, raw)


def test_raw_shape_record_type_and_reference_presence_are_exact() -> None:
    tic = source_release("tic")
    raw = relationship_input(
        tic,
        "tic_provider_group_member",
        membership_state="members_present",
    )
    raw.pop("related_npi")
    raw[1] = SYNTHETIC_TYPE_1_NPI
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(tic, raw)

    raw = relationship_input(
        tic,
        "tic_provider_group_member",
        membership_state="members_present",
    )
    raw["record_type"] = "future_record"
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(tic, raw)

    raw = relationship_input(
        tic,
        "tic_provider_group_member",
        membership_state="members_present",
    )
    raw["provider_group"] = object()
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(tic, raw)

    fhir = source_release("public_provider_directory_fhir")
    raw = relationship_input(fhir, "fhir_same_organization_identifier")
    raw["provider_group"] = record.build_provider_group_reference(
        fhir,
        {
            "identity_contract_id": "synthetic_provider_group_digest_v1",
            "identity_sha256": "f" * 64,
        },
    )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(fhir, raw)


def test_malformed_and_closed_release_temporal_edges_fail_closed() -> None:
    nppes = source_release("nppes_entity_address")
    raw = enumeration_input(nppes)
    raw["observed_at"] = "2026-02-30T00:00:00Z"
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(nppes, raw)

    raw = enumeration_input(nppes)
    raw["effective_interval"] = object()
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(nppes, raw)

    interval = release.CanonicalUtcInterval("2026-07-01T00:00:00Z", None)
    object.__setattr__(interval, "start_at", "not-a-time")
    raw = enumeration_input(nppes)
    raw["effective_interval"] = interval
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(nppes, raw)

    closed_input = release_input("nppes_entity_address")
    closed_input["effective_interval"] = release.CanonicalUtcInterval(
        "2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z"
    )
    closed_source = release.build_public_evidence_source_release(closed_input)
    raw = enumeration_input(closed_source)
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(closed_source, raw)


@pytest.mark.parametrize(
    ("source_kind", "relationship"),
    (
        ("nppes_entity_address", "nppes_npi_practice_location"),
        ("nppes_entity_address", "nppes_npi_mailing_address"),
        ("public_provider_directory_fhir", "fhir_npi_directory_address"),
        ("public_provider_directory_fhir", "fhir_entity_directory_address"),
        ("public_hpt", "hpt_entity_location_candidate"),
    ),
)
def test_address_freshness_cannot_postdate_record_observation(
    source_kind: str,
    relationship: str,
) -> None:
    source = source_release(source_kind)
    raw = address_input(source, relationship)
    raw["address"] = raw["address"]._replace(freshness_as_of="2026-07-01T12:00:01Z")
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)


def test_network_freshness_cannot_postdate_observation_or_heal_on_revalidation() -> (
    None
):
    source = source_release("public_provider_directory_fhir")
    raw = network_input(source)
    valid = record.build_public_evidence_record(source, raw)
    future_address = valid.evidence.address._replace(
        freshness_as_of="2026-07-01T12:00:01Z"
    )

    raw["address"] = future_address
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.validate_public_evidence_record(
            valid._replace(evidence=valid.evidence._replace(address=future_address))
        )


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("candidate_only", False),
        ("name_kind", "legal_name"),
        ("name_normalization_contract_id", "wrong_name_contract_v1"),
        ("normalized_name_sha256", "0" * 64),
    ),
)
def test_revalidation_rejects_policy_derived_name_tampering(
    field_name: str,
    replacement: object,
) -> None:
    source = source_release("public_hpt")
    normalized = record.build_public_evidence_record(
        source,
        name_input(source, "hpt_source_reported_hospital_name_candidate"),
    )
    tampered_evidence = normalized.evidence._replace(**{field_name: replacement})

    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.validate_public_evidence_record(
            normalized._replace(evidence=tampered_evidence)
        )


def test_revalidation_rejects_foreign_evidence_and_noncanonical_source_order() -> None:
    source = source_release("public_provider_directory_fhir")
    normalized = record.build_public_evidence_record(source, network_input(source))
    foreign_evidence = SimpleNamespace(
        **normalized.evidence._asdict(),
    )

    for tampered in (
        normalized._replace(evidence=foreign_evidence),
        normalized._replace(source_records=tuple(reversed(normalized.source_records))),
    ):
        with pytest.raises(primitive.PublicEvidenceRecordError):
            record.validate_public_evidence_record(tampered)


def test_name_preserves_source_text_under_a_pinned_normalization_contract() -> None:
    source = source_release("tic")
    spaced_name = "  Synthetic\tHealth  Group  "
    spaced_record = record.build_public_evidence_record(
        source,
        name_input(
            source,
            "tic_source_reported_business_name",
            source_reported_name=spaced_name,
        ),
    )
    canonical_record = record.build_public_evidence_record(
        source,
        name_input(source, "tic_source_reported_business_name"),
    )

    assert spaced_record.evidence.source_reported_name == spaced_name
    assert spaced_record.evidence.normalized_name_sha256 == (
        canonical_record.evidence.normalized_name_sha256
    )
    assert spaced_record.evidence_ref != canonical_record.evidence_ref
    assert spaced_record.evidence.name_normalization_contract_id == (
        "unicode_16_0_0_nfkc_whitespace_casefold_sha256_v1"
    )


def test_name_normalization_fails_closed_on_unicode_version_drift(monkeypatch) -> None:
    monkeypatch.setattr(record.unicodedata, "unidata_version", "future")
    source = source_release("tic")
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(
            source,
            name_input(source, "tic_source_reported_business_name"),
        )


def test_token_policy_profiles_have_frozen_ein_and_npi_vectors() -> None:
    legacy_contract = token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT
    legacy_policy = "ptg-tin-hmac-sha256-v1:synthetic"
    public_contract = token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT
    public_policy = "healthporta-tax-identity-hmac-sha256-v1:synthetic"

    assert (
        token_policy.token_policy_descriptor_sha256(legacy_contract, legacy_policy)
        == "1d0c4d0ec36fcf66fb79c1c2f9d2b10f8eed066fc4f7f0755714a3a808bfe437"
    )
    assert (
        token_policy.token_policy_descriptor_sha256(public_contract, public_policy)
        == "11ee8cd5ad272d7d2aa61fe883747853c2ced65b50ce2445fcd4a1c54aa63198"
    )
    assert opaque_tax_identity("npi").tax_identity_ref == (
        "petax1_Bw_tM2a2TSgHIxn4zzGPpqpLWGLGXY5Ld3HNG0n0d0o"
    )


def test_token_policy_rejects_legacy_npi_and_binds_full_hmac() -> None:
    contract_id = token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT
    policy_id = "ptg-tin-hmac-sha256-v1:synthetic"
    with pytest.raises(primitive.PublicEvidenceRecordError):
        token_policy.build_opaque_tax_identity(
            _token_input(
                contract_id=contract_id,
                policy_id=policy_id,
                tin_type="npi",
                full_hmac="1" * 64,
            )
        )

    first = token_policy.build_opaque_tax_identity(
        _token_input(
            contract_id=contract_id,
            policy_id=policy_id,
            tin_type="ein",
            full_hmac="1" * 64,
        )
    )
    second = token_policy.build_opaque_tax_identity(
        _token_input(
            contract_id=contract_id,
            policy_id=policy_id,
            tin_type="ein",
            full_hmac="1" * 32 + "2" * 32,
        )
    )
    assert first.locator_128 == second.locator_128
    assert first.tax_identity_ref != second.tax_identity_ref


def test_non_ascii_derived_references_fail_with_uniform_error() -> None:
    identity = opaque_tax_identity()._replace(tax_identity_ref="petax1_é")
    with pytest.raises(primitive.PublicEvidenceRecordError):
        token_policy.validate_opaque_tax_identity(identity)

    source_release_descriptor = source_release("tic")
    source_ref = source_record(
        source_release_descriptor,
        "tic_provider_group_occurrence",
    )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive.validate_evidence_source_record_reference(
            source_release_descriptor,
            source_ref._replace(source_record_ref="pesr1_é"),
        )

    normalized = record.build_public_evidence_record(
        source_release_descriptor,
        relationship_input(
            source_release_descriptor,
            "tic_provider_group_member",
            membership_state="members_present",
        ),
    )
    tampered_group = provider_group(source_release_descriptor)._replace(
        provider_group_ref="pegrp1_é"
    )
    tampered_evidence = normalized.evidence._replace(provider_group=tampered_group)
    for tampered in (
        normalized._replace(evidence=tampered_evidence),
        normalized._replace(evidence_ref="peev1_é"),
        normalized._replace(contract_sha256="é" * 64),
    ):
        with pytest.raises(primitive.PublicEvidenceRecordError):
            record.validate_public_evidence_record(tampered)

    fhir_release = source_release("public_provider_directory_fhir")
    fhir_record = record.build_public_evidence_record(
        fhir_release,
        relationship_input(fhir_release, "fhir_same_organization_identifier"),
    )
    tampered_entity = fhir_record.evidence.source_entity._replace(
        source_entity_ref="peent1_é"
    )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.validate_public_evidence_record(
            fhir_record._replace(
                evidence=fhir_record.evidence._replace(source_entity=tampered_entity)
            )
        )
