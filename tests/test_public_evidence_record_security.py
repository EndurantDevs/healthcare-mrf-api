# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Privacy, tamper, and authority boundaries for public evidence records."""

from __future__ import annotations

from copy import copy
import operator

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


class StringSubclass(str):
    pass


class EqualityCompatible:
    def __eq__(self, _other: object) -> bool:
        return True

    def __hash__(self) -> int:
        return hash("relationship_class")


class DictSubclass(dict[str, object]):
    pass


def _valid_tic_record() -> record.PublicEvidenceRecord:
    source = source_release("tic")
    return record.build_public_evidence_record(
        source,
        relationship_input(
            source,
            "tic_provider_group_member",
            membership_state="members_present",
        ),
    )


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("tin_type", "ssn"),
        ("tin_type", StringSubclass("ein")),
        ("token_policy_contract_id", "unknown_policy_v1"),
        ("token_policy_id", "wrong-policy"),
        ("token_policy_id", "ptg-tin-hmac-sha256-v1:UPPER"),
        ("token_policy_id", "ptg-tin-hmac-sha256-v1:other"),
        ("token_policy_descriptor_sha256", "A" * 64),
        ("token_policy_descriptor_sha256", "0" * 64),
        ("locator_128", "1" * 31),
        ("locator_128", "2" * 32),
        ("full_hmac_sha256", "not-a-digest"),
    ),
)
def test_rejects_invalid_or_inconsistent_opaque_tax_identity(
    field_name: str, replacement: object
) -> None:
    identity_field_map = {
        "tin_type": "ein",
        "token_policy_contract_id": token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT,
        "token_policy_id": "ptg-tin-hmac-sha256-v1:synthetic",
        "token_policy_descriptor_sha256": token_policy.token_policy_descriptor_sha256(
            token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT,
            "ptg-tin-hmac-sha256-v1:synthetic",
        ),
        "locator_128": "1" * 32,
        "full_hmac_sha256": "1" * 64,
    }
    identity_field_map[field_name] = replacement
    with pytest.raises(primitive.PublicEvidenceRecordError):
        token_policy.build_opaque_tax_identity(identity_field_map)


def test_opaque_identity_revalidation_rejects_tampering() -> None:
    identity = opaque_tax_identity()
    with pytest.raises(primitive.PublicEvidenceRecordError):
        token_policy.validate_opaque_tax_identity(object())
    with pytest.raises(primitive.PublicEvidenceRecordError):
        token_policy.validate_opaque_tax_identity(
            identity._replace(normalization_contract_id="wrong_contract_v1")
        )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        token_policy.validate_opaque_tax_identity(
            identity._replace(tax_identity_ref="petax1_forged")
        )


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("record_kind", "Bad Kind"),
        ("identity_contract_id", "bad-contract"),
        ("record_hmac_sha256", "0" * 63),
        ("payload_sha256", "F" * 64),
    ),
)
def test_rejects_invalid_source_record_reference(
    field_name: str, replacement: object
) -> None:
    source = source_release("tic")
    source_record_field_map = {
        "record_kind": "tic_provider_group_occurrence",
        "identity_contract_id": "synthetic_record_hmac_v1",
        "record_hmac_sha256": "1" * 64,
        "payload_sha256": "2" * 64,
    }
    source_record_field_map[field_name] = replacement
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive.build_evidence_source_record_reference(
            source, source_record_field_map
        )


def test_source_record_is_bound_to_release_and_derived_reference() -> None:
    tic = source_release("tic")
    source_ref = source_record(tic, "tic_provider_group_occurrence")
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive.validate_evidence_source_record_reference(object(), source_ref)
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive.validate_evidence_source_record_reference(
            source_release("public_hpt"), source_ref
        )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive.validate_evidence_source_record_reference(
            tic, source_ref._replace(source_record_ref="pesr1_forged")
        )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive.validate_evidence_source_record_reference(tic, object())


def test_entity_and_group_references_are_release_bound_and_exact_typed() -> None:
    tic = source_release("tic")
    group = provider_group(tic)
    entity = source_entity(tic)
    for candidate in (
        group._replace(provider_group_ref="pegrp1_forged"),
        group._replace(source_release_ref="perel1_forged"),
        entity._replace(source_entity_ref="peent1_forged"),
        entity._replace(source_release_ref="perel1_forged"),
    ):
        raw = relationship_input(
            tic,
            "tic_billing_identity_provider_group",
            membership_state="members_present",
        )
        if type(candidate) is primitive.ProviderGroupReference:
            raw["provider_group"] = candidate
        else:
            raw["provider_group"] = None
            raw["source_entity"] = candidate
        with pytest.raises(primitive.PublicEvidenceRecordError):
            record.build_public_evidence_record(tic, raw)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("address_key", "00000000-0000-4000-8000-00000000000A"),
        ("address_site_key", "not-a-uuid"),
        ("canonicalization_contract_id", "bad-contract"),
        ("purpose", "tin_address"),
        ("zip5", "1234"),
        ("geo_derivation_contract_id", "bad"),
        ("geo_quality", "perfect"),
        ("freshness_state", "forever"),
        ("freshness_rule_version", "bad"),
        ("freshness_as_of", "2026-07-01T12:00:00+00:00"),
        ("selection_rule_version", "bad"),
        ("selection_eligible", 1),
    ),
)
def test_rejects_invalid_address_evidence(field_name: str, replacement: object) -> None:
    address = canonical_address(purpose="nppes_practice_location")
    raw = address._asdict()
    raw[field_name] = replacement
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive.build_canonical_address_evidence(raw)


def test_address_without_site_or_zip_rejects_zip_centroid() -> None:
    address = canonical_address(purpose="nppes_mailing")._asdict()
    address["address_site_key"] = None
    address["zip5"] = None
    address["geo_quality"] = "unavailable"
    normalized = primitive.build_canonical_address_evidence(address)
    assert normalized.address_site_key is None
    assert normalized.zip5 is None

    address["geo_quality"] = "zip5_centroid"
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive.build_canonical_address_evidence(address)
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive.validate_canonical_address_evidence(object())


@pytest.mark.parametrize(
    "candidate",
    (
        "0000000000",
        "9999999999",
        "1234567890",
        "123456789",
        StringSubclass(SYNTHETIC_TYPE_1_NPI),
    ),
)
def test_every_npi_role_requires_cms_range_and_checksum(candidate: object) -> None:
    source = source_release("nppes_entity_address")
    raw = enumeration_input(source)
    raw["npi"] = candidate
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)

    fhir = source_release("public_provider_directory_fhir")
    raw = network_input(fhir)
    raw["npi"] = candidate
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(fhir, raw)


def test_raw_identity_and_direct_tin_address_fields_fail_closed() -> None:
    source = source_release("tic")
    raw = relationship_input(
        source,
        "tic_billing_identity_provider_group",
        membership_state="members_present",
    )
    for forbidden_field in (
        "tin",
        "ein",
        "value",
        "raw_tax_identity",
        "internal_group_key",
        "source_url",
        "credential",
    ):
        hostile = copy(raw)
        hostile[forbidden_field] = "sensitive-input"
        with pytest.raises(primitive.PublicEvidenceRecordError) as exc_info:
            record.build_public_evidence_record(source, hostile)
        assert str(exc_info.value) == "public_evidence_record_invalid"
        assert "sensitive-input" not in str(exc_info.value)

    nppes = source_release("nppes_entity_address")
    hostile = address_input(nppes, "nppes_npi_practice_location")
    hostile["tax_identity"] = opaque_tax_identity()
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(nppes, hostile)


def test_exact_dict_and_bounded_tuple_checks_precede_traversal() -> None:
    source = source_release("tic")
    raw = DictSubclass(
        relationship_input(
            source,
            "tic_provider_group_member",
            membership_state="members_present",
        )
    )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)

    oversized = relationship_input(
        source,
        "tic_provider_group_member",
        membership_state="members_present",
    )
    oversized["source_records"] = (object(),) * 17
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, oversized)

    duplicated = relationship_input(
        source,
        "tic_provider_group_member",
        membership_state="members_present",
    )
    duplicated["source_records"] *= 2
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, duplicated)


def test_unknown_relationships_wrong_sources_and_optional_cross_products_fail() -> None:
    tic = source_release("tic")
    raw = relationship_input(
        tic,
        "tic_billing_identity_provider_group",
        membership_state="members_present",
    )
    raw["relationship_class"] = "future_relationship"
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(tic, raw)

    raw = relationship_input(
        tic,
        "fhir_same_organization_identifier",
    )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(tic, raw)

    raw = relationship_input(
        tic,
        "tic_billing_identity_provider_group",
        membership_state="members_present",
    )
    raw["related_npi"] = SYNTHETIC_TYPE_1_NPI
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(tic, raw)

    raw = relationship_input(
        tic,
        "tic_provider_group_member",
        membership_state="members_present",
    )
    raw["related_npi"] = None
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(tic, raw)


def test_membership_state_rejects_equality_spoofing_before_policy_membership() -> None:
    source = source_release("tic")
    raw = relationship_input(
        source,
        "tic_billing_identity_provider_group",
        membership_state="members_present",
    )
    raw["membership_state"] = EqualityCompatible()
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)


@pytest.mark.parametrize(
    "bad_name",
    (
        "",
        " " * 10,
        "Synthetic\u0000Group",
        "Synthetic\u202eGroup",
        "x" * 257,
        StringSubclass("Synthetic Group"),
    ),
)
def test_business_names_are_bounded_canonical_and_control_free(
    bad_name: object,
) -> None:
    source = source_release("tic")
    raw = name_input(source, "tic_source_reported_business_name")
    raw["source_reported_name"] = bad_name
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)


def test_network_context_requires_active_distinct_exact_resource_witnesses() -> None:
    source = source_release("public_provider_directory_fhir")
    raw = network_input(source)
    context = raw["network_context"]
    raw["network_context"] = context._replace(role_active=False)
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)

    raw = network_input(source)
    context = raw["network_context"]
    raw["network_context"] = context._replace(pricing_bridge_state="payer_confirmed")
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_public_evidence_record(source, raw)

    context_field_map = {
        name: getattr(context, name) for name, _kind in policies.NETWORK_RECORD_FIELDS
    }
    context_field_map["role_active"] = True
    context_field_map["network_source_record"] = context.location_source_record
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.build_provider_directory_network_context(source, context_field_map)


def test_policy_registries_and_policy_values_are_deeply_immutable() -> None:
    with pytest.raises(TypeError):
        operator.setitem(
            policies.TAX_IDENTITY_RELATIONSHIP_POLICIES,
            "future_relationship",
            next(iter(policies.TAX_IDENTITY_RELATIONSHIP_POLICIES.values())),
        )
    policy = policies.TAX_IDENTITY_RELATIONSHIP_POLICIES["tic_provider_group_member"]
    with pytest.raises(AttributeError):
        object.__setattr__(policy, "candidate_only", True)
    with pytest.raises(TypeError):
        operator.setitem(
            token_policy.TOKEN_POLICY_PROFILES, "future_policy_v1", object()
        )
    token_profile = token_policy.TOKEN_POLICY_PROFILES[
        token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT
    ]
    with pytest.raises(TypeError):
        operator.setitem(token_profile.normalization_by_type, "ssn", "unsupported_v1")


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("contract", "wrong-contract"),
        ("contract", StringSubclass(record.PUBLIC_EVIDENCE_RECORD_CONTRACT)),
        ("foundation_scope", "wrong-scope"),
        ("evidence_ref", "peev1_forged"),
        ("contract_sha256", "0" * 64),
    ),
)
def test_record_revalidation_rejects_fixed_and_derived_state_tampering(
    field_name: str, replacement: object
) -> None:
    normalized = _valid_tic_record()._replace(**{field_name: replacement})
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.validate_public_evidence_record(normalized)


def test_record_revalidation_rejects_authority_escalation_and_wrong_types() -> None:
    normalized = _valid_tic_record()
    for field_name, replacement in (
        ("serving_authority", "enabled"),
        ("positive_evidence_only", 1),
        ("legal_ownership_claimed", True),
        ("publication_enabled", True),
    ):
        state = normalized.authority_state._replace(**{field_name: replacement})
        with pytest.raises(primitive.PublicEvidenceRecordError):
            record.validate_public_evidence_record(
                normalized._replace(authority_state=state)
            )
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record.validate_public_evidence_record(object())


def test_record_revalidation_wraps_unexpected_failures_without_echo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    normalized = _valid_tic_record()
    monkeypatch.setattr(
        record,
        "build_public_evidence_record",
        lambda *_args: (_ for _ in ()).throw(ZeroDivisionError("secret-value")),
    )
    with pytest.raises(primitive.PublicEvidenceRecordError) as exc_info:
        record.validate_public_evidence_record(normalized)
    assert str(exc_info.value) == "public_evidence_record_invalid"


def test_canonical_serializer_rejects_foreign_objects_uniformly() -> None:
    with pytest.raises(primitive.PublicEvidenceRecordError):
        record._json_value(object())
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive._canonical_json(object())


def test_low_level_shape_and_fixed_state_guards_require_exact_types() -> None:
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive._exact_dict({1: "value"}, frozenset({"field"}))
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive._exact_dict({"other": "value"}, frozenset({"field"}))
    with pytest.raises(primitive.PublicEvidenceRecordError):
        primitive._strict_uuid(1)
    with pytest.raises(primitive.PublicEvidenceRecordError):
        policies._validated_authority_state(object())


def test_builder_wraps_unexpected_normalizer_and_unknown_record_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_release("nppes_entity_address")

    def explode(*_args: object) -> None:
        raise ZeroDivisionError("sensitive")

    normalizer_by_record_type = dict(record._NORMALIZERS)
    normalizer_by_record_type["npi_enumeration"] = explode
    monkeypatch.setattr(record, "_NORMALIZERS", normalizer_by_record_type)
    with pytest.raises(primitive.PublicEvidenceRecordError) as exc_info:
        record.build_public_evidence_record(source, enumeration_input(source))
    assert str(exc_info.value) == "public_evidence_record_invalid"

    with pytest.raises(primitive.PublicEvidenceRecordError):
        record._raw_from_record(_valid_tic_record()._replace(record_type="future"))
