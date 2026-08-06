# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Tamper, privacy, and authority tests for adapter projections."""

from __future__ import annotations

import operator

import pytest

from public_evidence import adapter_projection_contract as projection
from public_evidence import adapter_projection_policies as policies
from public_evidence import evidence_record_contract as record
from public_evidence import source_record_inclusion_contract as inclusion
from public_evidence import source_record_inclusion_primitives as primitive
from public_evidence import source_release_contract as release
from tests.public_evidence_adapter_projection_support import (
    inclusion_witnesses_for_record,
    inventory_namespace,
    multi_member_inventory,
    valid_projection,
)
from tests.public_evidence_record_support import (
    network_input,
    source_record,
    source_release,
)
from tests.public_evidence_source_release_support import release_input


class StringSubclass(str):
    pass


class TupleSubclass(tuple):
    pass


def _different_release(
    source_kind: str,
) -> release.PublicEvidenceSourceReleaseDescriptor:
    raw = release_input(source_kind)
    raw["rights_proof_sha256"] = "f" * 64
    return release.build_public_evidence_source_release(raw)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("contract", "wrong_contract_v1"),
        ("foundation_scope", "wrong_scope"),
        ("tree_contract_id", "wrong_tree_v1"),
        ("ordering_contract_id", "wrong_order_v1"),
        ("source_kind", "public_hpt"),
        ("source_binding_fingerprint_sha256", "0" * 64),
        ("inventory_policy_descriptor_sha256", "1" * 64),
        ("inventory_ref", "peinv1_forged"),
        ("contract_sha256", "2" * 64),
    ),
)
def test_inventory_revalidation_rejects_fixed_and_derived_tampering(
    field_name: str, replacement: object
) -> None:
    _source, _records, inventory, _witnesses = multi_member_inventory()
    hostile = inventory._replace(**{field_name: replacement})

    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        inclusion.validate_source_record_inventory_descriptor(hostile)


def test_inventory_revalidation_rejects_authority_escalation() -> None:
    _source, _records, inventory, _witnesses = multi_member_inventory()
    for field_name, replacement in (
        ("source_bytes_authenticated", True),
        ("complete_inventory_scan_verified", True),
        ("whole_source_complete", True),
        ("publication_enabled", True),
        ("serving_authority", "enabled"),
    ):
        state = inventory.authority_state._replace(**{field_name: replacement})
        with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
            inclusion.validate_source_record_inventory_descriptor(
                inventory._replace(authority_state=state)
            )


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("contract", "wrong_contract_v1"),
        ("tree_contract_id", "wrong_tree_v1"),
        ("member_ordinal", True),
        ("leaf_sha256", "0" * 64),
        ("audit_path_sha256s", TupleSubclass()),
        ("inclusion_ref", "peinc1_forged"),
        ("contract_sha256", "3" * 64),
        ("membership_state", "source_authenticated"),
        ("authenticated_replay_state", "complete"),
        ("source_bytes_authenticated", True),
        ("complete_inventory_scan_verified", True),
        ("payload_derivation_verified", True),
        ("source_authenticity_claimed", True),
    ),
)
def test_inclusion_revalidation_rejects_tampering(
    field_name: str, replacement: object
) -> None:
    _source, _records, _inventory, witnesses = multi_member_inventory()

    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        inclusion.validate_source_record_inclusion_witness(
            witnesses[0]._replace(**{field_name: replacement})
        )


def test_valid_looking_absent_record_cannot_reuse_an_audit_path() -> None:
    source, _records, inventory, witnesses = multi_member_inventory()
    absent = source_record(source, inventory.record_kind, seed="f")

    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        inclusion.build_source_record_inclusion_witness(
            inventory,
            absent,
            witnesses[0].member_ordinal,
            witnesses[0].audit_path_sha256s,
        )


@pytest.mark.parametrize(
    "path_transform",
    (
        lambda path: path[:-1],
        lambda path: (*path, "0" * 64),
        lambda path: tuple(reversed(path)),
        lambda path: ("f" * 64, *path[1:]),
    ),
)
def test_path_truncation_extension_reordering_and_sibling_flip_fail(
    path_transform,
) -> None:
    _source, _records, inventory, witnesses = multi_member_inventory(member_count=5)
    witness = witnesses[2]
    hostile_path = path_transform(witness.audit_path_sha256s)
    assert hostile_path != witness.audit_path_sha256s

    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
        inclusion.build_source_record_inclusion_witness(
            inventory, witness.source_record, witness.member_ordinal, hostile_path
        )


def test_inventory_contract_and_member_identity_must_match() -> None:
    source = source_release("tic")
    source_reference = source_record(source, "tic_provider_group_occurrence")
    namespace = inventory_namespace(source_reference, 1)
    for field_name, replacement in (
        ("record_kind", "hpt_hospital_record"),
        ("record_identity_contract_id", "other_record_hmac_v1"),
        ("member_count", True),
        ("member_count", 2**53),
        ("payload_canonicalization_contract_id", "bad-contract"),
    ):
        hostile_namespace_map = {**namespace, field_name: replacement}
        with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError):
            inclusion.derive_inventory_leaf_sha256(
                source, hostile_namespace_map, source_reference, 0
            )


def test_projection_rejects_missing_duplicate_and_cross_release_witnesses() -> None:
    source = source_release("public_provider_directory_fhir")
    normalized_record = record.build_public_evidence_record(
        source, network_input(source)
    )
    witnesses = inclusion_witnesses_for_record(normalized_record)

    for hostile in (witnesses[:-1], (*witnesses, witnesses[0])):
        with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
            projection.build_public_evidence_adapter_projection(
                normalized_record, hostile
            )

    other_release = _different_release("public_provider_directory_fhir")
    other_record = record.build_public_evidence_record(
        other_release, network_input(other_release)
    )
    other_witness = inclusion_witnesses_for_record(other_record)[0]
    with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
        projection.build_public_evidence_adapter_projection(
            normalized_record, (other_witness, *witnesses[1:])
        )


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("contract", "wrong_contract_v1"),
        ("foundation_scope", "wrong_scope"),
        ("source_release_ref", "perel1_forged"),
        ("source_release_contract_sha256", "0" * 64),
        ("source_kind", "public_hpt"),
        ("planned_adapter_contract_id", "wrong_adapter_v1"),
        ("projection_rule_id", "wrong_rule_v1"),
        ("projection_rule_descriptor_sha256", "1" * 64),
        ("source_record_count", True),
        ("source_record_vector_sha256", "2" * 64),
        ("output_record_count", True),
        ("output_record_vector_sha256", "3" * 64),
        ("projection_ref", "peproj1_forged"),
        ("contract_sha256", "4" * 64),
    ),
)
def test_projection_revalidation_rejects_fixed_and_derived_tampering(
    field_name: str, replacement: object
) -> None:
    normalized = valid_projection()._replace(**{field_name: replacement})
    with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
        projection.validate_public_evidence_adapter_projection(normalized)


def test_projection_revalidation_rejects_every_authority_escalation() -> None:
    normalized = valid_projection()
    for field_name, replacement in (
        ("source_bytes_authenticated", True),
        ("complete_inventory_scan_verified", True),
        ("adapter_implementation_verified", True),
        ("source_authenticity_claimed", True),
        ("whole_source_complete", True),
        ("legal_ownership_claimed", True),
        ("payer_confirmed_site_claimed", True),
        ("confidence_claimed", True),
        ("database_io_enabled", True),
        ("publication_enabled", True),
    ):
        authority = normalized.authority_state._replace(**{field_name: replacement})
        with pytest.raises(projection.PublicEvidenceAdapterProjectionError):
            projection.validate_public_evidence_adapter_projection(
                normalized._replace(authority_state=authority)
            )


def test_policy_registry_and_values_are_deeply_immutable() -> None:
    with pytest.raises(TypeError):
        operator.setitem(
            policies.ADAPTER_PROJECTION_RULES,
            ("future", "future"),
            next(iter(policies.ADAPTER_PROJECTION_RULES.values())),
        )
    rule = next(iter(policies.ADAPTER_PROJECTION_RULES.values()))
    with pytest.raises(AttributeError):
        object.__setattr__(rule, "source_kind", "public_hpt")


def test_raw_identity_and_source_fields_are_rejected_without_echo() -> None:
    source = source_release("tic")
    source_reference = source_record(source, "tic_provider_group_occurrence")
    inventory_descriptor_map = {
        **inventory_namespace(source_reference, 1),
        "member_root_sha256": "0" * 64,
        "raw_tin": "sensitive-input",
    }
    with pytest.raises(primitive.PublicEvidenceSourceRecordInclusionError) as exc_info:
        inclusion.build_source_record_inventory_descriptor(
            source, inventory_descriptor_map
        )
    assert str(exc_info.value) == "public_evidence_source_record_inclusion_invalid"
    assert "sensitive-input" not in str(exc_info.value)


def test_repr_redacts_record_hmac_and_normalized_evidence() -> None:
    normalized = valid_projection()
    rendered = " ".join(
        repr(value)
        for value in (
            normalized,
            normalized.inclusion_witnesses[0],
            normalized.inclusion_witnesses[0].inventory,
        )
    )
    assert normalized.record.source_records[0].record_hmac_sha256 not in rendered
    assert normalized.record.evidence_ref not in rendered
