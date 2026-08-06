# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Behavior tests for typed source-record inventories and projections."""

from __future__ import annotations

import pytest

from public_evidence import adapter_projection_contract as projection
from public_evidence import adapter_projection_policies as policies
from public_evidence import evidence_record_contract as record
from public_evidence import evidence_record_policies as record_policies
from public_evidence import source_record_inclusion_contract as inclusion
from public_evidence import source_record_inclusion_primitives as primitive
from tests.public_evidence_adapter_projection_support import (
    inclusion_witnesses_for_record,
    inventory_namespace,
    merkle_audit_path,
    merkle_root,
    multi_member_inventory,
    projection_from_input,
    rule_scenarios,
    source_kind_for_relationship,
)
from tests.public_evidence_record_support import (
    enumeration_input,
    network_input,
    relationship_input,
    source_record,
    source_release,
)


@pytest.mark.parametrize(
    ("relationship_class", "record_input_builder"), rule_scenarios()
)
def test_every_closed_projection_rule_preserves_one_exact_record(
    relationship_class: str,
    record_input_builder,
) -> None:
    source_kind = source_kind_for_relationship(relationship_class)
    source = source_release(source_kind)
    normalized = projection_from_input(source, record_input_builder(source))
    rule = policies.ADAPTER_PROJECTION_RULES[
        (normalized.record.record_type, relationship_class)
    ]

    assert normalized.contract == projection.PUBLIC_EVIDENCE_ADAPTER_PROJECTION_CONTRACT
    assert normalized.source_kind == source_kind
    assert normalized.planned_adapter_contract_id == rule.planned_adapter_contract_id
    assert normalized.authority_state.adapter_contract_state == (
        "planned_descriptor_only"
    )
    assert normalized.projection_rule_id == rule.projection_rule_id
    assert normalized.source_record_count == len(normalized.record.source_records)
    assert normalized.output_record_count == 1
    assert tuple(
        item.source_record.source_record_ref for item in normalized.inclusion_witnesses
    ) == tuple(item.source_record_ref for item in normalized.record.source_records)
    assert (
        projection.validate_public_evidence_adapter_projection(normalized) == normalized
    )


def test_tic_npi_identity_and_tin_only_membership_remain_distinct() -> None:
    source = source_release("tic")
    raw = relationship_input(
        source,
        "tic_billing_identity_provider_group",
        tin_type="npi",
        membership_state="tin_only",
    )
    normalized = projection_from_input(source, raw)

    assert normalized.record.evidence.tax_identity.tin_type == "npi"
    assert normalized.record.evidence.membership_state == "tin_only"
    assert normalized.record.evidence.related_npi is None
    assert normalized.authority_state.legal_ownership_claimed is False
    assert normalized.authority_state.exact_rate_site_claimed is False


@pytest.mark.parametrize(
    ("npi_entity_type", "enumeration_state"),
    (
        ("individual_type_1", "active"),
        ("organization_type_2", "active"),
        ("individual_type_1", "deactivated"),
        ("organization_type_2", "deactivated"),
    ),
)
def test_nppes_projection_preserves_entity_type_and_enumeration_state(
    npi_entity_type: str, enumeration_state: str
) -> None:
    source = source_release("nppes_entity_address")
    normalized = projection_from_input(
        source,
        enumeration_input(
            source,
            npi_entity_type=npi_entity_type,
            enumeration_state=enumeration_state,
        ),
    )

    assert normalized.record.evidence.npi_entity_type == npi_entity_type
    assert normalized.record.evidence.enumeration_state == enumeration_state
    if enumeration_state == "deactivated":
        assert normalized.record.effective_interval.end_at is not None


def test_declared_inventory_root_is_separate_from_release_evidence_root() -> None:
    source, _records, inventory, witnesses = multi_member_inventory(member_count=3)

    assert inventory.member_count == 3
    assert inventory.member_root_sha256 != (
        source.completeness_attestation.evidence_root_sha256
    )
    assert inventory.source_binding_fingerprint_sha256 is not None
    assert inventory.authority_state.authenticated_replay_state == (
        "required_not_executed"
    )
    assert inventory.authority_state.complete_inventory_scan_verified is False
    assert inventory.authority_state.member_ordering_verified is False
    assert inventory.authority_state.duplicate_rejection_verified is False
    assert tuple(item.member_ordinal for item in witnesses) == (0, 1, 2)
    assert all(
        item.membership_state == "verified_against_declared_inventory"
        for item in witnesses
    )
    assert all(item.source_bytes_authenticated is False for item in witnesses)


@pytest.mark.parametrize("duplicate_member", (False, True))
def test_declared_inventory_does_not_claim_order_or_duplicate_verification(
    duplicate_member: bool,
) -> None:
    source_release_descriptor = source_release("tic")
    first_reference = source_record(
        source_release_descriptor, "tic_provider_group_occurrence", seed="1"
    )
    second_reference = source_record(
        source_release_descriptor, "tic_provider_group_occurrence", seed="2"
    )
    declared_records = (
        (first_reference, first_reference)
        if duplicate_member
        else (second_reference, first_reference)
    )
    namespace_map = inventory_namespace(first_reference, len(declared_records))
    leaf_sha256s = tuple(
        inclusion.derive_inventory_leaf_sha256(
            source_release_descriptor, namespace_map, source_reference, ordinal
        )
        for ordinal, source_reference in enumerate(declared_records)
    )
    inventory = inclusion.build_source_record_inventory_descriptor(
        source_release_descriptor,
        {**namespace_map, "member_root_sha256": merkle_root(leaf_sha256s)},
    )
    witnesses = tuple(
        inclusion.build_source_record_inclusion_witness(
            inventory,
            source_reference,
            ordinal,
            merkle_audit_path(leaf_sha256s, ordinal),
        )
        for ordinal, source_reference in enumerate(declared_records)
    )

    assert inventory.ordering_contract_id == ("declared_member_ordinal_not_verified_v1")
    assert primitive.REQUIRED_AUTHENTICATED_REPLAY_ORDERING_CONTRACT == (
        "source_record_ref_ascii_ascending_unique_v1"
    )
    assert inventory.authority_state.member_ordering_verified is False
    assert inventory.authority_state.duplicate_rejection_verified is False
    assert all(
        inclusion.validate_source_record_inclusion_witness(witness) == witness
        for witness in witnesses
    )


def test_projection_rules_cover_every_closed_normalized_relationship() -> None:
    expected_rule_keys = {
        *(
            ("tax_identity_relationship", relationship_class)
            for relationship_class in record_policies.TAX_IDENTITY_RELATIONSHIP_POLICIES
        ),
        *(
            ("tax_identity_name", relationship_class)
            for relationship_class in record_policies.TAX_IDENTITY_NAME_POLICIES
        ),
        *(
            ("entity_address", relationship_class)
            for relationship_class in record_policies.ENTITY_ADDRESS_POLICIES
        ),
        ("npi_enumeration", record_policies.NPI_ENUMERATION_RELATIONSHIP),
        (
            "provider_directory_network_location",
            record_policies.PROVIDER_DIRECTORY_NETWORK_RELATIONSHIP,
        ),
    }

    assert set(policies.ADAPTER_PROJECTION_RULES) == expected_rule_keys


@pytest.mark.parametrize("member_count", (1, 2, 3, 5, 8))
def test_tree_paths_verify_first_middle_and_last_members(member_count: int) -> None:
    _source, _records, inventory, witnesses = multi_member_inventory(
        member_count=member_count
    )
    indexes = {0, member_count // 2, member_count - 1}

    for index in sorted(indexes):
        witness = witnesses[index]
        assert inclusion.validate_source_record_inclusion_witness(witness) == witness
        assert witness.inventory.inventory_ref == inventory.inventory_ref


def test_projection_is_order_invariant_but_stored_canonically() -> None:
    source = source_release("public_provider_directory_fhir")
    normalized_record = record.build_public_evidence_record(
        source,
        network_input(source),
    )
    witnesses = inclusion_witnesses_for_record(normalized_record)

    forward = projection.build_public_evidence_adapter_projection(
        normalized_record, witnesses
    )
    reversed_input = projection.build_public_evidence_adapter_projection(
        normalized_record, tuple(reversed(witnesses))
    )

    assert forward == reversed_input
    assert tuple(
        item.source_record.source_record_ref for item in forward.inclusion_witnesses
    ) == tuple(
        sorted(item.source_record_ref for item in normalized_record.source_records)
    )


def test_frozen_one_and_three_member_vectors() -> None:
    _source, _records, one_inventory, one_witnesses = multi_member_inventory(
        member_count=1
    )
    _source, _records, three_inventory, three_witnesses = multi_member_inventory(
        member_count=3
    )

    assert one_inventory.member_root_sha256 == (
        "01a9392f616b4898c747c38ec833e1563ede7c3cb314308471ce9bc4f43dbaac"
    )
    assert one_witnesses[0].inclusion_ref == (
        "peinc1_SLRm8Axmz-EID3emWLdNsn5XngHG9L1Of4cf9077Jpk"
    )
    assert three_inventory.member_root_sha256 == (
        "eeb88b756e1595d65e3f22c7e415743ba71ef020ee2859509c444c58a7d69477"
    )
    assert tuple(item.leaf_sha256 for item in three_witnesses) == (
        "114db860c512c5e3ce7663ceb418003e2faec13e49a7e4c23e33e7cf93342a85",
        "ea3aacb7e2b97d1223aa70436c8f7f79554b9a1e6acf7bd17b28669f8fb95bc5",
        "b5a718f8fe26d65fa869a0963dac2f35306d672f1d1a24bf7820d64f0992d902",
    )
