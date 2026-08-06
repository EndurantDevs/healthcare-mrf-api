# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic builders for typed inventory and adapter-projection tests."""

from __future__ import annotations

from collections.abc import Callable

from public_evidence import adapter_projection_contract as projection
from public_evidence import evidence_record_contract as record
from public_evidence import evidence_record_primitives as record_primitive
from public_evidence import source_record_inclusion_contract as inclusion
from public_evidence import source_record_inclusion_primitives as primitive
from public_evidence import source_release_contract as release
from tests.public_evidence_record_support import (
    address_input,
    enumeration_input,
    name_input,
    network_input,
    relationship_input,
    source_record,
    source_release,
)

PAYLOAD_CANONICALIZATION_CONTRACT = "synthetic_source_payload_canonical_json_v1"
ProjectionInputBuilder = Callable[
    [release.PublicEvidenceSourceReleaseDescriptor], dict[str, object]
]
RuleScenario = tuple[str, ProjectionInputBuilder]


def inventory_namespace(
    source_record_reference: record_primitive.EvidenceSourceRecordReference,
    member_count: int,
) -> dict[str, object]:
    return {
        "record_kind": source_record_reference.record_kind,
        "record_identity_contract_id": (source_record_reference.identity_contract_id),
        "payload_canonicalization_contract_id": (PAYLOAD_CANONICALIZATION_CONTRACT),
        "member_count": member_count,
    }


def _largest_power_of_two_less_than(value: int) -> int:
    return 1 << ((value - 1).bit_length() - 1)


def merkle_root(leaf_sha256s: tuple[str, ...]) -> str:
    if len(leaf_sha256s) == 1:
        return leaf_sha256s[0]
    split = _largest_power_of_two_less_than(len(leaf_sha256s))
    return primitive.derive_inventory_node_sha256(
        merkle_root(leaf_sha256s[:split]),
        merkle_root(leaf_sha256s[split:]),
    )


def merkle_audit_path(
    leaf_sha256s: tuple[str, ...], member_ordinal: int
) -> tuple[str, ...]:
    if len(leaf_sha256s) == 1:
        return ()
    split = _largest_power_of_two_less_than(len(leaf_sha256s))
    if member_ordinal < split:
        return (
            *merkle_audit_path(leaf_sha256s[:split], member_ordinal),
            merkle_root(leaf_sha256s[split:]),
        )
    return (
        *merkle_audit_path(leaf_sha256s[split:], member_ordinal - split),
        merkle_root(leaf_sha256s[:split]),
    )


def inventory_with_witnesses(
    source_release_descriptor: release.PublicEvidenceSourceReleaseDescriptor,
    source_records: tuple[record_primitive.EvidenceSourceRecordReference, ...],
) -> tuple[
    primitive.PublicEvidenceSourceRecordInventoryDescriptor,
    tuple[primitive.PublicEvidenceSourceRecordInclusionWitness, ...],
]:
    ordered_records = tuple(
        sorted(
            source_records,
            key=lambda source_reference: source_reference.source_record_ref,
        )
    )
    assert ordered_records
    assert (
        len({source_reference.record_kind for source_reference in ordered_records}) == 1
    )
    assert (
        len(
            {
                source_reference.identity_contract_id
                for source_reference in ordered_records
            }
        )
        == 1
    )
    inventory_namespace_fields = inventory_namespace(
        ordered_records[0], len(ordered_records)
    )
    leaves = tuple(
        inclusion.derive_inventory_leaf_sha256(
            source_release_descriptor,
            inventory_namespace_fields,
            source_reference,
            index,
        )
        for index, source_reference in enumerate(ordered_records)
    )
    inventory = inclusion.build_source_record_inventory_descriptor(
        source_release_descriptor,
        {
            **inventory_namespace_fields,
            "member_root_sha256": merkle_root(leaves),
        },
    )
    witnesses = tuple(
        inclusion.build_source_record_inclusion_witness(
            inventory,
            source_reference,
            index,
            merkle_audit_path(leaves, index),
        )
        for index, source_reference in enumerate(ordered_records)
    )
    return inventory, witnesses


def inclusion_witnesses_for_record(
    normalized_record: record.PublicEvidenceRecord,
) -> tuple[primitive.PublicEvidenceSourceRecordInclusionWitness, ...]:
    witnesses: list[primitive.PublicEvidenceSourceRecordInclusionWitness] = []
    record_kinds = sorted(
        {
            source_reference.record_kind
            for source_reference in normalized_record.source_records
        }
    )
    for record_kind in record_kinds:
        same_kind_records = tuple(
            source_reference
            for source_reference in normalized_record.source_records
            if source_reference.record_kind == record_kind
        )
        _inventory, kind_witnesses = inventory_with_witnesses(
            normalized_record.release, same_kind_records
        )
        witnesses.extend(kind_witnesses)
    return tuple(
        sorted(
            witnesses,
            key=lambda witness: witness.source_record.source_record_ref,
        )
    )


def projection_from_input(
    source_release_descriptor: release.PublicEvidenceSourceReleaseDescriptor,
    raw_record: dict[str, object],
) -> projection.PublicEvidenceAdapterProjection:
    normalized = record.build_public_evidence_record(
        source_release_descriptor, raw_record
    )
    return projection.build_public_evidence_adapter_projection(
        normalized, inclusion_witnesses_for_record(normalized)
    )


def multi_member_inventory(source_kind: str = "tic", *, member_count: int = 3) -> tuple[
    release.PublicEvidenceSourceReleaseDescriptor,
    tuple[record_primitive.EvidenceSourceRecordReference, ...],
    primitive.PublicEvidenceSourceRecordInventoryDescriptor,
    tuple[primitive.PublicEvidenceSourceRecordInclusionWitness, ...],
]:
    source_release_descriptor = source_release(source_kind)
    record_kind = {
        "tic": "tic_provider_group_occurrence",
        "public_provider_directory_fhir": "fhir_organization",
        "nppes_entity_address": "nppes_registry_record",
        "public_hpt": "hpt_hospital_record",
    }[source_kind]
    source_records = tuple(
        source_record(
            source_release_descriptor,
            record_kind,
            seed=hex(index + 1)[2:],
        )
        for index in range(member_count)
    )
    inventory, witnesses = inventory_with_witnesses(
        source_release_descriptor, source_records
    )
    return source_release_descriptor, source_records, inventory, witnesses


def valid_projection() -> projection.PublicEvidenceAdapterProjection:
    source_release_descriptor = source_release("tic")
    return projection_from_input(
        source_release_descriptor,
        relationship_input(
            source_release_descriptor,
            "tic_billing_identity_provider_group",
            membership_state="members_present",
        ),
    )


_RULE_SCENARIOS: tuple[RuleScenario, ...] = (
    (
        "tic_billing_identity_provider_group",
        lambda source_release_descriptor: relationship_input(
            source_release_descriptor,
            "tic_billing_identity_provider_group",
            membership_state="members_present",
        ),
    ),
    (
        "tic_provider_group_member",
        lambda source_release_descriptor: relationship_input(
            source_release_descriptor,
            "tic_provider_group_member",
            membership_state="members_present",
        ),
    ),
    (
        "tic_source_reported_business_name",
        lambda source_release_descriptor: name_input(
            source_release_descriptor, "tic_source_reported_business_name"
        ),
    ),
    (
        "fhir_same_organization_identifier",
        lambda source_release_descriptor: relationship_input(
            source_release_descriptor, "fhir_same_organization_identifier"
        ),
    ),
    (
        "fhir_same_organization_reported_name",
        lambda source_release_descriptor: name_input(
            source_release_descriptor,
            "fhir_same_organization_reported_name",
        ),
    ),
    ("nppes_npi_enumeration", enumeration_input),
    (
        "nppes_npi_practice_location",
        lambda source_release_descriptor: address_input(
            source_release_descriptor, "nppes_npi_practice_location"
        ),
    ),
    (
        "nppes_npi_mailing_address",
        lambda source_release_descriptor: address_input(
            source_release_descriptor, "nppes_npi_mailing_address"
        ),
    ),
    (
        "fhir_npi_directory_address",
        lambda source_release_descriptor: address_input(
            source_release_descriptor, "fhir_npi_directory_address"
        ),
    ),
    (
        "fhir_entity_directory_address",
        lambda source_release_descriptor: address_input(
            source_release_descriptor, "fhir_entity_directory_address"
        ),
    ),
    ("fhir_provider_directory_network_location", network_input),
    (
        "hpt_hospital_tax_identity_entity_candidate",
        lambda source_release_descriptor: relationship_input(
            source_release_descriptor,
            "hpt_hospital_tax_identity_entity_candidate",
        ),
    ),
    (
        "hpt_hospital_tax_identity_npi_candidate",
        lambda source_release_descriptor: relationship_input(
            source_release_descriptor,
            "hpt_hospital_tax_identity_npi_candidate",
        ),
    ),
    (
        "hpt_source_reported_hospital_name_candidate",
        lambda source_release_descriptor: name_input(
            source_release_descriptor,
            "hpt_source_reported_hospital_name_candidate",
        ),
    ),
    (
        "hpt_entity_location_candidate",
        lambda source_release_descriptor: address_input(
            source_release_descriptor, "hpt_entity_location_candidate"
        ),
    ),
)


def rule_scenarios() -> tuple[RuleScenario, ...]:
    """Return every closed projection rule with its synthetic input builder."""
    return _RULE_SCENARIOS


def source_kind_for_relationship(relationship_class: str) -> str:
    if relationship_class.startswith("tic_"):
        return "tic"
    if relationship_class.startswith("fhir_"):
        return "public_provider_directory_fhir"
    if relationship_class.startswith("nppes_"):
        return "nppes_entity_address"
    return "public_hpt"
