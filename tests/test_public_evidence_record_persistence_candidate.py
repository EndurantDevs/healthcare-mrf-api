# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Behavior contracts for dormant public-evidence row candidates."""

from __future__ import annotations

import hashlib

import pytest

from public_evidence import evidence_record_contract as record_contract
from public_evidence.evidence_record_policies import NETWORK_RECORD_FIELDS
from public_evidence import record_persistence_candidate_contract as candidate_contract
from public_evidence import record_persistence_candidate_primitives as candidate_primitive
from tests.public_evidence_record_support import (
    address_input,
    enumeration_input,
    name_input,
    network_input,
    relationship_input,
    source_record,
    source_release,
)


def _records_by_name() -> dict[str, record_contract.PublicEvidenceRecord]:
    tic = source_release("tic")
    nppes = source_release("nppes_entity_address")
    fhir = source_release("public_provider_directory_fhir")
    return {
        "relationship": record_contract.build_public_evidence_record(
            tic,
            relationship_input(
                tic,
                "tic_provider_group_member",
                membership_state="members_present",
            ),
        ),
        "name": record_contract.build_public_evidence_record(
            tic,
            name_input(tic, "tic_source_reported_business_name"),
        ),
        "enumeration": record_contract.build_public_evidence_record(
            nppes,
            enumeration_input(nppes),
        ),
        "address": record_contract.build_public_evidence_record(
            nppes,
            address_input(nppes, "nppes_npi_practice_location"),
        ),
        "network": record_contract.build_public_evidence_record(
            fhir,
            network_input(fhir),
        ),
    }


@pytest.mark.parametrize(
    ("case_name", "row_type", "link_count"),
    (
        ("relationship", candidate_primitive.TaxIdentityRelationshipRow, 1),
        ("name", candidate_primitive.TaxIdentityNameRow, 1),
        ("enumeration", candidate_primitive.NpiEnumerationRow, 1),
        ("address", candidate_primitive.EntityAddressRow, 1),
        (
            "network",
            candidate_primitive.ProviderDirectoryNetworkLocationRow,
            5,
        ),
    ),
)
def test_all_record_variants_build_one_common_link_vector_and_typed_row(
    case_name: str,
    row_type: type,
    link_count: int,
) -> None:
    source_record_value = _records_by_name()[case_name]
    candidate = candidate_contract.build_public_evidence_record_persistence_candidate(
        source_record_value
    )

    assert type(candidate.typed_row) is row_type
    assert str(candidate.typed_row) == repr(candidate.typed_row)
    assert type(candidate.common_row) is candidate_primitive.PublicEvidenceRecordCommonRow
    assert type(candidate.source_link_rows) is tuple
    assert len(candidate.source_link_rows) == link_count
    assert candidate.common_row.source_record_count == link_count
    assert candidate.common_row.typed_row_sha256 == candidate.typed_row.row_sha256
    assert candidate.common_row.record_type == source_record_value.record_type
    assert candidate.common_row.relationship_class == (
        source_record_value.evidence.relationship_class
    )
    assert tuple(link.source_record_ordinal for link in candidate.source_link_rows) == (
        tuple(range(link_count))
    )
    assert tuple(link.source_record_ref for link in candidate.source_link_rows) == (
        tuple(
            source_record_value_item.source_record_ref
            for source_record_value_item in source_record_value.source_records
        )
    )
    assert all(
        link.evidence_ref == candidate.common_row.evidence_ref
        and link.source_release_ref == candidate.common_row.source_release_ref
        and link.source_release_contract_sha256
        == candidate.common_row.source_release_contract_sha256
        and link.source_kind == candidate.common_row.source_kind
        for link in candidate.source_link_rows
    )

    rebuilt = (
        candidate_contract.validate_public_evidence_record_persistence_candidate(
            candidate
        )
    )
    assert rebuilt == candidate
    assert rebuilt is not candidate
    assert rebuilt.record is not candidate.record
    assert rebuilt.source_link_rows is not candidate.source_link_rows


def test_relationship_and_name_rows_preserve_exact_normalized_semantics() -> None:
    candidate_by_name = {
        name: candidate_contract.build_public_evidence_record_persistence_candidate(
            evidence_record
        )
        for name, evidence_record in _records_by_name().items()
    }
    relationship = candidate_by_name["relationship"].typed_row
    relationship_evidence = candidate_by_name["relationship"].record.evidence
    assert relationship.tax_identity_ref == relationship_evidence.tax_identity.tax_identity_ref
    assert relationship.provider_group_ref == (
        relationship_evidence.provider_group.provider_group_ref
    )
    assert relationship.related_npi == relationship_evidence.related_npi
    assert relationship.source_entity_ref is None
    assert relationship.membership_state == "members_present"
    assert relationship.candidate_only is False

    name = candidate_by_name["name"].typed_row
    name_evidence = candidate_by_name["name"].record.evidence
    assert name.tax_identity_ref == name_evidence.tax_identity.tax_identity_ref
    assert name.provider_group_ref == name_evidence.provider_group.provider_group_ref
    assert name.source_entity_ref is None
    assert name.source_reported_name == name_evidence.source_reported_name
    assert name.name_kind == name_evidence.name_kind
    assert name.name_normalization_contract_id == (
        name_evidence.name_normalization_contract_id
    )
    assert name.normalized_name_sha256 == name_evidence.normalized_name_sha256
    assert name.candidate_only is False


def test_enumeration_address_and_network_rows_flatten_every_nested_value() -> None:
    candidate_by_name = {
        name: candidate_contract.build_public_evidence_record_persistence_candidate(
            evidence_record
        )
        for name, evidence_record in _records_by_name().items()
    }
    enumeration = candidate_by_name["enumeration"].typed_row
    enumeration_evidence = candidate_by_name["enumeration"].record.evidence
    assert enumeration.npi == enumeration_evidence.npi
    assert enumeration.npi_entity_type == enumeration_evidence.npi_entity_type
    assert enumeration.enumeration_state == enumeration_evidence.enumeration_state

    address = candidate_by_name["address"].typed_row
    address_evidence = candidate_by_name["address"].record.evidence
    assert address.subject_npi == address_evidence.subject_npi
    assert address.source_entity_ref is None
    for field_name in address_evidence.address._fields:
        assert getattr(address, field_name) == getattr(
            address_evidence.address, field_name
        )

    network = candidate_by_name["network"].typed_row
    network_evidence = candidate_by_name["network"].record.evidence
    assert network.npi == network_evidence.npi
    for field_name in network_evidence.address._fields:
        assert getattr(network, field_name) == getattr(
            network_evidence.address, field_name
        )
    for field_name, _record_kind in NETWORK_RECORD_FIELDS:
        assert getattr(network, f"{field_name}_ref") == getattr(
            network_evidence.network_context, field_name
        ).source_record_ref
    assert network.role_active is True
    assert network.pricing_bridge_state == "not_evaluated"


def test_record_input_reordering_is_canonical_but_membership_changes_identity() -> None:
    fhir = source_release("public_provider_directory_fhir")
    raw = network_input(fhir)
    first = candidate_contract.build_public_evidence_record_persistence_candidate(
        record_contract.build_public_evidence_record(fhir, raw)
    )
    reordered_input_by_field = dict(raw)
    reordered_input_by_field["source_records"] = tuple(
        reversed(raw["source_records"])
    )
    second = candidate_contract.build_public_evidence_record_persistence_candidate(
        record_contract.build_public_evidence_record(fhir, reordered_input_by_field)
    )
    assert second == first

    nppes = source_release("nppes_entity_address")
    first_raw = enumeration_input(nppes)
    changed_input_by_field = dict(first_raw)
    changed_input_by_field["source_records"] = (
        source_record(nppes, "nppes_registry_record", seed="3"),
    )
    first_member = candidate_contract.build_public_evidence_record_persistence_candidate(
        record_contract.build_public_evidence_record(nppes, first_raw)
    )
    changed_member = (
        candidate_contract.build_public_evidence_record_persistence_candidate(
            record_contract.build_public_evidence_record(
                nppes, changed_input_by_field
            )
        )
    )
    assert first_member.typed_row._replace(evidence_ref="", row_sha256="") == (
        changed_member.typed_row._replace(evidence_ref="", row_sha256="")
    )
    assert first_member.common_row.source_link_vector_sha256 != (
        changed_member.common_row.source_link_vector_sha256
    )
    assert first_member.candidate_ref != changed_member.candidate_ref


def test_exact_utf8_name_digest_avoids_json_unicode_parity_dependency() -> None:
    source_release_descriptor = source_release("tic")
    composed = "Synthetic Caf\u00e9"
    decomposed = "Synthetic Cafe\u0301"
    candidates = []
    for reported_name in (composed, decomposed):
        normalized = record_contract.build_public_evidence_record(
            source_release_descriptor,
            name_input(
                source_release_descriptor,
                "tic_source_reported_business_name",
                source_reported_name=reported_name,
            ),
        )
        candidates.append(
            candidate_contract.build_public_evidence_record_persistence_candidate(
                normalized
            )
        )

    first, second = candidates
    assert first.typed_row.normalized_name_sha256 == (
        second.typed_row.normalized_name_sha256
    )
    assert first.typed_row.source_reported_name_utf8_sha256 == hashlib.sha256(
        composed.encode("utf-8")
    ).hexdigest()
    assert second.typed_row.source_reported_name_utf8_sha256 == hashlib.sha256(
        decomposed.encode("utf-8")
    ).hexdigest()
    assert first.typed_row.source_reported_name_utf8_sha256 != (
        second.typed_row.source_reported_name_utf8_sha256
    )
    assert first.typed_row.row_sha256 != second.typed_row.row_sha256
    assert "source_reported_name" not in candidate_contract._row_payload(
        first.typed_row
    )


def test_frozen_all_variant_candidate_vectors() -> None:
    candidate_by_name = {
        name: candidate_contract.build_public_evidence_record_persistence_candidate(
            evidence_record
        )
        for name, evidence_record in _records_by_name().items()
    }
    vector_by_name = {
        name: (
            candidate.candidate_ref,
            candidate.contract_sha256,
            candidate.common_row.row_sha256,
            candidate.common_row.source_link_vector_sha256,
            candidate.typed_row.row_sha256,
        )
        for name, candidate in candidate_by_name.items()
    }
    assert vector_by_name == {
        "relationship": (
            "pepc1_EgyV6Mm_O8Q_UcCcy6d8sk7I6qykXQZCOE6PMd6optc",
            "7a76079f0711653d80e54c8d4004569a6aae68119cf33ebb2c26b62dcac33f96",
            "0da07c0d12a2ca7debaa3a5da0aee1f6b4cc430935cd35742db6e10029bb071e",
            "55cf4d386da03619fc9fa0f736d7b009073c25510cc3ac0d66d5d4d784d85692",
            "f00b8e95a87407f9ed1577c751c9393aaa4a94c9047aea0becc40386c912ec46",
        ),
        "name": (
            "pepc1_8lyb0rR7UStFNEK8h5W4ETNdZu-2QTgsPi2iMYSAeWQ",
            "b05db87b5cd845645cc9fec0b220b3f817004b42123f4c17618dd15767db1c57",
            "993d3cf89eeda797e62ccc86410f4ea3e1d92bfe667729f3ff859e0c3a95c426",
            "3aec9bfca33a139893af7c986d5f62f7159755728995d7f2d3f0e0f5ef68e6fd",
            "17310b08c62631b4e51841f18af96094b7f2981df3de0a8b5c99441498e16c91",
        ),
        "enumeration": (
            "pepc1_ZNUNv6VH-mOpKEDaJlJWXQt8yqR_BuqL8QCBuddZ-X0",
            "de4717e8ac99ea26039cbdf834ae9c4641d18b6bf73c2842168a1b8979afde91",
            "018f379e719c1c9e9289cb062ce8bafd6d85fe869adf06fb4f169cb2349d766e",
            "4b27ef8b65682e9cf0e26e3d47eccf34eb61ebf683f37e6e417b3e28a7ff6dc7",
            "e1edba22da2d7e715b5cfad3fabf9973f45aa1a43e7e32d5042123eb9521cbce",
        ),
        "address": (
            "pepc1_9cNRvZEj4nk88C1ULiTl9qC9veryO9ObgU7NSll5grs",
            "a938f52b5421783193273325e0fc7dbe0754e8be3d81e5f715ee3c944775b05e",
            "aebeab38357b5ee080c938c78c06d762c8409124c88a0ffcd4d06a6285113db0",
            "e9a25bb091d8020041649c9db4c6508d5ec308450bbdbeb4c1a119f1009d3c33",
            "840db7b3e65c7f7f754d682c84161eefc9f1b4eca4e6a52f4c3705a9eb63e7a8",
        ),
        "network": (
            "pepc1_y0vCOOU4fOXwpO4GPdVg34t9eljD5HTsT7BgxBLBkvs",
            "4901a8b630ec9c3558f9a34f8936a6b4bac40b004777559610f62b3ce0b08f4f",
            "535f5a3870f11d3f402ff7e38dcee29c0a3ad16601e18e2d0e66fb3d9113c8cf",
            "1fe564c98286bea5f52095477495826f420efd20edc73a2d5f3e085eaf37062b",
            "7eae8addecf01fb4672ad068cf0c714f7b9a322134166da88035874ed53a3240",
        ),
    }
