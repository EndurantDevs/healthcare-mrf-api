# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import struct
import hashlib

import pytest

from api import ptg2_manifest_artifacts as serving_manifest
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT,
    PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE,
    PTG2_MANIFEST_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_MAPPING_RECORD_SIZE,
    PTG2ManifestArtifactError,
    build_dense_id_mapping,
    read_global_membership_sidecar,
    read_global_sidecar_entries,
    read_global_local_id_mapping,
    lookup_global_sidecar_members,
    lookup_global_sidecar_members_many,
    write_global_membership_sidecar,
    write_global_local_id_mapping,
)


GLOBAL_A = bytes.fromhex("0000000000000000000000000000000a")
GLOBAL_B = bytes.fromhex("0000000000000000000000000000000b")
GLOBAL_C = bytes.fromhex("0000000000000000000000000000000c")


def test_manifest_artifact_provider_address_verification_marks_inferred_nppes_address():
    item = {
        "network_names": ["C2"],
        "address": {
            "first_line": "900 W Temple Ave",
            "city": "Effingham",
            "state": "IL",
            "postal_code": "62401",
            "address_sources": ["nppes"],
        },
        "location_source": "npi_address",
    }

    verification = serving_manifest._manifest_address_verification(item)

    assert verification["rate_network_binding"] == "tic_provider_group_npi_tin"
    assert verification["address_network_binding"] == "inferred_from_provider_identity"
    assert verification["address_evidence_level"] == "nppes_provider_address"
    assert verification["requires_location_confirmation"] is True
    assert verification["displayed_address_present"] is True
    assert verification["network_bound_address"] is False
    assert verification["address_sources"] == ["nppes"]


def test_manifest_artifact_does_not_treat_bare_ptg_source_as_payer_confirmed():
    verification = serving_manifest._manifest_address_verification(
        {
            "address": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["nppes", "ptg"],
            }
        }
    )

    assert verification["address_network_binding"] == "inferred_from_provider_identity"
    assert verification["address_evidence_level"] == "nppes_provider_address"
    assert verification["requires_location_confirmation"] is True
    assert verification["address_sources"] == ["nppes"]


def test_manifest_artifact_marks_explicit_payer_location_as_payer_confirmed():
    verification = serving_manifest._manifest_address_verification(
        {
            "location_source": "payer_provider_group_location",
            "source_trace": [
                {
                    "source_file_version_id": "source-version-1",
                    "original_url": "https://example.test/in-network-rates.json.gz",
                }
            ],
            "address": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["ptg"],
                "address_verification_evidence": {
                    "source": "payer_provider_group_location",
                    "provider_group_id": 1662,
                    "json_pointer": "/provider_references/0/provider_groups/0/address",
                },
            },
        }
    )

    assert verification["address_network_binding"] == "payer_confirmed_location"
    assert verification["address_evidence_level"] == "payer_confirmed_location"
    assert verification["requires_location_confirmation"] is False
    assert verification["network_bound_address"] is True
    assert verification["address_sources"] == ["ptg"]


def test_manifest_artifact_does_not_mark_payer_location_without_source_trace():
    verification = serving_manifest._manifest_address_verification(
        {
            "location_source": "payer_provider_group_location",
            "address": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["ptg"],
                "address_verification_evidence": {
                    "source": "payer_provider_group_location",
                    "provider_group_id": 1662,
                    "json_pointer": "/provider_references/0/provider_groups/0/address",
                },
            },
        }
    )

    assert verification["address_network_binding"] == "inferred_from_provider_identity"
    assert verification["address_evidence_level"] == "unified_provider_address"
    assert verification["requires_location_confirmation"] is True
    assert "address_sources" not in verification


def test_manifest_artifact_does_not_mark_payer_location_without_source_record_evidence():
    verification = serving_manifest._manifest_address_verification(
        {
            "location_source": "payer_provider_group_location",
            "address": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["ptg"],
            },
        }
    )

    assert verification["address_network_binding"] == "inferred_from_provider_identity"
    assert verification["requires_location_confirmation"] is True
    assert "address_sources" not in verification


def test_manifest_artifact_marks_provider_directory_network_name_match_as_corroborated():
    verification = serving_manifest._manifest_address_verification(
        {
            "network_names": ["C2"],
            "address": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": False,
                "provider_directory_network_names": ["C2"],
                "provider_directory_network_matches": [
                    {
                        "ref": "Organization/network-1",
                        "resource_id": "network-1",
                        "name": "C2",
                        "aliases": ["C Two"],
                    }
                ],
                "address_verification_evidence": {
                    "source": "provider_directory_fhir",
                    "matched_on": "npi_address_key_role_location",
                },
            },
        }
    )

    assert verification["address_network_binding"] == "payer_directory_corroborated_location"
    assert verification["address_evidence_level"] == "payer_directory_network_location"
    assert verification["requires_location_confirmation"] is False
    assert verification["provider_directory_plan_context_matched"] is False
    assert verification["provider_directory_network_name_matched"] is True
    assert verification["provider_directory_network_matches"] == [
        {
            "ptg_network_name": "C2",
            "provider_directory_network_name": "C2",
            "provider_directory_network_resource_id": "network-1",
            "provider_directory_network_ref": "Organization/network-1",
        }
    ]
    assert verification["address_verification_evidence"]["matched_on"] == (
        "npi_address_key_role_location_network_name"
    )
    assert verification["address_verification_evidence"]["network_name_context_matched"] is True


def test_manifest_artifact_network_name_matching_does_not_mutate_payload():
    raw_match = {
        "ref": "Organization/network-1",
        "resource_id": "network-1",
        "name": "C2",
        "aliases": ["C Two"],
    }
    item = {
        "network_names": ["C2"],
        "address": {
            "first_line": "900 W Temple Ave",
            "city": "Effingham",
            "state": "IL",
            "postal_code": "62401",
            "address_sources": ["provider_directory_fhir"],
            "provider_directory_plan_context_matched": False,
            "provider_directory_network_names": ["C2"],
            "provider_directory_network_matches": [raw_match],
            "address_verification_evidence": {
                "source": "provider_directory_fhir",
                "matched_on": "npi_address_key_role_location",
            },
        },
    }

    first = serving_manifest._manifest_address_verification(item)
    second = serving_manifest._manifest_address_verification(item)

    assert item["address"]["provider_directory_network_matches"] == [raw_match]
    assert second["provider_directory_network_matches"] == first["provider_directory_network_matches"]
    assert len(second["provider_directory_network_matches"]) == 1


def test_manifest_artifact_accepts_pre_shaped_provider_directory_network_match():
    verification = serving_manifest._manifest_address_verification(
        {
            "network_names": ["C2"],
            "address": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": False,
                "provider_directory_network_matches": [
                    {
                        "ptg_network_name": "C2",
                        "provider_directory_network_name": "C2",
                        "provider_directory_network_resource_id": "network-1",
                        "provider_directory_network_ref": "Organization/network-1",
                    }
                ],
                "address_verification_evidence": {
                    "source": "provider_directory_fhir",
                    "matched_on": "npi_address_key_role_location",
                },
            },
        }
    )

    assert verification["address_network_binding"] == "payer_directory_corroborated_location"
    assert verification["provider_directory_network_name_matched"] is True
    assert verification["provider_directory_network_matches"] == [
        {
            "ptg_network_name": "C2",
            "provider_directory_network_name": "C2",
            "provider_directory_network_resource_id": "network-1",
            "provider_directory_network_ref": "Organization/network-1",
        }
    ]


def test_manifest_artifact_downgrades_pre_shaped_network_match_without_served_network_names():
    verification = serving_manifest._manifest_address_verification(
        {
            "address": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": False,
                "provider_directory_network_matches": [
                    {
                        "ptg_network_name": "C2",
                        "provider_directory_network_name": "C2",
                    }
                ],
                "address_verification_evidence": {
                    "source": "provider_directory_fhir",
                    "matched_on": "npi_address_key_role_location_network_name",
                    "network_name_context_matched": True,
                },
            },
        }
    )

    assert verification["address_network_binding"] == "inferred_from_provider_identity"
    assert verification["address_evidence_level"] == "provider_directory_address"
    assert verification["requires_location_confirmation"] is True
    assert "provider_directory_network_name_matched" not in verification
    assert "provider_directory_network_matches" not in verification
    assert verification["address_verification_evidence"]["matched_on"] == "npi_address_key_role_location"


def test_manifest_artifact_normalizes_provider_directory_boolean_strings():
    verification = serving_manifest._manifest_address_verification(
        {
            "network_names": ["C2"],
            "address": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": "false",
                "provider_directory_network_names": '["C2"]',
                "provider_directory_network_matches": [
                    {
                        "ref": "Organization/network-1",
                        "resource_id": "network-1",
                        "name": "C2",
                    }
                ],
                "address_verification_evidence": {
                    "source": "provider_directory_fhir",
                    "matched_on": "npi_address_key_role_location",
                },
            },
        }
    )

    assert verification["provider_directory_plan_context_matched"] is False
    assert verification["provider_directory_network_name_matched"] is True
    assert verification["provider_directory_network_names"] == ["C2"]


def test_manifest_artifact_does_not_accept_loose_provider_directory_network_marker():
    verification = serving_manifest._manifest_address_verification(
        {
            "network_names": ["C2"],
            "address": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": False,
                "provider_directory_network_name_matched": True,
                "address_verification_evidence": {
                    "source": "provider_directory_fhir",
                    "matched_on": "npi_address_key_role_location",
                },
            },
        }
    )

    assert verification["address_network_binding"] == "inferred_from_provider_identity"
    assert verification["address_evidence_level"] == "provider_directory_address"
    assert verification["requires_location_confirmation"] is True
    assert "provider_directory_network_name_matched" not in verification


def test_manifest_artifact_strips_stale_provider_directory_network_evidence():
    verification = serving_manifest._manifest_address_verification(
        {
            "network_names": ["C2"],
            "address": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": False,
                "provider_directory_network_name_matched": True,
                "provider_directory_network_matches": [
                    {
                        "ref": "Organization/network-2",
                        "resource_id": "network-2",
                        "name": "PPO NDC",
                    }
                ],
                "address_verification_evidence": {
                    "source": "provider_directory_fhir",
                    "matched_on": "npi_address_key_role_location_network_name",
                    "network_name_context_matched": True,
                    "network_name_matches": [
                        {
                            "ptg_network_name": "C2",
                            "provider_directory_network_name": "C2",
                        }
                    ],
                },
            },
        }
    )

    assert verification["address_network_binding"] == "inferred_from_provider_identity"
    assert verification["address_evidence_level"] == "provider_directory_address"
    assert verification["requires_location_confirmation"] is True
    assert "provider_directory_network_name_matched" not in verification
    assert "provider_directory_network_matches" not in verification
    assert verification["address_verification_evidence"]["matched_on"] == "npi_address_key_role_location"
    assert "network_name_context_matched" not in verification["address_verification_evidence"]
    assert "network_name_matches" not in verification["address_verification_evidence"]


def test_manifest_artifact_provider_set_without_address_has_explicit_unknown_verification():
    verification = serving_manifest._manifest_address_verification({"provider_name": "TiC provider set"})

    assert verification["rate_network_binding"] == "tic_provider_group_npi_tin"
    assert verification["address_network_binding"] == "inferred_from_provider_identity"
    assert verification["address_evidence_level"] == "unknown"
    assert verification["requires_location_confirmation"] is True
    assert verification["displayed_address_present"] is False
    assert verification["network_bound_address"] is False
    assert verification["reason"] == (
        "PTG proves the provider identity is in network, but no displayable address is available."
    )


def test_manifest_artifact_no_display_verification_drops_nested_address_evidence():
    verification = serving_manifest._manifest_address_verification(
        {
            "provider_name": "Example Surgeon",
            "location_source": "entity_address_unified",
            "address_sources": ["nppes"],
            "address_payload": {
                "address_key": "00000000-0000-0000-0000-000000000001",
                "location_source": "entity_address_unified",
                "address_sources": ["nppes"],
            },
        }
    )

    assert verification == {
        "rate_network_binding": "tic_provider_group_npi_tin",
        "address_network_binding": "inferred_from_provider_identity",
        "address_evidence_level": "unknown",
        "requires_location_confirmation": True,
        "reason": "PTG proves the provider identity is in network, but no displayable address is available.",
        "displayed_address_present": False,
        "network_bound_address": False,
    }


def test_manifest_artifact_treats_nested_address_as_displayable():
    verification = serving_manifest._manifest_address_verification(
        {
            "network_names": ["C2"],
            "address": {
                "first_line": "100 Test St",
                "city": "Chicago",
                "state": "IL",
                "postal_code": "60601",
            },
            "address_payload": {
                "location_source": "provider_directory_fhir",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": True,
            },
        }
    )

    assert verification["displayed_address_present"] is True
    assert verification["address_network_binding"] == "payer_directory_corroborated_location"
    assert verification["address_evidence_level"] == "payer_directory_network_location"


def test_search_manifest_snapshot_adds_address_verification_to_expanded_provider_rows():
    snapshot = serving_manifest.PTG2ManifestSnapshot(
        snapshot_id="ptg2:202606:test",
        source_uri="file:///tmp/manifest.json",
        manifest={},
        plans={"010854205": {"plan_name": "Heartland Dental"}},
        procedures={"CPT:29888": {"procedure_name": "ACL reconstruction"}},
        rows=(
            {
                "plan_id": "010854205",
                "reported_code": "29888",
                "reported_code_system": "CPT",
                "provider_set_hash": "provider-set",
                "prices": [{"negotiated_rate": 1138.57, "negotiated_type": "negotiated"}],
                "network_names": ["C2"],
            },
        ),
        providers={
            "provider-1": {
                "npi": 1234567890,
                "provider_name": "Example Surgeon",
                "location_source": "npi_address",
                "address_payload": {
                    "first_line": "900 W Temple Ave",
                    "city": "Effingham",
                    "state": "IL",
                    "postal_code": "62401",
                    "address_sources": ["nppes"],
                },
            }
        },
        price_sets={},
        price_atoms={},
        provider_set_members={"provider-set": ("provider-1",)},
        price_set_members={},
        source_trace_sets={},
    )
    pagination = type("Pagination", (), {"limit": 5, "offset": 0})()

    payload = serving_manifest.search_ptg2_manifest_snapshot(
        snapshot,
        {
            "plan_id": "010854205",
            "code": "29888",
            "code_system": "CPT",
            "include_providers": "true",
        },
        pagination,
        mode_value="product_search",
    )

    assert payload is not None
    item = payload["items"][0]
    assert item["address"]["first_line"] == "900 W Temple Ave"
    assert item["address_verification"]["rate_network_binding"] == "tic_provider_group_npi_tin"
    assert item["address_verification"]["address_evidence_level"] == "nppes_provider_address"
    assert item["address_verification"]["displayed_address_present"] is True


def test_search_manifest_snapshot_strips_no_display_provider_address_fields():
    snapshot = serving_manifest.PTG2ManifestSnapshot(
        snapshot_id="ptg2:202606:test",
        source_uri="file:///tmp/manifest.json",
        manifest={},
        plans={"010854205": {"plan_name": "Heartland Dental"}},
        procedures={"CPT:29888": {"procedure_name": "ACL reconstruction"}},
        rows=(
            {
                "plan_id": "010854205",
                "reported_code": "29888",
                "reported_code_system": "CPT",
                "provider_set_hash": "provider-set",
                "prices": [{"negotiated_rate": 1138.57, "negotiated_type": "negotiated"}],
                "network_names": ["C2"],
            },
        ),
        providers={
            "provider-1": {
                "npi": 1234567890,
                "provider_name": "Example Surgeon",
                "location_source": "entity_address_unified",
                "address_payload": {
                    "address_key": "00000000-0000-0000-0000-000000000001",
                    "telephone_number": "217-555-0100",
                },
                "telephone_number": "217-555-0100",
                "coordinates": {"lat": 39.12004, "long": -88.54338},
            }
        },
        price_sets={},
        price_atoms={},
        provider_set_members={"provider-set": ("provider-1",)},
        price_set_members={},
        source_trace_sets={},
    )
    pagination = type("Pagination", (), {"limit": 5, "offset": 0})()

    payload = serving_manifest.search_ptg2_manifest_snapshot(
        snapshot,
        {
            "plan_id": "010854205",
            "code": "29888",
            "code_system": "CPT",
            "include_providers": "true",
        },
        pagination,
        mode_value="product_search",
    )

    assert payload is not None
    item = payload["items"][0]
    assert item["address_verification"]["displayed_address_present"] is False
    assert item["address_verification"]["address_evidence_level"] == "unknown"
    assert item["address_verification"]["reason"] == (
        "PTG proves the provider identity is in network, but no displayable address is available."
    )
    for key in ("location_source", "address_sources", "address_precision", "source_count"):
        assert key not in item["address_verification"]
    for key in ("address", "telephone_number", "phone", "location_source", "city", "state", "zip5", "coordinates"):
        assert key not in item


def test_ptg2_manifest_dense_id_mapping_is_sorted_and_deduped():
    mapping = build_dense_id_mapping([GLOBAL_B.hex(), GLOBAL_A, GLOBAL_B, GLOBAL_C])

    assert mapping == {
        GLOBAL_A: 0,
        GLOBAL_B: 1,
        GLOBAL_C: 2,
    }


def test_ptg2_manifest_mapping_roundtrip_validates_manifest_sidecar(tmp_path):
    manifest = write_global_local_id_mapping(
        tmp_path,
        "provider_sets",
        {
            GLOBAL_B.hex(): [9, 3, 3],
            GLOBAL_A: [2, 1],
        },
    )

    assert manifest["global_id_count"] == 2
    assert manifest["record_count"] == 4
    sidecar = manifest["sidecars"][0]
    assert sidecar["byte_count"] == 4 * PTG2_MANIFEST_MAPPING_RECORD_SIZE
    assert len(sidecar["sha256"]) == 64

    mapping = read_global_local_id_mapping(tmp_path / "provider_sets.manifest.json")

    assert mapping == {
        GLOBAL_A: (1, 2),
        GLOBAL_B: (3, 9),
    }


def test_ptg2_manifest_mapping_writes_deterministic_order(tmp_path):
    left = tmp_path / "left"
    right = tmp_path / "right"
    write_global_local_id_mapping(
        left,
        "provider_sets",
        {
            GLOBAL_B: [4, 1],
            GLOBAL_A: [8, 2],
        },
    )
    write_global_local_id_mapping(
        right,
        "provider_sets",
        {
            GLOBAL_A.hex(): [2, 8],
            GLOBAL_B.hex(): [1, 4],
        },
    )

    assert (left / "provider_sets.global_local_ids.bin").read_bytes() == (
        right / "provider_sets.global_local_ids.bin"
    ).read_bytes()
    assert (left / "provider_sets.manifest.json").read_text(encoding="utf-8") == (
        right / "provider_sets.manifest.json"
    ).read_text(encoding="utf-8")


def test_ptg2_manifest_mapping_read_rejects_checksum_failure(tmp_path):
    write_global_local_id_mapping(tmp_path, "provider_sets", {GLOBAL_A: [1]})
    sidecar_path = tmp_path / "provider_sets.global_local_ids.bin"
    payload = bytearray(sidecar_path.read_bytes())
    payload[-1] ^= 0x01
    sidecar_path.write_bytes(payload)

    with pytest.raises(PTG2ManifestArtifactError, match="checksum mismatch"):
        read_global_local_id_mapping(tmp_path / "provider_sets.manifest.json")


def test_ptg2_manifest_membership_sidecar_roundtrip_uses_rust_layout(tmp_path):
    manifest = write_global_membership_sidecar(
        tmp_path,
        "provider_set_members",
        {
            GLOBAL_B.hex(): [GLOBAL_C, GLOBAL_A, GLOBAL_A],
            GLOBAL_A: [GLOBAL_B],
        },
    )

    sidecar = manifest["sidecars"][0]
    assert manifest["owner_count"] == 2
    assert manifest["member_count"] == 3
    assert sidecar["byte_count"] == 8 + 4 + 8 + 2 * PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE + 3 * 16
    raw = (tmp_path / "provider_set_members.global_membership.bin").read_bytes()
    assert raw[:8] == PTG2_MANIFEST_MEMBERSHIP_MAGIC

    mapping = read_global_membership_sidecar(tmp_path / "provider_set_members.manifest.json")
    entries = read_global_sidecar_entries(tmp_path / "provider_set_members.global_membership.bin", metadata=sidecar)

    assert mapping == {
        GLOBAL_A: (GLOBAL_B,),
        GLOBAL_B: (GLOBAL_A, GLOBAL_C),
    }
    assert entries[0].owner == GLOBAL_A
    assert entries[0].members == (GLOBAL_B,)


def test_ptg2_manifest_dense_membership_sidecar_roundtrip_and_lookup(tmp_path):
    sidecar_path = tmp_path / "provider_forward.ptg2sc"
    payload = bytearray()
    payload.extend(struct.pack("<8sIQQ", PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC, 1, 2, 2))
    payload.extend(struct.pack("<16sQI", GLOBAL_A, 0, 2))
    payload.extend(struct.pack("<16sQI", GLOBAL_B, 2, 1))
    payload.extend(GLOBAL_B)
    payload.extend(GLOBAL_C)
    payload.extend(struct.pack("<III", 0, 1, 1))
    sidecar_path.write_bytes(payload)
    metadata = {
        "record_format": PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT,
        "byte_count": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "owner_count": 2,
        "member_count": 3,
        "member_global_count": 2,
    }

    entries = read_global_sidecar_entries(sidecar_path, metadata=metadata)
    members = lookup_global_sidecar_members(sidecar_path, GLOBAL_A.hex(), metadata=metadata)

    assert entries[0].owner == GLOBAL_A
    assert entries[0].members == (GLOBAL_B, GLOBAL_C)
    assert entries[1].members == (GLOBAL_C,)
    assert members == (GLOBAL_B, GLOBAL_C)


def test_ptg2_manifest_membership_sidecar_writes_deterministic_order(tmp_path):
    left = tmp_path / "left"
    right = tmp_path / "right"
    write_global_membership_sidecar(left, "provider_sets", {GLOBAL_B: [GLOBAL_C, GLOBAL_A], GLOBAL_A: [GLOBAL_B]})
    write_global_membership_sidecar(right, "provider_sets", {GLOBAL_A.hex(): [GLOBAL_B], GLOBAL_B.hex(): [GLOBAL_A, GLOBAL_C]})

    assert (left / "provider_sets.global_membership.bin").read_bytes() == (
        right / "provider_sets.global_membership.bin"
    ).read_bytes()
    assert (left / "provider_sets.manifest.json").read_text(encoding="utf-8") == (
        right / "provider_sets.manifest.json"
    ).read_text(encoding="utf-8")


def test_ptg2_manifest_membership_sidecar_read_rejects_checksum_failure(tmp_path):
    write_global_membership_sidecar(tmp_path, "provider_sets", {GLOBAL_A: [GLOBAL_B]})
    sidecar_path = tmp_path / "provider_sets.global_membership.bin"
    payload = bytearray(sidecar_path.read_bytes())
    payload[-1] ^= 0x01
    sidecar_path.write_bytes(payload)

    with pytest.raises(PTG2ManifestArtifactError, match="checksum mismatch"):
        read_global_membership_sidecar(tmp_path / "provider_sets.manifest.json")


def test_ptg2_manifest_membership_sidecar_read_rejects_byte_count_failure(tmp_path):
    manifest = write_global_membership_sidecar(tmp_path, "provider_sets", {GLOBAL_A: [GLOBAL_B]})
    sidecar = dict(manifest["sidecars"][0])
    sidecar["byte_count"] += 1

    with pytest.raises(PTG2ManifestArtifactError, match="byte_count mismatch"):
        read_global_sidecar_entries(tmp_path / "provider_sets.global_membership.bin", metadata=sidecar)


def test_ptg2_manifest_membership_lookup_infers_format_when_metadata_omits_it(tmp_path):
    manifest = write_global_membership_sidecar(tmp_path, "provider_sets", {GLOBAL_A: [GLOBAL_B], GLOBAL_B: [GLOBAL_C]})
    sidecar = dict(manifest["sidecars"][0])
    sidecar.pop("record_format")
    sidecar_path = tmp_path / "provider_sets.global_membership.bin"

    assert lookup_global_sidecar_members(sidecar_path, GLOBAL_A, metadata=sidecar) == (GLOBAL_B,)
    assert lookup_global_sidecar_members_many(sidecar_path, [GLOBAL_A, GLOBAL_B], metadata=sidecar) == {
        GLOBAL_A: (GLOBAL_B,),
        GLOBAL_B: (GLOBAL_C,),
    }
