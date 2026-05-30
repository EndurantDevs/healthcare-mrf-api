# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import struct
import hashlib

import pytest

from process.ptg_parts.ptg2_v3_artifacts import (
    PTG2_V3_DENSE_MEMBERSHIP_FORMAT,
    PTG2_V3_DENSE_MEMBERSHIP_MAGIC,
    PTG2_V3_MEMBERSHIP_INDEX_RECORD_SIZE,
    PTG2_V3_MEMBERSHIP_MAGIC,
    PTG2_V3_MAPPING_RECORD_SIZE,
    PTG2V3ArtifactError,
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


def test_ptg2_v3_dense_id_mapping_is_sorted_and_deduped():
    mapping = build_dense_id_mapping([GLOBAL_B.hex(), GLOBAL_A, GLOBAL_B, GLOBAL_C])

    assert mapping == {
        GLOBAL_A: 0,
        GLOBAL_B: 1,
        GLOBAL_C: 2,
    }


def test_ptg2_v3_mapping_roundtrip_validates_manifest_sidecar(tmp_path):
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
    assert sidecar["byte_count"] == 4 * PTG2_V3_MAPPING_RECORD_SIZE
    assert len(sidecar["sha256"]) == 64

    mapping = read_global_local_id_mapping(tmp_path / "provider_sets.manifest.json")

    assert mapping == {
        GLOBAL_A: (1, 2),
        GLOBAL_B: (3, 9),
    }


def test_ptg2_v3_mapping_writes_deterministic_order(tmp_path):
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


def test_ptg2_v3_mapping_read_rejects_checksum_failure(tmp_path):
    write_global_local_id_mapping(tmp_path, "provider_sets", {GLOBAL_A: [1]})
    sidecar_path = tmp_path / "provider_sets.global_local_ids.bin"
    payload = bytearray(sidecar_path.read_bytes())
    payload[-1] ^= 0x01
    sidecar_path.write_bytes(payload)

    with pytest.raises(PTG2V3ArtifactError, match="checksum mismatch"):
        read_global_local_id_mapping(tmp_path / "provider_sets.manifest.json")


def test_ptg2_v3_membership_sidecar_roundtrip_uses_rust_layout(tmp_path):
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
    assert sidecar["byte_count"] == 8 + 4 + 8 + 2 * PTG2_V3_MEMBERSHIP_INDEX_RECORD_SIZE + 3 * 16
    raw = (tmp_path / "provider_set_members.global_membership.bin").read_bytes()
    assert raw[:8] == PTG2_V3_MEMBERSHIP_MAGIC

    mapping = read_global_membership_sidecar(tmp_path / "provider_set_members.manifest.json")
    entries = read_global_sidecar_entries(tmp_path / "provider_set_members.global_membership.bin", metadata=sidecar)

    assert mapping == {
        GLOBAL_A: (GLOBAL_B,),
        GLOBAL_B: (GLOBAL_A, GLOBAL_C),
    }
    assert entries[0].owner == GLOBAL_A
    assert entries[0].members == (GLOBAL_B,)


def test_ptg2_v3_dense_membership_sidecar_roundtrip_and_lookup(tmp_path):
    sidecar_path = tmp_path / "provider_forward.ptg2v3sc"
    payload = bytearray()
    payload.extend(struct.pack("<8sIQQ", PTG2_V3_DENSE_MEMBERSHIP_MAGIC, 1, 2, 2))
    payload.extend(struct.pack("<16sQI", GLOBAL_A, 0, 2))
    payload.extend(struct.pack("<16sQI", GLOBAL_B, 2, 1))
    payload.extend(GLOBAL_B)
    payload.extend(GLOBAL_C)
    payload.extend(struct.pack("<III", 0, 1, 1))
    sidecar_path.write_bytes(payload)
    metadata = {
        "record_format": PTG2_V3_DENSE_MEMBERSHIP_FORMAT,
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


def test_ptg2_v3_membership_sidecar_writes_deterministic_order(tmp_path):
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


def test_ptg2_v3_membership_sidecar_read_rejects_checksum_failure(tmp_path):
    write_global_membership_sidecar(tmp_path, "provider_sets", {GLOBAL_A: [GLOBAL_B]})
    sidecar_path = tmp_path / "provider_sets.global_membership.bin"
    payload = bytearray(sidecar_path.read_bytes())
    payload[-1] ^= 0x01
    sidecar_path.write_bytes(payload)

    with pytest.raises(PTG2V3ArtifactError, match="checksum mismatch"):
        read_global_membership_sidecar(tmp_path / "provider_sets.manifest.json")


def test_ptg2_v3_membership_sidecar_read_rejects_byte_count_failure(tmp_path):
    manifest = write_global_membership_sidecar(tmp_path, "provider_sets", {GLOBAL_A: [GLOBAL_B]})
    sidecar = dict(manifest["sidecars"][0])
    sidecar["byte_count"] += 1

    with pytest.raises(PTG2V3ArtifactError, match="byte_count mismatch"):
        read_global_sidecar_entries(tmp_path / "provider_sets.global_membership.bin", metadata=sidecar)


def test_ptg2_v3_membership_lookup_infers_format_when_metadata_omits_it(tmp_path):
    manifest = write_global_membership_sidecar(tmp_path, "provider_sets", {GLOBAL_A: [GLOBAL_B], GLOBAL_B: [GLOBAL_C]})
    sidecar = dict(manifest["sidecars"][0])
    sidecar.pop("record_format")
    sidecar_path = tmp_path / "provider_sets.global_membership.bin"

    assert lookup_global_sidecar_members(sidecar_path, GLOBAL_A, metadata=sidecar) == (GLOBAL_B,)
    assert lookup_global_sidecar_members_many(sidecar_path, [GLOBAL_A, GLOBAL_B], metadata=sidecar) == {
        GLOBAL_A: (GLOBAL_B,),
        GLOBAL_B: (GLOBAL_C,),
    }
