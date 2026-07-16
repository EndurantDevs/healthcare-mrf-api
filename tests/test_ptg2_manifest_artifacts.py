# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import struct

import pytest

from process.ptg_parts import ptg2_manifest_publish as manifest_publish
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT,
    PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_FORMAT,
    PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_STRIDE,
    PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE,
    PTG2_MANIFEST_MEMBERSHIP_MAGIC,
    PTG2ManifestArtifactError,
    lookup_global_sidecar_members,
    lookup_global_sidecar_members_many,
    membership_index_fence_metadata,
    read_global_membership_sidecar,
    read_global_sidecar_entries,
    write_global_membership_sidecar,
)


GLOBAL_A = bytes.fromhex("0000000000000000000000000000000a")
GLOBAL_B = bytes.fromhex("0000000000000000000000000000000b")
GLOBAL_C = bytes.fromhex("0000000000000000000000000000000c")


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


def test_membership_index_fences_sample_fixed_owner_intervals(tmp_path):
    sidecar_path = tmp_path / "provider_npi_group.ptg2sc"
    owner_count = PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_STRIDE + 1
    with sidecar_path.open("wb") as sidecar_file:
        sidecar_file.write(struct.pack("<8sIQ", PTG2_MANIFEST_MEMBERSHIP_MAGIC, 1, owner_count))
        for owner_ordinal in range(owner_count):
            sidecar_file.write(struct.pack("<16sQI", owner_ordinal.to_bytes(16, "big"), 0, 0))

    metadata = membership_index_fence_metadata(sidecar_path)

    assert metadata["owner_count"] == owner_count
    assert metadata["member_count"] == 0
    assert metadata["membership_version"] == 1
    assert metadata["owner_index_fence_format"] == PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_FORMAT
    assert metadata["owner_index_fence_stride"] == PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_STRIDE
    assert metadata["owner_index_fence_owners"] == [
        (0).to_bytes(16, "big").hex(),
        PTG2_MANIFEST_MEMBERSHIP_INDEX_FENCE_STRIDE.to_bytes(16, "big").hex(),
    ]


def test_membership_index_fences_reject_unordered_owner_index(tmp_path):
    sidecar_path = tmp_path / "provider_npi_group.ptg2sc"
    with sidecar_path.open("wb") as sidecar_file:
        sidecar_file.write(struct.pack("<8sIQ", PTG2_MANIFEST_MEMBERSHIP_MAGIC, 1, 2))
        sidecar_file.write(struct.pack("<16sQI", (2).to_bytes(16, "big"), 0, 0))
        sidecar_file.write(struct.pack("<16sQI", (1).to_bytes(16, "big"), 0, 0))

    with pytest.raises(PTG2ManifestArtifactError, match="owner index is not ordered"):
        membership_index_fence_metadata(sidecar_path)


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
    sidecar_metadata_map = dict(manifest["sidecars"][0])
    sidecar_metadata_map["byte_count"] += 1

    with pytest.raises(PTG2ManifestArtifactError, match="byte_count mismatch"):
        read_global_sidecar_entries(
            tmp_path / "provider_sets.global_membership.bin",
            metadata=sidecar_metadata_map,
        )


def test_ptg2_manifest_membership_lookup_infers_format_when_metadata_omits_it(tmp_path):
    manifest = write_global_membership_sidecar(tmp_path, "provider_sets", {GLOBAL_A: [GLOBAL_B], GLOBAL_B: [GLOBAL_C]})
    sidecar_metadata_map = dict(manifest["sidecars"][0])
    sidecar_metadata_map.pop("record_format")
    sidecar_path = tmp_path / "provider_sets.global_membership.bin"

    assert lookup_global_sidecar_members(
        sidecar_path,
        GLOBAL_A,
        metadata=sidecar_metadata_map,
    ) == (GLOBAL_B,)
    assert lookup_global_sidecar_members_many(
        sidecar_path,
        [GLOBAL_A, GLOBAL_B],
        metadata=sidecar_metadata_map,
    ) == {
        GLOBAL_A: (GLOBAL_B,),
        GLOBAL_B: (GLOBAL_C,),
    }


def test_v3_graph_completeness_is_required_per_source_shard():
    graph_names = (
        "provider_forward",
        "provider_inverted",
        "provider_group_npi",
        "provider_npi_group",
    )
    sidecars = [{"name": name, "source_shard_id": "source-shard-a"} for name in graph_names]
    sidecars.extend(
        {"name": name, "source_shard_id": "source-shard-b"}
        for name in graph_names
        if name != "provider_npi_group"
    )

    with pytest.raises(RuntimeError, match="source-shard-b.*provider_npi_group"):
        manifest_publish._retain_v3_provider_graph_artifacts({"sidecars": sidecars})
