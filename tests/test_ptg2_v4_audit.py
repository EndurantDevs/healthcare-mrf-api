# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import struct
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_shared_audit as v3_audit
from process.ptg_parts import ptg2_v4_audit as v4_audit
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    SharedLayoutBuildOwnership,
    shared_block_hash,
)


_PG_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)


def _graph_compilation(
    tmp_path: Path,
    witness_rows: list[tuple[int, int, int]],
    *,
    selected_layout: str = "pattern",
) -> SimpleNamespace:
    copy_payload = bytearray(_PG_COPY_HEADER)
    for provider_set_key, provider_group_key, npi in witness_rows:
        copy_payload.extend(struct.pack(">h", 3))
        for field_value in (
            struct.pack(">i", provider_set_key),
            struct.pack(">i", provider_group_key),
            struct.pack(">q", npi),
        ):
            copy_payload.extend(struct.pack(">i", len(field_value)))
            copy_payload.extend(field_value)
    copy_payload.extend(struct.pack(">h", -1))
    path = tmp_path / "v4-provider-set-audit-npi.copy"
    path.write_bytes(copy_payload)
    digest = hashlib.sha256(copy_payload).hexdigest()
    artifact = SimpleNamespace(
        name="provider_set_audit_npi",
        path=path,
        byte_count=len(copy_payload),
        sha256=digest,
        row_count=len(witness_rows),
    )
    return SimpleNamespace(
        output_artifacts=(artifact,),
        provider_set_audit_npi_copy_path=path,
        selected_layout=selected_layout,
    )


def test_provider_set_witness_stream_is_authenticated_sorted_and_bounded(tmp_path):
    compilation = _graph_compilation(
        tmp_path,
        [
            (1, 10, 1_111_111_111),
            (3, 30, 1_333_333_333),
            (7, 70, 1_777_777_777),
        ],
    )

    witnesses = v4_audit.load_v4_audit_witnesses(
        compilation,
        provider_set_keys=(3, 7),
    )

    assert witnesses == {
        3: v4_audit.V4ProviderSetAuditWitness(3, 30, 1_333_333_333),
        7: v4_audit.V4ProviderSetAuditWitness(7, 70, 1_777_777_777),
    }


def test_provider_set_witness_rejects_digest_or_order_corruption(tmp_path):
    compilation = _graph_compilation(
        tmp_path,
        [(2, 20, 1_222_222_222), (1, 10, 1_111_111_111)],
    )
    with pytest.raises(RuntimeError, match="invalid or unordered"):
        v4_audit.load_v4_audit_witnesses(
            compilation,
            provider_set_keys=(1, 2),
        )

    ordered = _graph_compilation(
        tmp_path,
        [(1, 10, 1_111_111_111)],
    )
    changed = ordered.provider_set_audit_npi_copy_path.read_bytes().replace(
        struct.pack(">q", 1_111_111_111),
        struct.pack(">q", 1_111_111_112),
    )
    ordered.provider_set_audit_npi_copy_path.write_bytes(changed)
    with pytest.raises(RuntimeError, match="authentication changed"):
        v4_audit.load_v4_audit_witnesses(
            ordered,
            provider_set_keys=(1,),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("representation", "expected_relations"),
    [
        (
            "pattern_v1",
            ("group_patterns", "set_patterns", "pattern_groups", "group_npis_exact"),
        ),
        ("direct_v1", ("set_groups_direct", "group_npis_exact")),
    ],
)
async def test_v4_one_member_witness_matches_v3_occurrence_fixture(
    representation,
    expected_relations,
):
    """Prove direct and pattern witnesses preserve V3 occurrence semantics."""

    class Session:
        async def execute(self, _statement, _parameters):
            return ({"npi": 1_234_567_890, "npi_key": 4},)

    class Reader:
        def __init__(self):
            self.representation = representation
            self.calls = []

        async def single_members(self, relation, owner_keys):
            owner_keys = tuple(owner_keys)
            self.calls.append((relation, owner_keys))
            assert relation == "group_patterns"
            assert owner_keys == (7,)
            return {7: 9}

        async def contains_edges(self, relation, edges):
            edges = tuple(sorted(edges))
            self.calls.append((relation, edges))
            return {edge: True for edge in edges}

    candidate = v3_audit.AuditCandidate(
        code_key=1,
        provider_set_key=5,
        price_key=2,
        source_key=0,
        provider_count=1,
        candidate_ordinal=0,
    )
    reader = Reader()
    verified = await v4_audit._verified_provider_npis_by_candidate(
        Session(),
        schema_name="mrf",
        snapshot_key=11,
        candidates=(candidate,),
        witnesses={
            5: v4_audit.V4ProviderSetAuditWitness(
                provider_set_key=5,
                provider_group_key=7,
                npi=1_234_567_890,
            )
        },
        reader=reader,
    )
    v4_rows = v3_audit.build_audit_occurrences(
        candidates=(candidate,),
        provider_npis=verified,
        price_memberships={2: (13,)},
        core_layout_id=b"\x71" * 32,
        budget=v3_audit._ReadBudget(),
    )
    v3_rows = v3_audit.build_audit_occurrences(
        candidates=(candidate,),
        provider_npis={0: (1_234_567_890,)},
        price_memberships={2: (13,)},
        core_layout_id=b"\x71" * 32,
        budget=v3_audit._ReadBudget(),
    )

    assert verified == {0: (1_234_567_890,)}
    assert v4_rows == v3_rows
    assert tuple(call[0] for call in reader.calls) == expected_relations


def test_persisted_v4_cas_corruption_fails_closed():
    object_kind = "v4_group_npis_exact_members_v1"
    original_payload = struct.pack("<I", 4)
    block_hash = shared_block_hash(
        format_version=PTG2_V3_SHARED_FORMAT_VERSION,
        object_kind=object_kind,
        codec="none",
        payload=original_payload,
    )
    corrupted_payload = struct.pack("<I", 5)
    block_field_map = {
        "block_hash": block_hash,
        "format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "object_kind": object_kind,
        "codec": "none",
        "block_entry_count": 1,
        "raw_byte_count": len(corrupted_payload),
        "stored_byte_count": len(corrupted_payload),
        "payload": corrupted_payload,
    }

    with pytest.raises(RuntimeError, match="CAS block authentication failed"):
        v4_audit._validated_physical_payload(
            block_field_map,
            expected_kind=object_kind,
            maximum_raw_bytes=16 * 1024,
        )


@pytest.mark.asyncio
async def test_audit_heavy_bitmap_fragments_use_physical_entry_counts(
    monkeypatch,
):
    """Prove fragmented bitmap coordinates count physical set bits exactly."""

    object_kind = "v4_npi_groups_exact_heavy_bitmap_v1"
    header = b"PTG2V4BM" + struct.pack("<IIII", 7, 100, 24, 6)

    def frame(fragment_no, entry_count, payload):
        return struct.pack(
            "<8sIIIIII",
            b"PTG2V4BF",
            7,
            100,
            24,
            6,
            fragment_no,
            entry_count,
        ) + payload

    logical_payloads = (
        header + bytes((0b00000111,)),
        bytes((0b00000011, 0b00000001)),
    )
    payloads = (
        frame(0, 3, logical_payloads[0]),
        frame(1, 3, logical_payloads[1]),
    )
    coordinates_by_pair = {
        (7, 0): v4_audit.V4SnapshotMapCoordinate(
            object_kind, 7, 0, 3, b"a" * 32
        ),
        (7, 1): v4_audit.V4SnapshotMapCoordinate(
            object_kind, 7, 1, 3, b"b" * 32
        ),
    }
    blocks_by_hash = {
        b"a" * 32: v4_audit._V4PhysicalBlock(
            b"a" * 32, object_kind, 3, payloads[0]
        ),
        b"b" * 32: v4_audit._V4PhysicalBlock(
            b"b" * 32, object_kind, 3, payloads[1]
        ),
    }
    reader = v4_audit._V4PersistedGraphReader(
        object(),
        schema_name="mrf",
        snapshot_key=17,
        representation="pattern_v1",
        budget=v3_audit._ReadBudget(),
    )

    async def fake_coordinates(**_kwargs):
        return coordinates_by_pair

    async def fake_blocks(**_kwargs):
        return blocks_by_hash

    monkeypatch.setattr(reader, "_map_coordinates", fake_coordinates)
    monkeypatch.setattr(reader, "_physical_blocks", fake_blocks)
    manifest = v4_audit._V4RelationManifest(
        relation="npi_groups_exact",
        member_object_kind="v4_npi_groups_exact_members_v1",
        locator_object_kind="v4_npi_groups_exact_locators_v1",
        owner_base=0,
        owner_count=8,
        logical_member_count=6,
        vector_member_count=0,
        member_width=4,
        member_page_bytes=32,
        locator_page_bytes=32,
        locator_owner_span=2,
    )
    owner = v4_audit._V4HeavyOwner(
        relation="npi_groups_exact",
        owner_key=7,
        object_kind=object_kind,
        member_count=6,
        member_base=100,
        member_span=24,
        fragment_count=2,
    )

    assert await reader._heavy_payloads(manifest, {7: owner}) == {
        7: b"".join(logical_payloads)
    }
    coordinates_by_pair[(7, 0)] = v4_audit.V4SnapshotMapCoordinate(
        object_kind, 7, 0, 2, b"a" * 32
    )
    with pytest.raises(RuntimeError, match="fragment conflicts"):
        await reader._heavy_payloads(manifest, {7: owner})


@pytest.mark.asyncio
async def test_building_root_validation_fences_snapshot_token_and_zero_counts():
    root_by_field = {
        "snapshot_key": 17,
        "generation": "shared_blocks_v4",
        "layout_state": "building",
        "build_token": "owned-token",
        "root_state": "building",
        "format_version": 1,
        "map_format": "packed_coordinate_hash_v1",
        "representation": "pattern_v1",
        "projection_id_scope": "snapshot_local_v1",
        "map_digest": None,
        "object_kind_count": 0,
        "map_pack_count": 0,
        "coordinate_count": 0,
        "entry_count": 0,
        "logical_byte_count": 0,
        "stored_map_byte_count": 0,
        "npi_count": 0,
        "component_count": 0,
        "pattern_count": 0,
        "relation_count": 0,
        "heavy_owner_count": 0,
        "completed_at": None,
    }

    class Session:
        async def execute(self, _statement, _parameters):
            return (root_by_field,)

    ownership = SharedLayoutBuildOwnership(17, "owned-token")
    await v4_audit._validate_building_v4_context(
        Session(),
        schema_name="mrf",
        build_ownership=ownership,
        representation="pattern_v1",
    )

    root_by_field["map_pack_count"] = 1
    with pytest.raises(RuntimeError, match="building-root ownership"):
        await v4_audit._validate_building_v4_context(
            Session(),
            schema_name="mrf",
            build_ownership=ownership,
            representation="pattern_v1",
        )
