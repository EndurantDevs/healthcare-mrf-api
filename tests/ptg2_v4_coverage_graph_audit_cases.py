from __future__ import annotations

from tests.ptg2_v4_coverage_support import (
    AuditCandidate,
    PTG2SharedBlockError,
    PTG2_V3_SHARED_FORMAT_VERSION,
    _ReadBudget,
    _Result,
    _ScriptedSession,
    _owner_row,
    _reference,
    _relation_row,
    audit,
    graph,
    pytest,
    shared_block_hash,
    snapshot_maps,
    struct,
)

async def _graph_root_fixture(monkeypatch):
    monkeypatch.setattr(graph, "_ROOT_CACHE", graph.OrderedDict())
    monkeypatch.setattr(graph, "_RELATION_CACHE", graph.OrderedDict())
    root_row_by_field = {
        "snapshot_key": 17,
        "representation": "pattern_v1",
        "map_digest": b"d" * 32,
        "format_version": snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "projection_id_scope": snapshot_maps.PTG2_V4_PROJECTION_ID_SCOPE,
    }
    root_session = _ScriptedSession(_Result(rows=(root_row_by_field,)))
    root = await graph.load_v4_graph_root(
        root_session,
        17,
        schema_name="mrf",
    )
    assert root.representation == "pattern_v1"
    assert await graph.load_v4_graph_root(root_session, 17, schema_name="mrf") == root
    with pytest.raises(PTG2SharedBlockError, match="unavailable"):
        await graph.load_v4_graph_root(
            _ScriptedSession(_Result()),
            18,
            schema_name="mrf",
        )
    return root


async def _assert_relation_manifest_reads():
    relation_row_by_field = {
        "snapshot_key": 17,
        **_relation_row("group_patterns"),
    }
    relation_session = _ScriptedSession(_Result(rows=(relation_row_by_field,)))
    manifest = await graph.load_v4_relation_manifest(
        relation_session,
        snapshot_key=17,
        relation="group_patterns",
        schema_name="mrf",
    )
    assert manifest.members_per_page == 4
    assert await graph.load_v4_relation_manifest(
        relation_session,
        snapshot_key=17,
        relation="group_patterns",
        schema_name="mrf",
    ) == manifest
    with pytest.raises(PTG2SharedBlockError, match="inconsistent"):
        await graph.load_v4_relation_manifest(
            _ScriptedSession(
                _Result(rows=({**relation_row_by_field, "member_width": 8},))
            ),
            snapshot_key=19,
            relation="group_patterns",
            schema_name="mrf",
        )


async def _assert_map_coordinate_reads(monkeypatch):
    reference = _reference("v4_group_patterns_members_v1", 0, 0, entry_count=2)
    pack = snapshot_maps._make_map_pack(
        object_kind=reference.object_kind,
        pack_no=0,
        references=(reference,),
    )
    map_row_by_field = {
        "pack_no": 0,
        "first_block_key": 0,
        "first_fragment_no": 0,
        "last_block_key": 0,
        "last_fragment_no": 0,
        "coordinate_count": 1,
        "block_hash": pack.map_block.block_hash,
        "format_version": pack.map_block.format_version,
        "object_kind": pack.map_block.object_kind,
        "codec": "none",
        "block_entry_count": 1,
        "raw_byte_count": len(pack.map_block.payload),
        "stored_byte_count": len(pack.map_block.payload),
        "payload": pack.map_block.payload,
    }
    monkeypatch.setattr(graph, "_MAP_COORDINATE_CACHE", graph._ByteLRU(1024))
    map_session = _ScriptedSession(_Result(rows=(map_row_by_field,)))
    with graph.v4_graph_request_scope() as request_io:
        coordinates = await graph._load_map_coordinate_pairs(
            map_session,
            schema_name="mrf",
            snapshot_key=17,
            object_kind=reference.object_kind,
            coordinate_pairs=((0, 0),),
        )
        cached_coordinates = await graph._load_map_coordinate_pairs(
            map_session,
            schema_name="mrf",
            snapshot_key=17,
            object_kind=reference.object_kind,
            coordinate_pairs=((0, 0),),
        )
    assert coordinates == cached_coordinates
    assert request_io.database_blocks == 1
    return reference


async def _assert_physical_block_reads(monkeypatch, reference) -> None:
    target_payload = struct.pack("<II", 5, 9)
    target_hash = shared_block_hash(
        format_version=PTG2_V3_SHARED_FORMAT_VERSION,
        object_kind=reference.object_kind,
        codec="none",
        payload=target_payload,
    )
    coordinate = snapshot_maps.V4SnapshotMapCoordinate(
        reference.object_kind,
        0,
        0,
        2,
        target_hash,
    )
    physical_row_by_field = {
        "block_hash": target_hash,
        "format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "object_kind": reference.object_kind,
        "codec": "none",
        "block_entry_count": 2,
        "raw_byte_count": len(target_payload),
        "stored_byte_count": len(target_payload),
        "payload": target_payload,
    }
    monkeypatch.setattr(graph, "_PHYSICAL_BLOCK_CACHE", graph._ByteLRU(1024))
    physical_session = _ScriptedSession(_Result(rows=(physical_row_by_field,)))
    with graph.v4_graph_request_scope() as request_io:
        blocks = await graph._load_physical_blocks(
            physical_session,
            schema_name="mrf",
            object_kind=reference.object_kind,
            coordinates=(coordinate,),
            maximum_raw_bytes=16,
        )
        cached_blocks = await graph._load_physical_blocks(
            physical_session,
            schema_name="mrf",
            object_kind=reference.object_kind,
            coordinates=(coordinate,),
            maximum_raw_bytes=16,
        )
    assert blocks[target_hash].payload == target_payload
    assert cached_blocks == blocks
    assert request_io.database_blocks == 1
    assert request_io.cache_hit_bytes == len(target_payload)


def _assert_graph_validation_helpers(root) -> None:
    with pytest.raises(RuntimeError, match="request scope"):
        graph._request_io()
    assert graph._normalized_owner_keys((2, 1, 2)) == (1, 2)
    with pytest.raises(PTG2SharedBlockError, match="not an integer"):
        graph._normalized_owner_keys((True,))
    with pytest.raises(PTG2SharedBlockError, match="uint32"):
        graph._normalized_owner_keys((-1,))
    with pytest.raises(PTG2SharedBlockError, match="unsupported"):
        graph._relation_kinds("missing")
    graph._validate_relation_for_root(root, "group_patterns")
    with pytest.raises(PTG2SharedBlockError, match="does not publish"):
        graph._validate_relation_for_root(root, "set_groups_direct")


@pytest.mark.asyncio
async def test_graph_root_manifest_map_and_physical_cas_reads(monkeypatch) -> None:
    """Read graph roots, relation maps, and physical CAS rows fail-closed."""
    root = await _graph_root_fixture(monkeypatch)
    await _assert_relation_manifest_reads()
    reference = await _assert_map_coordinate_reads(monkeypatch)
    await _assert_physical_block_reads(monkeypatch, reference)
    _assert_graph_validation_helpers(root)


def _audit_reader_values():
    manifest = audit._V4RelationManifest(
        relation="group_patterns",
        member_object_kind="v4_group_patterns_members_v1",
        locator_object_kind="v4_group_patterns_locators_v1",
        owner_base=0,
        owner_count=4,
        logical_member_count=5,
        vector_member_count=4,
        member_width=4,
        member_page_bytes=16,
        locator_page_bytes=24,
        locator_owner_span=2,
    )
    heavy_owner = audit._V4HeavyOwner(
        relation="group_patterns",
        owner_key=2,
        object_kind="v4_group_patterns_heavy_bitmap_v1",
        member_count=1,
        member_base=100,
        member_span=8,
        fragment_count=1,
    )
    heavy_payload = (
        audit.PTG2_V4_HEAVY_BITMAP_MAGIC
        + struct.pack("<IIII", 2, 100, 8, 1)
        + b"\x08"
    )
    return manifest, heavy_owner, heavy_payload


def _configure_audit_reader(monkeypatch):
    reader = audit._V4PersistedGraphReader(
        object(),
        schema_name="mrf",
        snapshot_key=17,
        representation="pattern_v1",
        budget=_ReadBudget(),
    )
    manifest, heavy_owner, heavy_payload = _audit_reader_values()

    async def fake_manifest(_relation):
        return manifest

    async def fake_heavy(_manifest, owner_keys):
        return {2: heavy_owner} if 2 in set(owner_keys) else {}

    scalar_lookup_flags = [True]

    async def fake_locators(_manifest, owner_keys):
        return {
            owner_key: (
                (0, 1)
                if owner_key == 1 and scalar_lookup_flags[0]
                else ((0, 4) if owner_key == 1 else (4, 0))
            )
            for owner_key in set(owner_keys)
        }

    async def fake_pages(_manifest, page_keys):
        return {
            page_key: ((10, 20, 30, 40) if page_key == 0 else ())
            for page_key in set(page_keys)
        }

    async def fake_heavy_payloads(_manifest, owners):
        return {owner_key: heavy_payload for owner_key in owners}

    monkeypatch.setattr(reader, "_manifest", fake_manifest)
    monkeypatch.setattr(reader, "_heavy_owners", fake_heavy)
    monkeypatch.setattr(reader, "_locators", fake_locators)
    monkeypatch.setattr(reader, "_member_pages", fake_pages)
    monkeypatch.setattr(reader, "_heavy_payloads", fake_heavy_payloads)
    return reader, scalar_lookup_flags


async def _assert_audit_reader_edges(reader, scalar_lookup_flags) -> None:
    scalar = await reader.single_members("group_patterns", (1, 2))
    assert scalar == {1: 10, 2: 103}
    scalar_lookup_flags[0] = False
    membership = await reader.contains_edges(
        "group_patterns",
        ((1, 20), (1, 25), (2, 103), (2, 109), (3, 1)),
    )
    assert membership == {
        (1, 20): True,
        (1, 25): False,
        (2, 103): True,
        (2, 109): False,
        (3, 1): False,
    }
    with pytest.raises(RuntimeError, match="uint32"):
        await reader.contains_edges("group_patterns", ((-1, 0),))


class _ProviderReader:
    def __init__(self, representation: str) -> None:
        self.representation = representation
        self.calls = []

    async def single_members(self, relation, owner_keys):
        keys = tuple(owner_keys)
        self.calls.append((relation, keys))
        return {key: key + 100 for key in keys}

    async def contains_edges(self, relation, edges):
        normalized_edges = set(edges)
        self.calls.append((relation, normalized_edges))
        return {edge: True for edge in normalized_edges}


async def _provider_witness_fixture(monkeypatch):
    candidates = (
        AuditCandidate(1, 7, 2, 0, 1, 0),
        AuditCandidate(1, 8, 2, 0, 0, 1),
    )
    witness_by_set = {
        7: audit.V4ProviderSetAuditWitness(7, 11, 1_234_567_890),
    }

    async def fake_npi_keys(*_args, **_kwargs):
        return {1_234_567_890: 3}

    monkeypatch.setattr(audit, "_npi_keys_for_values", fake_npi_keys)
    pattern_reader = _ProviderReader("pattern_v1")
    pattern_result = await audit._verified_provider_npis_by_candidate(
        object(),
        schema_name="mrf",
        snapshot_key=17,
        candidates=candidates,
        witnesses=witness_by_set,
        reader=pattern_reader,
    )
    assert pattern_result == {0: (1_234_567_890,), 1: ()}
    assert [call[0] for call in pattern_reader.calls] == [
        "group_patterns",
        "set_patterns",
        "pattern_groups",
        "group_npis_exact",
    ]
    direct_reader = _ProviderReader("direct_v1")
    assert await audit._verified_provider_npis_by_candidate(
        object(),
        schema_name="mrf",
        snapshot_key=17,
        candidates=candidates,
        witnesses=witness_by_set,
        reader=direct_reader,
    ) == pattern_result
    assert direct_reader.calls[0][0] == "set_groups_direct"
    return candidates, witness_by_set, direct_reader


async def _assert_provider_witness_rejections(
    candidates,
    witness_by_set,
    direct_reader,
) -> None:
    with pytest.raises(RuntimeError, match="provider counts"):
        await audit._verified_provider_npis_by_candidate(
            object(),
            schema_name="mrf",
            snapshot_key=17,
            candidates=candidates,
            witnesses={},
            reader=direct_reader,
        )
    unsupported_reader = _ProviderReader("source_component_v1")
    with pytest.raises(RuntimeError, match="unsupported"):
        await audit._verified_provider_npis_by_candidate(
            object(),
            schema_name="mrf",
            snapshot_key=17,
            candidates=candidates,
            witnesses=witness_by_set,
            reader=unsupported_reader,
        )


@pytest.mark.asyncio
async def test_audit_reader_scalar_edges_and_provider_witnesses(monkeypatch) -> None:
    """Audit scalar counts, reciprocal edges, and provider witness sampling."""
    reader, scalar_flags = _configure_audit_reader(monkeypatch)
    await _assert_audit_reader_edges(reader, scalar_flags)
    witness_fixture = await _provider_witness_fixture(monkeypatch)
    await _assert_provider_witness_rejections(*witness_fixture)



async def _audit_manifest_fixture():
    manifest_row = _relation_row("group_patterns")
    reader = audit._V4PersistedGraphReader(
        _ScriptedSession(_Result(rows=(manifest_row,))),
        schema_name="mrf",
        snapshot_key=17,
        representation="pattern_v1",
        budget=_ReadBudget(),
    )
    manifest = await reader._manifest("group_patterns")
    assert await reader._manifest("group_patterns") is manifest
    with pytest.raises(RuntimeError, match="missing or duplicated"):
        await audit._V4PersistedGraphReader(
            _ScriptedSession(_Result()),
            schema_name="mrf",
            snapshot_key=17,
            representation="pattern_v1",
            budget=_ReadBudget(),
        )._manifest("group_patterns")
    with pytest.raises(RuntimeError, match="inconsistent"):
        await audit._V4PersistedGraphReader(
            _ScriptedSession(
                _Result(rows=({**manifest_row, "member_width": 8},))
            ),
            schema_name="mrf",
            snapshot_key=17,
            representation="pattern_v1",
            budget=_ReadBudget(),
        )._manifest("group_patterns")
    return reader, manifest


async def _assert_audit_heavy_owners(reader, manifest) -> None:
    assert await reader._heavy_owners(manifest, ()) == {}
    owner_row = _owner_row("group_patterns", 1)
    owners = await audit._V4PersistedGraphReader(
        _ScriptedSession(_Result(rows=(owner_row,))),
        schema_name="mrf",
        snapshot_key=17,
        representation="pattern_v1",
        budget=_ReadBudget(),
    )._heavy_owners(manifest, (1,))
    assert owners[1].member_count == 2
    with pytest.raises(RuntimeError, match="inconsistent"):
        await audit._V4PersistedGraphReader(
            _ScriptedSession(
                _Result(rows=({**owner_row, "object_kind": "wrong"},))
            ),
            schema_name="mrf",
            snapshot_key=17,
            representation="pattern_v1",
            budget=_ReadBudget(),
        )._heavy_owners(manifest, (1,))


def _assert_audit_relation_validation() -> None:
    for invalid_count in (True, "bad", -1, 2**63):
        with pytest.raises(RuntimeError, match="invalid test"):
            audit._nonnegative_int(invalid_count, label="test")
    assert audit._nonnegative_int(7, label="test") == 7
    assert audit._relation_kinds("group_patterns") == (
        "v4_group_patterns_locators_v1",
        "v4_group_patterns_members_v1",
    )
    audit._validate_relation_for_representation("pattern_v1", "group_patterns")
    audit._validate_relation_for_representation("direct_v1", "set_groups_direct")
    audit._validate_relation_for_representation("direct_v1", "npi_groups_exact")
    with pytest.raises(RuntimeError, match="does not publish"):
        audit._validate_relation_for_representation(
            "direct_v1",
            "group_patterns",
        )


def _assert_audit_physical_payload() -> None:
    block_payload = b"payload"
    block_hash = shared_block_hash(
        format_version=PTG2_V3_SHARED_FORMAT_VERSION,
        object_kind="kind",
        codec="none",
        payload=block_payload,
    )
    block_row_by_field = {
        "block_hash": block_hash,
        "format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "object_kind": "kind",
        "codec": "none",
        "block_entry_count": 1,
        "raw_byte_count": len(block_payload),
        "stored_byte_count": len(block_payload),
        "payload": block_payload,
    }
    physical = audit._validated_physical_payload(
        block_row_by_field,
        expected_kind="kind",
        maximum_raw_bytes=16,
    )
    assert physical.payload == block_payload
    with pytest.raises(RuntimeError, match="authentication failed"):
        audit._validated_physical_payload(
            {**block_row_by_field, "codec": "gzip"},
            expected_kind="kind",
            maximum_raw_bytes=16,
        )


@pytest.mark.asyncio
async def test_audit_manifest_heavy_owner_and_payload_branch_matrix() -> None:
    """Reject malformed audit manifests, heavy owners, and decoded payloads."""
    reader, manifest = await _audit_manifest_fixture()
    await _assert_audit_heavy_owners(reader, manifest)
    _assert_audit_relation_validation()
    _assert_audit_physical_payload()
