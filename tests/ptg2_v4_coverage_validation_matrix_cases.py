from __future__ import annotations

from tests.ptg2_v4_coverage_support import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    SharedBlock,
    SharedBlockReference,
    SimpleNamespace,
    _Result,
    _ScriptedSession,
    _reference,
    pytest,
    shared_block_hash,
    snapshot_maps,
    struct,
)

def _assert_snapshot_row_mapping() -> None:
    class Row:
        _mapping = {"value": 7}

    assert snapshot_maps._row_mapping(Row()) == {"value": 7}
    assert snapshot_maps._row_mapping(None) == {}


def _assert_map_pack_decoding() -> None:
    packed_record = snapshot_maps._MAP_RECORD.pack(0, 0, 1, b"h" * 32)
    invalid_padding = snapshot_maps._MAP_HEADER.pack(
        snapshot_maps._MAP_MAGIC,
        snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        1,
        1,
        b"a" + b"x" + b"\0" * 62,
    ) + packed_record
    with pytest.raises(ValueError, match="object_kind field"):
        snapshot_maps.decode_v4_snapshot_map_pack(invalid_padding)
    invalid_utf8 = snapshot_maps._MAP_HEADER.pack(
        snapshot_maps._MAP_MAGIC,
        snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        1,
        1,
        b"\xff" + b"\0" * 63,
    ) + packed_record
    with pytest.raises(ValueError, match="valid UTF-8"):
        snapshot_maps.decode_v4_snapshot_map_pack(invalid_utf8)
    zero_records = snapshot_maps._MAP_HEADER.pack(
        snapshot_maps._MAP_MAGIC,
        snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        1,
        0,
        b"a" + b"\0" * 63,
    )
    with pytest.raises(ValueError, match="coordinate count"):
        snapshot_maps.decode_v4_snapshot_map_pack(zero_records)
    duplicate_records = snapshot_maps._MAP_HEADER.pack(
        snapshot_maps._MAP_MAGIC,
        snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        1,
        2,
        b"a" + b"\0" * 63,
    ) + packed_record + packed_record
    with pytest.raises(ValueError, match="strictly ordered"):
        snapshot_maps.decode_v4_snapshot_map_pack(duplicate_records)


def _assert_map_pack_sequence() -> None:
    with pytest.raises(RuntimeError, match="strictly ordered"):
        snapshot_maps._advance_map_pack_sequence(
            {"object_kind": "a", "pack_no": 0},
            snapshot_maps._MapSummaryCursor(
                previous_kind="b",
                previous_pack_no=0,
            ),
        )
    with pytest.raises(RuntimeError, match="begin at zero"):
        snapshot_maps._advance_map_pack_sequence(
            {"object_kind": "a", "pack_no": 1},
            snapshot_maps._MapSummaryCursor(),
        )


def _assert_map_coordinate_validation():
    coordinate = snapshot_maps.V4SnapshotMapCoordinate(
        "a",
        4,
        0,
        1,
        b"h" * 32,
    )
    pack_row_by_field = {
        "first_block_key": 4,
        "first_fragment_no": 0,
        "last_block_key": 4,
        "last_fragment_no": 0,
        "coordinate_count": 1,
        "entry_count": 1,
    }
    with pytest.raises(RuntimeError, match="ranges overlap"):
        snapshot_maps._validate_map_pack_coordinates(
            pack_row_by_field,
            (coordinate,),
            snapshot_maps._MapSummaryCursor(previous_last_coordinate=(4, 0)),
        )
    with pytest.raises(RuntimeError, match="coordinate count"):
        snapshot_maps._validate_map_pack_coordinates(
            {**pack_row_by_field, "coordinate_count": 2},
            (coordinate,),
            snapshot_maps._MapSummaryCursor(),
        )
    with pytest.raises(RuntimeError, match="logical entry count"):
        snapshot_maps._validate_map_pack_coordinates(
            {**pack_row_by_field, "entry_count": 2},
            (coordinate,),
            snapshot_maps._MapSummaryCursor(),
        )
    return coordinate


def _assert_locator_page_validation(coordinate) -> None:
    with pytest.raises(RuntimeError, match="identity is ambiguous"):
        snapshot_maps._locator_identity_by_hash(
            {
                ("a", 0): snapshot_maps.V4SnapshotMapCoordinate(
                    "a", 0, 0, 1, b"h" * 32
                ),
                ("b", 0): snapshot_maps.V4SnapshotMapCoordinate(
                    "b", 0, 0, 2, b"h" * 32
                ),
            }
        )
    with pytest.raises(RuntimeError, match="page is malformed"):
        snapshot_maps._verify_zero_locator_members(
            page_by_owner={("r", 0): ("a", 0, 0)},
            coordinate_by_page={("a", 0): coordinate},
            payload_by_hash={},
        )


def _assert_heavy_owner_validation() -> None:
    relation_metadata_by_name = {"r": {
            "owner_base": 8,
            "owner_count": 4,
            "locator_object_kind": "locator",
            "member_object_kind": "member",
            "logical_member_count": 2,
            "vector_member_count": 0,
        }
    }
    with pytest.raises(RuntimeError, match="vector member count"):
        snapshot_maps._validate_relation_entry_counts(
            relation_metadata_by_name,
            {"locator": 4, "member": 1},
        )
    valid_owner_by_field = {
        "object_kind": "bitmap",
        "member_count": 2,
        "member_base": 10,
        "member_span": 8,
        "fragment_count": 1,
        "fragments": {0},
        "entry_count": 2,
    }
    invalid_owners = (
        ({("missing", 9): valid_owner_by_field}, "no relation"),
        ({("r", 20): valid_owner_by_field}, "outside"),
        (
            {
                ("r", 9): {
                    **valid_owner_by_field,
                    "object_kind": "missing",
                }
            },
            "missing map",
        ),
        ({("r", 9): {**valid_owner_by_field, "fragments": set()}}, "incomplete"),
        (
            {("r", 9): {**valid_owner_by_field, "entry_count": 1}},
            "member count",
        ),
        (
            {
                ("r", 9): {
                    **valid_owner_by_field,
                    "member_base": 2**63 - 1,
                    "member_span": 2,
                }
            },
            "exceeds bigint",
        ),
    )
    for owners, message in invalid_owners:
        with pytest.raises(RuntimeError, match=message):
            snapshot_maps._validate_heavy_owners(
                owners,
                relation_metadata_by_name,
                {"bitmap", "locator", "member"},
            )


def _assert_target_metadata_conflict() -> None:
    first = _reference("a", 0, 0, entry_count=1)
    second = SharedBlockReference(
        object_kind="a",
        block_key=1,
        fragment_no=0,
        entry_count=2,
        block_hash=first.block_hash,
        raw_byte_count=first.raw_byte_count,
    )
    with pytest.raises(ValueError, match="inconsistent logical metadata"):
        snapshot_maps._target_metadata_by_hash(
            SimpleNamespace(references=(first, second))
        )


def test_snapshot_additional_fail_closed_branch_matrix() -> None:
    """Cover additional malformed snapshot rows and ownership conflicts."""
    _assert_snapshot_row_mapping()
    _assert_map_pack_decoding()
    _assert_map_pack_sequence()
    coordinate = _assert_map_coordinate_validation()
    _assert_locator_page_validation(coordinate)
    _assert_heavy_owner_validation()
    _assert_target_metadata_conflict()


@pytest.mark.asyncio
async def _assert_target_metadata_rows():
    target_hash = b"t" * 32
    target_metadata_by_field = {
        "block_hash": target_hash,
        "format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "object_kind": "members",
        "codec": "none",
        "entry_count": 2,
        "raw_byte_count": 8,
        "stored_byte_count": 8,
    }
    assert await snapshot_maps._load_target_metadata(
        _ScriptedSession(_Result(rows=(target_metadata_by_field,))),
        schema='"mrf"',
        target_hashes={target_hash},
    ) == {target_hash: ("members", 2, 8)}
    with pytest.raises(RuntimeError, match="metadata is invalid"):
        await snapshot_maps._load_target_metadata(
            _ScriptedSession(
                _Result(rows=({**target_metadata_by_field, "codec": "gzip"},))
            ),
            schema='"mrf"',
            target_hashes={target_hash},
        )
    with pytest.raises(RuntimeError, match="resolve every"):
        await snapshot_maps._load_target_metadata(
            _ScriptedSession(_Result()),
            schema='"mrf"',
            target_hashes={target_hash},
        )
    return target_hash, target_metadata_by_field


async def _assert_locator_payload_rows() -> None:
    locator_payload = struct.pack("<QI", 0, 0)
    locator_hash = shared_block_hash(
        format_version=PTG2_V3_SHARED_FORMAT_VERSION,
        object_kind="locator",
        codec="none",
        payload=locator_payload,
    )
    locator_row_by_field = {
        "block_hash": locator_hash,
        "format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "object_kind": "locator",
        "codec": "none",
        "entry_count": 1,
        "raw_byte_count": len(locator_payload),
        "stored_byte_count": len(locator_payload),
        "payload": locator_payload,
    }
    assert await snapshot_maps._load_locator_payloads(
        _ScriptedSession(_Result(rows=(locator_row_by_field,))),
        schema='"mrf"',
        identity_by_hash={locator_hash: ("locator", 1)},
    ) == {locator_hash: locator_payload}
    with pytest.raises(RuntimeError, match="CAS block is invalid"):
        await snapshot_maps._load_locator_payloads(
            _ScriptedSession(
                _Result(rows=({**locator_row_by_field, "entry_count": 2},))
            ),
            schema='"mrf"',
            identity_by_hash={locator_hash: ("locator", 1)},
        )


async def _assert_dense_table_keys() -> None:
    assert await snapshot_maps._verify_dense_table_keys(
        _ScriptedSession(_Result(rows=((2, 0, 1),))),
        schema_name="mrf",
        table_name="dictionary",
        key_column="key",
        snapshot_key=17,
        expected_count=2,
        context="test",
    ) == 2
    with pytest.raises(RuntimeError, match="dense keyspace"):
        await snapshot_maps._verify_dense_table_keys(
            _ScriptedSession(_Result(rows=((2, 1, 2),))),
            schema_name="mrf",
            table_name="dictionary",
            key_column="key",
            snapshot_key=17,
            expected_count=2,
            context="test",
        )


async def _assert_dictionary_batches() -> None:
    npi_rows = ({"npi_key": 0, "npi": 1_234_567_890},)
    empty_session = _ScriptedSession()
    await snapshot_maps._publish_v4_npi_batch(
        empty_session,
        schema_name="mrf",
        snapshot_key=17,
        npi_rows=(),
    )
    assert empty_session.calls == []
    await snapshot_maps._publish_v4_npi_batch(
        _ScriptedSession(_Result(), _Result(rows=npi_rows)),
        schema_name="mrf",
        snapshot_key=17,
        npi_rows=npi_rows,
    )
    with pytest.raises(RuntimeError, match="conflicts"):
        await snapshot_maps._publish_v4_npi_batch(
            _ScriptedSession(
                _Result(),
                _Result(rows=({"npi_key": 0, "npi": 1_234_567_891},)),
            ),
            schema_name="mrf",
            snapshot_key=17,
            npi_rows=npi_rows,
        )
    component_rows = (
        {"component_key": 0, "component_global_id_128": b"c" * 16},
    )
    await snapshot_maps._publish_v4_component_batch(
        _ScriptedSession(_Result(), _Result(rows=component_rows)),
        schema_name="mrf",
        snapshot_key=17,
        component_rows=component_rows,
    )
    with pytest.raises(RuntimeError, match="conflicts"):
        await snapshot_maps._publish_v4_component_batch(
            _ScriptedSession(_Result(), _Result()),
            schema_name="mrf",
            snapshot_key=17,
            component_rows=component_rows,
        )

    pattern_rows = (
        {"pattern_key": 0, "pattern_digest": b"p" * 32, "set_count": 2},
    )
    await snapshot_maps._publish_v4_pattern_batch(
        _ScriptedSession(_Result(), _Result(rows=pattern_rows)),
        schema_name="mrf",
        snapshot_key=17,
        pattern_rows=pattern_rows,
    )
    with pytest.raises(RuntimeError, match="conflicts"):
        await snapshot_maps._publish_v4_pattern_batch(
            _ScriptedSession(_Result(), _Result()),
            schema_name="mrf",
            snapshot_key=17,
            pattern_rows=pattern_rows,
        )


async def _assert_target_block_rows(
    target_hash,
    target_metadata_by_field,
) -> None:
    metadata = {
        target_hash: (
            PTG2_V3_SHARED_FORMAT_VERSION,
            "members",
            "none",
            2,
            8,
            8,
        )
    }
    await snapshot_maps._verify_target_blocks(
        _ScriptedSession(_Result(rows=(target_metadata_by_field,))),
        schema='"mrf"',
        metadata_by_hash=metadata,
    )
    with pytest.raises(RuntimeError, match="resolve every"):
        await snapshot_maps._verify_target_blocks(
            _ScriptedSession(_Result()),
            schema='"mrf"',
            metadata_by_hash=metadata,
        )


async def _assert_map_block_rows() -> None:
    map_block = SharedBlock(
        object_kind=snapshot_maps.PTG2_V4_MAP_BLOCK_KIND,
        block_key=0,
        fragment_no=0,
        entry_count=1,
        codec="none",
        raw_byte_count=4,
        payload=b"data",
    )
    map_metadata_by_field = {
        "format_version": map_block.format_version,
        "object_kind": map_block.object_kind,
        "codec": map_block.codec,
        "entry_count": map_block.entry_count,
        "raw_byte_count": map_block.raw_byte_count,
        "stored_byte_count": map_block.stored_byte_count,
    }
    await snapshot_maps._verify_map_block(
        _ScriptedSession(_Result(rows=(map_metadata_by_field,))),
        schema='"mrf"',
        map_block=map_block,
    )
    with pytest.raises(RuntimeError, match="conflicts"):
        await snapshot_maps._verify_map_block(
            _ScriptedSession(_Result()),
            schema='"mrf"',
            map_block=map_block,
        )


@pytest.mark.asyncio
async def test_snapshot_database_validation_branch_matrix() -> None:
    """Validate database rows for locators, targets, maps, and metadata."""
    target_hash, target_metadata = await _assert_target_metadata_rows()
    await _assert_locator_payload_rows()
    await _assert_dense_table_keys()
    await _assert_dictionary_batches()
    await _assert_target_block_rows(target_hash, target_metadata)
    await _assert_map_block_rows()
