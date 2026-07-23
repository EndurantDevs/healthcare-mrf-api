from __future__ import annotations

from tests.ptg2_v4_coverage_support import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    SharedBlockReference,
    SimpleNamespace,
    _metadata,
    _owner_row,
    _reference,
    _relation_row,
    _summary,
    json,
    pytest,
    snapshot_maps,
    struct,
)

def _assert_snapshot_environment(monkeypatch) -> None:
    monkeypatch.delenv("PTG_TEST_SECONDS", raising=False)
    assert snapshot_maps._env_non_negative_seconds("PTG_TEST_SECONDS", 9) == 9
    monkeypatch.setenv("PTG_TEST_SECONDS", "-2")
    assert snapshot_maps._env_non_negative_seconds("PTG_TEST_SECONDS", 9) == 0
    monkeypatch.setenv("PTG_TEST_SECONDS", "broken")
    assert snapshot_maps._env_non_negative_seconds("PTG_TEST_SECONDS", 9) == 9

    with pytest.raises(ValueError, match="32 bytes"):
        snapshot_maps._advisory_lock_key(b"short")
    with pytest.raises(ValueError, match="semantic fingerprint"):
        snapshot_maps.v4_layout_fingerprint(b"short")
    assert len(snapshot_maps.v4_layout_fingerprint(b"s" * 32)) == 32


def _snapshot_map_round_trip():
    valid = _reference("v4_relation_members_v1", 1, 0, entry_count=2)
    encoded = snapshot_maps.encode_v4_snapshot_map_pack(
        valid.object_kind,
        (valid,),
    )
    decoded = snapshot_maps.decode_v4_snapshot_map_pack(
        encoded,
        expected_object_kind=valid.object_kind,
    )
    assert decoded[0].coordinate == (valid.object_kind, 1, 0)
    assert snapshot_maps.v4_map_pack_target_hashes(encoded) == (valid.block_hash,)
    return valid, encoded


def _assert_snapshot_map_codec_rejections(valid, encoded) -> None:
    with pytest.raises(ValueError, match="object_kind"):
        snapshot_maps.V4SnapshotMapCoordinate("", 0, 0, 0, b"x" * 32)
    with pytest.raises(ValueError, match="block_key"):
        snapshot_maps.V4SnapshotMapCoordinate("kind", -1, 0, 0, b"x" * 32)
    with pytest.raises(ValueError, match="fragment_no"):
        snapshot_maps.V4SnapshotMapCoordinate("kind", 0, -1, 0, b"x" * 32)
    with pytest.raises(ValueError, match="entry_count"):
        snapshot_maps.V4SnapshotMapCoordinate("kind", 0, 0, -1, b"x" * 32)
    with pytest.raises(ValueError, match="block_hash"):
        snapshot_maps.V4SnapshotMapCoordinate("kind", 0, 0, 0, b"x")
    with pytest.raises(ValueError, match="coordinate count"):
        snapshot_maps.encode_v4_snapshot_map_pack("kind", ())
    with pytest.raises(ValueError, match="mixes object kinds"):
        snapshot_maps.encode_v4_snapshot_map_pack("other", (valid,))
    with pytest.raises(ValueError, match="strictly ordered"):
        snapshot_maps.encode_v4_snapshot_map_pack(
            valid.object_kind,
            (valid, valid),
        )
    with pytest.raises(ValueError, match="truncated"):
        snapshot_maps.decode_v4_snapshot_map_pack(b"short")
    with pytest.raises(ValueError, match="incompatible format"):
        snapshot_maps.decode_v4_snapshot_map_pack(b"BADMAGIC" + encoded[8:])
    with pytest.raises(ValueError, match="does not match"):
        snapshot_maps.decode_v4_snapshot_map_pack(
            encoded,
            expected_object_kind="other",
        )
    with pytest.raises(ValueError, match="byte count"):
        snapshot_maps.decode_v4_snapshot_map_pack(encoded + b"x")


def _snapshot_map_pack_summary():
    first = _reference("a", 0, 0)
    second = _reference("a", 1, 0)
    third = _reference("b", 0, 0)
    packs = tuple(
        snapshot_maps.iter_v4_snapshot_map_packs(
            (first, second, third),
            max_coordinates_per_pack=1,
        )
    )
    assert [(pack.object_kind, pack.pack_no) for pack in packs] == [
        ("a", 0),
        ("a", 1),
        ("b", 0),
    ]
    assert packs[0].first_coordinate == packs[0].last_coordinate == (0, 0)
    assert packs[0].coordinate_count == 1
    assert packs[0].entry_count == 1
    summary = snapshot_maps.summarize_v4_snapshot_map_packs(packs)
    assert summary.object_kinds == ("a", "b")
    assert summary.object_kind_count == 2
    with pytest.raises(ValueError, match="pack limit"):
        tuple(
            snapshot_maps.iter_v4_snapshot_map_packs(
                (first,),
                max_coordinates_per_pack=0,
            )
        )
    with pytest.raises(ValueError, match="strictly ordered"):
        tuple(snapshot_maps.iter_v4_snapshot_map_packs((second, first)))
    with pytest.raises(ValueError, match="cannot be empty"):
        snapshot_maps.summarize_v4_snapshot_map_packs(())
    return summary


def _snapshot_manifest_fixture(summary):
    metadata = _metadata()
    manifest = snapshot_maps._manifest_with_v4_root(
        {},
        representation="pattern_v1",
        summary=summary,
        metadata=metadata,
    )
    snapshot_maps._validate_v4_manifest_root(
        manifest,
        representation="pattern_v1",
        summary=summary,
        metadata=metadata,
    )
    assert manifest["serving_index"]["snapshot_map"]["map_digest"] == (
        summary.map_digest.hex()
    )
    with pytest.raises(ValueError, match="serving_index"):
        snapshot_maps._manifest_copy_with_index({"serving_index": "bad"})
    with pytest.raises(ValueError, match="storage generation"):
        snapshot_maps._manifest_copy_with_index(
            {"serving_index": {"storage_generation": "foreign"}}
        )
    with pytest.raises(ValueError, match="incompatible type"):
        snapshot_maps._apply_v4_index_markers({"type": "foreign"})
    with pytest.raises(ValueError, match="serving_binary"):
        snapshot_maps._serving_binary_v4_map(
            {"serving_binary": "bad"},
            representation="pattern_v1",
            summary=summary,
            metadata=metadata,
        )
    with pytest.raises(ValueError, match="must remain"):
        snapshot_maps._serving_binary_v4_map(
            {"serving_binary": {"format": "gzip"}},
            representation="pattern_v1",
            summary=summary,
            metadata=metadata,
        )
    with pytest.raises(ValueError, match="conflicting provider_graph"):
        snapshot_maps._serving_binary_v4_map(
            {"serving_binary": {"provider_graph_v4": {"bad": True}}},
            representation="pattern_v1",
            summary=summary,
            metadata=metadata,
        )
    with pytest.raises(ValueError, match="conflicting snapshot-map"):
        snapshot_maps._manifest_with_v4_root(
            {"serving_index": {"snapshot_map": {"bad": True}}},
            representation="pattern_v1",
            summary=summary,
            metadata=metadata,
        )
    return manifest, metadata


def _assert_snapshot_manifest_rejections(manifest, summary, metadata) -> None:
    broken = json.loads(json.dumps(manifest))
    broken["serving_index"]["type"] = "broken"
    with pytest.raises(RuntimeError, match="markers"):
        snapshot_maps._validate_v4_manifest_root(
            broken,
            representation="pattern_v1",
            summary=summary,
            metadata=metadata,
        )
    broken = json.loads(json.dumps(manifest))
    broken["serving_index"]["snapshot_map"]["entry_count"] += 1
    with pytest.raises(RuntimeError, match="root"):
        snapshot_maps._validate_v4_manifest_root(
            broken,
            representation="pattern_v1",
            summary=summary,
            metadata=metadata,
        )
    broken = json.loads(json.dumps(manifest))
    broken["serving_index"]["serving_binary"]["format"] = "bad"
    with pytest.raises(RuntimeError, match="price serving"):
        snapshot_maps._validate_v4_manifest_root(
            broken,
            representation="pattern_v1",
            summary=summary,
            metadata=metadata,
        )


def test_snapshot_map_manifest_and_codec_fail_closed(monkeypatch) -> None:
    """Reject malformed snapshot-map codecs, coordinates, and manifest roots."""
    _assert_snapshot_environment(monkeypatch)
    valid, encoded = _snapshot_map_round_trip()
    _assert_snapshot_map_codec_rejections(valid, encoded)
    summary = _snapshot_map_pack_summary()
    manifest, metadata = _snapshot_manifest_fixture(summary)
    _assert_snapshot_manifest_rejections(manifest, summary, metadata)


def _persisted_pack_fixture():
    reference = _reference(
        "v4_relation_members_v1",
        4,
        0,
        payload=b"\x01\x00\x00\x00\x02\x00\x00\x00",
        entry_count=2,
    )
    pack = snapshot_maps._make_map_pack(
        object_kind=reference.object_kind,
        pack_no=0,
        references=(reference,),
    )
    pack_row_by_field = {
        **snapshot_maps._map_pack_row(pack, 17),
        "map_format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "map_object_kind": snapshot_maps.PTG2_V4_MAP_BLOCK_KIND,
        "map_codec": "none",
        "map_entry_count": 1,
        "map_raw_byte_count": len(pack.map_block.payload),
        "map_stored_byte_count": len(pack.map_block.payload),
        "map_payload": pack.map_block.payload,
    }
    cursor = snapshot_maps._MapSummaryCursor()
    decoded_rows, hashes = snapshot_maps._decode_persisted_map_rows(
        (pack_row_by_field,),
        cursor,
    )
    assert hashes == {reference.block_hash}
    coordinates = decoded_rows[0][1]
    persisted = snapshot_maps._persisted_map_pack(
        pack_row_by_field,
        coordinates,
        {
            reference.block_hash: (
                reference.object_kind,
                reference.entry_count,
                reference.raw_byte_count,
            )
        },
    )
    assert persisted.references == (reference,)
    assert snapshot_maps.summarize_v4_snapshot_map_packs((persisted,)).entry_count == 2
    return reference, pack_row_by_field, cursor, coordinates


def _assert_persisted_pack_rejections(
    reference,
    pack_row_by_field,
    cursor,
    coordinates,
) -> None:
    with pytest.raises(RuntimeError, match="contiguously numbered"):
        snapshot_maps._advance_map_pack_sequence(
            {**pack_row_by_field, "pack_no": 2},
            cursor,
        )
    with pytest.raises(RuntimeError, match="CAS metadata"):
        snapshot_maps._decode_persisted_map_payload(
            {**pack_row_by_field, "map_codec": "gzip"},
            object_kind=reference.object_kind,
        )
    with pytest.raises(RuntimeError, match="CAS hash"):
        snapshot_maps._decode_persisted_map_payload(
            {**pack_row_by_field, "map_block_hash": b"x" * 32},
            object_kind=reference.object_kind,
        )
    with pytest.raises(RuntimeError, match="bounds"):
        snapshot_maps._validate_map_pack_coordinates(
            {**pack_row_by_field, "first_block_key": 5},
            coordinates,
            snapshot_maps._MapSummaryCursor(),
        )
    with pytest.raises(RuntimeError, match="logical byte count"):
        snapshot_maps._persisted_map_pack(
            {**pack_row_by_field, "logical_byte_count": 7},
            coordinates,
            {
                reference.block_hash: (
                    reference.object_kind,
                    reference.entry_count,
                    reference.raw_byte_count,
                )
            },
        )
    with pytest.raises(RuntimeError, match="identity"):
        snapshot_maps._persisted_map_pack(
            pack_row_by_field,
            coordinates,
            {
                reference.block_hash: (
                    "wrong-kind",
                    reference.entry_count,
                    reference.raw_byte_count,
                )
            },
        )


def _assert_root_summary_validation() -> None:
    root_summary = snapshot_maps._summary_from_root_row(
        {
            "map_digest": b"z" * 32,
            "object_kinds": ["b", "a"],
            "map_pack_count": 2,
            "coordinate_count": 3,
            "entry_count": 4,
            "logical_byte_count": 5,
            "stored_map_byte_count": 6,
        }
    )
    assert root_summary.object_kinds == ("b", "a")
    with pytest.raises(RuntimeError, match="entry_count"):
        snapshot_maps._require_matching_summaries(
            _summary(),
            snapshot_maps.V4SnapshotMapSummary(
                **{
                    **_summary().__dict__,
                    "entry_count": 99,
                }
            ),
            context="test",
        )


def _heavy_locator_fixture():
    relation_metadata_by_name = {
        "r": {
            "owner_base": 8,
            "owner_count": 4,
            "locator_owner_span": 2,
            "locator_object_kind": "locator",
            "member_object_kind": "member",
            "logical_member_count": 2,
            "vector_member_count": 0,
        }
    }
    heavy_owner_by_coordinate = {
        ("r", 9): {
            "object_kind": "bitmap",
            "member_count": 2,
            "member_base": 10,
            "member_span": 8,
            "fragment_count": 1,
            "fragments": {0},
            "entry_count": 2,
        },
        ("missing", 1): {},
    }
    requested, page_by_owner = snapshot_maps._heavy_locator_requests(
        relation_metadata_by_name,
        heavy_owner_by_coordinate,
    )
    assert requested == {"locator": {8}}
    assert page_by_owner[("r", 9)] == ("locator", 8, 1)
    coordinate = snapshot_maps.V4SnapshotMapCoordinate(
        "locator",
        8,
        0,
        2,
        b"l" * 32,
    )
    assert snapshot_maps._locator_identity_by_hash(
        {("locator", 8): coordinate}
    ) == {b"l" * 32: ("locator", 2)}
    return (
        relation_metadata_by_name,
        heavy_owner_by_coordinate,
        page_by_owner,
        coordinate,
    )


def _assert_zero_locator_validation(page_by_owner, coordinate) -> None:
    locator_payload = struct.pack("<QI", 0, 0) * 2
    snapshot_maps._verify_zero_locator_members(
        page_by_owner={("r", 9): ("locator", 8, 1)},
        coordinate_by_page={("locator", 8): coordinate},
        payload_by_hash={b"l" * 32: locator_payload},
    )
    with pytest.raises(RuntimeError, match="duplicate vector"):
        snapshot_maps._verify_zero_locator_members(
            page_by_owner={("r", 9): ("locator", 8, 1)},
            coordinate_by_page={("locator", 8): coordinate},
            payload_by_hash={
                b"l" * 32: struct.pack("<QI", 0, 0) + struct.pack("<QI", 0, 1)
            },
        )


def _assert_metadata_count_validation() -> None:
    aggregate_counts_by_field = {
        "npi_count": 2,
        "npi_min": 0,
        "npi_max": 1,
        "component_count": 1,
        "component_min": 0,
        "component_max": 0,
        "pattern_count": 0,
        "pattern_min": None,
        "pattern_max": None,
        "relation_count": 1,
        "heavy_owner_count": 1,
    }
    counts = snapshot_maps._validated_metadata_counts(aggregate_counts_by_field)
    assert (counts.npi_count, counts.component_count, counts.pattern_count) == (
        2,
        1,
        0,
    )
    with pytest.raises(RuntimeError, match="not dense"):
        snapshot_maps._validated_metadata_counts(
            {**aggregate_counts_by_field, "npi_min": 1}
        )


def _assert_relation_entry_validation(
    relation_metadata_by_name,
    heavy_owner_by_coordinate,
) -> None:
    snapshot_maps._validate_relation_entry_counts(
        relation_metadata_by_name,
        {"locator": 4, "member": 0},
    )
    with pytest.raises(RuntimeError, match="locator count"):
        snapshot_maps._validate_relation_entry_counts(
            relation_metadata_by_name,
            {"locator": 3, "member": 0},
        )
    member_counts = snapshot_maps._validate_heavy_owners(
        {("r", 9): heavy_owner_by_coordinate[("r", 9)]},
        relation_metadata_by_name,
        {"locator", "member", "bitmap"},
    )
    assert member_counts == {"r": 2}
    snapshot_maps._validate_replaced_member_counts(
        relation_metadata_by_name,
        member_counts,
    )
    with pytest.raises(RuntimeError, match="logical/vector"):
        snapshot_maps._validate_replaced_member_counts(relation_metadata_by_name, {})


def _assert_normalized_relation_rows() -> None:
    normalized_relation = snapshot_maps._relation_manifest_row(
        _relation_row("a")
    )
    assert normalized_relation["relation"] == "a"
    assert snapshot_maps._normalized_relation_rows(
        (_relation_row("a"), _relation_row("b"))
    )[1]["relation"] == "b"
    with pytest.raises(ValueError, match="strictly ordered"):
        snapshot_maps._normalized_relation_rows(
            (_relation_row("b"), _relation_row("a"))
        )
    with pytest.raises(ValueError, match="invalid page"):
        snapshot_maps._relation_manifest_row(
            {
                **_relation_row("a"),
                "member_object_kind": "same",
                "locator_object_kind": "same",
            }
        )

    assert snapshot_maps._normalized_heavy_owner_rows(
        (_owner_row("a", 1), _owner_row("a", 2))
    )[0]["owner_key"] == 1
    with pytest.raises(ValueError, match="strictly ordered"):
        snapshot_maps._normalized_heavy_owner_rows(
            (_owner_row("a", 2), _owner_row("a", 1))
        )
    with pytest.raises(ValueError, match="invalid bitmap"):
        snapshot_maps._heavy_owner_row(
            {**_owner_row(), "fragment_count": 0}
        )


def test_snapshot_persisted_pack_and_metadata_validators() -> None:
    """Validate persisted map-pack metadata, dense identities, and heavy owners."""
    pack_fixture = _persisted_pack_fixture()
    _assert_persisted_pack_rejections(*pack_fixture)
    _assert_root_summary_validation()
    relation_rows, heavy_owners, locator_pages, coordinate = (
        _heavy_locator_fixture()
    )
    _assert_zero_locator_validation(locator_pages, coordinate)
    _assert_metadata_count_validation()
    _assert_relation_entry_validation(relation_rows, heavy_owners)
    _assert_normalized_relation_rows()
