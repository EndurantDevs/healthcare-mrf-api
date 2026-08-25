# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed contracts for packed-finalizer mapping and range readers."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_v4_finalizer_mapping_reader as mapping_reader
from process.ptg_parts import ptg2_v4_finalizer_native as native
from process.ptg_parts import ptg2_v4_finalizer_range_reader as range_reader
from process.ptg_parts import ptg2_v4_finalizer_maps as finalizer_maps
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
    FinalizerMapError,
    FinalizerMapReadLimitError,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import V4SnapshotMapCoordinate
from tests.test_ptg2_v4_finalizer_maps import (
    _Rows,
    _ScriptedSession,
    _packed_fixture,
    _root_row,
)

@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("state", "root is unavailable or incomplete"),
        ("fields", "manifest fields are incompatible"),
        ("contract", "manifest contract is incompatible"),
        ("map_digest", "digest does not match its manifest"),
        ("native_digest", "native receipt does not match its manifest"),
    ),
)
def test_packed_root_identity_rejects_contract_drift(case: str, message: str) -> None:
    root = deepcopy(_root_row())
    manifest = root["finalizer_manifest"]
    if case == "state":
        root["layout_state"] = "building"
    elif case == "fields":
        manifest["unexpected"] = True
    elif case == "contract":
        manifest["object_kinds"] = "not-a-list"
    elif case == "map_digest":
        root["root_map_digest"] = b"x" * 32
    else:
        root["root_canonical_mapping_digest"] = b"short"
    with pytest.raises(FinalizerMapError, match=message):
        finalizer_maps._validate_root_manifest_identity(root, manifest)


def test_packed_root_rejects_invalid_count_and_geometry() -> None:
    with pytest.raises(FinalizerMapError, match="count is invalid"):
        finalizer_maps._strict_count(True, "count")
    root = deepcopy(_root_row())
    manifest = root["finalizer_manifest"]
    root["root_target_block_count"] = 0
    manifest["target_block_count"] = 0
    with pytest.raises(FinalizerMapError, match="root geometry is invalid"):
        finalizer_maps._validated_root(root, manifest)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("empty", "invalid", "missing"))
async def test_packed_target_metadata_fails_closed(case: str) -> None:
    object_kind = PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0]
    _pack, targets, _mapping = _packed_fixture(object_kind)
    block_hash = bytes(targets[0]["block_hash"])
    if case == "empty":
        assert await finalizer_maps._load_target_metadata(
            object(), schema='"mrf"', snapshot_key=7, target_hashes=set()
        ) == {}
        return
    if case == "invalid":
        targets[0]["format_version"] = -1
        rows = _Rows((targets[0],))
        message = "target CAS metadata is invalid"
    else:
        rows = _Rows()
        message = "missing a durable target anchor"
    session = _ScriptedSession((rows,))
    with pytest.raises(FinalizerMapError, match=message):
        await finalizer_maps._load_target_metadata(
            session,
            schema='"mrf"',
            snapshot_key=7,
            target_hashes={block_hash},
        )


@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("unexpected", "unexpected pack"),
        ("payload", "incompatible CAS metadata"),
        ("geometry", "pack geometry is inconsistent"),
        ("overlap", "pack ranges overlap"),
    ),
)
def test_packed_map_decoder_rejects_pack_drift(case: str, message: str) -> None:
    object_kind = PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0]
    pack, _targets, _mapping = _packed_fixture(object_kind)
    rows = [deepcopy(pack)]
    if case == "unexpected":
        rows[0]["object_kind"] = "other"
    elif case == "payload":
        rows[0]["map_payload"] = b"corrupt"
    elif case == "geometry":
        rows[0]["coordinate_count"] = 1
    else:
        second = deepcopy(pack)
        second["pack_no"] = 1
        rows.append(second)
    with pytest.raises(FinalizerMapError, match=message):
        finalizer_maps._decode_map_packs(rows, object_kind=object_kind)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("kind", "legacy", "limit", "empty"))
async def test_packed_mapping_loader_shortcuts_and_limits(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    object_kind = PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0]
    if case == "kind":
        assert await finalizer_maps.load_v4_finalizer_mapping_records(
            object(),
            schema_name="mrf",
            snapshot_key=7,
            object_kind="relational-kind",
            block_keys=(1,),
            fragment_nos=None,
            row_limit=1,
        ) is None
        return
    monkeypatch.setattr(
        finalizer_maps,
        "has_complete_v4_finalizer_map",
        AsyncMock(return_value=case != "legacy"),
    )
    if case == "legacy":
        assert await finalizer_maps.load_v4_finalizer_mapping_records(
            object(),
            schema_name="mrf",
            snapshot_key=7,
            object_kind=object_kind,
            block_keys=(1,),
            fragment_nos=None,
            row_limit=1,
        ) is None
    elif case == "limit":
        with pytest.raises(ValueError, match="row limit must be positive"):
            await finalizer_maps.load_v4_finalizer_mapping_records(
                object(),
                schema_name="mrf",
                snapshot_key=7,
                object_kind=object_kind,
                block_keys=(1,),
                fragment_nos=None,
                row_limit=0,
            )
    else:
        assert await finalizer_maps.load_v4_finalizer_mapping_records(
            object(),
            schema_name="mrf",
            snapshot_key=7,
            object_kind=object_kind,
            block_keys=(),
            fragment_nos=None,
            row_limit=1,
        ) == ()


def _decoded_pack():
    object_kind = PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0]
    pack, targets, _mapping = _packed_fixture(object_kind)
    decoded = finalizer_maps._decode_map_packs((pack,), object_kind=object_kind)[0]
    metadata = {
        bytes(row["block_hash"]): (
            str(row["object_kind"]),
            int(row["entry_count"]),
            int(row["raw_byte_count"]),
        )
        for row in targets
    }
    return object_kind, decoded, metadata


def test_packed_mapping_reader_rejects_overlap_identity_limit_and_bytes() -> None:
    _kind, decoded, metadata = _decoded_pack()
    with pytest.raises(FinalizerMapError, match="pack ranges overlap"):
        mapping_reader._assert_pack_page_order((decoded,), (2, 1))

    invalid_metadata_by_hash = dict(metadata)
    first_hash = bytes(decoded[1][0].block_hash)
    invalid_metadata_by_hash[first_hash] = ("wrong", 1, 1)
    with pytest.raises(FinalizerMapError, match="target identity is inconsistent"):
        mapping_reader._selected_mapping_rows(
            (decoded,),
            metadata_by_hash=invalid_metadata_by_hash,
            block_keys=frozenset((1, 2)),
            fragment_nos=frozenset(),
            has_fragment_filter=False,
            row_limit=2,
        )
    with pytest.raises(FinalizerMapReadLimitError, match="bounded row limit"):
        mapping_reader._selected_mapping_rows(
            (decoded,),
            metadata_by_hash=metadata,
            block_keys=frozenset((1, 2)),
            fragment_nos=frozenset(),
            has_fragment_filter=False,
            row_limit=0,
        )
    with pytest.raises(FinalizerMapError, match="logical byte count is inconsistent"):
        mapping_reader._selected_mapping_rows(
            (({**decoded[0], "logical_byte_count": 0}, decoded[1]),),
            metadata_by_hash=metadata,
            block_keys=frozenset(),
            fragment_nos=frozenset(),
            has_fragment_filter=False,
            row_limit=1,
        )


@pytest.mark.asyncio
async def test_packed_mapping_reader_accepts_empty_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mapping_reader, "_load_map_packs", AsyncMock(return_value=[]))
    assert await mapping_reader.load_selected_mapping_rows(
        object(),
        schema='"mrf"',
        snapshot_key=7,
        object_kind=PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0],
        block_keys=(1,),
        fragment_nos=(),
        has_fragment_filter=False,
        row_limit=1,
    ) == ()


@pytest.mark.parametrize(
    "ranges",
    (
        ((1, 2),),
        ((True, 0, 1),),
        (("bad", 0, 1),),
        ((1, 2, 1),),
        ((1, 0, 1), (1, 2, 3)),
    ),
)
def test_packed_range_normalization_rejects_invalid_ranges(ranges) -> None:
    with pytest.raises(ValueError, match="range is invalid"):
        range_reader._normalized_map_ranges(ranges)


def test_packed_range_limit_accepts_none_and_positive_only() -> None:
    assert range_reader._normalized_range_pack_limit(None) is None
    assert range_reader._normalized_range_pack_limit(1) == 1
    with pytest.raises(ValueError, match="pack limit must be positive"):
        range_reader._normalized_range_pack_limit(False)


@pytest.mark.asyncio
async def test_packed_range_query_rejects_oversized_batch() -> None:
    state = range_reader._RangeReadState.create(((1, 0, 2),), None)
    session = _ScriptedSession((_Rows(({}, {})),))
    with pytest.raises(FinalizerMapError, match="batch exceeded its limit"):
        await range_reader._load_range_pack_batch(
            session,
            schema='"mrf"',
            snapshot_key=7,
            object_kind=PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0],
            state=state,
            batch_rows=1,
        )


@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("coordinate", "invalid coordinate"),
        ("unexpected", "unexpected pack"),
        ("oversized", "pack is oversized"),
        ("overlap", "pack ranges overlap"),
        ("empty", "batch is empty"),
    ),
)
def test_packed_range_decoder_rejects_invalid_pages(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    message: str,
) -> None:
    state = range_reader._RangeReadState.create(((1, 0, 2),), None)
    pack_record_by_field = {"range_key": 1, "pack_no": 0}
    pack_records = (pack_record_by_field,)
    if case == "coordinate":
        pack_record_by_field["range_key"] = None
    elif case == "unexpected":
        pack_record_by_field["range_key"] = 2
    elif case == "empty":
        pack_records = ()
    else:
        coordinates = tuple(
            V4SnapshotMapCoordinate("kind", index, 0, 1, bytes([index % 256]) * 32)
            for index in range(257 if case == "oversized" else 1)
        )
        monkeypatch.setattr(
            range_reader,
            "_decode_map_packs",
            lambda *_args, **_kwargs: ((pack_record_by_field, coordinates),),
        )
        if case == "overlap":
            state.previous_pack_by_range[1] = 0
    with pytest.raises(FinalizerMapError, match=message):
        range_reader._decode_range_pack_batch(
            pack_records,
            object_kind="kind",
            state=state,
        )


@pytest.mark.parametrize("case", ("identity", "bytes", "success"))
def test_packed_range_retention_authenticates_before_keeping_keys(case: str) -> None:
    _kind, decoded, metadata = _decoded_pack()
    state = range_reader._RangeReadState.create(((7, 1, 2),), None)
    pack_by_field, coordinates = decoded
    decoded_ranges = ((7, pack_by_field, coordinates),)
    if case == "identity":
        metadata = dict(metadata)
        metadata[bytes(coordinates[0].block_hash)] = ("wrong", 1, 1)
        message = "target identity is inconsistent"
    elif case == "bytes":
        pack_by_field = {**pack_by_field, "logical_byte_count": 0}
        decoded_ranges = ((7, pack_by_field, coordinates),)
        message = "logical byte count is inconsistent"
    else:
        claimed_coordinates = []
        range_reader._append_validated_range_keys(
            decoded_ranges,
            metadata_by_hash=metadata,
            state=state,
            claim_block_key=lambda *coordinate: claimed_coordinates.append(coordinate),
        )
        assert claimed_coordinates == [(7, 1), (7, 2)]
        assert state.result() == {7: (1, 2)}
        return
    with pytest.raises(FinalizerMapError, match=message):
        range_reader._append_validated_range_keys(
            decoded_ranges,
            metadata_by_hash=metadata,
            state=state,
            claim_block_key=None,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("kind", "legacy", "callback", "empty", "limit"))
async def test_packed_range_loader_shortcuts_and_guards(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    object_kind = PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0]
    if case == "kind":
        assert await range_reader.load_v4_finalizer_range_keys(
            object(), schema_name="mrf", snapshot_key=7,
            object_kind="relational", ranges=((1, 0, 1),),
        ) is None
        return
    monkeypatch.setattr(
        range_reader,
        "has_complete_v4_finalizer_map",
        AsyncMock(return_value=case != "legacy"),
    )
    if case == "legacy":
        assert await range_reader.load_v4_finalizer_range_keys(
            object(), schema_name="mrf", snapshot_key=7,
            object_kind=object_kind, ranges=((1, 0, 1),),
        ) is None
    elif case == "callback":
        with pytest.raises(ValueError, match="retention callback is invalid"):
            await range_reader.load_v4_finalizer_range_keys(
                object(), schema_name="mrf", snapshot_key=7,
                object_kind=object_kind, ranges=((1, 0, 1),),
                claim_block_key=object(),
            )
    elif case == "empty":
        assert await range_reader.load_v4_finalizer_range_keys(
            object(), schema_name="mrf", snapshot_key=7,
            object_kind=object_kind, ranges=(),
        ) == {}
    else:
        state = range_reader._RangeReadState.create(((1, 0, 1),), 0)
        await range_reader._load_range_pack_pages(
            object(), schema_name="mrf", snapshot_key=7,
            object_kind=object_kind, state=state, claim_block_key=None,
        )
