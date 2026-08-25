# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed selection and decoding for packed finalizer mappings."""

from __future__ import annotations

from copy import deepcopy

import pytest

from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    SharedMappingReadLimitError,
    _shared_block_read_request,
    _stream_shared_mapping_records,
    fetch_shared_blocks,
    shared_block_read_once_scope,
)
from process.ptg_parts.ptg2_shared_blocks import SharedBlock
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_CONTRACT,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
    FinalizerMapError,
    has_complete_v4_finalizer_map,
    has_valid_finalizer_map,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_BLOCK_KIND,
    PTG2_V4_MAP_FORMAT,
    PTG2_V4_SHARED_GENERATION,
    encode_v4_snapshot_map_pack,
)


class _Rows:
    def __init__(self, rows=()) -> None:
        self.rows = list(rows)

    def __iter__(self):
        return iter(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def scalar(self):
        row = self.first()
        return row[0] if isinstance(row, tuple) else row


class _ScriptedSession:
    def __init__(self, results, *, finalizer_tables=True) -> None:
        self.results = list(results)
        self.finalizer_tables = finalizer_tables
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def execute(self, statement, params=None):
        sql = str(statement)
        parameters_by_name = dict(params or {})
        self.calls.append((sql, parameters_by_name))
        if "to_regclass" in sql and set(parameters_by_name) == {
            "ptg2_v4_finalizer_map_root",
            "ptg2_v4_finalizer_map_pack",
            "ptg2_v4_finalizer_map_target",
        }:
            table_present = (
                self.finalizer_tables.get
                if isinstance(self.finalizer_tables, dict)
                else lambda _name: self.finalizer_tables
            )
            return _Rows(
                ({name: table_present(name) for name in parameters_by_name},)
            )
        assert self.results, f"unexpected SQL: {statement}"
        return self.results.pop(0)


def _manifest() -> dict[str, object]:
    return {
        "contract": PTG2_V4_FINALIZER_MAP_CONTRACT,
        "map_format": PTG2_V4_MAP_FORMAT,
        "map_digest": (b"d" * 32).hex(),
        "object_kinds": list(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS),
        "object_kind_count": 6,
        "map_pack_count": 6,
        "coordinate_count": 6,
        "entry_count": 9,
        "logical_byte_count": 12,
        "stored_map_byte_count": 600,
        "target_block_count": 6,
        "canonical_mapping_digest": (b"c" * 32).hex(),
        "canonical_byte_count": 640,
        "target_identity_digest": (b"t" * 32).hex(),
    }


def _root_row(*, relational_mapping_present: bool = False) -> dict[str, object]:
    manifest = _manifest()
    return {
        "root_present": True,
        "layout_state": "sealed",
        "layout_generation": PTG2_V4_SHARED_GENERATION,
        "manifest_present": True,
        "finalizer_manifest": manifest,
        "root_state": "complete",
        "root_contract": PTG2_V4_FINALIZER_MAP_CONTRACT,
        "root_map_format": PTG2_V4_MAP_FORMAT,
        "root_map_digest": b"d" * 32,
        "root_canonical_mapping_digest": b"c" * 32,
        "root_canonical_byte_count": manifest["canonical_byte_count"],
        "root_target_identity_digest": b"t" * 32,
        "root_object_kind_count": manifest["object_kind_count"],
        "root_map_pack_count": manifest["map_pack_count"],
        "root_coordinate_count": manifest["coordinate_count"],
        "root_entry_count": manifest["entry_count"],
        "root_logical_byte_count": manifest["logical_byte_count"],
        "root_stored_map_byte_count": manifest["stored_map_byte_count"],
        "root_target_block_count": manifest["target_block_count"],
        "root_completed_at": "complete",
        "relational_mapping_present": relational_mapping_present,
    }


def _packed_fixture(object_kind: str):
    first = SharedBlock(object_kind, 1, 0, 2, "none", 1, b"a")
    requested = SharedBlock(object_kind, 2, 1, 3, "none", 2, b"bc")
    map_payload = encode_v4_snapshot_map_pack(
        object_kind,
        (first.reference(), requested.reference()),
    )
    map_block = SharedBlock(
        PTG2_V4_MAP_BLOCK_KIND,
        0,
        0,
        2,
        "none",
        len(map_payload),
        map_payload,
    )
    pack_by_field = {
        "object_kind": object_kind,
        "pack_no": 0,
        "first_block_key": 1,
        "first_fragment_no": 0,
        "last_block_key": 2,
        "last_fragment_no": 1,
        "coordinate_count": 2,
        "entry_count": 5,
        "logical_byte_count": 3,
        "map_block_hash": map_block.block_hash,
        "map_format_version": map_block.format_version,
        "map_object_kind": map_block.object_kind,
        "map_codec": map_block.codec,
        "map_entry_count": map_block.entry_count,
        "map_raw_byte_count": map_block.raw_byte_count,
        "map_stored_byte_count": map_block.stored_byte_count,
        "map_payload": map_block.payload,
    }
    target_rows = [
        {
            "block_hash": block.block_hash,
            "format_version": block.format_version,
            "object_kind": block.object_kind,
            "codec": block.codec,
            "entry_count": block.entry_count,
            "block_entry_count": block.entry_count,
            "raw_byte_count": block.raw_byte_count,
            "stored_byte_count": block.stored_byte_count,
            "payload": block.payload,
        }
        for block in (first, requested)
    ]
    requested_mapping_by_field = {
        "object_kind": object_kind,
        "block_key": 2,
        "fragment_no": 1,
        "mapping_entry_count": 3,
        "block_hash": requested.block_hash,
    }
    return pack_by_field, target_rows, requested_mapping_by_field


def _single_packed_fixture(object_kind: str, pack_no: int):
    block_payload = pack_no.to_bytes(4, "big")
    target_block = SharedBlock(
        object_kind, pack_no, 0, 1, "none", len(block_payload), block_payload
    )
    map_payload = encode_v4_snapshot_map_pack(
        object_kind, (target_block.reference(),)
    )
    map_block = SharedBlock(
        PTG2_V4_MAP_BLOCK_KIND,
        pack_no,
        0,
        1,
        "none",
        len(map_payload),
        map_payload,
    )
    pack_by_field = {
        "object_kind": object_kind,
        "pack_no": pack_no,
        "first_block_key": pack_no,
        "first_fragment_no": 0,
        "last_block_key": pack_no,
        "last_fragment_no": 0,
        "coordinate_count": 1,
        "entry_count": 1,
        "logical_byte_count": len(block_payload),
        "map_block_hash": map_block.block_hash,
        "map_format_version": map_block.format_version,
        "map_object_kind": map_block.object_kind,
        "map_codec": map_block.codec,
        "map_entry_count": map_block.entry_count,
        "map_raw_byte_count": map_block.raw_byte_count,
        "map_stored_byte_count": map_block.stored_byte_count,
        "map_payload": map_block.payload,
    }
    target_by_field = {
        "block_hash": target_block.block_hash,
        "format_version": target_block.format_version,
        "object_kind": target_block.object_kind,
        "codec": target_block.codec,
        "entry_count": target_block.entry_count,
        "raw_byte_count": target_block.raw_byte_count,
        "stored_byte_count": target_block.stored_byte_count,
    }
    return pack_by_field, target_by_field


async def _records(session, *, object_kind: str, block_keys=(2,), fragments=(1,)):
    request = _shared_block_read_request(
        schema_name="mrf",
        snapshot_key=17,
        object_kind=object_kind,
        block_keys=block_keys,
        fragment_nos=fragments,
        require_all=True,
    )
    return [
        row
        async for row in _stream_shared_mapping_records(
            session,
            request,
            row_limit=8,
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("object_kind", PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS)
async def test_packed_reader_matches_legacy_rows_for_all_six_kinds(
    object_kind: str,
) -> None:
    pack_by_field, target_rows, expected = _packed_fixture(object_kind)
    packed = _ScriptedSession(
        (_Rows((_root_row(),)), _Rows((pack_by_field,)), _Rows(target_rows))
    )
    legacy = _ScriptedSession(
        (
            _Rows(({"root_present": False, "finalizer_manifest": None},)),
            _Rows((expected,)),
        )
    )
    assert await _records(packed, object_kind=object_kind) == [expected]
    assert await _records(legacy, object_kind=object_kind) == [expected]
    assert not any(
        "ptg2_v3_snapshot_block mapping" in sql for sql, _params in packed.calls
    )
    assert any(
        "ptg2_v3_snapshot_block mapping" in sql for sql, _params in legacy.calls
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("use_scope", (False, True))
async def test_packed_reader_validates_and_returns_target_cas_payload(
    use_scope: bool,
) -> None:
    object_kind = PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0]
    pack_by_field, target_rows, expected = _packed_fixture(object_kind)
    session = _ScriptedSession(
        (
            _Rows((_root_row(),)),
            _Rows((pack_by_field,)),
            _Rows(target_rows),
            _Rows((target_rows[1],)),
        )
    )
    payload_fetch = fetch_shared_blocks(
        session,
        schema_name="mrf",
        snapshot_key=17,
        object_kind=object_kind,
        block_keys=(2,),
        fragment_nos=(1,),
        require_all=True,
    )
    if use_scope:
        with shared_block_read_once_scope(max_retained_raw_bytes=16):
            payloads_by_key = await payload_fetch
    else:
        payloads_by_key = await payload_fetch
    assert payloads_by_key[2][0].payload == b"bc"
    assert payloads_by_key[2][0].block_hash == expected["block_hash"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (
            lambda row: row.update(manifest_present=False, finalizer_manifest=None),
            "root and manifest must appear together",
        ),
        (
            lambda row: row.update(finalizer_manifest=None),
            "manifest is not an object",
        ),
        (
            lambda row: row["finalizer_manifest"].update(contract="unknown"),
            "manifest contract is incompatible",
        ),
        (
            lambda row: row.update(root_state="building"),
            "root is unavailable or incomplete",
        ),
        (
            lambda row: row["finalizer_manifest"].update(map_pack_count=7),
            "map_pack_count does not match its manifest",
        ),
        (
            lambda row: row.update(relational_mapping_present=True),
            "also contains relational mappings",
        ),
    ),
)
async def test_packed_root_selection_fails_closed(mutate, message: str) -> None:
    row = deepcopy(_root_row())
    mutate(row)
    session = _ScriptedSession((_Rows((row,)),))
    with pytest.raises(FinalizerMapError, match=message):
        await has_complete_v4_finalizer_map(
            session,
            schema_name="mrf",
            snapshot_key=17,
        )


@pytest.mark.asyncio
async def test_manifest_without_root_fails_closed() -> None:
    session = _ScriptedSession(
        (
            _Rows(
                (
                    {
                        "root_present": False,
                        "manifest_present": True,
                        "finalizer_manifest": _manifest(),
                    },
                )
            ),
        )
    )

    with pytest.raises(
        FinalizerMapError,
        match="root and manifest must appear together",
    ):
        await has_complete_v4_finalizer_map(
            session,
            schema_name="mrf",
            snapshot_key=17,
        )


@pytest.mark.asyncio
async def test_seal_allows_pre_table_legacy_but_not_manifest_only() -> None:
    legacy = _ScriptedSession((), finalizer_tables=False)
    assert not await has_valid_finalizer_map(
        legacy,
        schema_name="mrf",
        snapshot_key=17,
        layout_manifest={},
    )
    manifest_only = _ScriptedSession((), finalizer_tables=False)
    with pytest.raises(FinalizerMapError, match="no storage contract"):
        await has_valid_finalizer_map(
            manifest_only,
            schema_name="mrf",
            snapshot_key=17,
            layout_manifest={"serving_index": {"finalizer_mapping": _manifest()}},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("map_hash", "CAS hash does not match"),
        ("coordinate_count", "pack geometry is inconsistent"),
        ("target_entry", "target identity is inconsistent"),
        ("logical_bytes", "logical byte count is inconsistent"),
    ),
)
async def test_packed_map_and_target_mismatches_fail_closed(
    case: str,
    message: str,
) -> None:
    object_kind = PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0]
    pack_by_field, target_rows, _expected = _packed_fixture(object_kind)
    if case == "map_hash":
        pack_by_field["map_block_hash"] = b"x" * 32
    elif case == "coordinate_count":
        pack_by_field["coordinate_count"] = 1
    elif case == "target_entry":
        target_rows[1]["entry_count"] = 4
    else:
        target_rows[1]["raw_byte_count"] = 4
        target_rows[1]["stored_byte_count"] = 4
    session = _ScriptedSession(
        (_Rows((_root_row(),)), _Rows((pack_by_field,)), _Rows(target_rows))
    )
    with pytest.raises(PTG2SharedBlockError, match=message):
        await _records(session, object_kind=object_kind)


@pytest.mark.asyncio
async def test_missing_target_anchor_and_row_overflow_fail_closed() -> None:
    object_kind = PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0]
    pack_by_field, target_rows, _expected = _packed_fixture(object_kind)
    missing_anchor = _ScriptedSession(
        (_Rows((_root_row(),)), _Rows((pack_by_field,)), _Rows(target_rows[:1]))
    )
    with pytest.raises(PTG2SharedBlockError, match="missing a durable target anchor"):
        await _records(
            missing_anchor,
            object_kind=object_kind,
            block_keys=(1, 2),
            fragments=None,
        )

    overflow = _ScriptedSession(
        (_Rows((_root_row(),)), _Rows((pack_by_field,)), _Rows(target_rows))
    )
    request = _shared_block_read_request(
        schema_name="mrf",
        snapshot_key=17,
        object_kind=object_kind,
        block_keys=(1, 2),
        fragment_nos=None,
        require_all=True,
    )
    with pytest.raises(SharedMappingReadLimitError, match="bounded row limit"):
        async for _mapping_fields in _stream_shared_mapping_records(
            overflow,
            request,
            row_limit=1,
        ):
            raise AssertionError("mapping overflow should fail before delivery")


@pytest.mark.asyncio
async def test_sparse_packed_reads_page_before_row_overflow() -> None:
    object_kind = PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0]
    fixtures = tuple(_single_packed_fixture(object_kind, index) for index in range(129))
    session = _ScriptedSession(
        (
            _Rows((_root_row(),)),
            _Rows(pack_by_field for pack_by_field, _target_by_field in fixtures[:128]),
            _Rows(target_by_field for _pack_by_field, target_by_field in fixtures[:128]),
            _Rows((fixtures[128][0],)),
            _Rows((fixtures[128][1],)),
        )
    )
    request = _shared_block_read_request(
        schema_name="mrf",
        snapshot_key=17,
        object_kind=object_kind,
        block_keys=range(129),
        fragment_nos=None,
        require_all=True,
    )

    with pytest.raises(SharedMappingReadLimitError, match="bounded row limit"):
        async for _mapping_fields in _stream_shared_mapping_records(
            session,
            request,
            row_limit=128,
        ):
            raise AssertionError("mapping overflow should fail before delivery")

    pack_calls = [params for sql, params in session.calls if "LIMIT" in sql]
    assert [params["after_pack_no"] for params in pack_calls] == [-1, 127]
    assert all(params["pack_limit"] == 128 for params in pack_calls)
    target_calls = [
        params
        for sql, params in session.calls
        if "ptg2_v4_finalizer_map_target" in sql and "block_hashes" in params
    ]
    assert max(len(params["block_hashes"]) for params in target_calls) == 128
