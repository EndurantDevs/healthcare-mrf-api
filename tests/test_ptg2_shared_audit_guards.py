# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed guard coverage for strict shared-block PTG V3 audits."""

from __future__ import annotations

import zlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_audit as audit


def _candidate_summary() -> dict:
    return {
        "audit_candidates": {
            "record_format": audit.PTG2_V3_AUDIT_CANDIDATE_FORMAT,
            "format_version": 2,
            "record_bytes": audit.PTG2_V3_AUDIT_CANDIDATE_RECORD_BYTES,
            "maximum_rows": audit.PTG2_V3_AUDIT_MAX_CANDIDATES,
            "selection_method": audit.PTG2_V3_AUDIT_CANDIDATE_SELECTION,
            "row_count": 1,
            "source_row_count": 1,
            "row_digest": "a" * 64,
        }
    }


def _fragment_row() -> dict:
    payload = b"abc"
    return {
        "block_key": 1,
        "fragment_no": 0,
        "mapping_entry_count": 1,
        "object_kind": "kind",
        "block_object_kind": "kind",
        "format_version": audit.PTG2_V3_SHARED_FORMAT_VERSION,
        "codec": "none",
        "block_entry_count": 1,
        "raw_byte_count": len(payload),
        "stored_byte_count": len(payload),
        "payload": payload,
        "block_hash": audit.shared_block_hash(
            format_version=audit.PTG2_V3_SHARED_FORMAT_VERSION,
            object_kind="kind",
            codec="none",
            payload=payload,
        ),
    }


def _reader(session=None) -> audit._BuildingBlockReader:
    return audit._BuildingBlockReader(
        session,
        schema_name="mrf",
        snapshot_key=1,
        budget=audit._ReadBudget(),
    )


def test_scalar_and_candidate_path_guards(tmp_path):
    with pytest.raises(RuntimeError, match="missing value"):
        audit._mapping(None, "value")
    with pytest.raises(RuntimeError, match="invalid value"):
        audit._integer(True, "value")
    with pytest.raises(RuntimeError, match="invalid value"):
        audit._integer("not-an-integer", "value")
    with pytest.raises(RuntimeError, match="negative value"):
        audit._integer(-1, "value")

    assert audit._row_mapping((("key", 1),)) == {"key": 1}

    with pytest.raises(RuntimeError, match="path is invalid"):
        audit._candidate_path(
            {"output_directory": str(tmp_path)},
            {"path": str(tmp_path / "absolute.bin")},
        )
    with pytest.raises(RuntimeError, match="file is missing"):
        audit._candidate_path(
            {"output_directory": str(tmp_path)},
            {"path": "missing.bin"},
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("record_format", "unsupported", "format is unsupported"),
        ("format_version", 1, "format version is unsupported"),
        ("maximum_rows", 4095, "maximum is unsupported"),
        ("row_digest", "", "digest is invalid"),
    ],
)
def test_candidate_summary_guards(field, value, message):
    summary = _candidate_summary()
    summary["audit_candidates"][field] = value

    with pytest.raises(RuntimeError, match=message):
        audit._validated_candidate_summary(summary)


@pytest.mark.asyncio
async def test_logical_block_reader_handles_empty_cached_and_absent_blocks(
    monkeypatch,
):
    reader = _reader()

    assert await reader.logical_blocks("kind", ()) == {}

    reader._cache[("kind", 1)] = (b"payload", 1)
    assert await reader.logical_blocks("kind", (1,)) == {1: (b"payload", 1)}

    reader._cache.clear()
    monkeypatch.setattr(reader, "_fetch", AsyncMock())
    with pytest.raises(RuntimeError, match="missing kind blocks"):
        await reader.logical_blocks("kind", (1,))


@pytest.mark.asyncio
async def test_fragment_fetch_rejects_duplicate_rows():
    row = _fragment_row()

    class Session:
        async def execute(self, _statement, _params):
            return (row, row)

    with pytest.raises(RuntimeError, match="duplicate fragments"):
        await _reader(Session())._fetch_fragment_rows("kind", (1,))


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"object_kind": "other"}, "kind changed"),
        ({"block_entry_count": 2}, "entry count is inconsistent"),
        ({"format_version": 0}, "format version is incompatible"),
        ({"stored_byte_count": 4}, "stored block length is inconsistent"),
        ({"raw_byte_count": 4}, "raw block length is inconsistent"),
    ],
)
def test_fragment_decode_guards(mutation, message):
    row = _fragment_row()
    row.update(mutation)

    with pytest.raises(RuntimeError, match=message):
        _reader()._decode_fragment("kind", row)


def test_fragment_codec_and_assembly_guards():
    assert audit._BuildingBlockReader._decompress_payload(
        "zlib",
        zlib.compress(b"payload"),
    ) == b"payload"

    with pytest.raises(RuntimeError, match="decompression failed"):
        audit._BuildingBlockReader._decompress_payload("zlib", b"invalid")
    with pytest.raises(RuntimeError, match="codec is unsupported"):
        audit._BuildingBlockReader._decompress_payload("brotli", b"payload")
    with pytest.raises(RuntimeError, match="not contiguous"):
        audit._BuildingBlockReader._assemble_fragments({1: (b"x", 1)})
    with pytest.raises(RuntimeError, match="continuation fragment has entries"):
        audit._BuildingBlockReader._assemble_fragments(
            {0: (b"x", 1), 1: (b"y", 1)}
        )


@pytest.mark.parametrize(
    ("core_layout_id", "selection_kind", "member_count", "message"),
    [
        (b"x" * 31, b"kind", 1, "layout id"),
        (b"x" * 32, b"", 1, "selection kind"),
        (b"x" * 32, b"kind", 0, "empty graph owner"),
    ],
)
def test_deterministic_member_selection_guards(
    core_layout_id,
    selection_kind,
    member_count,
    message,
):
    with pytest.raises(ValueError, match=message):
        audit._deterministic_member_ordinal(
            core_layout_id,
            candidate_ordinal=0,
            selection_kind=selection_kind,
            owner_key=1,
            expansion_ordinal=0,
            member_count=member_count,
        )


@pytest.mark.asyncio
async def test_graph_owner_locator_guards():
    class Session:
        def __init__(self, rows=()):
            self.rows = rows

        async def execute(self, _statement, _params):
            return self.rows

    reader = SimpleNamespace(
        schema_name="mrf",
        snapshot_key=1,
        session=Session(),
    )
    with pytest.raises(ValueError, match="unsupported"):
        await audit._graph_owner_locators(reader, direction=99, owner_keys=(1,))
    assert await audit._graph_owner_locators(
        reader,
        direction=audit.PTG2_V3_GRAPH_GROUP_TO_NPI,
        owner_keys=(),
    ) == {}

    invalid_database_row_by_column = {
        "owner_key": 1,
        "first_chunk": -1,
        "member_offset": 0,
        "member_count": 1,
    }
    reader.session = Session((invalid_database_row_by_column,))
    with pytest.raises(RuntimeError, match="locator is invalid"):
        await audit._graph_owner_locators(
            reader,
            direction=audit.PTG2_V3_GRAPH_GROUP_TO_NPI,
            owner_keys=(1,),
        )

    valid_database_row_by_column = {
        **invalid_database_row_by_column,
        "first_chunk": 0,
    }
    reader.session = Session(
        (valid_database_row_by_column, valid_database_row_by_column)
    )
    with pytest.raises(RuntimeError, match="locator is duplicated"):
        await audit._graph_owner_locators(
            reader,
            direction=audit.PTG2_V3_GRAPH_GROUP_TO_NPI,
            owner_keys=(1,),
        )


@pytest.mark.asyncio
async def test_graph_targeted_member_guards():
    with pytest.raises(ValueError, match="unsupported"):
        await audit._graph_targeted_members(
            object(),
            direction=99,
            member_ordinals_by_owner={},
        )

    assert await audit._graph_targeted_members(
        object(),
        direction=audit.PTG2_V3_GRAPH_GROUP_TO_NPI,
        member_ordinals_by_owner={},
    ) == {}


def test_required_graph_block_key_guards():
    with pytest.raises(RuntimeError, match="owner is missing"):
        audit._required_graph_block_keys({1: (0,)}, {}, 4)
    assert audit._required_graph_block_keys({1: ()}, {}, 4) == set()
    with pytest.raises(RuntimeError, match="offset is unaligned"):
        audit._required_graph_block_keys(
            {1: (0,)},
            {1: audit._GraphOwnerLocator(0, 1, 1)},
            4,
        )
    with pytest.raises(RuntimeError, match="ordinal is out of range"):
        audit._required_graph_block_keys(
            {1: (1,)},
            {1: audit._GraphOwnerLocator(0, 0, 1)},
            4,
        )


def test_graph_member_decode_guards():
    assert audit._decode_requested_graph_members({1: (0,)}, {}, {}, 4) == {}

    with pytest.raises(RuntimeError, match="not strictly ordered"):
        audit._decode_requested_graph_members(
            {1: (0, 1)},
            {1: audit._GraphOwnerLocator(0, 0, 2)},
            {0: ((2).to_bytes(4, "little") + (1).to_bytes(4, "little"), 0)},
            4,
        )

    with pytest.raises(RuntimeError, match="offset is truncated"):
        audit._decode_graph_member(
            {0: (b"abc", 0)},
            audit._GraphOwnerLocator(0, 4, 1),
            0,
            4,
        )

    chunk_end = audit.PTG2_V3_GRAPH_CHUNK_BYTES - 4
    locator = audit._GraphOwnerLocator(0, chunk_end, 1)
    graph_chunk_payload = b"\x01" * audit.PTG2_V3_GRAPH_CHUNK_BYTES
    assert audit._decode_graph_member(
        {0: (graph_chunk_payload, 0), 1: (b"\x02" * 4, 0)},
        locator,
        0,
        8,
    ) == int.from_bytes(b"\x01" * 4 + b"\x02" * 4, "little")
    with pytest.raises(RuntimeError, match="stream is truncated"):
        audit._decode_graph_member({0: (graph_chunk_payload, 0)}, locator, 0, 8)


@pytest.mark.asyncio
async def test_price_membership_span_guard():
    with pytest.raises(RuntimeError, match="block span is invalid"):
        await audit._price_memberships(
            object(),
            price_keys=(),
            atom_key_bits=24,
            block_span=0,
        )


def test_occurrence_coordinate_guards():
    with pytest.raises(ValueError, match="layout id"):
        audit.occurrence_id(
            b"x" * 31,
            code_key=1,
            provider_set_key=2,
            price_key=3,
            source_key=0,
            npi=1_234_567_890,
            atom_ordinal=0,
            atom_key=4,
        )
    with pytest.raises(RuntimeError, match="invalid NPI"):
        audit._occurrence(
            b"x" * 32,
            audit.AuditCandidate(1, 2, 3, 0, 1),
            123,
            0,
            4,
        )
