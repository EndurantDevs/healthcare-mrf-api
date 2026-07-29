# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Selection contracts for authenticated V4 coordinate-map packs."""

from __future__ import annotations

import pytest

from process.ptg_parts import ptg2_v4_audit as audit
from process.ptg_parts.ptg2_shared_audit import _ReadBudget
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    SharedBlock,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_BLOCK_KIND,
    encode_v4_snapshot_map_pack,
)


class _RowsSession:
    def __init__(self, rows) -> None:
        self.rows = tuple(rows)

    async def execute(self, _statement, _parameters=None):
        return self.rows


def _map_pack_row(
    first: SharedBlock,
    requested: SharedBlock,
) -> dict[str, object]:
    map_payload = encode_v4_snapshot_map_pack(
        first.object_kind,
        (first.reference(), requested.reference()),
    )
    map_block = SharedBlock(
        object_kind=PTG2_V4_MAP_BLOCK_KIND,
        block_key=0,
        fragment_no=0,
        entry_count=2,
        codec="none",
        raw_byte_count=len(map_payload),
        payload=map_payload,
    )
    return {
        "pack_no": 0,
        "first_block_key": first.block_key,
        "first_fragment_no": first.fragment_no,
        "last_block_key": requested.block_key,
        "last_fragment_no": requested.fragment_no,
        "coordinate_count": 2,
        "pack_entry_count": first.entry_count + requested.entry_count,
        "map_block_hash": map_block.block_hash,
        "map_format_version": PTG2_V3_SHARED_FORMAT_VERSION,
        "map_object_kind": PTG2_V4_MAP_BLOCK_KIND,
        "map_codec": "none",
        "map_block_entry_count": 2,
        "map_raw_byte_count": len(map_payload),
        "map_stored_byte_count": len(map_payload),
        "map_payload": map_payload,
    }


@pytest.mark.asyncio
async def test_reader_selects_requested_coordinate_from_shared_map_pack() -> None:
    """Retain only requested results while caching every authenticated coordinate."""

    first = SharedBlock("kind", 1, 0, 2, "none", 1, b"a")
    requested = SharedBlock("kind", 2, 0, 3, "none", 1, b"b")
    reader = audit._V4PersistedGraphReader(
        _RowsSession((_map_pack_row(first, requested),)),
        schema_name="mrf",
        snapshot_key=17,
        representation="direct_v1",
        budget=_ReadBudget(),
    )

    coordinates = await reader._map_coordinates(
        object_kind="kind",
        coordinate_pairs=((2, 0),),
    )

    assert coordinates == {
        (2, 0): audit.V4SnapshotMapCoordinate(
            "kind",
            2,
            0,
            3,
            requested.block_hash,
        )
    }
    assert ("kind", 1, 0) in reader._coordinate_cache
