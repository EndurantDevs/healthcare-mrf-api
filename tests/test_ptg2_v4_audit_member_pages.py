# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import struct

import pytest

from process.ptg_parts import ptg2_v4_audit as audit
from process.ptg_parts.ptg2_shared_audit import _ReadBudget


def _shared_owner_page_parts():
    object_kind = "v4_set_groups_direct_members_v1"
    block_hash = b"h" * 32
    manifest = audit._V4RelationManifest(
        relation="set_groups_direct",
        member_object_kind=object_kind,
        locator_object_kind="v4_set_groups_direct_locators_v1",
        owner_base=0,
        owner_count=2,
        logical_member_count=4,
        vector_member_count=4,
        member_width=4,
        member_page_bytes=16,
        locator_page_bytes=24,
        locator_owner_span=2,
    )
    coordinate = audit.V4SnapshotMapCoordinate(
        object_kind,
        0,
        0,
        4,
        block_hash,
    )
    block = audit._V4PhysicalBlock(
        block_hash,
        object_kind,
        4,
        struct.pack("<IIII", 10, 20, 1, 2),
    )
    return manifest, coordinate, block


def _reader_for_shared_owner_page(monkeypatch):
    manifest, coordinate, block = _shared_owner_page_parts()
    reader = audit._V4PersistedGraphReader(
        object(),
        schema_name="mrf",
        snapshot_key=17,
        representation="direct_v1",
        budget=_ReadBudget(),
    )

    async def fake_manifest(_relation):
        return manifest

    async def fake_heavy(_manifest, _owner_keys):
        return {}

    async def fake_locators(_manifest, owner_keys):
        locator_by_owner = {0: (0, 2), 1: (2, 2)}
        return {
            owner_key: locator_by_owner[owner_key]
            for owner_key in set(owner_keys)
        }

    async def fake_coordinates(**_kwargs):
        return {(0, 0): coordinate}

    async def fake_blocks(**_kwargs):
        return {block.block_hash: block}

    monkeypatch.setattr(reader, "_manifest", fake_manifest)
    monkeypatch.setattr(reader, "_heavy_owners", fake_heavy)
    monkeypatch.setattr(reader, "_locators", fake_locators)
    monkeypatch.setattr(reader, "_map_coordinates", fake_coordinates)
    monkeypatch.setattr(reader, "_physical_blocks", fake_blocks)
    return reader


@pytest.mark.asyncio
async def test_audit_accepts_sorted_owner_spans_that_share_a_page(
    monkeypatch,
) -> None:
    """A valid member page may restart its key order at an owner boundary."""

    reader = _reader_for_shared_owner_page(monkeypatch)
    assert await reader.contains_edges(
        "set_groups_direct",
        ((0, 20), (0, 15), (1, 1), (1, 3)),
    ) == {
        (0, 15): False,
        (0, 20): True,
        (1, 1): True,
        (1, 3): False,
    }
