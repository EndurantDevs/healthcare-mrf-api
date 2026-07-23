# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import struct

import pytest

from api import ptg2_v4_graph as graph
from api.ptg2_shared_blocks import PTG2SharedBlockError
from tests.ptg2_v4_provider_prefix_support import Coordinate


class _OrderedPrefixHarness:
    def __init__(self, members: tuple[int, ...]) -> None:
        self.members = members

    async def root(self, *_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)

    async def manifest(self, *_args, **_kwargs):
        return graph.V4RelationManifest(
            snapshot_key=17,
            relation="set_npi_prefix_override",
            member_object_kind="v4_set_npi_prefix_override_members_v1",
            locator_object_kind="v4_set_npi_prefix_override_locators_v1",
            owner_base=4,
            owner_count=1,
            logical_member_count=len(self.members),
            vector_member_count=len(self.members),
            member_width=4,
            member_page_bytes=16,
            locator_page_bytes=12,
            locator_owner_span=1,
        )

    async def heavy_owners(self, *_args, **_kwargs):
        return {}

    async def coordinates(self, *_args, **kwargs):
        object_kind = kwargs["object_kind"]
        if object_kind.endswith("locators_v1"):
            return {4: Coordinate(4, b"l" * 32, 1)}
        return {0: Coordinate(0, b"m" * 32, len(self.members))}

    async def blocks(self, *_args, **kwargs):
        object_kind = kwargs["object_kind"]
        if object_kind.endswith("locators_v1"):
            return {
                b"l" * 32: graph._CachedPhysicalBlock(
                    b"l" * 32,
                    object_kind,
                    1,
                    struct.pack("<QI", 0, len(self.members)),
                )
            }
        return {
            b"m" * 32: graph._CachedPhysicalBlock(
                b"m" * 32,
                object_kind,
                len(self.members),
                struct.pack(
                    f"<{len(self.members)}I",
                    *self.members,
                ),
            )
        }


def _patch_ordered_harness(monkeypatch, members: tuple[int, ...]) -> None:
    harness = _OrderedPrefixHarness(members)
    monkeypatch.setattr(graph, "load_v4_graph_root", harness.root)
    monkeypatch.setattr(graph, "load_v4_relation_manifest", harness.manifest)
    monkeypatch.setattr(graph, "load_v4_heavy_owners", harness.heavy_owners)
    monkeypatch.setattr(graph, "_load_map_coordinates", harness.coordinates)
    monkeypatch.setattr(graph, "_load_physical_blocks", harness.blocks)


@pytest.mark.asyncio
async def test_ordered_prefix_wire_read_preserves_group_first_order(
    monkeypatch,
) -> None:
    _patch_ordered_harness(monkeypatch, (9, 2, 7))

    observed = await graph.lookup_v4_ordered_npi_prefix_overrides(
        object(),
        snapshot_key=17,
        provider_set_keys=(4,),
        schema_name="mrf",
        max_members=3,
    )

    assert observed == {4: (9, 2, 7)}


@pytest.mark.asyncio
async def test_ordered_prefix_wire_read_rejects_duplicate_members(
    monkeypatch,
) -> None:
    _patch_ordered_harness(monkeypatch, (9, 2, 9))

    with pytest.raises(PTG2SharedBlockError, match="not unique"):
        await graph.lookup_v4_ordered_npi_prefix_overrides(
            object(),
            snapshot_key=17,
            provider_set_keys=(4,),
            schema_name="mrf",
            max_members=3,
        )


@pytest.mark.asyncio
async def test_ordered_prefix_relation_rejects_bitmap_owner(
    monkeypatch,
) -> None:
    harness = _OrderedPrefixHarness((9, 2, 7))
    bitmap_owner = graph.V4HeavyOwner(
        relation="set_npi_prefix_override",
        owner_key=4,
        object_kind="v4_set_npi_prefix_override_heavy_bitmap_v1",
        member_count=3,
        member_base=2,
        member_span=8,
        fragment_count=1,
    )
    monkeypatch.setattr(graph, "load_v4_graph_root", harness.root)
    monkeypatch.setattr(graph, "load_v4_relation_manifest", harness.manifest)
    monkeypatch.setattr(
        graph,
        "load_v4_heavy_owners",
        lambda *_args, **_kwargs: _async_value({4: bitmap_owner}),
    )

    with pytest.raises(PTG2SharedBlockError, match="cannot use bitmap"):
        await graph.lookup_v4_ordered_npi_prefix_overrides(
            object(),
            snapshot_key=17,
            provider_set_keys=(4,),
            schema_name="mrf",
            max_members=3,
        )


async def _async_value(value):
    return value
