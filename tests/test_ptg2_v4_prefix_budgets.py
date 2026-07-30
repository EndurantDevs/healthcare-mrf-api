# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Aggregate and per-owner V4 prefix budget contracts."""

from __future__ import annotations

import struct
from unittest.mock import AsyncMock

import pytest

from api import ptg2_v4_graph as graph
from api.ptg2_shared_blocks import PTG2SharedBlockError
from tests.ptg2_v4_provider_prefix_support import (
    Coordinate,
    HeavyPrefixHarness,
)


class _AggregateRegularPrefixHarness:
    def __init__(self) -> None:
        self.requested_object_kinds: list[str] = []

    async def root(self, *_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)

    async def manifest(self, *_args, **_kwargs):
        return graph.V4RelationManifest(
            snapshot_key=17,
            relation="pattern_groups",
            member_object_kind="v4_pattern_groups_members_v1",
            locator_object_kind="v4_pattern_groups_locators_v1",
            owner_base=0,
            owner_count=2,
            logical_member_count=8,
            vector_member_count=8,
            member_width=4,
            member_page_bytes=16,
            locator_page_bytes=24,
            locator_owner_span=2,
        )

    async def heavy_owners(self, *_args, **_kwargs):
        return {}

    async def coordinates(self, *_args, **kwargs):
        object_kind = kwargs["object_kind"]
        self.requested_object_kinds.append(object_kind)
        assert object_kind.endswith("locators_v1")
        return {0: Coordinate(0, b"l" * 32, 2)}

    async def blocks(self, *_args, **kwargs):
        object_kind = kwargs["object_kind"]
        return {
            b"l" * 32: graph._CachedPhysicalBlock(
                b"l" * 32,
                object_kind,
                2,
                struct.pack("<QIQI", 0, 4, 4, 4),
            )
        }


def _patch_regular_harness(monkeypatch, harness) -> None:
    monkeypatch.setattr(graph, "load_v4_graph_root", harness.root)
    monkeypatch.setattr(graph, "load_v4_relation_manifest", harness.manifest)
    monkeypatch.setattr(graph, "load_v4_heavy_owners", harness.heavy_owners)
    monkeypatch.setattr(graph, "_load_map_coordinates", harness.coordinates)
    monkeypatch.setattr(graph, "_load_physical_blocks", harness.blocks)


def _patch_heavy_harness(monkeypatch, harness) -> None:
    monkeypatch.setattr(graph, "load_v4_graph_root", harness.root)
    monkeypatch.setattr(graph, "load_v4_relation_manifest", harness.manifest)
    monkeypatch.setattr(graph, "load_v4_heavy_owners", harness.heavy_owners)
    monkeypatch.setattr(graph, "_load_map_coordinate_pairs", harness.coordinates)
    monkeypatch.setattr(graph, "_load_physical_blocks", harness.blocks)


@pytest.mark.asyncio
async def test_prefix_preserves_combined_limits_in_active_scope(
    monkeypatch,
) -> None:
    scoped_lookup = AsyncMock(return_value={0: (1,)})
    monkeypatch.setattr(graph, "_lookup_v4_relation_members_scoped", scoped_lookup)

    with graph.v4_graph_request_scope():
        observed = await graph.lookup_v4_relation_member_prefixes(
            object(),
            snapshot_key=17,
            relation="pattern_groups",
            owner_keys=(0,),
            schema_name="mrf",
            limit_per_owner=2,
            max_members=3,
        )

    assert observed == {0: (1,)}
    request = scoped_lookup.await_args.args[1]
    assert isinstance(request, graph._V4RelationLookupRequest)
    assert request.prefix_members_per_owner == 2
    assert request.max_members == 3


@pytest.mark.asyncio
async def test_prefix_rejects_aggregate_before_member_pages(monkeypatch) -> None:
    """Locator counts must reject overflow before member payload reads."""

    harness = _AggregateRegularPrefixHarness()
    _patch_regular_harness(monkeypatch, harness)

    with pytest.raises(PTG2SharedBlockError, match="exceeds max_members"):
        await graph.lookup_v4_relation_member_prefixes(
            object(),
            snapshot_key=17,
            relation="pattern_groups",
            owner_keys=(0, 1),
            schema_name="mrf",
            limit_per_owner=2,
            max_members=3,
        )

    assert harness.requested_object_kinds == ["v4_pattern_groups_locators_v1"]


@pytest.mark.asyncio
async def test_heavy_prefix_rejects_aggregate_before_fragments(monkeypatch) -> None:
    harness = HeavyPrefixHarness()
    _patch_heavy_harness(monkeypatch, harness)

    with pytest.raises(PTG2SharedBlockError, match="exceeds max_members"):
        await graph.lookup_v4_relation_member_prefixes(
            object(),
            snapshot_key=17,
            relation="pattern_groups",
            owner_keys=(0,),
            schema_name="mrf",
            limit_per_owner=3,
            max_members=2,
        )

    assert harness.requested_coordinate_pairs == []
    assert harness.requested_block_hashes == []
