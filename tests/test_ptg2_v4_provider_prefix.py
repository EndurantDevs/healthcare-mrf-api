# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import struct

import pytest

from api import ptg2_serving as serving
from api import ptg2_v4_graph as graph
from api.ptg2_shared_blocks import PTG2SharedBlockError
from api.ptg2_types import PTG2ServingTables
from tests.ptg2_v4_provider_prefix_support import (
    Coordinate,
    DuplicateNpiPrefixHarness,
    HeavyPrefixHarness,
    PatternServingHarness,
    RegularPrefixHarness,
    sealed_v4_hot_prefix,
)


def _v4_tables(*, patterns: object = 1024) -> PTG2ServingTables:
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        provider_graph_v4_hot_prefix=sealed_v4_hot_prefix(
            max_set_patterns_per_set=patterns,
        ),
    )


@pytest.mark.asyncio
async def test_v4_owner_prefix_reads_only_intersecting_vector_pages(
    monkeypatch,
) -> None:
    """A large owner must not pull its complete authenticated member vector."""

    harness = RegularPrefixHarness()
    monkeypatch.setattr(graph, "load_v4_graph_root", harness.root)
    monkeypatch.setattr(graph, "load_v4_relation_manifest", harness.manifest)
    monkeypatch.setattr(graph, "load_v4_heavy_owners", harness.heavy_owners)
    monkeypatch.setattr(graph, "_load_map_coordinates", harness.coordinates)
    monkeypatch.setattr(graph, "_load_physical_blocks", harness.blocks)

    assert await graph.lookup_v4_relation_member_prefixes(
        object(),
        snapshot_key=17,
        relation="pattern_groups",
        owner_keys=(0,),
        schema_name="mrf",
        limit_per_owner=5,
    ) == {0: (1, 2, 3, 4, 5)}
    assert harness.requested_pages == [
        ("v4_pattern_groups_locators_v1", (0,)),
        ("v4_pattern_groups_members_v1", (0, 4)),
    ]


@pytest.mark.asyncio
async def test_v4_owner_prefix_rejects_locator_beyond_manifest(
    monkeypatch,
) -> None:
    """Prefix mode keeps the full locator-to-manifest fail-closed check."""

    async def fake_root(*_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)

    async def fake_manifest(*_args, **_kwargs):
        return graph.V4RelationManifest(
            snapshot_key=17,
            relation="pattern_groups",
            member_object_kind="v4_pattern_groups_members_v1",
            locator_object_kind="v4_pattern_groups_locators_v1",
            owner_base=0,
            owner_count=1,
            logical_member_count=10,
            vector_member_count=10,
            member_width=4,
            member_page_bytes=16,
            locator_page_bytes=12,
            locator_owner_span=1,
        )

    async def fake_coordinates(*_args, **kwargs):
        assert kwargs["object_kind"].endswith("locators_v1")
        return {0: Coordinate(0, b"l" * 32, 1)}

    async def fake_blocks(*_args, **kwargs):
        object_kind = kwargs["object_kind"]
        return {
            b"l" * 32: graph._CachedPhysicalBlock(
                b"l" * 32,
                object_kind,
                1,
                struct.pack("<QI", 9, 2),
            )
        }

    monkeypatch.setattr(graph, "load_v4_graph_root", fake_root)
    monkeypatch.setattr(graph, "load_v4_relation_manifest", fake_manifest)
    monkeypatch.setattr(
        graph,
        "load_v4_heavy_owners",
        lambda *_args, **_kwargs: _async_value({}),
    )
    monkeypatch.setattr(graph, "_load_map_coordinates", fake_coordinates)
    monkeypatch.setattr(graph, "_load_physical_blocks", fake_blocks)

    with pytest.raises(PTG2SharedBlockError, match="exceeds its manifest"):
        await graph.lookup_v4_relation_member_prefixes(
            object(),
            snapshot_key=17,
            relation="pattern_groups",
            owner_keys=(0,),
            schema_name="mrf",
            limit_per_owner=1,
        )


async def _async_value(value):
    return value


def _patch_provider_metadata(
    monkeypatch,
    provider_set_key_by_id: dict[str, int],
    provider_count_by_key: dict[int, int],
) -> None:
    """Install one-query provider-set keys and exact counts."""

    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        lambda *_args, **_kwargs: _async_value(
            {
                provider_set_id: serving._ProviderSetGraphMetadata(
                    provider_set_key=provider_set_key,
                    provider_count=provider_count_by_key[provider_set_key],
                )
                for provider_set_id, provider_set_key in (
                    provider_set_key_by_id.items()
                )
            }
        ),
    )


@pytest.mark.asyncio
async def test_v4_heavy_owner_prefix_does_not_fetch_tail_fragments(
    monkeypatch,
) -> None:
    harness = HeavyPrefixHarness()
    monkeypatch.setattr(graph, "load_v4_graph_root", harness.root)
    monkeypatch.setattr(graph, "load_v4_relation_manifest", harness.manifest)
    monkeypatch.setattr(graph, "load_v4_heavy_owners", harness.heavy_owners)
    monkeypatch.setattr(graph, "_load_map_coordinate_pairs", harness.coordinates)
    monkeypatch.setattr(graph, "_load_physical_blocks", harness.blocks)

    assert await graph.lookup_v4_relation_member_prefixes(
        object(),
        snapshot_key=17,
        relation="pattern_groups",
        owner_keys=(0,),
        schema_name="mrf",
        limit_per_owner=3,
    ) == {0: (10, 11, 18)}
    assert harness.requested_coordinate_pairs == [(0, 0), (0, 1)]
    assert harness.requested_block_hashes == [b"a" * 32, b"b" * 32]
    assert b"c" * 32 not in harness.requested_block_hashes


@pytest.mark.asyncio
async def test_v4_pattern_provider_prefix_is_exact_without_full_group_expansion(
    monkeypatch,
) -> None:
    harness = PatternServingHarness()
    monkeypatch.setenv(
        "HLTHPRT_PTG2_V4_GRAPH_MAX_SET_PATTERNS_PER_SET",
        "1",
    )
    monkeypatch.setattr(serving, "_provider_set_keys_for_ids", harness.set_keys)
    monkeypatch.setattr(serving, "load_v4_graph_root", harness.root)
    monkeypatch.setattr(serving, "lookup_v4_relation_members", harness.full_lookup)
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        harness.prefix_lookup,
    )
    monkeypatch.setattr(serving, "v4_npi_values_for_keys", harness.npi_values)
    _patch_provider_metadata(
        monkeypatch,
        {harness.first_set: 1, harness.second_set: 2},
        {1: 3, 2: 3},
    )

    observed = await serving._provider_npi_member_ids_by_set(
        object(),
        _v4_tables(),
        (harness.first_set, harness.second_set),
        limit_per_set=3,
    )
    assert observed == {
        harness.first_set: tuple(
            serving._ptg2_npi_member_id(npi)
            for npi in (1_000_000_010, 1_000_000_011, 1_000_000_012)
        ),
        harness.second_set: tuple(
            serving._ptg2_npi_member_id(npi)
            for npi in (1_000_000_010, 1_000_000_011, 1_000_000_013)
        ),
    }
    assert harness.full_relations == []
    assert harness.prefix_calls == [
        ("set_patterns", (1, 2), 1025),
        ("pattern_groups", (7, 9), 4097),
        ("group_npis_exact", (1, 2, 3), 3),
    ]


@pytest.mark.asyncio
async def test_v4_provider_prefix_grows_group_window_after_duplicate_npis(
    monkeypatch,
) -> None:
    """Grow the group window until duplicate NPIs yield the requested prefix."""
    provider_set_id = "03" * 16
    harness = DuplicateNpiPrefixHarness()

    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        lambda *_args, **_kwargs: _async_value({provider_set_id: 1}),
    )
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        lambda *_args, **_kwargs: _async_value(
            graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)
        ),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        lambda *_args, **_kwargs: _async_value({1: (7,)}),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        harness.prefix_lookup,
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        lambda *_args, **kwargs: _async_value(
            {npi_key: 1_000_000_000 + npi_key for npi_key in kwargs["npi_keys"]}
        ),
    )
    _patch_provider_metadata(monkeypatch, {provider_set_id: 1}, {1: 2})

    observed = await serving._provider_npi_member_ids_by_set(
        object(),
        _v4_tables(),
        (provider_set_id,),
        limit_per_set=2,
    )
    assert observed[provider_set_id] == tuple(
        serving._ptg2_npi_member_id(npi)
        for npi in (1_000_000_010, 1_000_000_011)
    )
    assert harness.pattern_limits == [4097]
    assert harness.npi_group_batches == [(1, 2), (3,)]


@pytest.mark.asyncio
async def test_v4_provider_count_stops_duplicate_pattern_tail(
    monkeypatch,
) -> None:
    """Stop after the exact one-provider target instead of scanning duplicates."""

    provider_set_id = "09" * 16
    harness = DuplicateNpiPrefixHarness()
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        lambda *_args, **_kwargs: _async_value({provider_set_id: 1}),
    )
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        lambda *_args, **_kwargs: _async_value(
            graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)
        ),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        harness.prefix_lookup,
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        lambda *_args, **kwargs: _async_value(
            {
                npi_key: 1_000_000_000 + npi_key
                for npi_key in kwargs["npi_keys"]
            }
        ),
    )
    _patch_provider_metadata(monkeypatch, {provider_set_id: 1}, {1: 1})

    observed = await serving._provider_npi_member_ids_by_set(
        object(),
        _v4_tables(),
        (provider_set_id,),
        limit_per_set=2,
    )

    assert observed[provider_set_id] == (
        serving._ptg2_npi_member_id(1_000_000_010),
    )
    assert harness.pattern_limits == [4097]
    assert harness.npi_group_batches == [(1,)]


@pytest.mark.asyncio
async def test_v4_zero_provider_count_skips_group_traversal(
    monkeypatch,
) -> None:
    """A declared empty set reads its first hop but no group or NPI owners."""

    provider_set_id = "0b" * 16
    relations: list[str] = []

    async def fake_prefix_lookup(*_args, **kwargs):
        relation = kwargs["relation"]
        relations.append(relation)
        if relation != "set_patterns":
            raise AssertionError(f"unexpected traversal relation: {relation}")
        return {1: (7,)}

    _patch_provider_metadata(monkeypatch, {provider_set_id: 1}, {1: 0})
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        lambda *_args, **_kwargs: _async_value(
            graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)
        ),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        fake_prefix_lookup,
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        lambda *_args, **_kwargs: _async_value({}),
    )

    observed = await serving._provider_npi_member_ids_by_set(
        object(),
        _v4_tables(),
        (provider_set_id,),
        limit_per_set=10,
    )

    assert observed == {provider_set_id: ()}
    assert relations == ["set_patterns"]


@pytest.mark.asyncio
async def test_v4_provider_count_above_graph_membership_fails_closed(
    monkeypatch,
) -> None:
    """Reject a provider count that cannot be satisfied by the exact graph."""

    provider_set_id = "0c" * 16

    async def fake_prefix_lookup(*_args, **kwargs):
        relation = kwargs["relation"]
        members_by_relation = {
            "set_patterns": {1: (7,)},
            "pattern_groups": {7: (1,)},
            "group_npis_exact": {1: (10,)},
        }
        return members_by_relation[relation]

    _patch_provider_metadata(monkeypatch, {provider_set_id: 1}, {1: 2})
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        lambda *_args, **_kwargs: _async_value(
            graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)
        ),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        fake_prefix_lookup,
    )

    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="provider count exceeds exact graph membership",
    ):
        await serving._provider_npi_member_ids_by_set(
            object(),
            _v4_tables(),
            (provider_set_id,),
            limit_per_set=10,
        )


@pytest.mark.parametrize("sealed_limit", (0, -1, "not-an-integer"))
def test_v4_set_pattern_degree_guard_rejects_invalid_sealed_limit(
    sealed_limit: object,
) -> None:
    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="sealed hot-prefix limits",
    ):
        serving._v4_hot_prefix_limits(_v4_tables(patterns=sealed_limit))


@pytest.mark.asyncio
async def test_v4_direct_provider_prefix_uses_direct_groups(
    monkeypatch,
) -> None:
    provider_set_id = "04" * 16
    relations: list[str] = []

    async def fake_prefix_lookup(*_args, **kwargs):
        relation = kwargs["relation"]
        relations.append(relation)
        if relation == "set_groups_direct":
            return {5: (2,)}
        return {2: (10,)}

    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        lambda *_args, **_kwargs: _async_value({provider_set_id: 5}),
    )
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        lambda *_args, **_kwargs: _async_value(
            graph.V4GraphRoot(17, "direct_v1", b"r" * 32)
        ),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        fake_prefix_lookup,
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        lambda *_args, **_kwargs: _async_value({10: 1_000_000_010}),
    )
    _patch_provider_metadata(monkeypatch, {provider_set_id: 5}, {5: 1})

    observed = await serving._provider_npi_member_ids_by_set(
        object(),
        _v4_tables(),
        (provider_set_id,),
        limit_per_set=1,
    )
    assert observed[provider_set_id] == (
        serving._ptg2_npi_member_id(1_000_000_010),
    )
    assert relations == ["set_groups_direct", "group_npis_exact"]
