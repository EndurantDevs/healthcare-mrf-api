# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import ptg2_serving as serving
from api import ptg2_v4_graph as graph
from api.ptg2_types import PTG2ServingTables
from tests.ptg2_v4_component_fallback_support import (
    ComponentDuplicateTailHarness,
    MixedComponentPrefixHarness,
)
from tests.ptg2_v4_provider_prefix_support import (
    PathologicalSetPatternHarness,
    sealed_v4_hot_prefix,
)


def _v4_tables(
    *,
    patterns: object = 1024,
    components: object = 4096,
) -> PTG2ServingTables:
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        provider_graph_v4_hot_prefix=sealed_v4_hot_prefix(
            max_set_patterns_per_set=patterns,
            max_set_components_per_fallback_set=components,
        ),
    )


async def _async_value(value):
    return value


@pytest.mark.asyncio
async def test_overflow_reads_only_bounded_component_fallback(
    monkeypatch,
) -> None:
    """A 100k-pattern owner switches after bounded first-hop reads."""

    harness = PathologicalSetPatternHarness()
    provider_set_id = "05" * 16
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        lambda *_args, **_kwargs: _async_value(
            {
                provider_set_id: serving._ProviderSetGraphMetadata(
                    harness.provider_set_key,
                    2,
                )
            }
        ),
    )
    monkeypatch.setattr(serving, "load_v4_graph_root", harness.root)
    monkeypatch.setattr(graph, "load_v4_graph_root", harness.root)
    monkeypatch.setattr(graph, "load_v4_relation_manifest", harness.manifest)
    monkeypatch.setattr(graph, "load_v4_heavy_owners", harness.heavy_owners)
    monkeypatch.setattr(graph, "_load_map_coordinates", harness.coordinates)
    monkeypatch.setattr(graph, "_load_physical_blocks", harness.blocks)

    group_sources = await serving._load_v4_npi_group_sources(
        object(),
        _v4_tables(patterns=10, components=10),
        (provider_set_id,),
    )

    assert group_sources.pattern_keys_by_set == {}
    assert group_sources.component_keys_by_set == {
        harness.provider_set_key: (101, 102)
    }
    assert harness.requested_pages == [
        ("v4_set_patterns_locators_v1", (harness.provider_set_key,)),
        ("v4_set_patterns_members_v1", (0, 4, 8)),
        ("v4_set_components_locators_v1", (harness.provider_set_key,)),
        ("v4_set_components_members_v1", (0,)),
    ]
    assert harness.loaded_block_count == 6
    assert harness.loaded_raw_bytes == 80


@pytest.mark.asyncio
async def test_pattern_and_component_sets_mix_exactly(monkeypatch) -> None:
    """Resolve both first-hop layouts in one bounded request."""

    harness = MixedComponentPrefixHarness()
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        lambda *_args, **_kwargs: _async_value(
            {
                harness.normal_set_id: serving._ProviderSetGraphMetadata(1, 2),
                harness.overflow_set_id: serving._ProviderSetGraphMetadata(2, 2),
            }
        ),
    )
    monkeypatch.setattr(serving, "load_v4_graph_root", harness.root)
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        harness.prefix_lookup,
    )
    monkeypatch.setattr(serving, "v4_npi_values_for_keys", harness.npi_values)

    provider_npis_by_set = await serving._provider_npi_member_ids_by_set(
        object(),
        _v4_tables(patterns=2, components=2),
        (harness.normal_set_id, harness.overflow_set_id),
        limit_per_set=2,
    )

    assert provider_npis_by_set == {
        harness.normal_set_id: tuple(
            serving._ptg2_npi_member_id(npi)
            for npi in (1_000_000_010, 1_000_000_011)
        ),
        harness.overflow_set_id: tuple(
            serving._ptg2_npi_member_id(npi)
            for npi in (1_000_000_010, 1_000_000_012)
        ),
    }
    assert harness.calls == [
        ("set_patterns", (1, 2), 3),
        ("set_components", (2,), 3),
        ("pattern_groups", (7,), 4097),
        ("component_groups", (20, 21), 4097),
        ("group_npis_exact", (1, 2), 2),
        ("group_npis_exact", (3,), 2),
    ]
    assert sum(len(owner_keys) for _, owner_keys, _ in harness.calls) == 9


@pytest.mark.asyncio
async def test_provider_count_stops_duplicate_component_tail(
    monkeypatch,
) -> None:
    """Do not read later component groups after the exact NPI target."""

    harness = ComponentDuplicateTailHarness()
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        lambda *_args, **_kwargs: _async_value(
            {
                harness.provider_set_id: serving._ProviderSetGraphMetadata(1, 1)
            }
        ),
    )
    monkeypatch.setattr(serving, "load_v4_graph_root", harness.root)
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        harness.prefix_lookup,
    )
    monkeypatch.setattr(serving, "v4_npi_values_for_keys", harness.npi_values)

    observed = await serving._provider_npi_member_ids_by_set(
        object(),
        _v4_tables(patterns=1, components=2),
        (harness.provider_set_id,),
        limit_per_set=10,
    )

    assert observed[harness.provider_set_id] == (
        serving._ptg2_npi_member_id(1_000_000_010),
    )
    assert harness.calls == [
        ("set_patterns", (1,), 2),
        ("set_components", (1,), 3),
        ("component_groups", (20,), 4097),
        ("group_npis_exact", (1,), 1),
    ]


@pytest.mark.asyncio
async def test_component_fallback_rejects_after_cap_plus_one(
    monkeypatch,
) -> None:
    provider_set_id = "08" * 16
    calls: list[tuple[str, int]] = []

    async def fake_prefix_lookup(*_args, **kwargs):
        relation = kwargs["relation"]
        limit = int(kwargs["limit_per_owner"])
        calls.append((relation, limit))
        if relation == "set_patterns":
            return {1: (7, 8)}
        return {1: (20, 21, 22)}

    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        lambda *_args, **_kwargs: _async_value(
            {provider_set_id: serving._ProviderSetGraphMetadata(1, 1)}
        ),
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
        fake_prefix_lookup,
    )

    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="set-component fallback degree exceeds",
    ):
        await serving._load_v4_npi_group_sources(
            object(),
            _v4_tables(patterns=1, components=2),
            (provider_set_id,),
        )
    assert calls == [("set_patterns", 2), ("set_components", 3)]


@pytest.mark.parametrize("sealed_limit", (0, -1, "not-an-integer"))
def test_set_component_guard_rejects_invalid_sealed_limit(
    sealed_limit: object,
) -> None:
    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="sealed hot-prefix limits",
    ):
        serving._v4_hot_prefix_limits(
            _v4_tables(components=sealed_limit)
        )


@pytest.mark.asyncio
async def test_component_fallback_metric_counts_only_selected_sets(
    monkeypatch,
) -> None:
    """Expose a stable canary delta without changing public API payloads."""

    async def fake_prefix_lookup(*_args, **kwargs):
        owner_key = tuple(kwargs["owner_keys"])[0]
        if kwargs["relation"] == "set_patterns":
            return {owner_key: ((7,) if owner_key == 1 else (7, 8))}
        return {owner_key: (20,)}

    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        fake_prefix_lookup,
    )
    before = graph.v4_graph_metrics_snapshot()["component_fallback_sets"]
    with graph.v4_graph_request_scope():
        await serving._load_v4_pattern_set_group_sources(
            object(),
            snapshot_key=17,
            provider_set_keys=(1,),
            maximum_pattern_degree=1,
            maximum_component_degree=2,
        )
    after_ordinary = graph.v4_graph_metrics_snapshot()[
        "component_fallback_sets"
    ]
    with graph.v4_graph_request_scope():
        await serving._load_v4_pattern_set_group_sources(
            object(),
            snapshot_key=17,
            provider_set_keys=(2,),
            maximum_pattern_degree=1,
            maximum_component_degree=2,
        )
    after_fallback = graph.v4_graph_metrics_snapshot()[
        "component_fallback_sets"
    ]

    assert after_ordinary - before == 0
    assert after_fallback - after_ordinary == 1
