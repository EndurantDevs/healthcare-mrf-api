# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import ptg2_serving as serving
from api import ptg2_v4_graph as graph
from tests.test_ptg2_v4_provider_prefix import (
    _async_value,
    _v4_tables,
)


def _patch_sparse_override(
    monkeypatch,
    provider_set_id: str,
    ordered_prefix: tuple[int, ...],
) -> None:
    provider_metadata = serving._ProviderSetGraphMetadata(
        provider_set_key=4,
        provider_count=3,
        prefix_member_count=3,
        prefix_member_digest=serving._v4_npi_prefix_digest(ordered_prefix),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        lambda *_args, **_kwargs: _async_value({provider_set_id: provider_metadata}),
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
        "lookup_v4_ordered_npi_prefix_overrides",
        lambda *_args, **_kwargs: _async_value({4: ordered_prefix}),
    )

    async def fail_source_lookup(*_args, **_kwargs):
        raise AssertionError("override owner must bypass factor relations")

    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        fail_source_lookup,
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        lambda *_args, **kwargs: _async_value(
            {npi_key: 1_000_000_000 + npi_key for npi_key in kwargs["npi_keys"]}
        ),
    )


@pytest.mark.asyncio
async def test_v4_sparse_override_preserves_order_and_metrics(
    monkeypatch,
) -> None:
    """Serve the stored group-first prefix without touching factor relations."""

    provider_set_id = "0b" * 16
    ordered_prefix = (9, 2, 7)
    _patch_sparse_override(monkeypatch, provider_set_id, ordered_prefix)

    metrics_before_by_field = graph.v4_graph_metrics_snapshot()
    provider_npis_by_set = await serving._provider_npi_member_ids_by_set(
        object(),
        _v4_tables(),
        (provider_set_id,),
        limit_per_set=3,
    )
    metrics_after_by_field = graph.v4_graph_metrics_snapshot()

    assert provider_npis_by_set[provider_set_id] == tuple(
        serving._ptg2_npi_member_id(1_000_000_000 + npi_key)
        for npi_key in ordered_prefix
    )
    assert (
        metrics_after_by_field["request_count"]
        == metrics_before_by_field["request_count"] + 1
    )
    assert (
        metrics_after_by_field["hot_prefix_requests"]
        == metrics_before_by_field["hot_prefix_requests"] + 1
    )
    assert (
        metrics_after_by_field["cold_exact_requests"]
        == metrics_before_by_field["cold_exact_requests"]
    )
    assert (
        metrics_after_by_field["npi_prefix_override_sets"]
        == metrics_before_by_field["npi_prefix_override_sets"] + 1
    )


def _complete_prefix_metadata(
    first_set: str,
    second_set: str,
    prefix_by_key: dict[int, tuple[int, ...]],
) -> dict[str, serving._ProviderSetGraphMetadata]:
    """Return exact metadata for two complete direct prefixes."""

    return {
        provider_set_id: serving._ProviderSetGraphMetadata(
            provider_set_key=provider_set_key,
            provider_count=2,
            prefix_member_count=2,
            prefix_member_digest=serving._v4_npi_prefix_digest(
                prefix_by_key[provider_set_key]
            ),
        )
        for provider_set_id, provider_set_key in (
            (first_set, 4),
            (second_set, 5),
        )
    }


def _patch_complete_direct_prefixes(
    monkeypatch,
) -> tuple[str, str, dict[int, tuple[int, ...]]]:
    """Patch complete direct-prefix reads without factor traversal."""

    first_set, second_set = "0c" * 16, "0d" * 16
    prefix_by_key = {4: (9, 2), 5: (7, 3)}
    metadata_by_set = _complete_prefix_metadata(
        first_set,
        second_set,
        prefix_by_key,
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        lambda *_args, **_kwargs: _async_value(metadata_by_set),
    )
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        lambda *_args, **_kwargs: _async_value(
            graph.V4GraphRoot(17, "direct_v1", b"r" * 32)
        ),
    )

    async def ordered_prefixes(*_args, **kwargs):
        assert tuple(kwargs["provider_set_keys"]) == (4, 5)
        assert kwargs["max_members"] == 4
        return prefix_by_key

    async def fail_relation_lookup(*_args, **_kwargs):
        raise AssertionError("complete direct prefixes must bypass graph traversal")

    monkeypatch.setattr(
        serving,
        "lookup_v4_ordered_npi_prefix_overrides",
        ordered_prefixes,
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        fail_relation_lookup,
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        lambda *_args, **kwargs: _async_value(
            {npi_key: 1_000_000_000 + npi_key for npi_key in kwargs["npi_keys"]}
        ),
    )
    return first_set, second_set, prefix_by_key


def _expected_direct_prefix_members(
    first_set: str,
    second_set: str,
    prefix_by_key: dict[int, tuple[int, ...]],
) -> dict[str, tuple[str, ...]]:
    """Return the public NPI member IDs expected for both synthetic sets."""

    return {
        provider_set_id: tuple(
            serving._ptg2_npi_member_id(1_000_000_000 + npi_key)
            for npi_key in prefix_by_key[provider_set_key]
        )
        for provider_set_id, provider_set_key in (
            (first_set, 4),
            (second_set, 5),
        )
    }


@pytest.mark.asyncio
async def test_v4_complete_direct_prefix_bypasses_group_traversal_for_all_sets(
    monkeypatch,
) -> None:
    """A new direct layout serves every bounded prefix from authenticated rows."""

    first_set, second_set, prefix_by_key = _patch_complete_direct_prefixes(monkeypatch)

    observed = await serving._provider_npi_member_ids_by_set(
        object(),
        _v4_tables(),
        (first_set, second_set),
        limit_per_set=2,
    )

    assert observed == _expected_direct_prefix_members(
        first_set,
        second_set,
        prefix_by_key,
    )
