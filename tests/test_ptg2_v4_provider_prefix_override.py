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
        lambda *_args, **_kwargs: _async_value(
            {provider_set_id: provider_metadata}
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
            {
                npi_key: 1_000_000_000 + npi_key
                for npi_key in kwargs["npi_keys"]
            }
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
