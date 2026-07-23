# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_types import PTG2ServingTables


def _tables() -> PTG2ServingTables:
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        source_key="source",
    )


def _sealed_expansion_limits(_tables):
    return SimpleNamespace(
        target=201,
        provider_expansion_rate_page_rows=64,
        maximum_provider_expansion_rate_rows=256,
        maximum_provider_expansion_provider_sets=64,
        maximum_provider_expansion_graph_batches=64,
    )


def _patch_cost_scope_dependencies(monkeypatch):
    provider_set_id = "01" * 16
    serving_row_by_field = {
        "provider_set_global_id_128": provider_set_id,
        "serving_content_hash_128": "02" * 16,
        "reported_code_system": "CPT",
        "reported_code": "70553",
        "negotiation_arrangement": "FFS",
        "source_key": 0,
        "provider_count": 1,
        "_ptg_provider_set_key": 7,
    }
    merge_mock = AsyncMock(return_value=[serving_row_by_field])
    reverse_lookup_mock = AsyncMock(
        return_value={1_111_111_111: (7,)}
    )
    full_scope_mock = AsyncMock(
        return_value=(
            SimpleNamespace(provider_set_key=7),
            SimpleNamespace(provider_set_key=9),
        )
    )
    dependencies_by_name = {
        "_v4_hot_prefix_limits": _sealed_expansion_limits,
        "_merge_manifest_code_variant_rows": merge_mock,
        "_provider_npis_for_sets": AsyncMock(
            return_value={provider_set_id: (1_111_111_111,)}
        ),
        "_shared_forward_entries_for_code_rows": full_scope_mock,
        "_v4_sets_by_npi": reverse_lookup_mock,
        "_provider_set_ids_for_keys": AsyncMock(
            return_value={7: provider_set_id}
        ),
        "_selected_provider_rows_by_set": AsyncMock(
            return_value={provider_set_id: []}
        ),
    }
    for dependency_name, dependency in dependencies_by_name.items():
        monkeypatch.setattr(serving, dependency_name, dependency)
    return provider_set_id, merge_mock, reverse_lookup_mock, full_scope_mock


@pytest.mark.asyncio
async def test_cost_expansion_completes_selected_npi_inside_compact_cpt_scope(
    monkeypatch,
) -> None:
    """Use compact code keys to resolve every selected-NPI rate membership."""

    _, merge_mock, reverse_lookup_mock, full_scope_mock = (
        _patch_cost_scope_dependencies(monkeypatch)
    )

    selection = await serving._strict_cost_provider_expansion_selection(
        object(),
        _tables(),
        code_rows=[{"code_key": 4, "rate_count": 1}],
        args={"plan_id": "plan"},
        snapshot_id="snapshot",
        source_trace_set_hash=None,
        network_names=[],
        target_count=1,
        descending=False,
    )

    assert selection is not None
    assert full_scope_mock.await_count == 1
    assert reverse_lookup_mock.await_count == 1
    assert reverse_lookup_mock.await_args.kwargs[
        "allowed_provider_set_keys"
    ] == frozenset({7, 9})
    assert merge_mock.await_count == 2
    rank_call, completion_call = merge_mock.await_args_list
    assert rank_call.kwargs["provider_set_keys"] is None
    assert rank_call.kwargs["limit"] == 1
    assert rank_call.kwargs["offset"] == 0
    assert completion_call.kwargs["provider_set_keys"] == frozenset({7})
    assert completion_call.kwargs["limit"] == 1
    assert completion_call.kwargs["offset"] == 0
