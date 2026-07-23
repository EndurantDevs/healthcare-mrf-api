# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api import ptg2_v4_graph as graph
from api.ptg2_types import PTG2ServingTables


def _tables(*, uses_v4: bool) -> PTG2ServingTables:
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation=(
            "shared_blocks_v4" if uses_v4 else "shared_blocks_v3"
        ),
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout=(
            "packed_snapshot_maps_v4"
            if uses_v4
            else "dense_shared_blocks_v3"
        ),
        source_count=1,
    )


@pytest.mark.asyncio
async def test_public_search_counts_nested_v4_graph_work_once(
    monkeypatch,
) -> None:
    async def nested_search(*_args, **_kwargs):
        with graph.v4_graph_request_scope():
            with graph.v4_graph_request_scope():
                return {"items": []}

    monkeypatch.setattr(
        serving,
        "_search_manifest_serving_table",
        nested_search,
    )
    before = graph.v4_graph_metrics_snapshot()["request_count"]

    result = await serving.search_ptg2_serving_table(
        object(),
        "snapshot",
        {},
        SimpleNamespace(),
        serving_tables=_tables(uses_v4=True),
    )

    after = graph.v4_graph_metrics_snapshot()["request_count"]
    assert result == {"items": []}
    assert after == before + 1


@pytest.mark.asyncio
async def test_failed_public_v4_scope_resets_before_next_request(
    monkeypatch,
) -> None:
    should_fail_by_call = iter((True, False))

    async def nested_search(*_args, **_kwargs):
        with graph.v4_graph_request_scope():
            if next(should_fail_by_call):
                raise RuntimeError("failed lookup")
            return {"items": []}

    monkeypatch.setattr(
        serving,
        "_search_manifest_serving_table",
        nested_search,
    )
    before = graph.v4_graph_metrics_snapshot()["request_count"]
    with pytest.raises(RuntimeError, match="failed lookup"):
        await serving.search_ptg2_serving_table(
            object(),
            "snapshot",
            {},
            SimpleNamespace(),
            serving_tables=_tables(uses_v4=True),
        )
    assert await serving.search_ptg2_serving_table(
        object(),
        "snapshot",
        {},
        SimpleNamespace(),
        serving_tables=_tables(uses_v4=True),
    ) == {"items": []}

    after = graph.v4_graph_metrics_snapshot()["request_count"]
    assert after == before + 2


@pytest.mark.asyncio
async def test_v3_public_search_does_not_emit_v4_request_metric(
    monkeypatch,
) -> None:
    search = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(serving, "_search_manifest_serving_table", search)
    before = graph.v4_graph_metrics_snapshot()["request_count"]

    assert await serving.search_ptg2_serving_table(
        object(),
        "snapshot",
        {},
        SimpleNamespace(),
        serving_tables=_tables(uses_v4=False),
    ) == {"items": []}

    assert graph.v4_graph_metrics_snapshot()["request_count"] == before


@pytest.mark.asyncio
async def test_provider_procedure_entrypoint_owns_one_v4_scope(
    monkeypatch,
) -> None:
    async def nested_search(*_args, **_kwargs):
        with graph.v4_graph_request_scope():
            return {"items": []}

    monkeypatch.setattr(
        serving,
        "snapshot_serving_tables",
        AsyncMock(return_value=_tables(uses_v4=True)),
    )
    monkeypatch.setattr(
        serving,
        "_search_ptg2_manifest_provider_procedures",
        nested_search,
    )
    before = graph.v4_graph_metrics_snapshot()["request_count"]

    result = await serving._search_ptg2_provider_procedures_snapshot(
        object(),
        1_234_567_890,
        {},
        SimpleNamespace(),
        snapshot_id="snapshot",
    )

    assert result == {"items": []}
    assert graph.v4_graph_metrics_snapshot()["request_count"] == before + 1
