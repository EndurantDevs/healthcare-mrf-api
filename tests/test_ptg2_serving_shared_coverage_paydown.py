# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables


@pytest.mark.asyncio
async def test_dense_npi_graph_intersects_rate_sets_without_id_hydration(
    monkeypatch,
):
    graph_lookup = AsyncMock(
        side_effect=(
            {11: (7, 8), 12: (8, 9)},
            {7: (101, 101), 8: (102, 103), 9: (104,)},
        )
    )
    monkeypatch.setattr(
        serving, "lookup_shared_graph_members_from_db", graph_lookup
    )
    hydrate_group_ids = AsyncMock(
        side_effect=AssertionError("dense location traversal hydrated group IDs")
    )
    monkeypatch.setattr(
        serving, "_shared_provider_group_ids_for_keys", hydrate_group_ids
    )

    matches = await serving._shared_provider_set_keys_by_npi(
        object(),
        strict_v3_tables(),
        (12, 11, 11),
        (101, 103, 999),
    )

    assert matches == {11: {101, 103}, 12: {103}}
    assert [call.args[2] for call in graph_lookup.await_args_list] == [
        serving.PTG2_V3_GRAPH_NPI_TO_GROUP,
        serving.PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
    ]
    assert graph_lookup.await_args_list[0].args[3] == (11, 12)
    assert graph_lookup.await_args_list[1].args[3] == (7, 8, 9)
    hydrate_group_ids.assert_not_awaited()


@pytest.mark.asyncio
async def test_dense_npi_graph_fails_closed_on_missing_reverse_owner(
    monkeypatch,
):
    monkeypatch.setattr(
        serving,
        "lookup_shared_graph_members_from_db",
        AsyncMock(side_effect=({11: (7, 8)}, {7: (101,)})),
    )

    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="missing a group-to-provider-set owner",
    ):
        await serving._shared_provider_set_keys_by_npi(
            object(),
            strict_v3_tables(),
            (11,),
            (101,),
        )


@pytest.mark.asyncio
async def test_append_rate_matched_locations_skips_seen_and_unpriced_sets(
    monkeypatch,
):
    rate_scope = frozenset({101})
    matched_rows = []
    provider_set_keys_by_npi = defaultdict(set)
    seen_npis = {10}
    graph_lookup = AsyncMock(
        return_value={
            11: {101},
        }
    )
    monkeypatch.setattr(
        serving, "_shared_provider_set_keys_by_npi", graph_lookup
    )

    added = await serving._append_rate_matched_locations(
        object(),
        strict_v3_tables(),
        rate_scope,
        [{"npi": 10}, {"npi": 11}, {"npi": 12}],
        matched_rows,
        provider_set_keys_by_npi,
        seen_npis,
    )

    assert added == 1
    assert matched_rows == [{"npi": 11}]
    assert provider_set_keys_by_npi == {11: {101}}
    assert seen_npis == {10, 11, 12}

    assert (
        await serving._append_rate_matched_locations(
            object(),
            strict_v3_tables(),
            rate_scope,
            [{"npi": 10}],
            matched_rows,
            provider_set_keys_by_npi,
            seen_npis,
        )
        == 0
    )


@pytest.mark.asyncio
async def test_taxonomy_filtered_candidates_handles_pre_filtered_empty_and_limited(
    monkeypatch,
):
    pre_filtered = serving._GraphLocationCandidates([], {}, taxonomy_filtered=True)
    assert (
        await serving._taxonomy_filtered_candidates(object(), {}, pre_filtered, 1)
        is pre_filtered
    )

    empty = await serving._taxonomy_filtered_candidates(
        object(), {}, serving._GraphLocationCandidates([], {}), 1
    )
    assert empty == serving._GraphLocationCandidates([], {}, taxonomy_filtered=True)

    taxonomy_filter = AsyncMock(return_value=(12, 13))
    monkeypatch.setattr(serving, "_filter_npis_by_taxonomy", taxonomy_filter)
    candidates = serving._GraphLocationCandidates(
        [{"npi": 11}, {"npi": 12}, {"npi": 13}],
        {11: {1}, 12: {2}, 13: {3}},
    )
    filtered = await serving._taxonomy_filtered_candidates(
        object(), {"provider_sex_code": "F"}, candidates, 1
    )

    assert filtered == serving._GraphLocationCandidates(
        [{"npi": 12}], {12: {2}}, taxonomy_filtered=True
    )


@pytest.mark.asyncio
async def test_shared_rows_for_code_shortcuts_and_page_fallbacks(monkeypatch):
    tables = strict_v3_tables()
    monkeypatch.setattr(
        serving, "_version_three_forward_lookup_hints", lambda _tables: {}
    )
    assert (
        await serving._shared_rows_for_code(
            object(),
            tables,
            code_data={},
            provider_set_keys=None,
            source_trace_set_hash=None,
            network_names=[],
        )
        is None
    )

    page_rows = AsyncMock(return_value=[{"page": True}])
    monkeypatch.setattr(serving, "_version_three_forward_page_rows", page_rows)
    assert await serving._shared_rows_for_code(
        object(),
        tables,
        code_data={"code_key": 7},
        provider_set_keys=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=2,
    ) == [{"page": True}]

    page_rows.return_value = None
    prefix_rows = AsyncMock(return_value=[])
    missing_block = AsyncMock()
    monkeypatch.setattr(serving, "lookup_code_prefix_rows_from_db", prefix_rows)
    monkeypatch.setattr(serving, "_raise_missing_v3_block", missing_block)
    assert (
        await serving._shared_rows_for_code(
            object(),
            tables,
            code_data={"code_key": 7},
            provider_set_keys=None,
            source_trace_set_hash=None,
            network_names=[],
            limit=2,
        )
        == []
    )
    missing_block.assert_awaited_once()


@pytest.mark.asyncio
async def test_shared_rows_for_code_projects_prefix_and_validates_dictionary(
    monkeypatch,
):
    tables = strict_v3_tables()
    monkeypatch.setattr(
        serving, "_version_three_forward_lookup_hints", lambda _tables: {}
    )
    forward_row = SimpleNamespace(provider_set_key=7)
    monkeypatch.setattr(
        serving, "_version_three_forward_page_rows", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        serving,
        "lookup_code_prefix_rows_from_db",
        AsyncMock(return_value=[forward_row]),
    )
    provider_ids = AsyncMock(return_value={7: "01" * 16})
    monkeypatch.setattr(serving, "_provider_set_ids_for_keys", provider_ids)
    monkeypatch.setattr(
        serving,
        "_shared_forward_response_row",
        lambda row, provider_id, *_args: {
            "provider_set_key": row.provider_set_key,
            "provider_set_id": provider_id,
        },
    )

    projected_rows = await serving._shared_rows_for_code(
        object(),
        tables,
        code_data={"code_key": 7},
        provider_set_keys=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=2,
    )
    assert projected_rows == [{"provider_set_key": 7, "provider_set_id": "01" * 16}]

    provider_ids.return_value = {}
    with pytest.raises(serving.PTG2ManifestArtifactError, match="prefix-referenced"):
        await serving._shared_rows_for_code(
            object(),
            tables,
            code_data={"code_key": 7},
            provider_set_keys=None,
            source_trace_set_hash=None,
            network_names=[],
            limit=2,
        )


@pytest.mark.asyncio
async def test_shared_rows_for_code_delegates_non_page_reads(monkeypatch):
    full_rows = AsyncMock(return_value=[{"full": True}])
    monkeypatch.setattr(serving, "_full_shared_code_rows", full_rows)
    provider_pages_by_key = {7: SimpleNamespace(entries=())}

    delegated_rows = await serving._shared_rows_for_code(
        object(),
        strict_v3_tables(),
        code_data={"code_key": 7},
        provider_set_keys=(7,),
        provider_pages_by_key=provider_pages_by_key,
        source_trace_set_hash="trace-a",
        network_names=["Coverage Network"],
        limit=None,
        offset=3,
        descending=True,
    )

    assert delegated_rows == [{"full": True}]
    full_rows.assert_awaited_once()
