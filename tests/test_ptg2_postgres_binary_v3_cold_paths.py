# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving


def _version_three_tables():
    return ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v3",
        storage="manifest_snapshot",
        shared_snapshot_key=41,
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="dense_shared_blocks_v3",
        source_count=2,
        serving_table_layout="lean_provider_key_v1",
        price_dictionary_item_count=1024,
        price_dictionary_block_bytes=65536,
    )


@pytest.mark.asyncio
async def test_v3_filtered_forward_reuses_provider_page_counts(monkeypatch):
    sparse_count_probe = AsyncMock(return_value={3: 9})
    forward_probe = AsyncMock(return_value=(object(),))
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_provider_counts_for_keys",
        sparse_count_probe,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_lookup_shared_forward_rows",
        forward_probe,
    )
    serving_tables = _version_three_tables()
    session = object()

    rows = await ptg2_serving._shared_forward_entries_for_code_rows(
        session,
        serving_tables,
        ({"code_key": 7},),
        provider_set_keys=(3,),
    )

    assert len(rows) == 1
    sparse_count_probe.assert_awaited_once_with(session, serving_tables, (3,))
    assert forward_probe.await_args.kwargs == {
        "provider_set_keys": (3,),
        "provider_counts_by_key": {3: 9},
    }


@pytest.mark.asyncio
async def test_v3_sparse_counts_without_projection(monkeypatch):
    serving_tables = _version_three_tables()
    page_probe = AsyncMock(return_value=None)
    monkeypatch.setattr(ptg2_serving, "lookup_shared_provider_pages_from_db", page_probe)

    provider_counts = await ptg2_serving._version_three_provider_counts_for_keys(
        object(),
        serving_tables,
        (3,),
    )

    assert provider_counts is None
    page_probe.assert_awaited_once()


def _provider_page(
    provider_set_key: int,
    code_price_pairs: tuple[tuple[int, int], ...],
    *,
    total_row_count: int | None = None,
):
    entries = tuple(
        ptg2_serving.PTG2V3PageRecord(
            code_key=code_key,
            provider_set_key=provider_set_key,
            provider_count=7,
            price_key=price_key,
            source_key=0,
        )
        for code_key, price_key in code_price_pairs
    )
    return ptg2_serving.PTG2V3ProviderPage(
        entries=entries,
        total_row_count=total_row_count or len(entries),
    )


def test_v3_provider_page_order():
    pages_by_provider_set = {
        3: _provider_page(3, ((6, 9), (7, 8), (8, 4))),
        4: _provider_page(4, ((7, 5), (9, 3))),
    }

    selected = ptg2_serving._version_three_provider_code_entries(
        pages_by_provider_set,
        7,
    )

    assert selected is not None
    assert [(entry.price_key, entry.provider_set_key) for entry in selected] == [
        (5, 4),
        (8, 3),
    ]


@pytest.mark.parametrize("last_code_key", (6, 7))
def test_v3_truncated_page_fallback(
    last_code_key,
):
    pages_by_provider_set = {
        3: _provider_page(
            3,
            ((5, 1), (last_code_key, 2)),
            total_row_count=3,
        )
    }

    assert (
        ptg2_serving._version_three_provider_code_entries(
            pages_by_provider_set,
            7,
        )
        is None
    )


def test_v3_truncated_page_complete():
    pages_by_provider_set = {
        3: _provider_page(
            3,
            ((7, 2), (8, 1)),
            total_row_count=3,
        )
    }

    selected = ptg2_serving._version_three_provider_code_entries(
        pages_by_provider_set,
        7,
    )

    assert selected is not None
    assert [(entry.code_key, entry.price_key) for entry in selected] == [(7, 2)]


@pytest.mark.asyncio
async def test_v3_partial_page_fails(monkeypatch):
    page = _provider_page(3, ((7, 2),))
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_provider_pages_from_db",
        AsyncMock(return_value={3: page}),
    )

    with pytest.raises(
        ptg2_serving.PTG2ManifestArtifactError,
        match="missing a referenced provider set",
    ):
        await ptg2_serving._version_three_provider_pages_for_keys(
            object(),
            _version_three_tables(),
            (3, 4),
        )


@pytest.mark.asyncio
async def test_v3_variants_share_page(monkeypatch):
    page = _provider_page(3, ((7, 2), (8, 3)))
    page_probe = AsyncMock(return_value={3: page})
    row_probe = AsyncMock(return_value=[])
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_provider_pages_for_keys",
        page_probe,
    )
    monkeypatch.setattr(ptg2_serving, "_shared_rows_for_code", row_probe)

    rows = await ptg2_serving._merge_manifest_code_variant_rows(
        object(),
        _version_three_tables(),
        code_rows=[{"code_key": 7}, {"code_key": 8}],
        provider_set_keys=(3,),
        source_trace_set_hash=None,
        network_names=[],
        limit=None,
        offset=0,
    )

    assert rows == []
    page_probe.assert_awaited_once()
    assert row_probe.await_count == 2
    assert all(
        call.kwargs["provider_pages_by_key"] == {3: page}
        for call in row_probe.await_args_list
    )


@pytest.mark.asyncio
async def test_v3_large_scope_skips_pages(monkeypatch):
    page_probe = AsyncMock(
        side_effect=AssertionError("large provider scopes must use filtered code shards")
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_provider_pages_for_keys",
        page_probe,
    )

    read_scope = await ptg2_serving._version_three_provider_filter_scope(
        object(),
        _version_three_tables(),
        range(ptg2_serving._PTG2_VERSION_THREE_PAGE_PROVIDER_SET_LIMIT + 1),
        None,
        [],
        descending=False,
    )

    assert read_scope.provider_pages_by_key is None
    page_probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_v3_page_skips_forward_block(monkeypatch):
    page = _provider_page(3, ((7, 2), (8, 3)))
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_forward_page_ids",
        AsyncMock(
            return_value=(
                {3: "00000000000000000000000000000003"},
                {2: "00000000000000000000000000000002"},
            )
        ),
    )
    forward_probe = AsyncMock(
        side_effect=AssertionError("complete provider page must skip the full block")
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_lookup_shared_forward_rows",
        forward_probe,
    )

    serving_rows = await ptg2_serving._shared_rows_for_code(
        object(),
        _version_three_tables(),
        code_data={
            "code_key": 7,
            "reported_code_system": "CPT",
            "reported_code": "00001",
        },
        provider_set_keys=(3,),
        provider_pages_by_key={3: page},
        source_trace_set_hash=None,
        network_names=["Synthetic Network"],
    )

    assert serving_rows is not None
    assert len(serving_rows) == 1
    assert serving_rows[0]["provider_set_global_id_128"].endswith("03")
    assert serving_rows[0]["price_set_global_id_128"].endswith("02")
    assert serving_rows[0]["reported_code"] == "00001"
    forward_probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_v3_explicit_npi_scope_resolves_dense_provider_keys(monkeypatch):
    group_id = "00000000000000000000000000000021"
    serving_tables = _version_three_tables()
    graph_probe = AsyncMock(
        side_effect=(
            {1234567890: (2,)},
            {2: (3,)},
        )
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_graph_members_from_db",
        graph_probe,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_shared_provider_group_ids_for_keys",
        AsyncMock(return_value={2: group_id}),
    )

    scope = await ptg2_serving._version_three_explicit_npi_graph_scope(
        object(),
        serving_tables,
        {"npi": "1234567890"},
    )

    assert scope == ptg2_serving._ExplicitNpiGraphScope(
        npi=1234567890,
        group_ids=(group_id,),
        provider_set_keys=(3,),
    )
    assert [call.args[2] for call in graph_probe.await_args_list] == [
        ptg2_serving.PTG2_V3_GRAPH_NPI_TO_GROUP,
        ptg2_serving.PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
    ]
    assert [call.args[3] for call in graph_probe.await_args_list] == [
        (1234567890,),
        (2,),
    ]


@pytest.mark.asyncio
async def test_v3_provider_set_scope_uses_dense_graph_keys(monkeypatch):
    group_id = "00000000000000000000000000000021"
    serving_tables = _version_three_tables()
    graph_probe = AsyncMock(return_value={3: (2,)})
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_graph_members_from_db",
        graph_probe,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_shared_provider_group_ids_for_keys",
        AsyncMock(return_value={2: group_id}),
    )

    group_ids = await ptg2_serving._shared_group_ids_for_set_keys(
        object(),
        serving_tables,
        (3,),
    )

    assert group_ids == (group_id,)
    assert graph_probe.await_args.args[2:] == (
        ptg2_serving.PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
        (3,),
    )


@pytest.mark.asyncio
async def test_v3_exact_npi_graph_filters_code_before_location_scan(monkeypatch):
    group_id = "00000000000000000000000000000021"
    explicit_scope = ptg2_serving._ExplicitNpiGraphScope(
        npi=1234567890,
        group_ids=(group_id,),
        provider_set_keys=(3,),
    )
    rate_scope = ptg2_serving._ptg2_build_rate_scope((group_id,))
    rate_scope_probe = AsyncMock(return_value=rate_scope)
    location_rows = [{"npi": 1234567890}]
    projected_result = ({"provider-set"}, {"provider-set": location_rows})
    explicit_scope_probe = AsyncMock(
        side_effect=AssertionError("precomputed NPI scope must not be resolved twice")
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_explicit_npi_graph_scope",
        explicit_scope_probe,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_shared_rate_scope",
        rate_scope_probe,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_graph_location_candidates",
        AsyncMock(side_effect=AssertionError("exact NPI must not use broad graph traversal")),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_membership_location_rows",
        AsyncMock(return_value=location_rows),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_taxonomy_filtered_candidates",
        AsyncMock(side_effect=lambda _session, _args, candidates, _limit: candidates),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_project_graph_candidates",
        AsyncMock(return_value=projected_result),
    )
    serving_tables = _version_three_tables()

    graph_result = await ptg2_serving._graph_location_matches(
        object(),
        serving_tables,
        {"npi": "1234567890", "code": "99213", "code_system": "CPT"},
        candidate_limit=5,
        plan_id="plan",
        explicit_npi_scope=explicit_scope,
    )

    assert graph_result == projected_result
    assert rate_scope_probe.await_args.kwargs["provider_set_keys"] == (3,)
    explicit_scope_probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_strict_snapshot_requests_never_use_an_in_process_response_cache(
    monkeypatch,
):
    tables = _version_three_tables()
    database_search = AsyncMock(return_value={"items": [], "pagination": {}})
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", AsyncMock(return_value=tables))
    monkeypatch.setattr(ptg2_serving, "search_ptg2_serving_table", database_search)
    monkeypatch.setattr(
        ptg2_serving,
        "_enrich_ptg2_code_details",
        AsyncMock(side_effect=lambda _session, payload, _args: payload),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_shape_ptg2_response",
        lambda payload, _args: payload,
    )
    pagination = type("Pagination", (), {"limit": 25, "offset": 0})()

    for _request in range(2):
        assert await ptg2_serving._search_one_ptg2_snapshot(
            object(),
            "ptg2:strict",
            {"code_system": "CPT", "code": "99213"},
            pagination,
        ) == {"items": [], "pagination": {}}

    assert database_search.await_count == 2
