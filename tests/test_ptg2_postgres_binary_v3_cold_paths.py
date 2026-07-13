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
async def test_v3_sparse_counts_skip_page_reads_when_projection_is_absent(monkeypatch):
    serving_tables = _version_three_tables()
    monkeypatch.setattr(
        ptg2_serving,
        "has_shared_provider_pages_in_db",
        AsyncMock(return_value=False),
    )
    page_probe = AsyncMock(side_effect=AssertionError("page-less snapshot must not read page keys"))
    monkeypatch.setattr(ptg2_serving, "lookup_shared_provider_pages_from_db", page_probe)

    provider_counts = await ptg2_serving._version_three_provider_counts_for_keys(
        object(),
        serving_tables,
        (3,),
    )

    assert provider_counts is None
    page_probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_v3_explicit_npi_scope_resolves_dense_provider_keys(monkeypatch):
    group_id = "00000000000000000000000000000021"
    provider_set_id = "00000000000000000000000000000031"
    serving_tables = _version_three_tables()
    monkeypatch.setattr(
        ptg2_serving,
        "_shared_graph_members_for_id",
        AsyncMock(return_value=(group_id,)),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_sets_by_group",
        AsyncMock(return_value={group_id: (provider_set_id,)}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_set_keys_for_ids",
        AsyncMock(return_value={provider_set_id: 3}),
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
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=explicit_scope),
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
    )

    assert graph_result == projected_result
    assert rate_scope_probe.await_args.kwargs["provider_set_keys"] == (3,)


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
