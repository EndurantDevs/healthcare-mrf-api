# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Local-first provider expansion for oversized cost-ordered geo scopes."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_v4_graph import V4GraphRoot
from tests.test_ptg2_geo_provider_expansion import (
    _MATCHING_NPI,
    _PROVIDER_SET_ID,
    _geo_tables,
    _install_provider_enrichment,
    _rate_row,
    _select_geo,
)


_LOCAL_NPI = 1234567899
_LOCAL_PROVIDER_SET_IDS = tuple(f"{number:032x}" for number in range(1, 10))
_LOCAL_RATE_ROWS = tuple(
    _rate_row(provider_set_id, index, index)
    for index, provider_set_id in enumerate(_LOCAL_PROVIDER_SET_IDS, start=1)
)
_LOCAL_LOCATION_ROW = {
    "npi": _LOCAL_NPI,
    "distance_miles": 1.5,
    "location_hash": "entity_address_unified:local-nine",
    "state": "FL",
    "city": "Gainesville",
    "zip5": "32207",
    "address_payload": '{"first_line":"9 Local Way"}',
}


async def _read_local_rate_rows(*_args, **kwargs):
    selected_rows = _LOCAL_RATE_ROWS
    if kwargs["provider_set_keys"] is not None:
        selected_keys = set(kwargs["provider_set_keys"])
        selected_rows = tuple(
            rate_row
            for rate_row in _LOCAL_RATE_ROWS
            if rate_row["_ptg_provider_set_key"] in selected_keys
        )
    offset = kwargs["offset"]
    return [dict(row) for row in selected_rows[offset : offset + kwargs["limit"]]]


def _read_local_location_rows(*_args, **kwargs):
    if _LOCAL_NPI not in (kwargs["candidate_npis"] or ()):
        return []
    return [dict(_LOCAL_LOCATION_ROW)]


async def _read_local_set_keys(_session, _tables, candidate_npis, **_kwargs):
    return {npi: (9,) for npi in candidate_npis}


def _install_local_after_cap(monkeypatch):
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(41, "direct_v1", b"r" * 32)),
    )
    tables = _geo_tables(
        provider_expansion_rate_page_rows=4,
        max_online_provider_expansion_rate_rows=8,
        max_online_provider_expansion_provider_sets=8,
        max_online_provider_expansion_graph_batches=8,
    )
    budget = serving._geo_rate_selection_budget(tables)
    rate_reads = AsyncMock(side_effect=_read_local_rate_rows)
    location_reads = AsyncMock(side_effect=_read_local_location_rows)
    graph_reads = AsyncMock(side_effect=_read_local_set_keys)
    forbidden_member_read = AsyncMock(
        side_effect=AssertionError("bounded local memberships must be reused")
    )
    forbidden_rate_completion = AsyncMock(
        side_effect=AssertionError("exhausted local rates must be reused")
    )
    monkeypatch.setattr(serving, "_geo_rate_selection_budget", lambda _tables: budget)
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", rate_reads)
    monkeypatch.setattr(serving, "_provider_npis_for_sets", forbidden_member_read)
    monkeypatch.setattr(
        serving,
        "_membership_npi_rows",
        AsyncMock(return_value=[{"npi": _LOCAL_NPI, "_ptg_source_exhausted": True}]),
    )
    monkeypatch.setattr(serving, "_membership_location_rows", location_reads)
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_reads)
    monkeypatch.setattr(
        serving,
        "lookup_shared_provider_code_intersections_from_db",
        AsyncMock(return_value={9: (7,)}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_keys",
        AsyncMock(return_value={9: _LOCAL_PROVIDER_SET_IDS[-1]}),
    )
    monkeypatch.setattr(serving, "_v4_direct_npi_memberships", forbidden_member_read)
    monkeypatch.setattr(
        serving,
        "_v4_pattern_completion_rows",
        forbidden_rate_completion,
    )
    _install_provider_enrichment(monkeypatch)
    return SimpleNamespace(
        tables=tables,
        budget=budget,
        rate_reads=rate_reads,
        location_reads=location_reads,
        graph_reads=graph_reads,
        forbidden_member_read=forbidden_member_read,
        forbidden_rate_completion=forbidden_rate_completion,
    )


@pytest.mark.asyncio
async def test_strict_geo_oversized_scope_finds_local_provider_after_rate_cap(
    monkeypatch,
):
    """Reverse local scope before reading an oversized national rate prefix."""

    fixture = _install_local_after_cap(monkeypatch)
    selection = await _select_geo(
        fixture.tables,
        rate_count=9,
        target_count=1,
        descending=False,
        request_args={
            "plan_id": "synthetic-plan",
            "zip5": "32207",
            "zip_radius_miles": 25,
        },
        source_trace_set_hash="synthetic-trace-set",
    )

    assert selection is not None
    assert selection.row_data == [_LOCAL_RATE_ROWS[-1]]
    assert selection.exhausted is False
    assert list(selection.rank_by_key) == [
        ("npi", str(_LOCAL_NPI), "CPT", "99213", "FFS", "0")
    ]
    providers = selection.providers_by_set[_LOCAL_PROVIDER_SET_IDS[-1]]
    assert providers[0]["location_hash"] == _LOCAL_LOCATION_ROW["location_hash"]
    assert fixture.budget.rate_rows == 1
    assert fixture.budget.caps.maximum_rate_rows - fixture.budget.rate_rows == 7
    assert fixture.budget.reverse_geo_scope == (
        (_LOCAL_NPI,),
        True,
        serving._ptg2_manifest_location_match_limit(),
    )
    assert all(
        call.kwargs["provider_set_keys"] is not None
        for call in fixture.rate_reads.await_args_list
    )
    scoped_read = fixture.rate_reads.await_args_list[0]
    assert scoped_read.kwargs["provider_set_keys"] == (9,)
    assert scoped_read.kwargs["limit"] == 8
    assert scoped_read.kwargs["source_trace_set_hash"] == "synthetic-trace-set"
    assert fixture.graph_reads.await_count == 1
    assert fixture.budget.reverse_provider_set_keys_by_npi == {_LOCAL_NPI: (9,)}
    fixture.forbidden_member_read.assert_not_awaited()
    fixture.forbidden_rate_completion.assert_not_awaited()


def _install_tied_local_scope(monkeypatch):
    provider_set_ids = tuple(f"{number:032x}" for number in range(9, 12))
    local_npis = (1234567891, 1234567892, 1234567893)
    rate_rows = [
        _rate_row(provider_set_ids[0], 9, 1),
        _rate_row(provider_set_ids[0], 9, 1),
        _rate_row(provider_set_ids[1], 10, 1),
        _rate_row(provider_set_ids[2], 11, 2),
    ]
    rate_rows[1]["serving_content_hash_128"] = "ff" * 16
    tables = _geo_tables(
        max_online_provider_expansion_rate_rows=8,
        max_online_provider_expansion_provider_sets=8,
    )
    budget = serving._geo_rate_selection_budget(tables)
    budget.reverse_geo_scope = (local_npis, True, 8)
    budget.reverse_provider_set_keys_by_npi = {
        npi: (provider_set_key,)
        for npi, provider_set_key in zip(local_npis, range(9, 12))
    }
    scoped_selection = AsyncMock(
        return_value=serving._GeoRateSelection(tuple(rate_rows), True)
    )
    unsafe_membership_read = AsyncMock(
        side_effect=AssertionError("bounded local memberships must be reused")
    )
    location_reads = AsyncMock(
        side_effect=lambda *_args, **kwargs: [
            {"npi": npi, "distance_miles": float(index)}
            for index, npi in enumerate(kwargs["candidate_npis"], start=1)
        ]
    )
    monkeypatch.setattr(serving, "_geo_rate_selection_budget", lambda _tables: budget)
    monkeypatch.setattr(serving, "_select_oversized_geo_rate_scope", scoped_selection)
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_selected_npis",
        unsafe_membership_read,
    )
    monkeypatch.setattr(serving, "_membership_location_rows", location_reads)
    monkeypatch.setattr(serving, "_v4_direct_npi_memberships", unsafe_membership_read)
    _install_provider_enrichment(monkeypatch)
    return SimpleNamespace(
        tables=tables,
        budget=budget,
        local_npis=local_npis,
        rate_rows=rate_rows,
        scoped_selection=scoped_selection,
        location_reads=location_reads,
        unsafe_membership_read=unsafe_membership_read,
    )


@pytest.mark.asyncio
async def test_strict_geo_oversized_scope_completes_ties_after_duplicate_rates(
    monkeypatch,
):
    """Rank the full exhausted local scope and retain a provider-page sentinel."""

    fixture = _install_tied_local_scope(monkeypatch)
    selection = await _select_geo(
        fixture.tables,
        rate_count=9,
        target_count=2,
        descending=False,
    )

    assert selection is not None
    assert selection.row_data == fixture.rate_rows
    assert selection.exhausted is False
    assert list(selection.rank_by_key) == [
        ("npi", str(fixture.local_npis[0]), "CPT", "99213", "FFS", "0"),
        ("npi", str(fixture.local_npis[1]), "CPT", "99213", "FFS", "0"),
    ]
    request = fixture.scoped_selection.await_args.args[2]
    assert request.target_count == 7
    assert fixture.location_reads.await_args.kwargs["candidate_npis"] == (
        fixture.local_npis[:2]
    )
    fixture.unsafe_membership_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_strict_geo_oversized_scope_rejects_unexhausted_local_rates(
    monkeypatch,
):
    """Keep wider local scopes typed as a narrowing request, not a 503."""

    tables = _geo_tables(
        max_online_provider_expansion_rate_rows=8,
        max_online_provider_expansion_provider_sets=8,
    )
    budget = serving._geo_rate_selection_budget(tables)
    budget.reverse_geo_scope = ((_MATCHING_NPI,), True, 8)
    scoped_selection = AsyncMock(
        return_value=serving._GeoRateSelection(
            (_rate_row(_PROVIDER_SET_ID, 1, 1),),
            False,
        )
    )
    membership_rows = AsyncMock()
    monkeypatch.setattr(serving, "_geo_rate_selection_budget", lambda _tables: budget)
    monkeypatch.setattr(serving, "_select_oversized_geo_rate_scope", scoped_selection)
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_selected_npis",
        membership_rows,
    )

    with pytest.raises(serving.PTG2LocationScopeError) as exc_info:
        await _select_geo(
            tables,
            rate_count=9,
            target_count=1,
            descending=False,
        )

    assert exc_info.value.error_code == "ptg2_location_scope_too_broad"
    assert "order_by=distance" in str(exc_info.value)
    assert scoped_selection.await_args.args[2].target_count == 7
    membership_rows.assert_not_awaited()
