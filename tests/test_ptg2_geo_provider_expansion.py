# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact rate-first provider expansion for geographic pricing pages."""

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.test_ptg2_geo_rate_prefix import _production_tables, _tables


_PROVIDER_SET_ID = "11" * 16
_MATCHING_NPI = 1234567930


def _geo_tables(**limit_overrides):
    return replace(
        _tables(**limit_overrides),
        provider_graph_v4_inferred_taxonomy_candidates=(
            _production_tables().provider_graph_v4_inferred_taxonomy_candidates
        ),
    )


def _rate_row(provider_set_id, provider_set_key, price_key, provider_count=1):
    return {
        "provider_set_global_id_128": provider_set_id,
        "provider_count": provider_count,
        "serving_content_hash_128": f"{provider_set_key + 32:032x}",
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "FFS",
        "price_key": price_key,
        "source_key": 0,
        "_ptg_provider_set_key": provider_set_key,
    }


def _install_geo_completion(
    monkeypatch,
    *,
    provider_set_keys_by_npi,
    provider_set_id_by_key,
    completion_rows,
):
    reverse_memberships = AsyncMock(
        side_effect=lambda _session, _tables, selected_npis, **_kwargs: {
            npi: provider_set_keys_by_npi[npi] for npi in selected_npis
        }
    )
    completion = AsyncMock(return_value=(completion_rows, provider_set_id_by_key))
    monkeypatch.setattr(serving, "_v4_direct_npi_memberships", reverse_memberships)
    monkeypatch.setattr(
        serving,
        "_oversized_geo_code_provider_sets",
        AsyncMock(return_value=tuple(provider_set_id_by_key)),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_keys",
        AsyncMock(return_value=provider_set_id_by_key),
    )
    monkeypatch.setattr(serving, "_v4_pattern_completion_rows", completion)
    return reverse_memberships, completion


def _install_rate_scan(monkeypatch, rate_rows, npis_by_set, distances_by_npi):
    async def merge_rate_rows(*_args, **kwargs):
        selected_rows = rate_rows
        if kwargs["provider_set_keys"] is not None:
            selected_keys = set(kwargs["provider_set_keys"])
            selected_rows = [
                rate_row_by_field
                for rate_row_by_field in rate_rows
                if rate_row_by_field["_ptg_provider_set_key"] in selected_keys
            ]
        start = kwargs["offset"]
        return [
            dict(rate_row_by_field)
            for rate_row_by_field in selected_rows[
                start : start + kwargs["limit"]
            ]
        ]

    merge_rows = AsyncMock(side_effect=merge_rate_rows)
    member_rows = AsyncMock(
        side_effect=lambda _session, _tables, requested_ids, **_kwargs: {
            provider_set_id: npis_by_set[provider_set_id]
            for provider_set_id in requested_ids
        }
    )
    location_rows = AsyncMock(
        side_effect=lambda *_args, **kwargs: [
            {"npi": npi, "distance_miles": distances_by_npi[npi]}
            for npi in kwargs["candidate_npis"]
        ]
    )
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge_rows)
    monkeypatch.setattr(serving, "_provider_npis_for_sets", member_rows)
    monkeypatch.setattr(serving, "_membership_location_rows", location_rows)
    return merge_rows, member_rows


def _install_provider_enrichment(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_enriched_provider_rows_for_npis",
        AsyncMock(
            side_effect=lambda _session, *, npis, **_kwargs: [
                {"npi": npi, "provider_name": f"Provider {npi}"}
                for npi in npis
            ]
        ),
    )


async def _select_geo(
    tables,
    *,
    rate_count,
    target_count,
    descending,
    request_args=None,
    source_trace_set_hash=None,
):
    args_by_name = request_args or {
        "plan_id": "synthetic-plan",
        "zip5": "60611",
    }
    return await serving._strict_cost_provider_expansion_selection(
        object(),
        tables,
        code_rows=[{"code_key": 7, "rate_count": rate_count}],
        args=args_by_name,
        snapshot_id="synthetic-snapshot",
        source_trace_set_hash=source_trace_set_hash,
        network_names=[],
        target_count=target_count,
        descending=descending,
    )


@pytest.mark.asyncio
async def test_strict_geo_cost_rejects_unprovable_complete_set(monkeypatch):
    """Keep oversized rate membership typed and bounded instead of truncating it."""

    monkeypatch.setattr(serving, "_ptg2_manifest_location_match_limit", lambda: 1)
    with pytest.raises(serving.PTG2LocationScopeError) as exc_info:
        await serving._is_geo_provider_expansion_batch_loaded(
            object(),
            _production_tables(),
            [{"provider_set_global_id_128": _PROVIDER_SET_ID, "provider_count": 21}],
            {"zip5": "60611"},
            serving._geo_rate_selection_budget(_production_tables()),
            {},
            {},
        )

    assert exc_info.value.error_code == "ptg2_location_scope_too_broad"


@pytest.mark.asyncio
async def test_strict_geo_cost_rejects_local_rates_at_sealed_read_budget(monkeypatch):
    """Keep a cap-sized local scope typed without widening its rate read."""

    rate_rows = [dict(_rate_row(_PROVIDER_SET_ID, 1, 1, 0)) for _ in range(65)]
    merge_rows = AsyncMock(
        side_effect=lambda *_args, **kwargs: rate_rows[
            kwargs["offset"] : kwargs["offset"] + kwargs["limit"]
        ]
    )
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge_rows)
    monkeypatch.setattr(
        serving,
        "_membership_npi_rows",
        AsyncMock(
            return_value=[{"npi": _MATCHING_NPI, "_ptg_source_exhausted": True}]
        ),
    )
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(return_value={_MATCHING_NPI: (1,)}),
    )
    monkeypatch.setattr(
        serving,
        "lookup_shared_provider_code_intersections_from_db",
        AsyncMock(return_value={1: (7,)}),
    )
    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(return_value=[]),
    )

    with pytest.raises(serving.PTG2LocationScopeError) as exc_info:
        await _select_geo(
            _geo_tables(
                provider_expansion_rate_page_rows=64,
                max_online_provider_expansion_rate_rows=64,
            ),
            rate_count=65,
            target_count=64,
            descending=False,
        )

    assert exc_info.value.error_code == "ptg2_location_scope_too_broad"
    assert sum(call.kwargs["limit"] for call in merge_rows.await_args_list) == 64
    assert all(
        call.kwargs["provider_set_keys"] == (1,)
        for call in merge_rows.await_args_list
    )


@pytest.mark.asyncio
async def test_geo_set_loader_batches_multiple_sets_in_one_graph_read(monkeypatch):
    """One sealed set batch consumes one graph traversal, not one per set."""

    provider_set_ids = tuple(f"{number:032x}" for number in range(1, 7))
    npis_by_provider_set = {
        provider_set_id: (1234567800 + index,)
        for index, provider_set_id in enumerate(provider_set_ids, start=1)
    }
    member_rows = AsyncMock(
        side_effect=lambda _session, _tables, requested_ids, **_kwargs: {
            provider_set_id: npis_by_provider_set[provider_set_id]
            for provider_set_id in requested_ids
        }
    )
    local_npi = npis_by_provider_set[provider_set_ids[-1]][0]
    monkeypatch.setattr(serving, "_provider_npis_for_sets", member_rows)
    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(return_value=[{"npi": local_npi, "distance_miles": 1.0}]),
    )
    tables = _geo_tables(
        provider_expansion_rate_page_rows=6,
        max_online_provider_expansion_graph_batches=2,
    )
    budget = serving._geo_rate_selection_budget(tables)
    matched_npis_by_set = {}

    loaded = await serving._is_geo_provider_expansion_batch_loaded(
        object(),
        tables,
        [
            {"provider_set_global_id_128": provider_set_id, "provider_count": 1}
            for provider_set_id in provider_set_ids
        ],
        {"zip5": "60611"},
        budget,
        matched_npis_by_set,
        {},
    )

    assert loaded is True
    assert budget.graph_batches == member_rows.await_count == 1
    assert member_rows.await_args.args[2] == provider_set_ids
    assert matched_npis_by_set[provider_set_ids[-1]] == (local_npi,)


@pytest.mark.asyncio
async def test_strict_geo_ascending_completes_tie_and_stops_at_boundary(monkeypatch):
    """Complete an equal-price tie without expanding the later price key."""

    provider_set_ids = tuple(f"{number:032x}" for number in range(1, 4))
    npis_by_set = {
        provider_set_id: (1234567890 + index,)
        for index, provider_set_id in enumerate(provider_set_ids, start=1)
    }
    distances_by_npi = {
        provider_npis[0]: float(index)
        for index, provider_npis in enumerate(npis_by_set.values(), start=1)
    }
    rate_rows = [
        _rate_row(provider_set_id, index, 1 if index < 3 else 2)
        for index, provider_set_id in enumerate(provider_set_ids, start=1)
    ]
    merge_rows, member_rows = _install_rate_scan(
        monkeypatch, rate_rows, npis_by_set, distances_by_npi
    )
    _reverse_memberships, completion = _install_geo_completion(
        monkeypatch,
        provider_set_keys_by_npi={
            provider_npis[0]: (index,)
            for index, provider_npis in enumerate(npis_by_set.values(), start=1)
        },
        provider_set_id_by_key=dict(enumerate(provider_set_ids, start=1)),
        completion_rows=rate_rows,
    )
    _install_provider_enrichment(monkeypatch)

    selection = await _select_geo(
        _geo_tables(provider_expansion_rate_page_rows=1),
        rate_count=3,
        target_count=1,
        descending=False,
    )

    assert selection is not None
    assert selection.exhausted is True
    assert {int(key[1]) for key in selection.rank_by_key} == {
        npis_by_set[provider_set_ids[0]][0],
        npis_by_set[provider_set_ids[1]][0],
    }
    requested_provider_set_ids = {
        provider_set_id
        for call in member_rows.await_args_list
        for provider_set_id in call.args[2]
    }
    assert provider_set_ids[2] not in requested_provider_set_ids
    prefix_calls = [
        call
        for call in merge_rows.await_args_list
        if call.kwargs["provider_set_keys"] is None
    ]
    assert [call.kwargs["offset"] for call in prefix_calls] == [0, 1, 2]
    completion_request = completion.await_args.args[2]
    assert len(completion_request.prefix_rows) == len(rate_rows)


@pytest.mark.asyncio
async def test_exhausted_geo_prefix_reuses_memberships_at_graph_cap(monkeypatch):
    """Complete an exhausted exact prefix without another graph traversal."""

    rate_row = _rate_row(_PROVIDER_SET_ID, 1, 1)
    _install_rate_scan(
        monkeypatch,
        [rate_row],
        {_PROVIDER_SET_ID: (_MATCHING_NPI,)},
        {_MATCHING_NPI: 1.0},
    )
    reverse_memberships = AsyncMock()
    completion_rows = AsyncMock()
    monkeypatch.setattr(
        serving, "_v4_direct_npi_memberships", reverse_memberships
    )
    monkeypatch.setattr(
        serving, "_v4_pattern_completion_rows", completion_rows
    )
    _install_provider_enrichment(monkeypatch)

    selection = await _select_geo(
        _geo_tables(max_online_provider_expansion_graph_batches=1),
        rate_count=1,
        target_count=1,
        descending=False,
    )

    assert selection is not None
    assert selection.exhausted is True
    assert list(selection.rank_by_key) == [
        ("npi", str(_MATCHING_NPI), "CPT", "99213", "FFS", "0")
    ]
    reverse_memberships.assert_not_awaited()
    completion_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_strict_geo_descending_scans_to_source_exhaustion(monkeypatch):
    """Descending provider minima remain exact by scanning every price page."""

    provider_set_ids = tuple(f"{number:032x}" for number in range(1, 4))
    npis_by_set = {
        provider_set_id: (1234567890 + index,)
        for index, provider_set_id in enumerate(provider_set_ids, start=1)
    }
    distances_by_npi = {provider_npis[0]: 1.0 for provider_npis in npis_by_set.values()}
    rate_rows = [
        _rate_row(provider_set_id, index, 4 - index)
        for index, provider_set_id in enumerate(provider_set_ids, start=1)
    ]
    merge_rows, _member_rows = _install_rate_scan(
        monkeypatch, rate_rows, npis_by_set, distances_by_npi
    )
    _install_geo_completion(
        monkeypatch,
        provider_set_keys_by_npi={
            provider_npis[0]: (index,)
            for index, provider_npis in enumerate(npis_by_set.values(), start=1)
        },
        provider_set_id_by_key=dict(enumerate(provider_set_ids, start=1)),
        completion_rows=rate_rows,
    )
    _install_provider_enrichment(monkeypatch)

    selection = await _select_geo(
        _geo_tables(
            provider_expansion_rate_page_rows=1,
            max_online_provider_expansion_graph_batches=4,
        ),
        rate_count=3,
        target_count=1,
        descending=True,
    )

    assert selection is not None and selection.exhausted is True
    prefix_calls = [
        call
        for call in merge_rows.await_args_list
        if call.kwargs["provider_set_keys"] is None
    ]
    assert [call.kwargs["offset"] for call in prefix_calls] == [0, 1, 2]
    assert len({id(call.kwargs["scan_budget"]) for call in prefix_calls}) == 1


@pytest.mark.asyncio
async def test_strict_geo_completes_full_membership_and_keeps_location(monkeypatch):
    """Keep a local member beyond the old prefix with its indexed geo evidence."""

    member_npis = tuple(range(1234567891, _MATCHING_NPI + 1))
    rate_row_by_field = _rate_row(_PROVIDER_SET_ID, 3, 1, len(member_npis))
    member_rows = AsyncMock(return_value={_PROVIDER_SET_ID: member_npis})
    matched_location_by_field = {
        "npi": _MATCHING_NPI,
        "distance_miles": 2.5,
        "location_hash": "entity_address_unified:matched-location",
        "state": "IL",
        "city": "Chicago",
        "zip5": "60611",
        "address_payload": '{"first_line":"1 Test Way"}',
    }
    location_rows = AsyncMock(return_value=[matched_location_by_field])
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=[dict(rate_row_by_field)]),
    )
    monkeypatch.setattr(serving, "_provider_npis_for_sets", member_rows)
    monkeypatch.setattr(serving, "_membership_location_rows", location_rows)
    full_scope_read = AsyncMock(side_effect=AssertionError("full scope must stay unread"))
    monkeypatch.setattr(serving, "_shared_forward_entries_for_code_rows", full_scope_read)
    _install_geo_completion(
        monkeypatch,
        provider_set_keys_by_npi={_MATCHING_NPI: (3,)},
        provider_set_id_by_key={3: _PROVIDER_SET_ID},
        completion_rows=[rate_row_by_field],
    )
    _install_provider_enrichment(monkeypatch)

    selection = await _select_geo(
        _production_tables(),
        rate_count=1,
        target_count=2,
        descending=False,
        request_args={
            "plan_id": "synthetic-plan",
            "lat": 41.8781,
            "long": -87.6298,
            "radius_miles": 25,
        },
    )

    provider = selection.providers_by_set[_PROVIDER_SET_ID][0]
    assert list(selection.rank_by_key) == [
        ("npi", str(_MATCHING_NPI), "CPT", "99213", "FFS", "0")
    ]
    assert provider["distance_miles"] == 2.5
    assert provider["location_hash"] == "entity_address_unified:matched-location"
    assert member_rows.await_args.kwargs["limit_per_set"] == len(member_npis) + 1
    assert location_rows.await_args.kwargs["candidate_npis"] == member_npis
    full_scope_read.assert_not_awaited()


def test_unified_taxonomy_location_scan_has_fast_and_exact_fallback():
    """Use the composite GiST branch without dropping stale taxonomy arrays."""

    parameter_map = {}
    taxonomy_index_sql = serving._membership_taxonomy_index_sql(
        {"taxonomy_codes": ["207Q00000X"]},
        parameter_map,
    )
    context = serving._MembershipLocationQuery(
        address_table="mrf.entity_address_unified",
        npi_scope_table="mrf.ptg2_v4_npi_scope",
        filter_sql="npi_scope.snapshot_key = :shared_snapshot_key AND canonical_taxonomy",
        parameter_map=parameter_map,
        distance_sql="distance_expression",
        knn_order_sql=None,
        address_assurance_sql="assured_address",
        taxonomy_index_sql=taxonomy_index_sql,
    )

    location_sql = serving._membership_location_sql(context, limit=5, offset=0)

    assert parameter_map["membership_index_taxonomy_codes"] == ["207Q00000X"]
    assert location_sql.count("addr.taxonomy_array && ARRAY(") == 2
    assert "AND addr.taxonomy_array && ARRAY(" in location_sql
    assert ") IS NOT TRUE" in location_sql
    assert location_sql.count("canonical_taxonomy") == 2
