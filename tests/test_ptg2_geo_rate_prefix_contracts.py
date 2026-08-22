# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Corruption, routing, and public paging contracts for geo rate prefixes."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables
from tests.test_ptg2_geo_rate_prefix import (
    _code_rows,
    _install_geo_prefix_reads,
    _production_tables,
    _rate_row,
    _tables,
)
from tests.test_ptg2_provider_set_geo_coverage import _G0289ServingHarness


@pytest.mark.parametrize("repeated_provider_count", (None, 2, False))
@pytest.mark.asyncio
async def test_geo_rate_prefix_rejects_changed_repeated_count(
    monkeypatch,
    repeated_provider_count,
):
    rate_rows = [_rate_row(rank) for rank in range(4)]
    repeated_rate_row = _rate_row(0)
    if repeated_provider_count is None:
        repeated_rate_row.pop("provider_count")
    else:
        repeated_rate_row["provider_count"] = repeated_provider_count
    rate_rows.extend([repeated_rate_row, *(_rate_row(rank) for rank in range(4, 7))])
    _install_geo_prefix_reads(monkeypatch, rate_rows, set())

    with pytest.raises(serving.PTG2ManifestArtifactError, match="provider.*count"):
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _tables(),
            code_rows=_code_rows(8),
            args={"zip5": "48201"},
            network_names=[],
            target_count=1,
            descending=False,
        )


@pytest.mark.parametrize("declared_count", (None, False, -1))
@pytest.mark.asyncio
async def test_geo_rate_prefix_rejects_invalid_declared_count(declared_count):
    with pytest.raises(serving.PTG2ManifestArtifactError, match="rate count"):
        await serving._select_geo_filtered_rate_prefix(
            object(),
            _tables(),
            code_rows=[{"code_key": 1, "rate_count": declared_count}],
            args={"zip5": "48201"},
            network_names=[],
            target_count=1,
            descending=False,
        )


@pytest.mark.asyncio
async def test_geo_rate_prefix_accepts_authenticated_empty_source():
    selection = await serving._select_geo_filtered_rate_prefix(
        object(),
        _tables(),
        code_rows=_code_rows(0),
        args={"zip5": "48201"},
        network_names=[],
        target_count=1,
        descending=False,
    )

    assert selection == serving._GeoRateSelection((), True)


@pytest.mark.parametrize(
    (
        "rate_page_rows",
        "maximum_rate_rows",
        "maximum_provider_sets",
        "graph_batches",
        "expected",
    ),
    (
        (64, 256, 64, 4, 256),
        (128, 512, 32, 4, 128),
        (16, 48, 64, 4, 48),
        (16, 512, 64, 4, 64),
        (64, 512, 64, 3, 192),
    ),
)
def test_geo_rate_prefix_derives_cumulative_set_cap_from_sealed_limits(
    rate_page_rows,
    maximum_rate_rows,
    maximum_provider_sets,
    graph_batches,
    expected,
):
    budget = serving._GeoRateSelectionBudget(
        serving._V4ProviderExpansionRequestCaps(
            rate_page_rows=rate_page_rows,
            maximum_rate_rows=maximum_rate_rows,
            maximum_provider_sets=maximum_provider_sets,
            maximum_graph_batches=graph_batches,
        ),
        maximum_candidate_members=100_000,
    )

    assert budget.maximum_geo_provider_sets == expected


def test_geo_rate_prefix_routes_only_v4_aggregate_cost_geo():
    query_args_by_name = {"zip5": "48201", "order_by": "rate"}
    tables = _tables()

    assert serving._uses_geo_rate_prefix_selection(
        tables,
        query_args_by_name,
        location_filter_requested=True,
        include_providers=False,
        price_filter_requested=False,
        direct_npi_filter_requested=False,
    )
    assert not serving._uses_geo_rate_prefix_selection(
        tables,
        query_args_by_name,
        location_filter_requested=True,
        include_providers=True,
        price_filter_requested=False,
        direct_npi_filter_requested=False,
    )
    assert not serving._uses_geo_rate_prefix_selection(
        strict_v3_tables(),
        query_args_by_name,
        location_filter_requested=True,
        include_providers=False,
        price_filter_requested=False,
        direct_npi_filter_requested=False,
    )


@pytest.mark.parametrize(
    ("is_descending", "expected_price_keys"),
    ((False, [7, 8, 9, None]), (True, [9, 8, 7, None])),
)
def test_cost_sort_tied_prices_follow_dense_prefix_direction(
    is_descending,
    expected_price_keys,
):
    provider_items = [
        {
            "prices": [{"negotiated_rate": "30.00"}],
            "_ptg_price_key": price_key,
        }
        for price_key in (8, None, 7, 9)
    ]

    ordered_items = sorted(
        provider_items,
        key=lambda item: serving._ptg2_cost_sort_key(
            item,
            is_descending=is_descending,
        ),
    )

    assert [item["_ptg_price_key"] for item in ordered_items] == expected_price_keys


@pytest.mark.parametrize(
    ("query_args_by_name", "route_flags_by_name"),
    (
        ({"zip5": "48201", "order_by": "rate"}, {"location_filter_requested": False}),
        ({"zip5": "48201", "order_by": "rate"}, {"price_filter_requested": True}),
        ({"zip5": "48201", "order_by": "rate"}, {"direct_npi_filter_requested": True}),
        ({"zip5": "48201", "order_by": "distance"}, {}),
        ({"zip5": "48201", "order_by": "rate", "npi": "TEST-NPI"}, {}),
    ),
)
def test_geo_rate_prefix_excludes_other_query_shapes(
    monkeypatch,
    query_args_by_name,
    route_flags_by_name,
):
    monkeypatch.setattr(
        serving,
        "_normalize_npi",
        lambda raw_npi: 101 if raw_npi == "TEST-NPI" else None,
    )
    default_flags_by_name = {
        "location_filter_requested": True,
        "include_providers": False,
        "price_filter_requested": False,
        "direct_npi_filter_requested": False,
    }
    default_flags_by_name.update(route_flags_by_name)

    assert not serving._uses_geo_rate_prefix_selection(
        _production_tables(),
        query_args_by_name,
        **default_flags_by_name,
    )


@pytest.mark.asyncio
async def test_g0289_geo_response_uses_prefix_lower_bound_and_stable_offsets(
    monkeypatch,
):
    harness = _G0289ServingHarness()
    harness.install(monkeypatch)
    ordered_rows = tuple(
        harness._rate_row(provider_set_key) for provider_set_key in (9, 8, 7)
    )
    selection = AsyncMock(return_value=serving._GeoRateSelection(ordered_rows, False))
    monkeypatch.setattr(
        serving,
        "_uses_geo_rate_prefix_selection",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(serving, "_select_geo_filtered_rate_prefix", selection)

    full_page = await harness.response(limit=2, offset=0)
    second_page = await harness.response(limit=1, offset=1)

    assert [
        response_item["prices"][0]["negotiated_rate"]
        for response_item in full_page["items"]
    ] == [5, 20]
    assert second_page["items"][0]["prices"][0]["negotiated_rate"] == 20
    assert full_page["pagination"] == {
        "total": 3,
        "total_is_exact": False,
        "total_lower_bound": 3,
        "limit": 2,
        "offset": 0,
        "page": 1,
        "has_more": True,
    }
    assert [call.kwargs["target_count"] for call in selection.await_args_list] == [
        3,
        3,
    ]


def _tied_prefix_selector(prefix_rows, order):
    async def select_prefix(*_args, **kwargs):
        assert kwargs["descending"] is (order == "desc")
        return serving._GeoRateSelection(
            prefix_rows[: kwargs["target_count"]],
            len(prefix_rows) < kwargs["target_count"],
        )

    return select_prefix


async def _tied_geo_response(
    harness,
    *,
    order,
    limit,
    offset,
    serving_tables=None,
):
    return await serving._search_manifest_serving_table(
        harness.session(),
        "ptg2:209901:synthetic",
        {
            "plan_id": "TEST-PLAN-001",
            "plan_market_type": "group",
            "code_system": "HCPCS",
            "code": "G0289",
            "zip5": "48201",
            "zip_radius_miles": 30,
            "order_by": "rate",
            "order": order,
            "include_providers": "false",
        },
        SimpleNamespace(limit=limit, offset=offset),
        serving_tables or strict_v3_tables(snapshot_id="ptg2:209901:synthetic"),
        "product_search",
    )


@pytest.mark.parametrize(
    ("order", "provider_set_order"),
    (("asc", (7, 8, 9)), ("desc", (9, 8, 7))),
)
@pytest.mark.asyncio
async def test_geo_rate_prefix_price_ties_keep_stable_offsets(
    monkeypatch,
    order,
    provider_set_order,
):
    """Keep tied dense-price prefixes stable as their requested window grows."""

    harness = _G0289ServingHarness()
    harness.rates_by_key = {7: "30.00", 8: "30.00", 9: "30.00"}
    harness.install(monkeypatch)
    prefix_rows_by_key = {
        provider_set_key: {
            **harness._rate_row(provider_set_key),
            "source_procedure_name": f"Synthetic rate {provider_set_key}",
        }
        for provider_set_key in provider_set_order
    }
    prefix_rows = tuple(prefix_rows_by_key.values())
    selection = AsyncMock(side_effect=_tied_prefix_selector(prefix_rows, order))
    monkeypatch.setattr(
        serving,
        "_uses_geo_rate_prefix_selection",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(serving, "_select_geo_filtered_rate_prefix", selection)

    first_page = await _tied_geo_response(harness, order=order, limit=1, offset=0)
    second_page = await _tied_geo_response(harness, order=order, limit=1, offset=1)
    third_page = await _tied_geo_response(harness, order=order, limit=1, offset=2)

    assert [
        page["items"][0]["procedure_name"]
        for page in (first_page, second_page, third_page)
    ] == [
        f"Synthetic rate {provider_set_key}" for provider_set_key in provider_set_order
    ]
    assert [call.kwargs["target_count"] for call in selection.await_args_list] == [
        2,
        3,
        4,
    ]


@pytest.mark.asyncio
async def test_g0289_geo_response_reports_exact_exhausted_total(monkeypatch):
    harness = _G0289ServingHarness()
    harness.install(monkeypatch)
    exhausted_rows = tuple(
        harness._rate_row(provider_set_key) for provider_set_key in (9, 8)
    )
    selection = AsyncMock(return_value=serving._GeoRateSelection(exhausted_rows, True))
    monkeypatch.setattr(
        serving,
        "_uses_geo_rate_prefix_selection",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(serving, "_select_geo_filtered_rate_prefix", selection)

    response = await harness.response(limit=4, offset=0)

    assert len(response["items"]) == 2
    assert response["pagination"] == {
        "total": 2,
        "total_is_exact": True,
        "total_lower_bound": 2,
        "limit": 4,
        "offset": 0,
        "page": 1,
        "has_more": False,
    }


@pytest.mark.asyncio
async def test_g0289_geo_response_reverses_oversized_candidate_sets(monkeypatch):
    harness = _G0289ServingHarness()
    harness._rate_row = lambda provider_set_key: {
        **_G0289ServingHarness._rate_row(harness, provider_set_key),
        "provider_count": 100_001,
    }
    harness.install(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_uses_geo_rate_prefix_selection",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(
            return_value=[
                harness._rate_row(provider_set_key)
                for provider_set_key in harness.provider_set_ids_by_key
            ]
        ),
    )

    response = await _tied_geo_response(
        harness,
        order="asc",
        limit=2,
        offset=0,
        serving_tables=_production_tables(),
    )

    assert [
        pricing_item["prices"][0]["negotiated_rate"]
        for pricing_item in response["items"]
    ] == [20, 30]
    assert response["pagination"]["total"] == 2
    assert response["pagination"]["total_is_exact"] is True
