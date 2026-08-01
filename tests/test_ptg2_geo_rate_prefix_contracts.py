# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Corruption, routing, and public paging contracts for geo rate prefixes."""

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
