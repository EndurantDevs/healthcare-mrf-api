# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Route geographic provider searches through the bounded selector."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.test_ptg2_geo_rate_prefix import _production_tables, _tables as _v4_tables
from tests.test_ptg2_manifest_search_transitions import (
    _CODE_ROW,
    _NPI,
    _PROVIDER_ROW,
    _PROVIDER_SET_ID,
    _RATE_ROW,
    _install_base_dependencies,
    _query_args,
    _search,
)


def _bounded_geo_selection(nearer_npi: int) -> serving._ProviderExpansionSelection:
    selected_provider_by_field = {
        **_PROVIDER_ROW,
        "distance_miles": 5.0,
        "location_hash": "entity_address_unified:far-location",
    }
    nearer_provider_by_field = {
        **_PROVIDER_ROW,
        "npi": nearer_npi,
        "provider_name": "Nearer Provider",
        "distance_miles": 1.25,
        "location_hash": "entity_address_unified:near-location",
    }
    selected_key = ("npi", str(_NPI), "CPT", "99213", "FFS", "0")
    nearer_key = ("npi", str(nearer_npi), "CPT", "99213", "FFS", "0")
    return serving._ProviderExpansionSelection(
        row_data=[dict(_RATE_ROW)],
        providers_by_set={
            _PROVIDER_SET_ID: [
                selected_provider_by_field,
                nearer_provider_by_field,
            ]
        },
        rank_by_key={selected_key: 0, nearer_key: 1},
        exhausted=True,
    )


@pytest.mark.asyncio
async def test_manifest_cost_geo_uses_strict_rate_first_route(monkeypatch):
    """Keep inferred-taxonomy scopes on the strict local-first route."""

    _install_base_dependencies(monkeypatch)
    broad_location_lookup = AsyncMock(
        side_effect=AssertionError("cost-first search must skip broad geo traversal")
    )
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_location_provider_matches",
        broad_location_lookup,
    )
    nearer_npi = _NPI + 1
    selector = AsyncMock(return_value=_bounded_geo_selection(nearer_npi))
    monkeypatch.setattr(
        serving,
        "_strict_cost_provider_expansion_selection",
        selector,
    )
    serving_tables = _production_tables()
    rate_count = (
        serving._v4_hot_prefix_limits(
            serving_tables
        ).maximum_provider_expansion_rate_rows
        + 1
    )

    response, _session = await _search(
        args=_query_args(
            code="27447",
            include_providers=True,
            lat=41.8781,
            long=-87.6298,
            radius_miles=25,
            order_by="total_allowed_amount",
        ),
        code_rows=[
            {**_CODE_ROW, "reported_code": "27447", "rate_count": rate_count}
        ],
        serving_tables=serving_tables,
    )

    assert [provider["npi"] for provider in response["items"]] == [nearer_npi, _NPI]
    assert response["items"][0]["distance_miles"] == 1.25
    broad_location_lookup.assert_not_awaited()
    selector.assert_awaited_once()


@pytest.mark.asyncio
async def test_manifest_cost_geo_refuses_oversized_unfiltered_scope(monkeypatch):
    """Refuse broad unfiltered work before the reverse-geo database scan."""

    _install_base_dependencies(monkeypatch)
    selector = AsyncMock(
        side_effect=AssertionError("oversized unfiltered scope must be refused")
    )
    monkeypatch.setattr(serving, "_strict_cost_provider_expansion_selection", selector)
    location_lookup = AsyncMock()
    monkeypatch.setattr(
        serving, "_ptg2_manifest_location_provider_matches", location_lookup
    )
    serving_tables = _production_tables()
    rate_count = (
        serving._v4_hot_prefix_limits(
            serving_tables
        ).maximum_provider_expansion_rate_rows
        + 1
    )

    with pytest.raises(serving.PTG2LocationScopeError) as exc_info:
        await _search(
            args=_query_args(
                code="36415",
                include_providers=True,
                lat=41.8781,
                long=-87.6298,
                radius_miles=25,
                order_by="total_allowed_amount",
            ),
            code_rows=[
                {**_CODE_ROW, "reported_code": "36415", "rate_count": rate_count}
            ],
            serving_tables=serving_tables,
        )

    assert exc_info.value.error_code == "ptg2_location_scope_too_broad"
    assert "order_by=distance" in str(exc_info.value)
    selector.assert_not_awaited()
    location_lookup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_cost_order", [True, False])
async def test_manifest_geo_without_projection_keeps_location_first_route(
    monkeypatch,
    explicit_cost_order,
):
    """Keep the compatible location-first route for older V4 snapshots."""

    _install_base_dependencies(monkeypatch)
    location_lookup = AsyncMock(
        return_value=(
            {_PROVIDER_SET_ID},
            {_PROVIDER_SET_ID: [dict(_PROVIDER_ROW)]},
        )
    )
    strict_selector = AsyncMock(
        side_effect=AssertionError("optional projection route must stay disabled")
    )
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_location_provider_matches",
        location_lookup,
    )
    monkeypatch.setattr(
        serving,
        "_strict_cost_provider_expansion_selection",
        strict_selector,
    )
    serving_tables = _v4_tables()
    rate_count = 1 if explicit_cost_order else (
        serving._v4_hot_prefix_limits(
            serving_tables
        ).maximum_provider_expansion_rate_rows
        + 1
    )
    order_args_by_name = (
        {"order_by": "total_allowed_amount"} if explicit_cost_order else {}
    )

    response, _session = await _search(
        args=_query_args(include_providers=True, state="IL", **order_args_by_name),
        code_rows=[{**_CODE_ROW, "rate_count": rate_count}],
        serving_tables=serving_tables,
    )

    assert response["items"][0]["npi"] == _NPI
    location_lookup.assert_awaited_once()
    strict_selector.assert_not_awaited()
