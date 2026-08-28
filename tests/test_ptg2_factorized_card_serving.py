# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Release-wide response tests for factorized provider cards."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from unittest.mock import ANY, AsyncMock

import pytest

from api import ptg2_serving as serving
from api.plan_pricing_projection import PlanPricingProjectionUnavailable
from api.plan_pricing_projection_contract import (
    LEGACY_PROJECTION_CONTRACT,
    PROJECTION_CONTRACT,
)
from tests.ptg2_factorized_card_support import (
    card_pagination,
    factorized_selection,
)


def _card_args():
    """Return the explicit factorized provider-card request shape."""

    return {
        "view": "card",
        "include_providers": True,
        "code_system": "CPT",
        "code": "27447",
        "zip5": "60601",
    }


def _card_item(npi, minimum_rate):
    """Return one compact card after selected-NPI completion."""

    return {
        "npi": npi,
        "provider_name": f"Frozen provider {npi}",
        "entity_type_code": 1,
        "credential": "MD",
        "taxonomy_code": "207X00000X",
        "primary_specialty": "Orthopaedic Surgery",
        "classification": "Orthopaedic Surgery",
        "city": "Chicago",
        "state": "IL",
        "zip5": "60601",
        "minimum_negotiated_rate": minimum_rate,
        "maximum_negotiated_rate": minimum_rate + 5,
        "rate_count": 2,
    }


@pytest.mark.asyncio
async def test_search_pages_one_release_wide_candidate_completion(monkeypatch):
    """Global candidates complete once before pagination and keep totals."""

    candidate_selection = serving._FactorizedCardCandidateSelection(
        {
            101: Decimal("10"),
            102: Decimal("20"),
            103: Decimal("30"),
        },
        total_lower_bound=9,
        total_is_exact=False,
    )
    geo_lookup = AsyncMock(return_value=["60601", "60602"])
    candidate_lookup = AsyncMock(return_value=candidate_selection)
    completion_lookup = AsyncMock(
        return_value=[
            _card_item(101, 10),
            _card_item(102, 20),
            _card_item(103, 30),
        ]
    )
    monkeypatch.setattr(
        serving, "_plan_pricing_projection_geo_cells", geo_lookup
    )
    monkeypatch.setattr(
        serving, "_factorized_card_candidates", candidate_lookup
    )
    monkeypatch.setattr(
        serving,
        "_factorized_card_complete_selected_npis",
        completion_lookup,
    )

    response_by_field = await serving._search_factorized_plan_release_cards(
        object(),
        _card_args(),
        card_pagination(limit=1, offset=1),
        factorized_selection(binding_count=3),
    )

    assert [
        card_by_field["npi"]
        for card_by_field in response_by_field["items"]
    ] == [102]
    assert response_by_field["pagination"]["total"] == 9
    assert response_by_field["pagination"]["total_is_exact"] is False
    assert response_by_field["pagination"]["has_more"] is True
    assert response_by_field["query"]["projection_contract"] == (
        PROJECTION_CONTRACT
    )
    geo_lookup.assert_awaited_once_with(
        ANY,
        _card_args(),
        result_type="provider_cards",
    )
    assert candidate_lookup.await_args.args[-1] == 3
    assert completion_lookup.await_args.args[5] == (101, 102, 103)


@pytest.mark.asyncio
async def test_coordinate_radius_search_uses_frozen_geo_cells(monkeypatch):
    """Coordinates retain the same immutable geo-cell selection contract."""

    coordinate_args_by_name = {
        **_card_args(),
        "zip5": None,
        "lat": 41.88,
        "long": -87.63,
        "radius_miles": 25,
    }
    geo_lookup = AsyncMock(return_value=[])
    monkeypatch.setattr(
        serving, "_plan_pricing_projection_geo_cells", geo_lookup
    )
    session = AsyncMock()

    response_by_field = await serving._search_factorized_plan_release_cards(
        session,
        coordinate_args_by_name,
        card_pagination(),
        factorized_selection(),
    )

    assert response_by_field["items"] == []
    assert response_by_field["result_state"] == "no_match_in_radius"
    assert response_by_field["pagination"]["total_is_exact"] is True
    geo_lookup.assert_awaited_once_with(
        session,
        coordinate_args_by_name,
        result_type="provider_cards",
    )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_existing_geo_cell_without_rates_is_not_a_radius_miss(monkeypatch):
    """An exhausted empty profile keeps the established no-rate state."""

    monkeypatch.setattr(
        serving,
        "_plan_pricing_projection_geo_cells",
        AsyncMock(return_value=["60601"]),
    )
    monkeypatch.setattr(
        serving,
        "_factorized_card_candidates",
        AsyncMock(
            return_value=serving._FactorizedCardCandidateSelection(
                {}, 0, True
            )
        ),
    )
    response_by_field = await serving._search_factorized_plan_release_cards(
        AsyncMock(),
        _card_args(),
        card_pagination(),
        factorized_selection(),
    )

    assert response_by_field["items"] == []
    assert response_by_field["result_state"] == "no_matching_rates"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("projection_id", "contract"),
    [
        (None, PROJECTION_CONTRACT),
        ("f" * 64, "unknown-contract"),
    ],
)
async def test_search_rejects_unknown_or_missing_projection(
    projection_id,
    contract,
):
    """Unknown or absent factorized projection identity fails closed."""

    release_selection = replace(
        factorized_selection(),
        pricing_projection_id=projection_id,
        pricing_projection_contract=contract,
    )
    with pytest.raises(
        PlanPricingProjectionUnavailable,
        match="compatible factorized projection",
    ):
        await serving._search_factorized_plan_release_cards(
            object(),
            _card_args(),
            card_pagination(),
            release_selection,
        )


@pytest.mark.asyncio
async def test_v3_route_avoids_919_binding_fanout(monkeypatch):
    """A release with many bindings still performs one factorized read."""

    release_selection = factorized_selection(binding_count=919)
    factorized_reader = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(
        serving,
        "_search_factorized_plan_release_cards",
        factorized_reader,
    )
    monkeypatch.setattr(
        type(release_selection),
        "network_tables_by_snapshot",
        lambda _selection: pytest.fail("binding descriptors were expanded"),
    )

    response_by_field = await serving._search_plan_release_index(
        object(),
        _card_args(),
        card_pagination(),
        release_selection,
    )

    assert response_by_field == {"items": []}
    factorized_reader.assert_awaited_once()


@pytest.mark.asyncio
async def test_v2_card_and_v3_full_retain_existing_serving_lane(monkeypatch):
    """Legacy cards and every full view stay on established PTG serving."""

    existing_response_by_field = {
        "items": [],
        "pagination": {
            "total": 0,
            "limit": 2,
            "offset": 0,
            "page": 1,
            "has_more": False,
        },
        "query": {},
    }
    existing_reader = AsyncMock(return_value=existing_response_by_field)
    factorized_reader = AsyncMock(
        side_effect=AssertionError("factorized lane used")
    )
    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", existing_reader)
    monkeypatch.setattr(
        serving,
        "_search_factorized_plan_release_cards",
        factorized_reader,
    )

    v2_response_by_field = await serving._search_plan_release_index(
        object(),
        _card_args(),
        card_pagination(),
        factorized_selection(contract=LEGACY_PROJECTION_CONTRACT),
    )
    full_response_by_field = await serving._search_plan_release_index(
        object(),
        {**_card_args(), "view": "full"},
        card_pagination(),
        factorized_selection(),
    )

    assert v2_response_by_field["items"] == []
    assert full_response_by_field["items"] == []
    assert existing_reader.await_count == 2
    factorized_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_card_contract_fails_before_established_reader(
    monkeypatch,
):
    """A future card contract cannot silently fall into the full reader."""

    existing_reader = AsyncMock()
    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", existing_reader)
    release_selection = replace(
        factorized_selection(),
        pricing_projection_contract="unknown-contract",
    )

    with pytest.raises(
        PlanPricingProjectionUnavailable,
        match="projection contract is unsupported",
    ):
        await serving._search_plan_release_index(
            object(),
            _card_args(),
            card_pagination(),
            release_selection,
        )
    existing_reader.assert_not_awaited()
