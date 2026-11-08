"""Branch-focused provider procedure cost-level coverage."""

from __future__ import annotations

import json

import pytest
from sanic.exceptions import InvalidUsage, NotFound

from tests.test_pricing_api import FakeResult, make_request, pricing_module


def _cost_profile_row(*, specialty_key: str = "radiology"):
    return {
        "npi": 123,
        "year": 2024,
        "procedure_code": 123,
        "geography_scope": "national",
        "geography_value": "US",
        "specialty_key": specialty_key,
        "setting_key": "all",
        "claim_count": 20,
        "avg_submitted_charge": 50.0,
        "total_submitted_charge": 1000.0,
    }


def _cost_peer_row(*, specialty_key: str = "radiology"):
    return {
        "procedure_code": 123,
        "year": 2024,
        "geography_scope": "national",
        "geography_value": "US",
        "specialty_key": specialty_key,
        "setting_key": "all",
        "provider_count": 10,
        "p10": 10.0,
        "p20": 20.0,
        "p40": 40.0,
        "p50": 50.0,
        "p60": 60.0,
        "p80": 80.0,
        "p90": 90.0,
    }


@pytest.mark.asyncio
async def test_provider_cost_level_rejects_path_and_missing_tables():
    with pytest.raises(InvalidUsage, match="npi"):
        await pricing_module.get_provider_procedure_cost_level(
            make_request([]),
            "",
            "123",
        )

    with pytest.raises(NotFound, match="Cost profile data"):
        await pricing_module.get_provider_procedure_cost_level(
            make_request([FakeResult(scalar=None)]),
            "123",
            "123",
        )

    with pytest.raises(NotFound, match="Peer stats data"):
        await pricing_module.get_provider_procedure_cost_level(
            make_request(
                [
                    FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
                    FakeResult(scalar=None),
                ]
            ),
            "123",
            "123",
        )


@pytest.mark.asyncio
async def test_provider_cost_level_exhausts_specialty_profile_fallback():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
            FakeResult(scalar="mrf.pricing_procedure_peer_stats"),
            FakeResult(rows=[]),
            FakeResult(rows=[]),
        ],
        args={"year": "2024", "specialty": "radiology"},
    )

    with pytest.raises(NotFound, match="cost profile not found"):
        await pricing_module.get_provider_procedure_cost_level(
            request,
            "123",
            "123",
        )


@pytest.mark.asyncio
async def test_provider_cost_level_rejects_malformed_and_missing_peer(
    monkeypatch,
):
    monkeypatch.setattr(
        pricing_module,
        "_geography_candidates",
        lambda **_kwargs: (("", ""), ("national", "US"), ("national", "US")),
    )
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
            FakeResult(scalar="mrf.pricing_procedure_peer_stats"),
            FakeResult(rows=[_cost_profile_row()]),
            FakeResult(rows=[{"geography_scope": "", "geography_value": ""}]),
        ],
        args={"year": "2024"},
    )

    with pytest.raises(NotFound, match="Peer group is not available"):
        await pricing_module.get_provider_procedure_cost_level(
            request,
            "123",
            "123",
        )


def _cost_level_request(
    procedure_rows,
    catalog_rows,
    *,
    specialty_key="radiology",
):
    return make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
            FakeResult(scalar="mrf.pricing_procedure_peer_stats"),
            FakeResult(rows=[_cost_profile_row(specialty_key=specialty_key)]),
            FakeResult(rows=[_cost_peer_row(specialty_key=specialty_key or "__all__")]),
            FakeResult(rows=procedure_rows),
            FakeResult(rows=catalog_rows),
        ],
        args={"year": "2024"},
    )


@pytest.mark.asyncio
async def test_provider_cost_level_uses_catalog_and_empty_name_fallbacks():
    catalog_request = _cost_level_request(
        [],
        [
            {
                "display_name": "Synthetic catalog procedure",
                "short_description": None,
            }
        ],
    )
    catalog_response = await pricing_module.get_provider_procedure_cost_level(
        catalog_request,
        "123",
        "123",
    )
    catalog_by_field = json.loads(catalog_response.body)
    assert catalog_by_field["procedure"]["name"] == ("Synthetic catalog procedure")

    empty_name_request = _cost_level_request(
        [{"service_description": None, "reported_code": None}],
        [],
        specialty_key="",
    )
    empty_name_response = await pricing_module.get_provider_procedure_cost_level(
        empty_name_request,
        "123",
        "123",
    )
    empty_name_by_field = json.loads(empty_name_response.body)
    assert empty_name_by_field["procedure"]["name"] is None
    assert empty_name_by_field["procedure"]["reported_code"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["default", "static"])
async def test_requested_specialty_survives_profile_fallback(strategy):
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
            FakeResult(scalar="mrf.pricing_procedure_peer_stats"),
            FakeResult(rows=[]),
            FakeResult(rows=[_cost_profile_row(specialty_key="cardiology")]),
            FakeResult(rows=[_cost_peer_row(specialty_key="__all__")]),
            FakeResult(rows=[{"service_description": "Synthetic procedure", "reported_code": " a1234 "}]),
        ],
        args={"year": "2024", "specialty": " Radiology ", "cohort_strategy": strategy},
    )

    document = json.loads((await pricing_module.get_provider_procedure_cost_level(request, "123", "123")).body)

    assert document["provider"]["specialty_key"] == "cardiology"
    assert document["peer_group"]["specialty_key"] == "__all__"
    assert document["peer_group"]["specialty_scope"] == "all_specialties"
    assert document["query"]["specialty"] == "radiology"
    assert document["query"]["cohort_strategy"] == "precomputed"
    assert document["procedure"]["reported_code"] == "A1234"
    assert document["procedure"]["reported_code_system"] == "HCPCS"
    assert document["estimated_cost_level"] == "$$$"
    initial_parameters = request.ctx.sa_session.executions[2][0][0].compile().params
    fallback_parameters = request.ctx.sa_session.executions[3][0][0].compile().params
    peer_parameters = request.ctx.sa_session.executions[4][0][0].compile().params
    assert initial_parameters["specialty_key_1"] == "radiology"
    assert "specialty_key_1" not in fallback_parameters
    assert peer_parameters["specialty_key_1"] == ["radiology", "__all__"]


def _dynamic_cost_results():
    minimum_peers = pricing_module.PROCEDURE_COST_DYNAMIC_MIN_PEER_PROVIDERS
    outlier_charges = [100 + index for index in range(minimum_peers - 1)] + [1000000]
    broad_charges = [200 + index for index in range(minimum_peers + 2)]
    peer_rows = [
        [
            {"npi": 1000000000 + index, "claim_count": 20, "avg_submitted_charge": charge, "geography_value": "60601"}
            for index, charge in enumerate(charges)
        ]
        for charges in (outlier_charges, broad_charges)
    ]
    profile_by_field = {**_cost_profile_row(), "claim_count": 52}
    return [
        FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
        FakeResult(scalar="mrf.pricing_procedure_peer_stats"),
        FakeResult(rows=[{"zip5": "60601", "state": "IL", "city_lower": "example"}]),
        FakeResult(rows=[profile_by_field]),
        *[FakeResult(rows=rows) for rows in peer_rows],
        FakeResult(rows=[{"service_description": "Synthetic procedure", "reported_code": "12345"}]),
    ]


@pytest.mark.asyncio
async def test_dynamic_cost_expands_after_outlier_trim():
    request = make_request(
        _dynamic_cost_results(),
        args={"year": "2024", "zip5": "60601", "zip_radius_miles": "5", "cohort_strategy": "near_dynamic"},
    )

    document = json.loads((await pricing_module.get_provider_procedure_cost_level(request, "123", "123")).body)

    assert document["confidence"] == "high"
    assert document["estimated_cost_level"] == "$"
    assert document["peer_group"]["cohort_strategy_used"] == "near_dynamic"
    assert document["peer_group"]["provider_count"] == pricing_module.PROCEDURE_COST_DYNAMIC_MIN_PEER_PROVIDERS + 2
    assert document["peer_group"]["specialty_key"] == "__all__"
    assert document["peer_group"]["geography_value"] == "60601|5mi"
    assert document["query"]["state"] == "IL"
    assert document["query"]["city"] == "example"
    specific_query = request.ctx.sa_session.executions[4][0][0]
    broad_query = request.ctx.sa_session.executions[5][0][0]
    assert specific_query.compile().params["specialty_key_1"] == "radiology"
    assert "specialty_key_1" not in broad_query.compile().params
    assert broad_query.compile().params["geography_value_1"] == ["60601"]
    assert document["peer_group"]["median_avg_submitted_charge"] == 205.5
