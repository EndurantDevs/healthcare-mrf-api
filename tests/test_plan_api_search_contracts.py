# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio

import datetime

import json

import types

import pytest

import sanic.exceptions

from sqlalchemy.exc import ProgrammingError

import api.endpoint.plan as plan_module

from api.endpoint.plan import (
    _fetch_network_entry,
    _get_session,
    _result_rows,
    _result_scalar,
    _row_to_dict,
    all_plans,
    all_plans_variants,
    find_a_plan,
    get_autocomplete_list,
    get_network_by_checksum,
    get_network_batch_by_checksums,
    get_plan,
    get_price_plan,
    get_price_plans_bulk,
    index_status,
)

from tests.test_plan_api import (
    FakeResult,
    FakeSession,
    SequenceSession,
    SimpleResult,
    _find_plan_success_request,
    _variant_identifier_results,
    make_facet_results,
    make_request,
)

def test_collect_price_bounds_merges():
    class Result:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

    fake_rows = [
        {"plan_id": "P1", "year": 2024, "individual_rate": 10, "couple": 20},
        {"plan_id": "P1", "year": 2024, "individual_rate": 5, "couple": None},
    ]
    bounds = plan_module._collect_price_bounds(Result(fake_rows))
    assert bounds[("P1", 2024)] == {"min": 5.0, "max": 20.0}

def test_get_list_param_with_getlist():
    class Args:
        def getlist(self, name):
            if name == "values":
                return ["a, b", ["c", "d"]]
            return []

    values = plan_module._get_list_param(Args(), "values")
    assert values == ["a", "b", "c", "d"]

@pytest.mark.asyncio
async def test_states_for_zip_short_input():
    assert await plan_module._states_for_zip(FakeSession([]), "12") == []

@pytest.mark.asyncio
async def test_states_for_zip_geo_states(monkeypatch):
    session = SequenceSession([SimpleResult([("CA",), ("",)] )])
    assert await plan_module._states_for_zip(session, "90001") == ["CA"]

@pytest.mark.asyncio
async def test_states_for_zip_programming_error_fallback(monkeypatch):
    class DummyUndefined(Exception):
        pass

    monkeypatch.setattr(plan_module, "UndefinedTableError", DummyUndefined)
    exc = ProgrammingError("stmt", {}, DummyUndefined())
    session = SequenceSession([
        exc,
        SimpleResult([]),  # rating area
        SimpleResult([("TX",)])
    ])
    assert await plan_module._states_for_zip(session, "73301") == ["TX"]

@pytest.mark.asyncio
async def test_states_for_zip_programming_error_final_fallback(monkeypatch):
    class DummyUndefined(Exception):
        pass

    monkeypatch.setattr(plan_module, "UndefinedTableError", DummyUndefined)
    exc = ProgrammingError("stmt", {}, DummyUndefined())
    session = SequenceSession([
        exc,
        SimpleResult([]),
        ProgrammingError("stmt", {}, Exception()),
    ])
    assert await plan_module._states_for_zip(session, "90210") == []

@pytest.mark.asyncio
async def test_fetch_network_entry(monkeypatch):
    rows = [
        {
            "plan_id": "P1",
            "year": 2024,
            "checksum_network": 10,
            "network_tier": "PREFERRED",
            "issuer_id": 5,
            "issuer_name": "Issuer",
            "issuer_marketing_name": "Issuer Inc",
            "issuer_state": "CA",
        },
        {
            "plan_id": "P1",
            "year": 2024,
            "checksum_network": 10,
            "network_tier": "PREFERRED",
            "issuer_id": 5,
            "issuer_name": "Issuer",
            "issuer_marketing_name": "Issuer Inc",
            "issuer_state": "CA",
        },
    ]
    session = FakeSession([FakeResult(rows=rows)])
    entry = await _fetch_network_entry(session, 10)
    assert entry["issuer"] == 5
    assert entry["plans"] == [{"plan_id": "P1", "year": 2024}]

@pytest.mark.asyncio
async def test_fetch_network_entry_missing():
    session = FakeSession([FakeResult(rows=[])])
    assert await _fetch_network_entry(session, 99) is None

@pytest.mark.asyncio
async def test_plan_get_autocomplete_with_state():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "plan_id": "PX",
                        "marketing_name": "Plan X",
                        "network_checksum": {},
                    }
                ]
            ),
            FakeResult(rows=[("PX", 222, "PLATINUM")]),
        ],
        args={"query": "plan", "state": "tx"},
    )
    response = await get_autocomplete_list(request)
    response_payload = json.loads(response.body)
    assert response_payload["plans"][0]["network_checksum"] == {"222": "PLATINUM"}

@pytest.mark.asyncio
async def test_plan_get_autocomplete_with_zip(monkeypatch):
    async def fake_states(_session, zip_code):
        assert zip_code == "02110"
        return ["MA"]

    monkeypatch.setattr(plan_module, "_states_for_zip", fake_states)
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "plan_id": "PZ",
                        "marketing_name": "Plan Z",
                        "network_checksum": {},
                    }
                ]
            ),
            FakeResult(rows=[("PZ", 333, "GOLD")]),
        ],
        args={"query": "plan", "zip_code": "02110"},
    )
    response = await get_autocomplete_list(request)
    response_payload = json.loads(response.body)
    assert response_payload["plans"][0]["network_checksum"] == {"333": "GOLD"}

@pytest.mark.asyncio
async def test_find_a_plan_success():
    request = make_request(
        [
            FakeResult(rows=[(5,)], scalar=5),
            *make_facet_results(),
            FakeResult(
                rows=[
                    {
                        "plan_id": "P1",
                        "year": 2024,
                        "issuer_id": 42,
                        "state": "TX",
                        "has_adult_dental": True,
                        "has_child_dental": False,
                        "has_adult_vision": None,
                        "has_child_vision": None,
                        "telehealth_supported": None,
                        "is_hsa": False,
                        "is_dental_only": False,
                        "is_catastrophic": False,
                        "is_on_exchange": True,
                        "is_off_exchange": False,
                        "market_coverage": "On Exchange",
                        "deductible_inn_individual": 500.0,
                        "moop_inn_individual": 3000.0,
                        "attributes": [
                            {"attr_name": "FormularyId", "attr_value": "val"},
                        ],
                        "plan_benefits": [
                            {
                                "benefit_name": "benefit_name",
                                "copay_inn_tier1": "10",
                                "coins_inn_tier1": "20",
                                "copay_inn_tier2": "Not Applicable",
                                "coins_inn_tier2": None,
                                "copay_outof_net": "30",
                                "coins_outof_net": "40",
                            }
                        ],
                    }
                ]
            ),
            FakeResult(rows=[{"plan_id": "P1", "year": 2024, "individual_rate": 10, "couple": 20}]),
            FakeResult(rows=[{"issuer_id": 42, "issuer_name": "Issuer X", "plan_count": 1}]),
        ],
        args={"age": "30", "year": "2024", "order": "invalid"},
    )
    response = await find_a_plan(request)
    response_payload = json.loads(response.body)
    assert response_payload["total"] == 5
    assert "facets" in response_payload
    assert response_payload["issuers"][0]["issuer_id"] == 42
    assert response_payload["applied_filters"]["age"] == 30
    assert response_payload["results"][0]["has_adult_dental"] is True
    assert response_payload["results"][0]["deductible_inn_individual"] == 500.0
    assert response_payload["results"][0]["attributes"]["FormularyId"]["attr_value"] == "val"
    assert response_payload["results"][0]["plan_benefits"]["benefit_name"]["copay_inn_tier1"] == "10"
    assert response_payload["results"][0]["price_range"] == {"min": 10.0, "max": 20.0}
    assert response_payload["warnings"] == []

@pytest.mark.asyncio
async def test_find_a_plan_no_results():
    request = make_request(
        [
            FakeResult(rows=[(0,)]),
            FakeResult(rows=[]),
        ],
        args={},
    )
    response = await find_a_plan(request)
    response_payload = json.loads(response.body)
    assert response_payload["total"] == 0
    assert response_payload["results"] == []
    assert response_payload["issuers"] == []
    assert response_payload["facets"]["plan_types"] == []
    assert response_payload["warnings"] == []
    assert response_payload["applied_filters"]["limit"] == 100
    assert response_payload["facets"]["plan_types"] == []

@pytest.mark.asyncio
async def test_find_a_plan_with_new_filters():
    plan_entry_by_field = {
        "plan_id": "PX",
        "year": 2024,
        "issuer_id": 42,
        "state": "TX",
        "premium_min": 150.0,
        "premium_max": 200.0,
        "plan_type": "HMO",
        "metal_level": "Bronze",
        "csr_variation": "Standard",
        "has_adult_dental": False,
        "has_child_dental": False,
        "has_adult_vision": False,
        "has_child_vision": False,
        "telehealth_supported": False,
        "is_hsa": False,
        "is_dental_only": False,
        "is_catastrophic": False,
        "is_on_exchange": True,
        "is_off_exchange": False,
        "market_coverage": "Individual",
        "deductible_inn_individual": 500.0,
        "moop_inn_individual": 3000.0,
        "attributes": [],
        "plan_benefits": [],
    }
    request = make_request(
        [
            FakeResult(rows=[(1,)], scalar=1),
            *make_facet_results(),
            FakeResult(rows=[plan_entry_by_field]),
            FakeResult(rows=[{"plan_id": "PX", "year": 2024, "individual_rate": 150.0}]),
            FakeResult(rows=[{"issuer_id": 42, "issuer_name": "Issuer", "plan_count": 1}]),
        ],
        args={
            "plan_types": "HMO",
            "metal_levels": "bronze",
            "csr_variations": "standard",
            "premium_min": "100",
            "premium_max": "250",
            "issuer_ids": "42",
        },
    )
    response = await find_a_plan(request)
    response_payload = json.loads(response.body)
    assert response_payload["results"][0]["plan_type"] == "HMO"
    assert response_payload["applied_filters"]["plan_types"] == ["HMO"]
    assert response_payload["applied_filters"].get("issuer_ids") in (None, [42])
    assert response_payload["applied_filters"]["issuer_id"] == 42

@pytest.mark.asyncio
async def test_find_a_plan_returns_facets():
    facet_rows = make_facet_results(
        plan_types=[{"value": "HMO", "count": 2}],
        metal_levels=[{"value": "BRONZE", "count": 2}],
        csr_variations=[{"value": "Standard", "count": 2}],
        boolean_counts={
            "has_adult_dental_true": 1,
            "has_adult_dental_false": 1,
            "is_hsa_true": 2,
            "is_hsa_false": 0,
        },
    )
    request = make_request(
        [
            FakeResult(rows=[(2,)], scalar=2),
            *facet_rows,
            FakeResult(
                rows=[
                    {
                        "plan_id": "PFACET",
                        "year": 2024,
                        "issuer_id": 99,
                        "state": "IL",
                        "attributes": [],
                        "plan_benefits": [],
                    }
                ]
            ),
            FakeResult(rows=[]),
            FakeResult(rows=[{"issuer_id": 99, "issuer_name": "Issuer Facet", "plan_count": 2}]),
        ],
        args={},
    )
    response = await find_a_plan(request)
    response_payload = json.loads(response.body)
    assert response_payload["facets"]["plan_types"][0] == {"value": "HMO", "count": 2}
    assert response_payload["facets"]["metal_levels"][0] == {"value": "BRONZE", "count": 2}
    assert response_payload["facets"]["boolean_filters"]["has_adult_dental"]["true"] == 1
    assert response_payload["facets"]["boolean_filters"]["is_hsa"]["true"] == 2

@pytest.mark.asyncio
async def test_find_a_plan_zip_warning():
    request = make_request(
        [
            FakeResult(rows=[]),  # geo lookup
            FakeResult(rows=[]),  # rating area
            FakeResult(rows=[]),  # tiger fallback
        ],
        args={"zip_code": "99999"},
    )
    response = await find_a_plan(request)
    response_payload = json.loads(response.body)
    assert response_payload["total"] == 0
    assert response_payload["warnings"][0]["code"] == "zip_not_found"
    assert response_payload["applied_filters"]["zip_code"] == "99999"
    assert response_payload["facets"]["plan_types"] == []

@pytest.mark.asyncio
async def test_get_price_plan_with_year():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {"plan_id": "P1", "year": 2024, "individual_rate": 100},
                    {"plan_id": "P1", "year": 2025, "individual_rate": 200},
                ]
            )
        ],
        args={"age": "30", "year": "2024"},
    )
    response = await get_price_plan(request, "P1")
    response_payload = json.loads(response.body)
    assert response_payload == [{"plan_id": "P1", "year": 2024, "individual_rate": 100}]

@pytest.mark.asyncio
async def test_get_price_plans_bulk_success():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {"plan_id": "P1", "year": 2024, "rate": 100},
                    {"plan_id": "P2", "year": 2024, "rate": 200},
                ]
            )
        ],
        json_data={"plan_ids": ["P1", "P2", "P3"], "year": 2024, "age": 30, "rating_area": "A"},
    )
    response = await get_price_plans_bulk(request)
    response_payload = json.loads(response.body)
    assert response_payload["results"]["P1"][0]["plan_id"] == "P1"
    assert response_payload["results"]["P2"][0]["plan_id"] == "P2"
    assert response_payload["missing"] == ["P3"]

@pytest.mark.asyncio
async def test_get_price_plans_bulk_requires_ids():
    request = make_request([], json_data={})
    with pytest.raises(sanic.exceptions.BadRequest):
        await get_price_plans_bulk(request)

@pytest.mark.asyncio
async def test_get_price_plans_bulk_requires_json_object():
    request = make_request([], json_data=[])
    with pytest.raises(sanic.exceptions.BadRequest):
        await get_price_plans_bulk(request)

@pytest.mark.asyncio
async def test_get_price_plans_bulk_plan_ids_type():
    request = make_request([], json_data={"plan_ids": "not-list"})
    with pytest.raises(sanic.exceptions.BadRequest):
        await get_price_plans_bulk(request)

@pytest.mark.asyncio
async def test_get_price_plans_bulk_plan_ids_not_empty():
    request = make_request([], json_data={"plan_ids": [None, "  "]})
    with pytest.raises(sanic.exceptions.BadRequest):
        await get_price_plans_bulk(request)

@pytest.mark.asyncio
async def test_get_price_plans_bulk_year_age_validation():
    request = make_request([], json_data={"plan_ids": ["P1"], "year": "bad"})
    with pytest.raises(sanic.exceptions.BadRequest):
        await get_price_plans_bulk(request)

    request = make_request([], json_data={"plan_ids": ["P1"], "age": "bad"})
    with pytest.raises(sanic.exceptions.BadRequest):
        await get_price_plans_bulk(request)

@pytest.mark.asyncio
async def test_get_price_plans_bulk_rating_area_trimmed():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {"plan_id": "P1", "year": 2024, "rate": 100},
                ]
            )
        ],
        json_data={"plan_ids": ["P1"], "rating_area": "  "},
    )
    response = await get_price_plans_bulk(request)
    response_payload = json.loads(response.body)
    assert response_payload["results"]["P1"][0]["rate"] == 100
