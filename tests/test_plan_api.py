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

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

class FakeResult:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar

    def __iter__(self):
        return iter(self._rows)

class FakeSession:
    def __init__(self, results=None):
        self._results = list(results or [])

    async def execute(self, *_args, **_kwargs):
        if self._results:
            return self._results.pop(0)
        return FakeResult()

def make_request(results, args=None, json_data=None, app_config=None):
    session = FakeSession(results)
    return types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=session),
        args=args or {},
        json=json_data,
        app=types.SimpleNamespace(
            config=app_config or {"RELEASE": "dev", "ENVIRONMENT": "test"}
        ),
    )

def make_facet_results(
    plan_types=None,
    metal_levels=None,
    csr_variations=None,
    boolean_counts=None,
):
    return [
        FakeResult(rows=plan_types or []),
        FakeResult(rows=metal_levels or []),
        FakeResult(rows=csr_variations or []),
        FakeResult(rows=[boolean_counts or {}]),
    ]

@pytest.mark.asyncio
async def test_get_network_by_checksum(monkeypatch):
    async def fake_fetch(_session, checksum):
        return {"checksum": checksum, "plans": []}

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)

    request = make_request([])
    response = await get_network_by_checksum(request, "123")
    response_payload = json.loads(response.body)
    assert response_payload["checksum"] == 123

@pytest.mark.asyncio
async def test_get_network_batch_by_checksums(monkeypatch):
    async def fake_fetch(_session, checksum):
        return {"checksum": checksum, "plans": []}

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)

    request = make_request([])
    response = await get_network_batch_by_checksums(request, "1,2,1")
    response_payload = json.loads(response.body)
    assert [entry["checksum"] for entry in response_payload] == [1, 2]

@pytest.mark.asyncio
async def test_get_network_batch_by_checksums_not_found(monkeypatch):
    async def fake_fetch(_session, _checksum):
        return None

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)

    request = make_request([])
    with pytest.raises(sanic.exceptions.NotFound):
        await get_network_batch_by_checksums(request, "1,2")

def test_network_batch_route_keeps_legacy_reverse_name():
    route = next(
        route
        for route in plan_module.blueprint._future_routes
        if route.uri == "/network/multiple/<checksums>"
    )

    assert route.name == "plan.get_networks_by_checksums"

@pytest.mark.asyncio
async def test_plan_index_status():
    request = make_request(
        [
            FakeResult(scalar=5),
            FakeResult(scalar=3),
            FakeResult(scalar=2),
        ]
    )
    response = await index_status(request)
    response_payload = json.loads(response.body)
    assert response_payload["plan_count"] == 5
    assert response_payload["import_log_errors"] == 3
    assert response_payload["plans_network_count"] == 2

@pytest.mark.asyncio
async def test_plan_all_plans():
    request = make_request([FakeResult(rows=[{"plan_id": "123"}])])
    response = await all_plans(request)
    response_payload = json.loads(response.body)
    assert response_payload == [{"plan_id": "123"}]

@pytest.mark.asyncio
async def test_plan_all_plans_variants():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "marketing_name": "Plan",
                        "plan_id": "123",
                        "full_plan_id": "123-00",
                        "year": 2024,
                    }
                ]
            )
        ],
        args={"limit": "10", "offset": "5"},
    )
    response = await all_plans_variants(request)
    response_payload = json.loads(response.body)
    assert response_payload == [
        {"marketing_name": "Plan", "plan_id": "123", "full_plan_id": "123-00", "year": 2024}
    ]

@pytest.mark.asyncio
async def test_plan_get_autocomplete_empty():
    request = make_request([FakeResult(rows=[])], args={"query": "Silver"})
    response = await get_autocomplete_list(request)
    response_payload = json.loads(response.body)
    assert response_payload == {"plans": []}

@pytest.mark.asyncio
async def test_plan_get_autocomplete_success():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {"plan_id": "P123", "marketing_name": "Alpha", "network_checksum": {}},
                    {"plan_id": "P456", "marketing_name": "Beta", "network_checksum": {}},
                ]
            ),
            FakeResult(rows=[("P123", 111, "GOLD"), ("P456", 222, "SILVER")]),
        ],
        args={"query": "plan"},
    )
    response = await get_autocomplete_list(request)
    response_payload = json.loads(response.body)
    lookup_by_key = {item["plan_id"]: item for item in response_payload["plans"]}
    assert lookup_by_key["P123"]["network_checksum"] == {"111": "GOLD"}
    assert lookup_by_key["P456"]["network_checksum"] == {"222": "SILVER"}

@pytest.mark.asyncio
async def test_plan_find_plan_bad_year():
    request = make_request([], args={"year": "bad"})
    with pytest.raises(sanic.exceptions.BadRequest):
        await find_a_plan(request)

@pytest.mark.asyncio
async def test_plan_get_price_plan_bad_age():
    request = make_request([], args={"age": "bad"})
    with pytest.raises(sanic.exceptions.BadRequest):
        await get_price_plan(request, "123")

@pytest.mark.asyncio
async def test_plan_get_plan_not_found():
    request = make_request([FakeResult(rows=[])])
    with pytest.raises(sanic.exceptions.NotFound):
        await get_plan(request, "123")

@pytest.mark.asyncio
async def test_plan_get_price_plan_success():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "plan_id": "P123",
                        "year": 2024,
                        "min_age": 21,
                        "max_age": 64,
                        "rating_area_id": "A",
                        "individual_rate": 199.99,
                    }
                ]
            )
        ],
        args={"age": "30", "rating_area": "A"},
    )
    response = await get_price_plan(request, "P123", year="2024")
    response_payload = json.loads(response.body)
    assert response_payload == [
        {
            "plan_id": "P123",
            "year": 2024,
            "min_age": 21,
            "max_age": 64,
            "rating_area_id": "A",
            "individual_rate": 199.99,
        }
    ]

@pytest.mark.asyncio
async def test_plan_get_plan_success():
    plan_row_by_field = {
        "plan_id": "P123",
        "year": 2024,
        "issuer_id": 42,
        "state": "TX",
        "rate_effective_date": datetime.datetime(2025, 3, 25),
    }
    request = make_request(
        [
            FakeResult(rows=[plan_row_by_field]),
            FakeResult(rows=[(77777, "PREFERRED")]),
            FakeResult(scalar="Sample Issuer"),
            FakeResult(rows=[{"plan_id": "P123", "year": 2024, "drug_tier": "Tier 1", "pharmacy_type": "Retail"}]),
            FakeResult(rows=[], scalar=1),
        ]
    )
    response = await get_plan(request, "P123")
    response_payload = json.loads(response.body)
    assert response_payload["plan_id"] == "P123"
    assert response_payload["rate_effective_date"] == "2025-03-25 00:00:00"
    assert response_payload["network_checksum"] == {"77777": "PREFERRED"}
    assert response_payload["issuer_name"] == "Sample Issuer"
    assert response_payload["formulary"][0]["drug_tier"] == "Tier 1"
    assert response_payload["formulary_drug_count"] == 1
    assert response_payload["formulary_has_drug_data"] is True
    assert response_payload["formulary_uri"] == "P123/2024"

def _find_plan_success_request():
    plan_entry_by_field = {
        "plan_id": "P123",
        "year": 2024,
        "issuer_id": 42,
        "state": "TX",
        "min_rate": 100.0,
        "max_rate": 200.0,
        "rate_expiration_date": datetime.datetime(2025, 6, 4),
    }
    return make_request(
        [
            FakeResult(rows=[(1,)], scalar=1),
            *make_facet_results(),
            FakeResult(
                rows=[
                    {
                        **plan_entry_by_field,
                        "market_coverage": "On Exchange",
                        "is_on_exchange": True,
                        "is_off_exchange": False,
                        "is_hsa": False,
                        "is_dental_only": False,
                        "is_catastrophic": False,
                        "has_adult_dental": True,
                        "has_child_dental": False,
                        "has_adult_vision": False,
                        "has_child_vision": False,
                        "telehealth_supported": False,
                        "deductible_inn_individual": 500.0,
                        "moop_inn_individual": 3000.0,
                        "attributes": [
                            {"attr_name": "Coverage", "attr_value": "Standard"},
                        ],
                        "plan_benefits": [
                            {
                                "benefit_name": "Primary Care Visit",
                                "copay_inn_tier1": "No Charge",
                                "coins_inn_tier1": "No Charge",
                                "copay_inn_tier2": "Not Applicable",
                                "coins_inn_tier2": "Not Applicable",
                                "copay_outof_net": "Not Applicable",
                                "coins_outof_net": "Not Applicable",
                            }
                        ],
                    }
                ]
            ),
            FakeResult(rows=[{"plan_id": "P123", "year": 2024, "individual_rate": 100.0, "couple": 200.0}]),
            FakeResult(rows=[{"issuer_id": 42, "issuer_name": "Issuer X", "plan_count": 1}]),
        ],
        args={"year": "2024", "age": "30", "rating_area": "A", "limit": "1", "page": "1"},
    )

@pytest.mark.asyncio
async def test_plan_find_plan_success():
    """Verify plan find plan success."""
    request = _find_plan_success_request()
    response = await find_a_plan(request)
    response_payload = json.loads(response.body)
    assert response_payload["total"] == 1
    assert response_payload["facets"]["plan_types"] == []
    matched_plan = response_payload["results"][0]
    assert matched_plan["price_range"] == {"min": 100.0, "max": 200.0}
    assert matched_plan["attributes"]["Coverage"]["attr_value"] == "Standard"
    assert matched_plan["plan_benefits"]["Primary Care Visit"]["benefit_name"] == "Primary Care Visit"
    assert matched_plan["rate_expiration_date"] == "2025-06-04 00:00:00"

def test_get_session_missing():
    with pytest.raises(RuntimeError):
        _get_session(types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=None)))

def test_row_to_dict_variants():
    class MappingRow:
        def __init__(self):
            self._mapping = {"a": 1}

    assert _row_to_dict(MappingRow()) == {"a": 1}
    assert _row_to_dict({"b": 2}) == {"b": 2}
    class BadRow:
        def __iter__(self):
            raise TypeError
    assert _row_to_dict(BadRow()) == {}

def test_result_rows_and_scalar_helpers():
    class NoAll:
        def __iter__(self):
            return iter([(1,), (2,)])
    assert _result_rows(NoAll()) == [(1,), (2,)]

    class BareResult:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

    result = BareResult([("value",)])
    assert _result_scalar(result) == "value"
    dict_result = BareResult([{"k": "v"}])
    assert _result_scalar(dict_result) == "v"

def test_parse_bool_invalid():
    with pytest.raises(sanic.exceptions.InvalidUsage):
        plan_module._parse_bool("maybe", "flag")

def test_parse_float_invalid():
    with pytest.raises(sanic.exceptions.InvalidUsage):
        plan_module._parse_float("bad", "number")

def test_append_filter_skips_empty():
    applied_filter_map = {"existing": 1}
    plan_module._append_filter(applied_filter_map, "empty", "")
    assert applied_filter_map == {"existing": 1}
    plan_module._append_filter(applied_filter_map, "filled", 2)
    assert applied_filter_map["filled"] == 2

def test_summary_attributes_and_benefits_conversion():
    attr_payload = json.dumps([{"attr_name": "Foo", "attr_value": "bar"}])
    attrs = plan_module._summary_attributes_to_dict(attr_payload)
    assert attrs["Foo"]["attr_value"] == "bar"

    benefit_payload = json.dumps([{"benefit_name": "Primary", "copay_inn_tier1": "10"}])
    benefits = plan_module._summary_benefits_to_dict(benefit_payload)
    assert benefits["Primary"]["copay_inn_tier1"] == "10"

class SequenceSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self._idx = 0

    async def execute(self, _stmt):
        response = self._responses[self._idx]
        self._idx += 1
        if isinstance(response, Exception):
            raise response
        return response

class SimpleResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows

def _variant_identifier_results() -> list[FakeResult]:
    """Return plan-query results containing tuple-shaped variant IDs."""
    return [
            FakeResult(rows=[{"plan_id": "P1", "year": 2024, "issuer_id": 7}]),
            FakeResult(rows=[("checksum", "TIER1")]),
            FakeResult(rows=[], scalar="Issuer"),
            FakeResult(rows=[]),
            FakeResult(rows=[], scalar=0),
            FakeResult(
                rows=[("P1-01",), ("('P1-02',)",)]
            ),
            FakeResult(
                rows=[
                    {"full_plan_id": ("P1-03",), "attr_name": "SomeAttr", "attr_value": "value"}
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "full_plan_id": ("P1-04",), "benefit_name": "GeneralBenefit",
                        "copay_inn_tier1": "$5", "coins_inn_tier1": "25%",
                        "copay_inn_tier2": "Not Applicable", "coins_inn_tier2": None,
                        "copay_outof_net": "$15", "coins_outof_net": "20%", "year": 2024,
                        "plan_id": "P1",
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {"full_plan_id": "P1-01", "attr_name": "VariantAttr", "attr_value": "X"}
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "full_plan_id": "P1-01",
                        "benefit_name": "VariantBenefit",
                        "copay_inn_tier1": "$10",
                        "coins_inn_tier1": "50%",
                        "copay_inn_tier2": "Not Applicable",
                        "coins_inn_tier2": None,
                        "copay_outof_net": "Not Applicable",
                        "coins_outof_net": None,
                        "year": 2024,
                        "plan_id": "P1",
                    }
                ]
            ),
        ]
