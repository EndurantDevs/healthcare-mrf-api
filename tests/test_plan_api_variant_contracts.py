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

@pytest.mark.asyncio
async def test_get_plan_with_variant(monkeypatch):
    request = make_request(
        [
            FakeResult(rows=[{"plan_id": "P1", "year": 2024, "issuer_id": 7}]),
            FakeResult(rows=[("checksum", "TIER1")]),
            FakeResult(rows=[], scalar="Issuer"),
            FakeResult(rows=[{"drug": "abc"}]),
            FakeResult(rows=[], scalar=1),
            FakeResult(rows=[("P1-01",)]),
            FakeResult(rows=[{"attr_name": "FormularyId", "attr_value": "val"}]),
            FakeResult(
                rows=[
                    {
                        "benefit_name": "benefit_name",
                        "copay_inn_tier1": "5",
                        "coins_inn_tier1": "10",
                        "copay_inn_tier2": "Not Applicable",
                        "coins_inn_tier2": None,
                        "copay_outof_net": "15",
                        "coins_outof_net": "20",
                        "full_plan_id": "P1-01",
                        "year": 2024,
                        "plan_id": "P1",
                    }
                ]
            ),
        ]
    )
    response = await get_plan(request, "P1", year="2024", variant="P1-01")
    response_payload = json.loads(response.body)
    assert response_payload["issuer_name"] == "Issuer"
    assert response_payload["variant_attributes"]["FormularyId"]["attr_value"] == "val"
    assert response_payload["variant_attributes"]["FormularyId"]["human_attr_name"] == "Formulary ID"
    assert response_payload["variant_benefits"]["benefit_name"]["in_network_tier1"] == "5, 10"
    assert response_payload["variant_benefits"]["benefit_name"]["human_attr_name"] == "Benefit Name"
    assert response_payload["formulary_has_drug_data"] is True
    assert response_payload["formulary_uri"] == "P1/2024"

@pytest.mark.asyncio
async def test_get_plan_normalizes_variant_identifiers():
    """Verify get plan normalizes variant identifiers."""
    request = make_request(_variant_identifier_results())
    response = await get_plan(request, "P1", year="2024")
    response_payload = json.loads(response.body)
    assert response_payload["variants"] == ["P1-01", "P1-02", "P1-03", "P1-04"]
    assert response_payload["attributes"]["SomeAttr"]["attr_value"] == "value"
    assert response_payload["plan_benefits"]["GeneralBenefit"]["in_network_tier1"] == "$5, 25%"
    assert response_payload["variant_attributes"]["VariantAttr"]["attr_value"] == "X"
    assert response_payload["variant_benefits"]["VariantBenefit"]["in_network_tier1"] == "$10, 50%"
    assert response_payload["formulary_has_drug_data"] is False
    assert response_payload["formulary_uri"] == "P1/2024"

@pytest.mark.asyncio
async def test_get_plan_uses_fallback_variants_when_missing():
    request = make_request(
        [
            FakeResult(rows=[{"plan_id": "P2", "year": 2024, "issuer_id": 8}]),
            FakeResult(rows=[]),
            FakeResult(rows=[], scalar="Issuer"),
            FakeResult(rows=[]),
            FakeResult(rows=[], scalar=0),
            FakeResult(rows=[]),
            FakeResult(
                rows=[
                    {"full_plan_id": None, "attr_name": "FormularyId", "attr_value": "PlanLevelValue"}
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "full_plan_id": None,
                        "benefit_name": "GeneralBenefit",
                        "copay_inn_tier1": "$15",
                        "coins_inn_tier1": "75%",
                        "copay_inn_tier2": "Not Applicable",
                        "coins_inn_tier2": None,
                        "copay_outof_net": "Not Applicable",
                        "coins_outof_net": None,
                        "year": 2024,
                        "plan_id": "P2",
                    }
                ]
            ),
            FakeResult(rows=[("P2-09",)]),
            FakeResult(rows=[]),
            FakeResult(rows=[]),
        ]
    )
    response = await get_plan(request, "P2", year="2024")
    response_payload = json.loads(response.body)
    assert response_payload["variants"] == ["P2-09"]
    assert response_payload["active_variant"] == "P2-09"
    assert response_payload["variant_attributes"]["FormularyId"]["attr_value"] == "PlanLevelValue"
    assert response_payload["variant_benefits"]["GeneralBenefit"]["in_network_tier1"] == "$15, 75%"
    assert response_payload["formulary_has_drug_data"] is False
    assert response_payload["formulary_uri"] == "P2/2024"

@pytest.mark.asyncio
async def test_get_plan_variant_not_found():
    request = make_request(
        [
            FakeResult(rows=[{"plan_id": "P1", "year": 2024, "issuer_id": 7}]),
            FakeResult(rows=[("checksum", "TIER1")]),
            FakeResult(rows=[], scalar="Issuer"),
            FakeResult(rows=[{"drug": "abc"}]),
            FakeResult(rows=[], scalar=1),
            FakeResult(rows=[("P1-01",)]),
        ]
    )
    with pytest.raises(sanic.exceptions.NotFound):
        await get_plan(request, "P1", year="2024", variant="P1-99")

@pytest.mark.asyncio
async def test_plan_variants_unique_and_clean():
    request = make_request(
        [
            FakeResult(rows=[{"plan_id": "P1", "year": 2024, "issuer_id": 7}]),
            FakeResult(rows=[("checksum", "TIER1")]),
            FakeResult(rows=[], scalar="Issuer Name"),
            FakeResult(rows=[]),
            FakeResult(rows=[], scalar=0),
            FakeResult(rows=[("('P1-00',)",), ("('P1-01',)",)]),
            FakeResult(
                rows=[
                    {"full_plan_id": "('P1-00',)", "attr_name": "AttrA", "attr_value": "A"},
                    {"full_plan_id": "P1-02", "attr_name": "AttrB", "attr_value": "B"},
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "full_plan_id": "('P1-00',)",
                        "benefit_name": "BenefitA",
                        "copay_inn_tier1": "$5",
                        "coins_inn_tier1": "10%",
                        "copay_inn_tier2": None,
                        "coins_inn_tier2": None,
                        "copay_outof_net": None,
                        "coins_outof_net": None,
                        "year": 2024,
                        "plan_id": "P1",
                    }
                ]
            ),
            FakeResult(rows=[{"attr_name": "AttrA", "attr_value": "A"}]),
            FakeResult(
                rows=[
                    {
                        "benefit_name": "BenefitA",
                        "copay_inn_tier1": "$5",
                        "coins_inn_tier1": "10%",
                        "copay_inn_tier2": None,
                        "coins_inn_tier2": None,
                        "copay_outof_net": None,
                        "coins_outof_net": None,
                        "year": 2024,
                        "plan_id": "P1",
                    }
                ]
            ),
        ]
    )
    response = await get_plan(request, "P1", year="2024")
    response_payload = json.loads(response.body)
    assert response_payload["variants"] == ["P1-00", "P1-01", "P1-02"]
    assert response_payload["formulary_has_drug_data"] is False
    assert response_payload["formulary_uri"] == "P1/2024"

def test_result_rows_handles_typeerror():
    class BadAll:
        def all(self):
            raise TypeError

        def __iter__(self):
            return iter([1, 2])

    assert _result_rows(BadAll()) == [1, 2]

def test_result_scalar_empty():
    assert _result_scalar(FakeResult(rows=[])) is None

@pytest.mark.asyncio
async def test_get_network_batch_by_checksums_skips_invalid(monkeypatch):
    async def fake_fetch(_session, checksum):
        return {"checksum": checksum, "plans": []}

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)
    session = FakeSession([])
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=session))
    response = await get_network_batch_by_checksums(request, "bad,1")
    response_payload = json.loads(response.body)
    assert [entry["checksum"] for entry in response_payload] == [1]

@pytest.mark.asyncio
async def test_find_a_plan_skips_missing_plan_id():
    request = make_request(
        [
            FakeResult(rows=[(0,)]),
            FakeResult(rows=[{"plan_id": None, "year": 2024, "attributes": [], "plan_benefits": []}]),
        ],
        args={},
    )
    response = await find_a_plan(request)
    response_payload = json.loads(response.body)
    assert response_payload["total"] == 0
    assert response_payload["results"] == []
    assert response_payload["issuers"] == []

@pytest.mark.asyncio
async def test_plan_get_price_plan_bad_year_value():
    request = make_request([], args={})
    with pytest.raises(sanic.exceptions.BadRequest):
        await get_price_plan(request, "123", year="bad")

def test_result_scalar_tuple():
    class TupleResult:
        def __init__(self):
            self._rows = [(1, 2)]

        def all(self):
            return self._rows

    assert _result_scalar(TupleResult()) == 1

@pytest.mark.asyncio
async def test_get_network_by_checksum_not_found(monkeypatch):
    async def fake_fetch(_session, _checksum):
        return None

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)
    request = make_request([], args={})
    with pytest.raises(sanic.exceptions.NotFound):
        await get_network_by_checksum(request, "123")

@pytest.mark.asyncio
async def test_plan_get_autocomplete_no_query():
    request = make_request([], args={})
    response = await get_autocomplete_list(request)
    assert json.loads(response.body) == {"plans": []}

@pytest.mark.asyncio
async def test_find_a_plan_invalid_limit_page():
    request = make_request(
        [
            FakeResult(rows=[(0,)]),
            FakeResult(rows=[]),
        ],
        args={"limit": "bad", "page": "bad"},
    )
    with pytest.raises(sanic.exceptions.BadRequest):
        await find_a_plan(request)

@pytest.mark.asyncio
async def test_find_a_plan_boolean_filter_without_metadata():
    request = make_request(
        [
            FakeResult(rows=[(0,)]),
            FakeResult(rows=[]),
        ],
        args={"has_adult_dental": "true"},
    )
    response = await find_a_plan(request)
    response_payload = json.loads(response.body)
    assert response_payload["total"] == 0
    assert response_payload["warnings"] == []
    assert response_payload["applied_filters"]["has_adult_dental"] is True
    assert "facets" in response_payload

@pytest.mark.asyncio
async def test_find_a_plan_include_facets_false_hides_facets_payload():
    request = make_request(
        [
            FakeResult(rows=[(0,)]),
            FakeResult(rows=[]),
        ],
        args={"include_facets": "false"},
    )
    response = await find_a_plan(request)
    response_payload = json.loads(response.body)
    assert response_payload["facets"] == {}
    assert response_payload["applied_filters"]["include_facets"] is False

@pytest.mark.asyncio
async def test_find_a_plan_include_aggregations_alias_controls_facets():
    request = make_request(
        [
            FakeResult(rows=[(0,)]),
            FakeResult(rows=[]),
        ],
        args={"include_aggregations": "0"},
    )
    response = await find_a_plan(request)
    response_payload = json.loads(response.body)
    assert response_payload["facets"] == {}
    assert response_payload["applied_filters"]["include_facets"] is False

@pytest.mark.asyncio
async def test_find_a_plan_rejects_conflicting_facet_aliases():
    request = make_request([], args={"include_facets": "true", "include_aggregations": "false"})
    with pytest.raises(sanic.exceptions.BadRequest):
        await find_a_plan(request)

@pytest.mark.asyncio
async def test_find_a_plan_returns_pagination_metadata():
    request = make_request(
        [
            FakeResult(rows=[(0,)]),
            FakeResult(rows=[]),
        ],
        args={"page": "3", "limit": "20"},
    )
    response = await find_a_plan(request)
    response_payload = json.loads(response.body)
    assert response_payload["page"] == 3
    assert response_payload["limit"] == 20
    assert response_payload["offset"] == 40

def test_result_scalar_empty_iterable():
    assert _result_scalar([]) is None

def test_result_scalar_simple_value():
    assert _result_scalar(['alpha']) == 'alpha'

@pytest.mark.asyncio
async def test_find_a_plan_invalid_age():
    request = make_request([], args={'age': 'not-a-number'})
    with pytest.raises(sanic.exceptions.BadRequest):
        await find_a_plan(request)

def test_normalize_attribute_map_converts_scalars():
    raw_by_key = {"PlanMarketingName": "Bronze ABC"}
    normalized = plan_module._normalize_attribute_map(raw_by_key)
    assert normalized["PlanMarketingName"]["attr_value"] == "Bronze ABC"
    assert "human_attr_name" in normalized["PlanMarketingName"]
