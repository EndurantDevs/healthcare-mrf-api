import asyncio
import json
import types

import pytest
import sanic.exceptions

from api.endpoint.plan import (
    get_network_by_checksum,
    get_networks_by_checksums,
    index_status,
    all_plans,
    all_plans_variants,
    get_autocomplete_list,
    find_a_plan,
    get_price_plan,
    get_plan,
)


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


def make_request(results, args=None, app_config=None):
    session = FakeSession(results)
    return types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=session),
        args=args or {},
        app=types.SimpleNamespace(
            config=app_config or {"RELEASE": "dev", "ENVIRONMENT": "test"}
        ),
    )


@pytest.mark.asyncio
async def test_get_network_by_checksum(monkeypatch):
    async def fake_fetch(_session, checksum):
        return {"checksum": checksum, "plans": []}

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)

    request = make_request([])
    response = await get_network_by_checksum(request, "123")
    payload = json.loads(response.body)
    assert payload["checksum"] == 123


@pytest.mark.asyncio
async def test_get_networks_by_checksums(monkeypatch):
    async def fake_fetch(_session, checksum):
        return {"checksum": checksum, "plans": []}

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)

    request = make_request([])
    response = await get_networks_by_checksums(request, "1,2,1")
    payload = json.loads(response.body)
    assert [entry["checksum"] for entry in payload] == [1, 2]


@pytest.mark.asyncio
async def test_get_networks_by_checksums_not_found(monkeypatch):
    async def fake_fetch(_session, _checksum):
        return None

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)

    request = make_request([])
    with pytest.raises(sanic.exceptions.NotFound):
        await get_networks_by_checksums(request, "1,2")


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
    payload = json.loads(response.body)
    assert payload["plan_count"] == 5
    assert payload["import_log_errors"] == 3
    assert payload["plans_network_count"] == 2


@pytest.mark.asyncio
async def test_plan_all_plans():
    request = make_request([FakeResult(rows=[{"plan_id": "123"}])])
    response = await all_plans(request)
    payload = json.loads(response.body)
    assert payload == [{"plan_id": "123"}]


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
        ]
    )
    response = await all_plans_variants(request)
    payload = json.loads(response.body)
    assert payload == [
        {"marketing_name": "Plan", "plan_id": "123", "full_plan_id": "123-00", "year": 2024}
    ]


@pytest.mark.asyncio
async def test_plan_get_autocomplete_empty():
    request = make_request([FakeResult(rows=[])], args={"query": "Silver"})
    response = await get_autocomplete_list(request)
    payload = json.loads(response.body)
    assert payload == {"plans": []}


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
    payload = json.loads(response.body)
    lookup = {item["plan_id"]: item for item in payload["plans"]}
    assert lookup["P123"]["network_checksum"] == {"111": "GOLD"}
    assert lookup["P456"]["network_checksum"] == {"222": "SILVER"}


def test_plan_find_plan_bad_year():
    request = make_request([], args={"year": "bad"})
    with pytest.raises(sanic.exceptions.BadRequest):
        asyncio.get_event_loop().run_until_complete(find_a_plan(request))


def test_plan_get_price_plan_bad_age():
    request = make_request([], args={"age": "bad"})
    with pytest.raises(sanic.exceptions.BadRequest):
        asyncio.get_event_loop().run_until_complete(get_price_plan(request, "123"))


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
    payload = json.loads(response.body)
    assert payload == [
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
    plan_row = {
        "plan_id": "P123",
        "year": 2024,
        "issuer_id": 42,
        "state": "TX",
    }
    request = make_request(
        [
            FakeResult(rows=[plan_row]),
            FakeResult(rows=[(77777, "PREFERRED")]),
            FakeResult(scalar="Sample Issuer"),
            FakeResult(rows=[{"plan_id": "P123", "year": 2024, "drug_tier": "Tier 1", "pharmacy_type": "Retail"}]),
        ]
    )
    response = await get_plan(request, "P123")
    payload = json.loads(response.body)
    assert payload["plan_id"] == "P123"
    assert payload["network_checksum"] == {"77777": "PREFERRED"}
    assert payload["issuer_name"] == "Sample Issuer"
    assert payload["formulary"][0]["drug_tier"] == "Tier 1"


@pytest.mark.asyncio
async def test_plan_find_plan_success():
    plan_entry = {
        "plan_id": "P123",
        "year": 2024,
        "issuer_id": 42,
        "state": "TX",
        "min_rate": 100.0,
        "max_rate": 200.0,
    }
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(rows=[plan_entry]),
            FakeResult(rows=[{"plan_id": "P123", "attr_name": "Coverage", "attr_value": "Standard"}]),
            FakeResult(
                rows=[
                    {
                        "plan_id": "P123",
                        "benefit_name": "Primary Care Visit",
                        "copay_inn_tier1": "No Charge",
                        "coins_inn_tier1": "No Charge",
                        "copay_inn_tier2": "Not Applicable",
                        "coins_inn_tier2": "Not Applicable",
                        "copay_outof_net": "Not Applicable",
                        "coins_outof_net": "Not Applicable",
                    }
                ]
            ),
        ],
        args={"year": "2024", "age": "30", "rating_area": "A", "limit": "1", "page": "1"},
    )
    response = await find_a_plan(request)
    payload = json.loads(response.body)
    assert payload["total"] == 1
    result = payload["results"][0]
    assert result["price_range"] == {"min": 100.0, "max": 200.0}
    assert result["attributes"]["Coverage"] == "Standard"
    assert result["plan_benefits"]["Primary Care Visit"]["benefit_name"] == "Primary Care Visit"
