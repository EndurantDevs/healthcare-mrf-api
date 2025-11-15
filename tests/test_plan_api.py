# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import json
import types

import pytest
import sanic.exceptions

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
    get_networks_by_checksums,
    get_plan,
    get_price_plan,
    index_status,
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
        ],
        args={"limit": "10", "offset": "5"},
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


@pytest.mark.asyncio
async def test_plan_find_plan_bad_year():
    request = make_request([], args={"year": "bad"})
    with pytest.raises(sanic.exceptions.BadRequest):
        await find_a_plan(request)


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
        "rate_effective_date": datetime.datetime(2025, 3, 25),
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
    assert payload["rate_effective_date"] == "2025-03-25 00:00:00"
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
        "rate_expiration_date": datetime.datetime(2025, 6, 4),
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
    assert result["rate_expiration_date"] == "2025-06-04 00:00:00"


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
    payload = json.loads(response.body)
    assert payload["plans"][0]["network_checksum"] == {"222": "PLATINUM"}


@pytest.mark.asyncio
async def test_plan_get_autocomplete_with_zip():
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
    payload = json.loads(response.body)
    assert payload["plans"][0]["network_checksum"] == {"333": "GOLD"}


@pytest.mark.asyncio
async def test_find_a_plan_success():
    request = make_request(
        [
            FakeResult(rows=[(5,)], scalar=5),
            FakeResult(rows=[{"plan_id": "P1", "min_rate": 10, "max_rate": 20}]),
            FakeResult(rows=[{"plan_id": "P1", "attr_name": "FormularyId", "attr_value": "val"}]),
            FakeResult(rows=[{"plan_id": "P1", "benefit_name": "benefit_name", "copay_inn_tier1": "10", "coins_inn_tier1": "20", "copay_inn_tier2": "Not Applicable", "coins_inn_tier2": None, "copay_outof_net": "30", "coins_outof_net": "40"}]),
        ],
        args={"age": "30", "year": "2024", "order": "invalid"},
    )
    response = await find_a_plan(request)
    payload = json.loads(response.body)
    assert payload["total"] == 5
    assert payload["results"][0]["attributes"]["FormularyId"] == "val"
    assert payload["results"][0]["plan_benefits"]["benefit_name"]["copay_inn_tier1"] == "10"


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
    payload = json.loads(response.body)
    assert payload == {"total": 0, "results": []}


@pytest.mark.asyncio
async def test_get_price_plan_with_year():
    request = make_request(
        [FakeResult(rows=[{"plan_id": "P1"}])],
        args={"age": "30"},
    )
    response = await get_price_plan(request, "P1", year="2024")
    payload = json.loads(response.body)
    assert payload == [{"plan_id": "P1"}]


@pytest.mark.asyncio
async def test_get_plan_with_variant(monkeypatch):
    request = make_request(
        [
            FakeResult(rows=[{"plan_id": "P1", "year": 2024, "issuer_id": 7}]),
            FakeResult(rows=[("checksum", "TIER1")]),
            FakeResult(rows=[], scalar="Issuer"),
            FakeResult(rows=[{"drug": "abc"}]),
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
    payload = json.loads(response.body)
    assert payload["issuer_name"] == "Issuer"
    assert payload["variant_attributes"]["FormularyId"]["attr_value"] == "val"
    assert payload["variant_attributes"]["FormularyId"]["human_attr_name"] == "Formulary ID"
    assert payload["variant_benefits"]["benefit_name"]["in_network_tier1"] == "5, 10"
    assert payload["variant_benefits"]["benefit_name"]["human_attr_name"] == "Benefit Name"


@pytest.mark.asyncio
async def test_get_plan_normalizes_variant_identifiers():
    request = make_request(
        [
            FakeResult(rows=[{"plan_id": "P1", "year": 2024, "issuer_id": 7}]),
            FakeResult(rows=[("checksum", "TIER1")]),
            FakeResult(rows=[], scalar="Issuer"),
            FakeResult(rows=[]),
            FakeResult(
                rows=[
                    ("P1-01",),
                    ("('P1-02',)",),
                ]
            ),
            FakeResult(
                rows=[
                    {"full_plan_id": ("P1-03",), "attr_name": "SomeAttr", "attr_value": "value"}
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "full_plan_id": ("P1-04",),
                        "benefit_name": "GeneralBenefit",
                        "copay_inn_tier1": "$5",
                        "coins_inn_tier1": "25%",
                        "copay_inn_tier2": "Not Applicable",
                        "coins_inn_tier2": None,
                        "copay_outof_net": "$15",
                        "coins_outof_net": "20%",
                        "year": 2024,
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
    )
    response = await get_plan(request, "P1", year="2024")
    payload = json.loads(response.body)
    assert payload["variants"] == ["P1-01", "P1-02", "P1-03", "P1-04"]
    assert payload["attributes"]["SomeAttr"]["attr_value"] == "value"
    assert payload["plan_benefits"]["GeneralBenefit"]["in_network_tier1"] == "$5, 25%"
    assert payload["variant_attributes"]["VariantAttr"]["attr_value"] == "X"
    assert payload["variant_benefits"]["VariantBenefit"]["in_network_tier1"] == "$10, 50%"


@pytest.mark.asyncio
async def test_get_plan_uses_fallback_variants_when_missing():
    request = make_request(
        [
            FakeResult(rows=[{"plan_id": "P2", "year": 2024, "issuer_id": 8}]),
            FakeResult(rows=[]),
            FakeResult(rows=[], scalar="Issuer"),
            FakeResult(rows=[]),
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
    payload = json.loads(response.body)
    assert payload["variants"] == ["P2-09"]
    assert payload["active_variant"] == "P2-09"
    assert payload["variant_attributes"]["FormularyId"]["attr_value"] == "PlanLevelValue"
    assert payload["variant_benefits"]["GeneralBenefit"]["in_network_tier1"] == "$15, 75%"


@pytest.mark.asyncio
async def test_get_plan_variant_not_found():
    request = make_request(
        [
            FakeResult(rows=[{"plan_id": "P1", "year": 2024, "issuer_id": 7}]),
            FakeResult(rows=[("checksum", "TIER1")]),
            FakeResult(rows=[], scalar="Issuer"),
            FakeResult(rows=[{"drug": "abc"}]),
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
    payload = json.loads(response.body)
    assert payload["variants"] == ["P1-00", "P1-01", "P1-02"]

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
async def test_get_networks_by_checksums_skips_invalid(monkeypatch):
    async def fake_fetch(_session, checksum):
        return {"checksum": checksum, "plans": []}

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)
    session = FakeSession([])
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=session))
    response = await get_networks_by_checksums(request, "bad,1")
    payload = json.loads(response.body)
    assert [entry["checksum"] for entry in payload] == [1]


@pytest.mark.asyncio
async def test_find_a_plan_skips_missing_plan_id():
    request = make_request(
        [
            FakeResult(rows=[(0,)]),
            FakeResult(rows=[{"plan_id": None}]),
        ],
        args={},
    )
    response = await find_a_plan(request)
    payload = json.loads(response.body)
    assert payload == {"total": 0, "results": []}


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
    response = await find_a_plan(request)
    payload = json.loads(response.body)
    assert payload == {"total": 0, "results": []}


def test_result_scalar_empty_iterable():
    assert _result_scalar([]) is None


def test_result_scalar_simple_value():
    assert _result_scalar(['alpha']) == 'alpha'


@pytest.mark.asyncio
async def test_find_a_plan_invalid_age():
    request = make_request([], args={'age': 'not-a-number'})
    with pytest.raises(sanic.exceptions.BadRequest):
        await find_a_plan(request)
