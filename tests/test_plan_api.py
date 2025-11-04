import asyncio
import json
import types

import pytest
import sanic.exceptions

from api.endpoint import plan as plan_module
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


class FakeFunc:
    def __init__(self, values):
        self._values = values

    def count(self, *_args, **_kwargs):
        return FakeScalar(self._values.pop(0))

    def distinct(self, *_args, **_kwargs):
        return object()


class FakeAcquire:
    class _Connection:
        class _Transaction:
            async def __aenter__(self):
                return None

            async def __aexit__(self, exc_type, exc, tb):
                return False

        def transaction(self):
            return self._Transaction()

    async def __aenter__(self):
        return self._Connection()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeScalar:
    def __init__(self, value):
        self.value = value
        self.gino = self

    def where(self, *_args, **_kwargs):
        return self

    async def scalar(self):
        return self.value


class FakePlan:
    def __init__(self, payload):
        self._payload = payload

    def to_json_dict(self):
        return dict(self._payload)


class FakePrice:
    def __init__(self, payload):
        self._payload = payload

    def to_json_dict(self):
        return dict(self._payload)


class FakeFormulary:
    def __init__(self, payload):
        self._payload = payload

    def to_json_dict(self):
        return dict(self._payload)


class FakePlanNetwork:
    def __init__(self, plan_id, checksum, tier):
        self.plan_id = plan_id
        self.checksum = checksum
        self.tier = tier


class FakeNetworkSelect:
    def __init__(self, rows):
        self._rows = rows
        self.gino = self

    def where(self, *_args, **_kwargs):
        return self

    async def scalar(self):
        return self._rows


class FakeAttribute:
    def __init__(self, full_plan_id, attr_name, attr_value):
        self.full_plan_id = full_plan_id
        self.attr_name = attr_name
        self.attr_value = attr_value


class FakeBenefit:
    def __init__(
        self,
        full_plan_id,
        benefit_name,
        plan_id,
        year,
        copay_inn_tier1="No Charge",
        coins_inn_tier1="No Charge",
        copay_inn_tier2="Not Applicable",
        coins_inn_tier2="Not Applicable",
        copay_outof_net="Not Applicable",
        coins_outof_net="Not Applicable",
    ):
        self.full_plan_id = full_plan_id
        self.benefit_name = benefit_name
        self.plan_id = plan_id
        self.year = year
        self.copay_inn_tier1 = copay_inn_tier1
        self.coins_inn_tier1 = coins_inn_tier1
        self.copay_inn_tier2 = copay_inn_tier2
        self.coins_inn_tier2 = coins_inn_tier2
        self.copay_outof_net = copay_outof_net
        self.coins_outof_net = coins_outof_net

    def to_json_dict(self):
        return {
            "full_plan_id": self.full_plan_id,
            "benefit_name": self.benefit_name,
            "plan_id": self.plan_id,
            "year": self.year,
            "copay_inn_tier1": self.copay_inn_tier1,
            "coins_inn_tier1": self.coins_inn_tier1,
            "copay_inn_tier2": self.copay_inn_tier2,
            "coins_inn_tier2": self.coins_inn_tier2,
            "copay_outof_net": self.copay_outof_net,
            "coins_outof_net": self.coins_outof_net,
        }


class FakeQuery:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.gino = self

    def where(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def offset(self, *_args, **_kwargs):
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    async def all(self):
        return self._rows

    async def first(self):
        return self._rows[0] if self._rows else None

    async def iterate(self):
        for row in self._rows:
            yield row


class FakeSelect:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.gino = self

    def select_from(self, *_args, **_kwargs):
        return self

    def where(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def offset(self, *_args, **_kwargs):
        return self

    def group_by(self, *_args, **_kwargs):
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def load(self, *_args, **_kwargs):
        return FakeLoaderResult(self._rows)

    async def all(self):
        return self._rows

    async def scalar(self):
        return None


class FakeSelectWithAlias(FakeSelect):
    def alias(self, _name):
        return FakeAlias()


class FakeLoaderResult:
    def __init__(self, rows):
        self._rows = rows

    async def iterate(self):
        for row in self._rows:
            yield row


class FakeAlias:
    def __init__(self):
        self.c = types.SimpleNamespace(
            found_plan_id="found_plan_id",
            year="year",
            min_individual_rate="min_individual_rate",
            max_individual_rate="max_individual_rate",
        )


class FakeCountSelect:
    def __init__(self, value):
        self.value = value
        self.gino = self

    def where(self, *_args, **_kwargs):
        return self

    async def scalar(self):
        return self.value


class FakeIssuerSelect:
    def __init__(self, value):
        self.value = value
        self.gino = self

    def where(self, *_args, **_kwargs):
        return self

    async def scalar(self):
        return self.value


@pytest.mark.asyncio
async def test_get_network_by_checksum(monkeypatch):
    async def fake_fetch(checksum):
        return {"checksum": checksum, "plans": []}

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)

    request = types.SimpleNamespace()
    response = await get_network_by_checksum(request, "123")
    payload = json.loads(response.body)
    assert payload["checksum"] == 123


@pytest.mark.asyncio
async def test_get_networks_by_checksums(monkeypatch):
    async def fake_fetch(checksum):
        return {"checksum": checksum, "plans": []}

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)

    request = types.SimpleNamespace()
    response = await get_networks_by_checksums(request, "1,2,1")
    payload = json.loads(response.body)
    assert [entry["checksum"] for entry in payload] == [1, 2]


@pytest.mark.asyncio
async def test_get_networks_by_checksums_not_found(monkeypatch):
    async def fake_fetch(_checksum):
        return None

    monkeypatch.setattr("api.endpoint.plan._fetch_network_entry", fake_fetch)

    request = types.SimpleNamespace()
    with pytest.raises(sanic.exceptions.NotFound):
        await get_networks_by_checksums(request, "1,2")


@pytest.mark.asyncio
async def test_plan_index_status(monkeypatch):
    monkeypatch.setattr(plan_module.db, "func", FakeFunc([5, 3, 2]))

    monkeypatch.setattr(plan_module.db, "acquire", lambda: FakeAcquire())

    request = types.SimpleNamespace(app=types.SimpleNamespace(config={"RELEASE": "dev", "ENVIRONMENT": "test"}))
    response = await index_status(request)
    payload = json.loads(response.body)
    assert payload["plan_count"] == 5
    assert payload["import_log_errors"] == 3
    assert payload["plans_network_count"] == 2


@pytest.mark.asyncio
async def test_plan_all_plans(monkeypatch):
    monkeypatch.setattr(plan_module.Plan, "query", FakeQuery([FakePlan({"plan_id": "123"})]))
    response = await all_plans(types.SimpleNamespace())
    payload = json.loads(response.body)
    assert payload == [{"plan_id": "123"}]


@pytest.mark.asyncio
async def test_plan_all_plans_variants(monkeypatch):
    monkeypatch.setattr(plan_module.db, "select", lambda *_args, **_kwargs: FakeSelect([
        ("Plan", "123", "123-00", 2024)
    ]))
    response = await all_plans_variants(types.SimpleNamespace(args={}))
    payload = json.loads(response.body)
    assert payload == [
        {"marketing_name": "Plan", "plan_id": "123", "full_plan_id": "123-00", "year": 2024}
    ]


@pytest.mark.asyncio
async def test_plan_get_autocomplete_empty(monkeypatch):
    monkeypatch.setattr(plan_module.Plan, "query", FakeQuery([FakePlan({"plan_id": "P1", "network": {}})]))
    monkeypatch.setattr(plan_module.db, "select", lambda *_args, **_kwargs: FakeSelect())
    response = await get_autocomplete_list(types.SimpleNamespace(args={"query": "Silver"}))
    payload = json.loads(response.body)
    assert payload == {"plans": []}


@pytest.mark.asyncio
async def test_plan_get_autocomplete_success(monkeypatch):
    plans = [
        FakePlan({"plan_id": "P123", "marketing_name": "Alpha", "year": 2024, "issuer_id": 1}),
        FakePlan({"plan_id": "P456", "marketing_name": "Beta", "year": 2024, "issuer_id": 2}),
    ]
    monkeypatch.setattr(plan_module.Plan, "query", FakeQuery(plans))
    monkeypatch.setattr(
        plan_module.db,
        "select",
        lambda *_args, **_kwargs: FakeNetworkSelect([
            ("P123", 111, "GOLD"),
            ("P123", 222, "SILVER"),
            ("P456", 333, "BRONZE"),
        ]),
    )

    response = await get_autocomplete_list(types.SimpleNamespace(args={"query": "plan"}))
    payload = json.loads(response.body)
    assert len(payload["plans"]) == 2
    lookup = {item["plan_id"]: item for item in payload["plans"]}
    assert lookup["P123"]["network_checksum"] == {"111": "GOLD", "222": "SILVER"}
    assert lookup["P456"]["network_checksum"] == {"333": "BRONZE"}


def test_plan_find_plan_bad_year():
    request = types.SimpleNamespace(args={"year": "bad"})
    with pytest.raises(sanic.exceptions.BadRequest):
        asyncio.get_event_loop().run_until_complete(find_a_plan(request))


def test_plan_get_price_plan_bad_age():
    request = types.SimpleNamespace(args={"age": "bad"})
    with pytest.raises(sanic.exceptions.BadRequest):
        asyncio.get_event_loop().run_until_complete(get_price_plan(request, "123"))


@pytest.mark.asyncio
async def test_plan_get_plan_not_found(monkeypatch):
    monkeypatch.setattr(plan_module.Plan, "query", FakeQuery([]))
    with pytest.raises(sanic.exceptions.NotFound):
        await get_plan(types.SimpleNamespace(), "123")


@pytest.mark.asyncio
async def test_plan_get_price_plan_success(monkeypatch):
    price_records = [
        FakePrice({"plan_id": "P123", "year": 2024, "min_age": 21, "max_age": 64, "rating_area_id": "A", "individual_rate": 199.99})
    ]
    monkeypatch.setattr(plan_module.PlanPrices, "query", FakeQuery(price_records))
    monkeypatch.setattr(plan_module.db, "acquire", lambda: FakeAcquire())

    request = types.SimpleNamespace(args={"age": "30", "rating_area": "A"})
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
async def test_plan_get_plan_success(monkeypatch):
    plan_payload = {"plan_id": "P123", "year": 2024, "issuer_id": 42, "state": "TX"}
    monkeypatch.setattr(plan_module.Plan, "query", FakeQuery([FakePlan(plan_payload)]))
    monkeypatch.setattr(plan_module.PlanFormulary, "query", FakeQuery([
        FakeFormulary({"plan_id": "P123", "year": 2024, "drug_tier": "Tier 1", "pharmacy_type": "Retail"})
    ]))
    monkeypatch.setattr(plan_module.Issuer, "select", lambda *_args, **_kwargs: FakeIssuerSelect("Sample Issuer"))

    select_sequence = iter([
        FakeScalar([(77777, "PREFERRED")])
    ])
    monkeypatch.setattr(plan_module.db, "select", lambda *_args, **_kwargs: next(select_sequence))

    response = await get_plan(types.SimpleNamespace(), "P123")
    payload = json.loads(response.body)
    assert payload["plan_id"] == "P123"
    assert payload["network_checksum"] == {"77777": "PREFERRED"}
    assert payload["issuer_name"] == "Sample Issuer"
    assert payload["formulary"][0]["drug_tier"] == "Tier 1"


@pytest.mark.asyncio
async def test_plan_find_plan_success(monkeypatch):
    plan = FakePlan({"plan_id": "P123", "year": 2024, "issuer_id": 42, "state": "TX"})
    attribute = FakeAttribute("P123-00", "Coverage", "Standard")
    benefit = FakeBenefit("P123-00", "Primary Care Visit", "P123", 2024)

    monkeypatch.setattr(plan_module.Plan, "join", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(plan_module.PlanAttributes, "query", FakeQuery([attribute]))
    monkeypatch.setattr(plan_module.PlanBenefits, "query", FakeQuery([benefit]))
    monkeypatch.setattr(plan_module.db, "acquire", lambda: FakeAcquire())

    select_sequence = iter([
        FakeSelectWithAlias(),
        FakeCountSelect(1),
        FakeSelect([(plan, 100.0, 200.0)]),
    ])
    monkeypatch.setattr(plan_module.db, "select", lambda *_args, **_kwargs: next(select_sequence))

    request = types.SimpleNamespace(args={
        "year": "2024",
        "age": "30",
        "rating_area": "A",
        "limit": "1",
        "page": "1",
    })

    response = await find_a_plan(request)
    payload = json.loads(response.body)
    assert payload["total"] == 1
    result = payload["results"][0]
    assert result["plan_id"] == "P123"
    assert result["price_range"] == {"min": 100.0, "max": 200.0}
    assert result["attributes"]["Coverage"] == "Standard"
    benefit_entry = result["plan_benefits"]["Primary Care Visit"]
    assert benefit_entry["benefit_name"] == "Primary Care Visit"
    assert benefit_entry["copay_inn_tier1"] == "No Charge"
