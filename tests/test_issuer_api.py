# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest
import sanic.exceptions

from api.endpoint.issuer import get_issuer_data, get_issuers, _row_to_dict


class FakeRow(dict):
    @property
    def _mapping(self):
        return self


class FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = rows or []
        self._scalar_value = scalar_value

    def first(self):
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)

    def scalar(self):
        return self._scalar_value

    def all(self):
        return list(self._rows)


class FakeSession:
    def __init__(self, results):
        self._results = list(results)

    async def execute(self, *_args, **_kwargs):
        if not self._results:
            return FakeResult()
        return self._results.pop(0)


@pytest.mark.asyncio
async def test_get_issuer_data_not_found(monkeypatch):
    with pytest.raises(sanic.exceptions.NotFound):
        await get_issuer_data(
            types.SimpleNamespace(
                ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult([])]))
            ),
            "999",
        )


@pytest.mark.asyncio
async def test_get_issuer_data(monkeypatch):
    issuer_row = FakeRow({"issuer_id": 1001, "issuer_name": "Issuer", "state": "WI"})
    plan_row = FakeRow(
        {
            "plan_id": "P123",
            "year": 2024,
            "issuer_id": 1001,
            "state": "WI",
            "plan_id_type": "HMO",
            "marketing_name": "Sample Plan",
            "summary_url": None,
            "marketing_url": None,
            "formulary_url": None,
            "plan_contact": None,
            "network": ["A"],
            "benefits": None,
            "last_updated_on": None,
            "checksum": 1,
            "network_checksum": 999,
            "network_tier_value": "PREFERRED",
        }
    )
    stats_row = FakeRow(
        {
            "total_drugs": 10,
            "auth_required": 4,
            "auth_not_required": 6,
            "step_required": 2,
            "step_not_required": 8,
            "quantity_limit": 3,
            "quantity_no_limit": 7,
        }
    )
    tier_rows = [("Tier 1", 6)]

    session = FakeSession(
        [
            FakeResult([issuer_row]),
            FakeResult(scalar_value=2),
            FakeResult([plan_row]),
            FakeResult([stats_row]),
            FakeResult(tier_rows),
        ]
    )

    response = await get_issuer_data(
        types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=session)), "1001"
    )
    payload = json.loads(response.body)
    assert payload["import_errors"] == 2
    assert payload["plans_count"] == 1
    assert payload["drug_summary"]["total_drugs"] == 10
    assert payload["drug_summary"]["quantity_limits"]["has_limit"] == 3
    assert payload["drug_summary"]["tiers"][0]["tier_slug"] == "tier_1"


@pytest.mark.asyncio
async def test_get_issuers_not_found(monkeypatch):
    with pytest.raises(sanic.exceptions.NotFound):
        await get_issuers(
            types.SimpleNamespace(
                args={}, ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult([])]))
            ),
            None,
        )


@pytest.mark.asyncio
async def test_get_issuers(monkeypatch):
    issuer_rows = [FakeRow({"issuer_id": 1, "issuer_name": "A", "state": "WI"}), FakeRow({"issuer_id": 2, "issuer_name": "B", "state": "WI"})]
    error_rows = [(1, 2), (2, 3)]
    plan_rows = [(1, 5), (2, 6)]

    session = FakeSession(
        [
            FakeResult(issuer_rows),
            FakeResult(error_rows),
            FakeResult(plan_rows),
        ]
    )

    response = await get_issuers(
        types.SimpleNamespace(args={}, ctx=types.SimpleNamespace(sa_session=session)), None
    )
    payload = json.loads(response.body)
    assert len(payload) == 2
    assert payload[0]["import_errors"] == 2
    assert payload[1]["plan_count"] == 6


@pytest.mark.asyncio
async def test_get_issuers_with_state():
    issuer_rows = [FakeRow({"issuer_id": 1, "issuer_name": "A", "state": "WI"})]
    error_rows = [(1, 2)]
    plan_rows = [(1, 5)]
    session = FakeSession(
        [FakeResult(issuer_rows), FakeResult(error_rows), FakeResult(plan_rows)]
    )
    response = await get_issuers(
        types.SimpleNamespace(args={}, ctx=types.SimpleNamespace(sa_session=session)), "wi"
    )
    payload = json.loads(response.body)
    assert payload[0]["plan_count"] == 5
    assert payload[0]["import_errors"] == 2



@pytest.mark.asyncio
async def test_get_issuer_data_missing_session():
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=None))
    with pytest.raises(RuntimeError):
        await get_issuer_data(request, "1")


def test_row_to_dict_plain_dict():
    assert _row_to_dict({"k": "v"}) == {"k": "v"}


@pytest.mark.asyncio
async def test_get_issuers_missing_session():
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=None), args={})
    with pytest.raises(RuntimeError):
        await get_issuers(request, None)
