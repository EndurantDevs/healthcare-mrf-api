import json
import types

import pytest
import sanic.exceptions

from api.endpoint import issuer as issuer_module
from api.endpoint.issuer import get_issuer_data, get_issuers


class FakeIssuer:
    def __init__(self, payload):
        self._payload = payload

    def to_json_dict(self):
        return dict(self._payload)


class FakeQuery:
    def __init__(self, rows):
        self._rows = rows
        self.gino = self

    def where(self, *_args, **_kwargs):
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    async def first(self):
        return self._rows[0] if self._rows else None

    async def all(self):
        return self._rows


class FakeScalarSelect:
    def __init__(self, value):
        self.value = value
        self.gino = self

    def where(self, *_args, **_kwargs):
        return self

    async def scalar(self):
        return self.value


class FakeAllSelect:
    def __init__(self, rows):
        self.rows = rows
        self.gino = self

    def where(self, *_args, **_kwargs):
        return self

    def group_by(self, *_args, **_kwargs):
        return self

    async def all(self):
        return self.rows


class FakeLoader:
    def __init__(self, rows):
        self.rows = rows

    def select_from(self, *_args, **_kwargs):
        return self

    def where(self, *_args, **_kwargs):
        return self

    def group_by(self, *_args, **_kwargs):
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    @property
    def gino(self):
        return self

    def load(self, *_args, **_kwargs):
        return self

    async def iterate(self):
        for row in self.rows:
            yield row


@pytest.mark.asyncio
async def test_get_issuer_data_not_found(monkeypatch):
    monkeypatch.setattr(issuer_module.Issuer, "query", FakeQuery([]))
    with pytest.raises(sanic.exceptions.NotFound):
        await get_issuer_data(types.SimpleNamespace(), "999")


@pytest.mark.asyncio
async def test_get_issuer_data(monkeypatch):
    fake_plan = FakeIssuer({"plan_id": "P123", "year": 2024, "network": {}})
    monkeypatch.setattr(issuer_module.Issuer, "query", FakeQuery([FakeIssuer({"issuer_id": 1001})]))

    select_sequence = iter([
        FakeScalarSelect(2),
        FakeLoader([(fake_plan, None)]),
    ])

    class FakeAcquire:
        class _FakeConn:
            class _Txn:
                async def __aenter__(self):
                    return None

                async def __aexit__(self, exc_type, exc, tb):
                    return False

            def transaction(self):
                return self._Txn()

        async def __aenter__(self):
            return self._FakeConn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(issuer_module.db, "select", lambda *_args, **_kwargs: next(select_sequence))
    monkeypatch.setattr(issuer_module.db, "acquire", lambda: FakeAcquire())

    response = await get_issuer_data(types.SimpleNamespace(), "1001")
    payload = json.loads(response.body)
    assert payload["import_errors"] == 2
    assert payload["plans_count"] == 1


@pytest.mark.asyncio
async def test_get_issuers_not_found(monkeypatch):
    monkeypatch.setattr(issuer_module.Issuer, "query", FakeQuery([]))
    monkeypatch.setattr(issuer_module.db, "select", lambda *_args, **_kwargs: FakeAllSelect([]))
    with pytest.raises(sanic.exceptions.NotFound):
        await get_issuers(types.SimpleNamespace(args={}), None)


@pytest.mark.asyncio
async def test_get_issuers(monkeypatch):
    rows = [FakeIssuer({"issuer_id": 1}), FakeIssuer({"issuer_id": 2})]
    monkeypatch.setattr(issuer_module.Issuer, "query", FakeQuery(rows))

    select_sequence = iter([
        FakeAllSelect([(1, 2), (2, 3)]),
        FakeAllSelect([(1, 5), (2, 6)]),
    ])

    monkeypatch.setattr(issuer_module.db, "select", lambda *_args, **_kwargs: next(select_sequence))

    response = await get_issuers(types.SimpleNamespace(args={}), None)
    payload = json.loads(response.body)
    assert len(payload) == 2
    assert payload[0]["import_errors"] == 2
    assert payload[1]["plan_count"] == 6
