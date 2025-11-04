import json
import types

import pytest

from api.endpoint import npi as npi_module
from api.endpoint.npi import (
    active_pharmacists,
    pharmacists_per_pharmacy,
    pharmacists_in_pharmacies,
)


class FakeConnection:
    def __init__(self, first_result=None, all_result=None):
        self._first = first_result
        self._all = all_result

    async def first(self, *_args, **_kwargs):
        return self._first

    async def all(self, *_args, **_kwargs):
        return self._all


class FakeAcquire:
    def __init__(self, connection):
        self._connection = connection

    async def __aenter__(self):
        return self._connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_active_pharmacists(monkeypatch):
    fake_conn = FakeConnection(first_result=(10,))
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={"state": "TX"})
    response = await active_pharmacists(request)
    assert json.loads(response.body) == {"count": 10}


@pytest.mark.asyncio
async def test_pharmacists_per_pharmacy(monkeypatch):
    fake_conn = FakeConnection(all_result=[("1", 5)])
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={})
    response = await pharmacists_per_pharmacy(request)
    assert json.loads(response.body) == [{"pharmacist_group": "1", "pharmacy_count": 5}]


@pytest.mark.asyncio
async def test_pharmacists_in_pharmacies(monkeypatch):
    fake_conn = FakeConnection(first_result=(7,))
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={"name_like": "clinic"})
    response = await pharmacists_in_pharmacies(request)
    assert json.loads(response.body) == {"count": 7}


@pytest.mark.asyncio
async def test_pharmacists_in_pharmacies_blank():
    request = types.SimpleNamespace(args={"name_like": ""})
    response = await pharmacists_in_pharmacies(request)
    assert json.loads(response.body) == {"count": 0}
