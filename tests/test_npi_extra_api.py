# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

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


@pytest.mark.asyncio
async def test_active_pharmacists_invalid_state(monkeypatch):
    fake_conn = FakeConnection(first_result=(0,))
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={"state": "Texas"})
    response = await active_pharmacists(request)
    assert json.loads(response.body) == {"count": 0}


@pytest.mark.asyncio
async def test_pharmacists_per_pharmacy_invalid_state(monkeypatch):
    fake_conn = FakeConnection(all_result=[("1", 5)])
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={"state": "California"})
    response = await pharmacists_per_pharmacy(request)
    assert json.loads(response.body) == [{"pharmacist_group": "1", "pharmacy_count": 5}]


@pytest.mark.asyncio
async def test_pharmacists_per_pharmacy_state_and_name(monkeypatch):
    captured = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            captured['sql'] = sql
            captured['params'] = params
            return [("1", 2)]

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module.db, 'acquire', lambda: FakeAcquire(RecordingConnection()))

    request = types.SimpleNamespace(args={'state': 'ny', 'name_like': 'clinic'})
    response = await pharmacists_per_pharmacy(request)
    payload = json.loads(response.body)

    assert payload[0]['pharmacist_group'] == '1'
    assert captured['params'] == {'state': 'NY', 'name_like': '%clinic%'}
    sql_text = str(captured['sql'])
    assert 'ph.state_name = :state' in sql_text
    assert 'LIKE :name_like' in sql_text


@pytest.mark.asyncio
async def test_pharmacists_per_pharmacy_full_groups(monkeypatch):
    expected_groups = ['25+'] + [str(i) for i in range(25, 0, -1)]
    fake_rows = [(group, idx) for idx, group in enumerate(expected_groups, start=1)]

    fake_conn = FakeConnection(all_result=fake_rows)
    monkeypatch.setattr(npi_module.db, 'acquire', lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={})
    response = await pharmacists_per_pharmacy(request)
    payload = json.loads(response.body)

    assert [entry['pharmacist_group'] for entry in payload] == expected_groups
    assert [entry['pharmacy_count'] for entry in payload] == list(range(1, len(expected_groups) + 1))