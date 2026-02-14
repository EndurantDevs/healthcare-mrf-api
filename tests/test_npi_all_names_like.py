# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module
from api.endpoint.npi import get_all


class RecordingConnection:
    def __init__(self):
        self.calls = 0
        self.last_sql = None
        self.last_params = None
        self.sql_calls = []

    async def all(self, sql, **params):
        self.calls += 1
        self.last_sql = str(sql)
        self.last_params = params
        self.sql_calls.append((str(sql), dict(params)))
        # first call: count; second call: results
        if self.calls == 1:
            return [(5,)]
        return []

    async def first(self, *_args, **_kwargs):
        return None


class FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_get_all_q_filter(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "classification": "Pharmacy",
            "state": "RI",
            "q": "CVS Pharmacy",
            "has_insurance": "1",
            "limit": "0",
            "start": "0",
        }
    )
    resp = await get_all(request)
    data = json.loads(resp.body)
    assert data["total"] == 5  # count from first call
    assert data["page"] == 1
    assert data["limit"] == 50
    assert data["offset"] == 0
    assert "b.npi" in conn.last_sql and "name_like_0" in conn.last_params
    assert conn.last_params["limit"] > 0  # limit expanded from 0


@pytest.mark.asyncio
async def test_get_all_count_only_supports_response_format_alias(monkeypatch):
    class MappingConnection:
        async def all(self, *_args, **_kwargs):
            return [("taxonomy_a", 3), ("taxonomy_b", 7)]

        async def first(self, *_args, **_kwargs):
            return None

    conn = MappingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={"count_only": "1", "response_format": "full_taxonomy"}
    )
    resp = await get_all(request)
    data = json.loads(resp.body)
    assert data["rows"]["taxonomy_a"] == 3
    assert data["rows"]["taxonomy_b"] == 7


@pytest.mark.asyncio
async def test_get_all_q_alias_matches_provider_name(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "Ryan James Pasiewicz",
            "city": "chicago",
            "limit": "5",
            "start": "0",
        }
    )
    resp = await get_all(request)
    data = json.loads(resp.body)
    assert data["total"] == 5
    assert "name_like_0" in conn.last_params
    assert conn.last_params["name_like_0"] == "%ryan james pasiewicz%"
    assert "EXISTS (SELECT 1 FROM mrf.npi AS b" in conn.last_sql


@pytest.mark.asyncio
async def test_get_all_accepts_name_like_legacy_alias(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(args={"name_like": "clinic", "limit": "5", "start": "0"})
    resp = await get_all(request)
    data = json.loads(resp.body)

    assert data["total"] == 5
    assert "name_like_0" in conn.last_params
    assert conn.last_params["name_like_0"] == "%clinic%"


@pytest.mark.asyncio
async def test_get_all_without_taxonomy_filters_skips_expensive_overlap(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "clinic",
            "city": "chicago",
            "limit": "5",
            "start": "0",
        }
    )
    await get_all(request)

    assert conn.sql_calls
    assert all("taxonomy_array && q.int_codes" not in sql for sql, _ in conn.sql_calls)


@pytest.mark.asyncio
async def test_get_all_zip_phone_and_name_filters_are_applied(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "walgreen",
            "zip_code": "60601-1234",
            "phone": "(312) 555-1212",
            "first_name": "john",
            "last_name": "doe",
            "organization_name": "walgreens",
            "entity_type_code": "2",
            "limit": "5",
            "start": "0",
        }
    )
    await get_all(request)

    assert conn.last_params["zip_code"] == "60601"
    assert conn.last_params["phone_digits"] == "3125551212"
    assert conn.last_params["first_name"] == "%john%"
    assert conn.last_params["last_name"] == "%doe%"
    assert conn.last_params["organization_name"] == "%walgreens%"
    assert conn.last_params["entity_type_code"] == 2
    assert "LEFT(c.postal_code, 5) = :zip_code" in conn.last_sql
    assert "regexp_replace(COALESCE(c.telephone_number, ''), '[^0-9]', '', 'g') = :phone_digits" in conn.last_sql


@pytest.mark.asyncio
async def test_get_all_rejects_invalid_entity_type_code(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(args={"q": "walgreen", "entity_type_code": "7"})
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await get_all(request)


@pytest.mark.asyncio
async def test_get_all_rejects_conflicting_zip_aliases(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "walgreen",
            "zip_code": "60601",
            "postal_code": "60602",
        }
    )
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await get_all(request)
