# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest
from asyncpg import UndefinedTableError
from sqlalchemy.exc import ProgrammingError

from api.endpoint import geo as geo_module


@pytest.mark.asyncio
async def test_geo_index_handler():
    handler = next(route.handler for route in geo_module.blueprint._future_routes if route.uri == "/get")
    request = types.SimpleNamespace(
        args={"zip_code": "60601", "lat": "41.0", "long": "-87.0"},
        app=types.SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "dev"}),
    )
    response = await handler(request)
    payload = json.loads(response.body)
    assert payload["release"] == "test"
    assert payload["environment"] == "dev"
    assert "date" in payload


class FakeResult:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class FakeSession:
    def __init__(self, row=None, error=None):
        self._row = row
        self._error = error

    async def execute(self, *_args, **_kwargs):
        if self._error:
            raise self._error
        return FakeResult(self._row)


@pytest.mark.asyncio
async def test_geo_by_zip_success():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession(("12345", "41.1", "-87.6", "IL"))),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "12345")
    payload = json.loads(response.body)
    assert payload == {"zip_code": "12345", "lat": 41.1, "long": -87.6, "state": "IL"}


@pytest.mark.asyncio
async def test_geo_by_zip_not_found():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession(None)),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "00000")
    assert response.status == 404


@pytest.mark.asyncio
async def test_geo_by_zip_bad_row():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession(("99999", "not-a-number", None, "NY"))),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "99999")
    assert response.status == 404


@pytest.mark.asyncio
async def test_geo_by_zip_missing_table():
    error = ProgrammingError("select", {}, None)
    error.orig = UndefinedTableError("tiger schema")
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession(error=error)),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "12345")
    assert response.status == 503
    assert json.loads(response.body)["error"] == "tiger schema not available"


@pytest.mark.asyncio
async def test_geo_by_zip_missing_session():
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=None), app=types.SimpleNamespace())
    with pytest.raises(RuntimeError):
        await geo_module.get_geo(request, "12345")


@pytest.mark.asyncio
async def test_geo_by_zip_other_programming_error():
    error = ProgrammingError("select", {}, None)
    error.orig = Exception("other")
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=FakeSession(error=error)), app=types.SimpleNamespace())
    with pytest.raises(ProgrammingError):
        await geo_module.get_geo(request, "12345")