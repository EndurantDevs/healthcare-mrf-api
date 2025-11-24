# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest
from asyncpg import UndefinedTableError
from sanic.exceptions import InvalidUsage
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
    def __init__(self, row=None, rows=None):
        self._row = row
        self._rows = rows

    def first(self):
        return self._row

    def all(self):
        if self._rows is not None:
            return self._rows
        return [] if self._row is None else [self._row]


class FakeSession:
    def __init__(self, responses=None):
        self._responses = list(responses or [])

    async def execute(self, *_args, **_kwargs):
        if not self._responses:
            return FakeResult()
        result = self._responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class MappingRow:
    def __init__(self, **mapping):
        self._mapping = mapping


@pytest.mark.asyncio
async def test_geo_by_zip_success():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession([FakeResult(row=MappingRow(zip_code="12345", city="Chicago", state="IL", latitude=41.1, longitude=-87.6))])
        ),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "12345")
    payload = json.loads(response.body)
    assert payload["zip_code"] == "12345"
    assert payload["lat"] == 41.1
    assert payload["city"] == "Chicago"


@pytest.mark.asyncio
async def test_geo_by_zip_not_found():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(row=None), FakeResult(row=None)])),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "00000")
    assert response.status == 404


@pytest.mark.asyncio
async def test_geo_by_zip_bad_row():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession([
            FakeResult(row=None),
            FakeResult(row=("99999", "not-a-number", None, "NY")),
        ])),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "99999")
    assert response.status == 404


@pytest.mark.asyncio
async def test_geo_by_zip_missing_table():
    error = ProgrammingError("select", {}, None)
    error.orig = UndefinedTableError("tiger schema")
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(row=None), error])),
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
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(row=None), error])),
        app=types.SimpleNamespace(),
    )
    with pytest.raises(ProgrammingError):
        await geo_module.get_geo(request, "12345")


@pytest.mark.asyncio
async def test_geo_states_invalid_sort():
    request = types.SimpleNamespace(
        args={"sort": "invalid"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([])),
    )
    with pytest.raises(InvalidUsage):
        await geo_module.list_geo_states(request)


def test_row_mapping_and_serialize_helpers():
    assert geo_module._row_mapping({"zip_code": "123"})["zip_code"] == "123"

    class RowIterable:
        def __iter__(self):
            return iter([("zip_code", "99999"), ("city", "Foo")])

    mapping = geo_module._row_mapping(RowIterable())
    assert mapping["zip_code"] == "99999"
    assert geo_module._serialize_geo_row(None) is None


@pytest.mark.asyncio
async def test_geo_by_zip_local_lat_long_fallback():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession([
                FakeResult(
                    row=MappingRow(
                        zip_code="11111",
                        city="City",
                        state="IL",
                        latitude=None,
                        longitude=None,
                        state_name="Illinois",
                        county_name="Cook",
                        timezone="CST",
                    )
                )
            ])
        ),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "11111")
    payload = json.loads(response.body)
    assert payload["zip_code"] == "11111"
    assert payload["lat"] is None
    assert payload["long"] is None


@pytest.mark.asyncio
async def test_geo_by_zip_tiger_fallback():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession([
                FakeResult(row=None),
                FakeResult(row=("22222", "41.0", "-87.0", "IL")),
            ])
        ),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "22222")
    payload = json.loads(response.body)
    assert payload == {"zip_code": "22222", "lat": 41.0, "long": -87.0, "state": "IL"}


@pytest.mark.asyncio
async def test_geo_city_missing_param():
    request = types.SimpleNamespace(args={}, ctx=types.SimpleNamespace(sa_session=FakeSession([])))
    with pytest.raises(InvalidUsage):
        await geo_module.get_geo_by_city(request)


@pytest.mark.asyncio
async def test_geo_city_with_state_filter():
    rows = [
        MappingRow(zip_code="73301", city="Austin", state="TX", state_name="Texas", county_name="Travis", latitude=30.3, longitude=-97.7, timezone="CST"),
        MappingRow(zip_code="90001", city="Austin", state="CA", state_name="California", county_name="L.A.", latitude=34.0, longitude=-118.2, timezone="PST"),
    ]
    request = types.SimpleNamespace(
        args={"city": "Austin", "state": "tx"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(rows=rows)])),
    )
    response = await geo_module.get_geo_by_city(request)
    payload = json.loads(response.body)
    assert payload["state"] == "TX"


@pytest.mark.asyncio
async def test_geo_states_invalid_order():
    request = types.SimpleNamespace(
        args={"order": "invalid"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([])),
    )
    with pytest.raises(InvalidUsage):
        await geo_module.list_geo_states(request)


@pytest.mark.asyncio
async def test_geo_states_invalid_limit():
    request = types.SimpleNamespace(
        args={"limit": "bad"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([])),
    )
    with pytest.raises(InvalidUsage):
        await geo_module.list_geo_states(request)


@pytest.mark.asyncio
async def test_geo_states_invalid_top_zip_limit():
    request = types.SimpleNamespace(
        args={"top_zip_limit": "bad"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([])),
    )
    with pytest.raises(InvalidUsage):
        await geo_module.list_geo_states(request)


@pytest.mark.asyncio
async def test_geo_states_sorted_asc_with_limit_and_skip_missing_top_zip():
    state_rows = [
        MappingRow(
            state="TX",
            state_name="Texas",
            zip_count=10,
            city_count=5,
            population=1000,
            avg_lat=31.0,
            avg_long=-100.0,
        )
    ]
    top_zip_rows = [
        MappingRow(state=None, zip_code="00000", city="N/A", population=0, lat=0, long=0),
        MappingRow(state="TX", zip_code="73301", city="Austin", population=500, lat=30.3, long=-97.7),
    ]
    request = types.SimpleNamespace(
        args={"order": "asc", "limit": "1", "top_zip_limit": "2"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(rows=state_rows), FakeResult(rows=top_zip_rows)])),
    )
    response = await geo_module.list_geo_states(request)
    payload = json.loads(response.body)
    assert payload["states"][0]["state"] == "TX"
    assert payload["states"][0]["top_zips"][0]["zip_code"] == "73301"


@pytest.mark.asyncio
async def test_geo_state_invalid_length():
    request = types.SimpleNamespace(args={}, ctx=types.SimpleNamespace(sa_session=FakeSession([])))
    with pytest.raises(InvalidUsage):
        await geo_module.get_top_cities_by_state(request, "ABC")


@pytest.mark.asyncio
async def test_geo_state_invalid_limit():
    request = types.SimpleNamespace(args={"limit": "bad"}, ctx=types.SimpleNamespace(sa_session=FakeSession([])))
    with pytest.raises(InvalidUsage):
        await geo_module.get_top_cities_by_state(request, "CA")
