# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest

from api.endpoint.geo import (
    get_geo as get_geo_zip,
    get_geo_by_city,
    get_top_cities_by_state,
    list_geo_states,
)
import types


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
async def test_geo_zip_not_found():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(row=None), FakeResult(row=None)]))
    )
    response = await get_geo_zip(request, "99999")
    payload = json.loads(response.body)
    assert payload["error"] == "Not found"


@pytest.mark.asyncio
async def test_geo_zip_success():
    row = ("12345", "43.12", "-89.45", "WI")
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession([
            FakeResult(row=MappingRow(zip_code="12345", city="Test City", state="WI", latitude=43.12, longitude=-89.45, state_name="Wisconsin", county_name="Dane", timezone="America/Chicago"))
        ]))
    )
    response = await get_geo_zip(request, "12345")
    payload = json.loads(response.body)
    assert payload == {
        "zip_code": "12345",
        "lat": 43.12,
        "long": -89.45,
        "state": "WI",
        "city": "Test City",
        "state_name": "Wisconsin",
        "county_name": "Dane",
        "timezone": "America/Chicago",
    }


@pytest.mark.asyncio
async def test_geo_city_lookup_success():
    rows = [
        MappingRow(zip_code="99501", city="Anchorage", state="AK", state_name="Alaska", county_name="Anchorage", latitude=61.2175, longitude=-149.8584, timezone="America/Anchorage"),
        MappingRow(zip_code="99502", city="Anchorage", state="AK", state_name="Alaska", county_name="Anchorage", latitude=61.1538, longitude=-149.9985, timezone="America/Anchorage"),
    ]
    request = types.SimpleNamespace(
        args={"city": "Anchorage"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(rows=rows)])),
    )
    response = await get_geo_by_city(request)
    payload = json.loads(response.body)
    assert payload["normalized_city"] == "Anchorage"
    assert len(payload["items"]) == 2


@pytest.mark.asyncio
async def test_geo_city_lookup_not_found():
    request = types.SimpleNamespace(
        args={"city": "Nowhere"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(rows=[])])),
    )
    response = await get_geo_by_city(request)
    assert response.status == 404


@pytest.mark.asyncio
async def test_geo_states_summary_success():
    state_rows = [
        MappingRow(
            state="AK",
            state_name="Alaska",
            zip_count=5,
            city_count=2,
            population=100000,
            avg_lat=61.1,
            avg_long=-149.9,
        )
    ]
    top_zip_rows = [
        MappingRow(
            state="AK",
            zip_code="99501",
            city="Anchorage",
            population=50000,
            lat=61.2,
            long=-149.9,
        )
    ]
    request = types.SimpleNamespace(
        args={"top_zip_limit": "1"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(rows=state_rows), FakeResult(rows=top_zip_rows)])),
    )
    response = await list_geo_states(request)
    payload = json.loads(response.body)
    assert payload["states"][0]["state"] == "AK"
    assert payload["states"][0]["top_zips"][0]["zip_code"] == "99501"


@pytest.mark.asyncio
async def test_geo_state_top_cities_success():
    rows = [
        MappingRow(city="Anchorage", state="AK", zip_count=10, population=100000, avg_lat=61.2, avg_long=-149.9),
        MappingRow(city="Fairbanks", state="AK", zip_count=5, population=30000, avg_lat=64.8, avg_long=-147.7),
    ]
    request = types.SimpleNamespace(
        args={"limit": "2"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(rows=rows)])),
    )
    response = await get_top_cities_by_state(request, state="AK")
    payload = json.loads(response.body)
    assert payload["state"] == "AK"
    assert len(payload["items"]) == 2
    assert payload["items"][0]["city"] == "Anchorage"


@pytest.mark.asyncio
async def test_geo_state_top_cities_not_found():
    request = types.SimpleNamespace(
        args={},
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(rows=[])])),
    )
    response = await get_top_cities_by_state(request, state="ZZ")
    assert response.status == 404
