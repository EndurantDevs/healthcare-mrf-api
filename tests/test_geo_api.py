import json
import types

import pytest

from api.endpoint.geo import get_geo as get_geo_zip


class FakeResult:
    def __init__(self, row=None):
        self._row = row

    def first(self):
        return self._row


class FakeSession:
    def __init__(self, row=None):
        self._row = row

    async def execute(self, *_args, **_kwargs):
        return FakeResult(self._row)


@pytest.mark.asyncio
async def test_geo_zip_not_found():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession(None))
    )
    response = await get_geo_zip(request, "99999")
    payload = json.loads(response.body)
    assert payload["error"] == "Not found"


@pytest.mark.asyncio
async def test_geo_zip_success():
    row = ("12345", "43.12", "-89.45", "WI")
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession(row))
    )
    response = await get_geo_zip(request, "12345")
    payload = json.loads(response.body)
    assert payload == {
        "zip_code": "12345",
        "lat": 43.12,
        "long": -89.45,
        "state": "WI",
    }
