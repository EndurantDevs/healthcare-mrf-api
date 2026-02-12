# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest

from api.endpoint.nucc import index_status_nucc, all_of_nucc


@pytest.mark.asyncio
async def test_nucc_endpoint():
    request = types.SimpleNamespace(
        app=types.SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "test"})
    )
    response = await index_status_nucc(request)
    payload = json.loads(response.body)
    assert payload == {}


class FakeRow:
    def __init__(self, code="1234"):
        self.code = code

    def to_json_dict(self):
        return {"code": self.code}


class FakeResult:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows
        self._scalar = scalar

    def scalars(self):
        return types.SimpleNamespace(all=lambda: self._rows)

    def scalar(self):
        return self._scalar


class FakeSession:
    def __init__(self, results):
        self._results = list(results)

    async def execute(self, *_args, **_kwargs):
        if not self._results:
            return FakeResult(rows=[])
        return self._results.pop(0)


@pytest.mark.asyncio
async def test_nucc_all_success():
    session = FakeSession(
        [
            FakeResult(scalar=1),
            FakeResult(rows=[FakeRow()]),
        ]
    )
    request = types.SimpleNamespace(args={}, ctx=types.SimpleNamespace(sa_session=session))
    response = await all_of_nucc(request)
    payload = json.loads(response.body)
    assert payload["total"] == 1
    assert payload["limit"] == 50
    assert payload["offset"] == 0
    assert payload["items"] == [{"code": "1234"}]


@pytest.mark.asyncio
async def test_nucc_all_with_filters_and_offset_window():
    session = FakeSession(
        [
            FakeResult(scalar=10),
            FakeResult(rows=[FakeRow("261Q00000X"), FakeRow("3336C0003X")]),
        ]
    )
    request = types.SimpleNamespace(
        args={"q": "clinic", "code": "261Q", "offset": "20", "limit": "20", "order": "desc"},
        ctx=types.SimpleNamespace(sa_session=session),
    )
    response = await all_of_nucc(request)
    payload = json.loads(response.body)
    assert payload["total"] == 10
    assert payload["page"] == 2
    assert payload["offset"] == 20
    assert payload["limit"] == 20
    assert payload["applied_filters"]["q"] == "clinic"
    assert payload["applied_filters"]["code"] == "261Q"
    assert len(payload["items"]) == 2


@pytest.mark.asyncio
async def test_nucc_all_invalid_order_falls_back_to_asc():
    session = FakeSession(
        [
            FakeResult(scalar=1),
            FakeResult(rows=[FakeRow("261Q00000X")]),
        ]
    )
    request = types.SimpleNamespace(
        args={"order": "sideways"},
        ctx=types.SimpleNamespace(sa_session=session),
    )
    response = await all_of_nucc(request)
    payload = json.loads(response.body)
    assert payload["total"] == 1
    assert payload["items"][0]["code"] == "261Q00000X"


@pytest.mark.asyncio
async def test_nucc_all_missing_session():
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=None))
    with pytest.raises(RuntimeError):
        await all_of_nucc(request)
