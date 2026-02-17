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
    assert payload == [{"code": "1234"}]


@pytest.mark.asyncio
async def test_nucc_all_with_include_meta_returns_paginated_payload():
    session = FakeSession(
        [
            FakeResult(scalar=1),
            FakeResult(rows=[FakeRow("261Q00000X")]),
        ]
    )
    request = types.SimpleNamespace(
        args={"include_meta": "true"},
        ctx=types.SimpleNamespace(sa_session=session),
    )
    response = await all_of_nucc(request)
    payload = json.loads(response.body)
    assert payload["total"] == 1
    assert payload["limit"] == 50
    assert payload["offset"] == 0
    assert payload["items"] == [{"code": "261Q00000X"}]


@pytest.mark.asyncio
async def test_nucc_all_with_paginate_alias_returns_paginated_payload():
    session = FakeSession(
        [
            FakeResult(scalar=12),
            FakeResult(rows=[FakeRow("261Q00000X")]),
        ]
    )
    request = types.SimpleNamespace(
        args={"paginate": "1", "offset": "20", "limit": "10"},
        ctx=types.SimpleNamespace(sa_session=session),
    )
    response = await all_of_nucc(request)
    payload = json.loads(response.body)
    assert payload["total"] == 12
    assert payload["page"] == 3
    assert payload["offset"] == 20
    assert payload["limit"] == 10
    assert payload["items"] == [{"code": "261Q00000X"}]


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
    assert isinstance(payload, list)
    assert len(payload) == 2
    assert payload[0]["code"] == "261Q00000X"
    assert payload[1]["code"] == "3336C0003X"


@pytest.mark.asyncio
async def test_nucc_all_with_legacy_offset_limit_keeps_list_shape():
    session = FakeSession(
        [
            FakeResult(scalar=2),
            FakeResult(rows=[FakeRow("261Q00000X"), FakeRow("3336C0003X")]),
        ]
    )
    request = types.SimpleNamespace(
        args={"offset": "20", "limit": "10"},
        ctx=types.SimpleNamespace(sa_session=session),
    )
    response = await all_of_nucc(request)
    payload = json.loads(response.body)
    assert isinstance(payload, list)
    assert payload == [{"code": "261Q00000X"}, {"code": "3336C0003X"}]


@pytest.mark.asyncio
async def test_nucc_all_with_legacy_page_limit_keeps_list_shape():
    session = FakeSession(
        [
            FakeResult(scalar=1),
            FakeResult(rows=[FakeRow("261Q00000X")]),
        ]
    )
    request = types.SimpleNamespace(
        args={"page": "2", "limit": "1"},
        ctx=types.SimpleNamespace(sa_session=session),
    )
    response = await all_of_nucc(request)
    payload = json.loads(response.body)
    assert isinstance(payload, list)
    assert payload == [{"code": "261Q00000X"}]


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
    assert payload == [{"code": "261Q00000X"}]


@pytest.mark.asyncio
async def test_nucc_all_missing_session():
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=None))
    with pytest.raises(RuntimeError):
        await all_of_nucc(request)
