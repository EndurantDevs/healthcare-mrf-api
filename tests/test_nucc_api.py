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
    def to_json_dict(self):
        return {"code": "1234"}


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return types.SimpleNamespace(all=lambda: self._rows)


class FakeSession:
    def __init__(self, result):
        self._result = result

    async def execute(self, *_args, **_kwargs):
        return self._result


@pytest.mark.asyncio
async def test_nucc_all_success():
    session = FakeSession(FakeResult([FakeRow()]))
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=session))
    response = await all_of_nucc(request)
    payload = json.loads(response.body)
    assert payload == [{"code": "1234"}]


@pytest.mark.asyncio
async def test_nucc_all_missing_session():
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=None))
    with pytest.raises(RuntimeError):
        await all_of_nucc(request)