# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest
from sqlalchemy.exc import SQLAlchemyError

from api.endpoint import healthcheck as health_module


class FakeSession:
    def __init__(self, error=None):
        self._error = error
        self.executed = False

    async def execute(self, *_args, **_kwargs):
        self.executed = True
        if self._error:
            raise self._error
        return types.SimpleNamespace(first=lambda: None)


@pytest.mark.asyncio
async def test_healthcheck_success():
    session = FakeSession()
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=session),
        app=types.SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "ci"}),
    )
    response = await health_module.healthcheck(request)
    payload = json.loads(response.body)
    assert payload["database"] == {"status": "OK"}
    assert "date" in payload


@pytest.mark.asyncio
async def test_healthcheck_missing_session():
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=None))
    with pytest.raises(RuntimeError):
        await health_module.healthcheck(request)


@pytest.mark.asyncio
async def test_check_db_failure():
    error = SQLAlchemyError("boom")
    session = FakeSession(error=error)
    payload = await health_module._check_db(session)
    assert payload["status"] == "Fail"
    assert "boom" in payload["details"]