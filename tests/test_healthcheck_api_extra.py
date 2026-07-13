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
async def test_liveness_does_not_require_database_session():
    request = types.SimpleNamespace(
        app=types.SimpleNamespace(config={"RELEASE": "test"}),
    )
    response = await health_module.liveness(request)
    payload = json.loads(response.body)
    assert response.status == 200
    assert payload == {"status": "OK", "release": "test"}


@pytest.mark.asyncio
async def test_readiness_requires_session():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=None),
        app=types.SimpleNamespace(config={"RELEASE": "test"}),
    )
    response = await health_module.readiness(request)
    assert response.status == 503
    assert json.loads(response.body)["status"] == "Fail"


@pytest.mark.asyncio
async def test_readiness_checks_database_and_v3_schema():
    session = FakeSession()
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=session),
        app=types.SimpleNamespace(config={"RELEASE": "test"}),
    )
    response = await health_module.readiness(request)
    payload = json.loads(response.body)
    assert response.status == 200
    assert payload["status"] == "OK"
    assert payload["database"] == {"status": "OK"}
    assert payload["ptg_v3_schema"] == {"status": "OK"}


@pytest.mark.asyncio
async def test_readiness_fails_when_v3_schema_check_fails(monkeypatch):
    async def fail_v3_schema(_session):
        return {"status": "Fail", "details": "missing V3 schema"}

    monkeypatch.setattr(health_module, "_check_v3_schema", fail_v3_schema)
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession()),
        app=types.SimpleNamespace(config={"RELEASE": "test"}),
    )
    response = await health_module.readiness(request)
    payload = json.loads(response.body)
    assert response.status == 503
    assert payload["status"] == "Fail"
    assert payload["ptg_v3_schema"]["details"] == "missing V3 schema"


@pytest.mark.asyncio
async def test_check_db_failure():
    error = SQLAlchemyError("boom")
    session = FakeSession(error=error)
    payload = await health_module._check_db(session)
    assert payload["status"] == "Fail"
    assert "boom" in payload["details"]
