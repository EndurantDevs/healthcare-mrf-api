# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest

from api.endpoint.healthcheck import healthcheck


@pytest.mark.asyncio
async def test_healthcheck(monkeypatch):
    async def fake_check_db(_session):
        return {"status": "OK"}

    monkeypatch.setattr("api.endpoint.healthcheck._check_db", fake_check_db)

    request = types.SimpleNamespace(
        app=types.SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "test"}),
        ctx=types.SimpleNamespace(sa_session=object()),
    )

    response = await healthcheck(request)
    payload = json.loads(response.body)
    assert payload["database"] == {"status": "OK"}