# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest

from api.endpoint import npi as npi_module
from api.endpoint.npi import get_all


class RecordingConnection:
    def __init__(self):
        self.calls = 0
        self.last_sql = None
        self.last_params = None

    async def all(self, sql, **params):
        self.calls += 1
        self.last_sql = str(sql)
        self.last_params = params
        # first call: count; second call: results
        if self.calls == 1:
            return [(5,)]
        return []

    async def first(self, *_args, **_kwargs):
        return None


class FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_get_all_name_like_array(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "classification": "Pharmacy",
            "state": "RI",
            "name_like": ["CVS", "CVS Pharmacy"],
            "has_insurance": "1",
            "limit": "0",
            "start": "0",
        }
    )
    resp = await get_all(request)
    data = json.loads(resp.body)
    assert data["total"] == 5  # count from first call
    assert "b.npi" in conn.last_sql and "name_like_0" in conn.last_params
    assert conn.last_params["limit"] > 0  # limit expanded from 0
