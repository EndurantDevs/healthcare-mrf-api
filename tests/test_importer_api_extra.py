# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
from unittest.mock import AsyncMock

import pytest
import sanic.exceptions

from api.endpoint import importer as importer_module


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return types.SimpleNamespace(all=lambda: self._rows)


class FakeDB:
    def __init__(self, *, all_responses=None, scalar_responses=None, execute_rows=None):
        self._all = list(all_responses or [])
        self._scalar = list(scalar_responses or [])
        self._execute_rows = execute_rows or []

    def transaction(self):
        return FakeTransaction()

    async def all(self, *_args, **_kwargs):
        return self._all.pop(0)

    async def scalar(self, *_args, **_kwargs):
        if not self._scalar:
            return None
        value = self._scalar.pop(0)
        if callable(value):
            return value()
        return value

    async def execute(self, *_args, **_kwargs):
        return FakeExecuteResult(self._execute_rows)


class FakeIssuer:
    def __init__(self, payload):
        self._payload = payload

    def to_json_dict(self):
        return dict(self._payload)


@pytest.mark.asyncio
async def test_collect_import_stats(monkeypatch):
    fake_db = FakeDB(
        all_responses=[
            [("CA", 2), ("NY", 1)],
            [("CA", 3), ("NY", 4)],
        ],
        scalar_responses=[10, 5, "2024-01-01", 7],
    )
    monkeypatch.setattr(importer_module, "db", fake_db)

    stats = await importer_module._collect_import_stats()
    assert stats["plans_count"] == 10
    assert stats["issuers_number"] == 7
    assert stats["issuers_by_state"] == {"CA": 2, "NY": 1}
    assert stats["plans_by_state"] == {"CA": 3, "NY": 4}


@pytest.mark.asyncio
async def test_last_import_stats(monkeypatch):
    monkeypatch.setattr(
        importer_module,
        "_collect_import_stats",
        AsyncMock(return_value={"plans_count": 1}),
    )

    response = await importer_module.last_import_stats(types.SimpleNamespace())
    assert json.loads(response.body) == {"plans_count": 1}


@pytest.mark.asyncio
async def test_issuer_import_data_success(monkeypatch):
    issuer_payload = {"issuer_id": 101, "issuer_name": "ACME"}
    fake_db = FakeDB(
        scalar_responses=[lambda: FakeIssuer(issuer_payload)],
        execute_rows=[
            types.SimpleNamespace(
                to_json_dict=lambda: {"checksum": 1, "message": "duplicate"}
            )
        ],
    )
    monkeypatch.setattr(importer_module, "db", fake_db)

    request = types.SimpleNamespace()
    response = await importer_module.issuer_import_data(request, "101")
    payload = json.loads(response.body)
    assert payload["issuer_name"] == "ACME"
    assert payload["import_errors_count"] == 1


@pytest.mark.asyncio
async def test_issuer_import_data_not_found(monkeypatch):
    fake_db = FakeDB(scalar_responses=[None])
    monkeypatch.setattr(importer_module, "db", fake_db)

    with pytest.raises(sanic.exceptions.NotFound):
        await importer_module.issuer_import_data(types.SimpleNamespace(), "999")