# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Importer startup authenticates existing storage before creating private stages."""

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

native = importlib.import_module("process.cms_doctors")


def _startup_boundaries(monkeypatch):
    events = []
    database = SimpleNamespace(status=AsyncMock(side_effect=AssertionError("unexpected schema DDL")))
    publisher = SimpleNamespace(connection=object(), policy=SimpleNamespace(schema="mrf"))

    async def require_roots(selected):
        assert selected is database
        events.append("authenticated")

    @asynccontextmanager
    async def transaction(selected):
        assert selected is database and events == ["authenticated"]
        events.append("source transaction")
        yield

    async def create_stage(selected, model, import_date, schema):
        assert selected is database and model is native.DoctorClinicianAddress
        assert schema == "mrf" and import_date == "a" * 32
        assert events == ["authenticated", "source transaction"]
        events.append("private stage")

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "a" * 32)
    monkeypatch.setattr(native, "db", database)
    monkeypatch.setattr(native, "my_init_db", AsyncMock())
    monkeypatch.setattr(native, "ensure_database", AsyncMock())
    monkeypatch.setattr(native, "require_doctors_identity_roots", require_roots)
    monkeypatch.setattr(native, "source_transaction", transaction)
    monkeypatch.setattr(native, "create_doctors_stage", create_stage)
    monkeypatch.setattr(native, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(native.source_storage, "_close_grants", AsyncMock())
    return database, events


@pytest.mark.asyncio
async def test_startup_authenticates_before_private_stage(monkeypatch):
    database, events = _startup_boundaries(monkeypatch)
    context_by_name = {}
    await native.startup(context_by_name)
    assert events == ["authenticated", "source transaction", "private stage"]
    assert context_by_name["context"]["run"] == 0
    database.status.assert_not_awaited()
    native.source_storage._close_grants.assert_awaited_once()


@pytest.mark.asyncio
async def test_missing_custody_prevents_private_stage(monkeypatch):
    database, events = _startup_boundaries(monkeypatch)
    monkeypatch.setattr(native, "require_doctors_identity_roots", AsyncMock(side_effect=RuntimeError("unprovisioned")))
    with pytest.raises(RuntimeError, match="unprovisioned"):
        await native.startup({})
    assert events == []
    database.status.assert_not_awaited()
    native.source_storage._close_grants.assert_not_awaited()
