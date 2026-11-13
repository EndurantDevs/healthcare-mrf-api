# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider optional relation availability stays inside the read snapshot."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_profile_snapshot as snapshots


@pytest.mark.parametrize("include_detail", [False, True])
@pytest.mark.parametrize("available", [False, True])
async def test_optional_relation_availability_is_scoped_and_cleaned(monkeypatch, include_detail, available):
    statements = []

    async def execute(statement, parameters=None):
        sql = str(statement)
        statements.append(sql)
        if parameters is not None:
            return SimpleNamespace(all=lambda: [(name, available) for name in parameters["names"]])

    session = SimpleNamespace(info={"api_reader_verified": True}, execute=execute)
    monkeypatch.setattr(snapshots, "_requires_cms_receipt", AsyncMock(return_value=False))
    monkeypatch.setattr(snapshots, "_read_native_authorities", AsyncMock(return_value={}))
    async with snapshots._read_snapshot_scope(session, "published", include_detail=include_detail):
        assert snapshots.snapshot_relation_available("published.cms_doctor_education") is available
        assert session.info["provider_profile_native_authorities"] == {}
    assert snapshots.snapshot_relation_available("published.cms_doctor_education") is None
    assert session.info == {"api_reader_verified": True}
    assert len(statements) == 3
    assert "IS NOT NULL" in statements[-1] and "LOCK TABLE" not in " ".join(statements)


@pytest.mark.parametrize("failure", [RuntimeError("loader failed"), asyncio.CancelledError()])
async def test_loader_failure_and_cancellation_cleanup_snapshot_metadata(monkeypatch, failure):
    async def execute(statement, parameters=None):
        if parameters is not None:
            return SimpleNamespace(all=lambda: [(name, False) for name in parameters["names"]])

    session = SimpleNamespace(info={"api_reader_verified": True}, execute=execute)
    monkeypatch.setattr(snapshots, "_requires_cms_receipt", AsyncMock(return_value=False))
    monkeypatch.setattr(snapshots, "_read_native_authorities", AsyncMock(return_value={}))
    with pytest.raises(type(failure)) as caught:
        async with snapshots._read_snapshot_scope(session, "published", include_detail=False):
            raise failure
    assert caught.value is failure and session.info == {"api_reader_verified": True}
    assert snapshots.snapshot_relation_available("published.cms_doctor_education") is None
