# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small preparation preconditions and shared source-validation contracts."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import cms_doctors_preparation as preparation
from tests.test_cms_doctors_organizations import _mock_publication, _publication_context

native = importlib.import_module("process.cms_doctors")


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["_apply_cms_doctors_stage", "_apply_locked_cms_doctors_stage"])
async def test_publication_refuses_missing_owner_transaction(monkeypatch, entrypoint):
    database = SimpleNamespace(_transaction_binding=lambda: None, status=AsyncMock())
    monkeypatch.setattr(native, "db", database)
    with pytest.raises(RuntimeError, match="^cms_doctors_publication_requires_transaction$"):
        await getattr(native, entrypoint)(object(), "mrf", "synthetic")
    database.status.assert_not_awaited()


def test_uuid_length_stage_indexes_are_distinct_and_bounded():
    """PostgreSQL must not silently truncate several index names into one."""
    stage = "doctor_clinician_address_" + "a" * 32
    index_names = [
        native._stage_index_name(stage, suffix)
        for suffix in ("primary", "zip_provider_type", "provider_type", "address_key")
    ]
    assert len(set(index_names)) == len(index_names)
    assert all(len(name) <= 63 for name in index_names)


@pytest.mark.asyncio
@pytest.mark.parametrize("import_date", [None, 4, "", "bad-name", "a" * 33])
async def test_preparation_rejects_invalid_stage_identity(import_date):
    with pytest.raises(RuntimeError, match="requires_completed_import"):
        async with preparation.prepare_cms_doctors_generation({"import_date": import_date, "context": {"run": True}}):
            pytest.fail("invalid stage identity must not reach preparation")


@pytest.mark.asyncio
async def test_source_preparation_reuses_validation_without_publication(monkeypatch):
    calls = _mock_publication(monkeypatch, None)
    ctx = _publication_context()
    metrics = await native._prepare_cms_doctors_sources(ctx, object(), "mrf", 20000)
    assert calls == ["education", "group", "artifact", "binding", "sites", "cancel"]
    assert metrics["organization_groups"] == 101 and metrics["sites"] == 71
    native._publish_cms_doctors_stage.assert_not_awaited()
    native.mark_control_run.assert_not_awaited()
