# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained export authority cannot become physical publication authority."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import cms_doctors_immutable as immutable
from process import provider_directory_generic_source_policy as source
from process import reference_family_archive as archive


@pytest.mark.asyncio
async def test_protected_destination_precedes_ambient_source_dispatch(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value='"retained_owner"'))
    spec = SimpleNamespace(importer_id="cms-doctors")
    heaps = AsyncMock()
    monkeypatch.setattr(archive, "protected_publisher_owner", AsyncMock(return_value=7))
    monkeypatch.setattr(archive, "_create_model_heaps", heaps)
    writer = AsyncMock(side_effect=AssertionError("physical writer must not authorize retained destination"))
    monkeypatch.setattr(immutable, "_writer", writer)
    token = source._PUBLISHER.set(object())
    try:
        await archive._create_model_family(
            session, spec, "reference_family_archive_" + UUID(int=1).hex, protected_owner_oid=7
        )
    finally:
        source._PUBLISHER.reset(token)
    writer.assert_not_awaited()
    assert 'AUTHORIZATION "retained_owner"' in str(session.execute.await_args.args[0])
    heaps.assert_awaited_once()


@pytest.mark.asyncio
async def test_protected_destination_refuses_wrong_native_owner_before_create(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(archive, "protected_publisher_owner", AsyncMock(return_value=7))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="publisher owner differs"):
        await archive._create_model_family(
            session,
            SimpleNamespace(importer_id="cms-doctors"),
            "reference_family_archive_" + UUID(int=1).hex,
            protected_owner_oid=8,
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_physical_destination_still_requires_the_source_writer(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(immutable, "_writer", AsyncMock(side_effect=RuntimeError("genuine source required")))
    token = source._PUBLISHER.set(object())
    try:
        with pytest.raises(RuntimeError, match="genuine source required"):
            await archive._create_model_family(
                session, SimpleNamespace(importer_id="cms-doctors"), "reference_family_archive_" + UUID(int=1).hex
            )
    finally:
        source._PUBLISHER.reset(token)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["cms-doctors", "places-zcta"])
async def test_export_clone_preserves_retained_custody_before_payload(monkeypatch, importer):
    events = []
    session, ownership = object(), object()
    spec = SimpleNamespace(importer_id=importer)

    def record(label, result=None):
        async def call(*args, **kwargs):
            assert args[0] is session
            events.append((label, kwargs))
            return result

        return call

    monkeypatch.setattr(archive, "protected_publisher_owner", record("authority", 7))
    monkeypatch.setattr(archive, "_create_model_family", record("create"))
    monkeypatch.setattr(archive, "_capture_model_family_ownership", record("capture", ownership))
    seal = AsyncMock(side_effect=record("seal"))
    monkeypatch.setattr(archive, "seal_model_family_storage", seal)
    schema = "reference_family_archive_" + UUID(int=1).hex
    assert await immutable.create_model_family_export_clone(session, spec, schema) is ownership
    assert [label for label, _ in events] == (
        ["authority", "create", "capture", "seal"] if importer == "cms-doctors" else ["create", "capture"]
    )
    create = next(kwargs for label, kwargs in events if label == "create")
    assert create == {
        "create_indexes": False,
        "ordinary_heaps": True,
        "protected_owner_oid": 7 if importer == "cms-doctors" else None,
    }
    if importer == "cms-doctors":
        seal.assert_awaited_once_with(session, ownership, 7, require_empty=True)
