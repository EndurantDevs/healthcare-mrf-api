# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit dispatch/rollback checks; native roles and archive COPY qualify separately."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import cms_doctors_immutable as doctors
from process import geo_source_archive as geo
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation


@pytest.mark.asyncio
async def test_explicit_retained_owner_precedes_physical_geo_dispatch(monkeypatch):
    active = AsyncMock()
    monkeypatch.setattr(geo, "is_source_geo_active", active)
    session = SimpleNamespace(execute=AsyncMock())
    assert await geo.create_geo_archive_schema(session, "untrusted", 12) is None
    active.assert_not_called()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_retained_geo_clone_uses_protected_owner_before_creation_and_seals_empty(monkeypatch):
    from db.models import GeoZipLookup

    monkeypatch.setattr(geo, "is_source_geo_active", lambda _importer: False)
    protected = AsyncMock(return_value=29)
    monkeypatch.setattr(archive, "protected_publisher_owner", protected)
    create, capture, seal = AsyncMock(), AsyncMock(return_value="owned"), AsyncMock()
    monkeypatch.setattr(archive, "_create_model_family", create)
    monkeypatch.setattr(archive, "_capture_model_family_ownership", capture)
    monkeypatch.setattr(archive, "seal_model_family_storage", seal)
    writer = AsyncMock()
    monkeypatch.setattr(geo, "_writer", writer)
    session, spec = object(), SimpleNamespace(importer_id="geo", model_types=(GeoZipLookup,))
    schema = archive.reference_family_stage_schema(UUID(int=3))
    assert await geo.create_model_family_export_clone(session, spec, schema) == "owned"
    protected.assert_awaited_once_with(session)
    create.assert_awaited_once_with(
        session,
        spec,
        schema,
        create_indexes=False,
        ordinary_heaps=True,
        protected_owner_oid=29,
    )
    seal.assert_awaited_once_with(session, "owned", 29, require_empty=True)
    writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_physical_schema_uses_original_publisher_create_and_exact_source_owner(monkeypatch):
    events = []
    publisher = SimpleNamespace(policy=SimpleNamespace(owner_role="physical_owner"))

    @asynccontextmanager
    async def publisher_body(session):
        events.append("publisher")
        yield publisher
        events.append("source_owner_restored")

    monkeypatch.setattr(geo, "is_source_geo_active", lambda importer: importer == "geo")
    monkeypatch.setattr(geo, "_writer", AsyncMock(return_value=17))
    monkeypatch.setattr(geo, "source_publisher_body", publisher_body)
    session = SimpleNamespace(execute=AsyncMock())
    schema = archive.reference_family_stage_schema(UUID(int=1))
    assert await geo.create_geo_archive_schema(session, schema) == 17
    assert str(session.execute.await_args.args[0]) == f'CREATE SCHEMA "{schema}" AUTHORIZATION "physical_owner"'
    assert events == ["publisher", "source_owner_restored"]
    with pytest.raises(ValueError):
        await geo.create_geo_archive_schema(session, "other_schema")
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_geo_rotation_failure_rolls_back_even_when_outer_caller_catches(monkeypatch):
    events = []

    @asynccontextmanager
    async def savepoint():
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    monkeypatch.setattr(geo, "_writer", AsyncMock())
    session = SimpleNamespace(begin_nested=savepoint)
    with pytest.raises(RuntimeError, match="after_rotation"):
        async with geo.geo_cut_savepoint(session, "geo"):
            raise RuntimeError("after_rotation")
    assert events == ["rollback"]


@pytest.mark.asyncio
async def test_other_family_dispatch_keeps_original_implementation(monkeypatch):
    export = AsyncMock(return_value=object())
    predecessor = AsyncMock(return_value=object())
    owner = AsyncMock(return_value=object())
    seal = AsyncMock(return_value=object())
    monkeypatch.setattr(doctors, "create_model_family_export_clone", export)
    monkeypatch.setattr(doctors, "create_model_family_predecessor_schema", predecessor)
    monkeypatch.setattr(doctors, "model_family_publisher_owner", owner)
    monkeypatch.setattr(doctors, "seal_model_family_relation", seal)
    session, spec, ownership = object(), SimpleNamespace(importer_id="nucc"), SimpleNamespace(importer_id="nucc")
    assert await geo.create_model_family_export_clone(session, spec, "stage") is export.return_value
    assert await geo.create_model_family_predecessor_schema(session, "nucc", "old") is predecessor.return_value
    assert await geo.model_family_publisher_owner(session, "nucc") is owner.return_value
    assert await geo.seal_model_family_relation(session, ownership, 2, 3) is seal.return_value
    export.assert_awaited_once_with(session, spec, "stage")
    predecessor.assert_awaited_once_with(session, "nucc", "old")
    owner.assert_awaited_once_with(session, "nucc")
    seal.assert_awaited_once_with(session, ownership, 2, 3)


@pytest.mark.asyncio
async def test_validated_guarded_geo_adoption_keeps_origin_without_installing_hooks(monkeypatch):
    from process import geo_source_storage

    publish = AsyncMock(return_value="published")
    monkeypatch.setattr(geo_source_storage, "publish_geo_generation", publish)
    bound = Mock()
    monkeypatch.setattr(archive, "_require_published_generation_binding", bound)
    incoming, predecessor = object(), object()
    manifest = SimpleNamespace(importer_id="geo", source_capture_contract=archive.GUARDED_SOURCE_CAPTURE_CONTRACT)
    incumbent = SimpleNamespace(schema_name="mrf")
    assert (
        await generation._adopt_validated_family_generation(
            None,
            manifest,
            incumbent,
            incoming,
            (("geo_zip_lookup", 15),),
            predecessor,
        )
        == "published"
    )
    publish.assert_awaited_once_with(
        None,
        schema_name="mrf",
        source_generation=incoming,
        expected_authority=predecessor,
        local=False,
    )
    assert bound.call_args.args[2:] == (incoming, "published")
