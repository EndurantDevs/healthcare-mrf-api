# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Geo archive reads cross only a verified, locked incumbent-to-successor cut."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from db.models import GeoZipLookup
from process import geo_source_archive as geo
from process import geo_source_storage as storage
from process import reference_family_archive as archive


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "family", "same_oid", "unlocked", "old_custody", "new_custody"])
async def test_cut_replays_reads_only_after_both_heap_custody_and_exact_locks(monkeypatch, damage):
    events = []
    incumbent = SimpleNamespace(importer_id="geo", schema_name="old_geo", relation_oids=((storage.ROOT, 41),))
    successor = SimpleNamespace(
        importer_id="different" if damage == "family" else "geo",
        schema_name="next_geo",
        relation_oids=((storage.ROOT, 41 if damage == "same_oid" else 42),),
    )

    async def old_custody(session, *, schema_name, expected_oid, is_incumbent):
        assert session is connection and (schema_name, expected_oid, is_incumbent) == ("old_geo", 41, True)
        events.append("old-custody")
        if damage == "old_custody":
            raise RuntimeError("old changed")

    async def new_custody(session, model, schema, table, oid, owner):
        assert session is connection and (model, schema, table, oid, owner) == (
            GeoZipLookup,
            "next_geo",
            storage.ROOT,
            42,
            17,
        )
        events.append("new-custody")
        if damage == "new_custody":
            raise RuntimeError("new changed")

    async def has_cut_locks(query, parameters):
        assert parameters == {"oids": [41, 42]}
        assert "AccessExclusiveLock" in str(query) and "count(DISTINCT relation)=2" in str(query)
        events.append("locks")
        return damage != "unlocked"

    connection = SimpleNamespace(scalar=has_cut_locks)
    replay = AsyncMock()
    monkeypatch.setattr(geo, "_writer", AsyncMock(return_value=17))
    monkeypatch.setattr(storage, "require_geo_storage", old_custody)
    monkeypatch.setattr(geo, "_heap_custody", new_custody)
    monkeypatch.setattr(geo, "replay_geo_reads", replay)
    if damage:
        with pytest.raises(RuntimeError):
            await geo.preserve_geo_cut_reads(connection, successor, incumbent)
        replay.assert_not_awaited()
    else:
        await geo.preserve_geo_cut_reads(connection, successor, incumbent)
        assert events == ["old-custody", "new-custody", "locks"]
        replay.assert_awaited_once_with(connection, 41, '"next_geo"."geo_zip_lookup"')


@pytest.mark.asyncio
async def test_physical_export_closes_empty_heaps_under_actual_writer_before_copy(monkeypatch):
    session = object()
    spec = SimpleNamespace(importer_id="geo", model_types=(GeoZipLookup,))
    stage_id = UUID(int=7)
    schema = archive.reference_family_stage_schema(stage_id)
    ownership = object()
    events = []

    async def create(actual, actual_spec, actual_schema, **options):
        assert (actual, actual_spec, actual_schema) == (session, spec, schema)
        assert options == {"create_indexes": False, "ordinary_heaps": True, "protected_owner_oid": None}
        events.append("created")

    async def capture(actual, actual_spec, identity):
        assert (actual, actual_spec, identity) == (session, spec, stage_id)
        events.append("captured")
        return ownership

    async def seal(actual, actual_ownership, owner, *, require_empty):
        assert (actual, actual_ownership, owner, require_empty) == (session, ownership, 17, True)
        events.append("sealed")

    monkeypatch.setattr(archive, "_create_model_family", create)
    monkeypatch.setattr(archive, "_capture_model_family_ownership", capture)
    monkeypatch.setattr(archive, "seal_model_family_storage", seal)
    monkeypatch.setattr(geo, "_writer", AsyncMock(return_value=17))
    assert await geo.create_geo_export_clone(session, spec, schema) is ownership
    assert events == ["created", "captured", "sealed"]


@pytest.mark.asyncio
async def test_predecessor_creation_authenticates_before_entering_publisher_body(monkeypatch):
    events = []
    publisher = SimpleNamespace(policy=SimpleNamespace(owner_role="source_owner"))

    async def writer(session):
        assert session is connection
        events.append("writer")

    @asynccontextmanager
    async def body(session):
        assert session is connection
        events.append("publisher")
        try:
            yield publisher
        finally:
            events.append("restored")

    connection = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("create failed")))
    monkeypatch.setattr(geo, "_writer", writer)
    monkeypatch.setattr(geo, "source_publisher_body", body)
    schema = archive.reference_family_predecessor_schema(UUID(int=9))
    with pytest.raises(RuntimeError, match="create failed"):
        await geo.create_geo_predecessor_schema(connection, schema)
    assert events == ["writer", "publisher", "restored"]
    assert str(connection.execute.await_args.args[0]) == f'CREATE SCHEMA "{schema}" AUTHORIZATION "source_owner"'
