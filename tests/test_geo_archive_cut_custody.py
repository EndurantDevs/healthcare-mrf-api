# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Geo archive reads cross only a verified, locked incumbent-to-successor cut."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from db.models import GeoZipLookup
from process import cms_doctors_immutable as immutable
from process import geo_source_archive as geo
from process import geo_source_storage as storage
from process import native_publication_read_access as read_access
from process import provider_directory_projection_stage as projection_stage
from process import reference_family_archive as archive


def _install_cut_steps(monkeypatch, damage, connection, events):
    """Record the incumbent check, successor heap lock and named read replay, failing the damaged step."""
    driver = object()

    async def incumbent_storage(session, *, schema_name, is_incumbent):
        assert session is connection and (schema_name, is_incumbent) == ("old_geo", True)
        events.append("incumbent-storage")
        if damage == "incumbent_storage":
            raise RuntimeError("incumbent changed")

    async def successor_heap(session, model, schema, table):
        # The declared successor heap is checked by name and its reader lock is held for the rest of the cut.
        assert session is connection
        assert (model, schema, table) == (GeoZipLookup, "next_geo", storage.ROOT)
        events.append("successor-heap-locked")
        if damage == "successor_heap":
            raise RuntimeError("successor changed")

    async def copy_driver(session):
        assert session is connection
        return driver

    async def named_read_replay(actual_driver, **names):
        assert actual_driver is driver
        assert names == {
            "schema": "old_geo",
            "live_table": storage.ROOT,
            "stage_schema": "next_geo",
            "stage_table": storage.ROOT,
        }
        events.append("read-grant-replay")
        if damage == "read_grant_refused":
            raise RuntimeError("publication_read_access_relations_changed")

    monkeypatch.setattr(storage, "require_geo_storage", incumbent_storage)
    monkeypatch.setattr(geo, "_heap_custody", successor_heap)
    monkeypatch.setattr(projection_stage, "_copy_driver", copy_driver)
    monkeypatch.setattr(read_access, "preserve_publication_reads", named_read_replay)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", [None, "family", "renamed_root", "incumbent_storage", "successor_heap", "read_grant_refused"]
)
async def test_cut_replays_reads_only_after_both_heap_custody_and_exact_locks(monkeypatch, damage):
    """Read grants are replayed by qualified name only after both named heaps are checked under held locks."""
    events = []
    incumbent = SimpleNamespace(importer_id="geo", schema_name="old_geo", relation_oids=((storage.ROOT, None),))
    successor = SimpleNamespace(
        importer_id="different" if damage == "family" else "geo",
        schema_name="next_geo",
        relation_oids=(("geo_zip_lookup_renamed" if damage == "renamed_root" else storage.ROOT, None),),
    )
    connection = object()
    _install_cut_steps(monkeypatch, damage, connection, events)
    writer = AsyncMock(return_value=17)
    monkeypatch.setattr(geo, "_archive_writer", writer)
    if damage:
        refusal = {
            "family": "geo_source_family_changed",
            "renamed_root": "geo_source_family_changed",
            "incumbent_storage": "incumbent changed",
            "successor_heap": "successor changed",
            "read_grant_refused": "publication_read_access_relations_changed",
        }[damage]
        with pytest.raises(RuntimeError, match="^" + refusal + "$"):
            await geo.preserve_geo_cut_reads(connection, successor, incumbent)
    else:
        await geo.preserve_geo_cut_reads(connection, successor, incumbent)
    if damage in {None, "read_grant_refused"}:
        assert events == ["incumbent-storage", "successor-heap-locked", "read-grant-replay"]
    else:
        assert "read-grant-replay" not in events
    writer.assert_awaited_once_with(connection, "geo", "old_geo")


@pytest.mark.asyncio
async def test_physical_export_copies_into_open_heaps_before_the_stage_freezes(monkeypatch):
    session = object()
    spec = SimpleNamespace(importer_id="geo", model_types=(GeoZipLookup,))
    stage_id = UUID(int=7)
    schema = archive.reference_family_stage_schema(stage_id)
    ownership = SimpleNamespace(importer_id="geo", schema_name=schema)
    events = []

    async def create(actual, actual_spec, actual_schema, **options):
        assert (actual, actual_spec, actual_schema) == (session, spec, schema)
        assert options == {"create_indexes": False, "ordinary_heaps": True}
        events.append("created")

    async def capture(actual, actual_spec, identity):
        assert (actual, actual_spec, identity) == (session, spec, stage_id)
        events.append("captured")
        return ownership

    async def early_freeze(*_args, **_kwargs):
        raise AssertionError("the stage froze before its payload COPY")

    async def freeze(actual, actual_ownership):
        assert (actual, actual_ownership) == (session, ownership)
        events.append("frozen")

    monkeypatch.setattr(archive, "_create_model_family", create)
    monkeypatch.setattr(archive, "_capture_model_family_ownership", capture)
    monkeypatch.setattr(archive, "seal_model_family_storage", early_freeze)
    monkeypatch.setattr(immutable, "seal_model_family_relation", freeze)
    assert await geo.create_geo_export_clone(session, spec, schema) is ownership
    events.append("copied")
    await geo.seal_geo_model_relation(session, ownership, None, None)
    assert events == ["created", "captured", "copied", "frozen"]


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
