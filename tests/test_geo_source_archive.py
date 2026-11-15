# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit dispatch/rollback checks; native roles and archive COPY qualify separately."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import cms_doctors_immutable as doctors
from process import geo_source_archive as geo
from process import geo_source_storage as storage
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
async def test_retained_geo_clone_is_writable_until_copy_and_validation(monkeypatch):
    from db.models import GeoZipLookup

    monkeypatch.setattr(geo, "is_source_geo_active", lambda _importer: False)
    protected = AsyncMock()
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
    create.assert_awaited_once_with(session, spec, schema, create_indexes=False, ordinary_heaps=True)
    capture.assert_awaited_once_with(session, spec, UUID(int=3))
    protected.assert_not_awaited()
    seal.assert_not_awaited()
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

    monkeypatch.setattr(geo, "_archive_writer", AsyncMock())
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
        geo_retained_cut=None,
    )
    assert bound.call_args.args[2:] == (incoming, "published")


@pytest.fixture
async def retained_geo(monkeypatch):
    from process import entity_address_snapshot_receipt as rows

    source_hash, target_hash = next(pair for pair in geo.geo_index_schema_hashes() if pair[0] != pair[1])
    table = archive.ReferenceTableReceipt("GeoZipLookup", storage.ROOT, source_hash, 2)
    metadata, metadata_hash = archive._source_metadata({"source_release": "synthetic"})
    origin = generation.validate_reference_family_serving_generation(
        {"origin_lineage_id": str(UUID(int=1)), "origin_generation": 3, "published_at": "2026-01-01T00:00:00Z"}
    )
    manifest = archive.ReferenceFamilyManifest(
        "geo",
        (table,),
        metadata,
        metadata_hash,
        archive._schema_digest((table,)),
        publication_authority="tracked-generation",
        source_serving_generation=origin,
        source_capture_contract=archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT,
    )
    publisher = SimpleNamespace(policy=SimpleNamespace(schema="mrf"))
    named_storage = AsyncMock(return_value=17)
    catalog = AsyncMock(return_value=(table, "a" * 64))
    row_content = AsyncMock(return_value=(2, "c" * 64))
    monkeypatch.setattr(storage, "_archive_writer", AsyncMock())
    monkeypatch.setattr(storage, "_geo_archive_publisher", AsyncMock(return_value=publisher))
    monkeypatch.setattr(storage, "require_geo_storage", named_storage)
    monkeypatch.setattr(storage, "_retained_geo_catalog", catalog)
    monkeypatch.setattr(rows, "_projected_row_identity", row_content)
    session = SimpleNamespace(in_transaction=lambda: True)
    cut = await storage.capture_geo_retained_cut(
        session,
        schema_name="retained_geo",
        expected_relation_oid="old edition",
        expected_relfilenode=None,
        manifest=manifest,
    )
    return SimpleNamespace(
        cut=cut,
        session=session,
        publisher=publisher,
        manifest=manifest,
        table=table,
        target_hash=target_hash,
        named_storage=named_storage,
        catalog=catalog,
        rows=row_content,
    )


@pytest.mark.asyncio
async def test_retained_capture_binds_names_schema_indexes_full_content_and_origin(retained_geo):
    observed = retained_geo
    cut = observed.cut
    assert (
        cut.source_schema,
        cut.source_generation,
        cut.schema_sha256,
        cut.index_profile_sha256,
        cut.row_count,
        cut.row_sha256,
    ) == (
        "retained_geo",
        observed.manifest.source_serving_generation,
        observed.table.schema_sha256,
        "a" * 64,
        2,
        "c" * 64,
    )
    observed.named_storage.assert_awaited_once_with(observed.session, schema_name="retained_geo", is_incumbent=True)
    observed.rows.assert_awaited_once_with(
        observed.session, "retained_geo", storage.ROOT, row_json_sql="pg_catalog.to_jsonb(row_value)"
    )
    assert not any(hasattr(cut, field) for field in ("session", "publisher", "owner_oid", "relation_oid", "index_oids"))
    observed.catalog.return_value = (replace(observed.table, schema_sha256=observed.target_hash), "b" * 64)
    copied = await storage.capture_geo_retained_cut(
        observed.session, schema_name="retained_geo", manifest=observed.manifest
    )
    assert copied.schema_sha256 == observed.target_hash and copied.index_profile_sha256 == "b" * 64


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ("schema", "rows", "source_schema", "data_count", "importer"))
async def test_retained_capture_rejects_changed_input(retained_geo, damage):
    observed = retained_geo
    manifest = observed.manifest
    if damage in {"schema", "rows"}:
        changed = {"schema_sha256": "f" * 64} if damage == "schema" else {"row_count": 3}
        observed.catalog.return_value = (replace(observed.table, **changed), "a" * 64)
    if damage == "data_count":
        observed.rows.return_value = (3, "c" * 64)
    if damage == "importer":
        manifest = replace(manifest, importer_id="nucc")
    with pytest.raises((RuntimeError, archive.ReferenceFamilyArchiveError), match="retained_|manifest|table receipt"):
        await storage.capture_geo_retained_cut(
            observed.session, schema_name="mrf" if damage == "source_schema" else "retained_geo", manifest=manifest
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", (None, "location", "schema", "profile", "rows", "data", "generation", "transaction"))
async def test_retained_consumption_rechecks_names_complete_data_and_origin(retained_geo, damage):
    observed = retained_geo
    session, origin = observed.session, observed.manifest.source_serving_generation
    schema = "mrf"
    if damage == "location":
        schema = "other_schema"
    if damage in {"schema", "profile", "rows"}:
        changes_by_damage = {"schema": {"schema_sha256": "f" * 64}, "rows": {"row_count": 3}, "profile": {}}
        observed.catalog.return_value = (
            replace(observed.table, **changes_by_damage[damage]),
            "b" * 64 if damage == "profile" else "a" * 64,
        )
    if damage == "data":
        observed.rows.return_value = (2, "d" * 64)
    if damage == "generation":
        origin = replace(origin, origin_generation=4)
    if damage == "transaction":
        session = SimpleNamespace(in_transaction=lambda: False)
    if damage:
        with pytest.raises((RuntimeError, ValueError), match="retained_|transaction_required"):
            await storage._consume_geo_retained_cut(session, schema, origin, observed.cut)
    else:
        # A separate connection and different catalog address still serve the copied named data.
        session = SimpleNamespace(is_in_transaction=lambda: True)
        observed.named_storage.return_value = 99
        assert await storage._consume_geo_retained_cut(session, schema, origin, observed.cut) == 99


@pytest.mark.asyncio
async def test_retained_adoption_binds_logical_inventory_and_ignores_old_addresses(retained_geo, monkeypatch):
    observed = retained_geo
    publish = AsyncMock(return_value="published")
    monkeypatch.setattr(storage, "publish_geo_generation", publish)
    monkeypatch.setattr(archive, "_require_published_generation_binding", Mock())
    incumbent, predecessor = SimpleNamespace(schema_name="mrf"), object()
    for pairs in ((("different", 17),), ((storage.ROOT, 17), (storage.ROOT, 18)), ()):
        with pytest.raises(RuntimeError, match="retained_manifest_changed"):
            await generation._adopt_validated_family_generation(
                observed.session,
                observed.manifest,
                incumbent,
                observed.cut.source_generation,
                pairs,
                predecessor,
                geo_retained_cut=observed.cut,
            )
    publish.assert_not_awaited()
    await generation._adopt_validated_family_generation(
        observed.session,
        observed.manifest,
        incumbent,
        observed.cut.source_generation,
        ((storage.ROOT, "opaque old edition"),),
        predecessor,
        geo_retained_cut=observed.cut,
    )
    assert publish.await_args.kwargs["geo_retained_cut"] is observed.cut


@pytest.mark.asyncio
async def test_retained_refusal_precedes_generation_write(retained_geo, monkeypatch):
    first = AsyncMock()
    monkeypatch.setattr(storage, "_writer", AsyncMock())
    monkeypatch.setattr(generation, "_first", first)
    with pytest.raises(RuntimeError, match="retained_local_publication_forbidden"):
        await storage.publish_geo_generation(
            retained_geo.session, schema_name="mrf", expected_authority=object(), geo_retained_cut=retained_geo.cut
        )
    retained_geo.catalog.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await storage.publish_geo_generation(
            retained_geo.session,
            schema_name="mrf",
            expected_authority=object(),
            source_generation=retained_geo.cut.source_generation,
            local=False,
            geo_retained_cut=retained_geo.cut,
        )
    first.assert_not_awaited()


@pytest.mark.asyncio
async def test_retained_manifest_refuses_other_provenance(retained_geo, monkeypatch):
    publish = AsyncMock()
    monkeypatch.setattr(storage, "publish_geo_generation", publish)
    changed = replace(
        retained_geo.manifest,
        source_serving_generation=replace(retained_geo.cut.source_generation, origin_generation=4),
    )
    with pytest.raises(RuntimeError, match="retained_manifest_changed"):
        await generation._adopt_validated_family_generation(
            retained_geo.session,
            changed,
            SimpleNamespace(schema_name="mrf"),
            changed.source_serving_generation,
            ((storage.ROOT, None),),
            object(),
            geo_retained_cut=retained_geo.cut,
        )
    publish.assert_not_awaited()
