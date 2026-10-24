# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real Geo producer/archive swaps under native component roles, not a runtime manifest."""

import csv
import subprocess
from contextlib import asynccontextmanager
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import text

from process import geo_import, geo_source_archive, geo_source_storage
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process.cms_doctors_immutable import _writer
from process.geo_source_storage import require_geo_storage
from process.provider_directory_generic_source_policy import source_transaction
from tests.geo_source_postgres_support import geo_source_component_database

_EXPECTED_INDEX_NAMES = {
    "geo_zip_lookup_pkey",
    "geo_zip_lookup_idx_geo_zip_lookup_state_idx",
    "geo_zip_lookup_idx_geo_zip_lookup_city_state_idx",
    "geo_zip_lookup_idx_geo_zip_lookup_city_idx",
    "geo_zip_lookup_idx_geo_zip_lookup_lat_lng_idx",
    "geo_zip_lookup_idx_geo_zip_lookup_coordinates_idx",
}


async def _index_names(session, schema_name):
    rows = await session.execute(
        text("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname=:schema AND tablename='geo_zip_lookup'"),
        {"schema": schema_name},
    )
    index_rows = rows.all()
    assert len(index_rows) == 6
    assert {(row[1].split(" USING ", 1)[1], row[1].startswith("CREATE UNIQUE INDEX ")) for row in index_rows} == {
        ("btree (zip_code)", True),
        ("btree (state)", False),
        ("btree (city_lower, state)", False),
        ("btree (city_lower)", False),
        ("btree (latitude, longitude)", False),
        ("btree (latitude, longitude, zip_code)", False),
    }
    return {row[0] for row in index_rows}


async def _load_snapshot(component, tmp_path, city):
    path = tmp_path / (city.replace(" ", "-") + ".csv")
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=geo_import.REQUIRED_GEO_SOURCE_FIELDS, delimiter=";")
        writer.writeheader()
        writer.writerow(
            {
                "Zip Code": "10001",
                "Official USPS city name": city,
                "Official USPS State Code": "NY",
                "Geo Point": "40.7,-73.9",
            }
        )
    await geo_import.load_geo_lookup(path)
    async with source_transaction(component.database, isolation="repeatable read") as session:
        return await generation.read_reference_family_result_generation_authority(
            session,
            importer_id="geo",
            schema_name="mrf",
        )


async def _assert_bootstrap_archive_transition(component, index_profile):
    """Validate actual old/new catalogs without inventing a published source generation."""
    spec = archive.reference_family_spec("geo")
    async with source_transaction(component.database, isolation="repeatable read") as session:
        original_oid = await require_geo_storage(session, schema_name="mrf", is_incumbent=True)
        original = await archive._family_manifest(
            session, spec=spec, schema_name="mrf", source_metadata={"source_release": "synthetic"}
        )
        assert original.source_serving_generation is None and original.publication_authority == "manual-only"
        assert original.tables[0].row_count == 0
        if index_profile != "current":
            with pytest.raises(RuntimeError, match="indexes_incomplete"):
                await require_geo_storage(session, schema_name="mrf")
        async with session.begin_nested() as rollback:
            ownership = await archive.precreate_reference_family_restore(session, importer_id="geo", dataset_id=uuid4())
            await archive.complete_reference_family_restore(session, ownership)
            owner = await _writer(session)
            await archive.seal_model_family_storage(session, ownership, owner)
            receipts = await archive._validate_stage_manifest(session, ownership=ownership, manifest=original)
            assert (
                original.tables[0].schema_sha256,
                receipts[0].schema_sha256,
            ) in geo_source_archive.geo_index_schema_hashes()
            await _index_names(session, ownership.schema_name)
            await rollback.rollback()
        assert await require_geo_storage(session, schema_name="mrf", is_incumbent=True) == original_oid
        unchanged = await archive._family_manifest(
            session, spec=spec, schema_name="mrf", source_metadata={"source_release": "synthetic"}
        )
        assert unchanged == original
        return original, original_oid


async def _retain_geo_heap(session, oid):
    """Move only the actual predecessor heap, preserving its original file and indexes."""
    schema = archive.reference_family_predecessor_schema(uuid4())
    await archive.create_model_family_predecessor_schema(session, "geo", schema)
    name, filenode = (
        await session.execute(
            text("SELECT relname,pg_relation_filenode(oid)::bigint FROM pg_class WHERE oid=:oid"), {"oid": oid}
        )
    ).one()
    await session.execute(text(f'ALTER TABLE "mrf".{archive._quoted(name)} SET SCHEMA {archive._quoted(schema)}'))
    await session.execute(
        text(f'ALTER TABLE {archive._quoted(schema)}.{archive._quoted(name)} RENAME TO "geo_zip_lookup"')
    )
    return schema, filenode


async def _capture_geo_manifest(component):
    """Use the genuine read-only source session to retain portable published provenance."""
    reader = component.database._reader_database
    async with reader.session_factory() as session, session.begin():
        capture = await archive.capture_reference_family_source(
            session, importer_id="geo", schema_name="mrf", source_metadata={"source_release": "synthetic"}
        )
        return capture.manifest, session


async def _refuse_stale_geo_storage(session, schema, oid, filenode, manifest):
    """Invalidate only private retained identity inside savepoints, preserving original payloads."""
    relation = f'{archive._quoted(schema)}."geo_zip_lookup"'
    before = f'{archive._quoted(schema)}."geo_before"'
    async with session.begin_nested() as rollback:
        await session.execute(text(f'ALTER TABLE {relation} RENAME TO "geo_before"'))
        await session.execute(text(f"CREATE TABLE {relation} (LIKE {before})"))
        with pytest.raises(RuntimeError, match="retained_identity_changed"):
            await geo_source_storage.capture_geo_retained_cut(
                session,
                schema_name=schema,
                expected_relation_oid=oid,
                expected_relfilenode=filenode,
                manifest=manifest,
            )
        await rollback.rollback()
    async with session.begin_nested() as rollback:
        await session.execute(text(f"TRUNCATE {relation}"))
        assert (
            await session.scalar(text("SELECT pg_relation_filenode(CAST(:oid AS oid))::bigint"), {"oid": oid})
            != filenode
        )
        with pytest.raises(RuntimeError, match="retained_identity_changed"):
            await geo_source_storage.capture_geo_retained_cut(
                session,
                schema_name=schema,
                expected_relation_oid=oid,
                expected_relfilenode=filenode,
                manifest=manifest,
            )
        await rollback.rollback()


async def _refuse_geo_retained_drift(session, schema, oid, filenode, manifest, serving_oid, former_session):
    """Reject changed retained inputs before touching the serving heap or generation."""
    await session.execute(
        text(f'LOCK TABLE ONLY "mrf".geo_zip_lookup,{archive._quoted(schema)}.geo_zip_lookup IN ACCESS EXCLUSIVE MODE')
    )
    baseline = await generation.read_reference_family_result_generation_authority(
        session, importer_id="geo", schema_name="mrf"
    )
    other_file = await session.scalar(
        text("SELECT pg_relation_filenode(CAST(:oid AS oid))::bigint"), {"oid": serving_oid}
    )
    changed_table = replace(manifest.tables[0], schema_sha256="f" * 64)
    changed_manifest = replace(
        manifest, tables=(changed_table,), schema_sha256=archive._schema_digest((changed_table,))
    )
    for wrong_oid, wrong_file, wrong_manifest in (
        (serving_oid, filenode, manifest),
        (oid, other_file, manifest),
        (oid, filenode, changed_manifest),
    ):
        with pytest.raises(RuntimeError, match="retained_(identity|manifest)_changed"):
            await geo_source_storage.capture_geo_retained_cut(
                session,
                schema_name=schema,
                expected_relation_oid=wrong_oid,
                expected_relfilenode=wrong_file,
                manifest=wrong_manifest,
            )
    with pytest.raises(RuntimeError, match="original_session_changed"):
        await geo_source_storage.capture_geo_retained_cut(
            former_session,
            schema_name=schema,
            expected_relation_oid=oid,
            expected_relfilenode=filenode,
            manifest=manifest,
        )
    await _refuse_stale_geo_storage(session, schema, oid, filenode, manifest)
    assert await session.scalar(text("SELECT 'mrf.geo_zip_lookup'::regclass::oid::bigint")) == serving_oid
    assert (
        await generation.read_reference_family_result_generation_authority(
            session, importer_id="geo", schema_name="mrf"
        )
        == baseline
    )


async def _exchange_retained_geo(session, manifest, schema, oid, filenode):
    """Adopt the same authenticated retained heap through the normal generation CAS."""
    await session.execute(
        text(f'LOCK TABLE ONLY "mrf".geo_zip_lookup,{archive._quoted(schema)}.geo_zip_lookup IN ACCESS EXCLUSIVE MODE')
    )
    current_oid, current_file = (
        await session.execute(
            text(
                "SELECT 'mrf.geo_zip_lookup'::regclass::oid::bigint,pg_relation_filenode('mrf.geo_zip_lookup')::bigint"
            )
        )
    ).one()
    cut = await geo_source_storage.capture_geo_retained_cut(
        session, schema_name=schema, expected_relation_oid=oid, expected_relfilenode=filenode, manifest=manifest
    )
    incumbent = archive.ReferenceFamilyIncumbent("geo", "mrf", (("geo_zip_lookup", current_oid),))
    predecessor = await generation._read_immutable_activation_predecessor(session, manifest, incumbent)
    displaced = archive.reference_family_predecessor_schema(uuid4())
    await archive.create_model_family_predecessor_schema(session, "geo", displaced)
    await session.execute(text(f'ALTER TABLE "mrf".geo_zip_lookup SET SCHEMA {archive._quoted(displaced)}'))
    await session.execute(text(f'ALTER TABLE {archive._quoted(schema)}.geo_zip_lookup SET SCHEMA "mrf"'))
    arguments = (
        session,
        manifest,
        incumbent,
        manifest.source_serving_generation,
        (("geo_zip_lookup", oid),),
        predecessor,
    )
    if len(cut.index_oids) != 6:
        with pytest.raises(RuntimeError, match="indexes_incomplete"):
            await generation._adopt_validated_family_generation(*arguments)
    adopted = await generation._adopt_validated_family_generation(*arguments, geo_retained_cut=cut)
    assert adopted.local_generation == predecessor.local_generation
    assert adopted.serving_generation == manifest.source_serving_generation
    assert await session.scalar(text("SELECT 'mrf.geo_zip_lookup'::regclass::oid::bigint")) == oid
    assert await session.scalar(text("SELECT count(*) FROM mrf.geo_zip_lookup")) == manifest.tables[0].row_count
    return displaced, current_oid, current_file, cut


async def _assert_retained_cut_roundtrip(component, manifest, retained_oid, serving):
    """Commit both directions of a retained exchange using actual captured provenance."""
    original_manifest, former_session = await _capture_geo_manifest(component)
    async with source_transaction(component.database, isolation="repeatable read") as session:
        schema, filenode = await _retain_geo_heap(session, retained_oid)
        await _refuse_geo_retained_drift(
            session, schema, retained_oid, filenode, manifest, serving.relation_oids[0], former_session
        )
        displaced, current_oid, current_file, cut = await _exchange_retained_geo(
            session, manifest, schema, retained_oid, filenode
        )
    async with source_transaction(component.database, isolation="repeatable read") as session:
        assert await session.scalar(text("SELECT 'mrf.geo_zip_lookup'::regclass::oid::bigint")) == retained_oid
        with pytest.raises(RuntimeError, match="retained_cut_required"):
            await geo_source_storage._consume_geo_retained_cut(session, "mrf", manifest.source_serving_generation, cut)
        await _exchange_retained_geo(session, original_manifest, displaced, current_oid, current_file)
    async with source_transaction(component.database, isolation="repeatable read") as session:
        assert (
            await generation.read_reference_family_result_generation_authority(
                session, importer_id="geo", schema_name="mrf"
            )
            == serving
        )
        assert await session.scalar(text("SELECT 'mrf.geo_zip_lookup'::regclass::oid::bigint")) == current_oid


async def _prepare_geo_candidate(component):
    """Capture real published provenance and COPY it into closed native stage heaps."""
    reader_database = component.database._reader_database
    assert reader_database is not None and reader_database.session_factory is not None
    async with reader_database.session_factory() as source_session, source_session.begin():
        identity = await source_session.execute(
            text(
                "SELECT session_user,current_user,current_setting('transaction_isolation'),"
                "current_setting('transaction_read_only')"
            )
        )
        assert tuple(identity.first()) == (component.reader, component.reader, "repeatable read", "on")
        capture = await archive.capture_reference_family_source(
            source_session,
            importer_id="geo",
            schema_name="mrf",
            source_metadata={"source_release": "synthetic"},
        )
        assert capture.manifest.source_capture_contract == archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT
        async with source_transaction(component.database, isolation="repeatable read") as session:
            ownership = await archive.precreate_reference_family_restore(session, importer_id="geo", dataset_id=uuid4())
            owner = await _writer(session)
            await archive.seal_model_family_storage(session, ownership, owner, require_empty=True)
            columns = tuple(str(column.name) for column in geo_import.GeoZipLookup.__table__.columns)
            projected = ",".join(archive._quoted(name) for name in columns)
            await archive.native_copy_projection(
                session,
                f"SELECT {projected} FROM mrf.geo_zip_lookup",
                schema_name=ownership.schema_name,
                table_name="geo_zip_lookup",
                columns=columns,
                max_bytes=1024 * 1024,
                timeout=30,
            )
            await archive.complete_reference_family_restore(session, ownership)
            await archive.seal_model_family_storage(session, ownership, owner)
            indexes = await _index_names(session, ownership.schema_name)
            assert indexes == {
                name
                if name == "geo_zip_lookup_pkey"
                else archive._index_name_for_table("geo_zip_lookup", f"{ownership.schema_name}_{name}")
                for name in _EXPECTED_INDEX_NAMES
            }
            return ownership, capture.manifest, indexes


async def _roundtrip_native_archive(component, ownership, path):
    """Dump/restore only this task-owned stage, without changing its physical identity."""
    schema = ownership.schema_name
    url = component.admin_engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    subprocess.run(
        ["pg_dump", "-Fc", "--data-only", "--table", f"{schema}.geo_zip_lookup", "--file", str(path), url],
        check=True,
        capture_output=True,
        timeout=30,
    )
    assert path.stat().st_size <= 1024 * 1024
    async with component.admin_engine.begin() as connection:
        await connection.execute(text(f'TRUNCATE "{schema}".geo_zip_lookup'))
    subprocess.run(
        ["pg_restore", "--data-only", "--dbname", url, str(path)], check=True, capture_output=True, timeout=30
    )
    async with source_transaction(component.database) as session:
        await archive.verify_reference_family_stage_ownership(session, ownership)
        assert await session.scalar(text(f'SELECT city FROM "{schema}".geo_zip_lookup')) == "New York"


async def _activate(session, ownership, manifest, *, mode, incoming=None):
    incumbent = await archive.capture_reference_family_incumbent(session, importer_id="geo", schema_name="mrf")
    owner = await _writer(session)
    package = "a" * 64
    receipt = await archive.prepare_reference_family_activation(
        session,
        ownership=ownership,
        manifest=manifest,
        package_id=package,
        profile_contract=archive.CONTRACT,
        sealed_owner_oid=owner,
    )
    source_generation = manifest.source_serving_generation.as_dict() if incoming is None else incoming
    return await archive.activate_validated_reference_family_stage(
        session,
        ownership=ownership,
        manifest=manifest,
        expected_incumbent=incumbent,
        validation_receipt=receipt,
        cutover=archive.ReferenceFamilyCutoverAuthority(
            package, archive.CONTRACT, owner, owner, mode, source_generation
        ),
    )


async def _assert_atomic_activation(component, old_stage, new_stage, initial_authority, successor_authority):
    old_ownership, old_manifest, _ = old_stage
    ownership, manifest, stage_indexes = new_stage
    async with source_transaction(component.database, isolation="repeatable read") as session:
        await _activate(session, old_ownership, old_manifest, mode="manual")
    async with source_transaction(component.database, isolation="repeatable read") as session:
        baseline = await generation.read_reference_family_result_generation_authority(
            session, importer_id="geo", schema_name="mrf"
        )
        assert baseline.serving_generation == initial_authority.serving_generation
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="stale|unrelated|generation"):
            await _activate(
                session,
                ownership,
                manifest,
                mode="automatic",
                incoming=old_manifest.source_serving_generation.as_dict(),
            )
    async with source_transaction(component.database, isolation="repeatable read") as session:
        async with session.begin_nested() as rollback:
            await _activate(session, ownership, manifest, mode="automatic")
            assert await session.scalar(text("SELECT city FROM mrf.geo_zip_lookup")) == "New York"
            await rollback.rollback()
    async with source_transaction(component.database, isolation="repeatable read") as session:
        assert await session.scalar(text("SELECT city FROM mrf.geo_zip_lookup")) == "Old City"
        assert (
            await generation.read_reference_family_result_generation_authority(
                session, importer_id="geo", schema_name="mrf"
            )
            == baseline
        )
        await _activate(session, ownership, manifest, mode="automatic")
    async with source_transaction(component.database, isolation="repeatable read") as session:
        assert await session.scalar(text("SELECT city FROM mrf.geo_zip_lookup")) == "New York"
        authority = await generation.read_reference_family_result_generation_authority(
            session, importer_id="geo", schema_name="mrf"
        )
        assert authority.serving_generation == successor_authority.serving_generation
        assert await _index_names(session, "mrf") == stage_indexes
        await require_geo_storage(session, schema_name="mrf")
        predecessor = archive.reference_family_predecessor_schema(ownership.dataset_id)
        assert await session.scalar(text(f'SELECT city FROM "{predecessor}".geo_zip_lookup')) == "Old City"


@pytest.mark.asyncio
@pytest.mark.parametrize("index_profile", ("current", "legacy-pricing", "legacy-native"))
async def test_geo_replacement_keeps_indexes_and_fences_generation_and_failure(tmp_path, monkeypatch, index_profile):
    """Actual producer generations, COPY, native archive and atomic rollback; component only."""
    async with geo_source_component_database(monkeypatch, index_profile=index_profile) as component:
        bootstrap_manifest, bootstrap_oid = await _assert_bootstrap_archive_transition(component, index_profile)
        monkeypatch.setattr(geo_import, "db", component.database)
        initial_authority = await _load_snapshot(component, tmp_path, "Old City")
        await _assert_retained_cut_roundtrip(component, bootstrap_manifest, bootstrap_oid, initial_authority)
        old_stage = await _prepare_geo_candidate(component)
        successor_authority = await _load_snapshot(component, tmp_path, "New York")
        await _assert_retained_cut_roundtrip(
            component, old_stage[1], initial_authority.relation_oids[0], successor_authority
        )
        new_stage = await _prepare_geo_candidate(component)
        await _roundtrip_native_archive(component, new_stage[0], tmp_path / "geo.dump")
        await _assert_atomic_activation(component, old_stage, new_stage, initial_authority, successor_authority)
