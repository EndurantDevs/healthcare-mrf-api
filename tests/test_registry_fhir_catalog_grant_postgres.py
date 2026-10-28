# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Owned native SourceReader sessions fence current publication and close on cancellation."""

import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from db.registry_fhir_catalog import table_sql
from process import registry_fhir_catalog_grant as grants
from process.registry_fhir_catalog_source_fence import require_fhir_catalog_source_publication_fence
from process.registry_fhir_catalog_validation import canonical_json, metadata_generation
from tests.test_registry_fhir_catalog_custody_postgres import expected_snapshot, freeze_metadata
from tests.test_registry_fhir_catalog_custody_postgres import metadata_custody as metadata_custody
from tests.test_registry_fhir_catalog_grant import command, snapshot
from tests.test_registry_fhir_catalog_sources import descriptor


@pytest.fixture
async def grant_reader(metadata_custody, monkeypatch):
    fixture = metadata_custody
    source_schema = "fc_grant_source_" + uuid4().hex
    fixture.schemas.append(source_schema)
    namespace = '"' + source_schema + '"'
    await fixture.connection.execute(f'CREATE SCHEMA {namespace} AUTHORIZATION "{fixture.roles["owner"]}"')
    await fixture.connection.execute(f"CREATE TABLE {namespace}.source_state(head text NOT NULL)")
    await fixture.connection.execute(f"INSERT INTO {namespace}.source_state VALUES('old')")
    await fixture.connection.execute(f'ALTER TABLE {namespace}.source_state OWNER TO "{fixture.roles["owner"]}"')
    await fixture.connection.execute(f'GRANT USAGE ON SCHEMA {namespace} TO "{fixture.roles["reader"]}"')
    await fixture.connection.execute(f'GRANT SELECT ON {namespace}.source_state TO "{fixture.roles["reader"]}"')
    profile = grants.FHIRCatalogSourceReadProfile(source_schema, fixture.roles["owner"], (fixture.roles["reader"],))
    grant = grants.FHIRCatalogStandingGrant("synthetic-native-policy", 1, fixture.control_schema, (profile,))
    source_descriptor = copy.deepcopy(descriptor())
    source_descriptor["endpoint_id"] = "grant-endpoint-" + uuid4().hex
    source_descriptor["coordinates"]["dataset_schema"] = source_schema
    source_descriptor["native_read_profile"] = profile.document()
    source_descriptor["database_oid"] = await fixture.connection.fetchval(
        "SELECT oid::bigint FROM pg_database WHERE datname=current_database()"
    )
    selected = snapshot(grant, (source_descriptor,))
    selected["schema_name"] = fixture.target.schema_name
    # Compact metadata custody and Source whole-family validation each have independent native coverage.
    # This component exercises the runtime's real reader identity, transaction and session-fence ownership.
    monkeypatch.setattr(grants, "_protected_snapshot", AsyncMock(return_value=selected))
    generation = selected["source_vector_json"][0]["source_generation"]
    monkeypatch.setattr(grants, "_descriptors", AsyncMock(return_value={generation: source_descriptor}))
    pool = await asyncpg.create_pool(fixture.dsn, user=fixture.roles["reader"], min_size=1, max_size=1)
    try:
        yield SimpleNamespace(
            native=fixture,
            source_schema=source_schema,
            profile=profile,
            source_descriptor=source_descriptor,
            grant=grant,
            snapshot=selected,
            pool=pool,
            runtime=grants.build_fhir_catalog_publication_runtime(grant, pool),
        )
    finally:
        await pool.close()


async def _authorize(fixture):
    return await fixture.runtime.authorize_metadata(
        command=command(fixture.grant, fixture.snapshot),
        snapshot=fixture.snapshot,
        connection=fixture.native.connection,
    )


async def _current_head(fixture, reader, descriptors, *, lock):
    assert lock is True and descriptors == (fixture.source_descriptor,)
    assert await reader.fetchval("SHOW transaction_isolation") == "repeatable read"
    assert await reader.fetchval("SHOW transaction_read_only") == "on"
    await require_fhir_catalog_source_publication_fence(reader, descriptors)
    return await reader.fetchval(f'SELECT head FROM "{fixture.source_schema}".source_state')


@pytest.mark.asyncio
async def test_fresh_reader_snapshot_after_publisher_commit_and_fence_through_writer_commit(grant_reader, monkeypatch):
    fixture, entered = grant_reader, asyncio.Event()
    observed_heads = []

    async def current(reader, descriptors, *, lock):
        observed_heads.append(await _current_head(fixture, reader, descriptors, lock=lock))
        return (fixture.snapshot["source_vector_json"][0]["source_generation"],)

    monkeypatch.setattr(grants, "require_current_fhir_catalog_sources", current)
    publisher = fixture.native.publisher
    transaction = publisher.transaction()
    await transaction.start()
    await publisher.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", fixture.source_descriptor["endpoint_id"]
    )
    await publisher.execute(f"UPDATE \"{fixture.source_schema}\".source_state SET head='new'")

    async def publish_metadata():
        async with fixture.runtime.source_fence(snapshot=fixture.snapshot, connection=fixture.native.connection):
            entered.set()
            await _authorize(fixture)
            assert (
                await fixture.runtime.verify_sources(snapshot=fixture.snapshot, connection=fixture.native.connection)
                is True
            )
            async with fixture.native.connection.transaction():
                await fixture.native.connection.execute("SELECT 1")
            # The Source session fence still spans the writer's successful transaction completion.
            assert not await publisher.fetchval(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", fixture.source_descriptor["endpoint_id"]
            )

    publication = asyncio.create_task(publish_metadata())
    try:
        await asyncio.sleep(0.05)
        assert not entered.is_set()
        await transaction.commit()
        await publication
        assert observed_heads == ["new"]
        assert await publisher.fetchval(
            "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", fixture.source_descriptor["endpoint_id"]
        )
    finally:
        if publisher.is_in_transaction():
            await transaction.rollback()
        if not publication.done():
            publication.cancel()
            with pytest.raises(asyncio.CancelledError):
                await publication


@pytest.mark.asyncio
async def test_reader_snapshot_and_native_locks_close_after_cancellation(grant_reader, monkeypatch):
    fixture, entered = grant_reader, asyncio.Event()
    monkeypatch.setattr(grants, "require_current_fhir_catalog_sources", AsyncMock(return_value=()))

    async def publication():
        async with fixture.runtime.source_fence(snapshot=fixture.snapshot, connection=fixture.native.connection):
            await _authorize(fixture)
            assert (
                await fixture.runtime.verify_sources(snapshot=fixture.snapshot, connection=fixture.native.connection)
                is True
            )
            entered.set()
            await asyncio.Event().wait()

    running = asyncio.create_task(publication())
    await asyncio.wait_for(entered.wait(), 5)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    async with fixture.pool.acquire() as reader:
        assert not reader.is_in_transaction()
        assert not await reader.fetchval(
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory')"
        )
    with pytest.raises(grants.FHIRCatalogGrantError, match="source_context_required"):
        await fixture.runtime.verify_sources(snapshot=fixture.snapshot, connection=fixture.native.connection)


@pytest.mark.asyncio
async def test_inherited_reader_accepts_select_and_rejects_owner_path(grant_reader):
    fixture = grant_reader
    await fixture.native.connection.execute(
        f'GRANT "{fixture.native.roles["reader"]}" TO "{fixture.native.roles["loader"]}"'
    )
    await grants._reader_identity(fixture.native.loader, fixture.native.connection, (fixture.source_descriptor,))
    changed_descriptor_by_field = {
        **fixture.source_descriptor,
        "database_oid": fixture.source_descriptor["database_oid"] + 1,
    }
    with pytest.raises(grants.FHIRCatalogGrantError, match="database_changed"):
        await grants._reader_identity(fixture.native.loader, fixture.native.connection, (changed_descriptor_by_field,))
    await fixture.native.connection.execute(
        f'GRANT "{fixture.native.roles["owner"]}" TO "{fixture.native.roles["reader"]}" WITH INHERIT FALSE,SET TRUE'
    )
    with pytest.raises(grants.FHIRCatalogGrantError, match="reader_role_unsafe"):
        await grants._reader_identity(fixture.native.reader, fixture.native.connection, (fixture.source_descriptor,))


@pytest.mark.asyncio
async def test_real_closed_empty_snapshot_authority_and_no_source_reader_required(metadata_custody):
    fixture = metadata_custody
    grant = grants.FHIRCatalogStandingGrant("synthetic-empty-policy", 1, fixture.control_schema, ())
    prepared_by_field = {**expected_snapshot(fixture), "standing_grant_sha256": grant.sha256}
    prepared_by_field["metadata_generation"] = metadata_generation(prepared_by_field)
    sealed = await freeze_metadata(fixture, snapshot=prepared_by_field)
    namespace = '"' + fixture.control_schema + '"'
    for statement in table_sql(namespace, "registry_fhir_catalog_snapshot"):
        await fixture.connection.execute(statement)
    fields = tuple(field for field in sealed if field != "published_at")
    values = tuple(canonical_json(sealed[field]) if field.endswith("_json") else sealed[field] for field in fields)
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.registry_fhir_catalog_snapshot ("
        + ",".join(fields)
        + ") VALUES("
        + ",".join(
            f"${index + 1}" + ("::jsonb" if field.endswith("_json") else "") for index, field in enumerate(fields)
        )
        + ")",
        *values,
    )
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    async with runtime.source_fence(snapshot=sealed, connection=fixture.connection):
        async with fixture.connection.transaction(readonly=True):
            receipt = await runtime.authorize_metadata(
                command=command(grant, sealed), snapshot=sealed, connection=fixture.connection
            )
            assert receipt["authorized"] is True
            assert await runtime.verify_sources(snapshot=sealed, connection=fixture.connection) is True
