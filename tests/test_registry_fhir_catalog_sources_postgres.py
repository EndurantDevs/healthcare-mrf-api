# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native paged inventory SQL and existing endpoint advisory publication ordering."""

import asyncio
import copy
import os
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from process import registry_fhir_catalog_source_fence as fence
from process import registry_fhir_catalog_sources as sources
from tests.test_registry_fhir_catalog_sources import descriptor, published_row


@pytest.fixture
async def source_database():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an isolated database")
    connections, schemas = [], []
    try:
        for _ in range(3):
            connections.append(await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://")))
        fixture = SimpleNamespace(connection=connections[0], publisher=connections[1], observer=connections[2])
        for kind in ("catalog", "payload"):
            schema = "fc_sources_" + kind + "_" + uuid4().hex
            schemas.append(schema)
            await fixture.connection.execute(f'CREATE SCHEMA "{schema}"')
            setattr(fixture, kind, schema)
        fixture.profile = sources.FHIRCatalogSourceReadProfile(
            fixture.payload, "synthetic_source_owner", ("synthetic_source_reader",), fixture.catalog
        )
        fixture.endpoint = "source-fence-" + uuid4().hex
        fixture.descriptor = copy.deepcopy(descriptor())
        fixture.descriptor["endpoint_id"] = fixture.endpoint
        await fixture.connection.execute(f'CREATE TABLE "{fixture.catalog}".publication_state(head text NOT NULL)')
        await fixture.connection.execute(f"INSERT INTO \"{fixture.catalog}\".publication_state VALUES('old')")
        yield fixture
    finally:
        for connection in connections:
            if not connection.is_closed() and connection.is_in_transaction():
                await connection.execute("ROLLBACK")
        cleanup_connection = next((connection for connection in connections if not connection.is_closed()), None)
        if cleanup_connection is not None:
            for schema in reversed(schemas):
                await cleanup_connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                assert await cleanup_connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        for connection in connections:
            if not connection.is_closed():
                await connection.close()


async def _inventory_tables(fixture):
    catalog = '"' + fixture.catalog + '"'
    await fixture.connection.execute(
        f"CREATE TABLE {catalog}.provider_directory_api_endpoint(endpoint_id text PRIMARY KEY,canonical_api_base text)"
    )
    await fixture.connection.execute(
        f"CREATE TABLE {catalog}.provider_directory_source(source_id text PRIMARY KEY,endpoint_id text)"
    )
    await fixture.connection.execute(
        f"CREATE INDEX source_endpoint_idx ON {catalog}.provider_directory_source(endpoint_id)"
    )
    for schema in (fixture.catalog, fixture.payload):
        await fixture.connection.execute(f"""CREATE TABLE "{schema}".provider_directory_endpoint_dataset(
          dataset_id text PRIMARY KEY,endpoint_id text,dataset_hash text,status text,is_current boolean,
          published_at timestamptz,resource_count bigint,previous_dataset_id text,content_proof_admission_version integer,
          content_proof_admission_kind text,content_proof_admission_sha256 text,content_proof_resource_types text[],
          publication_metadata_sha256 text,publication_metadata_summary_json jsonb,completion_proof_required_version integer,
          completion_proof_json jsonb,completion_proof_sha256 text)""")
        await fixture.connection.execute(
            f'CREATE UNIQUE INDEX source_current_endpoint_idx ON "{schema}".provider_directory_endpoint_dataset(endpoint_id) WHERE is_current'
        )
    await fixture.connection.execute(f"""CREATE TABLE "{fixture.payload}".provider_directory_dataset_resource(
      dataset_id text,resource_type text,resource_id text,payload_hash text,payload_json jsonb) PARTITION BY LIST(dataset_id)""")
    await fixture.connection.execute(
        f'CREATE TABLE "{fixture.payload}".dataset_one PARTITION OF "{fixture.payload}".provider_directory_dataset_resource FOR VALUES IN (\'dataset-one\')'
    )


async def _insert_header(fixture, schema, row):
    fields = [field for field in sources._HEADER_FIELDS if field != "endpoint_id"]
    fields.insert(0, "endpoint_id")
    values_by_field = {
        **row,
        "publication_metadata_summary_json": row["metadata_json"],
        "completion_proof_json": row["completion_json"],
    }
    parameters = [
        f"${index + 1}" + ("::jsonb" if field.endswith("_json") else "") for index, field in enumerate(fields)
    ]
    await fixture.connection.execute(
        f'INSERT INTO "{schema}".provider_directory_endpoint_dataset ('
        + ",".join(fields)
        + ") VALUES("
        + ",".join(parameters)
        + ")",
        *(values_by_field[field] for field in fields),
    )


async def _native_scope(fixture):
    records = await fixture.connection.fetch(
        "SELECT c.oid::bigint,c.relname,c.relowner::bigint,pg_get_expr(c.relpartbound,c.oid) AS bound,"
        "ARRAY(SELECT inhparent::bigint FROM pg_inherits WHERE inhrelid=c.oid) AS parents "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind IN ('r','p')",
        fixture.payload,
    )
    return {
        "family": [dict(record) for record in records if record["relname"] != "provider_directory_endpoint_dataset"],
        "header": next(
            dict(record) for record in records if record["relname"] == "provider_directory_endpoint_dataset"
        ),
        "database_oid": await fixture.connection.fetchval(
            "SELECT oid::bigint FROM pg_database WHERE datname=current_database()"
        ),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("mixed_catalog", [False, True])
async def test_catalog_inventory_needs_only_registration_columns(source_database, mixed_catalog):
    fixture = source_database
    await _inventory_tables(fixture)
    role = "fc_catalog_reader_" + uuid4().hex
    original_catalog = fixture.catalog
    await fixture.connection.execute(f'CREATE ROLE "{role}" NOLOGIN')
    try:
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.catalog}","{fixture.payload}" TO "{role}"')
        await fixture.connection.execute(
            f'GRANT SELECT(source_id,endpoint_id) ON "{fixture.catalog}".provider_directory_source TO "{role}"'
        )
        await fixture.connection.execute(
            f'GRANT SELECT(endpoint_id,canonical_api_base) ON "{fixture.catalog}".provider_directory_api_endpoint TO "{role}"'
        )
        if mixed_catalog:
            fixture.catalog = "CatalogCase_" + uuid4().hex
            await fixture.connection.execute(f'ALTER SCHEMA "{original_catalog}" RENAME TO "{fixture.catalog}"')
        profile = replace(fixture.profile, catalog_schema_name=fixture.catalog)
        assert profile.published_read_scope is None
        async with fixture.observer.transaction(isolation="repeatable_read", readonly=True):
            await fixture.observer.execute(f'SET LOCAL ROLE "{role}"')
            access = await sources._catalog_access(fixture.observer, profile)
            assert access[fixture.catalog + ".provider_directory_source"]["readable"] is True
            assert access[fixture.catalog + ".provider_directory_api_endpoint"]["readable"] is True
            assert (
                await fixture.observer.fetchval(
                    "SELECT has_table_privilege(current_user,$1::oid,'SELECT')",
                    access[fixture.catalog + ".provider_directory_source"]["oid"],
                )
                is False
            )
    finally:
        await fixture.connection.execute(f'DROP OWNED BY "{role}"')
        await fixture.connection.execute(f'DROP ROLE "{role}"')
        if mixed_catalog:
            await fixture.connection.execute(f'ALTER SCHEMA "{fixture.catalog}" RENAME TO "{original_catalog}"')
            fixture.catalog = original_catalog


@pytest.mark.asyncio
async def test_unscoped_access_preserves_registration_identity_after_catalog_rename(source_database):
    fixture = source_database
    await _inventory_tables(fixture)
    original_catalog = fixture.catalog
    renamed_catalog = "CatalogCase_" + uuid4().hex
    original_oid = await fixture.connection.fetchval("SELECT to_regnamespace($1)::oid::bigint", original_catalog)
    original_acl = await fixture.connection.fetchval("SELECT nspacl::text FROM pg_namespace WHERE oid=$1", original_oid)
    prior = await sources._catalog_access(fixture.connection, fixture.profile)
    try:
        await fixture.connection.execute(f'ALTER SCHEMA "{original_catalog}" RENAME TO "{renamed_catalog}"')
        profile = replace(fixture.profile, catalog_schema_name=renamed_catalog)
        assert profile.published_read_scope is None and profile.schema_name == fixture.payload
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            access = await sources._catalog_access(fixture.connection, profile)
        expected_access_by_name = {
            key.replace(original_catalog + ".", renamed_catalog + ".", 1): {
                **access_record,
                "name": key.replace(original_catalog + ".", renamed_catalog + ".", 1),
            }
            for key, access_record in prior.items()
        }
        assert access == expected_access_by_name
        assert (
            await fixture.connection.fetchval(
                "SELECT pg_catalog.to_regnamespace(pg_catalog.format('%I',$1::text))::oid::bigint", renamed_catalog
            )
            == original_oid
        )
        assert (
            await fixture.connection.fetchval("SELECT nspacl::text FROM pg_namespace WHERE oid=$1", original_oid)
            == original_acl
        )
    finally:
        await fixture.connection.execute(f'ALTER SCHEMA "{renamed_catalog}" RENAME TO "{original_catalog}"')


@pytest.mark.asyncio
async def test_native_inventory_groups_aliases_and_reports_legacy_and_missing_heads(source_database, monkeypatch):
    fixture = source_database
    await _inventory_tables(fixture)
    for endpoint, aliases in (
        ("endpoint-one", ("alias-a", "alias-b")),
        ("endpoint-two", ("legacy-alias",)),
        ("endpoint-three", ("unavailable-alias",)),
    ):
        await fixture.connection.execute(
            f'INSERT INTO "{fixture.catalog}".provider_directory_api_endpoint VALUES($1,$2)',
            endpoint,
            "https://example.test/fhir",
        )
        await fixture.connection.executemany(
            f'INSERT INTO "{fixture.catalog}".provider_directory_source VALUES($1,$2)',
            [(alias, endpoint) for alias in aliases],
        )
    await _insert_header(fixture, fixture.payload, published_row())
    await _insert_header(
        fixture,
        fixture.catalog,
        published_row(endpoint_id="endpoint-two", dataset_id="legacy-dataset", aliases=("legacy-alias",)),
    )
    # This isolates native query behavior; the production Source custody verifier has separate coverage.
    monkeypatch.setattr(sources, "_native_source", AsyncMock(return_value=(await _native_scope(fixture), None)))
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        pages = [
            page async for page in sources.iter_fhir_catalog_sources(fixture.connection, fixture.profile, page_size=1)
        ]
    dispositions_by_endpoint = {page[0].endpoint_id: page[0] for page in pages}
    assert len(pages) == 3 and dispositions_by_endpoint["endpoint-one"].status == "accepted"
    assert dispositions_by_endpoint["endpoint-one"].descriptor["source_aliases"] == ["alias-a", "alias-b"]
    assert dispositions_by_endpoint["endpoint-two"].reason == "legacy_source_not_enrolled"
    assert dispositions_by_endpoint["endpoint-three"].reason == "current_dataset_missing"


@pytest.mark.asyncio
async def test_publisher_commit_precedes_fenced_repeatable_snapshot(source_database):
    fixture = source_database
    transaction = fixture.publisher.transaction()
    await transaction.start()
    await fixture.publisher.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", fixture.endpoint)
    entered = asyncio.Event()

    async def metadata_publication():
        async with fence.fhir_catalog_source_publication_fence(
            fixture.connection, (fixture.descriptor,), timeout_seconds=2
        ):
            async with fixture.connection.transaction(isolation="repeatable_read"):
                entered.set()
                await fence.require_fhir_catalog_source_publication_fence(fixture.connection, (fixture.descriptor,))
                return await fixture.connection.fetchval(f'SELECT head FROM "{fixture.catalog}".publication_state')

    publication = asyncio.create_task(metadata_publication())
    try:
        await asyncio.sleep(0.05)
        assert not entered.is_set()
        await fixture.publisher.execute(f"UPDATE \"{fixture.catalog}\".publication_state SET head='new'")
        await transaction.commit()
        assert await publication == "new"
    finally:
        if not publication.done():
            publication.cancel()
        await asyncio.gather(publication, return_exceptions=True)


@pytest.mark.asyncio
async def test_shared_fence_blocks_existing_publisher_lock_until_metadata_commit(source_database):
    fixture = source_database
    async with fence.fhir_catalog_source_publication_fence(fixture.connection, (fixture.descriptor,)):
        async with fixture.connection.transaction(isolation="repeatable_read"):
            async with fixture.publisher.transaction():
                assert (
                    await fixture.publisher.fetchval(
                        "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", fixture.endpoint
                    )
                    is False
                )
            await fence.require_fhir_catalog_source_publication_fence(fixture.connection, (fixture.descriptor,))
    async with fixture.publisher.transaction():
        assert (
            await fixture.publisher.fetchval(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", fixture.endpoint
            )
            is True
        )


@pytest.mark.asyncio
async def test_cancelled_owner_releases_exact_locks_and_preserves_unrelated_session_lock(source_database):
    fixture = source_database
    unrelated = "other-source-fence-" + uuid4().hex
    await fixture.connection.execute("SELECT pg_advisory_lock_shared(hashtextextended($1,0))", unrelated)
    entered = asyncio.Event()

    async def cancelled_publication():
        async with fence.fhir_catalog_source_publication_fence(fixture.connection, (fixture.descriptor,)):
            async with fixture.connection.transaction(isolation="repeatable_read"):
                entered.set()
                await asyncio.Event().wait()

    publication = asyncio.create_task(cancelled_publication())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        publication.cancel()
        with pytest.raises(asyncio.CancelledError):
            await publication
        async with fixture.observer.transaction():
            assert (
                await fixture.observer.fetchval(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", fixture.endpoint
                )
                is True
            )
            assert (
                await fixture.observer.fetchval("SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", unrelated)
                is False
            )
    finally:
        if not publication.done():
            publication.cancel()
        await asyncio.gather(publication, return_exceptions=True)
        assert (
            await fixture.connection.fetchval("SELECT pg_advisory_unlock_shared(hashtextextended($1,0))", unrelated)
            is True
        )


@pytest.mark.asyncio
async def test_busy_fence_timeout_releases_partial_acquisition(source_database):
    fixture = source_database
    second = copy.deepcopy(fixture.descriptor)
    second["endpoint_id"] += "-two"
    async with fixture.publisher.transaction():
        await fixture.publisher.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", second["endpoint_id"])
        with pytest.raises(fence.FHIRCatalogSourceFenceError, match="busy"):
            async with fence.fhir_catalog_source_publication_fence(
                fixture.connection, (fixture.descriptor, second), timeout_seconds=0.1
            ):
                pytest.fail("busy endpoint was admitted")
        async with fixture.observer.transaction():
            assert (
                await fixture.observer.fetchval(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", fixture.endpoint
                )
                is True
            )


@pytest.mark.asyncio
async def test_preexisting_exact_lock_is_refused_and_preserved(source_database):
    fixture = source_database
    await fixture.connection.execute("SELECT pg_advisory_lock_shared(hashtextextended($1,0))", fixture.endpoint)
    try:
        with pytest.raises(fence.FHIRCatalogSourceFenceError, match="preexisting_lock"):
            async with fence.fhir_catalog_source_publication_fence(fixture.connection, (fixture.descriptor,)):
                pytest.fail("preexisting lock was adopted")
        async with fixture.observer.transaction():
            assert (
                await fixture.observer.fetchval(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", fixture.endpoint
                )
                is False
            )
    finally:
        assert (
            await fixture.connection.fetchval(
                "SELECT pg_advisory_unlock_shared(hashtextextended($1,0))", fixture.endpoint
            )
            is True
        )


@pytest.mark.asyncio
async def test_uncommitted_metadata_transaction_retires_connection_and_rolls_back(source_database):
    fixture = source_database
    with pytest.raises(fence.FHIRCatalogSourceFenceError, match="transaction_open"):
        async with fence.fhir_catalog_source_publication_fence(fixture.connection, (fixture.descriptor,)):
            await fixture.connection.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            await fixture.connection.execute(f"UPDATE \"{fixture.catalog}\".publication_state SET head='uncommitted'")
    assert fixture.connection.is_closed()
    assert await fixture.observer.fetchval(f'SELECT head FROM "{fixture.catalog}".publication_state') == "old"
    async with fixture.observer.transaction():
        assert (
            await fixture.observer.fetchval(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", fixture.endpoint
            )
            is True
        )
