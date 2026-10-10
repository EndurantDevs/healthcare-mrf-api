# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise actual additive Alembic upgrades on an explicitly isolated DB."""

import importlib.util
import os
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.fixture
async def serving_schema():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    schema = "network_serving_test_" + uuid4().hex
    engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://"))
    previous_schema_by_name = {name: os.environ.get(name) for name in ("HLTHPRT_DB_SCHEMA", "DB_SCHEMA")}
    os.environ.update({name: schema for name in previous_schema_by_name})
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        filenames = (
            "20261007010000_managed_network_registry.py",
            "20261007020000_registry_revision_history.py",
            "20261007030000_network_serving_control.py",
            "20261007040000_registry_source_evidence.py",
            "20261007050000_canonical_address_network_ids.py",
            "20261007060000_registry_approved_selection.py",
            "20261007070000_registry_company_links.py",
            "20261007080000_manual_directory_registry.py",
            "20261007090000_network_membership_drafts.py",
            "20261007100000_registry_publication_requests.py",
            "20261007110000_registry_site_bindings.py",
            "20261007120000_company_network_assertions.py",
            "20261007130000_registry_network_bindings.py",
            "20261009020000_company_registry_assertions.py",
            "20261009030000_network_catalog_evidence.py",
            "20261007140000_registry_source_recipes.py",
        )
        async with engine.begin() as sqlalchemy_connection:
            for filename in filenames:
                path = Path(__file__).parents[1] / "alembic/versions" / filename
                spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), path)
                migration = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(migration)

                def upgrade(sync_connection):
                    with Operations.context(MigrationContext.configure(sync_connection)):
                        migration.upgrade()

                await sqlalchemy_connection.run_sync(upgrade)
        yield connection, schema, engine
    finally:
        for name, previous in previous_schema_by_name.items():
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous
        await engine.dispose()
        try:
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        finally:
            await connection.close()


async def _candidate(connection, schema, candidate_id, *, source="edition-a"):
    await connection.execute(
        f'INSERT INTO "{schema}".network_membership_candidate '
        "(candidate_id,dataset_id,schema_id,producer_id,schema_name,source_generations,approved_custom_revision,expected_head,expected_rows) "
        "VALUES($1,$2,$3,$4,$5,jsonb_build_object('fhir',$6::text),0,0,0)",
        candidate_id,
        uuid4(),
        uuid4(),
        uuid4(),
        "network_candidate_" + candidate_id.hex,
        source,
    )


@pytest.mark.asyncio
async def test_migration_and_independent_draft_source_boundaries(serving_schema):
    connection, schema, engine = serving_schema
    await connection.execute(f'UPDATE "{schema}".registry_revision_control SET draft_revision=7,approved_revision=5')
    candidate_ids = [uuid4(), uuid4()]
    for candidate_id in candidate_ids:
        await _candidate(connection, schema, candidate_id)
        await connection.execute(
            f"UPDATE \"{schema}\".network_membership_candidate SET state='ready',validation_json='{{\"valid\":true}}',index_ready=true WHERE candidate_id=$1",
            candidate_id,
        )
    generation_ids = []
    for candidate_id in candidate_ids:
        generation_ids.append(
            await connection.fetchval(
                f'INSERT INTO "{schema}".network_serving_manifest '
                "(candidate_id,schema_revision,source_generations,approved_custom_revision,manifest_sha256) "
                "VALUES($1,1,jsonb_build_object('fhir','edition-a'),5,$2) RETURNING generation_id",
                candidate_id,
                "a" * 64,
            )
        )
    await connection.execute(
        f'UPDATE "{schema}".network_serving_control SET generation_id=$1 WHERE id=1', generation_ids[0]
    )
    async with engine.connect() as reader:
        await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        head_query = text(f'SELECT generation_id FROM "{schema}".network_serving_control WHERE id=1')
        assert await reader.scalar(head_query) == generation_ids[0]
        await connection.execute(
            f'UPDATE "{schema}".network_serving_control SET generation_id=$1 WHERE id=1', generation_ids[1]
        )
        assert await reader.scalar(head_query) == generation_ids[0]
    assert (
        await connection.fetchval(f'SELECT generation_id FROM "{schema}".network_serving_control') == generation_ids[1]
    )
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (7, 5)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_serving_manifest') == 2
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND NOT t.tgisinternal",
            schema,
        )
        == 0
    )


@pytest.mark.asyncio
async def test_native_scope_accounting_and_ready_constraints(serving_schema):
    connection, schema, _ = serving_schema
    candidate_id = uuid4()
    await _candidate(connection, schema, candidate_id)
    rejected_statements = [
        "state='ready'",
        "state='validated'",
        "accepted_rows=1",
        "schema_name='wrong-candidate'",
        "producer_id='00000000-0000-0000-0000-000000000000'::uuid",
        "source_generations='[]'::jsonb",
        "source_recipes_json='{}'::jsonb",
        "source_recipes_json=jsonb_build_array(repeat('x',1048576))",
        "source_recipes_json=(SELECT jsonb_agg(0) FROM generate_series(1,101))",
    ]
    for assignment in rejected_statements:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f'UPDATE "{schema}".network_membership_candidate SET {assignment} WHERE candidate_id=$1', candidate_id
            )
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await connection.execute(f'UPDATE "{schema}".network_serving_control SET generation_id=1')
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await connection.execute(
            f'INSERT INTO "{schema}".network_membership_batch(candidate_id,batch_id,row_count,input_sha256,copy_sha256,input_bytes,copy_bytes) '
            "VALUES($1,$2,0,$3,$3,0,21)",
            uuid4(),
            uuid4(),
            "a" * 64,
        )


@pytest.mark.asyncio
async def test_source_edition_history_and_explicit_unresolved_assertions(serving_schema):
    connection, schema, _ = serving_schema
    snapshot_id = uuid4()
    await connection.execute(
        f'INSERT INTO "{schema}".registry_source_snapshot '
        "(snapshot_id,source_system,source_id,edition_id,source_url,artifact_sha256,input_sha256,parser_version,reporting_year,published_at) "
        "VALUES($1,'cms','commercial-mlr','2024','https://example.test/source.zip',$2,$3,'parser-v1',2024,'2025-09-12T00:00:00Z')",
        snapshot_id,
        "a" * 64,
        "b" * 64,
    )
    await connection.execute(
        f'INSERT INTO "{schema}".registry_source_observation VALUES($1,\'row:1\',1,\'unresolved\',\'{{"raw_company_pk":"","hios":"00123"}}\',\'[]\')',
        snapshot_id,
    )
    await connection.execute(
        f'INSERT INTO "{schema}".registry_issuer_company_assertion '
        "(snapshot_id,source_record_key,hios_issuer_id,state,resolution_status) VALUES($1,'row:1','00123','NY','unresolved')",
        snapshot_id,
    )
    issuer_assertion = await connection.fetchrow(
        f'SELECT hios_issuer_id,company_id,valid_from,valid_to FROM "{schema}".registry_issuer_company_assertion'
    )
    assert tuple(issuer_assertion) == ("00123", None, None, None)
    assert await connection.fetchval(f'SELECT reporting_year FROM "{schema}".registry_source_snapshot') == 2024
    assert (
        await connection.fetchval(
            f'SELECT extract(year FROM published_at)::int FROM "{schema}".registry_source_snapshot'
        )
        == 2025
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            f"UPDATE \"{schema}\".registry_issuer_company_assertion SET resolution_status='resolved'"
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(f"UPDATE \"{schema}\".registry_issuer_company_assertion SET hios_issuer_id='123'")
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_group_registry') == 0


async def _upgrade_address_column(engine):
    filename = "20261007050000_canonical_address_network_ids.py"
    path = Path(__file__).parents[1] / "alembic/versions" / filename
    spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    async with engine.begin() as connection:

        def upgrade(sync_connection):
            with Operations.context(MigrationContext.configure(sync_connection)):
                migration.upgrade()

        await connection.run_sync(upgrade)


@pytest.mark.asyncio
async def test_address_ids_are_additive_and_keep_checksums(serving_schema):
    connection, schema, engine = serving_schema
    await connection.execute(
        f'CREATE TABLE "{schema}".entity_address_unified '
        "(location_key TEXT PRIMARY KEY,plans_network_array INTEGER[] NOT NULL)"
    )
    await connection.execute(f"INSERT INTO \"{schema}\".entity_address_unified VALUES('office-a',ARRAY[42,-5])")
    await _upgrade_address_column(engine)
    assert tuple(await connection.fetchrow(f'SELECT * FROM "{schema}".entity_address_unified')) == (
        "office-a",
        [42, -5],
        [],
    )
    await connection.execute(f'UPDATE "{schema}".entity_address_unified SET canonical_network_ids=ARRAY[7,42]')
    await _upgrade_address_column(engine)
    assert await connection.fetchval(f'SELECT canonical_network_ids FROM "{schema}".entity_address_unified') == [7, 42]
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM pg_indexes WHERE schemaname=$1 AND tablename='entity_address_unified'", schema
        )
        == 1
    )
    with pytest.raises(asyncpg.NotNullViolationError):
        await connection.execute(f'UPDATE "{schema}".entity_address_unified SET canonical_network_ids=NULL')


@pytest.mark.asyncio
async def test_address_id_migration_refuses_text_aliases(serving_schema):
    connection, schema, engine = serving_schema
    await connection.execute(f'CREATE TABLE "{schema}".entity_address_unified(canonical_network_ids TEXT[])')
    with pytest.raises(ValueError, match="integer array"):
        await _upgrade_address_column(engine)
    assert (
        await connection.fetchval(
            "SELECT atttypid::regtype::text FROM pg_attribute WHERE attrelid=to_regclass($1) AND attname='canonical_network_ids'",
            f'"{schema}".entity_address_unified',
        )
        == "text[]"
    )
