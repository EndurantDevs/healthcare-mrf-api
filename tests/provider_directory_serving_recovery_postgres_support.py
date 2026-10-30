# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real ordinary serving heaps in the existing disposable database boundary."""

import importlib
import importlib.util
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import entity_address_result_generation as generations
from tests.provider_directory_entities_postgres_support import _database_url
from tests.ptg2_serving_address_evidence_postgres_support import _create_geo_assurance_state_table
from tests.test_provider_directory_dataset_artifact_db import _create_artifact_tables, _insert_fixture_rows

directory = importlib.import_module("process.provider_directory_fhir")


async def _create_serving(database, schema):
    for root in ("provider_directory_address_overlay", *generations.RELATION_NAMES):
        await database.status(f'CREATE TABLE "{schema}"."{root}" (marker text PRIMARY KEY)')
        await database.status(f'INSERT INTO "{schema}"."{root}" VALUES (\'old\')')
    await database.status(f'''CREATE TABLE "{schema}".address_alias_state_v1 (
        singleton boolean PRIMARY KEY, schema_version smallint NOT NULL,
        active_ruleset_version smallint NOT NULL, generation bigint NOT NULL)''')
    await database.status(f'INSERT INTO "{schema}".address_alias_state_v1 VALUES (true,2,1,0)')
    await database.status(f'''CREATE TABLE "{schema}".address_alias_artifact_state_v1 (
        artifact_name text PRIMARY KEY, generation bigint NOT NULL, updated_at timestamptz NOT NULL)''')
    await database.status(f'''INSERT INTO "{schema}".address_alias_artifact_state_v1
        VALUES ('provider_directory_address_overlay',0,clock_timestamp())''')
    await _create_geo_assurance_state_table(database, schema)
    await database.status(f'''CREATE TABLE "{schema}".entity_address_result_generation (
        singleton boolean PRIMARY KEY, local_lineage_id uuid NOT NULL,
        local_generation bigint NOT NULL, origin_lineage_id uuid, origin_generation bigint,
        published_at timestamptz, relation_oids bigint[])''')
    await database.status(
        f'INSERT INTO "{schema}".entity_address_result_generation VALUES (true,:lineage,0,NULL,NULL,NULL,NULL)',
        lineage=uuid4(),
    )
    async with database.transaction():
        await generations.publish_local_entity_address_generation(database, schema_name=schema)
    path = Path(__file__).parents[1] / "alembic/versions/20261023100000_provider_directory_serving_recovery.py"
    spec = importlib.util.spec_from_file_location("native_serving_recovery_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    def apply(connection):
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()

    async with database.engine.begin() as connection:
        await connection.run_sync(apply)


async def _remove_owned_catalog(administrator, ownership):
    if not ownership.is_registered:
        return
    async with administrator.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA IF EXISTS "{ownership.schema}" CASCADE'))
        await connection.execute(text(f'DROP ROLE IF EXISTS "{ownership.role}"'))
        assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": ownership.schema}) is None
        assert not await connection.scalar(
            text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:role)"), {"role": ownership.role}
        )


async def _seed_source(database, schema):
    await _create_artifact_tables(database, schema)
    await _insert_fixture_rows(database, schema)
    await database.status(f'''UPDATE "{schema}".provider_directory_endpoint_dataset dataset
        SET dataset_hash=(SELECT encode(sha256(convert_to(string_agg(payload_hash,''
            ORDER BY resource_type,resource_id),'UTF8')),'hex')
            FROM "{schema}".provider_directory_dataset_resource resource
            WHERE resource.dataset_id=dataset.dataset_id)
        WHERE dataset_id='dataset_shared' ''')
    await _create_serving(database, schema)
    return await directory._resolve_provider_directory_artifact_datasets(["source_primary"])


@asynccontextmanager
async def ordinary_serving_case(monkeypatch):
    """Register one role/schema before creation and close resources in dependency order."""
    url = _database_url()
    identity, password = uuid4().hex, uuid4().hex
    role, schema = "serving_builder_" + identity[:16], "serving_recovery_" + identity
    administrator = create_async_engine(url)
    engine = create_async_engine(url.set(username=role, password=password))
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    ownership = SimpleNamespace(schema=schema, role=role, is_registered=False)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    monkeypatch.setattr(directory, "db", database)
    async with AsyncExitStack() as contexts:
        contexts.push_async_callback(administrator.dispose)
        contexts.push_async_callback(_remove_owned_catalog, administrator, ownership)
        contexts.push_async_callback(database.disconnect)
        async with administrator.begin() as connection:
            assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
            assert not await connection.scalar(
                text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:role)"), {"role": role}
            )
            ownership.is_registered = True
            await connection.execute(
                text(
                    f'CREATE ROLE "{role}" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS '
                    f"PASSWORD '{password}'"
                )
            )
            await connection.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{role}"'))
        async with database.transaction():
            actor = await database.first(
                "SELECT session_user::text,current_user::text,rolsuper,rolcreatedb,"
                "rolcreaterole,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=current_user"
            )
            assert tuple(actor[:2]) == (role, role) and not any(actor[2:])
        fence = await _seed_source(database, schema)
        selected = fence.datasets[0]
        pin_by_field = dict(
            source_id=selected.source_id,
            endpoint_id=selected.endpoint_id,
            dataset_id=selected.dataset_id,
            dataset_hash=selected.dataset_hash,
            parent_run_id=selected.evidence_run_id,
        )
        yield SimpleNamespace(
            database=database,
            schema=schema,
            directory=directory,
            pin=pin_by_field,
            fence=fence,
            role=role,
            administrator=administrator,
        )
