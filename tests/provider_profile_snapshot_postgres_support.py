# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Small serving families in the existing UUID-owned native database lifecycle."""

from contextlib import asynccontextmanager
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import provider_profile, provider_profile_cms, provider_profile_states
from api import provider_profile_snapshot as snapshot
from api.endpoint import npi
from db.connection import Database
from tests.provider_directory_entities_postgres_support import _database_url
from tests.reference_family_generation_fixture import generation_shape_check, install_source_generation_guards

TABLES = (
    *snapshot._DOCTORS_TABLES,
    "provider_directory_profile",
    "provider_directory_address_overlay",
    *snapshot.address_generation.RELATION_NAMES,
)


@asynccontextmanager
async def snapshot_database(monkeypatch, *, populated=True):
    """Create and remove only one task-owned schema on an explicitly owned database."""
    schema = "profile_snapshot_" + uuid4().hex
    engine = create_async_engine(_database_url())
    database = Database()
    database.engine = engine
    database.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    for module in (npi, provider_profile, provider_profile_cms, provider_profile_states):
        monkeypatch.setattr(module, "db", database)
    for model in (
        provider_profile.ProviderProfileProjection,
        provider_profile_cms.CMSDoctorEducation,
        provider_profile_states.ProviderProfileSourcePublication,
    ):
        monkeypatch.setattr(model.__table__, "schema", schema)
    try:
        await database.status(f'CREATE SCHEMA "{schema}"')
        if populated:
            await create_families(database, schema)
        yield database, schema
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
        await engine.dispose()


async def create_families(database, schema):
    """Stage old and new marker rows before a reader takes its snapshot."""
    for table in TABLES:
        for suffix, marker in (("", "old"), ("_next", "new")):
            await database.status(f'CREATE TABLE "{schema}".{table}{suffix} (marker text)')
            await database.status(f'INSERT INTO "{schema}".{table}{suffix} VALUES (:marker)', marker=marker)
    await database.status(f'''CREATE TABLE "{schema}".reference_family_result_generation (
        importer_id text PRIMARY KEY, local_lineage_id uuid, local_generation bigint,
        origin_lineage_id uuid, origin_generation bigint, published_at timestamptz, relation_oids bigint[],
        CONSTRAINT reference_family_result_generation_shape_check CHECK ({generation_shape_check()}))''')
    await database.status(
        f'''INSERT INTO "{schema}".reference_family_result_generation
        (importer_id,local_lineage_id,local_generation) VALUES ('cms-doctors',:lineage,0)''',
        lineage=uuid4(),
    )
    async with database.transaction() as session:
        await install_source_generation_guards(await session.connection(), schema)
        await snapshot.reference_generation.publish_local_reference_family_generation(
            session,
            importer_id="cms-doctors",
            schema_name=schema,
        )
    await database.status(f'''CREATE TABLE "{schema}".entity_address_result_generation (
        singleton boolean PRIMARY KEY, local_lineage_id uuid, local_generation bigint,
        origin_lineage_id uuid, origin_generation bigint, published_at timestamptz, relation_oids bigint[])''')
    await database.status(
        f'''INSERT INTO "{schema}".entity_address_result_generation
        VALUES (true,:lineage,1,:lineage,1,now(),{address_oids_sql(schema)})''',
        lineage=uuid4(),
    )


async def create_composite_families(database, schema, *, install_receipt=True):
    """Reuse the receipt's real native migrations in the API fixture's owned schema."""
    from tests import test_provider_directory_cms_serving_receipt_postgres as receipt_fixture

    async with database.engine.begin() as connection:
        await receipt_fixture._create_scalar_tables(connection, schema)
        await connection.run_sync(lambda sync: receipt_fixture._create_profile_table(sync, schema))
        await receipt_fixture._create_native_tables(connection, schema)
        if install_receipt:
            await connection.run_sync(lambda sync: receipt_fixture._apply(sync, "20260930100000"))


def address_oids_sql(schema):
    """Resolve the address authority's fixed native family order."""
    return (
        "ARRAY["
        + ",".join(
            f"'\"{schema}\".{table}'::regclass::oid::bigint" for table in snapshot.address_generation.RELATION_NAMES
        )
        + "]"
    )


async def publish_family(session, schema, started=None):
    """Rename every serving family and advance its native receipt in one commit."""
    async with session.begin():
        await session.execute(text("SET LOCAL statement_timeout = '5s'"))
        if started is not None:
            started.set()
        for table in TABLES:
            await session.execute(text(f'ALTER TABLE "{schema}".{table} RENAME TO {table}_old'))
            await session.execute(text(f'ALTER TABLE "{schema}".{table}_next RENAME TO {table}'))
        await snapshot.reference_generation.publish_local_reference_family_generation(
            session,
            importer_id="cms-doctors",
            schema_name=schema,
        )
        await session.execute(
            text(f'''UPDATE "{schema}".entity_address_result_generation
            SET local_generation=2,origin_generation=2,published_at=clock_timestamp(),
                relation_oids={address_oids_sql(schema)}''')
        )
