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

READ_TABLES = tuple(
    dict.fromkeys(
        (
            *snapshot._DOCTORS_TABLES,
            *snapshot._PROFILE_TABLES,
            *snapshot._DETAIL_TABLES,
            snapshot._PROFILE_GENERATION_TABLE,
            snapshot.reference_generation.TABLE_NAME,
            snapshot.address_generation.TABLE_NAME,
            snapshot._CMS_RECEIPT_TABLE,
            "address_alias_state_v1",
            "address_alias_artifact_state_v1",
            "provider_directory_cms_candidate_coverage",
            "provider_directory_cms_serving_coverage",
            "provider_directory_cms_npd_relationship_receipt",
            "cms_native_input_revision",
        )
    )
)


async def grant_profile_reader(database, schema, table_names):
    """Grant only existing, explicitly declared current or successor fixture relations."""
    role = database._reader_database._reader_login[0]
    async with database.engine.begin() as connection:
        for name in dict.fromkeys(table_names):
            if await connection.scalar(text("SELECT to_regclass(:table)"), {"table": f'"{schema}"."{name}"'}):
                await connection.execute(text(f'GRANT SELECT ON "{schema}"."{name}" TO "{role}"'))


async def _cleanup_profile_reader(database, schema, role):
    """Close the Reader before revoking its exact schema grants, including renamed heaps."""
    reader = database._reader_database
    if reader is not None:
        await reader.disconnect()
        database._reader_database = None
    async with database.engine.begin() as connection:
        if await connection.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}):
            rows = await connection.execute(
                text(
                    "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "CROSS JOIN LATERAL aclexplode(c.relacl) acl JOIN pg_roles r ON r.oid=acl.grantee "
                    "WHERE n.nspname=:schema AND r.rolname=:role AND acl.privilege_type='SELECT'"
                ),
                {"schema": schema, "role": role},
            )
            for name in rows.scalars():
                await connection.execute(text(f'REVOKE SELECT ON "{schema}"."{name}" FROM "{role}"'))
            await connection.execute(text(f'REVOKE USAGE ON SCHEMA "{schema}" FROM "{role}"'))
            await connection.execute(text(f'DROP ROLE "{role}"'))
        assert not await connection.scalar(
            text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
        )


@asynccontextmanager
async def profile_reader(database, schema, monkeypatch):
    """Authenticate a distinct read-only login within the caller's guarded database lifecycle."""
    role = "profile_reader_" + uuid4().hex
    password = uuid4().hex
    url = database.engine.url
    has_cleanup_authority = False
    assert database._reader_database is None
    try:
        async with database.engine.begin() as connection:
            assert not await connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
            )
            has_cleanup_authority = True
            await connection.execute(
                text(
                    f'CREATE ROLE "{role}" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
                )
            )
            try:
                await connection.execute(text(f"ALTER ROLE \"{role}\" PASSWORD '{password}'"))
            except Exception:
                raise RuntimeError("Reader fixture login provisioning failed") from None
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"'))
        settings_by_name = {
            "DRIVER": "asyncpg",
            "HOST": url.host,
            "PORT": str(url.port or 5432),
            "DATABASE": url.database,
            "READER_USER": role,
            "READER_PASSWORD": password,
            "READER_POOL_MIN_SIZE": "1",
            "READER_POOL_MAX_SIZE": "5",
            "ECHO": "False",
        }
        for name, setting_value in settings_by_name.items():
            monkeypatch.setenv("HLTHPRT_DB_" + name, setting_value)
        await database._connect_reader()
        await grant_profile_reader(database, schema, READ_TABLES)
        yield
    finally:
        if has_cleanup_authority:
            await _cleanup_profile_reader(database, schema, role)


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
        async with profile_reader(database, schema, monkeypatch):
            if populated:
                await grant_profile_reader(database, schema, (name + "_next" for name in TABLES))
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
    await grant_profile_reader(database, schema, READ_TABLES)


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
