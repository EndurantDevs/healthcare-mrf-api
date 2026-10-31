# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Small serving families in the existing UUID-owned native database lifecycle."""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import Context
from uuid import uuid4

from sqlalchemy import Index, MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import provider_profile, provider_profile_cms, provider_profile_states
from api import provider_profile_snapshot as snapshot
from api.endpoint import npi
from db.connection import Database
from process import cms_doctors_immutable as doctors
from process.provider_directory_generic_source_policy import bind_generic_source_transaction_factory, source_transaction
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


async def _enroll_snapshot_doctors(engine, database, schema, monkeypatch, custody):
    """Keep the real Doctors publisher and its exact fixture custody inside the caller's stack."""
    await grant_profile_reader(database, schema, (name + "_next" for name in TABLES))
    from tests.cms_doctors_component_postgres_support import doctors_component_source
    from tests.cms_native_inputs_postgres_support import source_header_models

    component = await custody.enter_async_context(
        doctors_component_source(engine, schema, monkeypatch, reader=database._reader_database._reader_login[0])
    )
    database._snapshot_doctors_component = component
    async with source_transaction(component.database) as session:
        connection = await session.connection()
        for table in source_header_models(schema):
            await connection.run_sync(lambda sync, table=table: table.create(sync, checkfirst=False))
    await grant_profile_reader(database, schema, ("provider_directory_endpoint_dataset",))
    async with engine.begin() as connection:
        for name in TABLES:
            for suffix in ("", "_next"):
                await connection.execute(text(f'ALTER TABLE "{schema}"."{name}{suffix}" OWNER TO "{component.owner}"'))


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
        async with profile_reader(database, schema, monkeypatch), AsyncExitStack() as custody:
            if populated:
                await _enroll_snapshot_doctors(engine, database, schema, monkeypatch, custody)
            yield database, schema
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
        await engine.dispose()


async def create_families(database, schema, *, unified_exists=False):
    """Stage old and new marker rows before a reader takes its snapshot."""
    for name in TABLES:
        if name in snapshot._DOCTORS_TABLES or (unified_exists and name == "entity_address_unified"):
            continue
        for suffix, marker in (("", "old"), ("_next", "new")):
            await database.status(f'CREATE TABLE "{schema}".{name}{suffix} (marker text)')
            await database.status(f'INSERT INTO "{schema}".{name}{suffix} VALUES (:marker)', marker=marker)
    await _create_doctors_families(database, schema)
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
    await database.status(f'''CREATE TABLE "{schema}".entity_address_result_generation (
        singleton boolean PRIMARY KEY, local_lineage_id uuid, local_generation bigint,
        origin_lineage_id uuid, origin_generation bigint, published_at timestamptz, relation_oids bigint[])''')
    await database.status(
        f'''INSERT INTO "{schema}".entity_address_result_generation
        VALUES (true,:lineage,1,:lineage,1,now(),{address_oids_sql(schema)})''',
        lineage=uuid4(),
    )


async def _create_doctors_families(database, schema):
    """Use the fixed native model shapes for both genuine publication generations."""
    from types import SimpleNamespace

    from tests.cms_doctors_preparation_postgres_support import _insert_marker

    async with database.transaction() as session:
        connection = await session.connection()
        for model in doctors._models():
            for suffix, marker in (("", "old"), ("_next", "new")):
                table = model.__table__.to_metadata(MetaData(), schema=schema, name=model.__tablename__ + suffix)
                for column in table.columns:
                    column.autoincrement = False
                await connection.run_sync(lambda sync, table=table: table.create(sync, checkfirst=False))
                for index, spec in enumerate(getattr(model, "__my_additional_indexes__", ())):
                    native_index = Index(
                        table.name + "_snapshot_" + str(index), *(table.c[name] for name in spec["index_elements"])
                    )
                    await connection.run_sync(lambda sync, native_index=native_index: native_index.create(sync))
                await _insert_marker(
                    session, SimpleNamespace(__table__=table, __main_table__=model.__main_table__), marker
                )
        await session.execute(
            text(
                f'CREATE TABLE "{schema}".provider_directory_cms_serving_receipt '
                "(receipt_id text,payload jsonb,publication_xid xid8)"
            )
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


async def publish_family(database, schema, started=None, *, locked=None, release=None):
    """Run the independent native publisher without inheriting a Reader request session."""
    component = database._snapshot_doctors_component

    async def publish():
        async with bind_generic_source_transaction_factory(component.factory):
            await _publish_family(component, schema, started, locked=locked, release=release)

    await asyncio.create_task(publish(), context=Context())


async def _publish_family(component, schema, started, *, locked, release):
    """Rename every serving family and advance its native receipt in one commit."""
    async with source_transaction(component.database) as session:
        previous = await doctors.capture_doctors_predecessor(session, schema)
        await session.execute(text("SET LOCAL statement_timeout = '5s'"))
        if locked is not None:
            assert release is not None
            assert await session.scalar(text("SELECT current_user")) == component.owner
            assert not await session.scalar(text("SELECT rolsuper FROM pg_roles WHERE rolname=current_user"))
            names = ",".join(f'"{schema}"."{name}"' for name in sorted(TABLES))
            await session.execute(text(f"LOCK TABLE {names} IN ACCESS EXCLUSIVE MODE"))
            locked.set()
            await release.wait()
        if started is not None:
            started.set()
        for table in TABLES:
            await session.execute(text(f'ALTER TABLE "{schema}".{table} RENAME TO {table}_old'))
            await session.execute(text(f'ALTER TABLE "{schema}".{table}_next RENAME TO {table}'))
        await doctors.publish_doctors_generation(session, schema_name=schema, expected_authority=previous)
        await session.execute(
            text(f'''UPDATE "{schema}".entity_address_result_generation
            SET local_generation=2,origin_generation=2,published_at=clock_timestamp(),
                relation_oids={address_oids_sql(schema)}''')
        )


@asynccontextmanager
async def near_snapshot_database(monkeypatch):
    """Add the existing native publisher around the geo fixture's real Unified heaps."""
    from tests.npi_near_snapshot_postgres_support import near_database

    async with near_database(monkeypatch) as (database, schema), AsyncExitStack() as custody:
        await create_families(database, schema, unified_exists=True)
        await grant_profile_reader(database, schema, READ_TABLES)
        await _enroll_snapshot_doctors(database.engine, database, schema, monkeypatch, custody)
        yield database, schema


@asynccontextmanager
async def held_family_publication(database, schema):
    """Hold the genuine publisher's complete family lock until the caller releases it."""
    locked, release = asyncio.Event(), asyncio.Event()
    publication = asyncio.create_task(publish_family(database, schema, locked=locked, release=release))
    try:
        await asyncio.wait_for(locked.wait(), 3)
        yield release
    finally:
        release.set()
        try:
            await asyncio.wait_for(publication, 3)
        finally:
            publication.cancel()
            await asyncio.gather(publication, return_exceptions=True)


async def wait_for_snapshot_lock(database, schema):
    """Observe the actual Reader wait without starting its catalog snapshot early."""

    async def waiting_pid():
        async with database.engine.connect() as observer:
            while True:
                await observer.execute(text("SELECT pg_stat_clear_snapshot()"))
                pid = await observer.scalar(
                    text("""SELECT activity.pid FROM pg_locks locked
                        JOIN pg_stat_activity activity ON activity.pid=locked.pid
                        JOIN pg_class relation ON relation.oid=locked.relation
                        JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
                        WHERE activity.usename=:reader AND namespace.nspname=:schema
                          AND locked.mode='AccessShareLock' AND NOT locked.granted LIMIT 1"""),
                    {"reader": database._reader_database._reader_login[0], "schema": schema},
                )
                if pid is not None:
                    return pid
                await asyncio.sleep(0.01)

    return await asyncio.wait_for(waiting_pid(), 3)


async def release_after_snapshot_wait(database, schema, release):
    """Keep the real lock held longer than all three former 250ms attempts."""
    pid = await wait_for_snapshot_lock(database, schema)
    await asyncio.sleep(1.1)
    release.set()
    return pid
