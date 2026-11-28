# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native CMS catalog layout and disposable allocation fixtures shared by PostgreSQL suites."""

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import Index, MetaData, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db.connection import Database
from db.migration_provider_directory_dataset_candidates import TABLES
from db.models import CMSDoctorEducation, DoctorClinicianAddress
from db.plan_release_storage import storage_relation
from tests.cms_npd_admission_postgres_support import _owned_database, _run_migrations
from tests.nppes_candidate_postgres_support import (
    install_source_canonical_functions,
    nppes_source_component_migrations,
)
from tests.provider_directory_source_postgres_support import source_component_migrations
from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope


def _migrations():
    """Extend the existing genuine dependency slice in actual ancestry order."""
    root = Path(__file__).parents[1]
    selected = set(nppes_source_component_migrations()) | {
        "20261007150000",
        "20261017210000",
        "20261022120000",
        "20261023100000_provider_directory_retirement_parent_updates",
        "20261024050000",
    }
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    nodes = tuple(reversed(tuple(ScriptDirectory.from_config(config).walk_revisions())))
    nodes_by_prefix = {prefix: [n for n in nodes if Path(n.path).name.startswith(prefix)] for prefix in selected}
    nodes_by_prefix = {
        prefix: ([n for n in found if "provider_directory" in Path(n.path).name] if len(found) > 1 else found)
        for prefix, found in nodes_by_prefix.items()
    }
    assert all(len(found) == 1 for found in nodes_by_prefix.values())
    by_revision = {found[0].revision: prefix for prefix, found in nodes_by_prefix.items()}
    prefixes = tuple(by_revision[n.revision] for n in nodes if n.revision in by_revision)
    baseline = source_component_migrations()
    assert len(prefixes) == len(selected) == 115
    assert tuple(prefix for prefix in prefixes if prefix in baseline) == baseline
    return prefixes


async def _create_native_models(engine):
    async with engine.begin() as native:
        await native.exec_driver_sql("CREATE SCHEMA mrf")
        for model in (DoctorClinicianAddress, CMSDoctorEducation):
            table = model.__table__.to_metadata(MetaData(), schema="mrf")
            await native.run_sync(lambda sync: table.create(sync, checkfirst=False))
            for spec in getattr(model, "__my_additional_indexes__", ()):
                index = Index(table.name + "_idx_" + spec["name"], *(table.c[n] for n in spec["index_elements"]))
                await native.run_sync(lambda sync: index.create(sync, checkfirst=False))


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def native_layout():
    raw = os.getenv("HLTHPRT_CMS_LAYOUT_TEST_DSN")
    if not raw:
        pytest.skip("set HLTHPRT_CMS_LAYOUT_TEST_DSN for the native fixed-layout proof")
    url = make_url(raw)
    assert url.host in {"127.0.0.1", "localhost"} and url.port and not url.query
    # Existing lifecycle registers exact cleanup before creating its UUID database.
    async with _owned_database(url) as (owned_url, _admin):
        with pytest.MonkeyPatch.context() as settings:
            settings.setenv("HLTHPRT_DB_SCHEMA", "mrf")
            settings.setenv("DB_SCHEMA", "mrf")
            engine = create_async_engine(owned_url.set(drivername="postgresql+asyncpg"))
            connection = None
            try:
                await _create_native_models(engine)
                prefixes = _migrations()
                boundary = prefixes.index("20261016040000")
                async with engine.connect() as native:
                    await native.run_sync(_run_migrations, prefixes[:boundary])
                await install_source_canonical_functions(engine, "mrf")
                async with engine.connect() as native:
                    await native.run_sync(_run_migrations, prefixes[boundary:])
                connection = await asyncpg.connect(
                    owned_url.set(drivername="postgresql").render_as_string(hide_password=False)
                )
                await connection.execute("SET search_path=pg_catalog")
                yield connection
            finally:
                if connection is not None:
                    await connection.close()
                await engine.dispose()


@asynccontextmanager
async def _reader(admin, *, readonly=True, isolation="REPEATABLE READ", assumed=False):
    url = make_url(os.environ["HLTHPRT_CMS_LAYOUT_TEST_DSN"]).set(
        database=await admin.fetchval("SELECT current_database()")
    )
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = Database()
    database._database_override = url.database
    try:
        async with AsyncSession(bind=engine) as session, session.begin(), database.bind_existing_session(session):
            await session.execute(
                text("SET TRANSACTION ISOLATION LEVEL " + isolation + (" READ ONLY" if readonly else " READ WRITE"))
            )
            await session.execute(text("SET LOCAL search_path=pg_catalog"))
            await session.execute(text("SET LOCAL statement_timeout='10s'"))
            await session.execute(text("SET LOCAL lock_timeout='100ms'"))
            if assumed:
                await session.execute(text("SET LOCAL ROLE pg_read_all_data"))
            native = await session.connection()
            yield database, session, (await native.get_raw_connection()).driver_connection
    finally:
        await engine.dispose()


@asynccontextmanager
async def _native_allocations(monkeypatch):
    """Register exact disposable database/role cleanup before creation."""
    dsn = os.getenv("HLTHPRT_CMS_CUSTODY_TEST_DSN")
    if not dsn:
        pytest.skip("set HLTHPRT_CMS_CUSTODY_TEST_DSN for local native allocation proof")
    url = make_url(dsn)
    assert url.host in {"localhost", "127.0.0.1"} and url.port is not None and not url.query
    token = uuid4().hex
    name, owner = "cms_semantics_test_" + token, "cms_semantics_owner_" + token[:16]
    publisher = "cms_semantics_publisher_" + token[:16]
    admin = await asyncpg.connect(dsn)
    has_attempted_database, roles = False, []
    try:
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", name)
        for role in (owner, publisher):
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
        has_attempted_database = True
        await admin.execute('CREATE DATABASE "' + name + '" TEMPLATE template0')
        roles.append(owner)
        await admin.execute('CREATE ROLE "' + owner + '" NOLOGIN')
        roles.append(publisher)
        await admin.execute('CREATE ROLE "' + publisher + '" LOGIN')
        monkeypatch.setenv(
            "HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN",
            url.set(drivername="postgresql+asyncpg", database=name).render_as_string(hide_password=False),
        )
        async with _lifecycle_scope(monkeypatch, request_failure_budget=False) as catalog:
            await _protect_allocator(catalog, owner)
            async with _publisher_reader(catalog, url, name, publisher) as connection:
                yield SimpleNamespace(catalog=catalog, owner=owner, publisher=connection)
    finally:
        try:
            if has_attempted_database:
                await _wait_for_database_backends(admin, name)
                assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=$1)", name)
                await admin.execute('DROP DATABASE IF EXISTS "' + name + '"')
                assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", name)
            for role in reversed(roles):
                await admin.execute('DROP ROLE IF EXISTS "' + role + '"')
                assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
        finally:
            await admin.close()


async def _wait_for_database_backends(admin, name):
    async with asyncio.timeout(3):
        while await admin.fetchval("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=$1)", name):
            await asyncio.sleep(0.01)


@asynccontextmanager
async def _publisher_reader(catalog, url, database, publisher):
    """Use an original login with parent reads and no child or owner authority."""
    await catalog.connection.execute(f'GRANT USAGE ON SCHEMA {catalog.schema} TO "{publisher}"')
    for root in (*TABLES[:4], "provider_directory_endpoint_dataset", "pd_dataset_migration_plan"):
        await catalog.connection.execute(
            f'GRANT SELECT ON {storage_relation(catalog.schema_name, root)} TO "{publisher}"'
        )
    connection = await asyncpg.connect(
        url.set(database=database, username=publisher).render_as_string(hide_password=False)
    )
    try:
        yield connection
    finally:
        await connection.close()


async def _protect_allocator(catalog, owner):
    """Give the real fixed allocator a disposable protected owner, without changing its body."""
    await catalog.connection.execute(f'GRANT USAGE,CREATE ON SCHEMA {catalog.schema} TO "{owner}"')
    for root in TABLES[:4]:
        await catalog.connection.execute(
            f'ALTER TABLE {storage_relation(catalog.schema_name, root)} OWNER TO "{owner}"'
        )
    await catalog.connection.execute(
        f'ALTER FUNCTION {catalog.schema}.prepare_pd_generic_dataset_storage() OWNER TO "{owner}"'
    )
    await catalog.connection.execute(
        f'GRANT EXECUTE ON FUNCTION {catalog.schema}.guard_pd_dataset_candidate_parent() TO "{owner}"'
    )
