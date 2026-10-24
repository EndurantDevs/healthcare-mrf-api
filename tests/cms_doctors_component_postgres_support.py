# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine native roles for component proofs, not enrolled runtime acceptance."""

import os
import re
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import cms_doctors_immutable as immutable
from process.provider_directory_cms_doctors_provision import provision_doctors_source_storage
from process.provider_directory_projection_stage import _copy_driver
from tests.provider_directory_source_postgres_support import (
    native_source_component,
    source_component_database,
    source_component_templates,
    verify_native_source_profile,
)


@asynccontextmanager
async def doctors_component_scope():
    """Keep the owned anchor alive until every closed migration template is removed."""
    from tests import cms_npd_admission_postgres_support as admission

    if os.getenv(admission._DSN_ENV):
        source_url = admission._database_url()
    else:
        name = os.getenv("HLTHPRT_DB_DATABASE", "")
        if not re.fullmatch(r"ptg2_v3_lifecycle_test_[a-z0-9_]{8,}", name):
            pytest.skip("requires an explicitly selected disposable PostgreSQL database")
        source_url = URL.create(
            "postgresql+asyncpg",
            username=os.getenv("HLTHPRT_DB_USER", "postgres"),
            password=os.getenv("HLTHPRT_DB_PASSWORD") or None,
            host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
            port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
            database=name,
        )
        assert source_url.host in {"127.0.0.1", "localhost", "postgres"}
    async with admission._owned_database(source_url) as (database_url, _admin), source_component_templates():
        with pytest.MonkeyPatch.context() as settings:
            settings.setenv(admission._DSN_ENV, database_url.render_as_string(hide_password=False))
            assert admission._database_url() == database_url
            yield


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def doctors_component_template():
    """Reuse migrations while each case retains its own database and native roles."""
    async with doctors_component_scope():
        yield


async def _native_preimage(driver, schema):
    return await driver.fetchval(
        "SELECT jsonb_agg(jsonb_build_array(c.oid,c.relowner,c.relacl,"
        "(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_trigger t WHERE t.tgrelid=c.oid),"
        "(SELECT jsonb_agg(pg_get_indexdef(i.indexrelid) ORDER BY i.indexrelid) "
        "FROM pg_index i WHERE i.indrelid=c.oid)) ORDER BY c.relname) "
        "FROM pg_class c WHERE c.relnamespace=$1::regnamespace AND c.relname=ANY($2::text[])",
        schema,
        list(model.__tablename__ for model in immutable._models()),
    )


async def _provision_component(engine, policy):
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        driver = await _copy_driver(session)
        expected = await _native_preimage(driver, policy.schema)

        async def legacy(connection, current, family):
            assert len(family) == 3 and current == policy
            assert await _native_preimage(connection, policy.schema) == expected

        async def writers(connection, current):
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_class c,pg_roles r WHERE c.relnamespace=$1::regnamespace "
                "AND c.relname=ANY($2::text[]) AND r.rolname=ANY($3::text[]) AND "
                "(has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR "
                "has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')))",
                current.schema,
                list(model.__tablename__ for model in immutable._models()),
                list(current.runtime_roles),
            )

        await provision_doctors_source_storage(
            session,
            policy,
            verify_role_profile=verify_native_source_profile,
            verify_writer_closure=writers,
            verify_legacy_guards=legacy,
        )


@asynccontextmanager
async def doctors_component_source(engine, schema, monkeypatch, *, reader=None):
    """Native fixed-family component custody, independent of enrolled runtime proof."""
    async with native_source_component(engine, schema, monkeypatch, reader=reader) as component:
        await _provision_component(engine, component.policy)
        async with component.database.transaction() as session:
            current = await immutable.capture_doctors_predecessor(session, schema)
            if current.serving_generation is None:
                current = await immutable.publish_doctors_generation(
                    session, schema_name=schema, expected_authority=current
                )
        component.authority = current
        yield component


async def prepare_doctors_component(component):
    """Seed and provision the real incumbent before any common CMS receipt exists."""
    from sqlalchemy import Index, MetaData

    from tests.cms_doctors_preparation_postgres_support import _insert_marker

    schema = component.policy.schema
    async with component.admin_engine.begin() as connection:
        for model in immutable._models():
            await _insert_marker(connection, model, "incumbent")
            table = model.__table__.to_metadata(MetaData(), schema=schema)
            for spec in getattr(model, "__my_additional_indexes__", ()):
                index = Index(
                    str(model.__table__.name) + "_idx_" + spec["name"],
                    *(table.c[name] for name in spec["index_elements"]),
                )
                await connection.run_sync(lambda sync: index.create(sync, checkfirst=True))
    await _provision_component(component.admin_engine, component.policy)
    async with component.database.transaction() as session:
        previous = await immutable.capture_doctors_predecessor(session, schema)
        assert previous.local_generation == 0 and previous.serving_generation is None
        component.authority = await immutable.publish_doctors_generation(
            session,
            schema_name=schema,
            expected_authority=previous,
        )
    component.database._doctors_source_component = component


@asynccontextmanager
async def doctors_source_component_database(monkeypatch, *, before_provision=None):
    """Real SOURCE family and maps, with a native-role-only fixed Doctors cut."""
    async with source_component_database(monkeypatch, before_provision=before_provision) as component:
        await prepare_doctors_component(component)
        yield component


@asynccontextmanager
async def ordinary_doctors_reader(database):
    """Prove ACL denial under a genuine read-write ordinary login, not READ ONLY."""
    login, password = database._reader_database._reader_login
    engine = create_async_engine(database.engine.url.set(username=login, password=password))
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            assert await session.scalar(text("SELECT current_user=session_user")) is True
            assert await session.scalar(text("SHOW transaction_read_only")) == "off"
            yield session
    finally:
        await engine.dispose()
