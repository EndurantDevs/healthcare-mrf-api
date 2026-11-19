# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable Postgres roles for the isolated catalog publisher proof."""

from contextlib import asynccontextmanager
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import scoped_catalog_named_storage as storage
from process.scoped_catalog_layout import DATA_SCHEMA, FENCE_OWNER_ROLE, OWNER_ROLE, STAGE_ROLE
from process.scoped_catalog_publication import CatalogPublicationPolicy
from process.scoped_catalog_publisher import PublisherSettings


@asynccontextmanager
async def _split_roles(engine, builder):
    publisher = "scoped_test_publisher_" + uuid4().hex
    password = uuid4().hex
    roles = (OWNER_ROLE, STAGE_ROLE, FENCE_OWNER_ROLE, publisher)
    assert_existing = text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY(CAST(:roles AS text[])))")
    async with engine.begin() as connection:
        assert not await connection.scalar(assert_existing, {"roles": list(roles)})
        for role in roles:
            login = "LOGIN" if role == publisher else "NOLOGIN"
            await connection.execute(text(f'CREATE ROLE "{role}" {login}'))
        driver = (await connection.get_raw_connection()).driver_connection
        await driver.execute(f"ALTER ROLE \"{publisher}\" PASSWORD '{password}'")
        await connection.execute(text(f'GRANT "{OWNER_ROLE}","{STAGE_ROLE}" TO "{publisher}"'))
        await connection.execute(text(f'GRANT "{STAGE_ROLE}" TO "{builder}" WITH INHERIT FALSE'))
        await connection.execute(
            text(f'GRANT CREATE ON DATABASE "{engine.url.database}" TO "{STAGE_ROLE}","{OWNER_ROLE}"')
        )
        await connection.execute(text(f'GRANT USAGE ON SCHEMA mrf TO "{builder}","{publisher}","{FENCE_OWNER_ROLE}"'))
        await connection.execute(text(f'ALTER TABLE mrf.import_run OWNER TO "{builder}"'))
        for model in storage.MODELS:
            relation = storage.qualified("mrf", model.__tablename__)
            await connection.execute(text(f'ALTER TABLE {relation} OWNER TO "{builder}"'))
            await connection.execute(text(f'GRANT SELECT ON {relation} TO "{publisher}"'))
            await connection.execute(text(f'GRANT SELECT, MAINTAIN ON {relation} TO "{FENCE_OWNER_ROLE}"'))
        await connection.execute(text(f'GRANT SELECT ON mrf.import_run TO "{publisher}"'))
        await connection.execute(
            text(
                f'GRANT UPDATE (status,phase_detail,finished_at,heartbeat_at,error,metrics) ON mrf.import_run TO "{publisher}"'
            )
        )
    publisher_engine = create_async_engine(engine.url.set(username=publisher, password=password), hide_parameters=True)
    try:
        policy = CatalogPublicationPolicy(DATA_SCHEMA, OWNER_ROLE, (builder, STAGE_ROLE))
        settings = PublisherSettings(
            publisher_engine.url.render_as_string(hide_password=False),
            DATA_SCHEMA,
            OWNER_ROLE,
            policy.reader_roles,
            1024**2,
            60,
        )
        yield settings, policy, async_sessionmaker(publisher_engine, expire_on_commit=False)
    finally:
        await publisher_engine.dispose()
        async with engine.begin() as connection:
            for role in reversed(roles):
                await connection.execute(text(f'DROP OWNED BY "{role}"'))
            for role in reversed(roles):
                await connection.execute(text(f'DROP ROLE "{role}"'))
            assert not await connection.scalar(assert_existing, {"roles": list(roles)})
