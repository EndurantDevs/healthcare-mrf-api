# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native zero-family owner membership; complete source service is a separate gate."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_ptg_office_custody as custody
from process.registry_ptg_published_plan_contract import RegistryPTGPublishedPlanSourceSpecification
from process.registry_ptg_published_provisioning import HEADER_TABLES, LOCK_COLUMN
from tests.test_registry_management_permissions_postgres import permissions_db as permissions_db
from tests.test_registry_management_permissions_postgres import provider_db as provider_db
from tests.test_registry_management_permissions_postgres import serving_schema as serving_schema
from tests.test_registry_ptg_office_capture import _context


@asynccontextmanager
async def _office_logins(fixture, tmp_path, schema):
    """Create exact disposable LOGIN identities without changing retained role flags."""
    publisher, reader = "scope_native_approver_" + uuid4().hex, "scope_native_reader_" + uuid4().hex
    password = uuid4().hex
    created_roles_by_name = {}
    (tmp_path / "office-custody-cleanup.json").write_text(
        json.dumps(
            {"source_schema": schema, "fixture_roles": fixture.roles_by_kind, "office_logins": [publisher, reader]}
        )
    )
    try:
        for role in (publisher, reader):
            assert not await fixture.connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role)
            created_roles_by_name[role] = None
            await fixture.connection.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
            created_roles_by_name[role] = dict(
                await fixture.connection.fetchrow("SELECT oid,rolcanlogin FROM pg_roles WHERE rolname=$1", role)
            )
            assert created_roles_by_name[role]["rolcanlogin"] is True
        yield publisher, reader, password
    finally:
        for role, expected in reversed(list(created_roles_by_name.items())):
            observed = await fixture.connection.fetchrow("SELECT oid,rolcanlogin FROM pg_roles WHERE rolname=$1", role)
            if observed is not None:
                assert expected is not None and dict(observed) == expected
                assert (
                    await fixture.connection.fetchval("SELECT count(*) FROM pg_stat_activity WHERE usename=$1", role)
                    == 0
                )
                await fixture.connection.execute(f'DROP OWNED BY "{role}"')
                await fixture.connection.execute(f'DROP ROLE "{role}"')
        assert not await fixture.connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))", list(created_roles_by_name)
        )


@pytest.fixture
async def empty_office_owner(permissions_db, tmp_path):
    """Reuse registered test roles; register the exact source schema before creation."""
    fixture = permissions_db
    owner = fixture.roles_by_kind["ancestor"]
    store_owner = fixture.roles_by_kind["owner"]
    schema = "office_custody_test_" + uuid4().hex
    async with _office_logins(fixture, tmp_path, schema) as (publisher, reader, password):
        engine = None
        try:
            await fixture.connection.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{store_owner}"')
            for table in HEADER_TABLES:
                await fixture.connection.execute(
                    f'CREATE TABLE "{schema}"."{table}" ({LOCK_COLUMN} smallint NOT NULL DEFAULT 0 CHECK({LOCK_COLUMN}=0))'
                )
                await fixture.connection.execute(f'ALTER TABLE "{schema}"."{table}" OWNER TO "{store_owner}"')
                await fixture.connection.execute(f'GRANT SELECT ON "{schema}"."{table}" TO "{publisher}"')
                await fixture.connection.execute(
                    f'GRANT UPDATE({LOCK_COLUMN}) ON "{schema}"."{table}" TO "{publisher}"'
                )
            await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{publisher}"')
            url = fixture.sessions.kw["bind"].url.set(username=publisher, password=password)
            engine = create_async_engine(url, pool_size=1, max_overflow=0, hide_parameters=True)
            profile = custody.RegistryPTGOfficeCustodyProfile(owner, publisher)
            context = replace(
                _context(),
                owner_role=owner,
                reader_roles=(reader,),
                office_custody=profile,
                scope_store=SimpleNamespace(owner_role=store_owner),
                source_specification=RegistryPTGPublishedPlanSourceSpecification(
                    "22222222-2222-4222-8222-222222222222", schema, "snapshot", "source"
                ),
            )
            yield SimpleNamespace(
                connection=fixture.connection,
                sessions=async_sessionmaker(engine),
                engine=engine,
                context=context,
                owner=owner,
                publisher=publisher,
            )
        finally:
            if engine is not None:
                await engine.dispose()
            await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await fixture.connection.fetchval("SELECT pg_catalog.to_regnamespace($1)", schema) is None


@pytest.mark.asyncio
async def test_native_publisher_writes_only_granted_header_lock_columns(empty_office_owner):
    fixture = empty_office_owner
    roles = custody.office_preparation_roles(fixture.context)
    assert roles["namespace_owner_role"] == fixture.owner and roles["publisher_role"] == fixture.publisher
    table = f'"{fixture.context.source_specification.ptg_schema_name}"."{HEADER_TABLES[0]}"'
    async with fixture.sessions() as session, session.begin():
        await session.execute(text(f"UPDATE {table} SET {LOCK_COLUMN}=0"))
    with pytest.raises(DBAPIError) as refused:
        async with fixture.sessions() as session, session.begin():
            await session.execute(text(f"INSERT INTO {table} DEFAULT VALUES"))
    assert refused.value.orig.sqlstate == "42501"
