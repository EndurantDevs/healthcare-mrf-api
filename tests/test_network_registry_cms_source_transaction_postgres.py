# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Use the actual protected login without replacing the ordinary source pool."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process.network_registry_cms_prepared_pair import _retention_transaction
from tests.cms_npd_admission_postgres_support import _database_url, _owned_database
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template


@asynccontextmanager
async def _source_pools(monkeypatch):
    """Register exact database, role and pool cleanup before creating resources."""
    source_url = _database_url()
    role_names = tuple("registry_source_" + uuid4().hex for _ in range(2))
    password = uuid4().hex
    admin = await asyncpg.connect(source_url.set(drivername="postgresql").render_as_string(hide_password=False))
    engines = []
    try:
        async with _owned_database(source_url) as (test_url, _admin):
            for role in role_names:
                await admin.execute(
                    f'CREATE ROLE "{role}" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION '
                    f"PASSWORD '{password}'"
                )
            setup = await asyncpg.connect(test_url.set(drivername="postgresql").render_as_string(hide_password=False))
            try:
                await setup.execute(f'CREATE SCHEMA source_private AUTHORIZATION "{role_names[1]}"')
                await setup.execute("CREATE TABLE source_private.membership (network_id integer NOT NULL)")
                await setup.execute(f'ALTER TABLE source_private.membership OWNER TO "{role_names[1]}"')
            finally:
                await setup.close()
            for role in role_names:
                engine = create_async_engine(
                    test_url.set(drivername="postgresql+asyncpg", username=role, password=password),
                    pool_size=1,
                    max_overflow=0,
                    hide_parameters=True,
                )
                engines.append(engine)
            ordinary, publisher = map(async_sessionmaker, engines)
            database = Database(engine=engines[0], session_factory=ordinary)
            monkeypatch.setenv("HLTHPRT_DB_DATABASE_OVERRIDE", test_url.database)
            yield database, publisher, test_url, role_names
    finally:
        for engine in reversed(engines):
            await engine.dispose()
        for role in reversed(role_names):
            await admin.execute(f'DROP ROLE IF EXISTS "{role}"')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
        await admin.close()
        assert admin.is_closed()


async def _write_membership(fhir, publisher, observer, publisher_role, outcome):
    """Observe one native transaction before committing or forcing rollback."""
    database = fhir.db
    async with _retention_transaction(fhir, publisher) as session:
        assert not session.in_nested_transaction()
        assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
        assert await database.scalar("SELECT current_user") == publisher_role
        assert await database.scalar("SELECT pg_backend_pid()") == await session.scalar(text("SELECT pg_backend_pid()"))
        await database.status("INSERT INTO source_private.membership VALUES (17)")
        assert await session.scalar(text("SELECT count(*) FROM source_private.membership")) == 1
        assert await observer.fetchval("SELECT count(*) FROM source_private.membership") == 0
        if outcome == "failure":
            raise RuntimeError("source publication failed")
        if outcome == "cancellation":
            raise asyncio.CancelledError()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["commit", "failure", "cancellation"])
async def test_protected_source_transaction_keeps_exact_backend_and_atomic_visibility(monkeypatch, outcome):
    async with _source_pools(monkeypatch) as (database, publisher, test_url, roles):
        ordinary_factory = database.session_factory
        ordinary_role = await database.scalar("SELECT current_user")
        observer = await asyncpg.connect(test_url.set(drivername="postgresql").render_as_string(hide_password=False))
        try:
            fhir = SimpleNamespace(db=database)
            if outcome == "commit":
                await _write_membership(fhir, publisher, observer, roles[1], outcome)
            else:
                operation_error = RuntimeError if outcome == "failure" else asyncio.CancelledError
                with pytest.raises(operation_error):
                    await _write_membership(fhir, publisher, observer, roles[1], outcome)
            assert await observer.fetchval("SELECT count(*) FROM source_private.membership") == (outcome == "commit")
            assert database._transaction_binding() is None
            assert database.session_factory is ordinary_factory
            assert await database.scalar("SELECT current_user") == ordinary_role == roles[0]
        finally:
            await observer.close()


@pytest.mark.asyncio
async def test_protected_source_transaction_rejects_nested_owner_and_child_task(monkeypatch):
    async with _source_pools(monkeypatch) as (database, publisher, _test_url, _roles):
        fhir = SimpleNamespace(db=database)
        async with _retention_transaction(fhir, publisher):
            with pytest.raises(ValueError, match="requires_own_transaction"):
                async with _retention_transaction(fhir, publisher):
                    pytest.fail("nested owner was admitted")
            with pytest.raises(RuntimeError, match="child asyncio task"):
                await asyncio.create_task(database.scalar("SELECT current_user"))
        assert database._transaction_binding() is None
