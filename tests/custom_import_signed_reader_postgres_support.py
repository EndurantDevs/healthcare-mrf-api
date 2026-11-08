# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Enroll a real signed HTTP Reader before protected snapshots are created."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker

from api import signed_request_reader
from db.connection import Database
from tests.custom_import_postgres_support import _database_url
from tests.signed_request_reader_postgres_support import native_reader_witness


async def grant_signed_reader_tables(case, *names):
    """Grant SELECT on exact newly seeded ordinary native fixture tables only."""
    async with case.engine.begin() as connection:
        for name in names:
            assert name.replace("_", "").isalnum()
            relation = f'"{case.schema_name}"."{name}"'
            await connection.execute(text(f'GRANT SELECT ON {relation} TO "{case.reader}"'))
            assert (
                await connection.scalar(
                    text("SELECT has_table_privilege(:role,:relation,'SELECT')"),
                    {"role": case.reader, "relation": relation},
                )
                is True
            )
            assert (
                await connection.scalar(
                    text(
                        "SELECT has_table_privilege(:role,:relation,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')"
                    ),
                    {"role": case.reader, "relation": relation},
                )
                is False
            )


def _configure_reader_environment(monkeypatch, role):
    """Use the guarded task database and its disposable authentication only."""
    url = _database_url()
    for key, value in {
        "USER": url.username,
        "PASSWORD": url.password or "",
        "HOST": url.host,
        "PORT": str(url.port or 5432),
        "DATABASE": url.database,
        "DATABASE_OVERRIDE": url.database,
        "READER_USER": role,
        "READER_PASSWORD": url.password or "",
    }.items():
        monkeypatch.setenv("HLTHPRT_DB_" + key, value)


async def _require_reader_permissions(database, role, schema):
    """Prove the native login and actual write refusal without synthetic claims."""
    async with database.reader_session() as session:
        await native_reader_witness(session, role)
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(text(f'DELETE FROM "{schema}".custom_import_dataset WHERE false'))


async def _enroll_reader_access(connection, case, role):
    """Enroll canonical read grants before snapshot creation, with no write reach."""
    from tests.test_custom_import_snapshot_reads_postgres import _grant_reader_access

    await _grant_reader_access(connection, case, role)
    assert (
        await connection.scalar(
            text(
                "SELECT NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls FROM pg_roles WHERE rolname=:role"
            ),
            {"role": role},
        )
        is True
    )
    assert (
        await connection.scalar(
            text("SELECT count(*)=0 FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=:role)"),
            {"role": role},
        )
        is True
    )
    assert (
        await connection.scalar(
            text(
                "SELECT bool_and(NOT has_table_privilege(:role,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema AND c.relkind IN ('r','p')"
            ),
            {"role": role, "schema": case.schema_name},
        )
        is True
    )


async def _drop_reader_role(case, role):
    """Remove the exact task login even after enrollment transaction rollback."""
    async with case.engine.begin() as connection:
        if await connection.scalar(text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:role)"), {"role": role}):
            await connection.execute(text(f'DROP OWNED BY "{role}"'))
            await connection.execute(text(f'DROP ROLE "{role}"'))


@asynccontextmanager
async def signed_publication_reader(case, monkeypatch=None):
    """Leave direct SQL fixtures unchanged; signed cases use the real Reader API."""
    if monkeypatch is None:
        yield case
        return
    role = "custom_signed_reader_" + uuid4().hex[:16]
    database = Database()
    is_created = False
    try:
        async with case.engine.begin() as connection:
            password = await connection.scalar(
                text("SELECT quote_literal(CAST(:password AS text))"), {"password": _database_url().password or ""}
            )
            await connection.execute(
                text(
                    f'CREATE ROLE "{role}" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD {password}'
                )
            )
            is_created = True
            await _enroll_reader_access(connection, case, role)
        with monkeypatch.context() as scoped:
            _configure_reader_environment(scoped, role)
            reader = await database._connect_reader()
            reader.engine = reader.engine.execution_options(schema_translate_map={"mrf": case.schema_name})
            reader.session_factory = async_sessionmaker(reader.engine, expire_on_commit=False, autoflush=False)
            scoped.setattr(signed_request_reader, "db", database)
            await _require_reader_permissions(database, role, case.schema_name)
            sessions = []
            original_reader_session = database.reader_session

            @asynccontextmanager
            async def witnessed_reader_session(**arguments):
                """Observe, without replacing, the authentic Reader session lifecycle."""
                async with original_reader_session(**arguments) as session:
                    await native_reader_witness(session, role)
                    sessions.append(session)
                    yield session

            scoped.setattr(database, "reader_session", witnessed_reader_session)
            yield SimpleNamespace(**vars(case), reader=role, reader_sessions=sessions)
    finally:
        try:
            await database.disconnect()
        finally:
            if is_created:
                await _drop_reader_role(case, role)
