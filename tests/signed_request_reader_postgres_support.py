# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual Reader factory and SELECT-only actor for signed HTTP qualification."""

from types import SimpleNamespace

import pytest_asyncio

from api import billing_search_http, plan_release_read_boundary, signed_request_reader
from api import custom_import_read_http as transport
from db.connection import Database
from tests import test_custom_import_read_http as signatures
from tests.plan_release_storage_postgres_support import storage_case


@pytest_asyncio.fixture
async def signed_reader_case(monkeypatch):
    async with storage_case(provisioned=True) as case:
        admin = await case.connect()
        await admin.execute(f"CREATE TABLE {case.relation('reader_payload')}(value text)")
        await admin.execute(f"INSERT INTO {case.relation('reader_payload')} VALUES('old')")
        await admin.execute(f"CREATE TABLE {case.relation('native_npi_fixture')}(snapshot_key bigint,npi bigint)")
        await admin.execute(
            f"INSERT INTO {case.relation('native_npi_fixture')} VALUES(1,1000000001),(1,1000000002),(1,1000000003)"
        )
        await admin.execute(
            f'GRANT SELECT ON {case.relation("reader_payload")},{case.relation("native_npi_fixture")} TO "{case.reader}"'
        )
        monkeypatch.setenv("HLTHPRT_DB_READER_USER", case.reader)
        monkeypatch.setenv("HLTHPRT_DB_READER_PASSWORD", "")
        monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
        database = Database()
        await database.connect()
        monkeypatch.setattr(signed_request_reader, "db", database)
        monkeypatch.setattr(plan_release_read_boundary, "db", database)
        monkeypatch.setattr(billing_search_http, "db", database)
        transport._keyring_for_document.cache_clear()
        transport._cursor_secret_for_document.cache_clear()
        monkeypatch.setenv(transport.CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_ENV, signatures._keyring_document())
        monkeypatch.setenv(transport.CUSTOM_IMPORT_READ_CURSOR_SECRET_ENV, signatures._encoded(signatures._CURSOR_KEY))
        monkeypatch.setattr(transport, "_trusted_now", lambda: signatures._NOW)
        try:
            yield SimpleNamespace(case=case, admin=admin, database=database)
        finally:
            await database.disconnect()


async def native_reader_witness(session, reader):
    from sqlalchemy import text

    from db.connection import current_session

    assert current_session() is session and session.info["api_reader_verified"] is True
    row = (
        await session.execute(
            text(
                "SELECT session_user,current_user,current_setting('transaction_isolation'),current_setting('transaction_read_only'),pg_backend_pid()"
            )
        )
    ).first()
    assert tuple(row[:4]) == (reader, reader, "repeatable read", "on")
    return row[4]


async def observe_reader_phases(database):
    from sqlalchemy import event

    reader = await database._connect_reader()
    phases = []

    def observe(connection, _cursor, statement, _parameters, _context, _many):
        driver = connection.connection.driver_connection
        phases.append(
            (statement, connection.get_execution_options().get("postgresql_readonly"), driver.get_server_pid())
        )

    event.listen(reader.engine.sync_engine, "before_cursor_execute", observe)
    return phases


def assert_fixed_preparation(phases, backend):
    preparation_statements = [(statement, readonly, pid) for statement, readonly, pid in phases if readonly is False]
    assert len(preparation_statements) == 9 and {pid for _statement, _readonly, pid in phases} == {backend}
    assert all(
        statement.startswith(("CREATE TEMP TABLE", "CREATE INDEX"))
        for statement, _readonly, _pid in preparation_statements
    )
    assert all(
        readonly is True
        for statement, readonly, _pid in phases
        if (statement, readonly, _pid) not in preparation_statements
    )
    last_ddl = max(index for index, phase in enumerate(phases) if phase in preparation_statements)
    assert [statement for statement, _readonly, _pid in phases[: last_ddl + 1] if statement.startswith("SELECT")] == [
        "SELECT session_user,current_user"
    ]


async def legacy_plan_result(case):
    """Compare the unchanged computation against its original TEMP-before-RO flow."""
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession

    from api.custom_import_plan_pricing import search_imported_plan_providers
    from api.custom_import_plan_sql import prepare_plan_query_tables
    from tests import test_custom_import_plan_pricing as plan_fixture

    async with AsyncSession(case.engine) as session:
        async with session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await prepare_plan_query_tables(session)
            await session.execute(text("SET TRANSACTION READ ONLY"))
            return await search_imported_plan_providers(
                session,
                {},
                SimpleNamespace(limit=2, offset=0, page=1),
                plan_fixture._selection(),
                plan_fixture._import_context(),
            )


async def assert_reader_write_denials(reader, case):
    import pytest
    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError

    for statement in (
        f"INSERT INTO {case.relation('reader_payload')} VALUES('forbidden')",
        "CREATE TEMP TABLE extra_reader_heap(id int)",
    ):
        with pytest.raises(DBAPIError):
            async with reader.begin_nested():
                await reader.execute(text(statement))
