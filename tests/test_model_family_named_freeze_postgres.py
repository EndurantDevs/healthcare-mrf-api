# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native grants close selected immutable heaps without freezing shared controls."""

from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot
from tests.test_reference_family_archive_postgres import _database_url


async def _assert_selected_mutations_refused(connection, schema):
    """Each rejected write rolls back its own native savepoint."""
    for mutation in (
        f"INSERT INTO \"{schema}\".finished(value) VALUES ('changed')",
        f"UPDATE \"{schema}\".finished SET value='changed'",
        f'DELETE FROM "{schema}".finished',
        f'TRUNCATE "{schema}".finished',
        f"SELECT nextval('\"{schema}\".finished_id_seq')",
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(mutation)


async def _assert_named_role_permissions(connection, schema, builder_role, owner_role, application_role):
    """Selected heaps stay immutable while unselected storage and schema access remain usable."""
    for role in (builder_role, owner_role, application_role):
        await connection.execute(f'SET LOCAL ROLE "{role}"')
        try:
            await _assert_selected_mutations_refused(connection, schema)
            if role in (owner_role, application_role):
                assert await connection.fetchval(f'SELECT value FROM "{schema}".finished') == "original"
            await connection.execute(f"UPDATE \"{schema}\".mutable SET value='still mutable'")
            await connection.execute(f"INSERT INTO \"{schema}\".mutable(value) VALUES ('new row')")
            async with connection.transaction():
                await connection.execute(f'CREATE TABLE "{schema}"."probe_{role}" (value text)')
        finally:
            await connection.execute("RESET ROLE")


@pytest.mark.asyncio
async def test_selected_freeze_closes_dml_and_preserves_unselected_schema_permissions():
    """Selected freeze preserves data, refuses every DML path and leaves other storage writable."""
    url = make_url(_database_url()).set(drivername="postgresql")
    connection = await asyncpg.connect(url.render_as_string(hide_password=False), command_timeout=10)
    suffix = uuid4().hex
    schema = "freeze_" + suffix
    builder_role = "builder_" + suffix
    owner_role = "owner_" + suffix
    application_role = "reader_" + suffix
    transaction = connection.transaction()
    await transaction.start()
    try:
        for role in (builder_role, owner_role, application_role):
            await connection.execute(f'CREATE ROLE "{role}" NOLOGIN')
        await connection.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{builder_role}"')
        await connection.execute(f'GRANT USAGE,CREATE ON SCHEMA "{schema}" TO "{owner_role}","{application_role}"')
        await connection.execute(f'SET LOCAL ROLE "{builder_role}"')
        for table in ("finished", "mutable"):
            await connection.execute(f'CREATE TABLE "{schema}"."{table}" (id serial PRIMARY KEY,value text)')
            await connection.execute(f'INSERT INTO "{schema}"."{table}" (value) VALUES (\'original\')')
            await connection.execute(f'GRANT ALL ON TABLE "{schema}"."{table}" TO "{owner_role}","{application_role}"')
            await connection.execute(
                f'GRANT UPDATE(value) ON TABLE "{schema}"."{table}" TO "{owner_role}","{application_role}"'
            )
            await connection.execute(
                f'GRANT ALL ON SEQUENCE "{schema}"."{table}_id_seq" TO "{owner_role}","{application_role}"'
            )
        await connection.execute("RESET ROLE")
        schema_grants = await connection.fetchval("SELECT nspacl::text FROM pg_namespace WHERE nspname=$1", schema)
        await freeze_fhir_source_snapshot(
            connection,
            schema,
            namespace_owner=owner_role,
            reader_roles=(owner_role, application_role),
            application_roles=(application_role,),
            relation_names=("finished",),
            freeze_schema=False,
        )
        assert (
            await connection.fetchval("SELECT nspacl::text FROM pg_namespace WHERE nspname=$1", schema) == schema_grants
        )

        await _assert_named_role_permissions(connection, schema, builder_role, owner_role, application_role)
        assert await connection.fetchval(f'SELECT value FROM "{schema}".finished') == "original"
    finally:
        await transaction.rollback()
        await connection.close()


async def _prepare_named_counter_fixture(connection, schema):
    """Install the actual named transition and register the initial native input."""
    from types import SimpleNamespace

    from process import provider_directory_cms_native_registration as registration
    from tests.test_cms_archive_transaction_migration import _load
    from tests.test_cms_native_input_named_revision_migration import _migration

    await connection.execute(f'CREATE SCHEMA "{schema}"')
    statements = []
    historical = _load("20260930120000")
    historical._schema = lambda: f'"{schema}"'
    historical.op = SimpleNamespace(execute=statements.append)
    historical.upgrade()
    receipt = _load("20260930100000")
    receipt.op = SimpleNamespace(execute=statements.append)
    receipt._create_archive_check(f'"{schema}"')
    for statement in statements:
        await connection.execute(statement)
    mutation = _load("20261019130000")
    await connection.execute(mutation._counter_sql(schema))
    await connection.execute(mutation._repair_sql(schema))
    named = _migration()
    check, key = named._key_sql(schema)
    await connection.execute(check)
    await connection.execute(
        named._function_sql(
            schema,
            names=(
                "cms_native_input_advance",
                "cms_native_input_revision_guard",
                "cms_serving_archive_result_matches",
            ),
        )
    )
    await connection.execute(key)
    for name, marker in (("mrf_address", "old"), ("incoming", "new")):
        await connection.execute(f'CREATE TABLE "{schema}"."{name}" (id int PRIMARY KEY,value text)')
        await connection.execute(f'INSERT INTO "{schema}"."{name}" VALUES (1,$1)', marker)
    current = await registration.capture_native_input_candidate(
        connection,
        ledger_schema=schema,
        target_schema=schema,
        target_table="mrf_address",
        stage_schema=schema,
        stage_table="mrf_address",
    )
    await registration.register_native_input_candidates(connection, ledger_schema=schema, candidates=(current,))


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", [False, True])
async def test_named_input_rename_advances_existing_counter_atomically(monkeypatch, rollback):
    """The existing counter advances with a named swap and rolls back with failed publication."""
    from types import SimpleNamespace

    from process import provider_directory_cms_native_registration as registration
    from process import provider_directory_projection_stage as projection

    url = make_url(_database_url()).set(drivername="postgresql")
    connection = await asyncpg.connect(url.render_as_string(hide_password=False), command_timeout=10)
    schema = "counter_" + uuid4().hex
    transaction = connection.transaction()
    await transaction.start()
    try:
        await _prepare_named_counter_fixture(connection, schema)
        candidate = await registration.capture_native_input_candidate(
            connection,
            ledger_schema=schema,
            target_schema=schema,
            target_table="mrf_address",
            stage_schema=schema,
            stage_table="incoming",
        )
        prepared = await registration.register_native_input_candidates(
            connection, ledger_schema=schema, candidates=(candidate,)
        )
        monkeypatch.setattr(projection, "_copy_driver", AsyncMock(return_value=connection))
        session = SimpleNamespace(in_transaction=lambda: True)
        try:
            async with connection.transaction():
                await connection.execute(f'ALTER TABLE "{schema}".mrf_address RENAME TO previous')
                await connection.execute(f'ALTER TABLE "{schema}".incoming RENAME TO mrf_address')
                await registration.finish_local_native_input_publication(session, prepared=prepared)
                assert (
                    await connection.fetchval(
                        f'SELECT revision FROM "{schema}".cms_native_input_revision WHERE schema_name=$1 AND table_name=$2',
                        schema,
                        "mrf_address",
                    )
                    == 1
                )
                assert await connection.fetchval(f'SELECT value FROM "{schema}".mrf_address') == "new"
                if rollback:
                    raise RuntimeError("late publication failure")
        except RuntimeError as error:
            assert rollback and str(error) == "late publication failure"
        assert await connection.fetchval(f'SELECT value FROM "{schema}".mrf_address') == ("old" if rollback else "new")
        assert await connection.fetchval(
            f'SELECT revision FROM "{schema}".cms_native_input_revision WHERE schema_name=$1 AND table_name=$2',
            schema,
            "mrf_address",
        ) == int(not rollback)
    finally:
        await transaction.rollback()
        await connection.close()


@pytest.mark.asyncio
async def test_executor_without_admin_freezes_unless_an_application_owns_membership():
    """A SET-only executor can freeze; an existing application membership still needs ADMIN to close."""
    url = make_url(_database_url()).set(drivername="postgresql")
    connection = await asyncpg.connect(url.render_as_string(hide_password=False), command_timeout=10)
    suffix = uuid4().hex
    schema, owner_role, executor, reader = "freeze_" + suffix, "owner_" + suffix, "exec_" + suffix, "reader_" + suffix
    transaction = connection.transaction()
    await transaction.start()
    try:
        for role in (owner_role, executor, reader):
            await connection.execute(f'CREATE ROLE "{role}" NOLOGIN')
        await connection.execute(f'GRANT "{owner_role}" TO "{executor}" WITH ADMIN FALSE, INHERIT TRUE, SET TRUE')
        await connection.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{owner_role}"')
        await connection.execute(f'CREATE TABLE "{schema}".finished(value text)')
        await connection.execute(f'ALTER TABLE "{schema}".finished OWNER TO "{owner_role}"')
        await connection.execute(f'SET LOCAL ROLE "{executor}"')
        await freeze_fhir_source_snapshot(connection, schema, namespace_owner=owner_role, reader_roles=(reader,))
        await connection.execute("RESET ROLE")
        await connection.execute(f'GRANT "{owner_role}" TO "{reader}"')
        await connection.execute(f'SET LOCAL ROLE "{executor}"')
        with pytest.raises(asyncpg.InsufficientPrivilegeError, match="revoke"):
            async with connection.transaction():
                await freeze_fhir_source_snapshot(
                    connection, schema, namespace_owner=owner_role, reader_roles=(reader,)
                )
    finally:
        await transaction.rollback()
        await connection.close()
