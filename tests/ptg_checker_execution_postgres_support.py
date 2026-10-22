# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native authenticated-role phases for the checker execution regression."""

import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process.network_fhir_source_custody import _require_routine_capabilities
from process.ptg_parts.ptg2_snapshot_candidates import (
    attach_snapshot_candidates,
    begin_snapshot_candidate,
    candidate_driver,
    finish_snapshot_candidate,
)


async def _create_login_roles(engine, schema, writer, role_names, password, ownership):
    reader, inherited, setter = role_names
    database = engine.url.database.replace('"', '""')
    async with engine.begin() as connection:
        assert not await connection.scalar(
            text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY(CAST(:roles AS text[])))"),
            {"roles": list(role_names)},
        )
        for role in role_names:
            await connection.exec_driver_sql(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
            await connection.exec_driver_sql(f"COMMENT ON ROLE \"{role}\" IS '{ownership}'")
        await connection.exec_driver_sql(f"ALTER ROLE \"{writer}\" LOGIN PASSWORD '{password}'")
        await connection.exec_driver_sql(f'GRANT TEMP ON DATABASE "{database}" TO "{reader}"')
        await connection.exec_driver_sql(f'GRANT USAGE ON SCHEMA "{schema}" TO "{reader}"')
        await connection.exec_driver_sql(f'GRANT "{writer}" TO "{inherited}" WITH INHERIT TRUE, SET FALSE')
        await connection.exec_driver_sql(f'GRANT "{writer}" TO "{setter}" WITH INHERIT FALSE, SET TRUE')
        await connection.exec_driver_sql(
            f'GRANT EXECUTE ON FUNCTION {schema}.check_ptg_snapshot_write(oid,text,name) TO PUBLIC,"{reader}"'
        )


async def _cleanup_roles(engine, schema, role_names, engine_by_role, ownership):
    failures = []
    for login_engine in engine_by_role.values():
        try:
            await login_engine.dispose()
        except Exception as error:
            failures.append(error)
    database = engine.url.database.replace('"', '""')
    for role in reversed(role_names):
        try:
            async with engine.begin() as connection:
                exists = await connection.scalar(
                    text("SELECT shobj_description(oid,'pg_authid') FROM pg_roles WHERE rolname=:role"), {"role": role}
                )
                if exists == ownership:
                    await connection.exec_driver_sql(f'REVOKE ALL ON SCHEMA "{schema}" FROM "{role}"')
                    await connection.exec_driver_sql(f'REVOKE ALL ON DATABASE "{database}" FROM "{role}"')
                    await connection.exec_driver_sql(
                        f'REVOKE ALL ON FUNCTION {schema}.check_ptg_snapshot_write(oid,text,name) FROM "{role}"'
                    )
                    await connection.exec_driver_sql(f'DROP ROLE "{role}"')
        except Exception as error:
            failures.append(error)
    if failures:
        raise ExceptionGroup("checker role cleanup failed", failures)


@asynccontextmanager
async def _authenticated_roles(engine, schema, writer):
    role_names = tuple("ptg_checker_" + uuid4().hex for _ in range(3))
    password = uuid4().hex
    ownership = "checker-test-" + uuid4().hex
    engine_by_role = {
        role: create_async_engine(engine.url.set(username=role, password=password)) for role in (*role_names, writer)
    }
    primary_error = None
    try:
        await _create_login_roles(engine, schema, writer, role_names, password, ownership)
        yield role_names, engine_by_role
    except BaseException as error:
        primary_error = error
        raise
    finally:
        cleanup_task = asyncio.create_task(
            asyncio.wait_for(_cleanup_roles(engine, schema, role_names, engine_by_role, ownership), timeout=20)
        )
        cleanup_cancellation = None
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as cancellation:
                if cleanup_cancellation is None:
                    cleanup_cancellation = cancellation
            except Exception:
                break
        try:
            cleanup_task.result()
        except BaseException as error:
            reported_error = primary_error if primary_error is not None else cleanup_cancellation
            if reported_error is None:
                raise
            reported_error.add_note(f"Checker role cleanup also failed: {error!r}")
        if cleanup_cancellation is not None:
            if primary_error is None:
                raise cleanup_cancellation
            primary_error.add_note("Cancellation requested during checker role cleanup")


async def _assert_permanent_locks(engine, schema, blocked):
    async with engine.begin() as observer:
        statement = f"SELECT 1 FROM {schema}.ptg2_v3_snapshot_layout WHERE snapshot_key=10 FOR UPDATE NOWAIT"
        if blocked:
            with pytest.raises(DBAPIError) as locked:
                async with observer.begin_nested():
                    await observer.exec_driver_sql(statement)
            assert locked.value.orig.sqlstate == "55P03"
        else:
            await observer.exec_driver_sql(statement)
            await observer.exec_driver_sql(
                f"SELECT 1 FROM {schema}.ptg2_v4_snapshot_map_root WHERE snapshot_key=10 FOR UPDATE NOWAIT"
            )


async def _reader_trigger_probe(reader_engine, engine, schema, writer, candidate_oid, allowed):
    async with reader_engine.begin() as connection:
        assert await connection.scalar(text("SELECT session_user=current_user"))
        await connection.exec_driver_sql("CREATE TEMP TABLE checker_probe(value integer) ON COMMIT DROP")
        await connection.exec_driver_sql(f"""CREATE FUNCTION pg_temp.checker_probe() RETURNS trigger
            LANGUAGE plpgsql AS $body$ BEGIN PERFORM {schema}.check_ptg_snapshot_write(
            {candidate_oid},'INSERT','{writer}'::name); RETURN NULL; END $body$""")
        await connection.exec_driver_sql("""CREATE TRIGGER checker_probe AFTER INSERT ON checker_probe
            FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.checker_probe()""")
        if allowed:
            await connection.exec_driver_sql("INSERT INTO checker_probe VALUES(1)")
        else:
            with pytest.raises(DBAPIError) as denied:
                async with connection.begin_nested():
                    await connection.exec_driver_sql("INSERT INTO checker_probe VALUES(1)")
            assert denied.value.orig.sqlstate == "42501"
        await _assert_permanent_locks(engine, schema, allowed)
        await connection.rollback()


async def _upgrade_twice(engine, schema, reader):
    from tests.test_ptg_snapshot_candidates_postgres import _migration

    checker = f"{schema}.check_ptg_snapshot_write(oid,text,name)"
    identity_query = text(
        "SELECT oid,proowner,prosrc,proacl::text FROM pg_proc WHERE oid=CAST(:function AS regprocedure)"
    )
    async with engine.begin() as connection:
        before = (await connection.execute(identity_query, {"function": checker})).one()

        def close(sync):
            migration = _migration()
            migration.op = Operations(MigrationContext.configure(sync))
            migration._close_checker_execution(schema)

        await connection.run_sync(close)
        once = (await connection.execute(identity_query, {"function": checker})).one()
        await connection.run_sync(close)
        twice = (await connection.execute(identity_query, {"function": checker})).one()
        assert before[:3] == once[:3] == twice[:3] and once == twice
        assert not await connection.scalar(
            text("SELECT has_function_privilege(:role,:function,'EXECUTE')"), {"role": reader, "function": checker}
        )
    return before.proowner, before.oid


async def _writer_copy_case(login_engine, schema, actor, writer, needs_set, checker_oid):
    async with login_engine.connect() as connection:
        async with connection.begin():
            assert await connection.scalar(text("SELECT session_user::text")) == actor
            assert await connection.scalar(text("SELECT current_user::text")) == actor
            if needs_set:
                assert not await connection.scalar(
                    text("SELECT has_function_privilege(current_user,CAST(:function_oid AS oid),'EXECUTE')"),
                    {"function_oid": checker_oid},
                )
                assert await connection.scalar(
                    text("SELECT pg_has_role(current_user,:writer,'SET')"), {"writer": writer}
                )
                await connection.exec_driver_sql(f'SET LOCAL ROLE "{writer}"')
            candidate = await begin_snapshot_candidate(connection, schema, "ptg2_provider_tax_identity", 10, "owned")
            await (await candidate_driver(connection)).copy_records_to_table(
                candidate,
                schema_name=schema,
                columns=("snapshot_key", "tin_key", "tin_id_128", "tin_hmac_sha256"),
                records=[(10, 0, b"a" * 16, b"a" * 32)],
            )
            assert await finish_snapshot_candidate(connection, schema, candidate, 1) == 1
            await attach_snapshot_candidates(connection, schema, 10, "owned")
            assert (
                await connection.scalar(
                    text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity WHERE snapshot_key=10")
                )
                == 1
            )
            await connection.rollback()


async def checker_execution_case(engine, schema, writer):
    from tests.test_ptg_snapshot_candidates_postgres import _publish_map_fixture

    async with _authenticated_roles(engine, schema, writer) as (role_names, engine_by_role):
        reader, inherited, setter = role_names
        async with engine_by_role[writer].begin() as connection:
            assert await connection.scalar(text("SELECT session_user::text")) == writer
            candidate = await begin_snapshot_candidate(connection, schema, "ptg2_provider_tax_identity", 10, "owned")
            candidate_oid = await connection.scalar(
                text("SELECT CAST(:relation AS regclass)::oid"), {"relation": f"{schema}.{candidate}"}
            )
        await _reader_trigger_probe(engine_by_role[reader], engine, schema, writer, candidate_oid, True)
        owner_oid, checker_oid = await _upgrade_twice(engine, schema, reader)
        await _reader_trigger_probe(engine_by_role[reader], engine, schema, writer, candidate_oid, False)
        async with engine_by_role[reader].begin() as connection:
            await _require_routine_capabilities(await candidate_driver(connection), (reader,), schema, owner_oid)
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'DROP TABLE "{schema}"."{candidate}"')
            await connection.exec_driver_sql(
                f'DELETE FROM "{schema}".ptg2_snapshot_candidate WHERE table_oid={candidate_oid}'
            )
        for actor in (writer, inherited, setter):
            await _writer_copy_case(engine_by_role[actor], schema, actor, writer, actor == setter, checker_oid)
        await _publish_map_fixture(engine_by_role[writer], schema, writer)
