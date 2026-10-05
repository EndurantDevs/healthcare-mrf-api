# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Practitioner validation remains readable across committed migration retries."""

import asyncio
import uuid
from contextlib import asynccontextmanager

import asyncpg
import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.util.concurrency import await_only

from db import migration_practitioner_candidates as online
from tests.formulary_fhir_twin_admission_pg_support import connect, database_url, drop_schema, run_migration
from tests.test_practitioner_migration_boundaries_postgres import _migration
from tests.test_practitioner_set_validation_postgres import _legacy_acquisition
from tests.test_provider_directory_uhc_flex_practitioner_acquisition_postgres import (
    _configure_database,
    _prepare_schema,
    _role_identities,
)


@asynccontextmanager
async def _migration_scope(monkeypatch):
    url = database_url()
    schema = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    connection = await connect(url)
    database = _configure_database(monkeypatch, url)
    try:
        await _prepare_schema(engine, url, schema, latest=False)
        await database.connect()
        active, sealed = _role_identities()
        await _legacy_acquisition(database, active, sealed=False)
        await _legacy_acquisition(database, sealed, sealed=True)
        yield engine, connection, schema
    finally:
        await database.disconnect()
        await connection.close()
        await drop_schema(engine, schema)
        await engine.dispose()


async def _assert_fenced(connection, schema):
    for suffix in ("acquisition", "work", "resource"):
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="migration_incomplete_rerun_migration"):
            await connection.execute(f"DELETE FROM {online._relation(schema, suffix)} WHERE false")


async def _inspect_reader(connection, schema, backend):
    locks = await connection.fetch(
        "SELECT mode FROM pg_locks WHERE pid=$1 AND locktype='relation' AND granted", backend
    )
    assert any(row["mode"] == "ShareUpdateExclusiveLock" for row in locks)
    assert not any(row["mode"] == "AccessExclusiveLock" for row in locks)
    for suffix in ("acquisition", "work", "resource"):
        assert (
            await asyncio.wait_for(
                connection.fetchval(f"SELECT count(*) FROM {online._relation(schema, suffix)}"), timeout=1
            )
            > 0
        )
    await _assert_fenced(connection, schema)


def _watch_cutover_scans(monkeypatch, old_oids):
    finish = online._finish

    def inspect_cutover(op, candidate_schema, sql):
        before_scan = op.get_bind().scalar(
            text("SELECT sum(seq_tup_read) FROM pg_stat_xact_user_tables WHERE relid=ANY(:oids)"),
            {"oids": old_oids},
        )
        finish(op, candidate_schema, sql)
        scanned = op.get_bind().scalar(
            text("SELECT sum(seq_tup_read) FROM pg_stat_xact_user_tables WHERE relid=ANY(:oids)"),
            {"oids": old_oids},
        )
        assert scanned == before_scan

    monkeypatch.setattr(online, "_finish", inspect_cutover)


@pytest.mark.asyncio
async def test_practitioner_validation_preserves_readers_and_keys(monkeypatch):
    async with _migration_scope(monkeypatch) as (engine, connection, schema):
        resource = online._relation(schema, "resource")
        work = online._relation(schema, "work")
        old_resource = await connection.fetchval("SELECT $1::regclass::oid", resource)
        old_work = await connection.fetchval("SELECT $1::regclass::oid", work)
        indexes = await connection.fetch(
            "SELECT indexrelid,indrelid FROM pg_index WHERE indrelid=ANY($1::oid[]) ORDER BY indexrelid",
            [old_resource, old_work],
        )
        foreign_key = await connection.fetchrow(
            "SELECT oid,conrelid,confrelid FROM pg_constraint WHERE conrelid=$1 AND contype='f'", old_resource
        )
        validation_calls = []
        _watch_cutover_scans(monkeypatch, [old_resource, old_work])

        def inspect_validation(sync_connection, _cursor, statement, _parameters, _context, _executemany):
            if " VALIDATE CONSTRAINT " in statement:
                backend = sync_connection.exec_driver_sql("SELECT pg_backend_pid()").scalar()
                await_only(_inspect_reader(connection, schema, backend))
                validation_calls.append(statement)

        event.listen(engine.sync_engine, "after_cursor_execute", inspect_validation)
        try:
            await run_migration(engine, _migration(), "upgrade")
        finally:
            event.remove(engine.sync_engine, "after_cursor_execute", inspect_validation)
        assert len(validation_calls) == 2
        assert (
            await connection.fetchval("SELECT $1::regclass::oid", online._relation(schema, "resource_legacy"))
            == old_resource
        )
        assert (
            await connection.fetchval("SELECT $1::regclass::oid", online._relation(schema, "work_legacy")) == old_work
        )
        assert (
            await connection.fetch(
                "SELECT indexrelid,indrelid FROM pg_index WHERE indrelid=ANY($1::oid[]) ORDER BY indexrelid",
                [old_resource, old_work],
            )
            == indexes
        )
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE oid=$1)", foreign_key["oid"]
        )
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE contype='f' AND conrelid=ANY($1::oid[]))",
            [
                old_resource,
                old_work,
                await connection.fetchval("SELECT $1::regclass::oid", resource),
                await connection.fetchval("SELECT $1::regclass::oid", work),
            ],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_after", ["preparation", "validation", "conversion"])
async def test_practitioner_online_migration_resumes(monkeypatch, stop_after):
    async with _migration_scope(monkeypatch) as (engine, connection, schema):
        resource = online._relation(schema, "resource")
        before = await connection.fetch(f"SELECT tableoid,* FROM {resource} ORDER BY acquisition_id,npi")
        finish = online._finish

        def interrupt_conversion(*args):
            if stop_after == "conversion":
                finish(*args)
            raise RuntimeError("injected_practitioner_migration_failure")

        def interrupt_validation(_connection, _cursor, statement, _parameters, _context, _executemany):
            if stop_after == "preparation" and " VALIDATE CONSTRAINT " in statement:
                raise RuntimeError("injected_practitioner_migration_failure")

        monkeypatch.setattr(online, "_finish", interrupt_conversion)
        event.listen(engine.sync_engine, "before_cursor_execute", interrupt_validation)
        try:
            with pytest.raises(RuntimeError, match="injected_practitioner_migration_failure"):
                await run_migration(engine, _migration(), "upgrade")
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", interrupt_validation)
        await _assert_fenced(connection, schema)
        assert await connection.fetch(f"SELECT tableoid,* FROM {resource} ORDER BY acquisition_id,npi") == before
        assert await connection.fetchval(
            "SELECT convalidated FROM pg_constraint WHERE conrelid=$1::regclass AND conname=$2", resource, online._BOUND
        ) is (stop_after != "preparation")
        monkeypatch.setattr(online, "_finish", finish)
        await run_migration(engine, _migration(), "upgrade")
        await run_migration(engine, _migration(), "upgrade")
        assert await connection.fetch(f"SELECT tableoid,* FROM {resource} ORDER BY acquisition_id,npi") == before
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=$1::regclass AND tgname=$2)", resource, online._FENCE
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("completed", [False, True])
@pytest.mark.parametrize("suffix", ("acquisition", "work", "resource"))
async def test_practitioner_retry_rejects_replaced_relation(monkeypatch, completed, suffix):
    async with _migration_scope(monkeypatch) as (engine, connection, schema):
        finish = online._finish
        if completed:
            await run_migration(engine, _migration(), "upgrade")
        else:

            def interrupt(*_args):
                raise RuntimeError("injected_practitioner_cutover_failure")

            monkeypatch.setattr(online, "_finish", interrupt)
            with pytest.raises(RuntimeError, match="injected_practitioner_cutover_failure"):
                await run_migration(engine, _migration(), "upgrade")
            monkeypatch.setattr(online, "_finish", finish)
        canonical = online._relation(schema, suffix)
        original_oid = await connection.fetchval("SELECT $1::regclass::oid", canonical)
        original_count = await connection.fetchval(f"SELECT count(*) FROM {canonical}")
        displaced = f"{online._quote(schema)}.synthetic_displaced_resource"
        await connection.execute(f"ALTER TABLE {canonical} RENAME TO synthetic_displaced_resource")
        await connection.execute(f"CREATE TABLE {canonical}(LIKE {displaced}) PARTITION BY LIST(acquisition_id)")
        replacement_oid = await connection.fetchval("SELECT $1::regclass::oid", canonical)
        with pytest.raises(RuntimeError, match="practitioner_migration_relation_drift"):
            await run_migration(engine, _migration(), "upgrade")
        assert await connection.fetchval("SELECT $1::regclass::oid", canonical) == replacement_oid
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=$1)", replacement_oid
        )
        assert await connection.fetchval(f"SELECT count(*) FROM {displaced}") == original_count
        await connection.execute(f"DROP TABLE {canonical}")
        await connection.execute(f"ALTER TABLE {displaced} RENAME TO provider_directory_uhc_flex_practitioner_{suffix}")
        await run_migration(engine, _migration(), "upgrade")
        retained = canonical if completed or suffix == "acquisition" else online._relation(schema, suffix + "_legacy")
        assert await connection.fetchval("SELECT $1::regclass::oid", retained) == original_oid


@pytest.mark.asyncio
async def test_practitioner_retry_requires_prepared_writer_fences(monkeypatch):
    async with _migration_scope(monkeypatch) as (engine, connection, schema):

        def interrupt(*_arguments):
            raise RuntimeError("synthetic_prepared_interruption")

        with monkeypatch.context() as patch:
            patch.setattr(online, "_finish", interrupt)
            with pytest.raises(RuntimeError, match="synthetic_prepared_interruption"):
                await run_migration(engine, _migration(), "upgrade")
        work = online._relation(schema, "work")
        trigger = await connection.fetchval(
            "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid=$1::regclass AND tgname=$2",
            work,
            online._FENCE,
        )
        await connection.execute(f"DROP TRIGGER {online._FENCE} ON {work}")
        with pytest.raises(RuntimeError, match="practitioner_migration_fence_drift"):
            await run_migration(engine, _migration(), "upgrade")
        assert await connection.fetchval("SELECT to_regclass($1)", online._relation(schema, "work_legacy")) is None
        await connection.execute(trigger)
        await connection.execute(f"ALTER TABLE {work} ENABLE ALWAYS TRIGGER {online._FENCE}")
        await _assert_fenced(connection, schema)
        await run_migration(engine, _migration(), "upgrade")


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ("receipt", "attachment"))
async def test_practitioner_completed_retry_requires_receipt_and_history(monkeypatch, drift):
    async with _migration_scope(monkeypatch) as (engine, connection, schema):
        await run_migration(engine, _migration(), "upgrade")
        work = online._relation(schema, "work")
        history = online._relation(schema, "work_legacy")
        before = await connection.fetch(f"SELECT tableoid,* FROM {work} ORDER BY acquisition_id,npi")
        receipt = f"{online._quote(schema)}.{online._PLAN}"
        if drift == "receipt":
            await connection.execute(f"ALTER TABLE {receipt} RENAME TO synthetic_displaced_receipt")
            restore = f"ALTER TABLE {online._quote(schema)}.synthetic_displaced_receipt RENAME TO {online._PLAN}"
        else:
            bound = await connection.fetchval(
                "SELECT pg_get_expr(relpartbound,oid) FROM pg_class WHERE oid=$1::regclass", history
            )
            await connection.execute(f"ALTER TABLE {work} DETACH PARTITION {history}")
            restore = f"ALTER TABLE {work} ATTACH PARTITION {history} {bound}"
        with pytest.raises(RuntimeError, match="practitioner_migration_(catalog|relation)_drift"):
            await run_migration(engine, _migration(), "upgrade")
        assert await connection.fetch(f"SELECT tableoid,* FROM {history} ORDER BY acquisition_id,npi") == before
        await connection.execute(restore)
        await run_migration(engine, _migration(), "upgrade")
        assert await connection.fetch(f"SELECT tableoid,* FROM {work} ORDER BY acquisition_id,npi") == before


@pytest.mark.asyncio
async def test_practitioner_migration_contention_refuses_then_recovers(monkeypatch):
    async with _migration_scope(monkeypatch) as (engine, connection, schema):
        lock_key = "practitioner_candidates:" + schema
        await connection.execute("SELECT pg_advisory_lock(hashtextextended($1,0))", lock_key)
        try:
            with pytest.raises(RuntimeError, match="practitioner_migration_already_running"):
                await asyncio.wait_for(run_migration(engine, _migration(), "upgrade"), timeout=2)
            assert await connection.fetchval("SELECT to_regclass($1)", f"{schema}.{online._PLAN}") is None
        finally:
            await connection.execute("SELECT pg_advisory_unlock(hashtextextended($1,0))", lock_key)
        await run_migration(engine, _migration(), "upgrade")
        assert (
            await connection.fetchval(
                "SELECT relkind::text FROM pg_class WHERE oid=$1::regclass", online._relation(schema, "work")
            )
            == "p"
        )
