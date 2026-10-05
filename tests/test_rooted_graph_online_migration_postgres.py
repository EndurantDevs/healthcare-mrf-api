# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Rooted history validation releases metadata locks and resumes safely."""

import asyncio

import asyncpg
import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.util.concurrency import await_only

from db import migration_rooted_graph_candidates as online
from tests.formulary_fhir_twin_admission_pg_support import load_migration, run_migration
from tests.test_provider_directory_rooted_graph_bulk_postgres import (
    MIGRATION_PATH,
    _historical_acquisitions,
    _physical_storage,
)
from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope


async def _assert_writers_fenced(context):
    for suffix in ("acquisition", "work", "resource", "edge"):
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="migration_incomplete_rerun_migration"):
            await context.connection.execute(
                f"DELETE FROM {context.schema}.provider_directory_rooted_graph_{suffix} WHERE false"
            )


async def _inspect_validation(context, backend, observations):
    locks = await context.connection.fetch(
        "SELECT mode FROM pg_locks WHERE pid=$1 AND locktype='relation' AND granted",
        backend,
    )
    assert not any(row["mode"] == "AccessExclusiveLock" for row in locks)
    assert any(row["mode"] == "ShareUpdateExclusiveLock" for row in locks)
    assert (
        await asyncio.wait_for(
            context.connection.fetchval(
                f"SELECT count(*) FROM {context.schema}.provider_directory_rooted_graph_work",
            ),
            timeout=1,
        )
        > 0
    )
    await _assert_writers_fenced(context)
    observations.append(backend)


async def _assert_initial_reader_blocks_only_cutover(context, migration):
    async with context.connection.transaction():
        await context.connection.fetchval(f"SELECT count(*) FROM {context.schema}.provider_directory_rooted_graph_work")
        with pytest.raises(DBAPIError) as failure:
            await run_migration(context.engine, migration, "upgrade")
        assert failure.value.orig.sqlstate == "55P03"
    assert (
        await context.connection.fetchval(
            "SELECT to_regclass($1)",
            f"{context.schema_name}.{online._PLAN}",
        )
        is None
    )


async def _interrupt_finish(context, migration, monkeypatch):
    def fail_before_cutover(*_arguments):
        raise RuntimeError("synthetic_cutover_failure")

    with monkeypatch.context() as patch:
        patch.setattr(online, "_finish", fail_before_cutover)
        with pytest.raises(RuntimeError, match="synthetic_cutover_failure"):
            await run_migration(context.engine, migration, "upgrade")
    await _assert_writers_fenced(context)
    assert await context.connection.fetchval(
        f"SELECT bool_and(convalidated) FROM pg_constraint WHERE connamespace='{context.schema}'::regnamespace "
        "AND conname IN ('pdrg_work_legacy_scope','pdrg_resource_legacy_scope','pdrg_edge_legacy_scope')",
    )


@pytest.mark.asyncio
async def test_rooted_online_validation_keeps_readers_and_retry_preserves_storage(monkeypatch):
    async with _lifecycle_scope(monkeypatch, set_validation=False) as context:
        await _historical_acquisitions(context, monkeypatch)
        migration = load_migration(
            MIGRATION_PATH.with_name("20261005100000_rooted_graph_set_validation.py"), "rooted_online"
        )
        original_storage = await _physical_storage(context.connection, context.schema_name)
        await _assert_initial_reader_blocks_only_cutover(context, migration)
        observations = []
        cutover_scans = []
        work_oid = await context.connection.fetchval(
            "SELECT $1::regclass::oid",
            f"{context.schema}.provider_directory_rooted_graph_work",
        )
        prior_scan_counts = []

        def before_statement(connection, _cursor, statement, _parameters, _context, _executemany):
            if "CREATE TEMP TABLE pdrg_work_writer_acl" in statement:
                prior_scan_counts.append(
                    connection.execute(
                        text(
                            "SELECT seq_tup_read FROM pg_stat_xact_user_tables WHERE relid=:oid",
                        ),
                        {"oid": work_oid},
                    ).scalar()
                )

        def after_statement(connection, _cursor, statement, _parameters, _context, _executemany):
            if " VALIDATE CONSTRAINT pdrg_" in statement:
                backend = connection.exec_driver_sql("SELECT pg_backend_pid()").scalar()
                await_only(_inspect_validation(context, backend, observations))
            elif "CREATE TEMP TABLE pdrg_work_writer_acl" in statement:
                cutover_scans.append(
                    connection.exec_driver_sql(
                        "SELECT seq_tup_read FROM pg_stat_xact_user_tables WHERE relid=" + str(work_oid),
                    ).scalar()
                    - prior_scan_counts[-1]
                )

        event.listen(context.engine.sync_engine, "before_cursor_execute", before_statement)
        event.listen(context.engine.sync_engine, "after_cursor_execute", after_statement)
        try:
            await _interrupt_finish(context, migration, monkeypatch)
            await run_migration(context.engine, migration, "upgrade")
            await run_migration(context.engine, migration, "upgrade")
        finally:
            event.remove(context.engine.sync_engine, "before_cursor_execute", before_statement)
            event.remove(context.engine.sync_engine, "after_cursor_execute", after_statement)
        assert len(observations) == 6
        assert cutover_scans == [0]
        await _assert_completed_storage(context, original_storage)


async def _assert_completed_storage(context, original_storage):
    assert (
        await context.connection.fetch(
            "SELECT oid,relfilenode FROM pg_class WHERE oid=ANY($1::oid[]) ORDER BY oid",
            [relation["oid"] for relation in original_storage],
        )
        == original_storage
    )
    assert await context.connection.fetchval(f"SELECT complete FROM {context.schema}.{online._PLAN}")
    assert not await context.connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_constraint WHERE contype='f' AND conrelid=ANY($1::regclass[]))",
        [
            f"{context.schema}.provider_directory_rooted_graph_{suffix}{legacy}"
            for suffix in ("work", "resource", "edge")
            for legacy in ("", "_legacy")
        ],
    )
    assert not await context.connection.fetchval(
        "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname=$1 AND tgrelid=$2::regclass)",
        online._FENCE,
        f"{context.schema}.provider_directory_rooted_graph_acquisition",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ("constraint_unvalidated", "index_invalid"))
async def test_rooted_catalog_drift_fails_before_committed_writer_fences(monkeypatch, drift):
    async with _lifecycle_scope(monkeypatch, set_validation=False) as context:
        await _historical_acquisitions(context, monkeypatch)
        resource = f"{context.schema}.provider_directory_rooted_graph_resource"
        if drift == "constraint_unvalidated":
            constraint = "provider_directory_rooted_graph_resource_work_fkey"
            definition = await context.connection.fetchval(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=$1::regclass AND conname=$2",
                resource,
                constraint,
            )
            await context.connection.execute(f"ALTER TABLE {resource} DROP CONSTRAINT {constraint}")
            await context.connection.execute(
                f"ALTER TABLE {resource} ADD CONSTRAINT {constraint} {definition} NOT VALID"
            )
        else:
            with pytest.raises(asyncpg.UniqueViolationError):
                await context.connection.execute(
                    f"CREATE UNIQUE INDEX CONCURRENTLY synthetic_invalid_index ON {resource} ((1))"
                )
        migration = load_migration(
            MIGRATION_PATH.with_name("20261005100000_rooted_graph_set_validation.py"), "rooted_online_drift"
        )
        with pytest.raises(DBAPIError, match="rooted_graph_storage_" + drift):
            await run_migration(context.engine, migration, "upgrade")
        assert (
            await context.connection.fetchval("SELECT to_regclass($1)", f"{context.schema_name}.{online._PLAN}") is None
        )
        assert not await context.connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname=$1 AND tgrelid=$2::regclass)",
            online._FENCE,
            resource,
        )


@pytest.mark.asyncio
async def test_rooted_retry_refuses_unattached_shadow_and_recovers(monkeypatch):
    async with _lifecycle_scope(monkeypatch, set_validation=False) as context:
        await _historical_acquisitions(context, monkeypatch)
        migration = load_migration(
            MIGRATION_PATH.with_name("20261005100000_rooted_graph_set_validation.py"), "rooted_shadow"
        )
        original_storage = await _physical_storage(context.connection, context.schema_name)
        await _interrupt_finish(context, migration, monkeypatch)
        shadow = online._shadow_relation(context.schema_name, "work")
        work = online._relation(context.schema_name, "work")
        bound = await context.connection.fetchval(
            "SELECT pg_get_expr(relpartbound,oid) FROM pg_class WHERE oid=$1::regclass", work
        )
        await context.connection.execute(f"ALTER TABLE {shadow} DETACH PARTITION {work}")
        with pytest.raises(RuntimeError, match="rooted_graph_migration_shadow_invalid"):
            await run_migration(context.engine, migration, "upgrade")
        await _assert_writers_fenced(context)
        await context.connection.execute(f"ALTER TABLE {shadow} ATTACH PARTITION {work} {bound}")
        await run_migration(context.engine, migration, "upgrade")
        assert (
            await context.connection.fetch(
                "SELECT oid,relfilenode FROM pg_class WHERE oid=ANY($1::oid[]) ORDER BY oid",
                [relation["oid"] for relation in original_storage],
            )
            == original_storage
        )


@pytest.mark.asyncio
async def test_rooted_migration_contention_refuses_then_recovers(monkeypatch):
    async with _lifecycle_scope(monkeypatch, set_validation=False) as context:
        migration = load_migration(
            MIGRATION_PATH.with_name("20261005100000_rooted_graph_set_validation.py"), "rooted_contention"
        )
        lock_key = "rooted_graph_candidates:" + context.schema_name
        await context.connection.execute("SELECT pg_advisory_lock(hashtextextended($1,0))", lock_key)
        try:
            with pytest.raises(RuntimeError, match="rooted_graph_migration_already_running"):
                await asyncio.wait_for(run_migration(context.engine, migration, "upgrade"), timeout=2)
            assert (
                await context.connection.fetchval("SELECT to_regclass($1)", f"{context.schema}.{online._PLAN}") is None
            )
        finally:
            await context.connection.execute("SELECT pg_advisory_unlock(hashtextextended($1,0))", lock_key)
        await run_migration(context.engine, migration, "upgrade")
        assert await context.connection.fetchval(f"SELECT complete FROM {context.schema}.{online._PLAN}")
