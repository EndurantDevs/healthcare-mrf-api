# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL checks for completion, stale inputs and crash reconciliation."""

from functools import partial
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from db.connection import Database
from process import mrf_publication_receipt as receipt
from process import reference_family_result_generation as generation
from tests.test_reference_family_result_generation_postgres import (
    _MRF_MIGRATION_PATH,
    _REFERENCE_MIGRATION_PATH,
    _database_url,
    _migration_module,
    _run_migration,
)

_MIGRATION = Path(__file__).resolve().parents[1] / "alembic/versions/20260920170000_mrf_publication_receipt.py"


class _EmptyMigrationOperations:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(" ".join(str(statement).split()))
        return self

    def get_bind(self):
        return self

    def scalar(self):
        return 0


def test_empty_downgrade_fences_receipt_before_check_and_drop(monkeypatch):
    migration = _migration_module(_MIGRATION)
    operations = _EmptyMigrationOperations()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.op = operations

    migration.downgrade()

    assert operations.statements == [
        'LOCK TABLE "synthetic"."mrf_publication_receipt" IN ACCESS EXCLUSIVE MODE',
        'SELECT EXISTS(SELECT 1 FROM "synthetic"."mrf_publication_receipt")',
        'DROP TABLE "synthetic"."mrf_publication_receipt"',
    ]


async def _summary_database_ready(_test_mode):
    """Avoid external setup while the test uses a schema-bound database."""


async def _retain_prepared_source(_session, _prepared):
    """Leave cleanup to the test's finally block."""


async def _configure_summary_tables(connection, monkeypatch, schema, *, drop_existing=False):
    """Bind the real summary models to this test schema."""

    from process import plan_summary

    metadata = MetaData()
    for attribute_name in (
        "plan_table",
        "plan_attributes_table",
        "plan_benefits_table",
        "plan_prices_table",
        "summary_table",
    ):
        table = getattr(plan_summary, attribute_name).to_metadata(metadata, schema=schema)
        monkeypatch.setattr(plan_summary, attribute_name, table)
        if drop_existing:
            await connection.execute(text(f'DROP TABLE "{schema}"."{table.name}"'))
        if drop_existing or table.name != "plan":
            await connection.run_sync(lambda sync, table=table: table.create(sync, checkfirst=not drop_existing))
    monkeypatch.setattr(
        plan_summary,
        "PRICE_RATE_COLUMNS",
        tuple(plan_summary.plan_prices_table.c[column.name] for column in plan_summary.PRICE_RATE_COLUMNS),
    )


async def _restore_rotated_summary_relation(engine, schema, table_name, prepare):
    """Verify a changed serving relation invalidates a completed source receipt."""

    async with engine.begin() as connection:
        await connection.execute(text(f'ALTER TABLE "{schema}"."{table_name}" RENAME TO identity_old'))
        await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (LIKE "{schema}".identity_old)'))
    with pytest.raises(RuntimeError, match="completion summary identity differs"):
        await prepare()
    async with engine.begin() as connection:
        await connection.execute(text(f'DROP TABLE "{schema}"."{table_name}"'))
        await connection.execute(text(f'ALTER TABLE "{schema}".identity_old RENAME TO "{table_name}"'))


async def _assert_summary_failure_revokes_source_admission(
    monkeypatch, plan_summary, archive_prepare, schema, authority
):
    """Keep a failed summary rebuild from leaving a source archive admissible."""

    failed_attempt = await receipt.begin_publication(schema, "summary_failure")
    ensure_indexes = plan_summary._ensure_summary_indexes
    call_counts = [0]

    async def fail_final_indexes():
        call_counts[0] += 1
        if call_counts[0] == 2:
            raise RuntimeError("summary failure")
        await ensure_indexes()

    monkeypatch.setattr(plan_summary, "_ensure_summary_indexes", fail_final_indexes)
    with pytest.raises(RuntimeError, match="summary failure"):
        await plan_summary.rebuild_plan_search_summary(publication=(failed_attempt, authority, False))
    with pytest.raises(RuntimeError, match="completion is unavailable"):
        await archive_prepare()


async def _cleanup_prepared_mrf_stage(sessions, archive, prepared, engine, schema):
    """Remove only the prepared stage and disposable schema created by this test."""

    if prepared is not None:
        async with sessions() as session, session.begin():
            await archive.cleanup_reference_family_stage(session, prepared.ownership)
    async with engine.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    await engine.dispose()


async def _prepare_native_mrf_source(sessions, archive, schema):
    async def dependencies(_session):
        return {"plan-attributes": "b" * 64}

    return await archive.prepare_reference_family_archive_source(
        sessions,
        importer_id="mrf",
        schema_name=schema,
        dataset_id=uuid4(),
        source_metadata={"source_release": "synthetic"},
        on_prepared=_retain_prepared_source,
        dependency_factory=dependencies,
    )


async def _assert_completed_source_admission(database, plan_summary, prepare, schema):
    with pytest.raises(RuntimeError, match="completion is unavailable"):
        await prepare()
    attempt = await receipt.begin_publication(schema, "untracked")
    async with database.transaction() as session:
        authority = await generation.publish_local_reference_family_generation(
            session,
            importer_id="mrf",
            schema_name=schema,
        )
    with pytest.raises(RuntimeError, match="completion is unavailable"):
        await prepare()
    await plan_summary.rebuild_plan_search_summary(publication=(attempt, authority, False))
    return await prepare(), authority


async def _assert_completed_source_becomes_stale(engine, database, schema, prepare):
    for table_name in ("plan_search_summary", "plan_prices"):
        await _restore_rotated_summary_relation(engine, schema, table_name, prepare)
    async with database.transaction() as session:
        await generation.publish_local_reference_family_generation(
            session,
            importer_id="mrf",
            schema_name=schema,
        )
    with pytest.raises(RuntimeError, match="completion generation differs"):
        await prepare()


async def _assert_completion_rollback(engine, database, schema, attempt, authority, inputs):
    """Prove late failure rolls serving DDL and receipt state back together."""

    async with engine.connect() as observer:
        assert await observer.scalar(text(f'SELECT state FROM "{schema}".{receipt.TABLE}')) == "pending"
        with pytest.raises(RuntimeError, match="late failure"):
            async with database.transaction() as session:
                await session.execute(text(f'ALTER TABLE "{schema}".plan_search_summary RENAME TO summary_old'))
                await session.execute(text(f'CREATE TABLE "{schema}".plan_search_summary (value integer)'))
                await receipt.complete_publication(session, schema, attempt, authority, inputs, True)
                assert await observer.scalar(text(f'SELECT state FROM "{schema}".{receipt.TABLE}')) == "pending"
                raise RuntimeError("late failure")
        assert await observer.scalar(text(f'SELECT value FROM "{schema}".plan_search_summary')) == 1
        assert await observer.scalar(text(f'SELECT state FROM "{schema}".{receipt.TABLE}')) == "pending"
    async with database.transaction() as session:
        await receipt.complete_publication(session, schema, attempt, authority, inputs, True)
    async with engine.begin() as connection:
        with pytest.raises(RuntimeError, match="reconciled before downgrade"):
            await _run_migration(connection, _MIGRATION, "downgrade")
    async with engine.connect() as observer:
        completed_receipt = (await observer.execute(text(f'SELECT * FROM "{schema}".{receipt.TABLE}'))).mappings().one()
        assert completed_receipt["state"] == "complete"
        assert completed_receipt["generation"]["local_generation"] == authority.local_generation
        assert completed_receipt["summary_inputs"] == inputs
        assert completed_receipt["address_resolution_performed"] is True
        assert completed_receipt["summary_oid"] == await observer.scalar(
            text(f"SELECT '{schema}.plan_search_summary'::regclass::oid")
        )


async def _recover_failed_receipt(database, schema, authority, inputs):
    """Reject stale completion claims, then recover only the exact failed token."""

    failed_attempt = await receipt.begin_publication(schema, "later_failure")
    async with database.transaction() as session:
        newer_authority = await generation.publish_local_reference_family_generation(
            session,
            importer_id="mrf",
            schema_name=schema,
        )
    with pytest.raises(RuntimeError, match="generation changed"):
        async with database.transaction() as session:
            await receipt.complete_publication(session, schema, failed_attempt, authority, inputs, False)
    return failed_attempt, newer_authority


async def _assert_summary_input_drift_and_recover(engine, database, schema, failed_attempt, authority, inputs):
    """Reject changed summary inputs and return a fresh recovery attempt."""

    async with database.transaction() as session:
        await session.execute(text(f'ALTER TABLE "{schema}".plan_prices RENAME TO prices_old'))
        await session.execute(text(f'CREATE TABLE "{schema}".plan_prices (value integer)'))
    with pytest.raises(RuntimeError, match="inputs changed"):
        async with database.transaction() as session:
            await receipt.complete_publication(session, schema, failed_attempt, authority, inputs, False)
    with pytest.raises(RuntimeError, match="reconcile"):
        await receipt.begin_publication(schema, "retry_after_crash")
    async with database.transaction() as session:
        deletion_result = await session.execute(
            text(f"""
            DELETE FROM "{schema}".{receipt.TABLE}
            WHERE state='pending' AND attempt_id=CAST(:attempt AS uuid)
        """),
            {"attempt": failed_attempt},
        )
        assert deletion_result.rowcount == 1
    recovery_attempt = await receipt.begin_publication(schema, "full_rebuild")
    assert recovery_attempt != failed_attempt
    async with database.transaction() as session:
        new_inputs = await receipt.capture_summary_inputs(session, schema)
        with pytest.raises(RuntimeError, match="claim changed"):
            await receipt.complete_publication(session, schema, failed_attempt, authority, new_inputs, False)
    return recovery_attempt


async def _assert_summary_index_failure(monkeypatch, plan_summary, database, schema, authority, completed_summary_oid):
    """Keep a final index failure from replacing the completed summary receipt."""

    index_failure_attempt = await receipt.begin_publication(schema, "index_failure")
    ensure_indexes = plan_summary._ensure_summary_indexes
    failure_call_counts = [0]

    async def fail_final_indexes():
        failure_call_counts[0] += 1
        if failure_call_counts[0] == 2:
            raise RuntimeError("index failure")
        await ensure_indexes()

    monkeypatch.setattr(plan_summary, "_ensure_summary_indexes", fail_final_indexes)
    with pytest.raises(RuntimeError, match="index failure"):
        await plan_summary.rebuild_plan_search_summary(publication=(index_failure_attempt, authority, False))
    async with database.engine.connect() as observer:
        assert (
            await observer.scalar(text(f"SELECT '{schema}.plan_search_summary'::regclass::oid"))
            == completed_summary_oid
        )
        assert await observer.scalar(text(f'SELECT state FROM "{schema}".{receipt.TABLE}')) == "pending"


async def _rebuild_summary_with_input_fence(
    monkeypatch, plan_summary, engine, database, schema, recovery_attempt, authority
):
    """Prove final summary indexing keeps source writes fenced through completion."""

    ensure_indexes = plan_summary._ensure_summary_indexes
    index_call_counts = [0]

    async def verify_input_write_fence():
        index_call_counts[0] += 1
        if index_call_counts[0] == 2:
            with pytest.raises(DBAPIError) as blocked:
                async with engine.begin() as writer:
                    await writer.execute(text("SET LOCAL lock_timeout='100ms'"))
                    await writer.execute(text(f'UPDATE "{schema}".plan_prices SET year=year'))
            assert blocked.value.orig.sqlstate == "55P03"
        await ensure_indexes()

    monkeypatch.setattr(plan_summary, "_ensure_summary_indexes", verify_input_write_fence)
    assert await plan_summary.rebuild_plan_search_summary(publication=(recovery_attempt, authority, False)) == 0
    assert index_call_counts[0] == 2
    async with engine.connect() as observer:
        completed_summary_oid = await observer.scalar(
            text(f"SELECT summary_oid FROM \"{schema}\".{receipt.TABLE} WHERE state='complete'")
        )
        assert completed_summary_oid
        assert (
            await observer.scalar(text(f'SELECT address_resolution_performed FROM "{schema}".{receipt.TABLE}')) is False
        )
    return completed_summary_oid


async def _create_receipt_schema(engine, schema):
    """Create the minimal MRF family and receipt migrations for this lifecycle test."""

    async with engine.begin() as connection:
        assert 180000 <= int(await connection.scalar(text("SHOW server_version_num"))) < 190000
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        for name in dict.fromkeys(name for tables in generation.RELATION_NAMES_BY_IMPORTER.values() for name in tables):
            await connection.execute(text(f'CREATE TABLE "{schema}"."{name}" (value integer)'))
        await connection.execute(text(f'CREATE TABLE "{schema}".plan_search_summary (value integer)'))
        await connection.execute(text(f'INSERT INTO "{schema}".plan_search_summary VALUES (1)'))
        await _run_migration(connection, _REFERENCE_MIGRATION_PATH, "upgrade")
        await _run_migration(connection, _MRF_MIGRATION_PATH, "upgrade")
        await _run_migration(connection, _MIGRATION, "upgrade")


async def _publish_mrf_authority(database, schema):
    async with database.transaction() as session:
        return await generation.publish_local_reference_family_generation(
            session, importer_id="mrf", schema_name=schema
        )


@pytest.mark.asyncio
async def test_completion_visibility_rollback_input_drift_and_explicit_recovery(monkeypatch):
    """Preserve receipt visibility, rollback, drift rejection, and explicit recovery."""

    schema = "mrf_receipt_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url(), poolclass=NullPool)
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    monkeypatch.setattr(receipt, "db", database)
    try:
        await _create_receipt_schema(engine, schema)
        attempt = await receipt.begin_publication(schema, "synthetic_untracked")
        async with engine.begin() as connection:
            with pytest.raises(RuntimeError, match="reconciled before downgrade"):
                await _run_migration(connection, _MIGRATION, "downgrade")
        with pytest.raises(RuntimeError, match="reconcile"):
            await receipt.begin_publication(schema, "concurrent")
        authority = await _publish_mrf_authority(database, schema)
        async with database.transaction() as session:
            inputs = await receipt.capture_summary_inputs(session, schema)
        await _assert_completion_rollback(engine, database, schema, attempt, authority, inputs)
        failed_attempt, authority = await _recover_failed_receipt(database, schema, authority, inputs)
        recovery_attempt = await _assert_summary_input_drift_and_recover(
            engine,
            database,
            schema,
            failed_attempt,
            authority,
            inputs,
        )

        from process import plan_summary

        async with engine.begin() as connection:
            await _configure_summary_tables(connection, monkeypatch, schema, drop_existing=True)
        monkeypatch.setattr(plan_summary, "db", database)
        monkeypatch.setattr(plan_summary, "ensure_database", _summary_database_ready)
        authority = await _publish_mrf_authority(database, schema)
        completed_summary_oid = await _rebuild_summary_with_input_fence(
            monkeypatch,
            plan_summary,
            engine,
            database,
            schema,
            recovery_attempt,
            authority,
        )
        await _assert_summary_index_failure(
            monkeypatch,
            plan_summary,
            database,
            schema,
            authority,
            completed_summary_oid,
        )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_source_prepare_requires_matching_completed_finalizer(monkeypatch):
    """Admit native MRF source only while its exact finalizer receipt remains current."""

    from process import plan_summary
    from process import reference_family_archive as archive
    from tests.test_reference_family_archive_postgres import _create_live_family

    schema = "mrf_admission_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url(), poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    database = Database(engine=engine, session_factory=sessions)
    monkeypatch.setattr(receipt, "db", database)
    monkeypatch.setattr(plan_summary, "db", database)
    prepared = None
    prepare = partial(_prepare_native_mrf_source, sessions, archive, schema)
    monkeypatch.setattr(plan_summary, "ensure_database", _summary_database_ready)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gin"))
            await connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            await _create_live_family(connection, "mrf", schema)
            await connection.execute(
                text(
                    f'CREATE TABLE "{schema}".address_archive_v2 ('
                    "address_key uuid PRIMARY KEY, merged_into uuid, source_bits integer NOT NULL DEFAULT 0)"
                )
            )
            await _run_migration(connection, _MIGRATION, "upgrade")
            await _configure_summary_tables(connection, monkeypatch, schema)
        prepared, authority = await _assert_completed_source_admission(
            database,
            plan_summary,
            prepare,
            schema,
        )
        assert prepared.manifest.importer_id == "mrf"
        await _assert_completed_source_becomes_stale(engine, database, schema, prepare)
        await _assert_summary_failure_revokes_source_admission(monkeypatch, plan_summary, prepare, schema, authority)
    finally:
        await _cleanup_prepared_mrf_stage(sessions, archive, prepared, engine, schema)
