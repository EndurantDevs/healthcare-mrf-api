# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL checks for completion, stale inputs and crash reconciliation."""

import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from db.connection import Database
from process import mrf_publication_receipt as receipt
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process.network_legacy_membership_source import _read_aca_batch, read_aca_raw_fact_batch
from tests.reference_family_generation_fixture import install_source_generation_guards, native_reference_source
from tests.test_network_legacy_membership_source_postgres import _aca_binding, _approved_aca_pin, _write_aca_bindings
from tests.test_network_serving_schema_postgres import serving_schema as _registry_serving_schema
from tests.test_reference_family_result_generation_postgres import (
    _MRF_MIGRATION_PATH,
    _REFERENCE_MIGRATION_PATH,
    _database_url,
    _migration_module,
    _run_migration,
)
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft

_MIGRATION = Path(__file__).resolve().parents[1] / "alembic/versions/20260920170000_mrf_publication_receipt.py"


@pytest.fixture
async def serving_schema():
    """Share the explicitly selected reference DB with the unchanged registry fixture."""
    with pytest.MonkeyPatch.context() as scoped:
        if "NETWORK_REGISTRY_TEST_DSN" not in os.environ:
            scoped.setenv("NETWORK_REGISTRY_TEST_DSN", _database_url())
        async with asynccontextmanager(_registry_serving_schema.__wrapped__)() as fixture:
            yield fixture


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

    async def fail_final_indexes(session, schema_name, stage_name):
        call_counts[0] += 1
        assert call_counts[0] == 1 and stage_name.endswith("_refresh")
        await ensure_indexes(session, schema_name, stage_name)
        raise RuntimeError("summary failure")

    monkeypatch.setattr(plan_summary, "_ensure_summary_indexes", fail_final_indexes)
    with pytest.raises(RuntimeError, match="summary failure"):
        await plan_summary.rebuild_plan_search_summary(publication=(failed_attempt, authority, False))
    with pytest.raises(RuntimeError, match="completion is unavailable"):
        await archive_prepare()


async def _cleanup_prepared_mrf_stage(
    sessions, custody, prepared, engine, schema, is_shared_schema_new, is_shared_type_new, cleanup
):
    """Remove only the prepared stage and disposable schema created by this test."""

    if prepared is not None:
        async with sessions() as session, session.begin():
            await custody.retire(session, prepared.ownership)
    await cleanup.aclose()
    async with engine.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        if is_shared_type_new:
            await connection.execute(text('DROP TYPE IF EXISTS "mrf"."address_archive_geo_source" RESTRICT'))
        if is_shared_schema_new:
            await connection.execute(text('DROP SCHEMA IF EXISTS "mrf" RESTRICT'))
    await engine.dispose()


async def _prepare_native_mrf_source(sessions, archive, schema, custody, source_sessions):
    async def dependencies(_session):
        return {"plan-attributes": "b" * 64}

    return await archive.prepare_reference_family_archive_source(
        sessions,
        importer_id="mrf",
        schema_name=schema,
        dataset_id=uuid4(),
        source_metadata={"source_release": "synthetic"},
        on_prepared=custody.retain,
        source_copy=custody.source_copy,
        on_precreated=custody.precreate,
        source_sessions=source_sessions,
        dependency_factory=dependencies,
    )


async def _drop_mrf_source_role(sessions, role):
    """Revoke only the registered disposable principal's grants after its engine closes."""
    async with sessions.begin() as session:
        if await session.scalar(text("SELECT 1 FROM pg_roles WHERE rolname=:role"), {"role": role}):
            await session.execute(text(f'DROP OWNED BY "{role}"'))
            await session.execute(text(f'DROP ROLE "{role}"'))


@asynccontextmanager
async def _native_mrf_source_sessions(sessions, schema, database_url):
    """Read as a non-code-owner with native fence rights, never as the bootstrap superuser."""
    role = "mrf_source_" + uuid4().hex
    password = uuid4().hex
    source_engine = create_async_engine(
        make_url(database_url).set(username=role, password=password), poolclass=NullPool, hide_parameters=True
    )
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(_drop_mrf_source_role, sessions, role)
        cleanup.push_async_callback(source_engine.dispose)
        async with sessions.begin() as session:
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            try:
                await driver.execute(
                    f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS"
                )
            except Exception:
                raise RuntimeError("native source principal bootstrap failed") from None
            await session.execute(text(f'GRANT USAGE ON SCHEMA "{schema}","mrf" TO "{role}"'))
            await session.execute(text(f'GRANT SELECT,MAINTAIN ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"'))
            # Real summary/input replacement must inherit the same initial fence privileges.
            await session.execute(
                text(f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT SELECT,MAINTAIN ON TABLES TO "{role}"')
            )
        source_sessions = async_sessionmaker(source_engine, expire_on_commit=False)
        async with source_sessions.begin() as source_session:
            assert await source_session.scalar(text("SELECT current_user")) == role
            assert await source_session.scalar(text("SELECT session_user")) == role
            assert not await source_session.scalar(
                text("SELECT rolsuper OR rolcreaterole OR rolbypassrls FROM pg_roles WHERE rolname=current_user")
            )
            assert not await source_session.scalar(
                text(
                    "SELECT pg_has_role(current_user,typowner,'MEMBER') FROM pg_type "
                    "WHERE oid=to_regtype('mrf.address_archive_geo_source')"
                )
            )
            assert not await source_session.scalar(
                text("SELECT has_table_privilege(current_user,:table,'INSERT,UPDATE,DELETE,TRUNCATE')"),
                {"table": f'"{schema}".address_archive_v2'},
            )
        yield source_sessions


async def _create_canonical_address_source(connection, archive, schema):
    """Use the genuine declared enum, model indexes and installed source self edge."""
    from db.models import AddressArchiveV2
    from process import mrf_address_publication as canonical

    # The existing NPI canonical compiler preserves the shared source model's column order.
    spec = archive.ReferenceFamilySpec("npi", (AddressArchiveV2,))
    await archive._create_model_heaps(connection, spec, schema, create_indexes=False, ordinary_heaps=True)
    await archive._create_model_indexes(connection, spec, schema, create_constraints=True)
    await canonical.canonical_spatial_index(connection, schema, "address_archive_v2", canonical._qualified)
    await connection.execute(
        text(
            f'ALTER TABLE "{schema}".address_archive_v2 ADD FOREIGN KEY (merged_into) '
            f'REFERENCES "{schema}".address_archive_v2(address_key)'
        )
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

    async def fail_final_indexes(session, schema_name, stage_name):
        failure_call_counts[0] += 1
        assert failure_call_counts[0] == 1 and stage_name.endswith("_refresh")
        await ensure_indexes(session, schema_name, stage_name)
        raise RuntimeError("index failure")

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

    async def verify_input_write_fence(session, schema_name, stage_name):
        index_call_counts[0] += 1
        assert index_call_counts[0] == 1 and stage_name.endswith("_refresh")
        with pytest.raises(DBAPIError) as blocked:
            async with engine.begin() as writer:
                await writer.execute(text("SET LOCAL lock_timeout='100ms'"))
                await writer.execute(text(f'UPDATE "{schema}".plan_prices SET year=year'))
        assert blocked.value.orig.sqlstate == "55P03"
        await ensure_indexes(session, schema_name, stage_name)
        assert (
            await session.scalar(
                text("SELECT count(*) FROM pg_index WHERE indrelid=to_regclass(:stage) AND indisvalid AND indisready"),
                {"stage": f'"{schema_name}"."{stage_name}"'},
            )
            == 3
        )
        assert not await session.scalar(
            text(
                "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND relation=to_regclass(:live) "
                "AND mode='AccessExclusiveLock' AND granted)"
            ),
            {"live": f'"{schema_name}".plan_search_summary'},
        )

    with monkeypatch.context() as patch:
        patch.setattr(plan_summary, "_ensure_summary_indexes", verify_input_write_fence)
        assert await plan_summary.rebuild_plan_search_summary(publication=(recovery_attempt, authority, False)) == 0
    assert plan_summary._ensure_summary_indexes is ensure_indexes
    assert index_call_counts[0] == 1
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
        await install_source_generation_guards(connection, schema)


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
    custody = None
    cleanup = AsyncExitStack()
    is_shared_schema_new = is_shared_type_new = False
    monkeypatch.setattr(plan_summary, "ensure_database", _summary_database_ready)
    try:
        custody = await cleanup.enter_async_context(native_reference_source(sessions))
        async with engine.begin() as connection:
            await connection.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gin"))
            await connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            from db.models import AddressArchiveV2

            is_shared_schema_new = not await connection.scalar(text("SELECT to_regnamespace('mrf') IS NOT NULL"))
            is_shared_type_new = not await connection.scalar(
                text("SELECT to_regtype('mrf.address_archive_geo_source') IS NOT NULL")
            )
            if is_shared_schema_new:
                await connection.execute(text('CREATE SCHEMA "mrf"'))
            await connection.run_sync(
                lambda sync: AddressArchiveV2.__table__.c.geo_source.type.create(sync, checkfirst=True)
            )
            await _create_live_family(connection, "mrf", schema)
            await _create_canonical_address_source(connection, archive, schema)
            await _run_migration(connection, _MIGRATION, "upgrade")
            await _configure_summary_tables(connection, monkeypatch, schema)
        source_sessions = await cleanup.enter_async_context(
            _native_mrf_source_sessions(sessions, schema, _database_url())
        )
        prepare = partial(_prepare_native_mrf_source, sessions, archive, schema, custody, source_sessions)
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
        await _cleanup_prepared_mrf_stage(
            sessions, custody, prepared, engine, schema, is_shared_schema_new, is_shared_type_new, cleanup
        )


def _initial_batch_scopes(dataset_id, reader):
    """Declare three exact URL scopes over two issuers and one physical dataset."""
    scopes = []
    for issuer, label in ((7, "first"), (7, "second"), (8, "third")):
        scopes.append(
            {
                "network_membership": dict(
                    source_id=f"source-{label}",
                    release_id="release-one",
                    import_id="import-one",
                    issuer_id=issuer,
                    year=2026,
                    source_url=f"https://source.example.test/{label}",
                    alias_scope="medical",
                    reviewed_alias_sha256="a" * 64,
                    runtime_roles=[reader],
                ),
                "network_bindings": dict(
                    source_system="aca",
                    source_id=f"source-{label}",
                    dataset_schema=archive.reference_family_stage_schema(dataset_id),
                    dataset_id=str(dataset_id),
                    producer_id="producer-example",
                    edition_id="edition-one",
                    source_key_kind="hios_plan_id",
                ),
            }
        )
    return sorted(scopes, key=archive._canonical_json)


async def _seed_initial_batch(session, schema):
    """Retain all original offices even though the provider row has one address."""
    sites = [uuid4() for _ in range(4)]
    await session.execute(
        text(f'''INSERT INTO "{schema}".plan(plan_id,year,issuer_id,state) VALUES
      ('00007CA0000001',2026,7,'CA'),('00007CA0000002',2026,7,'CA'),('00008CA0000001',2026,8,'CA')''')
    )
    await session.execute(
        text(f'''INSERT INTO "{schema}".plan_networktier VALUES
      ('00007CA0000001','tier-42',7,2026,42),('00007CA0000002','tier-43',7,2026,43),('00008CA0000001','tier-44',8,2026,44)''')
    )
    await session.execute(
        text(f'''INSERT INTO "{schema}".plan_npi_raw(npi,checksum_network,network_tier,issuer_id,year,addresses) VALUES
      (1000000491,42,'tier-42',7,2026,ARRAY['{{"address":"one retained singleton"}}'::json]),
      (1000000491,43,'tier-43',7,2026,ARRAY['{{"address":"another singleton"}}'::json]),
      (1000000491,44,'tier-44',8,2026,ARRAY['{{"address":"third singleton"}}'::json])''')
    )
    evidence_records = [
        (1, 42, 7, "first", sites[0]),
        (2, 42, 7, "first", sites[1]),
        (3, 42, 7, "second", sites[1]),
        (4, 43, 7, "second", sites[2]),
        (5, 44, 8, "third", sites[3]),
    ]
    for key, network, issuer, label, site in evidence_records:
        await session.execute(
            text(f'''INSERT INTO "{schema}".mrf_address_evidence
          (evidence_checksum,npi,type,checksum,issuer_id,year,checksum_network,network_tier,import_id,source_url,source_record_id,address_key)
          VALUES(:key,1000000491,'primary',:key,:issuer,2026,:network,:tier,'import-one',:url,:record,:site)'''),
            dict(
                key=key,
                issuer=issuer,
                network=network,
                tier=f"tier-{network}",
                url=f"https://source.example.test/{label}",
                record=f"record-{key}",
                site=site,
            ),
        )
    for site in sites:
        await session.execute(
            text(
                f'INSERT INTO "{schema}".address_archive_v2(address_key,identity_key,source_bits) VALUES(:site,:identity,16)'
            ),
            dict(site=site, identity=str(site)),
        )
    return sites


async def _create_batch_fixture_storage(fixture, monkeypatch):
    from tests.test_reference_family_archive_postgres import _create_live_family

    async with fixture.engine.begin() as setup:
        await setup.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gin"))
        await setup.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        await _create_live_family(setup, "mrf", fixture.schema)
        await _create_canonical_address_source(setup, archive, fixture.schema)
        await _run_migration(setup, _MIGRATION, "upgrade")
        await _configure_summary_tables(setup, monkeypatch, fixture.schema)
        fixture.sites = await _seed_initial_batch(setup, fixture.schema)
        await setup.execute(
            text(
                f'CREATE TABLE "{fixture.schema}".capture_lease(id integer PRIMARY KEY,token uuid NOT NULL,dataset_id uuid)'
            )
        )
        await setup.execute(
            text(f'INSERT INTO "{fixture.schema}".capture_lease VALUES(1,:token,NULL)'), {"token": fixture.lease_token}
        )
        assert not await setup.scalar(text("SELECT to_regnamespace('hp_snapshot_retention')"))
        await setup.execute(
            text(f'CREATE ROLE "{fixture.owner}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT')
        )
        driver = (await setup.get_raw_connection()).driver_connection
        try:
            await driver.execute(
                f"CREATE ROLE \"{fixture.publisher}\" LOGIN PASSWORD '{fixture.publisher_password}' "
                "NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOBYPASSRLS"
            )
        except Exception:
            raise RuntimeError("native publisher principal bootstrap failed") from None
        await setup.execute(text(f'GRANT "{fixture.owner}" TO "{fixture.publisher}"'))
        await setup.execute(text(f'CREATE SCHEMA hp_snapshot_retention AUTHORIZATION "{fixture.owner}"'))
        fixture.has_retention = True
        dbname = await setup.scalar(text("SELECT current_database()"))
        await setup.execute(text(f'GRANT CREATE ON DATABASE "{dbname}" TO "{fixture.publisher}"'))
        await setup.execute(text(f'GRANT USAGE ON SCHEMA "{fixture.schema}",mrf TO "{fixture.publisher}"'))
        await setup.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.schema}" TO "{fixture.publisher}"'))
        await setup.execute(
            text(f'GRANT UPDATE(dataset_id) ON "{fixture.schema}".capture_lease TO "{fixture.publisher}"')
        )
        await setup.execute(
            text(
                f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{fixture.schema}" GRANT SELECT ON TABLES TO "{fixture.publisher}"'
            )
        )


async def _complete_batch_fixture_source(fixture, stack):
    from process import plan_summary

    fixture.source_sessions = await stack.enter_async_context(
        _native_mrf_source_sessions(
            fixture.sessions, fixture.schema, fixture.engine.url.render_as_string(hide_password=False)
        )
    )
    async with fixture.source_sessions.begin() as session:
        fixture.reader = await session.scalar(text("SELECT current_user"))
    attempt = await receipt.begin_publication(fixture.schema, "import-one")
    async with fixture.sessions.begin() as session:
        authority = await generation.publish_local_reference_family_generation(
            session, importer_id="mrf", schema_name=fixture.schema
        )
    await plan_summary.rebuild_plan_search_summary(publication=(attempt, authority, False))
    fixture.publisher_engine = create_async_engine(
        fixture.engine.url.set(username=fixture.publisher, password=fixture.publisher_password),
        poolclass=NullPool,
        hide_parameters=True,
    )
    fixture.publisher_sessions = async_sessionmaker(fixture.publisher_engine, expire_on_commit=False)
    fixture.custody = await stack.enter_async_context(
        native_reference_source(fixture.publisher_sessions, readers=(fixture.reader,), owner=fixture.owner)
    )
    fixture.scopes = _initial_batch_scopes(fixture.dataset_id, fixture.reader)


async def _retire_batch_fixture(fixture):
    from tests.test_mrf_result_archive_postgres import _retire_canonical_type

    if fixture.publisher_engine is not None:
        await fixture.publisher_engine.dispose()
    async with fixture.engine.begin() as setup:
        await setup.execute(text(f'DROP SCHEMA IF EXISTS "{fixture.schema}" CASCADE'))
        if fixture.has_retention:
            await setup.execute(text("DROP SCHEMA hp_snapshot_retention RESTRICT"))
        for role in (fixture.publisher, fixture.owner):
            if await setup.scalar(text("SELECT 1 FROM pg_roles WHERE rolname=:role"), {"role": role}):
                await setup.execute(text(f'DROP OWNED BY "{role}"'))
                await setup.execute(text(f'DROP ROLE "{role}"'))
    if fixture.canonical_type is not None:
        await _retire_canonical_type(fixture.sessions, fixture.canonical_type)


@asynccontextmanager
async def _native_initial_batch_source(serving_schema, monkeypatch):
    """Use genuine ordinary Publisher/source Reader and exact owned clone cleanup."""
    from process import plan_summary
    from tests.test_mrf_result_archive_postgres import _provision_canonical_type

    connection, control, engine = serving_schema
    token = uuid4().hex
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    fixture = SimpleNamespace(
        connection=connection,
        control=control,
        engine=engine,
        sessions=sessions,
        schema="batch_source_" + token,
        owner="batch_owner_" + token,
        publisher="batch_publisher_" + token,
        publisher_password=uuid4().hex,
        dataset_id=uuid4(),
        lease_token=uuid4(),
        publisher_engine=None,
        has_retention=False,
        canonical_type=None,
    )
    database = Database(engine=engine, session_factory=sessions)
    monkeypatch.setattr(receipt, "db", database)
    monkeypatch.setattr(plan_summary, "db", database)
    monkeypatch.setattr(plan_summary, "ensure_database", _summary_database_ready)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", fixture.schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    try:
        fixture.canonical_type = await _provision_canonical_type(sessions)
        await _create_batch_fixture_storage(fixture, monkeypatch)
        async with AsyncExitStack() as stack:
            await _complete_batch_fixture_source(fixture, stack)
            yield fixture
    finally:
        await _retire_batch_fixture(fixture)


@asynccontextmanager
async def _ordinary_summary_owner(fixture, monkeypatch):
    """Run the existing publisher as an ordinary original owner, with disposable credentials."""
    from process import plan_summary

    role = "summary_writer_" + uuid4().hex
    password = uuid4().hex
    engine = create_async_engine(
        fixture.engine.url.set(username=role, password=password), poolclass=NullPool, hide_parameters=True
    )
    async with fixture.engine.begin() as setup:
        original_owner = await setup.scalar(text("SELECT current_user"))
        driver = (await setup.get_raw_connection()).driver_connection
        try:
            await driver.execute(
                f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}' "
                "NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS"
            )
        except Exception:
            raise RuntimeError("native summary principal bootstrap failed") from None
    try:
        async with fixture.engine.begin() as setup:
            await setup.execute(text(f'GRANT USAGE ON SCHEMA mrf TO "{role}"'))
            await setup.execute(text(f'ALTER SCHEMA "{fixture.schema}" OWNER TO "{role}"'))
            tables = await setup.scalars(
                text("SELECT tablename FROM pg_tables WHERE schemaname=:schema"), {"schema": fixture.schema}
            )
            for name in tables:
                await setup.execute(text(f'ALTER TABLE "{fixture.schema}"."{name}" OWNER TO "{role}"'))
        database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
        with monkeypatch.context() as patch:
            patch.setattr(receipt, "db", database)
            patch.setattr(plan_summary, "db", database)
            async with database.transaction() as session:
                assert await session.scalar(
                    text("SELECT session_user=current_user AND current_user=:role"), {"role": role}
                )
                assert not await session.scalar(
                    text(
                        "SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolbypassrls FROM pg_roles WHERE rolname=current_user"
                    )
                )
            yield database
    finally:
        await engine.dispose()
        async with fixture.engine.begin() as setup:
            await setup.execute(text(f'REASSIGN OWNED BY "{role}" TO "{original_owner}"'))
            await setup.execute(text(f'DROP OWNED BY "{role}"'))
            await setup.execute(text(f'DROP ROLE "{role}"'))


async def _assert_summary_hash_concurrency(fixture, session, capture):
    """Use separate genuine connections while the real address hash transaction is open."""
    schema = fixture.schema
    assert not await session.scalar(
        text(
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND relation=to_regclass(:live) "
            "AND mode='AccessExclusiveLock' AND granted)"
        ),
        {"live": f'"{schema}".plan_search_summary'},
    )
    async with fixture.source_sessions.begin() as reader:
        await reader.execute(text("SET LOCAL lock_timeout='100ms'"))
        assert await reader.scalar(text(f'SELECT count(*) FROM "{schema}".plan_search_summary')) == 3
        assert await reader.scalar(text(f'SELECT state FROM "{schema}".{receipt.TABLE}')) == "pending"
        # Keep the genuine reader's ACCESS SHARE lock for the entire real content hash.
        content = await capture(session, schema, receipt.qualified)
        assert await reader.scalar(text(f'SELECT count(*) FROM "{schema}".plan_search_summary')) == 3
    assert content["archive_oid"] and all(
        table_counts["uncovered"] == 0 and len(table_counts["sha256"]) == 64
        for table_counts in content["tables"].values()
    )
    assert {name: table_counts["rows"] for name, table_counts in content["tables"].items()} == {
        "mrf_address": 0,
        "mrf_address_evidence": 5,
    }
    for name, column in (
        ("plan_prices", "year"),
        ("mrf_address_evidence", "import_id"),
        ("address_archive_v2", "source_bits"),
    ):
        with pytest.raises(DBAPIError) as blocked:
            async with fixture.engine.begin() as writer:
                await writer.execute(text("SET LOCAL lock_timeout='100ms'"))
                await writer.execute(text(f'UPDATE "{schema}"."{name}" SET "{column}"="{column}" WHERE FALSE'))
        assert blocked.value.orig.sqlstate == "55P03"
    return content


async def _prepare_summary_attempt(database, schema):
    """Retain the existing primary-key predecessor and begin one new source claim."""
    async with database.transaction() as session:
        # This legitimate historical primary-key name must not collide with the next private stage.
        await session.execute(
            text(
                f'ALTER TABLE "{schema}".plan_search_summary RENAME CONSTRAINT '
                "plan_search_summary_pkey TO plan_search_summary_refresh_pkey"
            )
        )
        old_oid = await session.scalar(text(f"SELECT '{schema}.plan_search_summary'::regclass::oid"))
        authority = await generation.read_reference_family_result_generation_authority(
            session, importer_id="mrf", schema_name=schema
        )
    attempt = await receipt.begin_publication(schema, "import-one")
    return old_oid, authority, attempt


async def _assert_summary_outcome(fixture, failure, old_oid, observed_by_name):
    """Prove rollback or full reader admission against the exact committed summary OID."""
    schema = fixture.schema
    async with fixture.source_sessions.begin() as reader:
        await reader.execute(text("SET LOCAL lock_timeout='100ms'"))
        assert await reader.scalar(text(f'SELECT count(*) FROM "{schema}".plan_search_summary')) == 3
        live_oid = await reader.scalar(text(f"SELECT '{schema}.plan_search_summary'::regclass::oid"))
        if failure:
            assert live_oid == old_oid
            with pytest.raises(RuntimeError, match="completion is unavailable"):
                await receipt.require_completed_publication(reader, schema)
            assert await reader.scalar(text(f'SELECT state FROM "{schema}".{receipt.TABLE}')) == "pending"
        else:
            assert live_oid == observed_by_name["stage_oid"] != old_oid
            await archive._lock_source_family(reader, archive.reference_family_spec("mrf", canonical=True), schema)
            completed = await receipt.require_completed_publication(reader, schema)
            assert completed["summary_oid"] == live_oid and completed["address_content"] == observed_by_name["content"]
            assert completed["address_resolution_performed"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "rotation", "index"])
async def test_summary_hash_precedes_live_lock_and_receipt_swaps_atomically(serving_schema, monkeypatch, failure):
    """Preserve live reads during real hashing and reject every unfinished or failed swap."""
    from process import plan_summary

    async with (
        _native_initial_batch_source(serving_schema, monkeypatch) as fixture,
        _ordinary_summary_owner(fixture, monkeypatch) as database,
    ):
        schema = fixture.schema
        old_oid, authority, attempt = await _prepare_summary_attempt(database, schema)
        capture, preserve = receipt.capture_address_content, plan_summary._preserve_summary_reads
        publish_indexes = plan_summary._publish_summary_index_names
        observed_by_name = {}

        async def checked_hash(session, schema_name, qualified):
            assert schema_name == schema and qualified is receipt.qualified
            observed_by_name["content"] = await _assert_summary_hash_concurrency(fixture, session, capture)
            return observed_by_name["content"]

        async def checked_preserve(session, schema_name, stage_name):
            assert observed_by_name["content"] and stage_name == "plan_search_summary_refresh"
            stage_oid = await session.scalar(
                text("SELECT to_regclass(:name)::oid"), {"name": f'"{schema}"."{stage_name}"'}
            )
            current = (await session.execute(text(f'SELECT * FROM "{schema}".{receipt.TABLE}'))).mappings().one()
            assert current["state"] == "complete" and current["summary_oid"] == stage_oid != old_oid
            observed_by_name["stage_oid"] = stage_oid
            with pytest.raises(RuntimeError, match="completion summary identity differs"):
                await receipt.require_completed_publication(session, schema)
            await preserve(session, schema_name, stage_name)
            if failure == "rotation":
                await session.execute(text(f'ALTER TABLE "{schema}".plan_search_summary RENAME TO plan'))

        async def checked_indexes(session, schema_name, stage_name):
            await publish_indexes(session, schema_name, stage_name)
            if failure == "index":
                await session.execute(
                    text(
                        f'ALTER INDEX "{schema}".plan_search_summary_issuer_year_idx '
                        "RENAME TO plan_search_summary_state_year_idx"
                    )
                )

        with monkeypatch.context() as patch:
            patch.setattr(receipt, "capture_address_content", checked_hash)
            patch.setattr(plan_summary, "_preserve_summary_reads", checked_preserve)
            patch.setattr(plan_summary, "_publish_summary_index_names", checked_indexes)
            if failure:
                with pytest.raises(DBAPIError):
                    await plan_summary.rebuild_plan_search_summary(publication=(attempt, authority, False))
            else:
                assert await plan_summary.rebuild_plan_search_summary(publication=(attempt, authority, False)) == 3
        assert observed_by_name["stage_oid"]
        await _assert_summary_outcome(fixture, failure, old_oid, observed_by_name)


async def _prepare_initial_batch(fixture, *, failure=None, on_admitted=None):
    """Capture once and place admission plus retention in its original transaction."""
    from process import registry_aca_source_recipe as producer

    dataset_id = fixture.dataset_id if failure is None else uuid4()
    scopes = _initial_batch_scopes(dataset_id, fixture.reader)
    copied, admitted = [], []

    async def metadata(session):
        return await producer.capture_registry_aca_source_batch_metadata(
            session, scopes, schema=fixture.schema, runtime_roles=(fixture.reader,), dataset_id=dataset_id
        )

    async def copy_rows(*args, **kwargs):
        copied.append(kwargs["table_name"])
        return await archive.native_copy_projection(*args, **kwargs)

    async def retain(session, prepared):
        recipes = await producer.admit_registry_aca_source_batch(session, prepared, runtime_roles=(fixture.reader,))
        admitted.extend(recipes)
        if on_admitted is not None:
            await on_admitted(session, recipes)
        await fixture.custody.retain(session, prepared)
        updated = await session.scalar(
            text(
                f'UPDATE "{fixture.schema}".capture_lease SET dataset_id=:dataset WHERE id=1 AND token=:token AND dataset_id IS NULL RETURNING dataset_id'
            ),
            {"dataset": prepared.ownership.dataset_id, "token": uuid4() if failure == "cas" else fixture.lease_token},
        )
        if updated != prepared.ownership.dataset_id:
            raise RuntimeError("synthetic lease compare failed")
        if failure == "cancel":
            raise asyncio.CancelledError

    async def dependency(_session):
        return {"plan-attributes": "b" * 64}

    prepared = await archive.prepare_reference_family_archive_source(
        fixture.publisher_sessions,
        importer_id="mrf",
        schema_name=fixture.schema,
        source_metadata=None,
        dataset_id=dataset_id,
        source_metadata_factory=metadata,
        source_sessions=fixture.source_sessions,
        source_copy=archive.ReferenceFamilySourceCopy(copy_rows, 1_000_000, 30),
        on_precreated=fixture.custody.precreate,
        on_prepared=retain,
        dependency_factory=dependency,
    )
    return prepared, tuple(admitted), copied


async def _approve_batch_fixture_bindings(fixture, recipes):
    actor = _actor()
    networks = [await _draft(fixture.engine, fixture.control, _create("network"), actor) for _ in (7, 8)]
    binding_records = []
    for recipe in recipes:
        issuer = recipe.source_pin.issuer_id
        for index in (1, 2) if issuer == 7 else (1,):
            binding = _aca_binding(recipe.binding_coordinates, networks[0 if issuer == 7 else 1]["record_id"], index)
            if issuer == 8:
                binding.update(
                    source_key="00008CA0000001",
                    source_scope_json={
                        "issuer_id": "00008",
                        "state": "CA",
                        "plan_year": 2026,
                        "plan_id": "00008CA0000001",
                        "checksum_network": 44,
                    },
                )
            binding_records.append(binding)
    bindings = await _write_aca_bindings(fixture.connection, fixture.control, actor, binding_records)
    await _approve(
        fixture.connection,
        fixture.control,
        await _command(fixture.connection, fixture.control, *networks, *bindings["records"]),
        actor,
    )
    return await _approved_aca_pin(fixture.connection, fixture.control)


async def _assert_batch_fixture_rows(fixture, recipes):
    from process.network_legacy_membership_source import _require_archive

    approved = await _approve_batch_fixture_bindings(fixture, recipes)
    observed_counts = []
    async with fixture.connection.transaction(isolation="repeatable_read"):
        for recipe in recipes:
            await _require_archive(fixture.connection, recipe.source_pin)
            batch = await read_aca_raw_fact_batch(
                fixture.connection,
                recipe.source_pin,
                registry_schema=fixture.control,
                approved_source=approved,
                binding_coordinates=recipe.binding_coordinates,
                approved_only=True,
            )
            assert not batch.unresolved_rows and not batch.unresolved_source_rows
            observed_counts.append(
                (recipe.source_pin.source_id, batch.source_rows, batch.membership_rows, batch.accepted_source_rows)
            )
    assert sorted(observed_counts) == [("source-first", 1, 2, 1), ("source-second", 2, 2, 2), ("source-third", 1, 1, 1)]


async def _assert_batch_tamper_refusal(fixture, prepared, recipes):
    from process.network_legacy_membership_source import _require_archive

    for damage in ("stamp", "role", "oid"):
        transaction = fixture.connection.transaction()
        await transaction.start()
        try:
            schema = prepared.ownership.schema_name
            statement = {
                "stamp": f"COMMENT ON SCHEMA \"{schema}\" IS 'unrelated'",
                "role": f'GRANT UPDATE ON "{schema}".mrf_address_evidence TO "{fixture.reader}"',
                "oid": f'ALTER TABLE "{schema}".mrf_address_evidence RENAME TO changed_evidence',
            }[damage]
            await fixture.connection.execute(statement)
            expected = asyncpg.UndefinedTableError if damage == "oid" else ValueError
            with pytest.raises(expected):
                await _require_archive(fixture.connection, recipes[0].source_pin)
        finally:
            await transaction.rollback()


@pytest.mark.asyncio
async def test_native_initial_batch_one_clone_exact_scopes_and_complete_raw_accounting(serving_schema, monkeypatch):
    from process import registry_aca_source_recipe as producer
    from process.registry_source_recipe_store import canonical_registry_source_recipes, decode_registry_source_recipes

    async with _native_initial_batch_source(serving_schema, monkeypatch) as fixture:
        prepared, recipes, copied = await _prepare_initial_batch(fixture)
        assert len(recipes) == 3
        assert sorted(copied) == sorted(archive.reference_family_spec("mrf", canonical=True).table_names)
        assert len(copied) == len(set(copied)) == 14
        assert (
            await fixture.connection.fetchval(f'SELECT dataset_id FROM "{fixture.schema}".capture_lease WHERE id=1')
            == prepared.ownership.dataset_id
        )
        assert decode_registry_source_recipes(canonical_registry_source_recipes(recipes)) == recipes
        coverage = prepared.manifest.source_metadata[producer._BATCH_KEY]["coverage"]
        assert sum(group["raw_rows"] for group in coverage) == 3
        assert sum(url["raw_rows"] for group in coverage for url in group["urls"]) == 4
        assert sum(url["evidence_rows"] for group in coverage for url in group["urls"]) == 5
        await _assert_batch_fixture_rows(fixture, recipes)
        async with fixture.publisher_sessions.begin() as session:
            with pytest.raises(ValueError):
                await producer.require_registry_aca_source_recipe(session, prepared, runtime_roles=(fixture.reader,))
            assert (
                await producer.require_registry_aca_source_batch(session, prepared, runtime_roles=(fixture.reader,))
                == recipes
            )
        await _assert_batch_tamper_refusal(fixture, prepared, recipes)


@pytest.mark.parametrize("failure", ["cas", "cancel"])
@pytest.mark.asyncio
async def test_native_initial_batch_admission_and_lease_failure_roll_back_whole_clone(
    serving_schema, monkeypatch, failure
):
    async with (
        _native_initial_batch_source(serving_schema, monkeypatch) as fixture,
        _native_batch_notification_store(fixture) as store,
    ):
        with pytest.raises(RuntimeError if failure == "cas" else asyncio.CancelledError):
            await _prepare_batch_notification_fixture(fixture, store, failure)
        assert len(fixture.custody.precreated) == 1
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{fixture.control}".registry_aca_source_notification'
            )
            == 0
        )
        assert (
            await fixture.connection.fetchval(f'SELECT dataset_id FROM "{fixture.schema}".capture_lease WHERE id=1')
            is None
        )
        for dataset_id, (_transaction, ownership) in fixture.custody.precreated.items():
            assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", ownership.schema_name) is None
            assert dataset_id in fixture.custody.closed


@pytest.mark.parametrize("damage", ["third_url", "uncovered_raw", "foreign_import", "foreign_issuer", "wrong_tier"])
@pytest.mark.asyncio
async def test_native_initial_batch_scope_union_refuses_incomplete_or_foreign_coverage(
    serving_schema, monkeypatch, damage
):
    from process import registry_aca_source_recipe as producer

    async with _native_initial_batch_source(serving_schema, monkeypatch) as fixture:
        async with fixture.sessions.begin() as session:
            if damage == "third_url":
                await session.execute(
                    text(f'''INSERT INTO "{fixture.schema}".mrf_address_evidence SELECT
                  (jsonb_populate_record(NULL::"{fixture.schema}".mrf_address_evidence,to_jsonb(e)||jsonb_build_object('evidence_checksum',99,'source_url','https://source.example.test/omitted'))).*
                  FROM "{fixture.schema}".mrf_address_evidence e WHERE evidence_checksum=1''')
                )
            elif damage == "uncovered_raw":
                await session.execute(
                    text(
                        f'''INSERT INTO "{fixture.schema}".plan_npi_raw(npi,checksum_network,network_tier,issuer_id,year) VALUES(1000000491,45,'tier-45',7,2026)'''
                    )
                )
            else:
                assignment = {
                    "foreign_import": "import_id='foreign'",
                    "foreign_issuer": "issuer_id=99",
                    "wrong_tier": "network_tier='foreign'",
                }[damage]
                await session.execute(
                    text(f'UPDATE "{fixture.schema}".mrf_address_evidence SET {assignment} WHERE evidence_checksum=4')
                )
            with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
                await producer._capture_batch_coverage(session, fixture.schema, fixture.scopes)


async def _capture_batch_fixture_refresh(fixture):
    from process import plan_summary
    from process import registry_aca_global_epoch as global_epoch

    attempt = await receipt.begin_publication(fixture.schema, "import-two")
    async with fixture.sessions.begin() as session:
        await session.execute(text(f"UPDATE \"{fixture.schema}\".mrf_address_evidence SET import_id='import-two'"))
        authority = await generation.publish_local_reference_family_generation(
            session, importer_id="mrf", schema_name=fixture.schema
        )
    await plan_summary.rebuild_plan_search_summary(publication=(attempt, authority, False))
    publication_map = {
        "contract": global_epoch.PUBLICATION_CONTRACT,
        "importer_id": "mrf",
        "serving_generation": authority.serving_generation.as_dict(),
    }
    epochs = []

    async def metadata(session):
        return await global_epoch.capture_registry_aca_global_metadata(
            session, publication_map, schema=fixture.schema, runtime_roles=(fixture.reader,)
        )

    async def retain(session, prepared):
        epochs.append(
            await global_epoch.admit_registry_aca_global_epoch(session, prepared, runtime_roles=(fixture.reader,))
        )
        await fixture.custody.retain(session, prepared)

    async def dependency(_session):
        return {"plan-attributes": "b" * 64}

    await archive.prepare_reference_family_archive_source(
        fixture.publisher_sessions,
        importer_id="mrf",
        schema_name=fixture.schema,
        source_metadata=None,
        dataset_id=uuid4(),
        source_metadata_factory=metadata,
        source_sessions=fixture.source_sessions,
        source_copy=fixture.custody.source_copy,
        on_precreated=fixture.custody.precreate,
        on_prepared=retain,
        dependency_factory=dependency,
    )
    assert len(epochs) == 1
    return epochs[0]


@pytest.mark.asyncio
async def test_native_initial_batch_all_anchors_reuse_one_genuine_global_refresh(serving_schema, monkeypatch):
    from process.network_legacy_membership_source import _require_archive, with_aca_global_epoch
    from process.registry_source_recipe_store import RegistrySourceMembershipRecipe

    async with _native_initial_batch_source(serving_schema, monkeypatch) as fixture:
        original, recipes, _copied = await _prepare_initial_batch(fixture)
        approved = await _approve_batch_fixture_bindings(fixture, recipes)
        epoch = await _capture_batch_fixture_refresh(fixture)
        assert epoch.prepared.ownership.dataset_id != original.ownership.dataset_id
        assert (
            epoch.prepared.manifest.source_serving_generation.origin_lineage_id
            == original.manifest.source_serving_generation.origin_lineage_id
        )
        assert (
            epoch.prepared.manifest.source_serving_generation.origin_generation
            > original.manifest.source_serving_generation.origin_generation
        )
        assert (
            epoch.prepared.manifest.source_metadata["registry_aca_global_epoch"]["completed_publication"]["import_id"]
            == "import-two"
        )
        refreshed_recipes = tuple(
            RegistrySourceMembershipRecipe(with_aca_global_epoch(recipe.source_pin, epoch), recipe.binding_coordinates)
            for recipe in recipes
        )
        assert len(fixture.custody.precreated) == 2
        async with fixture.connection.transaction(isolation="repeatable_read"):
            for anchor, recipe in zip(recipes, refreshed_recipes, strict=True):
                assert recipe.binding_coordinates == anchor.binding_coordinates
                await _require_archive(fixture.connection, recipe.source_pin)
                batch = await read_aca_raw_fact_batch(
                    fixture.connection,
                    recipe.source_pin,
                    registry_schema=fixture.control,
                    approved_source=approved,
                    binding_coordinates=recipe.binding_coordinates,
                    approved_only=True,
                )
                assert not batch.unresolved_rows and not batch.unresolved_source_rows
                assert batch.source_rows == batch.accepted_source_rows == (2 if recipe.source_pin.issuer_id == 7 else 1)
                assert batch.membership_rows == (4 if recipe.source_pin.issuer_id == 7 else 1)


@pytest.mark.asyncio
async def test_native_initial_batch_url_inventory_has_fixed_collation(serving_schema, monkeypatch):
    from process import registry_aca_source_recipe as producer

    async with _native_initial_batch_source(serving_schema, monkeypatch) as fixture:
        async with fixture.sessions.begin() as session:
            await session.execute(
                text(f'''CREATE COLLATION "{fixture.schema}".url_order (provider=icu,locale='en-US')''')
            )
            await session.execute(
                text(f'''ALTER TABLE "{fixture.schema}".mrf_address_evidence
                  ALTER COLUMN source_url TYPE text COLLATE "{fixture.schema}".url_order''')
            )
            scope_by_source_id = {scope["network_membership"]["source_id"]: scope for scope in fixture.scopes}
            for label, path in (("first", "B"), ("second", "a")):
                await session.execute(
                    text(
                        f'''UPDATE "{fixture.schema}".mrf_address_evidence SET source_url=:new WHERE source_url=:old'''
                    ),
                    {"old": f"https://source.example.test/{label}", "new": f"https://source.example.test/{path}"},
                )
                scope_by_source_id[f"source-{label}"]["network_membership"]["source_url"] = (
                    f"https://source.example.test/{path}"
                )
            with monkeypatch.context() as scoped:
                scoped.setattr(
                    producer, "_BATCH_COVERAGE_SQL", producer._BATCH_COVERAGE_SQL.replace(' COLLATE "C"', "")
                )
                with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
                    await producer._capture_batch_coverage(session, fixture.schema, fixture.scopes)
            coverage = await producer._capture_batch_coverage(session, fixture.schema, fixture.scopes)
            group = next(group for group in coverage if group["issuer_id"] == 7)
            assert [entry["source_url"] for entry in group["urls"]] == [
                "https://source.example.test/B",
                "https://source.example.test/a",
            ]


async def _seed_verified_batch_offices(session, schema, *, seed):
    """Replace synthetic locations with genuine native canonical office identities."""
    import ptg2_address_canon

    from db.models import AddressArchiveV2

    previous_sites = await seed(session, schema)
    addresses = [("123 Example Street", f"Suite {unit}", "Sample City", "CA", "90210", "US") for unit in (2, 3, 4, 5)]
    canonical_addresses = ptg2_address_canon.canonicalize_batch(addresses)
    sites = []
    for previous, address, canonical in zip(previous_sites, addresses, canonical_addresses, strict=True):
        fields_by_name = {
            name: column_value for name, column_value in canonical.items() if name in AddressArchiveV2.__table__.columns
        }
        fields_by_name.update(identity_version=2, precision="street", source_bits=16, merged_into=None)
        fields_by_name["address_key"], fields_by_name["premise_key"] = (
            UUID(fields_by_name["address_key"]),
            UUID(fields_by_name["premise_key"]),
        )
        assignments = ",".join(f'"{name}"=:{name}' for name in fields_by_name)
        await session.execute(
            text(f'UPDATE "{schema}".address_archive_v2 SET {assignments} WHERE address_key=:previous'),
            fields_by_name | {"previous": previous},
        )
        await session.execute(
            text(f'''UPDATE "{schema}".mrf_address_evidence SET address_key=:site,
              first_line=:first,second_line=:second,city_name=:city,state_name=:state,postal_code=:zip,country_code=:country
              WHERE address_key=:previous'''),
            dict(zip(("first", "second", "city", "state", "zip", "country"), address))
            | {"site": fields_by_name["address_key"], "previous": previous},
        )
        sites.append(fields_by_name["address_key"])
    return sites


async def _create_verified_office_storage(connection, schema):
    """Create only isolated model tables inside the caller's rollback scope."""
    from sqlalchemy.dialects.postgresql import dialect
    from sqlalchemy.schema import CreateTable

    from db.models import EntityAddressUnified

    await connection.execute(f'CREATE SCHEMA "{schema}"')
    table = EntityAddressUnified.__table__.to_metadata(MetaData(), schema=schema)
    await connection.execute(str(CreateTable(table).compile(dialect=dialect())))
    await connection.execute(f'''CREATE TABLE "{schema}".provider_location_binding(
      provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
      location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id))''')


async def _verified_copy_target(fixture, approved, stage, address_source):
    """Register the exact stage and address generations for the native COPY path."""
    from process.network_membership_candidate_lifecycle import create_network_candidate
    from process.network_membership_copy import MembershipCopyTarget

    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    await create_network_candidate(
        fixture.connection,
        copy_target,
        source_generations={
            "unified_address": address_source.generation_id,
            "custom_membership": approved.generation_id,
        }
        | {f"aca-{index}": generation_id for index, generation_id in enumerate(stage.source_generations)},
        approved_custom_revision=approved.approved_revision,
        expected_head=0,
        expected_rows=stage.membership_rows,
        control_schema=fixture.control,
    )
    return copy_target


async def _verified_office_route(fixture, recipes, approved, stage, monkeypatch):
    """Compare real carry, office pinning and candidate COPY with full-check replay."""
    from unittest.mock import AsyncMock

    from process import registry_aca_source_recipe as producer
    from process.network_address_projection import PinnedAddressSource
    from process.registry_aca_address_carry import prepare_aca_address_carry
    from process.registry_initial_source_composition import _office_summary

    connection = fixture.connection
    schema = "verified_offices_" + uuid4().hex
    address_source = PinnedAddressSource(schema, "entity_address_unified", "d" * 64)
    transaction = connection.transaction()
    await transaction.start()
    try:
        await _create_verified_office_storage(connection, schema)
        carried = await prepare_aca_address_carry(connection, recipes, approved, fixture.control, schema, writing=True)
        with monkeypatch.context() as unoptimized:
            unoptimized.setattr(producer, "prepare_registry_aca_batch_read", AsyncMock(return_value=None))
            replayed = await prepare_aca_address_carry(connection, recipes, approved, fixture.control, schema)
        assert carried == replayed and (carried.pages, carried.rows) == (3, 5)
        await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{fixture.owner}"')
        await connection.execute(f'ALTER TABLE "{schema}".entity_address_unified OWNER TO "{fixture.owner}"')
        await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{fixture.reader}"')
        await connection.execute(f'GRANT SELECT ON "{schema}".entity_address_unified TO "{fixture.reader}"')
        copy_target = await _verified_copy_target(fixture, approved, stage, address_source)
        with monkeypatch.context() as unoptimized:
            unoptimized.setattr(producer, "prepare_registry_aca_batch_read", AsyncMock(return_value=None))
            before = await _office_summary(
                connection, recipes, approved, address_source, (fixture.reader,), fixture.control
            )
        after = await _office_summary(
            connection, recipes, approved, address_source, (fixture.reader,), fixture.control, copy_target
        )
        assert before == after and (after["office_rows"], after["page_count"]) == (5, 3)
        carried_rows = await connection.fetch(
            f'SELECT * FROM "{schema}".provider_location_binding ORDER BY location_id'
        )
        copied_rows = await connection.fetch(
            f'SELECT * FROM "{copy_target.schema_name}".provider_location_binding ORDER BY location_id'
        )
        assert copied_rows == carried_rows and len(copied_rows) == 4
    finally:
        await transaction.rollback()
    assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
    assert await connection.fetchval("SELECT to_regnamespace($1)", copy_target.schema_name) is None


@pytest.mark.asyncio
async def test_native_initial_batch_verified_pages_keep_exact_offices_and_stage_hashes(serving_schema, monkeypatch):
    from unittest.mock import AsyncMock

    from process import registry_aca_source_recipe as producer
    from process.network_initial_source_office_bindings import _verify_batch
    from process.registry_source_recipe_composition import stage_registry_source_recipes

    monkeypatch.setattr(
        __name__ + "._seed_initial_batch", partial(_seed_verified_batch_offices, seed=_seed_initial_batch)
    )
    async with _native_initial_batch_source(serving_schema, monkeypatch) as fixture:
        _prepared, recipes, _copied = await _prepare_initial_batch(fixture)
        approved = await _approve_batch_fixture_bindings(fixture, recipes)
        async with fixture.connection.transaction(isolation="repeatable_read"):
            recipe = recipes[0]
            baseline = await read_aca_raw_fact_batch(
                fixture.connection,
                recipe.source_pin,
                registry_schema=fixture.control,
                approved_source=approved,
                binding_coordinates=recipe.binding_coordinates,
                approved_only=True,
            )
            verified = await producer.prepare_registry_aca_batch_read(fixture.connection, recipe.source_pin)
            async with fixture.connection.transaction():
                reused = await _read_aca_batch(
                    fixture.connection,
                    recipe.source_pin,
                    fixture.control,
                    approved,
                    recipe.binding_coordinates,
                    True,
                    (None, 1000, None, True),
                    verified,
                )
                assert reused == baseline and reused.verified_source is verified
                assert reused.input_bytes == baseline.input_bytes and reused.generation_id == baseline.generation_id
                await _verify_batch(fixture.connection, reused, fixture.control, None)
            with monkeypatch.context() as unoptimized:
                unoptimized.setattr(producer, "prepare_registry_aca_batch_read", AsyncMock(return_value=None))
                before = await stage_registry_source_recipes(
                    fixture.connection, recipes, approved, request_id=uuid4(), control_schema=fixture.control
                )
            after = await stage_registry_source_recipes(
                fixture.connection, recipes, approved, request_id=uuid4(), control_schema=fixture.control
            )
            assert before.generation_sha256 == after.generation_sha256
            assert before.source_generations == after.source_generations
            assert before.membership_rows == after.membership_rows
            assert before.raw_source_rows == after.raw_source_rows == 4
            assert await fixture.connection.fetch(
                f'SELECT * FROM pg_temp."{before.table_name}" ORDER BY ordinal'
            ) == await fixture.connection.fetch(f'SELECT * FROM pg_temp."{after.table_name}" ORDER BY ordinal')
            await _verified_office_route(fixture, recipes, approved, after, monkeypatch)


@pytest.mark.asyncio
async def test_native_initial_batch_verified_pages_refuse_reused_connection_transaction(
    serving_schema, monkeypatch, subtests
):
    from process import registry_aca_source_recipe as producer

    async with _native_initial_batch_source(serving_schema, monkeypatch) as fixture:
        _prepared, recipes, _copied = await _prepare_initial_batch(fixture)
        for end in ("commit", "rollback"):
            with subtests.test(end=end):
                await _assert_batch_transaction_expired(fixture, recipes, end, producer)


async def _assert_batch_transaction_expired(fixture, recipes, end, producer):
    first = fixture.connection.transaction(isolation="repeatable_read", readonly=True)
    await first.start()
    try:
        verified = await producer.prepare_registry_aca_batch_read(fixture.connection, recipes[0].source_pin)
    finally:
        await getattr(first, end)()
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
            await producer.require_registry_aca_batch_read(fixture.connection, recipes[0].source_pin, verified)


@pytest.mark.asyncio
async def test_native_initial_batch_verified_pages_refuse_authority_drift(serving_schema, monkeypatch, subtests):
    from process import registry_aca_source_recipe as producer

    async with _native_initial_batch_source(serving_schema, monkeypatch) as fixture:
        prepared, recipes, _copied = await _prepare_initial_batch(fixture)
        for damage in ("stamp", "actor", "privilege", "oid", "metadata", "source"):
            with subtests.test(damage=damage):
                await _assert_verified_batch_drift(fixture, prepared, recipes, damage, producer)


async def _assert_verified_batch_drift(fixture, prepared, recipes, damage, producer):
    from copy import deepcopy

    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        source_pin = recipes[0].source_pin
        verified = await producer.prepare_registry_aca_batch_read(fixture.connection, source_pin)
        schema = prepared.ownership.schema_name
        if damage == "metadata":
            source_pin = deepcopy(source_pin)
            source_pin.prepared.manifest.source_metadata[producer._BATCH_KEY]["coverage"] = []
        elif damage == "source":
            source_pin = deepcopy(source_pin)
            object.__setattr__(source_pin, "source_id", "other-source")
        else:
            await fixture.connection.execute(
                {
                    "stamp": f"COMMENT ON SCHEMA \"{schema}\" IS 'changed'",
                    "actor": f'SET LOCAL ROLE "{fixture.reader}"',
                    "privilege": f'GRANT UPDATE ON "{schema}".mrf_address_evidence TO "{fixture.reader}"',
                    "oid": f'ALTER TABLE "{schema}".mrf_address_evidence RENAME TO changed_evidence',
                }[damage]
            )
        with pytest.raises(asyncpg.UndefinedTableError if damage == "oid" else ValueError):
            await producer.require_registry_aca_batch_read(fixture.connection, source_pin, verified)
    finally:
        await transaction.rollback()
    async with fixture.source_sessions.begin() as session:
        assert (
            await producer.require_registry_aca_source_batch(session, prepared, runtime_roles=(fixture.reader,))
            == recipes
        )


@asynccontextmanager
async def _native_batch_notification_store(fixture):
    """Provision the unchanged inbox contract in this fixture's isolated registry."""
    from process.registry_aca_source_notification_permissions import (
        install_registry_aca_source_notification_permissions,
    )
    from process.registry_aca_source_notifications import RegistryACASourceNotificationStore

    owner = "event_owner_" + uuid4().hex
    connection = fixture.connection
    original_owner = await connection.fetchval(
        "SELECT quote_ident(pg_get_userbyid(nspowner)) FROM pg_namespace WHERE nspname=$1", fixture.control
    )
    original_tables = await connection.fetch(
        "SELECT relname,quote_ident(pg_get_userbyid(relowner)) AS owner FROM pg_class WHERE relnamespace=to_regnamespace($1) AND relname=ANY($2::text[])",
        fixture.control,
        ["network_serving_control", "registry_aca_source_notification"],
    )
    await connection.execute(f'CREATE ROLE "{owner}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT')
    try:
        async with connection.transaction():
            await connection.execute(f'ALTER SCHEMA "{fixture.control}" OWNER TO "{owner}"')
            await connection.execute(f'ALTER TABLE "{fixture.control}".network_serving_control OWNER TO "{owner}"')
            await install_registry_aca_source_notification_permissions(
                connection,
                control_schema=fixture.control,
                publisher_role=fixture.publisher,
                reader_roles=(fixture.reader,),
                runtime_roles=tuple(sorted((fixture.publisher, fixture.reader))),
            )
        yield RegistryACASourceNotificationStore(fixture.control, fixture.publisher, (fixture.reader,))
    finally:
        await connection.execute(f'ALTER SCHEMA "{fixture.control}" OWNER TO {original_owner}')
        for table in original_tables:
            await connection.execute(f'ALTER TABLE "{fixture.control}".{table["relname"]} OWNER TO {table["owner"]}')
        await connection.execute(f'DROP OWNED BY "{owner}"')
        await connection.execute(f'DROP ROLE "{owner}"')


async def _prepare_batch_notification_fixture(fixture, store, failure):
    """Use the original clone/lease fixture with one complete event in its transaction."""
    events = []

    async def notify(session, recipes):
        assert session.in_transaction()
        events.append(await store.append(session, recipes))

    prepared, recipes, copied = await _prepare_initial_batch(fixture, failure=failure, on_admitted=notify)
    return prepared, recipes, copied, events


@pytest.mark.asyncio
async def test_native_initial_batch_notification_preserves_complete_event_and_replay(serving_schema, monkeypatch):
    async with (
        _native_initial_batch_source(serving_schema, monkeypatch) as fixture,
        _native_batch_notification_store(fixture) as store,
    ):
        prepared, recipes, copied, events = await _prepare_batch_notification_fixture(fixture, store, None)
        assert len(copied) == len(set(copied)) == 14
        assert len(recipes) == 3 and len(events) == 1
        assert events[0]["recipes"] == recipes
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{fixture.control}".registry_aca_source_notification'
            )
            == 1
        )
        async with fixture.publisher_sessions.begin() as session:
            assert await store.append(session, recipes) == events[0]
            assert await store.hold(session, prepared) == events[0]
            with pytest.raises(ValueError):
                await store.append(session, recipes[:1])
        async with fixture.source_sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            assert await store.read(session, events[0]["event"]) == events[0]


async def _assert_source_family_write_refusal(fixture):
    """Even an administrative writer cannot pass the ordinary Reader's native locks."""
    for table, column in (
        ("plan_npi_raw", "npi"),
        ("mrf_address_evidence", "npi"),
        ("mrf_publication_receipt", "state"),
    ):
        with pytest.raises(DBAPIError) as blocked:
            async with fixture.engine.begin() as writer:
                await writer.execute(text("SET LOCAL lock_timeout='50ms'"))
                await writer.execute(text(f'UPDATE "{fixture.schema}".{table} SET {column}={column}'))
        assert blocked.value.orig.sqlstate == "55P03"


async def _assert_active_source_write_refusal(fixture):
    from process.registry_aca_source_consumer import _lock_current_source_family

    async with fixture.engine.connect() as writer:
        transaction = await writer.begin()
        try:
            await writer.execute(text(f'UPDATE "{fixture.schema}".mrf_address_evidence SET npi=npi'))
            with pytest.raises(DBAPIError) as blocked:
                async with fixture.source_sessions.begin() as session:
                    await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                    await _lock_current_source_family(session, fixture.schema)
            assert blocked.value.orig.sqlstate == "55P03"
        finally:
            await transaction.rollback()


@pytest.mark.asyncio
async def test_native_current_source_fence_blocks_mutation_and_reads_postlock_state(serving_schema, monkeypatch):
    from process.registry_aca_source_consumer import _lock_current_source_family

    async with _native_initial_batch_source(serving_schema, monkeypatch) as fixture:
        await _assert_active_source_write_refusal(fixture)
        async with fixture.source_sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            await _lock_current_source_family(session, fixture.schema)
            assert (await receipt.require_completed_publication(session, fixture.schema, native_set=True))[
                "state"
            ] == "complete"
            await _assert_source_family_write_refusal(fixture)
        async with fixture.source_sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            with pytest.raises(ValueError, match="registry_aca_source_consumer_invalid"):
                await _lock_current_source_family(session, fixture.schema)
        async with fixture.source_sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            # An earlier identity observation must not hide a committed native guarded mutation.
            original = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf", schema_name=fixture.schema
            )
            async with fixture.engine.begin() as writer:
                await writer.execute(text(f'UPDATE "{fixture.schema}".plan_npi_raw SET npi=npi'))
            await _lock_current_source_family(session, fixture.schema)
            current = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf", schema_name=fixture.schema
            )
            assert current.local_generation > original.local_generation
            with pytest.raises(RuntimeError, match="completion generation differs"):
                await receipt.require_completed_publication(session, fixture.schema, native_set=True)
        async with fixture.source_sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            assert (
                await session.scalar(text(f'SELECT state FROM "{fixture.schema}".mrf_publication_receipt'))
                == "complete"
            )
            await receipt.begin_publication(fixture.schema, "import-two")
            await _lock_current_source_family(session, fixture.schema)
            with pytest.raises(RuntimeError, match="completion is unavailable"):
                await receipt.require_completed_publication(session, fixture.schema, native_set=True)
