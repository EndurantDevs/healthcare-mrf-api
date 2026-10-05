# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Committed migration retries retain native integrity and readable history."""

import os
from uuid import uuid4

import pytest
import pytest_asyncio
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.test_ptg_snapshot_candidates_postgres import _migration, _schema


async def _upgrade(engine, migration):
    """Use real Alembic transactions, including committed preparation phases."""
    async with engine.connect() as connection:

        def upgrade(sync):
            context = MigrationContext.configure(sync)
            migration.op = Operations(context)
            with context.begin_transaction():
                migration.upgrade()

        await connection.run_sync(upgrade)


def _assert_validation_reads(engine, schema, observations):
    """Observe held validation locks and a separate reader before phase commit."""

    def observe(connection, _cursor, statement, _parameters, _context, _many):
        if not statement.startswith("ALTER TABLE") or " VALIDATE CONSTRAINT " not in statement:
            return
        locks = (
            connection.execute(
                text("""SELECT mode FROM pg_locks
            WHERE pid=pg_backend_pid() AND locktype='relation' AND granted""")
            )
            .scalars()
            .all()
        )
        assert "ShareUpdateExclusiveLock" in locks
        assert not {"AccessExclusiveLock", "ShareRowExclusiveLock", "ExclusiveLock"}.intersection(locks)
        with engine.sync_engine.connect() as reader:
            reader.exec_driver_sql("SET LOCAL statement_timeout='1s'")
            assert reader.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity")) == 1
            assert reader.scalar(text(f"SELECT count(*) FROM {schema}.witness")) == 1
        observations.append(statement)

    return observe


async def _assert_interrupted_state(engine, schema, original_oid, original_index):
    """An interrupted migration keeps its committed fence and original storage."""
    async with engine.connect() as connection:
        assert await connection.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity")) == 1
        assert (
            await connection.scalar(
                text("SELECT indrelid FROM pg_index WHERE indexrelid=:oid"), {"oid": original_index}
            )
            == original_oid
        )
        assert (
            await connection.scalar(
                text(f"SELECT count(*) FROM {schema}.ptg2_snapshot_legacy_build WHERE snapshot_key=30")
            )
            == 1
        )
        with pytest.raises(Exception, match="migration_incomplete"):
            async with connection.begin_nested():
                await connection.execute(
                    text(f"INSERT INTO {schema}.ptg2_v4_snapshot_map_root(snapshot_key,state) VALUES(30,'building')")
                )
        with pytest.raises(Exception, match="foreign key"):
            async with connection.begin_nested():
                await connection.execute(text(f"INSERT INTO {schema}.witness VALUES(100,99)"))


async def _owned_fixture(engine, schema, writer, owner):
    """Keep explicit ownership and both PUBLIC read ACL forms across conversion."""
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f'CREATE ROLE "{writer}" NOLOGIN')
        await connection.exec_driver_sql(f'CREATE ROLE "{owner}" NOLOGIN')
        await _schema(connection, schema, writer)
        await connection.exec_driver_sql(f'GRANT USAGE ON SCHEMA "{schema}" TO "{owner}"')
        for table in _migration().TABLES:
            await connection.exec_driver_sql(f'ALTER TABLE {schema}.{table} OWNER TO "{owner}"')
        await connection.exec_driver_sql(f"GRANT SELECT(tin_key) ON {schema}.ptg2_provider_tax_identity TO PUBLIC")
        await connection.exec_driver_sql(f"GRANT SELECT ON {schema}.ptg2_provider_group_tax_identity TO PUBLIC")
        heap_oid = await connection.scalar(text(f"SELECT '{schema}.ptg2_provider_tax_identity'::regclass::oid"))
        index_oid = await connection.scalar(
            text("SELECT indexrelid FROM pg_index WHERE indrelid=:oid"), {"oid": heap_oid}
        )
        return {
            "heap": heap_oid,
            "index": index_oid,
            "index_file": await connection.scalar(
                text("SELECT relfilenode FROM pg_class WHERE oid=:oid"), {"oid": index_oid}
            ),
            "map_fk": await connection.scalar(
                text(
                    f"SELECT oid FROM pg_constraint WHERE conrelid='{schema}.ptg2_v4_snapshot_map_pack'::regclass AND contype='f'"
                )
            ),
        }


async def _assert_completed_storage(engine, schema, owner, storage):
    """Ownership, read grants and index OIDs survive; scoped FKs become set checks."""
    async with engine.connect() as connection:
        assert (
            await connection.scalar(text(f"SELECT '{schema}.ptg2_provider_tax_identity_history'::regclass::oid"))
            == storage["heap"]
        )
        assert (
            await connection.scalar(
                text("SELECT indrelid FROM pg_index WHERE indexrelid=:oid"), {"oid": storage["index"]}
            )
            == storage["heap"]
        )
        assert (
            await connection.scalar(text("SELECT relfilenode FROM pg_class WHERE oid=:oid"), {"oid": storage["index"]})
            == storage["index_file"]
        )
        assert not await connection.scalar(
            text("SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE oid=:oid)"), {"oid": storage["map_fk"]}
        )
        assert await connection.scalar(
            text(
                f"SELECT jsonb_array_length(relationships)>0 FROM {schema}.ptg2_snapshot_partition_boundary WHERE table_name='ptg2_v4_snapshot_map_pack'"
            )
        )
        assert not await connection.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE connamespace=CAST(:schema AS regnamespace) AND NOT convalidated)"
            ),
            {"schema": schema},
        )
        for table in _migration().TABLES:
            for suffix in ("", "_history"):
                assert (
                    await connection.scalar(
                        text("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=CAST(:table AS regclass)"),
                        {"table": f"{schema}.{table}{suffix}"},
                    )
                    == owner
                )
        assert (
            await connection.scalar(
                text("""SELECT count(*) FROM pg_class relation CROSS JOIN LATERAL aclexplode(relacl) acl
            WHERE relnamespace=CAST(:schema AS regnamespace) AND relname LIKE 'ptg2_provider_group_tax_identity%'
              AND acl.grantee=0 AND privilege_type='SELECT'"""),
                {"schema": schema},
            )
            == 2
        )
        assert (
            await connection.scalar(
                text("""SELECT count(*) FROM pg_attribute attribute CROSS JOIN LATERAL aclexplode(attacl) acl
            JOIN pg_class relation ON relation.oid=attribute.attrelid WHERE relnamespace=CAST(:schema AS regnamespace)
              AND relname LIKE 'ptg2_provider_tax_identity%' AND attname='tin_key'
              AND acl.grantee=0 AND privilege_type='SELECT'"""),
                {"schema": schema},
            )
            == 2
        )


@pytest_asyncio.fixture
async def migration_database():
    """Use a unique native schema, migration role, and distinct relation owner."""
    dsn = (
        os.getenv("HLTHPRT_PTG_SET_VALIDATION_POSTGRES_DSN")
        or os.getenv("HLTHPRT_PTG2_TAX_IDENTITY_POSTGRES_DSN")
        or os.getenv("HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN")
    )
    if not dsn:
        pytest.skip("requires an explicit disposable PostgreSQL test DSN")
    if "test" not in dsn.rsplit("/", 1)[-1]:
        pytest.fail("disposable test database required")
    engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://", 1))
    schema, writer = "ptg_migration_test_" + uuid4().hex, "ptg_migration_writer_" + uuid4().hex
    owner = "ptg_migration_owner_" + uuid4().hex
    try:
        storage = await _owned_fixture(engine, schema, writer, owner)
        yield engine, schema, writer, owner, storage
    finally:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await connection.exec_driver_sql(f'DROP ROLE IF EXISTS "{writer}"')
            await connection.exec_driver_sql(f'DROP ROLE IF EXISTS "{owner}"')
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", ["conversion", "publication"])
async def test_migration_retries_without_holding_exclusive_locks_during_scans(
    monkeypatch, interruption, migration_database
):
    """Resume a partial conversion and fully prepared FK swap without new heap/index OIDs."""
    engine, schema, _writer, owner, storage = migration_database
    observations = []
    observer = _assert_validation_reads(engine, schema, observations)
    event.listen(engine.sync_engine, "after_cursor_execute", observer)
    try:
        migration = _migration()
        monkeypatch.setattr(migration, "_schema", lambda: schema)
        interruption_hook = "_partition_table" if interruption == "conversion" else "_finish_references"
        original = getattr(migration, interruption_hook)

        def interrupt(*arguments):
            if interruption != "conversion" or arguments[1]["table_name"] == "ptg2_provider_tax_identity":
                raise RuntimeError("injected migration interruption")
            return original(*arguments)

        monkeypatch.setattr(migration, interruption_hook, interrupt)
        with pytest.raises(RuntimeError, match="injected migration interruption"):
            await _upgrade(engine, migration)
        await _assert_interrupted_state(engine, schema, storage["heap"], storage["index"])
        monkeypatch.setattr(migration, interruption_hook, original)
        await _upgrade(engine, migration)
        assert observations
        await _assert_completed_storage(engine, schema, owner, storage)
    finally:
        event.remove(engine.sync_engine, "after_cursor_execute", observer)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dependency",
    ["rls", "policy", "view", "sql_function", "rowtype_function", "rowtype_column", "rule", "trigger", "inheritance"],
)
async def test_unsupported_dependencies_fail_before_committed_conversion(monkeypatch, migration_database, dependency):
    """Reject objects whose policy or OID binding cannot follow a new parent safely."""
    engine, schema, _writer, _owner, storage = migration_database
    table = f"{schema}.ptg2_provider_tax_identity"
    statements_by_kind = {
        "rls": f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY",
        "policy": f"CREATE POLICY read_policy ON {table} USING(true)",
        "view": f"CREATE VIEW {schema}.dependent_view AS SELECT * FROM {table}",
        "sql_function": f"CREATE FUNCTION {schema}.dependent_count() RETURNS bigint LANGUAGE sql BEGIN ATOMIC SELECT count(*) FROM {table}; END",
        "rowtype_function": f"CREATE FUNCTION {schema}.dependent_row({table}) RETURNS int LANGUAGE sql AS 'SELECT 1'",
        "rowtype_column": f"CREATE TABLE {schema}.dependent_column(value {table})",
        "rule": f"CREATE RULE ignore_update AS ON UPDATE TO {table} DO INSTEAD NOTHING",
        "trigger": f"CREATE TRIGGER extra_guard BEFORE INSERT ON {table} FOR EACH STATEMENT EXECUTE FUNCTION {schema}.guard_ptg2_provider_tax_identity()",
        "inheritance": f"CREATE TABLE {schema}.dependent_child() INHERITS ({table})",
    }
    async with engine.begin() as connection:
        await connection.exec_driver_sql(statements_by_kind[dependency])
    migration = _migration()
    monkeypatch.setattr(migration, "_schema", lambda: schema)
    with pytest.raises(RuntimeError, match="preparation_(dependency|guard)_changed"):
        await _upgrade(engine, migration)
    async with engine.connect() as connection:
        assert await connection.scalar(text(f"SELECT '{table}'::regclass::oid")) == storage["heap"]
        assert (
            await connection.scalar(
                text("SELECT to_regclass(:table)"), {"table": schema + ".ptg2_snapshot_partition_preparation"}
            )
            is None
        )
