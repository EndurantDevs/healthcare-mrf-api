# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native COPY, detached index, authorization, and atomic PTG partition proof."""

import asyncio
import importlib.util
import os
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.ptg_parts.ptg2_shared_blocks import SharedBlock
from process.ptg_parts.ptg2_snapshot_candidates import (
    attach_snapshot_candidates,
    begin_snapshot_candidate,
    candidate_driver,
    finish_snapshot_candidate,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import publish_v4_snapshot_maps


def _migration():
    path = Path(__file__).parents[1] / "alembic/versions/20261005110000_ptg_set_validation.py"
    spec = importlib.util.spec_from_file_location("ptg_set_validation", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema_statements():
    """Describe the small native relations used by every candidate proof."""
    return [
        "CREATE TABLE ptg2_v3_snapshot_layout(snapshot_key bigint PRIMARY KEY,build_token text,generation text,state text)",
        "CREATE TABLE ptg2_v4_snapshot_map_root(snapshot_key bigint PRIMARY KEY,state text,format_version smallint DEFAULT 1,map_format text DEFAULT 'packed_coordinate_hash_v1',representation text DEFAULT 'direct_v1',projection_id_scope text DEFAULT 'snapshot_local_v1')",
        "CREATE TABLE ptg2_v3_block(block_hash bytea PRIMARY KEY,format_version smallint,object_kind text,codec text,entry_count bigint,raw_byte_count bigint,stored_byte_count bigint,payload bytea NOT NULL,created_at timestamptz DEFAULT now())",
        "CREATE TABLE ptg2_v3_gc_candidate(block_hash bytea PRIMARY KEY)",
        "CREATE TABLE ptg2_provider_tax_identity_manifest(snapshot_key bigint PRIMARY KEY,source_shard_count int)",
        "CREATE TABLE ptg2_v3_provider_group(snapshot_key bigint,provider_group_global_id_128 bytea,PRIMARY KEY(snapshot_key,provider_group_global_id_128))",
        "CREATE TABLE ptg2_provider_tax_identity_source_binding(snapshot_key bigint,source_key integer,PRIMARY KEY(snapshot_key,source_key))",
        "CREATE TABLE ptg2_provider_group_tax_identity_source(snapshot_key bigint NOT NULL,source_key integer NOT NULL,provider_group_global_id_128 bytea NOT NULL,source_record_ordinal bigint NOT NULL,tax_identity_state text NOT NULL,tin_key integer,PRIMARY KEY(snapshot_key,source_key,provider_group_global_id_128),UNIQUE(snapshot_key,source_key,source_record_ordinal),FOREIGN KEY(snapshot_key,source_key) REFERENCES ptg2_provider_tax_identity_source_binding(snapshot_key,source_key),FOREIGN KEY(snapshot_key,provider_group_global_id_128) REFERENCES ptg2_v3_provider_group(snapshot_key,provider_group_global_id_128),FOREIGN KEY(snapshot_key,tin_key) REFERENCES ptg2_provider_tax_identity(snapshot_key,tin_key))",
        "CREATE TABLE ptg2_v4_snapshot_map_pack(snapshot_key bigint NOT NULL,object_kind text NOT NULL,pack_no int NOT NULL,first_block_key bigint NOT NULL,first_fragment_no int NOT NULL,last_block_key bigint NOT NULL,last_fragment_no int NOT NULL,coordinate_count int NOT NULL,entry_count bigint NOT NULL,logical_byte_count bigint NOT NULL,map_block_hash bytea NOT NULL,PRIMARY KEY(snapshot_key,object_kind,pack_no),FOREIGN KEY(map_block_hash) REFERENCES ptg2_v3_block(block_hash),CHECK(ROW(first_block_key,first_fragment_no)<=ROW(last_block_key,last_fragment_no)))",
        "CREATE INDEX map_hash ON ptg2_v4_snapshot_map_pack(map_block_hash)",
        "CREATE TABLE ptg2_provider_tax_identity(snapshot_key bigint NOT NULL,tin_key int NOT NULL,tin_id_128 bytea NOT NULL,tin_hmac_sha256 bytea NOT NULL,PRIMARY KEY(snapshot_key,tin_key),CHECK(tin_key>=0),CHECK(octet_length(tin_id_128)=16 AND octet_length(tin_hmac_sha256)=32 AND tin_id_128=substring(tin_hmac_sha256 FROM 1 FOR 16)))",
        "CREATE TABLE ptg2_provider_group_tax_identity(snapshot_key bigint NOT NULL,provider_group_global_id_128 bytea NOT NULL,tax_identity_state text NOT NULL,tin_key int,source_bitmap bytea NOT NULL,PRIMARY KEY(snapshot_key,provider_group_global_id_128),FOREIGN KEY(snapshot_key,tin_key) REFERENCES ptg2_provider_tax_identity(snapshot_key,tin_key))",
        "CREATE TABLE witness(snapshot_key bigint,tin_key int,FOREIGN KEY(snapshot_key,tin_key) REFERENCES ptg2_provider_tax_identity(snapshot_key,tin_key))",
        "CREATE TABLE ptg2_v3_provider_set(snapshot_key bigint,provider_set_key integer,PRIMARY KEY(snapshot_key,provider_set_key))",
        "CREATE TABLE ptg2_v4_relation_manifest(snapshot_key bigint,relation text,PRIMARY KEY(snapshot_key,relation))",
        "CREATE TABLE ptg2_v4_npi_scope(snapshot_key bigint NOT NULL,npi_key integer NOT NULL CHECK(npi_key>=0),npi bigint NOT NULL CHECK(npi BETWEEN 1000000000 AND 9999999999),PRIMARY KEY(snapshot_key,npi_key),UNIQUE(snapshot_key,npi),FOREIGN KEY(snapshot_key) REFERENCES ptg2_v4_snapshot_map_root(snapshot_key) ON DELETE CASCADE)",
        "CREATE TABLE ptg2_v4_provider_component(snapshot_key bigint NOT NULL,component_key integer NOT NULL CHECK(component_key>=0),component_global_id_128 bytea NOT NULL CHECK(octet_length(component_global_id_128)=16),PRIMARY KEY(snapshot_key,component_key),UNIQUE(snapshot_key,component_global_id_128),FOREIGN KEY(snapshot_key) REFERENCES ptg2_v4_snapshot_map_root(snapshot_key) ON DELETE CASCADE)",
        "CREATE TABLE ptg2_v4_pattern(snapshot_key bigint NOT NULL,pattern_key integer NOT NULL CHECK(pattern_key>=0),pattern_digest bytea NOT NULL CHECK(octet_length(pattern_digest)=32),set_count bigint NOT NULL CHECK(set_count>=0),PRIMARY KEY(snapshot_key,pattern_key),UNIQUE(snapshot_key,pattern_digest),FOREIGN KEY(snapshot_key) REFERENCES ptg2_v4_snapshot_map_root(snapshot_key) ON DELETE CASCADE)",
        "CREATE TABLE ptg2_v4_provider_set_npi_prefix(snapshot_key bigint NOT NULL,provider_set_key integer NOT NULL,member_count integer NOT NULL CHECK(member_count>=0),member_digest bytea NOT NULL CHECK(octet_length(member_digest)=32),PRIMARY KEY(snapshot_key,provider_set_key),FOREIGN KEY(snapshot_key,provider_set_key) REFERENCES ptg2_v3_provider_set(snapshot_key,provider_set_key))",
        "CREATE TABLE ptg2_v4_heavy_owner(snapshot_key bigint NOT NULL,relation text NOT NULL,owner_key bigint NOT NULL CHECK(owner_key>=0),object_kind text NOT NULL,member_count bigint NOT NULL CHECK(member_count>=0),member_base bigint NOT NULL CHECK(member_base>=0),member_span bigint NOT NULL CHECK(member_span>0),fragment_count integer NOT NULL CHECK(fragment_count>0),PRIMARY KEY(snapshot_key,relation,owner_key),FOREIGN KEY(snapshot_key,relation) REFERENCES ptg2_v4_relation_manifest(snapshot_key,relation))",
        "CREATE FUNCTION guard_ptg2_v4_snapshot_metadata() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
        "CREATE FUNCTION guard_ptg2_v4_snapshot_map_pack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
        "CREATE FUNCTION guard_ptg2_provider_tax_identity() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
        "INSERT INTO ptg2_v3_snapshot_layout VALUES(100,'history','shared_blocks_v4','sealed'),(20,'legacy','shared_blocks_v4','building'),(30,'rootless','shared_blocks_v4','building')",
        "INSERT INTO ptg2_v4_snapshot_map_root(snapshot_key,state) VALUES(100,'complete'),(20,'building')",
        "INSERT INTO ptg2_provider_tax_identity_manifest VALUES(100,1),(20,1)",
        "INSERT INTO ptg2_provider_tax_identity VALUES(100,0,decode(repeat('11',16),'hex'),decode(repeat('11',32),'hex'))",
        "INSERT INTO witness VALUES(100,0)",
    ]


async def _schema(connection, schema, writer):
    """Create scoped writers and known legacy guards before the real migration."""
    await connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    statements = _schema_statements()
    await connection.exec_driver_sql(f'SET LOCAL search_path="{schema}",pg_catalog')
    source_statement = next(
        statement
        for statement in statements
        if statement.startswith("CREATE TABLE ptg2_provider_group_tax_identity_source(")
    )
    statements.remove(source_statement)
    for statement in statements:
        await connection.exec_driver_sql(statement)
    await connection.exec_driver_sql(source_statement)
    for table in _migration().TABLES:
        if table == "ptg2_provider_group_tax_identity_source":
            for suffix, event, level in (
                ("insert", "AFTER INSERT", "STATEMENT"),
                ("mutation", "BEFORE UPDATE OR DELETE", "ROW"),
                ("truncate", "BEFORE TRUNCATE", "STATEMENT"),
            ):
                function = "guard_ptg2_provider_tax_identity_source_" + suffix
                await connection.exec_driver_sql(
                    f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$"
                )
                await connection.exec_driver_sql(
                    f"CREATE TRIGGER {table}_{suffix}_guard {event} ON {table} FOR EACH {level} EXECUTE FUNCTION {function}()"
                )
                await connection.exec_driver_sql(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {table}_{suffix}_guard")
            await connection.exec_driver_sql(f'GRANT SELECT,INSERT ON {table} TO "{writer}"')
            continue
        function = _migration()._guard_function(table)
        await connection.exec_driver_sql(
            f"CREATE TRIGGER {table}_guard BEFORE INSERT OR UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION {function}()"
        )
        await connection.exec_driver_sql(f'GRANT SELECT,INSERT ON {table} TO "{writer}"')
    await connection.exec_driver_sql(f'GRANT USAGE ON SCHEMA "{schema}" TO "{writer}"')
    await connection.exec_driver_sql(
        f'GRANT SELECT,INSERT,UPDATE ON ptg2_v4_snapshot_map_root,ptg2_v3_snapshot_layout,ptg2_v3_block TO "{writer}"'
    )
    await connection.exec_driver_sql(f'GRANT SELECT,DELETE ON ptg2_v3_gc_candidate TO "{writer}"')


@asynccontextmanager
async def _candidate_database(monkeypatch):
    """Keep each proof in a disposable schema with its own restricted writer."""
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
    schema, writer = "ptg_candidate_test_" + uuid4().hex, "ptg_writer_" + uuid4().hex
    try:
        async with engine.connect() as connection:
            await connection.exec_driver_sql(f'CREATE ROLE "{writer}" NOLOGIN')
            await _schema(connection, schema, writer)
            await _migrate_candidate_schema(connection, schema, writer, monkeypatch)
            await connection.execute(
                text(f"INSERT INTO {schema}.ptg2_v3_snapshot_layout VALUES(10,'owned','shared_blocks_v4','building')")
            )
            await connection.execute(
                text(f"INSERT INTO {schema}.ptg2_v4_snapshot_map_root(snapshot_key,state) VALUES(10,'building')")
            )
            await connection.execute(text(f"INSERT INTO {schema}.ptg2_provider_tax_identity_manifest VALUES(10,1)"))
            await connection.execute(
                text(f"INSERT INTO {schema}.ptg2_v4_snapshot_map_root(snapshot_key,state) VALUES(30,'building')")
            )
            await connection.commit()
        yield engine, schema, writer
    finally:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await connection.exec_driver_sql(f'DROP ROLE IF EXISTS "{writer}"')
        await engine.dispose()


async def _migrate_candidate_schema(connection, schema, writer, monkeypatch):
    """Retain historical heap/index OIDs and close both default and column grants."""
    await connection.execute(
        text(f"INSERT INTO {schema}.ptg2_provider_tax_identity VALUES(20,0,:short,:full)"),
        {"short": b"b" * 16, "full": b"b" * 32},
    )
    await connection.exec_driver_sql(f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT ALL ON TABLES TO "{writer}"')
    await connection.exec_driver_sql(
        f'GRANT INSERT(tin_key),UPDATE(tin_key) ON {schema}.ptg2_provider_tax_identity TO "{writer}"'
    )
    old_oid = await connection.scalar(text(f"SELECT '{schema}.ptg2_provider_tax_identity'::regclass::oid"))
    old_index = await connection.scalar(
        text(f"SELECT indexrelid FROM pg_index WHERE indrelid='{schema}.ptg2_provider_tax_identity'::regclass")
    )
    migration = _migration()
    monkeypatch.setattr(migration, "_schema", lambda: schema)
    await connection.commit()

    def migrate(sync):
        context = MigrationContext.configure(sync)
        migration.op = Operations(context)
        with context.begin_transaction():
            migration.upgrade()

    await connection.run_sync(migrate)
    assert (
        await connection.scalar(
            text("""SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema AND NOT t.tgisinternal
        AND t.tgtype & 4 <> 0 AND t.tgtype & 1 <> 0"""),
            {"schema": schema},
        )
        == 0
    )
    assert (
        await connection.scalar(text(f"SELECT '{schema}.ptg2_provider_tax_identity_history'::regclass::oid")) == old_oid
    )
    assert (
        await connection.scalar(text("SELECT indrelid FROM pg_index WHERE indexrelid=:oid"), {"oid": old_index})
        == old_oid
    )


async def _publish_map_fixture(engine, schema, writer):
    """Publish and replay a real encoded map through bounded COPY and CAS joins."""
    block = SharedBlock("alpha_v1", 1, 0, 2, "none", 3, b"abc")
    async with engine.begin() as connection:
        await connection.execute(
            text(f"""INSERT INTO {schema}.ptg2_v3_block
            (block_hash,format_version,object_kind,codec,entry_count,raw_byte_count,stored_byte_count,payload)
            VALUES(:hash,2,'alpha_v1','none',2,3,3,:payload)"""),
            {"hash": block.block_hash, "payload": block.payload},
        )
    sessions = async_sessionmaker(engine)
    async with sessions.begin() as session:
        await session.execute(text(f'SET LOCAL ROLE "{writer}"'))
        publication = await publish_v4_snapshot_maps(
            session,
            schema_name=schema,
            snapshot_key=10,
            build_token="owned",
            representation="direct_v1",
            references=[block.reference()],
        )
        assert publication.map_pack_count == 1 and publication.entry_count == 2
        replay = await publish_v4_snapshot_maps(
            session,
            schema_name=schema,
            snapshot_key=10,
            build_token="owned",
            representation="direct_v1",
            references=[block.reference()],
        )
        assert replay == publication
        await attach_snapshot_candidates(session, schema, 10, "owned")


@pytest.mark.asyncio
async def test_candidate_copy_constraints_and_atomic_publication(monkeypatch):
    """The candidate loads without indexes and becomes visible only after publication."""
    async with _candidate_database(monkeypatch) as (engine, schema, writer):
        await _publish_map_fixture(engine, schema, writer)
        sessions = async_sessionmaker(engine)
        async with sessions.begin() as session:
            await session.execute(text(f'SET LOCAL ROLE "{writer}"'))
            with pytest.raises(Exception, match="permission denied"):
                async with session.begin_nested():
                    await session.execute(text(f"INSERT INTO {schema}.ptg2_provider_tax_identity VALUES(10,0,'x','x')"))
            candidate = await begin_snapshot_candidate(session, schema, "ptg2_provider_tax_identity", 10, "owned")
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM pg_index WHERE indrelid=CAST(:name AS regclass)"),
                    {"name": f"{schema}.{candidate}"},
                )
                == 0
            )
            driver = await candidate_driver(session)
            copy_rows = [(10, 0, b"a" * 16, b"a" * 32)]
            await driver.copy_records_to_table(
                candidate,
                schema_name=schema,
                columns=("snapshot_key", "tin_key", "tin_id_128", "tin_hmac_sha256"),
                records=copy_rows,
            )
            assert (
                await session.scalar(
                    text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity WHERE snapshot_key=10")
                )
                == 0
            )
            assert await finish_snapshot_candidate(session, schema, candidate, 1) == 1
            await _assert_uncommitted_invisible(engine, schema)
            await attach_snapshot_candidates(session, schema, 10, "owned")
            assert (
                await session.scalar(
                    text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity WHERE snapshot_key=10")
                )
                == 1
            )
            with pytest.raises(Exception, match="permission denied"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f"INSERT INTO {schema}.{candidate} VALUES(10,1,decode(repeat('aa',16),'hex'),decode(repeat('aa',32),'hex'))"
                        )
                    )
        async with engine.begin() as connection:
            await connection.execute(text(f"INSERT INTO {schema}.witness VALUES(10,0)"))
            assert await connection.scalar(text(f"SELECT count(*) FROM {schema}.witness")) == 2


@pytest.mark.asyncio
async def test_candidate_authority_and_failed_copy_rollback(monkeypatch):
    """Reject legacy/stale authority and leave no candidate on transaction failure."""
    async with _candidate_database(monkeypatch) as (engine, schema, writer):
        sessions = async_sessionmaker(engine)
        async with sessions.begin() as session:
            await session.execute(text(f'SET LOCAL ROLE "{writer}"'))
            for key, token, message in (
                (20, "legacy", "rerun_required"),
                (30, "rootless", "rerun_required"),
                (10, "stale", "authority"),
                (100, "history", "authority"),
            ):
                await _assert_candidate_rejected(session, schema, key, token, message)
            for table in (
                "ptg2_provider_tax_identity_history",
                "ptg2_snapshot_candidate",
                "ptg2_snapshot_legacy_build",
            ):
                await _assert_delete_denied(session, schema, table)
        with pytest.raises(Exception, match="candidate_count"):
            async with sessions.begin() as session:
                await session.execute(text(f'SET LOCAL ROLE "{writer}"'))
                candidate = await begin_snapshot_candidate(session, schema, "ptg2_provider_tax_identity", 10, "owned")
                await finish_snapshot_candidate(session, schema, candidate, 1)
        async with engine.begin() as connection:
            assert await connection.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_snapshot_candidate")) == 0
            assert (
                await connection.scalar(
                    text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity WHERE snapshot_key=20")
                )
                == 1
            )


@pytest.mark.asyncio
async def test_empty_dictionary_replay(monkeypatch):
    """An empty complete dictionary has a reusable partition and no new live indexes."""
    async with _candidate_database(monkeypatch) as (engine, schema, writer):
        sessions = async_sessionmaker(engine)
        async with sessions.begin() as session:
            await session.execute(text(f'SET LOCAL ROLE "{writer}"'))
            for _ in range(2):
                candidate = await begin_snapshot_candidate(session, schema, "ptg2_provider_tax_identity", 10, "owned")
                assert await finish_snapshot_candidate(session, schema, candidate, 0) == 0
            await attach_snapshot_candidates(session, schema, 10, "owned")
        async with engine.begin() as connection:
            assert (
                await connection.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_snapshot_candidate WHERE published"))
                == 1
            )


@pytest.mark.asyncio
async def test_candidate_cleanup_never_queues_behind_incumbent_readers(monkeypatch):
    """Busy parents roll back GC immediately; an idle retry removes only its candidate."""
    async with _candidate_database(monkeypatch) as (engine, schema, writer):
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'SET LOCAL ROLE "{writer}"')
            candidate = await begin_snapshot_candidate(connection, schema, "ptg2_provider_tax_identity", 10, "owned")
            await finish_snapshot_candidate(connection, schema, candidate, 0)
            await attach_snapshot_candidates(connection, schema, 10, "owned")
        async with engine.connect() as reader, engine.connect() as cleanup:
            assert (
                await reader.scalar(
                    text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity WHERE snapshot_key=100")
                )
                == 1
            )
            with pytest.raises(DBAPIError) as failure:
                await asyncio.wait_for(
                    cleanup.execute(text(f"DELETE FROM {schema}.ptg2_v3_snapshot_layout WHERE snapshot_key=10")),
                    timeout=2,
                )
            assert failure.value.orig.sqlstate == "55P03"
            await cleanup.rollback()
            await _assert_gc_rollback_preserves_readers(engine, schema)
            await reader.rollback()
        async with engine.begin() as connection:
            await connection.execute(text(f"DELETE FROM {schema}.ptg2_v3_snapshot_layout WHERE snapshot_key=10"))
            assert (
                await connection.scalar(
                    text(f"SELECT count(*) FROM {schema}.ptg2_snapshot_candidate WHERE snapshot_key=10")
                )
                == 0
            )
            assert (
                await connection.scalar(text("SELECT to_regclass(:table)"), {"table": f"{schema}.{candidate}"}) is None
            )
        async with engine.connect() as connection:
            assert (
                await connection.scalar(text("SELECT to_regclass(:table)"), {"table": f"{schema}.{candidate}"}) is None
            )
            assert (
                await connection.scalar(
                    text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity WHERE snapshot_key=100")
                )
                == 1
            )


async def _assert_gc_rollback_preserves_readers(engine, schema):
    """A failed cleanup preserves its layout without leaving a queued reader blocker."""
    async with engine.connect() as later_reader:
        assert (
            await asyncio.wait_for(
                later_reader.scalar(
                    text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity WHERE snapshot_key=100")
                ),
                timeout=2,
            )
            == 1
        )
        assert (
            await later_reader.scalar(
                text(f"SELECT count(*) FROM {schema}.ptg2_v3_snapshot_layout WHERE snapshot_key=10")
            )
            == 1
        )
        assert (
            await later_reader.scalar(
                text("SELECT count(*) FROM pg_locks WHERE relation=CAST(:table AS regclass) AND NOT granted"),
                {"table": f"{schema}.ptg2_provider_tax_identity"},
            )
            == 0
        )


async def _assert_candidate_rejected(session, schema, key, token, message):
    """Check one authority error without aborting the enclosing proof transaction."""
    with pytest.raises(Exception, match=message):
        async with session.begin_nested():
            await begin_snapshot_candidate(session, schema, "ptg2_provider_tax_identity", key, token)


async def _assert_delete_denied(session, schema, table):
    """Ensure revoked table grants cannot bypass the candidate publisher."""
    with pytest.raises(Exception, match="permission denied"):
        async with session.begin_nested():
            await session.execute(text(f"DELETE FROM {schema}.{table}"))


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["overlap", "dense_key", "hmac_order", "bitmap", "missing_reference"])
async def test_candidate_set_checks_reject_invalid_rows(monkeypatch, case):
    """Semantic failures roll back the whole candidate before parent visibility."""
    async with _candidate_database(monkeypatch) as (engine, schema, writer):
        table, columns, records, message = _invalid_candidate(case)
        sessions = async_sessionmaker(engine)
        with pytest.raises(Exception, match=message):
            async with sessions.begin() as session:
                await session.execute(text(f'SET LOCAL ROLE "{writer}"'))
                candidate = await begin_snapshot_candidate(session, schema, table, 10, "owned")
                driver = await candidate_driver(session)
                await driver.copy_records_to_table(candidate, schema_name=schema, columns=columns, records=records)
                await finish_snapshot_candidate(session, schema, candidate, len(records))
        async with engine.begin() as connection:
            assert await connection.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_snapshot_candidate")) == 0
            assert await connection.scalar(text(f"SELECT count(*) FROM {schema}.{table} WHERE snapshot_key=10")) == 0


def _invalid_candidate(case):
    """Use valid native encodings with invalid cross-row snapshot semantics."""
    if case == "overlap":
        columns = (
            "snapshot_key",
            "object_kind",
            "pack_no",
            "first_block_key",
            "first_fragment_no",
            "last_block_key",
            "last_fragment_no",
            "coordinate_count",
            "entry_count",
            "logical_byte_count",
            "map_block_hash",
        )
        return (
            "ptg2_v4_snapshot_map_pack",
            columns,
            [
                (10, "a", 0, 1, 0, 3, 0, 1, 1, 1, b"m" * 32),
                (10, "a", 1, 2, 0, 4, 0, 1, 1, 1, b"m" * 32),
            ],
            "candidate_overlap",
        )
    if case in ("dense_key", "hmac_order"):
        columns = ("snapshot_key", "tin_key", "tin_id_128", "tin_hmac_sha256")
        copy_rows = (
            [(10, 2, b"a" * 16, b"a" * 32)]
            if case == "dense_key"
            else [(10, 0, b"b" * 16, b"b" * 32), (10, 1, b"a" * 16, b"a" * 32)]
        )
        return "ptg2_provider_tax_identity", columns, copy_rows, "candidate_reference"
    columns = ("snapshot_key", "provider_group_global_id_128", "tax_identity_state", "tin_key", "source_bitmap")
    copy_rows = (
        [(10, b"g" * 16, "missing", None, b"\x80")]
        if case == "bitmap"
        else [(10, b"g" * 16, "matched_ein", 7, b"\x01")]
    )
    return "ptg2_provider_group_tax_identity", columns, copy_rows, "candidate_reference"


async def _assert_uncommitted_invisible(engine, schema):
    """A concurrent reader keeps the previous snapshot while attachment awaits commit."""
    async with engine.connect() as reader:
        visible_count = await asyncio.wait_for(
            reader.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity WHERE snapshot_key=10")),
            timeout=3,
        )
        assert visible_count == 0
        assert (
            await reader.scalar(
                text(f"SELECT count(*) FROM {schema}.ptg2_provider_tax_identity WHERE snapshot_key=100")
            )
            == 1
        )


_METADATA_CASES = (
    ("ptg2_v4_npi_scope", ("npi_key", "npi"), ((0, 1234567890), (1, 1234567891))),
    ("ptg2_v4_provider_component", ("component_key", "component_global_id_128"), ((0, b"a" * 16), (1, b"b" * 16))),
    ("ptg2_v4_pattern", ("pattern_key", "pattern_digest", "set_count"), ((0, b"a" * 32, 2), (1, b"b" * 32, 3))),
    (
        "ptg2_v4_provider_set_npi_prefix",
        ("provider_set_key", "member_count", "member_digest"),
        ((7, 1, b"a" * 32), (9, 2, b"b" * 32)),
    ),
    (
        "ptg2_v4_heavy_owner",
        ("relation", "owner_key", "object_kind", "member_count", "member_base", "member_span", "fragment_count"),
        (("r", 7, "bitmap", 1, 0, 1, 1), ("r", 9, "bitmap", 2, 0, 2, 1)),
    ),
)


async def _metadata_stage(engine, schema, writer, table, columns, records):
    """Populate a typed isolated source with only the fixture's selected snapshot."""
    fields = ",".join(columns)
    async with engine.begin() as connection:
        await connection.execute(text(f"INSERT INTO {schema}.ptg2_v3_provider_set VALUES(10,7),(10,9)"))
        await connection.execute(text(f"INSERT INTO {schema}.ptg2_v4_relation_manifest VALUES(10,'r')"))
        await connection.execute(
            text(f"CREATE TABLE {schema}.metadata_stage AS SELECT {fields} FROM {schema}.{table} WITH NO DATA")
        )
        driver = await candidate_driver(connection)
        await driver.copy_records_to_table("metadata_stage", schema_name=schema, columns=columns, records=records)
        await connection.execute(text(f'GRANT SELECT ON {schema}.metadata_stage TO "{writer}"'))


async def _publish_metadata_stage(session, schema, table, columns, copy_rows):
    """Exercise the compiled-stage path and the streaming heavy-owner sibling."""
    from process.ptg_parts import ptg2_snapshot_candidates as candidates
    from process.ptg_parts import ptg2_v4_snapshot_maps as maps

    if table == "ptg2_v4_heavy_owner":
        publication = await maps.publish_v4_heavy_owners(
            session, schema_name=schema, snapshot_key=10, build_token="owned", entries=copy_rows, batch_rows=1
        )
        assert publication.row_count == 2
    else:
        assert (
            await candidates.copy_snapshot_candidate(
                session,
                schema_name=schema,
                table=table,
                snapshot_key=10,
                build_token="owned",
                stage_table="metadata_stage",
                columns=columns,
                expected_count=2,
            )
            == 2
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("table,columns,copy_rows", _METADATA_CASES)
async def test_guarded_metadata_candidates_preserve_sets_and_atomic_visibility(monkeypatch, table, columns, copy_rows):
    """Exercise every large metadata writer, detached indexes, replay and readers."""
    from process.ptg_parts import ptg2_snapshot_candidates as candidates

    async with _candidate_database(monkeypatch) as (engine, schema, writer):
        await _metadata_stage(engine, schema, writer, table, columns, copy_rows)
        sessions = async_sessionmaker(engine)
        async with sessions.begin() as session:
            await session.execute(text(f'SET LOCAL ROLE "{writer}"'))
            with pytest.raises(Exception, match="permission denied"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f"INSERT INTO {schema}.{table}(snapshot_key,{','.join(columns)}) SELECT 10,* FROM {schema}.metadata_stage"
                        )
                    )
            await _publish_metadata_stage(session, schema, table, columns, copy_rows)
            await attach_snapshot_candidates(session, schema, 10, "owned")
            await _assert_metadata_candidate(session, engine, schema, table)
            # Replay exercises the sibling iterable API against the same stored set.
            assert (
                await candidates.publish_snapshot_records(
                    session,
                    schema_name=schema,
                    table=table,
                    snapshot_key=10,
                    build_token="owned",
                    columns=columns,
                    entries=copy_rows,
                    batch_rows=1,
                )
                == 2
            )
            changed_rows = [*copy_rows, copy_rows[-1]]
            with pytest.raises(Exception, match="reference|replay|unique"):
                async with session.begin_nested():
                    await candidates.publish_snapshot_records(
                        session,
                        schema_name=schema,
                        table=table,
                        snapshot_key=10,
                        build_token="owned",
                        columns=columns,
                        entries=changed_rows,
                        batch_rows=1,
                    )
        async with engine.connect() as reader:
            assert await reader.scalar(text(f"SELECT count(*) FROM {schema}.{table} WHERE snapshot_key=10")) == 2


async def _assert_metadata_candidate(session, engine, schema, table):
    """Check completed child indexes and independent readers before commit."""
    assert await session.scalar(text(f"SELECT count(*) FROM {schema}.{table} WHERE snapshot_key=10")) == 2
    candidate_oid = await session.scalar(text(f"SELECT tableoid FROM {schema}.{table} WHERE snapshot_key=10 LIMIT 1"))
    assert await session.scalar(
        text("SELECT count(*) FROM pg_index WHERE indrelid=:oid AND indisvalid AND indisready"),
        {"oid": candidate_oid},
    ) == await session.scalar(
        text("SELECT count(*) FROM pg_index WHERE indrelid=CAST(:parent AS regclass)"), {"parent": f"{schema}.{table}"}
    )
    assert (
        await session.scalar(
            text(
                "SELECT count(*) FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal AND tgtype & 4<>0 AND tgtype & 1<>0"
            ),
            {"oid": candidate_oid},
        )
        == 0
    )
    async with engine.connect() as reader:
        await reader.execute(text("SET LOCAL statement_timeout='1s'"))
        assert await reader.scalar(text(f"SELECT count(*) FROM {schema}.{table} WHERE snapshot_key=10")) == 0


async def _assert_metadata_set_rejections(session, schema):
    """Exercise semantic gaps and native reference failures on isolated sets."""
    from process.ptg_parts import ptg2_snapshot_candidates as candidates

    for table, columns, copy_rows in (
        ("ptg2_v4_npi_scope", ("npi_key", "npi"), ((1, 1234567890),)),
        (
            "ptg2_v4_provider_set_npi_prefix",
            ("provider_set_key", "member_count", "member_digest"),
            ((99, 1, b"a" * 32),),
        ),
        ("ptg2_v4_heavy_owner", _METADATA_CASES[-1][1], (("missing", 1, "bitmap", 1, 0, 1, 1),)),
    ):
        with pytest.raises(Exception, match="reference|foreign key"):
            async with session.begin_nested():
                await candidates.publish_snapshot_records(
                    session,
                    schema_name=schema,
                    table=table,
                    snapshot_key=10,
                    build_token="owned",
                    columns=columns,
                    entries=copy_rows,
                    batch_rows=1,
                )


async def _seed_archive_npi_scope(engine, schema, writer, row_count):
    """Keep selected and unrelated native rows in an indexed disposable restore."""
    async with engine.begin() as connection:
        await connection.execute(text(f"CREATE SCHEMA {schema}_archive"))
        await connection.execute(
            text(
                f"CREATE TABLE {schema}_archive.ptg2_v4_npi_scope (LIKE {schema}.ptg2_v4_npi_scope INCLUDING DEFAULTS)"
            )
        )
        await connection.execute(
            text(f"ALTER TABLE {schema}_archive.ptg2_v4_npi_scope ADD PRIMARY KEY(snapshot_key,npi_key)")
        )
        await connection.execute(
            text(
                f"INSERT INTO {schema}_archive.ptg2_v4_npi_scope SELECT 71,n,1234567890+n "
                "FROM generate_series(0,:last_key) AS n"
            ),
            {"last_key": row_count - 1},
        )
        await connection.execute(text(f"INSERT INTO {schema}_archive.ptg2_v4_npi_scope VALUES(72,0,1234567891)"))
        await connection.execute(text(f'GRANT USAGE ON SCHEMA {schema}_archive TO "{writer}"'))
        await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema}_archive TO "{writer}"'))


@pytest.mark.asyncio
@pytest.mark.parametrize("row_count", (0, 1, 4096, 4097))
async def test_metadata_set_validation_and_archive_copy_fail_closed(monkeypatch, row_count):
    """Reject gaps and missing native references; archive replay uses protected COPY."""
    from process.ptg_parts import result_archive_adoption as adoption

    async with _candidate_database(monkeypatch) as (engine, schema, writer):
        sessions = async_sessionmaker(engine)
        async with sessions.begin() as session:
            await session.execute(text(f'SET LOCAL ROLE "{writer}"'))
            await _assert_metadata_set_rejections(session, schema)
        try:
            await _seed_archive_npi_scope(engine, schema, writer, row_count)
            async with sessions.begin() as session:
                await session.execute(text(f'SET LOCAL ROLE "{writer}"'))
                for _ in range(2):
                    await adoption._copy_rekeyed_table(
                        session,
                        schema_name=schema,
                        staging_schema_name=schema + "_archive",
                        table_name="ptg2_v4_npi_scope",
                        source_snapshot_key=71,
                        destination_snapshot_key=10,
                        build_token="owned",
                    )
                await attach_snapshot_candidates(session, schema, 10, "owned")
                actual = (
                    await session.execute(
                        text(f"SELECT count(*),min(npi),max(npi) FROM {schema}.ptg2_v4_npi_scope WHERE snapshot_key=10")
                    )
                ).one()
                assert tuple(actual) == (
                    row_count,
                    1234567890 if row_count else None,
                    1234567889 + row_count if row_count else None,
                )
                assert (
                    await session.scalar(
                        text("SELECT count(*) FROM pg_cursors WHERE position(:restore IN statement)>0"),
                        {"restore": schema + "_archive"},
                    )
                    == 0
                )
                await session.execute(text("SET LOCAL ROLE NONE"))
                await session.execute(text(f"DROP TABLE {schema}_archive.ptg2_v4_npi_scope"))
                await session.execute(text(f"DROP SCHEMA {schema}_archive"))
        finally:
            async with engine.begin() as connection:
                await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema}_archive CASCADE"))


@asynccontextmanager
async def _permission_database(monkeypatch):
    roles_by_kind = {kind: "ptg_permission_" + uuid4().hex for kind in ("column", "map", "tax", "member", "definer")}
    original_schema = _schema
    setup_engines = []

    async def scoped_schema(connection, schema, writer):
        setup_engines.append(connection.engine)
        await original_schema(connection, schema, writer)
        for role in roles_by_kind.values():
            await connection.exec_driver_sql(f'CREATE ROLE "{role}" NOLOGIN')
            await connection.exec_driver_sql(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
        await connection.exec_driver_sql(
            f'GRANT INSERT(tin_key) ON "{schema}".ptg2_provider_tax_identity TO "{roles_by_kind["column"]}"'
        )
        await connection.exec_driver_sql(
            f'GRANT INSERT ON "{schema}".ptg2_v4_snapshot_map_pack TO "{roles_by_kind["map"]}"'
        )
        await connection.exec_driver_sql(
            f'GRANT INSERT ON "{schema}".ptg2_provider_tax_identity TO "{roles_by_kind["tax"]}"'
        )
        await connection.exec_driver_sql(f'GRANT "{roles_by_kind["map"]}" TO "{roles_by_kind["member"]}"')

    monkeypatch.setitem(globals(), "_schema", scoped_schema)
    try:
        async with _candidate_database(monkeypatch) as (engine, schema, writer):
            yield engine, schema, writer, roles_by_kind
    finally:
        if setup_engines:
            engine = setup_engines[0]
            async with engine.begin() as connection:
                for role in roles_by_kind.values():
                    await connection.exec_driver_sql(f'DROP ROLE IF EXISTS "{role}"')
            await engine.dispose()


@pytest.mark.asyncio
async def test_permission_setup_failure_removes_committed_roles(monkeypatch):
    """A failure before the fixture yields must remove committed schema and roles."""
    identities = [uuid4() for _ in range(7)]
    schema_name = "ptg_candidate_test_" + identities[5].hex
    role_names = ["ptg_permission_" + identity.hex for identity in identities[:5]]
    role_names.append("ptg_writer_" + identities[6].hex)
    role_query = text("SELECT count(*) FROM pg_roles WHERE rolname=ANY(:roles)")
    setup_engines = []

    async def fail_committed_setup(connection, schema, writer, _monkeypatch):
        setup_engines.append(connection.engine)
        assert (schema, writer) == (schema_name, role_names[-1])
        await connection.commit()
        assert await connection.scalar(role_query, {"roles": role_names}) == len(role_names)
        raise RuntimeError("committed fixture setup failed")

    monkeypatch.setitem(globals(), "uuid4", iter(identities).__next__)
    monkeypatch.setitem(globals(), "_migrate_candidate_schema", fail_committed_setup)
    try:
        with pytest.raises(RuntimeError, match="committed fixture setup failed"):
            async with _permission_database(monkeypatch):
                pytest.fail("failed setup must not yield")
        async with setup_engines[0].connect() as connection:
            assert not await connection.scalar(
                text("SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname=:schema)"), {"schema": schema_name}
            )
            assert await connection.scalar(role_query, {"roles": role_names}) == 0
    finally:
        if setup_engines:
            engine = setup_engines[0]
            async with engine.begin() as connection:
                await connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
                for role in role_names:
                    await connection.exec_driver_sql(f'DROP ROLE IF EXISTS "{role}"')
            await engine.dispose()


async def _assert_tax_candidate_denied(connection, schema):
    with pytest.raises(DBAPIError) as failure:
        async with connection.begin_nested():
            await begin_snapshot_candidate(connection, schema, "ptg2_provider_tax_identity", 10, "owned")
    assert failure.value.orig.sqlstate == "42501"


@pytest.mark.asyncio
async def test_candidate_authority_preserves_table_and_column_scopes(monkeypatch):
    async with _permission_database(monkeypatch) as (engine, schema, _writer, roles_by_kind):
        async with engine.begin() as connection:
            signature = f"{schema}.begin_ptg_snapshot_candidate(text,bigint,text)"
            assert not await connection.scalar(
                text("SELECT has_function_privilege(:role,:signature,'EXECUTE')"),
                {"role": roles_by_kind["column"], "signature": signature},
            )
        for kind in ("map", "member"):
            async with engine.begin() as connection:
                await connection.exec_driver_sql(f'SET LOCAL ROLE "{roles_by_kind[kind]}"')
                assert await begin_snapshot_candidate(connection, schema, "ptg2_v4_snapshot_map_pack", 10, "owned")
                await _assert_tax_candidate_denied(connection, schema)


async def _set_map_writer_membership(engine, roles_by_kind, *, granted):
    """Commit only the map authority change independently of candidate transactions."""
    action, recipient = ("GRANT", "TO") if granted else ("REVOKE", "FROM")
    async with engine.begin() as authority:
        await authority.exec_driver_sql(f'{action} "{roles_by_kind["map"]}" {recipient} "{roles_by_kind["member"]}"')


async def _assert_revoked_map_writes(connection, schema, candidate):
    """Direct leaf grants and unrelated wrapper grants cannot outlive the parent role."""
    from process.ptg_parts.ptg2_lifecycle_lock import lifecycle_database_sqlstate

    for statement in (
        f"INSERT INTO {schema}.{candidate} SELECT * FROM {schema}.{candidate}",
        f"SELECT {schema}.finish_ptg_snapshot_candidate('{candidate}',1)",
    ):
        with pytest.raises(DBAPIError) as failure:
            async with connection.begin_nested():
                await connection.execute(text(statement))
        assert lifecycle_database_sqlstate(failure.value) == "42501"
    with pytest.raises(Exception) as failure:
        async with connection.begin_nested():
            driver = await candidate_driver(connection)
            await driver.copy_records_to_table(
                candidate,
                schema_name=schema,
                records=[(10, "a", 1, 4, 0, 4, 0, 1, 1, 1, b"m" * 32)],
            )
    assert lifecycle_database_sqlstate(failure.value) == "42501"


async def _assert_candidate_wrapper_permissions(connection, schema, candidate):
    """Unrelated tax authority retains public wrappers but not the private checker."""
    for signature in (
        "begin_ptg_snapshot_candidate(text,bigint,text)",
        "finish_ptg_snapshot_candidate(text,bigint)",
        "read_ptg_snapshot_candidates(bigint,text)",
        "attach_ptg_snapshot_candidates(bigint,text)",
    ):
        assert await connection.scalar(
            text("SELECT has_function_privilege(current_user,:signature,'EXECUTE')"),
            {"signature": f"{schema}.{signature}"},
        )
    assert await connection.scalar(
        text("SELECT has_table_privilege(current_user,:relation,'INSERT')"),
        {"relation": f"{schema}.{candidate}"},
    )
    assert not await connection.scalar(
        text("SELECT has_function_privilege(current_user,:signature,'EXECUTE')"),
        {"signature": f"{schema}.ptg_snapshot_writer_authorized(text,name)"},
    )


async def _assert_revoked_map_publication(connection, schema):
    """Reject both prepared reads and final attachment without losing the transaction."""
    for function in ("read_ptg_snapshot_candidates", "attach_ptg_snapshot_candidates"):
        with pytest.raises(DBAPIError) as failure:
            async with connection.begin_nested():
                await connection.execute(text(f"SELECT {schema}.{function}(10,'owned')"))
        assert failure.value.orig.sqlstate == "42501"


async def _assert_revoked_prepared_map(engine, schema, roles_by_kind, candidate):
    """Revoke committed authority after finish, then roll back the refused publication."""
    async with engine.connect() as connection:
        transaction = await connection.begin()
        try:
            await connection.exec_driver_sql(f'SET LOCAL ROLE "{roles_by_kind["member"]}"')
            assert await finish_snapshot_candidate(connection, schema, candidate, 1) == 1
            await _set_map_writer_membership(engine, roles_by_kind, granted=False)
            await _assert_revoked_map_publication(connection, schema)
        finally:
            await transaction.rollback()


@pytest.mark.asyncio
async def test_candidate_rechecks_parent_role_after_begin_and_finish(monkeypatch):
    """Committed revocation closes every candidate boundary; restored authority retries."""
    async with _permission_database(monkeypatch) as (engine, schema, _writer, roles_by_kind):
        member = roles_by_kind["member"]
        async with engine.begin() as authority:
            await authority.exec_driver_sql(f'GRANT "{roles_by_kind["tax"]}" TO "{member}"')
            await authority.execute(
                text(f"""
                INSERT INTO {schema}.ptg2_v3_block
                    (block_hash,format_version,object_kind,codec,entry_count,payload)
                VALUES(decode(repeat('6d',32),'hex'),2,'snapshot_coordinate_map_v1','none',1,'x')
            """)
            )
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'SET LOCAL ROLE "{member}"')
            candidate = await begin_snapshot_candidate(connection, schema, "ptg2_v4_snapshot_map_pack", 10, "owned")
            driver = await candidate_driver(connection)
            await driver.copy_records_to_table(
                candidate,
                schema_name=schema,
                records=[(10, "a", 0, 1, 0, 3, 0, 1, 1, 1, b"m" * 32)],
            )
        await _set_map_writer_membership(engine, roles_by_kind, granted=False)
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'SET LOCAL ROLE "{member}"')
            await _assert_candidate_wrapper_permissions(connection, schema, candidate)
            await _assert_revoked_map_writes(connection, schema, candidate)
        await _set_map_writer_membership(engine, roles_by_kind, granted=True)
        await _assert_revoked_prepared_map(engine, schema, roles_by_kind, candidate)
        await _set_map_writer_membership(engine, roles_by_kind, granted=True)
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'SET LOCAL ROLE "{member}"')
            driver = await candidate_driver(connection)
            await driver.copy_records_to_table(
                candidate,
                schema_name=schema,
                records=[(10, "a", 1, 4, 0, 4, 0, 1, 1, 1, b"m" * 32)],
            )
            assert await finish_snapshot_candidate(connection, schema, candidate, 2) == 2
            assert await connection.scalar(text(f"SELECT {schema}.read_ptg_snapshot_candidates(10,'owned')")) == {
                "ptg2_v4_snapshot_map_pack": candidate,
            }
            assert await attach_snapshot_candidates(connection, schema, 10, "owned") == 1
        async with engine.connect() as reader:
            assert (
                await reader.scalar(
                    text(f"SELECT count(*) FROM {schema}.ptg2_v4_snapshot_map_pack WHERE snapshot_key=10")
                )
                == 2
            )


async def _install_definer(engine, schema, owner):
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f'GRANT CREATE ON SCHEMA "{schema}" TO "{owner}"')
        await connection.exec_driver_sql(f'GRANT ALL ON ALL TABLES IN SCHEMA "{schema}" TO "{owner}"')
        table_names = (
            await connection.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname=:schema"), {"schema": schema}
            )
        ).scalars()
        for table in table_names:
            await connection.exec_driver_sql(f'ALTER TABLE "{schema}".{table} OWNER TO "{owner}"')
        for signature in (
            "ptg_snapshot_writer_authorized(text,name)",
            "begin_ptg_snapshot_candidate(text,bigint,text)",
            "finish_ptg_snapshot_candidate(text,bigint)",
            "validate_ptg_snapshot_relationships(text)",
            "ptg_snapshot_relation(text,bigint,text)",
            "attach_ptg_snapshot_candidates(bigint,text)",
            "guard_ptg_snapshot_write()",
            "check_ptg_snapshot_write(oid,text,name)",
        ):
            await connection.exec_driver_sql(f'ALTER FUNCTION "{schema}".{signature} OWNER TO "{owner}"')


@pytest.mark.asyncio
async def test_candidate_non_superuser_definer_keeps_native_privileges(monkeypatch):
    async with _permission_database(monkeypatch) as (engine, schema, writer, roles_by_kind):
        owner = roles_by_kind["definer"]
        await _install_definer(engine, schema, owner)
        async with engine.begin() as connection:
            assert not await connection.scalar(
                text("SELECT rolsuper FROM pg_roles WHERE rolname=:owner"), {"owner": owner}
            )
            await connection.exec_driver_sql(f'SET LOCAL ROLE "{writer}"')
            candidate = await begin_snapshot_candidate(connection, schema, "ptg2_provider_tax_identity", 10, "owned")
            for privilege in ("SELECT", "UPDATE", "INSERT"):
                assert await connection.scalar(
                    text("SELECT has_table_privilege(:owner,:relation,:privilege)"),
                    {"owner": owner, "relation": f"{schema}.{candidate}", "privilege": privilege},
                )
            driver = await candidate_driver(connection)
            await driver.copy_records_to_table(
                candidate,
                schema_name=schema,
                columns=("snapshot_key", "tin_key", "tin_id_128", "tin_hmac_sha256"),
                records=[(10, 0, b"a" * 16, b"a" * 32)],
            )
            assert await finish_snapshot_candidate(connection, schema, candidate, 1) == 1
            await attach_snapshot_candidates(connection, schema, 10, "owned")
        async with engine.begin() as connection:
            await connection.execute(text(f"INSERT INTO {schema}.witness VALUES(10,0)"))
            assert await connection.scalar(text(f"SELECT count(*) FROM {schema}.witness WHERE snapshot_key=10")) == 1


async def _prepare_dictionary_family(connection, schema, snapshot_key, token):
    """Prepare two indexed detached heaps through the real native COPY APIs."""
    from process.ptg_parts.ptg2_snapshot_candidates import copy_candidate_records

    candidate_names = []
    for table_name, columns, records in (
        ("ptg2_v4_npi_scope", ("snapshot_key", "npi_key", "npi"), [(snapshot_key, 0, 1234567890)]),
        (
            "ptg2_v4_provider_component",
            ("snapshot_key", "component_key", "component_global_id_128"),
            [(snapshot_key, 0, b"c" * 16)],
        ),
    ):
        candidate = await begin_snapshot_candidate(connection, schema, table_name, snapshot_key, token)
        assert await copy_candidate_records(connection, schema, candidate, columns, records) == 1
        assert await finish_snapshot_candidate(connection, schema, candidate, 1) == 1
        candidate_names.append(candidate)
    return candidate_names


async def _dictionary_index_oids(connection, schema, candidate_names):
    """Capture every finished physical index so attachment cannot rebuild it."""
    index_oids_by_candidate = {}
    for candidate_name in candidate_names:
        index_oids = await connection.scalar(
            text(
                "SELECT array_agg(indexrelid ORDER BY indexrelid) FROM pg_index "
                "WHERE indrelid=CAST(:relation AS regclass) AND indisvalid AND indisready"
            ),
            {"relation": f"{schema}.{candidate_name}"},
        )
        assert len(index_oids) == 2
        index_oids_by_candidate[candidate_name] = tuple(index_oids)
    return index_oids_by_candidate


@pytest.mark.asyncio
async def test_detached_family_preparation_does_not_hold_parent_publication_locks(monkeypatch):
    """A second snapshot can load and finish while the first indexed family waits."""
    async with _candidate_database(monkeypatch) as (engine, schema, writer):
        async with engine.begin() as connection:
            await connection.execute(
                text(f"INSERT INTO {schema}.ptg2_v3_snapshot_layout VALUES(11,'next','shared_blocks_v4','building')")
            )
            await connection.execute(
                text(f"INSERT INTO {schema}.ptg2_v4_snapshot_map_root(snapshot_key,state) VALUES(11,'building')")
            )
        async with engine.begin() as first:
            await first.exec_driver_sql(f'SET LOCAL ROLE "{writer}"')
            candidates = await _prepare_dictionary_family(first, schema, 10, "owned")
            before = await _dictionary_index_oids(first, schema, candidates)
            assert (
                await first.scalar(
                    text("""
                SELECT count(*) FROM pg_locks held
                  JOIN pg_class relation ON relation.oid=held.relation
                  JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
                 WHERE held.pid=pg_backend_pid() AND namespace.nspname=:schema
                   AND relation.relkind='p' AND held.mode IN ('ShareUpdateExclusiveLock','AccessExclusiveLock')
            """),
                    {"schema": schema},
                )
                == 0
            )
            async with engine.begin() as second:
                await second.exec_driver_sql(f'SET LOCAL ROLE "{writer}"')
                await second.exec_driver_sql("SET LOCAL statement_timeout='1s'")
                await _prepare_dictionary_family(second, schema, 11, "next")
            assert await first.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_v4_npi_scope")) == 0
            assert await attach_snapshot_candidates(first, schema, 10, "owned") == 2
            assert await _dictionary_index_oids(first, schema, candidates) == before
        async with engine.connect() as reader:
            assert (
                await reader.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_v4_npi_scope WHERE snapshot_key=10")) == 1
            )
            assert (
                await reader.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_v4_npi_scope WHERE snapshot_key=11")) == 0
            )


async def _assert_busy_prepared_family_stays_detached(engine, publisher, schema):
    """Reject a busy last parent before any family member becomes visible."""
    async with engine.begin() as holder:
        await holder.execute(
            text(f"LOCK TABLE ONLY {schema}.ptg2_v4_provider_component IN SHARE UPDATE EXCLUSIVE MODE")
        )
        with pytest.raises(DBAPIError) as failure:
            async with publisher.begin_nested():
                await asyncio.wait_for(attach_snapshot_candidates(publisher, schema, 10, "owned"), timeout=0.75)
        assert failure.value.orig.sqlstate == "55P03"
        async with engine.connect() as reader:
            for table_name in ("ptg2_v4_npi_scope", "ptg2_v4_provider_component"):
                assert (
                    await reader.scalar(text(f"SELECT count(*) FROM {schema}.{table_name} WHERE snapshot_key=10")) == 0
                )
            assert (
                await reader.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_snapshot_candidate WHERE published")) == 0
            )
            assert (
                await reader.scalar(
                    text("""
                SELECT count(*) FROM pg_locks held JOIN pg_class relation ON relation.oid=held.relation
                JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
                WHERE namespace.nspname=:schema AND NOT held.granted
            """),
                    {"schema": schema},
                )
                == 0
            )


@pytest.mark.asyncio
async def test_busy_family_publication_rolls_back_without_queued_parent_lock(monkeypatch):
    """A busy final parent leaves the frozen family detached for a same-transaction retry."""
    async with _candidate_database(monkeypatch) as (engine, schema, writer):
        async with engine.begin() as publisher:
            await publisher.exec_driver_sql(f'SET LOCAL ROLE "{writer}"')
            candidates = await _prepare_dictionary_family(publisher, schema, 10, "owned")
            before = await _dictionary_index_oids(publisher, schema, candidates)
            await _assert_busy_prepared_family_stays_detached(engine, publisher, schema)
            assert await attach_snapshot_candidates(publisher, schema, 10, "owned") == 2
            assert await _dictionary_index_oids(publisher, schema, candidates) == before
        async with engine.connect() as reader:
            for table_name in ("ptg2_v4_npi_scope", "ptg2_v4_provider_component"):
                assert (
                    await reader.scalar(text(f"SELECT count(*) FROM {schema}.{table_name} WHERE snapshot_key=10")) == 1
                )


async def _assert_global_writer_cannot_mutate_snapshot(connection, schema, relation):
    """Global table-write membership cannot bypass snapshot statement admission."""
    from process.ptg_parts.ptg2_lifecycle_lock import lifecycle_database_sqlstate

    for statement in (
        f"INSERT INTO {schema}.{relation}(snapshot_key,npi_key,npi) VALUES(10,9,1234567899)",
        f"UPDATE {schema}.{relation} SET npi=1234567899 WHERE snapshot_key=10",
        f"DELETE FROM {schema}.{relation} WHERE snapshot_key=10",
        f"TRUNCATE {schema}.{relation}",
    ):
        with pytest.raises(DBAPIError) as failure:
            async with connection.begin_nested():
                await connection.execute(text(statement))
        assert lifecycle_database_sqlstate(failure.value) == "42501"
    with pytest.raises(Exception) as failure:
        async with connection.begin_nested():
            driver = await candidate_driver(connection)
            await driver.copy_records_to_table(
                relation,
                schema_name=schema,
                columns=("snapshot_key", "npi_key", "npi"),
                records=[(10, 9, 1234567899)],
            )
    assert lifecycle_database_sqlstate(failure.value) == "42501"


@pytest.mark.asyncio
async def test_global_table_writer_cannot_bypass_frozen_snapshot_guards(monkeypatch):
    """Broad inherited DML still rejects parent, historical and frozen-leaf writes."""
    async with _candidate_database(monkeypatch) as (engine, schema, writer):
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                await connection.exec_driver_sql(f'GRANT pg_write_all_data,pg_read_all_data TO "{writer}"')
                await connection.exec_driver_sql(f'SET LOCAL ROLE "{writer}"')
                candidate_names = await _prepare_dictionary_family(connection, schema, 10, "owned")
                await _assert_global_writer_cannot_mutate_registry(connection, schema)
                await _assert_global_writer_cannot_mutate_snapshot(connection, schema, candidate_names[0])
                await attach_snapshot_candidates(connection, schema, 10, "owned")
                for relation in ("ptg2_v4_npi_scope", "ptg2_v4_npi_scope_history", candidate_names[0]):
                    await _assert_global_writer_cannot_mutate_snapshot(connection, schema, relation)
                assert (
                    await connection.scalar(text(f"SELECT npi FROM {schema}.ptg2_v4_npi_scope WHERE snapshot_key=10"))
                    == 1234567890
                )
            finally:
                await transaction.rollback()


@pytest.mark.asyncio
async def test_candidate_commit_gap_requires_complete_set_revalidation(monkeypatch):
    """A lost lifecycle fence expires admission; exact revalidation permits attachment."""
    async with _candidate_database(monkeypatch) as (engine, schema, writer):
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'SET LOCAL ROLE "{writer}"')
            candidates = await _prepare_dictionary_family(connection, schema, 10, "owned")
            original_indexes = await _dictionary_index_oids(connection, schema, candidates)
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f'SET LOCAL ROLE "{writer}"')
            with pytest.raises(DBAPIError) as failure:
                async with connection.begin_nested():
                    await attach_snapshot_candidates(connection, schema, 10, "owned")
            assert failure.value.orig.sqlstate == "55000"
            assert "validation_expired" in str(failure.value)
            assert (
                await connection.scalar(text(f"SELECT count(*) FROM {schema}.ptg2_v4_npi_scope WHERE snapshot_key=10"))
                == 0
            )
            await _prepare_dictionary_family(connection, schema, 10, "owned")
            assert await attach_snapshot_candidates(connection, schema, 10, "owned") == 2
            assert await _dictionary_index_oids(connection, schema, candidates) == original_indexes


async def _assert_global_writer_cannot_mutate_registry(connection, schema):
    """Global data privileges cannot forge admission, lifecycle or completion authority."""
    for statement in (
        f"UPDATE {schema}.ptg2_snapshot_candidate SET prepared=true",
        f"DELETE FROM {schema}.ptg2_snapshot_candidate",
        f"TRUNCATE {schema}.ptg2_snapshot_completion_receipt",
        f"INSERT INTO {schema}.ptg2_snapshot_lifecycle_writer VALUES(current_user)",
        f"UPDATE {schema}.ptg2_snapshot_partition_boundary SET writer_roles=ARRAY[current_user]::name[]",
        f"DELETE FROM {schema}.ptg2_snapshot_partition_preparation",
        f"DELETE FROM {schema}.ptg2_snapshot_legacy_build",
    ):
        with pytest.raises(DBAPIError) as failure:
            async with connection.begin_nested():
                await connection.execute(text(statement))
        assert failure.value.orig.sqlstate == "42501"
