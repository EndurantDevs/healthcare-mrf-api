"""PostgreSQL proof for short durable PTG block-build pins."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process.ptg_parts import ptg2_block_build_pins as build_pins
from process.ptg_parts import ptg2_shared_gc as shared_gc
from process.ptg_parts import ptg2_shared_publish as shared_publish
from process.ptg_parts.ptg2_block_build_pins import (
    delete_shared_block_build_pins,
)
from process.ptg_parts.ptg2_shared_blocks import shared_block_hash
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _load_migration,
    _quote,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260810130000_ptg2_block_build_pins.py"
)


def _sqlalchemy_dsn() -> str:
    return _dsn().replace("postgresql://", "postgresql+asyncpg://", 1)


async def _create_pin_base_tables(connection, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
            snapshot_key bigint PRIMARY KEY,
            build_token varchar(96) NOT NULL,
            generation varchar(32) NOT NULL,
            state varchar(16) NOT NULL
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_block (
            block_hash bytea PRIMARY KEY,
            format_version smallint NOT NULL,
            object_kind varchar(64) NOT NULL,
            codec varchar(16) NOT NULL,
            entry_count bigint NOT NULL,
            raw_byte_count bigint NOT NULL,
            stored_byte_count bigint NOT NULL,
            payload bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_gc_candidate (
            block_hash bytea PRIMARY KEY,
            eligible_at timestamptz NOT NULL,
            queued_at timestamptz NOT NULL
        )
        """,
    )
    for statement in statements:
        await connection.execute(text(statement))


async def _create_pin_mapping_tables(connection, schema: str) -> None:
    for statement in (
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_block (
            snapshot_key bigint NOT NULL,
            object_kind varchar(64) NOT NULL,
            block_key bigint NOT NULL,
            fragment_no integer NOT NULL,
            entry_count bigint NOT NULL,
            block_hash bytea NOT NULL,
            PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_block_build_pin (
            snapshot_key bigint NOT NULL REFERENCES
                {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
            build_token varchar(96) NOT NULL,
            pin_token varchar(96) NOT NULL,
            block_hash bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            heartbeat_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            lease_until timestamptz NOT NULL,
            PRIMARY KEY (snapshot_key, pin_token, block_hash)
        )
        """,
    ):
        await connection.execute(text(statement))


async def _seed_pin_layouts(connection, schema: str) -> None:
    for snapshot_key, build_token in ((1, "source-0"), (2, "source-1")):
        await connection.execute(
            text(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_layout
                    (snapshot_key, build_token, generation, state)
                VALUES (:snapshot_key, :build_token, 'shared_blocks_v4', 'building')
                """
            ),
            {"snapshot_key": snapshot_key, "build_token": build_token},
        )


async def _seed_shared_block(
    connection,
    schema: str,
    block_hash: bytes,
    block_payload: bytes,
) -> None:
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_block
                (block_hash, format_version, object_kind, codec, entry_count,
                 raw_byte_count, stored_byte_count, payload)
            VALUES (:block_hash, 2, 'v4_relation_members_v1', 'none', 1,
                    :byte_count, :byte_count, :payload)
            """
        ),
        {
            "block_hash": block_hash,
            "byte_count": len(block_payload),
            "payload": block_payload,
        },
    )


async def _create_pin_stages(
    connection,
    schema: str,
    block_hash: bytes,
    block_payload: bytes,
) -> None:
    for stage_suffix in ("source0", "source1"):
        await connection.execute(
            text(
                f"""
                CREATE UNLOGGED TABLE {schema}.block_stage_{stage_suffix} (
                    block_hash bytea NOT NULL,
                    format_version smallint NOT NULL,
                    object_kind varchar(64) NOT NULL,
                    block_key bigint NOT NULL,
                    fragment_no integer NOT NULL,
                    entry_count bigint NOT NULL,
                    codec varchar(16) NOT NULL,
                    raw_byte_count bigint NOT NULL,
                    stored_byte_count bigint NOT NULL,
                    payload bytea
                )
                """
            )
        )
        await connection.execute(
            text(
                f"""
                INSERT INTO {schema}.block_stage_{stage_suffix}
                    (block_hash, format_version, object_kind, block_key,
                     fragment_no, entry_count, codec, raw_byte_count,
                     stored_byte_count, payload)
                VALUES (:block_hash, 2, 'v4_relation_members_v1', 0, 0, 1,
                        'none', :byte_count, :byte_count, NULL)
                """
            ),
            {"block_hash": block_hash, "byte_count": len(block_payload)},
        )


async def _create_pin_schema(connection, schema: str) -> bytes:
    """Create the disposable block-pin schema and shared fixture."""
    block_payload = b"shared-cas-payload"
    block_hash = shared_block_hash(
        format_version=2,
        object_kind="v4_relation_members_v1",
        codec="none",
        payload=block_payload,
    )
    await _create_pin_base_tables(connection, schema)
    await _create_pin_mapping_tables(connection, schema)
    await _seed_pin_layouts(connection, schema)
    await _seed_shared_block(connection, schema, block_hash, block_payload)
    await _create_pin_stages(connection, schema, block_hash, block_payload)
    return block_hash


async def _prepare_peer_while_source_pin_locked(
    schema_name: str,
    schema: str,
    sessions,
    database,
) -> None:
    await shared_publish.prepare_v4_cas_block_stage(
        schema_name=schema_name,
        stage_table="block_stage_source0",
        snapshot_key=1,
        build_token="source-0",
    )
    async with sessions.begin() as holder:
        await holder.execute(
            text(
                f"SELECT block_hash FROM {schema}.ptg2_block_build_pin "
                "WHERE snapshot_key = 1 FOR UPDATE"
            )
        )
        await shared_publish.prepare_v4_cas_block_stage(
            schema_name=schema_name,
            stage_table="block_stage_source1",
            snapshot_key=2,
            build_token="source-1",
        )
        peer_pin_count = await database.scalar(
            text(
                f"SELECT COUNT(*) FROM {schema}.ptg2_block_build_pin "
                "WHERE snapshot_key = 2"
            )
        )
        assert peer_pin_count == 1


async def _assert_active_pin_excludes_gc(
    schema_name: str,
    schema: str,
    sessions,
    block_hash: bytes,
) -> None:
    async with sessions.begin() as session:
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_v3_gc_candidate "
                "(block_hash, eligible_at, queued_at) "
                "VALUES (:block_hash, "
                "transaction_timestamp() - INTERVAL '1 hour', "
                "transaction_timestamp())"
            ),
            {"block_hash": block_hash},
        )
    async with sessions() as session:
        eligible = (
            await session.execute(
                text(
                    shared_gc._eligible_blocks_sql(
                        schema_name,
                        lock_rows=False,
                    )
                ),
                {"max_bytes": 1_000_000, "max_rows": 10},
            )
        ).all()
        assert eligible == []


async def _assert_pin_release_commit_and_rollback(
    schema_name: str,
    schema: str,
    sessions,
    database,
    block_hash: bytes,
) -> None:
    async with sessions.begin() as session:
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_v3_snapshot_block "
                "(snapshot_key, object_kind, block_key, fragment_no, "
                "entry_count, block_hash) "
                "VALUES (2, 'v4_relation_members_v1', 0, 0, 1, :block_hash)"
            ),
            {"block_hash": block_hash},
        )
        assert await delete_shared_block_build_pins(
            session,
            schema_name=schema_name,
            snapshot_key=2,
            build_token="source-1",
            pin_token="block_stage_source1",
        ) == 1
    with pytest.raises(RuntimeError, match="rollback pin delete"):
        async with sessions.begin() as session:
            assert await delete_shared_block_build_pins(
                session,
                schema_name=schema_name,
                snapshot_key=1,
                build_token="source-0",
                pin_token="block_stage_source0",
            ) == 1
            raise RuntimeError("rollback pin delete")
    assert await database.scalar(
        text(
            f"SELECT COUNT(*) FROM {schema}.ptg2_block_build_pin "
            "WHERE snapshot_key = 1"
        )
    ) == 1


@pytest.mark.asyncio
async def test_pinned_source_does_not_hold_shared_cas_row_or_peer(
    monkeypatch,
) -> None:
    """A source paused after protection cannot block a peer or active-pin GC."""

    schema_name = f"ptg2_block_pin_{uuid.uuid4().hex}"
    schema = _quote(schema_name)
    engine = create_async_engine(_sqlalchemy_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    database = Database(engine=engine, session_factory=sessions)
    monkeypatch.setattr(shared_publish, "db", database)
    monkeypatch.setattr(build_pins, "db", database)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {schema}"))
            block_hash = await _create_pin_schema(connection, schema)

        await _prepare_peer_while_source_pin_locked(
            schema_name,
            schema,
            sessions,
            database,
        )
        await _assert_active_pin_excludes_gc(
            schema_name,
            schema,
            sessions,
            block_hash,
        )
        await _assert_pin_release_commit_and_rollback(
            schema_name,
            schema,
            sessions,
            database,
            block_hash,
        )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await engine.dispose()


def test_block_pin_migration_is_chained_and_gc_guarded(monkeypatch) -> None:
    migration = _load_migration(MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg2_block_pin_contract")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    sql = "\n".join(statements)
    assert migration.down_revision == "20260810120000_ptg2_layout_build_candidates"
    assert "ptg2_block_build_pin" in sql
    assert "PRIMARY KEY (snapshot_key, pin_token, block_hash)" in sql
    assert "ON DELETE CASCADE" in sql
    assert "lease_until" in shared_gc._eligible_blocks_sql("mrf", lock_rows=False)
    assert "ptg2_block_build_pin" in shared_gc._delete_blocks_sql(
        "mrf",
        v4_tables_available=False,
        finalizer_tables_available=False,
    )
