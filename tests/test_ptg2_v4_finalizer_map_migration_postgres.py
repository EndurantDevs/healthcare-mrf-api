# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL lifecycle proof for packed V4 finalizer maps."""

from __future__ import annotations

from contextlib import asynccontextmanager
import importlib.util
from pathlib import Path
import re
import uuid

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from tests.ptg2_v4_stale_metadata_postgres_support import (
    postgres_dsn,
    quoted,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260825120000_ptg_v4_finalizer_map_pack.py"
)
_SCHEMA_RE = re.compile(r"^ptg2_v4_finalizer_map_test_[0-9a-f]{24}$")


def _migration():
    spec = importlib.util.spec_from_file_location(
        "ptg2_v4_finalizer_map_postgres_proof",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _run_migration_action(dsn: str, action: str) -> None:
    """Exercise this revision through Alembic's normal asyncpg execution path."""

    engine = create_async_engine(
        dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    )
    try:
        async with engine.begin() as connection:
            def apply(sync_connection) -> None:
                migration = _migration()
                migration.op = Operations(
                    MigrationContext.configure(sync_connection)
                )
                getattr(migration, action)()

            await connection.run_sync(apply)
    finally:
        await engine.dispose()


@asynccontextmanager
async def _database(monkeypatch):
    dsn = postgres_dsn()
    schema_name = "ptg2_v4_finalizer_map_test_" + uuid.uuid4().hex[:24]
    assert _SCHEMA_RE.fullmatch(schema_name)
    schema = quoted(schema_name)
    connection = await asyncpg.connect(dsn)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    try:
        await connection.execute(
            f"""
            CREATE SCHEMA {schema};
            CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
                snapshot_key bigint PRIMARY KEY,
                generation varchar(32) NOT NULL,
                state varchar(16) NOT NULL
            );
            CREATE TABLE {schema}.ptg2_v3_block (
                block_hash bytea PRIMARY KEY,
                format_version smallint NOT NULL,
                object_kind varchar(64) NOT NULL,
                codec varchar(16) NOT NULL,
                entry_count bigint NOT NULL,
                raw_byte_count bigint NOT NULL,
                stored_byte_count bigint NOT NULL,
                payload bytea NOT NULL
            );
            CREATE TABLE {schema}.ptg2_v3_snapshot_block (
                snapshot_key bigint NOT NULL,
                object_kind varchar(64) NOT NULL
            );
            """
        )
        await _run_migration_action(dsn, "upgrade")
        yield dsn, schema_name, connection
    finally:
        if not connection.is_closed():
            await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            await connection.close()


def _seed_map_rows(snapshot_key: int, reverse_first_kind: bool):
    """Build deterministic map, pack, and target rows."""

    kinds = PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    map_rows = []
    pack_rows = []
    for kind_index, object_kind in enumerate(kinds):
        descriptors = ((0, 2), (1, 1)) if kind_index == 0 and reverse_first_kind else ((0, kind_index + 1),)
        for pack_no, block_key in descriptors:
            map_hash = bytes([len(map_rows) + 1]) * 32
            map_rows.append((map_hash, "snapshot_coordinate_map_v1"))
            pack_rows.append(
                (
                    snapshot_key,
                    object_kind,
                    pack_no,
                    block_key,
                    block_key,
                    map_hash,
                )
            )
    target_rows = [
        (bytes([kind_index + 101]) * 32, object_kind)
        for kind_index, object_kind in enumerate(kinds)
    ]
    return map_rows, pack_rows, target_rows


async def _insert_seed_map_rows(
    connection,
    schema: str,
    snapshot_key: int,
    map_rows,
    pack_rows,
    target_rows,
) -> None:
    """Insert CAS blocks, map descriptors, and target anchors."""

    await connection.executemany(
        f"""
        INSERT INTO {schema}.ptg2_v3_block
            (block_hash, format_version, object_kind, codec, entry_count,
             raw_byte_count, stored_byte_count, payload)
        VALUES ($1, 2, $2, 'none', 1, 1, 1, 'x'::bytea)
        """,
        (*map_rows, *target_rows),
    )
    await connection.executemany(
        f"""
        INSERT INTO {schema}.ptg2_v4_finalizer_map_pack
            (snapshot_key, object_kind, pack_no, first_block_key,
             first_fragment_no, last_block_key, last_fragment_no,
             coordinate_count, entry_count, logical_byte_count,
             map_block_hash)
        VALUES ($1, $2, $3, $4, 0, $5, 0, 1, 1, 1, $6)
        """,
        pack_rows,
    )
    await connection.executemany(
        f"""
        INSERT INTO {schema}.ptg2_v4_finalizer_map_target
            (snapshot_key, block_hash)
        VALUES ($1, $2)
        """,
        ((snapshot_key, block_hash) for block_hash, _kind in target_rows),
    )


async def _seed_map(
    connection,
    schema_name: str,
    snapshot_key: int,
    *,
    reverse_first_kind: bool = False,
):
    """Seed one building root and its complete child descriptor set."""

    schema = quoted(schema_name)
    map_rows, pack_rows, target_rows = _seed_map_rows(
        snapshot_key,
        reverse_first_kind,
    )
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_layout
            (snapshot_key, generation, state)
        VALUES ($1, 'shared_blocks_v4', 'building')
        """,
        snapshot_key,
    )
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg2_v4_finalizer_map_root
            (snapshot_key, state, contract, map_format)
        VALUES (
            $1, 'building', 'packed_finalizer_map_v2',
            'packed_coordinate_hash_v1'
        )
        """,
        snapshot_key,
    )
    await _insert_seed_map_rows(
        connection,
        schema,
        snapshot_key,
        map_rows,
        pack_rows,
        target_rows,
    )
    return len(pack_rows), len(target_rows)


async def _seal(connection, schema_name, snapshot_key, pack_count, target_count):
    schema = quoted(schema_name)
    await connection.execute(
        f"""
        UPDATE {schema}.ptg2_v4_finalizer_map_root
           SET state = 'complete',
               map_digest = repeat('d', 32)::bytea,
               canonical_mapping_digest = repeat('c', 32)::bytea,
               canonical_byte_count = $2,
               target_identity_digest = repeat('t', 32)::bytea,
               map_pack_count = $2,
               coordinate_count = $2,
               entry_count = $2,
               logical_byte_count = $2,
               stored_map_byte_count = $2,
               target_block_count = $3,
               completed_at = now()
         WHERE snapshot_key = $1
        """,
        snapshot_key,
        pack_count,
        target_count,
    )


@pytest.mark.asyncio
async def test_finalizer_map_rejects_reversed_pack_sequence(monkeypatch):
    async with _database(monkeypatch) as (_dsn, schema_name, connection):
        pack_count, target_count = await _seed_map(
            connection,
            schema_name,
            1,
            reverse_first_kind=True,
        )

        with pytest.raises(
            asyncpg.CheckViolationError,
            match="ptg2_v4_finalizer_map_pack_sequence_invalid",
        ):
            await _seal(connection, schema_name, 1, pack_count, target_count)


@pytest.mark.asyncio
async def test_finalizer_map_seal_guards_cascade_and_downgrade(monkeypatch):
    async with _database(monkeypatch) as (dsn, schema_name, connection):
        schema = quoted(schema_name)
        pack_count, target_count = await _seed_map(connection, schema_name, 2)
        await _seal(connection, schema_name, 2, pack_count, target_count)

        guarded_statements = (
            (
                f"UPDATE {schema}.ptg2_v4_finalizer_map_pack "
                "SET entry_count = entry_count WHERE snapshot_key = 2",
                "ptg2_v4_finalizer_map_pack_immutable",
            ),
            (
                f"DELETE FROM {schema}.ptg2_v4_finalizer_map_target "
                "WHERE snapshot_key = 2",
                "ptg2_v4_finalizer_map_target_immutable",
            ),
            (
                f"DELETE FROM {schema}.ptg2_v4_finalizer_map_root "
                "WHERE snapshot_key = 2",
                "ptg2_v4_finalizer_map_root_sealed_delete",
            ),
            (
                f"TRUNCATE TABLE {schema}.ptg2_v4_finalizer_map_pack",
                "ptg2_v4_finalizer_map_truncate_forbidden",
            ),
        )
        for statement, error in guarded_statements:
            with pytest.raises(asyncpg.PostgresError, match=error):
                await connection.execute(statement)

        with pytest.raises(
            DBAPIError,
            match="ptg2_v4_finalizer_map_downgrade_requires_empty_root",
        ):
            await _run_migration_action(dsn, "downgrade")

        await connection.execute(
            f"DELETE FROM {schema}.ptg2_v3_snapshot_layout WHERE snapshot_key = 2"
        )
        counts = await connection.fetchrow(
            f"""
            SELECT
              (SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_root),
              (SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_pack),
              (SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_target)
            """
        )
        assert tuple(counts) == (0, 0, 0)

        await _run_migration_action(dsn, "downgrade")
        assert await connection.fetchval(
            "SELECT to_regclass($1)",
            f"{schema_name}.ptg2_v4_finalizer_map_root",
        ) is None
