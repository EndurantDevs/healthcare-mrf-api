# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from process.ptg_parts import ptg2_shared_gc as shared_gc
from process.ptg_parts import ptg2_source_snapshot_gc as source_snapshot_gc
from process.ptg_parts import snapshot_cleanup

from tests.ptg2_shared_gc_test_support import (
    _SharedGCExecutor,
    _SourceGCProjectionExecutor,
    _hash,
    _patch_v4_abandonment_pipeline,
)
from tests.ptg2_shared_gc_schema_support import _create_gc_block_schema

async def _create_gc_layout_schema(connection, schema: str) -> None:
    await connection.status(
        f"CREATE TABLE {schema}.ptg2_snapshot "
        "(snapshot_id varchar(96) PRIMARY KEY, manifest jsonb)"
    )
    await connection.status(
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
            snapshot_key bigint PRIMARY KEY,
            generation varchar(32) NOT NULL,
            state varchar(16) NOT NULL,
            created_at timestamptz NOT NULL,
            heartbeat_at timestamptz NOT NULL,
            lease_until timestamptz
        )
        """
    )
    await connection.status(
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
            snapshot_id varchar(96) PRIMARY KEY
                REFERENCES {schema}.ptg2_snapshot(snapshot_id) ON DELETE CASCADE,
            snapshot_key bigint NOT NULL
                REFERENCES {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE RESTRICT
        )
        """
    )


async def _insert_gc_layout_fixture(connection, schema: str) -> None:
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_layout
            (snapshot_key, generation, state, created_at, heartbeat_at, lease_until)
        VALUES
            (10, :generation, 'sealed', transaction_timestamp(),
             transaction_timestamp(), NULL),
            (20, :generation, 'sealed', transaction_timestamp(),
             transaction_timestamp(), NULL)
        """,
        generation=shared_gc.PTG2_V3_SHARED_GENERATION,
    )
    for table_name in shared_gc.PTG2_V3_DENSE_LAYOUT_TABLES:
        await connection.status(
            f'INSERT INTO {schema}."{table_name}" (snapshot_key) VALUES (10)'
        )
    await connection.status(
        f"INSERT INTO {schema}.ptg2_snapshot (snapshot_id, manifest) "
        "VALUES ('selected-snapshot', '{}'::jsonb)"
    )
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_binding
            (snapshot_id, snapshot_key)
        VALUES ('selected-snapshot', 10)
        """
    )


async def _insert_gc_block_fixture(
    connection,
    schema: str,
    block_hash: bytes,
    unrelated_hash: bytes,
) -> None:
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_block
            (block_hash, stored_byte_count)
        VALUES (:block_hash, 25), (:unrelated_hash, 1000)
        """,
        block_hash=block_hash,
        unrelated_hash=unrelated_hash,
    )
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_block
            (snapshot_key, block_hash)
        VALUES (10, :block_hash), (20, :unrelated_hash)
        """,
        block_hash=block_hash,
        unrelated_hash=unrelated_hash,
    )


async def _release_and_sweep_gc_fixture(
    database: Database,
    schema_name: str,
    schema: str,
    block_hash: bytes,
) -> None:
    async with database.acquire() as connection:
        dry_run = await shared_gc.build_shared_layout_release_plan(
            schema_name=schema_name,
            executor=connection,
            removing_snapshot_ids=("selected-snapshot",),
            all_eligible_layouts=True,
            require_shared=True,
        )
    assert dry_run == shared_gc.PTG2SharedLayoutGCStats(1, 1, 25)
    async with database.acquire() as connection:
        await connection.status(
            f"DELETE FROM {schema}.ptg2_v3_snapshot_binding "
            "WHERE snapshot_id = 'selected-snapshot'"
        )
        released = await shared_gc.release_unbound_ptg2_shared_layouts(
            schema_name=schema_name,
            executor=connection,
            grace_seconds=0,
            require_shared=True,
            layout_keys=(10,),
        )
    assert released == shared_gc.PTG2SharedLayoutGCStats(1, 1, 25)
    async with database.acquire() as connection:
        swept = await shared_gc.sweep_ptg2_shared_blocks(
            schema_name=schema_name,
            executor=connection,
            max_bytes=25,
            require_shared=True,
        )
    assert swept == shared_gc.PTG2SharedBlockSweepPlan((block_hash,), 25)


async def _assert_gc_fixture_storage(database: Database, schema: str) -> None:
    async with database.acquire() as connection:
        assert await connection.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_layout"
        ) == 1
        assert await connection.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_block"
        ) == 1
        assert await connection.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_gc_candidate"
        ) == 0


_V4_ABANDONMENT_TABLE_TEMPLATES = (
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
        snapshot_key bigint PRIMARY KEY,
        generation varchar(32) NOT NULL,
        state varchar(16) NOT NULL,
        build_token varchar(96) NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_layout_fingerprint (
        semantic_fingerprint bytea PRIMARY KEY,
        snapshot_key bigint NOT NULL
            REFERENCES {schema}.ptg2_v3_snapshot_layout(snapshot_key)
            ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_layout_build_candidate (
        snapshot_key bigint PRIMARY KEY
            REFERENCES {schema}.ptg2_v3_snapshot_layout(snapshot_key)
            ON DELETE CASCADE,
        semantic_fingerprint bytea NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_block_build_pin (
        snapshot_key bigint NOT NULL REFERENCES
            {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
        build_token varchar(96) NOT NULL,
        pin_token varchar(96) NOT NULL,
        block_hash bytea NOT NULL,
        lease_until timestamptz NOT NULL,
        PRIMARY KEY (snapshot_key, pin_token, block_hash)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
        snapshot_id varchar(96) PRIMARY KEY,
        snapshot_key bigint NOT NULL
            REFERENCES {schema}.ptg2_v3_snapshot_layout(snapshot_key)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_block (
        block_hash bytea PRIMARY KEY,
        format_version smallint NOT NULL,
        object_kind varchar(64) NOT NULL,
        codec varchar(16) NOT NULL,
        entry_count bigint NOT NULL,
        raw_byte_count bigint NOT NULL,
        stored_byte_count bigint NOT NULL,
        payload bytea NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_block (
        snapshot_key bigint NOT NULL
            REFERENCES {schema}.ptg2_v3_snapshot_layout(snapshot_key)
            ON DELETE CASCADE,
        object_kind varchar(64) NOT NULL,
        block_key bigint NOT NULL,
        fragment_no integer NOT NULL,
        entry_count bigint NOT NULL,
        block_hash bytea NOT NULL
            REFERENCES {schema}.ptg2_v3_block(block_hash),
        PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_gc_candidate (
        block_hash bytea PRIMARY KEY
            REFERENCES {schema}.ptg2_v3_block(block_hash)
            ON DELETE CASCADE,
        eligible_at timestamptz NOT NULL,
        queued_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v4_snapshot_map_root (
        snapshot_key bigint PRIMARY KEY
            REFERENCES {schema}.ptg2_v3_snapshot_layout(snapshot_key)
            ON DELETE CASCADE,
        state varchar(16) NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v4_snapshot_map_pack (
        snapshot_key bigint NOT NULL
            REFERENCES {schema}.ptg2_v4_snapshot_map_root(snapshot_key)
            ON DELETE CASCADE,
        object_kind varchar(64) NOT NULL,
        pack_no integer NOT NULL,
        first_block_key bigint NOT NULL,
        first_fragment_no integer NOT NULL,
        last_block_key bigint NOT NULL,
        last_fragment_no integer NOT NULL,
        coordinate_count bigint NOT NULL,
        entry_count bigint NOT NULL,
        map_block_hash bytea NOT NULL
            REFERENCES {schema}.ptg2_v3_block(block_hash),
        PRIMARY KEY (snapshot_key, object_kind, pack_no)
    )
    """,
)
_V4_ABANDONMENT_RESERVED_TABLES = frozenset(
    {
        "ptg2_v3_snapshot_layout",
        "ptg2_v3_layout_fingerprint",
        "ptg2_layout_build_candidate",
        "ptg2_block_build_pin",
        "ptg2_v3_snapshot_binding",
        "ptg2_v3_block",
        "ptg2_v3_snapshot_block",
        "ptg2_v3_gc_candidate",
    }
)


async def _create_v4_abandonment_schema(
    connection,
    schema: str,
) -> None:
    """Create the isolated lifecycle subset used by the abandonment proof."""

    for table_template in _V4_ABANDONMENT_TABLE_TEMPLATES[:6]:
        await connection.status(table_template.format(schema=schema))
    for table_name in (
        set(shared_gc.PTG2_V3_MIGRATION_OWNED_TABLE_NAMES)
        - _V4_ABANDONMENT_RESERVED_TABLES
    ):
        await connection.status(
            f'CREATE TABLE {schema}."{table_name}" (snapshot_key bigint)'
        )
    for table_template in _V4_ABANDONMENT_TABLE_TEMPLATES[6:]:
        await connection.status(table_template.format(schema=schema))


async def _insert_v4_abandonment_fixture(
    connection,
    schema: str,
    *,
    build_token: str,
) -> tuple[bytes, ...]:
    """Populate one owned V4 layout with three mapped and dense records."""

    block_hashes = tuple(
        _hash(hash_seed)
        for hash_seed in (31, 32, 33)
    )
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_layout
            (snapshot_key, generation, state, build_token)
        VALUES (77, :generation, 'building', :build_token)
        """,
        generation=shared_gc.PTG2_V4_SHARED_GENERATION,
        build_token=build_token,
    )
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_layout_fingerprint
            (semantic_fingerprint, snapshot_key)
        VALUES (:semantic_fingerprint, 77)
        """,
        semantic_fingerprint=_hash(30),
    )
    for block_key, block_hash in enumerate(block_hashes):
        block_payload = bytes([block_key + 1])
        await connection.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_block
                (block_hash, format_version, object_kind, codec, entry_count,
                 raw_byte_count, stored_byte_count, payload)
            VALUES (:block_hash, 2, 'serving', 'none', 1, 1, 1, :payload)
            """,
            block_hash=block_hash,
            payload=block_payload,
        )
        await connection.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_block
                (snapshot_key, object_kind, block_key, fragment_no,
                 entry_count, block_hash)
            VALUES (77, 'serving', :block_key, 0, 1, :block_hash)
            """,
            block_key=block_key,
            block_hash=block_hash,
        )
    for table_name in shared_gc.PTG2_V3_DENSE_LAYOUT_TABLES:
        await connection.status(
            f"""
            INSERT INTO {schema}."{table_name}" (snapshot_key)
            VALUES (77), (77), (77)
            """
        )
    return block_hashes


async def _assert_partial_v4_abandonment(
    database: Database,
    schema: str,
    *,
    build_token: str,
) -> None:
    """Require a canceled first batch to preserve layout reachability."""

    async with database.acquire() as connection:
        assert await connection.scalar(
            f"""
            SELECT build_token
              FROM {schema}.ptg2_v3_snapshot_layout
             WHERE snapshot_key = 77
            """
        ) == shared_gc._owned_v4_abandonment_token(build_token)
        assert await connection.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_gc_candidate"
        ) == 2
        assert await connection.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_block"
        ) == 3


async def _assert_completed_v4_abandonment(
    database: Database,
    schema: str,
    block_hashes: tuple[bytes, ...],
) -> None:
    """Require resumed cleanup to remove layout edges but retain CAS bytes."""

    async with database.acquire() as connection:
        for table_name in (
            "ptg2_v3_snapshot_layout",
            "ptg2_v3_snapshot_block",
        ):
            assert await connection.scalar(
                f'SELECT COUNT(*) FROM {schema}."{table_name}"'
            ) == 0
        assert await connection.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_gc_candidate"
        ) == 3
        assert await connection.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_block"
        ) == 3
        for table_name in shared_gc.PTG2_V3_DENSE_LAYOUT_TABLES:
            assert await connection.scalar(
                f'SELECT COUNT(*) FROM {schema}."{table_name}"'
            ) == 0
        retained_records = await connection.all(
            f"SELECT block_hash FROM {schema}.ptg2_v3_block"
        )
        assert {
            bytes(block_record[0])
            for block_record in retained_records
        } == set(block_hashes)


def _cancel_after_candidate_batch(metric: str, _amount: int) -> None:
    """Cancel immediately after one candidate batch reports committed."""

    assert metric == "candidate_hashes"
    raise asyncio.CancelledError


async def _drop_test_schema_and_disconnect(
    database: Database,
    schema: str,
) -> None:
    """Drop one disposable schema before closing its database pool."""

    try:
        async with database.acquire() as connection:
            await connection.status(
                f"DROP SCHEMA IF EXISTS {schema} CASCADE"
            )
    finally:
        await database.disconnect()
