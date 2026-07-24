# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for targeted V4 snapshot retirement and physical release."""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import pytest

from db.connection import Database
from process.ptg_parts import source_snapshot_control
from process.ptg_parts.ptg2_shared_blocks import SharedBlock
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_BLOCK_KIND,
    PTG2_V4_SHARED_GENERATION,
    encode_v4_snapshot_map_pack,
)
from tests.test_ptg_source_snapshot_removal_v3 import (
    _count,
    _create_production_shaped_schema,
    _insert_shared_snapshots,
)


def _quoted(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _v4_blocks() -> tuple[SharedBlock, SharedBlock]:
    """Build one target block and its authenticated packed-map block."""

    target_block = SharedBlock(
        object_kind="v4_test_members",
        block_key=7,
        fragment_no=0,
        entry_count=3,
        codec="none",
        raw_byte_count=6,
        payload=b"target",
    )
    target_reference = target_block.reference()
    map_payload = encode_v4_snapshot_map_pack(
        target_block.object_kind,
        [target_reference],
    )
    map_block = SharedBlock(
        object_kind=PTG2_V4_MAP_BLOCK_KIND,
        block_key=0,
        fragment_no=0,
        entry_count=1,
        codec="none",
        raw_byte_count=len(map_payload),
        payload=map_payload,
    )
    return target_block, map_block


async def _add_v4_block_fields(connection: Any, schema: str) -> None:
    await connection.status(
        f"""
        ALTER TABLE {schema}.ptg2_v3_block
            ADD COLUMN format_version smallint NOT NULL DEFAULT 2,
            ADD COLUMN object_kind varchar(64) NOT NULL DEFAULT 'fixture',
            ADD COLUMN codec varchar(16) NOT NULL DEFAULT 'none',
            ADD COLUMN entry_count bigint NOT NULL DEFAULT 0,
            ADD COLUMN raw_byte_count bigint NOT NULL DEFAULT 0,
            ADD COLUMN payload bytea NOT NULL DEFAULT ''::bytea
        """
    )


async def _create_v4_root_table(connection: Any, schema: str) -> None:
    await connection.status(
        f"""
        CREATE TABLE {schema}.ptg2_v4_snapshot_map_root (
            snapshot_key bigint PRIMARY KEY REFERENCES
                {schema}.ptg2_v3_snapshot_layout(snapshot_key)
                ON DELETE CASCADE,
            state varchar(16) NOT NULL
        )
        """
    )


async def _create_v4_pack_table(connection: Any, schema: str) -> None:
    await connection.status(
        f"""
        CREATE TABLE {schema}.ptg2_v4_snapshot_map_pack (
            snapshot_key bigint NOT NULL REFERENCES
                {schema}.ptg2_v4_snapshot_map_root(snapshot_key)
                ON DELETE CASCADE,
            object_kind varchar(64) NOT NULL,
            pack_no integer NOT NULL,
            first_block_key bigint NOT NULL,
            first_fragment_no integer NOT NULL,
            last_block_key bigint NOT NULL,
            last_fragment_no integer NOT NULL,
            coordinate_count integer NOT NULL,
            entry_count bigint NOT NULL,
            logical_byte_count bigint NOT NULL,
            map_block_hash bytea NOT NULL REFERENCES
                {schema}.ptg2_v3_block(block_hash) ON DELETE RESTRICT,
            PRIMARY KEY (snapshot_key, object_kind, pack_no)
        )
        """
    )


async def _mark_layout_v4(connection: Any, schema: str) -> None:
    await connection.status(
        f"""
        UPDATE {schema}.ptg2_v3_snapshot_layout
           SET generation = :generation
         WHERE snapshot_key = 10
        """,
        generation=PTG2_V4_SHARED_GENERATION,
    )


async def _install_v4_manifests(connection: Any, schema: str) -> None:
    for snapshot_id, source_key in (
        ("shared-a", "source_a"),
        ("shared-b", "source_b"),
    ):
        manifest_by_field = {
            "serving_index": {
                "arch_version": "postgres_binary_v3",
                "type": "ptg2_shared_blocks_v4",
                "storage_generation": PTG2_V4_SHARED_GENERATION,
                "provider_scope_strategy": "postgres_packed_graph_v4",
                "shared_block_layout": "packed_snapshot_maps_v4",
                "shared_snapshot_key": 10,
                "source_key": source_key,
            }
        }
        await connection.status(
            f"""
            UPDATE {schema}.ptg2_snapshot
               SET manifest = CAST(:manifest AS jsonb)
             WHERE snapshot_id = :snapshot_id
            """,
            snapshot_id=snapshot_id,
            manifest=json.dumps(manifest_by_field),
        )


async def _persist_v4_blocks(
    connection: Any,
    schema: str,
    blocks: tuple[SharedBlock, ...],
) -> None:
    for block in blocks:
        await connection.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_block
                (block_hash, stored_byte_count, format_version, object_kind,
                 codec, entry_count, raw_byte_count, payload)
            VALUES
                (:block_hash, :stored_byte_count, :format_version,
                 :object_kind, :codec, :entry_count, :raw_byte_count,
                 :payload)
            """,
            block_hash=block.block_hash,
            stored_byte_count=block.stored_byte_count,
            format_version=block.format_version,
            object_kind=block.object_kind,
            codec=block.codec,
            entry_count=block.entry_count,
            raw_byte_count=block.raw_byte_count,
            payload=block.payload,
        )


async def _persist_v4_map(
    connection: Any,
    schema: str,
    target_block: SharedBlock,
    map_block: SharedBlock,
) -> None:
    target_reference = target_block.reference()
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v4_snapshot_map_root
            (snapshot_key, state)
        VALUES (10, 'complete')
        """
    )
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v4_snapshot_map_pack
            (snapshot_key, object_kind, pack_no,
             first_block_key, first_fragment_no,
             last_block_key, last_fragment_no,
             coordinate_count, entry_count, logical_byte_count,
             map_block_hash)
        VALUES
            (10, :object_kind, 0, :block_key, :fragment_no,
             :block_key, :fragment_no, 1, :entry_count,
             :logical_byte_count, :map_block_hash)
        """,
        object_kind=target_reference.object_kind,
        block_key=target_reference.block_key,
        fragment_no=target_reference.fragment_no,
        entry_count=target_reference.entry_count,
        logical_byte_count=target_reference.raw_byte_count,
        map_block_hash=map_block.block_hash,
    )


async def _install_v4_layout_fixture(
    database: Database,
    schema_name: str,
) -> int:
    """Turn the shared two-snapshot fixture into one authenticated V4 layout."""

    schema = _quoted(schema_name)
    await _insert_shared_snapshots(database, schema_name)
    target_block, map_block = _v4_blocks()
    async with database.acquire() as connection:
        await _add_v4_block_fields(connection, schema)
        await _create_v4_root_table(connection, schema)
        await _create_v4_pack_table(connection, schema)
        await _mark_layout_v4(connection, schema)
        await _install_v4_manifests(connection, schema)
        await _persist_v4_blocks(connection, schema, (target_block, map_block))
        await _persist_v4_map(connection, schema, target_block, map_block)
    return target_block.stored_byte_count + map_block.stored_byte_count


@pytest.mark.asyncio
async def test_targeted_v4_removal_releases_only_the_last_bound_layout(monkeypatch):
    """The control path retains shared V4 data until its last binding."""

    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 "
            "for the isolated PostgreSQL test"
        )

    database = Database()
    schema_name = f"ptg2_v4_snapshot_removal_{uuid.uuid4().hex}"
    schema = _quoted(schema_name)
    await database.connect()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setattr(source_snapshot_control, "db", database)
    try:
        await _create_production_shaped_schema(database, schema_name)
        stored_bytes = await _install_v4_layout_fixture(database, schema_name)

        first = await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id=" shared-a ",
            source_key=" source_a ",
        )

        assert first["storage_generation"] == PTG2_V4_SHARED_GENERATION
        assert first["released_shared_layouts"] == 0
        assert first["queued_shared_block_candidates"] == 0
        assert first["physical_cleanup"] == "deferred"
        assert await _count(database, schema_name, "ptg2_v3_snapshot_layout") == 1
        assert await _count(database, schema_name, "ptg2_v4_snapshot_map_root") == 1
        assert await _count(database, schema_name, "ptg2_v4_snapshot_map_pack") == 1
        assert await _count(database, schema_name, "ptg2_v3_gc_candidate") == 0

        second = await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="shared-b",
            source_key="source_b",
        )

        assert second["released_shared_layouts"] == 1
        assert second["queued_shared_block_candidates"] == 2
        assert second["queued_shared_block_bytes"] == stored_bytes
        assert second["physical_cleanup"] == "released"
        assert await _count(database, schema_name, "ptg2_snapshot") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_layout") == 0
        assert await _count(database, schema_name, "ptg2_v4_snapshot_map_root") == 0
        assert await _count(database, schema_name, "ptg2_v4_snapshot_map_pack") == 0
        assert await _count(database, schema_name, "ptg2_v3_gc_candidate") == 2
        assert await _count(database, schema_name, "ptg2_v3_block") == 2
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_targeted_v4_removal_rejects_missing_binding_before_delete(
    monkeypatch,
):
    """A corrupt V4 binding cannot turn targeted removal into metadata deletion."""

    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 "
            "for the isolated PostgreSQL test"
        )

    database = Database()
    schema_name = f"ptg2_v4_snapshot_binding_guard_{uuid.uuid4().hex}"
    schema = _quoted(schema_name)
    await database.connect()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setattr(source_snapshot_control, "db", database)
    try:
        await _create_production_shaped_schema(database, schema_name)
        await _install_v4_layout_fixture(database, schema_name)
        async with database.acquire() as connection:
            await connection.status(
                f"""
                DELETE FROM {schema}.ptg2_v3_snapshot_binding
                 WHERE snapshot_id = 'shared-a'
                """
            )

        with pytest.raises(
            ValueError,
            match="missing its shared layout binding",
        ):
            await source_snapshot_control.remove_ptg2_source_snapshot(
                snapshot_id="shared-a",
                source_key="source_a",
            )

        async with database.acquire() as connection:
            retained_snapshot = await connection.first(
                f"""
                SELECT snapshot_id
                  FROM {schema}.ptg2_snapshot
                 WHERE snapshot_id = 'shared-a'
                """
            )
        assert retained_snapshot is not None
        assert await _count(database, schema_name, "ptg2_snapshot") == 2
        assert await _count(database, schema_name, "ptg2_v3_snapshot_layout") == 1
        assert await _count(database, schema_name, "ptg2_v4_snapshot_map_root") == 1
        assert await _count(database, schema_name, "ptg2_v3_block") == 2
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()
