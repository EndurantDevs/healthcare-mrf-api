# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for exact V4 snapshot and tax-sidecar removal."""

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
from tests.ptg2_provider_tax_identity_postgres_support import (
    insert_candidate_sidecar,
    load_migration,
    quoted,
    run_migration_action,
)
from tests.ptg_source_snapshot_removal_postgres_support import (
    count_rows as _count,
    create_production_shaped_schema as _create_production_shaped_schema,
    insert_shared_snapshots as _insert_shared_snapshots,
)


_TAX_SIDECAR_TABLES = (
    "ptg2_provider_tax_identity_legacy_layout",
    "ptg2_provider_tax_identity_manifest",
    "ptg2_provider_tax_identity",
    "ptg2_provider_group_tax_identity",
)


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


async def _create_v4_map_tables(connection: Any, schema: str) -> None:
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


async def _shape_provider_group_table(connection: Any, schema: str) -> None:
    """Promote the shared-GC fixture table to the current sidecar FK shape."""

    await connection.status(
        f"""
        ALTER TABLE {schema}.ptg2_v3_provider_group
            ADD COLUMN provider_group_key integer,
            ADD COLUMN provider_group_global_id_128 bytea,
            ADD CONSTRAINT ptg2_v4_remove_provider_group_pkey
                PRIMARY KEY (snapshot_key, provider_group_key),
            ADD CONSTRAINT ptg2_v4_remove_provider_group_identity_key
                UNIQUE (snapshot_key, provider_group_global_id_128)
        """
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
        VALUES (10, 'building')
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


async def _insert_provider_groups(connection: Any, schema: str) -> None:
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_provider_group (
            snapshot_key,
            provider_group_key,
            provider_group_global_id_128
        )
        SELECT 10,
               ordinal,
               decode(repeat(to_hex(ordinal), 32), 'hex')
          FROM generate_series(1, 4) AS ordinal
        """
    )


async def _upgrade_tax_sidecar(database: Database, schema_name: str) -> None:
    migration = load_migration()
    assert database.engine is not None
    await run_migration_action(database.engine, migration, "upgrade")
    await insert_candidate_sidecar(
        database.engine,
        schema_name,
        snapshot_key=10,
        group_limit=4,
        bitmap_hex="01",
    )


async def _seal_v4_layout(database: Database, schema: str) -> None:
    async with database.acquire() as connection:
        await connection.status(
            f"""
            UPDATE {schema}.ptg2_v4_snapshot_map_root
               SET state = 'complete'
             WHERE snapshot_key = 10
            """
        )
        await connection.status(
            f"""
            UPDATE {schema}.ptg2_v3_snapshot_layout
               SET state = 'sealed'
             WHERE snapshot_key = 10
            """
        )


async def _install_v4_layout_fixture(
    database: Database,
    schema_name: str,
) -> int:
    """Build one shared V4 layout through the current tax-sidecar schema."""

    schema = quoted(schema_name)
    await _insert_shared_snapshots(database, schema_name)
    target_block, map_block = _v4_blocks()
    async with database.acquire() as connection:
        await _add_v4_block_fields(connection, schema)
        await _create_v4_map_tables(connection, schema)
        await _shape_provider_group_table(connection, schema)
        await connection.status(
            f"""
            UPDATE {schema}.ptg2_v3_snapshot_layout
               SET generation = :generation,
                   state = 'building'
             WHERE snapshot_key = 10
            """,
            generation=PTG2_V4_SHARED_GENERATION,
        )
        await _install_v4_manifests(connection, schema)
        await _persist_v4_blocks(
            connection,
            schema,
            (target_block, map_block),
        )
        await _persist_v4_map(
            connection,
            schema,
            target_block,
            map_block,
        )
        await _insert_provider_groups(connection, schema)

    await _upgrade_tax_sidecar(database, schema_name)
    await _seal_v4_layout(database, schema)
    return target_block.stored_byte_count + map_block.stored_byte_count


async def _sidecar_counts(
    database: Database,
    schema_name: str,
) -> dict[str, int]:
    return {
        table_name: await _count(database, schema_name, table_name)
        for table_name in _TAX_SIDECAR_TABLES
    }


async def _assert_first_removal_preserves_layout(
    database: Database,
    schema_name: str,
    first: dict[str, Any],
    initial_sidecar_counts: dict[str, int],
) -> None:
    assert first["storage_generation"] == PTG2_V4_SHARED_GENERATION
    assert first["released_shared_layouts"] == 0
    assert first["queued_shared_block_candidates"] == 0
    assert first["physical_cleanup"] == "deferred"
    assert await _count(database, schema_name, "ptg2_v3_snapshot_layout") == 1
    assert await _count(database, schema_name, "ptg2_v4_snapshot_map_root") == 1
    assert await _count(database, schema_name, "ptg2_v4_snapshot_map_pack") == 1
    assert await _sidecar_counts(database, schema_name) == initial_sidecar_counts
    assert await _count(database, schema_name, "ptg2_v3_gc_candidate") == 0


async def _assert_second_removal_releases_layout(
    database: Database,
    schema_name: str,
    second: dict[str, Any],
    stored_bytes: int,
) -> None:
    assert second["released_shared_layouts"] == 1
    assert second["queued_shared_block_candidates"] == 2
    assert second["queued_shared_block_bytes"] == stored_bytes
    assert second["physical_cleanup"] == "released"
    assert await _count(database, schema_name, "ptg2_snapshot") == 0
    assert await _count(database, schema_name, "ptg2_v3_snapshot_layout") == 0
    assert await _count(database, schema_name, "ptg2_v4_snapshot_map_root") == 0
    assert await _count(database, schema_name, "ptg2_v4_snapshot_map_pack") == 0
    assert set((await _sidecar_counts(database, schema_name)).values()) == {0}
    assert await _count(database, schema_name, "ptg2_v3_gc_candidate") == 2
    assert await _count(database, schema_name, "ptg2_v3_block") == 2


@pytest.mark.asyncio
async def test_targeted_v4_removal_cascades_sidecars_only_after_last_binding(
    monkeypatch,
) -> None:
    """Tax sidecars remain shared until the final V4 binding is removed."""

    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 "
            "for the isolated PostgreSQL test"
        )

    database = Database()
    schema_name = f"ptg2_snapshot_removal_{uuid.uuid4().hex}"
    schema = quoted(schema_name)
    await database.connect()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    monkeypatch.setattr(source_snapshot_control, "db", database)
    try:
        await _create_production_shaped_schema(database, schema_name)
        stored_bytes = await _install_v4_layout_fixture(database, schema_name)
        initial_sidecar_counts = await _sidecar_counts(database, schema_name)
        assert initial_sidecar_counts == {
            "ptg2_provider_tax_identity_legacy_layout": 1,
            "ptg2_provider_tax_identity_manifest": 1,
            "ptg2_provider_tax_identity": 1,
            "ptg2_provider_group_tax_identity": 4,
        }

        first = await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id=" shared-a ",
            source_key=" source_a ",
        )
        await _assert_first_removal_preserves_layout(
            database,
            schema_name,
            first,
            initial_sidecar_counts,
        )

        second = await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="shared-b",
            source_key="source_b",
        )

        await _assert_second_removal_releases_layout(
            database,
            schema_name,
            second,
            stored_bytes,
        )
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_targeted_v4_removal_rejects_missing_binding_before_delete(
    monkeypatch,
) -> None:
    """A corrupt V4 binding cannot turn targeted removal into metadata deletion."""

    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 "
            "for the isolated PostgreSQL test"
        )

    database = Database()
    schema_name = f"ptg2_snapshot_removal_{uuid.uuid4().hex}"
    schema = quoted(schema_name)
    await database.connect()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
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

        assert await _count(database, schema_name, "ptg2_snapshot") == 2
        assert await _count(database, schema_name, "ptg2_v3_snapshot_layout") == 1
        assert await _count(database, schema_name, "ptg2_v4_snapshot_map_root") == 1
        assert (await _sidecar_counts(database, schema_name))[
            "ptg2_provider_group_tax_identity"
        ] == 4
        assert await _count(database, schema_name, "ptg2_v3_block") == 2
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()
