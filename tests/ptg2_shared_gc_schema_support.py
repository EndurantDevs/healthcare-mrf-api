# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Schema helpers for disposable PTG shared-GC PostgreSQL tests."""

from __future__ import annotations

from process.ptg_parts import ptg2_shared_gc as shared_gc


async def _create_gc_build_tables(connection, schema: str) -> None:
    """Create candidate and live-build-pin relations for GC tests."""

    await connection.status(
        f"""
        CREATE TABLE {schema}.ptg2_layout_build_candidate (
            snapshot_key bigint PRIMARY KEY,
            cleanup_pending_at timestamptz
        )
        """
    )
    await connection.status(
        f"""
        CREATE TABLE {schema}.ptg2_block_build_pin (
            snapshot_key bigint NOT NULL REFERENCES
                {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
            build_token varchar(96) NOT NULL,
            pin_token varchar(96) NOT NULL,
            block_hash bytea NOT NULL,
            lease_until timestamptz NOT NULL,
            PRIMARY KEY (snapshot_key, pin_token, block_hash)
        )
        """
    )


async def _create_gc_storage_tables(connection, schema: str) -> None:
    """Create block, mapping, candidate, and remaining shared relations."""

    await connection.status(
        f"""
        CREATE TABLE {schema}.ptg2_v3_block (
            block_hash bytea PRIMARY KEY,
            stored_byte_count bigint NOT NULL
        )
        """
    )
    await connection.status(
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_block (
            snapshot_key bigint NOT NULL
                REFERENCES {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
            block_hash bytea NOT NULL REFERENCES {schema}.ptg2_v3_block(block_hash),
            PRIMARY KEY (snapshot_key, block_hash)
        )
        """
    )
    await connection.status(
        f"""
        CREATE TABLE {schema}.ptg2_v3_gc_candidate (
            block_hash bytea PRIMARY KEY
                REFERENCES {schema}.ptg2_v3_block(block_hash) ON DELETE CASCADE,
            eligible_at timestamptz NOT NULL,
            queued_at timestamptz NOT NULL
        )
        """
    )
    excluded_tables = {
        "ptg2_v3_snapshot_layout",
        "ptg2_v3_snapshot_binding",
        "ptg2_v3_block",
        "ptg2_v3_snapshot_block",
        "ptg2_v3_gc_candidate",
        "ptg2_layout_build_candidate",
        "ptg2_block_build_pin",
    }
    for table_name in set(shared_gc._SHARED_TABLE_NAMES) - excluded_tables:
        await connection.status(
            f'CREATE TABLE {schema}."{table_name}" (snapshot_key bigint)'
        )


async def _create_gc_block_schema(connection, schema: str) -> None:
    """Create all block-related relations in dependency order."""

    await _create_gc_build_tables(connection, schema)
    await _create_gc_storage_tables(connection, schema)


__all__ = ["_create_gc_block_schema"]
