# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Schema helpers for disposable PTG shared-GC PostgreSQL tests."""

from __future__ import annotations

from process.ptg_parts import ptg2_shared_gc as shared_gc


async def _create_gc_layout_schema(connection, schema: str) -> None:
    """Create snapshot ownership and binding relations for GC tests."""

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


async def _insert_gc_finalizer_fixture(
    connection,
    schema: str,
    finalizer_hashes: tuple[bytes, bytes],
) -> None:
    """Anchor one map CAS block and one mapped target CAS block."""

    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v4_finalizer_map_root (snapshot_key, state)
        VALUES (77, 'complete')
        """
    )
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v4_finalizer_map_pack
            (snapshot_key, map_block_hash)
        VALUES (77, :map_block_hash)
        """,
        map_block_hash=finalizer_hashes[0],
    )
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_v4_finalizer_map_target
            (snapshot_key, block_hash)
        VALUES (77, :block_hash)
        """,
        block_hash=finalizer_hashes[1],
    )


async def _insert_gc_build_pins(
    connection,
    schema: str,
    build_token: str,
    block_hashes: tuple[bytes, ...],
) -> None:
    """Pin every owned hash twice and retain one unrelated pin."""

    for pin_token in ("stage-a", "stage-b"):
        for block_hash in block_hashes:
            await connection.status(
                f"""
                INSERT INTO {schema}.ptg2_block_build_pin
                    (snapshot_key, build_token, pin_token, block_hash,
                     lease_until)
                VALUES (77, :build_token, :pin_token, :block_hash,
                        transaction_timestamp() + INTERVAL '1 hour')
                """,
                build_token=build_token,
                pin_token=pin_token,
                block_hash=block_hash,
            )
    await connection.status(
        f"""
        INSERT INTO {schema}.ptg2_block_build_pin
            (snapshot_key, build_token, pin_token, block_hash, lease_until)
        VALUES (78, 'unrelated-build-token', 'unrelated-stage', :block_hash,
                transaction_timestamp() + INTERVAL '1 hour')
        """,
        block_hash=block_hashes[0],
    )


__all__ = [
    "_create_gc_block_schema",
    "_create_gc_layout_schema",
    "_insert_gc_build_pins",
    "_insert_gc_finalizer_fixture",
]
