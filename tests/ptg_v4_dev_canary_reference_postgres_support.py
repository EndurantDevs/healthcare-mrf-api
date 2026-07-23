"""Disposable PostgreSQL fixture for frozen-reference canary proofs."""

from __future__ import annotations

import asyncpg


def quoted(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _snapshot_table_statements(schema: str) -> tuple[str, ...]:
    return (
        f"CREATE SCHEMA {schema}",
        f"""
        CREATE TABLE {schema}.ptg2_snapshot (
            snapshot_id varchar(96) PRIMARY KEY,
            status varchar(32) NOT NULL
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
            snapshot_key bigint PRIMARY KEY,
            state varchar(16) NOT NULL,
            generation varchar(32) NOT NULL
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
            snapshot_id varchar(96) PRIMARY KEY
                REFERENCES {schema}.ptg2_snapshot(snapshot_id),
            snapshot_key bigint NOT NULL
                REFERENCES {schema}.ptg2_v3_snapshot_layout(snapshot_key)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_block (
            block_hash bytea PRIMARY KEY
        )
        """,
    )


def _evidence_table_statements(schema: str) -> tuple[str, ...]:
    return (
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_block (
            snapshot_key bigint NOT NULL,
            object_kind varchar(64) NOT NULL,
            block_key bigint NOT NULL,
            fragment_no integer NOT NULL,
            block_hash bytea NOT NULL
                REFERENCES {schema}.ptg2_v3_block(block_hash),
            PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_snapshot_pin (
            owner_type varchar(48) NOT NULL,
            owner_id varchar(96) NOT NULL,
            snapshot_id varchar(96) NOT NULL,
            reason varchar(256),
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (owner_type, owner_id, snapshot_id)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_source (
            snapshot_id varchar(96) NOT NULL,
            source_key integer NOT NULL,
            raw_container_sha256 varchar(64) NOT NULL,
            source_trace_set_hash varchar(64) NOT NULL,
            PRIMARY KEY (snapshot_id, source_key)
        )
        """,
    )


def _fixture_data_statements(schema: str) -> tuple[str, ...]:
    return (
        f"""
        INSERT INTO {schema}.ptg2_snapshot (snapshot_id, status)
        VALUES ('v3-snapshot', 'published'), ('v4-snapshot', 'published')
        """,
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_layout
            (snapshot_key, state, generation)
        VALUES
            (3, 'sealed', 'shared_blocks_v3'),
            (4, 'sealed', 'shared_blocks_v4')
        """,
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_binding
            (snapshot_id, snapshot_key)
        VALUES ('v3-snapshot', 3), ('v4-snapshot', 4)
        """,
        f"""
        INSERT INTO {schema}.ptg2_v3_block (block_hash)
        VALUES (decode(repeat('ab', 32), 'hex'))
        """,
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_block
            (snapshot_key, object_kind, block_key, fragment_no, block_hash)
        VALUES (3, 'provider_set', 1, 0, decode(repeat('ab', 32), 'hex'))
        """,
        f"""
        INSERT INTO {schema}.ptg2_snapshot_pin
            (owner_type, owner_id, snapshot_id, reason)
        VALUES
            ('ptg_v4_rollback', 'aetna-391', 'v3-snapshot',
             'retain V3 during V4 review')
        """,
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_source
            (snapshot_id, source_key, raw_container_sha256,
             source_trace_set_hash)
        VALUES
            ('v3-snapshot', 0, repeat('11', 32), repeat('22', 32)),
            ('v4-snapshot', 0, repeat('11', 32), repeat('22', 32))
        """,
    )


async def create_reference_fixture(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Create the complete disposable V3/V4 source-equivalence fixture."""

    schema = quoted(schema_name)
    statements = (
        *_snapshot_table_statements(schema),
        *_evidence_table_statements(schema),
        *_fixture_data_statements(schema),
    )
    for statement in statements:
        await connection.execute(statement)
