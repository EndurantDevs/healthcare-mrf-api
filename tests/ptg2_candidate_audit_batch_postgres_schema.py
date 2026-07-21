# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Isolated PostgreSQL schema helpers for the candidate batch proof."""

from __future__ import annotations

import zlib

from db.connection import db
from process.ptg_parts.ptg2_shared_blocks import shared_block_hash


CANDIDATE_SCHEMA_DDL_TEMPLATES = (
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
        snapshot_key bigint PRIMARY KEY,
        state text NOT NULL,
        generation text NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_block (
        block_hash bytea PRIMARY KEY,
        format_version smallint NOT NULL,
        object_kind text NOT NULL,
        codec text NOT NULL,
        entry_count bigint NOT NULL,
        raw_byte_count bigint NOT NULL,
        stored_byte_count bigint NOT NULL,
        payload bytea NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_block (
        snapshot_key bigint NOT NULL,
        object_kind text NOT NULL,
        block_key bigint NOT NULL,
        fragment_no integer NOT NULL,
        entry_count bigint NOT NULL,
        block_hash bytea NOT NULL,
        PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_graph_owner (
        snapshot_key bigint NOT NULL,
        direction smallint NOT NULL,
        owner_key bigint NOT NULL,
        first_chunk integer NOT NULL,
        member_offset integer NOT NULL,
        member_count bigint NOT NULL,
        PRIMARY KEY (snapshot_key, direction, owner_key)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_source (
        snapshot_id text NOT NULL,
        source_key integer NOT NULL,
        raw_container_sha256 text NOT NULL,
        PRIMARY KEY (snapshot_id, source_key)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_audit_occurrence (
        snapshot_key bigint NOT NULL,
        occurrence_id bytea NOT NULL,
        code_key integer NOT NULL,
        provider_set_key integer NOT NULL,
        price_key bigint NOT NULL,
        source_key integer NOT NULL,
        npi bigint NOT NULL,
        atom_ordinal bigint NOT NULL,
        atom_key bigint NOT NULL,
        PRIMARY KEY (snapshot_key, occurrence_id)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_source_audit_witness (
        snapshot_key bigint PRIMARY KEY,
        contract text NOT NULL,
        selection_method text NOT NULL,
        source_set_digest bytea NOT NULL,
        sample_digest bytea NOT NULL,
        queryable_occurrence_population_count bigint NOT NULL,
        provider_population_count bigint NOT NULL,
        occurrence_witness_count integer NOT NULL,
        provider_witness_count integer NOT NULL,
        payload_sha256 bytea NOT NULL,
        payload bytea NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_source_audit_witness_part (
        snapshot_key bigint NOT NULL REFERENCES
            {schema}.ptg2_v3_source_audit_witness(snapshot_key)
            ON DELETE CASCADE,
        part_number integer NOT NULL,
        part_sha256 bytea NOT NULL,
        payload bytea NOT NULL,
        PRIMARY KEY (snapshot_key, part_number)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_scope (
        snapshot_id text PRIMARY KEY,
        coverage_scope_id bytea NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_plan_scope (
        snapshot_id text NOT NULL,
        plan_id text NOT NULL,
        plan_market_type text NOT NULL,
        PRIMARY KEY (snapshot_id, plan_id, plan_market_type)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_code (
        snapshot_key bigint NOT NULL,
        code_key integer NOT NULL,
        code_global_id_128 bytea NOT NULL,
        coverage_scope_id bytea NOT NULL,
        reported_code_system text,
        reported_code text,
        negotiation_arrangement text,
        billing_code_type_version text,
        source_name text,
        source_description text,
        rate_count bigint NOT NULL,
        PRIMARY KEY (snapshot_key, code_key)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_provider_set (
        snapshot_key bigint NOT NULL,
        provider_set_key integer NOT NULL,
        provider_set_global_id_128 bytea NOT NULL,
        provider_count bigint NOT NULL,
        network_names text[] NOT NULL,
        PRIMARY KEY (snapshot_key, provider_set_key)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_price_attr (
        snapshot_key bigint NOT NULL,
        attribute_kind text NOT NULL,
        attribute_key integer NOT NULL,
        value text,
        PRIMARY KEY (snapshot_key, attribute_kind, attribute_key)
    )
    """,
)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


async def create_candidate_schema(schema_name: str) -> None:
    """Create the minimal isolated serving schema used by the integration test."""

    schema = quote_identifier(schema_name)
    await db.execute_ddl(f"CREATE SCHEMA {schema}")
    for ddl_template in CANDIDATE_SCHEMA_DDL_TEMPLATES:
        await db.execute_ddl(ddl_template.format(schema=schema))


async def insert_shared_block(
    schema_name: str,
    snapshot_key: int,
    *,
    object_kind: str,
    raw_payload: bytes,
    coordinates: tuple[tuple[int, int], ...],
    entry_count: int,
) -> bytes:
    """Insert one immutable physical block and all of its logical mappings."""

    schema = quote_identifier(schema_name)
    stored_payload = zlib.compress(raw_payload, level=1)
    block_hash = shared_block_hash(
        format_version=2,
        object_kind=object_kind,
        codec="zlib",
        payload=stored_payload,
    )
    await db.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_block
            (block_hash, format_version, object_kind, codec, entry_count,
             raw_byte_count, stored_byte_count, payload)
        VALUES
            (:block_hash, 2, :object_kind, 'zlib', :entry_count,
             :raw_byte_count, :stored_byte_count, :payload)
        """,
        block_hash=block_hash,
        object_kind=object_kind,
        entry_count=entry_count,
        raw_byte_count=len(raw_payload),
        stored_byte_count=len(stored_payload),
        payload=stored_payload,
    )
    for block_key, fragment_no in coordinates:
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_block
                (snapshot_key, object_kind, block_key, fragment_no,
                 entry_count, block_hash)
            VALUES
                (:snapshot_key, :object_kind, :block_key, :fragment_no,
                 :entry_count, :block_hash)
            """,
            snapshot_key=snapshot_key,
            object_kind=object_kind,
            block_key=block_key,
            fragment_no=fragment_no,
            entry_count=entry_count,
            block_hash=block_hash,
        )
    return block_hash
