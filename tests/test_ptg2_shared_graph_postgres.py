# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Mapping

import pytest

from api.ptg2_shared_blocks import (
    PTG2_V3_GRAPH_CHUNK_BYTES,
    fetch_shared_graph_members,
    shared_block_read_once_scope,
)
from db.connection import db
from process.ptg_parts.ptg2_shared_graph import (
    PTG2_V3_GRAPH_GROUP_TO_NPI,
    PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
    PTG2_V3_GRAPH_NPI_TO_GROUP,
    PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
    convert_memberships_to_shared_graph,
)
from process.ptg_parts.ptg2_shared_publish import publish_shared_graph
from tests.test_ptg2_shared_graph import _fixtures


_GRAPH_TABLE_DDL = (
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
        snapshot_key bigint PRIMARY KEY,
        state varchar(16) NOT NULL,
        generation varchar(32) NOT NULL,
        build_token varchar(64)
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
        payload bytea NOT NULL,
        created_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_gc_candidate (
        block_hash bytea PRIMARY KEY,
        eligible_at timestamptz NOT NULL,
        queued_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_block_build_pin (
        snapshot_key bigint NOT NULL REFERENCES
            {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
        build_token varchar(96) NOT NULL,
        pin_token varchar(96) NOT NULL,
        block_hash bytea NOT NULL CHECK (octet_length(block_hash) = 32),
        created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
        heartbeat_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
        lease_until timestamptz NOT NULL,
        PRIMARY KEY (snapshot_key, pin_token, block_hash)
    )
    """,
    """
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
    CREATE TABLE {schema}.ptg2_v3_provider_group (
        snapshot_key bigint NOT NULL,
        provider_group_key integer NOT NULL,
        provider_group_global_id_128 bytea NOT NULL,
        PRIMARY KEY (snapshot_key, provider_group_key),
        UNIQUE (snapshot_key, provider_group_global_id_128)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_npi_scope (
        snapshot_key bigint NOT NULL,
        npi bigint NOT NULL,
        PRIMARY KEY (snapshot_key, npi)
    )
    """,
)


@dataclass(frozen=True)
class _GraphReadFixture:
    schema_name: str
    snapshot_key: int
    group_key_by_global_id: Mapping[bytes, int]
    provider_keys: Mapping[bytes, int]
    provider_ids: tuple[bytes, ...]
    groups: tuple[bytes, ...]
    npis: tuple[bytes, ...]


async def _create_graph_tables(schema: str) -> None:
    for table_ddl in _GRAPH_TABLE_DDL:
        await db.execute_ddl(table_ddl.format(schema=schema))


async def _seed_graph_layout(schema: str, snapshot_key: int) -> None:
    await db.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_layout
            (snapshot_key, state, generation, build_token)
        VALUES (:snapshot_key, 'building', 'shared_blocks_v3', 'graph-smoke')
        """,
        snapshot_key=snapshot_key,
    )


def _assert_graph_publication(publication, conversion) -> None:
    assert publication.block_count == conversion.block_count
    assert publication.mapping_count == conversion.block_count
    assert 0 < publication.unique_block_count <= publication.mapping_count
    assert publication.object_kinds == (
        "graph_group_npis_v1",
        "graph_group_provider_sets_v1",
        "graph_npi_groups_v1",
        "graph_provider_set_groups_v1",
    )
    assert publication.owner_count == conversion.owner_count
    assert publication.provider_group_count == 3
    assert publication.npi_count == 3
    assert publication.support_digest == conversion.support_digest


async def _seal_graph_layout(schema: str, snapshot_key: int) -> None:
    await db.status(
        f"""
        UPDATE {schema}.ptg2_v3_snapshot_layout
           SET state = 'sealed'
         WHERE snapshot_key = :snapshot_key
        """,
        snapshot_key=snapshot_key,
    )


async def _assert_npi_graph_reads(session, fixture: _GraphReadFixture) -> None:
    group_keys = fixture.group_key_by_global_id
    first_npi = int.from_bytes(fixture.npis[0][8:], "big")
    assert await fetch_shared_graph_members(
        session,
        schema_name=fixture.schema_name,
        snapshot_key=fixture.snapshot_key,
        direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
        owner_keys=(first_npi,),
    ) == {
        first_npi: (
            group_keys[fixture.groups[0]],
            group_keys[fixture.groups[1]],
        )
    }
    npi_owner_keys = tuple(
        int.from_bytes(npi[8:], "big") for npi in fixture.npis
    )
    with shared_block_read_once_scope(
        max_retained_raw_bytes=PTG2_V3_GRAPH_CHUNK_BYTES,
    ) as read_once:
        assert await fetch_shared_graph_members(
            session,
            schema_name=fixture.schema_name,
            snapshot_key=fixture.snapshot_key,
            direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
            owner_keys=npi_owner_keys,
        ) == {
            npi_owner_keys[0]: (
                group_keys[fixture.groups[0]],
                group_keys[fixture.groups[1]],
            ),
            npi_owner_keys[1]: (group_keys[fixture.groups[0]],),
            npi_owner_keys[2]: (group_keys[fixture.groups[0]],),
        }
        read_once.assert_read_once()
        read_once_ledger = read_once.ledger
    assert read_once_ledger["logical_block_deliveries"] == 1
    assert read_once_ledger["unique_physical_blocks"] == 1
    assert read_once_ledger["physical_block_reads"] == 1
    assert read_once_ledger["physical_block_decodes"] == 1


async def _assert_group_graph_reads(session, fixture: _GraphReadFixture) -> None:
    group_keys = fixture.group_key_by_global_id
    first_group_key = group_keys[fixture.groups[0]]
    assert await fetch_shared_graph_members(
        session,
        schema_name=fixture.schema_name,
        snapshot_key=fixture.snapshot_key,
        direction=PTG2_V3_GRAPH_GROUP_TO_NPI,
        owner_keys=(first_group_key,),
    ) == {
        first_group_key: tuple(
            int.from_bytes(npi[8:], "big") for npi in fixture.npis
        )
    }
    assert await fetch_shared_graph_members(
        session,
        schema_name=fixture.schema_name,
        snapshot_key=fixture.snapshot_key,
        direction=PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
        owner_keys=(first_group_key,),
    ) == {
        first_group_key: (
            fixture.provider_keys[fixture.provider_ids[0]],
            fixture.provider_keys[fixture.provider_ids[1]],
        )
    }
    second_provider_key = fixture.provider_keys[fixture.provider_ids[1]]
    assert await fetch_shared_graph_members(
        session,
        schema_name=fixture.schema_name,
        snapshot_key=fixture.snapshot_key,
        direction=PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
        owner_keys=(second_provider_key,),
    ) == {
        second_provider_key: (
            group_keys[fixture.groups[0]],
            group_keys[fixture.groups[1]],
        )
    }


@pytest.mark.asyncio
async def test_real_postgres_graph_binary_copy_publish_and_reads(tmp_path):
    """Publish graph COPY artifacts to PostgreSQL and read every graph direction."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    artifacts, provider_keys, groups, npis = _fixtures(tmp_path)
    conversion = convert_memberships_to_shared_graph(
        group_npi=artifacts[0],
        npi_group=artifacts[1],
        group_provider_set=artifacts[2],
        provider_set_group=artifacts[3],
        provider_set_key_by_global_id=provider_keys,
        spill_directory=tmp_path,
        external_sort_chunk_bytes=32,
    )
    scratch_directory = conversion.scratch_directory
    schema_name = f"ptg2_graph_{uuid.uuid4().hex[:16]}"
    schema = f'"{schema_name}"'
    snapshot_key = 1
    await db.disconnect()
    await db.connect()
    try:
        await db.execute_ddl(f"CREATE SCHEMA {schema}")
        await _create_graph_tables(schema)
        await _seed_graph_layout(schema, snapshot_key)

        publication = await publish_shared_graph(
            conversion,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            build_token="graph-smoke",
        )
        _assert_graph_publication(publication, conversion)
        await _seal_graph_layout(schema, snapshot_key)

        fixture = _GraphReadFixture(
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            group_key_by_global_id=dict(conversion.iter_group_key_items()),
            provider_keys=provider_keys,
            provider_ids=tuple(sorted(provider_keys)),
            groups=tuple(groups),
            npis=tuple(npis),
        )
        async with db.session() as session:
            await _assert_npi_graph_reads(session, fixture)
            await _assert_group_graph_reads(session, fixture)
    finally:
        conversion.cleanup()
        try:
            await db.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await db.disconnect()
    assert not scratch_directory.exists()
