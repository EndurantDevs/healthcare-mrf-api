# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL proof for local PTG archive-layout preparation."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from db.connection import Database
from process.ptg_parts import result_archive_adoption as adoption
from tests import test_ptg2_v4_postgres_e2e as v4_e2e

_FIXTURE_REKEYED_TABLES = frozenset(
    {
        "ptg2_v3_provider_group",
        "ptg2_v3_provider_set",
        "ptg2_v4_snapshot_map_pack",
        "ptg2_v4_npi_scope",
        "ptg2_v4_provider_component",
        "ptg2_v4_pattern",
        "ptg2_v4_relation_manifest",
        "ptg2_v4_heavy_owner",
        "ptg2_v4_provider_set_npi_prefix",
        "ptg2_v4_provider_graph_diagnostic",
        "ptg2_v4_inferred_taxonomy_candidate",
        "ptg2_provider_tax_identity_manifest",
        "ptg2_provider_tax_identity",
        "ptg2_provider_group_tax_identity",
    }
)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _require_native_postgres() -> None:
    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set guarded PTG V4 PostgreSQL test variables for native proof")


async def _create_destination_snapshot_table(database: Database, schema: str) -> None:
    await database.execute_ddl(f"CREATE TABLE {schema}.ptg2_snapshot (snapshot_id text PRIMARY KEY)")
    await database.execute_ddl(
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
            snapshot_id text PRIMARY KEY,
            snapshot_key bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
        )
        """
    )
    await database.execute_ddl(
        f"""
        CREATE OR REPLACE FUNCTION {schema}.guard_ptg2_v4_attempt(
            requested_snapshot_id text,
            requested_internal_run_id text,
            allow_reconciled boolean DEFAULT false
        ) RETURNS void LANGUAGE plpgsql AS $$ BEGIN END $$
        """
    )


async def _seed_preparation_catalog(
    database: Database,
    *,
    schema_name: str,
    tmp_path: Path,
    monkeypatch,
) -> int:
    compilation, _relation_names = await v4_e2e._compile_direct_v4_fixture(tmp_path)
    try:
        _publication, sealed = await v4_e2e._publish_direct_v4_fixture(
            database,
            schema_name=schema_name,
            compilation=compilation,
            monkeypatch=monkeypatch,
        )
    finally:
        compilation.cleanup()
    schema = _quoted(schema_name)
    await database.execute_ddl(
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
            snapshot_id text PRIMARY KEY,
            snapshot_key bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
        )
        """
    )
    await database.status(
        f"INSERT INTO {schema}.ptg2_v3_snapshot_binding (snapshot_id, snapshot_key) VALUES ('staged-snapshot', :snapshot_key)",
        snapshot_key=sealed.snapshot_key,
    )
    return int(sealed.snapshot_key)


async def _create_destination_catalog(database: Database, *, schema_name: str, snapshot_id: str, monkeypatch) -> None:
    await v4_e2e._create_v4_test_schema(database, schema_name=schema_name, monkeypatch=monkeypatch)
    schema = _quoted(schema_name)
    await _create_destination_snapshot_table(database, schema)
    await database.status(
        f"INSERT INTO {schema}.ptg2_snapshot (snapshot_id) VALUES (:snapshot_id)",
        snapshot_id=snapshot_id,
    )


async def _assert_prepared_destination(
    database: Database,
    *,
    destination_schema: str,
    staging_schema: str,
    source_snapshot_key: int,
    prepared,
) -> None:
    assert prepared.destination_snapshot_key != source_snapshot_key
    assert prepared.requires_fresh_destination_attestation is True
    assert (
        await database.scalar(
            f"SELECT snapshot_key FROM {destination_schema}.ptg2_v3_snapshot_binding WHERE snapshot_id = 'destination-snapshot'"
        )
        == prepared.destination_snapshot_key
    )
    assert (
        await database.scalar(
            f"SELECT COUNT(*) FROM {destination_schema}.ptg2_v3_block WHERE block_hash = :hash",
            hash=b"u" * 32,
        )
        == 1
    )
    assert (
        await database.scalar(
            f"SELECT COUNT(*) FROM {destination_schema}.ptg2_v3_block WHERE block_hash IN (SELECT block_hash FROM {staging_schema}.ptg2_v3_block)"
        )
        > 0
    )


@pytest.mark.asyncio
async def test_native_archive_preparation_remaps_local_layout_and_preserves_cas(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Seal a destination-local key without touching unrelated destination rows."""

    _require_native_postgres()
    stage_name = f"ptg_archive_stage_{uuid.uuid4().hex[:16]}"
    destination_name = f"ptg_archive_destination_{uuid.uuid4().hex[:16]}"
    database = Database()
    await database.connect()
    v4_e2e._bind_source_local_database(monkeypatch, database)
    monkeypatch.setattr(
        adoption,
        "_REKEYED_TABLES",
        tuple(table for table in adoption._REKEYED_TABLES if table in _FIXTURE_REKEYED_TABLES),
    )
    try:
        source_key = await _seed_preparation_catalog(
            database, schema_name=stage_name, tmp_path=tmp_path, monkeypatch=monkeypatch
        )
        await _create_destination_catalog(
            database, schema_name=destination_name, snapshot_id="destination-snapshot", monkeypatch=monkeypatch
        )
        destination = _quoted(destination_name)
        async with database.transaction() as session:
            await v4_e2e.reserve_v4_shared_layout(
                session,
                schema_name=destination_name,
                semantic_fingerprint=b"d" * 32,
                build_token="unrelated-layout",
            )
        await database.status(
            f"INSERT INTO {destination}.ptg2_v3_block (block_hash, format_version, object_kind, codec, entry_count, raw_byte_count, stored_byte_count, payload) VALUES (:hash, 2, 'unrelated', 'none', 1, 1, 1, 'u')",
            hash=b"u" * 32,
        )
        async with database.transaction() as session:
            prepared = await adoption.prepare_result_archive_layout(
                session,
                schema_name=destination_name,
                staging_schema_name=stage_name,
                source_snapshot_key=source_key,
                destination_snapshot_id="destination-snapshot",
                build_token=f"receiver-{uuid.uuid4().hex}",
            )
        await _assert_prepared_destination(
            database,
            destination_schema=destination,
            staging_schema=_quoted(stage_name),
            source_snapshot_key=source_key,
            prepared=prepared,
        )
    finally:
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(destination_name)} CASCADE")
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(stage_name)} CASCADE")
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_native_archive_preparation_rejects_collision_without_partial_layout(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A differing local row for one CAS hash leaves the caller transaction empty."""

    _require_native_postgres()
    stage_name = f"ptg_archive_stage_{uuid.uuid4().hex[:16]}"
    destination_name = f"ptg_archive_destination_{uuid.uuid4().hex[:16]}"
    database = Database()
    await database.connect()
    v4_e2e._bind_source_local_database(monkeypatch, database)
    monkeypatch.setattr(adoption, "_REKEYED_TABLES", ())
    try:
        source_key = await _seed_preparation_catalog(
            database, schema_name=stage_name, tmp_path=tmp_path, monkeypatch=monkeypatch
        )
        await _create_destination_catalog(
            database, schema_name=destination_name, snapshot_id="destination-snapshot", monkeypatch=monkeypatch
        )
        stage = _quoted(stage_name)
        destination = _quoted(destination_name)
        block_hash = await database.scalar(f"SELECT block_hash FROM {stage}.ptg2_v3_block LIMIT 1")
        await database.status(
            f"INSERT INTO {destination}.ptg2_v3_block (block_hash, format_version, object_kind, codec, entry_count, raw_byte_count, stored_byte_count, payload) VALUES (:hash, 2, 'collision', 'none', 1, 1, 1, 'x')",
            hash=block_hash,
        )
        with pytest.raises(adoption.ResultArchiveAdoptionError, match="CAS hash collides"):
            async with database.transaction() as session:
                await adoption.prepare_result_archive_layout(
                    session,
                    schema_name=destination_name,
                    staging_schema_name=stage_name,
                    source_snapshot_key=source_key,
                    destination_snapshot_id="destination-snapshot",
                    build_token=f"receiver-{uuid.uuid4().hex}",
                )
        assert await database.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_v3_snapshot_layout") == 0
    finally:
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(destination_name)} CASCADE")
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(stage_name)} CASCADE")
        finally:
            await database.disconnect()
