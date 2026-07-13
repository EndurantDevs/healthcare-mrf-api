# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import os
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from process.ptg_parts import source_snapshot_control
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_DENSE_LAYOUT_TABLES,
    PTG2_V3_SHARED_GENERATION,
)


class _RecordingTransaction:
    def __init__(self):
        self.active = False
        self.statements = []

    async def __aenter__(self):
        self.active = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.active = False
        return False

    async def execute(self, statement, params):
        assert self.active
        self.statements.append((str(statement), params))


@pytest.mark.asyncio
async def test_remove_v3_snapshot_releases_layout_in_the_removal_transaction(monkeypatch):
    transaction = _RecordingTransaction()
    events = []

    async def fake_plan(**_kwargs):
        assert transaction.active
        return {
            "snapshot_id": "shared-a",
            "source_key": "source_a",
            "exists": True,
            "removable": True,
            "tables": [],
            "artifact_manifest_ids": [],
            "current_references": {},
        }

    async def fake_status(statement, **params):
        assert transaction.active
        assert params == {"snapshot_id": "shared-a"}
        if "ptg2_v3_snapshot_scope" in statement:
            events.append("scope-delete")
            return 1
        if "ptg2_v3_snapshot_binding" in statement:
            events.append("binding-delete")
            return 1
        if "ptg2_artifact_manifest" in statement:
            events.append("artifact-delete")
            return 0
        if 'DELETE FROM "mrf".ptg2_snapshot WHERE' in statement:
            events.append("snapshot-delete")
            return 1
        raise AssertionError(statement)

    async def fake_release(*, schema_name, executor, require_shared):
        assert transaction.active
        assert schema_name == "mrf"
        assert require_shared is True
        assert executor._session is transaction
        events.append("layout-release")
        return SimpleNamespace(logical_layout_count=1)

    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)
    monkeypatch.setattr(source_snapshot_control.db, "status", fake_status)
    monkeypatch.setattr(source_snapshot_control, "build_ptg2_source_snapshot_remove_plan", fake_plan)
    monkeypatch.setattr(source_snapshot_control, "release_unbound_ptg2_shared_layouts", fake_release)

    result = await source_snapshot_control.remove_ptg2_source_snapshot(
        snapshot_id="shared-a",
        source_key="source_a",
    )

    assert events == [
        "scope-delete",
        "binding-delete",
        "artifact-delete",
        "snapshot-delete",
        "layout-release",
    ]
    assert result["deleted_v3_snapshot_scopes"] == 1
    assert result["deleted_v3_snapshot_bindings"] == 1
    assert result["deleted_snapshots"] == 1
    assert result["released_shared_layouts"] == 1


async def _create_production_shaped_schema(database, schema_name):
    schema = f'"{schema_name}"'
    async with database.acquire() as connection:
        await connection.status(f"CREATE SCHEMA {schema}")
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_snapshot (
                snapshot_id varchar(96) PRIMARY KEY,
                import_month date NOT NULL,
                previous_snapshot_id varchar(96),
                status varchar(32) NOT NULL,
                manifest jsonb NOT NULL
            )
            """
        )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_current_snapshot (
                slot varchar(32) PRIMARY KEY,
                snapshot_id varchar(96),
                previous_snapshot_id varchar(96)
            )
            """
        )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_current_source_snapshot (
                source_key varchar(255) PRIMARY KEY,
                snapshot_id varchar(96),
                previous_snapshot_id varchar(96)
            )
            """
        )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_current_plan_source (
                plan_source_key varchar(255) PRIMARY KEY,
                snapshot_id varchar(96),
                previous_snapshot_id varchar(96)
            )
            """
        )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_artifact_manifest (
                artifact_id varchar(255) PRIMARY KEY,
                snapshot_id varchar(96)
            )
            """
        )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
                snapshot_key bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                storage_shard_id smallint NOT NULL DEFAULT 0,
                build_token varchar(96) NOT NULL,
                generation varchar(32) NOT NULL,
                state varchar(16) NOT NULL CHECK (state IN ('building', 'sealed')),
                mapping_digest bytea,
                support_digest bytea,
                layout_manifest jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                logical_byte_count bigint NOT NULL DEFAULT 0,
                created_at timestamptz NOT NULL DEFAULT now(),
                heartbeat_at timestamptz NOT NULL DEFAULT now(),
                lease_until timestamptz,
                published_at timestamptz
            )
            """
        )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_v3_layout_fingerprint (
                semantic_fingerprint bytea PRIMARY KEY,
                snapshot_key bigint NOT NULL REFERENCES
                    {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
                created_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
                snapshot_id varchar(96) PRIMARY KEY,
                snapshot_key bigint NOT NULL REFERENCES
                    {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE RESTRICT,
                created_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_v3_snapshot_scope (
                snapshot_id varchar(96) PRIMARY KEY,
                plan_id varchar(64) NOT NULL,
                plan_market_type varchar(32) NOT NULL DEFAULT '',
                coverage_scope_id bytea NOT NULL CHECK (octet_length(coverage_scope_id) = 32),
                created_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_source_trace_set (
                source_trace_set_hash varchar(64) PRIMARY KEY,
                source_trace_hashes varchar(64)[] NOT NULL
            )
            """
        )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_v3_snapshot_source (
                snapshot_id varchar(96) NOT NULL REFERENCES
                    {schema}.ptg2_v3_snapshot_scope(snapshot_id) ON DELETE CASCADE,
                source_key integer NOT NULL,
                source_type varchar(32) NOT NULL,
                identity_kind varchar(64) NOT NULL,
                identity_sha256 varchar(64) NOT NULL,
                raw_container_sha256 varchar(64) NOT NULL,
                logical_json_sha256 varchar(64),
                logical_hash_deferred boolean NOT NULL,
                source_trace_set_hash varchar(64) NOT NULL REFERENCES
                    {schema}.ptg2_source_trace_set(source_trace_set_hash) ON DELETE RESTRICT,
                PRIMARY KEY (snapshot_id, source_key)
            )
            """
        )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_v3_candidate_audit_attestation (
                snapshot_id varchar(96) PRIMARY KEY REFERENCES
                    {schema}.ptg2_v3_snapshot_scope(snapshot_id) ON DELETE CASCADE,
                snapshot_key bigint NOT NULL REFERENCES
                    {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE RESTRICT,
                source_key varchar(96) NOT NULL,
                plan_id varchar(64) NOT NULL,
                plan_market_type varchar(32) NOT NULL,
                coverage_scope_id bytea NOT NULL,
                source_set_digest bytea NOT NULL,
                audit_sample_digest bytea NOT NULL,
                contract varchar(64) NOT NULL,
                tool_name varchar(64) NOT NULL,
                tool_version varchar(32) NOT NULL,
                report_digest bytea NOT NULL,
                report jsonb NOT NULL,
                attested_at timestamptz NOT NULL,
                expires_at timestamptz NOT NULL,
                activated_at timestamptz
            )
            """
        )
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
                snapshot_key bigint NOT NULL REFERENCES
                    {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
                object_kind varchar(64) NOT NULL,
                block_key bigint NOT NULL,
                fragment_no integer NOT NULL,
                entry_count bigint NOT NULL,
                block_hash bytea NOT NULL REFERENCES {schema}.ptg2_v3_block(block_hash),
                PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no)
            )
            """
        )
        for table_name in PTG2_V3_DENSE_LAYOUT_TABLES:
            await connection.status(
                f'CREATE TABLE {schema}."{table_name}" (snapshot_key bigint NOT NULL)'
            )
        await connection.status(
            f"""
            CREATE TABLE {schema}.ptg2_v3_gc_candidate (
                block_hash bytea PRIMARY KEY REFERENCES
                    {schema}.ptg2_v3_block(block_hash) ON DELETE CASCADE,
                eligible_at timestamptz NOT NULL,
                queued_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )


async def _insert_shared_snapshots(database, schema_name):
    schema = f'"{schema_name}"'
    manifests = {
        snapshot_id: json.dumps(
            {
                "serving_index": {
                    "arch_version": "postgres_binary_v3",
                    "storage_generation": PTG2_V3_SHARED_GENERATION,
                    "shared_snapshot_key": 10,
                    "source_key": source_key,
                }
            }
        )
        for snapshot_id, source_key in (
            ("shared-a", "source_a"),
            ("shared-b", "source_b"),
        )
    }
    async with database.acquire() as connection:
        await connection.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_layout
                (snapshot_key, build_token, generation, state, created_at, heartbeat_at, lease_until)
            VALUES
                (10, 'build-10', :generation, 'sealed', now() - INTERVAL '2 hours',
                 now() - INTERVAL '2 hours', NULL)
            """,
            generation=PTG2_V3_SHARED_GENERATION,
        )
        for snapshot_id, source_key in (("shared-a", "source_a"), ("shared-b", "source_b")):
            await connection.status(
                f"""
                INSERT INTO {schema}.ptg2_snapshot
                    (snapshot_id, import_month, previous_snapshot_id, status, manifest)
                VALUES (:snapshot_id, DATE '2026-07-01', NULL, 'published', CAST(:manifest AS jsonb))
                """,
                snapshot_id=snapshot_id,
                manifest=manifests[snapshot_id],
            )
            await connection.status(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_binding (snapshot_id, snapshot_key)
                VALUES (:snapshot_id, 10)
                """,
                snapshot_id=snapshot_id,
            )
            await connection.status(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_scope
                    (snapshot_id, plan_id, plan_market_type, coverage_scope_id)
                VALUES (:snapshot_id, :plan_id, 'group', :coverage_scope_id)
                """,
                snapshot_id=snapshot_id,
                plan_id=f"plan-{source_key}",
                coverage_scope_id=bytes([1 if source_key == "source_a" else 2]) * 32,
            )


async def _count(database, schema_name, table_name, *, snapshot_id=None):
    schema = f'"{schema_name}"'
    where_clause = ""
    params = {}
    if snapshot_id is not None:
        where_clause = " WHERE snapshot_id = :snapshot_id"
        params["snapshot_id"] = snapshot_id
    return int(
        await database.scalar(
            f'SELECT COUNT(*) FROM {schema}."{table_name}"{where_clause}',
            **params,
        )
        or 0
    )


@pytest.mark.asyncio
async def test_real_postgres_remove_v3_snapshot_matches_production_no_cascade_ddl(monkeypatch):
    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 for the isolated PostgreSQL test")

    database = Database()
    schema_name = f"ptg2_snapshot_removal_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    await database.connect()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setattr(source_snapshot_control, "db", database)
    try:
        await _create_production_shaped_schema(database, schema_name)
        await _insert_shared_snapshots(database, schema_name)

        first = await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="shared-a",
            source_key="source_a",
        )

        assert first["deleted_v3_snapshot_scopes"] == 1
        assert first["deleted_v3_snapshot_bindings"] == 1
        assert first["released_shared_layouts"] == 0
        assert await _count(database, schema_name, "ptg2_snapshot", snapshot_id="shared-a") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_scope", snapshot_id="shared-a") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_binding", snapshot_id="shared-a") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_layout") == 1
        assert await _count(database, schema_name, "ptg2_v3_snapshot_binding", snapshot_id="shared-b") == 1

        second = await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="shared-b",
            source_key="source_b",
        )

        assert second["deleted_v3_snapshot_scopes"] == 1
        assert second["deleted_v3_snapshot_bindings"] == 1
        assert second["released_shared_layouts"] == 1
        assert await _count(database, schema_name, "ptg2_snapshot") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_scope") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_binding") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_layout") == 0
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()
