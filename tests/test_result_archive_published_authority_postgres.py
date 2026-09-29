# See LICENSE.
"""Opt-in native PostgreSQL proof of published-result authority and pinning.

The synthetic tables exercise the SQL contract, not the full migration schema.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from contextlib import asynccontextmanager
from copy import deepcopy

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.ptg_parts import result_archive_published_authority as authority
from process.ptg_parts import result_archive_source_authority as dispatch
from process.ptg_parts.result_archive_published_identity import (
    load_published_result_identity,
    load_published_result_identity_asyncpg,
)
from process.ptg_parts.result_archive_source_authority import PtgResultArchiveSourceAuthorityError
from tests.test_result_archive_published_identity import _published_row


@asynccontextmanager
async def _database():
    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set guarded PTG PostgreSQL test variables for native proof")
    raw = os.getenv("HLTHPRT_PTG2_V4_MIGRATION_POSTGRES_DSN")
    if not raw:
        pytest.skip("set the native PostgreSQL test DSN")
    url = make_url(raw).set(drivername="postgresql+asyncpg")
    if url.host not in {"127.0.0.1", "localhost", "postgres"} or url.port not in {5432, 5440, None}:
        pytest.fail("published authority tests require a guarded PostgreSQL test service")
    engine = create_async_engine(url)
    name = "published_authority_" + uuid.uuid4().hex
    schema = f'"{name}"'
    is_schema_created = False
    try:
        async with engine.begin() as connection:
            version = int((await connection.execute(text("SHOW server_version_num"))).scalar_one())
            assert 180000 <= version < 190000
            await connection.exec_driver_sql(f"CREATE SCHEMA {schema}")
            is_schema_created = True
            columns_by_table = {
                "ptg2_import_run": "import_run_id text PRIMARY KEY, import_month date, options jsonb",
                "ptg2_artifact_manifest": "snapshot_id text",
                "ptg2_snapshot": "snapshot_id text PRIMARY KEY, import_month date, status text, import_run_id text, manifest jsonb",
                "ptg2_v3_snapshot_binding": "snapshot_id text PRIMARY KEY, snapshot_key bigint",
                "ptg2_v3_snapshot_scope": "snapshot_id text PRIMARY KEY, plan_id text, plan_market_type text, coverage_scope_id bytea",
                "ptg2_v3_snapshot_plan_scope": "snapshot_id text, plan_id text, plan_market_type text",
                "ptg2_v3_snapshot_source": "snapshot_id text, source_key integer, source_type text, identity_kind text, identity_sha256 text, raw_container_sha256 text, logical_json_sha256 text, logical_hash_deferred boolean, source_trace_set_hash text",
                "ptg2_v3_snapshot_layout": "snapshot_key bigint PRIMARY KEY, state text, generation text, mapping_digest bytea, layout_manifest jsonb",
                "ptg2_v4_snapshot_map_root": "snapshot_key bigint PRIMARY KEY, state text, map_format text, map_digest bytea",
                "ptg2_v4_finalizer_map_root": "snapshot_key bigint PRIMARY KEY, state text, contract text, map_format text, map_digest bytea",
                "ptg2_frozen_source_file_binding": "internal_run_id text PRIMARY KEY, binding_payload jsonb",
                "ptg2_snapshot_pin": f"owner_type text, owner_id text, snapshot_id text REFERENCES {schema}.ptg2_snapshot(snapshot_id), reason text, created_at timestamptz, PRIMARY KEY(owner_type, owner_id, snapshot_id)",
            }
            for table, columns in columns_by_table.items():
                await connection.exec_driver_sql(f"CREATE TABLE {schema}.{table} ({columns})")
        yield engine, name
    finally:
        try:
            if is_schema_created:
                async with engine.begin() as connection:
                    await connection.exec_driver_sql(f"DROP SCHEMA {schema} CASCADE")
                    assert (
                        await connection.execute(text("SELECT to_regnamespace(:name)"), {"name": name})
                    ).scalar_one() is None
        finally:
            await engine.dispose()


def _source_assignment_by_column(snapshot: str) -> dict:
    """Return one synthetic source assignment for the published snapshot."""
    return {
        "snapshot_id": snapshot,
        "source_key": 1,
        "source_type": "in_network",
        "identity_kind": "raw_container_sha256_v1",
        "identity_sha256": (b"r" * 32).hex(),
        "raw_container_sha256": (b"r" * 32).hex(),
        "logical_json_sha256": None,
        "logical_hash_deferred": True,
        "source_trace_set_hash": "ab" * 32,
    }


def _published_records_by_table(*, suffix="a", key=17):
    """Build independent synthetic source and layout records for one snapshot."""
    snapshot_by_field = deepcopy(_published_row())
    snapshot = f"published-{suffix}"
    source_key = f"source_{suffix}"
    plan = f"plan-{suffix}"
    snapshot_by_field["manifest"]["activation"]["source_key"] = source_key
    for manifest in (snapshot_by_field["manifest"], snapshot_by_field["layout_manifest"]):
        manifest["serving_index"]["shared_snapshot_key"] = key
    return snapshot, {
        "ptg2_import_run": {
            "import_run_id": f"run-{suffix}",
            "import_month": dt.date(2026, 9, 1),
            "options": {"source_key": source_key},
        },
        "ptg2_snapshot": {
            "import_month": dt.date(2026, 9, 1),
            "snapshot_id": snapshot,
            "status": "published",
            "import_run_id": f"run-{suffix}",
            "manifest": snapshot_by_field["manifest"],
        },
        "ptg2_v3_snapshot_binding": {"snapshot_id": snapshot, "snapshot_key": key},
        "ptg2_v3_snapshot_scope": {
            "snapshot_id": snapshot,
            "plan_id": plan,
            "plan_market_type": "group",
            "coverage_scope_id": snapshot_by_field["coverage_scope_id"],
        },
        "ptg2_v3_snapshot_plan_scope": {"snapshot_id": snapshot, "plan_id": plan, "plan_market_type": "group"},
        "ptg2_v3_snapshot_source": _source_assignment_by_column(snapshot),
        "ptg2_v3_snapshot_layout": {
            "snapshot_key": key,
            "state": "sealed",
            "generation": "shared_blocks_v4",
            "mapping_digest": snapshot_by_field["layout_mapping_digest"],
            "layout_manifest": snapshot_by_field["layout_manifest"],
        },
        "ptg2_v4_snapshot_map_root": dict(
            snapshot_key=key,
            state="complete",
            map_format=snapshot_by_field["map_format"],
            map_digest=snapshot_by_field["map_digest"],
        ),
        "ptg2_v4_finalizer_map_root": {
            "snapshot_key": key,
            "state": "complete",
            "contract": snapshot_by_field["finalizer_contract"],
            "map_format": snapshot_by_field["finalizer_map_format"],
            "map_digest": snapshot_by_field["finalizer_map_digest"],
        },
    }


async def _seed(engine, name, *, suffix="a", key=17):
    """Insert one synthetic published result in its task-owned schema."""
    schema = f'"{name}"'
    snapshot, records_by_table = _published_records_by_table(suffix=suffix, key=key)
    async with engine.begin() as connection:
        for table, record_by_column in records_by_table.items():
            insert_values_sql = ", ".join(
                f"CAST(:{field} AS jsonb)" if isinstance(column_value, dict) else f":{field}"
                for field, column_value in record_by_column.items()
            )
            await connection.execute(
                text(f"INSERT INTO {schema}.{table} ({', '.join(record_by_column)}) VALUES ({insert_values_sql})"),
                {
                    field: json.dumps(column_value) if isinstance(column_value, dict) else column_value
                    for field, column_value in record_by_column.items()
                },
            )
    return snapshot


@pytest.mark.asyncio
async def test_native_asyncpg_published_identity_matches_the_session_reader():
    async with _database() as (engine, name):
        selected = await _seed(engine, name)
        async with async_sessionmaker(engine)() as session:
            expected = await load_published_result_identity(session, schema_name=name, snapshot_id=selected)
        url = make_url(os.environ["HLTHPRT_PTG2_V4_MIGRATION_POSTGRES_DSN"])
        connection = await asyncpg.connect(url.set(drivername="postgresql").render_as_string(hide_password=False))
        try:
            async with connection.transaction(isolation="repeatable_read"):
                actual = await load_published_result_identity_asyncpg(
                    connection, schema_name=name, snapshot_id=selected, lock=True
                )
            assert actual == expected
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_native_published_authority_exact_snapshot_lifecycle():
    async with _database() as (engine, name):
        selected = await _seed(engine, name)
        sibling = await _seed(engine, name, suffix="b", key=18)
        sessions = async_sessionmaker(engine)
        async with sessions.begin() as session:
            first = await load_published_result_identity(session, schema_name=name, snapshot_id=selected)
            second = await load_published_result_identity(session, schema_name=name, snapshot_id=sibling)
            assert first["source_count"] == second["source_count"] == 1
            assert first["plan_id"] == "plan-a" and second["plan_id"] == "plan-b"
            assert first["plan_scopes_sha256"] != second["plan_scopes_sha256"]
            prepared = await dispatch.prepare_ptg_result_archive_source_authority(
                session, schema_name=name, operation_id="operation-a", snapshot_id=selected
            )
            assert (await session.execute(text(f'SELECT count(*) FROM "{name}".ptg2_snapshot_pin'))).scalar_one() == 0
        async with sessions.begin() as session:
            assert (
                await dispatch.commit_ptg_result_archive_source_authority(
                    session, schema_name=name, authority=prepared.as_dict()
                )
                == prepared
            )
            assert (
                await dispatch.commit_ptg_result_archive_source_authority(
                    session, schema_name=name, authority=prepared.as_dict()
                )
                == prepared
            )
        async with sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            assert (
                await dispatch.lock_ptg_result_archive_for_clone(
                    session, schema_name=name, authority=prepared.as_dict()
                )
                == prepared
            )
        async with sessions.begin() as session:
            assert (
                await dispatch.reconcile_ptg_archive_source_release(
                    session, schema_name=name, authority=prepared.as_dict()
                )
                == "released"
            )
            assert (
                await dispatch.reconcile_ptg_archive_source_release(
                    session, schema_name=name, authority=prepared.as_dict()
                )
                == "already_released"
            )
            assert (await session.execute(text(f'SELECT count(*) FROM "{name}".ptg2_snapshot'))).scalar_one() == 2
        with pytest.raises(authority.PtgPublishedResultSourceAuthorityError, match="pin changed"):
            async with sessions.begin() as session:
                await authority.lock_ptg_published_result_for_clone(
                    session, schema_name=name, authority=prepared.as_dict()
                )


@pytest.mark.asyncio
async def test_native_published_authority_rejects_tamper_and_operation_conflict():
    async with _database() as (engine, name):
        selected = await _seed(engine, name)
        sibling = await _seed(engine, name, suffix="b", key=18)
        sessions = async_sessionmaker(engine)
        async with sessions.begin() as session:
            prepared = await authority.prepare_ptg_published_result_source_authority(
                session, schema_name=name, operation_id="operation-a", snapshot_id=selected
            )
            await authority.commit_ptg_published_result_source_authority(
                session, schema_name=name, authority=prepared.as_dict()
            )
        with pytest.raises(PtgResultArchiveSourceAuthorityError, match="conflicts"):
            async with sessions.begin() as session:
                other = await authority.prepare_ptg_published_result_source_authority(
                    session, schema_name=name, operation_id="operation-a", snapshot_id=sibling
                )
                await authority.commit_ptg_published_result_source_authority(
                    session, schema_name=name, authority=other.as_dict()
                )
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f'UPDATE "{name}".ptg2_snapshot SET manifest = manifest || CAST(:changed AS jsonb) WHERE snapshot_id = :snapshot'
                ),
                {"snapshot": selected, "changed": json.dumps({"changed": True})},
            )
        with pytest.raises(authority.PtgPublishedResultSourceAuthorityError, match="changed after preparation"):
            async with sessions.begin() as session:
                await authority.commit_ptg_published_result_source_authority(
                    session, schema_name=name, authority=prepared.as_dict()
                )
        with pytest.raises(authority.PtgPublishedResultSourceAuthorityError, match="pin changed"):
            async with sessions.begin() as session:
                await authority.lock_ptg_published_result_for_clone(
                    session, schema_name=name, authority=prepared.as_dict()
                )
        async with sessions.begin() as session:
            assert (
                await authority.reconcile_ptg_published_result_source_release(
                    session, schema_name=name, authority=prepared.as_dict()
                )
                == "released"
            )


@pytest.mark.asyncio
async def test_native_published_authority_capture_revalidate_and_exact_release():
    async with _database() as (engine, name):
        selected = await _seed(engine, name)
        sessions = async_sessionmaker(engine)
        async with sessions.begin() as session:
            captured = await authority.capture_ptg_published_result_source_authority(
                session, schema_name=name, operation_id="operation-c", snapshot_id=selected
            )
        async with sessions.begin() as session:
            assert (
                await authority.revalidate_ptg_published_result_source_authority(
                    session, schema_name=name, authority=captured.as_dict()
                )
                == captured
            )
            assert (
                await dispatch.revalidate_ptg_result_archive_source_authority(
                    session, schema_name=name, authority=captured.as_dict()
                )
                == captured
            )
        async with sessions.begin() as session:
            assert (
                await dispatch.release_ptg_result_archive_source_authority(
                    session, schema_name=name, authority=captured.as_dict()
                )
                == 1
            )
        with pytest.raises(authority.PtgPublishedResultSourceAuthorityError, match="pin changed"):
            async with sessions.begin() as session:
                await authority.revalidate_ptg_published_result_source_authority(
                    session, schema_name=name, authority=captured.as_dict()
                )


@pytest.mark.asyncio
async def test_native_published_authority_rejects_missing_or_unpublished_source():
    async with _database() as (engine, name):
        selected = await _seed(engine, name)
        sessions = async_sessionmaker(engine)
        with pytest.raises(ValueError, match="snapshot_id is required"):
            async with sessions.begin() as session:
                await load_published_result_identity(session, schema_name=name, snapshot_id="")
        with pytest.raises(ValueError, match="missing or ambiguous"):
            async with sessions.begin() as session:
                await load_published_result_identity(session, schema_name=name, snapshot_id="absent")
        with pytest.raises(authority.PtgPublishedResultSourceAuthorityError, match="snapshot is required"):
            async with sessions.begin() as session:
                await authority.prepare_ptg_published_result_source_authority(
                    session, schema_name=name, operation_id="operation-d", snapshot_id=""
                )
        with pytest.raises(authority.PtgPublishedResultSourceAuthorityError, match="ownership is unavailable"):
            async with sessions.begin() as session:
                await authority.prepare_ptg_published_result_source_authority(
                    session, schema_name=name, operation_id="operation-d", snapshot_id="absent"
                )
        async with engine.begin() as connection:
            await connection.execute(
                text(f'UPDATE "{name}".ptg2_snapshot SET status = :status WHERE snapshot_id = :snapshot'),
                {"snapshot": selected, "status": "validated"},
            )
        with pytest.raises(authority.PtgPublishedResultSourceAuthorityError, match="evidence is unavailable"):
            async with sessions.begin() as session:
                await authority.prepare_ptg_published_result_source_authority(
                    session, schema_name=name, operation_id="operation-d", snapshot_id=selected
                )
