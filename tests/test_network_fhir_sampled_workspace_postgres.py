# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native sampled scratch isolation and cleanup; these checks confer no Source custody."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import network_fhir_sampled_validation as sampled
from process import network_fhir_typed_source as typed
from process.network_address_projection import _identifier
from process.network_fhir_typed_contract import TypedFHIRGraphError

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def sample_reader():
    configured = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not configured:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    dsn = configured.replace("postgresql+asyncpg://", "postgresql://")
    admin, reader = await asyncpg.connect(dsn), None
    role, schema = "sample_reader_" + uuid4().hex, "sample_workspace_" + uuid4().hex
    password = uuid4().hex
    try:
        await admin.execute(
            "CREATE ROLE " + _identifier(role) + " LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE "
            "NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "'"
        )
        await admin.execute("ALTER ROLE " + _identifier(role) + " SET temp_file_limit='256MB'")
        await admin.execute("ALTER ROLE " + _identifier(role) + " SET work_mem='16MB'")
        await admin.execute("CREATE SCHEMA " + _identifier(schema))
        await admin.execute("CREATE TABLE " + _identifier(schema) + ".sentinel(value integer NOT NULL)")
        await admin.execute("INSERT INTO " + _identifier(schema) + ".sentinel VALUES (1)")
        await admin.execute("GRANT USAGE ON SCHEMA " + _identifier(schema) + " TO " + _identifier(role))
        await admin.execute("GRANT SELECT ON " + _identifier(schema) + ".sentinel TO " + _identifier(role))
        reader = await asyncpg.connect(dsn, user=role, password=password)
        yield SimpleNamespace(admin=admin, reader=reader, role=role, schema=schema)
    finally:
        if reader is not None:
            await reader.close()
        try:
            await admin.execute("DROP SCHEMA IF EXISTS " + _identifier(schema) + " CASCADE")
            await admin.execute("DROP ROLE IF EXISTS " + _identifier(role))
        finally:
            await admin.close()


@asynccontextmanager
async def _stage(connection, *, schema="pg_temp", missing_index=False):
    names_by_kind = {kind: "sample_" + kind + "_" + uuid4().hex for kind in typed._DDL}
    relations_by_kind = {kind: _identifier(schema) + "." + _identifier(name) for kind, name in names_by_kind.items()}
    created_relations = []
    try:
        for kind, definition in typed._DDL.items():
            await connection.execute(
                ("CREATE TEMP TABLE " if schema == "pg_temp" else "CREATE TABLE ")
                + relations_by_kind[kind]
                + "("
                + definition
                + ")"
            )
            created_relations.append(relations_by_kind[kind])
        await connection.execute("CREATE INDEX ON " + relations_by_kind["addresses"] + "(address_key)")
        if missing_index:
            await connection.execute(
                "ALTER TABLE "
                + relations_by_kind["cells"]
                + " DROP CONSTRAINT "
                + _identifier(names_by_kind["cells"] + "_pkey")
            )
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            replay = await typed.bind_typed_graph_stage(connection, schema_name=schema, table_names=names_by_kind)
            yield replay
    finally:
        for relation in reversed(created_relations):
            await connection.execute("DROP TABLE " + relation)


async def _scratch_state(connection, replay):
    return {kind: await connection.fetchval("SELECT count(*) FROM " + replay.relation(kind)) for kind in replay.names}


async def _scope_state(connection):
    return await connection.fetchrow(
        "SELECT current_setting('transaction_read_only'),current_setting('transaction_isolation')"
    )


async def _fill_replay(connection, replay):
    await typed._check_page(
        connection,
        replay,
        [
            {
                "resource_type": "Location",
                "resource_id": "office",
                "payload_hash": "a" * 64,
                "payload_json": json.dumps({"status": "active"}),
                "site_id": str(UUID(int=1)),
            }
        ],
        "https://example.test/fhir",
        typed.policy_date("2026-01-01"),
    )
    await connection.copy_records_to_table(
        replay.names["addresses"],
        schema_name="pg_temp",
        records=[("a" * 64, None, None, None, None, None, None, UUID(int=1), "synthetic|street")],
    )
    await connection.copy_records_to_table(
        replay.names["pairs"],
        schema_name="pg_temp",
        records=[
            (
                "network",
                "npi",
                "1234567893",
                UUID(int=1),
                UUID(int=1),
                "synthetic|street",
                "PractitionerRole",
                "role",
                "office",
                "role_network_direct_location",
                1,
            )
        ],
    )


async def test_batches_restore_data_and_reader_snapshot(sample_reader):
    connection = sample_reader.reader
    async with _stage(connection) as replay:
        original, scope = await _scratch_state(connection, replay), await _scope_state(connection)
        assert tuple(scope) == ("on", "repeatable read")
        sentinel = "SELECT value FROM " + _identifier(sample_reader.schema) + ".sentinel"
        assert await connection.fetchval(sentinel) == 1
        await sample_reader.admin.execute("UPDATE " + _identifier(sample_reader.schema) + ".sentinel SET value=2")
        cursor = await connection.cursor("SELECT generate_series(1,70)")
        for index in range(1, 71):
            assert (await cursor.fetch(1))[0][0] == index
            async with sampled._sample_replay_batch(connection, replay):
                await _fill_replay(connection, replay)
                for kind in replay.names:
                    assert await connection.fetchval("SELECT count(*) FROM " + replay.relation(kind)) == 1
            assert await _scratch_state(connection, replay) == original
            replay.require_ready(complete=False)
        assert not await cursor.fetch(1)
        assert await _scope_state(connection) == scope
        assert await connection.fetchval(sentinel) == 1


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_failed_batch_restores_reader(sample_reader, failure):
    connection = sample_reader.reader
    async with _stage(connection) as replay:
        original, scope = await _scratch_state(connection, replay), await _scope_state(connection)
        cursor = await connection.cursor("SELECT generate_series(1,2)")
        with pytest.raises(failure):
            async with sampled._sample_replay_batch(connection, replay):
                await _fill_replay(connection, replay)
                assert (await cursor.fetch(1))[0][0] == 1
                if failure is asyncio.CancelledError:
                    asyncio.current_task().cancel()
                    await asyncio.sleep(0)
                else:
                    raise failure()
        while asyncio.current_task().cancelling():
            asyncio.current_task().uncancel()
        assert await _scratch_state(connection, replay) == original
        assert await _scope_state(connection) == scope
        assert (await cursor.fetch(1))[0][0] == 2
        async with sampled._sample_replay_batch(connection, replay):
            await _fill_replay(connection, replay)
        assert await _scratch_state(connection, replay) == original


async def test_nonempty_batch_refuses_without_rewrite(sample_reader):
    connection = sample_reader.reader
    async with _stage(connection) as replay:
        await _fill_replay(connection, replay)
        original = await _scratch_state(connection, replay)
        with pytest.raises(TypedFHIRGraphError, match="restart_from_zero"):
            async with sampled._sample_replay_batch(connection, replay):
                pytest.fail("nonempty scratch was admitted")
        assert await _scratch_state(connection, replay) == original
        for kind in replay.names:
            assert await connection.fetchval("SELECT count(*) FROM " + replay.relation(kind)) == 1


@pytest.mark.parametrize("invalid", ["permanent", "index"])
async def test_foreign_workspace_refuses_rewrite(sample_reader, invalid):
    connection = sample_reader.admin if invalid == "permanent" else sample_reader.reader
    async with _stage(
        connection,
        schema=sample_reader.schema if invalid == "permanent" else "pg_temp",
        missing_index=invalid == "index",
    ) as replay:
        original = await _scratch_state(connection, replay)
        with pytest.raises(TypedFHIRGraphError if invalid == "permanent" else asyncpg.UndefinedObjectError):
            async with sampled._sample_replay_batch(connection, replay):
                pytest.fail("foreign scratch was admitted")
        assert await _scratch_state(connection, replay) == original
