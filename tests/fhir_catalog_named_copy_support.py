# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native copied metadata and attached directory reads through the public API contract."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api import network_serving_read_context as reader_context
from api.endpoint import network_catalog as endpoint
from db.registry_fhir_catalog import SNAPSHOT_COLUMNS
from process.registry_fhir_catalog_custody import PHYSICAL_TABLES
from process.registry_fhir_catalog_validation import metadata_generation
from tests.test_network_catalog_routes import v2_envelope
from tests.test_registry_fhir_catalog_custody_postgres import (
    freeze_metadata,
    metadata_custody,
    seed_source_projection,
)


async def _publish_metadata(held, metadata):
    snapshot = await seed_source_projection(metadata)
    attached = held.retained
    snapshot.update(network_generation=attached.generation_id, manifest_sha256=attached.manifest_sha256)
    snapshot["metadata_generation"] = metadata_generation(snapshot)
    await metadata.connection.execute(f'UPDATE "{metadata.control_schema}".network_registry_identity SET network_id=42')
    for table in ("network", "binding", "relationship"):
        await metadata.connection.execute(
            f'UPDATE "{metadata.target.schema_name}".registry_fhir_catalog_{table} '
            "SET network_id=42,metadata_generation=$1",
            snapshot["metadata_generation"],
        )
    snapshot = await freeze_metadata(metadata, snapshot=snapshot)
    control, connection = held.fixture.control_schema, held.fixture.connection
    columns = tuple(field for field in SNAPSHOT_COLUMNS if field != "published_at")
    snapshot_values = [
        json.dumps(snapshot[field]) if isinstance(snapshot[field], (dict, list)) else snapshot[field]
        for field in columns
    ]
    await connection.execute(
        f'INSERT INTO "{control}".registry_fhir_catalog_snapshot ({",".join(columns)},published_at) '
        f"VALUES ({','.join('$' + str(index) for index in range(1, len(snapshot_values) + 1))},clock_timestamp())",
        *snapshot_values,
    )
    await connection.execute(
        f'UPDATE "{control}".network_serving_control SET metadata_generation=$1', snapshot["metadata_generation"]
    )
    reader = held.fixture.roles["reader"]
    for schema in (control, metadata.target.schema_name):
        await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{reader}"')
        await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{reader}"')
    return snapshot


async def _response(held, query=None, *, excluded=(), authorized=True):
    document = v2_envelope({"view": "fhir-plans-v1"} | (query or {}), excluded_network_ids=list(excluded))
    async with held.database.reader_session() as session:
        request = SimpleNamespace(
            headers={"Authorization": "Bearer synthetic-catalog-control-token" if authorized else "Bearer wrong"},
            query_string="",
            body=json.dumps(document).encode(),
            ctx=SimpleNamespace(sa_session=session),
        )
        transaction = session.get_transaction()
        reply = await endpoint._catalog_response(request, "list")
        assert session.get_transaction() is transaction and session.get_nested_transaction() is None
        return reply.status, json.loads(reply.body), reply.headers


@asynccontextmanager
async def _copied_schema(connection, schema, tables, reader):
    retained = schema + "_original"
    previous = await connection.fetchval("SELECT to_regnamespace($1)::oid", schema)
    assert await connection.fetchval("SELECT to_regnamespace($1)", retained) is None
    try:
        async with connection.transaction():
            await connection.execute(f'ALTER SCHEMA "{schema}" RENAME TO "{retained}"; CREATE SCHEMA "{schema}"')
            for table in tables:
                await connection.execute(
                    f'CREATE TABLE "{schema}"."{table}" (LIKE "{retained}"."{table}" INCLUDING ALL); '
                    f'INSERT INTO "{schema}"."{table}" OVERRIDING SYSTEM VALUE SELECT * FROM "{retained}"."{table}"'
                )
            await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{reader}"')
            await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{reader}"')
        assert await connection.fetchval("SELECT to_regnamespace($1)::oid", schema) != previous
        yield
    finally:
        if await connection.fetchval("SELECT to_regnamespace($1)", retained) is not None:
            async with connection.transaction():
                await connection.execute(
                    f'DROP SCHEMA "{schema}" CASCADE; ALTER SCHEMA "{retained}" RENAME TO "{schema}"'
                )


async def _native_refusals(held, metadata, snapshot):
    connection, reader = held.fixture.connection, held.fixture.roles["reader"]
    base = held.retained.compact_layout.base
    relation = f'"{base.schema_name}"."{base.table_name}"'
    await connection.execute(f'REVOKE SELECT ON {relation} FROM "{reader}"')
    try:
        assert (await _response(held))[0] == 503
    finally:
        await connection.execute(f'GRANT SELECT ON {relation} TO "{reader}"')
    table = f'"{metadata.target.schema_name}".registry_fhir_catalog_network'
    await connection.execute(f"ALTER TABLE {table} RENAME TO missing_network")
    try:
        assert (await _response(held))[0] == 503
    finally:
        await connection.execute(
            f'ALTER TABLE "{metadata.target.schema_name}".missing_network RENAME TO registry_fhir_catalog_network'
        )
    # The query still validates returned documents rather than trusting the stored receipt alone.
    await connection.execute(f"UPDATE {table} SET aliases='[null]'::jsonb")
    try:
        status, document, _ = await _response(held)
        assert status == 400 and document["error"]["code"] == "network_catalog_request_invalid"
    finally:
        await connection.execute(f"UPDATE {table} SET aliases='[]'::jsonb")
    manifest_table = f'"{held.fixture.control_schema}".network_serving_manifest'
    await connection.execute(
        f"UPDATE {manifest_table} SET manifest_sha256=$1 WHERE generation_id=$2",
        "e" * 64,
        snapshot["network_generation"],
    )
    try:
        assert (await _response(held))[0] == 503
    finally:
        await connection.execute(
            f"UPDATE {manifest_table} SET manifest_sha256=$1 WHERE generation_id=$2",
            snapshot["manifest_sha256"],
            snapshot["network_generation"],
        )
    status, _, _ = await _response(held, {"network_generation": str(held.current.generation_id)})
    assert status == 409 and snapshot["network_generation"] != held.current.generation_id
    assert (await _response(held, authorized=False))[0] == 403
    assert (await _response(held, excluded=(42,)))[1]["total"] == 0
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            await connection.execute(f'SET LOCAL ROLE "{reader}"')
            await connection.execute(f"DELETE FROM {table}")
    async with held.database.reader_session() as session:
        with pytest.raises(DBAPIError) as error:
            await session.execute(text(f"DELETE FROM {table}"))
        assert "42501" in str(error.value) or "read-only transaction" in str(error.value)


async def assert_copied_fhir_view(held, monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-catalog-control-token")
    monkeypatch.setattr(reader_context, "db", held.database)
    async with asynccontextmanager(metadata_custody.__wrapped__)() as metadata:
        snapshot = await _publish_metadata(held, metadata)
        before = await _response(held)
        assert before[0] == 200 and before[1]["total"] == 1
        assert before[1]["items"][0]["directory_available"] is True
        assert before[1]["generation"] == str(held.retained.generation_id)
        assert before[1]["metadata_generation"] == snapshot["metadata_generation"]
        controls = (
            "network_serving_control",
            "network_serving_manifest",
            "network_membership_candidate",
            "registry_fhir_catalog_snapshot",
            "registry_approved_record",
        )
        connection, reader = held.fixture.connection, held.fixture.roles["reader"]
        base = held.retained.compact_layout.base
        async with (
            _copied_schema(connection, held.fixture.control_schema, controls, reader),
            _copied_schema(connection, metadata.target.schema_name, sorted(PHYSICAL_TABLES), reader),
            _copied_schema(connection, base.schema_name, (base.table_name,), reader),
        ):
            assert await _response(held) == before
            raw = await connection.fetchval(
                f'SELECT custody_json::text FROM "{held.fixture.control_schema}".registry_fhir_catalog_snapshot '
                "WHERE metadata_generation=$1",
                snapshot["metadata_generation"],
            )
            assert json.loads(raw) == snapshot["custody_json"]
            await _native_refusals(held, metadata, snapshot)
            assert await _response(held) == before
        assert await _response(held) == before


@asynccontextmanager
async def _address_rotation(held):
    connection, base = held.fixture.connection, held.retained.compact_layout.base
    live = f'"{base.schema_name}"."{base.table_name}"'
    retained_name = f"{base.table_name}_retained_{base.table_oid:x}"
    retained = f'"{base.schema_name}"."{retained_name}"'
    state = SimpleNamespace(attempted=False, rotated=False, task=None, started=asyncio.Event())

    async def rotate():
        state.attempted = True
        async with connection.transaction():
            await connection.execute("SET LOCAL lock_timeout='5s'")
            state.started.set()
            await connection.execute(
                f'ALTER TABLE {live} RENAME TO "{retained_name}"; '
                f"CREATE TABLE {live} (LIKE {retained}); "
                f'GRANT SELECT ON {live} TO "{held.fixture.roles["reader"]}"'
            )
        state.rotated = True

    try:
        yield rotate, state
    finally:
        if state.task is not None:
            if not state.task.done():
                state.task.cancel()
            await asyncio.gather(state.task, return_exceptions=True)
        if state.rotated:
            async with connection.transaction():
                await connection.execute(
                    f'DROP TABLE IF EXISTS {live}; ALTER TABLE {retained} RENAME TO "{base.table_name}"'
                )


async def _is_rotation_waiting(connection, writer, state):
    await state.started.wait()
    while not state.task.done():
        if await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=$1 AND NOT granted)", writer.get_server_pid()
        ):
            return True
        await asyncio.sleep(0)
    return False


async def assert_fhir_address_rotation(held, monkeypatch, timing):
    """Interleave native rotation with the genuine Reader's metadata and page queries."""
    from process import registry_fhir_catalog_read as catalog
    from process.network_published_api_pin import published_api_pin_statement

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-catalog-control-token")
    monkeypatch.setattr(reader_context, "db", held.database)
    async with asynccontextmanager(metadata_custody.__wrapped__)() as metadata:
        snapshot = await _publish_metadata(held, metadata)
        before = await _response(held)
        assert before[0] == 200 and before[1]["items"][0]["directory_available"] is True
        async with _address_rotation(held) as (rotate, state):
            original_pin, original_fetchrow = catalog.read_published_api_pin, asyncpg.Connection.fetchrow
            metadata_sql = published_api_pin_statement(held.fixture.control_schema)

            async def before_pin(*args, **kwargs):
                if not state.attempted:
                    await rotate()
                return await original_pin(*args, **kwargs)

            async def after_selection(connection, statement, *args, **kwargs):
                entry = await original_fetchrow(connection, statement, *args, **kwargs)
                if statement == metadata_sql and not state.attempted:
                    state.task = asyncio.create_task(rotate())
                    assert await asyncio.wait_for(_is_rotation_waiting(connection, held.fixture.connection, state), 5)
                    assert not state.task.done()
                return entry

            with monkeypatch.context() as patch:
                if timing == "before_lock":
                    patch.setattr(catalog, "read_published_api_pin", before_pin)
                else:
                    patch.setattr(asyncpg.Connection, "fetchrow", after_selection)
                assert await _response(held) == before
            assert state.attempted
            if state.task is not None:
                await asyncio.wait_for(state.task, 5)
            assert state.rotated and await _response(held) == before
            base = held.retained.compact_layout.base
            live = f'"{base.schema_name}"."{base.table_name}"'
            await held.fixture.connection.execute(f'REVOKE SELECT ON {live} FROM "{held.fixture.roles["reader"]}"')
            assert await _response(held) == before
            await held.fixture.connection.execute(f"DROP TABLE {live}")
            assert await _response(held) == before
            assert before[1]["generation"] == str(snapshot["network_generation"])
            assert snapshot["network_generation"] != held.current.generation_id
        assert await _response(held) == before
