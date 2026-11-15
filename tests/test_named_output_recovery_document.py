# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The format transition keeps saved bytes and rejects unsupported documents."""

import importlib.util
import json
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from tests.test_cms_archive_transaction_migration import _load
from tests.test_reference_family_archive_postgres import _database_url


def _migration():
    path = Path(__file__).parents[1] / "alembic/versions/20261115120000_named_output_recovery_document.py"
    spec = importlib.util.spec_from_file_location("named_output_document_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_recovery_format_transition_preserves_old_bytes_and_document_limits():
    connection = await asyncpg.connect(
        make_url(_database_url()).set(drivername="postgresql").render_as_string(hide_password=False),
        command_timeout=10,
    )
    schema = "recovery_" + uuid4().hex
    transaction = connection.transaction()
    await transaction.start()
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        table = f'"{schema}".provider_directory_serving_recovery'
        await connection.execute(
            f"CREATE TABLE {table}(document jsonb NOT NULL, document_sha256 text NOT NULL, "
            "CONSTRAINT pd_serving_recovery_document CHECK (jsonb_typeof(document)='object' "
            "AND document->>'version'='1' AND octet_length(document::text)<=2097152), "
            "CHECK (document_sha256=encode(sha256(convert_to(document::text,'UTF8')),'hex')))"
        )
        insert = f"INSERT INTO {table} SELECT $1::jsonb,encode(sha256(convert_to(($1::jsonb)::text,'UTF8')),'hex')"
        await connection.execute(insert, json.dumps({"version": 1, "old_opaque_metadata": {"oid": 7}}))
        before = await connection.fetchrow(f"SELECT document::text,document_sha256 FROM {table}")
        await connection.execute(_migration()._statement(schema))
        assert await connection.fetchrow(f"SELECT document::text,document_sha256 FROM {table}") == before
        await connection.execute(insert, json.dumps({"version": 2, "relation": "snapshot.example"}))
        for document in ({"version": 3}, {"version": 2, "body": "x" * 2097152}, []):
            with pytest.raises(asyncpg.CheckViolationError):
                async with connection.transaction():
                    await connection.execute(insert, json.dumps(document))
        assert await connection.fetchval(f"SELECT count(*) FROM {table}") == 2
    finally:
        await transaction.rollback()
        await connection.close()


async def test_generation_transition_keeps_legacy_row_and_requires_complete_logical_origin():
    connection = await asyncpg.connect(
        make_url(_database_url()).set(drivername="postgresql").render_as_string(hide_password=False),
        command_timeout=10,
    )
    schema = "generation_" + uuid4().hex
    transaction = connection.transaction()
    await transaction.start()
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        table = f'"{schema}".entity_address_result_generation'
        previous = _load("20260914100000")._shape_check()
        await connection.execute(
            f"CREATE TABLE {table}(singleton boolean PRIMARY KEY, local_lineage_id uuid NOT NULL, "
            "local_generation bigint NOT NULL, origin_lineage_id uuid,origin_generation bigint,"
            "published_at timestamptz,relation_oids bigint[], "
            f"CONSTRAINT entity_address_result_generation_shape_check CHECK ({previous}))"
        )
        await connection.execute(
            f"INSERT INTO {table} VALUES(true,$1,1,$1,1,now(),ARRAY[1,2,3,4,5,6,7]::bigint[])", uuid4()
        )
        before = await connection.fetchrow(f"SELECT * FROM {table}")
        await connection.execute(_migration()._generation_statement(schema))
        assert await connection.fetchrow(f"SELECT * FROM {table}") == before
        await connection.execute(f"UPDATE {table} SET local_generation=2,origin_generation=2,relation_oids=NULL")
        assert await connection.fetchval(f"SELECT relation_oids IS NULL FROM {table}")
        for assignment in ("origin_generation=NULL", "published_at=NULL", "origin_generation=0", "local_generation=-1"):
            with pytest.raises(asyncpg.CheckViolationError):
                async with connection.transaction():
                    await connection.execute(f"UPDATE {table} SET {assignment}")
    finally:
        await transaction.rollback()
        await connection.close()


async def _install_historical_plan_owner(connection, schema, plan):
    """Install the actual historical preimage, never a replacement production guard."""
    from db.migration_provider_directory_dataset_candidates import candidate_functions

    table = f'"{schema}".pd_dataset_migration_plan'
    await connection.execute(
        f"CREATE TABLE {table}(singleton boolean PRIMARY KEY CHECK(singleton),plan jsonb NOT NULL)"
    )
    await connection.execute(f"INSERT INTO {table} VALUES(true,$1::jsonb)", json.dumps(plan))
    declaration = f'CREATE FUNCTION "{schema}".guard_pd_dataset_migration_owner()'
    script = candidate_functions(schema)
    assert script.count(declaration) == 1
    await connection.execute(declaration + script.split(declaration, 1)[1].split("$fn$;", 1)[0] + "$fn$;")
    await connection.execute(
        f'CREATE TRIGGER pd_dataset_plan_owner BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {table} FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".guard_pd_dataset_migration_owner()'
    )
    await connection.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER pd_dataset_plan_owner")
    return table


async def _assert_native_plan_permissions(connection, schema, table, reader, writer):
    """Real PostgreSQL grants keep ordinary readers read-only and the plan writer writable."""
    for role in (reader, writer):
        await connection.execute(f'CREATE ROLE "{role}" NOLOGIN')
        await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
        await connection.execute(f'GRANT SELECT ON {table} TO "{role}"')
    await connection.execute(f'GRANT UPDATE ON {table} TO "{writer}"')
    await connection.execute(f'SET LOCAL ROLE "{reader}"')
    try:
        assert await connection.fetchval(f"SELECT count(*) FROM {table}") == 1
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(f"UPDATE {table} SET plan=plan")
    finally:
        await connection.execute("RESET ROLE")
    await connection.execute(f'SET LOCAL ROLE "{writer}"')
    try:
        assert await connection.execute(f"UPDATE {table} SET plan=plan") == "UPDATE 1"
    finally:
        await connection.execute("RESET ROLE")


async def test_owner_guard_retirement_keeps_raw_plan_and_native_permissions():
    connection = await asyncpg.connect(
        make_url(_database_url()).set(drivername="postgresql").render_as_string(hide_password=False), command_timeout=10
    )
    identity = uuid4().hex
    schema = "plan_retirement_" + identity
    reader, writer = "plan_reader_" + identity, "plan_writer_" + identity
    transaction = connection.transaction()
    await transaction.start()
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        plan_by_field = {
            "complete": True,
            "tables": {"legacy": {"original_oid": {"copied": "opaque"}}},
            "extensions": {"journal": {"bytes": "unchanged"}},
        }
        table = await _install_historical_plan_owner(connection, schema, plan_by_field)
        before = await connection.fetchval(f"SELECT plan::text FROM {table}")
        for statement in _migration()._retire_dataset_plan_owner(schema):
            await connection.execute(statement)
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=to_regclass($1) AND tgname=$2)",
            table,
            "pd_dataset_plan_owner",
        )
        assert await connection.fetchval(
            "SELECT to_regprocedure($1) IS NULL", f'"{schema}".guard_pd_dataset_migration_owner()'
        )
        await _assert_native_plan_permissions(connection, schema, table, reader, writer)
        assert await connection.fetchval(f"SELECT plan::text FROM {table}") == before
    finally:
        await transaction.rollback()
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", [reader, writer]
        )
        await connection.close()
