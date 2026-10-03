# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in genuine-role proof using one disposable database and exact UUID roles."""

import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database, db
from process import entity_address_alias_guard as guard
from process import entity_address_snapshot_preparation as preparation
from tests import test_address_evidence_alias_lifecycle_db as evidence
from tests import test_address_numeric_grid_alias_lifecycle_db as numeric
from tests import test_address_strict_source_backfill_db as backfill
from tests.test_address_numeric_grid_alias_runtime_db import _create_alias_probe_schema


def _admin_url():
    raw = os.getenv("HLTHPRT_ALIAS_GUARD_TEST_DSN")
    if not raw:
        pytest.skip("HLTHPRT_ALIAS_GUARD_TEST_DSN is not set")
    url = make_url(raw)
    if not url.drivername.startswith("postgresql") or url.host not in {"127.0.0.1", "localhost"} or not url.username:
        pytest.fail("alias guard tests require an explicit local PostgreSQL administrator")
    return url.set(drivername="postgresql", database="postgres")


async def _remove_guard_resources(admin, database_name, role_by_kind, attempted_roles, is_database_attempted):
    if is_database_attempted:
        await admin.execute(f'DROP DATABASE IF EXISTS "{database_name}"')
    for name in reversed(attempted_roles):
        if await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", name):
            await admin.execute(f'REVOKE SET ON PARAMETER session_replication_role FROM "{name}"')
        await admin.execute(f'DROP ROLE IF EXISTS "{name}"')
    assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", database_name)
    assert not await admin.fetchval(
        "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))", list(role_by_kind.values())
    )
    await admin.close()
    print("Verified absent:", database_name, *role_by_kind.values())


@asynccontextmanager
async def _owned_guard_database(monkeypatch):
    """Register exact cleanup before creating anything; never force-drop active sessions."""
    admin_url = _admin_url()
    suffix = uuid4().hex
    database_name = "alias_guard_test_" + suffix
    role_by_kind = {kind: "alias_" + kind + "_" + suffix for kind in ("owner", "writer", "publisher")}
    login_password = uuid4().hex
    admin = await asyncpg.connect(admin_url.render_as_string(hide_password=False), timeout=10)
    databases = []
    attempted_roles = []
    connection = None
    is_database_attempted = False
    try:
        assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", database_name)
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))", list(role_by_kind.values())
        )
        for kind, name in role_by_kind.items():
            attempted_roles.append(name)
            await admin.execute(
                f'CREATE ROLE "{name}" {"NOLOGIN" if kind == "owner" else "LOGIN"} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
            )
            if kind != "owner":
                await admin.execute(f"ALTER ROLE \"{name}\" PASSWORD '{login_password}'")
        await admin.execute(f'GRANT "{role_by_kind["owner"]}" TO "{role_by_kind["publisher"]}"')
        is_database_attempted = True
        await admin.execute(f'CREATE DATABASE "{database_name}" TEMPLATE template0')
        connection = await asyncpg.connect(admin_url.set(database=database_name).render_as_string(hide_password=False))
        await connection.execute("CREATE EXTENSION postgis")
        await connection.execute(f'CREATE SCHEMA hp_snapshot_retention AUTHORIZATION "{role_by_kind["owner"]}"')
        for kind in ("writer", "publisher"):
            url = admin_url.set(
                drivername="postgresql+asyncpg",
                database=database_name,
                username=role_by_kind[kind],
                password=login_password,
            )
            engine = create_async_engine(url, pool_size=2, max_overflow=0, hide_parameters=True)
            databases.append(
                Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
            )
        monkeypatch.setenv("HLTHPRT_DB_DATABASE", database_name)
        monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
        monkeypatch.setattr(db, "engine", databases[0].engine)
        monkeypatch.setattr(db, "session_factory", databases[0].session_factory)
        monkeypatch.setattr(db, "_database_name", database_name)
        monkeypatch.setenv("HLTHPRT_ADDRESS_EVIDENCE_ALIAS_NATIVE", "false")
        yield SimpleNamespace(
            admin=connection, roles=role_by_kind, publisher=databases[1], admin_role=admin_url.username
        )
    finally:
        for database in databases:
            await database.disconnect()
        if connection is not None:
            await connection.close()
        await _remove_guard_resources(admin, database_name, role_by_kind, attempted_roles, is_database_attempted)


async def _provision_test_schema(resources, schema, *, initial_generation):
    """Mirror explicit narrow provisioning inside the task-owned test database only."""
    admin, roles = resources.admin, resources.roles
    await _create_alias_probe_schema(admin, schema, "generation_guard")
    await admin.execute(f'GRANT USAGE,CREATE ON SCHEMA "{schema}" TO "{roles["writer"]}","{roles["owner"]}"')
    tables = await admin.fetch(
        "SELECT relname FROM pg_class JOIN pg_namespace n ON n.oid=relnamespace WHERE n.nspname=$1 AND relkind='r'",
        schema,
    )
    for row in tables:
        owner = (
            roles["owner"]
            if row["relname"] in {guard.alias._STATE_TABLE, guard.alias._ALIAS_TABLE}
            else roles["writer"]
        )
        await admin.execute(f'ALTER TABLE "{schema}"."{row["relname"]}" OWNER TO "{owner}"')
    for statement in guard.generation_guard_statements(schema):
        await admin.execute(statement)
    for function in guard.GUARDS:
        await admin.execute(f'ALTER FUNCTION "{schema}"."{function}"() OWNER TO "{roles["owner"]}"')
    await admin.execute(f'GRANT SELECT ON "{schema}".address_alias_state_v1 TO "{roles["writer"]}"')
    await admin.execute(
        f'GRANT SELECT,INSERT,UPDATE ({",".join(guard.REVOCATION_COLUMNS)}) ON "{schema}".address_alias_v1 TO "{roles["writer"]}"'
    )
    await admin.execute(f'UPDATE "{schema}".address_alias_state_v1 SET generation=$1', initial_generation)
    return await _publisher_ready(resources, schema)


async def _publisher_ready(resources, schema):
    async with resources.publisher.transaction() as session:
        owner_oid = await preparation.require_entity_address_archive_publisher(session, db_schema=schema)
        return await guard.require_entity_address_alias_guard(session, schema=schema, owner_oid=owner_oid)


async def _ordinary_alias_capture(schema, *, generation, active_count):
    async with db.transaction() as session:
        assert not await db.scalar(
            "SELECT has_table_privilege(current_user,CAST(:relation AS regclass),'UPDATE,DELETE,TRUNCATE,MAINTAIN')",
            relation=f'"{schema}".address_alias_state_v1',
        )
        receipt = await guard.alias.capture_entity_address_alias_semantic_receipt(session, schema_name=schema)
        assert receipt.local_generation == generation
        assert receipt.active_alias_count == active_count
        return receipt


async def _numeric_lifecycle(resources):
    schema = "guard_numeric"
    signature = await _provision_test_schema(resources, schema, initial_generation=0)
    source_key = await numeric._insert_archive_address(
        schema, first_line="1548 E 4500", second_line="Suite 202", strict_source_bits=1
    )
    target_key = await numeric._insert_archive_address(
        schema, first_line="1548 E 4500 S", second_line="Suite 202", strict_source_bits=6
    )
    shadow = await numeric.run_numeric_grid_alias(mode="shadow", schema=schema)
    await numeric._apply_reviewed_alias(schema, source_key, target_key, shadow)
    await numeric._assert_alias_retry(schema, shadow)
    active_receipt = await _ordinary_alias_capture(schema, generation=1, active_count=1)
    with pytest.raises(RuntimeError, match="rollback probe"):
        async with db.transaction():
            await db.status(
                f"UPDATE \"{schema}\".address_alias_v1 SET revoked_at=now(),revoked_reason='rollback',revoked_by='reviewer',revoke_run_id=apply_run_id"
            )
            assert await db.scalar(f'SELECT generation FROM "{schema}".address_alias_state_v1') == 2
            raise RuntimeError("rollback probe")
    assert await db.scalar(f'SELECT generation FROM "{schema}".address_alias_state_v1') == 1
    await numeric._revoke_reviewed_alias(schema, source_key, target_key, shadow)
    revoked_receipt = await _ordinary_alias_capture(schema, generation=2, active_count=0)
    assert active_receipt.active_alias_sha256 != revoked_receipt.active_alias_sha256
    assert await _publisher_ready(resources, schema) == signature
    return schema


async def _evidence_lifecycle(resources):
    schema = "guard_evidence"
    signature = await _provision_test_schema(resources, schema, initial_generation=1)
    keys = await evidence._seed_archive_matrix(schema)
    await evidence._seed_visible_matrix(schema, keys)
    shadow = await evidence._shadow_and_assert_matrix(schema, keys)
    await evidence._apply_and_assert_alias(schema, keys, shadow)
    await evidence._revoke_and_assert_alias(schema, keys, shadow)
    assert await _publisher_ready(resources, schema) == signature


async def _backfill_lifecycle(resources):
    schema = "guard_backfill"
    signature = await _provision_test_schema(resources, schema, initial_generation=1)
    probe = await backfill._seed_backfill_probe(schema)
    await backfill._seed_independent_source_evidence(probe)
    result = await backfill._assert_successful_backfill(probe)
    await backfill._assert_archive_evidence(probe)
    await backfill._assert_backfill_retry(probe, result)
    await backfill._assert_fresh_shadow_eligible(probe)
    assert await _publisher_ready(resources, schema) == signature


async def _ordinary_refusals(resources, schema):
    """No ordinary direct counter/code/DDL bypass, including the identity sequence."""
    statements = [
        f'UPDATE "{schema}".address_alias_state_v1 SET generation=0',
        f'UPDATE "{schema}".address_alias_state_v1 SET schema_version=1',
        f'DELETE FROM "{schema}".address_alias_state_v1',
        f'TRUNCATE "{schema}".address_alias_v1',
        f"UPDATE \"{schema}\".address_alias_v1 SET source_identity_key='changed'",
        f'DELETE FROM "{schema}".address_alias_v1',
        f'ALTER TABLE "{schema}".address_alias_v1 DISABLE TRIGGER ALL',
        f'ALTER FUNCTION "{schema}".addr_alias_generation_after_insert_v1() SECURITY INVOKER',
        f'ALTER SCHEMA "{schema}" RENAME TO guard_replaced',
        f"SELECT pg_catalog.setval('\"{schema}\".address_alias_v1_alias_id_seq',1)",
    ]
    for statement in statements:
        with pytest.raises(Exception, match="permission denied|must be owner"):
            await db.status(statement)
    async with db.transaction() as session:
        with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="publisher is unavailable"):
            await preparation.require_entity_address_archive_publisher(session, db_schema=schema)
    for function in guard.GUARDS:
        assert (
            await db.scalar(
                "SELECT has_function_privilege(current_user,CAST(:function AS regprocedure),'EXECUTE')",
                function=f'"{schema}"."{function}"()',
            )
            is False
        )
    await db.status(f'CREATE TABLE "{schema}".ordinary_create_still_works (value integer)')


async def _authority_refusals(resources, schema):
    admin, writer = resources.admin, resources.roles["writer"]
    await admin.execute(f'GRANT UPDATE(generation) ON "{schema}".address_alias_state_v1 TO "{writer}"')
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="ordinary mutation bypass"):
        await _publisher_ready(resources, schema)
    await admin.execute(f'REVOKE UPDATE(generation) ON "{schema}".address_alias_state_v1 FROM "{writer}"')
    await admin.execute(f'GRANT EXECUTE ON FUNCTION "{schema}".addr_alias_generation_after_insert_v1() TO PUBLIC')
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="trigger authority differs"):
        await _publisher_ready(resources, schema)
    await admin.execute(f'REVOKE EXECUTE ON FUNCTION "{schema}".addr_alias_generation_after_insert_v1() FROM PUBLIC')
    await admin.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{writer}"')
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="relation authority is unavailable"):
        await _publisher_ready(resources, schema)
    await admin.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{resources.admin_role}"')
    await admin.execute(f'GRANT USAGE,CREATE ON SCHEMA "{schema}" TO "{writer}"')
    await _publisher_ready(resources, schema)


async def _guard_code_refusals(resources, schema):
    """Reject benign but unreviewed code, disabled replication guards and state hooks."""
    admin = resources.admin
    await admin.execute(
        f'ALTER TABLE "{schema}".address_alias_v1 ENABLE TRIGGER address_alias_v1_generation_insert_trg'
    )
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="trigger authority differs"):
        await _publisher_ready(resources, schema)
    await admin.execute(
        f'ALTER TABLE "{schema}".address_alias_v1 ENABLE ALWAYS TRIGGER address_alias_v1_generation_insert_trg'
    )
    await admin.execute(
        f'CREATE OR REPLACE FUNCTION "{schema}".addr_alias_generation_after_insert_v1() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$ BEGIN RETURN NULL; END; $$'
    )
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="trigger authority differs"):
        await _publisher_ready(resources, schema)
    for statement in guard.generation_guard_statements(schema):
        await admin.execute(statement)
    await admin.execute(
        f'CREATE FUNCTION "{schema}".benign_state_check(bigint) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT true $$'
    )
    await admin.execute(
        f'ALTER TABLE "{schema}".address_alias_state_v1 ADD CONSTRAINT benign_probe CHECK ("{schema}".benign_state_check(generation))'
    )
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="state expressions are unsupported"):
        await _publisher_ready(resources, schema)
    await admin.execute(f'ALTER TABLE "{schema}".address_alias_state_v1 DROP CONSTRAINT benign_probe')
    await admin.execute(f'DROP FUNCTION "{schema}".benign_state_check(bigint)')
    await _publisher_ready(resources, schema)


async def _state_expression_refusals(resources, schema):
    admin = resources.admin
    await admin.execute(
        f'ALTER TABLE "{schema}".address_alias_state_v1 ADD CONSTRAINT benign_builtin_check CHECK (generation >= -1)'
    )
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="state expressions are unsupported"):
        await _publisher_ready(resources, schema)
    async with db.transaction() as session:
        with pytest.raises(guard.alias.EntityAddressSnapshotAliasError, match="state expressions are unsupported"):
            await guard.alias.capture_entity_address_alias_semantic_receipt(session, schema_name=schema)
    await admin.execute(f'ALTER TABLE "{schema}".address_alias_state_v1 DROP CONSTRAINT benign_builtin_check')
    await admin.execute(f'ALTER TABLE "{schema}".address_alias_state_v1 ALTER COLUMN generation SET DEFAULT abs(0)')
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="state expressions are unsupported"):
        await _publisher_ready(resources, schema)
    await admin.execute(f'ALTER TABLE "{schema}".address_alias_state_v1 ALTER COLUMN generation SET DEFAULT 0')
    await _publisher_ready(resources, schema)
    await _ordinary_alias_capture(schema, generation=2, active_count=0)


async def _role_and_replication_refusals(resources, schema):
    """SET ROLE reachability and replica mode cannot evade generation authority."""
    admin, roles = resources.admin, resources.roles
    await admin.execute(f'GRANT "{roles["owner"]}" TO "{roles["writer"]}" WITH INHERIT FALSE')
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="ordinary mutation bypass"):
        await _publisher_ready(resources, schema)
    await admin.execute(f'REVOKE "{roles["owner"]}" FROM "{roles["writer"]}"')
    await admin.execute(f'GRANT SET ON PARAMETER session_replication_role TO "{roles["writer"]}"')
    with pytest.raises(Exception, match="revocation is immutable"):
        async with db.transaction():
            await db.status("SET LOCAL session_replication_role=replica")
            await db.status(f"UPDATE \"{schema}\".address_alias_v1 SET revoked_by='changed reviewer'")
    await admin.execute(f'REVOKE SET ON PARAMETER session_replication_role FROM "{roles["writer"]}"')
    await _publisher_ready(resources, schema)


async def test_guard_preserves_genuine_ordinary_workflows_and_rejects_bypass(monkeypatch):
    async with _owned_guard_database(monkeypatch) as resources:
        schema = await _numeric_lifecycle(resources)
        await _evidence_lifecycle(resources)
        await _backfill_lifecycle(resources)
        await _ordinary_refusals(resources, schema)
        await _authority_refusals(resources, schema)
        await _guard_code_refusals(resources, schema)
        await _state_expression_refusals(resources, schema)
        await _role_and_replication_refusals(resources, schema)
