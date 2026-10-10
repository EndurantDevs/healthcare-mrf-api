# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native candidate ownership transfer, runtime denial and rollback checks."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process.network_membership_writer_closure import (
    NetworkWriterClosureError,
    freeze_network_candidate_writers,
    verify_network_candidate_writer_closure,
)
from tests.test_network_address_projection_postgres import projection_db
from tests.test_network_serving_schema_postgres import serving_schema


@pytest.fixture
async def writer_db(projection_db):
    fixture = projection_db
    suffix = uuid4().hex
    role_by_kind = {name: f"nc_{name}_{suffix}" for name in ("owner", "loader", "reader", "publisher", "outsider")}
    created_roles = []
    try:
        for name in role_by_kind.values():
            await fixture.connection.execute(f'CREATE ROLE "{name}" NOLOGIN')
            created_roles.append(name)
        await _prepare_roles(fixture, role_by_kind)
        yield SimpleNamespace(**vars(fixture), roles=role_by_kind)
    finally:
        await fixture.connection.execute("RESET ROLE")
        await fixture.observer.execute("RESET ROLE")
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{fixture.copy_target.schema_name}" CASCADE')
        for name in reversed(created_roles):
            await fixture.connection.execute(f'DROP OWNED BY "{name}"')
            await fixture.connection.execute(f'DROP ROLE "{name}"')
        assert not await fixture.connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles
        )


async def _prepare_roles(fixture, roles):
    connection = fixture.connection
    namespace = f'"{fixture.copy_target.schema_name}"'
    database = await connection.fetchval("SELECT quote_ident(current_database())")
    await connection.execute(f'GRANT CREATE ON DATABASE {database} TO "{roles["owner"]}","{roles["publisher"]}"')
    await connection.execute(f'GRANT "{roles["loader"]}","{roles["owner"]}" TO "{roles["publisher"]}"')
    await connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{roles["loader"]}"')
    await connection.execute(
        f'GRANT SELECT,UPDATE ON "{fixture.control_schema}".network_membership_candidate TO "{roles["loader"]}"'
    )
    await connection.execute(f'ALTER SCHEMA {namespace} OWNER TO "{roles["loader"]}"')
    for table in ("network_membership", "provider_location_binding"):
        await connection.execute(f'ALTER TABLE {namespace}.{table} OWNER TO "{roles["loader"]}"')


def _arguments(fixture):
    return {
        "owner_role": fixture.roles["owner"],
        "loader_roles": (fixture.roles["loader"],),
        "reader_roles": (fixture.roles["reader"],),
        "control_schema": fixture.control_schema,
    }


async def _freeze(fixture, *, retain=True):
    await fixture.connection.execute(f'SET ROLE "{fixture.roles["publisher"]}"')
    try:
        async with fixture.connection.transaction():
            receipt = await freeze_network_candidate_writers(
                fixture.connection, fixture.copy_target, **_arguments(fixture)
            )
            if retain:
                await fixture.connection.execute(
                    f'UPDATE "{fixture.control_schema}".network_membership_candidate '
                    "SET validation_json=coalesce(validation_json,'{}'::jsonb)||jsonb_build_object('writer_closure',$1::jsonb) "
                    "WHERE candidate_id=$2",
                    json.dumps(receipt),
                    fixture.copy_target.candidate_id,
                )
            return receipt
    finally:
        await fixture.connection.execute("RESET ROLE")


async def _verify(fixture):
    await fixture.connection.execute(f'SET ROLE "{fixture.roles["publisher"]}"')
    try:
        async with fixture.connection.transaction():
            return await verify_network_candidate_writer_closure(
                fixture.connection, fixture.copy_target, **_arguments(fixture)
            )
    finally:
        await fixture.connection.execute("RESET ROLE")


async def test_freeze_closes_copy_dml_ddl_and_preserves_select(writer_db):
    fixture = writer_db
    namespace = f'"{fixture.copy_target.schema_name}"'
    source_indexes = await fixture.connection.fetch(
        "SELECT oid,relname FROM pg_class WHERE relnamespace=to_regnamespace($1) AND relkind='i' ORDER BY oid",
        fixture.control_schema,
    )
    await fixture.connection.execute(f"GRANT ALL ON SCHEMA {namespace} TO PUBLIC")
    await fixture.connection.execute(f"GRANT ALL ON {namespace}.network_membership TO PUBLIC")
    await fixture.connection.execute(
        f'GRANT UPDATE(network_id) ON {namespace}.network_membership TO "{fixture.roles["outsider"]}"'
    )
    receipt = await _freeze(fixture)
    assert receipt["component"] == "network_candidate_writer_closure" and receipt["revision"] == 1
    assert set(receipt["relation_oids"]) == {"network_membership", "provider_location_binding"}
    assert receipt["index_oids"] and receipt["index_table_oids"]
    assert await _verify(fixture) == receipt
    observer = fixture.observer
    await observer.execute(f'SET ROLE "{fixture.roles["loader"]}"')
    assert await observer.fetchval(f"SELECT count(*) FROM {namespace}.network_membership") == 4
    statements = [
        f"INSERT INTO {namespace}.network_membership SELECT * FROM {namespace}.network_membership",
        f"UPDATE {namespace}.network_membership SET network_id=7",
        f"DELETE FROM {namespace}.network_membership",
        f"TRUNCATE {namespace}.network_membership",
        f"CREATE TABLE {namespace}.extra(id integer)",
        f"ALTER TABLE {namespace}.network_membership ADD COLUMN extra integer",
    ]
    for statement in statements:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await observer.execute(statement)
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await observer.copy_records_to_table(
            "network_membership",
            schema_name=fixture.copy_target.schema_name,
            records=[(42, "npi", "provider-a", fixture.location_ids[0], "evidence")],
        )
    await observer.execute("RESET ROLE")
    await observer.execute(f'SET ROLE "{fixture.roles["reader"]}"')
    assert await observer.fetchval(f"SELECT count(*) FROM {namespace}.provider_location_binding") == 3
    assert (
        await fixture.connection.fetch(
            "SELECT oid,relname FROM pg_class WHERE relnamespace=to_regnamespace($1) AND relkind='i' ORDER BY oid",
            fixture.control_schema,
        )
        == source_indexes
    )


async def test_strict_replay_has_no_ddl(writer_db):
    fixture = writer_db
    receipt = await _freeze(fixture)
    queries = []
    fixture.connection.add_query_logger(queries.append)
    await asyncio.sleep(0)
    queries.clear()
    assert await _freeze(fixture) == receipt
    assert await _verify(fixture) == receipt
    await asyncio.sleep(0)
    assert not any(query.query.lstrip().split()[0] in {"ALTER", "GRANT", "REVOKE", "CREATE"} for query in queries)


@pytest.mark.parametrize(
    "tamper", ["table_grant", "column_grant", "owner_membership", "reader_createrole", "default_grant", "grant_option"]
)
async def test_native_privilege_tampering_fails_closed(writer_db, tamper):
    fixture = writer_db
    await _freeze(fixture)
    table = f'"{fixture.copy_target.schema_name}".network_membership'
    command_by_tamper = {
        "table_grant": f'GRANT SELECT ON {table} TO "{fixture.roles["outsider"]}"',
        "column_grant": f'GRANT UPDATE(network_id) ON {table} TO "{fixture.roles["loader"]}"',
        "owner_membership": f'GRANT "{fixture.roles["owner"]}" TO "{fixture.roles["loader"]}"',
        "reader_createrole": f'ALTER ROLE "{fixture.roles["reader"]}" CREATEROLE',
        "default_grant": f'ALTER DEFAULT PRIVILEGES FOR ROLE "{fixture.roles["owner"]}" '
        f'IN SCHEMA "{fixture.copy_target.schema_name}" GRANT UPDATE ON TABLES TO PUBLIC',
        "grant_option": f'GRANT SELECT ON {table} TO "{fixture.roles["reader"]}" WITH GRANT OPTION',
    }
    await fixture.connection.execute(command_by_tamper[tamper])
    with pytest.raises(NetworkWriterClosureError):
        await _verify(fixture)
    with pytest.raises(NetworkWriterClosureError):
        await _freeze(fixture)


@pytest.mark.parametrize("tamper", ["table", "view", "sequence", "heap_oid", "index_oid"])
async def test_native_object_tampering_fails_closed(writer_db, tamper):
    fixture = writer_db
    receipt = await _freeze(fixture)
    namespace = f'"{fixture.copy_target.schema_name}"'
    if tamper == "heap_oid":
        await fixture.connection.execute(f"DROP TABLE {namespace}.network_membership")
        await fixture.connection.execute(f"CREATE TABLE {namespace}.network_membership(id integer)")
    elif tamper == "index_oid":
        index_name = next(iter(receipt["index_oids"]))
        constraint_name = await fixture.connection.fetchval(
            "SELECT quote_ident(conname) FROM pg_constraint WHERE conindid=$1::oid", receipt["index_oids"][index_name]
        )
        await fixture.connection.execute(
            f"ALTER TABLE {namespace}.provider_location_binding DROP CONSTRAINT {constraint_name}"
        )
        await fixture.connection.execute(
            f'CREATE UNIQUE INDEX "{index_name}" ON {namespace}.provider_location_binding(provider_system,provider_id,location_id)'
        )
    else:
        statement = {"table": "CREATE TABLE", "view": "CREATE VIEW", "sequence": "CREATE SEQUENCE"}[tamper]
        definition = {"table": "(id integer)", "view": "AS SELECT 1 id", "sequence": ""}[tamper]
        await fixture.connection.execute(f"{statement} {namespace}.extra {definition}")
    with pytest.raises(NetworkWriterClosureError):
        await _verify(fixture)


async def test_raw_closure_can_capture_projection_once(writer_db):
    fixture = writer_db
    first = await _freeze(fixture)
    namespace = f'"{fixture.copy_target.schema_name}"'
    await fixture.connection.execute(f"CREATE TABLE {namespace}.entity_address_unified(location_key text)")
    await fixture.connection.execute(
        f'ALTER TABLE {namespace}.entity_address_unified OWNER TO "{fixture.roles["loader"]}"'
    )
    await fixture.connection.execute(f"CREATE INDEX projected_key ON {namespace}.entity_address_unified(location_key)")
    await fixture.connection.execute(
        f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET state='ready',index_ready=true WHERE candidate_id=$1",
        fixture.copy_target.candidate_id,
    )
    final = await _freeze(fixture)
    assert final["schema_oid"] == first["schema_oid"]
    assert final["relation_oids"].items() >= first["relation_oids"].items()
    assert "entity_address_unified" in final["relation_oids"]
    assert await _verify(fixture) == final
    await fixture.connection.execute(f"CREATE INDEX later_index ON {namespace}.entity_address_unified(location_key)")
    with pytest.raises(NetworkWriterClosureError):
        await _freeze(fixture)


async def test_caller_rollback_restores_native_owners_and_grants(writer_db):
    fixture = writer_db
    namespace = f'"{fixture.copy_target.schema_name}"'
    await fixture.connection.execute(f"GRANT UPDATE ON {namespace}.network_membership TO PUBLIC")
    before = await _owners_and_acl(fixture)
    await fixture.connection.execute(f'SET ROLE "{fixture.roles["publisher"]}"')
    with pytest.raises(RuntimeError, match="rollback"):
        async with fixture.connection.transaction():
            await freeze_network_candidate_writers(fixture.connection, fixture.copy_target, **_arguments(fixture))
            raise RuntimeError("rollback")
    await fixture.connection.execute("RESET ROLE")
    assert await _owners_and_acl(fixture) == before


async def _owners_and_acl(fixture):
    return await fixture.connection.fetch(
        """SELECT n.nspowner,n.nspacl,c.oid,c.relowner,c.relacl FROM pg_namespace n
        JOIN pg_class c ON c.relnamespace=n.oid WHERE n.nspname=$1 ORDER BY c.oid""",
        fixture.copy_target.schema_name,
    )


async def test_nowait_lock_failure_preserves_candidate(writer_db):
    fixture = writer_db
    before = await _owners_and_acl(fixture)
    async with fixture.observer.transaction():
        await fixture.observer.execute(f'SELECT * FROM "{fixture.copy_target.schema_name}".network_membership')
        with pytest.raises(asyncpg.LockNotAvailableError):
            await asyncio.wait_for(_freeze(fixture), timeout=2)
    assert await _owners_and_acl(fixture) == before


async def test_freeze_removes_scoped_default_acl_leaks(writer_db):
    fixture = writer_db
    namespace = f'"{fixture.copy_target.schema_name}"'
    await fixture.connection.execute(
        f'ALTER DEFAULT PRIVILEGES FOR ROLE "{fixture.roles["loader"]}" IN SCHEMA {namespace} '
        "GRANT ALL ON TABLES TO PUBLIC"
    )
    await _freeze(fixture)
    assert await fixture.connection.fetchval(
        """SELECT NOT EXISTS(SELECT 1 FROM pg_default_acl d CROSS JOIN LATERAL aclexplode(d.defaclacl) a
        WHERE d.defaclnamespace=to_regnamespace($1) AND a.grantee=0)""",
        fixture.copy_target.schema_name,
    )
    assert await _verify(fixture)


async def test_publisher_set_only_owner_membership_supported(writer_db):
    fixture = writer_db
    await fixture.connection.execute(
        f'GRANT "{fixture.roles["owner"]}" TO "{fixture.roles["publisher"]}" WITH INHERIT FALSE, SET TRUE'
    )
    receipt = await _freeze(fixture)
    assert await _verify(fixture) == receipt


@pytest.mark.parametrize("attribute", ["LOGIN", "SUPERUSER", "CREATEDB", "CREATEROLE", "REPLICATION", "BYPASSRLS"])
async def test_unsafe_owner_attributes_rejected(writer_db, attribute):
    fixture = writer_db
    await fixture.connection.execute(f'ALTER ROLE "{fixture.roles["owner"]}" {attribute}')
    with pytest.raises(NetworkWriterClosureError):
        await _freeze(fixture)


async def test_wrong_scope_state_transaction_and_owner_set_rejected(writer_db):
    fixture = writer_db
    with pytest.raises(ValueError, match="caller-owned transaction"):
        await freeze_network_candidate_writers(fixture.connection, fixture.copy_target, **_arguments(fixture))
    async with fixture.connection.transaction():
        with pytest.raises(ValueError, match="ownership scope"):
            await freeze_network_candidate_writers(
                fixture.connection, replace(fixture.copy_target, dataset_id=str(uuid4())), **_arguments(fixture)
            )
    await fixture.connection.execute(
        f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET state='open' WHERE candidate_id=$1",
        fixture.copy_target.candidate_id,
    )
    with pytest.raises(NetworkWriterClosureError, match="sealed"):
        await _freeze(fixture)
    await fixture.connection.execute(
        f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET state='sealed' WHERE candidate_id=$1",
        fixture.copy_target.candidate_id,
    )
    await fixture.connection.execute(
        f'GRANT "{fixture.roles["owner"]}" TO "{fixture.roles["publisher"]}" WITH SET FALSE'
    )
    with pytest.raises(NetworkWriterClosureError, match="assume"):
        await _freeze(fixture)


async def test_global_default_acl_propagation_removed_without_global_changes(writer_db):
    fixture = writer_db
    namespace = f'"{fixture.copy_target.schema_name}"'
    await fixture.connection.execute(
        f'ALTER DEFAULT PRIVILEGES FOR ROLE "{fixture.roles["loader"]}" GRANT ALL ON TABLES TO "{fixture.roles["outsider"]}"'
    )
    await fixture.connection.execute(f'SET ROLE "{fixture.roles["loader"]}"')
    try:
        await fixture.connection.execute(f"CREATE TABLE {namespace}.entity_address_unified(location_key text)")
    finally:
        await fixture.connection.execute("RESET ROLE")
    before = await fixture.connection.fetch(
        "SELECT oid,defaclacl FROM pg_default_acl WHERE defaclnamespace=0 AND defaclrole=$1::regrole",
        fixture.roles["loader"],
    )
    receipt = await _freeze(fixture)
    assert "entity_address_unified" in receipt["relation_oids"]
    assert (
        await fixture.connection.fetch(
            "SELECT oid,defaclacl FROM pg_default_acl WHERE defaclnamespace=0 AND defaclrole=$1::regrole",
            fixture.roles["loader"],
        )
        == before
    )
    assert await _verify(fixture) == receipt


async def test_database_create_needed_only_for_transfer(writer_db):
    fixture = writer_db
    database = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
    revoke = f'REVOKE CREATE ON DATABASE {database} FROM "{fixture.roles["owner"]}","{fixture.roles["publisher"]}"'
    await fixture.connection.execute(revoke)
    with pytest.raises(NetworkWriterClosureError, match="database CREATE"):
        await _freeze(fixture)
    await fixture.connection.execute(
        f'GRANT CREATE ON DATABASE {database} TO "{fixture.roles["owner"]}","{fixture.roles["publisher"]}"'
    )
    receipt = await _freeze(fixture)
    await fixture.connection.execute(revoke)
    assert await _verify(fixture) == receipt
    assert await _freeze(fixture) == receipt


@pytest.mark.parametrize("tamper", ["extra_key", "revision_type", "oid_type"])
async def test_retained_proof_shape_is_exact(writer_db, tamper):
    fixture = writer_db
    receipt = await _freeze(fixture)
    if tamper == "extra_key":
        receipt["extra"] = True
    elif tamper == "revision_type":
        receipt["revision"] = True
    else:
        receipt["relation_oids"]["network_membership"] = float(receipt["relation_oids"]["network_membership"])
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate '
        "SET validation_json=jsonb_build_object('writer_closure',$1::jsonb) WHERE candidate_id=$2",
        json.dumps(receipt),
        fixture.copy_target.candidate_id,
    )
    with pytest.raises(NetworkWriterClosureError):
        await _verify(fixture)


async def test_cancellation_rolls_back_transfer(writer_db):
    fixture = writer_db
    before = await _owners_and_acl(fixture)
    entered = asyncio.Event()
    continue_after_transfer = asyncio.Event()
    original_execute = fixture.connection.execute

    class InterruptedConnection:
        def __getattr__(self, name):
            return getattr(fixture.connection, name)

        async def execute(self, query, *args):
            result = await original_execute(query, *args)
            if query.startswith("ALTER TABLE"):
                entered.set()
                await continue_after_transfer.wait()
            return result

    async def freeze_until_cancelled():
        await fixture.connection.execute(f'SET ROLE "{fixture.roles["publisher"]}"')
        try:
            async with fixture.connection.transaction():
                await freeze_network_candidate_writers(
                    InterruptedConnection(), fixture.copy_target, **_arguments(fixture)
                )
        finally:
            await fixture.connection.execute("RESET ROLE")

    task = asyncio.create_task(freeze_until_cancelled())
    await asyncio.wait_for(entered.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await _owners_and_acl(fixture) == before
