# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Connected native compact preparation, independent custody and retained reads."""

import asyncio
import hashlib
import json
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from api.network_address_scope import (
    current_network_address_scope,
    network_address_read_scope,
    scoped_address_parameters,
    scoped_address_relation_sql,
    scoped_address_statement,
)
from db.models import EntityAddressUnified
from process.network_address_projection import _identifier
from process.network_compact_layout import CompactAddressRelation
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate, prepare_network_candidate
from process.network_membership_publication import NetworkPublicationError, publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from tests import test_network_address_artifact_postgres as address_tests
from tests.registry_candidate_custody_postgres_support import administrative_freeze_callback, install_candidate_custody
from tests.test_network_serving_schema_postgres import serving_schema


async def _full_two_offices(connection, roles, original):
    from process.network_membership_serving_indexes import _build_serving_indexes, _serving_plan

    schema = await original(connection, roles)
    table = f'"{schema}".entity_address_unified'
    await connection.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS postal_address")
    await connection.execute(f"UPDATE {table} SET npi=entity_id::bigint,type='practice',long=-73.5")
    columns = list(EntityAddressUnified.__table__.columns.keys())
    selected_columns = [
        "'other-office'"
        if name == "location_key"
        else "$1::uuid"
        if name == "address_key"
        else 'original."' + name + '"'
        for name in columns
    ]
    await connection.execute(
        f"INSERT INTO {table}("
        + ",".join('"' + name + '"' for name in columns)
        + ") SELECT "
        + ",".join(selected_columns)
        + f" FROM {table} original WHERE location_key='old-office'",
        uuid4(),
    )
    for extension in ("intarray", "btree_gin", "btree_gist"):
        await connection.execute("CREATE EXTENSION IF NOT EXISTS " + extension)
    await _build_serving_indexes(connection, schema, _serving_plan(schema))
    return schema


async def _drop_final_owner(connection, role):
    if not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role):
        return
    await connection.execute(f'DROP OWNED BY "{role}"')
    await connection.execute(f'DROP ROLE "{role}"')


async def _cleanup_named_preparations(fixture):
    from directory_membership_test_support import cleanup_registry_network_preparations

    await cleanup_registry_network_preparations(fixture.connection, fixture.preparation_ids, fixture.control_schema)
    for namespace in reversed(fixture.created_control_namespaces):
        await fixture.connection.execute("DROP SCHEMA IF EXISTS " + _identifier(namespace) + " CASCADE")


async def _named_compact_runtime(fixture, stack, monkeypatch):
    """Reuse normal loading roles, preparation controls and the administrator handoff."""
    from directory_membership_test_support import prepare_registry_catalog_foundation

    from process.registry_publication_consumer import RegistryPublicationConsumerConfig
    from process.registry_publication_worker import _configured_worker_runtime

    fixture.created_control_namespaces, fixture.preparation_ids = [], []
    stack.push_async_callback(_cleanup_named_preparations, fixture)
    for extension in ("intarray", "btree_gin", "btree_gist", "postgis"):
        await fixture.connection.execute("CREATE EXTENSION IF NOT EXISTS " + extension + " WITH SCHEMA public")
    await prepare_registry_catalog_foundation(fixture, monkeypatch)
    await install_candidate_custody(fixture, stack)
    roles = fixture.custody_roles
    consumer = RegistryPublicationConsumerConfig(
        fixture.control_schema,
        roles["custodian"],
        fixture.executor_role,
        fixture.roles["reader"],
        fixture.roles["owner"],
        roles["loading"],
        reader_roles=fixture.writer_roles["reader_roles"],
    )
    applications = (
        set(roles.values())
        | set(fixture.roles.values())
        | set(getattr(getattr(fixture, "address", None), "roles", {}).values())
    )
    settings = SimpleNamespace(
        consumer=consumer,
        execution_url=fixture.execution_url,
        sql_limits=fixture.sql_limits,
        ptg=None,
        preparation_roles={
            "namespace_owner_role": roles["owner"],
            "publisher_role": fixture.executor_role,
            "application_roles": sorted(applications - {roles["owner"]}),
        },
    )
    configured = await _configured_worker_runtime(settings, stack)
    fixture.preparation_roles = configured.preparation_roles
    fixture.seal_candidate = administrative_freeze_callback(fixture, configured)


@pytest.fixture
async def compact_publication_db(monkeypatch):
    original_source = address_tests._prepare_source

    async def prepare_source(connection, roles):
        return await _full_two_offices(connection, roles, original_source)

    monkeypatch.setattr(address_tests, "_prepare_source", prepare_source)
    async with AsyncExitStack() as stack:
        address = await stack.enter_async_context(asynccontextmanager(address_tests.address_db.__wrapped__)())
        final_owner = "compact_final_" + uuid4().hex
        stack.push_async_callback(_drop_final_owner, address.admin, final_owner)
        await address.admin.execute(f'CREATE ROLE "{final_owner}" NOLOGIN')
        monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", address.url.render_as_string(hide_password=False))
        connection, control_schema, engine = await stack.enter_async_context(
            asynccontextmanager(serving_schema.__wrapped__)()
        )
        fixture = SimpleNamespace(
            connection=connection,
            control_schema=control_schema,
            engine=engine,
            roles={**address.roles, "owner": final_owner},
            address=address,
            source=address.source,
            dataset_id=str(uuid4()),
            schema_id=str(uuid4()),
            producer_id=str(uuid4()),
        )
        from directory_membership_test_support import retire_empty_registry_catalog_placeholder

        await retire_empty_registry_catalog_placeholder(connection)
        await _named_compact_runtime(fixture, stack, monkeypatch)
        fixture.runtime_roles = tuple(sorted((fixture.executor_role, address.roles["reader"])))
        await connection.execute(
            f"GRANT INSERT(candidate_id,batch_id,row_count,input_sha256,copy_sha256,input_bytes,copy_bytes) "
            f'ON "{control_schema}".network_membership_batch TO "{fixture.executor_role}"'
        )
        await connection.execute(f'GRANT USAGE ON SCHEMA "{control_schema}" TO "{address.roles["reader"]}"')
        await connection.execute(
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{control_schema}" TO "{address.roles["reader"]}"'
        )
        await connection.execute(
            f'INSERT INTO "{control_schema}".network_registry_identity(network_id,allocation_key) '
            "OVERRIDING SYSTEM VALUE VALUES(42,$1),(88,$2),(99,$3)",
            uuid4(),
            uuid4(),
            uuid4(),
        )
        fixture.custodian = await asyncpg.connect(
            fixture.custody_url.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        stack.push_async_callback(fixture.custodian.close)
        fixture.reader_engine = create_async_engine(
            address.reader_url.set(drivername="postgresql+asyncpg"), poolclass=NullPool
        )
        stack.push_async_callback(fixture.reader_engine.dispose)
        yield fixture


async def _candidate(fixture, networks, expected_head, *, locations=None, source_manifest=None):
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        fixture.dataset_id,
        fixture.schema_id,
        fixture.producer_id,
        str(candidate_id),
        "network_candidate_" + candidate_id.hex,
    )
    if locations is None:
        locations = await fixture.connection.fetch(
            f'SELECT location_key,entity_type,entity_id,address_key FROM "{fixture.address.schema}".entity_address_unified ORDER BY location_key'
        )
    facts = [
        {
            "network_id": network_id,
            "provider_system": office["entity_type"],
            "provider_id": office["entity_id"],
            "location_id": str(office["address_key"]),
            "evidence_id": hashlib.sha256(("synthetic-office-" + str(index)).encode()).hexdigest(),
        }
        for index, (network_id, office) in enumerate(zip(networks, locations, strict=True))
    ]
    input_bytes = json.dumps(facts, separators=(",", ":")).encode()
    connection = fixture.execution_connection
    async with connection.transaction():
        await create_network_candidate(
            connection,
            copy_target,
            source_generations={
                "unified_address": fixture.source.generation_id,
                **({"retained_network_source": source_manifest.manifest_sha256} if source_manifest else {}),
            },
            approved_custom_revision=0,
            expected_head=expected_head,
            expected_rows=len(facts),
            control_schema=fixture.control_schema,
        )
        await _bind_offices(connection, copy_target, locations)
        await admit_network_membership_batch(
            connection,
            copy_target,
            batch_id=uuid4(),
            input_bytes=input_bytes,
            expected_input_sha256=hashlib.sha256(input_bytes).hexdigest(),
            control_schema=fixture.control_schema,
        )
        await seal_network_candidate(connection, copy_target, control_schema=fixture.control_schema)
    return copy_target


async def _bind_offices(connection, target, locations):
    await connection.execute(f"""CREATE TABLE "{target.schema_name}".provider_location_binding(
        provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
        location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
        PRIMARY KEY(provider_system,provider_id,location_id))""")
    await connection.copy_records_to_table(
        "provider_location_binding",
        schema_name=target.schema_name,
        records=[
            (
                office["entity_type"],
                office["entity_id"],
                office["address_key"],
                office["location_key"],
                office["entity_type"],
                office["entity_id"],
            )
            for office in locations
        ],
    )


async def _prepare(fixture, target):
    return await prepare_network_candidate(
        fixture.execution_connection,
        target,
        fixture.source,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
        compact=True,
    )


async def _seal(fixture, target):
    if target.candidate_id not in fixture.preparation_ids:
        fixture.preparation_ids.append(target.candidate_id)
    return await fixture.seal_candidate(target)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError("validation failed"), asyncio.CancelledError()])
async def test_compact_callback_registers_cleanup_before_validation(failure):
    identity = uuid4()
    target = MembershipCopyTarget(*(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex)
    fixture = SimpleNamespace(preparation_ids=[])

    async def validate(supplied):
        assert supplied == target and fixture.preparation_ids == [target.candidate_id]
        if failure is not None:
            raise failure
        return {"candidate_id": target.candidate_id, "state": "validated"}

    fixture.seal_candidate = AsyncMock(side_effect=validate)
    if failure is None:
        assert await _seal(fixture, target) == {"candidate_id": target.candidate_id, "state": "validated"}
        await _seal(fixture, target)
        assert fixture.preparation_ids == [target.candidate_id]
    else:
        with pytest.raises(type(failure)) as caught:
            await _seal(fixture, target)
        assert caught.value is failure


async def _publish(fixture, target):
    async with fixture.execution_connection.transaction(isolation="repeatable_read"):
        return await publish_network_candidate(
            fixture.execution_connection,
            target,
            control_schema=fixture.control_schema,
            preparation_roles=fixture.preparation_roles,
        )


@asynccontextmanager
async def _reader(fixture):
    async with fixture.reader_engine.connect() as connection, connection.begin():
        await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        native = (await connection.get_raw_connection()).driver_connection
        assert native.is_in_transaction()
        yield connection, native


async def _read(fixture, generation_id=None, network_ids=(42,)):
    async with _reader(fixture) as (connection, native):
        manifest = await resolve_network_serving_manifest(
            native, generation_id=generation_id, control_schema=fixture.control_schema
        )
        offices = await _scoped_rows(connection, manifest, network_ids)
    return manifest, offices


async def _scoped_rows(connection, manifest, network_ids):
    assert manifest.compact_layout is not None
    with network_address_read_scope(manifest, network_ids):
        relation_sql = scoped_address_relation_sql("mrf.entity_address_unified")
        parameters = scoped_address_parameters({})
        original = EntityAddressUnified.__table__
        raw = text(f"SELECT * FROM {relation_sql} offices ORDER BY location_key")
        typed = scoped_address_statement(select(original).order_by(original.c.location_key))
        raw_rows = [dict(row) for row in (await connection.execute(raw, parameters)).mappings()]
        typed_rows = [dict(row) for row in (await connection.execute(typed, parameters)).mappings()]
        assert raw_rows == typed_rows
    assert current_network_address_scope() is None
    return raw_rows


async def _refs(fixture, target):
    rows = await fixture.connection.fetch(
        f'SELECT * FROM "{target.schema_name}".network_slice_ref ORDER BY network_id,office_bucket'
    )
    return {(row["network_id"], row["office_bucket"]): dict(row) for row in rows}


@pytest.mark.asyncio
async def test_connected_compact_custody_publication_and_reuse(compact_publication_db):
    fixture = compact_publication_db
    source_relation = CompactAddressRelation(fixture.source.schema_name, fixture.source.table_name)
    source_before = await fixture.connection.fetch(
        f'SELECT * FROM "{fixture.address.schema}".entity_address_unified ORDER BY location_key'
    )
    first = await _candidate(fixture, (42, 88), 0)
    prepared = await _prepare(fixture, first)
    assert prepared["state"] == "ready"
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", first.schema_name + ".entity_address_unified")
        is None
    )
    with pytest.raises(NetworkPublicationError, match="Generation data validation is incomplete"):
        await _publish(fixture, first)
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
        )
        is None
    )
    validation = await _seal(fixture, first)
    assert validation == {"candidate_id": first.candidate_id, "state": "validated"}
    first_publication = await _publish(fixture, first)
    first_pin, first_rows = await _read(fixture)
    assert len(first_rows) == 1 and first_rows[0]["canonical_network_ids"] == [42]
    assert first_pin.address_relation == first_pin.compact_layout.base == source_relation
    first_refs = await _refs(fixture, first)
    await _assert_sealed_reader(fixture, first)
    await _assert_readonly_replay(fixture, first, first_publication)
    second = await _candidate(fixture, (42, 99), first_pin.generation_id)
    await _prepare(fixture, second)
    await _seal(fixture, second)
    second_refs = await _refs(fixture, second)
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{second.schema_name}".network_office_edge') == 1
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", second.schema_name + ".entity_address_unified")
        is None
    )
    unchanged = next(key for key in first_refs if key[0] == 42)
    assert second_refs[unchanged] == first_refs[unchanged]
    assert len({entry["artifact_candidate_id"] for entry in second_refs.values()}) == 2
    await _assert_rollback_then_publish(fixture, second, first_pin, first_rows)
    assert (
        await fixture.connection.fetch(
            f'SELECT * FROM "{fixture.address.schema}".entity_address_unified ORDER BY location_key'
        )
        == source_before
    )
    assert (await _read(fixture))[0].compact_layout.base == source_relation


async def _assert_sealed_reader(fixture, target):
    reader = fixture.address.reader
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await reader.execute(f'UPDATE "{target.schema_name}".network_office_edge SET location_key=location_key')
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await fixture.execution_connection.execute(f'SET ROLE "{fixture.roles["owner"]}"')


async def _assert_readonly_replay(fixture, target, published):
    statements = []
    connection = fixture.execution_connection
    connection.add_query_logger(statements.append)
    try:
        replay = await prepare_and_publish_network_candidate(
            connection,
            target,
            fixture.source,
            **fixture.writer_roles,
            control_schema=fixture.control_schema,
        )
        await asyncio.sleep(0)
    finally:
        connection.remove_query_logger(statements.append)
    assert replay == published | {"replayed": True}
    forbidden = ("ALTER", "CREATE", "INSERT", "UPDATE", "DELETE", "GRANT", "REVOKE", "COMMENT")
    assert not any(
        statement.query.lstrip().upper().startswith(forbidden) or "FOR UPDATE" in statement.query
        for statement in statements
    )


async def _assert_rollback_then_publish(fixture, copy_target, first_pin, first_rows):
    async with _reader(fixture) as (connection, native):
        pinned = await resolve_network_serving_manifest(native, control_schema=fixture.control_schema)
        old_membership = await _scoped_rows(connection, pinned, (88,))
        assert len(old_membership) == 1 and old_membership[0]["canonical_network_ids"] == [88]
        transaction = fixture.execution_connection.transaction(isolation="repeatable_read")
        await transaction.start()
        try:
            provisional = await publish_network_candidate(
                fixture.execution_connection,
                copy_target,
                control_schema=fixture.control_schema,
                preparation_roles=fixture.preparation_roles,
            )
            assert provisional["generation_id"] != first_pin.generation_id
        finally:
            await transaction.rollback()
        assert (
            await fixture.connection.fetchval(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
            )
            == first_pin.generation_id
        )
        assert await _scoped_rows(connection, pinned, (88,)) == old_membership
        second_publication = await asyncio.wait_for(_publish(fixture, copy_target), timeout=10)
        assert await _scoped_rows(connection, pinned, (88,)) == old_membership
        assert (
            await resolve_network_serving_manifest(native, control_schema=fixture.control_schema)
        ).generation_id == first_pin.generation_id
    pin_after_rollback, rows_after_rollback = await _read(fixture)
    assert pin_after_rollback.generation_id == second_publication["generation_id"] and rows_after_rollback == first_rows
    latest, latest_rows = await _read(fixture)
    retained, retained_rows = await _read(fixture, first_pin.generation_id)
    assert latest.generation_id == second_publication["generation_id"] != retained.generation_id
    assert latest_rows == retained_rows == first_rows
    assert (await _read(fixture, network_ids=(88,)))[1] == []
    assert {artifact.candidate_id for artifact in latest.compact_layout.artifacts} >= {
        first_pin.candidate_id,
        copy_target.candidate_id,
    }
