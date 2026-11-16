# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native retained address reads keep their data when an optional index is added."""

import asyncio
import json
from contextlib import AsyncExitStack
from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import EntityAddressUnified, NPIData
from process import network_indexed_address_artifact as indexed
from process.entity_address_unified import _post_publish_index_plan
from process.network_address_artifact import (
    NetworkAddressArtifactError,
    verify_network_address_artifact,
)
from process.network_bootstrap_sources import _copy_native_batch
from process.network_compact_layout import CompactAddressRelation
from process.network_membership_serving_indexes import (
    _build_serving_indexes,
    _serving_plan,
)
from process.provider_directory_generic_source_provision import (
    freeze_fhir_source_snapshot,
)
from tests.test_network_address_artifact_postgres import _admin_url, _cleanup
from tests.test_network_compact_address_origin_postgres import (
    _address_descriptor,
    _adopt,
    _publish,
    _read,
    _seal,
)


async def check_optional_index_publication_reads(indexed_db):
    fixture = indexed_db
    schema = fixture.indexed_source.schema_name
    optional_index = f'"{schema}".entity_address_unified_idx_source_count'
    ready = await fixture.connection.fetchval(
        "SELECT indisvalid AND indisready AND indislive FROM pg_index WHERE indexrelid=to_regclass($1)", optional_index
    )
    assert ready is True
    await fixture.connection.execute("DROP INDEX " + optional_index)
    target = await _adopt(fixture)
    descriptor = await _address_descriptor(fixture, target)
    before = deepcopy(descriptor)
    assert (await _read(fixture))[1] == fixture.previous_rows
    await fixture.connection.execute(
        f'CREATE INDEX entity_address_unified_idx_source_count ON "{schema}".entity_address_unified(source_count)'
    )

    await _seal(fixture, target)
    await _publish(fixture, target)
    _, rows = await _read(fixture)
    assert rows == fixture.previous_rows
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        relation = await verify_network_address_artifact(fixture.execution_connection, descriptor)

    assert relation == CompactAddressRelation(**descriptor["table"])
    assert descriptor == before


async def check_required_index_removal(indexed_db, suffix):
    fixture = indexed_db
    target = await _adopt(fixture)
    descriptor = await _address_descriptor(fixture, target)
    schema = fixture.indexed_source.schema_name
    await fixture.connection.execute(f'DROP INDEX "{schema}"."entity_address_unified_idx_{suffix}"')

    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(ValueError):
            await verify_network_address_artifact(fixture.execution_connection, descriptor)


async def _wait_for_optional_index_build(connection, task, qualified_index):
    for _ in range(250):
        state = await connection.fetchrow(
            "SELECT indisready,indisvalid FROM pg_index WHERE indexrelid=to_regclass($1)", qualified_index
        )
        if state and state["indisready"] and not state["indisvalid"]:
            assert not task.done()
            return
        if task.done():
            await task
            pytest.fail("The concurrent build did not wait for the older snapshot")
        await asyncio.sleep(0.02)
    pytest.fail("The concurrent index build did not reach its bounded validation wait")


async def check_concurrent_count_index_reads(indexed_db):
    fixture = indexed_db
    candidate = await _adopt(fixture)
    await _seal(fixture, candidate)
    await _publish(fixture, candidate)
    descriptor = await _address_descriptor(fixture, candidate)
    before = deepcopy(descriptor)
    schema = fixture.indexed_source.schema_name
    qualified_index = f'"{schema}".entity_address_unified_idx_source_count'
    await fixture.connection.execute("DROP INDEX " + qualified_index)
    builder = await asyncpg.connect(
        fixture.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    task = None
    try:
        async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
            await fixture.execution_connection.fetchval(f'SELECT count(*) FROM "{schema}".entity_address_unified')
            task = asyncio.create_task(
                builder.execute(
                    f"CREATE INDEX CONCURRENTLY entity_address_unified_idx_source_count "
                    f'ON "{schema}".entity_address_unified(source_count)'
                )
            )
            await _wait_for_optional_index_build(fixture.connection, task, qualified_index)
            async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
                assert await verify_network_address_artifact(fixture.connection, descriptor) == CompactAddressRelation(
                    **descriptor["table"]
                )
            assert (await _read(fixture))[1] == fixture.previous_rows
        await asyncio.wait_for(asyncio.shield(task), timeout=5)
        assert await fixture.connection.fetchval(
            "SELECT indisvalid AND indisready AND indislive FROM pg_index WHERE indexrelid=to_regclass($1)",
            qualified_index,
        )
        assert (await _read(fixture))[1] == fixture.previous_rows
        assert descriptor == before
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await builder.close()


async def check_optional_custom_address_count_index(fixture, candidate, publication, network_id, expected_rows):
    from process.network_compact_readiness import compact_index_digest
    from tests.test_network_compact_publication_postgres import _assert_readonly_replay

    schema = candidate.schema_name
    qualified_index = f'"{schema}".entity_address_unified_idx_source_count'
    assert await fixture.connection.fetchval(
        "SELECT indisvalid AND indisready AND indislive FROM pg_index WHERE indexrelid=to_regclass($1)",
        qualified_index,
    )
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        before = await compact_index_digest(fixture.connection, schema)
    await fixture.connection.execute("DROP INDEX " + qualified_index)
    await _assert_readonly_replay(fixture, candidate, publication)
    assert (await _read(fixture, network_ids=(network_id,)))[1] == expected_rows
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        assert await compact_index_digest(fixture.connection, schema) == before
    await fixture.connection.execute(
        f'CREATE INDEX entity_address_unified_idx_source_count ON "{schema}".entity_address_unified(source_count)'
    )
    await _assert_readonly_replay(fixture, candidate, publication)
    assert (await _read(fixture, network_ids=(network_id,)))[1] == expected_rows
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        assert await compact_index_digest(fixture.connection, schema) == before


async def _copy_named_address_heap(source, target, schema, model):
    table = model.__table__.to_metadata(MetaData(), schema=schema)
    await target.execute(str(CreateTable(table).compile(dialect=postgresql.dialect())))
    columns = ",".join('"' + column.name + '"' for column in table.columns)
    qualified = f'"{schema}"."{table.name}"'
    expected = await source.fetchval(f"SELECT count(*) FROM {qualified}")
    transfer = SimpleNamespace(
        transaction=target.transaction,
        copy_from_query=source.copy_from_query,
        copy_to_table=target.copy_to_table,
    )
    await _copy_native_batch(transfer, f"SELECT {columns} FROM {qualified}", (), schema, table.name, expected)


async def _restore_named_address_dependencies(fixture, source_connection, target_connection, descriptor):
    origin_schema = descriptor["origin_artifact"]["namespace"]["name"]
    indexed_schema = descriptor["namespace"]["name"]
    for extension in ("postgis", "intarray", "btree_gin", "btree_gist"):
        await target_connection.execute("CREATE EXTENSION IF NOT EXISTS " + extension)
    for schema, models in (
        (origin_schema, (EntityAddressUnified, NPIData)),
        (indexed_schema, (EntityAddressUnified,)),
    ):
        await target_connection.execute(f'CREATE SCHEMA "{schema}"')
        for model in models:
            await _copy_named_address_heap(source_connection, target_connection, schema, model)
        comment = await source_connection.fetchval(
            "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1",
            schema,
        )
        if comment is not None:
            literal = await target_connection.fetchval("SELECT quote_literal($1::text)", comment)
            await target_connection.execute(f'COMMENT ON SCHEMA "{schema}" IS {literal}')
            restored = await target_connection.fetchval(
                "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", schema
            )
            assert restored.encode() == comment.encode()
    statements, _ = _post_publish_index_plan(f'"{indexed_schema}"', "serving", build_concurrently=False)
    await target_connection.execute(dict(statements)["canonical_network_ids"])
    await _build_serving_indexes(target_connection, indexed_schema, _serving_plan(indexed_schema))
    await target_connection.execute(
        f'CREATE INDEX restored_optional_count_idx ON "{indexed_schema}".entity_address_unified(source_count)'
    )
    for schema in (origin_schema, indexed_schema):
        await freeze_fhir_source_snapshot(
            target_connection,
            schema,
            namespace_owner=fixture.roles["owner"],
            reader_roles=(fixture.roles["reader"],),
            application_roles=tuple(sorted(set(fixture.roles.values()) - {fixture.roles["owner"]})),
        )


async def _verify_restored_indexed_data(fixture, target, descriptor):
    before = json.dumps(descriptor, sort_keys=True, separators=(",", ":"))
    async with target.transaction(isolation="repeatable_read", readonly=True):
        await target.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
        relation = await verify_network_address_artifact(target, descriptor)
        assert relation == CompactAddressRelation(**descriptor["table"])
        assert await indexed.prove_indexed_address_equivalence(target, descriptor) == {
            "source_rows": 1,
            "address_rows": 1,
            "missing_locations": 0,
            "extra_locations": 0,
            "changed_address_rows": 0,
        }
    assert json.dumps(descriptor, sort_keys=True, separators=(",", ":")) == before
    schema = descriptor["namespace"]["name"]
    async with target.transaction():
        await target.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with target.transaction():
                await target.execute(f"UPDATE \"{schema}\".entity_address_unified SET first_line='Forbidden'")
    await target.execute(f"UPDATE \"{schema}\".entity_address_unified SET first_line='Different office'")
    async with target.transaction(isolation="repeatable_read", readonly=True):
        await target.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
        with pytest.raises(NetworkAddressArtifactError, match="indexed_content_differs"):
            await indexed.prove_indexed_address_equivalence(target, descriptor)
    assert json.dumps(descriptor, sort_keys=True, separators=(",", ":")) == before


async def check_named_indexed_database_copy(indexed_db, record_property):
    fixture = indexed_db
    candidate = await _adopt(fixture)
    await _seal(fixture, candidate)
    await _publish(fixture, candidate)
    descriptor = await _address_descriptor(fixture, candidate)
    before = deepcopy(descriptor)
    database_name = "indexed_copy_" + uuid4().hex
    created_by_kind = {"database": False}
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(_cleanup, fixture.connection, database_name, [], created_by_kind)
        assert not await fixture.connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database_name
        )
        created_by_kind["database"] = True
        await fixture.connection.execute(f'CREATE DATABASE "{database_name}" TEMPLATE template0')
        target_connection = await asyncpg.connect(
            _admin_url().set(database=database_name).render_as_string(hide_password=False)
        )
        cleanup.push_async_callback(target_connection.close)
        async with (
            fixture.connection.transaction(isolation="repeatable_read", readonly=True),
            target_connection.transaction(isolation="repeatable_read"),
        ):
            await _restore_named_address_dependencies(fixture, fixture.connection, target_connection, descriptor)
        await _verify_restored_indexed_data(fixture, target_connection, descriptor)
    assert descriptor == before
    assert not await fixture.connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database_name
    )
    record_property("component", "synthetic_indexed_address_cross_database_copy")
    record_property("metadata", "Original descriptor and bootstrap schema comment preserved")
    record_property(
        "limits",
        "Named indexed data and complete equivalence; not API or deployment acceptance",
    )
