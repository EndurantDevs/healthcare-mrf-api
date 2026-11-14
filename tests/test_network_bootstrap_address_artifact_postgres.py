# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named bootstrap artifact reads through genuine restricted PostgreSQL roles."""

import copy
import json
from contextlib import AsyncExitStack
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import EntityAddressUnified, NPIData
from process import network_address_artifact as artifact
from process import network_bootstrap_address_artifact as bootstrap_artifact
from process import network_bootstrap_sources as bootstrap
from process.network_compact_layout import CompactAddressRelation
from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot
from tests.test_network_address_artifact_postgres import _admin_url, _cleanup
from tests.test_network_custom_address_source_postgres import (
    _bootstrap_access_login,
    _change_bootstrap_access,
)
from tests.test_network_custom_address_source_postgres import (
    bootstrap_access_fixture as bootstrap_access_fixture,
)
from tests.test_network_custom_address_source_postgres import (
    bootstrap_db as bootstrap_db,
)
from tests.test_network_custom_address_source_postgres import (
    serving_schema as serving_schema,
)

pytestmark = pytest.mark.asyncio


async def _capture(fixture, connection):
    async with connection.transaction(isolation="repeatable_read"):
        return await artifact.capture_network_address_artifact(connection, fixture.receipt.address_source)


async def _verify(fixture, connection, descriptor):
    async with connection.transaction(isolation="repeatable_read"):
        return await artifact.verify_network_address_artifact(connection, descriptor)


@pytest.mark.parametrize("bootstrap_access_fixture", ["constant", "serial"], indirect=True)
async def test_genuine_bootstrap_artifact_reuses_named_content_for_worker(bootstrap_access_fixture, monkeypatch):
    fixture, receipt = bootstrap_access_fixture, bootstrap_access_fixture.receipt
    await _change_bootstrap_access(fixture)

    async def no_content_scan(*_):
        pytest.fail("Frozen bootstrap artifact reuse must not rescan either heap")

    monkeypatch.setattr(bootstrap, "_content", no_content_scan)
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        descriptor = await _capture(fixture, reader)
        assert descriptor["contract"] == bootstrap_artifact.CONTRACT
        assert descriptor["bootstrap_receipt"] == json.loads(artifact._json(receipt.as_dict()))
        assert descriptor["generation_id"] == receipt.generation_sha256
        assert descriptor["heaps"] == {
            name: {"schema_name": receipt.address_source.schema_name, "table_name": name}
            for name in ("entity_address_unified", "npi")
        }
        assert await _verify(fixture, reader, descriptor) == CompactAddressRelation(
            receipt.address_source.schema_name, "entity_address_unified"
        )
        assert not await reader.fetchval(
            "SELECT has_table_privilege($1,$2,'SELECT')",
            fixture.stranger,
            f'"{receipt.address_source.schema_name}".npi',
        )


@pytest.mark.parametrize("changed", ["edition", "heap", "receipt"])
async def test_bootstrap_refuses_changed_logical_binding(bootstrap_access_fixture, changed):
    fixture = bootstrap_access_fixture
    await _change_bootstrap_access(fixture)
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        descriptor = copy.deepcopy(await _capture(fixture, reader))
        if changed == "edition":
            descriptor["generation_id"] = "f" * 64
        elif changed == "heap":
            descriptor["heaps"]["npi"]["table_name"] = "different_npi"
        else:
            descriptor["bootstrap_receipt"]["parent_receipts"][0]["row_count"] = -1
        descriptor["descriptor_sha256"] = artifact._digest(
            {key: value for key, value in descriptor.items() if key != "descriptor_sha256"}
        )
        with pytest.raises(ValueError):
            await _verify(fixture, reader, descriptor)


async def test_capture_refuses_changed_stored_edition_and_missing_heap(bootstrap_access_fixture):
    fixture, receipt = bootstrap_access_fixture, bootstrap_access_fixture.receipt
    await _change_bootstrap_access(fixture)
    connection, schema = fixture.fixture.connection, receipt.address_source.schema_name
    async with connection.transaction(isolation="repeatable_read"):
        changed = replace(receipt, captured_at="2026-02-01T00:00:00Z")
        changed = replace(changed, generation_sha256=bootstrap._generation(changed))
        document = await connection.fetchval("SELECT quote_literal($1::text)", json.dumps(changed.as_dict()))
        await connection.execute(f'COMMENT ON SCHEMA "{schema}" IS {document}')
        with pytest.raises(ValueError):
            await _capture(fixture, connection)
    await connection.execute(f'ALTER TABLE "{schema}".npi RENAME TO previous_npi')
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        with pytest.raises(asyncpg.UndefinedTableError):
            await _capture(fixture, reader)


async def test_worker_reuse_keeps_both_named_heap_locks_until_transaction_end(bootstrap_access_fixture):
    fixture = bootstrap_access_fixture
    await _change_bootstrap_access(fixture)
    namespace = '"' + fixture.receipt.address_source.schema_name + '"'
    tables = f"{namespace}.entity_address_unified,{namespace}.npi"
    administrator = fixture.fixture.connection
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        async with reader.transaction(isolation="repeatable_read"):
            descriptor = await _capture(fixture, reader)
            assert await _verify(fixture, reader, descriptor)
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with administrator.transaction():
                    await administrator.execute(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE NOWAIT")
        async with administrator.transaction():
            await administrator.execute(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE NOWAIT")


@pytest.mark.parametrize("heap", ["entity_address_unified", "npi"])
@pytest.mark.parametrize("damage", ["content", "shape"])
async def test_full_verification_rejects_changed_content_or_shape(bootstrap_access_fixture, heap, damage):
    fixture = bootstrap_access_fixture
    await _change_bootstrap_access(fixture)
    qualified = f'"{fixture.receipt.address_source.schema_name}"."{heap}"'
    field = "first_line" if heap == "entity_address_unified" else "provider_first_name"
    connection = fixture.fixture.connection
    async with connection.transaction(isolation="repeatable_read"):
        if damage == "content":
            await connection.execute(f"UPDATE {qualified} SET {field}='changed'")
        else:
            await connection.execute(f"ALTER TABLE {qualified} ADD COLUMN unexpected text")
        await connection.execute(f'SET LOCAL ROLE "{fixture.reader}"')
        with pytest.raises(bootstrap.NetworkBootstrapSourceError, match="content|shape"):
            await bootstrap.verify_network_bootstrap_sources(connection, fixture.receipt)


@pytest.mark.parametrize("bootstrap_access_fixture", ["serial"], indirect=True)
async def test_named_bootstrap_read_and_copy_never_execute_serial_default(bootstrap_access_fixture):
    fixture = bootstrap_access_fixture
    await _change_bootstrap_access(fixture)
    parent = f'"{fixture.fixture.source_schema}".npi'
    sequence = await fixture.fixture.connection.fetchval("SELECT pg_get_serial_sequence($1,'npi')", parent)
    before = await fixture.fixture.connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}")
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        async with reader.transaction(isolation="repeatable_read"):
            descriptor = await _capture(fixture, reader)
            assert await _verify(fixture, reader, descriptor)
            source = f'"{fixture.receipt.address_source.schema_name}".npi'
            content = await bootstrap._content(reader, fixture.receipt.address_source.schema_name, "npi")
            await reader.execute(f"CREATE TEMP TABLE copied_npi(LIKE {source} INCLUDING DEFAULTS)")
            await bootstrap._copy_native_batch(
                reader, f"SELECT * FROM {source}", (), "pg_temp", "copied_npi", content[0]
            )
            assert await bootstrap._content(reader, "pg_temp", "copied_npi") == content
    assert await fixture.fixture.connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}") == before


def _synthetic_legacy_comment(receipt):
    """Encode real data accounting in the exact legacy layout; obsolete fields carry no authority."""
    metadata = receipt.as_dict()
    document_by_field = {
        "component": "network_bootstrap_sources",
        "revision": 1,
        "specification": {
            "bootstrap_id": receipt.specification.bootstrap_id,
            "address_schema": receipt.specification.address_schema,
            "npi_schema": receipt.specification.npi_schema,
            "address_table_oid": None,
            "npi_table_oid": None,
        },
        "parent_receipts": [
            {
                **parent,
                **dict.fromkeys(
                    ("schema_oid", "table_oid", "owner_oid", "authority_table_oid", "authority_json", "shape_sha256")
                ),
            }
            for parent in metadata["parent_receipts"]
        ],
        "captured_at": receipt.captured_at,
        **dict.fromkeys(
            (
                "owner_role",
                "runtime_roles",
                "capture_snapshot",
                "schema_oid",
                "owner_oid",
                "relation_oids",
                "index_oids",
            )
        ),
    }
    document_by_field["generation_sha256"] = artifact._digest(document_by_field)
    return json.dumps(document_by_field, indent=2) + "\n"


async def _restore_bootstrap_copy(fixture, source_reader, target_admin, legacy, raw_comment):
    """Copy all native columns and freeze the restored schema with the same reader roles."""
    schema_name = legacy.address_source.schema_name
    namespace = bootstrap._identifier(schema_name)
    await target_admin.execute(f"CREATE SCHEMA {namespace}")
    transfer = SimpleNamespace(
        transaction=target_admin.transaction,
        copy_from_query=source_reader.copy_from_query,
        copy_to_table=target_admin.copy_to_table,
    )
    for model, parent in zip((EntityAddressUnified, NPIData), legacy.parent_receipts, strict=True):
        table = model.__table__.to_metadata(MetaData(), schema=schema_name)
        await target_admin.execute(str(CreateTable(table).compile(dialect=postgresql.dialect())))
        columns = ",".join(bootstrap._identifier(str(column.name)) for column in table.columns)
        await bootstrap._copy_native_batch(
            transfer,
            f"SELECT {columns} FROM {namespace}.{bootstrap._identifier(parent.table_name)}",
            (),
            schema_name,
            parent.table_name,
            parent.row_count,
        )
    target_literal = await target_admin.fetchval("SELECT quote_literal($1::text)", raw_comment)
    await target_admin.execute(f"COMMENT ON SCHEMA {namespace} IS {target_literal}")
    await freeze_fhir_source_snapshot(
        target_admin,
        schema_name,
        namespace_owner=fixture.fixture.roles["owner"],
        reader_roles=(fixture.stranger,),
        application_roles=tuple(
            sorted(
                {
                    fixture.reader,
                    fixture.stranger,
                    fixture.fixture.roles["loader"],
                    fixture.fixture.roles["reader"],
                }
            )
        ),
    )


async def _assert_restored_bootstrap_copy(
    target_reader, target_admin, legacy, raw_comment, source_descriptor, comment_query
):
    """Verify copied data, denied writes and later content corruption without altering the stored comment."""
    schema_name = legacy.address_source.schema_name
    namespace = bootstrap._identifier(schema_name)
    async with target_reader.transaction(isolation="repeatable_read", readonly=True):
        restored_comment = await target_reader.fetchval(comment_query, schema_name)
        assert restored_comment.encode() == raw_comment.encode()
        restored = bootstrap._decode(restored_comment)
        assert restored == legacy and restored.generation_sha256 == legacy.generation_sha256
        assert await bootstrap.verify_network_bootstrap_sources(target_reader, restored) == restored
        descriptor = await artifact.capture_network_address_artifact(target_reader, legacy.address_source)
        assert descriptor == source_descriptor
        assert await artifact.verify_network_address_artifact(
            target_reader, descriptor, api_read=True
        ) == CompactAddressRelation(schema_name, "entity_address_unified")
        assert (
            await target_reader.fetchval(f"SELECT first_line FROM {namespace}.entity_address_unified")
            == "123 Example Street"
        )
        assert await target_reader.fetchval(f"SELECT provider_first_name FROM {namespace}.npi") == "Example"

    for statement in (
        f"UPDATE {namespace}.npi SET provider_first_name='Forbidden'",
        f"INSERT INTO {namespace}.npi(npi) VALUES(2000000001)",
        f"CREATE TABLE {namespace}.unexpected(value int)",
        f"DROP TABLE {namespace}.npi",
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await target_reader.execute(statement)

    await target_admin.execute(f"UPDATE {namespace}.npi SET provider_first_name='Changed'")
    async with target_reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(bootstrap.NetworkBootstrapSourceError, match="content"):
            await bootstrap.verify_network_bootstrap_sources(target_reader, legacy)
        assert await target_reader.fetchval(comment_query, schema_name) == raw_comment


@pytest.mark.parametrize("bootstrap_access_fixture", ["serial"], indirect=True)
async def test_legacy_bootstrap_comment_survives_cross_database_copy(bootstrap_access_fixture, record_property):
    """Synthetic legacy-format compatibility through real COPY; not a historical export."""
    fixture = bootstrap_access_fixture
    source_admin = fixture.fixture.connection
    await _change_bootstrap_access(fixture)
    raw_comment = _synthetic_legacy_comment(fixture.receipt)
    legacy = bootstrap._decode(raw_comment)
    schema_name = legacy.address_source.schema_name
    namespace = bootstrap._identifier(schema_name)
    comment_query = "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1"
    literal = await source_admin.fetchval("SELECT quote_literal($1::text)", raw_comment)
    await source_admin.execute(f"COMMENT ON SCHEMA {namespace} IS {literal}")
    assert await source_admin.fetchval(comment_query, schema_name) == raw_comment

    database_name = "bootstrap_copy_" + uuid4().hex
    created_by_kind = {"database": False}
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(_cleanup, source_admin, database_name, [], created_by_kind)
        assert not await source_admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database_name)
        created_by_kind["database"] = True
        await source_admin.execute(f'CREATE DATABASE "{database_name}" TEMPLATE template0')
        target_url = _admin_url().set(database=database_name)
        target_admin = await asyncpg.connect(target_url.render_as_string(hide_password=False))
        cleanup.push_async_callback(target_admin.close)
        source_reader = await cleanup.enter_async_context(_bootstrap_access_login(fixture.reader, fixture.password))
        async with source_reader.transaction(isolation="repeatable_read", readonly=True):
            assert await bootstrap.verify_network_bootstrap_sources(source_reader, legacy) == legacy
            source_descriptor = await artifact.capture_network_address_artifact(source_reader, legacy.address_source)
            async with target_admin.transaction(isolation="repeatable_read"):
                await _restore_bootstrap_copy(fixture, source_reader, target_admin, legacy, raw_comment)
            assert await bootstrap.verify_network_bootstrap_sources(source_reader, legacy) == legacy
            assert await source_reader.fetchval(comment_query, schema_name) == raw_comment

        target_reader = await asyncpg.connect(
            target_url.set(username=fixture.stranger, password=fixture.password).render_as_string(hide_password=False)
        )
        cleanup.push_async_callback(target_reader.close)
        await _assert_restored_bootstrap_copy(
            target_reader, target_admin, legacy, raw_comment, source_descriptor, comment_query
        )

    record_property("component", "synthetic_legacy_bootstrap_cross_database_copy")
    record_property("metadata", "raw comment and stored generation preserved")
    record_property("limits", "Synthetic legacy-format compatibility; not historical export or deployment acceptance")
