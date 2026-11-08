# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary-created hooks cannot acquire allocator authority after its body upgrade."""

import json
from uuid import uuid4

import asyncpg
import pytest

from db import migration_cms_snapshot_allocator as migration
from db.plan_release_storage import storage_identifier, storage_relation
from tests.test_provider_directory_cms_snapshot_allocator_postgres import (
    _CMS_METADATA,
    _case,
    _children,
    _identity,
    _install,
    _metadata,
)


async def _allocator_hooks(admin, oid):
    return await admin.fetch(
        "SELECT to_jsonb(t) AS native FROM pg_catalog.pg_trigger t WHERE tgfoid=$1::oid ORDER BY t.oid", oid
    )


async def _refuse_invocation(actor, table, operation):
    """Forged selection cannot avoid invocation binding with genuine CMS-shaped metadata."""
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="allocator_invocation_invalid"):
        async with actor.transaction():
            await actor.fetchval("SELECT set_config($1,'allocator-probe',true)", migration.SELECTOR)
            if operation == "UPDATE":
                await actor.execute(
                    "UPDATE " + table + " SET publication_metadata_json=$1::jsonb", json.dumps(_CMS_METADATA)
                )
            else:
                await actor.execute(
                    "INSERT INTO " + table + " VALUES('allocator-probe','acquiring',false,NULL,$1::jsonb)",
                    json.dumps(_CMS_METADATA),
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sibling", "foreign", "before", "update", "statement"])
async def test_ordinary_preinstalled_hook_refuses_without_allocation_or_catalog_changes(monkeypatch, kind):
    """Preinstalled ordinary hooks cannot gain the protected allocator's DDL authority."""
    async with _case(monkeypatch) as case:
        admin, actor = case.catalog.connection, case.publisher
        role = await actor.fetchval("SELECT session_user::text")
        schema, name = case.catalog.schema_name, "allocator_probe_" + uuid4().hex
        foreign_schema = "allocator_probe_" + uuid4().hex if kind == "foreign" else None
        function = storage_relation(schema, "prepare_pd_generic_dataset_storage") + "()"
        await admin.execute("GRANT CREATE ON SCHEMA " + storage_identifier(schema) + " TO " + storage_identifier(role))
        await admin.execute("GRANT EXECUTE ON FUNCTION " + function + " TO " + storage_identifier(role))
        if foreign_schema is not None:
            schema, name = foreign_schema, "provider_directory_endpoint_dataset"
        table = storage_relation(schema, name)
        try:
            if foreign_schema is not None:
                await admin.execute(
                    "CREATE SCHEMA " + storage_identifier(schema) + " AUTHORIZATION " + storage_identifier(role)
                )
            await actor.execute(
                "CREATE TABLE "
                + table
                + "(dataset_id text,status text,is_current bool,published_at timestamptz,publication_metadata_json jsonb)"
            )
            timing, operation, level = (
                "BEFORE" if kind == "before" else "AFTER",
                "UPDATE" if kind == "update" else "INSERT",
                "STATEMENT" if kind == "statement" else "ROW",
            )
            if operation == "UPDATE":
                await actor.execute(
                    "INSERT INTO " + table + " VALUES('allocator-probe','acquiring',false,NULL,'{}'::jsonb)"
                )
            await actor.execute(
                f"CREATE TRIGGER allocator_probe {timing} {operation} ON {table} "
                f"FOR EACH {level} EXECUTE FUNCTION {function}"
            )
            expected, before = await _identity(case), await _metadata(case)
            hooks = await _allocator_hooks(admin, expected["oid"])
            await _install(case, expected)
            installed = await _metadata(case)
            assert installed["prosrc"] == migration.bound_cms_allocator_body(case.catalog.schema_name)
            assert installed["identity"] == before["identity"] and installed["comment"] == before["comment"]
            await _refuse_invocation(actor, table, operation)
            assert await _children(case, "allocator-probe") == 0
            assert await _metadata(case) == installed
            assert await _allocator_hooks(admin, expected["oid"]) == hooks
            assert await actor.fetchval("SELECT count(*) FROM " + table) == (1 if operation == "UPDATE" else 0)
        finally:
            await admin.execute("DROP TABLE IF EXISTS " + table)
            if foreign_schema is not None:
                await admin.execute("DROP SCHEMA IF EXISTS " + storage_identifier(foreign_schema))
            assert await admin.fetchval("SELECT pg_catalog.to_regclass($1) IS NULL", table)


@pytest.mark.asyncio
async def test_prior_cms_body_upgrades_and_rejects_unbound_execution_before_ordinary_fallthrough(monkeypatch):
    async with _case(monkeypatch) as case:
        admin, schema = case.catalog.connection, case.catalog.schema_name
        expected = await _identity(case)
        definition = await admin.fetchval("SELECT pg_catalog.pg_get_functiondef($1::oid)", expected["oid"])
        original, previous = migration.allocator_bodies(schema)
        assert definition.count(original) == 1
        await admin.execute(definition.replace(original, previous, 1))
        before = await _metadata(case)
        async with admin.transaction():
            with pytest.raises(RuntimeError, match="function_changed"):
                await migration.require_cms_snapshot_allocator(admin, schema, expected=expected)
        await _install(case, expected)
        installed = await _metadata(case)
        assert installed["identity"] == before["identity"] and installed["comment"] == before["comment"]
        assert installed["prosrc"] == migration.bound_cms_allocator_body(schema)
        async with admin.transaction():
            assert await migration.require_cms_snapshot_allocator(admin, schema, expected=expected) == expected
