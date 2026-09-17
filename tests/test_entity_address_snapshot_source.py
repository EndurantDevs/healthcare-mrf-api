# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

source = importlib.import_module("process.entity_address_snapshot_source")
native = importlib.import_module("process.entity_address_unified")


def test_archive_relation_manifest_uses_exact_model_defined_seven_table_family():
    relations = source.entity_address_archive_relations()
    assert [relation.table_name for relation in relations] == [
        model.__tablename__ for model in (native.EntityAddressUnified, *native.SUPPORT_TABLE_MODELS)
    ]
    assert len(relations) == len(set(relations)) == 7
    assert all("oid" not in relation.model_name.casefold() for relation in relations)


@pytest.mark.asyncio
async def test_capture_locks_every_portable_relation_before_exporting_snapshot():
    session = AsyncMock()
    session.execute.side_effect = [None] * 8 + [AsyncMock(scalar_one=lambda: "00000003-0000001B-1")]
    capture = await source.capture_entity_address_archive_source(session, schema_name=" mrf ")
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert capture.schema_name == "mrf"
    assert capture.postgres_snapshot == "00000003-0000001B-1"
    assert len(capture.relations) == 7
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    assert all('LOCK TABLE "mrf".' in statement and "IN SHARE MODE" in statement for statement in statements[1:-1])
    assert statements[-1] == "SELECT pg_export_snapshot()"


@pytest.mark.asyncio
async def test_capture_rejects_unsafe_schema_without_touching_the_database():
    session = AsyncMock()
    with pytest.raises(ValueError):
        await source.capture_entity_address_archive_source(session, schema_name="mrf; DROP SCHEMA mrf")
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_capture_rejects_oversize_schema_without_touching_the_database():
    session = AsyncMock()
    with pytest.raises(ValueError, match="safe schema name"):
        await source.capture_entity_address_archive_source(session, schema_name="a" * 64)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_export_returns_manifest_only_after_awaiting_copy_in_held_transaction():
    session = AsyncMock()

    @asynccontextmanager
    async def session_scope():
        yield session

    @asynccontextmanager
    async def transaction_scope():
        yield

    session.begin = transaction_scope
    session.execute.side_effect = [None] * 8 + [AsyncMock(scalar_one=lambda: "00000003-0000001B-1")]
    copied = AsyncMock()

    manifest = await source.export_entity_address_archive_source(
        session_scope,
        schema_name="mrf",
        archive_copy=copied,
    )

    copied.assert_awaited_once()
    assert manifest.contract == "entity_address_unified.postgres.v1"
    assert len(manifest.relations) == 7
