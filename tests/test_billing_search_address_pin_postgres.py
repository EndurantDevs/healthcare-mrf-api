# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Billing pins lock the address names first and refuse a cutover committed after the snapshot began."""

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from api import billing_search_pagination as pagination
from db.connection import Database
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests import test_billing_search_pagination as fixture
from tests.provider_directory_profile_delta_test_support import _require_disposable_database

_LINEAGE = "11111111-1111-4111-8111-111111111111"


async def _address_schema(database, schema):
    for statement in (
        f'CREATE SCHEMA "{schema}"',
        f'CREATE TABLE "{schema}".entity_address_unified (edition int)',
        f'CREATE TABLE "{schema}".entity_address_evidence (edition int)',
        f'CREATE TABLE "{schema}".entity_address_unified_stage (edition int)',
        f'INSERT INTO "{schema}".entity_address_unified VALUES (1)',
        f'INSERT INTO "{schema}".entity_address_unified_stage VALUES (2)',
        f'CREATE TABLE "{schema}".entity_address_result_generation (singleton boolean PRIMARY KEY,'
        "local_lineage_id uuid,local_generation bigint,origin_lineage_id uuid,origin_generation bigint)",
        f"INSERT INTO \"{schema}\".entity_address_result_generation VALUES (true,'{_LINEAGE}',1,'{_LINEAGE}',1)",
    ):
        await database.status(statement)


async def _cutover(database, schema):
    async with database.engine.begin() as writer:
        await writer.execute(text("SET LOCAL lock_timeout='200ms'"))
        await writer.execute(
            text(f'ALTER TABLE "{schema}".entity_address_unified RENAME TO entity_address_unified_old')
        )
        await writer.execute(
            text(f'ALTER TABLE "{schema}".entity_address_unified_stage RENAME TO entity_address_unified')
        )
        await writer.execute(
            text(f'UPDATE "{schema}".entity_address_result_generation SET origin_generation=2,local_generation=2')
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("cutover_first", [True, False])
async def test_billing_pin_refuses_cutover_committed_after_snapshot_start(monkeypatch, cutover_first):
    schema = "billing_pin_" + uuid.uuid4().hex[:12]
    database = Database()
    await database.connect()
    is_disposable_database_verified = False
    try:
        await _require_disposable_database(database)
        is_disposable_database_verified = True
        await _address_schema(database, schema)
        monkeypatch.setattr(pagination.ptg2_serving, "PTG2_SCHEMA", schema)
        async with database.engine.connect() as connection, AsyncSession(bind=connection) as reader:
            await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            if not cutover_first:
                from api.provider_profile_snapshot import _prepare_provider_reader

                await _prepare_provider_reader(reader, schema, fresh=True, table_names=pagination._ADDRESS_TABLES)
            # The plan release read starts the snapshot before the address pin.
            assert (await reader.execute(text("SELECT 1"))).scalar() == 1
            if cutover_first:
                await _cutover(database, schema)
                with pytest.raises(PTG2ManifestArtifactError):
                    await pagination.capture_billing_search_generation_pin(reader, fixture._selection(), api_read=True)
            else:
                pin = await pagination.capture_billing_search_generation_pin(
                    reader, fixture._selection(), api_read=True
                )
                assert pin.address_relation_name == schema + ".entity_address_unified"
                with pytest.raises(DBAPIError) as blocked:
                    await _cutover(database, schema)
                assert "lock timeout" in str(blocked.value)
                edition = await reader.execute(text(f'SELECT edition FROM "{schema}".entity_address_unified'))
                assert edition.scalar() == 1
            await reader.rollback()
    finally:
        try:
            if is_disposable_database_verified:
                await database.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await database.disconnect()
