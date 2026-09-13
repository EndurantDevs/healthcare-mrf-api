# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native PostgreSQL proof for the seven-table archive semantic receipt."""

from __future__ import annotations

import importlib
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from tests.test_entity_address_snapshot_stage_postgres import _create_model_family, _native_test_connection


receipt = importlib.import_module("process.entity_address_snapshot_receipt")


async def _seed_family(connection, schema_name: str, *, reversed_rows: bool) -> None:
    rows = [("first", 1), ("second", 2)]
    for location_key, checksum in reversed(rows) if reversed_rows else rows:
        await connection.execute(
            text(
                f'INSERT INTO "{schema_name}"."entity_address_unified" '
                "(entity_type, entity_id, location_key, checksum, type) "
                "VALUES (:entity_type, :entity_id, :location_key, :checksum, 'primary')"
            ),
            {"entity_type": "synthetic", "entity_id": location_key, "location_key": location_key, "checksum": checksum},
        )
    await connection.execute(
        text(
            f'INSERT INTO "{schema_name}"."entity_address_evidence" '
            "(evidence_id, location_key, entity_type, entity_id, source_id, source_run_id, observed_at) "
            "VALUES (1, 'first', 'synthetic', 'first', 1, 'synthetic-run', TIMESTAMPTZ '2026-01-02 03:04:05+00')"
        )
    )


async def _capture(sessions, schema_name: str, timezone: str):
    async with sessions() as session, session.begin():
        await session.execute(text(f"SET LOCAL TimeZone TO '{timezone}'"))
        return await receipt.capture_entity_address_archive_receipt(session, schema_name=schema_name)


@pytest.mark.asyncio
async def test_native_receipt_is_order_and_session_setting_independent_and_detects_drift():
    """Compare same seven-table data across schemas, then reject every local drift."""
    async_dsn, _ = _native_test_connection()
    engine = create_async_engine(async_dsn)
    source_schema = "address_receipt_source_" + uuid4().hex
    restored_schema = "address_receipt_restored_" + uuid4().hex
    try:
        async with engine.begin() as connection:
            await _create_model_family(connection, source_schema)
            await _create_model_family(connection, restored_schema)
            await _seed_family(connection, source_schema, reversed_rows=False)
            await _seed_family(connection, restored_schema, reversed_rows=True)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        source_receipt = await _capture(sessions, source_schema, "America/Los_Angeles")
        restored_receipt = await _capture(sessions, restored_schema, "Asia/Tokyo")
        assert source_receipt.as_dict() == restored_receipt.as_dict()

        async with sessions() as session, session.begin():
            await session.execute(
                text(f'UPDATE "{restored_schema}"."entity_address_unified" SET checksum=3 WHERE location_key=\'first\'')
            )
        assert (await _capture(sessions, restored_schema, "UTC")).content_sha256 != source_receipt.content_sha256

        async with sessions() as session, session.begin():
            await session.execute(
                text(
                    f"INSERT INTO \"{restored_schema}\".\"entity_address_unified\" (entity_type, entity_id, location_key, checksum, type) VALUES ('synthetic', 'third', 'third', 3, 'primary')"
                )
            )
        assert (await _capture(sessions, restored_schema, "UTC")).content_sha256 != source_receipt.content_sha256

        async with sessions() as session, session.begin():
            await session.execute(
                text(f'ALTER TABLE "{restored_schema}"."entity_address_unified" ADD COLUMN archive_tamper text')
            )
        assert (await _capture(sessions, restored_schema, "UTC")).schema_sha256 != source_receipt.schema_sha256
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{source_schema}" CASCADE'))
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{restored_schema}" CASCADE'))
        await engine.dispose()
