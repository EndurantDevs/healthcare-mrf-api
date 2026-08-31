# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable-PostgreSQL proof for source-faithful legacy hospital headers."""

from __future__ import annotations

import uuid

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from db.models.hospital_price_header import HospitalPriceVersion
from tests.test_hospital_price_storage import (
    LEGACY_HEADER_MIGRATION_PATH,
    _database_url,
    _drop_schema,
    _load_migration,
    _prepare_schema,
    _quote,
    _run_migration,
)


def test_legacy_header_schema_preserves_absent_profile_fields() -> None:
    """Keep legacy-only successor fields absent without relaxing v3."""

    shape_check = next(
        constraint
        for constraint in HospitalPriceVersion.__table__.constraints
        if constraint.name == "hospital_price_version_shape_check"
    )
    assert HospitalPriceVersion.attester_name.nullable is True
    model_sql = str(shape_check.sqltext)
    assert "template_version = '3.0.0' AND npi_count > 0" in model_sql
    assert "template_version IN ('2.0.0', '2.2.0', '2.2.1')" in model_sql
    assert "npi_count = 0 AND attester_name IS NULL" in model_sql
    migration = _load_migration(LEGACY_HEADER_MIGRATION_PATH)
    assert migration.revision == "20260831100000_hospital_price_legacy_header"
    assert migration.down_revision == (
        "20260830100000_provider_directory_rooted_partial_lineage"
    )


async def _insert_legacy_header(connection: asyncpg.Connection, quoted: str) -> None:
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_content "
        "(content_sha256, byte_count, media_type) VALUES ($1, 1, 'application/json')",
        "b" * 64,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_version ("
        "version_id, content_sha256, parser_contract_sha256, semantic_sha256, "
        "source_format, source_hospital_name, last_updated_on, template_version, "
        "attestation_text, confirm_attestation, attester_name, location_count, "
        "npi_count, license_count, service_count, charge_count, payer_charge_count) "
        "VALUES ($1, $2, $3, $4, 'json', 'Legacy Hospital', DATE '2026-01-01', "
        "'2.2.0', 'affirmation', true, NULL, 1, 0, 1, 1, 1, 0)",
        "a" * 64,
        "b" * 64,
        "c" * 64,
        "d" * 64,
    )


async def _assert_profile_constraints(
    connection: asyncpg.Connection, quoted: str
) -> None:
    header_record = await connection.fetchrow(
        f"SELECT attester_name, npi_count FROM {quoted}.hospital_price_version "
        "WHERE version_id=$1",
        "a" * 64,
    )
    assert header_record is not None
    assert header_record["attester_name"] is None
    assert header_record["npi_count"] == 0
    for invalid_set in (
        "npi_count=-1",
        "template_version='3.0.0'",
        "template_version='2.0.0'",
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"UPDATE {quoted}.hospital_price_version SET {invalid_set} "
                "WHERE version_id=$1",
                "a" * 64,
            )


@pytest.mark.asyncio
async def test_postgres_legacy_header_keeps_absent_fields_absent(monkeypatch) -> None:
    """Accept source-faithful v2 metadata without weakening numeric bounds."""

    database_url = _database_url()
    schema = f"hospital_price_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"), poolclass=NullPool
    )
    await _prepare_schema(engine, schema)
    try:
        await _run_migration(engine, _load_migration(), "upgrade")
        await _run_migration(
            engine, _load_migration(LEGACY_HEADER_MIGRATION_PATH), "upgrade"
        )
        quoted = _quote(schema)
        connection = await asyncpg.connect(
            str(database_url.set(drivername="postgresql"))
        )
        try:
            await _insert_legacy_header(connection, quoted)
            await _assert_profile_constraints(connection, quoted)
        finally:
            await connection.close()
    finally:
        await _drop_schema(engine, schema)
        await engine.dispose()
