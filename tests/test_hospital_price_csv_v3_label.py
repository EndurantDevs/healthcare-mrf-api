# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact CSV Version=3 parser and database admission boundaries."""

from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from db.models.hospital_price_header import HospitalPriceVersion
from support.hospital_price_native_validation import (
    HOSPITAL_MRF_PACKED_V7_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
)
from tests.test_hospital_price_storage import (
    _database_url,
    _drop_schema,
    _prepare_schema,
    _quote,
    _run_migration,
)

MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260917100000_hospital_price_csv_v3_label.py"


def _load_migration():
    module_spec = importlib.util.spec_from_file_location("hospital_price_csv_v3_label_test_migration", MIGRATION_PATH)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def test_csv_v3_label_migration_is_current_parser_only() -> None:
    migration = _load_migration()
    assert migration.revision == "20260917100000_hospital_price_csv_v3_label"
    assert migration.down_revision == "20260914120000_custom_import_v1_schema"
    _, statement = migration._upgrade_statements()
    model_shape = next(
        str(constraint.sqltext)
        for constraint in HospitalPriceVersion.__table__.constraints
        if constraint.name == "hospital_price_version_shape_check"
    )
    assert statement.endswith(f"CHECK ({model_shape});")
    exact_branch = (
        f"parser_contract_sha256 = '{HOSPITAL_MRF_PARSER_CONTRACT_SHA256}' "
        "AND template_version = '3' AND npi_count > 0 "
        "AND attester_name IS NOT NULL"
    )
    assert statement.count(exact_branch) == 1
    assert "template_version = '3.0'" not in statement
    assert "template_version IN ('3.0.1', '4.0.0')" in statement
    assert migration.downgrade() is None


async def _create_version_table(engine, table: str) -> None:
    async with engine.begin() as connection:
        await connection.exec_driver_sql(
            f"CREATE TABLE {table} ("
            "version_id text NOT NULL, semantic_sha256 text NOT NULL, "
            "parser_contract_sha256 text NOT NULL, source_format text NOT NULL, "
            "template_version text NOT NULL, npi_count integer NOT NULL, "
            "attester_name text, attestation_text text NOT NULL, "
            "location_count integer NOT NULL, license_count integer NOT NULL, "
            "service_count integer NOT NULL, charge_count integer NOT NULL, "
            "payer_charge_count integer NOT NULL, "
            "CONSTRAINT hospital_price_version_shape_check CHECK (true))"
        )


async def _insert_header(
    connection: asyncpg.Connection,
    table: str,
    marker: str,
    *,
    parser_contract_sha256: str = HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
    source_format: str = "csv-tall",
    template_version: str = "3",
    npi_count: int = 1,
    attester_name: str | None = "Attester",
) -> None:
    await connection.execute(
        f"INSERT INTO {table} (version_id, semantic_sha256, "
        "parser_contract_sha256, source_format, template_version, npi_count, "
        "attester_name, attestation_text, location_count, license_count, "
        "service_count, charge_count, payer_charge_count) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, 'affirmation', 1, 1, 1, 1, 0)",
        marker * 64,
        "d" * 64,
        parser_contract_sha256,
        source_format,
        template_version,
        npi_count,
        attester_name,
    )


async def _assert_rejected(connection: asyncpg.Connection, table: str, marker: str, **fields) -> None:
    with pytest.raises(asyncpg.CheckViolationError):
        await _insert_header(connection, table, marker, **fields)


async def _insert_valid_v3_labels(connection: asyncpg.Connection, table: str, valid_field_map: dict) -> None:
    await _insert_header(connection, table, "1", **valid_field_map)
    await _insert_header(connection, table, "2", **{**valid_field_map, "source_format": "csv-wide"})
    await _insert_header(connection, table, "3", **{**valid_field_map, "template_version": "3.0.0"})
    await _insert_header(connection, table, "4", **{**valid_field_map, "template_version": "3.0.1"})
    assert (
        await connection.fetchval(
            f"SELECT template_version FROM {table} WHERE version_id=$1",
            "1" * 64,
        )
        == "3"
    )


async def _assert_invalid_v3_labels(connection: asyncpg.Connection, table: str, valid_field_map: dict) -> None:
    await _assert_rejected(connection, table, "5", **{**valid_field_map, "template_version": "3.0"})
    await _assert_rejected(connection, table, "6", **{**valid_field_map, "source_format": "json"})
    await _assert_rejected(
        connection,
        table,
        "7",
        **{
            **valid_field_map,
            "parser_contract_sha256": HOSPITAL_MRF_PACKED_V7_PARSER_CONTRACT_SHA256,
        },
    )
    await _assert_rejected(connection, table, "8", **{**valid_field_map, "npi_count": 0})
    await _assert_rejected(connection, table, "9", **{**valid_field_map, "attester_name": None})


async def prove_csv_v3_label_constraints(monkeypatch) -> None:
    """Prove the hosted PostgreSQL boundary through the existing core inventory."""

    database_url = _database_url()
    schema = f"hospital_price_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"), poolclass=NullPool)
    await _prepare_schema(engine, schema)
    table = f'{_quote(schema)}."hospital_price_version"'
    try:
        await _create_version_table(engine, table)
        await _run_migration(engine, _load_migration(), "upgrade")
        connection = await asyncpg.connect(
            database_url.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        try:
            valid_field_map = {
                "parser_contract_sha256": HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
                "source_format": "csv-tall",
                "template_version": "3",
                "npi_count": 1,
                "attester_name": "Attester",
            }
            await _insert_valid_v3_labels(connection, table, valid_field_map)
            await _assert_invalid_v3_labels(connection, table, valid_field_map)
        finally:
            await connection.close()
    finally:
        await _drop_schema(engine, schema)
        await engine.dispose()
