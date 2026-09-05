# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CSV 1 and 1.0.0 receipt and database admission boundaries."""

from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from support.hospital_price_native_validation import (
    HOSPITAL_MRF_PACKED_V5_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
)
from tests.test_hospital_price_storage import (
    _database_url,
    _drop_schema,
    _prepare_schema,
    _quote,
    _run_migration,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260903130000_hospital_price_csv_v1_labels.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "hospital_price_csv_v1_labels_test_migration", MIGRATION_PATH
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


class _OperationsRecorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def test_csv_v1_labels_migration_retains_its_original_boundary() -> None:
    """Keep the historical V1 migration immutable."""

    migration = _load_migration()
    assert migration.revision == "20260903130000_hospital_price_csv_v1_labels"
    assert migration.down_revision == (
        "20260903100000_hospital_price_producer_csv_4_0_0"
    )
    recorder = _OperationsRecorder()
    migration.op = recorder
    migration.upgrade()
    migration_check = recorder.statements[1].split(" CHECK (", 1)[1][:-2]
    assert "template_version IN ('1', '1.0.0')" in migration_check
    assert "'3.0.1'" not in migration_check
    assert migration.downgrade() is None


async def _create_version_table(engine, table: str) -> None:
    async with engine.begin() as connection:
        await connection.exec_driver_sql(
            f"CREATE TABLE {table} ("
            "version_id text NOT NULL, semantic_sha256 text NOT NULL, "
            "parser_contract_sha256 text NOT NULL, source_format text NOT NULL, "
            "template_version text NOT NULL, npi_count integer NOT NULL, "
            "attester_name text, location_count integer NOT NULL, "
            "license_count integer NOT NULL, service_count integer NOT NULL, "
            "charge_count integer NOT NULL, payer_charge_count integer NOT NULL, "
            "CONSTRAINT hospital_price_version_shape_check CHECK (true))"
        )


async def _insert_header(
    connection: asyncpg.Connection,
    table: str,
    marker: str,
    parser_contract_sha256: str,
    source_format: str,
    template_version: str,
    *,
    npi_count: int = 0,
    attester_name: str | None = None,
) -> None:
    await connection.execute(
        f"INSERT INTO {table} (version_id, semantic_sha256, "
        "parser_contract_sha256, source_format, template_version, npi_count, "
        "attester_name, location_count, license_count, service_count, "
        "charge_count, payer_charge_count) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, 1, 1, 1, 1, 0)",
        marker * 64,
        "d" * 64,
        parser_contract_sha256,
        source_format,
        template_version,
        npi_count,
        attester_name,
    )


async def _assert_rejected(
    connection: asyncpg.Connection, table: str, marker: str, **fields
) -> None:
    with pytest.raises(asyncpg.CheckViolationError):
        await _insert_header(connection, table, marker, **fields)


async def _assert_v1_shape_boundaries(
    connection: asyncpg.Connection, table: str
) -> None:
    for marker, source_format, template_version in (
        ("1", "csv-tall", "1"),
        ("2", "csv-wide", "1.0.0"),
    ):
        await _insert_header(
            connection,
            table,
            marker,
            HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
            source_format,
            template_version,
        )
    preserved_versions = await connection.fetch(
        f"SELECT template_version FROM {table} ORDER BY version_id"
    )
    assert [
        version_record["template_version"] for version_record in preserved_versions
    ] == ["1", "1.0.0"]
    v1_fields_by_name = {
        "parser_contract_sha256": HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
        "source_format": "csv-tall",
        "template_version": "1",
    }
    await _assert_rejected(
        connection, table, "3", **{**v1_fields_by_name, "source_format": "json"}
    )
    await _assert_rejected(
        connection,
        table,
        "4",
        **{
            **v1_fields_by_name,
            "parser_contract_sha256": HOSPITAL_MRF_PACKED_V5_PARSER_CONTRACT_SHA256,
        },
    )
    await _assert_rejected(
        connection, table, "5", **v1_fields_by_name, npi_count=-1
    )
    await _assert_rejected(
        connection, table, "6", **v1_fields_by_name, attester_name="   "
    )


@pytest.mark.asyncio
async def test_postgres_csv_v1_labels_stay_csv_and_current_parser_only(
    monkeypatch,
) -> None:
    """Admit source-faithful V2 rows without relaxing JSON or older parsers."""

    database_url = _database_url()
    schema = f"hospital_price_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"), poolclass=NullPool
    )
    await _prepare_schema(engine, schema)
    table = f'{_quote(schema)}."hospital_price_version"'
    try:
        await _create_version_table(engine, table)
        await _run_migration(engine, _load_migration(), "upgrade")
        connection = await asyncpg.connect(
            str(database_url.set(drivername="postgresql"))
        )
        try:
            await _assert_v1_shape_boundaries(connection, table)
        finally:
            await connection.close()
    finally:
        await _drop_schema(engine, schema)
        await engine.dispose()
