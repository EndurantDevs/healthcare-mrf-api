# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CSV 3.0.1 parser-receipt and database admission boundary."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import uuid

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from db.models.hospital_price_header import HospitalPriceVersion
from support.hospital_price_native_validation import (
    HOSPITAL_MRF_PACKED_V5_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
)
from tests.test_hospital_price_csv_v1_labels import (
    _assert_rejected,
    _create_version_table,
    _insert_header,
    _load_migration as _load_v1_migration,
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
    / "alembic/versions/20260905130000_hospital_price_csv_3_0_1.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "hospital_price_csv_v3_0_1_test_migration", MIGRATION_PATH
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def test_csv_v3_0_1_migration_matches_current_model() -> None:
    migration = _load_migration()
    assert migration.revision == "20260905130000_hospital_price_csv_3_0_1"
    assert migration.down_revision == (
        "20260904163000_provider_directory_exact_guard_scope"
    )
    drop, add = migration._upgrade_statements()
    assert "DROP CONSTRAINT hospital_price_version_shape_check" in drop
    migration_check = add.split(" CHECK (", 1)[1][:-2]
    shape_check = next(
        constraint
        for constraint in HospitalPriceVersion.__table__.constraints
        if constraint.name == "hospital_price_version_shape_check"
    )
    assert migration_check == str(shape_check.sqltext)
    assert migration.downgrade() is None


@pytest.mark.asyncio
async def test_postgres_csv_v3_0_1_is_current_parser_csv_only(monkeypatch) -> None:
    """Keep the new producer label inside current-parser CSV boundaries."""
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
        await _run_migration(engine, _load_v1_migration(), "upgrade")
        await _run_migration(engine, _load_migration(), "upgrade")
        connection = await asyncpg.connect(str(database_url.set(drivername="postgresql")))
        try:
            valid_fields_by_name = {
                "parser_contract_sha256": HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
                "source_format": "csv-wide",
                "template_version": "3.0.1",
                "npi_count": 1,
                "attester_name": "Current Attester",
            }
            await _insert_header(connection, table, "1", **valid_fields_by_name)
            assert await connection.fetchval(
                f"SELECT template_version FROM {table} WHERE version_id=$1",
                "1" * 64,
            ) == "3.0.1"
            await _assert_rejected(
                connection,
                table,
                "2",
                **{**valid_fields_by_name, "template_version": "3.0.2"},
            )
            await _assert_rejected(
                connection,
                table,
                "3",
                **{**valid_fields_by_name, "source_format": "json"},
            )
            await _assert_rejected(
                connection,
                table,
                "4",
                **{
                    **valid_fields_by_name,
                    "parser_contract_sha256": (
                        HOSPITAL_MRF_PACKED_V5_PARSER_CONTRACT_SHA256
                    ),
                },
            )
            await _assert_rejected(
                connection, table, "5", **{**valid_fields_by_name, "npi_count": 0}
            )
        finally:
            await connection.close()
    finally:
        await _drop_schema(engine, schema)
        await engine.dispose()
