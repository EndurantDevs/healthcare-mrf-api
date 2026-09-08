# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact V2 metadata admission without rewriting the producer's CSV label."""

from __future__ import annotations

import hashlib
from pathlib import Path
import uuid

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from api.hospital_price_status import _CMS_V3_ATTESTATION_TEXT
from support.hospital_price_native_validation import (
    _CMS_V2_AFFIRMATION_TEXT,
    HOSPITAL_MRF_PACKED_V5_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PACKED_V6_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PACKED_V7_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
)
from tests.test_hospital_price_csv_v1_labels import _create_version_table, _insert_header
from tests.test_hospital_price_storage import (
    _database_url, _drop_schema, _load_migration, _prepare_schema, _quote, _run_migration,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260907193000_hospital_price_csv_v4_v2.py"
)


def test_v4_v2_keeps_original_boundary() -> None:
    migration = _load_migration(MIGRATION_PATH)
    assert migration.revision == "20260907193000_hospital_price_csv_v4_v2"
    assert migration.down_revision == (
        "20260904223000_provider_directory_michigan_generation_retirement"
    )
    drop, add = migration._upgrade_statements()
    assert "DROP CONSTRAINT hospital_price_version_shape_check" in drop
    shape = add.split(" CHECK (", 1)[1][:-2]
    assert _CMS_V2_AFFIRMATION_TEXT in shape
    assert "template_version IN ('3.0.1', '4.0.0') AND npi_count > 0" in shape
    assert HOSPITAL_MRF_PACKED_V6_PARSER_CONTRACT_SHA256 in shape
    assert HOSPITAL_MRF_PARSER_CONTRACT_SHA256 not in shape
    assert hashlib.sha256(shape.encode()).hexdigest() == (
        "d1378406bfcd25835ac83ab9b2ce8f2b8bd610f6c038fd030e20932c933f6de1"
    )
    assert migration.downgrade() is None


async def prove_csv_profile_constraints(monkeypatch) -> None:
    """Keep genuine V3 metadata strict inside the new admission constraint."""

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
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                f"ALTER TABLE {table} ADD COLUMN attestation_text text NOT NULL "
                f"DEFAULT '{_CMS_V2_AFFIRMATION_TEXT}'"
            )
        await _run_migration(engine, _load_migration(MIGRATION_PATH), "upgrade")
        await _run_migration(engine, _load_migration(MIGRATION_PATH.with_name(
            "20260907220000_hospital_price_missing_plan.py"
        )), "upgrade")
        await _run_migration(engine, _load_migration(MIGRATION_PATH.with_name(
            "20260908160000_hospital_price_tall_notes.py"
        )), "upgrade")
        connection = await asyncpg.connect(database_url.set(
            drivername="postgresql").render_as_string(hide_password=False))
        try:
            for parser_contract in (
                HOSPITAL_MRF_PACKED_V6_PARSER_CONTRACT_SHA256,
                HOSPITAL_MRF_PACKED_V7_PARSER_CONTRACT_SHA256,
                HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
            ):
                await _check_v4_v2_headers(connection, table, parser_contract)
                await connection.execute(f"DELETE FROM {table}")
        finally:
            await connection.close()
    finally:
        await _drop_schema(engine, schema)
        await engine.dispose()


async def _check_v4_v2_headers(connection, table, parser_contract):
    fields_by_name = {
        "parser_contract_sha256": parser_contract,
        "source_format": "csv-tall", "template_version": "4.0.0",
    }
    for marker, source_format in (("1", "csv-tall"), ("2", "csv-wide")):
        await _insert_header(connection, table, marker, **{
            **fields_by_name, "source_format": source_format,
        })
    for changed in (
        {"source_format": "json"}, {"template_version": "3.0.1"},
        {"parser_contract_sha256": HOSPITAL_MRF_PACKED_V5_PARSER_CONTRACT_SHA256},
        {"npi_count": -1}, {"attester_name": " "},
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await _insert_header(connection, table, "3", **{**fields_by_name, **changed})
    for text in ("not V2 affirmation", _CMS_V2_AFFIRMATION_TEXT + " changed"):
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(f"UPDATE {table} SET attestation_text=$1", text)
    await _insert_header(connection, table, "4", **fields_by_name,
                         npi_count=1, attester_name="Example Attester")
    await connection.execute(
        f"UPDATE {table} SET attestation_text=$1 WHERE version_id=$2",
        _CMS_V3_ATTESTATION_TEXT, "4" * 64,
    )
    for column in ("npi_count=0", "attester_name=NULL"):
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(f"UPDATE {table} SET {column} WHERE version_id=$1", "4" * 64)
    assert await connection.fetchval(f"SELECT count(*) FROM {table}") == 3
