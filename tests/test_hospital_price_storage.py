# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused metadata and disposable-PostgreSQL proof for hospital prices."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import re
import uuid

from alembic.migration import MigrationContext
from alembic.operations import Operations
import asyncpg
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from db.connection import Base
from db.models.hospital_price import HospitalPriceLocatorObservation
from db.models.hospital_price_facts import HospitalPricePayerCharge
from db.models.hospital_price_header import HospitalPriceVersion
from process import hospital_price_store
from tests.hospital_price_storage_assertions import (
    _StoreConnection,
    assert_bad_allowed_count_rejected as _assert_bad_allowed_count_rejected,
    assert_lossless_values as _assert_lossless_values,
    prove_unchanged_reimport as _prove_unchanged_reimport,
)

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic/versions/20260825120000_hospital_price_storage.py"
SOURCE_FORMAT_MIGRATION_PATH = (
    ROOT / "alembic/versions/20260827120000_hospital_price_source_format.py"
)
LEGACY_HEADER_MIGRATION_PATH = (
    ROOT / "alembic/versions/20260831100000_hospital_price_legacy_header.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_HOSPITAL_PRICE_MIGRATION_POSTGRES_DSN"
DATABASE_RE = re.compile(
    r"^(?:hospital_price_schema_test_[a-z0-9_]+|ptg2_v3_lifecycle_test_ci_runner)$"
)
SCHEMA_RE = re.compile(r"^hospital_price_test_[0-9a-f]{32}$")


def _load_migration(path: Path = MIGRATION_PATH):
    module_spec = importlib.util.spec_from_file_location(
        f"hospital_price_storage_test_migration_{path.stem}", path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def test_source_format_migration_history_reaches_native_receipts() -> None:
    historical_sql = MIGRATION_PATH.read_text(encoding="utf-8")
    repair_sql = SOURCE_FORMAT_MIGRATION_PATH.read_text(encoding="utf-8")
    shape_check = next(
        constraint
        for constraint in HospitalPriceVersion.__table__.constraints
        if constraint.name == "hospital_price_version_shape_check"
    )
    model_sql = str(shape_check.sqltext)
    for value in ("csv_tall", "csv_wide"):
        assert value in historical_sql
        assert value not in model_sql
    for value in ("csv-tall", "csv-wide"):
        assert value in repair_sql
        assert value in model_sql
    repair = _load_migration(SOURCE_FORMAT_MIGRATION_PATH)
    assert repair.revision == "20260827120000_hospital_price_source_format"
    assert repair.down_revision == (
        "20260826200000_hospital_price_selector_range_index"
    )


def _database_url() -> sa.URL:
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    database_url = make_url(raw_dsn)
    if not database_url.drivername.startswith("postgresql") or not DATABASE_RE.fullmatch(
        str(database_url.database or "")
    ):
        pytest.fail(f"{POSTGRES_DSN_ENV} must name an approved disposable test database")
    return database_url


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


async def _run_migration(engine, migration, action: str) -> None:
    async with engine.begin() as connection:
        def run(sync_connection) -> None:
            migration.op = Operations(MigrationContext.configure(sync_connection))
            getattr(migration, action)()

        await connection.run_sync(run)


async def _drop_schema(engine, schema: str) -> None:
    if not SCHEMA_RE.fullmatch(schema):
        raise RuntimeError(f"refusing to drop schema {schema!r}")
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        remaining = await connection.scalar(sa.text("SELECT to_regnamespace(:schema)"),
                                             {"schema": schema})
    assert remaining is None


async def _prepare_schema(engine, schema: str) -> None:
    quoted = _quote(schema)
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f"CREATE SCHEMA {quoted}")


async def _seed_registry(connection, quoted: str) -> None:
    await connection.execute(f"INSERT INTO {quoted}.hospital_price_locator(locator_id, cms_hpt_url) "
                             "VALUES ('locator-1', 'https://hospital.example/cms-hpt.txt')")
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_locator_observation ("
        "observation_id, locator_id, registry_version, requested_url, final_url, result_status, "
        "http_status, response_sha256, response_byte_count, checked_at) "
        "VALUES ('observation-1', 'locator-1', 1, "
        "'https://hospital.example/cms-hpt.txt', "
        "'https://www.hospital.example/cms-hpt.txt', 'redirected_verified', 200, $1, 321, "
        "'2026-08-25T10:00:00Z')",
        "e" * 64,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_hospital (hospital_id, facility_anchor_id, "
        "locator_id, name, registry_version) VALUES "
        "('hospital-a', 'facility-a', 'locator-1', 'Hospital A', 1), "
        "('hospital-b', 'facility-b', 'locator-1', 'Hospital B', 1), "
        "('hospital-unbound', NULL, 'locator-1', 'Unbound Hospital', 1)"
    )


async def _seed_version_header(connection, quoted: str, content_sha: str, version_id: str) -> None:
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_content "
        "(content_sha256, byte_count, media_type) VALUES ($1, 12345, 'application/json')",
        content_sha,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_version ("
        "version_id, content_sha256, parser_contract_sha256, semantic_sha256, "
        "source_format, source_hospital_name, last_updated_on, template_version, "
        "attestation_text, confirm_attestation, attester_name, "
        "location_count, npi_count, license_count, "
        "service_count, charge_count, payer_charge_count, financial_aid_policy) VALUES ("
        "$1, $2, $3, $4, 'json', 'Hospital System', DATE '2026-04-01', "
        "'3.0.0', 'attestation', true, 'Attester Name', 2, 3, 2, 1, 1, 1, "
        "'https://hospital.example/financial-aid')",
        version_id,
        content_sha,
        "c" * 64,
        "d" * 64,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_version_location VALUES "
        "($1, 0, 'Hospital A', 'Address A'), "
        "($1, 1, 'Hospital B', NULL)",
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_version_npi VALUES "
        "($1, 0, '0000000001'), ($1, 1, '0000000002'), "
        "($1, 2, 'taxonomy-not-an-npi')",
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_version_license VALUES "
        "($1, 0, 'CA', '50056'), ($1, 1, 'NV', '70001')",
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_contract_provision VALUES "
        "($1, 0, NULL, NULL, 'Default contract provision'), "
        "($1, 1, 'Payer', 'Plan', 'Plan-specific provision')",
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_version_hospital VALUES "
        "($1, 'hospital-a', 0), ($1, 'hospital-b', 1)",
        version_id,
    )


async def _seed_version(connection, schema: str) -> tuple[str, str, str]:
    quoted = _quote(schema)
    content_sha = "b" * 64
    version_id = "a" * 64
    await _seed_registry(connection, quoted)
    await _seed_version_header(connection, quoted, content_sha, version_id)
    await connection.execute(
        f"DELETE FROM {quoted}.hospital_price_version_hospital "
        "WHERE version_id=$1 AND hospital_id='hospital-a'",
        version_id,
    )
    return content_sha, version_id, quoted


async def _seed_content_version(
    connection, quoted: str, content_sha: str, version_id: str
) -> None:
    await _seed_version_header(connection, quoted, content_sha, version_id)


async def _seed_full_v3_facts(connection, quoted: str, version_id: str) -> None:
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_service VALUES "
        "($1, 0, 'Synthetic drug service', 0.12345, 'ML')",
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_service_code VALUES "
        "($1, 0, 0, 'HCPCS', 'J1450'), "
        "($1, 0, 1, 'NDC', '25021-0184-82')",
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_charge VALUES ("
        "$1, 0, 0, 'both', 35.0001, 37.0001, 75.12345, 45.98765, "
        "ARRAY['50','62'], 'generic note', 'professional')",
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_payer_charge VALUES ("
        "$1, 0, 0, 0, 'Payer', 'Plan', 'other', NULL, 70.5001, "
        "'base rate plus percentage', 50.12345, 40.12345, 60.12345, "
        "'1 through 10', 'payer note')",
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_modifier VALUES "
        "($1, 0, '50|62', 'Combined modifier', 'both', 'modifier generic note')",
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_modifier_payer VALUES "
        "($1, 0, 0, 'Payer', 'Plan', '93.75 percent adjustment', "
        "NULL, 93.7501, NULL)",
        version_id,
    )


async def _seed_attempt(connection, quoted: str) -> None:
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_import_attempt ("
        "attempt_id, hospital_id, locator_id, locator_observation_id, registry_version, "
        "requested_source_url, expected_generation, status, lease_owner, heartbeat_at, "
        "lease_expires_at) VALUES "
        "('attempt-a', 'hospital-a', 'locator-1', "
        "'observation-1', 1, 'https://hospital.example/prices.json', "
        "0, 'running', 'hospital-prices:test', clock_timestamp(), "
        "clock_timestamp() + interval '5 minutes')"
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_current(hospital_id, latest_attempt_id) "
        "VALUES ('hospital-a', 'attempt-a'), ('hospital-b', NULL), "
        "('hospital-unbound', NULL)"
    )


async def _publish_initial(
    engine, content_sha: str, version_id: str
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(
                "CREATE TEMP TABLE hospital_price_initial_stage ("
                "hospital_id varchar(64), attempt_id varchar(64), "
                "expected_generation bigint, source_location_ordinal integer, "
                "final_source_url text, source_http_status integer, ein varchar(9)) "
                "ON COMMIT DROP"
            )
        )
        await connection.execute(
            sa.text(
                "INSERT INTO hospital_price_initial_stage VALUES ("
                "'hospital-a', 'attempt-a', 0, NULL, "
                "'https://cdn.hospital.example/prices.json', 200, '001234567')"
            )
        )
        adapter = _StoreConnection(connection)
        await hospital_price_store._bind_evidence(
            adapter, '"hospital_price_initial_stage"', version_id, content_sha, 1
        )
        assert await hospital_price_store._cas_publish(
            adapter, '"hospital_price_initial_stage"', version_id
        ) == (1, 0, 0)


async def _record_failed_attempt(connection, quoted: str, version_id: str) -> None:
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_import_attempt ("
        "attempt_id, hospital_id, locator_id, locator_observation_id, "
        "registry_version, requested_source_url, expected_generation, "
        "status, lease_owner, heartbeat_at, lease_expires_at, finished_at, "
        "error_code) VALUES ('attempt-failed', 'hospital-a', "
        "'locator-1', 'observation-1', 1, "
        "'https://hospital.example/failure.json', 1, 'failed', "
        "'hospital-prices:test', clock_timestamp(), "
        "clock_timestamp() + interval '5 minutes', clock_timestamp(), "
        "'invalid_source')"
    )
    await connection.execute(
        f"UPDATE {quoted}.hospital_price_current "
        "SET latest_attempt_id='attempt-failed' WHERE hospital_id='hospital-a'"
    )
    failed_state = await connection.fetchrow(
        f"SELECT current.version_id, current.generation, attempt.status "
        f"FROM {quoted}.hospital_price_current current "
        f"JOIN {quoted}.hospital_price_import_attempt attempt "
        "ON attempt.attempt_id=current.latest_attempt_id "
        "WHERE current.hospital_id='hospital-a'"
    )
    assert dict(failed_state) == {
        "version_id": version_id,
        "generation": 1,
        "status": "failed",
    }
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await connection.execute(
            f"UPDATE {quoted}.hospital_price_current "
            "SET published_attempt_id='attempt-failed' WHERE hospital_id='hospital-a'"
        )


async def _prove_stale_cas(connection, quoted: str, version_id: str) -> None:
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_import_attempt ("
        "attempt_id, hospital_id, locator_id, locator_observation_id, "
        "registry_version, requested_source_url, expected_generation, status, "
        "content_sha256, version_id, lease_owner, heartbeat_at, lease_expires_at) "
        "VALUES ('attempt-stale', 'hospital-a', "
        "'locator-1', 'observation-1', 1, 'https://hospital.example/prices.json', "
        "0, 'verified', $1, $2, 'hospital-prices:test', clock_timestamp(), "
        "clock_timestamp() + interval '5 minutes')",
        "b" * 64,
        version_id,
    )
    current_before = await connection.fetchrow(
        f"SELECT * FROM {quoted}.hospital_price_current "
        "WHERE hospital_id='hospital-a'"
    )
    stale = await connection.fetchrow(
        f"UPDATE {quoted}.hospital_price_current AS current "
        "SET generation=current.generation+1 "
        f"FROM {quoted}.hospital_price_import_attempt AS attempt "
        "WHERE attempt.attempt_id='attempt-stale' "
        "AND current.hospital_id=attempt.hospital_id "
        "AND current.generation=attempt.expected_generation RETURNING current.generation"
    )
    current_after = await connection.fetchrow(
        f"SELECT * FROM {quoted}.hospital_price_current "
        "WHERE hospital_id='hospital-a'"
    )
    assert stale is None
    assert dict(current_after) == dict(current_before)


async def _publish_and_prove_cas(
    connection, engine, quoted: str, content_sha: str, version_id: str
) -> None:
    await _publish_initial(engine, content_sha, version_id)
    await _record_failed_attempt(connection, quoted, version_id)
    await _prove_stale_cas(connection, quoted, version_id)


def test_models_use_lossless_types_and_optional_facility_anchor() -> None:
    tables = Base.metadata.tables
    assert HospitalPriceVersion.__table__ is tables["mrf.hospital_price_version"]
    assert HospitalPriceLocatorObservation.__table__ is tables["mrf.hospital_price_locator_observation"]
    assert HospitalPricePayerCharge.__table__ is tables["mrf.hospital_price_payer_charge"]
    assert len([name for name in tables if name.startswith("mrf.hospital_price_")]) == 22
    assert "mrf.hospital_price_facility" not in tables
    hospital = tables["mrf.hospital_price_hospital"]
    foreign_key_targets = {
        element.target_fullname
        for foreign_key in hospital.foreign_key_constraints
        for element in foreign_key.elements
    }
    assert hospital.c.facility_anchor_id.nullable
    assert "mrf.facility_anchor.id" not in foreign_key_targets
    payer = tables["mrf.hospital_price_payer_charge"]
    assert isinstance(payer.c.standard_charge_percentage.type, sa.Numeric)
    assert payer.c.standard_charge_percentage.type.scale is None
    assert isinstance(payer.c.allowed_count.type, sa.String)
    charge = tables["mrf.hospital_price_charge"]
    assert isinstance(charge.c.modifier_codes.type, sa.ARRAY)
    modifier_payer = tables["mrf.hospital_price_modifier_payer"]
    assert isinstance(modifier_payer.c.standard_charge_percentage.type, sa.Numeric)
    assert modifier_payer.c.description.nullable
    location = tables["mrf.hospital_price_version_location"]
    assert location.c.location_name.nullable and location.c.hospital_address.nullable
    attempt = tables["mrf.hospital_price_import_attempt"]
    assert not attempt.c.lease_owner.nullable
    assert not attempt.c.lease_expires_at.nullable


def test_migration_is_the_single_linear_head() -> None:
    migration = _load_migration()
    assert migration.down_revision == "20260820200000_provider_directory_projection_finalizer"
    assert migration.revision == "20260825120000_hospital_price_storage"


@pytest.mark.asyncio
async def test_postgres_round_trip_and_last_known_good_cas(monkeypatch) -> None:
    """Prove real DDL, lossless fields, CAS retention, and clean downgrade."""
    database_url = _database_url()
    schema = f"hospital_price_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(hospital_price_store, "schema_name", lambda: schema)
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"), poolclass=NullPool)
    migration = _load_migration()
    await _prepare_schema(engine, schema)
    try:
        await _run_migration(engine, migration, "upgrade")
        connection = await asyncpg.connect(str(database_url.set(drivername="postgresql")))
        try:
            content_sha, version_id, quoted = await _seed_version(connection, schema)
            await _seed_full_v3_facts(connection, quoted, version_id)
            await _seed_attempt(connection, quoted)
            await _publish_and_prove_cas(
                connection, engine, quoted, content_sha, version_id
            )
            await _assert_lossless_values(connection, quoted)
            await _assert_bad_allowed_count_rejected(connection, quoted, version_id)
        finally:
            await connection.close()
        await _prove_unchanged_reimport(engine, schema, content_sha, version_id)
        await _run_migration(engine, migration, "downgrade")
        async with engine.connect() as connection:
            remaining = await connection.scalar(sa.text(
                "SELECT count(*) FROM pg_tables "
                "WHERE schemaname=:schema AND tablename LIKE 'hospital_price_%'"
            ), {"schema": schema})
        assert remaining == 0
    finally:
        await _drop_schema(engine, schema)
        await engine.dispose()


@pytest.mark.asyncio
async def test_postgres_source_format_forward_and_rollback(monkeypatch) -> None:
    """Rewrite legacy spellings and keep predecessor rollback compatible."""

    from tests.hospital_price_source_format_migration_postgres import (
        prove_source_format_forward_and_rollback,
    )

    await prove_source_format_forward_and_rollback(monkeypatch)


@pytest.mark.asyncio
async def test_postgres_packed_copy_integrity_and_rollback(
    monkeypatch, tmp_path: Path
) -> None:
    """Prove binary COPY integrity, immutable blocks, rollback, and cascade."""

    from tests.hospital_price_packed_storage_postgres import _prove_packed_integrity

    await _prove_packed_integrity(monkeypatch, tmp_path)


@pytest.mark.asyncio
async def test_postgres_gc_preserves_shared_lkg_and_active_versions(
    monkeypatch, tmp_path: Path
) -> None:
    """Keep shared LKG and active versions, then collect only stale storage."""

    from tests.hospital_price_packed_storage_postgres import _prove_gc_retention

    await _prove_gc_retention(monkeypatch, tmp_path)
