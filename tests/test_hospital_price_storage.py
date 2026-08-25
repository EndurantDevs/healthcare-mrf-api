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

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic/versions/20260825120000_hospital_price_storage.py"
POSTGRES_DSN_ENV = "HLTHPRT_HOSPITAL_PRICE_MIGRATION_POSTGRES_DSN"
DATABASE_RE = re.compile(
    r"^(?:hospital_price_schema_test_[a-z0-9_]+|ptg2_v3_lifecycle_test_ci_runner)$"
)
SCHEMA_RE = re.compile(r"^hospital_price_test_[0-9a-f]{32}$")


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "hospital_price_storage_test_migration", MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


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
        "'3.0.0', 'attestation', true, 'Attester Name', 2, 2, 2, 1, 1, 1, "
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
        "($1, 0, '0000000001'), ($1, 1, '0000000002')",
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
    return content_sha, version_id, quoted


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


async def _seed_attempt_and_identity(connection, quoted: str, content_sha: str, version_id: str) -> None:
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_import_attempt ("
        "attempt_id, hospital_id, locator_id, locator_observation_id, "
        "registry_version, requested_source_url, final_source_url, "
        "source_http_status, expected_generation, status, content_sha256, "
        "version_id, lease_owner, heartbeat_at, lease_expires_at) VALUES "
        "('attempt-a', 'hospital-a', 'locator-1', "
        "'observation-1', 1, 'https://hospital.example/prices.json', "
        "'https://cdn.hospital.example/prices.json', 200, 0, 'verified', $1, $2, "
        "'hospital-prices:test', clock_timestamp(), "
        "clock_timestamp() + interval '5 minutes')",
        content_sha,
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_hospital_npi VALUES "
        "('hospital-a', $1, 0, '0000000001', 'mrf_header_file'), "
        "('hospital-a', $1, 1, '0000000002', 'mrf_header_file')",
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_hospital_tax_identity VALUES "
        "('hospital-a', $1, 'attempt-a', 'ein', '001234567', 'filename', 0)",
        version_id,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_current(hospital_id, latest_attempt_id) "
        "VALUES ('hospital-a', 'attempt-a'), ('hospital-b', NULL), "
        "('hospital-unbound', NULL)"
    )


async def _publish_initial(connection, quoted: str) -> None:
    published = await connection.fetchrow(
        f"UPDATE {quoted}.hospital_price_current AS current SET "
        "version_id=attempt.version_id, generation=current.generation+1, "
        "published_attempt_id=attempt.attempt_id, latest_attempt_id=attempt.attempt_id, "
        "service_count=1, charge_count=1, payer_charge_count=1, "
        "npi_count=2, tax_identity_count=1, last_success_at=clock_timestamp() "
        f"FROM {quoted}.hospital_price_import_attempt AS attempt "
        "WHERE attempt.attempt_id='attempt-a' AND attempt.status='verified' "
        "AND current.hospital_id=attempt.hospital_id "
        "AND current.generation=attempt.expected_generation "
        "RETURNING current.generation"
    )
    assert published["generation"] == 1
    await connection.execute(
        f"UPDATE {quoted}.hospital_price_import_attempt "
        "SET status='published', finished_at=clock_timestamp() "
        "WHERE attempt_id='attempt-a'"
    )


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
    stale = await connection.fetchrow(
        f"UPDATE {quoted}.hospital_price_current AS current "
        "SET generation=current.generation+1 "
        f"FROM {quoted}.hospital_price_import_attempt AS attempt "
        "WHERE attempt.attempt_id='attempt-stale' "
        "AND current.hospital_id=attempt.hospital_id "
        "AND current.generation=attempt.expected_generation RETURNING current.generation"
    )
    current = await connection.fetchrow(
        f"SELECT version_id, generation FROM {quoted}.hospital_price_current "
        "WHERE hospital_id='hospital-a'"
    )
    assert stale is None
    assert dict(current) == {"version_id": version_id, "generation": 1}


async def _publish_and_prove_cas(connection, quoted: str, version_id: str) -> None:
    await _publish_initial(connection, quoted)
    await _record_failed_attempt(connection, quoted, version_id)
    await _prove_stale_cas(connection, quoted, version_id)


class _StoreConnection:
    def __init__(self, connection) -> None:
        self.connection = connection

    async def all(self, statement: str, **values):
        return (await self.connection.execute(sa.text(statement), values)).all()

    async def scalar(self, statement: str, **values):
        return (await self.connection.execute(sa.text(statement), values)).scalar()

    async def status(self, statement: str, **values) -> int:
        return (await self.connection.execute(sa.text(statement), values)).rowcount


async def _prove_unchanged_reimport(
    engine, schema: str, content_sha: str, version_id: str
) -> None:
    quoted = _quote(schema)
    preserved_fields = (
        "version_id, generation, published_attempt_id, service_count, charge_count, "
        "payer_charge_count, npi_count, tax_identity_count, last_success_at"
    )
    async with engine.begin() as connection:
        before = dict((await connection.execute(sa.text(
            f"SELECT {preserved_fields} FROM {quoted}.hospital_price_current "
            "WHERE hospital_id='hospital-a'"
        ))).mappings().one())
        await connection.execute(sa.text(
            f"INSERT INTO {quoted}.hospital_price_import_attempt ("
            "attempt_id, hospital_id, locator_id, locator_observation_id, "
            "registry_version, requested_source_url, expected_generation, status, "
            "lease_owner, heartbeat_at, lease_expires_at) VALUES ("
            "'attempt-unchanged', 'hospital-a', 'locator-1', 'observation-1', 1, "
            "'https://hospital.example/prices.json', 1, 'running', "
            "'hospital-prices:test', clock_timestamp(), "
            "clock_timestamp() + interval '5 minutes')"
        ))
        await connection.execute(sa.text(
            f"UPDATE {quoted}.hospital_price_current SET "
            "latest_attempt_id='attempt-unchanged' WHERE hospital_id='hospital-a'"
        ))
        await connection.execute(sa.text(
            "CREATE TEMP TABLE hospital_price_reimport_stage ("
            "hospital_id varchar(64), attempt_id varchar(64), expected_generation bigint, "
            "source_location_ordinal integer, final_source_url text, "
            "source_http_status integer, ein varchar(9)) ON COMMIT DROP"
        ))
        await connection.execute(sa.text(
            "INSERT INTO hospital_price_reimport_stage VALUES ("
            "'hospital-a', 'attempt-unchanged', 1, 0, "
            "'https://cdn.hospital.example/prices.json', 200, '009876543')"
        ))
        adapter = _StoreConnection(connection)
        await hospital_price_store._bind_evidence(
            adapter, '"hospital_price_reimport_stage"', version_id, content_sha, 1
        )
        assert await hospital_price_store._cas_publish(
            adapter, '"hospital_price_reimport_stage"', version_id
        ) == (0, 0, 1)
        after = dict((await connection.execute(sa.text(
            f"SELECT {preserved_fields} FROM {quoted}.hospital_price_current "
            "WHERE hospital_id='hospital-a'"
        ))).mappings().one())
        assert {key: value for key, value in after.items() if key != "tax_identity_count"} == {
            key: value for key, value in before.items() if key != "tax_identity_count"
        }
        assert after["tax_identity_count"] == before["tax_identity_count"] + 1
        assert await connection.scalar(sa.text(
            f"SELECT status FROM {quoted}.hospital_price_import_attempt "
            "WHERE attempt_id='attempt-unchanged'"
        )) == "unchanged"
        assert await connection.scalar(sa.text(
            f"SELECT count(*) FROM {quoted}.hospital_price_version"
        )) == 1
        await connection.execute(sa.text(
            f"INSERT INTO {quoted}.hospital_price_import_attempt ("
            "attempt_id, hospital_id, locator_id, locator_observation_id, "
            "registry_version, requested_source_url, expected_generation, status, "
            "lease_owner, heartbeat_at, lease_expires_at) VALUES ("
            "'attempt-superseded', 'hospital-a', 'locator-1', 'observation-1', 1, "
            "'https://hospital.example/prices.json', 0, 'running', "
            "'hospital-prices:test', clock_timestamp(), "
            "clock_timestamp() + interval '5 minutes')"
        ))
        await connection.execute(sa.text(
            f"UPDATE {quoted}.hospital_price_current SET "
            "latest_attempt_id='attempt-superseded' WHERE hospital_id='hospital-a'"
        ))
        await connection.execute(sa.text(
            "TRUNCATE hospital_price_reimport_stage"
        ))
        await connection.execute(sa.text(
            "INSERT INTO hospital_price_reimport_stage VALUES ("
            "'hospital-a', 'attempt-superseded', 0, 0, "
            "'https://cdn.hospital.example/prices.json', 200, '008765432')"
        ))
        await hospital_price_store._bind_evidence(
            adapter, '"hospital_price_reimport_stage"', version_id, content_sha, 1
        )
        assert await hospital_price_store._cas_publish(
            adapter, '"hospital_price_reimport_stage"', version_id
        ) == (0, 1, 0)
        assert await connection.scalar(sa.text(
            f"SELECT count(*) FROM {quoted}.hospital_price_hospital_tax_identity "
            "WHERE tin_value='008765432'"
        )) == 0
        assert await connection.scalar(sa.text(
            f"SELECT tax_identity_count FROM {quoted}.hospital_price_current "
            "WHERE hospital_id='hospital-a'"
        )) == after["tax_identity_count"]


async def _assert_v3_metadata(connection, quoted: str) -> None:
    """Verify optional metadata and ordered header arrays survive exactly."""
    header = await connection.fetchrow(
        f"SELECT count(*) FILTER (WHERE license_number IS NOT NULL) AS licenses, "
        "array_agg(state ORDER BY license_ordinal) AS states "
        f"FROM {quoted}.hospital_price_version_license"
    )
    assert dict(header) == {"licenses": 2, "states": ["CA", "NV"]}
    locations = await connection.fetch(
        f"SELECT location_name, hospital_address "
        f"FROM {quoted}.hospital_price_version_location "
        "ORDER BY location_ordinal"
    )
    assert [tuple(location) for location in locations] == [
        ("Hospital A", "Address A"), ("Hospital B", None),
    ]
    financial_aid = await connection.fetchval(
        f"SELECT financial_aid_policy FROM {quoted}.hospital_price_version"
    )
    assert financial_aid.endswith("/financial-aid")
    provisions = await connection.fetch(
        f"SELECT payer_name, plan_name, provisions "
        f"FROM {quoted}.hospital_price_contract_provision "
        "ORDER BY provision_ordinal"
    )
    assert [tuple(provision) for provision in provisions] == [
        (None, None, "Default contract provision"), ("Payer", "Plan", "Plan-specific provision"),
    ]
    assert await connection.fetchval(
        f"SELECT billing_class FROM {quoted}.hospital_price_charge"
    ) == "professional"


async def _assert_lossless_values(connection, quoted: str) -> None:
    """Verify identifiers, typed values, and provenance survived persistence."""
    exact = await connection.fetchval(
        f"SELECT hospital_id FROM {quoted}.hospital_price_hospital_tax_identity "
        "WHERE tin_type=$1 AND tin_value=$2",
        "ein",
        "001234567",
    )
    inexact = await connection.fetchval(
        f"SELECT hospital_id FROM {quoted}.hospital_price_hospital_tax_identity "
        "WHERE tin_type=$1 AND tin_value=$2",
        "EIN",
        "1234567",
    )
    decimal_and_count = await connection.fetchrow(
        f"SELECT standard_charge_percentage::text AS percentage, allowed_count "
        f"FROM {quoted}.hospital_price_payer_charge"
    )
    assert exact == "hospital-a" and inexact is None
    assert dict(decimal_and_count) == {
        "percentage": "70.5001",
        "allowed_count": "1 through 10",
    }
    modifier_evidence = await connection.fetchrow(
        f"SELECT modifier.additional_generic_notes, "
        "payer.standard_charge_percentage::text AS percentage "
        f"FROM {quoted}.hospital_price_modifier modifier "
        f"JOIN {quoted}.hospital_price_modifier_payer payer "
        "USING (version_id, modifier_ordinal)"
    )
    assert dict(modifier_evidence) == {
        "additional_generic_notes": "modifier generic note",
        "percentage": "93.7501",
    }
    shared = await connection.fetchrow(
        f"SELECT count(DISTINCT hospital.locator_id) AS locators, "
        "count(DISTINCT binding.hospital_id) AS hospitals "
        f"FROM {quoted}.hospital_price_hospital hospital "
        f"JOIN {quoted}.hospital_price_version_hospital binding ON true"
    )
    assert dict(shared) == {"locators": 1, "hospitals": 2}
    await _assert_v3_metadata(connection, quoted)
    observation = await connection.fetchrow(
        f"SELECT registry_version, requested_url, final_url, result_status, http_status "
        f"FROM {quoted}.hospital_price_locator_observation"
    )
    assert dict(observation) == {
        "registry_version": 1,
        "requested_url": "https://hospital.example/cms-hpt.txt",
        "final_url": "https://www.hospital.example/cms-hpt.txt",
        "result_status": "redirected_verified",
        "http_status": 200,
    }
    unbound = await connection.fetchval(
        f"SELECT facility_anchor_id IS NULL FROM {quoted}.hospital_price_hospital "
        "WHERE hospital_id='hospital-unbound'"
    )
    assert unbound is True


async def _assert_bad_allowed_count_rejected(connection, quoted, version_id) -> None:
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            f"INSERT INTO {quoted}.hospital_price_payer_charge VALUES ("
            "$1, 0, 0, 1, 'Payer', 'Bad Plan', 'other', NULL, NULL, "
            "'algorithm', 50, 40, 60, '10', 'invalid count')",
            version_id,
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            f"INSERT INTO {quoted}.hospital_price_hospital_npi VALUES "
            "('hospital-b', $1, 99, '0000000099', 'facility')",
            version_id,
        )


def test_models_use_lossless_types_and_optional_facility_anchor() -> None:
    tables = Base.metadata.tables
    assert HospitalPriceVersion.__table__ is tables["mrf.hospital_price_version"]
    assert HospitalPriceLocatorObservation.__table__ is tables["mrf.hospital_price_locator_observation"]
    assert HospitalPricePayerCharge.__table__ is tables["mrf.hospital_price_payer_charge"]
    assert len([name for name in tables if name.startswith("mrf.hospital_price_")]) == 20
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
            await _seed_attempt_and_identity(connection, quoted, content_sha, version_id)
            await _publish_and_prove_cas(connection, quoted, version_id)
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
