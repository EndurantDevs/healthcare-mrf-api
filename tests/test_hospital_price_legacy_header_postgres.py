# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable-PostgreSQL proof for source-faithful legacy hospital headers."""

from __future__ import annotations

import hashlib
import inspect
import uuid

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from db.models.hospital_price_header import HospitalPriceVersion
from support.hospital_price_native_validation import (
    HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PACKED_V2_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PACKED_V3_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PACKED_V4_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PACKED_V5_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
)
from tests.test_hospital_price_storage import (
    CSV_SHORT_V2_MIGRATION_PATH,
    CSV_TRANSITION_MIGRATION_PATH,
    LEGACY_HEADER_MIGRATION_PATH,
    _database_url,
    _drop_schema,
    _load_migration,
    _prepare_schema,
    _quote,
    _run_migration,
)


COUNT_INVARIANTS_MIGRATION_PATH = CSV_SHORT_V2_MIGRATION_PATH.with_name(
    "20260902103500_hospital_price_count_invariants.py"
)
RATE_TERM_MIGRATION_PATH = CSV_SHORT_V2_MIGRATION_PATH.with_name(
    "20260902160000_hospital_price_rate_term.py"
)
PRODUCER_CSV_V4_MIGRATION_PATH = CSV_SHORT_V2_MIGRATION_PATH.with_name(
    "20260903100000_hospital_price_producer_csv_4_0_0.py"
)


def test_legacy_header_schema_preserves_absent_profile_fields() -> None:
    """Keep legacy-only successor fields absent without relaxing v3."""

    shape_check = next(
        constraint
        for constraint in HospitalPriceVersion.__table__.constraints
        if constraint.name == "hospital_price_version_shape_check"
    )
    assert HospitalPriceVersion.__table__.c.attester_name.nullable is True
    model_sql = str(shape_check.sqltext)
    assert HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256 in model_sql
    assert HOSPITAL_MRF_PACKED_V2_PARSER_CONTRACT_SHA256 in model_sql
    assert HOSPITAL_MRF_PACKED_V3_PARSER_CONTRACT_SHA256 in model_sql
    assert HOSPITAL_MRF_PACKED_V4_PARSER_CONTRACT_SHA256 in model_sql
    assert HOSPITAL_MRF_PACKED_V5_PARSER_CONTRACT_SHA256 in model_sql
    assert HOSPITAL_MRF_PARSER_CONTRACT_SHA256 in model_sql
    assert "template_version = '3.0.0' AND npi_count > 0" in model_sql
    assert (
        "template_version IN ('3.0.1', '4.0.0') AND npi_count > 0"
        in model_sql
    )
    assert model_sql.count("'3.0.1'") == 1
    assert model_sql.count("'4.0.0'") == 1
    assert "template_version IN ('2.0.0', '2.2.0', '2.2.1')" in model_sql
    assert "npi_count = 0 AND attester_name IS NULL" in model_sql
    assert "source_format IN ('csv-tall', 'csv-wide') AND npi_count >= 0" in model_sql
    migration = _load_migration(LEGACY_HEADER_MIGRATION_PATH)
    assert migration.revision == "20260831100000_hospital_price_legacy_header"
    assert migration.down_revision == (
        "20260830100000_provider_directory_rooted_partial_lineage"
    )
    migration_sql = inspect.getsource(migration.upgrade)
    assert HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256 in migration_sql
    assert HOSPITAL_MRF_PACKED_V2_PARSER_CONTRACT_SHA256 in migration_sql
    assert HOSPITAL_MRF_PACKED_V3_PARSER_CONTRACT_SHA256 in migration_sql
    transition = _load_migration(CSV_TRANSITION_MIGRATION_PATH)
    assert transition.revision == (
        "20260831180000_hospital_price_csv_transition_metadata"
    )
    assert transition.down_revision == "20260831100000_hospital_price_legacy_header"
    transition_sql = inspect.getsource(transition.upgrade)
    assert HOSPITAL_MRF_PACKED_V3_PARSER_CONTRACT_SHA256 in transition_sql
    assert HOSPITAL_MRF_PACKED_V4_PARSER_CONTRACT_SHA256 in transition_sql
    short_v2 = _load_migration(CSV_SHORT_V2_MIGRATION_PATH)
    assert short_v2.revision == "20260901000000_hospital_price_csv_short_v2"
    assert short_v2.down_revision == (
        "20260831180000_hospital_price_csv_transition_metadata"
    )
    short_v2_sql = inspect.getsource(short_v2.upgrade)
    assert HOSPITAL_MRF_PACKED_V4_PARSER_CONTRACT_SHA256 in short_v2_sql
    assert HOSPITAL_MRF_PACKED_V5_PARSER_CONTRACT_SHA256 in short_v2_sql
    count_invariants = _load_migration(COUNT_INVARIANTS_MIGRATION_PATH)
    assert count_invariants.revision == (
        "20260902103500_hospital_price_count_invariants"
    )
    assert count_invariants.down_revision == (
        "20260901103000_plan_pricing_em_distance"
    )


def test_rate_term_migration_admits_current_contract_and_modifier_metadata() -> None:
    rate_term = _load_migration(RATE_TERM_MIGRATION_PATH)
    assert rate_term.revision == "20260902160000_hospital_price_rate_term"
    assert rate_term.down_revision == "20260902103500_hospital_price_count_invariants"
    rate_term_sql = inspect.getsource(rate_term.upgrade)
    assert HOSPITAL_MRF_PACKED_V5_PARSER_CONTRACT_SHA256 in rate_term_sql
    assert HOSPITAL_MRF_PARSER_CONTRACT_SHA256 in rate_term_sql
    assert "ADD COLUMN negotiated_rate_term text" in rate_term_sql
    assert "ALTER COLUMN payer_name DROP NOT NULL" in rate_term_sql
    assert "ALTER COLUMN plan_name DROP NOT NULL" in rate_term_sql
    assert "DROP CONSTRAINT hospital_price_modifier_payer_shape_check" in rate_term_sql
    assert rate_term.downgrade() is None


def test_producer_csv_v4_migration_is_current_parser_only() -> None:
    migration = _load_migration(PRODUCER_CSV_V4_MIGRATION_PATH)
    assert migration.revision == "20260903100000_hospital_price_producer_csv_4_0_0"
    assert migration.down_revision == "20260902160000_hospital_price_rate_term"
    migration_sql = inspect.getsource(migration.upgrade)
    assert HOSPITAL_MRF_PARSER_CONTRACT_SHA256 in migration_sql
    assert "template_version = '4.0.0' AND npi_count > 0" in migration_sql
    assert "source_format IN ('csv-tall', 'csv-wide')" in migration_sql
    assert migration_sql.count("'4.0.0'") == 1
    assert migration.downgrade() is None


async def _insert_header(
    connection: asyncpg.Connection,
    quoted: str,
    *,
    version_id: str,
    parser_contract_sha256: str,
    source_format: str,
    template_version: str,
    attester_name: str | None,
    npi_count: int,
) -> None:
    content_sha256 = hashlib.sha256(f"content:{version_id}".encode()).hexdigest()
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_content "
        "(content_sha256, byte_count, media_type) VALUES ($1, 1, 'application/json')",
        content_sha256,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_version ("
        "version_id, content_sha256, parser_contract_sha256, semantic_sha256, "
        "source_format, source_hospital_name, last_updated_on, template_version, "
        "attestation_text, confirm_attestation, attester_name, location_count, "
        "npi_count, license_count, service_count, charge_count, payer_charge_count) "
        "VALUES ($1, $2, $3, $4, $5, 'Legacy Hospital', DATE '2026-01-01', "
        "$6, 'affirmation', true, $7, 1, $8, 1, 1, 1, 0)",
        version_id,
        content_sha256,
        parser_contract_sha256,
        "d" * 64,
        source_format,
        template_version,
        attester_name,
        npi_count,
    )


async def _assert_profile_constraints(connection: asyncpg.Connection, quoted: str) -> None:
    """Verify migrated header rows and every profile shape guard."""
    legacy_headers = await connection.fetch(
        f"SELECT template_version, attester_name, npi_count "
        f"FROM {quoted}.hospital_price_version "
        "WHERE parser_contract_sha256=$1 ORDER BY version_id",
        HOSPITAL_MRF_PACKED_V2_PARSER_CONTRACT_SHA256,
    )
    assert [dict(header) for header in legacy_headers] == [
        {
            "template_version": "2.0.0",
            "attester_name": "Historical Attester",
            "npi_count": 1,
        },
        {
            "template_version": "3.0.1",
            "attester_name": "Historical Attester",
            "npi_count": 1,
        },
    ]
    assert await connection.fetchval(
        "SELECT convalidated FROM pg_constraint WHERE conrelid=$1::regclass "
        "AND conname='hospital_price_version_shape_check'",
        f"{quoted}.hospital_price_version",
    ) is True
    current_record = await connection.fetchrow(
        f"SELECT attester_name, npi_count FROM {quoted}.hospital_price_version "
        "WHERE version_id=$1",
        "a" * 64,
    )
    assert current_record is not None
    assert current_record["attester_name"] is None
    assert current_record["npi_count"] == 0
    for invalid_set in (
        "npi_count=-1",
        "template_version='3.0.0'",
        "template_version='2.0.0'",
        f"parser_contract_sha256='{'f' * 64}'",
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"UPDATE {quoted}.hospital_price_version SET {invalid_set} "
                "WHERE version_id=$1",
                "a" * 64,
            )
    for invalid_set in (
        "location_count=0",
        "license_count=0",
        "service_count=0",
        "charge_count=0",
        "payer_charge_count=-1",
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"UPDATE {quoted}.hospital_price_version SET {invalid_set} "
                "WHERE version_id=$1",
                "a" * 64,
            )
    await _assert_forward_metadata_constraints(connection, quoted)


async def _assert_forward_metadata_constraints(
    connection: asyncpg.Connection, quoted: str
) -> None:
    csv_record = await connection.fetchrow(
        f"SELECT source_format, template_version, attester_name, npi_count "
        f"FROM {quoted}.hospital_price_version WHERE version_id=$1",
        "c" * 64,
    )
    assert dict(csv_record) == {
        "source_format": "csv-tall",
        "template_version": "2.0.0",
        "attester_name": "Ben Levin",
        "npi_count": 1,
    }
    short_v2_record = await connection.fetchrow(
        f"SELECT parser_contract_sha256, source_format, template_version "
        f"FROM {quoted}.hospital_price_version WHERE version_id=$1",
        "5" * 64,
    )
    assert dict(short_v2_record) == {
        "parser_contract_sha256": HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
        "source_format": "csv-wide",
        "template_version": "2",
    }
    producer_v4_record = await connection.fetchrow(
        f"SELECT parser_contract_sha256, source_format, template_version, "
        f"attester_name, npi_count FROM {quoted}.hospital_price_version "
        "WHERE version_id=$1",
        "7" * 64,
    )
    assert dict(producer_v4_record) == {
        "parser_contract_sha256": HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
        "source_format": "csv-tall",
        "template_version": "4.0.0",
        "attester_name": "Current Attester",
        "npi_count": 1,
    }
    for version_id, invalid_set in (
        ("c" * 64, "attester_name='   '"),
        ("6" * 64, "npi_count=1, attester_name='Unexpected'"),
        ("8" * 64, "npi_count=0"),
        ("4" * 64, "template_version='2'"),
        ("5" * 64, "source_format='json'"),
        ("7" * 64, "source_format='json'"),
        ("7" * 64, "npi_count=0"),
        ("7" * 64, "attester_name=NULL"),
        (
            "7" * 64,
            f"parser_contract_sha256='{HOSPITAL_MRF_PACKED_V5_PARSER_CONTRACT_SHA256}'",
        ),
        ("c" * 64, "semantic_sha256='not-a-hash'"),
        ("5" * 64, "version_id='not-a-hash'"),
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"UPDATE {quoted}.hospital_price_version SET {invalid_set} "
                "WHERE version_id=$1",
                version_id,
            )


async def _seed_historical_headers(database_url, quoted: str) -> None:
    connection = await asyncpg.connect(
        str(database_url.set(drivername="postgresql"))
    )
    try:
        for version_id, template_version in (
            ("2" * 64, "2.0.0"),
            ("e" * 64, "3.0.1"),
        ):
            await _insert_header(
                connection,
                quoted,
                version_id=version_id,
                parser_contract_sha256=(
                    HOSPITAL_MRF_PACKED_V2_PARSER_CONTRACT_SHA256
                ),
                source_format="json",
                template_version=template_version,
                attester_name="Historical Attester",
                npi_count=1,
            )
    finally:
        await connection.close()


async def _seed_packed_v3_headers(database_url, quoted: str) -> None:
    connection = await asyncpg.connect(
        str(database_url.set(drivername="postgresql"))
    )
    try:
        await _insert_header(
            connection,
            quoted,
            version_id="a" * 64,
            parser_contract_sha256=HOSPITAL_MRF_PACKED_V3_PARSER_CONTRACT_SHA256,
            source_format="json",
            template_version="2.2.0",
            attester_name=None,
            npi_count=0,
        )
        await _insert_header(
            connection,
            quoted,
            version_id="3" * 64,
            parser_contract_sha256=HOSPITAL_MRF_PACKED_V3_PARSER_CONTRACT_SHA256,
            source_format="json",
            template_version="3.0.0",
            attester_name="Current Attester",
            npi_count=1,
        )
    finally:
        await connection.close()


async def _prove_current_headers(database_url, quoted: str) -> None:
    connection = await asyncpg.connect(
        str(database_url.set(drivername="postgresql"))
    )
    try:
        for fields in (
            {
                "version_id": "5" * 64,
                "source_format": "csv-wide",
                "template_version": "2",
                "attester_name": None,
                "npi_count": 0,
            },
            {
                "version_id": "c" * 64,
                "source_format": "csv-tall",
                "template_version": "2.0.0",
                "attester_name": "Ben Levin",
                "npi_count": 1,
            },
            {
                "version_id": "6" * 64,
                "source_format": "json",
                "template_version": "2.2.0",
                "attester_name": None,
                "npi_count": 0,
            },
            {
                "version_id": "8" * 64,
                "source_format": "json",
                "template_version": "3.0.0",
                "attester_name": "Current Attester",
                "npi_count": 1,
            },
            {
                "version_id": "7" * 64,
                "source_format": "csv-tall",
                "template_version": "4.0.0",
                "attester_name": "Current Attester",
                "npi_count": 1,
            },
        ):
            await _insert_header(
                connection,
                quoted,
                parser_contract_sha256=HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
                **fields,
            )
        await _assert_profile_constraints(connection, quoted)
        await _assert_modifier_rate_term_storage(connection, quoted)
    finally:
        await connection.close()


async def _assert_modifier_rate_term_storage(
    connection: asyncpg.Connection, quoted: str
) -> None:
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_modifier "
        "(version_id, modifier_ordinal, code, description) "
        "VALUES ($1, 0, 'TC', 'Technical component')",
        "5" * 64,
    )
    await connection.execute(
        f"INSERT INTO {quoted}.hospital_price_modifier_payer "
        "(version_id, modifier_ordinal, payer_ordinal, payer_name, plan_name, "
        "negotiated_rate_term, standard_charge_percentage) "
        "VALUES ($1, 0, 0, 'Payer', 'Plan', $2, 62.5)",
        "5" * 64,
        "JAN 2026-MAY 2026",
    )
    assert await connection.fetchval(
        f"SELECT negotiated_rate_term FROM "
        f"{quoted}.hospital_price_modifier_payer WHERE version_id=$1",
        "5" * 64,
    ) == "JAN 2026-MAY 2026"
    assert await connection.fetchval(
        "SELECT convalidated FROM pg_constraint WHERE conrelid=$1::regclass "
        "AND conname='hospital_price_modifier_payer_shape_check'",
        f"{quoted}.hospital_price_modifier_payer",
    ) is True
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            f"UPDATE {quoted}.hospital_price_modifier_payer "
            "SET negotiated_rate_term='   ' WHERE version_id=$1",
            "5" * 64,
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            f"UPDATE {quoted}.hospital_price_modifier_payer "
            "SET payer_name=NULL, plan_name=NULL WHERE version_id=$1",
            "5" * 64,
        )


async def _seed_packed_v4_header(database_url, quoted: str) -> None:
    connection = await asyncpg.connect(
        str(database_url.set(drivername="postgresql"))
    )
    try:
        await _insert_header(
            connection,
            quoted,
            version_id="4" * 64,
            parser_contract_sha256=HOSPITAL_MRF_PACKED_V4_PARSER_CONTRACT_SHA256,
            source_format="csv-tall",
            template_version="2.0.0",
            attester_name=None,
            npi_count=0,
        )
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_postgres_legacy_header_keeps_absent_fields_absent(monkeypatch) -> None:
    """Keep deployed packed-v2 rows while enforcing the current contract."""

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
        quoted = _quote(schema)
        await _seed_historical_headers(database_url, quoted)
        await _run_migration(
            engine, _load_migration(LEGACY_HEADER_MIGRATION_PATH), "upgrade"
        )
        await _seed_packed_v3_headers(database_url, quoted)
        await _run_migration(
            engine, _load_migration(CSV_TRANSITION_MIGRATION_PATH), "upgrade"
        )
        await _seed_packed_v4_header(database_url, quoted)
        await _run_migration(
            engine, _load_migration(CSV_SHORT_V2_MIGRATION_PATH), "upgrade"
        )
        await _run_migration(
            engine, _load_migration(COUNT_INVARIANTS_MIGRATION_PATH), "upgrade"
        )
        await _run_migration(
            engine, _load_migration(RATE_TERM_MIGRATION_PATH), "upgrade"
        )
        await _run_migration(
            engine, _load_migration(PRODUCER_CSV_V4_MIGRATION_PATH), "upgrade"
        )
        await _prove_current_headers(database_url, quoted)
    finally:
        await _drop_schema(engine, schema)
        await engine.dispose()
