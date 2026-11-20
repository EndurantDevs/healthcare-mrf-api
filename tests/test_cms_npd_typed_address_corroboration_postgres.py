# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS NPD addresses merge with and corroborate other sources through the real unified-address raw SQL."""

import importlib
import os
from uuid import uuid4

import pytest
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from db.models import DoctorClinicianAddress, NPIAddress
from process import entity_address_batch as batch
from process.cms_npd_typed_addresses import CREATE_STAGE
from tests.test_address_format_db import FOUNDATION_MIGRATION_PATH, _load_migration

DSN_ENV = "HLTHPRT_CMS_NPD_TYPED_IMPORT_TEST_DSN"
pytestmark = pytest.mark.skipif(not os.getenv(DSN_ENV), reason=f"{DSN_ENV} names a disposable PostgreSQL database")
NPI = 1000000001
MAIN = ("10 Main St", "Suite 5", "Boston", "MA", "02108")
OTHER = ("99 Harbor Rd", None, "Boston", "MA", "02110")


@pytest.fixture
async def database(monkeypatch):
    """An empty schema with the canonical address functions, the archive, and the two source tables."""
    sql = importlib.import_module("process.entity_address_unified")
    dsn = os.environ[DSN_ENV].replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(dsn, hide_parameters=True)
    connected = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    schema = "cms_npd_corroboration_" + uuid4().hex[:12]
    await connected.status(f"CREATE SCHEMA {schema}")
    monkeypatch.setattr(sql, "db", connected)
    monkeypatch.setattr(sql, "_entity_address_sql_settings", lambda: [])
    try:
        await _create_tables(connected, schema, sql)
        yield sql, connected, schema
    finally:
        await connected.status(f"DROP SCHEMA {schema} CASCADE")
        await connected.disconnect()


async def _create_tables(database, schema, sql):
    async with database.engine.begin() as connection:
        for model, name in (
            (NPIAddress, "npi_address"),
            (DoctorClinicianAddress, "doctor_clinician_address"),
            (sql.EntityAddressUnified, "unified"),
        ):
            table = model.__table__.to_metadata(MetaData(), schema=schema, name=name)
            for index in list(table.indexes):
                table.indexes.discard(index)
            await connection.run_sync(table.create)
        foundation = _load_migration(FOUNDATION_MIGRATION_PATH, "cms_npd_corroboration_foundation")
        await connection.run_sync(
            lambda sync: foundation._exec_sql_batch(sync, foundation._create_functions_sql(schema))
        )
    formatter = _load_migration()
    await database.status(formatter._humanize_component_function_sql(schema))
    await database.status(formatter._formatted_address_function_sql(schema))
    await database.status(CREATE_STAGE.format(stage=f"{schema}.cms_npd_address"))
    await database.status(sql._prepare_raw_stage_sql(schema, "raw", unlogged=False))
    await database.status(
        f"CREATE TABLE {schema}.address_alias_state_v1 (singleton boolean PRIMARY KEY,generation bigint,"
        "schema_version int DEFAULT 2,active_ruleset_version int DEFAULT 1)"
    )
    await database.status(f"INSERT INTO {schema}.address_alias_state_v1(singleton,generation) VALUES (TRUE,0)")
    await database.status(
        f"CREATE TABLE {schema}.address_archive_v2 (address_key uuid PRIMARY KEY,identity_key text NOT NULL,"
        "identity_version smallint DEFAULT 2,precision text DEFAULT 'street',premise_key uuid,zip5 text,"
        "state_code text,city_norm text,lat numeric,long numeric,place_id text,merged_into uuid)"
    )
    await database.status(
        f"CREATE TABLE {schema}.address_alias_v1 (source_address_key uuid PRIMARY KEY,target_address_key uuid,"
        "source_identity_key text,target_identity_key text,revoked_at timestamptz)"
    )


async def _insert_sources(database, schema):
    """NPPES primary and CMS Doctors practice at MAIN; CMS NPD at MAIN (another spelling) and OTHER."""
    await database.status(
        f"""INSERT INTO {schema}.npi_address (npi, type, checksum, first_line, second_line, city_name, state_name,
                postal_code, country_code, date_added)
            VALUES ({NPI}, 'primary', 1, '10 Main Street', 'Suite 5', 'Boston', 'MA', '02108', 'US', '2026-01-01')"""
    )
    await database.status(
        f"""INSERT INTO {schema}.doctor_clinician_address (npi, address_checksum, address_line1, address_line2,
                city, state, zip_code, updated_at)
            VALUES ({NPI}, 7, '10 Main St', 'Suite 5', 'Boston', 'MA', '02108', '2026-01-02')"""
    )
    for kind, (line1, line2, city, state, postal), latitude in (
        ("role_location", MAIN, 42.35),
        ("practitioner", OTHER, None),
    ):
        await database.status(
            f"""INSERT INTO {schema}.cms_npd_address (npi, source_kind, first_line, second_line, city_name, state_name,
                    postal_code, country_code, latitude, longitude, source_record_id)
                VALUES (:npi, :kind, :line1, :line2, :city, :state, :postal, 'US', :latitude,
                        CASE WHEN CAST(:latitude AS float8) IS NULL THEN NULL ELSE -71.05 END, :rid)""",
            npi=NPI,
            kind=kind,
            line1=line1.upper() if kind == "role_location" else line1,
            line2=line2,
            city=city,
            state=state,
            postal=postal,
            latitude=latitude,
            rid=f"cms_npd:{NPI}:{kind}",
        )
    await database.status(
        f"""INSERT INTO {schema}.address_archive_v2(address_key,identity_key)
            SELECT DISTINCT {schema}.addr_key_v1(l1,l2,c,s,z,'US'), {schema}.addr_identity_key_v1(l1,l2,c,s,z,'US')
            FROM (SELECT first_line, second_line, city_name, state_name, postal_code FROM {schema}.cms_npd_address
                  UNION ALL SELECT first_line, second_line, city_name, state_name, postal_code
                  FROM {schema}.npi_address) a(l1,l2,c,s,z)"""
    )


async def test_cms_npd_address_merges_with_cms_doctors_and_corroborates_nppes(database):
    sql, connected, schema = database
    await _insert_sources(connected, schema)
    available_by_name = {"npi_address": True, "doctor_clinician_address": True, "cms_npd_address": True}
    selects = sql._source_selects(schema, available_by_name)
    assert len(selects) == 3

    @batch.scope
    async def materialize():
        for select in selects:
            await connected.status(sql._insert_raw_from_source_sql(schema, "raw", select))
        await connected.status(sql._enrich_raw_stage_sql(schema, "raw", evidence_shards=3))
        await sql._validate_raw_alias_integrity(schema, "raw", is_address_canon_available=True)
        await connected.status(sql._materialize_from_raw_sql(schema, "unified", "raw", inline_source_evidence=True))

    await materialize()
    unified_rows = await connected.all(
        f"""SELECT type, first_line, address_sources, source_count, independent_source_count,
                   multi_source_confirmed, address_source_mask, source_mask, lat, address_key IS NOT NULL AS keyed
            FROM {schema}.unified ORDER BY type, first_line"""
    )
    by_type_line = {(unified_row.type, unified_row.first_line.upper()): unified_row for unified_row in unified_rows}
    assert len(unified_rows) == 3, unified_rows
    practice = by_type_line[("practice", "10 MAIN ST")]
    assert practice.keyed and practice.address_sources == ["cms_doctors", "cms_npd", "nppes"]
    assert (practice.source_count, practice.independent_source_count, practice.multi_source_confirmed) == (3, 3, True)
    assert practice.address_source_mask == 4 | 256 and practice.source_mask == 4 | 256  # masks stay per unified_row
    assert practice.lat is not None  # the CMS location coordinates fill the merged unified_row
    primary = by_type_line[("primary", "10 MAIN STREET")]
    assert primary.address_sources == ["cms_doctors", "cms_npd", "nppes"] and primary.multi_source_confirmed
    assert primary.address_source_mask == 1  # the evidence pass does not share masks across types
    only_cms = by_type_line[("practice", "99 HARBOR RD")]
    assert only_cms.address_sources == ["cms_npd"] and not only_cms.multi_source_confirmed
    assert (only_cms.source_count, only_cms.address_source_mask) == (1, 256)
