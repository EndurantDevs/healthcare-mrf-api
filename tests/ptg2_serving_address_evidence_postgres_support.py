# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
import os
import re
import uuid

import pytest

from api import ptg2_serving as serving
from db.connection import Database


ZCTA5_ZIP_INDEX_NAME = "zcta5_zcta5ce_idx"


def _require_disposable_postgres() -> None:
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database_name.lower():
        pytest.skip("address evidence integration tests require a disposable test database")


@asynccontextmanager
async def _temporary_schema():
    _require_disposable_postgres()
    database = Database()
    await database.connect()
    schema = f"ptg2_address_evidence_{uuid.uuid4().hex[:12]}"
    await database.status(f"CREATE SCHEMA {schema};")
    try:
        await _create_tables(database, schema)
        yield database, schema
    finally:
        await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def _create_unified_address_table(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE {schema}.entity_address_unified (
            location_key varchar PRIMARY KEY,
            npi bigint,
            address_key uuid,
            premise_key uuid,
            address_source_mask bigint NOT NULL DEFAULT 0,
            geo_evidence_source_id smallint,
            geo_identity_coherent boolean,
            geo_point_coherent boolean,
            geo_assurance_version smallint,
            address_sources varchar[] NOT NULL DEFAULT '{{}}',
            source_record_ids varchar[] NOT NULL DEFAULT '{{}}',
            source_count int NOT NULL DEFAULT 0,
            multi_source_confirmed boolean NOT NULL DEFAULT false,
            source_mask bigint NOT NULL DEFAULT 0,
            location_confidence_id smallint NOT NULL DEFAULT 0,
            address_precision varchar NOT NULL DEFAULT 'street',
            zip5 varchar,
            state_code varchar,
            state_name varchar,
            city_name varchar,
            postal_code varchar,
            type varchar,
            checksum bigint,
            telephone_number varchar,
            fax_number varchar,
            phone_number varchar,
            phone_extension varchar,
            fax_number_digits varchar,
            fax_extension varchar,
            first_line varchar,
            second_line varchar,
            country_code varchar,
            lat numeric,
            long numeric,
            updated_at timestamptz,
            last_seen_at timestamptz
        )
        """
    )


async def _create_geo_assurance_state_table(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE {schema}.entity_address_geo_assurance_state (
            singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
            active_geo_assurance_version smallint,
            active_table_oid oid,
            active_relation_signature jsonb,
            candidate_geo_assurance_version smallint,
            candidate_table_oid oid,
            candidate_relation_signature jsonb,
            candidate_projected_rows bigint
        )
        """
    )
    await database.status(
        f"INSERT INTO {schema}.entity_address_geo_assurance_state (singleton) "
        "VALUES (true)"
    )


async def _create_unified_address_geo_index(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE INDEX entity_address_unified_test_geo_idx
            ON {schema}.entity_address_unified
         USING gist (
            Geography(
                ST_MakePoint(
                    (long)::double precision,
                    (lat)::double precision
                )
            )
         )
         WHERE type IN ('primary', 'secondary', 'practice', 'site')
           AND COALESCE(address_precision, '') <> 'city_zip'
           AND lat IS NOT NULL
           AND long IS NOT NULL
        """
    )


async def _create_evidence_table(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE {schema}.entity_address_evidence (
            evidence_id bigint PRIMARY KEY,
            location_key varchar NOT NULL,
            address_key uuid,
            premise_key uuid,
            npi bigint,
            source_id smallint NOT NULL,
            source_record_key varchar,
            source_run_id varchar NOT NULL,
            source_snapshot_id varchar,
            observed_at timestamptz,
            last_seen_at timestamptz,
            retired_at timestamptz
        )
        """
    )


async def _create_mrf_address_table(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE {schema}.mrf_address (
            npi bigint,
            address_key uuid,
            type varchar,
            checksum bigint,
            date_added date,
            source_import_ids varchar[],
            source_import_dates date[],
            source_issuer_names varchar[],
            source_urls varchar[]
        )
        """
    )


async def _create_npi_address_table(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE {schema}.npi_address (
            npi bigint,
            address_key uuid,
            type varchar,
            checksum bigint,
            date_added date
        )
        """
    )


async def _create_cms_address_table(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE {schema}.doctor_clinician_address (
            npi bigint,
            address_key uuid,
            address_checksum bigint,
            updated_at timestamp
        )
        """
    )


async def _create_npi_scope_table(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE {schema}.ptg2_v3_npi_scope (
            snapshot_key bigint NOT NULL,
            npi bigint NOT NULL
        )
        """
    )


async def _create_geo_reference_tables(database: Database, schema: str) -> None:
    table_sql_by_name = {
        "geo_zip_lookup": """
            zip_code varchar PRIMARY KEY,
            state varchar,
            state_name varchar
        """,
        "zip_state": """
            zip varchar PRIMARY KEY,
            stusps varchar
        """,
        "zcta5": """
            gid bigserial PRIMARY KEY,
            zcta5ce varchar NOT NULL,
            the_geom geometry(Polygon, 4269) NOT NULL
        """,
    }
    for table_name, column_sql in table_sql_by_name.items():
        await database.status(
            f"CREATE TABLE {schema}.{table_name} ({column_sql})"
        )
    await database.status(
        f"CREATE INDEX {ZCTA5_ZIP_INDEX_NAME} "
        f"ON {schema}.zcta5 (zcta5ce)"
    )


async def _create_tables(database: Database, schema: str) -> None:
    await database.status("CREATE EXTENSION IF NOT EXISTS postgis")
    await _create_unified_address_table(database, schema)
    await _create_geo_assurance_state_table(database, schema)
    await _create_unified_address_geo_index(database, schema)
    await _create_evidence_table(database, schema)
    await _create_mrf_address_table(database, schema)
    await _create_npi_address_table(database, schema)
    await _create_cms_address_table(database, schema)
    await _create_npi_scope_table(database, schema)
    await _create_geo_reference_tables(database, schema)


def _schema_sql(sql: str, schema: str) -> str:
    for table_name in (
        "doctor_clinician_address",
        "entity_address_geo_assurance_state",
        "entity_address_evidence",
        "entity_address_unified",
        "npi",
        "npi_taxonomy",
        "nucc_taxonomy",
        "mrf_address",
        "npi_address",
        "ptg2_allowed_amount_item",
        "ptg2_allowed_amount_payment",
        "ptg2_allowed_amount_plan",
        "ptg2_allowed_amount_provider_payment",
        "ptg2_current_source_snapshot",
        "ptg2_snapshot",
        "ptg2_v3_npi_scope",
        "geo_zip_lookup",
    ):
        source_relation = re.escape(
            f"{serving.PTG2_SCHEMA}.{table_name}"
        )
        sql = re.sub(
            rf"(?P<prefix>\b(?:FROM|JOIN|UPDATE|INTO)\s+)"
            rf"{source_relation}(?![A-Za-z0-9_])",
            rf"\g<prefix>{schema}.{table_name}",
            sql,
        )
        sql = sql.replace(
            f"'{serving.PTG2_SCHEMA}.{table_name}'",
            f"'{schema}.{table_name}'",
        )
    for source_relation, target_relation in (
        ("tiger.zip_state", f"{schema}.zip_state"),
        ("tiger.zcta5", f"{schema}.zcta5"),
    ):
        sql = sql.replace(source_relation, target_relation)
    return sql


def _has_complete_lineage(lineage_entry: dict[str, object]) -> bool:
    return all(
        lineage_entry.get(field_name) not in (None, "", [])
        for field_name in (
            "dataset_id",
            "source_record_id",
            "record_version_id",
            "retrieved_at",
        )
    )
