# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable provider geo-assurance projection contracts."""

from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path

import pytest

from api import ptg2_geo_projection as projection
from api import ptg2_serving as serving
from api.ptg2_geo_policy import (
    provider_address_identity_coherence_sql,
    provider_address_location_filter_sql,
    provider_address_point_coherence_sql,
)
from db.models import EntityAddressUnified
from tests.ptg2_serving_address_evidence_postgres_support import (
    _schema_sql,
    _temporary_schema,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260825090000_geo_assurance_projection.py"
)
entity_address_unified = importlib.import_module(
    "process.entity_address_unified"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "geo_assurance_projection_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_projection_is_nullable_for_rollout_and_materialized_setwise():
    for column_name in (
        "geo_evidence_source_id",
        "geo_identity_coherent",
        "geo_point_coherent",
        "geo_assurance_version",
    ):
        assert EntityAddressUnified.__table__.c[column_name].nullable

    sql = entity_address_unified._materialize_geo_assurance_sql(
        "mrf",
        "entity_address_unified_stage",
    )

    assert "WITH projection_targets AS MATERIALIZED" in sql
    assert "projection_identity_admitted AS MATERIALIZED" in sql
    assert "projection_point_admitted AS MATERIALIZED" in sql
    assert "projection_nppes AS MATERIALIZED" in sql
    assert "projection_mrf AS MATERIALIZED" in sql
    assert "projection_cms AS MATERIALIZED" in sql
    assert "JOIN mrf.entity_address_unified_stage AS candidate" in sql
    assert sql.count("UPDATE mrf.entity_address_unified_stage AS target") == 1
    assert "geo_assurance_version = 1" in sql
    assert ") IS NOT TRUE" in sql
    assert "WHERE TRUE" in entity_address_unified._materialize_geo_assurance_sql(
        "mrf",
        "entity_address_unified_stage",
        force=True,
    )


def test_runtime_uses_projection_and_falls_back_to_exact_legacy_predicates():
    evidence_sql = serving._ptg2_geo_evidence_level_sql("addr")
    location_sql = provider_address_location_filter_sql(
        "addr",
        schema_name="mrf",
        exact_zip_predicate="addr.zip5 = :zip5",
        radius_predicates=["ST_DWithin(TRUE)"],
    )

    assert "addr.geo_assurance_version = 1" in evidence_sql
    assert "addr.geo_evidence_source_id IN (0, 1, 2, 3)" in evidence_sql
    assert {
        "geo_evidence_source_id",
        "geo_identity_coherent",
        "geo_point_coherent",
        "geo_assurance_version",
    } <= serving._PTG2_UNIFIED_ADDRESS_COLUMNS
    assert "FROM mrf.npi_address AS geo_nppes" in evidence_sql
    assert "FROM mrf.mrf_address AS geo_mrf" in evidence_sql
    assert "FROM mrf.doctor_clinician_address AS geo_doctor" in evidence_sql
    assert "FROM mrf.entity_address_geo_assurance_state" in evidence_sql
    assert "active_relation_signature" in evidence_sql
    assert "pg_relation_filenode" in evidence_sql
    assert "addr.geo_identity_coherent" in location_sql
    assert "addr.geo_point_coherent" in location_sql
    assert "mrf.geo_zip_lookup" in location_sql
    assert "tiger.zcta5" in location_sql


def test_migration_adds_nullable_metadata_and_generation_state(monkeypatch):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")

    migration.upgrade()

    assert migration.down_revision == (
        "20260820200000_provider_directory_projection_finalizer"
    )
    assert len(statements) == 4
    assert '"fixture"."entity_address_unified"' in statements[0]
    assert '"fixture"."entity_address_unified_old"' in statements[1]
    assert all("UPDATE" not in statement.upper() for statement in statements)
    assert all("NOT NULL" not in statement.upper() for statement in statements[:2])
    assert all(
        "geo_evidence_source_id smallint" in statement
        and "geo_identity_coherent boolean" in statement
        and "geo_point_coherent boolean" in statement
        and "geo_assurance_version smallint" in statement
        for statement in statements[:2]
    )
    assert 'CREATE TABLE IF NOT EXISTS "fixture"."entity_address_geo_assurance_state"' in statements[2]
    assert "active_relation_signature jsonb" in statements[2]
    assert "candidate_relation_signature jsonb" in statements[2]
    assert "ON CONFLICT (singleton) DO NOTHING" in statements[3]


@pytest.mark.asyncio
async def test_legacy_reused_stage_adds_projection_columns_before_reuse(monkeypatch):
    async with _temporary_schema() as (database, schema):
        stage_table = "entity_address_unified_reused"
        await database.status(
            f"CREATE TABLE {schema}.{stage_table} "
            f"(LIKE {schema}.entity_address_unified INCLUDING ALL)"
        )
        for column_name in (
            "geo_evidence_source_id",
            "geo_identity_coherent",
            "geo_point_coherent",
            "geo_assurance_version",
        ):
            await database.status(
                f"ALTER TABLE {schema}.{stage_table} DROP COLUMN {column_name}"
            )
        monkeypatch.setattr(entity_address_unified, "db", database)

        await entity_address_unified._ensure_entity_address_unified_live_columns(
            schema,
            stage_table,
        )

        assert await database.scalar(
            """
            SELECT COUNT(*)
              FROM information_schema.columns
             WHERE table_schema = :schema
               AND table_name = :table_name
               AND column_name IN (
                   'geo_evidence_source_id',
                   'geo_identity_coherent',
                   'geo_point_coherent',
                   'geo_assurance_version'
               )
            """,
            schema=schema,
            table_name=stage_table,
        ) == 4


def test_projection_validation_rejects_every_transitional_or_invalid_row():
    sql = entity_address_unified._invalid_geo_assurance_projection_sql(
        "mrf",
        "entity_address_unified_stage",
    )

    assert f"geo_assurance_version IS DISTINCT FROM {projection.GEO_ASSURANCE_VERSION}" in sql
    assert "geo_evidence_source_id NOT IN (0, 1, 2, 3)" in sql
    assert "geo_identity_coherent IS NULL" in sql
    assert "geo_point_coherent IS NULL" in sql


async def _insert_projection_references(database, schema: str) -> None:
    """Insert the identity and point reference rows for projection parity."""

    for reference_insert_sql in (
        f"""INSERT INTO {schema}.geo_zip_lookup
                (zip_code, state, state_name)
              VALUES ('00001', 'TS', 'TEST STATE')""",
        f"""INSERT INTO {schema}.zip_state (zip, stusps)
              VALUES ('00001', 'TS')""",
        f"""INSERT INTO {schema}.zcta5 (zcta5ce, the_geom)
              VALUES (
                '00001',
                ST_GeomFromText(
                  'POLYGON((-83.2 41.8,-82.8 41.8,-82.8 42.2,-83.2 42.2,-83.2 41.8))',
                  4269
                )
              )""",
    ):
        await database.status(reference_insert_sql)


async def _insert_projection_addresses(database, schema: str) -> None:
    """Insert every evidence class plus the CMS identity anchor."""

    await database.status(
        f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key,
            address_source_mask, type, checksum, first_line,
            city_name, state_name, state_code, postal_code, zip5,
            country_code, lat, long
        ) VALUES
            ('nppes', 7001, '00000000-0000-0000-0000-000000000001',
             '10000000-0000-0000-0000-000000000001', 1, 'practice', 1,
             '1 TEST STREET', 'TEST CITY', 'TS', 'TS', '00001', '00001',
             'US', 42.0, -83.0),
            ('mrf', 7002, '00000000-0000-0000-0000-000000000002',
             '10000000-0000-0000-0000-000000000002', 2, 'practice', 2,
             '2 TEST STREET', 'TEST CITY', 'TS', 'TS', '00001', '00001',
             'US', 42.0, -83.0),
            ('cms', 7003, '00000000-0000-0000-0000-000000000003',
             '10000000-0000-0000-0000-000000000003', 4, 'practice', 3,
             '3 TEST STREET', 'TEST CITY', 'TS', 'TS', '00001', '00001',
             'US', 42.0, -83.0),
            ('none', 7004, '00000000-0000-0000-0000-000000000004',
             '10000000-0000-0000-0000-000000000004', 0, 'practice', 4,
             '4 TEST STREET', 'TEST CITY', 'TS', 'TS', '00002', '00002',
             'US', 42.0, -83.0),
            ('cms-anchor', 7003, '00000000-0000-0000-0000-000000000005',
             '10000000-0000-0000-0000-000000000003', 1, 'practice', 5,
             '5 TEST STREET', 'TEST CITY', 'TS', 'TS', '00001', '00001',
             'US', 42.0, -83.0)
        """
    )


async def _insert_projection_evidence(database, schema: str) -> None:
    """Insert source rows proving the NPPES, MRF, and CMS classes."""

    for evidence_insert_sql in (
        f"""INSERT INTO {schema}.npi_address (
                npi, address_key, type, checksum, date_added
              ) VALUES
                (7001, '00000000-0000-0000-0000-000000000001',
                 'practice', 1, '2026-08-25'),
                (7003, '00000000-0000-0000-0000-000000000005',
                 'practice', 5, '2026-08-25')""",
        f"""INSERT INTO {schema}.mrf_address (
                npi, address_key, type, checksum, date_added,
                source_import_ids, source_import_dates, source_issuer_names
              ) VALUES (
                7002, '00000000-0000-0000-0000-000000000002',
                'practice', 2, '2026-08-25', ARRAY['mrf-v1']::varchar[],
                ARRAY['2026-08-25'::date],
                ARRAY['ISSUER A', 'ISSUER B']::varchar[]
              )""",
        f"""INSERT INTO {schema}.doctor_clinician_address (
                npi, address_key, address_checksum, updated_at
              ) VALUES (
                7003, '00000000-0000-0000-0000-000000000003',
                3, '2026-08-25 12:00:00'
              )""",
    ):
        await database.status(evidence_insert_sql)


async def _legacy_assurance_by_key(database, schema: str) -> dict:
    """Evaluate the pre-projection evidence and coherence predicates."""

    source_id_sql = projection.legacy_evidence_source_id_sql(
        "addr",
        schema_name=schema,
    )
    identity_predicate_sql = provider_address_identity_coherence_sql(
        "addr",
        schema_name=schema,
        use_projection=False,
    )
    point_predicate_sql = provider_address_point_coherence_sql(
        "addr",
        use_projection=False,
    )
    legacy_records = await database.all(
        _schema_sql(
            f"""
            SELECT location_key,
                   {source_id_sql} AS source_id,
                   {identity_predicate_sql} AS identity_coherent,
                   {point_predicate_sql} AS point_coherent
              FROM {schema}.entity_address_unified AS addr
             WHERE location_key IN ('nppes', 'mrf', 'cms', 'none')
          ORDER BY location_key
            """,
            schema,
        )
    )
    return {
        address_record._mapping["location_key"]: (
            address_record._mapping["source_id"],
            address_record._mapping["identity_coherent"],
            address_record._mapping["point_coherent"],
        )
        for address_record in legacy_records
    }


async def _assert_stored_projection(
    database,
    schema: str,
    legacy_by_key: dict,
) -> None:
    """Assert stored fields equal legacy evaluation for all evidence classes."""

    stored_records = await database.all(
        f"""
        SELECT location_key, geo_evidence_source_id,
               geo_identity_coherent, geo_point_coherent,
               geo_assurance_version
          FROM {schema}.entity_address_unified
         WHERE location_key IN ('nppes', 'mrf', 'cms', 'none')
      ORDER BY location_key
        """
    )
    stored_by_key = {
        address_record._mapping["location_key"]: (
            address_record._mapping["geo_evidence_source_id"],
            address_record._mapping["geo_identity_coherent"],
            address_record._mapping["geo_point_coherent"],
        )
        for address_record in stored_records
    }
    assert stored_by_key == legacy_by_key
    assert {
        location_key: assurance_fields[0]
        for location_key, assurance_fields in stored_by_key.items()
    } == {"cms": 3, "mrf": 2, "none": 0, "nppes": 1}
    assert stored_by_key["none"][2] is False
    assert all(
        address_record._mapping["geo_assurance_version"]
        == projection.GEO_ASSURANCE_VERSION
        for address_record in stored_records
    )


async def _assert_forced_projection_refresh(database, schema: str) -> None:
    """Assert reused-stage forcing refreshes coherent point state."""

    await database.status(
        f"""
        UPDATE {schema}.entity_address_unified
           SET lat = 44.0,
               long = -83.0
         WHERE location_key = 'nppes'
        """
    )
    forced_materialize_sql = _schema_sql(
        entity_address_unified._materialize_geo_assurance_sql(
            schema,
            "entity_address_unified",
            force=True,
        ),
        schema,
    )
    assert await database.status(forced_materialize_sql) == 5
    assert await database.scalar(
        f"""
        SELECT geo_point_coherent
          FROM {schema}.entity_address_unified
         WHERE location_key = 'nppes'
        """
    ) is False


async def _assert_stale_projection_refresh(
    database,
    schema: str,
    materialize_sql: str,
) -> None:
    """Assert a transitional NULL row is selectively reprojected."""

    await database.status(
        f"""
        UPDATE {schema}.entity_address_unified
           SET geo_evidence_source_id = NULL,
               geo_identity_coherent = NULL,
               geo_point_coherent = NULL,
               geo_assurance_version = NULL
         WHERE location_key = 'mrf'
        """
    )
    assert await database.status(materialize_sql) == 1
    assert await database.scalar(
        f"""
        SELECT geo_evidence_source_id
          FROM {schema}.entity_address_unified
         WHERE location_key = 'mrf'
        """
    ) == 2


async def _assert_validation_and_runtime_fallback(
    database,
    schema: str,
    monkeypatch,
) -> None:
    """Assert NULL rollout rows fall back yet fail pre-cutover validation."""

    monkeypatch.setattr(entity_address_unified, "db", database)
    assert await entity_address_unified._validate_geo_assurance_projection(
        schema,
        "entity_address_unified",
    ) == 0
    await database.status(
        f"""
        UPDATE {schema}.entity_address_unified
           SET geo_assurance_version = NULL
         WHERE location_key = 'nppes'
        """
    )
    runtime_level_sql = _schema_sql(
        serving._ptg2_geo_evidence_level_sql("addr"),
        schema,
    )
    fallback_level = await database.scalar(
        f"""
        SELECT {runtime_level_sql}
          FROM {schema}.entity_address_unified AS addr
         WHERE location_key = 'nppes'
        """
    )
    assert fallback_level == "nppes_registry_address"
    with pytest.raises(
        RuntimeError,
        match="1 staged rows have incomplete geo assurance",
    ):
        await entity_address_unified._validate_geo_assurance_projection(
            schema,
            "entity_address_unified",
        )


@pytest.mark.asyncio
async def test_postgres_projection_matches_legacy_for_every_evidence_class(
    monkeypatch,
):
    """Prove set-wise stored assurance exactly matches runtime legacy SQL."""

    async with _temporary_schema() as (database, schema):
        await _insert_projection_references(database, schema)
        await _insert_projection_addresses(database, schema)
        await _insert_projection_evidence(database, schema)
        legacy_by_key = await _legacy_assurance_by_key(database, schema)
        materialize_sql = _schema_sql(
            entity_address_unified._materialize_geo_assurance_sql(
                schema,
                "entity_address_unified",
            ),
            schema,
        )
        await database.status(materialize_sql)
        await _assert_stored_projection(database, schema, legacy_by_key)
        assert await database.status(materialize_sql) == 0
        await _assert_forced_projection_refresh(database, schema)
        await _assert_stale_projection_refresh(database, schema, materialize_sql)
        await _assert_validation_and_runtime_fallback(database, schema, monkeypatch)
