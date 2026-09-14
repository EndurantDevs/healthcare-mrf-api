# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import importlib
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import ptg2_geo_projection as geo_projection
from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_support import (
    _create_cms_address_table,
    _create_geo_assurance_state_table,
    _create_mrf_address_table,
    _create_npi_address_table,
)
from tests.test_entity_address_snapshot_stage_postgres import (
    _capture_alias_receipt,
    _capture_receipt,
    _create_model_family,
    _native_test_connection,
)

adoption = importlib.import_module("process.entity_address_snapshot_adoption")
alias_receipt = importlib.import_module("process.entity_address_snapshot_alias")
destination = importlib.import_module("process.entity_address_snapshot_destination")
models = importlib.import_module("db.models")
receipt = importlib.import_module("process.entity_address_snapshot_receipt")
restore = importlib.import_module("process.entity_address_snapshot_restore")
result_generation = importlib.import_module("process.entity_address_result_generation")


def _source_serving_generation() -> dict[str, object]:
    return {
        "origin_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        "origin_generation": 27,
        "published_at": "2026-09-14T08:30:00Z",
    }


async def _create_alias_relations(database: Database, schema: str, generation: int) -> None:
    """Create exact local alias models with an empty, generation-independent active set."""

    assert database.engine is not None
    async with database.engine.begin() as connection:
        metadata = MetaData(schema=schema)
        for model in (models.AddressAliasStateV1, models.AddressAliasV1):
            model.__table__.to_metadata(metadata, schema=schema)
        await connection.run_sync(metadata.create_all)
        await connection.execute(
            text(
                f'INSERT INTO "{schema}"."address_alias_state_v1" '
                "(singleton, schema_version, active_ruleset_version, generation, updated_at) "
                "VALUES (true, 2, 1, :generation, :updated_at)"
            ),
            {
                "generation": generation,
                "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            },
        )


async def _create_geo_dependencies(database: Database, schema: str) -> None:
    """Create the real local projection inputs and shared spatial references."""

    await _create_geo_assurance_state_table(database, schema)
    await _create_npi_address_table(database, schema)
    await _create_mrf_address_table(database, schema)
    await _create_cms_address_table(database, schema)
    await database.status(
        f"CREATE TABLE {schema}.geo_zip_lookup (zip_code varchar PRIMARY KEY, state varchar, state_name varchar)"
    )
    await database.status("CREATE TABLE IF NOT EXISTS tiger.zip_state (zip varchar PRIMARY KEY, stusps varchar)")
    await database.status(
        "CREATE TABLE IF NOT EXISTS tiger.zcta5 ("
        "gid bigserial PRIMARY KEY, zcta5ce varchar NOT NULL, "
        "the_geom geometry(Polygon, 4269) NOT NULL)"
    )


@asynccontextmanager
async def _owned_native_database(async_dsn: str, monkeypatch: pytest.MonkeyPatch):
    """Create an isolated database so every schema used by this test is owned."""

    import asyncpg

    admin_url = make_url(async_dsn).set(drivername="postgresql")
    database_name = "hc_entity_address_stage_" + uuid4().hex
    database_url = admin_url.set(database=database_name, drivername="postgresql+asyncpg")
    admin = await asyncpg.connect(admin_url.render_as_string(hide_password=False), timeout=10)
    database = None
    is_creation_attempted = False
    try:
        assert await admin.fetchval("SELECT count(*) FROM pg_database WHERE datname=$1", database_name) == 0
        is_creation_attempted = True
        await admin.execute(f'CREATE DATABASE "{database_name}" TEMPLATE template0')
        engine = create_async_engine(database_url, max_overflow=0, pool_size=2)
        database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
        monkeypatch.setenv("HLTHPRT_DB_DATABASE", database_name)
        monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
        assert await database.scalar("SELECT current_database()") == database_name
        yield database
    finally:
        try:
            disconnect_error = None
            try:
                if database is not None:
                    await database.disconnect()
            except BaseException as error:
                disconnect_error = error
            try:
                if is_creation_attempted:
                    await admin.execute(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
                    assert await admin.fetchval("SELECT count(*) FROM pg_database WHERE datname=$1", database_name) == 0
                    assert (
                        await admin.fetchval("SELECT count(*) FROM pg_stat_activity WHERE datname=$1", database_name)
                        == 0
                    )
            finally:
                if disconnect_error is not None:
                    raise disconnect_error
        finally:
            await admin.close(timeout=5)
            assert admin.is_closed()


async def _seed_geo_dependencies(database: Database, schema: str) -> None:
    """Seed identity, point, NPPES, MRF, and CMS evidence used by projection."""

    await database.status(f"INSERT INTO {schema}.geo_zip_lookup VALUES ('00001', 'TS', 'TEST STATE')")
    await database.status("INSERT INTO tiger.zip_state VALUES ('00001', 'TS') ON CONFLICT DO NOTHING")
    await database.status(
        "INSERT INTO tiger.zcta5 (zcta5ce, the_geom) VALUES ("
        "'00001', ST_GeomFromText("
        "'POLYGON((-83.2 41.8,-82.8 41.8,-82.8 42.2,-83.2 42.2,-83.2 41.8))', 4269)) "
        "ON CONFLICT DO NOTHING"
    )
    await database.status(
        f"INSERT INTO {schema}.npi_address (npi, address_key, type, checksum, date_added) VALUES "
        "(7001, '00000000-0000-0000-0000-000000000001', 'practice', 1, '2026-08-25'), "
        "(7003, '00000000-0000-0000-0000-000000000005', 'practice', 5, '2026-08-25')"
    )
    await database.status(
        f"INSERT INTO {schema}.mrf_address ("
        "npi, address_key, type, checksum, date_added, source_import_ids, "
        "source_import_dates, source_issuer_names) VALUES ("
        "7002, '00000000-0000-0000-0000-000000000002', 'practice', 2, '2026-08-25', "
        "ARRAY['mrf-v1']::varchar[], ARRAY['2026-08-25'::date], "
        "ARRAY['ISSUER A', 'ISSUER B']::varchar[])"
    )
    await database.status(
        f"INSERT INTO {schema}.doctor_clinician_address "
        "(npi, address_key, address_checksum, updated_at) VALUES ("
        "7003, '00000000-0000-0000-0000-000000000003', 3, '2026-08-25 12:00:00')"
    )


async def _seed_source_rows(session, schema: str, source_generation: int) -> None:
    """Seed every geo evidence class and every allowed base-version form."""

    alias_version = f"{destination.entity_address_unified.ALIAS_BASE_ADDRESS_VERSION_PREFIX}{source_generation}"
    plain_version = destination.entity_address_unified.BASE_ADDRESS_VERSION
    await session.execute(
        text(
            f'INSERT INTO "{schema}"."entity_address_unified" ('
            "entity_type, entity_id, location_key, checksum, type, npi, address_key, "
            "premise_key, address_source_mask, first_line, city_name, state_name, "
            "state_code, postal_code, zip5, country_code, lat, long, base_address_version) VALUES "
            "('synthetic', 'nppes', 'nppes', 1, 'practice', 7001, "
            "'00000000-0000-0000-0000-000000000001', "
            "'10000000-0000-0000-0000-000000000001', 1, '1 TEST STREET', "
            "'TEST CITY', 'TS', 'TS', '00001', '00001', 'US', 42.0, -83.0, :alias_version), "
            "('synthetic', 'mrf', 'mrf', 2, 'practice', 7002, "
            "'00000000-0000-0000-0000-000000000002', "
            "'10000000-0000-0000-0000-000000000002', 2, '2 TEST STREET', "
            "'TEST CITY', 'TS', 'TS', '00001', '00001', 'US', 42.0, -83.0, :alias_version), "
            "('synthetic', 'cms', 'cms', 3, 'practice', 7003, "
            "'00000000-0000-0000-0000-000000000003', "
            "'10000000-0000-0000-0000-000000000003', 4, '3 TEST STREET', "
            "'TEST CITY', 'TS', 'TS', '00001', '00001', 'US', 42.0, -83.0, :alias_version), "
            "('synthetic', 'none', 'none', 4, 'practice', 7004, "
            "'00000000-0000-0000-0000-000000000004', "
            "'10000000-0000-0000-0000-000000000004', 0, '4 TEST STREET', "
            "'TEST CITY', 'TS', 'TS', '00002', '00002', 'US', 42.0, -83.0, NULL), "
            "('synthetic', 'cms-anchor', 'cms-anchor', 5, 'practice', 7003, "
            "'00000000-0000-0000-0000-000000000005', "
            "'10000000-0000-0000-0000-000000000003', 1, '5 TEST STREET', "
            "'TEST CITY', 'TS', 'TS', '00001', '00001', 'US', 42.0, -83.0, :plain_version)"
        ),
        {"alias_version": alias_version, "plain_version": plain_version},
    )
    await _seed_support_rows(session, schema, location_key="nppes", entity_id="nppes")


async def _seed_support_rows(session, schema: str, *, location_key: str, entity_id: str) -> None:
    """Populate every immutable support relation for lineage and predecessor proof."""

    statements = (
        (
            "entity_address_evidence",
            "(evidence_id, location_key, entity_type, entity_id, source_id, source_run_id, observed_at) "
            "VALUES (1, :location_key, 'synthetic', :entity_id, 1, 'synthetic-run', "
            "TIMESTAMPTZ '2026-01-02 03:04:05+00')",
        ),
        (
            "entity_address_plan_bridge",
            "(location_key, entity_type, entity_id, plan_id) "
            "VALUES (:location_key, 'synthetic', :entity_id, 'synthetic-plan')",
        ),
        (
            "entity_address_network_bridge",
            "(location_key, entity_type, entity_id, network_id) "
            "VALUES (:location_key, 'synthetic', :entity_id, 'synthetic-network')",
        ),
        (
            "entity_address_procedure_bridge",
            "(location_key, npi, code_system, code) VALUES (:location_key, 1000000001, 'synthetic', 'procedure')",
        ),
        (
            "entity_address_medication_bridge",
            "(location_key, npi, code_system, code) VALUES (:location_key, 1000000001, 'synthetic', 'medication')",
        ),
        (
            "facility_anchor_npi_candidate",
            "(candidate_id, location_key, facility_anchor_id, source_run_id) "
            "VALUES ('synthetic-candidate', :location_key, 'synthetic-anchor', 'synthetic-run')",
        ),
    )
    for table_name, values_sql in statements:
        await session.execute(
            text(f'INSERT INTO "{schema}"."{table_name}" {values_sql}'),
            {"location_key": location_key, "entity_id": entity_id},
        )


async def _prepare_fixture(database: Database, schema: str):
    """Create incumbent state and return a fully prepared destination receipt."""

    assert database.engine is not None and database.session_factory is not None
    session_factory = database.session_factory
    async with database.engine.begin() as connection:
        await _create_model_family(connection, schema)
        await connection.execute(
            text(
                f'INSERT INTO "{schema}"."entity_address_unified" '
                "(entity_type, entity_id, location_key, checksum, type) "
                "VALUES ('synthetic', 'incumbent', 'live-sentinel', 99, 'primary')"
            )
        )
    async with database.engine.begin() as connection:
        await _seed_support_rows(connection, schema, location_key="live-sentinel", entity_id="incumbent")
    incumbent_oid_by_table = {
        relation_record._mapping["table_name"]: relation_record._mapping["relation_oid"]
        for relation_record in await database.all(
            "SELECT relation.relname AS table_name, relation.oid::bigint AS relation_oid "
            "FROM pg_catalog.pg_class AS relation "
            "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
            "WHERE namespace.nspname=:schema_name AND relation.relkind='r' "
            "AND relation.relname = ANY(:table_names)",
            schema_name=schema,
            table_names=[
                model.__tablename__
                for model in (
                    destination.entity_address_unified.EntityAddressUnified,
                    *destination.entity_address_unified.SUPPORT_TABLE_MODELS,
                )
            ],
        )
    }
    await _create_alias_relations(database, schema, generation=91)
    await _create_geo_dependencies(database, schema)
    await _seed_geo_dependencies(database, schema)
    async with session_factory() as session, session.begin():
        owner = await restore.precreate_entity_address_archive_restore(
            session,
            dataset_id=uuid4(),
            db_schema=schema,
            import_date="20260913",
        )
        await _seed_source_rows(session, owner.schema_name, source_generation=3)
    source_receipt = await _capture_receipt(session_factory, owner.schema_name, "UTC")
    local_alias = await _capture_alias_receipt(session_factory, schema)
    source_alias = replace(local_alias, local_generation=3)
    async with session_factory() as session, session.begin():
        prepared = await destination.prepare_entity_address_archive_destination(
            session,
            owner=owner,
            semantic_receipt=source_receipt,
            source_alias_receipt=source_alias,
            db_schema=schema,
            import_date="20260913",
            source_serving_generation=_source_serving_generation(),
        )
    return prepared, incumbent_oid_by_table


def _runtime_evidence_sql(schema: str) -> str:
    """Build the production evidence expression for the isolated schema."""

    legacy_source_id = geo_projection.legacy_evidence_source_id_sql(
        "address_row",
        schema_name=schema,
    )
    return geo_projection.projected_evidence_level_sql(
        "address_row",
        schema_name=schema,
        legacy_level_sql=geo_projection.evidence_level_from_source_id_sql(legacy_source_id),
    )


def _lineage_receipts(*, changed_link: str):
    """Build a synthetic three-receipt chain with one optional support mutation."""

    models = (
        destination.entity_address_unified.EntityAddressUnified,
        *destination.entity_address_unified.SUPPORT_TABLE_MODELS,
    )
    source_tables = tuple(
        receipt.EntityAddressArchiveTableReceipt(model.__name__, model.__tablename__, "a" * 64, 1, chr(98 + index) * 64)
        for index, model in enumerate(models)
    )
    restored_rows = list(source_tables)
    stage_rows = [replace(table, table_name=f"{table.table_name}_20260913") for table in source_tables]
    if changed_link == "source_to_restored":
        restored_rows[1] = replace(restored_rows[1], row_sha256="e" * 64)
        stage_rows[1] = replace(stage_rows[1], row_sha256="e" * 64)
    elif changed_link == "restored_to_stage":
        stage_rows[1] = replace(stage_rows[1], row_sha256="f" * 64)
    archive_type = receipt.EntityAddressArchiveReceipt
    stage_type = receipt.EntityAddressStageIntegrityReceipt
    return (
        archive_type(source_tables, "1" * 64, "2" * 64, "7" * 64),
        archive_type(tuple(restored_rows), "3" * 64, "4" * 64, "7" * 64),
        stage_type(tuple(stage_rows), "5" * 64, "6" * 64, "7" * 64),
    )


def _substitute_persisted_support_receipt(prepared) -> dict:
    """Return structurally valid metadata with one substituted stage support hash."""

    stored = copy.deepcopy(prepared.as_dict())
    stage_integrity = stored["restored"]["stage_integrity"]
    stage_integrity["tables"][1]["row_sha256"] = "f" * 64
    stage_integrity["content_sha256"] = receipt._content_identity(
        tuple(receipt.EntityAddressArchiveTableReceipt(**table) for table in stage_integrity["tables"]),
        stage_integrity["main_input_sha256"],
    )
    return stored


@pytest.mark.parametrize("changed_link", ["source_to_restored", "restored_to_stage"])
def test_support_receipt_lineage_rejects_substitute_metadata(changed_link: str) -> None:
    """Reject support rows changed before or after the retained remap receipt."""

    source_receipt, restored_receipt, stage_integrity = _lineage_receipts(changed_link=changed_link)
    with pytest.raises(
        destination.EntityAddressSnapshotDestinationError,
        match="support receipt lineage differs",
    ):
        destination._require_receipt_lineage(
            source_receipt,
            restored_receipt,
            stage_integrity,
        )


def test_main_input_lineage_rejects_non_geo_non_alias_change() -> None:
    """Reject a projected stage whose immutable per-row input differs."""

    source_receipt, restored_receipt, stage_integrity = _lineage_receipts(changed_link="none")
    changed_stage = replace(stage_integrity, main_input_sha256="8" * 64)
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="main input lineage differs"):
        destination._require_receipt_lineage(source_receipt, restored_receipt, changed_stage)


async def _assert_query_ready(database: Database, schema: str) -> None:
    """Require active local projection state and production query classifications."""

    assert await database.scalar(f"SELECT {geo_projection.projection_state_available_sql(schema)}") is True
    rows = await database.all(
        f"SELECT location_key, {_runtime_evidence_sql(schema)} AS evidence_level "
        f"FROM {schema}.entity_address_unified AS address_row "
        "WHERE location_key IN ('nppes', 'mrf', 'cms', 'none') ORDER BY location_key"
    )
    assert {row._mapping["location_key"]: row._mapping["evidence_level"] for row in rows} == {
        "cms": "cms_doctors_source_with_nppes_identity_anchor",
        "mrf": "multi_issuer_marketplace_address",
        "none": None,
        "nppes": "nppes_registry_address",
    }
    versions = await database.all(
        f"SELECT location_key, base_address_version FROM {schema}.entity_address_unified ORDER BY location_key"
    )
    version_by_location = {row._mapping["location_key"]: row._mapping["base_address_version"] for row in versions}
    destination_version = destination.entity_address_unified.ALIAS_BASE_ADDRESS_VERSION_PREFIX + "91"
    assert version_by_location == {
        "cms": destination_version,
        "cms-anchor": destination.entity_address_unified.BASE_ADDRESS_VERSION,
        "mrf": destination_version,
        "none": None,
        "nppes": destination_version,
    }


async def _assert_family_identity_and_rows(
    database: Database,
    schema: str,
    expected_oid_by_table: Mapping[str, int],
    *,
    suffix: str,
) -> None:
    """Require every populated family member to retain its expected OID."""

    for table_name, expected_oid in expected_oid_by_table.items():
        actual_table_name = table_name + suffix
        assert (
            await database.scalar(
                "SELECT to_regclass(:relation_name)::oid::bigint",
                relation_name=f"{schema}.{actual_table_name}",
            )
            == expected_oid
        )
        assert await database.scalar(f'SELECT count(*) FROM "{schema}"."{actual_table_name}"') == 1


async def _generation_authority(database: Database, schema: str):
    row = await database.first(
        f"SELECT singleton, local_lineage_id, local_generation, origin_lineage_id, "
        f"origin_generation, published_at, relation_oids FROM {schema}."
        "entity_address_result_generation WHERE singleton IS TRUE"
    )
    return result_generation.validate_entity_address_result_generation_authority(row)


async def _spliced_main_stage(database: Database, prepared) -> dict:
    """Substitute a non-derived main value and recompute the actual stage receipt."""

    assert database.session_factory is not None
    stored = copy.deepcopy(prepared.as_dict())
    db_schema = prepared.restored.db_schema
    _schema, _date, stage_names = restore._stage_plan(
        db_schema=db_schema,
        import_date=prepared.restored.import_date,
    )
    main_stage = stage_names[destination.entity_address_unified.EntityAddressUnified.__tablename__]
    async with database.session_factory() as session, session.begin():
        await session.execute(
            text(
                f'UPDATE "{db_schema}"."{main_stage}" SET first_line = \'SUBSTITUTED STREET\' '
                "WHERE location_key = 'nppes'"
            )
        )
        stage_integrity = await receipt.capture_entity_address_stage_integrity_receipt(
            session,
            schema_name=db_schema,
            stage_table_names=stage_names,
        )
    stored["restored"]["stage_integrity"] = stage_integrity.as_dict()
    return stored


async def _activate(database: Database, prepared_or_stored, *, fail_after_publish: bool) -> None:
    """Activate through the caller-owned transaction, optionally forcing rollback."""

    assert database.session_factory is not None
    stored = (
        copy.deepcopy(prepared_or_stored) if isinstance(prepared_or_stored, Mapping) else prepared_or_stored.as_dict()
    )
    db_schema = stored["restored"]["db_schema"]

    async def verify_local_state() -> None:
        assert destination.db._transaction_binding() is not None

    async def record_adoption() -> None:
        await destination.db.status(f"INSERT INTO {db_schema}.adoption_receipt VALUES ('recorded')")
        if fail_after_publish:
            raise RuntimeError("synthetic receipt failure")

    async with database.session_factory() as session, session.begin():
        await destination.activate_entity_address_archive_destination(
            session,
            stored=stored,
            callbacks=adoption.EntityAddressSnapshotAdoptionCallbacks(
                verify_local_state=verify_local_state,
                record_adoption=record_adoption,
            ),
        )


@pytest.mark.asyncio
async def test_native_destination_adoption_is_query_ready_and_failure_preserves_predecessor(
    monkeypatch: pytest.MonkeyPatch,
):
    """Prove real projection activation and post-swap rollback on native PostgreSQL."""

    async_dsn, _environment = _native_test_connection()
    async with _owned_native_database(async_dsn, monkeypatch) as database:
        await database.status("CREATE EXTENSION IF NOT EXISTS postgis")
        await database.status("CREATE EXTENSION IF NOT EXISTS intarray")
        await database.status("CREATE EXTENSION IF NOT EXISTS btree_gin")
        await database.status('CREATE SCHEMA "tiger"')
        successful_schema = "address_destination_success_" + uuid4().hex
        rollback_schema = "address_destination_rollback_" + uuid4().hex
        successful, successful_incumbent_oid_by_table = await _prepare_fixture(database, successful_schema)
        assert successful.base_version_remap.alias_rows_bound == 3
        assert successful.base_version_remap.alias_rows_rewritten == 3
        assert successful.base_version_remap.null_rows_preserved == 1
        assert successful.base_version_remap.plain_base_rows_preserved == 1
        assert successful.source_semantic_receipt.content_sha256 != (
            successful.restored.semantic_receipt.content_sha256
        )
        assert (
            successful.source_semantic_receipt.main_input_sha256
            == successful.restored.semantic_receipt.main_input_sha256
            == successful.restored.stage_integrity.main_input_sha256
        )
        with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="preparation is invalid"):
            destination._validated_destination_metadata(_substitute_persisted_support_receipt(successful))
        await database.status(f"CREATE TABLE {successful_schema}.adoption_receipt (marker text NOT NULL)")
        await _activate(database, successful, fail_after_publish=False)
        await _assert_query_ready(database, successful_schema)
        await _assert_family_identity_and_rows(
            database,
            successful_schema,
            successful_incumbent_oid_by_table,
            suffix="_old",
        )
        assert await database.scalar(f"SELECT count(*) FROM {successful_schema}.adoption_receipt") == 1
        successful_generation = await _generation_authority(database, successful_schema)
        assert successful_generation.local_generation == 0
        assert successful_generation.serving_generation.as_dict() == _source_serving_generation()
        serving_oids = []
        for table_name in result_generation.RELATION_NAMES:
            serving_oids.append(
                await database.scalar(
                "SELECT to_regclass(:relation_name)::oid::bigint",
                relation_name=f"{successful_schema}.{table_name}",
            )
            )
        assert successful_generation.relation_oids == tuple(serving_oids)

        rolled_back, rollback_incumbent_oid_by_table = await _prepare_fixture(database, rollback_schema)
        await database.status(f"CREATE TABLE {rollback_schema}.adoption_receipt (marker text NOT NULL)")
        spliced = await _spliced_main_stage(database, rolled_back)
        with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="preparation is invalid"):
            await _activate(database, spliced, fail_after_publish=False)
        main_stage = rolled_back.restored.prepared.stage_cls.__tablename__
        await database.status(
            f'UPDATE "{rollback_schema}"."{main_stage}" SET first_line = \'1 TEST STREET\' '
            "WHERE location_key = 'nppes'"
        )
        with pytest.raises(RuntimeError, match="synthetic receipt failure"):
            await _activate(database, rolled_back, fail_after_publish=True)
        await _assert_family_identity_and_rows(
            database,
            rollback_schema,
            rollback_incumbent_oid_by_table,
            suffix="",
        )
        assert await database.scalar(f"SELECT count(*) FROM {rollback_schema}.adoption_receipt") == 0
        rolled_back_generation = await _generation_authority(database, rollback_schema)
        assert rolled_back_generation.local_generation == 0
        assert rolled_back_generation.serving_generation is None
        assert rolled_back_generation.relation_oids is None


@pytest.mark.asyncio
async def test_unsupported_source_alias_version_fails_before_remap() -> None:
    """Reject a source row stamped by any generation other than its receipt."""

    query_result = MagicMock()
    session = MagicMock()
    session.execute = AsyncMock(return_value=query_result)
    counts_by_version = {
        "null_rows": 0,
        "plain_rows": 0,
        "alias_rows": 1,
        "unsupported_rows": 1,
    }
    query_result.mappings.return_value.one.return_value = counts_by_version
    alias = alias_receipt.EntityAddressAliasSemanticReceipt(2, 1, 3, 0, "a" * 64)
    semantic = SimpleNamespace(content_sha256="b" * 64)
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="unsupported or inconsistent"):
        await destination._remap_base_versions(
            session,
            schema_name="synthetic_stage",
            source_alias=alias,
            destination_alias=replace(alias, local_generation=91),
            pre_remap_receipt=semantic,
        )
    assert session.execute.await_count == 1
