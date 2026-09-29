# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual native heaps and declared PostGIS/GIN indexes; no full capacity-bound claim."""

import importlib
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import MetaData

from process import provider_directory_cms_native_layout as layout
from process.provider_directory_cms_preparation import OwnedRelation
from tests.provider_directory_profile_delta_test_support import _delta_database

native = importlib.import_module("process.entity_address_unified")
fhir = importlib.import_module("process.provider_directory_fhir")


async def _owned(database, schema, name):
    """Capture exact physical ownership using the same catalog identity as admission."""
    row = await database.first(
        "SELECT oid::bigint,relpersistence::text AS persistence,pg_total_relation_size(oid)::bigint AS bytes "
        "FROM pg_class WHERE oid=to_regclass(:name)",
        name=f"{schema}.{name}",
    )
    return OwnedRelation(schema, name, row._mapping["oid"], row._mapping["bytes"], row._mapping["persistence"])


async def _create_model(database, schema, model):
    """Create the actual model and every declared index through the native DDL producer."""
    name = model.__tablename__ + "_cms" + uuid4().hex[:20]
    table = model.__table__.to_metadata(MetaData(), schema=schema, name=name)
    async with database.engine.begin() as connection:
        await connection.run_sync(table.create)
    await database.status(f'ALTER TABLE "{schema}"."{name}" SET UNLOGGED')
    return SimpleNamespace(__tablename__=name, __main_table__=model.__tablename__), await _owned(database, schema, name)


async def _create_indexes(database, schema, stage, model):
    """Use exactly the existing builder statements and final geo semantic validator."""
    for _label, statement in native._stage_index_statements(stage, schema, model.__my_additional_indexes__, {}):
        await database.status(statement)
    if model.__tablename__ == "entity_address_unified":
        await native._require_geo_taxonomy_stage_index(stage, schema, {})


@pytest.mark.asyncio
async def test_real_native_family_accepts_declared_gin_gist_and_logging(monkeypatch):
    """Native states pass without weakening the Profile-only B-tree validator."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        monkeypatch.setattr(native, "db", database)
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE", "all")
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", "1")
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES", "0")
        for extension in ("postgis", "intarray", "btree_gin"):
            await database.status(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        for model in layout.ENTITY_ADDRESS_RESULT_MODELS:
            stage, relation = await _create_model(database, schema, model)
            targets = (model.__tablename__,)
            initial = await layout.capture_native_layout(fhir, relation, targets)
            await _create_indexes(database, schema, stage, model)
            indexed = await layout.capture_native_layout(fhir, relation, targets)
            assert indexed.exact_fingerprint != initial.exact_fingerprint
            if model.__tablename__ == "entity_address_unified":
                with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="index_shape_unsupported"):
                    await fhir._provider_directory_profile_relation_storage_fingerprint(
                        relation.oid, expected_persistence="u"
                    )
            await database.status(f'ALTER TABLE "{schema}"."{relation.relation}" SET LOGGED')
            logged = await layout.capture_native_layout(fhir, replace(relation, persistence="p"), targets)
            assert logged.exact_fingerprint != indexed.exact_fingerprint
            assert logged.relation_oid == indexed.relation_oid == relation.oid


@pytest.mark.asyncio
async def test_raw_index_free_heap_and_exact_identity(monkeypatch):
    """Raw heap creation is valid; rename, persistence mismatch and added trigger fail closed."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        name = "entity_address_unified_cms" + uuid4().hex[:20] + "_raw"
        await database.status(native._prepare_raw_stage_sql(schema, name, unlogged=True))
        relation = await _owned(database, schema, name)
        targets = ("entity_address_unified",)
        await layout.capture_native_layout(fhir, relation, targets)
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="storage_shape_unsupported"):
            await layout.capture_native_layout(fhir, replace(relation, persistence="p"), targets)
        await database.status(f"ALTER TABLE {schema}.{name} RENAME TO moved")
        with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
            await layout.capture_native_layout(fhir, relation, targets)
        await database.status(f"ALTER TABLE {schema}.moved RENAME TO {name}")
        await database.status(
            f"CREATE FUNCTION {schema}.unexpected() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$"
        )
        await database.status(
            f"CREATE TRIGGER unexpected AFTER INSERT ON {schema}.{name} FOR EACH STATEMENT EXECUTE FUNCTION {schema}.unexpected()"
        )
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="storage_shape_unsupported"):
            await layout.capture_native_layout(fhir, relation, targets)


@pytest.mark.asyncio
async def test_raw_index_names_remain_distinct_across_native_phases(monkeypatch):
    """Validate actual long-name raw indexes without changing the heap or its rows."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        monkeypatch.setattr(native, "db", database)
        name = "entity_address_unified_cms" + uuid4().hex[:20] + "_raw"
        await database.status(native._prepare_raw_stage_sql(schema, name, unlogged=True))
        await database.status(
            f"INSERT INTO {schema}.{name} (entity_type, entity_id, type, taxonomy_array, plans_network_array, "
            "procedures_array, medications_array, source_priority, checksum) "
            "VALUES ('example', 'one', 'primary', '{}', '{}', '{}', '{}', 1, 1)"
        )
        relation = await _owned(database, schema, name)
        for shards, include_inline_evidence, profile in (
            (2, True, "shard"),
            (2, True, "group"),
            (2, False, "group"),
            (1, False, "group"),
        ):
            monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_RAW_GROUP_INDEX_PROFILE", profile)
            await database.status(
                native._raw_aggregate_group_index_sql(
                    schema, name, aggregate_shards=shards, inline_source_evidence=include_inline_evidence
                )
            )
            await layout.capture_native_layout(fhir, relation, ("entity_address_unified",))
        for column in ("checksum", "location_key"):
            await database.status(
                f"CREATE INDEX {native._stage_index_name(name, column)} ON {schema}.{name} ({column})"
            )
        await layout.capture_native_layout(fhir, relation, ("entity_address_unified",))
        index_names = {
            index_row[0]
            for index_row in await database.all(
                "SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid=:oid", oid=relation.oid
            )
        }
        expected_index_names = {f"{schema}.{index_name}" for index_name in layout._declarations(name, None)}
        assert len(index_names) == 6 and index_names == expected_index_names
        assert await native._raw_alias_integrity_checksum_ranges(schema, name, [(0, 1)], is_raw_stage_reused=True) == [
            (0, 1)
        ]
        for index_name in layout._declarations(name, None):
            await database.status(f"DROP INDEX {schema}.{index_name}")
        await layout.capture_native_layout(fhir, relation, ("entity_address_unified",))
        assert await database.scalar(f"SELECT count(*) FROM {schema}.{name}") == 1


@pytest.mark.asyncio
async def test_inference_indexes_remain_distinct_on_long_native_stage(monkeypatch):
    """Create both real inference indexes without truncation or declaration collisions."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        monkeypatch.setattr(native, "db", database)
        await database.status("CREATE EXTENSION IF NOT EXISTS postgis")
        model = native.EntityAddressUnified
        stage, relation = await _create_model(database, schema, model)
        await native._prepare_inference_stage_indexes(schema, stage.__tablename__)
        await layout.capture_native_layout(fhir, relation, (model.__tablename__,))
        index_names = {
            index_row[0]
            for index_row in await database.all(
                "SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid=:oid AND NOT indisprimary",
                oid=relation.oid,
            )
        }
        expected_index_names = {
            f"{schema}.{native._stage_index_name(stage.__tablename__, 'facility_unresolved_' + suffix)}"
            for suffix in ("identity", "address")
        }
        assert len(index_names) == 2 and index_names == expected_index_names
        assert (
            await database.scalar("SELECT to_regclass(:name)::oid", name=f"{schema}.{stage.__tablename__}")
            == relation.oid
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["unknown", "method", "column", "predicate"])
async def test_native_declared_index_tampering_rejected(monkeypatch, mutation):
    """Known names cannot authorize a different access method, direct key or predicate shape."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        model = layout.ENTITY_ADDRESS_RESULT_MODELS[0]
        stage, relation = await _create_model(database, schema, model)
        name = native._stage_index_name(stage.__tablename__, "npi")
        statements_by_kind = {
            "unknown": f"CREATE INDEX unknown ON {schema}.{stage.__tablename__}(npi)",
            "method": f"CREATE INDEX {name} ON {schema}.{stage.__tablename__} USING hash(npi)",
            "column": f"CREATE INDEX {name} ON {schema}.{stage.__tablename__}(inferred_npi)",
            "predicate": f"CREATE INDEX {name} ON {schema}.{stage.__tablename__}(npi) WHERE npi IS NOT NULL",
        }
        await database.status(statements_by_kind[mutation])
        with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
            await layout.capture_native_layout(fhir, relation, (model.__tablename__,))
