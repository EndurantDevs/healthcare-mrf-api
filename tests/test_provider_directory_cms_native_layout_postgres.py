# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual native heaps and declared PostGIS/GIN indexes; no full capacity-bound claim."""

import importlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.schema import CreateTable

from db.models.provider_directory_entity_identity import (
    ProviderDirectoryEntitySourceBinding,
    ProviderDirectoryOrganizationIdentity,
    ProviderDirectorySiteIdentity,
)
from db.models.system import (
    ProviderDirectoryAPIEndpoint,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryEndpointDataset,
)
from process import provider_directory_cms_native_layout as layout
from process import provider_directory_cms_raw_layout as raw_layout
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_registry_cms_prepared_pair import RegistryCMSRetentionRequest
from process.provider_directory_cms_preparation import NonprofileAdmissionCheck, OwnedRelation, RetainedRawRelation
from tests.provider_directory_cms_capacity_test_support import signed_cms_plan
from tests.provider_directory_profile_delta_test_support import _delta_database
from tests.test_provider_directory_cms_nonprofile_capacity import _producer

native = importlib.import_module("process.entity_address_unified")
fhir = importlib.import_module("process.provider_directory_fhir")

_RAW_ENTITY = ProviderDirectoryEntitySourceBinding.__tablename__
_RAW_TOAST_TABLES = (ProviderDirectoryEndpointDataset.__tablename__, ProviderDirectoryDatasetResource.__tablename__)


async def _owned(database, schema, name):
    """Capture exact physical ownership using the same catalog identity as admission."""
    row = await database.first(
        "SELECT oid::bigint,relpersistence::text AS persistence,pg_total_relation_size(oid)::bigint AS bytes "
        "FROM pg_class WHERE oid=to_regclass(:name)",
        name=f"{schema}.{name}",
    )
    return OwnedRelation(schema, name, row._mapping["oid"], row._mapping["bytes"], row._mapping["persistence"])


async def _create_model(database, schema, model, *, dropped_slot=False):
    """Create the actual model and every declared index through the native DDL producer."""
    name = model.__tablename__ + "_cms" + uuid4().hex[:20]
    table = model.__table__.to_metadata(MetaData(), schema=schema, name=name)
    async with database.engine.begin() as connection:
        if dropped_slot:
            statement = str(CreateTable(table).compile(dialect=connection.dialect))
            prefix, separator, columns = statement.partition("(\n")
            assert separator
            await connection.execute(text(prefix + separator + "discarded_retained_slot integer,\n" + columns))
            await connection.execute(text(f'ALTER TABLE "{schema}"."{name}" DROP COLUMN discarded_retained_slot'))
        else:
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
async def test_retained_like_accepts_dropped_column_slots(monkeypatch):
    """Use real LIKE/index catalogs to retain a rebuilt heap with a dropped attribute slot."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        monkeypatch.setattr(native, "db", database)
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE", "all")
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", "1")
        for extension in ("postgis", "intarray", "btree_gin"):
            await database.status(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        model = next(
            model for model in layout.ENTITY_ADDRESS_RESULT_MODELS if model.__tablename__ == "entity_address_unified"
        )
        stage, source_relation = await _create_model(database, schema, model, dropped_slot=True)
        await _create_indexes(database, schema, stage, model)
        await database.status(f'ALTER TABLE "{schema}"."{source_relation.relation}" SET LOGGED')
        source_relation = await _owned(database, schema, source_relation.relation)
        captured_source = await layout.capture_retained_native_source(fhir, source_relation, (model.__tablename__,))
        await database.status(
            f'CREATE TABLE "{schema}"."{model.__tablename__}" (LIKE "{schema}"."{source_relation.relation}" INCLUDING ALL)'
        )
        clone = await _owned(database, schema, model.__tablename__)
        original = json.loads(captured_source.catalog_json)
        observed, _toast_oid = await layout._observe_catalog(fhir, clone)
        assert {
            entry["attnum"] for entry in original["attributes"] if entry["relation_oid"] == source_relation.oid
        } != {entry["attnum"] for entry in observed["attributes"] if entry["relation_oid"] == clone.oid}
        accepted = await layout.capture_retained_native_layout(fhir, clone, captured_source, (model.__tablename__,))
        assert accepted.relation_oid == clone.oid
        assert accepted.exact_fingerprint == fhir._identity_hash(observed)
        assert layout._structural_catalog(original) == layout._structural_catalog(observed)


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


async def _create_retention_source_models(database, schema):
    """Create the actual source columns and their referenced model tables."""
    metadata = MetaData()
    for model in (
        ProviderDirectoryAPIEndpoint,
        ProviderDirectoryEndpointDataset,
        ProviderDirectoryDatasetResource,
        ProviderDirectoryOrganizationIdentity,
        ProviderDirectorySiteIdentity,
        ProviderDirectoryEntitySourceBinding,
    ):
        model.__table__.to_metadata(metadata, schema=schema)
    async with database.engine.begin() as connection:
        await connection.run_sync(metadata.create_all)


async def _signed_retention_policy(database, schema, capture_id, plan):
    """Build the native DTO over the real pinned source resource heap."""
    source_resource = await _owned(database, schema, ProviderDirectoryDatasetResource.__tablename__)
    source_pin = PinnedFHIRMembershipSource(
        schema,
        "cms-npd",
        "endpoint-one",
        "dataset-one",
        "a" * 64,
        "release-one",
        source_resource.oid,
        "medical/example",
        plan.desired_profile_as_of,
    )
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", "cms-npd", schema, "dataset-one", "producer-one", "edition-one"
    )
    request = RegistryCMSRetentionRequest(
        source_pin, coordinates, plan.selection_proof_id, "b" * 64, "c" * 64, 8 * 1024**2, 32 * 1024**2
    )
    owner_role = await database.scalar("SELECT current_user::text")
    return request.policy(capture_id, owner_role, ("cms_retained_reader",))


@asynccontextmanager
async def _retained_raw_database(monkeypatch):
    """Keep source and raw schema cleanup explicit; use real catalog and WAL reads."""
    async with _delta_database(monkeypatch) as (database, source_schema):
        monkeypatch.setattr(fhir, "db", database)
        await _create_retention_source_models(database, source_schema)
        producer = _producer()
        producer.fhir = fhir
        producer.plan = replace(
            producer.plan,
            reservation_bytes=(("data", 8 * 1024**2), ("temp", 1024**2), ("wal", 64 * 1024**2)),
            logging_wal_upper_bound_bytes=32 * 1024**2,
            cutover_wal_upper_bound_bytes=1024**2,
        )
        capture_id = uuid4()
        raw_schema = "registry_cms_epoch_" + capture_id.hex
        policy = await _signed_retention_policy(database, source_schema, capture_id, producer.plan)
        producer.lease = signed_cms_plan(producer.plan, retention_policy=policy)
        producer.initial_wal_lsn = await database.scalar("SELECT pg_current_wal_insert_lsn()::text")
        try:
            await database.status(f'CREATE SCHEMA "{raw_schema}"')
            for name in (_RAW_ENTITY, *_RAW_TOAST_TABLES):
                await database.status(
                    f'CREATE TABLE "{raw_schema}"."{name}" (LIKE "{source_schema}"."{name}" INCLUDING STORAGE)'
                )
            yield SimpleNamespace(
                database=database, source_schema=source_schema, raw_schema=raw_schema, policy=policy, producer=producer
            )
        finally:
            await database.status(f'DROP SCHEMA IF EXISTS "{raw_schema}" CASCADE')
            assert await database.scalar("SELECT to_regnamespace(:name)", name=raw_schema) is None


async def _create_retained_raw_index(fixture, name, ordinal):
    """Use the creator's exact PK-then-lookup prefix and ordinary native DDL."""
    index_name, keys, primary = raw_layout._raw_profiles(name)[ordinal]
    relation_ref = f'"{fixture.raw_schema}"."{name}"'
    columns = ", ".join(f'"{key}"' for key in keys)
    if primary:
        statement = f'ALTER TABLE {relation_ref} ADD CONSTRAINT "{index_name}" PRIMARY KEY ({columns})'
    else:
        statement = f'CREATE INDEX "{index_name}" ON {relation_ref} ({columns})'
    await fixture.database.status(statement)


async def _retained_raw_check(fixture, name, prefix):
    """Remeasure physical bytes and original OID for every index admission phase."""
    relation = await _owned(fixture.database, fixture.raw_schema, name)
    names = tuple(profile[0] for profile in raw_layout._raw_profiles(name)[:prefix])
    annotation = RetainedRawRelation(
        relation.schema,
        name,
        relation.oid,
        json.dumps(fixture.policy, sort_keys=True, separators=(",", ":")),
        names,
    )
    producer = fixture.producer
    return NonprofileAdmissionCheck(
        "readiness", producer.lease, producer.plan, (relation,), raw_relations=(annotation,)
    )


async def _assert_retained_raw_physical(fixture, check):
    """Exercise the real aggregate checker, including its native raw dispatch."""
    relation = check.relations[0]
    relation_map, _ = await fhir._profile_capacity_relation_row(relation.oid, "p", 0)
    await fixture.producer._assert_physical(check, {"data_tablespace_oid": relation_map["effective_tablespace_oid"]})


@pytest.mark.asyncio
async def test_retained_entity_heap_accepts_only_annotated_native_index_prefix(monkeypatch):
    """A real no-TOAST heap stays rejected by generic Profile admission."""
    async with _retained_raw_database(monkeypatch) as fixture:
        fingerprints = []
        for prefix in range(4):
            if prefix:
                await _create_retained_raw_index(fixture, _RAW_ENTITY, prefix - 1)
            check = await _retained_raw_check(fixture, _RAW_ENTITY, prefix)
            relation = check.relations[0]
            _, toast_oid = await fhir._profile_capacity_relation_row(relation.oid, "p", 0)
            _, indexes, constraints, _ = await fhir._profile_capacity_relation_catalog([relation.oid])
            main_indexes, toast_indexes = fhir._profile_capacity_index_groups(indexes, relation.oid, toast_oid)
            assert toast_oid is None and toast_indexes == []
            assert len(main_indexes) == prefix
            assert all(constraint["relation_oid"] == relation.oid for constraint in constraints)
            if not prefix:
                assert main_indexes == []
                with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="index_shape_unsupported"):
                    await fhir._provider_directory_profile_relation_storage_fingerprint(
                        relation.oid, expected_persistence="p"
                    )
            await _assert_retained_raw_physical(fixture, check)
            observed = await raw_layout.capture_retained_raw_layout(
                fhir, relation, fixture.policy, check.raw_relations[0].index_names
            )
            assert observed.relation_oid == relation.oid
            fingerprints.append(observed.exact_fingerprint)
        assert len(set(fingerprints)) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("name", _RAW_TOAST_TABLES)
async def test_retained_toast_heaps_accept_native_catalog_and_primary_key(monkeypatch, name):
    """Native TOAST columns, indexes and PG NOT NULL rows remain observable."""
    async with _retained_raw_database(monkeypatch) as fixture:
        fingerprints = []
        for prefix in (0, 1):
            if prefix:
                await _create_retained_raw_index(fixture, name, 0)
            check = await _retained_raw_check(fixture, name, prefix)
            relation = check.relations[0]
            _, toast_oid = await fhir._profile_capacity_relation_row(relation.oid, "p", 0)
            assert type(toast_oid) is int and toast_oid > 0
            attributes, indexes, constraints, _ = await fhir._profile_capacity_relation_catalog(
                [relation.oid, toast_oid]
            )
            main_indexes, toast_indexes = fhir._profile_capacity_index_groups(indexes, relation.oid, toast_oid)
            assert len(main_indexes) == prefix and len(toast_indexes) == 1
            assert [attribute["attname"] for attribute in attributes if attribute["relation_oid"] == toast_oid] == [
                "chunk_id",
                "chunk_seq",
                "chunk_data",
            ]
            assert all(constraint["constraint_type"] in {"n", "p"} for constraint in constraints)
            await _assert_retained_raw_physical(fixture, check)
            observed = await raw_layout.capture_retained_raw_layout(
                fhir, relation, fixture.policy, check.raw_relations[0].index_names
            )
            fingerprints.append(observed.exact_fingerprint)
        assert len(set(fingerprints)) == 2


async def _mutate_retained_raw_check(fixture, mutation):
    """Change actual raw DDL or signed annotation coordinates before admission."""
    check = await _retained_raw_check(fixture, _RAW_ENTITY, 0)
    relation = check.relations[0]
    annotation = check.raw_relations[0]
    relation_ref = f'"{fixture.raw_schema}"."{_RAW_ENTITY}"'
    if mutation == "schema":
        relation = await _owned(fixture.database, fixture.source_schema, _RAW_ENTITY)
        annotation = replace(annotation, schema=relation.schema, oid=relation.oid)
    if mutation == "policy":
        changed_policy_dict = {**fixture.policy, "expected_metadata_sha256": "d" * 64}
        annotation = replace(
            annotation, policy_json=json.dumps(changed_policy_dict, sort_keys=True, separators=(",", ":"))
        )
    if mutation == "no_annotation":
        return replace(check, raw_relations=())
    if mutation == "replaced_oid":
        await fixture.database.status(f"DROP TABLE {relation_ref}")
        await fixture.database.status(
            f'CREATE TABLE {relation_ref} (LIKE "{fixture.source_schema}"."{_RAW_ENTITY}" INCLUDING STORAGE)'
        )
        relation = await _owned(fixture.database, fixture.raw_schema, _RAW_ENTITY)
        assert relation.oid != annotation.oid
    if mutation == "wrong_key":
        await _create_retained_raw_index(fixture, _RAW_ENTITY, 0)
        await fixture.database.status(f"CREATE INDEX cms_epoch_entity_site ON {relation_ref} (organization_id)")
        check = await _retained_raw_check(fixture, _RAW_ENTITY, 2)
        relation, annotation = check.relations[0], check.raw_relations[0]
    if mutation == "premature_index":
        await _create_retained_raw_index(fixture, _RAW_ENTITY, 0)
        await _create_retained_raw_index(fixture, _RAW_ENTITY, 1)
        check = await _retained_raw_check(fixture, _RAW_ENTITY, 1)
        relation, annotation = check.relations[0], check.raw_relations[0]
    if mutation == "missing_index":
        await _create_retained_raw_index(fixture, _RAW_ENTITY, 0)
        check = await _retained_raw_check(fixture, _RAW_ENTITY, 2)
        relation, annotation = check.relations[0], check.raw_relations[0]
    if mutation == "unknown_index":
        await fixture.database.status(f"CREATE INDEX unexpected ON {relation_ref} (site_id)")
        relation = await _owned(fixture.database, fixture.raw_schema, _RAW_ENTITY)
    return replace(check, relations=(relation,), raw_relations=(annotation,))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "schema",
        "policy",
        "no_annotation",
        "replaced_oid",
        "wrong_key",
        "premature_index",
        "missing_index",
        "unknown_index",
    ],
)
async def test_retained_raw_annotation_and_physical_tampering_are_rejected(monkeypatch, mutation):
    """A signed policy cannot authorize substituted identity or an unobserved index phase."""
    async with _retained_raw_database(monkeypatch) as fixture:
        check = await _mutate_retained_raw_check(fixture, mutation)
        expected_error = {
            "policy": "raw_identity_changed",
            "replaced_oid": "raw_identity_changed",
            "no_annotation": "index_shape_unsupported",
        }.get(mutation, "raw_storage_shape_unsupported")
        with pytest.raises(RuntimeError, match=expected_error):
            await _assert_retained_raw_physical(fixture, check)
