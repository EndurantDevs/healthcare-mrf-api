# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual native heaps and declared PostGIS/GIN indexes; no full capacity-bound claim."""

import asyncio
import importlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.schema import CreateTable

from db.connection import ConnectionProxy
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
from process import entity_address_snapshot_receipt as address_receipt
from process import entity_address_snapshot_source as address_source
from process import network_registry_cms_prepared_address as address_writer
from process import provider_directory_cms_native_layout as layout
from process import provider_directory_cms_raw_layout as raw_layout
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_address_capture import NativeReceiptSession
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_registry_cms_prepared_pair import RegistryCMSRetentionRequest
from process.provider_directory_cms_preparation import NonprofileAdmissionCheck, OwnedRelation, RetainedRawRelation
from tests.provider_directory_cms_capacity_test_support import signed_cms_plan
from tests.provider_directory_profile_delta_test_support import _delta_database
from tests.provider_directory_source_postgres_support import source_component_database
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


async def _assert_provisioned_payload_columns(database, policy, raw_schema, root):
    """A real logical parent supplies only the original LIKE column contract."""
    source_ref = f'"mrf"."{root}"'
    catalog = await database.first(
        "SELECT oid::bigint,relkind::text,relpersistence::text FROM pg_class WHERE oid=to_regclass(:name)",
        name=source_ref,
    )
    assert tuple(catalog)[1:] == ("p", "p")
    if root == raw_layout.RESOURCE:
        assert catalog._mapping["oid"] == policy["source_pin"]["resource_table_oid"]
    await database.status(f'CREATE TABLE "{raw_schema}"."{root}" (LIKE {source_ref} INCLUDING STORAGE)')
    relation = await _owned(database, raw_schema, root)
    observed = await raw_layout.capture_retained_raw_layout(fhir, relation, policy, (), database=database)
    assert observed.relation_oid == relation.oid
    if root == raw_layout.RESOURCE:
        wrong_policy_by_field = {**policy, "source_pin": {**policy["source_pin"], "resource_table_oid": catalog[0] + 1}}
        with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
            await raw_layout.capture_retained_raw_layout(fhir, relation, wrong_policy_by_field, (), database=database)
    await database.status(f'ALTER TABLE "{raw_schema}"."{root}" ADD COLUMN unexpected integer')
    with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
        await raw_layout.capture_retained_raw_layout(fhir, relation, policy, (), database=database)


@pytest.mark.asyncio
async def test_retained_raw_columns_accept_only_exact_provisioned_payload_parents(monkeypatch):
    """Real Source parents supply column ABI with exact OID and shape checks."""
    async with source_component_database(monkeypatch) as component, component.admin_engine.begin() as connection:
        database = ConnectionProxy(None, connection, None)
        capture_id = uuid4()
        raw_schema = "registry_cms_epoch_" + capture_id.hex
        policy = await _signed_retention_policy(database, "mrf", capture_id, _producer().plan)
        await database.status(f'CREATE SCHEMA "{raw_schema}"')
        try:
            for root in (raw_layout.RESOURCE, raw_layout.WITNESS, raw_layout.RELATIONSHIP):
                await _assert_provisioned_payload_columns(database, policy, raw_schema, root)
        finally:
            await database.status(f'DROP SCHEMA "{raw_schema}" CASCADE')
        assert await database.scalar("SELECT to_regnamespace(:name)", name=raw_schema) is None


@pytest.mark.asyncio
async def test_retained_raw_columns_reject_a_partitioned_nonpayload_source(monkeypatch):
    """Matching column shape cannot broaden the fixed three-parent allowance."""
    async with _retained_raw_database(monkeypatch) as fixture:
        source_ref = f'"{fixture.source_schema}"."{_RAW_ENTITY}"'
        history_ref = f'"{fixture.source_schema}"."column_history"'
        await fixture.database.status(f'ALTER TABLE {source_ref} RENAME TO "column_history"')
        await fixture.database.status(
            f"CREATE TABLE {source_ref} (LIKE {history_ref} INCLUDING STORAGE) PARTITION BY LIST (source_id)"
        )
        check = await _retained_raw_check(fixture, _RAW_ENTITY, 0)
        with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
            await raw_layout.capture_retained_raw_layout(fhir, check.relations[0], fixture.policy, ())


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


def _catalog_value_types(value):
    if type(value) is dict:
        return {key: _catalog_value_types(item) for key, item in value.items()}
    if type(value) in (tuple, list):
        return type(value).__name__, [_catalog_value_types(item) for item in value]
    return type(value).__name__


async def _original_observed_catalog(fhir, relation):
    """Keep the previous complete catalog read as the native parity reference."""
    relation_map, toast_oid = await fhir._profile_capacity_relation_row(relation.oid, relation.persistence, 0)
    if (
        relation_map["relation_oid"] != relation.oid
        or relation_map["schema_name"] != relation.schema
        or relation_map["relation_name"] != relation.relation
    ):
        layout._reject()
    attributes, indexes, constraints, triggers = await fhir._profile_capacity_relation_catalog(
        [relation.oid] + ([toast_oid] if toast_oid else [])
    )
    if triggers or any(
        constraint["constraint_type"] not in ("p", "n")
        or constraint["condeferrable"]
        or constraint["condeferred"]
        or not constraint["convalidated"]
        for constraint in constraints
    ):
        layout._reject()
    await layout._index_physical_details(fhir, indexes)
    catalog_by_field = {
        "contract": "cms-native-observed-layout.v1",
        "database_oid": await fhir.db.scalar("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"),
        "relation": relation_map,
        "attributes": attributes,
        "constraints": constraints,
        "triggers": triggers,
        "indexes": [
            {key: field_value for key, field_value in index.items() if key != "main_bytes"} for index in indexes
        ],
    }
    return catalog_by_field, toast_oid


@pytest.mark.asyncio
async def test_native_catalog_bundle_matches_original_heap_and_toast(monkeypatch):
    """The batched CMS read preserves every original catalog value, type and row order."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        await database.status(f'CREATE TABLE "{schema}".catalog_plain (id bigint NOT NULL)')
        await database.status(
            f'CREATE TABLE "{schema}".catalog_toasted (id bigint PRIMARY KEY, payload text NOT NULL, labels text[], position point)'
        )
        await database.status(f'CREATE INDEX catalog_labels ON "{schema}".catalog_toasted USING gin(labels)')
        await database.status(
            f'CREATE INDEX catalog_expression ON "{schema}".catalog_toasted (lower(payload)) WHERE id>0'
        )
        await database.status(f'CREATE INDEX catalog_position ON "{schema}".catalog_toasted USING gist(position)')
        await database.status(f'ALTER TABLE "{schema}".catalog_toasted ADD COLUMN discarded integer')
        await database.status(f'ALTER TABLE "{schema}".catalog_toasted DROP COLUMN discarded')
        for name in ("catalog_plain", "catalog_toasted"):
            relation = await _owned(database, schema, name)
            original_catalog, original_toast = await _original_observed_catalog(fhir, relation)
            observed_catalog, observed_toast = await layout._observe_catalog(fhir, relation)
            assert observed_toast == original_toast and observed_catalog == original_catalog
            assert _catalog_value_types(observed_catalog) == _catalog_value_types(original_catalog)
            assert layout._canonical_catalog(observed_catalog) == layout._canonical_catalog(original_catalog)
            assert fhir._identity_hash(observed_catalog) == fhir._identity_hash(original_catalog)
            assert fhir._profile_capacity_tablespaces(
                observed_catalog["relation"], observed_catalog["indexes"], observed_toast
            ) == fhir._profile_capacity_tablespaces(
                original_catalog["relation"], original_catalog["indexes"], original_toast
            )
            _row, toast_oid = await fhir._profile_capacity_relation_row(relation.oid, "p", 0)
            relation_oids = [relation.oid] + ([toast_oid] if toast_oid else [])
            for selected in ([], list(reversed(relation_oids))):
                original = await fhir._profile_capacity_relation_catalog(selected)
                await layout._index_physical_details(fhir, original[1])
                observed_with_database = await layout._native_catalog_rows(fhir, selected)
                observed = observed_with_database[:4]
                assert observed_with_database[4] == original_catalog["database_oid"]
                assert observed == original
                assert _catalog_value_types(observed) == _catalog_value_types(original)
                assert layout._canonical_catalog(observed) == layout._canonical_catalog(original)
                if not selected:
                    assert observed == ([], [], [], [])
            if name == "catalog_toasted":
                assert toast_oid is not None
                assert {entry["index_am"] for entry in observed[1]} == {"btree", "gin", "gist"}
                assert any(entry["index_predicate"] and entry["index_expressions"] for entry in observed[1])
            else:
                assert toast_oid is None and observed[1] == observed[3] == []


async def _retained_recipe_source(database, schema, model, hot_history):
    stage, relation = await _create_model(database, schema, model)
    async with database.transaction():
        if hot_history:
            await database.status(
                f'INSERT INTO "{schema}"."{relation.relation}" '
                "(entity_type,entity_id,location_key,npi,checksum,type) "
                "VALUES ('practitioner','synthetic','synthetic-location',1000000004,1,'primary')"
            )
            await database.status(f'UPDATE "{schema}"."{relation.relation}" SET npi=1000000491')
        await _create_indexes(database, schema, stage, model)
    await database.status(f'ALTER TABLE "{schema}"."{relation.relation}" SET LOGGED')
    relation = await _owned(database, schema, relation.relation)
    async with database.transaction():
        await database.status("SET LOCAL search_path=pg_catalog")
        captured = await layout.capture_retained_native_source(fhir, relation, (model.__tablename__,))
    return stage, relation, captured


async def _retained_recipe_heap(session, database, schema, model, relation, original):
    columns = await address_receipt._catalog_columns(session, relation.oid)
    attributes_by_name = {row["attname"]: row for row in original["attributes"] if row["relation_oid"] == relation.oid}
    for column in columns:
        column["storage"] = attributes_by_name[column["attname"]]["attstorage"]
        column["compression"] = attributes_by_name[column["attname"]]["attcompression"]
    await session.execute(text(address_writer._source_heap_sql(schema, model.__tablename__, columns)))
    for statement in address_writer._source_column_options(schema, model.__tablename__, columns):
        await session.execute(text(statement))
    await address_writer._preserve_prepared_storage_options(session, schema, model.__tablename__, relation.oid)
    return await _owned(database, schema, model.__tablename__)


async def _retain_every_index_statement(session, database, schema, source_stage, relation, clone, original):
    recipes = await address_source._legacy_source_indexes(session, {"relation_oid": relation.oid})
    statement_count = 0
    for recipe in recipes:
        for statement in address_writer._source_index_statements(
            recipe, schema=schema, table="entity_address_unified", source_stage=source_stage
        ):
            await session.execute(text(statement))
            actual, _ = await layout._observe_catalog(fhir, clone)
            try:
                layout._retained_phase_structure(original, actual, "indexing")
            except RuntimeError as error:
                raise AssertionError((recipe, statement)) from error
            statement_count += 1
    assert statement_count == len(recipes) + 1  # The primary key attaches the same created index.
    assert len(recipes) == 1 + len(native.EntityAddressUnified.__my_additional_indexes__)
    assert layout._structural_catalog(original) == layout._structural_catalog(actual)
    assert await address_receipt._catalog_indexes(session, relation.oid) == await address_receipt._catalog_indexes(
        session, clone.oid
    )
    assert await address_receipt._schema_identity(
        session, relation.oid, relation.schema, "entity_address_unified"
    ) == await address_receipt._schema_identity(session, clone.oid, clone.schema, "entity_address_unified")
    return actual


def _refuse_retained_index_mutations(original, actual, clone):
    for field, altered in (("indisunique", True), ("indclass", "0"), ("indkey", "0"), ("predicate", "false")):
        changed = json.loads(json.dumps(actual))
        index = next(
            entry for entry in changed["indexes"] if entry["relation_oid"] == clone.oid and not entry["indisprimary"]
        )
        index[field] = altered
        with pytest.raises(RuntimeError, match="native_storage_shape_unsupported"):
            layout._retained_phase_structure(original, changed, "indexing")


@pytest.mark.asyncio
@pytest.mark.parametrize("hot_history", [False, True])
@pytest.mark.parametrize("caller_search_path", ["pg_catalog", "public,pg_catalog"])
async def test_retained_unified_native_recipes_preserve_every_index_predicate(
    monkeypatch, hot_history, caller_search_path
):
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        monkeypatch.setattr(native, "db", database)
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE", "all")
        for extension in ("postgis", "intarray", "btree_gin"):
            await database.status("CREATE EXTENSION IF NOT EXISTS " + extension)
        model = native.EntityAddressUnified
        stage, relation, captured = await _retained_recipe_source(database, schema, model, hot_history)
        original = json.loads(captured.catalog_json)
        clone_schema = "retained_index_" + uuid4().hex
        try:
            await database.status(f'CREATE SCHEMA "{clone_schema}"')
            async with database.transaction() as session:
                await session.execute(text("SET LOCAL search_path=pg_catalog"))
                clone = await _retained_recipe_heap(session, database, clone_schema, model, relation, original)
                actual = await _retain_every_index_statement(
                    session, database, clone_schema, stage.__tablename__, relation, clone, original
                )
                await session.execute(text("SELECT set_config('search_path',:path,true)"), {"path": caller_search_path})
                same_heap, _ = await layout._observe_catalog(fhir, relation)
                assert same_heap == original
                await layout.capture_retained_native_layout(fhir, clone, captured, (model.__tablename__,))
                assert await database.scalar("SHOW search_path") == caller_search_path
                _refuse_retained_index_mutations(original, actual, clone)
                await _reject_changed_retained_expression(session, clone, captured, actual)
                assert await database.scalar("SHOW search_path") == caller_search_path
        finally:
            await database.status(f'DROP SCHEMA IF EXISTS "{clone_schema}" CASCADE')


async def _reject_changed_retained_expression(session, clone, captured, catalog):
    """Physical GIST identity cannot conceal a genuinely changed stored expression."""
    index = next(
        entry
        for entry in catalog["indexes"]
        if entry["relation_oid"] == clone.oid
        and entry["indnkeyatts"] == 1
        and entry["index_expressions"].startswith("public.geography(")
    )
    await session.execute(text(f'DROP INDEX "{clone.schema}"."{index["index_name"]}"'))
    await session.execute(
        text(
            f'CREATE INDEX "{index["index_name"]}" ON "{clone.schema}"."{clone.relation}" '
            "USING gist (public.geography(public.st_makepoint(long::double precision + 1,lat::double precision))) "
            "WHERE " + index["index_predicate"]
        )
    )
    with pytest.raises(RuntimeError, match="native_storage_shape_unsupported"):
        await layout.capture_retained_native_layout(fhir, clone, captured, (clone.relation,))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["sql_error", "cancel"])
async def test_catalog_path_failure_restores_original_readonly_transaction(monkeypatch, failure):
    async with _delta_database(monkeypatch) as (database, _schema):
        async with database.transaction() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL search_path=public,pg_catalog"))
            caller_search_path = await session.scalar(text("SHOW search_path"))
            driver = await address_writer.native_driver(session)
            backend = driver.get_server_pid()
            if failure == "sql_error":
                injected = (
                    "SELECT 0::bigint AS relation_oid,1 AS attnum,"
                    "1/(length(pg_catalog.current_setting('search_path'))-10) AS forced_failure"
                )
                expected = asyncpg.DivisionByZeroError
            else:
                injected = (
                    "SELECT 0::bigint AS relation_oid,1 AS attnum,"
                    "CASE WHEN pg_catalog.current_setting('search_path')='pg_catalog' "
                    "THEN pg_catalog.pg_sleep(10) ELSE pg_catalog.pg_sleep(0) END AS delayed"
                )
                expected = TimeoutError
            monkeypatch.setattr(fhir, "_PROFILE_CAPACITY_ATTRIBUTE_SQL", injected)
            with pytest.raises(expected):
                async with driver.transaction(), asyncio.timeout(0.2):
                    await NativeReceiptSession(driver).execute(
                        text(layout._native_catalog_statement(fhir)), {"relation_oids": []}
                    )
            assert driver.get_server_pid() == backend and driver.is_in_transaction()
            assert await session.scalar(text("SHOW search_path")) == caller_search_path
            assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            assert await session.scalar(text("SELECT 1")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("visible_schema", [False, True])
@pytest.mark.parametrize("mutation", [None, "default", "ownership"])
async def test_retained_evidence_sequence_keeps_exact_binding_across_paths(monkeypatch, visible_schema, mutation):
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        monkeypatch.setattr(native, "db", database)
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE", "all")
        model = native.EntityAddressEvidence
        stage, source_relation = await _create_model(database, schema, model)
        await _create_indexes(database, schema, stage, model)
        await database.status(f'ALTER TABLE "{schema}"."{source_relation.relation}" SET LOGGED')
        source_relation = await _owned(database, schema, source_relation.relation)
        captured = await layout.capture_retained_native_source(fhir, source_relation, (model.__tablename__,))
        async with database.transaction() as session:
            await session.execute(
                text(
                    f'CREATE TABLE "{schema}"."{model.__tablename__}" '
                    f'(LIKE "{schema}"."{source_relation.relation}" INCLUDING ALL)'
                )
            )
            await address_source._clone_entity_address_evidence_sequence(session, stage_schema=schema)
            clone = await _owned(database, schema, model.__tablename__)
            path = f'"{schema}",public,pg_catalog' if visible_schema else "pg_catalog"
            await session.execute(text("SELECT set_config('search_path',:path,true)"), {"path": path})
            caller_path = await database.scalar("SHOW search_path")
            if visible_schema:
                raw_default = await database.scalar(
                    "SELECT pg_get_expr(adbin,adrelid,true) FROM pg_attrdef "
                    "JOIN pg_attribute ON attrelid=adrelid AND attnum=adnum "
                    "WHERE adrelid=CAST(:oid AS oid) AND attname='evidence_id'",
                    oid=source_relation.oid,
                )
                assert raw_default != json.loads(captured.catalog_json)["evidence_sequence"]["default_expression"]
            same_source = await layout.capture_retained_native_source(fhir, source_relation, (model.__tablename__,))
            assert same_source.catalog_json == captured.catalog_json
            await layout.capture_retained_native_layout(fhir, clone, captured, (model.__tablename__,))
            assert await database.scalar("SHOW search_path") == caller_path
            sequence = f'"{schema}"."{model.__tablename__}_evidence_id_seq"'
            if mutation == "default":
                await session.execute(
                    text(
                        f'ALTER TABLE "{schema}"."{model.__tablename__}" ALTER COLUMN evidence_id '
                        f"SET DEFAULT nextval('{sequence}'::regclass) + 1"
                    )
                )
            elif mutation == "ownership":
                await session.execute(text(f"ALTER SEQUENCE {sequence} OWNED BY NONE"))
            if mutation is not None:
                with pytest.raises(RuntimeError, match="native_storage_shape_unsupported"):
                    await layout.capture_retained_native_layout(fhir, clone, captured, (model.__tablename__,))
            assert await database.scalar("SHOW search_path") == caller_path
            assert await database.scalar("SELECT 1") == 1


@pytest.mark.asyncio
async def test_native_catalog_batch_matches_each_heap_and_toast_then_refuses_rename(monkeypatch):
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        await database.status("CREATE EXTENSION IF NOT EXISTS postgis")
        members = []
        for model in layout.ENTITY_ADDRESS_RESULT_MODELS[:3]:
            _stage, relation = await _create_model(database, schema, model)
            members.append(relation)
        async with database.transaction():
            individual_by_identity = {
                (relation.schema, relation.relation, relation.oid): await layout._observe_catalog(fhir, relation)
                for relation in members
            }
            calls = []
            original_first, original_all = database.first, database.all

            async def first(statement, **params):
                calls.append("first")
                return await original_first(statement, **params)

            async def all_rows(statement, **params):
                calls.append("all")
                return await original_all(statement, **params)

            with monkeypatch.context() as scoped:
                scoped.setattr(database, "first", first)
                scoped.setattr(database, "all", all_rows)
                captured = await layout.observe_native_catalog_batch(fhir, members, database=database)
            assert calls == ["all", "first"]
            assert captured == individual_by_identity
            assert any(toast_oid is not None for _catalog, toast_oid in captured.values())
            for relation in members:
                identity = (relation.schema, relation.relation, relation.oid)
                assert layout._observed_layout(fhir, relation, *captured[identity]) == layout._observed_layout(
                    fhir, relation, *individual_by_identity[identity]
                )
            changed = members[0]
            await database.status(f'ALTER TABLE "{schema}"."{changed.relation}" RENAME TO changed_native_name')
            with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
                await layout.observe_native_catalog_batch(fhir, members, database=database)
