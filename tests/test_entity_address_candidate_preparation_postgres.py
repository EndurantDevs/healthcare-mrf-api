# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact candidate reads and prepared address cutover under an outer transaction."""

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_support import (
    _create_cms_address_table,
    _create_geo_reference_tables,
    _create_mrf_address_table,
)
from tests.test_entity_address_result_generation_postgres import _run_migration
from tests.test_entity_address_unified_no_live_mutation import _mock_shutdown_dependencies, _shutdown_context
from tests.test_entity_address_unified_publication_db import _prepare_live_and_stage, _StageTable, _temporary_schema

native = importlib.import_module("process.entity_address_unified")
preparation = importlib.import_module("process.entity_address_candidate_preparation")


def _inputs():
    return preparation.ProviderDirectoryAddressPreparationInput(
        dataset_pins=(
            preparation.ProviderDirectoryAddressDatasetPin("directory", "endpoint", "candidate", "b" * 64, "new-run"),
            preparation.ProviderDirectoryAddressDatasetPin(
                "other", "other-endpoint", "retained", "c" * 64, "other-run"
            ),
        ),
        overlay_table="prepared_overlay",
        address_alias_generation=0,
    )


@pytest.mark.parametrize(
    "changed",
    [
        {"overlay_table": "provider_directory_address_overlay"},
        {"overlay_table": "unsafe;table"},
        {"address_alias_generation": True},
        {"dataset_pins": (_inputs().dataset_pins[0], _inputs().dataset_pins[0])},
        {"relation_overrides": (("provider_directory_source", "prepared_source"),)},
    ],
)
def test_preparation_rejects_unsafe_or_ambiguous_inputs(changed):
    with pytest.raises(ValueError):
        preparation.validate_preparation_input(replace(_inputs(), **changed))


async def test_internal_scope_requires_indexes_and_restores_after_failure(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES", "true")
    monkeypatch.setattr(native.db, "_transaction_binding", lambda: None)
    monkeypatch.setattr(preparation, "_pinned_named_editions", AsyncMock(return_value=()))

    async def fail_build(_ctx, task):
        assert preparation.current() == _inputs()
        assert task["publish"] is True
        assert native._stage_index_profile() == "all"
        raise RuntimeError("preparation interrupted")

    monkeypatch.setattr(native, "process_entity_address_unified_data", fail_build)
    monkeypatch.setattr(native, "publish_entity_address_unified_generation", AsyncMock())
    with pytest.raises(RuntimeError, match="preparation interrupted"):
        await preparation.prepare_provider_directory_entity_address({}, {}, preparation_input=_inputs())
    assert preparation.current() is None
    assert native._stage_index_profile() == "none"
    native.publish_entity_address_unified_generation.assert_not_awaited()


async def test_candidate_finalization_stops_after_validation(monkeypatch):
    events = []
    _mock_shutdown_dependencies(monkeypatch, events)
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_PUBLISH_VALIDATION", "true")
    context = _shutdown_context(refresh_mode=native.ENTITY_ADDRESS_REFRESH_MODE_FULL)
    expected_prepared = object()

    async def record_indexes(*_args, **_kwargs):
        events.append(("indexes", "ready"))

    async def close_writes(*_args):
        events.append(("freeze", "ready"))

    async def finalize(*_args, **kwargs):
        assert kwargs["context"]["publish_validation_deferred"] is False
        events.append(("prepared", "ready"))
        return expected_prepared

    monkeypatch.setattr(native, "_create_stage_indexes", record_indexes)
    monkeypatch.setattr(preparation, "freeze_ordinary_prepared_stages", close_writes)
    monkeypatch.setattr(preparation, "prepare_finalized_generation", finalize)
    token = preparation._PREPARATION.set(_inputs())
    try:
        assert await native.publish_entity_address_unified_generation(context, prepare_only=True) is expected_prepared
    finally:
        preparation._PREPARATION.reset(token)
    kinds = [kind for kind, _detail in events]
    assert (
        kinds.index("validate_geo")
        < kinds.index("indexes")
        < kinds.index("freeze")
        < kinds.index("validate")
        < kinds.index("prepared")
    )
    assert "publish" not in kinds and "status" not in kinds and "ddl" not in kinds


async def test_candidate_indexes_cannot_skip_missing_postgis(monkeypatch):
    monkeypatch.setattr(native, "_run_sql_phase", AsyncMock(side_effect=RuntimeError("type geography does not exist")))
    token = preparation._PREPARATION.set(_inputs())
    try:
        with pytest.raises(RuntimeError, match="geography"):
            await native._build_stage_index("geo_idx", "CREATE INDEX", {}, [])
    finally:
        preparation._PREPARATION.reset(token)


async def _candidate_tables(database, schema):
    statements = [
        "CREATE TABLE {schema}.provider_directory_source (source_id varchar, endpoint_id varchar)",
        "INSERT INTO {schema}.provider_directory_source VALUES ('directory','endpoint'), ('other','other-endpoint')",
        "CREATE TABLE {schema}.provider_directory_endpoint_dataset (dataset_id varchar, endpoint_id varchar, "
        "dataset_hash varchar, acquisition_root_run_id varchar, import_run_id varchar, status varchar, "
        "is_current boolean, validated_at timestamp, published_at timestamp, superseded_at timestamp, "
        "publication_metadata_json json, publication_metadata_summary_json jsonb)",
        "CREATE TABLE {schema}.provider_directory_dataset_resource (dataset_id varchar, resource_type varchar, resource_id varchar)",
        "INSERT INTO {schema}.provider_directory_dataset_resource VALUES "
        "('incumbent','Location','old'), ('candidate','Location','new'), ('retained','Location','other')",
        "CREATE TABLE {schema}.provider_directory_address_overlay "
        "(source_id varchar, last_seen_run_id varchar, resource_type varchar, resource_id varchar, npi bigint)",
        "CREATE TABLE {schema}.prepared_overlay (LIKE {schema}.provider_directory_address_overlay)",
        "INSERT INTO {schema}.provider_directory_address_overlay VALUES ('directory','old-run','Location','old',1000000000)",
        "INSERT INTO {schema}.prepared_overlay VALUES ('directory','new-run','Location','new',1000000000), "
        "('directory','new-run','Location','unbound',1000000000), ('other','other-run','Location','other',1000000001)",
        "CREATE INDEX prepared_scope_idx ON {schema}.prepared_overlay (source_id,last_seen_run_id,resource_type,resource_id)",
    ]
    for statement in statements:
        await database.status(statement.format(schema=schema))
    await _candidate_datasets(database, schema)
    receipt_table = native.address_alias_sql.ADDRESS_ALIAS_ARTIFACT_STATE_TABLE
    await database.status(
        f"CREATE TABLE {schema}.{receipt_table} (artifact_name varchar(128) PRIMARY KEY,"
        "generation bigint NOT NULL DEFAULT 0,updated_at timestamptz NOT NULL DEFAULT now(),CHECK (generation>=0))"
    )
    await database.status(
        f"INSERT INTO {schema}.{receipt_table}(artifact_name,generation) VALUES ('provider_directory_address_overlay',0)"
    )
    return _inputs()


async def _candidate_datasets(database, schema):
    for dataset_id, endpoint, source_id, digest, root_run, status, current in (
        ("incumbent", "endpoint", "directory", "a" * 64, "old-run", "published", True),
        ("candidate", "endpoint", "directory", "b" * 64, "new-run", "validated", False),
        ("retained", "other-endpoint", "other", "c" * 64, "other-run", "published", True),
    ):
        # Only the candidate is sealed with a summary; the other datasets answer from their raw document.
        await database.status(
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset VALUES "
            "(:dataset,:endpoint,:digest,:root,:root,:status,:current,now(),"
            "CASE WHEN :current THEN now() ELSE NULL END,NULL,CAST(:metadata AS json),CAST(:summary AS jsonb))",
            dataset=dataset_id,
            endpoint=endpoint,
            digest=digest,
            root=root_run,
            status=status,
            current=current,
            metadata=(metadata := '{"source_ids":["' + source_id + '"]}'),
            summary=metadata if dataset_id == "candidate" else None,
        )


async def _overlay_datasets(database, schema):
    statement = native._provider_directory_current_overlay_ctes_sql(schema)
    return [
        row[0] for row in await database.all(statement + " SELECT dataset_id FROM current_overlay ORDER BY dataset_id")
    ]


async def test_candidate_reads_remain_noncurrent_and_detect_pin_drift(monkeypatch):
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(native, "db", database)
        inputs = await _candidate_tables(database, schema)
        assert await _overlay_datasets(database, schema) == ["incumbent"]
        token = preparation._PREPARATION.set(inputs)
        try:
            await preparation.assert_dataset_pins(schema, inputs)
            await native._preflight_provider_directory_partial_scope_index(schema)
            assert await _overlay_datasets(database, schema) == ["candidate", "retained"]
            await native._assert_current_provider_directory_dataset(
                schema, source_id="directory", expected_dataset_id="candidate", expected_root_run_id="new-run"
            )
            context_by_field = {}
            await native._capture_provider_directory_overlay_alias_fence(schema, [], context_by_field)
            assert context_by_field["provider_directory_overlay_relation"] == (
                schema,
                "provider_directory_address_overlay",
            )
            assert context_by_field["incumbent_provider_directory_overlay_fence"] == (
                0,
                (schema, "provider_directory_address_overlay"),
                (
                    await database.scalar(
                        f"SELECT updated_at FROM {schema}.address_alias_artifact_state_v1 "
                        "WHERE artifact_name='provider_directory_address_overlay'"
                    )
                ).isoformat(),
            )
            await database.status(
                f"UPDATE {schema}.provider_directory_endpoint_dataset SET dataset_hash='changed' WHERE dataset_id='candidate'"
            )
            with pytest.raises(RuntimeError, match="pins changed"):
                await preparation.assert_dataset_pins(schema, inputs)
        finally:
            preparation._PREPARATION.reset(token)
        assert await _overlay_datasets(database, schema) == ["incumbent"]


@pytest.mark.parametrize(
    ("dataset_id", "column", "cast"),
    [
        ("candidate", "publication_metadata_summary_json", "jsonb"),
        ("retained", "publication_metadata_json", "json"),
    ],
)
async def test_pin_membership_reads_summary_then_falls_back_to_raw_document(monkeypatch, dataset_id, column, cast):
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(native, "db", database)
        inputs = await _candidate_tables(database, schema)
        token = preparation._PREPARATION.set(inputs)
        try:
            await preparation.assert_dataset_pins(schema, inputs)
            await database.status(
                f"UPDATE {schema}.provider_directory_endpoint_dataset "
                f"SET {column}=CAST('{{\"source_ids\":[]}}' AS {cast}) WHERE dataset_id=:dataset",
                dataset=dataset_id,
            )
            with pytest.raises(RuntimeError, match="pins changed"):
                await preparation.assert_dataset_pins(schema, inputs)
        finally:
            preparation._PREPARATION.reset(token)


async def test_empty_desired_vector_prepares_withdrawal_without_changing_current(monkeypatch):
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(native, "db", database)
        inputs = replace(await _candidate_tables(database, schema), dataset_pins=())
        token = preparation._PREPARATION.set(inputs)
        try:
            await preparation.assert_dataset_pins(schema, inputs)
            assert await _overlay_datasets(database, schema) == []
        finally:
            preparation._PREPARATION.reset(token)
        assert await _overlay_datasets(database, schema) == ["incumbent"]


async def _generation_authority(database, schema, monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    async with database.transaction() as session:
        await _run_migration(await session.connection(), "upgrade")
    for relation in native.result_generation.RELATION_NAMES[1:]:
        await database.status(f"CREATE TABLE {schema}.{relation} (marker text)")


async def _install_overlay(database, schema, prepared):
    await database.status(f"ALTER TABLE {schema}.provider_directory_address_overlay RENAME TO previous_overlay")
    await database.status(f"ALTER TABLE {schema}.prepared_overlay RENAME TO provider_directory_address_overlay")
    await preparation.bind_published_overlay(
        prepared,
        [
            SimpleNamespace(
                schema=schema, target_relation="provider_directory_address_overlay", stage_table="prepared_overlay"
            )
        ],
    )


async def _prepared_fixture(database, schema, monkeypatch):
    monkeypatch.setattr(native, "db", database)
    inputs = await _candidate_tables(database, schema)
    await _prepare_live_and_stage(database, schema)
    await _prepare_geo_inputs(database, schema)
    await _generation_authority(database, schema, monkeypatch)
    context_by_field = {
        "address_alias_generation": 0,
        "publish_validation": {},
        "staged_rows": 1,
        "geo_assurance_candidate_relation": {"schema_name": schema, "relation_name": _StageTable.__tablename__},
    }
    token = preparation._PREPARATION.set(inputs)
    try:
        await preparation.capture_overlay_fence(schema, context_by_field)
        return await preparation.prepare_finalized_generation(schema, _StageTable, {}, context=context_by_field)
    finally:
        preparation._PREPARATION.reset(token)


async def _prepare_geo_inputs(database, schema):
    """Record the real complete named input family before testing prepared cutover."""
    from tests.test_entity_address_snapshot_destination import _publish_npi_fixture_generation

    await database.status("CREATE EXTENSION IF NOT EXISTS postgis")
    await _publish_npi_fixture_generation(database, schema)
    for create in (
        _create_mrf_address_table,
        _create_cms_address_table,
        _create_geo_reference_tables,
    ):
        await create(database, schema)
    bindings_by_name = {
        f"{namespace or schema}.{name}": {"schema_name": schema, "table_name": name}
        for namespace, name in native.geo_projection._PROJECTION_DEPENDENCIES
    }
    await native.prepared_doctors.record_applied_geo_assurance_candidate(
        database, schema, _StageTable.__tablename__, 1, bindings_by_name
    )


async def test_prepared_cutover_obeys_outer_rollback_and_native_authority(monkeypatch):
    async with _temporary_schema() as (database, schema):
        prepared = await _prepared_fixture(database, schema, monkeypatch)
        assert prepared.stage_relations[0][:2] == ("entity_address_unified", "entity_address_unified_stage")
        with pytest.raises(RuntimeError, match="caller-owned"):
            await preparation.publish_prepared_entity_address_generation(prepared)
        with pytest.raises(RuntimeError, match="outer rollback"):
            async with database.transaction():
                await _install_overlay(database, schema, prepared)
                receipt = await preparation.publish_prepared_entity_address_generation(prepared)
                assert receipt["local_generation"] == 1
                with pytest.raises(RuntimeError, match="has not committed"):
                    await prepared.mark_committed()
                raise RuntimeError("outer rollback")
        assert prepared.committed is False
        with pytest.raises(RuntimeError, match="has not committed"):
            await prepared.mark_committed()
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified") == "old"
        assert await database.scalar(f"SELECT local_generation FROM {schema}.entity_address_result_generation") == 0
        async with database.transaction():
            await _install_overlay(database, schema, prepared)
            receipt = await preparation.publish_prepared_entity_address_generation(prepared)
        await prepared.mark_committed()
        await preparation.cleanup_prepared_entity_address_generation(prepared)
        assert receipt["relation_oids"] is None
        assert await database.scalar(f"SELECT relation_oids IS NULL FROM {schema}.entity_address_result_generation")
        assert receipt["local_generation"] == 1
        assert prepared.context["publication_state"] == "published"
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified") == "new"


async def test_missing_secondary_index_blocks_prepared_result(monkeypatch):
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(native, "db", database)
        await database.status(f"CREATE TABLE {schema}.indexed_stage (marker text)")
        stage = SimpleNamespace(
            __tablename__="indexed_stage", __my_additional_indexes__=[{"name": "marker", "index_elements": ["marker"]}]
        )
        with pytest.raises(RuntimeError, match="all serving indexes"):
            await preparation._require_stage_indexes(schema, stage)
        name = native._stage_index_name(stage.__tablename__, "marker")
        await database.status(f"CREATE INDEX {name} ON {schema}.indexed_stage (marker)")
        await preparation._require_stage_indexes(schema, stage)


async def test_missing_named_stage_blocks_cutover_and_preserves_rollback(monkeypatch):
    async with _temporary_schema() as (database, schema):
        prepared = await _prepared_fixture(database, schema, monkeypatch)
        await database.status(f"ALTER TABLE {schema}.entity_address_unified_stage RENAME TO original_stage")
        with pytest.raises(RuntimeError, match="missing required staged relations"):
            async with database.transaction():
                await _install_overlay(database, schema, prepared)
                await preparation.publish_prepared_entity_address_generation(prepared)
        await preparation.cleanup_prepared_entity_address_generation(prepared)
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified") == "old"
        assert await database.scalar(f"SELECT to_regclass('{schema}.entity_address_unified_stage') IS NULL")
        assert await database.scalar(f"SELECT marker FROM {schema}.original_stage") == "new"


@asynccontextmanager
async def _ordinary_candidate_accounts(database, schema):
    """Create only disposable native actors, removing their objects and grants on exit."""
    roles, accounts = [], []
    try:
        for purpose in ("owner", "writer", "reader"):
            role = f"candidate_{purpose}_{uuid4().hex[:16]}"
            credential = uuid4().hex
            await database.status(f"CREATE ROLE \"{role}\" LOGIN NOSUPERUSER PASSWORD '{credential}'")
            roles.append(role)
            await database.status(f'GRANT USAGE ON SCHEMA {schema} TO "{role}"')
            engine = create_async_engine(
                database.engine.url.set(username=role, password=credential), pool_size=1, max_overflow=0
            )
            accounts.append(Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False)))
        await database.status(f'GRANT CREATE ON SCHEMA {schema} TO "{roles[0]}"')
        yield roles, accounts
    finally:
        for account in accounts:
            await account.disconnect()
        for role in reversed(roles):
            await database.status(f'DROP OWNED BY "{role}"')
            await database.status(f'DROP ROLE "{role}"')
        assert not await database.scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY(CAST(:roles AS text[])))", roles=roles
        )


async def _create_freeze_candidates(owner, schema, writer):
    """Populate native candidate tables and delegated mutation rights without foreign keys."""
    tables = ("freeze_stage", "freeze_support_a", "freeze_support_b")
    await owner.status(
        f"CREATE TABLE {schema}.freeze_stage (id bigserial PRIMARY KEY,location_key text NOT NULL UNIQUE,"
        "marker text,address_key uuid,base_address_version text,type text,address_sources text[],"
        "archive_identity_version text,lat double precision,long double precision)"
    )
    await owner.status(
        f"INSERT INTO {schema}.freeze_stage(location_key,marker,base_address_version,type,archive_identity_version) "
        f"VALUES ('location','candidate','{native.ALIAS_BASE_ADDRESS_VERSION_PREFIX}0','synthetic',"
        f"'{native.ARCHIVE_IDENTITY_VERSION}')"
    )
    for name in tables[1:]:
        await owner.status(f"CREATE TABLE {schema}.{name} (id bigserial PRIMARY KEY,location_key text,marker text)")
        await owner.status(f"INSERT INTO {schema}.{name}(location_key,marker) VALUES ('location','support')")
    for name in tables:
        await owner.status(f'GRANT SELECT,DELETE,TRUNCATE ON {schema}.{name} TO "{writer}"')
        await owner.status(f'GRANT INSERT(marker),UPDATE(marker) ON {schema}.{name} TO "{writer}"')
        await owner.status(f'GRANT USAGE,UPDATE ON SEQUENCE {schema}.{name}_id_seq TO "{writer}"')
    return tables


async def _assert_candidate_mutations_denied(account, schema, tables):
    """Require real privilege failures for table, column and owned-sequence writes."""
    for name in tables:
        for statement in (
            f"INSERT INTO {schema}.{name}(marker) VALUES ('late')",
            f"UPDATE {schema}.{name} SET marker='late'",
            f"DELETE FROM {schema}.{name}",
            f"TRUNCATE {schema}.{name}",
            f"SELECT nextval('{schema}.{name}_id_seq')",
            f"SELECT setval('{schema}.{name}_id_seq',99)",
        ):
            with pytest.raises(DBAPIError) as failure:
                async with account.transaction():
                    await account.status(statement)
            assert failure.value.orig.sqlstate == "42501"


async def _assert_busy_candidate_refuses_freeze(owner, writer, schema, stages, support_stage_class_map):
    """An already active writer drains or forces rollback before any candidate rights close."""
    async with writer.transaction():
        await writer.status(f"UPDATE {schema}.{stages[-1].__tablename__} SET marker='uncommitted'")
        with pytest.raises(DBAPIError) as failure:
            await preparation.freeze_ordinary_prepared_stages(schema, stages[0], support_stage_class_map)
        assert failure.value.orig.sqlstate == "55P03"
        assert await writer.scalar(f"SELECT marker FROM {schema}.{stages[-1].__tablename__}") == "uncommitted"
    for stage in stages:
        await owner.status(f"UPDATE {schema}.{stage.__tablename__} SET marker='owner-after-refusal'")
        await writer.status(f"UPDATE {schema}.{stage.__tablename__} SET marker='writer-after-refusal'")
        assert await writer.scalar(f"SELECT nextval('{schema}.{stage.__tablename__}_id_seq')") > 0


async def test_native_ordinary_preparation_freezes_writes_and_preserves_live_reads(monkeypatch):
    """Validate real closed candidates, preserve live reads, and roll back failed preparation."""
    async with (
        _temporary_schema() as (database, schema),
        _ordinary_candidate_accounts(database, schema) as (roles, accounts),
    ):
        owner, writer, reader = accounts
        assert await owner.scalar("SELECT NOT rolsuper FROM pg_roles WHERE rolname=current_user")
        monkeypatch.setattr(native, "db", owner)
        tables = await _create_freeze_candidates(owner, schema, roles[1])
        await database.status(f"CREATE TABLE {schema}.live_sentinel (marker text)")
        await database.status(f"INSERT INTO {schema}.live_sentinel VALUES ('serving')")
        await database.status(f'GRANT SELECT ON {schema}.live_sentinel TO "{roles[2]}"')
        await database.status(
            f"CREATE TABLE {schema}.address_alias_v1 (source_address_key uuid,revoked_at timestamptz)"
        )
        await database.status(
            f'GRANT SELECT ON {schema}.address_alias_state_v1,{schema}.address_alias_v1 TO "{roles[0]}"'
        )
        stages = [SimpleNamespace(__tablename__=name) for name in tables]
        support_stage_class_map = {
            native.EntityAddressPlanBridge: stages[1],
            native.EntityAddressNetworkBridge: stages[2],
        }
        await _assert_busy_candidate_refuses_freeze(owner, writer, schema, stages, support_stage_class_map)
        async with reader.transaction() as read_session:
            await read_session.execute(text("SET LOCAL statement_timeout='100ms'"))
            assert await reader.scalar(f"SELECT marker FROM {schema}.live_sentinel") == "serving"
            with pytest.raises(RuntimeError, match="failed preparation"):
                async with owner.transaction():
                    await preparation.freeze_ordinary_prepared_stages(schema, stages[0], support_stage_class_map)
                    metrics = await native._validate_publish_integrity(
                        schema, tables[0], support_stage_class_map, test_mode=False
                    )
                    assert set(metrics["bridge_orphans"].values()) == {0}
                    raise RuntimeError("failed preparation")
            for name in tables:
                await owner.status(f"UPDATE {schema}.{name} SET marker='rollback-restored'")
                await writer.status(f"UPDATE {schema}.{name} SET marker='delegated'")
                assert await writer.scalar(f"SELECT nextval('{schema}.{name}_id_seq')") > 0
            await preparation.freeze_ordinary_prepared_stages(schema, stages[0], support_stage_class_map)
            metrics = await native._validate_publish_integrity(
                schema, tables[0], support_stage_class_map, test_mode=False
            )
            assert metrics["null_location_keys"] == metrics["duplicate_location_keys"] == 0
            assert set(metrics["bridge_orphans"].values()) == {0}
            await _assert_candidate_mutations_denied(owner, schema, tables)
            await _assert_candidate_mutations_denied(writer, schema, tables)
            for name in tables:
                assert await owner.scalar(f"SELECT count(*) FROM {schema}.{name}") == 1
                assert await owner.scalar(f"SELECT marker FROM {schema}.{name}") == "delegated"
            assert await reader.scalar(f"SELECT marker FROM {schema}.live_sentinel") == "serving"


async def test_ordinary_freeze_keeps_issued_source_closure_in_its_existing_lifecycle(monkeypatch):
    """Do not close an issued Source family through an ordinary transaction."""
    from unittest.mock import Mock

    source_scope, issued = object(), object()
    token = preparation.admitted._ADMISSION.set(source_scope)
    transaction = Mock(side_effect=AssertionError("ordinary transaction must not run for Source"))
    monkeypatch.setattr(
        preparation.source_preparation, "require_building", lambda scope: issued if scope is source_scope else None
    )
    monkeypatch.setattr(preparation, "_native", lambda: SimpleNamespace(db=SimpleNamespace(transaction=transaction)))
    try:
        await preparation.freeze_ordinary_prepared_stages("fixture", SimpleNamespace(__tablename__="stage"), {})
    finally:
        preparation.admitted._ADMISSION.reset(token)
    transaction.assert_not_called()
