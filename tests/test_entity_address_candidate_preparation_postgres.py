# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact candidate reads and prepared address cutover under an outer transaction."""

from __future__ import annotations

import importlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.test_entity_address_result_generation_postgres import _run_migration
from tests.test_entity_address_unified_no_live_mutation import _mock_shutdown_dependencies, _shutdown_context
from tests.test_entity_address_unified_publication_db import _prepare_live_and_stage, _StageTable, _temporary_schema

native = importlib.import_module("process.entity_address_unified")
preparation = importlib.import_module("process.entity_address_candidate_preparation")


def _inputs(overlay_oid=41):
    return preparation.ProviderDirectoryAddressPreparationInput(
        dataset_pins=(
            preparation.ProviderDirectoryAddressDatasetPin("directory", "endpoint", "candidate", "b" * 64, "new-run"),
            preparation.ProviderDirectoryAddressDatasetPin(
                "other", "other-endpoint", "retained", "c" * 64, "other-run"
            ),
        ),
        overlay_table="prepared_overlay",
        overlay_relation_oid=overlay_oid,
        address_alias_generation=0,
    )


@pytest.mark.parametrize(
    "changed",
    [
        {"overlay_table": "provider_directory_address_overlay"},
        {"overlay_table": "unsafe;table"},
        {"overlay_relation_oid": True},
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

    async def finalize(*_args, **kwargs):
        assert kwargs["context"]["publish_validation_deferred"] is False
        events.append(("prepared", "ready"))
        return expected_prepared

    monkeypatch.setattr(native, "_create_stage_indexes", record_indexes)
    monkeypatch.setattr(preparation, "prepare_finalized_generation", finalize)
    token = preparation._PREPARATION.set(_inputs())
    try:
        assert await native.publish_entity_address_unified_generation(context, prepare_only=True) is expected_prepared
    finally:
        preparation._PREPARATION.reset(token)
    kinds = [kind for kind, _detail in events]
    assert kinds.index("validate_geo") < kinds.index("indexes") < kinds.index("validate") < kinds.index("prepared")
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
        "publication_metadata_json text)",
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
    await database.status(f"CREATE TABLE {schema}.{receipt_table} (artifact_name text, generation bigint)")
    await database.status(f"INSERT INTO {schema}.{receipt_table} VALUES ('provider_directory_address_overlay',0)")
    overlay_oid = await database.scalar(f"SELECT '{schema}.prepared_overlay'::regclass::oid::bigint")
    return _inputs(overlay_oid)


async def _candidate_datasets(database, schema):
    for dataset_id, endpoint, source_id, digest, root_run, status, current in (
        ("incumbent", "endpoint", "directory", "a" * 64, "old-run", "published", True),
        ("candidate", "endpoint", "directory", "b" * 64, "new-run", "validated", False),
        ("retained", "other-endpoint", "other", "c" * 64, "other-run", "published", True),
    ):
        await database.status(
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset VALUES "
            "(:dataset,:endpoint,:digest,:root,:root,:status,:current,now(),"
            "CASE WHEN :current THEN now() ELSE NULL END,NULL,:metadata)",
            dataset=dataset_id,
            endpoint=endpoint,
            digest=digest,
            root=root_run,
            status=status,
            current=current,
            metadata='{"source_ids":["' + source_id + '"]}',
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
            assert context_by_field["provider_directory_overlay_relation_oid"] == inputs.overlay_relation_oid
            assert context_by_field["incumbent_provider_directory_overlay_fence"][1] != inputs.overlay_relation_oid
            await database.status(
                f"UPDATE {schema}.provider_directory_endpoint_dataset SET dataset_hash='changed' WHERE dataset_id='candidate'"
            )
            with pytest.raises(RuntimeError, match="pins changed"):
                await preparation.assert_dataset_pins(schema, inputs)
        finally:
            preparation._PREPARATION.reset(token)
        assert await _overlay_datasets(database, schema) == ["incumbent"]


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


async def _install_overlay(database, schema):
    await database.status(f"ALTER TABLE {schema}.provider_directory_address_overlay RENAME TO previous_overlay")
    await database.status(f"ALTER TABLE {schema}.prepared_overlay RENAME TO provider_directory_address_overlay")


async def _prepared_fixture(database, schema, monkeypatch):
    monkeypatch.setattr(native, "db", database)
    inputs = await _candidate_tables(database, schema)
    await _prepare_live_and_stage(database, schema)
    await _generation_authority(database, schema, monkeypatch)
    context_by_field = {"address_alias_generation": 0, "publish_validation": {}, "staged_rows": 1}
    token = preparation._PREPARATION.set(inputs)
    try:
        await preparation.capture_overlay_fence(schema, context_by_field)
        return await preparation.prepare_finalized_generation(schema, _StageTable, {}, context=context_by_field)
    finally:
        preparation._PREPARATION.reset(token)


async def test_prepared_cutover_obeys_outer_rollback_and_native_authority(monkeypatch):
    async with _temporary_schema() as (database, schema):
        prepared = await _prepared_fixture(database, schema, monkeypatch)
        assert prepared.stage_oids[0][:2] == ("entity_address_unified", "entity_address_unified_stage")
        with pytest.raises(RuntimeError, match="caller-owned"):
            await preparation.publish_prepared_entity_address_generation(prepared)
        with pytest.raises(RuntimeError, match="outer rollback"):
            async with database.transaction():
                await _install_overlay(database, schema)
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
            await _install_overlay(database, schema)
            receipt = await preparation.publish_prepared_entity_address_generation(prepared)
        await prepared.mark_committed()
        await preparation.cleanup_prepared_entity_address_generation(prepared)
        assert receipt["relation_oids"][0] == prepared.stage_oids[0][2]
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


async def test_replaced_prepared_stage_blocks_cutover_and_cleanup(monkeypatch):
    async with _temporary_schema() as (database, schema):
        prepared = await _prepared_fixture(database, schema, monkeypatch)
        await database.status(f"ALTER TABLE {schema}.entity_address_unified_stage RENAME TO original_stage")
        await database.status(f"CREATE TABLE {schema}.entity_address_unified_stage (marker text)")
        with pytest.raises(RuntimeError, match="prepared stage changed"):
            async with database.transaction():
                await _install_overlay(database, schema)
                await preparation.publish_prepared_entity_address_generation(prepared)
        await preparation.cleanup_prepared_entity_address_generation(prepared)
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified") == "old"
        assert await database.scalar(f"SELECT to_regclass('{schema}.entity_address_unified_stage') IS NOT NULL")
