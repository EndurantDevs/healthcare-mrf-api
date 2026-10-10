# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native logging admission and exact stage cleanup across rename and cancellation races."""

import asyncio
import importlib
from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from db.connection import ConnectionProxy
from process import entity_address_candidate_preparation as preparation
from process import entity_address_preparation_admission as admitted
from process import provider_directory_cms_preparation as cms_preparation
from tests.test_entity_address_candidate_preparation_postgres import _candidate_tables, _prepared_fixture
from tests.test_entity_address_unified_publication_db import (
    _prepare_live_and_stage,
    _StageTable,
    _SupportLiveTable,
    _SupportStageTable,
    _temporary_schema,
)
from tests.test_provider_directory_cms_preparation import _admission, _inputs, _signed_lease

native = importlib.import_module("process.entity_address_unified")
fhir = importlib.import_module("process.provider_directory_fhir")
_INPUT_HASH = "a" * 64


async def _admitted_fixture(database, schema, monkeypatch, *, deny_logging_check=None):
    """Retain the real native logging and admission paths around a two-table synthetic build."""
    monkeypatch.setattr(native, "db", database)
    monkeypatch.setattr(fhir, "db", database)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    inputs = await _candidate_tables(database, schema)
    await _prepare_live_and_stage(database, schema)
    await database.status(f"DROP TABLE {schema}.entity_address_unified_stage")
    support_class_map = {_SupportLiveTable: _SupportStageTable}
    monkeypatch.setattr(native, "_support_stage_classes", lambda _import_id: support_class_map)
    observed_logging_states = []
    admission = _admission()
    admission.plan = replace(
        admission.plan,
        native_address_targets=("entity_address_evidence", "entity_address_unified"),
        native_address_input_hash=_INPUT_HASH,
        temp_file_limit_bytes_per_backend=32 * 1024,
    )
    admission.lease = _signed_lease(admission.plan)
    inputs = replace(inputs, semantic_as_of=admission.plan.desired_profile_as_of)

    async def observe(request):
        if request.phase == "pre_logging":
            observed_logging_states.append({relation.relation: relation.persistence for relation in request.relations})
            if len(observed_logging_states) == deny_logging_check:
                raise RuntimeError("synthetic logging admission denied")
        return cms_preparation.NonprofileAdmissionReceipt(
            request.phase,
            request.lease.lease_digest,
            request.lease.reservation_id,
            request.plan.capacity_geometry_hash,
            request.relations,
            request.logging_relations,
        )

    admission.check_phase = observe
    execution, fence, projection = _inputs()
    await admission.before_scratch(
        execution, fence, projection, set(admission.plan.publish_targets), frozenset(admission.plan.resource_types)
    )

    async def build(ctx, task):
        assert "admission" not in task and "native_input_hash" not in task
        await _build_synthetic_stages(database, schema, ctx)

    async def finalize(ctx, *, prepare_only):
        assert prepare_only
        await native._compact_geo_assurance_stage(schema, _StageTable.__tablename__)
        return await preparation.prepare_finalized_generation(
            schema, _StageTable, support_class_map, context=ctx["context"]
        )

    monkeypatch.setattr(native, "process_entity_address_unified_data", build)
    monkeypatch.setattr(native, "publish_entity_address_unified_generation", finalize)
    context_by_field = {
        "import_date": "stage",
        "context": {"run": True, "address_alias_generation": 0, "publish_validation": {}, "staged_rows": 1},
    }
    return context_by_field, inputs, admission, observed_logging_states


async def _build_synthetic_stages(database, schema, ctx):
    """Exercise creation-time ownership before the real final registration and logging."""
    await preparation.capture_overlay_fence(schema, ctx["context"])
    for stage in ("entity_address_unified_stage", "entity_address_evidence_stage"):
        await admitted.create_stage_sql(schema, stage, f"CREATE UNLOGGED TABLE {schema}.{stage} (marker text)")
    await native._status_with_entity_address_tuning(f"INSERT INTO {schema}.entity_address_unified_stage VALUES ('new')")
    await database.scalar(native._record_geo_assurance_candidate_sql(schema, "entity_address_unified_stage", 1))


async def _prepare(ctx, inputs, admission):
    """Use the private keyword-only carrier rather than a decoded worker callback."""
    return await preparation.prepare_provider_directory_entity_address(
        ctx, {}, preparation_input=inputs, admission=admission, native_input_hash=_INPUT_HASH
    )


async def test_all_native_stages_registered_before_geo_compaction_logs(monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, observed_states = await _admitted_fixture(database, schema, monkeypatch)
        prepared = await _prepare(ctx, inputs, admission)
        assert observed_states[0] == {"entity_address_evidence_stage": "u", "entity_address_unified_stage": "u"}
        assert observed_states[1] == observed_states[0]
        assert observed_states[-1] == {"entity_address_evidence_stage": "u", "entity_address_unified_stage": "p"}
        assert {(schema, stage) for _target, stage, _oid in prepared.stage_oids} <= admission._external_relations
        assert admitted._ADMISSION.get() is None
        await preparation.cleanup_prepared_entity_address_generation(prepared)
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified") == "old"


@pytest.mark.parametrize("denied_check", [1, 2, 3])
async def test_logging_denial_cleans_every_owned_native_stage(monkeypatch, denied_check):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, observed_states = await _admitted_fixture(
            database, schema, monkeypatch, deny_logging_check=denied_check
        )
        with pytest.raises(RuntimeError, match="logging admission denied"):
            await _prepare(ctx, inputs, admission)
        assert len(observed_states) == denied_check
        assert admitted._ADMISSION.get() is None
        for stage in ("entity_address_unified_stage", "entity_address_evidence_stage"):
            assert await database.scalar("SELECT to_regclass(:relation)", relation=f"{schema}.{stage}") is None
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified") == "old"


async def test_signed_limits_apply_to_actual_pooled_and_bound_workers(monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        build = native.process_entity_address_unified_data

        async def build_with_worker_probes(ctx, task):
            await build(ctx, task)
            await native._status_with_entity_address_tuning(_settings_probe(schema, "pooled"))
            async with database.transaction():
                previous = await database.scalar("SHOW temp_file_limit")
                await native._status_with_entity_address_tuning(_settings_probe(schema, "bound"))
                assert await database.scalar("SHOW temp_file_limit") == previous
                await native._apply_entity_address_transaction_settings()
                await database.status(_settings_probe(schema, "projection"))

        monkeypatch.setattr(native, "process_entity_address_unified_data", build_with_worker_probes)
        prepared = await _prepare(ctx, inputs, admission)
        for table in ("pooled", "bound", "projection"):
            row = await database.first(f"SELECT * FROM {schema}.settings_{table}")
            assert tuple(row) == (admission.plan.temp_file_limit_bytes_per_backend, 0, 0)
        await preparation.cleanup_prepared_entity_address_generation(prepared)


def _settings_probe(schema, label):
    """Persist the executing backend's settings in a small schema-owned test relation."""
    return (
        f"CREATE TABLE {schema}.settings_{label} AS SELECT "
        "pg_size_bytes(current_setting('temp_file_limit')) AS temp_bytes, "
        "current_setting('max_parallel_workers_per_gather')::int AS query_workers, "
        "current_setting('max_parallel_maintenance_workers')::int AS maintenance_workers"
    )


def _deny_temp_setting(monkeypatch, database):
    """Simulate a denied SET while keeping the executing backend and fallback path real."""
    database_status, connection_status = database.status, ConnectionProxy.status

    async def status(statement, **params):
        if statement.startswith("SET LOCAL temp_file_limit"):
            raise RuntimeError("permission denied to set parameter")
        return await database_status(statement, **params)

    async def connection(self, statement, **params):
        if statement.startswith("SET LOCAL temp_file_limit"):
            raise RuntimeError("permission denied to set parameter")
        return await connection_status(self, statement, **params)

    monkeypatch.setattr(database, "status", status)
    monkeypatch.setattr(ConnectionProxy, "status", connection)


@pytest.mark.parametrize("worker", ["pooled", "bound", "projection", "logging"])
async def test_denied_actual_backend_limit_stops_heavy_work(monkeypatch, worker):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        _deny_temp_setting(monkeypatch, database)
        if worker == "logging":
            with pytest.raises(RuntimeError, match="permission denied to set parameter"):
                await _prepare(ctx, inputs, admission)
        else:
            await _assert_denied_worker(database, schema, admission, worker)
        assert await database.scalar(f"SELECT to_regclass('{schema}.settings_denied')") is None
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified") == "old"


async def _assert_denied_worker(database, schema, admission, worker):
    """Reject a skipped signed bound before the pooled, borrowed, or projection SQL executes."""
    token = admitted._ADMISSION.set(admitted._admitted_preparation(admission, _INPUT_HASH))
    try:
        expected = "permission denied to set parameter" if worker == "bound" else "executing-session settings changed"
        with pytest.raises(RuntimeError, match=expected):
            await _run_worker(database, schema, worker)
    finally:
        admitted._ADMISSION.reset(token)


async def _run_worker(database, schema, worker):
    """Exercise each existing native statement path on a real backend."""
    if worker == "pooled":
        await native._status_with_entity_address_tuning(_settings_probe(schema, "denied"))
        return
    async with database.transaction():
        if worker == "bound":
            await native._status_with_entity_address_tuning(_settings_probe(schema, "denied"))
        else:
            await native._apply_entity_address_transaction_settings()
            await database.status(_settings_probe(schema, "denied"))


async def test_replacement_after_logging_check_is_never_logged_or_deleted(monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, observed_states = await _admitted_fixture(database, schema, monkeypatch)
        check = admission.check_phase

        async def replace_after_admission_check(request):
            receipt = await check(request)
            if request.phase == "pre_logging" and len(observed_states) == 2:
                await database.status(f"ALTER TABLE {schema}.entity_address_unified_stage RENAME TO original_stage")
                await database.status(f"CREATE UNLOGGED TABLE {schema}.entity_address_unified_stage (marker text)")
            return receipt

        admission.check_phase = replace_after_admission_check
        with pytest.raises(RuntimeError, match="relation_changed|stage changed"):
            await _prepare(ctx, inputs, admission)
        assert await native._stage_table_persistence(schema, "entity_address_unified_stage") == "u"
        assert await native._stage_table_persistence(schema, "original_stage") == "u"
        assert await native._stage_table_persistence(schema, "entity_address_evidence_stage") is None
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified") == "old"


async def test_cleanup_rechecks_oid_after_concurrent_name_replacement(monkeypatch):
    async with _temporary_schema() as (database, schema):
        prepared = await _prepared_fixture(database, schema, monkeypatch)
        acquire_locks = native._acquire_cutover_locks

        async def replace_then_lock(*args):
            async with database.engine.begin() as connection:
                await connection.execute(
                    text(f"ALTER TABLE {schema}.entity_address_unified_stage RENAME TO original_stage")
                )
                await connection.execute(text(f"CREATE TABLE {schema}.entity_address_unified_stage (marker text)"))
                await connection.execute(
                    text(f"INSERT INTO {schema}.entity_address_unified_stage VALUES ('replacement')")
                )
            await acquire_locks(*args)

        monkeypatch.setattr(native, "_acquire_cutover_locks", replace_then_lock)
        await preparation.cleanup_prepared_entity_address_generation(prepared)
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified_stage") == "replacement"
        assert await database.scalar(f"SELECT marker FROM {schema}.original_stage") == "new"
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified") == "old"


async def test_cleanup_holds_native_lock_until_drop(monkeypatch):
    async with _temporary_schema() as (database, schema):
        prepared = await _prepared_fixture(database, schema, monkeypatch)
        acquire_locks = native._acquire_cutover_locks

        async def lock_then_attempt_replacement(*args):
            await acquire_locks(*args)
            assert await database.scalar("SHOW lock_timeout") == "500ms"
            assert await database.scalar("SHOW statement_timeout") == "5s"
            with pytest.raises(DBAPIError, match="could not obtain lock"):
                async with database.engine.begin() as connection:
                    await connection.execute(
                        text(f"LOCK TABLE {schema}.entity_address_unified_stage IN ACCESS EXCLUSIVE MODE NOWAIT")
                    )

        monkeypatch.setattr(native, "_acquire_cutover_locks", lock_then_attempt_replacement)
        await preparation.cleanup_prepared_entity_address_generation(prepared)
        assert await database.scalar(f"SELECT to_regclass('{schema}.entity_address_unified_stage')") is None


async def test_cleanup_finishes_after_owner_cancellation(monkeypatch):
    async with _temporary_schema() as (database, schema):
        prepared = await _prepared_fixture(database, schema, monkeypatch)
        acquire_locks = native._acquire_cutover_locks
        is_locked, release_cleanup = asyncio.Event(), asyncio.Event()

        async def lock_then_wait(*args):
            await acquire_locks(*args)
            is_locked.set()
            await release_cleanup.wait()

        monkeypatch.setattr(native, "_acquire_cutover_locks", lock_then_wait)
        task = asyncio.create_task(preparation.cleanup_prepared_entity_address_generation(prepared))
        try:
            await asyncio.wait_for(is_locked.wait(), timeout=5)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release_cleanup.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert await database.scalar(f"SELECT to_regclass('{schema}.entity_address_unified_stage')") is None
