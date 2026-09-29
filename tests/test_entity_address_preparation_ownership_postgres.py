# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Creation ownership and bounded native workers before final address registration."""

import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, MetaData, String, Table
from sqlalchemy.exc import DBAPIError

from db.connection import ConnectionProxy
from process import entity_address_candidate_preparation as preparation
from process import entity_address_preparation_admission as admitted
from tests.test_entity_address_preparation_admission_postgres import _admitted_fixture, _prepare, native
from tests.test_entity_address_unified_publication_db import _temporary_schema
from tests.test_provider_directory_cms_preparation import _signed_lease


async def _assert_stages_absent(database, schema, *names):
    """Require exact task stages to be gone while the incumbent remains available."""
    for name in ("entity_address_unified_stage", "entity_address_evidence_stage", *names):
        assert await database.scalar("SELECT to_regclass(:relation)", relation=f"{schema}.{name}") is None
    assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified") == "old"


def _observe_primary_key_overlap(monkeypatch):
    """Keep the first real DDL transaction open while a sibling attempts its work."""
    first_key_held, sibling_ready, sibling_finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    progress_by_field = {"active": 0, "maximum": 0, "keys": 0}
    ordinal_by_task = {}
    original_status, original_index = ConnectionProxy.status, native._index_support_stage

    async def status(connection, statement, **params):
        if statement.startswith("LOCK TABLE"):
            ordinal = ordinal_by_task[asyncio.current_task()]
            if ordinal == 1:
                with suppress(TimeoutError):
                    await asyncio.wait_for(sibling_ready.wait(), timeout=0.2)
            else:
                sibling_ready.set()
                await asyncio.wait_for(first_key_held.wait(), timeout=2)
            try:
                return await original_status(connection, statement, **params)
            finally:
                if ordinal == 2:
                    sibling_finished.set()
        result = await original_status(connection, statement, **params)
        if "ADD CONSTRAINT %I PRIMARY KEY" in statement:
            progress_by_field["keys"] += 1
            if progress_by_field["keys"] == 1:
                first_key_held.set()
                with suppress(TimeoutError):
                    await asyncio.wait_for(sibling_finished.wait(), timeout=0.2)
            else:
                sibling_finished.set()
        return result

    async def index(progress_state, ordinal, stage):
        progress_by_field["active"] += 1
        progress_by_field["maximum"] = max(progress_by_field["maximum"], progress_by_field["active"])
        ordinal_by_task[asyncio.current_task()] = ordinal
        try:
            await original_index(progress_state, ordinal, stage)
        finally:
            ordinal_by_task.pop(asyncio.current_task())
            progress_by_field["active"] -= 1

    monkeypatch.setattr(ConnectionProxy, "status", status)
    monkeypatch.setattr(native, "_index_support_stage", index)
    return progress_by_field


@pytest.mark.parametrize("is_admitted", [False, True])
async def test_support_primary_keys_preserve_owned_tables(monkeypatch, is_admitted):
    """Restore two real keys without sibling lock conflicts or ordinary serialization."""
    async with _temporary_schema() as (database, schema):
        _ctx, _inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        admission.plan = replace(admission.plan, worker_count=2)
        admission.lease = _signed_lease(admission.plan)
        scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
        token = admitted._ADMISSION.set(scope)
        try:
            for name in ("entity_address_unified_stage", "entity_address_evidence_stage"):
                await admitted.create_stage_sql(schema, name, f"CREATE UNLOGGED TABLE {schema}.{name} (marker text)")
            await database.status(f"INSERT INTO {schema}.entity_address_unified_stage VALUES ('new')")
            oid_by_name = dict(scope.owned_oids)
            stage_by_name = {
                name: SimpleNamespace(
                    __tablename__=name,
                    __table__=Table(name, MetaData(), Column("marker", String, primary_key=True)),
                )
                for name in oid_by_name
            }
            if not is_admitted:
                admitted._ADMISSION.set(None)
            monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_INDEX_CONCURRENCY", "2")
            progress_by_field = _observe_primary_key_overlap(monkeypatch)
            context_by_field = {}
            await native._create_support_stage_indexes(stage_by_name, schema, context=context_by_field)
            assert progress_by_field == {"active": 0, "maximum": 1 if is_admitted else 2, "keys": 2}
            assert context_by_field["support_stage_index_concurrency"] == (1 if is_admitted else 2)
            for name, oid in oid_by_name.items():
                assert await database.scalar("SELECT to_regclass(:name)::oid", name=f"{schema}.{name}") == oid
                assert (
                    await database.scalar(
                        "SELECT count(*) FROM pg_constraint WHERE conrelid=:oid AND contype='p'", oid=oid
                    )
                    == 1
                )
            assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified_stage") == "new"
        finally:
            admitted._ADMISSION.reset(token)
            await preparation._drain_owned_stages(schema, scope.cleanup_oids)
        await _assert_stages_absent(database, schema)


async def test_incomplete_finalization_cleans_created_stages(monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)

        async def incomplete(*_args, **_kwargs):
            return None

        monkeypatch.setattr(native, "publish_entity_address_unified_generation", incomplete)
        with pytest.raises(RuntimeError, match="did not prepare"):
            await _prepare(ctx, inputs, admission)
        await _assert_stages_absent(database, schema)


@pytest.mark.parametrize("kind", ["support", "raw", "separate_evidence", "compact"])
async def test_intermediate_create_failure_cleans_captured_family(monkeypatch, kind):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        build = native.process_entity_address_unified_data
        register = admission.register_external_relation

        async def fail_after_registration(fhir, schema, name, oid):
            await register(fhir, schema, name, oid)
            if name == "extra_stage":
                assert admitted._ADMISSION.get().owned_oids[name] == oid
                assert (schema, name) in admission._external_relations
                assert await database.scalar("SELECT relpersistence::text FROM pg_class WHERE oid=:oid", oid=oid) == "u"
                raise RuntimeError("synthetic failure after scratch creation")

        async def create_scratch(ctx, task):
            await build(ctx, task)
            await _create_intermediate(schema, kind)

        monkeypatch.setattr(admission, "register_external_relation", fail_after_registration)
        monkeypatch.setattr(native, "process_entity_address_unified_data", create_scratch)
        with pytest.raises(RuntimeError, match="failure after scratch creation"):
            await _prepare(ctx, inputs, admission)
        await _assert_stages_absent(database, schema, "extra_stage")


async def _create_intermediate(schema, kind):
    """Exercise model DDL and each native full-build scratch SQL family."""
    if kind == "support":
        table = Table("extra_stage", MetaData(), Column("marker", String, primary_key=True), schema=schema)
        stage_class = SimpleNamespace(__tablename__=table.name, __table__=table)
        await admitted.create_stage_table(schema, stage_class)
        return
    statements_by_kind = {
        "raw": native._prepare_raw_stage_sql(schema, "extra_stage", unlogged=False),
        "separate_evidence": native._prepare_multi_source_evidence_table_sql(schema, "extra_stage", unlogged=False),
        "compact": f"CREATE TABLE {schema}.extra_stage (LIKE {schema}.entity_address_unified_stage INCLUDING ALL)",
    }
    await admitted.create_stage_sql(schema, "extra_stage", statements_by_kind[kind])


async def test_creation_name_collision_preserves_foreign_table(monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        await database.status(f"CREATE TABLE {schema}.entity_address_unified_stage (marker text)")
        await database.status(f"INSERT INTO {schema}.entity_address_unified_stage VALUES ('foreign')")
        with pytest.raises(DBAPIError, match="already exists"):
            await _prepare(ctx, inputs, admission)
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified_stage") == "foreign"
        assert (schema, "entity_address_unified_stage") not in admission._external_relations
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified") == "old"


async def test_create_commit_ack_loss_retains_cleanup_identity(monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        transaction = database.transaction
        has_lost_ack = asyncio.Event()

        @asynccontextmanager
        async def lose_creation_ack():
            async with transaction() as session:
                yield session
            scope = admitted._ADMISSION.get()
            if not has_lost_ack.is_set() and scope is not None and scope.owned_oids:
                has_lost_ack.set()
                raise RuntimeError("synthetic create commit acknowledgement lost")

        monkeypatch.setattr(database, "transaction", lose_creation_ack)
        with pytest.raises(RuntimeError, match="commit acknowledgement lost"):
            await _prepare(ctx, inputs, admission)
        assert has_lost_ack.is_set()
        await _assert_stages_absent(database, schema)


async def test_cancellation_drains_creation_before_exact_cleanup(monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        register = admission.register_external_relation
        created, finish_registration = asyncio.Event(), asyncio.Event()

        async def pause_after_create(*args):
            await register(*args)
            created.set()
            await finish_registration.wait()

        monkeypatch.setattr(admission, "register_external_relation", pause_after_create)
        task = asyncio.create_task(_prepare(ctx, inputs, admission))
        try:
            await asyncio.wait_for(created.wait(), timeout=5)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            finish_registration.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        await _assert_stages_absent(database, schema)


@pytest.mark.parametrize("fail_after_swap", [False, True])
async def test_compact_rewrite_retains_original_oid_ownership(monkeypatch, fail_after_swap):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        build = native.process_entity_address_unified_data
        oid_by_phase = {}
        monkeypatch.setattr(native, "_entity_address_unified_columns", lambda: ["marker"])

        async def rewrite(ctx, task):
            await build(ctx, task)
            oid_by_phase["before"] = admitted._ADMISSION.get().owned_oids["entity_address_unified_stage"]
            assert (
                await native._rewrite_compacted_source_record_ids_stage(schema, "entity_address_unified_stage", {}) == 1
            )
            oid_by_phase["after"] = admitted._ADMISSION.get().owned_oids["entity_address_unified_stage"]
            assert await native._stage_table_persistence(schema, "entity_address_unified_stage") == "u"
            assert admission._relations[(schema, "entity_address_unified_stage")] == oid_by_phase["after"]
            assert (schema, "entity_address_unified_stage_compact") not in admission._relations
            if fail_after_swap:
                raise RuntimeError("synthetic failure after compact swap")
            await database.scalar(native._record_geo_assurance_candidate_sql(schema, "entity_address_unified_stage", 1))

        monkeypatch.setattr(native, "process_entity_address_unified_data", rewrite)
        await _finish_rewrite(ctx, inputs, admission, fail_after_swap)
        assert oid_by_phase["before"] != oid_by_phase["after"]
        await _assert_stages_absent(database, schema, "entity_address_unified_stage_compact")


async def _finish_rewrite(ctx, inputs, admission, should_fail):
    """Check the normal prepared result and the post-swap failure ownership paths."""
    if should_fail:
        with pytest.raises(RuntimeError, match="failure after compact swap"):
            await _prepare(ctx, inputs, admission)
        return
    prepared = await _prepare(ctx, inputs, admission)
    await preparation.cleanup_prepared_entity_address_generation(prepared)


async def test_nested_source_and_shard_workers_share_backend_bound(monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        admission.plan = replace(admission.plan, worker_count=2)
        admission.lease = _signed_lease(admission.plan)
        build = native.process_entity_address_unified_data
        active_pids, observed_pids = set(), set()
        concurrency_samples = []
        statement = "SELECT pg_sleep(0.02) /* synthetic bounded native work */"
        connection_status = ConnectionProxy.status

        async def observe_backend(connection, sql, **params):
            if sql != statement:
                return await connection_status(connection, sql, **params)
            pid = await connection.scalar("SELECT pg_backend_pid()")
            active_pids.add(pid)
            observed_pids.add(pid)
            concurrency_samples.append(len(active_pids))
            try:
                assert len(active_pids) <= 2
                return await connection_status(connection, sql, **params)
            finally:
                active_pids.remove(pid)

        async def shards():
            await admitted.gather(*(native._status_with_entity_address_tuning(statement) for _ in range(4)))

        async def nested_build(ctx, task):
            await build(ctx, task)
            await admitted.gather(*(shards() for _ in range(3)))
            async with database.transaction():
                await native._status_with_entity_address_tuning("SELECT 1")
                assert await admitted.validation_operations(database, lambda: database.scalar("SELECT 1")) == (1,)

        monkeypatch.setattr(ConnectionProxy, "status", observe_backend)
        monkeypatch.setattr(native, "process_entity_address_unified_data", nested_build)
        prepared = await asyncio.wait_for(_prepare(ctx, inputs, admission), timeout=10)
        assert max(concurrency_samples) == 2 and len(observed_pids) >= 2 and not active_pids
        await preparation.cleanup_prepared_entity_address_generation(prepared)


async def test_failed_source_drains_sibling_before_cleanup(monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        build = native.process_entity_address_unified_data
        started, stopped = asyncio.Event(), asyncio.Event()
        status = ConnectionProxy.status

        async def observe_worker(connection, statement, **params):
            if "synthetic interrupted source" not in statement:
                return await status(connection, statement, **params)
            started.set()
            try:
                return await status(connection, statement, **params)
            finally:
                stopped.set()

        async def fail_source():
            await started.wait()
            raise RuntimeError("synthetic source failed")

        async def interrupted_build(ctx, task):
            await build(ctx, task)
            await admitted.gather(
                native._status_with_entity_address_tuning("SELECT pg_sleep(5) /* synthetic interrupted source */"),
                fail_source(),
            )

        monkeypatch.setattr(ConnectionProxy, "status", observe_worker)
        monkeypatch.setattr(native, "process_entity_address_unified_data", interrupted_build)
        with pytest.raises(ExceptionGroup, match="TaskGroup"):
            await asyncio.wait_for(_prepare(ctx, inputs, admission), timeout=5)
        assert stopped.is_set()
        await _assert_stages_absent(database, schema)


async def test_nested_validation_borrows_owner_without_child_bypass(monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        admission.plan = replace(admission.plan, worker_count=1)
        admission.lease = _signed_lease(admission.plan)
        build = native.process_entity_address_unified_data

        async def child_read():
            return await admitted.read_scalar(database, "SELECT pg_backend_pid()")

        async def nested_build(ctx, task):
            await build(ctx, task)
            async with admitted.native_transaction():
                await native._apply_entity_address_transaction_settings()
                owner_pid = await database.scalar("SELECT pg_backend_pid()")
                await native._status_with_entity_address_tuning("SELECT 1")
                pids = await admitted.validation_operations(
                    database, lambda: database.scalar("SELECT pg_backend_pid()")
                )
                assert pids == (owner_pid,)
                child = asyncio.create_task(child_read())
                with pytest.raises(RuntimeError, match="cannot run in a child asyncio task"):
                    await asyncio.wait_for(child, timeout=2)

        monkeypatch.setattr(native, "process_entity_address_unified_data", nested_build)
        prepared = await asyncio.wait_for(_prepare(ctx, inputs, admission), timeout=10)
        await preparation.cleanup_prepared_entity_address_generation(prepared)


@pytest.mark.parametrize(
    "task", [{"refresh_mode": "provider-directory-partial"}, {"reuse_raw_stage": True}, {"limit_per_source": 1}]
)
async def test_admitted_recipe_rejects_unowned_partial_paths(task, monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        with pytest.raises(RuntimeError, match="full fresh build"):
            await preparation.prepare_provider_directory_entity_address(
                ctx, task, preparation_input=inputs, admission=admission, native_input_hash="a" * 64
            )
        await _assert_stages_absent(database, schema)


@pytest.mark.parametrize("semantic_date", [None, "2000-01-01"])
async def test_admitted_date_drift_precedes_stage_creation(monkeypatch, semantic_date):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        with pytest.raises(RuntimeError, match="semantic date differs"):
            await _prepare(ctx, replace(inputs, semantic_as_of=semantic_date), admission)
        await _assert_stages_absent(database, schema)


async def test_missing_backend_binding_rejects_build_and_cleans(monkeypatch):
    async with _temporary_schema() as (database, schema):
        ctx, inputs, admission, _states = await _admitted_fixture(database, schema, monkeypatch)
        monkeypatch.setattr(database, "acquire", None)
        with pytest.raises(RuntimeError, match="executing-session binding unavailable"):
            await _prepare(ctx, inputs, admission)
        await _assert_stages_absent(database, schema)
