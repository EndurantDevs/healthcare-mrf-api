# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary PostgreSQL recovery preserves physical heaps and caller rollback."""

import asyncio
import importlib
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import entity_address_result_generation as generations
from process import provider_directory_artifact_read_access as read_access
from process import provider_directory_generic_source_artifacts as artifacts
from process import provider_directory_serving_recovery as recovery
from process import provider_directory_serving_recovery_receipts as receipts
from process.entity_address_cutover_contract import postgres_sqlstate
from process.entity_address_unified import _sql_literal
from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case


async def _state(case):
    async with case.database.transaction():
        return await receipts.state(case.database, case.schema)


async def _publish_overlay(case, generation):
    database, schema = case.database, case.schema
    request = SimpleNamespace(
        metrics={recovery.GENERATION_PARAM: generation}, publish_artifacts_targets={"address_overlay"}, run_id=None
    )
    assert await recovery.resume_overlay(case.directory, case.fence, request) is None
    assert await recovery.resume_address(database, {}, _address_request(case, generation)) is None
    await database.status(f'CREATE TABLE "{schema}".overlay_next (marker text PRIMARY KEY)')
    await database.status(f"INSERT INTO \"{schema}\".overlay_next VALUES ('new')")
    selected_by_field = dict(
        generation=generation,
        source=case.pin,
        run_id=None,
        stage=SimpleNamespace(schema=schema, stage_table="overlay_next", target_relation=receipts.ROOTS[0]),
    )
    async with database.transaction():
        await recovery.begin_overlay(case.directory, selected_by_field)
        await receipts.lock(database, schema, (*receipts.ROOTS, "overlay_next"))
        await read_access.recheck_ordinary_read_access(database, selected_by_field["read_access"])
        await receipts.retain(database, schema, selected_by_field["before"]["roots"][receipts.ROOTS[0]])
        await receipts.rename(database, schema, "overlay_next", receipts.ROOTS[0])
        await recovery.finish_overlay(case.directory, selected_by_field)
    resumed = await recovery.resume_overlay(case.directory, case.fence, request)
    assert resumed["serving_recovery"] == {"generation": generation, "phase": "overlay"}
    return request


async def _publish_address(case, generation):
    database, schema = case.database, case.schema
    await database.status(f'CREATE TABLE "{schema}".address_next (marker text PRIMARY KEY)')
    await database.status(f"INSERT INTO \"{schema}\".address_next VALUES ('new')")
    swaps = [
        SimpleNamespace(
            live_cls=SimpleNamespace(__main_table__=receipts.ROOTS[1]),
            stage_cls=SimpleNamespace(__tablename__="address_next"),
        )
    ]
    context_by_field = {
        recovery.GENERATION_PARAM: generation,
        "partial_provider_directory_source_ids": [case.pin["source_id"]],
        "partial_provider_directory_dataset_id": case.pin["dataset_id"],
        "partial_provider_directory_run_id": case.pin["parent_run_id"],
    }
    async with recovery.address_transaction(database, "50ms", _sql_literal, context_by_field):
        await recovery.prepare_address(database, schema, swaps, context_by_field)
        await receipts.lock(database, schema, (*receipts.ROOTS, "address_next"))
        await recovery.before_address(database, schema, swaps, context_by_field)
        await receipts.rename(database, schema, "address_next", receipts.ROOTS[1])
        await generations.publish_local_entity_address_generation(database, schema_name=schema)
        await recovery.after_address(database, schema, context_by_field)
    resumed = await recovery.resume_address(database, {}, _address_request(case, generation))
    assert resumed["serving_recovery"] == {"generation": generation, "phase": "published"}


def _address_request(case, generation):
    return {
        recovery.GENERATION_PARAM: generation,
        "refresh_mode": "provider-directory-partial",
        "serving_only_refresh": True,
        "publish": True,
        "provider_directory_partial_scope": "latest-run",
        "provider_directory_source_ids": [case.pin["source_id"]],
        "provider_directory_dataset_id": case.pin["dataset_id"],
        "provider_directory_run_id": case.pin["parent_run_id"],
    }


async def _forward(case, generation):
    request = await _publish_overlay(case, generation)
    await _publish_address(case, generation)
    resumed = await artifacts.publish_bundle(case.directory, case.fence, request, artifact_resource_types={"Location"})
    assert resumed["serving_recovery"]["phase"] == "published"
    return await _state(case)


@pytest.mark.asyncio
async def test_native_ordinary_recovery_replays_and_preserves_failed_transaction(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        baseline = await _state(case)
        generation = recovery.publication_generation(case.pin)
        forward = await _forward(case, generation)
        task_by_field = {
            recovery.OPERATION_PARAM: "recover",
            recovery.GENERATION_PARAM: generation,
            "source_ids": [case.pin["source_id"]],
        }
        async with case.database.transaction():
            history = await recovery._load_history(case.database, case.schema, generation, for_update=True)
            assert set(history) == {"overlay", "published"}
        original_append = recovery._append

        async def fail_after_append(*args, **kwargs):
            await original_append(*args, **kwargs)
            if args[3] == "recovered":
                await case.database.status("SELECT 1/0")

        with monkeypatch.context() as failure:
            failure.setattr(recovery, "_append", fail_after_append)
            with pytest.raises(DBAPIError) as failure:
                await recovery._recover(case.directory, task_by_field)
            assert postgres_sqlstate(failure.value) == "22012"
        assert await _state(case) == forward
        async with case.database.transaction():
            assert set(await recovery._load_history(case.database, case.schema, generation)) == {"overlay", "published"}
        recover_once = recovery._recover_once

        async def lose_acknowledgment(directory, request):
            await recover_once(directory, request)
            raise ConnectionError("publication acknowledgment lost")

        with monkeypatch.context() as lost_ack:
            lost_ack.setattr(recovery, "_recover_once", lose_acknowledgment)
            outcome_by_field = await recovery._recover(case.directory, task_by_field)
        restored = await _state(case)
        assert restored["roots"] == baseline["roots"]
        assert (
            restored["metadata"]["address"]["local_generation"]
            == forward["metadata"]["address"]["local_generation"] + 1
        )
        assert outcome_by_field == await recovery._recover(case.directory, task_by_field)
        assert await recovery._verify_recovered(case.directory, task_by_field) == outcome_by_field
        assert await _state(case) == restored
        assert outcome_by_field["serving_recovery"]["current_acquired"] == case.pin
        await _refuse_late_source(case, task_by_field, restored)
        async with case.database.transaction():
            assert set(await recovery._load_history(case.database, case.schema, generation)) == {
                "overlay",
                "published",
                "recovered",
            }


@pytest.mark.asyncio
async def test_native_ordinary_copy_catalog_and_cleanup_keep_exact_identity(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        database, directory, schema = case.database, case.directory, case.schema
        name, created = "ordinary_source_copy", {}
        admission = SimpleNamespace(fence=case.fence, prepared={}, resource_types=frozenset())
        await artifacts._prepare_model(directory, admission, directory.ProviderDirectorySource, name, created)
        original = await artifacts._catalog(directory, name)
        assert tuple(original) == created[name]
        assert await database.scalar(f'SELECT count(*) FROM "{schema}"."{name}"') == 1
        with pytest.raises(RuntimeError, match="rest_artifact_stage_exists"):
            await artifacts._prepare_model(directory, admission, directory.ProviderDirectorySource, name, {})
        assert await artifacts._catalog(directory, name) == original
        await artifacts._drain_cleanup(directory, created)
        await artifacts._cleanup(directory, created)
        assert await artifacts._catalog(directory, name) is None


@pytest.mark.asyncio
async def test_native_address_wrapper_replays_without_republishing(monkeypatch):
    entity_address_unified = importlib.import_module("process.entity_address_unified")

    async with ordinary_serving_case(monkeypatch) as case:
        generation = recovery.publication_generation(case.pin)
        published = await _forward(case, generation)
        async with case.database.transaction():
            history = await recovery._load_history(case.database, case.schema, generation)
        monkeypatch.setattr(entity_address_unified, "db", case.database)
        context_by_field = {"context": {"existing_value": "preserved"}}
        replayed = await entity_address_unified.process_entity_address_unified_data(
            context_by_field, _address_request(case, generation)
        )
        assert replayed["serving_recovery"] == {"generation": generation, "phase": "published"}
        assert replayed["rows"] == published["roots"][receipts.ROOTS[1]]["rows"]
        assert context_by_field["context"]["existing_value"] == "preserved"
        assert context_by_field["context"]["run"] == 0
        assert await _state(case) == published
        async with case.database.transaction():
            assert await recovery._load_history(case.database, case.schema, generation) == history


@pytest.mark.asyncio
async def test_native_access_and_cleanup_refuse_substituted_relations(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        database, directory, schema = case.database, case.directory, case.schema
        original = await _state(case)
        async with database.transaction():
            with pytest.raises(RuntimeError, match="ordinary_address_read_access_relations_changed"):
                await read_access._ordinary_access(database, schema, receipts.ROOTS[0], receipts.ROOTS[0])
        assert await _state(case) == original
        name, created_by_relation = "replaced_source_copy", {}
        admission = SimpleNamespace(fence=case.fence, prepared={}, resource_types=frozenset())
        await artifacts._prepare_model(
            directory, admission, directory.ProviderDirectorySource, name, created_by_relation
        )
        table = receipts.qualified(schema, name)
        await database.status(f"DROP TABLE {table}")
        await database.status(f"CREATE TABLE {table} (marker text PRIMARY KEY)")
        await database.status(f"INSERT INTO {table} VALUES ('replacement')")
        replacement = await artifacts._catalog(directory, name)
        assert tuple(replacement) != created_by_relation[name]
        with pytest.raises(RuntimeError, match="rest_artifact_cleanup_custody_changed"):
            await artifacts._cleanup(directory, created_by_relation)
        assert await artifacts._catalog(directory, name) == replacement
        assert await database.scalar(f"SELECT marker FROM {table}") == "replacement"
        assert await _state(case) == original


@pytest.mark.asyncio
async def test_native_geo_restore_accepts_absent_optional_table(monkeypatch):
    from api.ptg2_geo_projection import GEO_ASSURANCE_STATE_TABLE

    async with ordinary_serving_case(monkeypatch) as case:
        await case.database.status(f"DROP TABLE {receipts.qualified(case.schema, GEO_ASSURANCE_STATE_TABLE)}")
        without_geo = await _state(case)
        assert without_geo["metadata"]["geo"] is None
        async with case.database.transaction():
            await recovery._restore_geo(case.database, case.schema, without_geo)
        assert await _state(case) == without_geo


async def _refuse_late_source(case, task_by_field, restored):
    await case.database.status(
        f"UPDATE \"{case.schema}\".provider_directory_endpoint_dataset SET dataset_hash=repeat('b',64) "
        "WHERE dataset_id=:dataset",
        dataset=case.pin["dataset_id"],
    )
    with pytest.raises(ValueError, match="current_source_changed"):
        await recovery._recover(case.directory, task_by_field)
    assert await _state(case) == restored


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["content", "acl", "occupied"])
async def test_native_recovery_refuses_retained_drift(monkeypatch, damage):
    async with ordinary_serving_case(monkeypatch) as case:
        baseline = await _state(case)
        generation = recovery.publication_generation(case.pin)
        forward = await _forward(case, generation)
        previous = receipts.retained_name(baseline["roots"][receipts.ROOTS[0]]["native"]["oid"])
        if damage == "content":
            await case.database.status(f'UPDATE "{case.schema}"."{previous}" SET marker=\'changed\'')
            error = "content_changed"
        elif damage == "acl":
            await case.database.status(f'GRANT UPDATE(marker) ON "{case.schema}"."{previous}" TO PUBLIC')
            error = "native_acl_changed"
        else:
            occupied = receipts.retained_name(forward["roots"][receipts.ROOTS[0]]["native"]["oid"])
            await case.database.status(f'CREATE TABLE "{case.schema}"."{occupied}" (marker text)')
            error = "retained_name_occupied"
        task_by_field = {
            recovery.OPERATION_PARAM: "recover",
            recovery.GENERATION_PARAM: generation,
            "source_ids": [case.pin["source_id"]],
        }
        with pytest.raises(ValueError, match=error):
            await recovery._recover(case.directory, task_by_field)
        assert await _state(case) == forward
        async with case.database.transaction():
            assert set(await recovery._load_history(case.database, case.schema, generation)) == {"overlay", "published"}


@pytest.mark.asyncio
async def test_native_copy_repairs_npi_without_mutating_incumbent(monkeypatch):
    from process import provider_directory_output_npi_backfill as npi
    from process.provider_directory_artifact_scratch_inputs import _verify_set_parity

    async with ordinary_serving_case(monkeypatch) as case:
        database, directory, schema = case.database, case.directory, case.schema
        model, name = directory.ProviderDirectoryPractitioner, "ordinary_practitioner_copy"
        await database.status(
            f'INSERT INTO "{schema}".provider_directory_practitioner(source_id,resource_id) '
            "VALUES ('source_primary','1000000004')"
        )
        async with database.transaction() as session:
            await directory._create_provider_directory_artifact_scope_layout(
                model, schema, name, status_executor=database.status
            )
            inputs, expected, _count, params_by_name = npi.npi_projection(
                directory, model.__tablename__, dict(source_ids=["source_primary"], run_id=None)
            )
            decision = await database.scalar("SELECT localtimestamp")
            params_by_name["update_timestamp"] = decision
            enrichment_by_field = dict(
                plan=dict(decision_at=decision.isoformat()),
                metrics=dict(resource_id_npis_backfilled=dict(Practitioner=0)),
            )
            assert (
                await artifacts._copy(
                    directory, session, model, name, inputs, params_by_name, enrichment=enrichment_by_field, kind="npi"
                )
                == 1
            )
            await directory._build_artifact_scope_pk(model, schema, name, status_executor=database.status)
            await _verify_set_parity(session, expected, receipts.qualified(schema, name), params_by_name)
            assert enrichment_by_field["metrics"]["resource_id_npis_backfilled"] == {"Practitioner": 1}
            assert await database.scalar(f'SELECT npi FROM "{schema}"."{name}"') == 1000000004
            assert await database.scalar(f'SELECT npi FROM "{schema}".provider_directory_practitioner') is None


@pytest.mark.asyncio
async def test_native_overlay_marker_requires_issued_scope(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        request = SimpleNamespace(
            metrics={}, publish_artifacts_targets={"address_overlay"}, publish_corroboration=False
        )
        artifacts._require_published_overlay(case.directory, case.fence, request)
        async with artifacts.publication_scope(case.directory, case.fence, request, {"Location"}):
            assert artifacts.current(case.directory) is None
            assert recovery.preparation_timeout(case.directory, 7) == 7
            recovery.attach_overlay_stage(case.directory, object())
        request.metrics[recovery.GENERATION_PARAM] = recovery.publication_generation(case.pin)
        with pytest.raises(RuntimeError, match="rest_artifact_recovery_admission_required"):
            async with artifacts.publication_scope(case.directory, case.fence, request, {"Location"}):
                pytest.fail("an unissued recovery marker entered publication")


@pytest.mark.asyncio
async def test_native_metadata_refuses_ambiguous_receipt_tips(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        table = receipts.qualified(case.schema, "provider_directory_cms_serving_receipt")
        await case.database.status(
            f"CREATE TABLE {table} (receipt_id text PRIMARY KEY, predecessor_receipt_id text, payload jsonb)"
        )
        async with case.database.transaction():
            assert (await receipts.metadata(case.database, case.schema))["common_receipt"] is None
            await case.database.status(f"INSERT INTO {table} VALUES ('root',NULL,'{{}}')")
            assert (await receipts.metadata(case.database, case.schema))["common_receipt"] == {
                "receipt_id": "root",
                "payload": {},
            }
        await case.database.status(f"INSERT INTO {table} VALUES ('branch_a','root','{{}}'),('branch_b','root','{{}}')")
        async with case.database.transaction():
            with pytest.raises(ValueError, match="common_history_changed"):
                await receipts.metadata(case.database, case.schema)


async def _wait_for_lock(database, relation_oid):
    async with asyncio.timeout(3):
        while not await database.scalar(
            "SELECT EXISTS(SELECT FROM pg_locks pending JOIN pg_stat_activity activity ON activity.pid=pending.pid "
            "WHERE activity.usename=current_user AND pending.pid<>pg_backend_pid() "
            "AND pending.relation=CAST(:relation_oid AS oid) AND NOT pending.granted)",
            relation_oid=relation_oid,
        ):
            await asyncio.sleep(0.01)


async def _copy_again(case, name):
    async with case.database.transaction() as session:
        return await artifacts._copy(
            case.directory,
            session,
            case.directory.ProviderDirectorySource,
            name,
            case.directory._provider_directory_artifact_source_select_sql(
                case.directory.ProviderDirectorySource, case.schema
            ),
            {"source_ids": [case.pin["source_id"]]},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation_kind", ["copy", "cleanup"])
async def test_native_cancellation_drains_owned_copy_and_cleanup(monkeypatch, operation_kind):
    async with ordinary_serving_case(monkeypatch) as case:
        database, directory, schema = case.database, case.directory, case.schema
        name, created_by_relation = "cancellable_source_copy", {}
        admission = SimpleNamespace(fence=case.fence, prepared={}, resource_types=frozenset())
        await artifacts._prepare_model(
            directory, admission, directory.ProviderDirectorySource, name, created_by_relation
        )
        operation = None
        try:
            async with database.engine.connect() as blocker, blocker.begin():
                await blocker.execute(text(f'LOCK TABLE "{schema}"."{name}" IN ACCESS EXCLUSIVE MODE'))
                pending = (
                    artifacts._drain_cleanup(directory, created_by_relation)
                    if operation_kind == "cleanup"
                    else _copy_again(case, name)
                )
                operation = asyncio.create_task(pending)
                await _wait_for_lock(database, created_by_relation[name][0])
                operation.cancel()
                await asyncio.sleep(0)
                if operation_kind == "cleanup":
                    assert not operation.done(), "cleanup must drain its original native DROP"
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(operation, 3)
        finally:
            if operation is not None:
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)
        if operation_kind == "cleanup":
            assert await artifacts._catalog(directory, name) is None
        else:
            assert await database.scalar(f'SELECT count(*) FROM "{schema}"."{name}"') == 1
            await artifacts._cleanup(directory, created_by_relation)
            assert await artifacts._catalog(directory, name) is None
