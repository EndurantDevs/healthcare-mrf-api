# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained eight-file intake stops at a sealed candidate under an endpoint-wide owner."""

import asyncio
import json
from contextvars import Context
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api import provider_directory_cms_candidate_catalog as catalog
from process import provider_directory_cms_npd as cms
from tests import cms_npd_admission_postgres_support as support
from tests.cms_npd_admission_postgres_support import cms_artifact_root, fhir
from tests.test_cms_npd_admission_postgres import _admit_legacy_source
from tests.test_provider_directory_cms_npd import _dispatch_task, _retained_task

_CATALOG = {"items": [{"source_ids": ["cms-npd"], "runnable": True, "profile_enabled": True}]}


async def _admit(directory, receipt, run_id, task=None):
    with support.release_probe_client(directory) as client:
        return await cms._run_acquired({"context": {}}, task or {}, run_id, directory, receipt, client)


async def _record_run(database, run_id, admission_result, *, parent=None, status="succeeded", params=None):
    """Record synthetic terminal acquisition ancestry without rewriting the dataset owner."""
    await database.status(
        "INSERT INTO mrf.import_run (run_id,engine,node_id,importer,status,params,metrics,finished_at,retry_of_run_id) "
        "VALUES (:run_id,'healthcare-mrf-api','test-node','provider-directory-fhir',:status,"
        "CAST(:params AS json),CAST(:metrics AS json),CASE WHEN CAST(:status AS varchar)='running' THEN NULL ELSE now() END,:parent)",
        run_id=run_id,
        status=status,
        parent=parent,
        params=json.dumps(params if params is not None else {"source_ids": ["cms-npd"], "import_resources": True}),
        metrics=json.dumps(admission_result),
    )


async def _finish_run(database, run_id, metrics, status="failed"):
    await database.status(
        "UPDATE mrf.import_run SET status=:status,finished_at=now(),metrics=CAST(:metrics AS json) WHERE run_id=:run_id",
        status=status,
        metrics=json.dumps(metrics),
        run_id=run_id,
    )


async def _run_selected(directory, receipt, run_id, task):
    if task.get("cms_npd_retained_operation"):
        return await cms.run({"context": {}}, task, run_id)
    return await _admit(directory, receipt, run_id, task)


async def _failed_candidate(database, monkeypatch, directory, receipt, task, phase):
    """Retain real failed intake state without fabricating an accepted publication."""
    await _record_run(database, "failed-owner", {}, status="running", params=task)
    result_by_field = {}
    if phase == "acquiring":
        with monkeypatch.context() as interrupted:
            interrupted.setattr(
                cms, "_validate_candidate", AsyncMock(side_effect=RuntimeError("synthetic admission stop"))
            )
            with pytest.raises(RuntimeError, match="synthetic admission stop"):
                await _run_selected(directory, receipt, "failed-owner", task)
    else:
        result_by_field = await _run_selected(directory, receipt, "failed-owner", task)
    await _finish_run(database, "failed-owner", result_by_field)
    dataset_id = await database.scalar(
        "SELECT dataset_id FROM mrf.provider_directory_endpoint_dataset WHERE acquisition_root_run_id='failed-owner'"
    )
    state = await fhir._endpoint_dataset_state(dataset_id)
    assert state["status"] == phase and state["is_current"] is False and state["published_at"] is None
    return state


async def _assert_repair_owner_rejections(database, directory, receipt, task, old_state):
    """Active, successful and foreign owners cannot lose their sealed candidate."""
    for status in ("running", "succeeded"):
        await database.status(
            "UPDATE mrf.import_run SET status=:status,finished_at=CASE WHEN CAST(:status AS varchar)='running' THEN NULL ELSE now() END "
            "WHERE run_id='failed-owner'",
            status=status,
        )
        with pytest.raises(RuntimeError, match="cms_npd_(acquisition_lineage|repair_owner)_invalid"):
            await _run_selected(directory, receipt, "repair-owner", task)
        assert await fhir._endpoint_dataset_state(old_state["dataset_id"]) == old_state
    await _finish_run(database, "failed-owner", {})
    old_params = await database.scalar("SELECT params FROM mrf.import_run WHERE run_id='failed-owner'")
    await database.status(
        "UPDATE mrf.import_run SET params=CAST(:params AS json) WHERE run_id='failed-owner'",
        params=json.dumps({**old_params, "provider_directory_dispatch_id": "pdd_" + "e" * 32}),
    )
    with pytest.raises(RuntimeError, match="cms_npd_repair_owner_invalid"):
        await _run_selected(directory, receipt, "repair-owner", task)
    await database.status(
        "UPDATE mrf.import_run SET params=CAST(:params AS json) WHERE run_id='failed-owner'",
        params=json.dumps(old_params),
    )
    foreign_task_by_field = {**task, "provider_directory_dispatch_id": "pdd_" + "d" * 32}
    with pytest.raises(RuntimeError, match="cms_npd_acquisition_lineage_invalid"):
        await _run_selected(directory, receipt, "repair-owner", foreign_task_by_field)
    assert not await cms.recovery.is_disposed(fhir, old_state["dataset_id"])
    assert await fhir._endpoint_dataset_state(old_state["dataset_id"]) == old_state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation,phase,lose_creation",
    [
        ("baseline", "validated", False),
        ("baseline", "acquiring", True),
        ("rollback", "validated", True),
        (None, "validated", False),
    ],
)
async def test_repaired_intake_keeps_real_owners_and_reaches_catalog(
    monkeypatch, cms_artifact_root, operation, phase, lose_creation
):
    """Retire an exact failed generation and authenticate the fresh writer through genuine retries."""
    directory, receipt = support.retained_release(cms_artifact_root)
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: cms_artifact_root)
    migration_prefixes = support.LEGACY_MIGRATION_PREFIXES if operation == "rollback" else support.MIGRATION_PREFIXES
    async with support.admission_database(monkeypatch, migration_prefixes=migration_prefixes) as database:
        monkeypatch.setattr(catalog, "db", database)
        if operation == "rollback":
            await _admit_legacy_source(directory, receipt, "first-publication")
            next_directory, next_receipt = support.retained_release(cms_artifact_root, revision="next")
            await _admit_legacy_source(next_directory, next_receipt, "next-publication")
        source_task_by_field = (
            _retained_task(directory, receipt, operation)
            if operation
            else {"source_ids": ["cms-npd"], "import_resources": True, "full_refresh": True}
        )
        old_state = await _failed_candidate(
            database, monkeypatch, directory, receipt, _dispatch_task(source_task_by_field), phase
        )
        stale_fence = (
            await fhir._resolve_provider_directory_artifact_datasets(
                ["cms-npd"], should_select_validated_candidates=True
            )
            if phase == "validated"
            else None
        )
        repair_task_by_field = _dispatch_task(source_task_by_field, 1)
        await _record_run(database, "repair-owner", {}, status="running", params=repair_task_by_field)
        await _assert_repair_owner_rejections(database, directory, receipt, repair_task_by_field, old_state)
        current_run_id = "repair-owner"
        if lose_creation:
            with monkeypatch.context() as interrupted:
                interrupted.setattr(cms, "_candidate", AsyncMock(side_effect=RuntimeError("synthetic creation stop")))
                with pytest.raises(RuntimeError, match="synthetic creation stop"):
                    await _run_selected(directory, receipt, current_run_id, repair_task_by_field)
            await _finish_run(database, current_run_id, {})
            repair_task_by_field = {**repair_task_by_field, "provider_directory_pagination_root_run_id": current_run_id}
            await _record_run(
                database, "repair-retry", {}, parent=current_run_id, status="running", params=repair_task_by_field
            )
            current_run_id = "repair-retry"
        admission_result = await _run_selected(directory, receipt, current_run_id, repair_task_by_field)
        await _finish_run(database, current_run_id, admission_result, "succeeded")
        await _assert_repaired_candidate(database, old_state, admission_result, current_run_id, stale_fence)
        assert await cms.recovery.is_same_acquisition_replay(fhir, current_run_id, repair_task_by_field, current_run_id)
        with pytest.raises(RuntimeError, match="cms_npd_acquisition_lineage_invalid"):
            await cms.recovery.is_same_acquisition_replay(
                fhir,
                current_run_id,
                {**repair_task_by_field, "provider_directory_pagination_root_run_id": "unrelated"},
                current_run_id,
            )
        if lose_creation:
            await _assert_first_writer_replay(database, directory, receipt, repair_task_by_field, current_run_id)


async def _assert_first_writer_replay(database, directory, receipt, task, writer_run_id):
    """A later genuine descendant reuses the first writer even when the generation root never wrote."""
    await _record_run(database, "later-retry", {}, parent=writer_run_id, status="running", params=task)
    result = await _run_selected(directory, receipt, "later-retry", task)
    await _finish_run(database, "later-retry", result, "succeeded")
    assert result["cms_serving_candidate"]["desired_cms_dataset"]["acquisition_root_run_id"] == writer_run_id
    assert await cms.recovery.is_same_acquisition_replay(fhir, writer_run_id, task, "later-retry")
    assert not await cms.recovery.is_same_acquisition_replay(fhir, "failed-owner", task, "later-retry")
    assert (await catalog.cms_serving_candidate(_CATALOG))["acquisition_run_id"] == "later-retry"


async def _assert_repaired_candidate(database, old_state, result, current_run_id, stale_fence):
    """Verify immutable retirement, real catalog lineage and rejection of an already captured old cutover fence."""
    old_id = old_state["dataset_id"]
    assert await cms.recovery.is_disposed(fhir, old_id)
    old_after = await fhir._endpoint_dataset_state(old_id)
    if old_state["status"] == "validated":
        assert old_after == old_state
    else:
        assert old_after["status"] == "failed"
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_dataset_resource WHERE dataset_id=:dataset_id",
                dataset_id=old_id,
            )
            == 0
        )
    assert old_after["acquisition_root_run_id"] == old_after["import_run_id"] == "failed-owner"
    fresh = await fhir._endpoint_dataset_state(result["dataset_id"])
    assert result["dataset_id"] != old_id and fresh["status"] == "validated" and fresh["is_current"] is False
    assert fresh["acquisition_root_run_id"] == fresh["import_run_id"] == current_run_id
    projected = await catalog.cms_serving_candidate(_CATALOG)
    assert projected == {**result["cms_serving_candidate"], "acquisition_run_id": current_run_id}
    if stale_fence is not None:
        with pytest.raises(RuntimeError, match="candidate_changed"):
            async with database.transaction():
                await fhir._lock_artifact_cutover_fence(stale_fence)


async def _assert_unpublished(database, admission_result):
    """Verify exact scalar seals and every retained resource family without source-only publication."""
    assert admission_result["status"] == "validated" and "dataset_followup" not in admission_result
    descriptor = admission_result["cms_serving_candidate"]
    assert set(descriptor) == {
        "version",
        "status",
        "desired_cms_dataset",
        "expected_cms_incumbent",
        "release_id",
        "proof_version",
    }
    assert descriptor["version"] == 1 and descriptor["status"] == "ready" and descriptor["proof_version"] == 2
    assert descriptor["expected_cms_incumbent"] is None
    state = await fhir._endpoint_dataset_state(admission_result["dataset_id"])
    assert descriptor["desired_cms_dataset"] == cms._cms_dataset_pair(state, allow_desired=True)
    assert descriptor["release_id"] == admission_result["vector_sha256"]
    assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_endpoint_dataset WHERE is_current") == 0
    assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_cms_serving_coverage") == 0
    proof = await database.first(
        "SELECT endpoint_id,dataset_hash,release_id,proof_version FROM mrf.provider_directory_cms_candidate_coverage "
        "WHERE dataset_id=:dataset_id",
        dataset_id=admission_result["dataset_id"],
    )
    assert tuple(proof) == (state["endpoint_id"], state["dataset_hash"], admission_result["vector_sha256"], 2)
    resources = await database.all(
        "SELECT resource_type,count(*) FROM mrf.provider_directory_cms_npd_resource_witness "
        "WHERE dataset_id=:dataset_id GROUP BY resource_type",
        dataset_id=admission_result["dataset_id"],
    )
    assert {resource_row[0] for resource_row in resources} == set(cms.RESOURCE_TYPES)
    assert sum(resource_row[1] for resource_row in resources) == admission_result["resource_count"]


@pytest.mark.asyncio
async def test_selected_baseline_intake_replay_and_unpublished_rollback_rejection(monkeypatch, cms_artifact_root):
    """A pinned eight-file input seals a normal candidate; failed rollback cannot dispose it."""
    directory, receipt = support.retained_release(cms_artifact_root)
    task = _retained_task(directory, receipt)
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: cms_artifact_root)
    async with support.admission_database(monkeypatch) as database:
        result = await cms.run({"context": {}}, task, "retained-owner")
        await _assert_unpublished(database, result)
        assert result["cms_retained_input"] == cms.retained_input_selection(task)
        before = await fhir._endpoint_dataset_state(result["dataset_id"])
        retry_task_by_field = {**task, "provider_directory_pagination_root_run_id": "retained-owner"}
        retry = await cms.run({"context": {}}, retry_task_by_field, "retained-retry")
        assert retry["cms_serving_candidate"] == result["cms_serving_candidate"]
        assert await fhir._endpoint_dataset_state(result["dataset_id"]) == before
        other_directory, other_receipt = support.retained_release(cms_artifact_root, revision="other")
        rollback_task = _retained_task(other_directory, other_receipt, "rollback")
        with pytest.raises(RuntimeError, match="cms_npd_rollback_prior_publication_missing"):
            await cms.run({"context": {}}, rollback_task, "unpublished-rollback")
        assert await fhir._endpoint_dataset_state(result["dataset_id"]) == before
        assert not await cms.recovery.is_disposed(fhir, result["dataset_id"])
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_endpoint_dataset") == 1
        await _assert_unpublished(database, result)
        receipt_path = directory / "receipt.json"
        receipt_path.write_bytes(receipt_path.read_bytes() + b"\n")
        with pytest.raises(cms.source.CmsNpdSourceError, match="cms_npd_retained_receipt_changed"):
            await cms.run({"context": {}}, retry_task_by_field, "tampered-retry")
        assert await fhir._endpoint_dataset_state(result["dataset_id"]) == before


@pytest.mark.asyncio
async def test_selected_rollback_uses_real_publication_history_without_republishing(monkeypatch, cms_artifact_root):
    """Legacy source publications exercise predecessor SQL; retained intake still stops before composite serving."""
    directory, receipt = support.retained_release(cms_artifact_root)
    next_directory, next_receipt = support.retained_release(cms_artifact_root, revision="next")
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: cms_artifact_root)
    async with support.admission_database(
        monkeypatch, migration_prefixes=support.LEGACY_MIGRATION_PREFIXES
    ) as database:
        first = await _admit_legacy_source(directory, receipt, "first-owner")
        first_state = await fhir._endpoint_dataset_state(first["dataset_id"])
        with pytest.raises(RuntimeError, match="cms_npd_baseline_prior_publication_exists"):
            await cms.run({"context": {}}, _retained_task(next_directory, next_receipt), "late-baseline")
        assert await fhir._endpoint_dataset_state(first["dataset_id"]) == first_state
        second = await _admit_legacy_source(next_directory, next_receipt, "second-owner")
        second_state = await fhir._endpoint_dataset_state(second["dataset_id"])
        rollback_task = _retained_task(directory, receipt, "rollback")
        rollback = await cms.run({"context": {}}, rollback_task, "rollback-owner")
        descriptor = rollback["cms_serving_candidate"]
        assert rollback["status"] == "validated" and descriptor["release_id"] == receipt["vector_sha256"]
        assert descriptor["expected_cms_incumbent"] == cms._cms_dataset_pair(second_state)
        assert rollback["cms_retained_input"] == cms.retained_input_selection(rollback_task)
        candidate_state = await fhir._endpoint_dataset_state(rollback["dataset_id"])
        assert candidate_state["previous_dataset_id"] == second["dataset_id"]
        assert rollback["dataset_id"] not in {first["dataset_id"], second["dataset_id"]}
        retry = await cms.run(
            {"context": {}},
            {**rollback_task, "provider_directory_pagination_root_run_id": "rollback-owner"},
            "rollback-retry",
        )
        assert retry["cms_serving_candidate"] == descriptor
        assert await fhir._endpoint_dataset_state(rollback["dataset_id"]) == candidate_state
        assert await fhir._endpoint_dataset_state(second["dataset_id"]) == second_state
        assert (
            await database.scalar("SELECT dataset_id FROM mrf.provider_directory_endpoint_dataset WHERE is_current")
            == second["dataset_id"]
        )


@pytest.mark.asyncio
async def test_single_connection_pool_rejects_before_guard_or_staging(monkeypatch, cms_artifact_root):
    """Reject an inadequate pool promptly without staging rows or retaining its only backend."""
    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "1")
    directory, receipt = support.retained_release(cms_artifact_root)
    async with support.admission_database(monkeypatch) as database:
        acquire = AsyncMock(wraps=fhir._acquire_provider_directory_artifact_build_lock)
        staging = AsyncMock(side_effect=AssertionError("insufficient pool reached staging"))
        monkeypatch.setattr(fhir, "_acquire_provider_directory_artifact_build_lock", acquire)
        monkeypatch.setattr(cms, "_stage_acquired", staging)
        async with asyncio.timeout(2):
            with pytest.raises(RuntimeError, match="cms_npd_intake_pool_capacity_exceeded"):
                await _admit(directory, receipt, "single-connection")
            assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_endpoint_dataset") == 0
            assert await database.scalar(
                "SELECT NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' "
                "AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))"
            )
        acquire.assert_not_awaited()
        staging.assert_not_awaited()


@pytest.mark.asyncio
async def test_complete_intake_and_retry_preserve_owner_and_never_publish(monkeypatch, cms_artifact_root):
    """Use real migrations, admission, coverage and catalog SQL through retry and changed-vector recovery."""
    directory, receipt = support.retained_release(cms_artifact_root)
    async with support.admission_database(monkeypatch) as database:
        monkeypatch.setattr(catalog, "db", database)
        followup = AsyncMock(side_effect=AssertionError("intake must not request source-only follow-up"))
        monkeypatch.setattr(fhir, "_source_local_dataset_followup_if_current", followup)
        admission_result = await _admit(directory, receipt, "owner")
        await _assert_unpublished(database, admission_result)
        before = await fhir._endpoint_dataset_state(admission_result["dataset_id"])
        replay = await _admit(directory, receipt, "retry")
        after = await fhir._endpoint_dataset_state(admission_result["dataset_id"])
        assert replay["cms_serving_candidate"] == admission_result["cms_serving_candidate"]
        assert after == before and after["import_run_id"] == after["acquisition_root_run_id"] == "owner"
        await _record_run(database, "owner", {}, status="failed")
        await _record_run(database, "retry", replay, parent="owner")
        assert await catalog.cms_serving_candidate(_CATALOG) == {
            **replay["cms_serving_candidate"],
            "acquisition_run_id": "retry",
        }
        next_directory, next_receipt = support.retained_release(cms_artifact_root, revision="next")
        next_result = await _admit(next_directory, next_receipt, "next-owner")
        await _assert_unpublished(database, next_result)
        await _record_run(database, "next-owner", next_result)
        assert (await catalog.cms_serving_candidate(_CATALOG))["acquisition_run_id"] == "next-owner"
        assert await cms.recovery.is_disposed(fhir, admission_result["dataset_id"])
        followup.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancellation_after_seal_replays_without_owner_or_publication_change(monkeypatch, cms_artifact_root):
    """A lost ready acknowledgement leaves immutable proof reusable and releases the intake guard."""
    directory, receipt = support.retained_release(cms_artifact_root)
    async with support.admission_database(monkeypatch) as database:
        with monkeypatch.context() as cancelled:
            cancelled.setattr(cms, "_serving_candidate_descriptor", AsyncMock(side_effect=asyncio.CancelledError))
            with pytest.raises(asyncio.CancelledError):
                await _admit(directory, receipt, "interrupted-owner")
        before = await database.first(
            "SELECT dataset_id,import_run_id,acquisition_root_run_id FROM mrf.provider_directory_endpoint_dataset"
        )
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_cms_candidate_coverage") == 1
        admission_result = await _admit(directory, receipt, "retry-leaf")
        await _assert_unpublished(database, admission_result)
        after = await database.first(
            "SELECT dataset_id,import_run_id,acquisition_root_run_id FROM mrf.provider_directory_endpoint_dataset"
        )
        assert (
            tuple(after) == tuple(before) == (admission_result["dataset_id"], "interrupted-owner", "interrupted-owner")
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("is_rollback", [False, True])
async def test_endpoint_guard_serializes_distinct_vector_intake_and_rollback(
    monkeypatch, cms_artifact_root, is_rollback
):
    """Different retained directories cannot bypass the same endpoint owner, and cancellation frees it."""
    directory, receipt = support.retained_release(cms_artifact_root)
    other_directory, other_receipt = support.retained_release(cms_artifact_root, revision="other")
    async with support.admission_database(monkeypatch) as database:
        monkeypatch.setattr(fhir, "PROVIDER_DIRECTORY_ARTIFACT_BUILD_LOCK_ATTEMPTS", 1)
        entered, release = asyncio.Event(), asyncio.Event()
        original = cms._stage_acquired

        async def pause_before_stage(*args):
            entered.set()
            await release.wait()
            return await original(*args)

        monkeypatch.setattr(cms, "_stage_acquired", pause_before_stage)
        owner = asyncio.create_task(_admit(directory, receipt, "owner"), context=Context())
        try:
            await asyncio.wait_for(entered.wait(), 5)
            task = {"cms_npd_rollback_vector_sha256": other_receipt["vector_sha256"]} if is_rollback else {}
            with pytest.raises(RuntimeError, match="cms_npd_acquisition_in_progress"):
                await _admit(other_directory, other_receipt, "contender", task)
            assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_endpoint_dataset") == 0
        finally:
            owner.cancel()
            with pytest.raises(asyncio.CancelledError):
                await owner
        monkeypatch.setattr(cms, "_stage_acquired", original)
        admission_result = await _admit(directory, receipt, "after-cancellation")
        await _assert_unpublished(database, admission_result)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_active", [False, True])
async def test_disconnected_intake_owner_cannot_emit_ready(monkeypatch, cms_artifact_root, is_active):
    """Terminate only the owned guard backend and require the interrupted intake to fail closed."""
    directory, receipt = support.retained_release(cms_artifact_root)
    async with support.admission_database(monkeypatch) as database:
        acquired, release = asyncio.Event(), asyncio.Event()
        backend_ids = []
        original_acquire = fhir._acquire_provider_directory_artifact_build_lock
        original_stage = cms._stage_acquired

        async def capture_guard(*args):
            connection = await original_acquire(*args)
            backend_ids.append(await connection.scalar(text("SELECT pg_backend_pid()")))
            await connection.commit()
            return connection

        async def pause_stage(*args):
            acquired.set()
            await release.wait()
            return await original_stage(*args)

        monkeypatch.setattr(fhir, "_acquire_provider_directory_artifact_build_lock", capture_guard)
        monkeypatch.setattr(cms, "_stage_acquired", pause_stage)
        owner = asyncio.create_task(_admit(directory, receipt, "disconnected-owner"), context=Context())
        try:
            await asyncio.wait_for(acquired.wait(), 5)
            assert await database.scalar(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=:pid AND datname=current_database()",
                pid=backend_ids[0],
            )
            if not is_active:
                release.set()
            with pytest.raises((RuntimeError, DBAPIError)) as failure:
                await asyncio.wait_for(owner, 15)
            if isinstance(failure.value, RuntimeError):
                assert str(failure.value) == "cms_npd_intake_guard_lost"
            else:
                assert failure.value.connection_invalidated
        finally:
            if not owner.done():
                owner.cancel()
                await asyncio.gather(owner, return_exceptions=True)
        assert (
            await database.scalar("SELECT count(*) FROM mrf.provider_directory_endpoint_dataset WHERE is_current") == 0
        )
        monkeypatch.setattr(cms, "_stage_acquired", original_stage)
        admission_result = await _admit(directory, receipt, "after-disconnect")
        await _assert_unpublished(database, admission_result)
