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

_CATALOG = {"items": [{"source_ids": ["cms-npd"], "runnable": True, "profile_enabled": True}]}


async def _admit(directory, receipt, run_id, task=None):
    with support.release_probe_client(directory) as client:
        return await cms._run_acquired({"context": {}}, task or {}, run_id, directory, receipt, client)


async def _record_run(database, run_id, admission_result, *, parent=None, status="succeeded"):
    """Record synthetic terminal acquisition ancestry without rewriting the dataset owner."""
    await database.status(
        "INSERT INTO mrf.import_run (run_id,engine,node_id,importer,status,params,metrics,finished_at,retry_of_run_id) "
        "VALUES (:run_id,'healthcare-mrf-api','test-node','provider-directory-fhir',:status,"
        "CAST(:params AS json),CAST(:metrics AS json),now(),:parent)",
        run_id=run_id,
        status=status,
        parent=parent,
        params=json.dumps({"source_ids": ["cms-npd"], "import_resources": True}),
        metrics=json.dumps(admission_result),
    )


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
