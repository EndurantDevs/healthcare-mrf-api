# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native Kentucky completion fences, including independent Massachusetts owners."""

import asyncio
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, update

from process import kentucky_profile_completion as kentucky
from process import massachusetts_profile_completion as massachusetts
from process import provider_profile_source_completion as shared_completion
from process import provider_profile_source_store as shared_store
from process.control_cancel import ImportCancelledError
from tests.test_kentucky_profile_store import _seed_kentucky_run
from tests.test_massachusetts_profile_completion import (
    CONTROL_RUN_ID as MA_CONTROL_RUN_ID, _completion_database, _context as _ma_context,
    _fail_transaction_exit, _prepared_run, _stored_state,
)
from tests.test_massachusetts_profile_store import _metrics

CONTROL_RUN_ID = "run_ky_completion_synthetic"
ATTEMPT = {"attempt_id": CONTROL_RUN_ID + ":" + "c" * 32,
           "attempt_started_at": "2026-09-08T08:00:00.000000+00:00"}


def _context():
    return {"context": {"control_run_id": CONTROL_RUN_ID,
                        "_control_attempt_id": ATTEMPT["attempt_id"],
                        "_control_attempt_started_at": ATTEMPT["attempt_started_at"]}}


async def _prepared_kentucky_run(database, *, limit=None, control_changes=None):
    candidate_run = await _seed_kentucky_run(database, limit=limit)
    candidate_run["source_manifest"]["control_run_id"] = CONTROL_RUN_ID
    source_table = shared_completion.ProviderProfileImportRun.__table__
    await database.execute(update(source_table).where(source_table.c.run_id == candidate_run["run_id"]).values(
        source_manifest=candidate_run["source_manifest"]))
    await database.insert(shared_completion.ImportRun.__table__).values({
        "run_id": CONTROL_RUN_ID, "importer": kentucky.IMPORTER, "status": "running", "progress": ATTEMPT,
        "metrics": {}, **(control_changes or {}),
    }).status()
    return candidate_run


async def _stored_kentucky_state(database, candidate_run):
    source_table = shared_completion.ProviderProfileImportRun.__table__
    control_table = shared_completion.ImportRun.__table__
    source_row = await database.first(select(source_table).where(source_table.c.run_id == candidate_run["run_id"]))
    control_row = await database.first(select(control_table).where(control_table.c.run_id == CONTROL_RUN_ID))
    return dict(source_row._mapping), dict(control_row._mapping)


def test_completion_bindings_cannot_change_source_identity():
    with pytest.raises(FrozenInstanceError):
        kentucky._completion.store = massachusetts._completion.store
    with pytest.raises(FrozenInstanceError):
        kentucky._completion.importer = massachusetts.IMPORTER
    assert kentucky._completion.store is kentucky.store._store
    assert massachusetts._completion.store is massachusetts.store._store
    with pytest.raises(ValueError, match="^kentucky_profile_control_run_mismatch$"):
        kentucky._attempt(_context(), {"run_id": CONTROL_RUN_ID}, {"source_manifest": {"control_run_id": MA_CONTROL_RUN_ID}})
    with pytest.raises(ValueError, match="^kentucky_profile_control_attempt_missing$"):
        kentucky._attempt({}, {"run_id": CONTROL_RUN_ID}, {"source_manifest": {"control_run_id": CONTROL_RUN_ID}})


@pytest.mark.parametrize("limit", [None, 1])
async def test_native_completion_preserves_exact_source_and_control(monkeypatch, limit):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_kentucky_run(database, limit=limit)
        job_context_by_field = _context()
        completed = await kentucky.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics(limit or 10000))
        source_run, control_run = await _stored_kentucky_state(database, candidate_run)
        assert source_run["status"] == "completed" and source_run["source_manifest"] == candidate_run["source_manifest"]
        assert control_run["status"] == "succeeded" and control_run["importer"] == kentucky.IMPORTER
        assert control_run["metrics"] == {key: value for key, value in completed.items() if key != "terminal_progress"}
        assert control_run["progress"] == {**completed["terminal_progress"], **ATTEMPT}
        assert control_run["heartbeat_at"] == control_run["finished_at"]
        assert job_context_by_field["context"]["_control_committed_result"] == completed
        assert job_context_by_field["context"]["_control_committed_finished_at"].startswith(control_run["finished_at"].isoformat())
        assert job_context_by_field["context"]["control_run_terminal_committed"] is True
        pointer = await kentucky.store.read_publication()
        assert (pointer["current_run_id"] if pointer else None) == (candidate_run["run_id"] if limit is None else None)
        assert completed["published"] is (limit is None)
        assert completed["terminal_progress"]["phase"].startswith(kentucky.IMPORTER + " ")
        assert await massachusetts.store.read_publication() is None


async def test_native_failed_commit_preserves_massachusetts_publication(monkeypatch):
    async with _completion_database(monkeypatch) as database:
        ma_run = await _prepared_run(database)
        await massachusetts.complete_run(_ma_context(), {"run_id": MA_CONTROL_RUN_ID}, ma_run, _metrics())
        ma_before = await _stored_state(database, ma_run)
        ma_pointer = await massachusetts.store.read_publication()
        ky_run = await _prepared_kentucky_run(database)
        original_commit = shared_completion.SourceProfileCompletion._commit_control
        fact_table = shared_store.ProviderProfileFact.__table__

        async def failed_control_commit(completion, attempt, result_by_field):
            await original_commit(completion, attempt, result_by_field)
            assert (await kentucky.store.read_publication())["current_run_id"] == ky_run["run_id"]
            assert await database.scalar(select(fact_table.c.fact_id).where(
                fact_table.c.run_id == ky_run["run_id"], fact_table.c.published_at.is_not(None)).limit(1)) is not None
            raise RuntimeError("synthetic terminal update failure")

        monkeypatch.setattr(shared_completion.SourceProfileCompletion, "_commit_control", failed_control_commit)
        job_context_by_field = _context()
        with pytest.raises(RuntimeError, match="^synthetic terminal update failure$"):
            await kentucky.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, ky_run, _metrics())
        source_run, control_run = await _stored_kentucky_state(database, ky_run)
        assert source_run["status"] == control_run["status"] == "running"
        assert source_run["finished_at"] is control_run["finished_at"] is None
        assert "control_run_terminal_committed" not in job_context_by_field["context"]
        assert await kentucky.store.read_publication() is None
        assert await database.scalar(select(fact_table.c.fact_id).where(
            fact_table.c.run_id == ky_run["run_id"], fact_table.c.published_at.is_not(None)).limit(1)) is None
        assert await _stored_state(database, ma_run) == ma_before
        assert await massachusetts.store.read_publication() == ma_pointer


@pytest.mark.parametrize("held_source", ["MA", "KY"])
async def test_native_other_source_completes_during_owned_locks(monkeypatch, held_source):
    async with _completion_database(monkeypatch) as database:
        ma_run = await _prepared_run(database)
        ky_run = await _prepared_kentucky_run(database)
        locked, other = (massachusetts, kentucky) if held_source == "MA" else (kentucky, massachusetts)
        held_control_id = MA_CONTROL_RUN_ID if held_source == "MA" else CONTROL_RUN_ID
        other_control_id = CONTROL_RUN_ID if held_source == "MA" else MA_CONTROL_RUN_ID
        other_run, other_context = (ky_run, _context()) if held_source == "MA" else (ma_run, _ma_context())
        acquired, release = asyncio.Event(), asyncio.Event()

        async def hold_completion_locks():
            async with database.transaction():
                await locked._locked_control_run({"run_id": held_control_id})
                await locked.store._lock_source()
                acquired.set()
                await release.wait()

        holder = asyncio.create_task(hold_completion_locks())
        try:
            await asyncio.wait_for(acquired.wait(), timeout=5)
            completed = await asyncio.wait_for(other.complete_run(
                other_context, {"run_id": other_control_id}, other_run, _metrics()), timeout=5)
            assert completed["published"] is True
            assert (await other.store.read_publication())["current_run_id"] == other_run["run_id"]
            assert await locked.store.read_publication() is None
        finally:
            release.set()
            await holder


@pytest.mark.parametrize("control_changes", [
    {"importer": massachusetts.IMPORTER}, {"status": "canceling"},
    {"progress": {**ATTEMPT, "attempt_id": "stale"}},
    {"progress": {**ATTEMPT, "attempt_started_at": "2026-09-08T09:00:00+00:00"}},
])
async def test_native_changed_owner_cannot_complete_kentucky(monkeypatch, control_changes):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_kentucky_run(database, limit=1, control_changes=control_changes)
        before = await _stored_kentucky_state(database, candidate_run)
        job_context_by_field = _context()
        with pytest.raises(RuntimeError, match="^kentucky_profile_control_attempt_changed$"):
            await kentucky.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics(1))
        assert await _stored_kentucky_state(database, candidate_run) == before
        assert await kentucky.store.read_publication() is None
        assert "control_run_terminal_committed" not in job_context_by_field["context"]


async def test_native_recovery_changes_only_its_failed_owner(monkeypatch):
    async with _completion_database(monkeypatch) as database:
        ma_run = await _prepared_run(database, limit=1, control_changes={"status": "failed"})
        ky_run = await _prepared_kentucky_run(database, limit=1, control_changes={"status": "canceled"})
        ma_before = await _stored_state(database, ma_run)
        _, ky_owner_before = await _stored_kentucky_state(database, ky_run)
        retained = await kentucky.store.retained_counts(ky_run["run_id"])
        assert await kentucky.reconcile_failed_control_runs() == [ky_run["run_id"]]
        assert await _stored_state(database, ma_run) == ma_before
        source_run, control_run = await _stored_kentucky_state(database, ky_run)
        assert source_run["status"] == "failed" and source_run["source_manifest"] == ky_run["source_manifest"]
        assert control_run == ky_owner_before
        assert await kentucky.store.retained_counts(ky_run["run_id"]) == retained
        assert await kentucky.store.read_publication() is None
        assert await massachusetts.reconcile_failed_control_runs() == [ma_run["run_id"]]


@pytest.mark.parametrize("source_changes", [
    {"source_key": "massachusetts-borim"}, {"schema_version": "foreign-profile/v1"},
])
async def test_native_reconciliation_requires_kentucky_source(monkeypatch, source_changes):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_kentucky_run(database, limit=1)
        completed = await kentucky.complete_run(_context(), {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics(1))
        source_table = shared_completion.ProviderProfileImportRun.__table__
        await database.execute(update(source_table).where(source_table.c.run_id == candidate_run["run_id"]).values(source_changes))
        control_metrics_by_field = {key: value for key, value in completed.items() if key != "terminal_progress"}
        assert await kentucky._reconcile_commit({"run_id": CONTROL_RUN_ID, **ATTEMPT}, candidate_run, control_metrics_by_field) is None


@pytest.mark.parametrize("after_commit", [False, True])
async def test_native_cancellation_requires_proven_commit(monkeypatch, after_commit):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_kentucky_run(database)
        job_context_by_field = _context()
        _fail_transaction_exit(monkeypatch, database, after_commit=after_commit, failure=asyncio.CancelledError("synthetic exit"))
        if after_commit:
            completed = await kentucky.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics())
            assert completed["published"] is True
            assert job_context_by_field["context"]["control_run_terminal_committed"] is True
        else:
            with pytest.raises(asyncio.CancelledError, match="synthetic exit"):
                await kentucky.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics())
            assert "control_run_terminal_committed" not in job_context_by_field["context"]
        source_run, control_run = await _stored_kentucky_state(database, candidate_run)
        assert source_run["status"] == ("completed" if after_commit else "running")
        assert control_run["status"] == ("succeeded" if after_commit else "running")
        assert bool(await kentucky.store.read_publication()) is after_commit


async def test_native_late_cancel_reconciles_database_timestamps(monkeypatch):
    parent_task = asyncio.current_task()
    original_reconcile = shared_completion.SourceProfileCompletion._reconcile_commit

    async def reconcile_with_late_cancellation(completion, *args):
        assert asyncio.current_task() is not parent_task
        parent_task.cancel()
        await asyncio.sleep(0)
        return await original_reconcile(completion, *args)

    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_kentucky_run(database, limit=1)
        _fail_transaction_exit(monkeypatch, database, after_commit=True, failure=RuntimeError("ambiguous commit"))
        monkeypatch.setattr(shared_completion.SourceProfileCompletion, "_reconcile_commit", reconcile_with_late_cancellation)
        job_context_by_field = _context()
        completed = await kentucky.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics(1))
        _, control_run = await _stored_kentucky_state(database, candidate_run)
        assert completed["published"] is False
        assert job_context_by_field["context"]["control_run_terminal_committed"] is True
        assert job_context_by_field["context"]["_control_committed_finished_at"].startswith(control_run["finished_at"].isoformat())
        assert parent_task.cancelling() == 0


async def test_native_cancel_before_completion_retains_active_state(monkeypatch):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_kentucky_run(database, limit=1)
        before = await _stored_kentucky_state(database, candidate_run)
        job_context_by_field = {**_context(), "redis": SimpleNamespace(get=AsyncMock(return_value=b"1"))}
        with pytest.raises(ImportCancelledError):
            await kentucky.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics(1))
        assert await _stored_kentucky_state(database, candidate_run) == before
        assert "control_run_terminal_committed" not in job_context_by_field["context"]
