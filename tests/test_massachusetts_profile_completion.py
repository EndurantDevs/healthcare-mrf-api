# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Atomic source/control completion, with native PostgreSQL failure boundaries."""

import asyncio
from contextlib import asynccontextmanager
from itertools import count
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import MetaData, select, update
from sqlalchemy.orm import registry

from process import control_lifecycle as lifecycle
from process import massachusetts_profile_completion as completion
from process.control_cancel import ImportCancelledError
from tests.test_massachusetts_profile_store import _database, _metrics, _run, _seed_run

CONTROL_RUN_ID = "run_ma_completion_synthetic"
ATTEMPT = {"attempt_id": CONTROL_RUN_ID + ":" + "b" * 32,
           "attempt_started_at": "2026-09-08T08:00:00.000000+00:00"}


def _context():
    return {"context": {"control_run_id": CONTROL_RUN_ID,
                        "_control_attempt_id": ATTEMPT["attempt_id"],
                        "_control_attempt_started_at": ATTEMPT["attempt_started_at"]}}


@asynccontextmanager
async def _completion_database(monkeypatch):
    async with _database(monkeypatch) as database:
        source_model = completion.store.ProviderProfileImportRun
        control_table = completion.ImportRun.__table__.to_metadata(MetaData(), schema=source_model.__table__.schema)
        control_model = type("CompletionControlRun", (), {})
        mapper_registry = registry()
        mapper_registry.map_imperatively(control_model, control_table)
        monkeypatch.setattr(completion, "ImportRun", control_model)
        monkeypatch.setattr(completion, "ProviderProfileImportRun", source_model)
        monkeypatch.setattr(completion, "db", database)
        await database.create_table(control_table, checkfirst=False)
        try:
            yield database
        finally:
            mapper_registry.dispose()


async def _prepared_run(database, *, limit=None, control_changes=None, predecessor=None):
    candidate_run = await _seed_run(database, count=10000 if limit is None else 3, limit=limit, predecessor=predecessor)
    candidate_run["source_manifest"]["control_run_id"] = CONTROL_RUN_ID
    source_table = completion.ProviderProfileImportRun.__table__
    await database.execute(update(source_table).where(source_table.c.run_id == candidate_run["run_id"]).values(
        source_manifest=candidate_run["source_manifest"]))
    await database.insert(completion.ImportRun.__table__).values({
        "run_id": CONTROL_RUN_ID, "importer": completion.IMPORTER, "status": "running", "progress": ATTEMPT,
        "metrics": {}, **(control_changes or {}),
    }).status()
    return candidate_run


async def _stored_state(database, candidate_run):
    source_table = completion.ProviderProfileImportRun.__table__
    control_table = completion.ImportRun.__table__
    source_row = await database.first(select(source_table).where(source_table.c.run_id == candidate_run["run_id"]))
    control_row = await database.first(select(control_table).where(control_table.c.run_id == CONTROL_RUN_ID))
    return dict(source_row._mapping), dict(control_row._mapping)


@pytest.mark.parametrize("terminal_status", ["failed", "canceled", "cancelled", "dead_letter"])
async def test_native_stopped_owner_recovers_stranded_run_without_changing_publication(monkeypatch, terminal_status):
    async with _completion_database(monkeypatch) as database:
        incumbent = await _seed_run(database)
        await completion.store.publish_run(incumbent["run_id"], expected_current_run_id=None, metrics=_metrics())
        pointer = await completion.store.read_publication()
        incumbent_counts = await completion.store.retained_counts(incumbent["run_id"])
        candidate = await _prepared_run(database, limit=1, predecessor=incumbent["run_id"])
        source_table = completion.ProviderProfileImportRun.__table__
        foreign_run_by_field = {**_run("f" * 64, limit=1), "source_key": "florida-mqa", "jurisdiction": "FL"}
        await database.insert(source_table).values(foreign_run_by_field).status()
        fresh = _run(limit=1, count=3, predecessor=incumbent["run_id"], resume_from=candidate["run_id"])
        assert await completion.reconcile_failed_control_runs() == []
        with pytest.raises(RuntimeError, match="source_already_running"):
            await completion.store.claim_run(fresh)
        with pytest.raises(RuntimeError, match="resume_not_eligible"):
            await completion.store.read_resume_run(candidate["run_id"], max_providers=1, expected_current_run_id=incumbent["run_id"])

        control_table = completion.ImportRun.__table__
        await database.execute(update(control_table).where(control_table.c.run_id == CONTROL_RUN_ID).values(status=terminal_status))
        _, owner_before = await _stored_state(database, candidate)
        retained = await completion.store.retained_counts(candidate["run_id"])
        assert await completion.reconcile_failed_control_runs() == [candidate["run_id"]]
        source_run, owner_after = await _stored_state(database, candidate)
        assert source_run["status"] == "failed" and source_run["finished_at"] is not None
        assert source_run["source_manifest"] == candidate["source_manifest"]
        assert owner_after == owner_before
        assert await completion.store.retained_counts(candidate["run_id"]) == retained
        await completion.store.read_resume_run(candidate["run_id"], max_providers=1, expected_current_run_id=incumbent["run_id"])
        await completion.store.claim_run(fresh)
        with pytest.raises(RuntimeError, match="control_attempt_changed"):
            await completion.complete_run(_context(), {"run_id": CONTROL_RUN_ID}, candidate, _metrics(1))
        assert await completion.store.read_publication() == pointer
        assert await completion.store.retained_counts(incumbent["run_id"]) == incumbent_counts
        foreign_after = await database.first(select(source_table).where(source_table.c.run_id == foreign_run_by_field["run_id"]))
        assert all(foreign_after._mapping[key] == field_value for key, field_value in foreign_run_by_field.items())


@pytest.mark.parametrize("owner_case", ["running", "canceling", "succeeded", "foreign", "missing", "cli", "blank"])
async def test_native_recovery_keeps_live_or_ambiguous_source_claim(monkeypatch, owner_case):
    async with _completion_database(monkeypatch) as database:
        candidate = await _prepared_run(database, limit=1)
        control_table = completion.ImportRun.__table__
        source_table = completion.ProviderProfileImportRun.__table__
        if owner_case in {"cli", "blank"}:
            candidate["source_manifest"]["control_run_id"] = None if owner_case == "cli" else " "
            await database.execute(update(source_table).where(source_table.c.run_id == candidate["run_id"]).values(source_manifest=candidate["source_manifest"]))
        elif owner_case == "missing":
            await database.execute(control_table.delete())
        else:
            await database.execute(update(control_table).values(
                status="failed" if owner_case == "foreign" else owner_case,
                importer="florida-mqa-profile" if owner_case == "foreign" else completion.IMPORTER))
        before = await database.first(select(source_table).where(source_table.c.run_id == candidate["run_id"]))
        assert await completion.reconcile_failed_control_runs() == []
        after = await database.first(select(source_table).where(source_table.c.run_id == candidate["run_id"]))
        assert dict(after._mapping) == dict(before._mapping)
        with pytest.raises(RuntimeError, match="source_already_running"):
            await completion.store.claim_run(_run(limit=1, count=3))


@pytest.mark.parametrize("limit", [None, 1])
async def test_native_completion_commits_source_control_and_exact_times(monkeypatch, limit):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_run(database, limit=limit)
        job_context_by_field = _context()
        result_by_field = await completion.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics(limit or 10000))
        source_run, control_run = await _stored_state(database, candidate_run)
        assert source_run["status"] == "completed"
        assert control_run["status"] == "succeeded"
        assert control_run["metrics"] == {key: value for key, value in result_by_field.items() if key != "terminal_progress"}
        assert control_run["progress"] == {**result_by_field["terminal_progress"], **ATTEMPT}
        assert control_run["finished_at"] == control_run["heartbeat_at"]
        assert job_context_by_field["context"]["_control_committed_result"] == result_by_field
        assert job_context_by_field["context"]["_control_committed_finished_at"].startswith(control_run["finished_at"].isoformat())
        assert job_context_by_field["context"]["control_run_terminal_committed"] is True
        pointer = await completion.store.read_publication()
        assert (pointer["current_run_id"] if pointer else None) == (candidate_run["run_id"] if limit is None else None)
        assert result_by_field["published"] is (limit is None)


@pytest.mark.parametrize("control_changes", [
    {"status": "canceling"}, {"status": "failed"}, {"status": "succeeded"}, {"importer": "florida-mqa-profile"},
    {"progress": {**ATTEMPT, "attempt_id": "different"}},
    {"progress": {**ATTEMPT, "attempt_started_at": "2026-09-08T09:00:00+00:00"}},
])
async def test_native_changed_attempt_cannot_complete_source(monkeypatch, control_changes):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_run(database, limit=1, control_changes=control_changes)
        job_context_by_field = _context()
        with pytest.raises(RuntimeError, match="control_attempt_changed"):
            await completion.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics(1))
        source_run, control_run = await _stored_state(database, candidate_run)
        assert source_run["status"] == "running"
        assert control_run["status"] == control_changes.get("status", "running")
        assert "control_run_terminal_committed" not in job_context_by_field["context"]
        assert await completion.store.read_publication() is None


async def test_native_failure_after_pointer_write_rolls_back_both_runs(monkeypatch):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_run(database)
        job_context_by_field = _context()
        original_commit = completion._commit_control

        async def failed_control_commit(attempt, result_by_field):
            await original_commit(attempt, result_by_field)
            raise RuntimeError("synthetic terminal update failure")

        monkeypatch.setattr(completion, "_commit_control", failed_control_commit)
        with pytest.raises(RuntimeError, match="synthetic terminal update failure"):
            await completion.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics())
        source_run, control_run = await _stored_state(database, candidate_run)
        assert source_run["status"] == control_run["status"] == "running"
        assert source_run["finished_at"] is control_run["finished_at"] is None
        assert await completion.store.read_publication() is None
        assert "control_run_terminal_committed" not in job_context_by_field["context"]


def _fail_transaction_exit(monkeypatch, database, *, after_commit, failure):
    original_transaction = database.transaction
    transaction_calls = count()

    @asynccontextmanager
    async def interrupted_transaction():
        is_first_transaction = next(transaction_calls) == 0
        async with original_transaction():
            yield
            if is_first_transaction and not after_commit:
                raise failure
        if is_first_transaction and after_commit:
            raise failure

    monkeypatch.setattr(database, "transaction", interrupted_transaction)


@pytest.mark.parametrize("failure_type", [RuntimeError, asyncio.CancelledError])
@pytest.mark.parametrize("after_commit", [False, True])
async def test_native_uncertain_exit_reconciles_only_durable_commit(monkeypatch, failure_type, after_commit):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_run(database)
        job_context_by_field = _context()
        _fail_transaction_exit(monkeypatch, database, after_commit=after_commit, failure=failure_type("synthetic exit"))
        if after_commit:
            result_by_field = await completion.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics())
            assert result_by_field["published"] is True
            assert job_context_by_field["context"]["control_run_terminal_committed"] is True
        else:
            with pytest.raises(failure_type, match="synthetic exit"):
                await completion.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics())
            assert "control_run_terminal_committed" not in job_context_by_field["context"]
        source_run, control_run = await _stored_state(database, candidate_run)
        assert source_run["status"] == ("completed" if after_commit else "running")
        assert control_run["status"] == ("succeeded" if after_commit else "running")
        assert bool(await completion.store.read_publication()) is after_commit


async def test_native_cancel_flag_before_publication_leaves_source_active(monkeypatch):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_run(database, limit=1)
        job_context_by_field = {**_context(), "redis": SimpleNamespace(get=AsyncMock(return_value=b"1"))}
        with pytest.raises(ImportCancelledError):
            await completion.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics(1))
        source_run, control_run = await _stored_state(database, candidate_run)
        assert source_run["status"] == control_run["status"] == "running"
        assert "control_run_terminal_committed" not in job_context_by_field["context"]


async def test_native_terminal_status_blocks_late_heartbeat_and_cancel(monkeypatch):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_run(database, limit=1)
        await completion.complete_run(_context(), {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics(1))
        monkeypatch.setattr(lifecycle, "ImportRun", completion.ImportRun)

        async def execute_owned_update(statement):
            # Keep the production conditional UPDATE on this isolated database.
            returned = await database.execute(statement)
            return len(returned.all())

        monkeypatch.setattr(lifecycle, "_execute_control_run_update", execute_owned_update)
        assert await lifecycle._is_control_run_heartbeat_persisted(CONTROL_RUN_ID, "process_data", **ATTEMPT) is False
        assert await lifecycle.mark_control_run(
            CONTROL_RUN_ID, status="failed", phase_detail="late failure", progress_message="failed", **ATTEMPT,
        ) is False
        control_table = completion.ImportRun.__table__
        canceled = await database.execute(update(control_table).where(
            control_table.c.run_id == CONTROL_RUN_ID, control_table.c.status.notin_(lifecycle._TERMINAL_STATUSES),
        ).values(status="canceling"))
        assert canceled.rowcount == 0
        source_run, control_run = await _stored_state(database, candidate_run)
        assert source_run["status"] == "completed"
        assert control_run["status"] == "succeeded"


@pytest.mark.parametrize("limit", [None, 1])
async def test_unmanaged_completion_cannot_publish(monkeypatch, limit):
    result_by_field = {"run_id": "a" * 64, "published": limit is None, "requested_licenses": 1}
    source_finisher = AsyncMock(return_value=result_by_field)
    monkeypatch.setattr(completion.store, "publish_run", source_finisher)
    monkeypatch.setattr(completion.store, "finish_unpublished_run", source_finisher)
    job_context_by_field = {}
    run_by_field = {"run_id": result_by_field["run_id"], "source_manifest": {"max_providers": limit, "expected_current_run_id": None}}
    with pytest.raises(ValueError, match="control_attempt_missing"):
        await completion.complete_run(job_context_by_field, {}, run_by_field, _metrics(1))
    assert job_context_by_field == {}
    source_finisher.assert_not_awaited()


@pytest.mark.parametrize(("task_by_field", "run_by_field", "job_context_by_field", "error"), [
    ({"run_id": CONTROL_RUN_ID}, {"source_manifest": {}}, _context(), "control_run_mismatch"),
    ({"run_id": CONTROL_RUN_ID}, {"source_manifest": {"control_run_id": CONTROL_RUN_ID}}, {}, "control_attempt_missing"),
    ({"run_id": ""}, {"source_manifest": {"control_run_id": ""}}, _context(), "control_attempt_missing"),
])
def test_completion_requires_exact_control_binding(task_by_field, run_by_field, job_context_by_field, error):
    with pytest.raises(ValueError, match=error):
        completion._attempt(job_context_by_field, task_by_field, run_by_field)


def _committed_context():
    return {"control_run_terminal_committed": True, "preserve_control_run_finished_at": True,
            "_control_committed_heartbeat_at": "2026-09-08T08:01:00.000000+00:00",
            "_control_committed_finished_at": "2026-09-08T08:01:00.000000+00:00",
            "_control_committed_result": {"published": True, "requested_licenses": 1,
                                          "terminal_progress": completion._terminal_progress({"published": True, "requested_licenses": 1})}}


@pytest.mark.parametrize("target_module", ["process.massachusetts_profile", "process.npi", "process.unrelated"])
@pytest.mark.parametrize("late_error", [None, RuntimeError, asyncio.CancelledError, ImportCancelledError])
async def test_wrapper_trusts_only_supported_committed_results_without_shutdown(monkeypatch, target_module, late_error):
    async def target(job_context_by_field, _task):
        job_context_by_field["context"].update(_committed_context())
        if late_error:
            raise late_error("after commit")
        return {"incorrect_later_result": True}

    projection = AsyncMock()
    monkeypatch.setattr(lifecycle, "import_module", lambda _name: SimpleNamespace(process_data=target))
    monkeypatch.setattr(lifecycle, "mark_control_run", AsyncMock(return_value=True))
    monkeypatch.setattr(lifecycle, "_mark_and_flush_terminal_control_run", projection)
    monkeypatch.setattr(lifecycle, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(lifecycle, "_live_progress_heartbeat", AsyncMock())
    task_by_field = {"run_id": CONTROL_RUN_ID, "importer": completion.IMPORTER, "target_module": target_module,
            "target_function": "process_data", "run_shutdown": False, "task": {}}
    if target_module == "process.unrelated" and late_error is RuntimeError:
        with pytest.raises(RuntimeError, match="after commit"):
            await lifecycle.control_single_job_start({}, task_by_field)
        projection.assert_not_awaited()
        return
    result_by_field = await lifecycle.control_single_job_start({}, task_by_field)
    if target_module == "process.unrelated" and late_error:
        assert result_by_field["status"] in {"failed", "canceled"}
        projection.assert_not_awaited()
    else:
        assert result_by_field["status"] == "succeeded"
        projected = projection.await_args.kwargs
        assert projected["database_state_committed"] is (target_module != "process.unrelated")
        if target_module != "process.unrelated":
            assert result_by_field["result"] == _committed_context()["_control_committed_result"]
            assert projected["metrics"]["published"] is True
            assert projected["database_finished_at"] == _committed_context()["_control_committed_finished_at"]
            assert projected["phase_detail"] == f"{completion.IMPORTER} published"


async def test_control_update_rechecks_attempt_even_after_lock(monkeypatch):
    async with _completion_database(monkeypatch) as database:
        await _prepared_run(database, limit=1)
        with pytest.raises(RuntimeError, match="control_attempt_changed"):
            await completion._commit_control(
                {"run_id": CONTROL_RUN_ID, **ATTEMPT, "attempt_id": "stale"},
                {"published": False, "requested_licenses": 1},
            )


@pytest.mark.parametrize("source_changes", [{"status": "failed"}, {"metrics": {}}, {"source_manifest": {}}])
async def test_reconciliation_requires_exact_completed_source(monkeypatch, source_changes):
    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_run(database, limit=1)
        result_by_field = await completion.complete_run(_context(), {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics(1))
        source_table = completion.ProviderProfileImportRun.__table__
        await database.execute(update(source_table).where(source_table.c.run_id == candidate_run["run_id"]).values(source_changes))
        control_metrics_by_field = {key: value for key, value in result_by_field.items() if key != "terminal_progress"}
        assert await completion._reconcile_commit({"run_id": CONTROL_RUN_ID, **ATTEMPT}, candidate_run, control_metrics_by_field) is None


async def test_cancellation_during_reconciliation_finishes_committed_result(monkeypatch):
    parent_task = asyncio.current_task()
    original_reconcile = completion._reconcile_commit

    async def reconcile_with_late_cancellation(*args):
        assert asyncio.current_task() is not parent_task
        parent_task.cancel()
        await asyncio.sleep(0)
        return await original_reconcile(*args)

    async with _completion_database(monkeypatch) as database:
        candidate_run = await _prepared_run(database, limit=1)
        _fail_transaction_exit(monkeypatch, database, after_commit=True, failure=RuntimeError("ambiguous commit"))
        monkeypatch.setattr(completion, "_reconcile_commit", reconcile_with_late_cancellation)
        job_context_by_field = _context()
        completed = await completion.complete_run(job_context_by_field, {"run_id": CONTROL_RUN_ID}, candidate_run, _metrics(1))
        assert completed["published"] is False
        assert job_context_by_field["context"]["control_run_terminal_committed"] is True
        assert parent_task.cancelling() == 0


async def test_reconciliation_database_error_is_not_proof(monkeypatch):
    @asynccontextmanager
    async def unavailable_transaction():
        raise RuntimeError("synthetic database outage")
        yield

    monkeypatch.setattr(completion.db, "transaction", unavailable_transaction)
    assert await completion._reconcile_commit({}, {}, {}) is None


async def test_cancelled_reconciliation_does_not_spin(monkeypatch):
    monkeypatch.setattr(completion, "_reconcile_commit", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await completion._shielded_reconciliation({}, {}, {})


async def test_invalid_commit_timestamp_does_not_install_marker():
    job_context_by_field = {}
    with pytest.raises(RuntimeError, match="control_timestamp_invalid"):
        completion._install_committed_result(job_context_by_field, {"heartbeat_at": None}, {})
    assert job_context_by_field == {}
