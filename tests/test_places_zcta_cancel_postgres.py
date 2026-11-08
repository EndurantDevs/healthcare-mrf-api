# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native PostgreSQL races for the protected PLACES cancellation boundary."""

import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import insert, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import control_imports as control
from db.connection import Database
from db.models import ImportRun
from process.places_zcta_handoff import HANDOFF_FORMAT
from tests.test_reference_family_archive_postgres import _database_url


@pytest.fixture
async def cancel_database(monkeypatch):
    schema = "places_cancel_" + uuid4().hex
    engine = create_async_engine(_database_url(), execution_options={"schema_translate_map": {"mrf": schema}})
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.run_sync(ImportRun.__table__.create)
        monkeypatch.setattr(control, "db", database)
        monkeypatch.setattr(control, "read_live_progress", lambda _run_id: None)
        monkeypatch.setattr(control, "_write_run_live_progress", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(control, "enqueue_status_event", lambda _run: None)
        monkeypatch.setenv("HLTHPRT_PLACES_ZCTA_PROTECTED_PUBLICATION", "true")
        yield database
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


async def _insert_run(database, *, status="running", handoff=False):
    run_id = "synthetic-" + uuid4().hex
    attempt = (run_id + ":" + uuid4().hex, "2026-01-01T00:00:00+00:00")
    metrics_by_name = {"existing": "preserved"}
    if handoff:
        metrics_by_name["places_handoff"] = {
            "format": HANDOFF_FORMAT,
            "run_id": run_id,
            "attempt_id": attempt[0],
            "attempt_started_at": attempt[1],
            "handoff_sha256": "a" * 64,
        }
    await database.status(
        insert(ImportRun).values(
            run_id=run_id,
            engine="synthetic",
            importer="places-zcta",
            status=status,
            params={},
            progress={"attempt_id": attempt[0], "attempt_started_at": attempt[1]},
            metrics=metrics_by_name,
            heartbeat_at=control.utc_now(),
        )
    )
    return run_id, attempt, metrics_by_name


async def _paused_places_create(monkeypatch, run_id, *, enqueue_error=False):
    enqueue_started = asyncio.Event()
    release_enqueue = asyncio.Event()
    adapter = control._SINGLE_JOB_ADAPTERS["places-zcta"]

    async def enqueue(_run):
        enqueue_started.set()
        await release_enqueue.wait()
        if enqueue_error:
            return control._failed_enqueue_result(
                importer="places-zcta", params={}, adapter=adapter, error=RuntimeError("synthetic enqueue error")
            )
        return control._successful_enqueue_result(params={}, adapter=adapter, job_id=f"places_zcta_start_{run_id}")

    monkeypatch.setattr(control, "_enqueue_import_start", enqueue)
    creating = asyncio.create_task(
        control.create_import_run({"run_id": run_id, "importer": "places-zcta", "params": {}})
    )
    try:
        await asyncio.wait_for(enqueue_started.wait(), timeout=5)
    except BaseException:
        release_enqueue.set()
        await creating
        raise
    return creating, release_enqueue


@pytest.mark.asyncio
async def test_cancel_fences_exact_attempt_before_worker_signal(cancel_database, monkeypatch):
    database = cancel_database
    run_id, attempt, _ = await _insert_run(database)

    async def signal(_current, **_kwargs):
        row = (await database.execute(select(ImportRun).where(ImportRun.run_id == run_id))).scalar_one()
        assert row.status == "canceling"
        assert (row.progress["attempt_id"], row.progress["attempt_started_at"]) == attempt
        assert row.finished_at is None
        assert (
            await database.status(
                update(ImportRun)
                .where(ImportRun.run_id == run_id, ImportRun.status == "running")
                .values(status="finalizing")
            )
            == 0
        )
        return {"redis": True, "kubernetes": {"enabled": False}}

    monkeypatch.setattr(control, "_cancel_signal_for_run", signal)
    result = await control.request_cancel(run_id)
    assert result["status"] == "canceling"
    row = (await database.execute(select(ImportRun).where(ImportRun.run_id == run_id))).scalar_one()
    assert row.metrics["existing"] == "preserved"
    assert row.metrics["cancel_signal"]["redis"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", ["true", "false"])
async def test_cancel_of_handoff_keeps_receipt_and_never_signals(cancel_database, monkeypatch, gate):
    database = cancel_database
    monkeypatch.setenv("HLTHPRT_PLACES_ZCTA_PROTECTED_PUBLICATION", gate)
    run_id, attempt, metrics = await _insert_run(database, status="finalizing", handoff=True)
    signal = AsyncMock(side_effect=AssertionError("worker must not be signaled after handoff"))
    worker_state = AsyncMock(side_effect=AssertionError("handoff must not be worker-reconciled"))
    monkeypatch.setattr(control, "_cancel_signal_for_run", signal)
    monkeypatch.setattr(control, "_active_worker_state", worker_state)
    assert (await control.get_import_run(run_id))["status"] == "finalizing"
    result = await control.request_cancel(run_id)
    assert result["status"] == "canceling"
    row = (await database.execute(select(ImportRun).where(ImportRun.run_id == run_id))).scalar_one()
    assert row.metrics == metrics
    assert row.finished_at is None
    assert (row.progress["attempt_id"], row.progress["attempt_started_at"]) == attempt
    signal.assert_not_awaited()
    worker_state.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
async def test_worker_failure_cannot_replace_committed_handoff(cancel_database, monkeypatch, published):
    database = cancel_database
    run_id, attempt, metrics = await _insert_run(database, status="finalizing")
    receipt_by_name = {
        "format": HANDOFF_FORMAT,
        "run_id": run_id,
        "attempt_id": attempt[0],
        "attempt_started_at": attempt[1],
        "handoff_sha256": "a" * 64,
    }
    worker_read = asyncio.Event()
    release_worker = asyncio.Event()

    async def failed_worker(_run):
        worker_read.set()
        await release_worker.wait()
        return {"status": "failed", "items": [{"job_status": "failed"}]}

    monkeypatch.setattr(control, "_active_worker_state", failed_worker)
    reading = asyncio.create_task(control.get_import_run(run_id))
    try:
        await asyncio.wait_for(worker_read.wait(), timeout=5)
        await database.status(
            update(ImportRun)
            .where(ImportRun.run_id == run_id)
            .values(status="finalizing", metrics={**metrics, "places_handoff": receipt_by_name})
        )
        if published:
            await database.status(
                update(ImportRun)
                .where(ImportRun.run_id == run_id)
                .values(status="succeeded", finished_at=control.utc_now())
            )
    finally:
        release_worker.set()
    reported_run = await asyncio.wait_for(reading, timeout=5)
    assert reported_run["status"] == ("succeeded" if published else "finalizing")
    assert reported_run["metrics"] == {**metrics, "places_handoff": receipt_by_name}
    stored_run = (await database.execute(select(ImportRun).where(ImportRun.run_id == run_id))).scalar_one()
    assert stored_run.status == reported_run["status"]
    assert stored_run.metrics == reported_run["metrics"]


@pytest.mark.asyncio
async def test_worker_failure_cannot_replace_new_attempt(cancel_database, monkeypatch):
    database = cancel_database
    run_id, _old_attempt, metrics = await _insert_run(database)
    worker_read = asyncio.Event()
    release_worker = asyncio.Event()

    async def failed_worker(_run):
        worker_read.set()
        await release_worker.wait()
        return {"status": "failed", "items": [{"job_status": "failed"}]}

    monkeypatch.setattr(control, "_active_worker_state", failed_worker)
    reading = asyncio.create_task(control.get_import_run(run_id))
    try:
        await asyncio.wait_for(worker_read.wait(), timeout=5)
        new_attempt = (run_id + ":" + uuid4().hex, "2026-01-02T00:00:00+00:00")
        await database.status(
            update(ImportRun)
            .where(ImportRun.run_id == run_id)
            .values(progress={"attempt_id": new_attempt[0], "attempt_started_at": new_attempt[1]})
        )
    finally:
        release_worker.set()
    reported_run = await asyncio.wait_for(reading, timeout=5)
    assert reported_run["status"] == "running"
    assert reported_run["metrics"] == metrics
    assert reported_run["progress"]["attempt_id"] == new_attempt[0]


def test_places_job_id_recovers_without_acknowledgement_metrics():
    first_run = "synthetic-" + uuid4().hex
    second_run = "synthetic-" + uuid4().hex
    first_job_id = control._arq_cleanup_identity(
        {"run_id": first_run, "importer": "places-zcta", "params": {}, "metrics": {}}
    )[-1]
    second_job_id = control._arq_cleanup_identity(
        {"run_id": second_run, "importer": "places-zcta", "params": {}, "metrics": {}}
    )[-1]
    assert first_job_id == f"places_zcta_start_{first_run}"
    assert first_job_id != second_job_id
    assert (
        control._arq_cleanup_identity(
            {"run_id": first_run, "importer": "places-zcta", "params": {}, "metrics": {"job_id": "legacy-random"}}
        )[-1]
        == "legacy-random"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("advanced_status", ["running", "finalizing", "succeeded"])
@pytest.mark.parametrize("enqueue_error", [False, True])
async def test_late_enqueue_result_preserves_places_attempt(
    cancel_database, monkeypatch, advanced_status, enqueue_error
):
    database = cancel_database
    run_id = "synthetic-" + uuid4().hex
    events = []
    live_updates = []
    monkeypatch.setattr(control, "enqueue_status_event", events.append)
    monkeypatch.setattr(control, "_write_run_live_progress", lambda report, **_kwargs: live_updates.append(report))
    creating, release_enqueue = await _paused_places_create(monkeypatch, run_id, enqueue_error=enqueue_error)
    try:
        initial = (await database.execute(select(ImportRun).where(ImportRun.run_id == run_id))).scalar_one()
        expected_job_id = f"places_zcta_start_{run_id}"
        assert initial.metrics["job_id"] == expected_job_id
        assert initial.metrics["enqueue_adapter"] == "arq_single_job"
        attempt = (run_id + ":" + uuid4().hex, "2026-01-01T00:00:00+00:00")
        receipt_by_name = {
            "format": HANDOFF_FORMAT,
            "run_id": run_id,
            "attempt_id": attempt[0],
            "attempt_started_at": attempt[1],
            "handoff_sha256": "a" * 64,
        }
        metrics_by_name = {**initial.metrics, "worker_marker": "preserved"}
        if advanced_status != "running":
            metrics_by_name["places_handoff"] = receipt_by_name
        await database.status(
            update(ImportRun)
            .where(ImportRun.run_id == run_id)
            .values(
                status=advanced_status,
                progress={"attempt_id": attempt[0], "attempt_started_at": attempt[1]},
                metrics=metrics_by_name,
                finished_at=control.utc_now() if advanced_status == "succeeded" else None,
            )
        )
    finally:
        release_enqueue.set()
    reported_run, created = await asyncio.wait_for(creating, timeout=5)
    stored_run = (await database.execute(select(ImportRun).where(ImportRun.run_id == run_id))).scalar_one()
    assert created is True
    assert reported_run["status"] == stored_run.status == advanced_status
    assert reported_run["progress"] == stored_run.progress
    assert reported_run["metrics"] == stored_run.metrics == metrics_by_name
    assert reported_run["error"] is None
    assert events[-1]["status"] == live_updates[-1]["status"] == advanced_status
    assert bool(stored_run.finished_at) is (advanced_status == "succeeded")
    if advanced_status == "running":
        assert control._arq_cleanup_identity(reported_run)[-1] == expected_job_id


@pytest.mark.asyncio
async def test_pre_ack_places_cancel_uses_initial_job_identity(cancel_database, monkeypatch):
    database = cancel_database
    run_id = "synthetic-" + uuid4().hex
    creating, release_enqueue = await _paused_places_create(monkeypatch, run_id)

    async def queued_signal(current, **options):
        assert options["is_queued_arq"] is True
        assert control._arq_cleanup_identity(current)[-1] == f"places_zcta_start_{run_id}"
        return {"removed": True}

    monkeypatch.setattr(control, "_cancel_signal_for_run", queued_signal)
    try:
        canceled = await control.request_cancel(run_id)
    finally:
        release_enqueue.set()
    reported_run, created = await asyncio.wait_for(creating, timeout=5)
    stored_run = (await database.execute(select(ImportRun).where(ImportRun.run_id == run_id))).scalar_one()
    assert created is True
    assert canceled["status"] == reported_run["status"] == stored_run.status == "canceled"
    assert stored_run.metrics["job_id"] == f"places_zcta_start_{run_id}"


@pytest.mark.asyncio
async def test_cancel_rejects_partial_attempt_without_signal(cancel_database, monkeypatch):
    database = cancel_database
    run_id, attempt, _metrics = await _insert_run(database)
    await database.status(
        update(ImportRun).where(ImportRun.run_id == run_id).values(progress={"attempt_id": attempt[0]})
    )
    signal = AsyncMock(side_effect=AssertionError("partial attempt must not be signaled"))
    monkeypatch.setattr(control, "_cancel_signal_for_run", signal)
    with pytest.raises(RuntimeError, match="exact active attempt"):
        await control.request_cancel(run_id)
    assert (
        await database.execute(select(ImportRun.status).where(ImportRun.run_id == run_id))
    ).scalar_one() == "running"
    signal.assert_not_awaited()


@pytest.mark.asyncio
async def test_queued_cancel_preserves_terminal_before_start_behavior(cancel_database, monkeypatch):
    database = cancel_database
    run_id, _attempt, _metrics = await _insert_run(database, status="queued")
    await database.status(
        update(ImportRun).where(ImportRun.run_id == run_id).values(progress={}, metrics={"enqueue_adapter": "pending"})
    )

    async def signal(_current, **_kwargs):
        assert (
            await database.execute(select(ImportRun.status).where(ImportRun.run_id == run_id))
        ).scalar_one() == "canceling"
        return {"redis": False, "pending_adapter": True}

    monkeypatch.setattr(control, "_cancel_signal_for_run", signal)
    result = await control.request_cancel(run_id)
    assert result["status"] == "canceled"
    assert result["finished_at"] is not None


@pytest.mark.asyncio
async def test_publisher_that_holds_run_lock_wins_without_cancel_signal(cancel_database, monkeypatch):
    database = cancel_database
    run_id, _attempt, _metrics = await _insert_run(database, status="finalizing", handoff=True)
    signal = AsyncMock(side_effect=AssertionError("terminal publication must not be signaled"))
    monkeypatch.setattr(control, "_cancel_signal_for_run", signal)
    published = asyncio.Event()
    release = asyncio.Event()

    async def publisher():
        async with database.transaction():
            await database.execute(select(ImportRun).where(ImportRun.run_id == run_id).with_for_update())
            await database.status(
                update(ImportRun)
                .where(ImportRun.run_id == run_id)
                .values(status="succeeded", finished_at=control.utc_now())
            )
            published.set()
            await release.wait()

    publishing = asyncio.create_task(publisher())
    try:
        await published.wait()
        canceling = asyncio.create_task(control.request_cancel(run_id))
        await asyncio.sleep(0.05)
        assert not canceling.done()
        release.set()
        await publishing
        cancel_result = await canceling
        assert cancel_result["status"] == "succeeded"
        assert (
            await database.execute(select(ImportRun.status).where(ImportRun.run_id == run_id))
        ).scalar_one() == "succeeded"
        signal.assert_not_awaited()
    finally:
        release.set()
        if not publishing.done():
            await publishing
