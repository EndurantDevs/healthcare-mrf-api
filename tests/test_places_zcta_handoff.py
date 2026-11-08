# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import hashlib
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects import postgresql

from api import control_imports
from process import control_lifecycle
from process import places_zcta_handoff as handoff

places_zcta = importlib.import_module("process.places_zcta")


def _attempt_context():
    return {
        "control_run_id": "synthetic-run",
        "context": {
            "_control_attempt_id": "synthetic-run:" + "a" * 32,
            "_control_attempt_started_at": "2026-01-01T00:00:00.000000+00:00",
            "_places_incumbent_oid": 15,
            "audit": {
                "source_url": "https://example.org/places.csv",
                "latest_year": 2025,
                "processed_rows": 1000,
                "accepted_rows": 1000,
            },
        },
    }


def _stage_name(ctx):
    return "pricing_places_zcta_" + hashlib.sha256(ctx["context"]["_control_attempt_id"].encode()).hexdigest()[:20]


@asynccontextmanager
async def _transaction():
    yield


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", [None, "attempt", "census", "oid", "indexes", "cas", "envelope"])
async def test_handoff_validates_catalog_before_transition(mismatch):
    ctx = _attempt_context()
    ctx["context"]["_places_stage_oid"] = 101
    database = SimpleNamespace(
        transaction=_transaction,
        status=AsyncMock(),
        first=AsyncMock(
            side_effect=[
                None if mismatch == "attempt" else ("synthetic-run",),
                (1, 2, 102 if mismatch == "oid" else 101),
            ]
        ),
        scalar=AsyncMock(
            side_effect=[
                999 if mismatch == "census" else 1000,
                []
                if mismatch == "indexes"
                else [
                    {
                        "oid": 102,
                        "definition": "x" * handoff.MAX_HANDOFF_BYTES
                        if mismatch == "envelope"
                        else "CREATE UNIQUE INDEX",
                    }
                ],
                None if mismatch == "cas" else "synthetic-run",
                None,
            ]
        ),
    )
    if mismatch:
        with pytest.raises(RuntimeError):
            await handoff.handoff_places_stage(database, ctx, schema="mrf", table_name=_stage_name(ctx), row_count=1000)
        assert not ctx["context"].get("control_run_handoff_committed")
    else:
        outcome = await handoff.handoff_places_stage(
            database, ctx, schema="mrf", table_name=_stage_name(ctx), row_count=1000
        )
        receipt = outcome["places_handoff"]
        assert receipt["stage_oid"] == 101 and receipt["incumbent_oid"] == 15
        assert receipt["complete"] is True and receipt["published"] is False
        assert "source_url" not in receipt["audit"]
        assert len(receipt["handoff_sha256"]) == 64


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "blocked", [None, "handed_off", "rebound", "marked", "current", "unknown", "committed", "wrong_table"]
)
async def test_cleanup_fails_closed_on_publication_or_uncertain_identity(blocked):
    ctx = _attempt_context()
    ctx["context"].update(_places_stage_oid=101, _places_stage_schema="mrf", _places_stage_table=_stage_name(ctx))
    if blocked == "committed":
        ctx["context"]["control_run_handoff_committed"] = True
    elif blocked == "wrong_table":
        ctx["context"]["_places_stage_table"] = "pricing_places_zcta_" + "0" * 20
    database = SimpleNamespace(
        transaction=_transaction,
        status=AsyncMock(),
        first=AsyncMock(return_value=({"complete": True},) if blocked == "handed_off" else (None,)),
        scalar=AsyncMock(
            side_effect=RuntimeError("connection unavailable")
            if blocked == "unknown"
            else [102 if blocked == "rebound" else 101, blocked != "marked", blocked == "current"]
        ),
    )
    assert await handoff.cleanup_places_attempt(database, ctx) is (blocked is None)
    drop_calls = [call for call in database.status.await_args_list if "DROP TABLE" in call.args[0]]
    assert bool(drop_calls) is (blocked is None)
    if blocked == "wrong_table":
        database.first.assert_not_awaited()
        database.scalar.assert_not_awaited()
        database.status.assert_not_awaited()


@pytest.mark.parametrize("invalid", ["run_id", "attempt", "stage", "test", "count"])
def test_handoff_rejects_unbound_or_incomplete_attempt(invalid):
    ctx = _attempt_context()
    stage = _stage_name(ctx)
    count = 1000
    if invalid == "run_id":
        ctx["control_run_id"] = "different-run"
    elif invalid == "attempt":
        ctx["context"]["_control_attempt_id"] = "shared-date"
    elif invalid == "stage":
        stage = "pricing_places_zcta_20260101"
    elif invalid == "test":
        ctx["context"]["test_mode"] = True
    else:
        count = 0
    with pytest.raises(RuntimeError):
        handoff._attempt_parameters(ctx, "mrf", stage, count)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "commit_error,committed",
    [
        (None, True),
        (RuntimeError("lost acknowledgement"), True),
        (asyncio.CancelledError(), True),
        (RuntimeError("rollback"), False),
    ],
)
async def test_handoff_recovers_only_exact_durable_commit(monkeypatch, commit_error, committed):
    ctx = _attempt_context()
    receipt_by_field = {"attempt_id": ctx["context"]["_control_attempt_id"]}

    @asynccontextmanager
    async def transaction():
        yield
        if commit_error is not None:
            raise commit_error

    database = SimpleNamespace(transaction=transaction)
    monkeypatch.setattr(handoff, "_capture_handoff", AsyncMock(return_value=receipt_by_field))
    monkeypatch.setattr(handoff, "_write_handoff", AsyncMock())
    monkeypatch.setattr(handoff, "_read_handoff", AsyncMock(return_value=receipt_by_field if committed else None))
    if not committed:
        with pytest.raises(RuntimeError, match="rollback"):
            await handoff.handoff_places_stage(database, ctx, schema="mrf", table_name=_stage_name(ctx), row_count=1000)
        assert "control_run_handoff_committed" not in ctx["context"]
    else:
        outcome = await handoff.handoff_places_stage(
            database, ctx, schema="mrf", table_name=_stage_name(ctx), row_count=1000
        )
        assert outcome == {"places_handoff": receipt_by_field}
        assert ctx["context"]["control_run_handoff_committed"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("late_error", [None, RuntimeError("after handoff"), asyncio.CancelledError()])
async def test_wrapper_never_projects_success_or_failure_after_handoff(monkeypatch, late_error):
    async def target(ctx, _task):
        ctx["context"]["control_run_handoff_committed"] = True
        ctx["context"]["_control_committed_result"] = {"places_handoff": {"complete": True}}
        if late_error is not None:
            raise late_error
        return ctx["context"]["_control_committed_result"]

    marks = AsyncMock(return_value=True)
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(control_lifecycle, "mark_control_run", marks)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: SimpleNamespace(process_data=target))
    outcome = await control_lifecycle.control_single_job_start(
        {},
        {
            "run_id": "synthetic-run",
            "importer": "places-zcta",
            "target_module": "process.places_zcta",
            "target_function": "process_data",
        },
    )
    assert outcome["status"] == "finalizing"
    assert [call.kwargs["status"] for call in marks.await_args_list] == ["running"]


@pytest.mark.asyncio
async def test_protected_publication_never_executes_ordinary_swap(monkeypatch):
    ctx = _attempt_context()
    ctx["import_date"] = "attempt"
    ctx["context"]["run"] = 1
    monkeypatch.setenv("HLTHPRT_PLACES_ZCTA_PROTECTED_PUBLICATION", "true")
    monkeypatch.setattr(places_zcta, "ensure_database", AsyncMock())
    monkeypatch.setattr(places_zcta, "_validated_places_stage_rows", AsyncMock(return_value=1000))
    monkeypatch.setattr(places_zcta, "_create_places_stage_indexes", AsyncMock())
    monkeypatch.setattr(places_zcta.db, "execute_ddl", AsyncMock())
    swap = AsyncMock()
    monkeypatch.setattr(places_zcta.db, "status", swap)
    transfer = AsyncMock(return_value={"places_handoff": {"complete": True}})
    monkeypatch.setattr(places_zcta, "handoff_places_stage", transfer)
    assert await places_zcta.publish_places_zcta_generation(ctx) == transfer.return_value
    transfer.assert_awaited_once()
    swap.assert_not_awaited()


@pytest.mark.asyncio
async def test_controlled_attempts_use_distinct_stages(monkeypatch):
    worker_context_by_field = {"import_date": "20260101", "context": {"run": 0}}
    contexts = [
        control_lifecycle._isolated_control_job_context(worker_context_by_field, "synthetic-run") for _ in range(2)
    ]
    for attempt, ctx in zip(("a", "b"), contexts):
        ctx["context"]["_control_attempt_id"] = "synthetic-run:" + attempt * 32
    monkeypatch.setattr(places_zcta, "ensure_database", AsyncMock())
    monkeypatch.setattr(places_zcta.db, "scalar", AsyncMock(return_value=15))
    create_stage = AsyncMock()
    monkeypatch.setattr(places_zcta, "_create_places_stage", create_stage)
    prepared = await asyncio.gather(*(places_zcta._prepare_places_attempt(ctx, {}) for ctx in contexts))
    assert len({stage.__tablename__ for _ctx, stage in prepared}) == 2
    assert all(ctx["context"]["_places_incumbent_oid"] == 15 for ctx, _stage in prepared)
    assert worker_context_by_field == {"import_date": "20260101", "context": {"run": 0}}
    assert create_stage.await_count == 2


@pytest.mark.asyncio
async def test_manual_startup_preserves_import_id_override(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "2026-01-02")
    monkeypatch.delenv("HLTHPRT_PLACES_ZCTA_PROTECTED_PUBLICATION", raising=False)
    monkeypatch.setattr(places_zcta, "my_init_db", AsyncMock())
    monkeypatch.setattr(places_zcta, "ensure_database", AsyncMock())
    statements = AsyncMock()
    monkeypatch.setattr(places_zcta.db, "status", statements)
    create_stage = AsyncMock()
    monkeypatch.setattr(places_zcta, "_create_places_stage", create_stage)
    worker_context_by_field = {}
    await places_zcta.startup(worker_context_by_field)
    assert worker_context_by_field["import_date"] == "20260102"
    assert create_stage.await_args.args[0].__tablename__ == "pricing_places_zcta_20260102"
    assert any(
        "DROP TABLE IF EXISTS mrf.pricing_places_zcta_20260102" in call.args[0] for call in statements.await_args_list
    )


@pytest.mark.asyncio
async def test_controlled_job_propagates_inline_publication_failure(monkeypatch):
    ctx = _attempt_context()
    monkeypatch.setattr(places_zcta, "_prepare_places_attempt", AsyncMock(return_value=(ctx, object())))
    monkeypatch.setattr(places_zcta, "download_it_and_save", AsyncMock())
    monkeypatch.setattr(places_zcta, "_detect_latest_year", AsyncMock(return_value=2025))
    monkeypatch.setattr(places_zcta, "_read_places_rows", AsyncMock(return_value=(1000, 1000)))
    publish = AsyncMock(side_effect=RuntimeError("protected canonical cannot be replaced"))
    monkeypatch.setattr(places_zcta, "publish_places_zcta_generation", publish)
    with pytest.raises(RuntimeError, match="protected canonical"):
        await places_zcta.process_data(ctx, {})
    publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_protected_mode_rejects_manual_load(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PLACES_ZCTA_PROTECTED_PUBLICATION", "true")
    create_stage = AsyncMock()
    monkeypatch.setattr(places_zcta, "_create_places_stage", create_stage)
    with pytest.raises(RuntimeError, match="controlled attempt"):
        await places_zcta.process_data({"context": {}}, {})
    create_stage.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("after rows"), asyncio.CancelledError()])
async def test_failed_controlled_load_cleans_stage_without_shutdown_publish(monkeypatch, failure):
    worker_context_by_field = {"import_date": "20260101", "context": {"run": 0}}

    async def prepare_attempt(ctx, _task):
        ctx["context"]["_places_stage_oid"] = 101
        return ctx, object()

    monkeypatch.setattr(places_zcta, "_prepare_places_attempt", prepare_attempt)
    monkeypatch.setattr(places_zcta, "download_it_and_save", AsyncMock())
    monkeypatch.setattr(places_zcta, "_detect_latest_year", AsyncMock(return_value=2025))
    monkeypatch.setattr(places_zcta, "_read_places_rows", AsyncMock(return_value=(1000, 1000)))
    publish = AsyncMock(side_effect=failure)
    cleanup = AsyncMock(return_value=True)
    monkeypatch.setattr(places_zcta, "publish_places_zcta_generation", publish)
    monkeypatch.setattr(places_zcta, "cleanup_places_attempt", cleanup)
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(control_lifecycle, "mark_control_run", AsyncMock(return_value=True))
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: places_zcta)
    task_by_field = {
        "run_id": "synthetic-run",
        "importer": "places-zcta",
        "target_module": "process.places_zcta",
        "target_function": "process_data",
    }
    if isinstance(failure, RuntimeError):
        with pytest.raises(RuntimeError, match="after rows"):
            await control_lifecycle.control_single_job_start(worker_context_by_field, task_by_field)
    else:
        outcome = await control_lifecycle.control_single_job_start(worker_context_by_field, task_by_field)
        assert outcome["status"] == "failed"
    await places_zcta.shutdown(worker_context_by_field)
    assert worker_context_by_field["context"] == {"run": 0}
    cleanup.assert_awaited_once()
    publish.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["heartbeat", "running", "succeeded", "failed"])
async def test_all_ordinary_status_writes_fence_places_handoff(monkeypatch, transition):
    writes = AsyncMock(return_value=0)
    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", writes)
    monkeypatch.setattr(control_lifecycle, "_should_update_control_run_db", AsyncMock(return_value=True))
    monkeypatch.setattr(control_lifecycle, "read_live_progress", lambda _run: None)
    if transition == "heartbeat":
        await control_lifecycle._is_control_run_heartbeat_persisted("synthetic-run", "process_data")
    else:
        await control_lifecycle.mark_control_run(
            "synthetic-run", status=transition, phase_detail="phase", progress_message="message"
        )
    statement = str(
        writes.await_args.args[0].whereclause.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    assert "places-zcta" in statement and "places_handoff" in statement and "IS NULL" in statement


def _places_cancel_run():
    ctx = _attempt_context()
    return {
        "run_id": ctx["control_run_id"],
        "importer": "places-zcta",
        "status": "running",
        "progress": {
            "attempt_id": ctx["context"]["_control_attempt_id"],
            "attempt_started_at": ctx["context"]["_control_attempt_started_at"],
        },
        "metrics": {},
    }


@pytest.mark.asyncio
async def test_missing_cancel_run_never_signals_worker(monkeypatch):
    signal = AsyncMock()
    fence = AsyncMock()
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(transaction=_transaction))
    monkeypatch.setattr(control_imports, "_lock_places_cancel_run", AsyncMock(return_value=None))
    monkeypatch.setattr(control_imports, "_signal_places_cancel", signal)
    monkeypatch.setattr(control_imports, "_fence_places_cancel", fence)
    assert await control_imports._request_protected_places_cancel("synthetic-run") is None
    signal.assert_not_awaited()
    fence.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"importer": "synthetic-importer"}, "owner changed"),
        ({"status": "unknown"}, "requires an active run"),
        ({"progress": {}}, "requires an exact active attempt"),
        ({"status": "queued", "progress": {"attempt_id": "synthetic-run:" + "a" * 32}}, "incomplete attempt"),
        (
            {"progress": {"attempt_id": "another-run:" + "a" * 32, "attempt_started_at": "2026-01-01"}},
            "identity is invalid",
        ),
        ({"status": "finalizing", "metrics": {"places_handoff": []}}, "handoff identity differs"),
        ({"status": "finalizing"}, "has no protected handoff"),
    ],
)
async def test_invalid_cancel_identity_never_fences_or_signals_worker(monkeypatch, changes, expected):
    current_run_by_field = {**_places_cancel_run(), **changes}
    signal = AsyncMock()
    fence = AsyncMock()
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(transaction=_transaction))
    monkeypatch.setattr(control_imports, "_lock_places_cancel_run", AsyncMock(return_value=current_run_by_field))
    monkeypatch.setattr(control_imports, "_signal_places_cancel", signal)
    monkeypatch.setattr(control_imports, "_fence_places_cancel", fence)
    with pytest.raises(RuntimeError, match=expected):
        await control_imports._request_protected_places_cancel(current_run_by_field["run_id"])
    signal.assert_not_awaited()
    fence.assert_not_awaited()


@pytest.mark.parametrize("changed", ["format", "run_id", "attempt_id", "attempt_started_at"])
def test_cancel_rejects_each_mismatched_handoff_identity(changed):
    current = _places_cancel_run()
    current["status"] = "finalizing"
    handoff_by_field = {"format": handoff.HANDOFF_FORMAT, "run_id": current["run_id"], **current["progress"]}
    handoff_by_field[changed] = "synthetic-mismatch"
    current["metrics"]["places_handoff"] = handoff_by_field
    with pytest.raises(RuntimeError, match="handoff identity differs"):
        control_imports._places_cancel_attempt(current)


@pytest.mark.asyncio
async def test_lost_cancel_fence_never_signals_worker(monkeypatch):
    current = _places_cancel_run()
    signal = AsyncMock()
    status = AsyncMock(return_value=0)
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(transaction=_transaction, status=status))
    monkeypatch.setattr(control_imports, "_lock_places_cancel_run", AsyncMock(return_value=current))
    monkeypatch.setattr(control_imports, "_signal_places_cancel", signal)
    with pytest.raises(RuntimeError, match="cancellation attempt changed"):
        await control_imports._request_protected_places_cancel(current["run_id"])
    signal.assert_not_awaited()
    status.assert_awaited_once()


@pytest.mark.asyncio
async def test_lost_cancel_signal_persistence_cannot_report_success(monkeypatch):
    latest = _places_cancel_run()
    latest["status"] = "canceling"
    status = AsyncMock(return_value=0)
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(transaction=_transaction, status=status))
    monkeypatch.setattr(control_imports, "_lock_places_cancel_run", AsyncMock(return_value=latest))
    attempt = (latest["progress"]["attempt_id"], latest["progress"]["attempt_started_at"])
    with pytest.raises(RuntimeError, match="changed after worker signal"):
        await control_imports._record_places_cancel_signal(
            latest["run_id"], attempt, {"worker": "canceled"}, True, False
        )
    status.assert_awaited_once()


@pytest.mark.asyncio
async def test_late_enqueue_acknowledgement_rejects_disappeared_run(monkeypatch):
    execute = AsyncMock(side_effect=[SimpleNamespace(rowcount=0), SimpleNamespace(scalar_one_or_none=lambda: None)])
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(execute=execute))
    enqueue_values_by_field = {
        "status": "queued",
        "phase_detail": "enqueued",
        "heartbeat_at": None,
        "progress": {},
        "metrics": {},
        "error": None,
    }
    with pytest.raises(RuntimeError, match="run disappeared after enqueue acknowledgement"):
        await control_imports._persist_enqueue_result("synthetic-run", "places-zcta", enqueue_values_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "route,updated",
    [("handoff", None), ("signal", None), ("signal", {"run_id": "synthetic-run", "status": "starting"})],
)
async def test_cancel_does_not_publish_events_for_missing_or_changed_run(monkeypatch, route, updated):
    current = _places_cancel_run()
    if route == "handoff":
        current["status"] = "finalizing"
        current["metrics"]["places_handoff"] = {
            "format": handoff.HANDOFF_FORMAT,
            "run_id": current["run_id"],
            **current["progress"],
        }
    signal = AsyncMock(return_value=({"worker": "canceled"}, False, False))
    persist_signal = AsyncMock()
    live = Mock()
    events = Mock()
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(transaction=_transaction))
    monkeypatch.setattr(control_imports, "_lock_places_cancel_run", AsyncMock(return_value=current))
    monkeypatch.setattr(control_imports, "_fence_places_cancel", AsyncMock())
    monkeypatch.setattr(control_imports, "_signal_places_cancel", signal)
    monkeypatch.setattr(control_imports, "_record_places_cancel_signal", persist_signal)
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=updated))
    monkeypatch.setattr(control_imports, "_write_run_live_progress", live)
    monkeypatch.setattr(control_imports, "enqueue_status_event", events)
    assert await control_imports._request_protected_places_cancel(current["run_id"]) is updated
    live.assert_not_called()
    events.assert_not_called()
    if route == "handoff":
        signal.assert_not_awaited()
        persist_signal.assert_not_awaited()
    else:
        signal.assert_awaited_once()
        persist_signal.assert_awaited_once()
