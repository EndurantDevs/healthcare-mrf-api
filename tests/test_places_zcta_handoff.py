# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import hashlib
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

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
@pytest.mark.parametrize("blocked", [None, "handed_off", "rebound", "marked", "current", "unknown", "committed"])
async def test_cleanup_fails_closed_on_publication_or_uncertain_identity(blocked):
    ctx = _attempt_context()
    ctx["context"].update(_places_stage_oid=101, _places_stage_schema="mrf", _places_stage_table=_stage_name(ctx))
    if blocked == "committed":
        ctx["context"]["control_run_handoff_committed"] = True
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
