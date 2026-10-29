# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected taxonomy acquisition uses the managed Source boundary."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

nucc = importlib.import_module("process.nucc")
from process import nucc_protected_source as source


@pytest.mark.parametrize("kind", [False, True])
async def test_source_routing_uses_the_original_fixed_relation_probe(monkeypatch, kind):
    session = object()
    probe = AsyncMock(return_value=kind)
    monkeypatch.setattr(source, "_is_protected_nucc_claim_parent", probe)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    assert await source.is_protected_source(session) is kind
    probe.assert_awaited_once_with(session)


async def test_source_routing_propagates_native_refusal(monkeypatch):
    failure = RuntimeError("synthetic native custody refusal")
    monkeypatch.setattr(source, "_is_protected_nucc_claim_parent", AsyncMock(side_effect=failure))
    with pytest.raises(RuntimeError) as caught:
        await source.is_protected_source(object())
    assert caught.value is failure


async def test_protected_source_rejects_a_different_configured_schema(monkeypatch):
    monkeypatch.setattr(source, "_is_protected_nucc_claim_parent", AsyncMock(return_value=True))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_other")
    with pytest.raises(RuntimeError, match="schema differs"):
        await source.is_protected_source(object())


async def test_managed_source_cli_returns_the_actual_created_or_replayed_run(monkeypatch):
    from api import control_imports

    actual_by_field = {"run_id": "synthetic_control_run", "status": "queued"}
    create = AsyncMock(return_value=(actual_by_field, False))
    monkeypatch.setattr(control_imports, "create_import_run", create)
    assert await source.create_managed_source_run(test_mode=False) is actual_by_field
    create.assert_awaited_once_with({"importer": "nucc", "params": {}, "triggered_by": "manual"})


async def test_managed_source_cli_refuses_partial_live_publication_before_admission(monkeypatch):
    from api import control_imports

    create = AsyncMock()
    monkeypatch.setattr(control_imports, "create_import_run", create)
    with pytest.raises(ValueError, match="isolated database"):
        await source.create_managed_source_run(test_mode=True)
    create.assert_not_awaited()


@pytest.mark.parametrize("test_mode", [False, True])
async def test_protected_cli_never_enqueues_an_unmanaged_worker(monkeypatch, test_mode):
    monkeypatch.delenv("HLTHPRT_TEST_DATABASE_SUFFIX", raising=False)

    @asynccontextmanager
    async def transaction():
        yield object()

    actual_by_field = {"run_id": "synthetic_control_run"}
    managed = AsyncMock(return_value=actual_by_field)
    unmanaged = AsyncMock(side_effect=AssertionError("protected Source cannot enqueue an unmanaged worker"))
    monkeypatch.setattr(nucc, "db", SimpleNamespace(transaction=transaction))
    monkeypatch.setattr(source, "is_protected_source", AsyncMock(return_value=True))
    monkeypatch.setattr(source, "create_managed_source_run", managed)
    monkeypatch.setattr(nucc, "create_pool", unmanaged)
    if test_mode:
        with pytest.raises(ValueError, match="isolated database"):
            await nucc.main(test_mode=True)
        managed.assert_not_awaited()
    else:
        assert await nucc.main() is actual_by_field
        managed.assert_awaited_once_with(test_mode=False)
    unmanaged.assert_not_awaited()


def copying_marker():
    from tests.test_nucc_protected_control import handoff

    value = handoff()
    marker_by_field = {key: value[key] for key in source.MARKER_FIELDS & value.keys()}
    marker_by_field.update(
        contract=source.STAGE_CONTRACT,
        phase="copying",
        publisher_oid=109,
        builder_oid=110,
        handoff=None,
        ownership={
            "importer_id": "nucc",
            "dataset_id": "11111111-1111-1111-1111-111111111111",
            "schema_name": value["child_schema"],
            "schema_oid": value["child_schema_oid"],
            "relation_oids": [["nucc_taxonomy", value["child_oid"]]],
            "sequence_oids": [],
            "auxiliary_oid": None,
        },
    )
    marker_by_field["source_contract_sha256"] = source.control.source_contract_digest(marker_by_field, {})
    marker_by_field["marker_sha256"] = source.marker_digest(marker_by_field)
    return marker_by_field


def ready_input(marker):
    ready_by_field = {
        key: marker[key] for key in ("preparation_id", "admission_sha256", "attempt_id", "attempt_started_at")
    }
    ready_by_field.update(
        contract=source.READY_CONTRACT,
        stage_oid=105,
        source_files=[
            {
                "url": "https://example.invalid/taxonomy.csv",
                "sha256": "a" * 64,
                "bytes": 200,
                "row_count": 42,
            }
        ],
        total_rows=42,
    )
    ready_by_field["ready_sha256"] = source.ready_digest(ready_by_field)
    return ready_by_field


def closed_marker():
    from tests.test_nucc_protected_control import handoff

    marker = copying_marker()
    value = handoff()
    value["source_contract_sha256"] = marker["source_contract_sha256"]
    value["handoff_sha256"] = source.control.receipt_digest(
        {key: item for key, item in value.items() if key != "handoff_sha256"}
    )
    marker.update(phase="closed", handoff=value)
    marker["marker_sha256"] = source.marker_digest(marker)
    return marker


def source_run(marker, *, ready=None, status="running"):
    return {
        "run_id": marker["run_id"],
        "node_id": marker["node_id"],
        "status": status,
        "params": {},
        "progress": {key: marker[key] for key in ("attempt_id", "attempt_started_at")},
        "metrics": {"nucc_source_stage": marker, **({"nucc_source_ready": ready} if ready is not None else {})},
    }


@pytest.fixture(params=["stage", "handoff"])
def source_poll(monkeypatch, request):
    marker = copying_marker()
    stage = source.NuccSourceStage.from_wire(marker)
    if request.param == "handoff":
        stage = replace(stage, ready=ready_input(marker))
        observed = closed_marker()
        run = source_run(marker, ready=stage.ready, status="finalizing")
        run["metrics"]["nucc_handoff"] = observed["handoff"]
        expected = observed["handoff"]
    else:
        observed, run, expected = marker, source_run(marker), stage
    state = AsyncMock(return_value=(run, observed))
    monkeypatch.setattr(source, "_source_stage_state", state)
    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value="actor"))
    monkeypatch.setattr(source.control, "reconcile_source_publication", AsyncMock(return_value=None))
    monkeypatch.setattr(source.control, "_locked_source_run", AsyncMock(return_value=run))

    async def poll(transaction):
        database = SimpleNamespace(transaction=transaction)
        if request.param == "handoff":
            return await source.wait_nucc_source_handoff(database, stage)
        context_by_field = {
            "control_run_id": marker["run_id"],
            "context": {
                "_control_attempt_id": marker["attempt_id"],
                "_control_attempt_started_at": marker["attempt_started_at"],
            },
        }
        return await source.wait_nucc_source_stage(database, context_by_field)

    return SimpleNamespace(state=state, poll=poll, expected=expected)


@pytest.mark.parametrize("fault", ["lock", "commit-lock", "other-db", "custody", "cancel", "timeout"])
async def test_source_poll_retries_only_lock_contention_after_rollback(monkeypatch, source_poll, fault):
    driver_error = RuntimeError("synthetic database error")
    driver_error.sqlstate = "55P03" if fault in {"lock", "commit-lock", "timeout"} else "42501"
    failure = DBAPIError("synthetic poll", {}, driver_error)
    if fault == "custody":
        failure = RuntimeError("synthetic custody refusal")
    if fault == "cancel":
        failure = asyncio.CancelledError()
    poll_result = source_poll.state.return_value
    if fault != "commit-lock":
        source_poll.state.side_effect = [failure, poll_result] if fault == "lock" else failure
    events = []
    transaction_state = SimpleNamespace(is_active=False)

    @asynccontextmanager
    async def transaction():
        assert transaction_state.is_active is False
        transaction_state.is_active = True
        events.append("begin")
        try:
            yield object()
            if fault == "commit-lock" and events.count("begin") == 1:
                raise failure
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")
        finally:
            transaction_state.is_active = False

    original_sleep = asyncio.sleep

    async def sleep(seconds):
        assert transaction_state.is_active is False and seconds == source.POLL_SECONDS
        events.append("sleep")
        await original_sleep(1 if fault == "timeout" else 0)

    monkeypatch.setattr(source.asyncio, "sleep", sleep)
    if fault == "timeout":
        monkeypatch.setattr(source, "WAIT_SECONDS", 0.01)
        with pytest.raises(TimeoutError):
            await source_poll.poll(transaction)
        assert events == ["begin", "rollback", "sleep"]
    elif fault in {"lock", "commit-lock"}:
        assert await source_poll.poll(transaction) == source_poll.expected
        assert events == ["begin", "rollback", "sleep", "begin", "commit"]
        assert source_poll.state.await_count == 2
    else:
        with pytest.raises(type(failure)) as caught:
            await source_poll.poll(transaction)
        assert caught.value is failure and events == ["begin", "rollback"]
    assert transaction_state.is_active is False


def test_stage_and_ready_codecs_are_closed_local_copies():
    marker = copying_marker()
    stage = source.NuccSourceStage.from_wire(marker)
    ready = ready_input(marker)
    assert source.validate_source_ready(ready, marker) == ready
    assert stage.marker == marker and stage.marker is not marker
    assert stage.ownership.relation_oids == (("nucc_taxonomy", 105),)
    assert source.validate_source_stage_marker(closed_marker())["phase"] == "closed"


@pytest.mark.parametrize(
    "field,value",
    [("extra", True), ("publisher_oid", True), ("phase", "ready"), ("builder_oid", 0), ("marker_sha256", "f" * 64)],
)
def test_stage_codec_refuses_changed_shape_or_digest(field, value):
    marker = copying_marker()
    marker[field] = value
    with pytest.raises(RuntimeError):
        source.NuccSourceStage.from_wire(marker)


@pytest.mark.parametrize(
    "field,value",
    [("extra", True), ("stage_oid", True), ("total_rows", 41), ("attempt_id", "foreign"), ("ready_sha256", "f" * 64)],
)
def test_ready_codec_refuses_changed_shape_identity_or_census(field, value):
    marker = copying_marker()
    ready = ready_input(marker)
    ready[field] = value
    with pytest.raises(RuntimeError):
        source.validate_source_ready(ready, marker)


@pytest.mark.parametrize("publisher_safe", [False, True])
@pytest.mark.parametrize("extra_source_select", [False, True])
async def test_copy_grants_allow_only_the_original_safe_publisher_read_edge(
    monkeypatch, publisher_safe, extra_source_select
):
    stage = source.NuccSourceStage.from_wire(copying_marker())
    owner = stage.marker["owner_oid"]
    entries = [(owner, owner, right, False) for right in source.partition._OWNER_RIGHTS]
    entries += [(109, owner, "SELECT", False), (110, owner, "INSERT", False)]
    if extra_source_select:
        entries += [(110, owner, "SELECT", False)]
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: entries)),
        scalar=AsyncMock(side_effect=[publisher_safe, True]),
    )
    catalog = AsyncMock()
    monkeypatch.setattr(source.native, "require_native_read_catalog", catalog)
    if publisher_safe and not extra_source_select:
        await source._copying_grants(session, stage)
        catalog.assert_awaited_once_with(session, (105,))
    else:
        with pytest.raises(RuntimeError):
            await source._copying_grants(session, stage)
        catalog.assert_not_awaited()


@pytest.mark.parametrize("size", [0, 5001])
async def test_copy_bound_refuses_before_native_state_or_protocol(monkeypatch, size):
    state, copy = AsyncMock(), AsyncMock()
    monkeypatch.setattr(source, "_source_copy_state", state)
    monkeypatch.setattr(source.stages, "copy_nucc_stage", copy)
    with pytest.raises(RuntimeError, match="batch exceeds"):
        await source.copy_nucc_source_batch(object(), source.NuccSourceStage.from_wire(copying_marker()), [{}] * size)
    state.assert_not_awaited()
    copy.assert_not_awaited()


async def test_copy_uses_the_original_native_session_and_model_order(monkeypatch):
    session = object()
    stage = source.NuccSourceStage.from_wire(copying_marker())
    state = AsyncMock(return_value="original-actor")
    monkeypatch.setattr(source.control, "_recheck_source_transaction", AsyncMock())
    copy = AsyncMock(return_value=1)
    monkeypatch.setattr(source, "_source_copy_state", state)
    monkeypatch.setattr(source.stages, "copy_nucc_stage", copy)
    row_by_field = {"code": "207Q00000X", "int_code": 7}
    assert await source.copy_nucc_source_batch(session, stage, [row_by_field]) == 1
    state.assert_awaited_once_with(session, stage)
    columns = tuple(source.native.reference_family_spec("nucc").model_types[0].__table__.columns.keys())
    copy.assert_awaited_once_with(session, stage.ownership, [tuple(row_by_field.get(key) for key in columns)])


async def test_ready_records_only_input_on_the_exact_running_attempt(monkeypatch):
    marker = copying_marker()
    stage = source.NuccSourceStage.from_wire(marker)
    ready = ready_input(marker)
    session = SimpleNamespace(scalar=AsyncMock(return_value=marker["run_id"]))
    monkeypatch.setattr(source, "_source_stage_state", AsyncMock(return_value=(source_run(marker), marker)))
    assert await source.record_nucc_source_ready(session, stage, ready["source_files"]) == ready
    statement, parameters = session.scalar.await_args.args
    assert "status='running'" in str(statement) and "finalizing" not in str(statement)
    assert "SET status" not in str(statement) and parameters["attempt_id"] == marker["attempt_id"]
    assert "heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC'" in str(statement)


@pytest.mark.parametrize(
    "fault", ["no-ready", "foreign-ready", "unrecoverable-status", "canceling", "canceled", "no-closed"]
)
async def test_closed_reconciliation_refuses_or_leaves_unclosed_state_without_installing_handoff(monkeypatch, fault):
    marker = copying_marker()
    closed = closed_marker()
    ready = ready_input(marker)
    run = source_run(marker, ready=ready)
    if fault == "no-ready":
        run["metrics"].pop("nucc_source_ready")
    if fault == "foreign-ready":
        run["metrics"]["nucc_source_ready"]["attempt_id"] = "foreign"
    if fault == "unrecoverable-status":
        run["status"] = "finalizing"
    if fault in {"canceling", "canceled"}:
        run["status"] = fault
    session = SimpleNamespace(scalar=AsyncMock(return_value=marker["run_id"]))
    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value="actor"))
    monkeypatch.setattr(source.control, "reconcile_source_publication", AsyncMock(return_value=None))
    monkeypatch.setattr(source.control, "_locked_source_run", AsyncMock(return_value=run))
    monkeypatch.setattr(source.control, "_recheck_source_transaction", AsyncMock())
    monkeypatch.setattr(
        source, "_source_stage_state", AsyncMock(return_value=(run, marker if fault == "no-closed" else closed))
    )
    if fault == "no-closed":
        assert await source.reconcile_nucc_source_handoff(session, marker["run_id"]) is None
    else:
        with pytest.raises((RuntimeError, ValueError)):
            await source.reconcile_nucc_source_handoff(session, marker["run_id"])
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("status", ["running", "failed", "dead_letter"])
async def test_closed_reconciliation_installs_only_real_closed_handoff_after_owner_reconcile(monkeypatch, status):
    marker = copying_marker()
    closed = closed_marker()
    ready = ready_input(marker)
    run = source_run(marker, ready=ready)
    run["status"] = status
    events = []
    session = SimpleNamespace(scalar=AsyncMock(return_value=marker["run_id"]))

    async def reconcile(actual, run_id):
        assert actual is session and run_id == marker["run_id"]
        events.append("owner-outcome")

    async def state(actual, stage, *, copying, allow_pending_retry):
        assert actual is session and copying is False and stage.marker == marker and allow_pending_retry is True
        events.append("native-closed")
        return run, closed

    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value="actor"))
    monkeypatch.setattr(source.control, "reconcile_source_publication", reconcile)
    monkeypatch.setattr(source.control, "_locked_source_run", AsyncMock(return_value=run))
    monkeypatch.setattr(source.control, "_recheck_source_transaction", AsyncMock())
    monkeypatch.setattr(source, "_source_stage_state", state)
    assert await source.reconcile_nucc_source_handoff(session, marker["run_id"]) == closed["handoff"]
    assert events == ["owner-outcome", "native-closed"]
    statement, parameters = session.scalar.await_args.args
    assert "status='finalizing'" in str(statement) and "succeeded" not in str(statement)
    assert "nucc_source_ready" in str(statement) and "nucc_source_stage" in str(statement)
    assert parameters["attempt_id"] == marker["attempt_id"] and parameters["phase"] == source.control.HANDOFF_PHASE
    assert parameters["prior_status"] == status and "error=NULL,finished_at=NULL" in str(statement)
    assert "heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC'" in str(statement)


async def test_native_state_refusal_propagates_before_handoff_cas(monkeypatch):
    marker = copying_marker()
    run = source_run(marker, ready=ready_input(marker))
    session = SimpleNamespace(scalar=AsyncMock())
    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value="actor"))
    monkeypatch.setattr(source.control, "reconcile_source_publication", AsyncMock(return_value=None))
    monkeypatch.setattr(source.control, "_locked_source_run", AsyncMock(return_value=run))
    failure = RuntimeError("synthetic actual catalog refusal")
    monkeypatch.setattr(source, "_source_stage_state", AsyncMock(side_effect=failure))
    with pytest.raises(RuntimeError) as caught:
        await source.reconcile_nucc_source_handoff(session, marker["run_id"])
    assert caught.value is failure
    session.scalar.assert_not_awaited()


async def test_queue_admission_replays_actual_genuine_identity_before_creating_any_new_run(monkeypatch):
    from api import control_imports

    actual_by_field = {"run_id": "synthetic-existing", "importer": "nucc"}
    get, create = AsyncMock(return_value=actual_by_field), AsyncMock()
    monkeypatch.setattr(control_imports, "get_import_run", get)
    monkeypatch.setattr(control_imports, "create_import_run", create)
    assert (
        await source.create_managed_source_run(
            test_mode=False, queue_context={}, task={"run_id": actual_by_field["run_id"]}
        )
        is actual_by_field
    )
    create.assert_not_awaited()


async def test_raw_queue_retry_has_stable_managed_idempotency_without_worker_context_mutation(monkeypatch):
    from api import control_imports

    create = AsyncMock(return_value=({"run_id": "synthetic"}, True))
    monkeypatch.setattr(control_imports, "create_import_run", create)
    context_by_field = {"job_id": "synthetic-old-queue-job", "context": {"run": 0}}
    await source.create_managed_source_run(test_mode=False, queue_context=context_by_field, task={})
    first = create.await_args.args[0]
    await source.create_managed_source_run(test_mode=False, queue_context=context_by_field, task={})
    assert create.await_args.args[0] == first and first["idempotency_key"].startswith("taxonomy-queue-")
    assert context_by_field == {"job_id": "synthetic-old-queue-job", "context": {"run": 0}}


@pytest.mark.parametrize("commit_error", [None, RuntimeError("synthetic lost COMMIT ACK")])
@pytest.mark.parametrize("readback", ["closed", "absent", "unavailable"])
async def test_source_ready_does_not_arm_handoff_until_fresh_native_closed_readback(
    monkeypatch, commit_error, readback
):
    marker = copying_marker()
    stage = source.NuccSourceStage.from_wire(marker)
    ready = ready_input(marker)
    context_by_field = {"nucc_source_stage_bound": stage, "nucc_source_files": ready["source_files"]}

    @asynccontextmanager
    async def transaction():
        yield "original-session"
        if commit_error is not None:
            raise commit_error

    async def read(database, ready_stage):
        assert database is nucc.db and ready_stage.ready == ready
        assert context_by_field.get("control_run_handoff_committed") is not True
        assert context_by_field["nucc_native_commit_unknown"] is True
        if readback == "absent":
            raise source.NuccSourceReadyNotCommittedError("synthetic native ready absence")
        if readback == "unavailable":
            raise RuntimeError("synthetic catalog unavailable")
        return closed_marker()["handoff"]

    monkeypatch.setattr(nucc.db, "transaction", transaction)
    monkeypatch.setattr(source, "record_nucc_source_ready", AsyncMock(return_value=ready))
    monkeypatch.setattr(source, "wait_nucc_source_handoff", read)
    if readback == "closed":
        result_by_field = await nucc._handoff_protected_source({"context": context_by_field})
        assert context_by_field["control_run_handoff_committed"] is True and result_by_field["rows"] == 42
        assert "nucc_native_commit_unknown" not in context_by_field
    else:
        with pytest.raises(RuntimeError):
            await nucc._handoff_protected_source({"context": context_by_field})
        assert "control_run_handoff_committed" not in context_by_field
        assert context_by_field.get("nucc_native_commit_unknown", False) is (readback == "unavailable")


async def test_source_ready_commit_cancellation_preserves_recovery_without_waiting(monkeypatch):
    stage = source.NuccSourceStage.from_wire(copying_marker())
    ready = ready_input(stage.marker)
    context_by_field = {"nucc_source_stage_bound": stage, "nucc_source_files": ready["source_files"]}

    @asynccontextmanager
    async def transaction():
        yield "original-session"
        raise asyncio.CancelledError

    readback = AsyncMock(return_value=closed_marker()["handoff"])
    monkeypatch.setattr(nucc.db, "transaction", transaction)
    monkeypatch.setattr(source, "record_nucc_source_ready", AsyncMock(return_value=ready))
    monkeypatch.setattr(source, "wait_nucc_source_handoff", readback)
    with pytest.raises(asyncio.CancelledError):
        await nucc._handoff_protected_source({"context": context_by_field})
    readback.assert_not_awaited()
    assert context_by_field["nucc_source_stage_bound"].ready == ready
    assert context_by_field["nucc_native_commit_unknown"] is True
    assert (
        "control_run_handoff_committed" not in context_by_field and "_control_committed_result" not in context_by_field
    )


async def test_plain_copy_uses_no_legacy_insert_fallback_and_preserves_null_encoding(monkeypatch):
    stage_by_field = {"schema": "mrf", "name": "nucc_taxonomy_s_synthetic", "oid": 105, "owner_oid": 110}
    state, copy = AsyncMock(), AsyncMock(return_value=1)

    @asynccontextmanager
    async def transaction():
        yield "original-session"

    monkeypatch.setattr(nucc.db, "transaction", transaction)
    monkeypatch.setattr(nucc, "_require_plain_stage", state)
    monkeypatch.setattr(nucc.native, "native_copy_record_batch", copy)
    row_by_field = {"code": "207Q00000X", "int_code": 7, "classification": None}
    await nucc._write_nucc_batch({"context": {"nucc_plain_stage": stage_by_field}}, object(), [row_by_field])
    state.assert_awaited_once_with("original-session", stage_by_field)
    columns = tuple(nucc.NUCCTaxonomy.__table__.columns.keys())
    copy.assert_awaited_once_with(
        "original-session",
        nucc.NUCCTaxonomy,
        schema_name=stage_by_field["schema"],
        table_name=stage_by_field["name"],
        columns=columns,
        records=[tuple(row_by_field.get(key) for key in columns)],
    )


async def test_raw_job_only_admits_without_shared_context_mutation(monkeypatch):
    context_by_field = {"job_id": "synthetic-old-job", "context": {"run": 0}, "import_date": "20260101"}
    managed = AsyncMock(return_value={"run_id": "actual"})
    producer = AsyncMock(side_effect=AssertionError("raw shim must not copy or publish"))
    monkeypatch.setattr(source, "create_managed_source_run", managed)
    monkeypatch.setattr(nucc, "_process_admitted_nucc_data", producer)
    assert await nucc.process_nucc_data(context_by_field, {"test_mode": False}) == 1
    assert context_by_field == {"job_id": "synthetic-old-job", "context": {"run": 0}, "import_date": "20260101"}
    managed.assert_awaited_once_with(test_mode=False, queue_context=context_by_field, task={"test_mode": False})
    producer.assert_not_awaited()


@pytest.mark.parametrize(
    "fault", [None, "foreign-native-marker", "changed-attempt", "changed-params", "foreign-actor", "changed-history"]
)
async def test_actual_source_state_checks_full_original_binding_before_any_cas(monkeypatch, fault):
    from copy import deepcopy

    marker = copying_marker()
    stage = source.NuccSourceStage.from_wire(marker)
    observed = deepcopy(marker)
    run = source_run(marker)
    actor_by_field = {"database_oid": 100, "owner_oid": 102, "node_id": marker["node_id"], "actor_oid": 110}
    if fault == "foreign-native-marker":
        observed["preparation_id"] = "99999999-9999-4999-8999-999999999999"
    if fault == "changed-attempt":
        run["progress"]["attempt_id"] = marker["run_id"] + ":" + "f" * 32
    if fault == "changed-params":
        run["params"] = {"foreign": True}
    if fault == "foreign-actor":
        actor_by_field["actor_oid"] = 111
    session = SimpleNamespace(scalar=AsyncMock(return_value=999 if fault == "changed-history" else 101))
    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value=actor_by_field))
    monkeypatch.setattr(source.control, "reconcile_source_publication", AsyncMock(return_value=None))
    monkeypatch.setattr(source.control, "_locked_source_run", AsyncMock(return_value=run))
    monkeypatch.setattr(source.control, "_recheck_source_transaction", AsyncMock())
    monkeypatch.setattr(source, "_native_stage_marker", AsyncMock(return_value=(observed, False)))
    if fault:
        with pytest.raises(RuntimeError):
            await source._source_stage_state(session, stage, copying=False)
        source.control._recheck_source_transaction.assert_not_awaited()
    else:
        assert await source._source_stage_state(session, stage, copying=False) == (run, observed)
        source.control._recheck_source_transaction.assert_awaited_once_with(session, actor_by_field)


@pytest.mark.parametrize("fault", [None, "canceling", "changed-attempt", "already-ready"])
async def test_copy_authentication_has_no_action_row_or_shared_parent_fence(monkeypatch, fault):
    marker = copying_marker()
    stage = source.NuccSourceStage.from_wire(marker)
    run_by_field = {**source_run(marker), "engine": "healthcare-mrf-api", "importer": "nucc"}
    if fault == "canceling":
        run_by_field["status"] = "canceling"
    if fault == "changed-attempt":
        run_by_field["progress"]["attempt_id"] = marker["run_id"] + ":" + "f" * 32
    if fault == "already-ready":
        run_by_field["metrics"]["nucc_source_ready"] = ready_input(marker)
    actor_by_field = {"database_oid": 100, "owner_oid": 102, "node_id": marker["node_id"], "actor_oid": 110}
    statements = []

    async def execute(statement, _parameters):
        statements.append(str(statement))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: run_by_field))

    async def scalar(statement):
        statements.append(str(statement))
        return 101

    session = SimpleNamespace(execute=execute, scalar=scalar)
    forbidden = AsyncMock(side_effect=AssertionError("COPY must not acquire a control or parent fence"))
    monkeypatch.setattr(source.control, "reconcile_source_publication", forbidden)
    monkeypatch.setattr(source.control, "_locked_source_run", forbidden)
    monkeypatch.setattr(source.partition, "capture_nucc_parent", forbidden)
    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value=actor_by_field))
    monkeypatch.setattr(source, "_native_stage_marker", AsyncMock(return_value=(marker, False)))
    grants = AsyncMock()
    monkeypatch.setattr(source, "_copying_grants", grants)
    if fault:
        with pytest.raises(RuntimeError):
            await source._source_copy_state(session, stage)
        grants.assert_not_awaited()
    else:
        assert await source._source_copy_state(session, stage) is actor_by_field
        grants.assert_awaited_once_with(session, stage)
    forbidden.assert_not_awaited()
    assert statements and all("FOR UPDATE" not in statement and "LOCK" not in statement for statement in statements)


@pytest.mark.parametrize(
    "published,removed,status",
    [(True, False, "succeeded"), (False, True, "canceling"), (False, False, "canceling"), (False, True, "running")],
)
async def test_cancel_requires_cleanup_and_committed_publication_wins(monkeypatch, published, removed, status):
    marker = closed_marker()
    stage = source.NuccSourceStage.from_wire(marker)
    ready = ready_input(marker)
    run = source_run(marker, ready=ready, status=status)
    run["metrics"]["nucc_handoff"] = marker["handoff"]
    actor_by_field = {"database_oid": 100, "owner_oid": 102, "node_id": marker["node_id"], "actor_oid": 110}
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[removed, marker["run_id"]]))
    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value=actor_by_field))
    monkeypatch.setattr(
        source.control,
        "reconcile_source_publication",
        AsyncMock(return_value={"actual": "event"} if published else None),
    )
    monkeypatch.setattr(source.control, "_locked_source_run", AsyncMock(return_value=run))
    monkeypatch.setattr(source.control, "_recheck_source_transaction", AsyncMock())
    if published or removed and status == "canceling":
        assert await source.consume_nucc_source_cancel(session, stage) is run
        assert session.scalar.await_count == (0 if published else 2)
        if not published:
            statement, parameters = session.scalar.await_args.args
            assert "status='canceled'" in str(statement) and "metrics=" not in str(statement)
            assert parameters["attempt_id"] == marker["attempt_id"]
            assert "heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC'" in str(statement)
            assert "finished_at=clock_timestamp() AT TIME ZONE 'UTC'" in str(statement)
    else:
        with pytest.raises(RuntimeError):
            await source.consume_nucc_source_cancel(session, stage)
        assert session.scalar.await_count == int(status == "canceling")


async def test_isolated_test_job_uses_its_own_pool_and_disconnects_on_failure(monkeypatch):
    from copy import deepcopy

    context_by_field = {"import_date": "20260101", "context": {"start": "unchanged", "run": 0}}
    original = deepcopy(context_by_field)
    test_pool = SimpleNamespace(connect=AsyncMock(), disconnect=AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield "isolated-test-session"

    test_pool.transaction = transaction
    monkeypatch.setenv("HLTHPRT_TEST_DATABASE_SUFFIX", "_isolated")
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "synthetic-base")
    monkeypatch.setattr(nucc, "Database", lambda: test_pool)
    base = AsyncMock(side_effect=AssertionError("test jobs cannot switch the base pool"))
    monkeypatch.setattr(nucc, "ensure_database", base)
    monkeypatch.setattr(source, "is_protected_source", AsyncMock(return_value=False))
    monkeypatch.setattr(nucc, "_prepare_plain_stage", AsyncMock())
    monkeypatch.setattr(nucc, "_discover_nucc_source_files", AsyncMock(return_value=[]))
    with pytest.raises(RuntimeError, match="source files are unavailable"):
        await nucc.process_nucc_data(context_by_field, {"test_mode": True})
    assert context_by_field == original and test_pool._database_override == "synthetic-base_isolated"
    test_pool.connect.assert_awaited_once()
    test_pool.disconnect.assert_awaited_once()
    base.assert_not_awaited()


async def test_cancel_after_lost_locator_ack_is_ordinary_cancellation_without_publication(monkeypatch):
    marker = copying_marker()
    stage = source.NuccSourceStage.from_wire(marker)
    run = source_run(marker, status="canceling")
    run["metrics"] = {}
    actor_by_field = {"database_oid": 100, "owner_oid": 102, "node_id": marker["node_id"], "actor_oid": 110}
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[True, marker["run_id"]]))
    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value=actor_by_field))
    monkeypatch.setattr(source.control, "reconcile_source_publication", AsyncMock(return_value=None))
    monkeypatch.setattr(source.control, "_locked_source_run", AsyncMock(return_value=run))
    monkeypatch.setattr(source.control, "_recheck_source_transaction", AsyncMock())
    assert await source.consume_nucc_source_cancel(session, stage) is run
    absence, parameters = session.scalar.await_args_list[0].args
    assert "history_oid" in str(absence) and parameters["history_oid"] == marker["history_oid"]
    update, parameters = session.scalar.await_args_list[1].args
    assert "status='canceled'" in str(update) and "finalizing" not in str(update) and "succeeded" not in str(update)
    assert "nucc_handoff" not in parameters["metrics"]


async def test_test_mode_census_counts_only_the_rows_actually_copied(monkeypatch, tmp_path):
    path = tmp_path / "taxonomy.csv"
    path.write_text("Code,Definition\n" + "207Q00000X,Synthetic\n" * 501)
    batches = []

    async def capture(_ctx, _model, rows):
        batches.append(list(rows))

    monkeypatch.setattr(nucc, "_write_nucc_batch", capture)
    count = await nucc._stage_nucc_taxonomy_rows(
        {"context": {}},
        {},
        str(path),
        {"Code": "code", "Definition": "definition"},
        object(),
        test_mode=True,
        run_id="",
        source_file="synthetic.csv",
    )
    assert count == 500 and [len(batch) for batch in batches] == [500]


async def test_plain_validation_failure_cleans_only_its_unpublished_candidate(monkeypatch):
    stage_by_field = {"schema": "mrf", "name": "nucc_taxonomy_s_synthetic", "oid": 105}
    context_by_field = {"context": {"nucc_plain_stage": stage_by_field, "rows": 2, "run": 1}}

    @asynccontextmanager
    async def transaction():
        yield "original-session"

    monkeypatch.setattr(nucc.db, "transaction", transaction)
    monkeypatch.setattr(nucc, "_plain_run", AsyncMock())
    monkeypatch.setattr(nucc, "_finish_plain_stage", AsyncMock(side_effect=RuntimeError("synthetic invalid set")))
    cleanup = AsyncMock()
    monkeypatch.setattr(nucc, "_cleanup_plain_stage", cleanup)
    with pytest.raises(RuntimeError, match="invalid set"):
        await nucc._publish_plain_generation(context_by_field)
    cleanup.assert_awaited_once_with(context_by_field)
    assert "control_run_terminal_committed" not in context_by_field["context"]
    assert "nucc_native_commit_unknown" not in context_by_field["context"]


def _canceled_retry(monkeypatch, *, removed=True, changed=True):
    marker = copying_marker()
    stage = source.NuccSourceStage.from_wire(marker)
    run = source_run(marker, status="canceling")
    run["progress"].update(
        attempt_id=marker["run_id"] + ":" + "2" * 32,
        attempt_started_at="2026-10-17T05:00:00.000000+00:00",
        detail="current attempt",
    )
    actor_by_field = {"database_oid": 100, "owner_oid": 102, "node_id": marker["node_id"], "actor_oid": 110}
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[removed, marker["run_id"] if changed else None]))
    monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value=actor_by_field))
    monkeypatch.setattr(source.control, "reconcile_source_publication", AsyncMock(return_value=None))
    monkeypatch.setattr(source.control, "_locked_source_run", AsyncMock(return_value=run))
    monkeypatch.setattr(source.control, "_recheck_source_transaction", AsyncMock())
    return session, stage, run


@pytest.mark.parametrize("old_ready", [False, True])
async def test_retry_cancel_consumes_current_attempt_after_old_copying_cleanup(monkeypatch, old_ready):
    import json
    from copy import deepcopy

    session, stage, run = _canceled_retry(monkeypatch)
    if old_ready:
        run["metrics"]["nucc_source_ready"] = ready_input(stage.marker)
    before = deepcopy(stage.marker)
    assert await source.consume_nucc_source_cancel(session, stage) is run
    absence, absence_parameters = session.scalar.await_args_list[0].args
    assert "NOT EXISTS" in str(absence) and "to_regclass(:relation) IS NULL" in str(absence)
    assert absence_parameters["oid"] == stage.ownership.relation_oids[0][1]
    statement, parameters = session.scalar.await_args_list[1].args
    assert parameters["attempt_id"] == run["progress"]["attempt_id"] != stage.marker["attempt_id"]
    assert parameters["attempt_started_at"] == run["progress"]["attempt_started_at"]
    assert json.loads(parameters["progress"]) == run["progress"]
    assert json.loads(parameters["metrics"]) == run["metrics"]
    assert "progress::jsonb=CAST(:progress AS jsonb)" in str(statement)
    assert "metrics::jsonb=CAST(:metrics AS jsonb)" in str(statement)
    assert "status='canceled'" in str(statement) and "metrics=" not in str(statement)
    assert "heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC'" in str(statement)
    assert "finished_at=clock_timestamp() AT TIME ZONE 'UTC'" in str(statement)
    assert "succeeded" not in str(statement) and "finalizing" not in str(statement)
    assert stage.marker == before


@pytest.mark.parametrize(
    "fault",
    [
        "older",
        "same_time",
        "same_id",
        "invalid_time",
        "naive_time",
        "missing_time",
        "foreign_attempt",
        "foreign_locator",
        "current_stage",
        "closed_locator",
        "missing_locator",
        "current_ready",
        "foreign_ready",
        "handoff",
        "closed_stage",
        "changed_params",
        "running",
    ],
)
async def test_retry_cancel_refuses_changed_or_incomplete_stage_or_current_attempt(monkeypatch, fault):
    session, stage, run = _canceled_retry(monkeypatch)
    match fault:
        case "older":
            run["progress"]["attempt_started_at"] = "2026-10-17T03:00:00+00:00"
        case "same_time":
            run["progress"]["attempt_started_at"] = stage.marker["attempt_started_at"]
        case "same_id":
            run["progress"]["attempt_id"] = stage.marker["attempt_id"]
        case "invalid_time":
            run["progress"]["attempt_started_at"] = "invalid"
        case "naive_time":
            run["progress"]["attempt_started_at"] = "2026-10-17T05:00:00"
        case "missing_time":
            run["progress"].pop("attempt_started_at")
        case "foreign_attempt":
            run["progress"]["attempt_id"] = "foreign-run:" + "2" * 32
        case "foreign_locator" | "current_stage":
            locator_by_field = dict(stage.marker)
            locator_by_field["node_id" if fault == "foreign_locator" else "attempt_id"] = (
                "foreign-node" if fault == "foreign_locator" else run["progress"]["attempt_id"]
            )
            locator_by_field["source_contract_sha256"] = source.control.source_contract_digest(locator_by_field, {})
            locator_by_field["marker_sha256"] = source.marker_digest(locator_by_field)
            run["metrics"]["nucc_source_stage"] = locator_by_field
        case "closed_locator":
            run["metrics"]["nucc_source_stage"] = closed_marker()
        case "missing_locator":
            run["metrics"].clear()
        case "current_ready" | "foreign_ready":
            ready = ready_input(stage.marker)
            ready["attempt_id" if fault == "current_ready" else "stage_oid"] = (
                run["progress"]["attempt_id"] if fault == "current_ready" else 106
            )
            ready["ready_sha256"] = source.ready_digest(ready)
            run["metrics"]["nucc_source_ready"] = ready
        case "handoff":
            run["metrics"]["nucc_handoff"] = closed_marker()["handoff"]
        case "closed_stage":
            stage = source.NuccSourceStage.from_wire(closed_marker())
            run["metrics"]["nucc_source_stage"] = stage.marker
        case "changed_params":
            run["params"] = {"changed": True}
        case _:
            run["status"] = "running"
    with pytest.raises(RuntimeError):
        await source.consume_nucc_source_cancel(session, stage)
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("removed,changed", [(False, True), (True, False)])
async def test_retry_cancel_refuses_uncommitted_cleanup_or_stale_current_source_cas(monkeypatch, removed, changed):
    session, stage, _run = _canceled_retry(monkeypatch, removed=removed, changed=changed)
    with pytest.raises(RuntimeError):
        await source.consume_nucc_source_cancel(session, stage)
    assert session.scalar.await_count == (2 if removed else 1)


async def test_retry_cancel_cannot_acknowledge_a_different_native_cleanup_target(monkeypatch):
    from dataclasses import replace

    session, stage, _run = _canceled_retry(monkeypatch)
    other_ownership = replace(stage.ownership, relation_oids=(("nucc_taxonomy", 106),))
    with pytest.raises(RuntimeError, match="ownership differs"):
        await source.consume_nucc_source_cancel(session, replace(stage, ownership=other_ownership))
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("old_ready", [False, True])
async def test_dispatch_authenticates_old_copying_input_only_as_pending_allocation(monkeypatch, old_ready):
    session, stage, run = _canceled_retry(monkeypatch)
    run["status"] = "running"
    session.scalar = AsyncMock(return_value=stage.marker["history_oid"])
    if old_ready:
        run["metrics"]["nucc_source_ready"] = ready_input(stage.marker)
    monkeypatch.setattr(source, "_native_stage_marker", AsyncMock(return_value=(stage.marker, False)))
    assert await source.reconcile_nucc_source_handoff(session, run["run_id"]) is None
    assert session.scalar.await_count == 1
    assert "to_regclass('mrf.import_run')" in str(session.scalar.await_args.args[0])
    assert "nucc_handoff" not in run["metrics"] and run["status"] == "running"


@pytest.mark.parametrize("allow_pending_retry,closed", [(False, False), (True, True)])
async def test_retry_never_relaxes_regular_or_closed_source_stage_identity(monkeypatch, allow_pending_retry, closed):
    session, stage, run = _canceled_retry(monkeypatch)
    run["status"] = "running"
    if closed:
        stage = source.NuccSourceStage.from_wire(closed_marker())
        run["metrics"]["nucc_source_stage"] = stage.marker
    monkeypatch.setattr(source, "_native_stage_marker", AsyncMock(return_value=(stage.marker, False)))
    with pytest.raises(RuntimeError, match="original Source attempt changed"):
        await source._source_stage_state(session, stage, copying=False, allow_pending_retry=allow_pending_retry)
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("old_ready", [False, True])
async def test_new_worker_waits_outside_its_transaction_for_a_fresh_attempt_stage(monkeypatch, old_ready):
    from copy import deepcopy

    session, old_stage, run = _canceled_retry(monkeypatch)
    run["status"] = "running"
    if old_ready:
        run["metrics"]["nucc_source_ready"] = ready_input(old_stage.marker)
    current = deepcopy(old_stage.marker)
    current.update(run["progress"])
    current.pop("detail")
    current["ownership"].update(
        dataset_id="22222222-2222-2222-2222-222222222222",
        schema_name="reference_family_archive_" + "2" * 32,
        schema_oid=107,
        relation_oids=[["nucc_taxonomy", 106]],
    )
    current["source_contract_sha256"] = source.control.source_contract_digest(current, {})
    current["marker_sha256"] = source.marker_digest(current)
    transaction_state = SimpleNamespace(in_transaction=False)

    @asynccontextmanager
    async def transaction():
        assert transaction_state.in_transaction is False
        transaction_state.in_transaction = True
        try:
            yield session
        finally:
            transaction_state.in_transaction = False

    async def sleep(_seconds):
        assert transaction_state.in_transaction is False
        run["metrics"] = {"nucc_source_stage": current}

    state = AsyncMock(side_effect=[(run, old_stage.marker), (run, current)])
    monkeypatch.setattr(source, "_source_stage_state", state)
    monkeypatch.setattr(source.asyncio, "sleep", sleep)
    context_by_field = {
        "control_run_id": run["run_id"],
        "context": {
            "_control_attempt_id": run["progress"]["attempt_id"],
            "_control_attempt_started_at": run["progress"]["attempt_started_at"],
        },
    }
    received_stage = await source.wait_nucc_source_stage(SimpleNamespace(transaction=transaction), context_by_field)
    assert received_stage.marker == current and received_stage.marker != old_stage.marker
    assert state.await_count == 2
    assert all(call.kwargs == {"copying": True, "allow_pending_retry": True} for call in state.await_args_list)
