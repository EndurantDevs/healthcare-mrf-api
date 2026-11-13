# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic shared-entrypoint and exact-selector recovery checks."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_rooted_graph_operator as operator
from process import provider_directory_rooted_graph_publication_store as publication_store
from process import provider_directory_rooted_graph_worker as worker
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff
from tests.test_provider_directory_rooted_graph_operator import _legacy_current


def current_root():
    return replace(_legacy_current(), root_cohort_id="pdufc_" + "c" * 48)


def operation():
    current = current_root()
    identity = operator.build_rooted_graph_single_root_identity(current, operation_key="a" * 64)
    return {
        "contract_id": worker.ROOTED_GRAPH_CONTRACT,
        "operation_key": "a" * 64,
        "publication_acquisition_id": identity.candidate.acquisition_id,
        "root_dataset_id": current.dataset_id,
        "root_dataset_hash": current.dataset_hash,
        "root_content_proof_sha256": current.root_content_proof_sha256,
        "root_cohort_id": current.root_cohort_id,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("admission", ["present", "missing", "changed"])
async def test_admission_uses_configured_named_input_table(monkeypatch, admission):
    from process import provider_directory_rooted_graph_source_runtime as source_runtime

    root = "provider_directory_rooted_graph_twin_admission"
    coordinates = Mock(return_value=("candidate_generation", root))
    monkeypatch.setattr(
        source_runtime, "current_rooted_graph_runtime", lambda: SimpleNamespace(relation_coordinates=coordinates)
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "catalog")
    monkeypatch.setenv("DB_SCHEMA", "catalog")
    selected_operation = operation()
    row_by_field = {
        "acquisition_operation_key": selected_operation["operation_key"],
        **{name: selected_operation[name] for name in worker._ROOT_FIELDS},
    }
    if admission == "changed":
        row_by_field["root_dataset_hash"] = "changed"
    database = SimpleNamespace(first=AsyncMock(return_value=None if admission == "missing" else row_by_field))
    directory = SimpleNamespace(db=database)

    @asynccontextmanager
    async def transaction(selected_database):
        assert selected_database is database
        yield

    acquire = AsyncMock()
    monkeypatch.setattr(worker, "source_transaction", transaction)
    monkeypatch.setattr(worker, "_acquire_pinned_root", acquire)
    if admission == "changed":
        with pytest.raises(ValueError, match="admission_changed"):
            await worker._ensure_admission(directory, selected_operation)
    else:
        await worker._ensure_admission(directory, selected_operation)
    coordinates.assert_called_once_with("catalog", root, root)
    assert 'FROM "candidate_generation"."' + root + '"' in database.first.await_args.args[0]
    assert database.first.await_args.kwargs == {"selector": selected_operation["publication_acquisition_id"]}
    assert acquire.await_count == int(admission == "missing")


def task():
    return {
        **worker.ACQUISITION_FLAGS,
        "run_id": "run_" + "d" * 32,
        "source_ids": [worker.PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID],
        "rooted_graph": operation(),
    }


def publication_readiness():
    return SimpleNamespace(
        **{key: field_value for key, field_value in operation().items() if key not in {"contract_id", "operation_key"}},
        dataset_id="pdrgpd_" + "e" * 48,
        dataset_hash="f" * 64,
        resource_count=7,
        cohort_complete=False,
        source_id=worker.PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        endpoint_id="a" * 64,
        acquisition_root_run_id="pdrgpr_" + "a" * 48,
        request_failure_coverage=None,
        rooted_graph_complete=True,
        endpoint_collection_complete=False,
        endpoint_complete=False,
    )


def prepared_handoff():
    return FHIRSourceCandidateHandoff(
        json.dumps(
            {
                "contract": "healthporta.provider-directory.rooted-candidate.v1",
                "phase": "ready_for_freeze",
                "identity": {
                    "dataset_id": publication_readiness().dataset_id,
                    "publication_acquisition_id": operation()["publication_acquisition_id"],
                },
            }
        )
    )


@pytest.fixture
def runtime(monkeypatch):
    """Bind isolated synthetic acquisition and native publication collaborators."""
    task_by_field = task()
    root = SimpleNamespace(
        dataset_id=task_by_field["rooted_graph"]["root_dataset_id"],
        dataset_hash=task_by_field["rooted_graph"]["root_dataset_hash"],
        root_content_proof_sha256=task_by_field["rooted_graph"]["root_content_proof_sha256"],
        root_cohort_id=task_by_field["rooted_graph"]["root_cohort_id"],
    )
    database = SimpleNamespace(first=AsyncMock(return_value=None))
    directory = SimpleNamespace(
        db=database,
        _schema=lambda: "synthetic",
        _provider_directory_dataset_followup=lambda **fields: fields,
    )
    scope_events = []

    @asynccontextmanager
    async def transaction(actual):
        assert actual is database
        scope_events.append("enter")
        try:
            yield
        finally:
            scope_events.append("exit")

    monkeypatch.setattr(worker, "source_transaction", transaction)
    monkeypatch.setattr(worker, "source_transaction_available", Mock())
    complete = AsyncMock(side_effect=lambda _ctx, _task, metrics: metrics)
    scope = SimpleNamespace(
        operation=task_by_field["rooted_graph"], run_id=task_by_field["run_id"], publish_prepared_rooted_graph=complete
    )
    monkeypatch.setattr(worker, "current_rooted_graph_runtime", lambda _database: scope)
    monkeypatch.setattr(worker, "_record_phase", AsyncMock())
    original_summary = worker._publication_summary
    monkeypatch.setattr(worker, "_publication_summary", AsyncMock(return_value={"state": "partial"}))
    monkeypatch.setattr(operator, "_select_exact_current_root", AsyncMock(return_value=root))
    acquisition_by_field = {**task_by_field["rooted_graph"], "status": "admitted"}
    acquire = AsyncMock(return_value=json.dumps(acquisition_by_field))
    monkeypatch.setattr(operator, "acquire_pinned_root_operation", acquire)
    readiness = publication_readiness()
    handoff = prepared_handoff()
    publish = AsyncMock(return_value=handoff)
    monkeypatch.setattr(publication_store, "prepare_admitted_rooted_graph_dataset", publish, raising=False)
    return SimpleNamespace(
        task=task_by_field,
        ctx={},
        directory=directory,
        acquire=acquire,
        publish=publish,
        readiness=readiness,
        scope=scope_events,
        root=root,
        original_summary=original_summary,
        handoff=handoff,
        complete=complete,
    )


@pytest.mark.parametrize(
    "change",
    [
        {"source_ids": ["ordinary"]},
        {"probe": True},
        {"full_refresh": 1},
        {"test_mode": True},
        {"rooted_graph": {}},
        {"rooted_graph": {**operation(), "operation_key": "invalid"}},
        {"rooted_graph": {**operation(), "extra": "unexpected"}},
    ],
)
def test_exact_task_rejects_scope_and_contract_changes(change):
    with pytest.raises(ValueError, match="task_invalid"):
        worker.validated_rooted_graph_task({**task(), **change})


@pytest.mark.asyncio
async def test_normal_process_data_dispatches_exact_route_before_generic_flow(runtime, monkeypatch):
    import importlib

    directory = importlib.import_module("process.provider_directory_fhir")

    callback = AsyncMock(return_value={"synthetic": True})
    monkeypatch.setattr(worker, "process_rooted_graph_task", callback)
    assert await directory.process_data(runtime.ctx, runtime.task) == {"synthetic": True}
    callback.assert_awaited_once_with(directory, runtime.ctx, runtime.task)


@pytest.mark.asyncio
async def test_acquires_then_prepares_genuine_rooted_handoff(runtime):
    result = await worker.process_rooted_graph_task(runtime.directory, runtime.ctx, runtime.task)
    assert result["rooted_graph"]["state"] == "prepared"
    assert result["source_candidate_handoffs"][runtime.readiness.dataset_id] == {
        "document": runtime.handoff.document,
        "sha256": runtime.handoff.document_sha256,
    }
    assert runtime.ctx["context"]["audit"] == result
    assert runtime.scope == ["enter", "exit"]
    assert (
        runtime.acquire.await_args.kwargs["expected_publication_acquisition_id"]
        == operation()["publication_acquisition_id"]
    )
    assert runtime.acquire.await_args.kwargs["controls"].concurrency == 8
    runtime.publish.assert_awaited_once_with(operation()["publication_acquisition_id"], database=runtime.directory.db)


@pytest.mark.asyncio
async def test_admitted_retry_reuses_selector_without_http_or_current_root_selection(
    runtime,
):
    runtime.directory.db.first.return_value = {
        "acquisition_operation_key": operation()["operation_key"],
        **{name: operation()[name] for name in worker._ROOT_FIELDS},
    }
    await worker.process_rooted_graph_task(runtime.directory, runtime.ctx, runtime.task)
    runtime.acquire.assert_not_awaited()
    operator._select_exact_current_root.assert_not_awaited()
    runtime.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_publication_failure_preserves_admission_for_exact_retry(runtime):
    runtime.directory.db.first.return_value = {
        "acquisition_operation_key": operation()["operation_key"],
        **{name: operation()[name] for name in worker._ROOT_FIELDS},
    }
    runtime.publish.side_effect = RuntimeError("synthetic publication failure")
    with pytest.raises(RuntimeError):
        await worker.process_rooted_graph_task(runtime.directory, runtime.ctx, runtime.task)
    runtime.acquire.assert_not_awaited()
    assert runtime.ctx == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["root", "admission", "acquisition", "publication", "run", "cancel"])
async def test_changed_evidence_and_cancellation_fail_closed(runtime, failure):
    if failure == "root":
        runtime.root.dataset_hash = "0" * 64
    if failure == "admission":
        runtime.directory.db.first.return_value = {"acquisition_operation_key": "0" * 64}
    if failure == "acquisition":
        runtime.acquire.return_value = json.dumps({"publication_acquisition_id": "pdrga_" + "0" * 48})
    if failure == "publication":
        document = runtime.handoff.document
        document["identity"]["publication_acquisition_id"] = "pdrga_" + "0" * 48
        runtime.publish.return_value = FHIRSourceCandidateHandoff(json.dumps(document))
    if failure == "run":
        runtime.ctx["control_run_id"] = "another-run"
    if failure == "cancel":
        runtime.acquire.side_effect = asyncio.CancelledError
    with pytest.raises((ValueError, asyncio.CancelledError)):
        await worker.process_rooted_graph_task(runtime.directory, runtime.ctx, runtime.task)
    if failure != "publication":
        runtime.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_operator_checks_expected_selector_before_fetch(monkeypatch):
    import process.provider_directory_rooted_graph_acquisition as acquisition

    monkeypatch.setattr(
        operator,
        "_select_exact_current_root",
        AsyncMock(return_value=current_root()),
    )
    fetch = AsyncMock()
    monkeypatch.setattr(acquisition, "acquire_rooted_graph_single_root", fetch)
    controls = operator._AcquisitionControls(4, 3, 120, 1.0, 60.0, 3600.0)
    with pytest.raises(operator.ProviderDirectoryRootedGraphOperatorError) as error:
        await operator._run_single_root_acquisition_phase("a" * 64, controls, object(), "pdrga_" + "0" * 48)
    assert error.value.code == "input_drift"
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_matching_pinned_selector_reaches_real_operator_acquisition(monkeypatch):
    from process import provider_directory_rooted_graph_acquisition as acquisition

    current = current_root()
    monkeypatch.setattr(operator, "_select_exact_current_root", AsyncMock(return_value=current))
    fetch = AsyncMock(side_effect=RuntimeError("synthetic acquisition boundary"))
    monkeypatch.setattr(acquisition, "acquire_rooted_graph_single_root", fetch)
    controls = operator._AcquisitionControls(4, 3, 120, 1.0, 60.0, 3600.0)
    database = object()
    with pytest.raises(RuntimeError, match="synthetic acquisition boundary"):
        await operator._run_single_root_acquisition_phase(
            operation()["operation_key"], controls, database, operation()["publication_acquisition_id"]
        )
    candidate = operator.build_rooted_graph_single_root_identity(
        current, operation_key=operation()["operation_key"]
    ).candidate
    fetch.assert_awaited_once_with(candidate, config=operator._acquisition_config(controls), database=database)


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [True, False])
async def test_phase_checkpoint_uses_existing_attempt_fence(monkeypatch, accepted):
    import importlib

    lifecycle = importlib.import_module("process.control_lifecycle")
    mark = AsyncMock(return_value=accepted)
    monkeypatch.setattr(lifecycle, "mark_control_run", mark)
    context_by_field = {}
    if accepted:
        await worker._record_phase(context_by_field, task()["run_id"], operation(), "publishing")
    else:
        with pytest.raises(ValueError, match="attempt_changed"):
            await worker._record_phase(context_by_field, task()["run_id"], operation(), "publishing")
    assert mark.await_args.kwargs["expected_state"] == (
        "provider-directory-fhir",
        "running",
    )
    assert "metrics" not in mark.await_args.kwargs
    assert context_by_field["context"]["audit"]["rooted_graph"]["state"] == "publishing"


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_publication_summary_requires_authenticated_current_readback(runtime, missing):
    from datetime import UTC, datetime

    from tests.provider_directory_rooted_graph_publication_test_support import readiness

    proof = readiness(retry_exhausted_count=1)
    runtime.directory.db.first.return_value = (
        None
        if missing
        else {
            name: getattr(proof, name)
            for name in (
                "dataset_id",
                "endpoint_id",
                "acquisition_root_run_id",
                "dataset_hash",
                "resource_count",
            )
        }
    )
    if not missing:
        runtime.directory.db.first.return_value.update(
            status="published",
            is_current=True,
            published_at=datetime(2026, 8, 10, tzinfo=UTC),
        )
    original = runtime.original_summary
    if missing:
        with pytest.raises(ValueError, match="readback_changed"):
            await original(runtime.directory, proof)
    else:
        summary = await original(runtime.directory, proof)
        assert summary["state"] == "partial" and summary["cohort_complete"] is False
        assert summary["endpoint_complete"] is False
        assert summary["publication_acquisition_id"] == proof.publication_acquisition_id


@pytest.mark.asyncio
async def test_partial_request_coverage_withholds_profile_followup(runtime):
    runtime.readiness.request_failure_coverage = {"resource_coverage": "unknown"}
    result = await worker._publication_result(
        runtime.directory, operation(), SimpleNamespace(readiness=runtime.readiness, replayed=False)
    )
    assert "profile_followup" not in result
    assert result["dataset_followup"]["dataset_id"] == runtime.readiness.dataset_id


@pytest.mark.asyncio
async def test_rooted_task_requires_v2_runtime_before_sql_or_http(runtime, monkeypatch):
    monkeypatch.setattr(worker, "current_rooted_graph_runtime", lambda _database: None)
    with pytest.raises(ValueError, match="v2_runtime_required"):
        await worker.process_rooted_graph_task(runtime.directory, runtime.ctx, runtime.task)
    runtime.directory.db.first.assert_not_awaited()
    runtime.acquire.assert_not_awaited()
    runtime.publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_runtime", [False, True])
async def test_native_transactions_require_configured_generation(monkeypatch, has_runtime):
    from process import provider_directory_rooted_graph_source_runtime as source_runtime
    from process.provider_directory_rooted_graph_store_support import rooted_graph_transaction

    opened_contexts = []

    @asynccontextmanager
    async def ordinary():
        opened_contexts.append("ordinary")
        yield "ordinary-session"

    @asynccontextmanager
    async def publisher(actual):
        assert actual is database
        opened_contexts.append("publisher")
        yield SimpleNamespace(session="publisher-session")

    database = SimpleNamespace(transaction=ordinary)
    monkeypatch.setattr(
        source_runtime,
        "current_rooted_graph_runtime",
        lambda database: SimpleNamespace(transaction=publisher) if has_runtime else None,
    )
    if has_runtime:
        async with rooted_graph_transaction(database) as session:
            assert session == "publisher-session"
    else:
        with pytest.raises(RuntimeError, match="configured isolated generation"):
            async with rooted_graph_transaction(database):
                pytest.fail("Unconfigured writes must not open a transaction")
    assert opened_contexts == (["publisher"] if has_runtime else [])


@pytest.mark.asyncio
async def test_pinned_operation_rejects_an_absent_selector():
    with pytest.raises(operator.ProviderDirectoryRootedGraphOperatorError) as error:
        await operator.acquire_pinned_root_operation(
            operation_key="a" * 64,
            expected_publication_acquisition_id=None,
            controls=operator._AcquisitionControls(4, 3, 120, 1.0, 60.0, 3600.0),
            database=object(),
        )
    assert error.value.code == "invalid_request"


@pytest.mark.parametrize("kind", ["ordinary", "rooted", "profile", "invalid-profile", "wrong-importer"])
def test_control_parameter_validation_preserves_profile_proofs(kind):
    from api.provider_directory_control_params import validate_provider_directory_params
    from process.provider_directory_profile_selection_contract import ProviderDirectoryProfileSelectionError

    params_by_name = task() if kind in {"rooted", "wrong-importer"} else {}
    if kind in {"profile", "invalid-profile"}:
        params_by_name["provider_directory_profile_contract_id"] = "synthetic-proof"
    validator = Mock()
    if kind == "invalid-profile":
        validator.side_effect = ProviderDirectoryProfileSelectionError("synthetic invalid proof")
    importer = "nucc" if kind == "wrong-importer" else "provider-directory-fhir"
    if kind in {"invalid-profile", "wrong-importer"}:
        with pytest.raises(ValueError):
            validate_provider_directory_params(importer, params_by_name, profile_validator=validator)
    else:
        validate_provider_directory_params(importer, params_by_name, profile_validator=validator)
    if kind in {"profile", "invalid-profile"}:
        validator.assert_called_once_with(params_by_name)
    else:
        validator.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "admitted_on_retry"),
    [("acquisition", False), ("acquisition", True), ("publication", True), ("validation", True)],
)
async def test_redis_cancellation_drains_owned_work_and_preserves_exact_retry(runtime, phase, admitted_on_retry):
    from process.control_cancel import ImportCancelledError

    started = asyncio.Event()
    drained = asyncio.Event()
    is_cancel_requested = False
    owned_tasks = []

    async def read_flag(key):
        assert key == "cancel:" + runtime.task["run_id"]
        return b"1" if is_cancel_requested else None

    runtime.ctx["redis"] = SimpleNamespace(get=AsyncMock(side_effect=read_flag))

    async def interrupted_work(*arguments, **keywords):
        owned_tasks.append(asyncio.current_task())
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            drained.set()

    interrupted_by_phase = {
        "acquisition": runtime.acquire,
        "publication": runtime.publish,
        "validation": runtime.complete,
    }
    interrupted = interrupted_by_phase[phase]
    interrupted.side_effect = interrupted_work
    pending = asyncio.create_task(worker.process_rooted_graph_task(runtime.directory, runtime.ctx, runtime.task))
    await asyncio.wait_for(started.wait(), timeout=2)
    is_cancel_requested = True
    with pytest.raises(ImportCancelledError):
        await asyncio.wait_for(pending, timeout=2)
    assert drained.is_set() and all(owned.done() for owned in owned_tasks)
    assert runtime.task["rooted_graph"] == operation()
    if phase == "acquisition":
        runtime.publish.assert_not_awaited()
    is_cancel_requested = False
    runtime.directory.db.first.return_value = (
        {
            "acquisition_operation_key": operation()["operation_key"],
            **{name: operation()[name] for name in worker._ROOT_FIELDS},
        }
        if admitted_on_retry
        else None
    )
    runtime.acquire.reset_mock()
    operator._select_exact_current_root.reset_mock()
    runtime.acquire.side_effect = None
    runtime.publish.side_effect = None
    runtime.complete.side_effect = lambda _ctx, _task, metrics: metrics
    metrics_by_key = await worker.process_rooted_graph_task(runtime.directory, runtime.ctx, runtime.task)
    assert metrics_by_key["rooted_graph"]["publication_acquisition_id"] == operation()["publication_acquisition_id"]
    _assert_retry_acquisition(runtime, admitted_on_retry)


def _assert_retry_acquisition(runtime, admitted_on_retry):
    if admitted_on_retry:
        runtime.acquire.assert_not_awaited()
        operator._select_exact_current_root.assert_not_awaited()
        return
    runtime.acquire.assert_awaited_once()
    arguments_by_name = runtime.acquire.await_args.kwargs
    assert arguments_by_name["operation_key"] == operation()["operation_key"]
    assert arguments_by_name["expected_publication_acquisition_id"] == operation()["publication_acquisition_id"]
    operator._select_exact_current_root.assert_awaited_once_with(runtime.directory.db)


@pytest.mark.asyncio
async def test_outer_cancellation_drains_acquisition_before_return(runtime):
    started = asyncio.Event()
    drained = asyncio.Event()

    async def blocked_acquisition(**fields):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            drained.set()

    runtime.acquire.side_effect = blocked_acquisition
    pending = asyncio.create_task(worker.process_rooted_graph_task(runtime.directory, runtime.ctx, runtime.task))
    await asyncio.wait_for(started.wait(), timeout=2)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert drained.is_set()
    runtime.publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "parent", "hash"])
async def test_catalog_descriptor_reads_only_exact_metadata(monkeypatch, changed):
    from dataclasses import asdict, replace

    from tests.test_provider_directory_dataset_scoped_publication_contract import _LegacyCurrentDatabase

    current = replace(_legacy_current(), root_cohort_id="pdufc_" + "c" * 48)
    database = _LegacyCurrentDatabase(current)
    original_all = database.all

    async def rows(statement, **parameters):
        selected = await original_all(statement, **parameters)
        if changed == "parent" and "provider_directory_endpoint_dataset" in statement:
            selected[0]["dataset_hash"] = "0" * 64
        return selected

    @asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(database, "all", rows)
    monkeypatch.setattr(database, "transaction", transaction, raising=False)
    monkeypatch.setattr(database, "scalar", AsyncMock(side_effect=AssertionError("content validator called")))
    if changed == "hash":
        database.current = SimpleNamespace(**{**asdict(current), "dataset_hash": "invalid"})
    descriptor = await worker.rooted_graph_acquisition_descriptor(database)
    if changed is None:
        assert descriptor["root_dataset_id"] == current.dataset_id
        assert descriptor["root_dataset_hash"] == current.dataset_hash
        assert descriptor["root_cohort_id"] == current.root_cohort_id
    else:
        assert descriptor is None
    database.scalar.assert_not_awaited()
    assert len(database.all_reads) == 2 and len(database.header_reads) == 2
    assert all("FOR UPDATE" not in sql and "FOR SHARE" not in sql for sql in database.all_reads + database.header_reads)
    assert database.status_writes == ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"]
