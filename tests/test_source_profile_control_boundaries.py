# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deferred completion and control refusals preserve durable publication custody."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.exc import IntegrityError

from api import control_imports, control_workers
from process import provider_profile_source_completion as completion
from process import source_profile_result_archive as archive
from tests.test_florida_mqa_profile import _install_complete_catalog_input, _install_complete_catalog_publication
from tests.test_florida_mqa_profile_import_guards import _ArtifactClient, _configure_import_runtime
from tests.test_florida_mqa_profile_publication_contracts import (
    _completion_metrics,
    _one_projection_row,
    _PublicationDb,
)
from tests.test_source_profile_result_archive import maintenance_run

florida = importlib.import_module("process.florida_mqa_profile")


@asynccontextmanager
async def transaction(session=None):
    yield session


@pytest.mark.parametrize("run_id", [None, " synthetic_run ", "synthetic_run"])
async def test_finish_startup_validates_exact_selection_before_database_start(monkeypatch, run_id):
    from process.ext import utils

    if run_id is None:
        monkeypatch.delenv("HLTHPRT_CONTROL_RUN_ID", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_CONTROL_RUN_ID", run_id)
    startup, finish = AsyncMock(), AsyncMock(return_value={"published": True})
    monkeypatch.setattr(utils, "db_startup", startup)
    monkeypatch.setattr(completion, "source_profile_finish", finish)
    context_dict = {}
    if run_id == "synthetic_run":
        assert await completion.source_profile_finish_startup(context_dict) == {"published": True}
        startup.assert_awaited_once_with(context_dict)
        finish.assert_awaited_once_with(context_dict, run_id)
    else:
        with pytest.raises(ValueError, match="finish run is required"):
            await completion.source_profile_finish_startup(context_dict)
        startup.assert_not_awaited()
        finish.assert_not_awaited()


@pytest.mark.parametrize("outcome", ["interrupted", "absent", "canceled_read"])
async def test_finish_uncertain_commit_preserves_readback_and_original_failure(monkeypatch, outcome):
    run = maintenance_run("massachusetts-borim-profile")
    finished_metrics_dict = {"published": True, "source_profile_maintenance": {"status": "completed"}}
    session = SimpleNamespace(execute=AsyncMock())
    failure = RuntimeError("synthetic uncertain commit")

    @asynccontextmanager
    async def uncertain_transaction():
        yield session
        raise failure

    database = SimpleNamespace(
        transaction=uncertain_transaction, first=AsyncMock(return_value=SimpleNamespace(_mapping=run))
    )
    monkeypatch.setattr(completion, "db", database)
    monkeypatch.setenv("HLTHPRT_CONTROL_RUN_ID", run["run_id"])
    monkeypatch.setattr(archive, "require_native_maintenance", AsyncMock(return_value={}))
    monkeypatch.setattr(completion, "_native_source_maintenance", AsyncMock(return_value={"status": "completed"}))
    monkeypatch.setattr(completion, "_commit_native_maintenance", AsyncMock(return_value=finished_metrics_dict))
    started, released = asyncio.Event(), asyncio.Event()

    async def readback(run_id, expected):
        assert run_id == run["run_id"] and expected is finished_metrics_dict
        started.set()
        await released.wait()
        if outcome == "canceled_read":
            raise asyncio.CancelledError
        return None if outcome == "absent" else expected

    monkeypatch.setattr(completion, "_read_native_maintenance_commit", readback)
    task = asyncio.create_task(completion.source_profile_finish({}, run["run_id"]))
    async with asyncio.timeout(2):
        await started.wait()
        if outcome == "interrupted":
            task.cancel()
        released.set()
        if outcome == "interrupted":
            assert await task is finished_metrics_dict
        else:
            with pytest.raises(asyncio.CancelledError if outcome == "canceled_read" else RuntimeError) as caught:
                await task
            if outcome == "absent":
                assert caught.value is failure


@pytest.mark.parametrize("fault", [None, "missing", "status", "finished", "metrics", "authority"])
async def test_maintenance_readback_requires_terminal_result_and_fresh_custody(monkeypatch, fault):
    expected_metrics_dict = {"published": True}
    run = maintenance_run("massachusetts-borim-profile")
    run.update(status="succeeded", finished_at=datetime(2026, 1, 2), metrics=expected_metrics_dict)
    if fault == "status":
        run["status"] = "finalizing"
    elif fault == "finished":
        run["finished_at"] = None
    elif fault == "metrics":
        run["metrics"] = {"published": False}
    row = None if fault == "missing" else SimpleNamespace(**run, _mapping=run)
    session = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(
        completion, "db", SimpleNamespace(transaction=lambda: transaction(session), first=AsyncMock(return_value=row))
    )
    authority = AsyncMock(side_effect=RuntimeError("synthetic custody refusal") if fault == "authority" else None)
    monkeypatch.setattr(archive, "require_native_maintenance", authority)
    assert await completion._read_native_maintenance_commit(run["run_id"], expected_metrics_dict) == (
        expected_metrics_dict if fault is None else None
    )
    assert authority.await_count == (fault in {None, "authority"})


@pytest.mark.parametrize(
    "error", [ValueError("synthetic selector mismatch"), RuntimeError("synthetic presence unavailable")]
)
async def test_worker_absence_proof_refuses_errors_without_claiming_absence(monkeypatch, error):
    monkeypatch.setattr(
        control_imports, "_require_stale_worker_identity", lambda *args: ("attempt", "started", datetime(2026, 1, 2))
    )
    monkeypatch.setattr(control_imports, "_reconciliation_worker_payload", lambda run: {"run_id": "synthetic_run"})
    monkeypatch.setattr(control_imports, "exact_worker_presence", Mock(side_effect=error))
    arq = AsyncMock()
    monkeypatch.setattr(control_imports, "_arq_worker_presence", arq)
    expected = (
        control_imports.StaleWorkerReconciliationConflict
        if isinstance(error, ValueError)
        else control_imports.StaleWorkerReconciliationUnavailable
    )
    with pytest.raises(expected) as caught:
        await control_imports._verified_absent_worker({}, {}, datetime(2026, 1, 2))
    assert caught.value.__cause__ is error
    arq.assert_not_awaited()


@pytest.mark.parametrize("state", ["missing", "terminal"])
async def test_publisher_owned_cancel_is_read_only_when_missing_or_terminal(monkeypatch, state):
    run = maintenance_run("massachusetts-borim-profile")
    run["status"] = "succeeded"
    row = None if state == "missing" else SimpleNamespace(_mapping=run)
    database = SimpleNamespace(
        transaction=lambda: transaction(object()), first=AsyncMock(return_value=row), execute=AsyncMock()
    )
    monkeypatch.setattr(control_imports, "db", database)
    completion_result = await control_imports._request_source_profile_cancel(run["run_id"])
    assert completion_result is None if state == "missing" else completion_result["status"] == "succeeded"
    database.execute.assert_not_awaited()


async def test_cancel_routes_only_authenticated_source_handoffs_to_publisher(monkeypatch):
    run = maintenance_run("massachusetts-borim-profile")
    monkeypatch.setattr(control_imports, "is_protected_places_publication_enabled", lambda: False)
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=run))
    publisher, worker = AsyncMock(return_value={"status": "canceling"}), AsyncMock()
    monkeypatch.setattr(control_imports, "_request_source_profile_cancel", publisher)
    monkeypatch.setattr(control_imports, "_request_worker_cancel", worker)
    assert await control_imports.request_cancel(run["run_id"]) == {"status": "canceling"}
    publisher.assert_awaited_once_with(run["run_id"])
    worker.assert_not_awaited()


@pytest.mark.parametrize("same_scope", [False, True])
async def test_admission_integrity_race_rechecks_hospital_request_before_replay(monkeypatch, same_scope):
    params_dict = {"all_hospitals": True}
    replay_run_dict = {
        "run_id": "synthetic_existing",
        "importer": "hospital-prices",
        "status": "running",
        "params": params_dict if same_scope else {"all_hospitals": False},
    }
    monkeypatch.setattr(control_imports, "_validate_hospital_price_params", AsyncMock(return_value=params_dict))
    monkeypatch.setattr(
        control_imports,
        "_admit_import_row",
        AsyncMock(side_effect=IntegrityError("insert", {}, RuntimeError("synthetic race"))),
    )
    monkeypatch.setattr(control_imports, "_idempotent_import_run", AsyncMock(return_value=replay_run_dict))
    enqueue = AsyncMock()
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    request_dict = {"importer": "hospital-prices", "params": params_dict, "idempotency_key": "synthetic_key"}
    if same_scope:
        completion_result, created = await control_imports.create_import_run(request_dict)
        assert created is False and completion_result["run_id"] == replay_run_dict["run_id"]
    else:
        with pytest.raises(ValueError, match="different import request"):
            await control_imports.create_import_run(request_dict)
    enqueue.assert_not_awaited()


@pytest.mark.parametrize("importer", tuple(archive.SOURCES))
def test_finish_worker_selector_refuses_other_classes_even_with_matching_queue(importer):
    spec = control_workers._BY_IMPORTER_ROLE[(importer, "finish")]
    assert (
        control_workers._resolve_specs(
            {"importer": importer, "status": "finalizing", "worker_class": "unknown_worker", "queue": spec.queue}
        )
        == []
    )
    assert control_workers._resolve_specs(
        {"importer": importer, "role": "finish", "worker_class": spec.worker_class}
    ) == [spec]


@pytest.mark.parametrize("managed", [False, True])
async def test_projection_staging_refuses_unmanaged_custody_or_hands_off_without_cutover(monkeypatch, managed):
    from process import florida_projection_archive as projection
    from process import reference_family_archive as native

    database = _PublicationDb(scalar_results=[1, 1], all_results=[])
    monkeypatch.setattr(florida, "db", database)
    monkeypatch.setattr(archive, "is_native_handoff_required", AsyncMock(return_value=True))
    monkeypatch.setattr(
        projection,
        "isolate_ordinary_projection",
        AsyncMock(
            return_value={"relation_oid": 301, "owner_oid": 41, "table_name": "provider_profile_projection_" + "a" * 16}
        ),
    )
    monkeypatch.setattr(native, "native_copy_record_batch", AsyncMock())
    indexes = AsyncMock()
    monkeypatch.setattr(native, "_create_model_indexes", indexes)
    monkeypatch.setattr(florida, "enqueue_live_progress", Mock())
    handoff = AsyncMock(return_value={"source_profile_handoff": {"receipt": "synthetic"}})
    monkeypatch.setattr(archive, "handoff_native_publication", handoff)
    context_dict = {"context": {"control_run_id": "synthetic_control"}} if managed else None
    arguments_dict = dict(
        started_at=datetime(2026, 1, 2, tzinfo=UTC),
        completion_metrics=_completion_metrics(1),
        allow_volume_drop=False,
        min_first_publish_providers=1,
        min_publish_ratio=0.8,
        control_context=context_dict,
    )
    if managed:
        publication, completion_result = await florida._publish_projection_swap(
            "a" * 32, _one_projection_row("a" * 32), **arguments_dict
        )
        assert publication == {"publication": "awaiting_protected_publisher", "published_rows": 0}
        assert completion_result == handoff.return_value and indexes.await_count == 1
        assert handoff.await_args.kwargs["projection"]["row_count"] == 1
        assert all("RENAME" not in query and "INHERIT" not in query for query in database.status_calls)
    else:
        with pytest.raises(RuntimeError, match="managed_publication_required"):
            await florida._publish_projection_swap("a" * 32, _one_projection_row("a" * 32), **arguments_dict)
        assert database.status_calls == []
        indexes.assert_not_awaited()
        handoff.assert_not_awaited()


@pytest.mark.parametrize("flag", ["control_run_handoff_committed", "source_profile_commit_unknown"])
async def test_import_failure_never_cleans_up_publisher_or_uncertain_custody(monkeypatch, tmp_path, flag):
    _configure_import_runtime(monkeypatch)
    failure = RuntimeError("synthetic acquisition failure")

    class FailedClient(_ArtifactClient):
        def authenticate(self):
            raise failure

    monkeypatch.setattr(florida, "FloridaMQAClient", FailedClient)
    cleanup, mark = AsyncMock(), AsyncMock()
    monkeypatch.setattr(florida.db, "status", cleanup)
    monkeypatch.setattr(florida, "_mark_failed_run_status", mark)
    with pytest.raises(RuntimeError) as caught:
        await florida.import_florida_mqa_profile(
            source_keys=["profile_master"],
            artifact_root=tmp_path,
            manage_db=False,
            control_context={"context": {flag: True}},
        )
    assert caught.value is failure
    cleanup.assert_not_awaited()
    mark.assert_not_awaited()


async def test_complete_import_returns_durable_handoff_before_worker_retention(monkeypatch, tmp_path):
    context_dict = {"context": {"control_run_id": "synthetic_control"}}

    async def publish(*args, **kwargs):
        assert kwargs["control_context"] is context_dict
        context_dict["context"]["control_run_handoff_committed"] = True
        return {"publication": "awaiting_protected_publisher"}, {"source_profile_handoff": {"receipt": "synthetic"}}

    _install_complete_catalog_input(monkeypatch)
    _install_complete_catalog_publication(monkeypatch, AsyncMock(side_effect=publish))
    retention = AsyncMock()
    monkeypatch.setattr(florida, "_apply_post_success_retention", retention)
    completion_result = await florida.import_florida_mqa_profile(
        artifact_root=tmp_path, manage_db=False, control_run_id="synthetic_control", control_context=context_dict
    )
    assert completion_result["control_run_id"] == "synthetic_control" and "source_profile_handoff" in completion_result
    retention.assert_not_awaited()


async def test_fl_worker_adapter_forwards_context_and_rejects_unknown_execution_options(monkeypatch, tmp_path):
    importer = AsyncMock(return_value={"published": True})
    original = florida.import_florida_mqa_profile
    monkeypatch.setattr(florida, "import_florida_mqa_profile", importer)
    context_dict = {"context": {"control_run_id": "synthetic_control"}}
    assert await florida.process_data(run_id="synthetic_control", _control_context=context_dict) == {"published": True}
    assert (
        importer.await_args.kwargs["control_context"] is context_dict
        and importer.await_args.kwargs["manage_db"] is False
    )
    with pytest.raises(TypeError, match="unsupported Florida execution option"):
        await original(untrusted_option=True)
    monkeypatch.setenv("HLTHPRT_FL_MQA_ARTIFACT_ROOT", str(tmp_path))
    assert florida._artifact_root() == tmp_path
