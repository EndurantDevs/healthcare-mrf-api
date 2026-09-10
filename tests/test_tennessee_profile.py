# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exercise the managed paired-report import with synthetic I/O boundaries."""

import asyncio
import copy
import hashlib
import importlib
import json
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.control_cancel import ImportCancelledError
from tests.test_tennessee_profile_binding import _reports, _snapshot

worker = importlib.import_module("process.tennessee_profile")


class ImportHarness:
    def __init__(self, monkeypatch, tmp_path):
        self.directory = tmp_path / "retained"
        self.snapshot = _snapshot()
        self.reports = _reports()
        self.writes = []
        self.fail_model = None
        self.cancel_model = None
        self.fail_report = False
        self.redis = SimpleNamespace(get=AsyncMock(return_value=None))
        self.ctx = {"redis": self.redis}
        self.task = {"run_id": "synthetic-control"}
        self.capture = AsyncMock(return_value=self.snapshot)
        monkeypatch.setattr(worker, "capture_registry_snapshot", self.capture)
        monkeypatch.setattr(worker, "ensure_tables", AsyncMock())
        for name in ("claim_run", "update_run", "mark_run_failed", "retain_source_history"):
            monkeypatch.setattr(worker.store, name, AsyncMock())
        monkeypatch.setattr(worker.store, "read_publication", AsyncMock(return_value={"current_run_id": "a" * 64}))
        monkeypatch.setattr(worker.completion, "reconcile_failed_control_runs", AsyncMock())
        self.finish = AsyncMock(side_effect=lambda _ctx, _task, run, metrics: {"run_id": run["run_id"], **metrics})
        monkeypatch.setattr(worker.completion, "complete_run", self.finish)
        self.acquire = AsyncMock(side_effect=self.acquire_reports)
        monkeypatch.setattr(worker.acquisition, "acquire_reports", self.acquire)
        monkeypatch.setattr(worker, "_upsert_rows", self.upsert)
        monkeypatch.setattr(worker, "db", SimpleNamespace(transaction=self.transaction))
        monkeypatch.setattr(worker, "enqueue_live_progress", Mock())
        monkeypatch.setenv("HLTHPRT_TN_TDH_ARTIFACT_ROOT", str(self.directory))

    async def acquire_reports(self, directory, progress):
        worker.store.claim_run.assert_awaited_once()
        descriptors_by_profession = {}
        for label, _, profession, _ in worker.acquisition.REPORTS:
            content = self.reports[profession]["content"]
            path = directory / f"{label}-report.csv"
            path.write_bytes(content)
            if self.fail_report and profession == "1907":
                raise RuntimeError("synthetic second report failure")
            descriptors_by_profession[profession] = {
                "filepath": str(path),
                "content_bytes": len(content),
                "content_sha256": hashlib.sha256(content).hexdigest(),
                "source_url": worker.acquisition.REPORT_URL,
                "downloaded_at": "2026-09-09T00:00:00Z",
            }
        await progress(8, 8)
        return {
            "complete": True,
            "reports": descriptors_by_profession,
            "responses": [{"complete": True, "status": 200, "content_bytes": 100} for _ in range(8)],
        }

    @asynccontextmanager
    async def transaction(self):
        checkpoint = len(self.writes)
        try:
            yield
        except BaseException:
            del self.writes[checkpoint:]
            raise

    async def upsert(self, model, rows, key):
        if model is self.fail_model:
            raise RuntimeError("synthetic staging failure")
        self.writes.append((model, copy.deepcopy(rows), key))
        if model is self.cancel_model:
            self.redis.get.return_value = b"1"

    def rows_for(self, model):
        return [row for written_model, rows, _key in self.writes if model is written_model for row in rows]


@pytest.fixture
def harness(monkeypatch, tmp_path):
    return ImportHarness(monkeypatch, tmp_path)


async def test_complete_pair_has_one_bundle_and_shared_record_evidence(harness):
    async def claim(run):
        assert not harness.directory.exists()
        harness.capture.assert_awaited_once()
        harness.acquire.assert_not_awaited()
        assert run["source_manifest"]["snapshot_sha256"] == worker._hash(harness.snapshot)
        assert run["source_manifest"]["expected_current_run_id"] == "a" * 64

    worker.store.claim_run.side_effect = claim
    result = await worker.import_profiles(harness.ctx, harness.task)
    (artifact,) = harness.rows_for(worker.ProviderProfileArtifact)
    records = harness.rows_for(worker.ProviderProfileSourceRecord)
    facts = harness.rows_for(worker.ProviderProfileFact)
    assert len(records) == 2 and {record["matched_npi"] for record in records} == {1000000004, 1000000012}
    assert {record["artifact_id"] for record in records} == {artifact["artifact_id"]}
    assert facts and all(fact["published_at"] is None for fact in facts)
    assert {fact["source_json"]["artifact_id"] for fact in facts} == {artifact["artifact_id"]}
    path = harness.directory / artifact["run_id"] / artifact["file_name"]
    content = path.read_bytes()
    assert (
        len(content) == artifact["content_bytes"] and hashlib.sha256(content).hexdigest() == artifact["content_sha256"]
    )
    assert json.loads(content) == artifact["metadata_json"]
    assert result["report_responses"] == 2 and result["source_records_by_profession"] == {"1606": 1, "1907": 1}
    harness.finish.assert_awaited_once()
    worker.store.mark_run_failed.assert_not_awaited()
    worker.store.retain_source_history.assert_awaited_once()


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", None),
        ("run_id", " "),
        ("sources", []),
        ("professions", ["1606"]),
        ("max_providers", 1),
        ("resume_from", "a" * 64),
    ],
)
async def test_partial_or_unmanaged_scope_is_rejected_before_io(harness, field, value):
    with pytest.raises(ValueError):
        await worker.import_profiles(harness.ctx, {**harness.task, field: value})
    harness.capture.assert_not_awaited()
    worker.store.claim_run.assert_not_awaited()
    assert not harness.directory.exists()


async def test_capture_uses_registry_schema_independently_of_destination(harness, monkeypatch):
    for name in ("NPIData", "NPIDataTaxonomy", "NUCCTaxonomy"):
        monkeypatch.setattr(worker, name, SimpleNamespace(__table__=SimpleNamespace(schema="synthetic_registry")))
    await worker.import_profiles(harness.ctx, harness.task)
    assert harness.capture.call_args.args[0] == "synthetic_registry"


async def test_inconsistent_registry_schemas_fail_before_capture_or_claim(harness, monkeypatch):
    monkeypatch.setattr(worker, "NUCCTaxonomy", SimpleNamespace(__table__=SimpleNamespace(schema="foreign")))
    with pytest.raises(ValueError, match="registry_schema_mismatch"):
        await worker.import_profiles(harness.ctx, harness.task)
    harness.capture.assert_not_awaited()
    worker.store.claim_run.assert_not_awaited()


@pytest.mark.parametrize("failure", ["claim", "report", "artifact", "record", "fact", "cancel_record", "cancel_fact"])
async def test_failed_or_canceled_staging_never_completes_publication(harness, failure):
    if failure == "claim":
        worker.store.claim_run.side_effect = RuntimeError("synthetic occupied source")
    elif failure == "report":
        harness.fail_report = True
    elif failure.startswith("cancel_"):
        harness.cancel_model = (
            worker.ProviderProfileSourceRecord if failure == "cancel_record" else worker.ProviderProfileFact
        )
    else:
        harness.fail_model = {
            "artifact": worker.ProviderProfileArtifact,
            "record": worker.ProviderProfileSourceRecord,
            "fact": worker.ProviderProfileFact,
        }[failure]
    with pytest.raises(ImportCancelledError if failure.startswith("cancel_") else RuntimeError):
        await worker.import_profiles(harness.ctx, harness.task)
    harness.finish.assert_not_awaited()
    worker.store.retain_source_history.assert_not_awaited()
    if failure == "claim":
        assert not harness.directory.exists()
        harness.acquire.assert_not_awaited()
        worker.store.mark_run_failed.assert_not_awaited()
    else:
        worker.store.mark_run_failed.assert_awaited_once()
    if failure == "report":
        assert not harness.writes
        assert list(harness.directory.glob("*/md-report.csv"))


@pytest.mark.parametrize("parser_fails", [False, True])
async def test_parser_cancellation_drains_thread_without_blocking_event_loop(monkeypatch, tmp_path, parser_fails):
    started, release, finished = threading.Event(), threading.Event(), threading.Event()

    def parse(*_args, **_kwargs):
        started.set()
        try:
            assert release.wait(5)
            if parser_fails:
                raise ValueError("synthetic parser failure")
            return {}
        finally:
            finished.set()

    monkeypatch.setattr(worker.binding, "bind_reports", parse)
    task = asyncio.create_task(worker._bind_reports({}, "a" * 64, tmp_path, {"snapshot_sha256": "b" * 64}, "mrf"))
    try:
        async with asyncio.timeout(5):
            while not started.is_set():
                await asyncio.sleep(0.001)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


def test_registry_worker_and_committed_result_use_managed_entrypoint():
    from click.testing import CliRunner

    import process
    from api import control_imports, control_workers
    from process import control_lifecycle

    importer = worker.IMPORTER
    entry = next(entry for entry in control_imports.importer_registry() if entry["name"] == importer)
    assert entry["family"] == "provider" and entry["depends_on"] == ["npi"] and entry["cancelable"] is True
    assert entry["params_schema"] == []
    adapter = control_imports._SINGLE_JOB_ADAPTERS[importer]
    assert adapter["target_module"] == "process.tennessee_profile" and adapter["target_function"] == "import_profiles"
    spec = next(entry for entry in control_workers.worker_registry() if importer in entry["importers"])
    assert spec["queue"] == adapter["queue"] == process.TennesseeTDHProfile.queue_name
    assert process.TennesseeTDHProfile.max_jobs == process.TennesseeTDHProfile.functions[0].max_tries == 1
    cli = CliRunner().invoke(process.process_group, [importer])
    assert cli.exit_code == 2 and "managed import API" in cli.output
    committed_by_field = {"published": True, "retained_source_records": 2}
    assert (
        control_lifecycle._committed_target_result(
            {
                "context": {
                    "control_run_terminal_committed": True,
                    "_control_committed_result": committed_by_field,
                }
            },
            target_module="process.tennessee_profile",
        )
        is committed_by_field
    )


def test_tennessee_worker_has_report_capacity_and_respects_explicit_override(monkeypatch):
    from api import control_workers

    spec = control_workers.WorkerSpec("arq:TennesseeTDHProfile", "process.TennesseeTDHProfile", (worker.IMPORTER,))
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", raising=False)
    assert control_workers._worker_job_resources(spec) == {
        "requests": {"cpu": "1", "memory": "4Gi"},
        "limits": {"cpu": "4", "memory": "8Gi"},
    }
    resources_by_kind = {"requests": {"memory": "6Gi"}, "limits": {"memory": "12Gi"}}
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", json.dumps({spec.worker_class: resources_by_kind}))
    assert control_workers._worker_job_resources(spec) == resources_by_kind
