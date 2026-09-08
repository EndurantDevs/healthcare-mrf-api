# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exercise orchestration with retained public bytes and synthetic I/O boundaries."""

import asyncio
import copy
import hashlib
import importlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.control_cancel import ImportCancelledError

worker = importlib.import_module("process.massachusetts_profile")
PREDECESSOR = "a" * 64


def _cohort(licenses=("123", "456", "789")):
    return worker.acquisition.build_cohort([
        {"license_number": license_number, "npi": 1000000004,
         "first_name": "Alex", "last_name": "Example", "taxonomy": "207Q00000X"}
        for license_number in licenses
    ], {"mrf.npi": 101, "mrf.npi_taxonomy": 102, "mrf.nucc_taxonomy": 103})


def _response(license_number, *, profile=None, empty=False):
    profile_by_field = profile if profile is not None else {
        "licenseNumber": license_number, "licenseMetaId": 1,
        "firstName": "Alex", "lastName": "Example", "npiNumber": "1000000004",
        "educationAndTrainings": {"education": {"name": "École Synthetic Medical School", "graduationDate": "2001"}},
    }
    body = b"" if empty else worker.acquisition.encoded_json(profile_by_field)
    return {
        "schema_version": worker.acquisition.RESPONSE_SCHEMA,
        "license_number": license_number, "source_url": worker.acquisition.API_BASE + license_number,
        "downloaded_at": "2026-09-08T12:00:00+00:00", "status": 200,
        "content_type": "application/json", "body_text": body.decode(),
        "content_sha256": hashlib.sha256(body).hexdigest(),
    }


class ImportHarness:
    def __init__(self, monkeypatch, tmp_path):
        self.artifact_root = tmp_path / "artifacts"
        self.cohort = _cohort()
        self.responses_by_license = {root["license_number"]: _response(root["license_number"]) for root in self.cohort["roots"]}
        self.writes = []
        self.requests = []
        self.redis = SimpleNamespace(get=AsyncMock(return_value=None))
        self.ctx = {"redis": self.redis}
        self.task = {"run_id": "synthetic-control-run"}
        self.fail_model = None
        self.cancel_model = None
        self.store_by_name = {}
        for name in ("ensure_tables", "claim_run", "update_run", "mark_run_failed", "retain_source_history", "publish_run", "finish_unpublished_run"):
            self.store_by_name[name] = AsyncMock()
            monkeypatch.setattr(worker.store, name, self.store_by_name[name])
        monkeypatch.setattr(worker.store, "read_publication", AsyncMock(return_value={"current_run_id": PREDECESSOR}))
        monkeypatch.setattr(worker.acquisition, "capture_registry_cohort", AsyncMock(return_value=self.cohort))
        monkeypatch.setattr(worker.acquisition, "fetch_profile", self.fetch)
        monkeypatch.setattr(worker.acquisition.aiohttp, "ClientSession", self.session)
        monkeypatch.setattr(worker.acquisition, "REQUEST_INTERVAL_SECONDS", 0)
        monkeypatch.setattr(worker, "db", SimpleNamespace(transaction=self.transaction))
        monkeypatch.setattr(worker, "_upsert_rows", self.upsert)
        monkeypatch.setattr(worker, "enqueue_live_progress", Mock())
        self.finish = AsyncMock(side_effect=lambda _ctx, _task, run, metrics: {"run_id": run["run_id"], **metrics})
        monkeypatch.setattr(worker, "_finish_run", self.finish)
        monkeypatch.setenv("HLTHPRT_MA_BORIM_ARTIFACT_ROOT", str(self.artifact_root))

    @asynccontextmanager
    async def session(self, **_options):
        yield object()

    @asynccontextmanager
    async def transaction(self):
        checkpoint = len(self.writes)
        try:
            yield
        except BaseException:
            del self.writes[checkpoint:]
            raise

    async def fetch(self, _session, license_number):
        self.requests.append(license_number)
        response = self.responses_by_license[license_number]
        if isinstance(response, BaseException):
            raise response
        return copy.deepcopy(response)

    async def upsert(self, model, rows, key):
        if model is self.fail_model:
            raise RuntimeError("synthetic persistence failure")
        self.writes.append((model, copy.deepcopy(rows), key))
        if model is self.cancel_model:
            self.redis.get.return_value = b"1"

    def rows_for(self, model):
        return [source_row for written_model, source_rows, _key in self.writes if written_model is model for source_row in source_rows]


@pytest.fixture
def harness(monkeypatch, tmp_path):
    return ImportHarness(monkeypatch, tmp_path)


@pytest.mark.parametrize("limit", [None, 2, 10])
async def test_selected_scope_is_frozen_before_claim_and_retained(harness, limit):
    await worker.process_data(harness.ctx, {**harness.task, "max_providers": limit})
    claimed_run = harness.store_by_name["claim_run"].call_args.args[0]
    manifest = claimed_run["source_manifest"]
    expected = 3 if limit is None else min(limit, 3)
    assert manifest["full_cohort_licenses"] == 3
    assert manifest["requested_licenses"] == expected
    assert manifest["max_providers"] == limit
    assert manifest["expected_current_run_id"] == PREDECESSOR
    assert manifest["cohort_sha256"] == worker._hash(harness.cohort)
    assert manifest["source"]["registry_generation"] == harness.cohort["registry_generation"]
    assert len(harness.requests) == len(set(harness.requests)) == expected
    assert len(harness.rows_for(worker.ProviderProfileSourceRecord)) == expected
    assert len(harness.rows_for(worker.ProviderProfileFact)) == expected
    assert worker.acquisition.read_cohort(harness.artifact_root / claimed_run["run_id"] / "cohort.json") == harness.cohort
    harness.finish.assert_awaited_once()
    assert harness.finish.call_args.args[3]["responses"] == expected
    harness.store_by_name["mark_run_failed"].assert_not_called()


def test_bounded_selection_is_order_independent_and_preserves_roots():
    cohort = _cohort()
    original = copy.deepcopy(cohort)
    selected = worker._selected_roots(cohort, 2)
    shuffled_by_field = {**cohort, "roots": list(reversed(cohort["roots"]))}
    assert worker._selected_roots(shuffled_by_field, 2) == selected
    assert len(selected) == 2
    assert cohort == original
    assert worker._selected_roots(cohort, None) == cohort["roots"]


async def test_artifact_accounts_for_empty_response_without_inventing_facts(harness):
    harness.responses_by_license["456"] = _response("456", empty=True)
    await worker.process_data(harness.ctx, harness.task)
    artifact = harness.rows_for(worker.ProviderProfileArtifact)[0]
    path = harness.artifact_root / artifact["run_id"] / artifact["file_name"]
    assert artifact["content_bytes"] == path.stat().st_size
    assert artifact["content_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    metrics = json.loads(path.read_text())["acquisition"]
    expected_hash = hashlib.sha256()
    for license_number in harness.requests:
        response = harness.responses_by_license[license_number]
        expected_hash.update(worker.acquisition.encoded_json([license_number, response["content_sha256"], response["downloaded_at"]]))
    assert metrics["responses_sha256"] == expected_hash.hexdigest()
    assert metrics["response_bytes"] == sum(len(response["body_text"].encode()) for response in harness.responses_by_license.values())
    assert metrics["responses"] == 3 and metrics["reused_responses"] == 0
    assert metrics["acquisition_complete"] is True and metrics["transport_failures"] == 0
    missing = [source_row for source_row in harness.rows_for(worker.ProviderProfileSourceRecord) if source_row["license_number"] == "456"][0]
    assert missing["match_status"] == "not_found" and missing["matched_npi"] is None
    assert missing["raw_payload"] == {}
    assert len(harness.rows_for(worker.ProviderProfileFact)) == 2


@pytest.mark.parametrize("failure", [ValueError("synthetic transport failure"), asyncio.CancelledError("synthetic transport failure")])
async def test_acquisition_failure_keeps_checkpoint_without_finishing(harness, failure):
    harness.responses_by_license["456"] = failure
    with pytest.raises(type(failure), match="synthetic transport failure"):
        await worker.process_data(harness.ctx, harness.task)
    run_id = harness.store_by_name["claim_run"].call_args.args[0]["run_id"]
    assert harness.requests == ["123", "456"]
    assert worker.acquisition.read_response(harness.artifact_root / run_id / "profiles" / "123.json", "123") == harness.responses_by_license["123"]
    assert not (harness.artifact_root / run_id / "manifest.json").exists()
    assert harness.writes == []
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()
    assert harness.store_by_name["mark_run_failed"].call_args.args[0] == run_id
    harness.store_by_name["publish_run"].assert_not_called()
    harness.store_by_name["retain_source_history"].assert_not_called()


async def test_persistence_failure_rolls_back_batch_and_preserves_pointer(harness):
    harness.fail_model = worker.ProviderProfileFact
    with pytest.raises(RuntimeError, match="synthetic persistence failure"):
        await worker.process_data(harness.ctx, harness.task)
    assert len(harness.rows_for(worker.ProviderProfileArtifact)) == 1
    assert harness.rows_for(worker.ProviderProfileSourceRecord) == []
    assert harness.rows_for(worker.ProviderProfileFact) == []
    harness.finish.assert_not_called()
    harness.store_by_name["publish_run"].assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


async def test_error_response_fails_before_any_profile_batch_is_stored(harness):
    harness.responses_by_license["456"] = _response("456", profile={"error": "upstream unavailable"})
    with pytest.raises(ValueError, match="identity_schema_invalid"):
        await worker.process_data(harness.ctx, harness.task)
    assert harness.rows_for(worker.ProviderProfileSourceRecord) == []
    assert harness.rows_for(worker.ProviderProfileFact) == []
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


async def test_fresh_path_collision_never_overwrites_existing_artifacts(harness):
    run_id = worker._hash([worker.SOURCE_KEY, harness.task["run_id"]])
    existing = harness.artifact_root / run_id
    existing.mkdir(parents=True)
    retained = existing / "cohort.json"
    retained.write_text("existing evidence")
    with pytest.raises(FileExistsError):
        await worker.process_data(harness.ctx, harness.task)
    assert retained.read_text() == "existing evidence"
    assert harness.requests == [] and harness.writes == []
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


async def test_failed_claim_does_not_fail_someone_elses_run(harness):
    harness.store_by_name["claim_run"].side_effect = RuntimeError("synthetic occupied scope")
    with pytest.raises(RuntimeError, match="synthetic occupied scope"):
        await worker.process_data(harness.ctx, harness.task)
    assert not harness.artifact_root.exists()
    harness.store_by_name["mark_run_failed"].assert_not_called()


@pytest.mark.parametrize("task", [
    {"max_providers": True}, {"max_providers": 0}, {"max_providers": "2"},
    {"resume_from": "../previous"}, {"resume_from": 1},
])
async def test_invalid_task_is_rejected_before_side_effects(harness, task):
    with pytest.raises(ValueError, match="massachusetts_profile_"):
        await worker.process_data(harness.ctx, task)
    harness.store_by_name["ensure_tables"].assert_not_called()
    harness.store_by_name["claim_run"].assert_not_called()
    assert not harness.artifact_root.exists()


async def test_preflight_cancellation_claims_nothing(harness):
    harness.redis.get.return_value = b"1"
    with pytest.raises(ImportCancelledError):
        await worker.process_data(harness.ctx, harness.task)
    harness.store_by_name["ensure_tables"].assert_not_called()
    harness.store_by_name["claim_run"].assert_not_called()
    assert harness.requests == []


async def test_acquisition_cancellation_preserves_first_checkpoint(harness, monkeypatch):
    original_fetch = harness.fetch

    async def fetch_then_cancel(session, license_number):
        response = await original_fetch(session, license_number)
        harness.redis.get.return_value = b"1"
        return response

    monkeypatch.setattr(worker.acquisition, "fetch_profile", fetch_then_cancel)
    with pytest.raises(ImportCancelledError):
        await worker.process_data(harness.ctx, harness.task)
    run_id = harness.store_by_name["claim_run"].call_args.args[0]["run_id"]
    assert harness.requests == ["123"]
    assert (harness.artifact_root / run_id / "profiles" / "123.json").exists()
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


async def test_persistence_cancellation_stops_before_source_rows(harness):
    harness.cancel_model = worker.ProviderProfileArtifact
    with pytest.raises(ImportCancelledError):
        await worker.process_data(harness.ctx, harness.task)
    assert len(harness.requests) == 3
    assert len(harness.rows_for(worker.ProviderProfileArtifact)) == 1
    assert harness.rows_for(worker.ProviderProfileSourceRecord) == []
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


async def test_retention_failure_does_not_downgrade_completed_import(harness):
    harness.store_by_name["retain_source_history"].side_effect = RuntimeError("synthetic cleanup error")
    receipt = await worker.process_data(harness.ctx, harness.task)
    assert receipt["responses"] == 3
    harness.finish.assert_awaited_once()
    harness.store_by_name["mark_run_failed"].assert_not_called()


def _retained_run(harness, *, limit=2):
    run_id = "b" * 64
    directory = harness.artifact_root / run_id
    (directory / "profiles").mkdir(parents=True)
    worker.acquisition.write_new_json(directory / "cohort.json", harness.cohort)
    selected = worker._selected_roots(harness.cohort, limit)
    for root in selected:
        worker.acquisition.write_new_json(directory / "profiles" / f"{root['license_number']}.json", harness.responses_by_license[root["license_number"]])
    manifest = worker._source_manifest({"max_providers": limit}, harness.cohort, PREDECESSOR)
    return {"run_id": run_id, "source_manifest": manifest}, directory


async def test_resume_uses_frozen_cohort_and_exact_bytes_in_new_directory(harness, monkeypatch):
    previous_run, previous_directory = _retained_run(harness)
    bytes_by_name = {path.name: path.read_bytes() for path in (previous_directory / "profiles").iterdir()}
    monkeypatch.setattr(worker.store, "read_resume_run", AsyncMock(return_value=previous_run))
    worker.acquisition.capture_registry_cohort.side_effect = AssertionError("Resume must not read the current registry")
    await worker.process_data(harness.ctx, {**harness.task, "max_providers": 2, "resume_from": previous_run["run_id"]})
    run_row = harness.store_by_name["claim_run"].call_args.args[0]
    new_directory = harness.artifact_root / run_row["run_id"]
    assert new_directory != previous_directory and harness.requests == []
    assert {path.name: path.read_bytes() for path in (new_directory / "profiles").iterdir()} == bytes_by_name
    assert {path.name: path.read_bytes() for path in (previous_directory / "profiles").iterdir()} == bytes_by_name
    assert run_row["source_manifest"]["cohort_sha256"] == previous_run["source_manifest"]["cohort_sha256"]
    assert run_row["source_manifest"]["expected_current_run_id"] == PREDECESSOR
    assert harness.finish.call_args.args[3]["reused_responses"] == 2
    artifact = harness.rows_for(worker.ProviderProfileArtifact)[0]
    for root in worker._selected_roots(harness.cohort, 2):
        old_artifact_by_field = {**artifact, "run_id": previous_run["run_id"], "artifact_id": "c" * 64}
        _, old_facts = worker._source_rows(root, harness.responses_by_license[root["license_number"]], old_artifact_by_field, 1)
        _, new_facts = worker._source_rows(root, harness.responses_by_license[root["license_number"]], artifact, 1)
        assert old_facts[0]["logical_fact_key"] == new_facts[0]["logical_fact_key"]
        assert old_facts[0]["fact_id"] != new_facts[0]["fact_id"]


async def test_changed_resume_cohort_is_refused_before_new_claim(harness, monkeypatch):
    previous_run, previous_directory = _retained_run(harness)
    changed = _cohort(("123", "456", "999"))
    (previous_directory / "cohort.json").write_bytes(worker.acquisition.encoded_json(changed))
    monkeypatch.setattr(worker.store, "read_resume_run", AsyncMock(return_value=previous_run))
    with pytest.raises(ValueError, match="resume_cohort_changed"):
        await worker.process_data(harness.ctx, {**harness.task, "max_providers": 2, "resume_from": previous_run["run_id"]})
    harness.store_by_name["claim_run"].assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_not_called()
    assert harness.requests == []


async def test_changed_retained_response_is_never_replaced_by_network(harness, monkeypatch):
    previous_run, previous_directory = _retained_run(harness)
    path = next((previous_directory / "profiles").iterdir())
    response = json.loads(path.read_text())
    response["body_text"] += "changed"
    path.write_bytes(worker.acquisition.encoded_json(response))
    monkeypatch.setattr(worker.store, "read_resume_run", AsyncMock(return_value=previous_run))
    with pytest.raises(ValueError, match="response_changed"):
        await worker.process_data(harness.ctx, {**harness.task, "max_providers": 2, "resume_from": previous_run["run_id"]})
    assert harness.requests == []
    assert json.loads(path.read_text()) == response
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


def test_registry_adapter_cli_and_worker_agree():
    from click.testing import CliRunner
    from api import control_imports, control_workers
    import process

    importer = "massachusetts-borim-profile"
    registration = next(entry for entry in control_imports.importer_registry() if entry["name"] == importer)
    assert registration["family"] == "provider" and registration["depends_on"] == ["npi"]
    assert registration["cancelable"] is True and registration["enqueue_adapter"] == "arq_single_job"
    assert {parameter["name"] for parameter in registration["params_schema"]} == {"max_providers", "resume_from"}
    adapter = control_imports._SINGLE_JOB_ADAPTERS[importer]
    payload = control_imports._adapter_payload(adapter, {
        "run_id": "synthetic-control", "importer": importer, "family": "provider",
    }, {"max_providers": 2, "resume_from": "b" * 64})
    assert payload["target_module"] == "process.massachusetts_profile"
    assert payload["target_function"] == "process_data" and payload["call_style"] == "ctx_task"
    assert payload["task"]["max_providers"] == 2 and payload["task"]["resume_from"] == "b" * 64
    spec = next(entry for entry in control_workers.worker_registry() if importer in entry["importers"] and entry["role"] == "start")
    assert spec["worker_class"] == "process.MassachusettsBORIMProfile"
    assert spec["queue"] == adapter["queue"] == registration["queue"] == process.MassachusettsBORIMProfile.queue_name
    assert process.MassachusettsBORIMProfile.max_jobs == 1
    assert process.MassachusettsBORIMProfile.functions[0].name == adapter["function"]
    assert process.process_group.commands[importer] is worker.massachusetts_borim_profile
    cli_result = CliRunner().invoke(process.process_group, [importer, "--help"])
    assert cli_result.exit_code == 0
    assert "--max-providers" in cli_result.output and "--resume-from" in cli_result.output


@pytest.mark.parametrize("limit", [None, 2])
async def test_finish_delegates_reviewed_completion_after_validating(monkeypatch, limit):
    ctx_by_field = {"context": {"_control_attempt_id": "synthetic-attempt"}}
    task_by_field = {"run_id": "synthetic-control"}
    run_by_field = {"run_id": "c" * 64, "source_manifest": {"max_providers": limit}}
    metrics_by_field = {"responses": 2, "acquisition_complete": True}
    update = AsyncMock()
    cancel = AsyncMock()
    completed_by_field = {"run_id": run_by_field["run_id"], "published": limit is None, "terminal_progress": {"pct": 100}}

    async def complete(ctx, task, run, metrics):
        cancel.assert_awaited_once_with(ctx_by_field, task_by_field)
        update.assert_awaited_once_with(run_by_field["run_id"], {"status": "validating", "metrics": metrics_by_field})
        assert (ctx, task, run, metrics) == (ctx_by_field, task_by_field, run_by_field, metrics_by_field)
        return completed_by_field

    monkeypatch.setattr(worker, "raise_if_cancelled", cancel)
    monkeypatch.setattr(worker.store, "update_run", update)
    monkeypatch.setattr(worker, "complete_run", complete)
    assert await worker._finish_run(ctx_by_field, task_by_field, run_by_field, metrics_by_field) is completed_by_field


async def test_cancelled_finish_does_not_enter_completion(monkeypatch):
    update = AsyncMock()
    complete = AsyncMock()
    monkeypatch.setattr(worker, "raise_if_cancelled", AsyncMock(side_effect=ImportCancelledError("synthetic cancel")))
    monkeypatch.setattr(worker.store, "update_run", update)
    monkeypatch.setattr(worker, "complete_run", complete)
    with pytest.raises(ImportCancelledError):
        await worker._finish_run({}, {}, {"run_id": "c" * 64}, {})
    update.assert_not_called()
    complete.assert_not_called()


@pytest.mark.parametrize("failure", [None, ValueError("synthetic CLI failure"), asyncio.CancelledError("synthetic CLI cancellation")])
async def test_direct_cli_import_closes_database_after_work(monkeypatch, failure):
    database = SimpleNamespace(connect=AsyncMock(), disconnect=AsyncMock())
    process_data = AsyncMock(return_value={"published": False}, side_effect=failure)
    monkeypatch.setattr(worker, "db", database)
    monkeypatch.setattr(worker, "process_data", process_data)
    if failure is None:
        assert await worker._direct_import(2, "b" * 64) == {"published": False}
    else:
        with pytest.raises(type(failure)):
            await worker._direct_import(2, "b" * 64)
    database.connect.assert_awaited_once()
    database.disconnect.assert_awaited_once()
    process_data.assert_awaited_once_with({}, {"max_providers": 2, "resume_from": "b" * 64})


@pytest.mark.parametrize(("arguments", "expected"), [([], (None, None)), (["--max-providers", "2", "--resume-from", "b" * 64], (2, "b" * 64))])
def test_cli_routes_parameters_and_prints_result(monkeypatch, arguments, expected):
    from click.testing import CliRunner

    direct_import = AsyncMock(return_value={"published": expected[0] is None, "responses": 2})
    monkeypatch.setattr(worker, "_direct_import", direct_import)
    cli_result = CliRunner().invoke(worker.massachusetts_borim_profile, arguments)
    assert cli_result.exit_code == 0
    assert json.loads(cli_result.output) == direct_import.return_value
    direct_import.assert_awaited_once_with(*expected)


@pytest.mark.parametrize("component", ["run", "profiles"])
def test_retained_resume_rejects_symlink_directories(tmp_path, component):
    actual = tmp_path / "actual"
    actual.mkdir()
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    run_id = "b" * 64
    run_directory = artifact_root / run_id
    if component == "run":
        run_directory.symlink_to(actual, target_is_directory=True)
    else:
        run_directory.mkdir()
        (run_directory / "profiles").symlink_to(actual, target_is_directory=True)
    with pytest.raises(ValueError, match="retained_directory_invalid"):
        worker._retained_directory(artifact_root, run_id)
    assert actual.is_dir() and list(actual.iterdir()) == []


def test_artifact_root_uses_pvc_default_and_explicit_override(monkeypatch, tmp_path):
    from pathlib import Path

    monkeypatch.delenv("HLTHPRT_MA_BORIM_ARTIFACT_ROOT", raising=False)
    assert worker._artifact_root() == Path("/work/massachusetts-borim")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HLTHPRT_MA_BORIM_ARTIFACT_ROOT", "relative-artifacts")
    assert worker._artifact_root() == tmp_path / "relative-artifacts"
    assert not (tmp_path / "relative-artifacts").exists()


@pytest.mark.parametrize("profiles", ["", '{"process.PTGHuge":{"limits":{"memory":"64Gi"}}}'])
def test_ma_resources_fall_back_without_changing_other_workers(monkeypatch, profiles):
    from api import control_workers

    monkeypatch.setenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", profiles)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_CPU_REQUEST", "2")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_MEMORY_REQUEST", "4Gi")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_CPU_LIMIT", "16")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_MEMORY_LIMIT", "64Gi")
    ma_spec = control_workers.WorkerSpec("arq:MassachusettsBORIMProfile", "process.MassachusettsBORIMProfile", ("massachusetts-borim-profile",))
    other_spec = control_workers.WorkerSpec("arq:CMSDoctors", "process.CMSDoctors", ("cms-doctors",))
    assert control_workers._worker_job_resources(ma_spec) == {
        "requests": {"cpu": "500m", "memory": "512Mi"}, "limits": {"cpu": "4", "memory": "4Gi"},
    }
    assert control_workers._worker_job_resources(other_spec) == {
        "requests": {"cpu": "2", "memory": "4Gi"}, "limits": {"cpu": "16", "memory": "64Gi"},
    }


@pytest.mark.parametrize("selector", ["process.MassachusettsBORIMProfile", "arq:MassachusettsBORIMProfile"])
def test_explicit_ma_resource_profile_precedes_default(monkeypatch, selector):
    from api import control_workers

    resources_by_kind = {"requests": {"cpu": "1", "memory": "1Gi"}, "limits": {"cpu": "2", "memory": "2Gi"}}
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", json.dumps({selector: resources_by_kind}))
    spec = control_workers.WorkerSpec("arq:MassachusettsBORIMProfile", "process.MassachusettsBORIMProfile", ("massachusetts-borim-profile",))
    assert control_workers._worker_job_resources(spec) == resources_by_kind
