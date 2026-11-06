# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Collection images require persisted PTG source and exact single-job admission."""

from copy import deepcopy
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import cigna_collection_worker as collection_worker
from api import control_workers as workers
from process.ptg_parts.cigna_toc_collection import COLLECTION_PROOF_PARAM
from tests.test_cigna_toc_collection import _SIGNED_URL, _subject

GENERIC = "registry.example/ordinary@sha256:" + "a" * 64
COLLECTION = "registry.example/collection@sha256:" + "b" * 64


def _recorded(tmp_path, queue="arq:PTGNormal"):
    spec = workers._BY_QUEUE[queue]
    params_by_name = {
        COLLECTION_PROOF_PARAM: _subject(tmp_path).proof,
        "toc_urls": [_SIGNED_URL],
        "import_id": "source-attempt",
        "source_file_import_id": "source-attempt",
        "source_key": "synthetic-source",
        "import_month": "2026-11",
        "plan_ids": ["123456789"],
        "file_url_contains": ["synthetic-rate.json.gz"],
        "max_files": 1,
        "_expected_queue": queue,
        "_expected_worker_class": spec.worker_class,
    }
    run_by_field = {
        "run_id": "synthetic-run",
        "importer": "ptg",
        "status": "queued",
        "source_file_import_id": "source-attempt",
        "import_id": "source-attempt",
        "params": params_by_name,
        "metrics": {
            "queue": queue,
            "function": "ptg_control_start",
            "job_id": "ptg_start_synthetic-run",
            "worker_class": spec.worker_class,
        },
    }
    return spec, run_by_field


def _admitted(run):
    request = workers._AdmittedWorkerRequest(
        importer="ptg",
        run_id=run["run_id"],
        job_id=run["metrics"]["job_id"],
        queue=run["metrics"]["queue"],
        source_file_import_id="source-attempt",
        import_id="source-attempt",
    )
    workers._bind_admitted_job(request, run)
    assert collection_worker.admit_collection_worker(run, request) is None
    return request


@pytest.mark.parametrize("queue", ["arq:PTG", "arq:PTGSmall", "arq:PTGNormal", "arq:PTGLarge", "arq:PTGHuge"])
def test_all_existing_size_workers_keep_canonical_target_and_internal_finish(tmp_path, monkeypatch, queue):
    spec, run = _recorded(tmp_path, queue)
    before = deepcopy(run)
    request = _admitted(run)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", GENERIC)
    monkeypatch.setenv(collection_worker.IMAGE_ENV, COLLECTION)
    assert workers._configured_worker_job_image(spec, request) == COLLECTION
    assert workers._single_job_worker_target(spec, request) == run["metrics"]["job_id"]
    assert workers._worker_command("python", spec)[-2:] == ["worker-once", spec.worker_class]
    assert collection_worker.authorized_collection_worker_job(spec, request).source_file_import_id == "source-attempt"
    assert "cigna_collection_job" not in request and "admitted_job" not in request
    assert run == before
    assert not any(item.role == "finish" and "ptg" in item.importers for item in workers._WORKERS)
    monkeypatch.setattr(workers, "_kubernetes_namespace", lambda: "synthetic-workers")
    monkeypatch.setattr(workers, "_worker_state", lambda *_args: {})
    manifest_posts = []

    def record_post(method, _path, body=None):
        assert method == "POST"
        manifest_posts.append(body)
        return {}

    monkeypatch.setattr(workers, "_kubernetes_request", record_post)
    outcome = workers._ensure_kubernetes_job(spec, request, {})
    assert outcome["status"] == "started" and len(manifest_posts) == 1
    container = manifest_posts[0]["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == COLLECTION
    assert {"name": "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", "value": run["metrics"]["job_id"]} in container["env"]
    assert collection_worker.IMAGE_ENV not in {entry["name"] for entry in container["env"]}


@pytest.mark.parametrize("value", [None, "", "  "])
def test_unconfigured_collection_image_preserves_generic_fallback(tmp_path, monkeypatch, value):
    spec, run = _recorded(tmp_path)
    request = _admitted(run)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", GENERIC)
    if value is None:
        monkeypatch.delenv(collection_worker.IMAGE_ENV, raising=False)
    else:
        monkeypatch.setenv(collection_worker.IMAGE_ENV, value)
    assert workers._configured_worker_job_image(spec, request) == GENERIC
    assert workers._single_job_worker_target(spec, request) == "ptg_start_synthetic-run"


@pytest.mark.parametrize("value", ["registry.example/collection:mutable", "registry.example/collection@sha256:bad"])
def test_invalid_configured_collection_image_refuses(tmp_path, monkeypatch, value):
    spec, run = _recorded(tmp_path)
    request = _admitted(run)
    monkeypatch.setenv(collection_worker.IMAGE_ENV, value)
    with pytest.raises(ValueError, match="image_unavailable"):
        workers._configured_worker_job_image(spec, request)


@pytest.mark.parametrize(
    "field,value",
    [
        ("job_id", "foreign-job"),
        ("queue", "arq:PTGSmall"),
        ("function", "control_single_job_start"),
        ("worker_class", "process.PTGSmall"),
    ],
)
def test_persisted_adapter_drift_cannot_capture_capability(tmp_path, field, value):
    _spec, run = _recorded(tmp_path)
    run["metrics"][field] = value
    request = workers._AdmittedWorkerRequest(importer="ptg", run_id=run["run_id"])
    workers._bind_admitted_job(request, run)
    result = collection_worker.admit_collection_worker(run, request)
    assert result["status"] == "failed" and request.cigna_collection_job is None


@pytest.mark.parametrize(
    "scope,field,replacement",
    [
        ("params", COLLECTION_PROOF_PARAM, None),
        ("run", "import_id", "foreign-source"),
        ("params", "source_file_import_id", None),
        ("params", "_expected_queue", "arq:PTGSmall"),
        ("params", "_expected_worker_class", "process.PTGSmall"),
        ("params", "file_url_contains", ["one", "two"]),
    ],
    ids=["proof", "source-alias", "source-missing", "queue", "worker", "file"],
)
def test_invalid_stored_collection_scope_cannot_capture_capability(tmp_path, scope, field, replacement):
    _spec, run = _recorded(tmp_path)
    stored_by_field = run["params"] if scope == "params" else run
    if replacement is None:
        stored_by_field.pop(field)
    else:
        stored_by_field[field] = replacement
    request = workers._AdmittedWorkerRequest(importer="ptg", run_id=run["run_id"])
    workers._bind_admitted_job(request, run)
    result = collection_worker.admit_collection_worker(run, request)
    assert result["status"] == "failed" and request.cigna_collection_job is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("job_id", "foreign-job"),
        ("queue", "arq:PTGSmall"),
        ("run_id", "foreign-run"),
        ("source_file_import_id", "foreign-source"),
        ("source_key", "foreign-source"),
        ("worker_class", "process.PTGCandidateAudit"),
        ("role", "finish"),
        ("image", COLLECTION),
    ],
)
def test_caller_drift_refuses_image_and_canonical_job_target(tmp_path, monkeypatch, field, value):
    spec, run = _recorded(tmp_path)
    request = _admitted(run)
    request[field] = value
    monkeypatch.setenv(collection_worker.IMAGE_ENV, COLLECTION)
    with pytest.raises(ValueError):
        workers._configured_worker_job_image(spec, request)
    with pytest.raises(ValueError):
        workers._single_job_worker_target(spec, request)


def test_caller_proof_mismatch_and_wrong_spec_refuse(tmp_path, monkeypatch):
    spec, run = _recorded(tmp_path)
    request = _admitted(run)
    proof = deepcopy(run["params"][COLLECTION_PROOF_PARAM])
    proof["collection"]["archive_sha256"] = "c" * 64
    request[COLLECTION_PROOF_PARAM] = proof
    monkeypatch.setenv(collection_worker.IMAGE_ENV, COLLECTION)
    with pytest.raises(ValueError, match="proof_changed"):
        workers._configured_worker_job_image(spec, request)
    request.pop(COLLECTION_PROOF_PARAM)
    with pytest.raises(ValueError, match="selection_changed"):
        workers._configured_worker_job_image(workers._BY_QUEUE["arq:PTGCandidateAudit"], request)
    request.cigna_collection_job = replace(request.cigna_collection_job, identity=("other", *request.admitted_job[1:]))
    with pytest.raises(ValueError, match="selection_changed"):
        workers._single_job_worker_target(spec, request)


def test_serialized_fields_and_caller_proof_never_supply_admission(tmp_path, monkeypatch):
    spec, run = _recorded(tmp_path)
    admitted = _admitted(run)
    forged_request_by_field = {
        **admitted,
        "cigna_collection_job": admitted.cigna_collection_job,
        "admitted_job": admitted.admitted_job,
        COLLECTION_PROOF_PARAM: run["params"][COLLECTION_PROOF_PARAM],
        "image": COLLECTION,
    }
    monkeypatch.setenv(collection_worker.IMAGE_ENV, COLLECTION)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", GENERIC)
    assert workers._configured_worker_job_image(spec, forged_request_by_field) == GENERIC
    copied = workers._AdmittedWorkerRequest(forged_request_by_field)
    assert copied.cigna_collection_job is None
    assert workers._configured_worker_job_image(spec, copied) == GENERIC


def test_serialized_request_cannot_enter_collection_admission(tmp_path):
    _spec, run = _recorded(tmp_path)
    request = _admitted(run)
    with pytest.raises(ValueError, match="collection_worker_admission_required"):
        collection_worker._admitted_job(run, dict(request))


def test_consistent_foreign_source_aliases_cannot_retarget_admitted_job(tmp_path, monkeypatch):
    spec, run = _recorded(tmp_path)
    request = _admitted(run)
    before = deepcopy(run)
    request.update(source_file_import_id="foreign-source", import_id="foreign-source")
    monkeypatch.setenv(collection_worker.IMAGE_ENV, COLLECTION)
    with pytest.raises(ValueError, match="collection_worker_source_changed"):
        workers._configured_worker_job_image(spec, request)
    with pytest.raises(ValueError, match="collection_worker_source_changed"):
        workers._single_job_worker_target(spec, request)
    assert run == before


@pytest.mark.parametrize(
    "importer", ["ptg", "ptg-candidate-audit", "plan-pricing-projection", "plan-pricing-prewarm", "mrf"]
)
def test_other_jobs_preserve_generic_fallback_even_with_invalid_collection_setting(monkeypatch, importer):
    monkeypatch.setenv(collection_worker.IMAGE_ENV, "invalid")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", GENERIC)
    spec = workers._BY_IMPORTER_ROLE[(importer, "start")]
    request = workers._AdmittedWorkerRequest(importer=importer, run_id="ordinary-run")
    assert collection_worker.admit_collection_worker({"importer": importer, "params": {}}, request) is None
    assert workers._configured_worker_job_image(spec, request) == GENERIC


@pytest.mark.asyncio
async def test_capability_is_captured_only_after_existing_persisted_source_admission(tmp_path, monkeypatch):
    spec, run = _recorded(tmp_path)
    admit = AsyncMock(return_value=run)
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", admit)
    request = workers._AdmittedWorkerRequest(
        importer="ptg",
        run_id=run["run_id"],
        queue=spec.queue,
        source_file_import_id="source-attempt",
        import_id="source-attempt",
    )
    assert (
        await workers._admit_worker_ensure(request, run_id=run["run_id"], importer="ptg", selected_specs=[spec]) is None
    )
    assert admit.await_args.kwargs["expected_source_file_import_id"] == "source-attempt"
    assert request.cigna_collection_job.identity == request.admitted_job
    run["status"] = "succeeded"
    denied = workers._AdmittedWorkerRequest(importer="ptg", run_id=run["run_id"])
    result = await workers._admit_worker_ensure(denied, run_id=run["run_id"], importer="ptg", selected_specs=[spec])
    assert result["status"] == "failed" and denied.cigna_collection_job is None


def _tamper_existing_job(job_by_field, damage):
    container = job_by_field["spec"]["template"]["spec"]["containers"][0]
    if damage == "image":
        container["image"] = GENERIC
    if damage == "command":
        container["command"][-1] = "process.PTGCandidateAudit"
    if damage in {"target", "run", "source", "duplicate-target"}:
        name = {
            "target": "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID",
            "duplicate-target": "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID",
            "run": "HLTHPRT_CONTROL_RUN_ID",
            "source": "HLTHPRT_IMPORT_ID_OVERRIDE",
        }[damage]
        environment_entry = next(entry for entry in container["env"] if entry["name"] == name)
        if damage == "duplicate-target":
            container["env"].append(dict(environment_entry))
        else:
            environment_entry["value"] = "foreign"
    if damage == "sidecar":
        job_by_field["spec"]["template"]["spec"]["containers"].append(dict(container))


@pytest.mark.parametrize("damage", [None, "image", "command", "target", "run", "source", "duplicate-target", "sidecar"])
def test_existing_scoped_job_requires_exact_runtime_source_and_target(tmp_path, monkeypatch, damage):
    spec, run = _recorded(tmp_path)
    request = _admitted(run)
    monkeypatch.setenv(collection_worker.IMAGE_ENV, COLLECTION)
    job = workers._worker_job_manifest(spec, request, COLLECTION)
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == COLLECTION
    assert container["command"] == workers._worker_command(workers._worker_python(), spec)
    target_environment = next(
        entry for entry in container["env"] if entry["name"] == "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"
    )
    assert target_environment == {"name": "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", "value": "ptg_start_synthetic-run"}
    _tamper_existing_job(job, damage)
    job["status"] = {"active": 1}
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_kubernetes_namespace", lambda: "synthetic-workers")
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *_args: {"items": [job]})
    state = workers._kubernetes_worker_state(spec, request)
    assert state["job_status"] == ("active" if damage is None else "purpose_mismatch")
    assert state["running"] is (damage is None)


def test_existing_generic_job_matching_is_unchanged_when_selector_is_unset(tmp_path, monkeypatch):
    spec, run = _recorded(tmp_path)
    request = _admitted(run)
    monkeypatch.delenv(collection_worker.IMAGE_ENV, raising=False)
    assert collection_worker.collection_job_set_matches(spec, request, [{"unrelated": "legacy-shape"}]) is True


def test_equivalent_caller_proof_order_preserves_admitted_selection(tmp_path, monkeypatch):
    spec, run = _recorded(tmp_path)
    request = _admitted(run)
    proof = run["params"][COLLECTION_PROOF_PARAM]
    request[COLLECTION_PROOF_PARAM] = dict(reversed(list(proof.items())))
    monkeypatch.setenv(collection_worker.IMAGE_ENV, COLLECTION)
    assert workers._configured_worker_job_image(spec, request) == COLLECTION
