# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Importer-scoped credentials require durable single-job launch admission."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_workers as workers
from api import plan_pricing_worker_policy as pricing_workers


@pytest.mark.parametrize("role", ["start", "finish"])
@pytest.mark.parametrize("configured", [False, True])
def test_source_worker_policy_path_comes_only_from_deployment(monkeypatch, role, configured):
    setting = "HLTHPRT_SOURCE_PROFILE_ROLE_POLICY_FILE"
    if configured:
        monkeypatch.setenv(setting, "/synthetic-readonly/roles.json")
    else:
        monkeypatch.delenv(setting, raising=False)
    request_by_field = {"importer": "massachusetts-borim-profile", "role": role, setting: "/untrusted/roles.json"}
    spec = workers._resolve_specs(request_by_field)[0]
    environment, _run_id = workers._worker_job_environment(spec, request_by_field)
    policy_entries = [entry for entry in environment if entry["name"] == setting]
    assert policy_entries == ([{"name": setting, "value": "/synthetic-readonly/roles.json"}] if configured else [])


def test_process_finish_worker_inherits_independent_policy(monkeypatch, tmp_path):
    setting = "HLTHPRT_SOURCE_PROFILE_ROLE_POLICY_FILE"
    monkeypatch.setenv(setting, "/synthetic-readonly/roles.json")
    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))
    started_environments = []

    def start(_command, **options):
        started_environments.append(options["env"])
        return SimpleNamespace(pid=42)

    monkeypatch.setattr(workers.subprocess, "Popen", start)
    request_by_field = {
        "importer": "massachusetts-borim-profile",
        "status": "finalizing",
        "run_id": "synthetic_run",
        setting: "/untrusted/roles.json",
    }
    assert workers._start_process(workers._resolve_specs(request_by_field)[0], request_by_field) == 42
    assert started_environments[0][setting] == "/synthetic-readonly/roles.json"
    assert started_environments[0]["HLTHPRT_CONTROL_RUN_ID"] == "synthetic_run"


def _scope(monkeypatch, **selector):
    """Configure reference-only credentials for one importer of a shared class."""
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_ENV_JSON",
        json.dumps(
            [
                {"name": "ORDINARY_TOKEN", "secretName": "ordinary", "key": "token"},
                {
                    "name": "HLTHPRT_DB_USER",
                    "secretName": "audit-database",
                    "key": "username",
                    "workerClasses": ["process.PTGCandidateAudit"],
                    "importers": ["ptg-candidate-audit"],
                    **selector,
                },
            ]
        ),
    )


def _coordinates(importer="ptg-candidate-audit"):
    """Use the actual importer adapters, not a caller-selected queue/function."""
    from api.control_imports import _SINGLE_JOB_ADAPTERS

    adapter = _SINGLE_JOB_ADAPTERS[importer]
    run_id = "run_synthetic_audit"
    job_id = adapter["job_prefix"] + "_" + run_id
    request_dict = {"importer": importer, "run_id": run_id, "job_id": job_id}
    recorded_dict = {
        "importer": importer,
        "run_id": run_id,
        "status": "queued",
        "metrics": {"queue": adapter["queue"], "function": adapter["function"], "job_id": job_id},
    }
    return request_dict, recorded_dict


def _secrets(request):
    """Exercise the same environment and exact target builder as a real Job."""
    spec = workers._BY_WORKER_CLASS["process.PTGCandidateAudit"]
    environment, _run_id = workers._worker_job_environment(spec, request)
    return {entry["name"]: entry for entry in environment}


def _admitted(request, recorded):
    """Host-only construction isolates pure matching from real admission orchestration."""
    admitted = workers._AdmittedWorkerRequest(request)
    workers._bind_admitted_job(admitted, recorded)
    return admitted


def _drifted_request(request, recorded, drift):
    """Keep each request mutation independent of the recorded admission identity."""
    if drift in {"raw", "json_proof"}:
        candidate_request_dict = dict(request)
        if drift == "json_proof":
            candidate_request_dict["admitted_job"] = list(_admitted(request, recorded).admitted_job)
        return candidate_request_dict
    if drift == "matching_foreign_job":
        request["job_id"] = recorded["metrics"]["job_id"] = "plan_pricing_prewarm_run_other"
        return _admitted(request, recorded)
    candidate_request_dict = _admitted(request, recorded)
    if drift == "missing_target":
        candidate_request_dict.pop("job_id")
        return candidate_request_dict
    requested_field_by_drift = {
        "requested_job": "job_id",
        "requested_run": "run_id",
        "requested_importer": "importer",
    }
    if drift in requested_field_by_drift:
        candidate_request_dict[requested_field_by_drift[drift]] = "different"
    return candidate_request_dict


@pytest.mark.parametrize(
    "importer",
    (
        "ptg-candidate-audit",
        "plan-pricing-projection",
        "plan-pricing-prewarm",
        "plan-pricing-em-distance",
        "plan-pricing-em-attribution",
    ),
)
def test_shared_class_scopes_secrets_to_admitted_importer(monkeypatch, importer):
    """Sibling jobs retain ordinary credentials on the shared class."""
    _scope(monkeypatch)
    request, recorded = _coordinates(importer)
    environment = _secrets(_admitted(request, recorded))
    assert environment["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]["value"] == recorded["metrics"]["job_id"]
    assert "ORDINARY_TOKEN" in environment
    assert ("HLTHPRT_DB_USER" in environment) is (importer == "ptg-candidate-audit")
    if importer == "ptg-candidate-audit":
        assert environment["HLTHPRT_DB_USER"]["valueFrom"] == {
            "secretKeyRef": {"name": "audit-database", "key": "username"},
        }


@pytest.mark.parametrize("mode", ("audit_only", "audit_and_activate"))
def test_audit_mode_does_not_select_or_bypass_credentials(monkeypatch, mode):
    """Canonical audit modes keep their existing application checks and targets."""
    _scope(monkeypatch)
    request, recorded = _coordinates()
    request["candidate_audit_mode"] = mode
    assert "HLTHPRT_DB_USER" in _secrets(_admitted(request, recorded))


@pytest.mark.parametrize(
    "drift",
    (
        "raw",
        "json_proof",
        "missing_metrics",
        "run",
        "importer",
        "queue",
        "function",
        "job",
        "requested_job",
        "missing_target",
        "requested_run",
        "requested_importer",
        "matching_foreign_job",
    ),
)
def test_scoped_secret_rejects_unbound_job(monkeypatch, drift):
    """A request cannot transfer an admitted credential to another queued task."""
    _scope(monkeypatch)
    request, recorded = _coordinates()
    if drift == "missing_metrics":
        recorded.pop("metrics")
    elif drift in {"run", "importer"}:
        recorded[{"run": "run_id", "importer": "importer"}[drift]] = "different"
    elif drift in {"queue", "function", "job"}:
        recorded["metrics"][{"job": "job_id"}.get(drift, drift)] = "different"
    candidate = _drifted_request(request, recorded, drift)
    if drift == "importer":
        assert "HLTHPRT_DB_USER" not in _secrets(candidate)
    else:
        with pytest.raises(ValueError, match="exact admitted job"):
            _secrets(candidate)


@pytest.mark.parametrize("selected", (None, [], "ptg-candidate-audit", [""], [True], [" ptg-candidate-audit"]))
def test_malformed_importer_selection_refuses(monkeypatch, selected):
    """An invalid opt-in cannot accidentally become an unscoped secret."""
    _scope(monkeypatch, importers=selected)
    request, recorded = _coordinates()
    with pytest.raises(ValueError, match="selector is invalid"):
        _secrets(_admitted(request, recorded))


def test_scoped_secret_needs_fixed_class_and_no_wave_scope(monkeypatch):
    """Existing unscoped callers cannot construct an admitted importer identity."""
    _scope(monkeypatch)
    assert workers._worker_job_secret_env("process.PTGCandidateAudit") == [
        {"name": "ORDINARY_TOKEN", "valueFrom": {"secretKeyRef": {"name": "ordinary", "key": "token"}}},
    ]
    assert workers._worker_job_secret_env("process.PTGSmall") == workers._worker_job_secret_env()
    request, recorded = _coordinates()
    selector_dict = {"importers": ["ptg-candidate-audit"]}
    with pytest.raises(ValueError, match="selector is invalid"):
        workers._is_admitted_importer_selected(selector_dict, "process.PTGCandidateAudit", _admitted(request, recorded))


def _guarded_setup(monkeypatch, recorded, events, *, request_importer=None):
    """Keep real admission/launch code and replace only database and external I/O."""

    @asynccontextmanager
    async def acquire():
        events.append("enter")
        try:
            driver = SimpleNamespace(fetchrow=AsyncMock(return_value=deepcopy(recorded)))
            yield SimpleNamespace(raw_connection=SimpleNamespace(driver_connection=driver))
        finally:
            events.append("exit")

    async def admit(**arguments):
        expected_importer = recorded["importer"] if request_importer is None else request_importer
        assert arguments["worker_selection"].request_importer == (expected_importer or None)
        events.append("admitted")
        return deepcopy(recorded)

    monkeypatch.setattr(workers.db, "acquire", acquire)
    for name in (
        "acquire_ptg_admission_lock",
        "acquire_control_run_worker_action_lock",
        "require_not_wave_owned_run",
        "require_no_capacity_owning_wave",
    ):
        monkeypatch.setattr(workers, name, AsyncMock())
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", admit)
    monkeypatch.setattr(workers, "_launcher_mode", lambda: "kubernetes")
    monkeypatch.setattr(workers, "_kubernetes_namespace", lambda: "synthetic")
    monkeypatch.setattr(workers, "_worker_state", lambda *_args: {"running": False})
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "example.invalid/worker:test")


@pytest.mark.asyncio
async def test_guarded_launch_preserves_admission_until_exact_job_emission(monkeypatch):
    """Actual orchestration carries real admitted coordinates through Job construction."""
    _scope(monkeypatch)
    request, recorded = _coordinates()
    events = []
    _guarded_setup(monkeypatch, recorded, events)
    manifests = []

    def emit(method, _path, manifest):
        assert method == "POST" and events == ["enter", "admitted"]
        manifests.append(manifest)
        events.append("emitted")

    monkeypatch.setattr(workers, "_kubernetes_request", emit)
    assert (await workers.guarded_ensure_worker(request))["status"] == "started"
    assert type(request) is dict and "admitted_job" not in request
    environment_by_name = {
        entry["name"]: entry for entry in manifests[0]["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert environment_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]["value"] == request["job_id"]
    assert environment_by_name["HLTHPRT_DB_USER"]["valueFrom"]["secretKeyRef"]["name"] == "audit-database"
    assert events == ["enter", "admitted", "emitted", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("terminal", "job_mismatch", "admission"))
async def test_refused_admission_never_emits_scoped_credentials(monkeypatch, failure):
    """Refusal occurs before terminal Job cleanup or any new external mutation."""
    _scope(monkeypatch)
    request, recorded = _coordinates()
    events = []
    _guarded_setup(monkeypatch, recorded, events)
    external_calls = []
    monkeypatch.setattr(workers, "_worker_state", lambda *_args: {"running": False, "job_status": "succeeded"})
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *_args: external_calls.append("request"))
    monkeypatch.setattr(
        workers, "_delete_terminal_kubernetes_worker_jobs", lambda *_args: external_calls.append("delete")
    )
    if failure == "terminal":
        recorded["status"] = "succeeded"
    elif failure == "admission":
        monkeypatch.setattr(workers, "admit_existing_outer_run_action", AsyncMock(side_effect=ValueError("rejected")))
    else:
        request["job_id"] = "different"
    if failure == "job_mismatch":
        with pytest.raises(ValueError, match="exact admitted job"):
            await workers.guarded_ensure_worker(request)
    else:
        assert (await workers.guarded_ensure_worker(request))["status"] == "failed"
    assert external_calls == [] and events[-1] == "exit"


@pytest.mark.parametrize("proof", ["ordinary", "serialized", "cms"])
def test_fhir_publisher_secret_requires_its_exact_native_purpose(monkeypatch, proof):
    """Shared worker classes cannot mix ordinary Source and signed CMS credentials."""
    from api import provider_directory_cms_registry_worker as cms
    from api import provider_directory_source_worker as source

    monkeypatch.setenv(source.IMAGE_ENV, "example/source-worker@sha256:" + "a" * 64)
    request, recorded = _coordinates("provider-directory-fhir")
    admitted = _admitted(request, recorded)
    admitted.fhir_source_launch = source.admit_fhir_source_worker_launch(recorded)
    if proof == "serialized":
        admitted = json.loads(json.dumps({**admitted, "fhir_source_launch": {"image": "untrusted"}}))
    elif proof == "cms":
        admitted.cms_registry_launch = cms.CMSRegistryWorkerLaunch(
            request["run_id"], "example/cms@sha256:" + "b" * 64, "c" * 64
        )
    selector_by_field = {
        "name": "SOURCE_USER",
        "secretName": "synthetic-source-publisher",
        "key": "username",
        "workerClasses": ["process.ProviderDirectoryFHIR"],
        "importers": ["provider-directory-fhir"],
        "purposes": [source.PURPOSE],
    }
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", json.dumps([selector_by_field]))
    environment = workers._worker_job_secret_env("process.ProviderDirectoryFHIR", launch_request=admitted)
    assert bool(environment) is (proof == "ordinary")
    if proof == "ordinary":
        assert environment[0]["valueFrom"]["secretKeyRef"]["name"] == "synthetic-source-publisher"


@pytest.mark.asyncio
async def test_shared_audit_worker_keeps_native_pricing_authorization(monkeypatch):
    """Sharing a class never bypasses the real native importer guard."""
    from api import plan_pricing_worker_policy as pricing

    request, recorded = _coordinates()
    _guarded_setup(monkeypatch, recorded, [])
    failure = RuntimeError("native pricing admission unavailable")
    admission = AsyncMock(side_effect=failure)
    monkeypatch.setattr(pricing, "admit_pricing_worker", admission)
    launch = AsyncMock(side_effect=AssertionError("worker launched without native guard"))
    monkeypatch.setattr(workers, "ensure_worker", launch)
    with pytest.raises(RuntimeError) as caught:
        await workers.guarded_ensure_worker(request)
    assert caught.value is failure
    native_connection, run_id, admitted = admission.await_args.args
    assert callable(native_connection.fetchrow) and run_id == recorded["run_id"]
    assert admitted.admitted_job == (
        recorded["run_id"],
        recorded["importer"],
        recorded["metrics"]["queue"],
        recorded["metrics"]["function"],
        recorded["metrics"]["job_id"],
    )
    admission.assert_awaited_once()
    launch.assert_not_called()


_PRICING_READERS = (
    "plan-pricing-prewarm",
    "plan-pricing-code-projection",
    "plan-pricing-em-distance",
    "plan-pricing-em-attribution",
)
_GENERIC_IMAGE = "registry.example/ordinary@sha256:" + "a" * 64
_RELEASE_IMAGE = "registry.example/pricing@sha256:" + "b" * 64


def _release_image_request(monkeypatch, importer="plan-pricing-prewarm"):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", _GENERIC_IMAGE)
    monkeypatch.setenv(pricing_workers.RELEASE_IMAGE_ENV, _RELEASE_IMAGE)
    request, recorded = _coordinates(importer)
    return workers._BY_IMPORTER_ROLE[(importer, "start")], _admitted(request, recorded)


@pytest.mark.parametrize("importer", _PRICING_READERS)
def test_release_readers_select_only_the_deployment_image_and_recorded_target(monkeypatch, importer):
    spec, request = _release_image_request(monkeypatch, importer)
    assert workers._configured_worker_job_image(spec, request) == _RELEASE_IMAGE
    manifest = workers._worker_job_manifest(spec, request, _RELEASE_IMAGE)
    container = manifest["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == _RELEASE_IMAGE
    assert container["command"] == workers._worker_command(workers._worker_python(), spec)
    target = next(item for item in container["env"] if item["name"] == "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID")
    assert target["value"] == request.admitted_job[4]
    assert pricing_workers.RELEASE_IMAGE_ENV not in {item["name"] for item in container["env"]}


@pytest.mark.parametrize("importer", _PRICING_READERS)
@pytest.mark.parametrize("supplied_job", [False, True])
def test_release_reader_queue_selection_uses_admitted_importer_and_target(monkeypatch, importer, supplied_job):
    spec, request = _release_image_request(monkeypatch, importer)
    _scope(monkeypatch, importers=[importer])
    expected = workers._worker_job_manifest(spec, request, _RELEASE_IMAGE)
    request.pop("importer")
    request["queue"] = spec.queue
    if not supplied_job:
        request.pop("job_id")
    assert workers._resolve_specs(request) == [spec]
    assert workers._configured_worker_job_image(spec, request) == _RELEASE_IMAGE
    assert workers._worker_job_manifest(spec, request, _RELEASE_IMAGE) == expected
    assert "importer" not in request


@pytest.mark.asyncio
async def test_guarded_release_queue_launch_retains_persisted_identity(monkeypatch):
    request, recorded = _coordinates("plan-pricing-prewarm")
    request.pop("importer")
    request["queue"] = recorded["metrics"]["queue"]
    events, manifests = [], []
    _guarded_setup(monkeypatch, recorded, events, request_importer="")
    monkeypatch.setenv(pricing_workers.RELEASE_IMAGE_ENV, _RELEASE_IMAGE)

    def emit(method, _path, manifest):
        assert method == "POST" and events == ["enter", "admitted"]
        manifests.append(manifest)
        events.append("emitted")

    monkeypatch.setattr(workers, "_kubernetes_request", emit)
    assert (await workers.guarded_ensure_worker(request))["status"] == "started"
    container = manifests[0]["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == _RELEASE_IMAGE
    assert {"name": "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", "value": recorded["metrics"]["job_id"]} in container["env"]
    assert "importer" not in request
    assert events == ["enter", "admitted", "emitted", "exit"]


@pytest.mark.parametrize("configured", [None, "", "  "])
def test_unconfigured_release_image_preserves_legacy_selection_and_job_matching(monkeypatch, configured):
    spec, request = _release_image_request(monkeypatch)
    if configured is None:
        monkeypatch.delenv(pricing_workers.RELEASE_IMAGE_ENV)
    else:
        monkeypatch.setenv(pricing_workers.RELEASE_IMAGE_ENV, configured)
    assert workers._configured_worker_job_image(spec, request) == _GENERIC_IMAGE
    assert pricing_workers.release_worker_job_set_matches(spec, request, [{"legacy": "shape"}])


@pytest.mark.parametrize("image", ["registry.example/pricing:latest", "registry.example/pricing@sha256:bad"])
def test_release_image_refuses_mutable_or_malformed_references(monkeypatch, image):
    spec, request = _release_image_request(monkeypatch)
    monkeypatch.setenv(pricing_workers.RELEASE_IMAGE_ENV, image)
    with pytest.raises(ValueError, match="worker_image_unavailable"):
        workers._configured_worker_job_image(spec, request)


@pytest.mark.parametrize("serialized", [False, True])
def test_serialized_admission_cannot_select_release_image(monkeypatch, serialized):
    spec, admitted = _release_image_request(monkeypatch)
    request_by_field = dict(admitted)
    if serialized:
        request_by_field["admitted_job"] = list(admitted.admitted_job)
    assert workers._configured_worker_job_image(spec, request_by_field) == _GENERIC_IMAGE


@pytest.mark.parametrize("coordinate", range(5))
def test_changed_recorded_coordinate_cannot_select_release_image(monkeypatch, coordinate):
    spec, request = _release_image_request(monkeypatch)
    coordinates = list(request.admitted_job)
    coordinates[coordinate] = "foreign"
    request.admitted_job = tuple(coordinates)
    with pytest.raises(ValueError, match="worker_image"):
        workers._configured_worker_job_image(spec, request)


@pytest.mark.parametrize("field", ["run_id", "importer", "queue", "job_id", "worker_class", "role", "image"])
@pytest.mark.parametrize("omitted_importer", [False, True])
def test_caller_cannot_replace_release_worker_identity_or_image(monkeypatch, field, omitted_importer):
    spec, request = _release_image_request(monkeypatch)
    if omitted_importer:
        request.pop("importer")
    request[field] = "foreign"
    with pytest.raises(ValueError, match="worker_image"):
        workers._configured_worker_job_image(spec, request)


@pytest.mark.parametrize("importer", [None, ""])
def test_release_image_refuses_explicit_invalid_importer(monkeypatch, importer):
    spec, request = _release_image_request(monkeypatch)
    request["importer"] = importer
    with pytest.raises(ValueError, match="worker_image_request_changed"):
        workers._configured_worker_job_image(spec, request)


@pytest.mark.parametrize("change", [{"role": "finish"}, {"queue": "arq:PTG"}, {"worker_class": "process.PTG"}])
def test_release_image_requires_the_exact_start_worker(monkeypatch, change):
    spec, request = _release_image_request(monkeypatch)
    with pytest.raises(ValueError, match="worker_image_selection_changed"):
        workers._configured_worker_job_image(replace(spec, **change), request)


@pytest.mark.parametrize(
    "importer",
    [
        "plan-pricing-projection",
        "ptg-provider-membership-projection",
        "ptg-candidate-audit",
        "registry-publication-worker",
    ],
)
def test_other_audit_importers_keep_the_generic_image(monkeypatch, importer):
    spec, request = _release_image_request(monkeypatch, importer)
    assert workers._configured_worker_job_image(spec, request) == _GENERIC_IMAGE


def test_genuine_calibration_retains_its_image_and_existing_deadline(monkeypatch):
    from tests.test_plan_pricing_code_calibration import _params

    spec, _request = _release_image_request(monkeypatch, "plan-pricing-code-projection")
    request, recorded = _coordinates("plan-pricing-code-projection")
    recorded["params"] = _params()
    admitted = _admitted(request, recorded)
    assert workers._is_code_calibration_job(spec, admitted)
    assert workers._configured_worker_job_image(spec, admitted) == _GENERIC_IMAGE
    environment, _run_id = workers._worker_job_environment(spec, admitted)
    assert {"name": "HLTHPRT_PTG_CANDIDATE_AUDIT_JOB_TIMEOUT", "value": "385"} in environment
    forged = _admitted(request | {"code_calibration_job": admitted.admitted_job}, dict(recorded, params={}))
    assert workers._configured_worker_job_image(spec, forged) == _RELEASE_IMAGE


def _changed_release_job(job, damage):
    pod = job["spec"]["template"]["spec"]
    container = pod["containers"][0]
    if damage == "image":
        container["image"] = _GENERIC_IMAGE
    if damage == "command":
        container["command"] = ["python", "foreign.py"]
    if damage == "target":
        next(item for item in container["env"] if item["name"] == "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID")["value"] = (
            "foreign"
        )
    if damage == "duplicate-target":
        container["env"].append(dict(container["env"][-1]))
    if damage == "sidecar":
        pod["containers"].append(deepcopy(container))
    if damage == "policy-volume":
        pod["volumes"] = [{"name": "foreign-policy", "configMap": {"name": "foreign"}}]
    if damage == "environment":
        container["env"].append({"name": "FOREIGN_POLICY", "value": "true"})
    if damage == "short-deadline":
        job["spec"]["activeDeadlineSeconds"] = 60
    if damage == "missing-deadline":
        job["spec"].pop("activeDeadlineSeconds")
    if damage == "pod-deadline":
        pod["activeDeadlineSeconds"] = 60


@pytest.mark.parametrize(
    "damage",
    [
        None,
        "image",
        "command",
        "target",
        "duplicate-target",
        "sidecar",
        "policy-volume",
        "environment",
        "short-deadline",
        "missing-deadline",
        "pod-deadline",
    ],
)
@pytest.mark.parametrize("terminal", [False, True])
def test_existing_release_job_matches_complete_execution_before_reuse_or_deletion(monkeypatch, damage, terminal):
    spec, request = _release_image_request(monkeypatch)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", "3600")
    job = workers._worker_job_manifest(spec, request, _RELEASE_IMAGE)
    assert job["spec"]["activeDeadlineSeconds"] == 3600
    _changed_release_job(job, damage)
    job["status"] = {"succeeded": 1} if terminal else {"active": 1}
    methods = []

    def query(method, _path, _body=None):
        methods.append(method)
        assert method == "GET"
        return {"items": [job]}

    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_kubernetes_namespace", lambda: "synthetic-workers")
    monkeypatch.setattr(workers, "_kubernetes_request", query)
    state = workers._kubernetes_worker_state(spec, request)
    assert state["job_status"] == ("purpose_mismatch" if damage else "succeeded" if terminal else "active")
    if damage:
        monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
        result = workers._ensure_spec(spec, request)
        assert result["status"] == "failed" and result["job_status"] == "purpose_mismatch"
    assert methods and set(methods) == {"GET"}


@pytest.mark.parametrize("damage", ["image", "short-deadline"])
@pytest.mark.parametrize("has_conflict", [False, True])
def test_release_job_post_result_rechecks_full_execution(monkeypatch, damage, has_conflict):
    spec, request = _release_image_request(monkeypatch)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", "3600")
    old = workers._worker_job_manifest(spec, request, _RELEASE_IMAGE)
    _changed_release_job(old, damage)
    old["status"] = {"active": 1}
    methods = []

    def conflict(method, _path, _body=None):
        methods.append(method)
        if method == "POST":
            if has_conflict:
                raise workers._KubernetesApiError(409, "existing")
            return {}
        assert method == "GET"
        return {"items": [old]}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_kubernetes_namespace", lambda: "synthetic-workers")
    monkeypatch.setattr(workers, "_kubernetes_request", conflict)
    result = workers._ensure_kubernetes_job(spec, request, {})
    assert result["status"] == "failed" and result["job_status"] == "purpose_mismatch"
    assert methods == ["POST", "GET"]
