# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Importer-scoped credentials require durable single-job launch admission."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_workers as workers


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
    """Use the actual four importer adapters, not a caller-selected queue/function."""
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
    ),
)
def test_shared_class_scopes_secrets_to_admitted_importer(monkeypatch, importer):
    """The three sibling jobs retain ordinary credentials on the unchanged class."""
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


def _guarded_setup(monkeypatch, recorded, events):
    """Keep real admission/launch code and replace only database and external I/O."""

    @asynccontextmanager
    async def acquire():
        events.append("enter")
        try:
            yield object()
        finally:
            events.append("exit")

    async def admit(**arguments):
        assert arguments["worker_selection"].request_importer == recorded["importer"]
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
