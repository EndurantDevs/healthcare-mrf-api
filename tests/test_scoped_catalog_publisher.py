# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit publisher settings and exact, idempotent protected Job admission."""

import json
import re
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, urlsplit

import pytest

from api import control_workers as workers
from api import scoped_catalog_publisher_worker as launcher
from process.scoped_catalog_publisher import PREFIX, PublisherSettings, parse_attempt
from tests.test_scoped_catalog_handoff import _value


def _environment():
    return {
        PREFIX + key: value
        for key, value in {
            "DSN": "postgresql://publisher@localhost/catalog",
            "SCHEMA": "serving",
            "OWNER_ROLE": "publisher_owner",
            "READER_ROLES": '["catalog_reader"]',
            "COPY_MAX_BYTES": "1048576",
            "TIMEOUT_SECONDS": "30",
        }.items()
    }


@pytest.mark.parametrize("key", ["DSN", "SCHEMA", "OWNER_ROLE", "READER_ROLES", "COPY_MAX_BYTES", "TIMEOUT_SECONDS"])
def test_publisher_requires_each_explicit_target_setting(key):
    environment = _environment()
    settings = PublisherSettings.from_environment(environment)
    assert settings.max_copy_bytes == 1048576 and "dsn=" not in repr(settings)
    environment.pop(PREFIX + key)
    with pytest.raises(ValueError, match="configuration is invalid"):
        PublisherSettings.from_environment(environment)


@pytest.mark.parametrize(
    "key,value",
    [
        ("DSN", "invalid"),
        ("DSN", "sqlite:///catalog"),
        ("SCHEMA", "invalid.schema"),
        ("OWNER_ROLE", "PUBLIC"),
        ("READER_ROLES", '["publisher_owner"]'),
        ("READER_ROLES", "[]"),
        ("READER_ROLES", '["reader","reader"]'),
        ("COPY_MAX_BYTES", "0"),
        ("COPY_MAX_BYTES", "2147483649"),
        ("TIMEOUT_SECONDS", "601"),
    ],
)
def test_publisher_refuses_unsafe_or_unbounded_settings(key, value):
    environment = _environment()
    environment[PREFIX + key] = value
    with pytest.raises(ValueError, match="configuration is invalid"):
        PublisherSettings.from_environment(environment)


def _request():
    value = _value()
    generation = value["expected_generations"]["code-sets"]
    return workers._AdmittedWorkerRequest(
        importer="code-sets",
        role=launcher.ROLE,
        status="finalizing",
        reason="reconcile",
        **{key: value[key] for key in launcher.SELECTORS[:4]},
        expected_lineage_id=generation["local_lineage_id"],
        expected_generation=generation["local_generation"],
    )


def _spec(request):
    return launcher.selected_spec(workers._BY_IMPORTER_ROLE[(request["importer"], "start")], request)


def test_publisher_manifest_has_only_fixed_secret_refs_and_logical_arguments(monkeypatch):
    request = _request()
    image = "registry.example/image@sha256:" + "a" * 64
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", "ordinary-application")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_TTL_SECONDS", "60")
    resource_profile_map = {"requests": {"cpu": "250m", "memory": "512Mi"}, "limits": {"cpu": "1", "memory": "2Gi"}}
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", json.dumps({"process.CodeSets": resource_profile_map})
    )
    job = launcher.manifest(_spec(request), request, image)
    pod = job["spec"]["template"]["spec"]
    container = pod["containers"][0]
    dockerfile = (Path(__file__).resolve().parents[1] / "Dockerfile").read_text()
    assert container["workingDir"] == re.findall(r"(?m)^WORKDIR (\S+)$", dockerfile)[-1]
    assert container["resources"] == resource_profile_map
    assert job["metadata"]["annotations"]["healthporta.com/worker-class"] == "process.CodeSets"
    assert job["metadata"]["annotations"]["healthporta.com/run-id"] == request["run_id"]
    assert job == launcher.manifest(_spec(request), request, image)
    assert pod["serviceAccountName"] == launcher.SERVICE_ACCOUNT and pod["automountServiceAccountToken"] is False
    assert "envFrom" not in container and not pod.get("volumes") and job["spec"]["backoffLimit"] == 0
    assert "ttlSecondsAfterFinished" not in job["spec"]
    assert job["spec"]["parallelism"] == job["spec"]["completions"] == 1
    assert len(container["env"]) == 6 and all(
        "value" not in entry and entry["valueFrom"]["secretKeyRef"]["name"] == launcher.SECRET_NAME
        for entry in container["env"]
    )
    args = parse_attempt(container["args"])
    assert args.run_id == request["run_id"] and args.expected_generation == request["expected_generation"]
    changed_by_field = dict(request, attempt_id="new-attempt")
    assert (
        launcher.manifest(_spec(changed_by_field), changed_by_field, image)["metadata"]["name"]
        != job["metadata"]["name"]
    )


@pytest.mark.parametrize(
    "change", ["image", "secret", "arguments", "extra-env", "env-from", "service-account", "resources", "annotations"]
)
def test_existing_publisher_job_must_match_the_actual_admission(change):
    request = _request()
    expected = launcher.manifest(_spec(request), request, "registry.example/image@sha256:" + "a" * 64)
    actual = deepcopy(expected)
    pod = actual["spec"]["template"]["spec"]
    container = pod["containers"][0]
    if change == "resources":
        container["resources"] = {"limits": {"memory": "128Gi"}}
    if change == "annotations":
        actual["metadata"]["annotations"]["healthporta.com/run-id"] = "another-run"
    if change == "image":
        container["image"] += "0"
    if change == "secret":
        container["env"][0]["valueFrom"]["secretKeyRef"]["name"] = "other"
    if change == "arguments":
        container["args"][1] = "other-run"
    if change == "extra-env":
        container["env"].append({"name": "OTHER", "value": "other"})
    if change == "env-from":
        container["envFrom"] = [{"secretRef": {"name": "other"}}]
    if change == "service-account":
        pod["serviceAccountName"] = "ordinary"
    assert launcher._matches(expected, expected)
    assert not launcher._matches(actual, expected)


@pytest.mark.parametrize(
    "condition,expected", [(None, "already_running"), ("Failed", "failed"), ("Complete", "failed")]
)
def test_terminal_job_never_recreates_or_claims_native_success(monkeypatch, condition, expected):
    request = _request()
    request.scoped_catalog_admission = True
    image = "registry.example/image@sha256:" + "a" * 64
    job = launcher.manifest(_spec(request), request, image)
    job["status"] = {"conditions": [{"type": condition, "status": "True"}]} if condition else {"active": 1}
    api = Mock(return_value=job)
    monkeypatch.setattr(workers, "_kubernetes_request", api)
    monkeypatch.setattr(workers, "_launcher_mode", lambda: "kubernetes")
    monkeypatch.setattr(workers, "_configured_worker_job_image", lambda *_: image)
    original = deepcopy(job)
    results = [launcher.ensure(_spec(request), request) for _ in range(2)]
    assert all(result["status"] == expected for result in results)
    assert results[0] == results[1]
    assert job == original
    assert [call.args[0] for call in api.call_args_list] == ["GET", "GET"]
    assert api.call_args_list[0].args[1] == api.call_args_list[1].args[1]


def test_generic_terminal_cleanup_cannot_select_the_publisher_role(monkeypatch):
    request = _request()
    job = launcher.manifest(_spec(request), request, "registry.example/image@sha256:" + "a" * 64)
    job["metadata"]["uid"] = "synthetic-job-uid"
    job["status"] = {
        "failed": 1,
        "conditions": [{"type": "Failed", "status": "True", "reason": "DeadlineExceeded"}],
    }
    original = deepcopy(job)

    def matching_jobs(method, path):
        assert method == "GET"
        selector = parse_qs(urlsplit(path).query)["labelSelector"][0]
        pairs = (item.split("=", 1) for item in selector.split(","))
        matches = all(job["metadata"]["labels"].get(key) == value for key, value in pairs)
        return {"items": [job] if matches else []}

    api = Mock(side_effect=matching_jobs)
    monkeypatch.setattr(workers, "_kubernetes_request", api)
    spec = workers._BY_IMPORTER_ROLE[(request["importer"], "start")]
    workers._delete_terminal_kubernetes_worker_jobs("catalog", spec, request)
    workers._delete_terminal_kubernetes_worker_jobs("catalog", spec, request)
    record, error = workers._delete_kubernetes_worker_job_record("catalog", _spec(request), job)
    assert record["deleted"] is False and record["reason"] == "terminal" and error is None
    assert job == original
    assert [call.args[0] for call in api.call_args_list] == ["GET", "GET"]


@pytest.mark.parametrize("condition,expected", [("Failed", "failed"), ("Complete", "succeeded")])
def test_publisher_state_uses_terminal_condition_without_pod_counts(monkeypatch, condition, expected):
    request = _request()
    image = "registry.example/image@sha256:" + "a" * 64
    job = launcher.manifest(_spec(request), request, image)
    job["status"] = {"conditions": [{"type": condition, "status": "True"}]}
    api = Mock(return_value=job)
    monkeypatch.setattr(workers, "_kubernetes_request", api)
    monkeypatch.setattr(workers, "_configured_worker_job_image", lambda *_: image)
    result = launcher.state(request)
    assert result["items"][0]["job_status"] == expected
    assert result["items"][0]["running"] is False
    assert [call.args[0] for call in api.call_args_list] == ["GET"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["attempt", "node", "generation", "params", "error", "succeeded", "canceled", "none"]
)
async def test_launcher_admits_only_real_persisted_attempt(monkeypatch, change):
    request, candidate = _request(), _value()
    run_by_field = dict(
        run_id=candidate["run_id"],
        importer="code-sets",
        engine="healthcare-mrf-api",
        node_id="node-1",
        status="finalizing",
        finished_at=None,
        error=None,
        progress={key: candidate[key] for key in ("attempt_id", "attempt_started_at")},
        params={},
        metrics={"scoped_catalog_handoff": candidate},
    )
    if change == "attempt":
        request["attempt_id"] = "other"
    if change == "node":
        run_by_field["node_id"] = "other"
    if change == "generation":
        request["expected_generation"] = 2
    if change == "params":
        run_by_field["params"] = {"other": True}
    if change == "error":
        run_by_field["error"] = {"code": "synthetic_failure"}
    if change in {"succeeded", "canceled"}:
        run_by_field.update(status=change, finished_at="2026-01-01T00:01:00+00:00")
        metric = "scoped_catalog_publication" if change == "succeeded" else "scoped_catalog_cancellation"
        run_by_field["metrics"][metric] = {"handoff": candidate}
    connection = SimpleNamespace(all=AsyncMock(return_value=[run_by_field]))
    if change == "none":
        assert await launcher._admit(connection, request, "serving") is None
    elif change in {"succeeded", "canceled"}:
        assert await launcher._admit(connection, request, "serving") == change
    else:
        with pytest.raises(ValueError, match="attempt changed"):
            await launcher._admit(connection, request, "serving")


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["succeeded", "canceled"])
async def test_terminal_native_run_never_respawns_after_job_cleanup(monkeypatch, terminal):
    @asynccontextmanager
    async def acquire():
        yield SimpleNamespace()

    api = Mock(side_effect=AssertionError("terminal admission must not touch Kubernetes"))
    monkeypatch.setattr(workers.db, "acquire", acquire)
    monkeypatch.setattr(launcher, "acquire_control_run_worker_action_lock", AsyncMock())
    monkeypatch.setattr(launcher, "_admit", AsyncMock(return_value=terminal))
    monkeypatch.setattr(workers, "_kubernetes_request", api)
    results = [await launcher.guarded_ensure(_request()) for _ in range(2)]
    assert all(result["status"] == "blocked" for result in results)
    assert all(result["items"][0]["job_status"] == terminal for result in results)
    api.assert_not_called()


def test_cancel_stops_only_the_matching_publisher_job(monkeypatch):
    request, candidate = _request(), _value()
    image = "registry.example/image@sha256:" + "a" * 64
    job = launcher.manifest(_spec(request), request, image)
    job["metadata"]["uid"] = "synthetic-job-uid"
    job["status"] = {"active": 1}
    api = Mock(return_value=job)
    monkeypatch.setattr(workers, "_kubernetes_request", api)
    monkeypatch.setattr(workers, "_launcher_mode", lambda: "kubernetes")
    monkeypatch.setattr(workers, "_configured_worker_job_image", lambda *_: image)
    run_by_field = dict(
        run_id=candidate["run_id"],
        importer="code-sets",
        node_id="node-1",
        status="finalizing",
        progress={key: candidate[key] for key in ("attempt_id", "attempt_started_at")},
        metrics={"scoped_catalog_handoff": candidate},
    )
    assert launcher.stop_publisher(run_by_field) == {"deleted": 1}
    assert [call.args[0] for call in api.call_args_list] == ["GET", "DELETE"]
    assert api.call_args_list[1].args[2]["preconditions"] == {"uid": "synthetic-job-uid"}
    assert api.call_args_list[0].args[1] == api.call_args_list[1].args[1]


@pytest.mark.asyncio
async def test_cancel_stops_before_row_lock_and_preserves_committed_success(monkeypatch):
    from api import control_imports as imports

    events = []
    candidate = _value()
    observed_by_field = dict(status="finalizing", metrics={"scoped_catalog_handoff": candidate})
    native_by_field = dict(status="succeeded", run_id=candidate["run_id"], metrics=observed_by_field["metrics"])
    monkeypatch.setattr(launcher, "stop_publisher", lambda *_: events.append("stop"))

    @asynccontextmanager
    async def transaction():
        events.append("row-lock")
        yield SimpleNamespace()

    monkeypatch.setattr(imports.db, "transaction", transaction)
    monkeypatch.setattr(imports.db, "first", AsyncMock(return_value=SimpleNamespace(_mapping=native_by_field)))
    monkeypatch.setattr(imports, "normalize_run", lambda current: current)
    assert await launcher.request_cancel(candidate["run_id"], observed_by_field) == native_by_field
    assert events == ["stop", "row-lock"]


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
async def test_cancel_racing_catalog_handoff_uses_protected_catalog_stop(monkeypatch, importer):
    from api import control_imports as imports

    latest_run_dict = {
        "run_id": "synthetic-run",
        "importer": importer,
        "status": "finalizing",
        "metrics": {"scoped_catalog_handoff": _value()},
    }
    cancel = AsyncMock(return_value={"status": "canceling"})
    source_cancel = AsyncMock(side_effect=AssertionError("catalog handoff is not a source-profile handoff"))
    monkeypatch.setattr(imports, "db", SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(rowcount=0))))
    monkeypatch.setattr(imports, "get_import_run", AsyncMock(return_value=latest_run_dict))
    monkeypatch.setattr(imports, "request_catalog_stop", cancel)
    monkeypatch.setattr(imports, "_request_source_profile_cancel", source_cancel)
    outcome = await imports._persist_cancel_request(
        latest_run_dict["run_id"],
        current_run={"finished_at": None},
        requested_at=imports.utc_now(),
        cancel_state_by_name={
            "canceled_now": False,
            "status": "canceling",
            "phase_detail": "cancel requested",
            "progress": {},
        },
        run_metrics_by_name={},
    )
    assert outcome == {"status": "canceling"}
    cancel.assert_awaited_once_with(latest_run_dict["run_id"], latest_run_dict)
    source_cancel.assert_not_awaited()


@pytest.mark.parametrize(
    "change",
    ["ttl", "retry", "parallel", "deadline", "host-network", "pod-privilege", "container-privilege", "restart"],
)
def test_existing_publisher_cannot_gain_retries_or_privileges(change):
    request = _request()
    expected = launcher.manifest(_spec(request), request, "registry.example/image@sha256:" + "a" * 64)
    actual = deepcopy(expected)
    pod = actual["spec"]["template"]["spec"]
    if change == "ttl":
        actual["spec"]["ttlSecondsAfterFinished"] = 60
    if change == "retry":
        actual["spec"]["backoffLimit"] = 6
    if change == "parallel":
        actual["spec"]["parallelism"] = 2
    if change == "deadline":
        actual["spec"]["activeDeadlineSeconds"] = 0
    if change == "host-network":
        pod["hostNetwork"] = True
    if change == "pod-privilege":
        pod["securityContext"]["runAsUser"] = 0
    if change == "container-privilege":
        pod["containers"][0]["securityContext"]["privileged"] = True
    if change == "restart":
        pod["restartPolicy"] = "OnFailure"
    assert not launcher._matches(actual, expected)
