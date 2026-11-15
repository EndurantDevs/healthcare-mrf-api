# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary finish repair selects only the persisted protected publication."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from api import control_workers as workers
from api import scoped_catalog_publisher_worker as launcher
from api.control_imports import _SINGLE_JOB_ADAPTERS, _enqueue_job_options
from db.models import ImportRun
from process.scoped_catalog_publisher import parse_attempt
from tests.test_scoped_catalog_handoff import _value


def _ordinary_job_id(importer, run_id):
    return _enqueue_job_options(_SINGLE_JOB_ADAPTERS[importer], {"run_id": run_id}).get("_job_id", UUID(int=5).hex)


def _run(importer):
    candidate = _value(importer)
    spec = workers._BY_IMPORTER_ROLE[(importer, "start")]
    return dict(
        run_id=candidate["run_id"],
        importer=importer,
        engine="healthcare-mrf-api",
        node_id=candidate["node_id"],
        status="finalizing",
        finished_at=None,
        error=None,
        progress={key: candidate[key] for key in ("attempt_id", "attempt_started_at")},
        params={},
        metrics={
            "scoped_catalog_handoff": candidate,
            "queue": spec.queue,
            "job_id": _ordinary_job_id(importer, candidate["run_id"]),
        },
    )


def _repair_request(run):
    spec = workers._BY_IMPORTER_ROLE[(run["importer"], "start")]
    return {
        "importer": run["importer"],
        "run_id": run["run_id"],
        "status": run["status"],
        "reason": "queued_run_repair",
        "role": "finish",
        "queue": spec.queue,
        "worker_class": spec.worker_class,
        "job_id": run["metrics"]["job_id"],
    }


def _transport(monkeypatch):
    jobs_by_path = {}

    def request(method, path, body=None):
        if method == "POST":
            jobs_by_path[path + "/" + body["metadata"]["name"]] = deepcopy(body)
            return body
        if method == "GET" and path in jobs_by_path:
            return jobs_by_path[path]
        raise workers._KubernetesApiError(404, "not found")

    api = Mock(side_effect=request)
    monkeypatch.setattr(workers, "_kubernetes_request", api)
    monkeypatch.setattr(workers, "_launcher_mode", lambda: "kubernetes")
    monkeypatch.setattr(workers, "_configured_worker_job_image", lambda *_: "registry.example/image@sha256:" + "a" * 64)
    monkeypatch.setattr(workers, "ensure_worker", Mock(side_effect=AssertionError("ordinary worker is forbidden")))
    return api, jobs_by_path


def _database(monkeypatch, run):
    @asynccontextmanager
    async def acquire():
        yield SimpleNamespace(all=AsyncMock(return_value=[run]))

    monkeypatch.setattr(workers.db, "acquire", acquire)
    monkeypatch.setattr(ImportRun.__table__, "schema", "serving")
    monkeypatch.setattr(launcher, "acquire_control_run_worker_action_lock", AsyncMock())


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
@pytest.mark.parametrize("hints", [True, False])
async def test_normal_finish_request_uses_only_persisted_publication(monkeypatch, importer, hints):
    run = _run(importer)
    request = _repair_request(run)
    if not hints:
        for key in ("role", "queue", "worker_class", "job_id"):
            request.pop(key)
    original = deepcopy((run, request))
    _database(monkeypatch, run)
    api, jobs = _transport(monkeypatch)
    assert (await workers.guarded_ensure_worker(request))["status"] == "started"
    assert (await workers.guarded_ensure_worker(request))["status"] == "already_running"
    assert (run, request) == original
    assert [call.args[0] for call in api.call_args_list] == ["GET", "POST", "GET"]
    assert next(iter(jobs.values()))["metadata"]["name"] != run["metrics"]["job_id"]
    pod = next(iter(jobs.values()))["spec"]["template"]["spec"]
    container = pod["containers"][0]
    attempt = parse_attempt(container["args"])
    candidate = run["metrics"]["scoped_catalog_handoff"]
    assert all(getattr(attempt, key) == candidate[key] for key in launcher.SELECTORS[:4])
    assert attempt.expected_lineage_id == candidate["expected_generations"][importer]["local_lineage_id"]
    assert attempt.expected_generation == candidate["expected_generations"][importer]["local_generation"]
    assert container["command"][-1] == "process.scoped_catalog_publisher"
    assert pod["serviceAccountName"] == launcher.SERVICE_ACCOUNT
    assert len(container["env"]) == 6 and "envFrom" not in container
    assert {item["valueFrom"]["secretKeyRef"]["name"] for item in container["env"]} == {launcher.SECRET_NAME}


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
@pytest.mark.parametrize(
    "change",
    ["attempt", "node", "generation", "boolean", "job", "queue", "class", "resource", "role", "extra", "missing"],
)
async def test_finish_repair_rejects_changed_bindings_without_any_worker(monkeypatch, importer, change):
    run = _run(importer)
    request = _repair_request(run)
    changes_by_name = {
        "attempt": ("attempt_id", "different"),
        "node": ("node_id", "different"),
        "generation": ("expected_generation", 55),
        "boolean": ("expected_generation", False),
        "job": ("job_id", "different"),
        "queue": ("queue", "arq:Other"),
        "class": ("worker_class", "process.Other"),
        "resource": ("resource_class", "different"),
        "role": ("role", "start"),
        "extra": ("credentials", "different"),
    }
    if change == "missing":
        run["metrics"].pop("scoped_catalog_handoff")
    else:
        key, value = changes_by_name[change]
        request[key] = value
    _database(monkeypatch, run)
    api, jobs = _transport(monkeypatch)
    assert (await workers.guarded_ensure_worker(request))["status"] == "failed"
    api.assert_not_called()
    assert not jobs
