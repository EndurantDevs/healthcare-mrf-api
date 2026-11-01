# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original route preservation composes with fresh admission and frozen proxy audit."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_workers as workers
from api import provider_directory_source_worker as source
from tests.test_provider_directory_fresh_source_route import _admitted, _fresh_route, _fresh_run, configured_runtime
from tests.test_provider_directory_source_proxy_launch import _BINDING, _POLICY, _cooldowns, enabled_proxy
from tests.test_provider_directory_source_worker_launch import (
    IMAGE,
    ORDINARY_IMAGE,
    SPEC,
    _mock_original_worker_control,
    _original_route_run,
    _source_route_config,
    original_job_before_default_change,
)


def _original_request(monkeypatch):
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_fresh_route(), _source_route_config()]))
    recorded = _original_route_run()
    request = workers._AdmittedWorkerRequest(run_id=recorded["run_id"], importer="provider-directory-fhir")
    workers._bind_admitted_job(request, recorded)
    request.fhir_source_launch = source.admit_fhir_source_worker_launch(recorded)
    return request, recorded


@pytest.mark.asyncio
async def test_original_pin_precedes_fresh_route(monkeypatch, original_job_before_default_change):
    recorded, job = original_job_before_default_change
    fresh_by_field = {
        **_fresh_route(),
        "image": "example/fresh-worker@sha256:" + "f" * 64,
        "secret_name": "synthetic-fresh",
    }
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([fresh_by_field, _source_route_config()]))
    connection, calls = _mock_original_worker_control(monkeypatch, recorded, [deepcopy(recorded)], job)
    before = deepcopy(job)
    outcome = await workers.guarded_ensure_worker({"run_id": recorded["run_id"], "importer": "provider-directory-fhir"})
    assert outcome["status"] == "already_running"
    assert job == before and len(calls) == 1 and calls[0][0] == "GET"
    connection.all.assert_awaited_once()


@pytest.mark.usefixtures("enabled_proxy")
def test_fresh_proxy_ignores_other_original_pin(monkeypatch):
    original_by_field = {**_source_route_config(), "image": IMAGE, "secret_name": "synthetic-default"}
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([original_by_field, _fresh_route()]))
    request = _admitted(_fresh_run())
    assert request.fhir_source_launch.image == ORDINARY_IMAGE
    assert request.fhir_source_launch.root_run_id == request.fhir_source_launch.retry_of_run_id == ""
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    assert "healthporta.com/source-root-run-id" not in job["metadata"]["annotations"]
    assert source.is_fhir_job_set_matching(SPEC, request, [job])


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.asyncio
async def test_original_proxy_requires_observed_audit(monkeypatch):
    request, recorded = _original_request(monkeypatch)
    assert request.fhir_source_launch.proxy_cooldowns_json == ""
    with pytest.raises(ValueError, match="route_changed"):
        await source.require_fhir_source_route_ancestry(
            SimpleNamespace(all=AsyncMock(return_value=[recorded])), request.fhir_source_launch
        )


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.asyncio
async def test_original_proxy_restores_native_cooldowns(monkeypatch):
    request, recorded = _original_request(monkeypatch)
    snapshot_by_field = _cooldowns([("synthetic-egress", None)])
    recorded.update(source_http_proxy_binding=deepcopy(_BINDING), source_http_proxy_cooldowns=snapshot_by_field)
    original = request.fhir_source_launch
    request.fhir_source_launch = await source.require_fhir_source_route_ancestry(
        SimpleNamespace(all=AsyncMock(return_value=[recorded])), original
    )
    assert request.fhir_source_launch is not original
    assert json.loads(request.fhir_source_launch.proxy_cooldowns_json) == snapshot_by_field
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    assert "healthporta.com/source-root-run-id" not in job["metadata"]["annotations"]
    assert source.is_fhir_job_set_matching(SPEC, request, [job])


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.asyncio
async def test_original_proxy_cannot_drop_native_binding(monkeypatch):
    policy_by_field = deepcopy(_POLICY)
    policy_by_field["sources"][0]["use_proxy"] = False
    monkeypatch.setenv(source.PROXY_POLICY_ENV, json.dumps(policy_by_field))
    request, recorded = _original_request(monkeypatch)
    recorded.update(source_http_proxy_binding=deepcopy(_BINDING), source_http_proxy_cooldowns=_cooldowns())
    with pytest.raises(ValueError, match="route_changed"):
        await source.require_fhir_source_route_ancestry(
            SimpleNamespace(all=AsyncMock(return_value=[recorded])), request.fhir_source_launch
        )
