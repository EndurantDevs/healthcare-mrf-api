# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Route signed serving commands through the existing exact ordinary worker admission."""

from contextlib import asynccontextmanager
from copy import deepcopy
from importlib import import_module
from unittest.mock import AsyncMock

import pytest

from api import control_workers as workers
from api import provider_directory_source_worker as source
from process import provider_directory_cms_preflight as preflight
from process import provider_directory_ordinary_serving as ordinary
from process import provider_directory_profile_selection_contract as selection
from process import provider_directory_serving_capacity_contract as serving
from process import provider_directory_serving_inverse_capacity_contract as inverse
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
from tests.provider_directory_serving_capacity_signing_test_support import serving_case
from tests.provider_directory_serving_inverse_capacity_support import signed_envelope
from tests.test_provider_directory_source_worker_launch import (
    JOB_ID,
    ORDINARY_IMAGE,
    RUN_ID,
    SPEC,
    _change_overlay_job,
    _recorded,
)
from tests.test_provider_directory_source_worker_launch import (
    secret_selection as secret_selection,
)
from tests.test_provider_directory_source_worker_launch import (
    source_runtime_mount as source_runtime_mount,
)

fhir = import_module("process.provider_directory_fhir")


@pytest.fixture(params=["initial", "delta", "inverse"])
def serving_run(monkeypatch, request):
    """Use real signatures and both existing wire codecs; no native authority is fabricated."""
    monkeypatch.setattr(selection, "configured_node_id", lambda: "dev-node")
    if request.param == "inverse":
        envelope = signed_envelope()
        operation = envelope["lease"]["signing_preflight_guard"]["healthcare_request"]["recovery_execution"]
        params_by_field = {
            "provider_directory_serving_operation": "recover",
            "provider_directory_serving_generation": operation["generation"],
            "source_ids": operation["source_ids"],
            inverse.EVENT_FIELD: operation["published_event_sha256"],
            inverse.EXECUTION_FIELD: envelope,
        }
    else:
        case = serving_case(initial_mode=request.param == "initial")
        envelope = case.envelope
        params_by_field = {
            **deepcopy(selection._GLOBAL_PROFILE_PARAMS),
            "provider_directory_profile_generation": case.execution.generation,
            "provider_directory_profile_selection_attestation": case.execution.attestation.payload,
            "provider_directory_profile_capacity_attestation": case.pair,
            serving.EXECUTION_FIELD: envelope,
        }
    trust = capacity_trust_from_envelope(envelope)
    monkeypatch.setattr(ordinary, "configured_capacity_lease_trust", lambda: trust)
    monkeypatch.setattr(preflight.capacity_runtime, "configured_capacity_lease_trust", lambda: trust)
    monkeypatch.setattr(
        fhir,
        "_profile_capacity_preflight_clock",
        AsyncMock(return_value=inverse.utc_second(envelope["lease"]["issued_at"])),
    )
    monkeypatch.setattr(selection, "configured_node_id", lambda: "dev-node")
    monkeypatch.setenv(source.IMAGE_ENV, "example/source-worker@sha256:" + "c" * 64)
    return {**_recorded(), "params": params_by_field}


async def _admit(record):
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    workers._bind_admitted_job(request, record)
    result = await workers._admit_source_worker_launches(request, record)
    return request, result


@pytest.mark.asyncio
async def test_signed_serving_selects_ordinary_image_and_excludes_source_credentials(
    monkeypatch, serving_run, secret_selection, source_runtime_mount
):
    request, result = await _admit(serving_run)
    assert result is None and request.fhir_source_launch is None
    assert source.admitted_overlay_job(SPEC, request) == JOB_ID
    job = workers._worker_job_manifest(SPEC, request, ORDINARY_IMAGE)
    pod = job["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert container["image"] == ORDINARY_IMAGE
    assert container["command"] == workers._worker_command(workers._worker_python(), SPEC)
    assert pod["securityContext"]["runAsUser"] == 65534
    assert "healthporta.com/worker-purpose" not in job["metadata"]["labels"]
    assert "SOURCE_USER" not in {entry["name"] for entry in container["env"]}
    assert [entry["name"] for entry in pod["volumes"]] == ["ordinary-credentials"]


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["signature", "extra", "node", "expired", "job"])
async def test_invalid_serving_command_never_receives_ordinary_admission(monkeypatch, serving_run, drift):
    record = deepcopy(serving_run)
    params_by_field = record["params"]
    field = inverse.EXECUTION_FIELD if inverse.EXECUTION_FIELD in params_by_field else serving.EXECUTION_FIELD
    if drift == "signature":
        params_by_field[field]["signature"] = "A" * 86
    elif drift == "extra":
        params_by_field["provider_directory_pagination_root_run_id"] = "run_" + "b" * 32
    elif drift == "node":
        monkeypatch.setattr(selection, "configured_node_id", lambda: "other-node")
    elif drift == "expired":
        monkeypatch.setattr(
            fhir,
            "_profile_capacity_preflight_clock",
            AsyncMock(return_value=inverse.utc_second(params_by_field[field]["lease"]["expires_at"])),
        )
    else:
        record["metrics"]["job_id"] = "other-job"
    request, result = await _admit(record)
    assert result["status"] == "failed"
    assert request.fhir_source_launch is None and request.ordinary_fhir_overlay_job is None


@pytest.mark.asyncio
async def test_guarded_ensure_uses_persisted_serving_command(
    monkeypatch, serving_run, secret_selection, source_runtime_mount
):
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", ORDINARY_IMAGE)
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", AsyncMock(return_value=serving_run))
    monkeypatch.setattr(workers, "acquire_control_run_worker_action_lock", AsyncMock())
    calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: calls.append(args) or {"items": []})

    @asynccontextmanager
    async def connection():
        yield object()

    monkeypatch.setattr(workers.db, "acquire", connection)
    response = await workers.guarded_ensure_worker(
        {"run_id": RUN_ID, "importer": "provider-directory-fhir", "params": {"import_resources": True}}
    )
    assert response["status"] == "started"
    jobs = [call[2] for call in calls if call[0] == "POST"]
    assert len(jobs) == 1
    container = jobs[0]["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == ORDINARY_IMAGE
    assert {entry["name"]: entry for entry in container["env"]}["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]["value"] == JOB_ID


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift", [None, "image", "command", "target", "source_mount", "source_env", "purpose", "service_account"]
)
async def test_existing_serving_job_keeps_exact_ordinary_runtime(monkeypatch, serving_run, source_runtime_mount, drift):
    request, result = await _admit(serving_run)
    assert result is None
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", ORDINARY_IMAGE)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SERVICE_ACCOUNT", "synthetic-worker")
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_launcher_mode", lambda: "kubernetes")
    job = workers._worker_job_manifest(SPEC, request, ORDINARY_IMAGE)
    job["status"] = {"active": 1}
    _change_overlay_job(job, drift)
    calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: calls.append(args) or {"items": [job]})
    assert workers._ensure_spec(SPEC, request)["status"] == ("already_running" if drift is None else "failed")
    assert all(call[0] == "GET" for call in calls)


@pytest.mark.asyncio
async def test_serialized_serving_marker_cannot_create_admission(serving_run):
    request, result = await _admit(serving_run)
    assert result is None
    raw_by_field = {
        **request,
        "ordinary_fhir_overlay_job": list(request.admitted_job),
        "admitted_job": list(request.admitted_job),
    }
    assert source.admitted_overlay_job(SPEC, raw_by_field) is None
    assert workers._ensure_spec(SPEC, raw_by_field)["status"] == "failed"
