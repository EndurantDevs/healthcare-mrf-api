# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed launch-routing components; no native admission or runtime proof."""

import hashlib
import importlib
import json
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control
from api import control_workers as workers
from api import provider_directory_cms_registry_worker as registry
from process import provider_directory_cms_serving as serving
from process import provider_directory_profile_selection_contract as selection
from process.provider_directory_profile_capacity_attestation_contract import CAPACITY_LEASE_SIGNATURE_ALGORITHM
from process.provider_directory_profile_capacity_runtime_types import CAPACITY_TRUST_ENV
from process.provider_directory_profile_capacity_trust import CAPACITY_TRUST_CONTRACT_ID
from tests.provider_directory_cms_capacity_test_support import cms_guard, sign_guard
from tests.provider_directory_profile_capacity_signing_guard_test_support import _execution_by_field
from tests.test_provider_directory_cms_address import address_build_case as address_build_case
from tests.test_provider_directory_cms_serving import _retained_execution
from tests.test_provider_directory_cms_serving import execution as execution
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _trust

fhir = importlib.import_module("process.provider_directory_fhir")
RUN_ID = "run_" + "a" * 32
IMAGE = "example/registry-worker@sha256:" + "a" * 64
ORDINARY_IMAGE = "example/ordinary-worker@sha256:" + "b" * 64
SPEC = workers._BY_WORKER_CLASS["process.ProviderDirectoryFHIR"]


def _params(execution):
    params_by_field = _execution_by_field(execution)
    params_by_field["provider_directory_profile_capacity_attestation"] = execution.capacity_attestation
    params_by_field[selection.CMS_CAPACITY_EXECUTION_PARAM] = execution.cms_nonprofile_capacity_attestation
    return params_by_field


def _recorded(execution):
    return {
        "run_id": RUN_ID,
        "importer": "provider-directory-fhir",
        "status": "queued",
        "params": _params(execution),
        "metrics": {
            "queue": SPEC.queue,
            "function": "control_single_job_start",
            "job_id": "provider_directory_start_" + RUN_ID,
        },
    }


def _public_trust_json():
    """Enroll the existing synthetic public key through the actual trust parser."""
    trust_by_field = asdict(_trust())
    for key_by_field in trust_by_field["keys"]:
        key_by_field["public_key_hex"] = key_by_field.pop("public_key").hex()
    trust_by_field.update(
        contract_id=CAPACITY_TRUST_CONTRACT_ID, signature_algorithm=CAPACITY_LEASE_SIGNATURE_ALGORITHM
    )
    return json.dumps(trust_by_field)


@pytest.fixture
def signed_case(monkeypatch, execution, address_build_case):
    """Reuse real signed geometry and the actual retained address-input factory."""
    _factory_fhir, retained_execution, _plan, _snapshot, _factory, policy = _retained_execution(
        address_build_case, execution, None
    )
    monkeypatch.setenv(CAPACITY_TRUST_ENV, _public_trust_json())
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", policy["source_pin"]["schema_name"])
    monkeypatch.setenv(registry.IMAGE_ENV, IMAGE)
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_clock", AsyncMock(return_value=VALIDATION_TIME))
    return _recorded(retained_execution), policy


async def _admitted(recorded):
    """Host-only tuple binding follows real signature verification, without claiming SQL admission."""
    launch = await registry.admit_cms_registry_worker_launch(recorded)
    request = workers._AdmittedWorkerRequest(
        importer=recorded["importer"], run_id=recorded["run_id"], job_id=recorded["metrics"]["job_id"]
    )
    workers._bind_admitted_job(request, recorded)
    request.cms_registry_launch = launch
    return request


def _secrets(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_ENV_JSON",
        json.dumps(
            [
                {"name": "ORDINARY_TOKEN", "secretName": "synthetic-ordinary", "key": "token"},
                {
                    "name": "PUBLISHER_USER",
                    "secretName": "synthetic-publisher",
                    "key": "username",
                    "workerClasses": [SPEC.worker_class],
                    "importers": ["provider-directory-fhir"],
                    "purposes": [registry.PURPOSE],
                },
            ]
        ),
    )


def _kubernetes(monkeypatch, tmp_path):
    """Configure synthetic transport paths; requests never leave this test process."""
    token_path = tmp_path / "transport-token"
    token_path.write_text("synthetic")
    monkeypatch.setattr(workers, "_K8S_API_TOKEN", token_path)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "synthetic")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_NAMESPACE", "synthetic")
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", ORDINARY_IMAGE)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON", "[]")


@pytest.mark.asyncio
async def test_signed_policy_selects_immutable_launch(signed_case):
    recorded, policy = signed_case
    launch = await registry.admit_cms_registry_worker_launch(recorded)
    encoded = json.dumps(policy, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    assert launch == registry.CMSRegistryWorkerLaunch(RUN_ID, IMAGE, hashlib.sha256(encoded).hexdigest())
    request = await _admitted(recorded)
    assert registry.authorized_cms_registry_worker_launch(SPEC, request) == launch
    assert request.admitted_job == (
        RUN_ID,
        "provider-directory-fhir",
        SPEC.queue,
        "control_single_job_start",
        "provider_directory_start_" + RUN_ID,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["cms_signature", "profile_signature", "schema", "missing_image", "mutable_image"])
async def test_signed_launch_refuses_drift(monkeypatch, signed_case, damage):
    recorded, _policy = signed_case
    if damage == "profile_signature":
        current = selection.validated_profile_execution(recorded["params"])
        recorded["params"] = _params_from_changed_pair(current)
    elif damage.endswith("signature"):
        name = (
            selection.CMS_CAPACITY_EXECUTION_PARAM
            if damage == "cms_signature"
            else "provider_directory_profile_capacity_attestation"
        )
        recorded["params"][name]["signature"] = "A" * 86
    elif damage == "schema":
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "other_schema")
    else:
        monkeypatch.setenv(registry.IMAGE_ENV, "" if damage == "missing_image" else "example/registry-worker:latest")
    with pytest.raises(ValueError, match="invalid_signature|admission_unavailable"):
        await registry.admit_cms_registry_worker_launch(recorded)


def _params_from_changed_pair(current):
    """Re-sign the CMS envelope so rejection independently reaches the paired signature."""
    from process.provider_directory_profile_capacity_attestation_contract import (
        CAPACITY_LEASE_DIGEST_DOMAIN,
        _domain_hash,
    )

    pair = deepcopy(current.capacity_attestation)
    pair["signature"] = "A" * 86
    envelope, original_plan = serving._signed_plan(current)
    plan = replace(original_plan, paired_profile_lease_digest=_domain_hash(CAPACITY_LEASE_DIGEST_DOMAIN, pair))
    original_guard = envelope["lease"]["signing_preflight_guard"]
    policy = original_guard["healthcare_request"]["cms_nonprofile_admission"]["registry_source_retention"]
    guard = cms_guard(plan, paired_envelope=pair)
    for name in ("healthcare_request", "control_plane_request"):
        guard[name]["cms_nonprofile_admission"]["registry_source_retention"] = deepcopy(policy)
    from tests.provider_directory_cms_capacity_test_support import rehash_guard

    rehash_guard(guard)
    return _params(
        replace(current, capacity_attestation=pair, cms_nonprofile_capacity_attestation=sign_guard(guard, cms=True))
    )


@pytest.mark.asyncio
async def test_missing_policy_requires_historical_image_bypass(monkeypatch, execution, signed_case):
    historical = _recorded(execution)
    with pytest.raises(ValueError, match="admission_unavailable"):
        await registry.admit_cms_registry_worker_launch(historical)
    monkeypatch.delenv(registry.IMAGE_ENV)
    assert await registry.admit_cms_registry_worker_launch(historical) is None
    assert await registry.admit_cms_registry_worker_launch({"importer": "npi"}) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["run_id", "importer", "queue", "function", "job_id", "requested_run", "requested_job"]
)
async def test_launch_requires_exact_admitted_coordinates(signed_case, field):
    recorded, _policy = signed_case
    request = await _admitted(recorded)
    if field.startswith("requested"):
        request[{"requested_run": "run_id", "requested_job": "job_id"}[field]] = "different"
    else:
        identity_values = list(request.admitted_job)
        identity_values[{"run_id": 0, "importer": 1, "queue": 2, "function": 3, "job_id": 4}[field]] = "different"
        request.admitted_job = tuple(identity_values)
    with pytest.raises(ValueError, match="admission_unavailable"):
        registry.authorized_cms_registry_worker_launch(SPEC, request)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["requested_importer", "foreign_job"])
async def test_launch_refuses_foreign_request_targets(signed_case, drift):
    recorded, _policy = signed_case
    request = await _admitted(recorded)
    if drift == "requested_importer":
        request["importer"] = "npi"
    else:
        request["job_id"] = "foreign_" + RUN_ID
        request.admitted_job = (*request.admitted_job[:4], request["job_id"])
    with pytest.raises(ValueError, match="admission_unavailable"):
        registry.authorized_cms_registry_worker_launch(SPEC, request)


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["list", "short", "long", "missing", "wrong_capability"])
async def test_launch_admission_has_closed_shape(signed_case, shape):
    recorded, _policy = signed_case
    request = await _admitted(recorded)
    if shape == "wrong_capability":
        request.cms_registry_launch = asdict(request.cms_registry_launch)
    else:
        identity_by_shape = {
            "list": list(request.admitted_job),
            "short": request.admitted_job[:4],
            "long": (*request.admitted_job, "extra"),
            "missing": None,
        }
        request.admitted_job = identity_by_shape[shape]
    with pytest.raises(ValueError, match="admission_unavailable"):
        registry.authorized_cms_registry_worker_launch(SPEC, request)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["worker_class", "queue", "role"])
async def test_launch_requires_fixed_worker_spec(signed_case, field):
    recorded, _policy = signed_case
    request = await _admitted(recorded)
    changed_spec = replace(SPEC, **{field: "other"})
    with pytest.raises(ValueError, match="admission_unavailable"):
        registry.authorized_cms_registry_worker_launch(changed_spec, request)


@pytest.mark.asyncio
async def test_raw_json_cannot_select_purpose(monkeypatch, signed_case):
    recorded, _policy = signed_case
    _secrets(monkeypatch)
    admitted = await _admitted(recorded)
    raw_request_dict = {
        **admitted,
        "admitted_job": list(admitted.admitted_job),
        "cms_registry_launch": asdict(admitted.cms_registry_launch),
    }
    assert registry.authorized_cms_registry_worker_launch(SPEC, raw_request_dict) is None
    environment, _run = workers._worker_job_environment(SPEC, json.loads(json.dumps(raw_request_dict)))
    assert "PUBLISHER_USER" not in {entry["name"] for entry in environment}
    manifest = workers._worker_job_manifest(SPEC, raw_request_dict, ORDINARY_IMAGE)
    assert "healthporta.com/worker-purpose" not in manifest["metadata"]["labels"]
    assert manifest["spec"]["template"]["spec"]["securityContext"]["runAsUser"] == 65534


@pytest.mark.asyncio
async def test_manifest_has_fixed_purpose_identity(monkeypatch, signed_case):
    recorded, _policy = signed_case
    _secrets(monkeypatch)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_NAME", "synthetic-work")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "/work")
    request = await _admitted(recorded)
    manifest = workers._worker_job_manifest(SPEC, request, ORDINARY_IMAGE)
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert container["image"] == IMAGE
    assert container["command"] == ["/usr/local/bin/cms-registry-source-worker"]
    assert container["workingDir"] == "/app"
    assert pod["securityContext"]["runAsUser"] == pod["securityContext"]["runAsGroup"] == 10001
    assert pod["securityContext"]["fsGroup"] == 10001
    assert pod["automountServiceAccountToken"] is False
    labels = manifest["metadata"]["labels"]
    assert labels["healthporta.com/worker-purpose"] == registry.PURPOSE
    assert labels["healthporta.com/cms-registry-policy-hash"] == request.cms_registry_launch.policy_sha256[:32]
    assert manifest["spec"]["template"]["metadata"]["labels"] == labels
    environment_by_name = {entry["name"]: entry for entry in container["env"]}
    assert environment_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]["value"] == recorded["metrics"]["job_id"]
    assert environment_by_name["PUBLISHER_USER"]["valueFrom"] == {
        "secretKeyRef": {"name": "synthetic-publisher", "key": "username"}
    }
    assert "ORDINARY_TOKEN" in environment_by_name


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", [None, [], ["other"], [registry.PURPOSE, "other"]])
async def test_purpose_secret_selector_is_closed(signed_case, purpose):
    recorded, _policy = signed_case
    request = await _admitted(recorded)
    selector_by_field = {
        "purposes": purpose,
        "importers": ["provider-directory-fhir"],
        "workerClasses": [SPEC.worker_class],
    }
    with pytest.raises(ValueError, match="selector is invalid"):
        registry.is_cms_registry_secret_selected(selector_by_field, SPEC.worker_class, request)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["image", "purpose", "command", "policy", "template", "uid", "gid", "nonroot", "group"]
)
async def test_existing_mismatch_is_never_deleted(monkeypatch, tmp_path, signed_case, damage):
    recorded, _policy = signed_case
    request = await _admitted(recorded)
    _kubernetes(monkeypatch, tmp_path)
    existing = workers._worker_job_manifest(SPEC, request, IMAGE)
    existing["status"] = {"succeeded": 1}
    if damage in {"image", "command"}:
        existing["spec"]["template"]["spec"]["containers"][0][damage] = (
            ORDINARY_IMAGE if damage == "image" else ["other"]
        )
    elif damage in {"purpose", "policy"}:
        field = "healthporta.com/worker-purpose" if damage == "purpose" else "healthporta.com/cms-registry-policy-hash"
        existing["metadata"]["labels"][field] = "other"
    elif damage == "template":
        existing["spec"]["template"]["metadata"]["labels"] = {"healthporta.com/worker-purpose": "other"}
    else:
        field = {"uid": "runAsUser", "gid": "runAsGroup", "nonroot": "runAsNonRoot", "group": "fsGroup"}[damage]
        existing["spec"]["template"]["spec"]["securityContext"][field] = False if damage == "nonroot" else 65534
    transport_calls = []

    def transport(method, path, body=None):
        transport_calls.append(method)
        assert method == "GET"
        return {"items": [existing]}

    monkeypatch.setattr(workers, "_kubernetes_request", transport)
    response = workers.ensure_worker(request)
    assert response["status"] == "failed"
    assert response["items"][0]["job_status"] == "purpose_mismatch"
    assert transport_calls == ["GET"]


@pytest.mark.asyncio
async def test_process_launcher_refuses_owner_purpose(monkeypatch, signed_case):
    recorded, _policy = signed_case
    request = await _admitted(recorded)
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "process")
    response = workers.ensure_worker(request)
    assert response["status"] == "failed"
    assert "isolated launcher" in response["items"][0]["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", [False, True])
async def test_post_conflict_rechecks_purpose(monkeypatch, tmp_path, signed_case, mismatch):
    recorded, _policy = signed_case
    request = await _admitted(recorded)
    _kubernetes(monkeypatch, tmp_path)
    existing = workers._worker_job_manifest(SPEC, request, IMAGE)
    existing["status"] = {"active": 1}
    if mismatch:
        existing["metadata"]["labels"]["healthporta.com/worker-purpose"] = "other"
    transport_calls = []

    def transport(method, path, body=None):
        transport_calls.append(method)
        if method == "POST":
            raise workers._KubernetesApiError(409, "synthetic conflict")
        assert method == "GET"
        return {"items": [] if len(transport_calls) == 1 else [existing]}

    monkeypatch.setattr(workers, "_kubernetes_request", transport)
    response = workers.ensure_worker(request)
    assert response["status"] == ("failed" if mismatch else "already_running")
    assert transport_calls == ["GET", "POST", "GET"]


@pytest.mark.asyncio
async def test_plain_json_route_derives_signed_owner_launch(monkeypatch, tmp_path, signed_case):
    """The real route and guard bind ordinary JSON to the stored signed job."""
    recorded, policy = signed_case
    _kubernetes(monkeypatch, tmp_path)
    _secrets(monkeypatch)
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control-token")
    admissions = []
    manifests = []

    async def admit(**coordinates):
        admissions.append(coordinates)
        return recorded

    def transport(method, path, body=None):
        if method == "GET":
            return {"items": []}
        assert method == "POST" and path.endswith("/jobs")
        manifests.append(body)
        return body

    monkeypatch.setattr(workers, "admit_existing_outer_run_action", admit)
    monkeypatch.setattr(workers, "_kubernetes_request", transport)
    request_by_field = json.loads(
        json.dumps(
            {
                "importer": recorded["importer"],
                "run_id": recorded["run_id"],
                "job_id": recorded["metrics"]["job_id"],
                "cms_registry_launch": {"image": ORDINARY_IMAGE},
                "admitted_job": ["caller-selected"],
            }
        )
    )
    assert type(request_by_field) is dict
    request = SimpleNamespace(json=request_by_field, headers={"Authorization": "Bearer synthetic-control-token"})
    response = await control.control_ensure_worker(request)
    response_by_field = json.loads(response.body)
    assert response.status == 202 and response_by_field["status"] == "started"
    assert len(admissions) == len(manifests) == 1
    assert admissions[0]["run_id"] == recorded["run_id"]
    assert admissions[0]["expected_source_file_import_id"] is None
    assert admissions[0]["worker_selection"].allowed_importers == frozenset({recorded["importer"]})
    manifest = manifests[0]
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    encoded = json.dumps(policy, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    assert container["image"] == IMAGE and container["command"] == registry.COMMAND
    assert manifest["metadata"]["labels"]["healthporta.com/worker-purpose"] == registry.PURPOSE
    assert (
        manifest["metadata"]["labels"]["healthporta.com/cms-registry-policy-hash"]
        == hashlib.sha256(encoded).hexdigest()[:32]
    )
    assert pod["securityContext"]["runAsUser"] == pod["securityContext"]["runAsGroup"] == 10001
    environment_by_name = {entry["name"]: entry for entry in container["env"]}
    assert environment_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]["value"] == recorded["metrics"]["job_id"]
    assert environment_by_name["PUBLISHER_USER"]["valueFrom"]["secretKeyRef"]["name"] == "synthetic-publisher"
    assert type(request_by_field) is dict and request_by_field["admitted_job"] == ["caller-selected"]


@pytest.mark.asyncio
async def test_plain_json_route_refuses_invalid_stored_signature(monkeypatch, signed_case):
    """A real authenticated request cannot skip validation of its stored CMS lease."""
    recorded, _policy = signed_case
    name = selection.CMS_CAPACITY_EXECUTION_PARAM
    recorded["params"][name] = deepcopy(recorded["params"][name])
    recorded["params"][name]["signature"] = "A" * 86
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control-token")
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", AsyncMock(return_value=recorded))

    def fail_launch(_payload):
        raise AssertionError("invalid stored signature must fail before launch")

    monkeypatch.setattr(workers, "ensure_worker", fail_launch)
    request = SimpleNamespace(
        json={"importer": recorded["importer"], "run_id": recorded["run_id"], "job_id": recorded["metrics"]["job_id"]},
        headers={"Authorization": "Bearer synthetic-control-token"},
    )
    response = await control.control_ensure_worker(request)
    result = json.loads(response.body)
    assert response.status == 202 and result["status"] == "failed" and result["items"] == []
    assert result["message"] == "CMS registry worker admission is unavailable"
