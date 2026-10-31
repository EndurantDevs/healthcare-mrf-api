# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise fixed worker routing without claiming native admission or deployment."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_workers as workers
from api import provider_directory_source_worker as source

RUN_ID = "run_" + "a" * 32
JOB_ID = "provider_directory_start_" + RUN_ID
IMAGE = "example/source-worker@sha256:" + "c" * 64
ORDINARY_IMAGE = "example/ordinary-worker@sha256:" + "d" * 64
SPEC = workers._BY_WORKER_CLASS["process.ProviderDirectoryFHIR"]


def _overlay_run():
    """Use the same closed persisted descriptor as the ordinary execution regression."""
    from tests.test_provider_directory_artifact_followup import _overlay_followup_params

    return {**_recorded(), "params": _overlay_followup_params()}


@pytest.fixture
async def overlay_admitted(monkeypatch):
    """Classify a persisted run without any Source image or manifest configuration."""
    monkeypatch.delenv(source.IMAGE_ENV, raising=False)
    monkeypatch.delenv(source.REFERENCE_IMAGE_ENV, raising=False)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    workers._bind_admitted_job(request, _overlay_run())
    assert await workers._admit_source_worker_launches(request, _overlay_run()) is None
    return request


@pytest.fixture
def source_runtime_mount(monkeypatch):
    """Retain the configured Source-only mount alongside an ordinary credential mount."""
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(
            [
                {
                    "name": "source-runtime",
                    "secretName": "synthetic-source-runtime",
                    "mountPath": "/synthetic/source-policy",
                    "subPath": "policy.json",
                    "defaultMode": 0o444,
                    "workerClasses": [SPEC.worker_class],
                    "purposes": [source.PURPOSE],
                    "importers": ["provider-directory-fhir"],
                },
                {"name": "ordinary-credentials", "secretName": "ordinary-credentials", "mountPath": "/credentials"},
            ]
        ),
    )


@pytest.mark.asyncio
async def test_overlay_ensure_uses_native_ordinary_job(monkeypatch, secret_selection, source_runtime_mount):
    """The real ensure path chooses ordinary code only from persisted overlay parameters."""
    monkeypatch.delenv(source.IMAGE_ENV, raising=False)
    monkeypatch.delenv(source.REFERENCE_IMAGE_ENV, raising=False)
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", ORDINARY_IMAGE)
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", AsyncMock(return_value=_overlay_run()))
    monkeypatch.setattr(workers, "acquire_control_run_worker_action_lock", AsyncMock())
    api_calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: api_calls.append(args) or {"items": []})

    @asynccontextmanager
    async def connection():
        """Keep the actual exact-run admission path while replacing connection I/O."""
        yield object()

    monkeypatch.setattr(workers.db, "acquire", connection)
    response_by_field = await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"})
    assert response_by_field["status"] == "started"
    posted_jobs = [call[2] for call in api_calls if call[0] == "POST"]
    assert len(posted_jobs) == 1
    pod_by_field = posted_jobs[0]["spec"]["template"]["spec"]
    container_by_field = pod_by_field["containers"][0]
    assert container_by_field["image"] == ORDINARY_IMAGE
    assert container_by_field["command"] == workers._worker_command(workers._worker_python(), SPEC)
    assert pod_by_field["securityContext"]["runAsUser"] == 65534
    assert "healthporta.com/worker-purpose" not in posted_jobs[0]["metadata"]["labels"]
    environment_by_name = {entry["name"]: entry for entry in container_by_field["env"]}
    assert environment_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]["value"] == JOB_ID
    assert "SOURCE_USER" not in environment_by_name
    assert [entry["name"] for entry in pod_by_field["volumes"]] == ["ordinary-credentials"]


@pytest.mark.parametrize("field", ["run_id", "importer", "job_id"])
def test_overlay_target_rejects_request_drift(overlay_admitted, field):
    """The request cannot redirect the persisted overlay classification."""
    overlay_admitted[field] = "different"
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._single_job_worker_target(SPEC, overlay_admitted)


@pytest.mark.parametrize("index", range(5))
def test_overlay_target_rejects_native_drift(overlay_admitted, index):
    """Every observed coordinate remains required by ordinary routing."""
    identity_values = list(overlay_admitted.admitted_job)
    identity_values[index] = "different"
    overlay_admitted.admitted_job = tuple(identity_values)
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._single_job_worker_target(SPEC, overlay_admitted)


def test_overlay_serialization_cannot_admit_job(overlay_admitted):
    """Neither JSON markers nor a caller-supplied descriptor create admission."""
    raw_by_field = {
        **overlay_admitted,
        "ordinary_fhir_overlay_job": list(overlay_admitted.admitted_job),
        "admitted_job": list(overlay_admitted.admitted_job),
        "params": _overlay_run()["params"],
    }
    assert source.admitted_overlay_job(SPEC, raw_by_field) is None
    assert workers._ensure_spec(SPEC, raw_by_field)["status"] == "failed"


@pytest.mark.parametrize(
    "extra_by_name",
    [
        {"import_resources": True},
        {"probe": 0},
        {"publish_artifacts_only": 1},
        {"provider_directory_dataset_followup_version": True},
        {"provider_directory_dataset_followup_attempt": False},
        {"provider_directory_dataset_followup_stage": "profile"},
        {"publish_artifacts_targets": "profile"},
        {"serving_recovery_generation": None},
        {"run_id": RUN_ID},
        {"test_mode": False},
    ],
)
@pytest.mark.asyncio
async def test_mixed_overlay_never_selects_ordinary(monkeypatch, extra_by_name):
    """Malformed or broader persisted operations cannot use the overlay exception."""
    monkeypatch.delenv(source.IMAGE_ENV, raising=False)
    monkeypatch.delenv(source.REFERENCE_IMAGE_ENV, raising=False)
    native_run_by_field = _overlay_run()
    native_run_by_field["params"].update(extra_by_name)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    workers._bind_admitted_job(request, native_run_by_field)
    assert (await workers._admit_source_worker_launches(request, native_run_by_field))["status"] == "failed"
    assert request.ordinary_fhir_overlay_job is None


def test_overlay_uses_no_source_credentials(overlay_admitted, admitted, secret_selection, source_runtime_mount):
    """Only the existing Source capability selects the configured private mount."""
    assert workers._worker_job_secret_env(SPEC.worker_class, launch_request=overlay_admitted) == []
    assert [entry["volume"]["name"] for entry in workers._worker_job_secret_volumes(SPEC)] == ["ordinary-credentials"]
    source_job_by_field = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    assert source.is_fhir_source_job_matching(source_job_by_field, admitted.fhir_source_launch)
    assert [entry["name"] for entry in source_job_by_field["spec"]["template"]["spec"]["volumes"]] == [
        "source-runtime",
        "ordinary-credentials",
    ]


@pytest.mark.parametrize("observed_job", [{}, {"spec": None}])
def test_source_reuse_refuses_malformed_observation(admitted, secret_selection, observed_job):
    """An incomplete API response cannot establish the existing job's identity."""
    launch = admitted.fhir_source_launch
    expected_job_by_field = workers._worker_job_manifest(SPEC, admitted, IMAGE)
    assert source.is_fhir_source_job_matching(expected_job_by_field, launch)
    assert source.is_fhir_source_job_matching(observed_job, launch) is False


def test_source_reuse_requires_live_launch_capability(admitted, secret_selection):
    """Serialized request fields cannot substitute for the admitted capability."""
    launch = admitted.fhir_source_launch
    job_by_field = workers._worker_job_manifest(SPEC, admitted, IMAGE)
    assert source.is_fhir_source_job_matching(job_by_field, launch)
    assert source.is_fhir_source_job_matching(job_by_field, asdict(launch)) is False


@pytest.mark.parametrize("field", ["purposes", "importers", "workerClasses"])
def test_source_mount_rejects_broader_scope(admitted, monkeypatch, source_runtime_mount, field):
    """A mount cannot widen the existing exact Source credential selector."""
    setting = "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON"
    selectors = json.loads(workers.os.environ[setting])
    selectors[0][field].append("other")
    monkeypatch.setenv(setting, json.dumps(selectors))
    with pytest.raises(ValueError, match="selector is invalid"):
        workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)


def _change_overlay_job(job_by_field, drift):
    """Mutate one admitted runtime boundary at a time."""
    pod_by_field = job_by_field["spec"]["template"]["spec"]
    container_by_field = pod_by_field["containers"][0]
    if drift in {"image", "command"}:
        container_by_field[drift] = IMAGE if drift == "image" else source.COMMAND
    if drift == "target":
        _change_target(container_by_field, "target")
    if drift == "source_mount":
        pod_by_field["volumes"].append({"name": "source", "secret": {"secretName": "synthetic-source-runtime"}})
    if drift == "source_env":
        container_by_field["env"].append(
            {
                "name": "SOURCE_DSN",
                "valueFrom": {"secretKeyRef": {"name": "synthetic-source-runtime", "key": "publisher"}},
            }
        )
    if drift == "purpose":
        job_by_field["metadata"]["labels"]["healthporta.com/worker-purpose"] = source.PURPOSE
    if drift == "service_account":
        pod_by_field["serviceAccountName"] = "other-account"


@pytest.mark.parametrize(
    "drift", [None, "image", "command", "target", "source_mount", "source_env", "purpose", "service_account"]
)
def test_overlay_job_reuse_keeps_native_runtime(overlay_admitted, monkeypatch, source_runtime_mount, drift):
    """Existing Jobs must retain ordinary code, credentials and the exact queued target."""
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", ORDINARY_IMAGE)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SERVICE_ACCOUNT", "synthetic-worker")
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_launcher_mode", lambda: "kubernetes")
    job_by_field = workers._worker_job_manifest(SPEC, overlay_admitted, ORDINARY_IMAGE)
    job_by_field["status"] = {"active": 1}
    _change_overlay_job(job_by_field, drift)
    api_calls = []
    monkeypatch.setattr(
        workers, "_kubernetes_request", lambda *args: api_calls.append(args) or {"items": [job_by_field]}
    )
    response_by_field = workers._ensure_spec(SPEC, overlay_admitted)
    assert response_by_field["status"] == ("already_running" if drift is None else "failed")
    assert all(call[0] == "GET" for call in api_calls)


@pytest.mark.asyncio
async def test_overlay_request_cannot_reclassify_acquisition(admitted):
    """Caller parameters never replace the actual stored operation."""
    admitted["params"] = _overlay_run()["params"]
    assert await workers._admit_source_worker_launches(admitted, _recorded()) is None
    assert source.admitted_overlay_job(SPEC, admitted) is None
    assert source.authorized_fhir_source_worker_launch(SPEC, admitted).job_id == JOB_ID


def _recorded():
    return {
        "run_id": RUN_ID,
        "importer": "provider-directory-fhir",
        "status": "queued",
        "metrics": {"queue": SPEC.queue, "function": "control_single_job_start", "job_id": JOB_ID},
    }


@pytest.fixture
def admitted(monkeypatch):
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    recorded = _recorded()
    workers._bind_admitted_job(request, recorded)
    request.fhir_source_launch = source.admit_fhir_source_worker_launch(recorded)
    return request


@pytest.fixture
def secret_selection(monkeypatch):
    selection_by_field = {
        "name": "SOURCE_USER",
        "secretName": "synthetic-publisher",
        "key": "username",
        "workerClasses": [SPEC.worker_class],
        "importers": ["provider-directory-fhir"],
        "purposes": [source.PURPOSE],
    }
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", json.dumps([selection_by_field]))
    return selection_by_field


def test_admission_keeps_actual_observed_queue_id(admitted):
    assert admitted.fhir_source_launch == source.FHIRSourceWorkerLaunch(RUN_ID, JOB_ID, IMAGE)
    assert source.authorized_fhir_source_worker_launch(SPEC, admitted) == admitted.fhir_source_launch
    assert workers._single_job_worker_target(SPEC, admitted) == JOB_ID
    assert workers._worker_command("python", SPEC)[-2:] == ["worker-once", "process.ProviderDirectoryFHIR"]


@pytest.mark.parametrize("image", ["", "example/source-worker:latest", IMAGE[:-1] + "z"])
def test_fhir_admission_requires_immutable_image(monkeypatch, image):
    monkeypatch.setenv(source.IMAGE_ENV, image)
    monkeypatch.setenv(source.REFERENCE_IMAGE_ENV, ORDINARY_IMAGE)
    with pytest.raises(ValueError, match="fhir_source_worker_admission_unavailable"):
        source.admit_fhir_source_worker_launch(_recorded())
    assert source.admit_fhir_source_worker_launch({"importer": "nucc"}) is None


def test_fhir_image_setting_takes_precedence_without_changing_reference(monkeypatch):
    from api import reference_source_worker as reference

    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    monkeypatch.setenv(reference.IMAGE_ENV, ORDINARY_IMAGE)
    assert source.admit_fhir_source_worker_launch(_recorded()).image == IMAGE
    reference_run_by_field = {
        "run_id": RUN_ID,
        "importer": "geo",
        "metrics": {"queue": "arq:Geo", "function": "control_single_job_start", "job_id": "geo-job"},
    }
    assert reference.admit_reference_source_worker_launch(reference_run_by_field).image == ORDINARY_IMAGE


def test_absent_fhir_image_setting_retains_reference_fallback(monkeypatch):
    monkeypatch.delenv(source.IMAGE_ENV, raising=False)
    monkeypatch.setenv(source.REFERENCE_IMAGE_ENV, IMAGE)
    assert source.admit_fhir_source_worker_launch(_recorded()).image == IMAGE


def _source_route_config():
    return {
        "root_run_id": "run_" + "b" * 32,
        "source_id": "synthetic-source",
        "endpoint_scope": "https://example.test/fhir",
        "image": ORDINARY_IMAGE,
        "secret_name": "synthetic-successor",
    }


def _source_route_run():
    return {
        **_recorded(),
        "params": {
            "provider_directory_pagination_root_run_id": "run_" + "b" * 32,
            "source_ids": ["synthetic-source"],
            "provider_directory_endpoint_scope": "https://example.test/fhir",
            "retry_of_run_id": "run_" + "c" * 32,
        },
    }


def _both_source_secrets(monkeypatch):
    """Configure distinct default and route-only synthetic credentials."""
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_ENV_JSON",
        json.dumps(
            [
                {
                    "name": "SOURCE_USER",
                    "secretName": secret,
                    "key": "username",
                    "workerClasses": [SPEC.worker_class],
                    "purposes": [source.PURPOSE],
                    "importers": ["provider-directory-fhir"],
                }
                for secret in ("synthetic-default", "synthetic-successor")
            ]
            + [
                {
                    "name": name,
                    "secretName": secret,
                    "key": "dsn",
                    "workerClasses": [SPEC.worker_class],
                    "purposes": [source.PURPOSE],
                    "importers": ["provider-directory-fhir"],
                    "sharedForRoutes": True,
                }
                for name, secret in (
                    ("SHARED_DATABASE_DSN", "synthetic-database"),
                    ("SHARED_CATALOG_DSN", "synthetic-catalog"),
                )
            ]
        ),
    )
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(
            [
                {
                    "name": "source-policy",
                    "secretName": secret,
                    "mountPath": "/source-policy",
                    "workerClasses": [SPEC.worker_class],
                    "purposes": [source.PURPOSE],
                    "importers": ["provider-directory-fhir"],
                }
                for secret in ("synthetic-default", "synthetic-successor")
            ]
        ),
    )


def test_exact_server_route_selects_successor_image_and_secret(monkeypatch):
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_source_route_config()]))
    _both_source_secrets(monkeypatch)
    recorded = _source_route_run()
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    workers._bind_admitted_job(request, recorded)
    request.fhir_source_launch = source.admit_fhir_source_worker_launch(recorded)
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    pod = job["spec"]["template"]["spec"]
    assert pod["containers"][0]["image"] == ORDINARY_IMAGE
    assert [
        entry["valueFrom"]["secretKeyRef"]["name"]
        for entry in pod["containers"][0]["env"]
        if entry["name"] == "SOURCE_USER"
    ] == ["synthetic-successor"]
    assert {
        entry["name"]: entry["valueFrom"]["secretKeyRef"]["name"]
        for entry in pod["containers"][0]["env"]
        if entry["name"] in {"SHARED_DATABASE_DSN", "SHARED_CATALOG_DSN"}
    } == {
        "SHARED_DATABASE_DSN": "synthetic-database",
        "SHARED_CATALOG_DSN": "synthetic-catalog",
    }
    assert [entry["secret"]["secretName"] for entry in pod["volumes"]] == ["synthetic-successor"]
    assert job["metadata"]["annotations"]["healthporta.com/source-root-run-id"] == _source_route_config()["root_run_id"]
    assert source.is_fhir_job_set_matching(SPEC, request, [job])
    job["spec"]["template"]["spec"]["containers"][0]["env"][-1]["valueFrom"]["secretKeyRef"]["name"] = (
        "synthetic-default"
    )
    assert not source.is_fhir_job_set_matching(SPEC, request, [job])


@pytest.mark.parametrize(
    ("secret_name", "shared_marker"),
    [("synthetic-successor", True), (" synthetic-successor ", True), ("synthetic-successor", "true")],
)
def test_route_secret_cannot_claim_shared_selection(monkeypatch, secret_name, shared_marker):
    """A route-owned credential cannot impersonate a shared selector."""
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_source_route_config()]))
    selection_by_field = {
        "name": "ROUTE_USER",
        "secretName": secret_name,
        "key": "username",
        "workerClasses": [SPEC.worker_class],
        "purposes": [source.PURPOSE],
        "importers": ["provider-directory-fhir"],
        "sharedForRoutes": shared_marker,
    }
    launch = source.FHIRSourceWorkerLaunch(RUN_ID, JOB_ID, IMAGE, secret_name="synthetic-successor")
    with pytest.raises(ValueError, match="shared secret selector is invalid"):
        source._is_launch_secret_selected(selection_by_field, launch, allow_shared=True)


@pytest.mark.parametrize("drift", ["malformed_json", "wrong_container", "missing_field", "duplicate_route"])
def test_invalid_server_route_inventory_refuses_worker_launch(monkeypatch, drift):
    """Bad trusted route data must never select an image or credential."""
    route = _source_route_config()
    routes = [route]
    if drift == "malformed_json":
        serialized = "{"
    elif drift == "wrong_container":
        serialized = json.dumps(route)
    else:
        if drift == "missing_field":
            del route["secret_name"]
        else:
            routes.append(dict(route))
        serialized = json.dumps(routes)
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    monkeypatch.setenv(source.ROUTES_ENV, serialized)
    with pytest.raises(ValueError, match="fhir_source_worker_route_invalid"):
        source.admit_fhir_source_worker_launch(_source_route_run())


@pytest.mark.parametrize("field", ["source_ids", "provider_directory_endpoint_scope"])
def test_exact_route_refuses_other_same_root_identity(monkeypatch, field):
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_source_route_config()]))
    recorded = _source_route_run()
    recorded["params"][field] = "changed"
    with pytest.raises(ValueError, match="fhir_source_worker_route_changed"):
        source.admit_fhir_source_worker_launch(recorded)


@pytest.mark.asyncio
async def test_route_keeps_image_and_secret_across_valid_later_retries(monkeypatch):
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_source_route_config()]))
    recorded = _source_route_run()
    later_parent = "run_" + "d" * 32
    recorded["params"]["retry_of_run_id"] = later_parent
    launch = source.admit_fhir_source_worker_launch(recorded)
    assert launch.image == ORDINARY_IMAGE and launch.secret_name == "synthetic-successor"
    root = _source_route_config()["root_run_id"]
    current_params = recorded["params"]
    lineage_rows = [
        {
            "run_id": RUN_ID,
            "importer": "provider-directory-fhir",
            "status": "queued",
            "retry_of_run_id": later_parent,
            "params": current_params,
        },
        {
            "run_id": later_parent,
            "importer": "provider-directory-fhir",
            "status": "failed",
            "retry_of_run_id": "run_" + "c" * 32,
            "params": {**current_params, "retry_of_run_id": "run_" + "c" * 32},
        },
        {
            "run_id": "run_" + "c" * 32,
            "importer": "provider-directory-fhir",
            "status": "failed",
            "retry_of_run_id": root,
            "params": {**current_params, "retry_of_run_id": root},
        },
        {
            "run_id": root,
            "importer": "provider-directory-fhir",
            "status": "failed",
            "retry_of_run_id": None,
            "params": {**current_params, "retry_of_run_id": None},
        },
    ]
    connection = SimpleNamespace(all=AsyncMock(return_value=lineage_rows))
    await source.require_fhir_source_route_ancestry(connection, launch)
    lineage_rows[1]["params"] = {**lineage_rows[1]["params"], "source_ids": ["other-source"]}
    with pytest.raises(ValueError, match="route_changed"):
        await source.require_fhir_source_route_ancestry(connection, launch)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["missing_parent", "root_not_failed", "child_not_queued", "duplicate_id"])
async def test_route_ancestry_refuses_inconsistent_native_runs(monkeypatch, drift):
    """A matching configured root cannot excuse a broken persisted retry chain."""
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_source_route_config()]))
    recorded = _source_route_run()
    root_id = _source_route_config()["root_run_id"]
    recorded["params"]["retry_of_run_id"] = root_id
    launch = source.admit_fhir_source_worker_launch(recorded)
    lineage_rows = [
        {"run_id": RUN_ID, "status": "queued", "retry_of_run_id": root_id, "params": recorded["params"]},
        {"run_id": root_id, "status": "failed", "retry_of_run_id": None, "params": recorded["params"]},
    ]
    if drift == "missing_parent":
        lineage_rows.pop()
    elif drift == "root_not_failed":
        lineage_rows[1]["status"] = "succeeded"
    elif drift == "child_not_queued":
        lineage_rows[0]["status"] = "failed"
    else:
        lineage_rows.append(dict(lineage_rows[0]))
    connection = SimpleNamespace(all=AsyncMock(return_value=lineage_rows))
    with pytest.raises(ValueError, match="fhir_source_worker_route_changed"):
        await source.require_fhir_source_route_ancestry(connection, launch)


@pytest.mark.asyncio
async def test_guarded_ensure_checks_route_ancestry_before_launch(monkeypatch):
    from api import provider_directory_cms_registry_stop as stop

    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    request.fhir_source_launch = source.FHIRSourceWorkerLaunch(
        RUN_ID,
        JOB_ID,
        IMAGE,
        "run_" + "b" * 32,
        "synthetic-successor",
        "synthetic-source",
        "https://example.test/fhir",
        "run_" + "c" * 32,
    )

    @asynccontextmanager
    async def connection():
        yield object()

    monkeypatch.setattr(workers.db, "acquire", connection)
    monkeypatch.setattr(workers, "acquire_control_run_worker_action_lock", AsyncMock())
    monkeypatch.setattr(workers, "_admit_worker_ensure", AsyncMock(return_value=None))
    verify = AsyncMock(side_effect=ValueError("fhir_source_worker_route_changed"))
    monkeypatch.setattr(source, "require_fhir_source_route_ancestry", verify)
    monkeypatch.setattr(workers, "ensure_worker", lambda request: pytest.fail("worker launched"))
    ensure_result = await stop.guarded_ensure(
        request, run_id=RUN_ID, importer="provider-directory-fhir", selected_specs=[SPEC]
    )
    assert ensure_result["status"] == "failed"
    verify.assert_awaited_once()


def test_unrelated_root_keeps_existing_default_image(monkeypatch):
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_source_route_config()]))
    _both_source_secrets(monkeypatch)
    recorded = _recorded()
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    workers._bind_admitted_job(request, recorded)
    request.fhir_source_launch = source.admit_fhir_source_worker_launch(recorded)
    assert request.fhir_source_launch == source.FHIRSourceWorkerLaunch(RUN_ID, JOB_ID, IMAGE)
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    pod = job["spec"]["template"]["spec"]
    assert pod["containers"][0]["image"] == IMAGE
    assert [
        entry["valueFrom"]["secretKeyRef"]["name"]
        for entry in pod["containers"][0]["env"]
        if entry["name"] == "SOURCE_USER"
    ] == ["synthetic-default"]
    assert [entry["secret"]["secretName"] for entry in pod["volumes"]] == ["synthetic-default"]


@pytest.mark.parametrize("image", ["", "example/source-worker:latest", IMAGE[:-1] + "z"])
def test_absent_fhir_image_setting_rejects_invalid_reference_fallback(monkeypatch, image):
    monkeypatch.delenv(source.IMAGE_ENV, raising=False)
    monkeypatch.setenv(source.REFERENCE_IMAGE_ENV, image)
    with pytest.raises(ValueError, match="fhir_source_worker_admission_unavailable"):
        source.admit_fhir_source_worker_launch(_recorded())


@pytest.mark.parametrize("field", ["queue", "function", "job_id", "run_id", "metrics"])
def test_missing_or_foreign_observed_coordinates_refuse(monkeypatch, field):
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    recorded = _recorded()
    if field in {"run_id", "metrics"}:
        recorded[field] = None
    else:
        recorded["metrics"][field] = "other" if field != "job_id" else " "
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.admit_fhir_source_worker_launch(recorded)


@pytest.mark.parametrize("index", range(5))
def test_each_observed_coordinate_remains_required(admitted, index):
    identity_values = list(admitted.admitted_job)
    identity_values[index] = "different"
    admitted.admitted_job = tuple(identity_values)
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.authorized_fhir_source_worker_launch(SPEC, admitted)


@pytest.mark.parametrize("field", ["run_id", "importer", "job_id"])
def test_conflicting_request_cannot_redirect_native_job(admitted, field):
    admitted[field] = "different"
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._single_job_worker_target(SPEC, admitted)


@pytest.mark.parametrize("field", ["worker_class", "queue", "role", "importers"])
def test_capability_cannot_select_another_worker(admitted, field):
    spec = replace(SPEC, **{field: ("other",) if field == "importers" else "other"})
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.authorized_fhir_source_worker_launch(spec, admitted)


def test_serialized_capability_cannot_select_source_secrets_or_launcher(admitted, secret_selection):
    raw = json.loads(
        json.dumps(
            {
                **admitted,
                "admitted_job": admitted.admitted_job,
                "fhir_source_launch": asdict(admitted.fhir_source_launch),
            }
        )
    )
    assert source.authorized_fhir_source_worker_launch(SPEC, raw) is None
    assert workers._worker_job_secret_env(SPEC.worker_class, launch_request=raw) == []
    assert workers._ensure_spec(SPEC, raw)["status"] == "failed"
    admitted.fhir_source_launch = asdict(admitted.fhir_source_launch)
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.authorized_fhir_source_worker_launch(SPEC, admitted)


@pytest.mark.parametrize("field", ["purposes", "importers", "workerClasses"])
def test_secret_selector_cannot_broaden_source_purpose(admitted, secret_selection, field):
    secret_selection[field].append("other")
    with pytest.raises(ValueError, match="selector is invalid"):
        workers._is_admitted_importer_selected(secret_selection, SPEC.worker_class, admitted)


@pytest.mark.parametrize("has_volume", [False, True])
def test_manifest_keeps_fixed_image_command_actor_and_native_target(
    admitted, secret_selection, monkeypatch, has_volume
):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_NAME", "synthetic-work" if has_volume else "")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "/work" if has_volume else "")
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON", raising=False)
    manifest = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert container["image"] == IMAGE
    assert container["command"] == source.COMMAND
    assert container["workingDir"] == "/app"
    assert pod["securityContext"]["runAsUser"] == pod["securityContext"]["runAsGroup"] == 10001
    assert pod["securityContext"].get("fsGroup") == (65534 if has_volume else None)
    assert pod["securityContext"].get("fsGroupChangePolicy") == ("OnRootMismatch" if has_volume else None)
    assert pod["automountServiceAccountToken"] is False
    env_by_name = {entry["name"]: entry for entry in container["env"]}
    assert env_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]["value"] == JOB_ID
    assert env_by_name["SOURCE_USER"]["valueFrom"]["secretKeyRef"]["name"] == "synthetic-publisher"
    assert source.is_fhir_source_job_matching(manifest, admitted.fhir_source_launch)
    if has_volume:
        pod["securityContext"]["fsGroup"] = 10001
        assert not source.is_fhir_source_job_matching(manifest, admitted.fhir_source_launch)


@pytest.mark.parametrize("has_volume", [False, True])
def test_unadmitted_spec_keeps_shared_identity(has_volume):
    """The source-specific actor override never changes ordinary worker security."""
    pod_spec_dict = {"securityContext": workers._worker_job_pod_security_context(has_group_read_volume=has_volume)}
    original = deepcopy(pod_spec_dict)
    labels_by_key = {}
    source.apply_fhir_source_worker_spec(SPEC, {"run_id": RUN_ID}, labels_by_key, pod_spec_dict)
    assert pod_spec_dict == original and labels_by_key == {}
    assert pod_spec_dict["securityContext"]["runAsUser"] == pod_spec_dict["securityContext"]["runAsGroup"] == 65534


@pytest.mark.parametrize(
    "drift",
    [
        "image",
        "command",
        "actor",
        "purpose",
        "target",
        "duplicate_target",
        "container_actor",
        "container_nonroot",
        "escalation",
        "capabilities",
        "seccomp",
        "supplemental_groups",
        "service_account_token",
        "hostPID",
        "hostIPC",
        "hostNetwork",
    ],
)
def test_existing_job_cannot_substitute_source_identity(admitted, secret_selection, drift):
    manifest = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    if drift in {"image", "command"}:
        container[drift] = ORDINARY_IMAGE if drift == "image" else ["other"]
    elif drift == "actor":
        pod["securityContext"]["runAsUser"] = 65534
    elif drift == "purpose":
        manifest["metadata"]["labels"]["healthporta.com/worker-purpose"] = "other"
    elif drift in {"target", "duplicate_target"}:
        _change_target(container, drift)
    else:
        _change_isolation(pod, container, drift)
    assert not source.is_fhir_source_job_matching(manifest, admitted.fhir_source_launch)


def _change_target(container, drift):
    target = next(entry for entry in container["env"] if entry["name"] == "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID")
    if drift == "target":
        target["value"] = "other"
    else:
        container["env"].append(deepcopy(target))


def _change_isolation(pod, container, drift):
    if drift in {"hostPID", "hostIPC", "hostNetwork"}:
        pod[drift] = True
    elif drift == "service_account_token":
        pod["automountServiceAccountToken"] = True
    elif drift in {"seccomp", "supplemental_groups"}:
        key = "seccompProfile" if drift == "seccomp" else "supplementalGroups"
        pod["securityContext"][key] = {"type": "Unconfined"} if drift == "seccomp" else [0]
    else:
        key, override = {
            "container_actor": ("runAsUser", 0),
            "container_nonroot": ("runAsNonRoot", False),
            "escalation": ("allowPrivilegeEscalation", True),
            "capabilities": ("capabilities", {"add": ["SYS_ADMIN"], "drop": ["ALL"]}),
        }[drift]
        container["securityContext"][key] = override


@pytest.mark.asyncio
async def test_actual_ensure_path_binds_only_the_observed_native_job(monkeypatch):
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    observed = AsyncMock(return_value=_recorded())

    @asynccontextmanager
    async def connection():
        yield object()

    monkeypatch.setattr(workers.db, "acquire", connection)
    monkeypatch.setattr(workers, "acquire_control_run_worker_action_lock", AsyncMock())
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", observed)
    launched_requests = []
    monkeypatch.setattr(
        workers, "ensure_worker", lambda request: launched_requests.append(request) or {"status": "started"}
    )
    assert await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"}) == {
        "status": "started"
    }
    observed.assert_awaited_once()
    assert len(launched_requests) == 1
    assert source.authorized_fhir_source_worker_launch(SPEC, launched_requests[0]).job_id == JOB_ID


def test_source_launch_never_falls_back_to_local_process(admitted, monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "process")
    monkeypatch.setattr(workers, "_start_process", lambda *args: pytest.fail("ordinary source process started"))
    assert workers._ensure_spec(SPEC, admitted)["status"] == "failed"


def test_kubernetes_create_uses_fixed_source_launch(admitted, secret_selection, monkeypatch):
    monkeypatch.setattr(workers, "_worker_state", lambda *args: {"running": True, "job_status": "active"})
    calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: calls.append(args) or {})
    result = workers._ensure_kubernetes_job(SPEC, admitted, {"job_status": "missing"})
    assert result["status"] == "started"
    assert len(calls) == 1 and calls[0][0] == "POST"
    assert source.is_fhir_source_job_matching(calls[0][2], admitted.fhir_source_launch)


def test_existing_kubernetes_job_mismatch_refuses_launch(admitted, secret_selection, monkeypatch, tmp_path):
    token = tmp_path / "synthetic-token"
    token.write_text("synthetic")
    monkeypatch.setattr(workers, "_K8S_API_TOKEN", token)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "synthetic")
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    manifest = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    manifest["spec"]["template"]["spec"]["containers"][0]["command"] = ["other"]
    calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: calls.append(args) or {"items": [manifest]})
    result = workers._ensure_spec(SPEC, admitted)
    assert result["status"] == "failed" and result["job_status"] == "purpose_mismatch"
    assert all(call[0] == "GET" for call in calls)


@pytest.mark.parametrize("with_job_id", [False, True], ids=["api", "scheduled"])
@pytest.mark.asyncio
async def test_real_ensure_shapes_select_only_the_stored_fhir_job(monkeypatch, with_job_id):
    """API and scheduled-client request shapes cross the same locked admission path."""
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)

    @asynccontextmanager
    async def connection():
        yield object()

    monkeypatch.setattr(workers.db, "acquire", connection)
    monkeypatch.setattr(workers, "acquire_control_run_worker_action_lock", AsyncMock())
    observed = AsyncMock(return_value=_recorded())
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", observed)
    admitted_requests = []
    monkeypatch.setattr(
        workers, "ensure_worker", lambda request: admitted_requests.append(request) or {"status": "started"}
    )
    request_by_field = {"run_id": RUN_ID, "importer": "provider-directory-fhir"}
    if with_job_id:
        request_by_field.update(queue=SPEC.queue, job_id=JOB_ID)
    assert await workers.guarded_ensure_worker(request_by_field) == {"status": "started"}
    assert len(admitted_requests) == 1
    admitted = admitted_requests[0]
    assert source.authorized_fhir_source_worker_launch(SPEC, admitted).job_id == JOB_ID
    assert admitted.cms_registry_launch is None
    assert observed.await_args.kwargs["run_id"] == RUN_ID


def test_signed_cms_purpose_has_precedence_without_ordinary_credentials(admitted, monkeypatch):
    """An ordinary capability cannot replace an admitted signed CMS process."""
    from api import provider_directory_cms_registry_worker as cms

    cms_image = "example/cms-worker@sha256:" + "e" * 64
    admitted.cms_registry_launch = cms.CMSRegistryWorkerLaunch(RUN_ID, cms_image, "f" * 64)
    admitted["job_id"] = JOB_ID
    assert source.authorized_fhir_source_worker_launch(SPEC, admitted) is None
    manifest = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    pod = manifest["spec"]["template"]["spec"]
    assert pod["containers"][0]["image"] == cms_image
    assert pod["containers"][0]["command"] == cms.COMMAND
    assert pod["containers"][0]["workingDir"] == "/app"
    assert manifest["metadata"]["labels"]["healthporta.com/worker-purpose"] == cms.PURPOSE
    assert not source.is_fhir_source_secret_selected(
        {"purposes": [source.PURPOSE], "importers": ["provider-directory-fhir"], "workerClasses": [SPEC.worker_class]},
        SPEC.worker_class,
        admitted,
    )


def test_source_policy_path_uses_deployment_value_only(admitted, monkeypatch):
    setting = "HLTHPRT_SOURCE_PROFILE_ROLE_POLICY_FILE"
    monkeypatch.setenv(setting, "/synthetic-readonly/source-roles.json")
    admitted[setting] = "/untrusted/roles.json"
    environment, _ = workers._worker_job_environment(SPEC, admitted)
    assert [entry for entry in environment if entry["name"] == setting] == [
        {"name": setting, "value": "/synthetic-readonly/source-roles.json"}
    ]


@pytest.mark.asyncio
async def test_missing_paired_image_refuses_before_worker_launch(monkeypatch):
    monkeypatch.delenv(source.IMAGE_ENV, raising=False)
    monkeypatch.delenv(source.REFERENCE_IMAGE_ENV, raising=False)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    workers._bind_admitted_job(request, _recorded())
    result = await workers._admit_source_worker_launches(request, _recorded())
    assert result["status"] == "failed" and request.fhir_source_launch is None


@pytest.mark.asyncio
async def test_cms_admission_never_constructs_an_ordinary_source_capability(monkeypatch):
    from api import provider_directory_cms_registry_worker as cms

    cms_launch = cms.CMSRegistryWorkerLaunch(RUN_ID, IMAGE, "f" * 64)
    monkeypatch.setattr(cms, "admit_cms_registry_worker_launch", AsyncMock(return_value=cms_launch))
    ordinary = AsyncMock(side_effect=AssertionError("ordinary admission replaced signed CMS"))
    monkeypatch.setattr(source, "admit_fhir_source_worker_launch", ordinary)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    workers._bind_admitted_job(request, _recorded())
    assert await workers._admit_source_worker_launches(request, _recorded()) is None
    assert request.cms_registry_launch is cms_launch and request.fhir_source_launch is None
    ordinary.assert_not_called()


@pytest.fixture
def mounted_job(admitted, secret_selection, monkeypatch):
    """Keep the real generator's trusted work PVC and readonly Source policy mount."""
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_NAME", "synthetic-work")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "/work")
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(
            [
                {
                    "name": "source-policy",
                    "secretName": "synthetic-source-policy",
                    "mountPath": "/etc/source-policy",
                    "subPath": "roles.json",
                    "defaultMode": 0o440,
                    "workerClasses": [SPEC.worker_class],
                }
            ]
        ),
    )
    return workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)


def _change_job_bindings(manifest, drift):
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    if drift in {"postStart", "preStop"}:
        container["lifecycle"] = {drift: {"exec": {"command": ["/synthetic-hook"]}}}
        return
    if drift in {"/app", "/opt", "/usr/local/bin"}:
        pod["volumes"].append({"name": "unreviewed", "emptyDir": {}})
        container["volumeMounts"].append({"name": "unreviewed", "mountPath": drift})
        return
    if drift == "mount-path":
        container["volumeMounts"][0]["mountPath"] = "/app"
        return
    if drift == "pvc-source":
        pod["volumes"][0]["persistentVolumeClaim"]["claimName"] = "foreign-work"
        return
    if drift == "secret-source":
        pod["volumes"][1]["secret"]["secretName"] = "foreign-policy"
        return
    if drift == "secret-subpath":
        container["volumeMounts"][1]["subPath"] = "foreign.json"
        return
    if drift == "secret-writable":
        container["volumeMounts"][1]["readOnly"] = False
        return
    if drift == "secret-mode":
        pod["volumes"][1]["secret"]["defaultMode"] = 0o644
        return
    if drift == "volume-device":
        container["volumeDevices"] = [{"name": "source-policy", "devicePath": "/usr/local/bin/fhir-source-worker"}]
        return
    _change_binding_members(pod, container, drift)


def _change_binding_members(pod, container, drift):
    if drift == "extra-volume":
        pod["volumes"].append({"name": "unreviewed", "emptyDir": {}})
        return
    if drift == "missing-volume":
        pod["volumes"].pop()
        return
    if drift == "missing-mount":
        container["volumeMounts"].pop()
        return
    if drift == "duplicate-volume":
        pod["volumes"].append(deepcopy(pod["volumes"][0]))
        return
    if drift == "duplicate-mount":
        container["volumeMounts"].append(deepcopy(container["volumeMounts"][0]))
        return
    if drift == "swapped-name":
        container["volumeMounts"][0]["name"] = "source-policy"
        return
    raise AssertionError("unknown regression drift")


@pytest.mark.parametrize(
    "drift",
    [
        "postStart",
        "preStop",
        "/app",
        "/opt",
        "/usr/local/bin",
        "mount-path",
        "pvc-source",
        "secret-source",
        "secret-subpath",
        "secret-writable",
        "secret-mode",
        "volume-device",
        "extra-volume",
        "missing-volume",
        "missing-mount",
        "duplicate-volume",
        "duplicate-mount",
        "swapped-name",
    ],
)
def test_existing_job_refuses_unreviewed_process_or_mounts(admitted, mounted_job, drift):
    assert source.is_fhir_source_job_matching(mounted_job, admitted.fhir_source_launch)
    _change_job_bindings(mounted_job, drift)
    assert not source.is_fhir_source_job_matching(mounted_job, admitted.fhir_source_launch)


def test_existing_job_preserves_trusted_pvc_and_secret_bindings(admitted, mounted_job):
    pod = mounted_job["spec"]["template"]["spec"]
    assert pod["volumes"][0]["persistentVolumeClaim"]["claimName"] == "synthetic-work"
    assert pod["volumes"][1]["secret"]["defaultMode"] == 0o440
    assert pod["containers"][0]["volumeMounts"][1] == {
        "name": "source-policy",
        "mountPath": "/etc/source-policy",
        "readOnly": True,
        "subPath": "roles.json",
    }
    assert source.is_fhir_source_job_matching(mounted_job, admitted.fhir_source_launch)


def test_existing_job_accepts_only_the_secret_api_default(admitted, mounted_job, monkeypatch):
    """Kubernetes defaults an omitted Secret volume defaultMode to 0644."""
    setting = "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON"
    mounts = json.loads(workers.os.environ[setting])
    del mounts[0]["defaultMode"]
    monkeypatch.setenv(setting, json.dumps(mounts))
    manifest = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    secret = manifest["spec"]["template"]["spec"]["volumes"][1]["secret"]
    assert "defaultMode" not in secret
    assert source.is_fhir_source_job_matching(manifest, admitted.fhir_source_launch)
    secret["defaultMode"] = 0o644
    assert source.is_fhir_source_job_matching(manifest, admitted.fhir_source_launch)


@pytest.mark.parametrize("drift", ["postStart", "/app"])
def test_ensure_refuses_existing_job_code_changes(admitted, mounted_job, monkeypatch, drift):
    """The real state/ensure path cannot reuse an active Job with unreviewed code."""
    _change_job_bindings(mounted_job, drift)
    mounted_job["status"] = {"active": 1}
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_launcher_mode", lambda: "kubernetes")
    calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: calls.append(args) or {"items": [mounted_job]})
    result = workers._ensure_spec(SPEC, admitted)
    assert result["status"] == "failed" and result["job_status"] == "purpose_mismatch"
    assert len(calls) == 1 and calls[0][0] == "GET"


def _original_route_run():
    route = _source_route_config()
    return {
        **_recorded(),
        "run_id": route["root_run_id"],
        "status": "running",
        "retry_of_run_id": None,
        "metrics": {
            "queue": SPEC.queue,
            "function": "control_single_job_start",
            "job_id": "provider_directory_start_" + route["root_run_id"],
        },
        "params": {
            "source_ids": [route["source_id"]],
            "provider_directory_endpoint_scope": route["endpoint_scope"],
        },
    }


@pytest.fixture
def original_job_before_default_change(monkeypatch):
    """Create the original Job before installing a new default and its old-root route."""
    recorded = _original_route_run()
    monkeypatch.delenv(source.ROUTES_ENV, raising=False)
    monkeypatch.setenv(source.IMAGE_ENV, ORDINARY_IMAGE)
    _both_source_secrets(monkeypatch)
    for setting in ("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON"):
        selectors = json.loads(workers.os.environ[setting])
        monkeypatch.setenv(
            setting, json.dumps([entry for entry in selectors if entry["secretName"] != "synthetic-default"])
        )
    request = workers._AdmittedWorkerRequest(run_id=recorded["run_id"], importer="provider-directory-fhir")
    workers._bind_admitted_job(request, recorded)
    request.fhir_source_launch = source.admit_fhir_source_worker_launch(recorded)
    job = workers._worker_job_manifest(SPEC, request, ORDINARY_IMAGE)
    job["status"] = {"active": 1}
    assert "healthporta.com/source-root-run-id" not in job["metadata"]["annotations"]
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_source_route_config()]))
    _both_source_secrets(monkeypatch)
    return recorded, job


def _mock_original_worker_control(monkeypatch, recorded, lineage_rows, job):
    native_connection = SimpleNamespace(all=AsyncMock(return_value=lineage_rows))

    @asynccontextmanager
    async def connection():
        yield native_connection

    monkeypatch.setattr(workers.db, "acquire", connection)
    monkeypatch.setattr(workers, "acquire_control_run_worker_action_lock", AsyncMock())
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", AsyncMock(return_value=recorded))
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    api_calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: api_calls.append(args) or {"items": [job]})
    return native_connection, api_calls


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "image", "environment", "foreign_job", "retry_annotation", "policy"])
async def test_original_worker_survives_default_change(monkeypatch, original_job_before_default_change, drift):
    """Reuse an unchanged original Job, with every existing runtime check still enforced."""
    recorded, job = original_job_before_default_change
    pod = job["spec"]["template"]["spec"]
    container = pod["containers"][0]
    match drift:
        case "image":
            container["image"] = IMAGE
        case "environment":
            container["env"].append({"name": "UNREVIEWED", "value": "1"})
        case "foreign_job":
            next(entry for entry in container["env"] if entry["name"] == "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID")[
                "value"
            ] = JOB_ID
        case "retry_annotation":
            job["metadata"]["annotations"]["healthporta.com/source-root-run-id"] = recorded["run_id"]
        case "policy":
            pod["volumes"][0]["secret"]["secretName"] = "synthetic-default"
    native_connection, api_calls = _mock_original_worker_control(monkeypatch, recorded, [deepcopy(recorded)], job)
    before = deepcopy(job)
    outcome = await workers.guarded_ensure_worker({"run_id": recorded["run_id"], "importer": "provider-directory-fhir"})
    assert outcome["status"] == ("failed" if drift else "already_running")
    if drift:
        assert outcome["items"][0]["job_status"] == "purpose_mismatch"
    native_connection.all.assert_awaited_once()
    assert len(api_calls) == 1 and api_calls[0][0] == "GET"
    assert job == before
    assert "retry_of_run_id" not in recorded["params"]
    assert "provider_directory_pagination_root_run_id" not in recorded["params"]


@pytest.mark.parametrize("drift", ["source", "endpoint", "parent", "params_parent", "params_root"])
def test_original_route_refuses_changed_identity(monkeypatch, original_job_before_default_change, drift):
    recorded, _job = original_job_before_default_change
    if drift == "source":
        recorded["params"]["source_ids"] = ["other-source"]
    elif drift == "endpoint":
        recorded["params"]["provider_directory_endpoint_scope"] += "/other"
    elif drift == "parent":
        recorded["retry_of_run_id"] = RUN_ID
    else:
        key = "retry_of_run_id" if drift == "params_parent" else "provider_directory_pagination_root_run_id"
        recorded["params"][key] = RUN_ID
    with pytest.raises(ValueError, match="fhir_source_worker_route_changed"):
        source.admit_fhir_source_worker_launch(recorded)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["missing", "failed", "source", "endpoint", "parent", "params_parent", "params_root"])
async def test_original_route_checks_native_identity_before_job_lookup(
    monkeypatch, original_job_before_default_change, drift
):
    recorded, job = original_job_before_default_change
    native = deepcopy(recorded)
    match drift:
        case "failed":
            native["status"] = "failed"
        case "source":
            native["params"]["source_ids"] = ["other-source"]
        case "endpoint":
            native["params"]["provider_directory_endpoint_scope"] += "/other"
        case "parent":
            native["retry_of_run_id"] = RUN_ID
        case "params_parent" | "params_root":
            key = "retry_of_run_id" if drift == "params_parent" else "provider_directory_pagination_root_run_id"
            native["params"][key] = RUN_ID
    native_connection, api_calls = _mock_original_worker_control(
        monkeypatch, recorded, [] if drift == "missing" else [native], job
    )
    outcome = await workers.guarded_ensure_worker({"run_id": recorded["run_id"], "importer": "provider-directory-fhir"})
    assert outcome["status"] == "failed"
    native_connection.all.assert_awaited_once()
    assert api_calls == []
