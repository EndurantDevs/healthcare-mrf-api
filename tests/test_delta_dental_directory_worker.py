# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise exact source selection and the separated discovery catalog."""

import json
from importlib import import_module
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_source_worker as launcher
from api.provider_directory_source_worker import _uses_source_http
from api.provider_directory_sources import provider_directory_source_catalog
from process import delta_dental_directory_worker as worker
from process.delta_dental_directory_json import SOURCE_ENDPOINT, SOURCE_ID


def _task():
    return {
        "source_ids": [SOURCE_ID],
        "provider_directory_endpoint_scope": SOURCE_ENDPOINT,
        "delta_dental_directory_observation": True,
        "probe": False,
        "import_resources": False,
    }


@pytest.mark.asyncio
async def test_observation_bypasses_fhir(monkeypatch):
    provider_directory_fhir = import_module("process.provider_directory_fhir")
    dispatches = []

    async def run(directory, context_by_field, task):
        dispatches.append((directory, context_by_field, task))
        return {"status": "succeeded"}

    monkeypatch.setattr(worker, "run_delta_observation", run)
    task = _task()
    context_by_field = {"control_run_id": "example"}
    assert await provider_directory_fhir.process_provider_directory_fhir_data(context_by_field, task) == {
        "status": "succeeded"
    }
    assert dispatches == [(provider_directory_fhir, context_by_field, task)]
    assert _uses_source_http(task) is True
    with pytest.raises(ValueError, match="observation_request_invalid"):
        _uses_source_http({**task, "source_ids": [SOURCE_ID, "another-source"]})
    with pytest.raises(ValueError, match="observation_request_invalid"):
        _uses_source_http({**task, "publish_artifacts": True})


@pytest.mark.asyncio
async def test_rooted_entry_preserves_manual_dispatch(monkeypatch):
    provider_directory_fhir = import_module("process.provider_directory_fhir")
    rooted_worker = import_module("process.provider_directory_rooted_graph_worker")
    handler = AsyncMock(return_value={"status": "succeeded"})
    monkeypatch.setattr(rooted_worker, "process_rooted_graph_task", handler)
    context_by_field = {"control_run_id": "example"}
    rooted_by_field = {"rooted_graph": {"operation": "example"}}
    assert await provider_directory_fhir.process_provider_directory_fhir_data(context_by_field, rooted_by_field) == {
        "status": "succeeded"
    }
    handler.assert_awaited_once_with(provider_directory_fhir, context_by_field, rooted_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("rooted_task", [None, {}, {"operation": "example"}])
async def test_mixed_specialized_task_fails_closed(monkeypatch, rooted_task):
    provider_directory_fhir = import_module("process.provider_directory_fhir")
    rooted_worker = import_module("process.provider_directory_rooted_graph_worker")
    handler = AsyncMock()
    monkeypatch.setattr(rooted_worker, "process_rooted_graph_task", handler)
    with pytest.raises(ValueError, match="observation_request_invalid"):
        await provider_directory_fhir.process_provider_directory_fhir_data({}, {**_task(), "rooted_graph": rooted_task})
    handler.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_scope",
    [SOURCE_ID, f" {SOURCE_ID} ", f"other, {SOURCE_ID} ", [f" {SOURCE_ID} "], (SOURCE_ID,), [SOURCE_ID, "other"]],
)
async def test_reserved_scope_cannot_fall_through(source_scope):
    provider_directory_fhir = import_module("process.provider_directory_fhir")
    with pytest.raises(ValueError, match="observation_request_invalid"):
        await provider_directory_fhir.process_provider_directory_fhir_data({}, {"source_ids": source_scope})
    with pytest.raises(ValueError, match="observation_request_invalid"):
        _uses_source_http({**_task(), "source_ids": source_scope})


@pytest.mark.parametrize("scope", [f"like-{SOURCE_ID}", [f"{SOURCE_ID}-other"], (f" {SOURCE_ID}-regional ",)])
def test_similar_source_remains_independent(scope):
    assert worker.specialized_source_task(None, {}, {"source_ids": scope}) is None
    assert _uses_source_http({"source_ids": scope, "probe": True}) is True


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "setting"), [("dataset_rehydrate_only", True), ("rehydrate_dataset_id", " example ")])
async def test_observation_rehydrate_mix_rejected(mode, setting):
    provider_directory_fhir = import_module("process.provider_directory_fhir")
    with pytest.raises(ValueError, match="observation_request_invalid"):
        await provider_directory_fhir.process_provider_directory_fhir_data({}, {**_task(), mode: setting})
    with pytest.raises(ValueError, match="observation_request_invalid"):
        _uses_source_http({**_task(), mode: setting})


def test_observation_has_no_fhir_resources():
    catalog = provider_directory_source_catalog()
    delta_entries = [entry for entry in catalog["items"] if entry["source_ids"] == [SOURCE_ID]]
    assert len(delta_entries) == 1
    assert delta_entries[0]["canonical_base"] == SOURCE_ENDPOINT
    assert delta_entries[0]["classification"] == "observation"
    assert delta_entries[0]["resources"] == []
    assert delta_entries[0]["profile_enabled"] is False
    assert delta_entries[0]["runnable"] is False


def test_protected_manifest_scope(tmp_path, monkeypatch):
    path = tmp_path / "manifest.json"
    monkeypatch.setenv(worker.MANIFEST_ENV, str(path))
    manifest_by_field = {
        "authorization_scope": "example-authority",
        "data_edition": "example-edition",
        "searches": [{"selector": "Delta Dental Patient Direct", "zip_code": "00000", "radius_miles": 30}],
    }
    path.write_text(json.dumps(manifest_by_field))
    capture_id, searches, concurrency, rate, budget = worker._authorized_plan()
    assert (concurrency, rate, budget) == (1, None, 100_000)
    assert len(capture_id) == 64
    assert len(searches) == 1
    assert searches[0].selector == "Delta Dental Patient Direct"
    path.write_text(json.dumps({**manifest_by_field, "data_edition": "new-example-edition"}))
    assert worker._authorized_plan()[0] != capture_id
    path.write_text(json.dumps({**manifest_by_field, "authorization_scope": ""}))
    with pytest.raises(ValueError, match="authorized_manifest_invalid"):
        worker._authorized_plan()


def test_retry_refuses_edition_drift(tmp_path):
    retry_by_field = {"retry_of_run_id": "first", "provider_directory_pagination_root_run_id": "first"}
    with pytest.raises(ValueError, match="retained_edition_required"):
        worker._bind_observation_edition(tmp_path, "retry", "example-edition", retry_by_field)
    worker._bind_observation_edition(tmp_path, "first", "example-edition", {})
    worker._bind_observation_edition(tmp_path, "retry", "example-edition", retry_by_field)
    with pytest.raises(ValueError, match="run_edition_changed"):
        worker._bind_observation_edition(tmp_path, "retry", "changed-edition", retry_by_field)


@pytest.mark.asyncio
async def test_hold_blocks_new_editions(tmp_path, monkeypatch):
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    from process import delta_dental_directory_http as transport
    from process.ext.source_http_proxy import SourceProxyUnavailable

    attempted_launches = []

    def unexpected_egress(*_args, **_options):
        attempted_launches.append(True)
        raise AssertionError("transport should not open")

    monkeypatch.setattr(transport, "open_finder_http", unexpected_egress)
    monkeypatch.setattr(worker, "_artifact_root", lambda: tmp_path)
    search = worker.FinderSearch("Delta Dental Premier", "01234", 30)
    monkeypatch.setattr(worker, "_authorized_plan", lambda: ("new-edition", (search,), 1, None, 100_000))

    async def prepare(*_args, **_options):
        return None

    scope = SimpleNamespace(
        attach_audit=lambda _audit: None, prepare=prepare, bindings={SOURCE_ID: object()}, credentials={}
    )
    monkeypatch.setattr(worker, "enter_run", lambda _directory: (None, scope))
    monkeypatch.setattr(worker, "exit_run", lambda _token: None)
    for index, hold in enumerate(((datetime.now(UTC) + timedelta(hours=1)).isoformat(), None)):
        worker._persist_destination_hold(tmp_path, hold)
        with pytest.raises(SourceProxyUnavailable):
            await worker.run_delta_observation(None, {}, {**_task(), "run_id": f"new-example-{index}"})
        assert (tmp_path / f"run-new-example-{index}.edition").is_file()
    assert attempted_launches == []


def test_inventory_retains_zip_zeros():
    plan_by_field = {
        "authorization_scope": "example-authority",
        "data_edition": "example-edition",
        "zip_inventory": ["01234", "01234", "04321"],
        "radius_miles": 5,
    }
    searches = worker._searches_from_plan(plan_by_field)
    assert len(searches) == 10
    assert {search.zip_code for search in searches} == {"01234", "04321"}
    assert {search.radius_miles for search in searches} == {5}
    assert {search.selector for search in searches} == {
        "Delta Dental PPO",
        "Delta Dental Premier",
        "Delta Dental Medicare Advantage",
        "DeltaCare USA",
        "Delta Dental Patient Direct",
    }
    named = worker._searches_from_plan({**plan_by_field, "selector_scope": "named"})
    assert {search.selector for search in named} - {search.selector for search in searches} == {
        "Delta Dental PPO Plus Premier",
    }
    supplementary = worker._searches_from_plan({**plan_by_field, "selector_scope": "named-and-all"})
    assert {search.selector for search in supplementary} - {search.selector for search in named} == {"All networks"}


def test_catalog_requires_scoped_route(monkeypatch):
    route_by_field = {
        "root_run_id": None,
        "image": "ghcr.io/example/directory@sha256:" + "a" * 64,
        "secret_name": "example-runtime",
        "source_id": SOURCE_ID,
        "endpoint_scope": SOURCE_ENDPOINT,
    }
    policy_by_field = {
        "version": 1,
        "revision": "example-policy",
        "sources": [
            {
                "source_id": SOURCE_ID,
                "endpoint_scope": SOURCE_ENDPOINT,
                "use_proxy": True,
            }
        ],
    }
    monkeypatch.setenv(launcher.ROUTES_ENV, json.dumps([route_by_field]))
    assert launcher.is_delta_route_available() is False
    monkeypatch.setenv(launcher.PROXY_POLICY_ENV, json.dumps(policy_by_field))
    assert launcher.is_delta_route_available() is False
    monkeypatch.setenv(launcher.PROXY_POOL_SECRET_ENV, "example-proxy-pool")
    monkeypatch.setenv(launcher.PROXY_POOL_REVISION_ENV, "example-pool-revision")
    assert launcher.is_delta_route_available() is False
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(
            [
                {
                    "purposes": [launcher.PURPOSE],
                    "workerClasses": ["process.ProviderDirectoryFHIR"],
                    "importers": ["provider-directory-fhir"],
                    "secretName": "example-runtime",
                    "mountPath": worker.MANIFEST_PATH,
                    "subPath": "plan.json",
                    "readOnly": True,
                }
            ]
        ),
    )
    assert launcher.is_delta_route_available() is True
    _assert_observation_job_readback(monkeypatch)
    monkeypatch.setenv(launcher.ROUTES_ENV, "[]")
    assert launcher.is_delta_route_available() is False


def _assert_observation_job_readback(monkeypatch):
    """Exercise the persisted Kubernetes callback shape through the full matcher."""
    from api import control_workers

    worker_spec = control_workers._BY_WORKER_CLASS["process.ProviderDirectoryFHIR"]
    run_id = "run_" + "1" * 32
    run_by_field = {
        "run_id": run_id,
        "retry_of_run_id": None,
        "importer": "provider-directory-fhir",
        "status": "queued",
        "params": _task(),
        "metrics": {
            "queue": worker_spec.queue,
            "function": "control_single_job_start",
            "job_id": "provider_directory_start_" + run_id,
        },
    }
    launch = launcher.admit_fhir_source_worker_launch(run_by_field)
    proxy_selector_by_field = {
        **_delta_secret_scope(),
        "name": launcher.PROXY_POOL_ENV,
        "key": launcher.PROXY_POOL_ENV,
        "secretName": launch.pool_secret_name,
        "sourceProxyPool": True,
    }
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", json.dumps([proxy_selector_by_field]))
    request = control_workers._AdmittedWorkerRequest(run_id=run_id, importer="provider-directory-fhir")
    control_workers._bind_admitted_job(request, run_by_field)
    request.fhir_source_launch = launch
    job = control_workers._worker_job_manifest(worker_spec, request, launch.image)
    environment = job["spec"]["template"]["spec"]["containers"][0]["env"]
    callback_by_field = next(entry for entry in environment if entry["name"] == "HLTHPRT_IMPORT_STATUS_EVENT_URL")
    assert callback_by_field["value"] == ""
    assert launcher.is_fhir_source_job_matching(job, launch, request)
    del callback_by_field["value"]
    assert launcher.is_fhir_source_job_matching(job, launch, request)
    callback_by_field["value"] = "https://example.test"
    assert not launcher.is_fhir_source_job_matching(job, launch, request)
    del callback_by_field["value"]
    callback_by_field["valueFrom"] = {"configMapKeyRef": {"name": "example-callback", "key": "url"}}
    assert not launcher.is_fhir_source_job_matching(job, launch, request)


def test_lost_edition_blocks_redownload(tmp_path):
    capture = worker.FinderCapturePlan(tmp_path, "example-edition", "example-scope", (), plan_id="first")
    worker._guard_retained_edition(capture)
    database = tmp_path / "finder-example-edition.sqlite3"
    database.touch()
    worker._mark_egress_started(capture)
    worker._guard_retained_edition(capture)
    database.unlink()
    with pytest.raises(ValueError, match="retained_edition_required"):
        worker._guard_retained_edition(capture)


@pytest.fixture
def delta_launch():
    """Use a synthetic exact observation launch in the secret selection checks."""
    return launcher.FHIRSourceWorkerLaunch(
        "example-run",
        "example-job",
        "example-image",
        source_id=SOURCE_ID,
        endpoint_scope=SOURCE_ENDPOINT,
        secret_name="example-runtime",
        pool_secret_name="example-pool",
    )


def _delta_secret_scope():
    """Supply the exact Source worker purpose accepted by its selectors."""
    return {
        "purposes": [launcher.PURPOSE],
        "importers": ["provider-directory-fhir"],
        "workerClasses": ["process.ProviderDirectoryFHIR"],
    }


def test_observation_secret_refs_are_scoped(monkeypatch, delta_launch):
    """Only tagged control and queue connections can enter the observation environment."""
    from api import control_workers

    scope_by_field = _delta_secret_scope() | {"sharedForRoutes": True}
    selectors = [
        {
            **scope_by_field,
            "name": "CONTROL_CONNECTION",
            "secretName": "example-control",
            "key": "control",
            "observationConnection": "control",
        },
        {
            **scope_by_field,
            "name": "QUEUE_CONNECTION",
            "secretName": "example-queue",
            "key": "queue",
            "observationConnection": "queue",
        },
        {**scope_by_field, "name": "PUBLISH_CONNECTION", "secretName": "example-publisher", "key": "publish"},
        {
            "name": "UNSCOPED_CONNECTION",
            "secretName": "example-unscoped",
            "key": "unscoped",
            "observationConnection": "control",
        },
    ]
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", json.dumps(selectors))
    assert not launcher.is_observation_secret_allowed(selectors[0], None)
    assert not launcher.is_observation_secret_allowed(selectors[1], None)
    assert launcher.is_observation_secret_allowed(selectors[2], None)
    monkeypatch.setattr(control_workers, "_is_admitted_importer_selected", lambda *args: True)
    assert control_workers._worker_job_secret_env("process.ProviderDirectoryFHIR", source_launch=delta_launch) == [
        {"name": "CONTROL_CONNECTION", "valueFrom": {"secretKeyRef": {"name": "example-control", "key": "control"}}},
        {"name": "QUEUE_CONNECTION", "valueFrom": {"secretKeyRef": {"name": "example-queue", "key": "queue"}}},
    ]


def test_observation_mounts_only_the_selected_plan(monkeypatch, delta_launch):
    """Unrelated credentials and even another source plan stay outside the job."""
    from api import control_workers

    plan_mount_by_field = {
        **_delta_secret_scope(),
        "secretName": delta_launch.secret_name,
        "mountPath": worker.MANIFEST_PATH,
        "subPath": "plan.json",
        "readOnly": True,
    }
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(
            [
                {"secretName": "example-default", "mountPath": "/var/run/example"},
                plan_mount_by_field | {"secretName": "example-other-source"},
                plan_mount_by_field,
            ]
        ),
    )
    worker_spec = control_workers._BY_WORKER_CLASS["process.ProviderDirectoryFHIR"]
    [mount] = control_workers._worker_job_secret_volumes(worker_spec, delta_launch)
    assert mount["volume"]["secret"] == {"secretName": delta_launch.secret_name}
    assert mount["volumeMount"]["mountPath"] == worker.MANIFEST_PATH
    assert mount["volumeMount"]["subPath"] == "plan.json"


def test_observation_disables_the_inherited_status_event_route(delta_launch):
    """Observation jobs report through Core status without the shared Control bearer."""
    from api import control_workers

    expected_by_field = {"name": "HLTHPRT_IMPORT_STATUS_EVENT_URL", "value": ""}
    assert control_workers._worker_source_policy_container({"env": []}, [], delta_launch)["env"] == [expected_by_field]
    assert control_workers._worker_source_policy_container({"env": []}, [], None)["env"] == []


@pytest.mark.parametrize(
    "settings",
    [
        {"concurrency": True},
        {"concurrency": 0},
        {"concurrency": 9, "requests_per_second": 1},
        {"concurrency": 8},
        {"concurrency": 8, "requests_per_second": 0},
        {"concurrency": 8, "requests_per_second": float("inf")},
        {"concurrency": 8, "requests_per_second": float("nan")},
        {"requests_per_second": True},
        {"requests_per_second": "2"},
        {"requests_per_second": None},
    ],
)
def test_protected_manifest_validates_acquisition_settings(tmp_path, monkeypatch, settings):
    path = tmp_path / "manifest.json"
    monkeypatch.setenv(worker.MANIFEST_ENV, str(path))
    manifest_by_field = {
        "authorization_scope": "example-authority",
        "data_edition": "example-edition",
        "zip_inventory": ["01234"],
        "radius_miles": 30,
    }
    path.write_text(json.dumps({**manifest_by_field, "concurrency": 8, "requests_per_second": 2}))
    assert worker._authorized_plan()[2:] == (8, 2, 100_000)
    path.write_text(json.dumps({**manifest_by_field, **settings}))
    with pytest.raises(ValueError, match="authorized_manifest_invalid"):
        worker._authorized_plan()


@pytest.mark.parametrize(
    ("configured", "rate", "count", "expected"),
    [
        ({}, None, 100_000, 100_000),
        ({}, 2, 188_625, None),
        ({"max_origin_requests": 200_000}, 2, 188_625, 200_000),
        ({"max_origin_requests": 200_000}, None, 188_625, None),
        ({"max_origin_requests": 200_001}, 2, 188_625, None),
        ({"max_origin_requests": True}, 2, 1, None),
    ],
)
def test_origin_budget_is_explicit_and_bounds_the_plan(configured, rate, count, expected):
    if expected is None:
        with pytest.raises(ValueError, match="origin_request_budget_invalid"):
            worker._origin_request_budget(configured, rate, count)
    else:
        assert worker._origin_request_budget(configured, rate, count) == expected


@pytest.mark.parametrize("changed", ["concurrency", "rate", "budget"])
def test_retry_refuses_acquisition_settings_drift(tmp_path, changed):
    retry_by_field = {"retry_of_run_id": "first", "provider_directory_pagination_root_run_id": "first"}
    original_by_field = {"concurrency": 8, "rate": 2.0, "budget": 200_000}
    worker._bind_observation_edition(tmp_path, "first", "example-edition", {}, **original_by_field)
    worker._bind_observation_edition(tmp_path, "retry", "example-edition", retry_by_field, **original_by_field)
    changed_by_field = {**original_by_field, changed: original_by_field[changed] - 1}
    with pytest.raises(ValueError, match="run_edition_changed"):
        worker._bind_observation_edition(tmp_path, "retry", "example-edition", retry_by_field, **changed_by_field)


def test_destination_hold_keeps_the_latest_finite_boundary(tmp_path):
    shorter = "2099-01-01T00:00:01+00:00"
    longer = "2099-01-01T00:00:03+00:00"
    assert worker._persist_destination_hold(tmp_path, shorter) == shorter
    assert worker._persist_destination_hold(tmp_path, longer) == longer
    assert worker._persist_destination_hold(tmp_path, shorter) == longer
