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
from process.delta_dental_directory_browser import SOURCE_ID, SOURCE_ORIGIN


def _task():
    return {
        "source_ids": [SOURCE_ID],
        "provider_directory_endpoint_scope": SOURCE_ORIGIN,
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
    assert delta_entries[0]["canonical_base"] == SOURCE_ORIGIN
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
    capture_id, searches = worker._authorized_plan()
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

    from process import delta_dental_directory_transport as transport
    from process.ext.source_http_proxy import SourceProxyUnavailable

    attempted_launches = []

    def unexpected_browser(*_args, **_options):
        attempted_launches.append(True)
        raise AssertionError("transport should not open")

    monkeypatch.setattr(transport, "open_finder_page", unexpected_browser)
    monkeypatch.setattr(worker, "_artifact_root", lambda: tmp_path)
    search = worker.FinderSearch("Delta Dental Premier", "01234", 30)
    monkeypatch.setattr(worker, "_authorized_plan", lambda: ("new-edition", (search,)))

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
        "endpoint_scope": SOURCE_ORIGIN,
    }
    policy_by_field = {
        "version": 1,
        "revision": "example-policy",
        "sources": [
            {
                "source_id": SOURCE_ID,
                "endpoint_scope": SOURCE_ORIGIN,
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
    monkeypatch.setenv(launcher.ROUTES_ENV, "[]")
    assert launcher.is_delta_route_available() is False


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


def test_observation_secret_refs_are_scoped(monkeypatch):
    from api import control_workers

    launch = launcher.FHIRSourceWorkerLaunch(
        "example-run",
        "example-job",
        "example-image",
        source_id=SOURCE_ID,
        endpoint_scope=SOURCE_ORIGIN,
        secret_name="example-runtime",
        pool_secret_name="example-pool",
    )
    scope_by_field = {
        "purposes": [launcher.PURPOSE],
        "importers": ["provider-directory-fhir"],
        "workerClasses": ["process.ProviderDirectoryFHIR"],
        "sharedForRoutes": True,
    }
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
    monkeypatch.setattr(control_workers, "_is_admitted_importer_selected", lambda *args: True)
    assert control_workers._worker_job_secret_env("process.ProviderDirectoryFHIR", source_launch=launch) == [
        {"name": "CONTROL_CONNECTION", "valueFrom": {"secretKeyRef": {"name": "example-control", "key": "control"}}},
        {"name": "QUEUE_CONNECTION", "valueFrom": {"secretKeyRef": {"name": "example-queue", "key": "queue"}}},
    ]
