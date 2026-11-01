# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh source routing keeps native admission and credential isolation."""

import json
from copy import deepcopy
from dataclasses import asdict, replace

import pytest

from api import control_workers as workers
from api import provider_directory_source_worker as source
from tests.test_provider_directory_source_worker_launch import (
    IMAGE,
    JOB_ID,
    ORDINARY_IMAGE,
    RUN_ID,
    SPEC,
    _both_source_secrets,
    _recorded,
    _source_route_config,
    _source_route_run,
)


def _fresh_route():
    return {**_source_route_config(), "root_run_id": None}


def _fresh_run():
    return {
        **_recorded(),
        "retry_of_run_id": None,
        "params": {
            "source_ids": ["synthetic-source"],
            "provider_directory_endpoint_scope": "https://example.test/fhir",
        },
    }


def _admitted(recorded):
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    workers._bind_admitted_job(request, recorded)
    request.fhir_source_launch = source.admit_fhir_source_worker_launch(recorded)
    return request


@pytest.fixture
def configured_runtime(monkeypatch):
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_fresh_route()]))
    _both_source_secrets(monkeypatch)


def _publisher_secret_names(pod):
    return [
        entry["valueFrom"]["secretKeyRef"]["name"]
        for entry in pod["containers"][0]["env"]
        if entry["name"] == "SOURCE_USER"
    ]


@pytest.mark.usefixtures("configured_runtime")
def test_fresh_route_selects_exact_image_and_credentials():
    request = _admitted(_fresh_run())
    launch = request.fhir_source_launch
    assert launch == source.FHIRSourceWorkerLaunch(
        RUN_ID,
        JOB_ID,
        ORDINARY_IMAGE,
        secret_name="synthetic-successor",
        source_id="synthetic-source",
        endpoint_scope="https://example.test/fhir",
    )
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    pod = job["spec"]["template"]["spec"]
    assert pod["containers"][0]["image"] == ORDINARY_IMAGE
    assert pod["containers"][0]["command"] == source.COMMAND
    assert _publisher_secret_names(pod) == ["synthetic-successor"]
    assert [entry["secret"]["secretName"] for entry in pod["volumes"]] == ["synthetic-successor"]
    assert job["metadata"]["annotations"]["healthporta.com/source-id"] == "synthetic-source"
    assert job["metadata"]["annotations"]["healthporta.com/source-endpoint-scope"] == "https://example.test/fhir"
    assert "healthporta.com/source-root-run-id" not in job["metadata"]["annotations"]
    assert source.is_fhir_job_set_matching(SPEC, request, [job])
    source_user = next(entry for entry in pod["containers"][0]["env"] if entry["name"] == "SOURCE_USER")
    source_user["valueFrom"]["secretKeyRef"]["name"] = "synthetic-default"
    assert not source.is_fhir_job_set_matching(SPEC, request, [job])


@pytest.mark.usefixtures("configured_runtime")
@pytest.mark.parametrize("mode", [{"seed_only": True}, {"publish_artifacts_only": True}, {"probe": False}])
def test_fresh_route_non_http_modes_use_default_without_proxy_policy(monkeypatch, mode):
    monkeypatch.delenv(source.PROXY_POLICY_ENV, raising=False)
    recorded = _fresh_run()
    recorded["params"].update(mode)
    assert _admitted(recorded).fhir_source_launch == source.FHIRSourceWorkerLaunch(RUN_ID, JOB_ID, IMAGE)


@pytest.mark.usefixtures("configured_runtime")
@pytest.mark.parametrize("source_id", ["unrelated-source-a", "unrelated-source-b", "unrelated-source-c"])
def test_fresh_route_does_not_change_unrelated_sources(source_id):
    recorded = _fresh_run()
    recorded["params"]["source_ids"] = [source_id]
    request = _admitted(recorded)
    assert request.fhir_source_launch == source.FHIRSourceWorkerLaunch(RUN_ID, JOB_ID, IMAGE)
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    pod = job["spec"]["template"]["spec"]
    assert _publisher_secret_names(pod) == ["synthetic-default"]
    assert [entry["secret"]["secretName"] for entry in pod["volumes"]] == ["synthetic-default"]
    assert not {"healthporta.com/source-id", "healthporta.com/source-endpoint-scope"}.intersection(
        job["metadata"]["annotations"]
    )


@pytest.mark.usefixtures("configured_runtime")
@pytest.mark.parametrize(
    ("native_by_field", "params_by_name"),
    [
        ({}, {"provider_directory_endpoint_scope": "https://other.example.test/fhir"}),
        ({}, {"source_ids": ["synthetic-source", "unrelated-source"]}),
        ({"retry_of_run_id": "run_" + "e" * 32}, {}),
        ({}, {"retry_of_run_id": None}),
        ({}, {"provider_directory_pagination_root_run_id": None}),
        ({"status": "failed"}, {}),
    ],
)
def test_fresh_route_refuses_partial_or_nonfresh_native_identity(native_by_field, params_by_name):
    recorded = _fresh_run()
    recorded.update(native_by_field)
    recorded["params"].update(params_by_name)
    with pytest.raises(ValueError, match="fhir_source_worker_route_changed"):
        _admitted(recorded)


@pytest.mark.usefixtures("configured_runtime")
def test_fresh_route_requires_observed_absence_of_native_parent():
    recorded = _fresh_run()
    del recorded["retry_of_run_id"]
    with pytest.raises(ValueError, match="fhir_source_worker_route_changed"):
        _admitted(recorded)


@pytest.mark.usefixtures("configured_runtime")
def test_fresh_route_uses_stored_parameters_instead_of_request_assertions():
    recorded = _fresh_run()
    recorded["params"]["source_ids"] = ["unrelated-source"]
    request = _admitted(recorded)
    request["params"] = _fresh_run()["params"]
    pod = workers._worker_job_manifest(SPEC, request, IMAGE)["spec"]["template"]["spec"]
    assert request.fhir_source_launch.image == IMAGE
    assert _publisher_secret_names(pod) == ["synthetic-default"]


@pytest.mark.usefixtures("configured_runtime")
def test_serialized_fresh_capability_cannot_select_route_secret():
    request = _admitted(_fresh_run())
    request_by_field = {**request, "fhir_source_launch": asdict(request.fhir_source_launch)}
    selection_by_field = {
        "name": "SOURCE_USER",
        "secretName": "synthetic-successor",
        "key": "username",
        "workerClasses": [SPEC.worker_class],
        "purposes": [source.PURPOSE],
        "importers": ["provider-directory-fhir"],
    }
    assert not source.is_fhir_source_secret_selected(selection_by_field, SPEC.worker_class, request_by_field)
    assert source.fresh_route_annotations(SPEC, request_by_field) == {}
    request.fhir_source_launch = asdict(request.fhir_source_launch)
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._worker_job_manifest(SPEC, request, IMAGE)


@pytest.mark.usefixtures("configured_runtime")
def test_request_fields_cannot_supply_fresh_annotations():
    request = _admitted(_fresh_run())
    request.update(
        params={"source_ids": ["other-source"], "provider_directory_endpoint_scope": "https://other.example.test"},
        image=IMAGE,
        secret_name="other-secret",
        annotations={"healthporta.com/source-id": "other-source"},
    )
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    assert job["metadata"]["annotations"]["healthporta.com/source-id"] == "synthetic-source"
    assert job["metadata"]["annotations"]["healthporta.com/source-endpoint-scope"] == "https://example.test/fhir"
    assert job["spec"]["template"]["spec"]["containers"][0]["image"] == ORDINARY_IMAGE


@pytest.mark.usefixtures("configured_runtime")
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_id", "other-source"),
        ("endpoint_scope", "https://other.example.test/fhir"),
        ("image", IMAGE),
        ("secret_name", "other-secret"),
        ("retry_of_run_id", "run_" + "e" * 32),
    ],
)
def test_fresh_annotations_refuse_mutated_capability(field, value):
    request = _admitted(_fresh_run())
    request.fhir_source_launch = replace(request.fhir_source_launch, **{field: value})
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._worker_job_manifest(SPEC, request, IMAGE)


@pytest.mark.usefixtures("configured_runtime")
@pytest.mark.parametrize("field", ["worker_class", "queue", "role", "importers"])
def test_fresh_annotations_refuse_mutated_worker_spec(field):
    request = _admitted(_fresh_run())
    spec = replace(SPEC, **{field: ("other",) if field == "importers" else "other"})
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._worker_job_manifest(spec, request, IMAGE)


@pytest.mark.usefixtures("configured_runtime")
@pytest.mark.parametrize("key", ["healthporta.com/source-id", "healthporta.com/source-endpoint-scope"])
@pytest.mark.parametrize("value", [None, "other"])
def test_fresh_job_reuse_requires_exact_annotations(key, value):
    request = _admitted(_fresh_run())
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    if value is None:
        del job["metadata"]["annotations"][key]
    else:
        job["metadata"]["annotations"][key] = value
    assert not source.is_fhir_job_set_matching(SPEC, request, [job])


@pytest.mark.usefixtures("configured_runtime")
def test_fresh_and_exact_retry_routes_select_independently(monkeypatch):
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_fresh_route(), _source_route_config()]))
    retry_launch = source.admit_fhir_source_worker_launch(_source_route_run())
    assert retry_launch.root_run_id == _source_route_config()["root_run_id"]
    assert retry_launch.retry_of_run_id == _source_route_run()["params"]["retry_of_run_id"]
    assert retry_launch.image == ORDINARY_IMAGE
    assert source.admit_fhir_source_worker_launch(_fresh_run()).root_run_id == ""
    request = _admitted(_source_route_run())
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    assert job["metadata"]["annotations"]["healthporta.com/source-root-run-id"] == retry_launch.root_run_id
    assert source.fresh_route_annotations(SPEC, request) == {}
    assert not {"healthporta.com/source-id", "healthporta.com/source-endpoint-scope"}.intersection(
        job["metadata"]["annotations"]
    )


@pytest.mark.usefixtures("configured_runtime")
def test_fresh_route_does_not_implicitly_select_retained_retries():
    launch = source.admit_fhir_source_worker_launch(_source_route_run())
    assert launch == source.FHIRSourceWorkerLaunch(RUN_ID, JOB_ID, IMAGE)


@pytest.mark.usefixtures("configured_runtime")
@pytest.mark.parametrize("drift", ["duplicate", "invalid_root", "missing_root"])
def test_fresh_route_inventory_remains_strict(monkeypatch, drift):
    route = _fresh_route()
    routes = [route]
    if drift == "duplicate":
        duplicate = deepcopy(route)
        duplicate["secret_name"] = "different-secret"
        routes.append(duplicate)
    elif drift == "invalid_root":
        route["root_run_id"] = False
    else:
        del route["root_run_id"]
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps(routes))
    with pytest.raises(ValueError, match="fhir_source_worker_route_invalid"):
        source.admit_fhir_source_worker_launch(_fresh_run())
