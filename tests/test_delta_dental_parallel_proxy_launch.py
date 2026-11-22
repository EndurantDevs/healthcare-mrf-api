# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify the protected Delta route selects its own immutable proxy pool."""

import json
from dataclasses import replace

import pytest

from api import control_workers as workers
from api import provider_directory_source_worker as source
from api.provider_directory_source_publication import ROUTES_ENV as PUBLICATION_ROUTES_ENV
from process.delta_dental_directory_json import SOURCE_ENDPOINT, SOURCE_ID
from process.delta_dental_directory_worker import MANIFEST_PATH
from tests.test_provider_directory_source_proxy_launch import (
    _POLICY,
    _admitted,
    _environment,
    _fresh_route,
    _fresh_run,
    _pool_selector,
    configured_runtime,
    enabled_proxy,
)
from tests.test_provider_directory_source_worker_launch import IMAGE, SPEC

_POOL = {"revision": "example-dedicated-revision", "secret_name": "example-dedicated-pool"}


@pytest.fixture
def delta_route(monkeypatch, enabled_proxy):
    """Keep a regular Source and the dedicated Delta route available together."""
    route_by_field = {
        **_fresh_route(),
        "source_id": SOURCE_ID,
        "endpoint_scope": SOURCE_ENDPOINT,
        "secret_name": "example-delta-plan",
        "proxy_pool": _POOL,
    }
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_fresh_route(), route_by_field]))
    rule_by_field = {"source_id": SOURCE_ID, "endpoint_scope": SOURCE_ENDPOINT, "use_proxy": True}
    monkeypatch.setenv(
        source.PROXY_POLICY_ENV, json.dumps({**_POLICY, "sources": [*_POLICY["sources"], rule_by_field]})
    )
    selector_by_field = {**_pool_selector(), "secretName": _POOL["secret_name"]}
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", json.dumps([_pool_selector(), selector_by_field]))
    mount_by_field = {
        "purposes": [source.PURPOSE],
        "workerClasses": [SPEC.worker_class],
        "importers": ["provider-directory-fhir"],
        "secretName": route_by_field["secret_name"],
        "mountPath": MANIFEST_PATH,
        "subPath": "plan.json",
        "readOnly": True,
    }
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON", json.dumps([mount_by_field]))
    return route_by_field


def test_delta_job_mounts_only_the_bound_pool(delta_route):
    """The global pool remains available to other admitted Sources."""
    recorded = _fresh_run()
    recorded["params"] = {
        "source_ids": [SOURCE_ID],
        "provider_directory_endpoint_scope": SOURCE_ENDPOINT,
        "delta_dental_directory_observation": True,
        "probe": False,
        "import_resources": False,
    }
    request = _admitted(recorded)
    launch = request.fhir_source_launch
    assert launch.pool_revision == _POOL["revision"]
    assert launch.pool_secret_name == _POOL["secret_name"]
    assert launch.pool_secret_name in source.configured_source_proxy_pool_secrets()
    assert source.needs_source_proxy_admission({"name": "ANY_NAME", "secretName": launch.pool_secret_name})
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    assert _environment(job)[source.PROXY_POOL_ENV]["valueFrom"]["secretKeyRef"]["name"] == launch.pool_secret_name
    assert source.is_fhir_source_job_matching(job, launch)
    assert _admitted(_fresh_run()).fhir_source_launch.pool_secret_name == _pool_selector()["secretName"]
    with pytest.raises(ValueError, match="admission_unavailable"):
        source._require_proxy_launch(replace(launch, pool_secret_name=_pool_selector()["secretName"]))


@pytest.mark.parametrize(
    "change",
    [
        {"source_id": "different-source"},
        {"endpoint_scope": "https://different.example"},
        {"proxy_pool": None},
        {"proxy_pool": {**_POOL, "credentials": "example"}},
        {"proxy_pool": {**_POOL, "revision": ""}},
        {"proxy_pool": {**_POOL, "secret_name": "Missing-DNS-label"}},
        {"proxy_pool": {**_POOL, "secret_name": "example-delta-plan"}},
        {"proxy_pool_transition": None},
    ],
)
def test_delta_pool_refuses_malformed_or_unrelated_route(monkeypatch, delta_route, change):
    """Reject bad bindings before constructing a worker manifest."""
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([delta_route | change]))
    with pytest.raises(ValueError, match="route_invalid"):
        source._configured_routes()


def test_delta_pool_cannot_enter_publication_route(monkeypatch, delta_route):
    """Only the acquisition launcher accepts the pool override."""
    monkeypatch.setenv(PUBLICATION_ROUTES_ENV, json.dumps([delta_route]))
    with pytest.raises(ValueError, match="route_invalid"):
        source._configured_routes(setting=PUBLICATION_ROUTES_ENV)
