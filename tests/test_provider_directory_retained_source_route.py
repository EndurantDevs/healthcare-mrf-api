# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit source routes preserve native retry ancestry and credential isolation."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_workers as workers
from api import provider_directory_source_publication as publication
from api import provider_directory_source_worker as source
from tests.test_provider_directory_fresh_source_route import _admitted, _fresh_route, _fresh_run, configured_runtime
from tests.test_provider_directory_source_proxy_launch import _BINDING, _cooldowns, enabled_proxy
from tests.test_provider_directory_source_worker_launch import (
    IMAGE,
    RUN_ID,
    SPEC,
    _mock_original_worker_control,
    _source_route_config,
)


def _retry_rows(depth=1):
    root = _source_route_config()["root_run_id"]
    root_by_field = {**_fresh_run(), "run_id": root, "status": "failed"}
    rows = [root_by_field]
    for index in range(depth):
        parent = rows[-1]["run_id"]
        rows.append(
            {
                **_fresh_run(),
                "run_id": RUN_ID if index == depth - 1 else "run_" + "c" * 32,
                "status": "queued" if index == depth - 1 else "failed",
                "retry_of_run_id": parent,
                "params": {
                    **root_by_field["params"],
                    "retry_of_run_id": parent,
                    "provider_directory_pagination_root_run_id": root,
                    "allow_retained_pagination_reset": False,
                },
            }
        )
    return rows


@pytest.fixture
def retained_route(monkeypatch, configured_runtime):
    route_by_field = {**_fresh_route(), "allow_retained_retries": True}
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([route_by_field]))
    return route_by_field


@pytest.mark.parametrize("depth", [1, 2])
async def test_retained_route_uses_native_chain_and_exact_executor(retained_route, depth):
    rows = _retry_rows(depth)
    request = _admitted(rows[-1])
    original = deepcopy(rows)
    connection = SimpleNamespace(all=AsyncMock(return_value=rows))
    request.fhir_source_launch = await source.require_fhir_source_route_ancestry(connection, request.fhir_source_launch)
    launch = request.fhir_source_launch
    assert (launch.root_run_id, launch.retry_of_run_id) == (rows[0]["run_id"], rows[-2]["run_id"])
    assert (launch.image, launch.secret_name) == (retained_route["image"], retained_route["secret_name"])
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    annotations = job["metadata"]["annotations"]
    assert annotations["healthporta.com/source-root-run-id"] == rows[0]["run_id"]
    assert annotations["healthporta.com/source-id"] == retained_route["source_id"]
    assert annotations["healthporta.com/source-endpoint-scope"] == retained_route["endpoint_scope"]
    pod = job["spec"]["template"]["spec"]
    assert pod["containers"][0]["image"] == retained_route["image"]
    assert [volume["secret"]["secretName"] for volume in pod["volumes"]] == [retained_route["secret_name"]]
    assert source.is_fhir_job_set_matching(SPEC, request, [job])
    assert rows == original
    connection.all.assert_awaited_once()


async def test_retained_route_checks_ancestry_before_kubernetes(monkeypatch, retained_route):
    rows = _retry_rows()
    connection, calls = _mock_original_worker_control(monkeypatch, rows[-1], rows[1:], {})
    result = await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"})
    assert result["status"] == "failed" and calls == []
    connection.all.assert_awaited_once()


@pytest.mark.parametrize("flag", [False, None])
def test_retained_route_is_opt_in(monkeypatch, configured_runtime, flag):
    route = _fresh_route()
    if flag is not None:
        route["allow_retained_retries"] = flag
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([route]))
    launch = _admitted(_retry_rows()[-1]).fhir_source_launch
    assert launch.image == IMAGE and not launch.secret_name and not launch.root_run_id


@pytest.mark.parametrize("damage", ["parent", "missing_parent", "root", "self_root", "sources", "endpoint", "status"])
def test_retained_route_refuses_partial_native_selection(retained_route, damage):
    row = _retry_rows()[-1]
    match damage:
        case "parent":
            row["retry_of_run_id"] = "run_" + "e" * 32
        case "missing_parent":
            del row["retry_of_run_id"]
        case "root" | "self_root":
            row["params"]["provider_directory_pagination_root_run_id"] = RUN_ID if damage == "self_root" else "invalid"
        case "sources":
            row["params"]["source_ids"].append("other-source")
        case "endpoint":
            row["params"]["provider_directory_endpoint_scope"] += "/other"
        case "status":
            row["status"] = "failed"
    with pytest.raises(ValueError, match="route_changed"):
        _admitted(row)


@pytest.mark.parametrize("damage", ["missing", "active", "source", "endpoint", "parent", "cycle", "overflow"])
async def test_retained_route_refuses_broken_ancestry(retained_route, damage):
    rows = _retry_rows(2)
    launch = _admitted(rows[-1]).fhir_source_launch
    match damage:
        case "missing":
            rows.pop(0)
        case "active":
            rows[0]["status"] = "running"
        case "source" | "endpoint":
            key = "source_ids" if damage == "source" else "provider_directory_endpoint_scope"
            rows[1]["params"][key] = ["other-source"] if damage == "source" else "https://other.example/fhir"
        case "parent":
            rows[-1]["retry_of_run_id"] = rows[0]["run_id"]
        case "cycle":
            rows[1]["retry_of_run_id"] = rows[-1]["run_id"]
            rows[1]["params"]["retry_of_run_id"] = rows[-1]["run_id"]
        case "overflow":
            rows.extend(deepcopy(rows[0]) for _ in range(63))
    with pytest.raises(ValueError, match="route_changed"):
        await source.require_fhir_source_route_ancestry(SimpleNamespace(all=AsyncMock(return_value=rows)), launch)


@pytest.mark.parametrize("field", ["image", "secret_name", "allow_retained_retries"])
def test_retained_route_revalidates_current_executor(monkeypatch, retained_route, field):
    request = _admitted(_retry_rows()[-1])
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    retained_route[field] = {"image": IMAGE, "secret_name": "other-secret", "allow_retained_retries": False}[field]
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([retained_route]))
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._worker_job_manifest(SPEC, request, IMAGE)
    assert not source.is_fhir_source_job_matching(job, request.fhir_source_launch)
    if field != "allow_retained_retries":
        current = _admitted(_retry_rows()[-1]).fhir_source_launch
        assert getattr(current, field) == retained_route[field]


def test_exact_root_route_precedes_retained_opt_in(monkeypatch, retained_route):
    exact_by_field = {**_source_route_config(), "image": IMAGE, "secret_name": "synthetic-default"}
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([retained_route, exact_by_field]))
    request = _admitted(_retry_rows()[-1])
    assert request.fhir_source_launch.image == IMAGE
    assert request.fhir_source_launch.secret_name == "synthetic-default"
    assert source.fresh_route_annotations(SPEC, request) == {}


@pytest.mark.parametrize("key", ["source-id", "source-endpoint-scope", "source-root-run-id"])
def test_retained_job_reuse_refuses_annotation_drift(retained_route, key):
    request = _admitted(_retry_rows()[-1])
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    del job["metadata"]["annotations"]["healthporta.com/" + key]
    assert not source.is_fhir_job_set_matching(SPEC, request, [job])


@pytest.mark.parametrize("mode", [{"seed_only": True}, {"publish_artifacts_only": True}, {"probe": False}])
def test_retained_opt_in_excludes_local_modes(retained_route, mode):
    row = _retry_rows()[-1]
    row["params"].update(mode)
    launch = _admitted(row).fhir_source_launch
    assert launch.image == IMAGE and not launch.secret_name and not launch.root_run_id


@pytest.mark.parametrize("value", [None, 0, 1, "true"])
def test_retained_opt_in_requires_boolean(monkeypatch, configured_runtime, value):
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([{**_fresh_route(), "allow_retained_retries": value}]))
    with pytest.raises(ValueError, match="route_invalid"):
        source._configured_routes()


@pytest.mark.parametrize("setting,root", [(source.ROUTES_ENV, "run_" + "b" * 32), (publication.ROUTES_ENV, None)])
def test_retained_opt_in_cannot_expand_root_or_publication_routes(monkeypatch, setting, root):
    route_by_field = {**_fresh_route(), "root_run_id": root, "allow_retained_retries": True}
    monkeypatch.setenv(setting, json.dumps([route_by_field]))
    with pytest.raises(ValueError, match="route_invalid"):
        source._configured_routes(setting=setting)


@pytest.mark.usefixtures("enabled_proxy")
async def test_retained_opt_in_restores_ancestor_proxy_cooldowns(retained_route):
    rows = _retry_rows(2)
    for row, boundary in zip(rows[:-1], ["2099-01-01T01:00:00Z", "2099-01-01T02:00:00Z"]):
        row.update(
            source_http_proxy_binding=deepcopy(_BINDING), source_http_proxy_cooldowns=_cooldowns([("egress", boundary)])
        )
    request = _admitted(rows[-1])
    assert request.fhir_source_launch.proxy_cooldowns_json == ""
    with pytest.raises(ValueError, match="cooldowns_invalid"):
        workers._worker_job_manifest(SPEC, request, IMAGE)
    request.fhir_source_launch = await source.require_fhir_source_route_ancestry(
        SimpleNamespace(all=AsyncMock(return_value=rows)), request.fhir_source_launch
    )
    assert json.loads(request.fhir_source_launch.proxy_cooldowns_json) == rows[1]["source_http_proxy_cooldowns"]
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    assert source.is_fhir_job_set_matching(SPEC, request, [job])
    rows[0]["source_http_proxy_cooldowns"] = None
    with pytest.raises(ValueError, match="cooldowns_invalid"):
        await source.require_fhir_source_route_ancestry(
            SimpleNamespace(all=AsyncMock(return_value=rows)), request.fhir_source_launch
        )
