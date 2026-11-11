# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Normal Source launch after stopped rooted cancellation keeps exact authority."""

import json
import urllib.parse
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from api import control_workers as workers
from api import provider_directory_source_worker as source
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.provider_directory_rooted_graph_worker import ACQUISITION_FLAGS
from tests.test_provider_directory_rooted_graph_worker import operation
from tests.test_provider_directory_source_worker_launch import (
    IMAGE,
    ORDINARY_IMAGE,
    RUN_ID,
    SPEC,
    _both_source_secrets,
    _mock_original_worker_control,
    _recorded,
)


def rooted_rows(depth):
    root_id = "run_" + "b" * 32
    params_by_name = {
        **ACQUISITION_FLAGS,
        "rooted_graph": operation(),
        "source_ids": [PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID],
        "provider_directory_endpoint_scope": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    }
    lineage_rows = [
        {
            **_recorded(),
            "run_id": root_id,
            "status": "canceled",
            "retry_of_run_id": None,
            "params": params_by_name,
            "started_at": datetime(2026, 1, 1, tzinfo=UTC),
            "finished_at": datetime(2026, 1, 2, tzinfo=UTC),
            "phase_detail": "process_data canceled",
        }
    ]
    for index in range(depth):
        parent_id = lineage_rows[-1]["run_id"]
        lineage_rows.append(
            {
                **_recorded(),
                "run_id": RUN_ID if index == depth - 1 else "run_" + "c" * 32,
                "status": "queued" if index == depth - 1 else "canceled",
                "retry_of_run_id": parent_id,
                "params": {
                    **deepcopy(params_by_name),
                    "retry_of_run_id": parent_id,
                    "provider_directory_pagination_root_run_id": root_id,
                },
                "started_at": datetime(2026, 1, 3, tzinfo=UTC),
                "finished_at": datetime(2026, 1, 4, tzinfo=UTC),
            }
        )
    return lineage_rows


def configured_launch(monkeypatch, lineage_rows, *, active=False, unavailable=False):
    """Keep real Source admission, ancestry, manifest rendering and worker creation."""
    route_by_field = {
        "root_run_id": None,
        "source_id": lineage_rows[-1]["params"]["source_ids"][0],
        "endpoint_scope": lineage_rows[-1]["params"]["provider_directory_endpoint_scope"],
        "image": ORDINARY_IMAGE,
        "secret_name": "synthetic-successor",
        "allow_retained_retries": True,
    }
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([route_by_field]))
    _both_source_secrets(monkeypatch)
    connection, calls = _mock_original_worker_control(monkeypatch, lineage_rows[-1], lineage_rows, {})
    ancestor_by_selector = {
        workers._kubernetes_label_selector(
            SPEC, {"run_id": lineage_record["run_id"], "importer": "provider-directory-fhir"}
        ): lineage_record["run_id"]
        for lineage_record in lineage_rows[:-1]
    }

    def kubernetes(method, path, body=None):
        calls.append((method, path, body))
        if method == "POST":
            return body
        selector = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query).get("labelSelector", [None])[0]
        if selector in ancestor_by_selector:
            workers.acquire_control_run_worker_action_lock.assert_any_await(connection, ancestor_by_selector[selector])
            if unavailable:
                raise RuntimeError("synthetic lookup unavailable")
            if active:
                return {"items": [{"status": {"active": 1, "conditions": [], "phase": "Running"}}]}
        return {"items": []}

    monkeypatch.setattr(workers, "_kubernetes_request", kubernetes)
    return connection, calls, route_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("depth", [1, 2])
async def test_started_canceled_rooted_chain_launches_exact_v1_route_after_stop(monkeypatch, depth):
    lineage_rows = rooted_rows(depth)
    before = deepcopy(lineage_rows)
    connection, calls, route_by_field = configured_launch(monkeypatch, lineage_rows)
    result = await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"})
    assert result["status"] == "started"
    jobs = [body for method, _path, body in calls if method == "POST"]
    assert len(jobs) == 1
    job = jobs[0]
    pod = job["spec"]["template"]["spec"]
    assert pod["containers"][0]["image"] == route_by_field["image"]
    assert pod["containers"][0]["command"] == ["/usr/local/bin/fhir-source-worker"]
    assert [volume["secret"]["secretName"] for volume in pod["volumes"]] == [route_by_field["secret_name"]]
    assert job["metadata"]["annotations"]["healthporta.com/source-root-run-id"] == lineage_rows[0]["run_id"]
    assert lineage_rows == before
    for ancestor in lineage_rows[:-1]:
        workers.acquire_control_run_worker_action_lock.assert_any_await(connection, ancestor["run_id"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    [
        "operation",
        "selector",
        "intermediate",
        "generic",
        "active",
        "unavailable",
        "unfinished",
        "proxy",
        "invalid_contract",
        "nonroot_parent",
    ],
)
async def test_rooted_canceled_retry_refuses_changed_or_active_authority(monkeypatch, drift):
    lineage_rows = rooted_rows(2 if drift == "intermediate" else 1)
    if drift in {"operation", "intermediate"}:
        changed_index = 1 if drift == "intermediate" else 0
        lineage_rows[changed_index]["params"]["rooted_graph"]["operation_key"] = "0" * 64
    if drift == "selector":
        lineage_rows[0]["params"]["rooted_graph"]["publication_acquisition_id"] = "pdrga_" + "0" * 48
    if drift == "generic":
        for lineage_record in lineage_rows:
            lineage_record["params"].pop("rooted_graph")
            lineage_record["params"].update(
                source_ids=["synthetic-source"], provider_directory_endpoint_scope="https://example.test/fhir"
            )
    if drift == "nonroot_parent":
        lineage_rows[0]["retry_of_run_id"] = "run_" + "e" * 32
    if drift == "invalid_contract":
        lineage_rows[0]["params"]["full_refresh"] = False
    if drift == "unfinished":
        lineage_rows[0]["finished_at"] = None
    if drift == "proxy":
        lineage_rows[0]["source_http_proxy_binding"] = {}
    _connection, calls, _route = configured_launch(
        monkeypatch, lineage_rows, active=drift == "active", unavailable=drift == "unavailable"
    )
    result = await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"})
    assert result["status"] == "failed"
    assert all(method == "GET" for method, _path, _body in calls)
