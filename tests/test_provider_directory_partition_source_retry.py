# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stopped partition retries reuse the ordinary Source launch and native validation."""

from copy import deepcopy

import pytest

from api import control_workers as workers
from process.provider_directory_fhir import DEFAULT_RESOURCES
from tests.test_provider_directory_rooted_source_retry import configured_launch, rooted_rows
from tests.test_provider_directory_source_worker_launch import RUN_ID


def partition_rows(depth):
    lineage_records = rooted_rows(depth)
    for run_record in lineage_records:
        run_record["params"].pop("rooted_graph")
        run_record["params"].update(
            source_ids=["synthetic-source"], provider_directory_endpoint_scope="https://example.test/fhir"
        )
    if depth == 2:
        lineage_records[0]["status"] = "failed"
    parent = lineage_records[-2]
    scan_by_field = {
        "state": "complete",
        "attempt": 1,
        "consecutive_no_progress": 0,
        "error": None,
        "retry_not_before": None,
        "complete": True,
        "pages_processed": 1,
        "rows_processed": 0,
    }
    scan_by_resource = {name: dict(scan_by_field) for name in DEFAULT_RESOURCES}
    scan_by_resource["PractitionerRole"].update(
        state="fetching", complete=False, pages_processed=None, rows_processed=None
    )
    timing_by_resource = {name: {"fetch_mode": "checkpoint_complete"} for name in DEFAULT_RESOURCES}
    timing_by_resource["PractitionerRole"]["fetch_mode"] = "last_updated_partition"
    parent["source_audit"] = {
        **deepcopy(parent["params"]),
        "provider_directory_pagination_root_run_id": parent["params"].get("provider_directory_pagination_root_run_id"),
        "provider_directory_endpoint_scope_resolved": "https://example.test/fhir",
        "pagination_checkpoints_enabled": True,
        "full_refresh": True,
        "bulk_export": False,
        "allow_retained_pagination_reset": False,
        "resource_limit": 0,
        "page_limit": 0,
        "active_source_groups": [
            {
                "source_ids": ["synthetic-source"],
                "source_count": 1,
                "api_base": "https://example.test/fhir",
                "resource_fetch_timing": timing_by_resource,
                "resource_scans": scan_by_resource,
            }
        ],
    }
    return lineage_records


@pytest.mark.asyncio
@pytest.mark.parametrize("depth", [1, 2])
async def test_stopped_partition_chain_uses_existing_source_worker_without_reset(monkeypatch, depth):
    lineage_records = partition_rows(depth)
    before = deepcopy(lineage_records)
    _connection, calls, route = configured_launch(monkeypatch, lineage_records)
    result = await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"})
    assert result["status"] == "started"
    jobs = [body for method, _path, body in calls if method == "POST"]
    assert len(jobs) == 1
    assert jobs[0]["spec"]["template"]["spec"]["containers"][0]["image"] == route["image"]
    assert lineage_records == before
    assert lineage_records[-1]["params"]["allow_retained_pagination_reset"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("index", "path", "value"),
    [
        (-2, ("source_audit",), None),
        (-2, ("source_audit", "active_source_groups"), []),
        (-2, ("source_audit", "active_source_groups"), [{}, {}]),
        (-2, ("source_audit", "provider_directory_pagination_root_run_id"), "run_" + "e" * 32),
        (-2, ("source_audit", "provider_directory_pagination_root_run_id"), None),
        (-2, ("source_audit", "retry_of_run_id"), "run_" + "e" * 32),
        (-2, ("source_audit", "source_ids"), ["other-source"]),
        (-2, ("source_audit", "provider_directory_endpoint_scope_resolved"), "https://other.test/fhir"),
        (-2, ("source_audit", "allow_retained_pagination_reset"), True),
        (-2, ("source_audit", "pagination_checkpoints_enabled"), False),
        (-2, ("source_audit", "full_refresh"), False),
        (-2, ("source_audit", "bulk_export"), True),
        (-2, ("source_audit", "source_http_proxy"), {"binding": {}}),
        (-2, ("source_audit", "source_http_proxy"), {"cooldowns": {}}),
        (-2, ("source_audit", "source_http_proxy"), {"enabled": True}),
        (-2, ("source_audit", "resource_limit"), True),
        (-2, ("source_audit", "page_limit"), 1),
        (-2, ("source_audit", "active_source_groups", 0, "source_count"), True),
        (-2, ("source_audit", "active_source_groups", 0, "api_base"), "https://other.test/fhir"),
        (-2, ("source_audit", "active_source_groups", 0, "resource_fetch_timing"), {}),
        (-2, ("source_audit", "active_source_groups", 0, "resource_fetch_timing", "InsurancePlan"), {}),
        (-2, ("source_audit", "active_source_groups", 0, "resource_scans"), {}),
        (-2, ("source_audit", "active_source_groups", 0, "resource_fetch_timing", "PractitionerRole"), None),
        (
            -2,
            ("source_audit", "active_source_groups", 0, "resource_fetch_timing", "PractitionerRole", "fetch_mode"),
            "rest",
        ),
        (-2, ("source_audit", "active_source_groups", 0, "resource_scans", "PractitionerRole", "state"), "complete"),
        (-2, ("source_audit", "active_source_groups", 0, "resource_scans", "PractitionerRole", "error"), "failed"),
        (-2, ("source_audit", "active_source_groups", 0, "resource_scans", "InsurancePlan", "complete"), False),
        (-2, ("source_audit", "active_source_groups", 0, "resource_scans", "InsurancePlan", "pages_processed"), True),
        (-2, ("source_audit", "active_source_groups", 0, "resource_scans", "InsurancePlan", "rows_processed"), -1),
        (-2, ("source_http_proxy_binding",), {}),
        (-2, ("source_http_proxy_cooldowns",), {}),
        (-2, ("finished_at",), None),
        (-2, ("started_at",), None),
        (-1, ("params", "page_limit"), 1),
        (-1, ("params", "allow_retained_pagination_reset"), True),
    ],
)
async def test_partition_retry_refuses_missing_or_changed_scope(monkeypatch, index, path, value):
    lineage_records = partition_rows(2)
    selected = lineage_records[index]
    for key in path[:-1]:
        selected = selected[key]
    selected[path[-1]] = value
    _connection, calls, _route = configured_launch(monkeypatch, lineage_records)
    result = await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"})
    assert result["status"] == "failed"
    assert all(method == "GET" for method, _path, _body in calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("presence", ["active", "unavailable"])
async def test_partition_retry_requires_fresh_worker_quiescence(monkeypatch, presence):
    lineage_records = partition_rows(2)
    _connection, calls, _route = configured_launch(
        monkeypatch, lineage_records, active=presence == "active", unavailable=presence == "unavailable"
    )
    result = await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"})
    assert result["status"] == "failed"
    assert all(method == "GET" for method, _path, _body in calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(("depth", "parent_attempt", "child_attempt"), [(1, None, 2), (2, None, 3), (2, 4, 5)])
async def test_partition_retry_keeps_existing_v2_lineage_deltas(monkeypatch, depth, parent_attempt, child_attempt):
    lineage_records = partition_rows(depth)
    if parent_attempt is not None:
        lineage_records[-2]["params"]["provider_directory_pagination_attempt"] = parent_attempt
    lineage_records[-1]["params"]["provider_directory_pagination_attempt"] = child_attempt
    _connection, calls, _route = configured_launch(monkeypatch, lineage_records)
    result = await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"})
    assert result["status"] == "started"
    assert sum(method == "POST" for method, _path, _body in calls) == 1


@pytest.mark.asyncio
async def test_partition_retry_preserves_multiple_stopped_ancestors(monkeypatch):
    lineage_records = partition_rows(2)
    predecessor = lineage_records[-2]
    predecessor["params"]["provider_directory_pagination_attempt"] = 4
    parent = deepcopy(predecessor)
    parent["run_id"] = "run_" + "d" * 32
    parent["retry_of_run_id"] = predecessor["run_id"]
    parent["params"].update(retry_of_run_id=predecessor["run_id"], provider_directory_pagination_attempt=5)
    parent["source_audit"]["retry_of_run_id"] = predecessor["run_id"]
    lineage_records[-1]["retry_of_run_id"] = parent["run_id"]
    lineage_records[-1]["params"].update(retry_of_run_id=parent["run_id"], provider_directory_pagination_attempt=6)
    lineage_records.insert(-1, parent)
    before = deepcopy(lineage_records)
    _connection, calls, _route = configured_launch(monkeypatch, lineage_records)
    result = await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"})
    assert result["status"] == "started"
    assert sum(method == "POST" for method, _path, _body in calls) == 1
    assert lineage_records == before


@pytest.mark.asyncio
async def test_partition_retry_ignores_request_supplied_strategy_snapshot(monkeypatch):
    lineage_records = partition_rows(2)
    snapshot = lineage_records[-2].pop("source_audit")
    for run_record in lineage_records[-2:]:
        run_record["params"]["source_audit"] = deepcopy(snapshot)
    _connection, calls, _route = configured_launch(monkeypatch, lineage_records)
    result = await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"})
    assert result["status"] == "failed"
    assert all(method == "GET" for method, _path, _body in calls)


@pytest.mark.asyncio
async def test_original_partition_retry_requires_the_stored_null_root_field(monkeypatch):
    lineage_records = partition_rows(1)
    lineage_records[0]["source_audit"].pop("provider_directory_pagination_root_run_id")
    _connection, calls, _route = configured_launch(monkeypatch, lineage_records)
    result = await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"})
    assert result["status"] == "failed"
    assert all(method == "GET" for method, _path, _body in calls)
