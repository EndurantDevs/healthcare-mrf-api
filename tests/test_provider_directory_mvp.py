# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Direct-directory identity, common chunk configuration and transport gates."""

import contextlib
import dataclasses
import datetime
from unittest.mock import AsyncMock

import pytest

from api.provider_directory_sources import provider_directory_source_catalog
from process import provider_directory_mvp as mvp
from tests.test_process_provider_directory_fhir_unit import (
    _parallel_resource_source,
    _patch_resource_scan_storage,
    _resource_concurrency_options,
    _source_http_test_session,
    importer,
)

SOURCE_ID = "pdfhir_85af3c3ea1fc08c2516ef15d"


def test_mvp_seed_identity_and_complete_resource_contract_match_catalog(monkeypatch):
    monkeypatch.setattr(importer, "_remote_supplemental_catalogs", lambda **_kwargs: ([], {}))
    rows, metrics = importer._seed_rows_from_supplemental_catalogs(source_query="MVP Health Care")
    assert len(rows) == 1 and metrics["catalogs"]["mvp_health_care"]["rows"] == 1
    source = importer._source_row_from_seed(rows[0])
    entry = next(x for x in provider_directory_source_catalog()["items"] if x["entry_id"] == "mvp-health-care")
    assert source["source_id"] == SOURCE_ID and entry["source_ids"] == [SOURCE_ID]
    assert source["plan_name"] is None and source["canonical_api_base"] == mvp.API_BASE
    assert entry["runnable"] and entry["profile_enabled"]
    assert set(entry["resources"]) == set(mvp.RESOURCES)
    assert importer._initial_resource_import_source_decision(source, list(mvp.RESOURCES)) is None
    for resource in mvp.RESOURCES:
        assert (
            importer._resource_start_url(source, resource, page_count=1000) == mvp.API_BASE + f"/{resource}?_count=100"
        )
        config, _ = importer._last_updated_partition_config(source, resource)
        if resource in mvp.CHUNKED_RESOURCES:
            assert config.versioned_search_limit == 50000 and config.ceiling == 10000
            assert config.minimum_width == config.boundary_precision == datetime.timedelta(seconds=1)
            assert config.end_mode == "resolved_now" and config.volatile_metadata_paths == ()
        else:
            assert config is None
    assert importer._source_resource_fetch_order(source, entry["resources"]) == list(mvp.RESOURCES)
    assert (
        importer._source_row_from_seed({"org_name": "MVP Health Care", "api_base": "https://example.test/legacy"})[
            "api_base"
        ]
        == "https://example.test/legacy"
    )


def test_mvp_seed_metadata_is_fresh_between_attempts():
    first = mvp.seed_rows()[0]
    first["metadata_json"]["provider_directory_supported_resources"].clear()
    first["metadata_json"][importer.LAST_UPDATED_PARTITION_METADATA_KEY]["resources"].clear()
    assert mvp.seed_rows()[0]["metadata_json"]["provider_directory_supported_resources"] == list(mvp.RESOURCES)
    assert len(mvp.seed_rows()[0]["metadata_json"][importer.LAST_UPDATED_PARTITION_METADATA_KEY]["resources"]) == 4


def test_mvp_second_precision_chunks_include_the_fractional_root_end_second():
    source_record = importer._source_row_from_seed(mvp.seed_rows()[0])
    config, _ = importer._last_updated_partition_config(source_record, "Practitioner")
    start = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    config = dataclasses.replace(
        config, start=start, end=start + datetime.timedelta(seconds=3.25), end_mode="fixed", ceiling=1
    )
    plan = importer._new_last_updated_partition_plan(config)
    assert plan.windows["root"].end == start + datetime.timedelta(seconds=4)
    resources = [
        {"id": f"record-{second}", "meta": {"lastUpdated": (start + datetime.timedelta(seconds=second)).isoformat()}}
        for second in range(4)
    ]

    def second_precision_search(window):
        window_start = window.start.replace(microsecond=0)
        window_end = window.end.replace(microsecond=0)
        return [
            resource
            for resource in resources
            if window_start <= importer.parse_utc_instant(resource["meta"]["lastUpdated"]) < window_end
        ]

    while plan.pending_count_windows():
        window = plan.pending_count_windows()[0]
        plan.observe_count(window.window_id, importer.CountObservation.exact(len(second_precision_search(window))))
    assert all(window.start.microsecond == window.end.microsecond == 0 for window in plan.windows.values())
    assert sum(window.count for window in plan.leaf_windows()) == 4
    returned_ids = set()
    for window in plan.leaf_windows():
        selected_resources = second_precision_search(window)
        returned_ids.update(resource["id"] for resource in selected_resources)
        for pass_number in (1, 2):
            plan.record_pass(window.window_id, pass_number, selected_resources, complete=True)
    assert plan.status is importer.PlanStatus.SUCCEEDED
    assert returned_ids == {resource["id"] for resource in resources}


def test_mvp_plan_labels_do_not_replace_native_network_membership():
    _, plan_row = importer.parse_fhir_resource(
        SOURCE_ID,
        {
            "resourceType": "InsurancePlan",
            "id": "plan-a",
            "name": "Shared label",
            "ownedBy": {"reference": "Organization/network-a"},
        },
        run_id="run-synthetic",
    )
    assert plan_row["network_refs"] == []
    _, role_row = importer.parse_fhir_resource(
        SOURCE_ID,
        {
            "resourceType": "PractitionerRole",
            "id": "role-a",
            "practitioner": {"reference": "Practitioner/person-a"},
            "location": [{"reference": "Location/office-a"}],
            "extension": [
                {
                    "url": "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference",
                    "valueReference": {"reference": "Organization/network-b"},
                }
            ],
        },
        run_id="run-synthetic",
    )
    assert role_row["source_id"] == SOURCE_ID
    assert role_row["network_refs"] == ["Organization/network-b"]
    assert role_row["location_refs"] == ["Location/office-a"]


@pytest.mark.parametrize(
    "overrides,expected", [({}, 8), ({"source_concurrency": 2}, 1), ({"checkpointing_enabled": False}, 1)]
)
def test_mvp_uses_shared_parallel_safety_gates(monkeypatch, overrides, expected):
    source = _parallel_resource_source()
    source.update(importer._source_row_from_seed(mvp.seed_rows()[0]))
    source["_partition_checkpoint_source_ids"] = (SOURCE_ID,)
    source["_pagination_checkpoint_context"] = dataclasses.replace(
        source["_pagination_checkpoint_context"], canonical_api_base=mvp.API_BASE, source_ids=(SOURCE_ID,)
    )
    _patch_resource_scan_storage(monkeypatch, True)
    options = _resource_concurrency_options(resource_types=mvp.RESOURCES, requested_concurrency=8, **overrides)
    with _source_http_test_session():
        assert importer._effective_resource_scan_concurrency(source, options) == expected


@pytest.mark.asyncio
async def test_capped_run_forces_shared_verified_session(monkeypatch):
    calls = []

    @contextlib.asynccontextmanager
    async def session(**kwargs):
        calls.append(kwargs)
        yield

    monkeypatch.setattr(importer, "_source_http_session_scope", session)
    monkeypatch.setattr(importer, "_import_resources", AsyncMock(return_value={}))
    source = importer._source_row_from_seed(mvp.seed_rows()[0])
    await importer._import_resources_with_source_http_session([source], resource_scan_concurrency=8)
    assert calls == [{"force": True, "require_verified_tls": True, "request_concurrency": 8, "source_concurrency": 1}]


@pytest.mark.asyncio
async def test_capped_transport_preserves_issued_bytes_and_refuses_redirects(monkeypatch):
    source = importer._source_row_from_seed(mvp.seed_rows()[0])
    fetch = AsyncMock(return_value=(200, {}, None, 1))
    monkeypatch.setattr(importer, "_fetch_json_with_source_session", fetch)
    url = mvp.API_BASE + "/Practitioner?page=2&queryId=opaque%2B%2F%3D"
    with _source_http_test_session():
        await importer._fetch_source_json_unpaced(source, url, timeout=60)
        assert fetch.call_args.args[1] == url
        assert fetch.call_args.kwargs == {"timeout": 60, "allow_redirects": False, "preserve_url_bytes": True}
    with pytest.raises(RuntimeError, match="capped_search_session_required"):
        await importer._fetch_source_json_unpaced(source, url, timeout=60)


@pytest.mark.parametrize("order", [["Patient"], ["Location", "Location"], "Location", [False]])
def test_resource_priority_rejects_invalid_configuration(order):
    source = importer._source_row_from_seed(mvp.seed_rows()[0])
    source["metadata_json"]["provider_directory_resource_fetch_order"] = order
    with pytest.raises(ValueError, match="fetch_order_invalid"):
        importer._source_resource_fetch_order(source, list(mvp.RESOURCES))
