# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import dataclasses
import importlib
import typing
from unittest.mock import AsyncMock

import pytest

from tests.test_process_provider_directory_fhir_unit import (
    _assert_no_rest_page_prefetch_task,
    _RestPageHarness,
    _source_http_test_session,
    _stub_resource_import_metadata,
    unit_checkpoint_catalog,
    unit_source_factory,
)

importer = importlib.import_module("process.provider_directory_fhir")
BASE = "https://directory.example.test/fhir"
SOURCE = {"source_id": "source-test", "api_base": BASE}


def _result(resource_type="Practitioner", rows=0, fetch_mode="paged"):
    return importer.ResourceFetchResult(
        model=importer.RESOURCE_MODELS_BY_TYPE[resource_type],
        rows=[],
        rows_fetched=rows,
        rows_written=rows,
        pages_fetched=int(rows > 0),
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
        fetch_mode=fetch_mode,
    )


def test_extracted_fetch_types_keep_aliases_defaults_and_forward_hints():
    fetch_types = importlib.import_module("process.provider_directory_fetch_types")
    assert importer.ResourceFetchResult is fetch_types.ResourceFetchResult
    assert importer.PaginationCheckpointContext is fetch_types.PaginationCheckpointContext
    assert typing.get_type_hints(importer.RestPagePrefetchEligibility)["checkpoint_context"] == (
        importer.PaginationCheckpointContext | None
    )
    assert not _result().is_bounded
    assert dataclasses.replace(_result(), deadline_reached=True).is_bounded
    first = importer.PartitionFetchState(importer.ProviderDirectoryPractitioner, [])
    second = importer.PartitionFetchState(importer.ProviderDirectoryPractitioner, [])
    assert first.state_lock is not second.state_lock
    assert first.seen_urls is not second.seen_urls


async def _fetch(callback, **overrides):
    return await importer._fetch_resource_rows(
        **{
            "source_record": dict(SOURCE),
            "resource_type": "Practitioner",
            "per_resource_limit": 0,
            "page_limit": 0,
            "page_count": 100,
            "timeout": 3,
            "run_id": "run-test",
            "timing_progress_callback": callback,
            **overrides,
        }
    )


async def _run_pages(scan, callback):
    with _source_http_test_session(scan.source_session):
        return await _fetch(
            callback,
            source_record=scan.source,
            row_batch_handler=scan._write_rows,
            row_batch_size=1000,
            retain_rows=False,
            pagination_checkpoint=scan.checkpoint_context,
        )


@pytest.mark.asyncio
async def test_nonpilot_rest_reports_page_timings_before_collection_completes(monkeypatch):
    snapshots = []
    writes = AsyncMock(side_effect=lambda _model, rows: len(rows))
    calls = []

    async def fetch(_source, url, **_kwargs):
        calls.append(url)
        if len(calls) == 2:
            assert snapshots[-1]["attempt_pages_fetched"] == 1
            assert snapshots[-1]["attempt_rows_written"] == 1
        payload = {
            "resourceType": "Bundle",
            "type": "searchset",
            "entry": [{"resource": {"resourceType": "Practitioner", "id": str(len(calls))}}],
        }
        if len(calls) == 1:
            payload["link"] = [{"relation": "next", "url": BASE + "/Practitioner?cursor=next"}]
        return 200, payload, None, 7

    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    fetch_result = await _fetch(
        snapshots.append,
        row_batch_handler=writes,
        row_batch_size=1,
        retain_rows=False,
    )

    assert fetch_result.complete and fetch_result.rows_fetched == 2
    measured = snapshots[-1]
    assert measured["timing_available"] is True
    assert measured["timing_scope"] == "paged_acquisition_loop"
    assert measured["attempt_pages_fetched"] == measured["attempt_rows_fetched"] == 2
    assert measured["attempt_rows_written"] == 2
    assert measured["source_fetch_elapsed_ms"] == 14
    assert measured["attempt_elapsed_seconds"] >= 0
    assert measured["stream_write_elapsed_seconds"] >= 0
    assert measured["checkpoint_persist_elapsed_seconds"] == 0
    assert writes.await_count == 2
    assert not any("url" in key or "payload" in key for key in measured)


@pytest.mark.asyncio
@pytest.mark.usefixtures("unit_checkpoint_catalog")
async def test_resumed_rows_are_separate_from_current_attempt(monkeypatch):
    scan = _RestPageHarness(monkeypatch, prefetch=False)
    scan.resume_state = importer.PaginationResumeState(
        scan.urls[1],
        10,
        1000,
        (),
        resumed=True,
    )
    snapshots = []

    fetch_result = await _run_pages(scan, snapshots.append)

    assert fetch_result.rows_fetched == 1001
    measured = snapshots[-1]
    assert measured["attempt_pages_fetched"] == measured["attempt_rows_fetched"] == 1
    assert measured["attempt_rows_written"] == 1
    assert measured["resumed_rows_processed"] == 1000
    assert measured["lineage_rows_processed"] == 1001
    assert measured["resumed_pages_processed"] == 10
    assert measured["lineage_pages_processed"] == 11


@pytest.mark.asyncio
@pytest.mark.usefixtures("unit_checkpoint_catalog")
async def test_expired_resume_keeps_attempt_work_distinct_from_restarted_lineage(monkeypatch):
    scan = _RestPageHarness(monkeypatch, pages=3, prefetch=False)
    scan.resume_state = importer.PaginationResumeState(scan.urls[1], 10, 1000, (), resumed=True)
    terminal_response = scan.responses[scan.urls[2]]
    expiry_state_map = {"has_expired": False}

    async def expire_once(page_index):
        if page_index == 2:
            scan.responses[scan.urls[2]] = (
                terminal_response if expiry_state_map["has_expired"] else (410, None, None, 5)
            )
            expiry_state_map["has_expired"] = True

    scan.fetch_hook = expire_once
    monkeypatch.setattr(importer, "_reset_pagination_checkpoint", AsyncMock())
    snapshots = []

    fetch_result = await _run_pages(scan, snapshots.append)

    assert fetch_result.complete and fetch_result.rows_fetched == fetch_result.rows_written == 3
    measured = snapshots[-1]
    assert measured["pagination_restarted"] is True
    assert measured["attempt_pages_fetched"] == measured["attempt_rows_fetched"] == 4
    assert measured["attempt_rows_written"] == 4
    assert measured["lineage_pages_processed"] == measured["lineage_rows_processed"] == 3
    assert measured["resumed_rows_processed"] == 1000
    counts = [snapshot["attempt_rows_fetched"] for snapshot in snapshots if snapshot["timing_available"]]
    assert counts == sorted(counts)


@pytest.mark.asyncio
async def test_completed_checkpoint_reports_zero_attempt_work_without_invented_timings(monkeypatch):
    scan = _RestPageHarness(monkeypatch, prefetch=False)
    scan.resume_state = importer.PaginationResumeState(None, 10, 1000, (), complete=True, resumed=True)
    snapshots = []

    fetch_result = await _run_pages(scan, snapshots.append)

    assert fetch_result.complete and not scan.requests
    measured = snapshots[-1]
    assert measured["fetch_mode"] == "checkpoint_complete"
    assert measured["attempt_rows_fetched"] == measured["attempt_pages_fetched"] == 0
    assert measured["lineage_rows_processed"] == 1000
    assert measured["timing_available"] is False
    assert "source_fetch_elapsed_ms" not in measured


@pytest.mark.asyncio
@pytest.mark.usefixtures("unit_checkpoint_catalog")
async def test_timing_callback_failure_cancels_and_joins_prefetch(monkeypatch):
    scan = _RestPageHarness(monkeypatch)

    async def block_second_page(page_index):
        if page_index == 1:
            await asyncio.Event().wait()

    async def wait_for_prefetch(resource_ids):
        if resource_ids == ["prac-1"]:
            await asyncio.wait_for(scan.fetch_started[1].wait(), 1)

    def fail_after_page(snapshot):
        if snapshot.get("attempt_pages_fetched") == 1:
            raise RuntimeError("synthetic_timing_failure")

    scan.fetch_hook = block_second_page
    scan.write_hook = wait_for_prefetch
    with pytest.raises(RuntimeError, match="synthetic_timing_failure"):
        await _run_pages(scan, fail_after_page)

    assert scan.fetch_cancelled[1].is_set()
    assert scan.active_fetches == 0
    _assert_no_rest_page_prefetch_task()


@pytest.mark.parametrize(
    "mode", ["bulk_export", "partitioned_paged", "last_updated_partition", "source_specific_deferred"]
)
@pytest.mark.asyncio
async def test_specialized_modes_explicitly_report_unavailable_timings(monkeypatch, mode):
    source_by_field = dict(SOURCE)
    resource_type = "Practitioner"
    fetch_result = _result(fetch_mode=mode)
    if mode == "bulk_export":
        monkeypatch.setattr(importer, "_is_source_bulk_export_effective", lambda *_args, **_kwargs: True)
        monkeypatch.setattr(importer, "_fetch_bulk_export_resource_rows", AsyncMock(return_value=fetch_result))
    elif mode == "partitioned_paged":
        monkeypatch.setattr(
            importer, "_partitioned_resource_start_urls", lambda *_args, **_kwargs: [BASE + "/a", BASE + "/b"]
        )
        monkeypatch.setattr(importer, "_fetch_partitioned_resource_rows", AsyncMock(return_value=fetch_result))
    elif mode == "last_updated_partition":
        source_by_field["metadata_json"] = {importer.LAST_UPDATED_PARTITION_METADATA_KEY: {"enabled": True}}
        monkeypatch.setattr(importer, "_last_updated_partition_config", lambda *_args: (object(), None))
        monkeypatch.setattr(
            importer, "_fetch_last_updated_partition_resource_rows", AsyncMock(return_value=fetch_result)
        )
    else:
        resource_type = "PractitionerRole"
        monkeypatch.setattr(importer, "_needs_practitioner_role_reverse_lookup", lambda *_args: True)
    snapshots = []

    await _fetch(snapshots.append, source_record=source_by_field, resource_type=resource_type)

    assert snapshots[-1] == {
        "fetch_mode": mode,
        "timing_available": False,
        "timing_scope": "unavailable",
    }


@pytest.mark.asyncio
async def test_bulk_fallback_changes_timing_coverage_to_paged(monkeypatch):
    monkeypatch.setattr(importer, "_is_source_bulk_export_effective", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(importer, "_fetch_bulk_export_resource_rows", AsyncMock(return_value=None))
    monkeypatch.setattr(
        importer, "_fetch_source_json", AsyncMock(return_value=(200, {"resourceType": "Bundle", "entry": []}, None, 4))
    )
    snapshots = []

    await _fetch(snapshots.append)

    assert any(snapshot["fetch_mode"] == "bulk_export" and not snapshot["timing_available"] for snapshot in snapshots)
    assert snapshots[-1]["fetch_mode"] == "paged"
    assert snapshots[-1]["timing_available"] is True


@pytest.mark.parametrize(
    ("mode", "selector", "connector"),
    [
        ("graphql", "_uses_alohr_graphql_connector", "_import_alohr_graphql_source_group"),
        ("official_files", "_uses_uhc_official_file_connector", "_import_uhc_official_file_source_group"),
        ("plan_graph", "_should_use_uhc_plan_graph", "_import_uhc_plan_graph_source_group"),
    ],
)
async def test_connector_bypasses_report_their_active_mode(monkeypatch, mode, selector, connector):
    _stub_resource_import_metadata(monkeypatch)
    summary = ([SOURCE["source_id"]], {"InsurancePlan": {"complete": True}}, {"InsurancePlan": 3}, {}, {}, {}, {})
    progress = []

    async def report(_done, _total, _counts, details):
        progress.append(details)

    monkeypatch.setattr(importer, selector, lambda *_args, **_kwargs: True)
    monkeypatch.setattr(importer, connector, AsyncMock(return_value=summary))
    monkeypatch.setattr(importer, "_finalize_source_pagination_checkpoints", AsyncMock())
    counts = await importer._import_resources(
        [dict(SOURCE)],
        resources=["InsurancePlan"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run-test",
        progress_callback=report,
    )

    assert counts == {"InsurancePlan": 3}
    assert any(
        group_map["resource_fetch_timing"]["InsurancePlan"]
        == {"fetch_mode": mode, "timing_available": False, "timing_scope": "unavailable"}
        for details in progress
        for group_map in details["active_source_groups"]
    )


def test_resource_timing_snapshots_remain_stable_and_separate():
    group_map = {}
    importer._record_resource_group_fetch_timing(group_map, "Practitioner", {"attempt_rows_fetched": 1})
    published_map = dict(group_map)
    importer._record_resource_group_fetch_timing(group_map, "Location", {"attempt_rows_fetched": 3})
    importer._record_resource_group_fetch_timing(group_map, "Practitioner", {"attempt_rows_fetched": 2})

    assert published_map["resource_fetch_timing"] == {"Practitioner": {"attempt_rows_fetched": 1}}
    assert group_map["resource_fetch_timing"]["Location"]["attempt_rows_fetched"] == 3


@pytest.mark.asyncio
async def test_normal_and_zero_role_retry_callers_publish_through_existing_progress(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)
    calls = []
    progress = []

    async def fetch(_source, resource_type, **options):
        calls.append(resource_type)
        rows = int(resource_type != "PractitionerRole" or calls.count(resource_type) > 1)
        options["timing_progress_callback"](
            {
                "fetch_mode": "paged",
                "timing_available": True,
                "attempt_rows_fetched": rows,
            }
        )
        if rows:
            model = importer.RESOURCE_MODELS_BY_TYPE[resource_type]
            await options["row_batch_handler"](
                model, [{"source_id": SOURCE["source_id"], "resource_id": resource_type}]
            )
        return _result(resource_type, rows)

    async def report(_done, _total, counts, details):
        progress.append((dict(counts), details))

    monkeypatch.setattr(importer, "_fetch_resource_rows", fetch)
    monkeypatch.setattr(
        importer, "_upsert_resource_rows", AsyncMock(side_effect=lambda _model, rows, **_kwargs: len(rows))
    )
    counts = await importer._import_resources(
        [dict(SOURCE)],
        resources=["Practitioner", "Location", "PractitionerRole"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run-test",
        stream_batch_size=1,
        progress_callback=report,
    )

    assert calls.count("PractitionerRole") == 2
    assert counts == {"Practitioner": 1, "Location": 1, "PractitionerRole": 1}
    assert any(
        group_map["resource_fetch_timing"]["Practitioner"].get("attempt_rows_fetched") == 1
        for _, details in progress
        for group_map in details["active_source_groups"]
    )


@pytest.mark.asyncio
async def test_many_timing_updates_reuse_existing_progress_throttle(monkeypatch):
    _stub_resource_import_metadata(monkeypatch)
    progress = []

    async def fetch(_source, resource_type, **options):
        for count in range(1, 51):
            options["timing_progress_callback"](
                {
                    "fetch_mode": "paged",
                    "timing_available": True,
                    "attempt_rows_fetched": count,
                }
            )
            await options["row_batch_handler"](
                importer.RESOURCE_MODELS_BY_TYPE[resource_type],
                [{"source_id": SOURCE["source_id"], "resource_id": str(count)}],
            )
        return _result(resource_type, 50)

    async def report(done, _total, counts, _details):
        progress.append((done, counts["Location"]))

    monkeypatch.setattr(importer, "_fetch_resource_rows", fetch)
    monkeypatch.setattr(
        importer,
        "_upsert_resource_rows",
        AsyncMock(side_effect=lambda _model, rows, **_kwargs: len(rows)),
    )
    counts = await importer._import_resources(
        [dict(SOURCE)],
        resources=["Location"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run-test",
        stream_batch_size=1,
        progress_callback=report,
    )

    assert counts == {"Location": 50}
    assert [count for done, count in progress if done == 0 and count > 0] == [1]
    assert progress[-1] == (1, 50)
