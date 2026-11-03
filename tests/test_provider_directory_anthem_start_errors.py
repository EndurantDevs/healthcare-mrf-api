# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Invalid configured starts remain isolated from sibling scans and retained state."""

import asyncio
from contextlib import nullcontext
from unittest.mock import AsyncMock

import pytest

from tests.test_process_provider_directory_fhir_unit import importer
from tests.test_provider_directory_anthem import _source

BASE = importer.anthem.API_BASE
COLLECTION = BASE + "/Organization"


def _bundle(resource_type, *resource_ids):
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{"resource": {"resourceType": resource_type, "id": resource_id}} for resource_id in resource_ids],
    }


async def _read_resource(source_by_field, resource_type, *, checkpoint=None, writer=None):
    return await importer._fetch_resource_rows(
        source_by_field,
        resource_type,
        per_resource_limit=0 if checkpoint else 1,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id="run-example",
        row_batch_handler=writer,
        retain_rows=writer is None,
        pagination_checkpoint=checkpoint,
    )


def test_invalid_start_refuses_parallel_qualification_without_a_fallback():
    source_by_field = _source()
    source_by_field["endpoint_organization"] = COLLECTION + "?name=example"
    assert not importer._has_parallel_resource_transport(source_by_field, ("Organization", "Location"), 2)
    with pytest.raises(ValueError, match="untrusted_anthem_start_url"):
        importer._resource_start_url(source_by_field, "Organization", page_count=2)


@pytest.mark.asyncio
async def test_invalid_start_does_not_cancel_an_active_sibling_wave(monkeypatch):
    source_by_field = _source()
    source_by_field["endpoint_organization"] = COLLECTION + "?name=example"
    completed = asyncio.Event()

    async def fetch(_source_record, url, **_options):
        assert url == BASE + "/Location?_count=2&_sort=_id"
        await asyncio.sleep(0)
        completed.set()
        return 200, _bundle("Location", "office-1"), None, 1

    async def import_one(resource_type, **_options):
        return await _read_resource(source_by_field, resource_type)

    fetch_page = AsyncMock(side_effect=fetch)
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_page)
    runner = object.__new__(importer.ResourceGroupScanRunner)
    runner.source_record = source_by_field
    runner.import_one = import_one
    refused, successful = await runner.wave(("Organization", "Location"), 2)
    assert not refused.complete and refused.error == "untrusted_anthem_start_url"
    assert refused.rows == [] and refused.rows_written == 0
    assert successful.complete and successful.rows_fetched == 1 and completed.is_set()
    assert successful.rows[0]["resource_id"] == "office-1"
    fetch_page.assert_awaited_once()


@pytest.mark.asyncio
async def test_probe_reports_invalid_start_without_fetching_the_canonical_collection(monkeypatch):
    source_by_field = _source()
    source_by_field["endpoint_insurance_plan"] = BASE + "/InsurancePlan?name=example"
    fetch_page = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_page)
    probe_by_field = await importer._probe_resource_access(
        source_by_field, BASE, {}, timeout=3, resource_type="InsurancePlan"
    )
    assert probe_by_field == {
        "status": "resource_error",
        "http_status": None,
        "response_time_ms": 0,
        "url": None,
        "resource_type": "InsurancePlan",
        "error": "untrusted_anthem_start_url",
        "search_bundle_valid": False,
    }
    fetch_page.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_start_preserves_the_checkpoint_for_a_corrected_retry(monkeypatch):
    monkeypatch.setitem(importer.PREFERRED_FULL_REFRESH_PAGE_COUNT_BY_BASE, BASE, 2)
    source_by_field = _source()
    source_by_field["endpoint_organization"] = COLLECTION + "?name=example"
    context = importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="sorted-source-scope",
        source_ids=(source_by_field["source_id"],),
        owner_run_id="run-example",
        acquisition_root_run_id="root-example",
        endpoint_id="endpoint-example",
        dataset_id="dataset-example",
        lineage_verified=True,
    )
    cursor = COLLECTION + "?_count=2&_sort=_id&_offset=2"
    state = importer.PaginationResumeState(
        next_url=cursor,
        pages_processed=1,
        rows_processed=2,
        recent_url_hashes=(),
        resumed=True,
        completeness={importer.anthem.SORTED_LAST_ID_KEY: "b"},
    )
    load = AsyncMock(return_value=state)
    fetch_page = AsyncMock(return_value=(200, _bundle("Organization", "c", "d"), None, 1))
    save, reset = AsyncMock(), AsyncMock()
    writer = AsyncMock(side_effect=lambda _model, resource_rows: len(resource_rows))
    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", load)
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_page)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save)
    monkeypatch.setattr(importer, "_reset_pagination_checkpoint", reset)
    monkeypatch.setattr(importer, "source_checkpoint_transaction", lambda *_args, **_options: nullcontext())
    refused = await _read_resource(source_by_field, "Organization", checkpoint=context, writer=writer)
    assert not refused.complete and refused.error == "untrusted_anthem_start_url"
    for callback in (load, fetch_page, save, reset, writer):
        callback.assert_not_awaited()
    assert state.rows_processed == 2 and state.completeness == {importer.anthem.SORTED_LAST_ID_KEY: "b"}
    source_by_field.pop("endpoint_organization")
    resumed = await _read_resource(source_by_field, "Organization", checkpoint=context, writer=writer)
    assert resumed.complete and resumed.rows_fetched == 4 and resumed.pages_fetched == 2
    assert fetch_page.await_args.args[1] == cursor and writer.await_args.args[1][0]["resource_id"] == "c"
    assert save.await_args.kwargs["rows_processed"] == 4
    assert save.await_args.kwargs["completeness"] == {importer.anthem.SORTED_LAST_ID_KEY: "d"}
    reset.assert_not_awaited()


@pytest.mark.asyncio
async def test_start_guards_preserve_unrelated_configuration_errors(monkeypatch):
    source_by_field = _source()

    def invalid_configuration(*_args, **_options):
        raise ValueError("different_configuration_error")

    monkeypatch.setattr(importer, "_resource_start_url", invalid_configuration)
    with pytest.raises(ValueError, match="different_configuration_error"):
        importer._has_parallel_resource_transport(source_by_field, ("Organization", "Location"), 2)
    with pytest.raises(ValueError, match="different_configuration_error"):
        await _read_resource(source_by_field, "Organization")
    with pytest.raises(ValueError, match="different_configuration_error"):
        await importer._probe_resource_access(source_by_field, BASE, {}, timeout=3, resource_type="Organization")
