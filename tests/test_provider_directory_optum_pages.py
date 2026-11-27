# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep readable Optum pages without granting completion to a leaf with a gap."""

import asyncio
import importlib
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest

from process import provider_directory_optum_pages as pages
from process.provider_directory_fetch_types import PartitionChainRequest, PartitionFetchOptions, PartitionFetchState
from tests.test_provider_directory_optum_outage import PAGE_ERROR, WRAPPED_PAGE_ERROR

importer = importlib.import_module("process.provider_directory_fhir")
BASE = importer.UHC_PROVIDER_DIRECTORY_BASE
START = f"{BASE}/Location?address-postalcode=12345&_count=2"
CURSOR = f"{BASE}?_getpages=synthetic%2Bhandle&_getpagesoffset=2&_count=2&_pagetype=opaque"


def _search_page(start, stop, next_url=None):
    return (
        200,
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": 7,
            "entry": [
                {"resource": {"resourceType": "Location", "id": str(index), "address": {"postalCode": "12345"}}}
                for index in range(start, stop)
            ],
            "link": [{"relation": "next", "url": next_url}] if next_url else [],
        },
        None,
        1,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("page_error", [PAGE_ERROR, WRAPPED_PAGE_ERROR])
async def test_unreadable_position_saves_neighbors_and_later_pages_without_checkpoint(monkeypatch, page_error):
    offsets = []

    async def fetch(_directory, request):
        if request.current_url == START:
            return _search_page(0, 2, CURSOR)
        query = parse_qs(urlsplit(request.current_url).query)
        assert query["_getpages"] == ["synthetic+handle"] and query["_pagetype"] == ["opaque"]
        start, count = int(query["_getpagesoffset"][0]), int(query["_count"][0])
        offsets.append((start, count))
        if start <= 3 < start + count:
            return page_error
        stop = min(start + count, 7)
        next_url = pages.PageWindow(CURSOR, start, count, 7).slice_url(stop, count) if stop < 7 else None
        return _search_page(start, stop, next_url)

    saved_rows = []

    async def persist(model, resource_rows):
        assert model is importer.ProviderDirectoryLocation
        saved_rows.extend(resource_rows)
        return len(resource_rows)

    monkeypatch.setattr(importer.fhir_response, "fetch_partition_response", fetch)
    checkpoint = AsyncMock()
    monkeypatch.setattr(importer, "_record_partition_checkpoint", checkpoint)
    state = PartitionFetchState(result_model=importer.ProviderDirectoryLocation, retained_resource_rows=[])
    options = PartitionFetchOptions(5, "run", persist, 2, False, None, 0)
    source_by_field = {"source_id": "test-source", "api_base": BASE}
    await importer._fetch_partition_url_chain(
        PartitionChainRequest(source_by_field, "Location", asyncio.Queue(), state, START, options)
    )
    assert [saved_location["resource_id"] for saved_location in saved_rows] == ["0", "1", "2", "4", "5", "6"]
    assert saved_rows[2]["fhir_fetch_url"] == BASE
    assert saved_rows[2]["fhir_fetch_mode"] == "rest_bundle"
    assert offsets == [(2, 2), (2, 1), (3, 1), (3, 1), (4, 2), (6, 2)]
    assert state.unreadable_optum_slot_count == 1
    assert state.unreadable_optum_pages == [
        {"partition_url_hash": importer._pagination_url_hash(START), "offsets": (3,)}
    ]
    assert not state.retry_leaf_urls
    assert state.error_message == "partition_errors_1_last_optum_unreadable_search_slots"
    checkpoint.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url,total",
    [(CURSOR, 10000), (CURSOR + "&_count=1", 7), (CURSOR.replace("_count=2", "_count=101"), 7), (START, 7)],
)
async def test_offset_recovery_rejects_unproved_windows(url, total):
    send = AsyncMock()
    assert await pages.recover_page(url, total, "Location", send) is None
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_offset_recovery_rejects_ignored_slice_coordinates():
    send = AsyncMock(return_value=_search_page(2, 3))
    assert await pages.recover_page(CURSOR, 7, "Location", send) is None
    assert send.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource_type",
    ["Location", "Organization", "Practitioner", "OrganizationAffiliation", "PractitionerRole"],
)
async def test_recover_partition_page_covers_all_optum_partitioned_families(monkeypatch, resource_type):
    start_url = f"{BASE}/{resource_type}?_count=2"
    cursor_url = f"{BASE}?_getpages=synthetic%2Bhandle&_getpagesoffset=2&_count=2&_pagetype=opaque"

    def make_page(start, stop):
        return (
            200,
            {
                "resourceType": "Bundle",
                "type": "searchset",
                "total": 5,
                "entry": [{"resource": {"resourceType": resource_type, "id": str(idx)}} for idx in range(start, stop)],
            },
            None,
            1,
        )

    async def fetch(_directory, request):
        query = parse_qs(urlsplit(request.current_url).query)
        start, count = int(query["_getpagesoffset"][0]), int(query["_count"][0])
        if start <= 3 < start + count:
            return PAGE_ERROR
        return make_page(start, min(start + count, 5))

    monkeypatch.setattr(importer.fhir_response, "fetch_partition_response", fetch)
    state = PartitionFetchState(result_model=importer.ProviderDirectoryLocation, retained_resource_rows=[])
    options = PartitionFetchOptions(5, "run", None, 2, False, None, 0)
    request = PartitionChainRequest(
        {"source_id": "test-source", "api_base": BASE}, resource_type, asyncio.Queue(), state, start_url, options
    ).page_request(cursor_url, False, [], 5)
    recovered = await pages.recover_partition_page(importer, request, PAGE_ERROR)
    assert recovered is not None
    assert recovered.unreadable_offsets == (3,)
    assert [entry["resource"]["id"] for _, entries in recovered.batches for entry in entries] == ["2"]


@pytest.mark.asyncio
async def test_recover_page_handles_multiple_unreadable_slots_in_full_100_row_window():
    bad_offsets = {12, 47, 89}
    cursor_100 = f"{BASE}?_getpages=synthetic%2Bhandle&_getpagesoffset=100&_count=100&_pagetype=opaque"

    async def send(url):
        query = parse_qs(urlsplit(url).query)
        start, count = int(query["_getpagesoffset"][0]), int(query["_count"][0])
        if any(start <= bad < start + count for bad in {100 + offset for offset in bad_offsets}):
            return PAGE_ERROR
        return (
            200,
            {
                "resourceType": "Bundle",
                "type": "searchset",
                "total": 300,
                "entry": [
                    {"resource": {"resourceType": "PractitionerRole", "id": str(idx)}}
                    for idx in range(start, start + count)
                ],
            },
            None,
            1,
        )

    recovered = await pages.recover_page(cursor_100, 300, "PractitionerRole", send)
    assert recovered is not None
    assert recovered.unreadable_offsets == (112, 147, 189)
    assert sum(len(entries) for _, entries in recovered.batches) == 97


@pytest.mark.asyncio
@pytest.mark.parametrize("summary_total", [1, 2])
async def test_first_page_error_recovers_only_verified_single_row_leaf(monkeypatch, summary_total):
    start_url = f"{BASE}/Organization?name=ABZZ&_count=100"
    calls = []

    async def fetch(_directory, request):
        calls.append(request.current_url)
        query = parse_qs(urlsplit(request.current_url).query)
        if query.get("_summary") == ["count"]:
            return 200, {"resourceType": "Bundle", "type": "searchset", "total": summary_total}, None, 1
        return PAGE_ERROR

    monkeypatch.setattr(importer.fhir_response, "fetch_partition_response", fetch)
    state = PartitionFetchState(result_model=importer.ProviderDirectoryOrganization, retained_resource_rows=[])
    options = PartitionFetchOptions(5, "run", None, 2, False, None, 0)
    request = PartitionChainRequest(
        {"source_id": "test-source", "api_base": BASE}, "Organization", asyncio.Queue(), state, start_url, options
    )
    await importer._fetch_partition_url_chain(request)
    if summary_total == 1:
        assert state.unreadable_optum_slot_count == 1
        assert state.unreadable_optum_page_count == 1
        assert state.error_message == "partition_errors_1_last_optum_unreadable_search_slots"
        assert not state.retry_leaf_urls
    else:
        assert state.unreadable_optum_slot_count == 0
        assert start_url in state.retry_leaf_urls
    assert any("_summary=count" in requested_url for requested_url in calls)


@pytest.mark.asyncio
async def test_first_page_error_splits_splittable_prefix_to_recover_siblings(monkeypatch):
    start_url = f"{BASE}/Organization?name=AB&_count=100"
    queue: asyncio.Queue[str] = asyncio.Queue()

    async def fetch(_directory, request):
        query = parse_qs(urlsplit(request.current_url).query)
        if query.get("_summary") == ["count"]:
            return 200, {"resourceType": "Bundle", "type": "searchset", "total": 12}, None, 1
        return PAGE_ERROR

    monkeypatch.setattr(importer.fhir_response, "fetch_partition_response", fetch)
    checkpoint = AsyncMock()
    unreadable_checkpoint = AsyncMock()
    monkeypatch.setattr(importer, "_record_partition_checkpoint", checkpoint)
    monkeypatch.setattr(importer.postal_checkpoint, "record_unreadable_partition", unreadable_checkpoint)
    state = PartitionFetchState(result_model=importer.ProviderDirectoryOrganization, retained_resource_rows=[])
    options = PartitionFetchOptions(5, "run", None, 2, False, None, 0)
    await importer._fetch_partition_url_chain(
        PartitionChainRequest(
            {"source_id": "test-source", "api_base": BASE}, "Organization", queue, state, start_url, options
        )
    )
    queued_urls = [queue.get_nowait() for _ in range(queue.qsize())]
    assert len(queued_urls) == len(importer.UHC_TEXT_SEARCH_ALPHABET)
    assert f"{BASE}/Organization?_count=100&name=ABA" in queued_urls
    assert not state.retry_leaf_urls
    assert state.error_message is None
    checkpoint.assert_not_awaited()
    unreadable_checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_deferred_leaf_rolls_back_tentative_unreadable_counts_before_retry(monkeypatch):
    monkeypatch.setattr(importer.uhc_scan, "LEAF_RETRY_DELAYS_SECONDS", (0.0,))
    second_page = f"{BASE}?_getpages=synthetic%2Bhandle&_getpagesoffset=2&_count=2&_pagetype=opaque"
    third_page = f"{BASE}?_getpages=synthetic%2Bhandle&_getpagesoffset=4&_count=2&_pagetype=opaque"
    gateway_failures = ["http_502"]

    async def fetch(_directory, request):
        if request.current_url == START:
            return _search_page(0, 2, second_page)
        query = parse_qs(urlsplit(request.current_url).query)
        start, count = int(query["_getpagesoffset"][0]), int(query["_count"][0])
        if start <= 3 < start + count:
            return PAGE_ERROR
        if start == 4 and gateway_failures:
            gateway_failures.pop()
            return 502, {}, None, 1
        stop = min(start + count, 6)
        next_url = third_page if stop == 4 else None
        return _search_page(start, stop, next_url)

    monkeypatch.setattr(importer.fhir_response, "fetch_partition_response", fetch)
    monkeypatch.setattr(importer, "_load_partition_checkpoints", AsyncMock(return_value=set()))
    monkeypatch.setattr(importer, "_record_partition_checkpoint", AsyncMock())
    options = PartitionFetchOptions(5, "run", None, 2, True, None, 0)
    fetch_result = await importer._fetch_partitioned_resource_rows(
        {"source_id": "test-source", "api_base": BASE},
        "Location",
        importer.ProviderDirectoryLocation,
        [START],
        options,
    )
    assert not gateway_failures
    assert fetch_result.error == "partition_errors_1_last_optum_unreadable_search_slots"
    assert fetch_result.fetch_diagnostic["unreadable_optum_page_count"] == 1
    assert fetch_result.fetch_diagnostic["unreadable_optum_slot_count"] == 1
    diagnostic = importer._resource_fetch_diagnostic(fetch_result, rows_written=fetch_result.rows_written)
    assert pages.has_only_unreadable_slots(diagnostic)
