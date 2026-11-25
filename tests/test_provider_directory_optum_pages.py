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
