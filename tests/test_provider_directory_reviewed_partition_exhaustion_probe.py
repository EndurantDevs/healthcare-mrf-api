# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _reviewed_partition_source():
    """Build the one reviewed source carrying the bounded partition."""

    seed_rows = [
        seed_row
        for seed_row in importer._reviewed_provider_directory_candidate_seed_rows()
        if importer.LAST_UPDATED_PARTITION_METADATA_KEY in seed_row["metadata_json"]
    ]
    assert len(seed_rows) == 1
    return importer._source_row_from_seed(seed_rows[0])


def _exact_ceiling_window():
    """Return one leaf whose authoritative count fills three data pages."""

    return importer.TimeWindow(
        "root",
        datetime.datetime(2026, 8, 7, 12, 51, 29, tzinfo=datetime.UTC),
        datetime.datetime(2026, 8, 7, 12, 51, 39, tzinfo=datetime.UTC),
        count=3000,
    )


class _PageSequence:
    """Serve three full pages and one controlled exhaustion response."""

    def __init__(self, fourth_page_mode):
        self.fourth_page_mode = fourth_page_mode
        self.requested_urls = []

    async def fetch_page(
        self,
        _source_record,
        _resource_type,
        request_url,
        _window,
        *,
        timeout,
    ):
        self.requested_urls.append((request_url, timeout))
        page_number = len(self.requested_urls)
        if page_number <= 3:
            page_resources = tuple(
                {"id": f"role-{page_number}-{row_number}"} for row_number in range(1000)
            )
            next_url = f"page:{page_number + 1}"
        elif self.fourth_page_mode == "unexpected_row":
            page_resources = ({"id": "unexpected-role"},)
            next_url = None
        else:
            page_resources = ()
            next_url = "page:5" if self.fourth_page_mode == "continuing_empty" else None
        return importer.LastUpdatedWindowPage(page_resources, next_url)


async def _fetch_exact_ceiling_window(monkeypatch, fourth_page_mode):
    """Run the reviewed paginator against one controlled four-page sequence."""

    page_sequence = _PageSequence(fourth_page_mode)
    monkeypatch.setattr(
        importer,
        "_fetch_last_updated_window_page",
        page_sequence.fetch_page,
    )
    monkeypatch.setattr(importer, "_max_page_count", lambda: 10_000)
    window_fetch = await importer._fetch_last_updated_partition_window(
        _reviewed_partition_source(),
        "PractitionerRole",
        "page:1",
        _exact_ceiling_window(),
        timeout=300,
        cancel_ctx=None,
        cancel_task=None,
        deadline_at=None,
    )
    return page_sequence, window_fetch


@pytest.mark.asyncio
async def test_exact_ceiling_uses_one_empty_exhaustion_page(monkeypatch):
    """Accept an exact-count leaf only after an empty terminal response."""

    page_sequence, window_fetch = await _fetch_exact_ceiling_window(
        monkeypatch,
        "terminal_empty",
    )

    assert len(page_sequence.requested_urls) == 4
    assert window_fetch.pages_fetched == 4
    assert window_fetch.complete is True
    assert window_fetch.bounded is False
    assert window_fetch.error is None
    assert len(window_fetch.resources) == 3000


@pytest.mark.asyncio
async def test_exact_ceiling_rejects_data_on_exhaustion_page(monkeypatch):
    """Reject any fourth-page row beyond the authoritative leaf count."""

    page_sequence, window_fetch = await _fetch_exact_ceiling_window(
        monkeypatch,
        "unexpected_row",
    )

    assert len(page_sequence.requested_urls) == 4
    assert window_fetch.pages_fetched == 4
    assert window_fetch.complete is False
    assert window_fetch.bounded is True
    assert window_fetch.error == "window_resource_ceiling_reached"
    assert len(window_fetch.resources) == 3000


@pytest.mark.asyncio
async def test_exact_ceiling_never_requests_after_exhaustion_page(monkeypatch):
    """Stop before page five when the empty probe still advertises a next link."""

    page_sequence, window_fetch = await _fetch_exact_ceiling_window(
        monkeypatch,
        "continuing_empty",
    )

    assert len(page_sequence.requested_urls) == 4
    assert window_fetch.pages_fetched == 4
    assert window_fetch.complete is False
    assert window_fetch.bounded is True
    assert window_fetch.error == "window_page_limit_reached"
    assert len(window_fetch.resources) == 3000
