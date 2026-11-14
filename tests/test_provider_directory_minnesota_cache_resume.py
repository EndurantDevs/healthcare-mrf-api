# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cached totals survive interruption and cannot disappear at completion."""

import asyncio
import copy
import dataclasses
import urllib.parse
from contextlib import nullcontext
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_bcbs_minnesota import BASE, directory
from tests.test_provider_directory_bcbs_minnesota import carrier_source as carrier_source


@pytest.fixture
def cache_context(monkeypatch, carrier_source):
    monkeypatch.setattr(directory, "source_checkpoint_transaction", lambda *_args, **_kwargs: nullcontext())
    return directory.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="example-cache-scope",
        source_ids=(carrier_source["source_id"],),
        owner_run_id="example-root",
        acquisition_root_run_id="example-root",
        dataset_id="example-candidate",
        lineage_verified=True,
        allow_retained_pagination_reset=False,
    )


class _InterruptedCachedSearch:
    """Retain committed pages before interrupting the next request."""

    def __init__(self):
        self.saved_pages = []
        self.checkpoint = None
        self.requested_urls = []

    async def load(self, _context, _resource, start_url, **_kwargs):
        return self.checkpoint or directory.PaginationResumeState(start_url, 0, 0, ())

    async def save(self, _context, _resource, **kwargs):
        self.saved_pages.append(copy.deepcopy(kwargs))
        self.checkpoint = directory.PaginationResumeState(
            next_url=kwargs["next_url"],
            pages_processed=kwargs["pages_processed"],
            rows_processed=kwargs["rows_processed"],
            recent_url_hashes=tuple(kwargs["recent_url_hashes"]),
            complete=kwargs["next_url"] is None,
            resumed=True,
            completeness=copy.deepcopy(kwargs["completeness"]),
        )

    async def fetch(self, _source, url, **_kwargs):
        self.requested_urls.append(url)
        if len(self.requested_urls) > 2:
            raise asyncio.CancelledError
        is_initial = len(self.requested_urls) == 1
        offset, count = (0, 50) if is_initial else (50, 500)
        payload = {
            "resourceType": "Bundle",
            "type": "searchset",
            "entry": [
                {"resource": {"resourceType": "Location", "id": f"location-{number}"}}
                for number in range(offset, offset + count)
            ],
            "link": [{"relation": "next", "url": BASE + "?_getpages=example-cache-H&_getpagesoffset=50&_count=50"}]
            if is_initial
            else [],
        }
        if not is_initial:
            payload["total"] = 1200
        return 200, payload, None, 1


@pytest.mark.asyncio
async def test_resumed_empty_page_cannot_forget_a_previously_advertised_total(
    monkeypatch, carrier_source, cache_context
):
    cache = _InterruptedCachedSearch()
    writer = AsyncMock(side_effect=lambda _model, rows: len(rows))
    reset = AsyncMock(side_effect=AssertionError("retained cache must not reset"))
    monkeypatch.setattr(directory, "_load_or_initialize_pagination_checkpoint", cache.load)
    monkeypatch.setattr(directory, "_save_pagination_checkpoint", cache.save)
    monkeypatch.setattr(directory, "_reset_pagination_checkpoint", reset)
    monkeypatch.setattr(directory, "_fetch_source_json", cache.fetch)
    fetch_options_by_name = {
        "per_resource_limit": 0,
        "page_limit": 0,
        "page_count": 500,
        "timeout": 3,
        "row_batch_handler": writer,
        "row_batch_size": 1000,
        "retain_rows": False,
    }
    with pytest.raises(asyncio.CancelledError):
        await directory._fetch_resource_rows(
            carrier_source,
            "Location",
            run_id="example-root",
            pagination_checkpoint=cache_context,
            **fetch_options_by_name,
        )
    assert len(cache.saved_pages) == 2
    checkpoint = cache.checkpoint
    assert checkpoint is not None and checkpoint.rows_processed == 550 and checkpoint.pages_processed == 2
    assert checkpoint.completeness[directory.bcbs_minnesota.CACHE_TOTAL_KEY] == 1200
    assert directory.SYNTHETIC_POSITION_PAGE_GUARD_KEY in checkpoint.completeness
    assert urllib.parse.parse_qs(urllib.parse.urlsplit(checkpoint.next_url).query)["_getpagesoffset"] == ["550"]
    retained_checkpoint = copy.deepcopy(checkpoint)
    terminal_fetch = AsyncMock(
        return_value=(200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 1)
    )
    monkeypatch.setattr(directory, "_fetch_source_json", terminal_fetch)
    writer.reset_mock()
    retry_context = dataclasses.replace(cache_context, owner_run_id="example-retry", retry_of_run_id="example-root")
    fetch_result = await directory._fetch_resource_rows(
        carrier_source, "Location", run_id="example-retry", pagination_checkpoint=retry_context, **fetch_options_by_name
    )
    assert not fetch_result.complete and fetch_result.rows_fetched == 550 and fetch_result.next_url_remaining
    assert fetch_result.error == "minnesota_cache_short_page_before_total"
    terminal_fetch.assert_awaited_once_with(carrier_source, retained_checkpoint.next_url, timeout=3)
    assert len(cache.saved_pages) == 2 and cache.checkpoint == retained_checkpoint
    writer.assert_not_awaited()
    reset.assert_not_awaited()


def test_a_later_page_cannot_change_the_remembered_total():
    payload = {
        "resourceType": "Bundle",
        "total": 600,
        "entry": [{"resource": {"resourceType": "Location", "id": f"location-{number}"}} for number in range(50, 550)],
    }
    current_url = BASE + "?_getpages=example-cache-H&_getpagesoffset=50&_count=500"
    error = directory.bcbs_minnesota.page_error(payload, "Location", current_url, expected_total=1200)
    assert error == "minnesota_cache_total_changed"


@pytest.mark.asyncio
async def test_complete_checkpoint_cannot_bypass_its_remembered_total(monkeypatch, carrier_source, cache_context):
    identity = directory._synthetic_position_page_identity(
        carrier_source, [{"resource": {"resourceType": "Location", "id": "location-549"}}]
    )
    assert identity is not None
    proof_by_field = {
        **directory._synthetic_position_page_guard_completeness(identity, (identity[1],)),
        directory.bcbs_minnesota.CACHE_TOTAL_KEY: 1200,
    }
    checkpoint = directory.PaginationResumeState(
        next_url=None,
        pages_processed=2,
        rows_processed=550,
        recent_url_hashes=("example-request-hash",),
        complete=True,
        resumed=True,
        completeness=proof_by_field,
    )
    fetch = AsyncMock(side_effect=AssertionError("invalid completed checkpoint must fail before fetching"))
    save = AsyncMock()
    monkeypatch.setattr(directory, "_load_or_initialize_pagination_checkpoint", AsyncMock(return_value=checkpoint))
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    monkeypatch.setattr(directory, "_save_pagination_checkpoint", save)
    fetch_result = await directory._fetch_resource_rows(
        carrier_source,
        "Location",
        per_resource_limit=0,
        page_limit=0,
        page_count=500,
        timeout=3,
        run_id="example-root",
        row_batch_handler=AsyncMock(return_value=0),
        retain_rows=False,
        pagination_checkpoint=cache_context,
    )
    assert not fetch_result.complete and fetch_result.rows_fetched == 550
    assert fetch_result.error == "minnesota_cache_checkpoint_total_mismatch"
    fetch.assert_not_awaited()
    save.assert_not_awaited()
