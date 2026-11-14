# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cached completion reconciles distinct persisted identities with the total."""

import copy
import urllib.parse
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_bcbs_minnesota import BASE, directory
from tests.test_provider_directory_bcbs_minnesota import carrier_source as carrier_source
from tests.test_provider_directory_minnesota_cache_resume import cache_context as cache_context


class _CachedCandidate:
    """Keep copied identities and checkpoint commits visible to the count probe."""

    def __init__(self, repeats_first_page=False, *, advertises_total=True):
        self.repeats_first_page = repeats_first_page
        self.advertises_total = advertises_total
        self.resource_ids = set()
        self.saved_pages = []
        self.requested_urls = []
        self.aggregate_observations = []
        self.transaction_depth = 0
        self.rollbacks = 0

    @asynccontextmanager
    async def transaction(self, *_args, **_kwargs):
        before_ids = set(self.resource_ids)
        saved_count = len(self.saved_pages)
        self.transaction_depth += 1
        try:
            yield
        except BaseException:
            self.resource_ids = before_ids
            del self.saved_pages[saved_count:]
            self.rollbacks += 1
            raise
        finally:
            self.transaction_depth -= 1

    async def load(self, _context, _resource, start_url, **_kwargs):
        return directory.PaginationResumeState(start_url, 0, 0, ())

    async def save(self, _context, _resource, **kwargs):
        self.saved_pages.append(copy.deepcopy(kwargs))

    async def write(self, _model, rows):
        self.resource_ids.update(row["resource_id"] for row in rows)
        return len(rows)

    async def count(self, _context, _resource):
        self.aggregate_observations.append((len(self.resource_ids), self.transaction_depth))
        return len(self.resource_ids)

    async def fetch(self, _source, url, **_kwargs):
        self.requested_urls.append(url)
        query_by_name = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        offset = int(query_by_name.get("_getpagesoffset", ["0"])[0])
        count = 50 if offset == 0 else 500 if offset < 1050 else 10
        identifiers = list(range(offset, offset + count))
        if self.repeats_first_page and offset == 550:
            identifiers[0] = 0
        payload = {
            "resourceType": "Bundle",
            "type": "searchset",
            "entry": [{"resource": {"resourceType": "Location", "id": f"location-{number}"}} for number in identifiers],
            "link": [{"relation": "next", "url": BASE + "?_getpages=example-cache-H&_getpagesoffset=50&_count=50"}]
            if offset == 0
            else [],
        }
        if offset == 50 and self.advertises_total:
            payload["total"] = 1060
        return 200, payload, None, 1


@pytest.mark.parametrize("repeats_first_page", [False, True])
@pytest.mark.parametrize("advertises_total", [False, True])
@pytest.mark.asyncio
async def test_cached_retained_rows_require_distinct_total(
    monkeypatch, carrier_source, repeats_first_page, advertises_total
):
    candidate = _CachedCandidate(repeats_first_page, advertises_total=advertises_total)
    monkeypatch.setattr(directory, "_fetch_source_json", candidate.fetch)
    fetch_result = await directory._fetch_resource_rows(
        carrier_source, "Location", per_resource_limit=0, page_limit=0, page_count=500, timeout=3, run_id=None
    )
    assert fetch_result.rows_fetched == 1060 and len(fetch_result.rows) == 1060
    assert fetch_result.complete is not repeats_first_page
    assert fetch_result.error == ("minnesota_cache_unique_candidate_total_mismatch" if repeats_first_page else None)
    assert len(candidate.requested_urls) == 4


@pytest.mark.parametrize("repeats_first_page", [False, True])
@pytest.mark.parametrize("advertises_total", [False, True])
@pytest.mark.asyncio
async def test_cached_terminal_count_checks_persisted_unique_rows(
    monkeypatch, carrier_source, cache_context, repeats_first_page, advertises_total
):
    candidate = _CachedCandidate(repeats_first_page, advertises_total=advertises_total)
    monkeypatch.setattr(directory, "source_checkpoint_transaction", candidate.transaction)
    monkeypatch.setattr(directory, "_load_or_initialize_pagination_checkpoint", candidate.load)
    monkeypatch.setattr(directory, "_save_pagination_checkpoint", candidate.save)
    monkeypatch.setattr(directory, "_fetch_source_json", candidate.fetch)
    monkeypatch.setattr(directory, "_caresource_unique_candidate_count", candidate.count)
    fetch = directory._fetch_resource_rows(
        carrier_source,
        "Location",
        per_resource_limit=0,
        page_limit=0,
        page_count=500,
        timeout=3,
        run_id="example-root",
        row_batch_handler=candidate.write,
        row_batch_size=1000,
        retain_rows=False,
        pagination_checkpoint=cache_context,
    )
    if repeats_first_page:
        with pytest.raises(RuntimeError, match="^minnesota_cache_unique_candidate_total_mismatch$"):
            await fetch
        assert candidate.rollbacks == 1 and len(candidate.saved_pages) == 3
        assert candidate.saved_pages[-1]["rows_processed"] == 1050
        assert candidate.saved_pages[-1]["next_url"] is not None
        assert len(candidate.resource_ids) == 1049
    else:
        fetch_result = await fetch
        assert fetch_result.complete and fetch_result.rows_fetched == 1060
        assert candidate.rollbacks == 0 and len(candidate.saved_pages) == 4
        assert candidate.saved_pages[-1]["next_url"] is None
        assert len(candidate.resource_ids) == 1060
    assert candidate.aggregate_observations == [(1060 - int(repeats_first_page), 1)]
    assert len(candidate.requested_urls) == 4


@pytest.mark.parametrize("stored_count", [1059, 1060])
@pytest.mark.parametrize("advertises_total", [False, True])
@pytest.mark.asyncio
async def test_cached_complete_reuse_rechecks_persisted_unique_rows(
    monkeypatch, carrier_source, cache_context, stored_count, advertises_total
):
    identity = directory._synthetic_position_page_identity(
        carrier_source, [{"resource": {"resourceType": "Location", "id": "location-1059"}}]
    )
    assert identity is not None
    proof_by_field = directory._synthetic_position_page_guard_completeness(
        identity, (identity[1],), cached_total=1060 if advertises_total else None
    )
    checkpoint = directory.PaginationResumeState(
        None, 4, 1060, (), complete=True, resumed=True, completeness=proof_by_field
    )
    aggregate = AsyncMock(return_value=stored_count)
    fetch = AsyncMock(side_effect=AssertionError("completed reuse must not fetch"))
    save = AsyncMock()
    monkeypatch.setattr(directory, "_load_or_initialize_pagination_checkpoint", AsyncMock(return_value=checkpoint))
    monkeypatch.setattr(directory, "_caresource_unique_candidate_count", aggregate)
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
    assert fetch_result.complete is (stored_count == 1060)
    assert fetch_result.error == (None if stored_count == 1060 else "minnesota_cache_unique_candidate_total_mismatch")
    aggregate.assert_awaited_once_with(cache_context, "Location")
    fetch.assert_not_awaited()
    save.assert_not_awaited()
