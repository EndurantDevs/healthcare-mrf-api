# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complementary service searches share one durable resource cursor."""

import collections
import contextlib
import dataclasses
import hashlib
import importlib
from unittest.mock import AsyncMock
from urllib.parse import urlencode

import pytest

from process import provider_directory_molina as molina

importer = importlib.import_module("process.provider_directory_fhir")


def _url(phase, cursor=None):
    query_items = [("name:missing", phase), ("_count", "100")]
    if cursor is not None:
        query_items.append(("cursorMark", cursor))
    return molina.API_BASE + "/HealthcareService?" + urlencode(query_items)


def _source():
    return {
        "source_id": "synthetic_service_source",
        "api_base": molina.API_BASE,
        "canonical_api_base": molina.API_BASE,
        "endpoint_healthcare_service": molina.API_BASE + "/HealthcareService?name:missing=false",
    }


def _bundle(resource_id=None, next_url=None):
    bundle_by_field = {"resourceType": "Bundle", "type": "searchset", "entry": []}
    if resource_id:
        bundle_by_field["entry"] = [{"resource": {"resourceType": "HealthcareService", "id": resource_id}}]
    if next_url:
        bundle_by_field["link"] = [{"relation": "next", "url": next_url}]
    return bundle_by_field


class PresenceCursor:
    """Keep checkpoint counters and response queues around the real shared fetch loop."""

    def __init__(self, responses):
        self.responses = {url: collections.deque(values) for url, values in responses.items()}
        self.requests = []
        self.saves = []
        self.saved = None
        self.context = importer.PaginationCheckpointContext(
            canonical_api_base=molina.API_BASE,
            source_scope_hash="synthetic_presence_scope",
            source_ids=("synthetic_service_source",),
            owner_run_id="synthetic_presence_run",
            acquisition_root_run_id="synthetic_presence_run",
            dataset_id="synthetic_presence_dataset",
        )

    async def load(self, _context, _resource_type, start_url):
        if self.saved is not None:
            return dataclasses.replace(self.saved, resumed=True)
        return importer.PaginationResumeState(start_url, 0, 0, ())

    async def save(self, _context, resource_type, **values):
        assert resource_type == "HealthcareService"
        self.saves.append(values)
        self.saved = importer.PaginationResumeState(
            values["next_url"],
            values["pages_processed"],
            values["rows_processed"],
            tuple(values["recent_url_hashes"]),
            complete=values["next_url"] is None,
        )

    async def fetch(self, _source, url, **_options):
        self.requests.append(url)
        response = self.responses[url].popleft()
        return response if isinstance(response, tuple) else (200, response, None, 1)

    async def run(self, monkeypatch):
        monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", self.load)
        monkeypatch.setattr(importer, "_save_pagination_checkpoint", self.save)
        monkeypatch.setattr(importer, "_fetch_source_json", self.fetch)
        monkeypatch.setattr(
            importer, "source_checkpoint_transaction", lambda *_args, **_kwargs: contextlib.nullcontext()
        )
        monkeypatch.setattr(
            importer, "_reset_pagination_checkpoint", AsyncMock(side_effect=AssertionError("cursor reset"))
        )
        return await importer._fetch_resource_rows(
            _source(),
            "HealthcareService",
            per_resource_limit=0,
            page_limit=0,
            page_count=100,
            timeout=3,
            run_id="synthetic_presence_run",
            row_batch_handler=AsyncMock(return_value=0),
            retain_rows=False,
            pagination_checkpoint=self.context,
        )


@pytest.mark.asyncio
async def test_empty_presence_queries_complete_after_both(monkeypatch):
    cursor = PresenceCursor({_url("false"): [_bundle()], _url("true"): [_bundle()]})
    result = await cursor.run(monkeypatch)
    assert cursor.requests == [_url("false"), _url("true")]
    assert [saved["next_url"] for saved in cursor.saves] == [_url("true"), None]
    assert result.complete and result.pages_fetched == 2 and result.rows_fetched == 0
    assert cursor.saved.complete and cursor.saved.pages_processed == 2
    assert cursor.saves[-1]["completeness"] is None


@pytest.mark.asyncio
async def test_second_phase_quota_resumes_without_replaying_first(monkeypatch):
    quota = (
        403,
        {"message": "Out of call volume quota. Quota will be replenished in 00:10:00."},
        importer.SOURCE_QUOTA_EXHAUSTED_ERROR,
        1,
    )
    cursor = PresenceCursor({_url("false"): [_bundle()], _url("true"): [quota, _bundle()]})
    first = await cursor.run(monkeypatch)
    assert not first.complete and first.error == importer.SOURCE_QUOTA_EXHAUSTED_ERROR
    assert first.retry_not_before and first.next_url_remaining
    assert cursor.saved.next_url == _url("true") and cursor.saved.pages_processed == 1
    result = await cursor.run(monkeypatch)
    assert cursor.requests == [_url("false"), _url("true"), _url("true")]
    assert result.complete and result.pages_fetched == 2
    assert [saved["next_url"] for saved in cursor.saves] == [_url("true"), None]


@pytest.mark.asyncio
async def test_both_presence_phases_follow_vendor_pages(monkeypatch):
    cursor = PresenceCursor(
        {
            _url("false"): [_bundle("present-1", _url("false", "present-next"))],
            _url("false", "present-next"): [_bundle("present-2")],
            _url("true"): [_bundle("missing-1", _url("true", "missing-next"))],
            _url("true", "missing-next"): [_bundle("missing-2")],
        }
    )
    result = await cursor.run(monkeypatch)
    assert result.complete and result.pages_fetched == 4 and result.rows_fetched == 4
    assert [saved["next_url"] for saved in cursor.saves] == [
        _url("false", "present-next"),
        _url("true"),
        _url("true", "missing-next"),
        None,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "next_url",
    [
        molina.API_BASE + "/HealthcareService?_count=100&cursorMark=lost-phase",
        "https://" + molina.PAGINATION_HOST + "/fhir/HealthcareService?_count=100&cursorMark=lost-phase",
        _url("true"),
        _url("false") + "&name%3Amissing=false",
        molina.API_BASE + "/Practitioner?name%3Amissing=false&_count=100",
        "https://untrusted.example/fhir/HealthcareService?name%3Amissing=false&_count=100",
    ],
)
async def test_vendor_continuation_requires_same_presence_phase(monkeypatch, next_url):
    cursor = PresenceCursor({_url("false"): [_bundle(next_url=next_url)]})
    result = await cursor.run(monkeypatch)
    assert not result.complete and result.error and result.next_url_remaining
    assert cursor.requests == [_url("false")] and cursor.saves == []


@pytest.mark.asyncio
async def test_empty_self_next_advances_then_completes(monkeypatch):
    reordered = molina.API_BASE + "/HealthcareService?_count=100&name:missing=false"
    cursor = PresenceCursor(
        {_url("false"): [_bundle(next_url=reordered)], _url("true"): [_bundle(next_url=_url("true"))]}
    )
    result = await cursor.run(monkeypatch)
    assert result.complete and cursor.requests == [_url("false"), _url("true")]
    assert [saved["next_url"] for saved in cursor.saves] == [_url("true"), None]


@pytest.mark.asyncio
async def test_empty_self_next_count_change_advances_complement(monkeypatch):
    current_url = _url("false", "same-cursor")
    self_next_url = current_url.replace("_count=100", "_count=50")
    complement_url = _url("true").replace("_count=100", "_count=50")
    cursor = PresenceCursor(
        {
            current_url: [_bundle(next_url=self_next_url)],
            complement_url: [_bundle()],
        }
    )
    cursor.saved = importer.PaginationResumeState(current_url, 3, 0, (), resumed=True)
    result = await cursor.run(monkeypatch)
    assert cursor.requests == [current_url, complement_url]
    assert [saved["next_url"] for saved in cursor.saves] == [complement_url, None]
    assert result.complete and result.pages_fetched == 5


@pytest.mark.asyncio
async def test_nonempty_self_next_remains_a_loop_error(monkeypatch):
    cursor = PresenceCursor({_url("false"): [_bundle("present", _url("false"))]})
    result = await cursor.run(monkeypatch)
    assert not result.complete and result.error == "pagination loop detected"
    assert cursor.requests == [_url("false")] and not cursor.saved.complete


@pytest.mark.asyncio
async def test_resume_requires_presence_phase_before_get(monkeypatch):
    cursor = PresenceCursor({})
    cursor.saved = importer.PaginationResumeState(
        molina.API_BASE + "/HealthcareService?_count=100&cursorMark=lost-phase",
        3,
        10,
        (),
        resumed=True,
    )
    result = await cursor.run(monkeypatch)
    assert not result.complete and result.error == "molina_service_presence_query_invalid"
    assert result.pages_fetched == 3 and result.rows_fetched == 10
    assert cursor.requests == [] and cursor.saves == []


def test_second_phase_keeps_initial_start_fingerprint():
    context = PresenceCursor({}).context
    start_hash = hashlib.sha256(_url("false").encode()).hexdigest()
    checkpoint_by_field = {
        "canonical_api_base": molina.API_BASE,
        "resource_type": "HealthcareService",
        "source_ids": list(context.source_ids),
        "source_scope_hash": context.source_scope_hash,
        "dataset_id": context.dataset_id,
        "owner_run_id": context.owner_run_id,
        "acquisition_root_run_id": context.acquisition_root_run_id,
        "state": "active",
        "start_url_hash": start_hash,
        "next_url": _url("true"),
        "pages_processed": 1,
        "rows_processed": 0,
    }
    resumed = importer._compatible_pagination_resume_state(checkpoint_by_field, context, start_hash)
    assert resumed and resumed.next_url == _url("true") and resumed.resumed
    assert (
        importer._compatible_pagination_resume_state(
            checkpoint_by_field, context, hashlib.sha256(_url("true").encode()).hexdigest()
        )
        is None
    )


def test_presence_chain_retains_one_parallel_resource_cursor(monkeypatch):
    source = _source()
    start = importer._resource_start_url(source, "HealthcareService", page_count=100)
    assert importer._partitioned_resource_start_urls(source, "HealthcareService", page_count=100) == [start]
    monkeypatch.setattr(importer, "_rest_page_prefetch_session", lambda *_args: object())
    assert len(importer.DEFAULT_RESOURCES) == 8
    assert importer._has_parallel_resource_transport(source, tuple(importer.DEFAULT_RESOURCES), 100)


@pytest.mark.parametrize(
    "url",
    [
        _url("false").replace("_count=100", "_count=0"),
        _url("false").replace("https:", "http:"),
        _url("false") + "&identifier=partial-subset",
        _url("false").replace("name%3Amissing=false", "name%3Amissing=unknown"),
    ],
)
def test_presence_search_rejects_invalid_scope(url):
    with pytest.raises(ValueError, match="molina_service_presence_query_invalid"):
        molina.healthcare_service_next_url(url, None, 0)


def test_presence_transition_does_not_expand_other_callers():
    assert importer._resolved_fhir_next_url(_source(), _url("false"), None) is None
    other_source_by_field = {"canonical_api_base": "https://directory.example/fhir"}
    assert (
        importer._resolved_fhir_next_url(other_source_by_field, _url("false"), None, resource_type="HealthcareService")
        is None
    )
    assert (
        importer._resolved_fhir_next_url(
            _source(), molina.API_BASE + "/Endpoint?_count=100", None, resource_type="Endpoint"
        )
        is None
    )


def test_sapphire_service_continuation_preserves_phase():
    query = urlencode({"name:missing": "false", "_count": "50", "cursorMark": "opaque+cursor"})
    vendor_url = "https://" + molina.PAGINATION_HOST + "/fhir/HealthcareService?" + query
    assert (
        importer._resolved_fhir_next_url(
            _source(),
            _url("false"),
            vendor_url,
            resource_type="HealthcareService",
            page_entry_count=3,
        )
        == molina.API_BASE + "/HealthcareService?" + query
    )
