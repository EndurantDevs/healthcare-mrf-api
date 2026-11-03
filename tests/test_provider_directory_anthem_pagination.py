# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sorted source-issued continuations retain their scope and committed boundary."""

import asyncio
import datetime
from contextlib import nullcontext
from unittest.mock import AsyncMock

import pytest

from tests.test_process_provider_directory_fhir_unit import importer
from tests.test_provider_directory_anthem import _source

BASE = importer.anthem.API_BASE
COLLECTION = BASE + "/Organization"
START = COLLECTION + "?_count=100&_sort=_id"


def _resolve(next_url, *, current=START, entries=100):
    return importer._resolved_fhir_next_url(
        _source(), current, next_url, resource_type="Organization", page_entry_count=entries
    )


def _bundle(*ids, next_url=None):
    bundle_by_field = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{"resource": {"resourceType": "Organization", "id": resource_id}} for resource_id in ids],
    }
    if next_url is not None:
        bundle_by_field["link"] = [{"relation": "next", "url": next_url}]
    return bundle_by_field


def _sorted_resume_state(next_url, *, is_resumed):
    return importer.PaginationResumeState(
        next_url=next_url,
        pages_processed=1 if is_resumed else 0,
        rows_processed=2 if is_resumed else 0,
        recent_url_hashes=(),
        resumed=is_resumed,
        completeness={importer.anthem.SORTED_LAST_ID_KEY: "b"} if is_resumed else {},
    )


def _checkpoint_context():
    return importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="source-example",
        source_ids=("source-example",),
        owner_run_id="run-example",
        acquisition_root_run_id="root-example",
    )


def test_start_sort_is_source_scoped_without_fabricating_a_position():
    source_by_field = _source()
    source_by_field["endpoint_organization"] = "Organization"
    assert importer._resource_start_url(source_by_field, "Organization", page_count=100) == START
    for endpoint in (COLLECTION + "?name=example", COLLECTION + "?_count=2", "https://other.example.test/Organization"):
        source_by_field["endpoint_organization"] = endpoint
        with pytest.raises(ValueError, match="untrusted_anthem_start_url"):
            importer._resource_start_url(source_by_field, "Organization", page_count=100)
    assert BASE not in importer.FHIR_OFFSET_PAGINATION_BASES
    assert BASE not in importer.FHIR_SYNTHETIC_OFFSET_PAGINATION_BASES
    assert BASE not in importer.FHIR_SYNTHETIC_SKIP_PAGINATION_BASES
    other_source_by_field = {"source_id": "other", "api_base": "https://directory.example.test/fhir"}
    assert importer._resource_start_url(other_source_by_field, "Organization", page_count=100) == (
        "https://directory.example.test/fhir/Organization?_count=100"
    )


def test_issued_next_bytes_and_terminal_page_are_preserved():
    issued = COLLECTION + "?_offset=000100&_sort=%5Fid&_count=100"
    assert _resolve(issued) is issued
    later = COLLECTION + "?_sort=_id&_count=100&_offset=00200"
    assert _resolve(later, current=issued) is later
    assert _resolve(None) is None
    with pytest.raises(ValueError, match="untrusted_anthem_pagination_link"):
        _resolve(issued, entries=0)


@pytest.mark.parametrize("entries,offset", [(100, 101), (100, 99), (1, 100), (None, 100), (True, 100), (-1, 100)])
def test_new_page_cannot_skip_or_overlap_unfetched_rows(entries, offset):
    with pytest.raises(ValueError, match="untrusted_anthem_pagination_link"):
        _resolve(COLLECTION + f"?_count=100&_sort=_id&_offset={offset}", entries=entries)


def test_short_page_and_saved_cursor_use_distinct_validation():
    issued = COLLECTION + "?_count=100&_sort=_id&_offset=0002"
    assert _resolve(issued, entries=2) is issued
    saved = COLLECTION + "?_count=100&_sort=_id&_offset=00200"
    assert importer.anthem.resume_url(START, saved, "Organization", 100) is saved
    for invalid in (None, "", START, saved.replace("_sort=_id", "_sort=name")):
        with pytest.raises(ValueError, match="untrusted_anthem_pagination_link"):
            importer.anthem.resume_url(START, invalid, "Organization", 100)


@pytest.mark.parametrize(
    "suffix",
    [
        "?_count=100&_offset=100",
        "?_count=100&_sort=name&_offset=100",
        "?_count=100&_sort=_id&_sort=_id&_offset=100",
        "?_count=100&_sort=_id&_count=100&_offset=100",
        "?_sort=_id&_offset=100",
        "?_count=99&_sort=_id&_offset=100",
        "?_count=101&_sort=_id&_offset=100",
        "?_count=0&_sort=_id&_offset=100",
        "?_count=100&_sort=_id",
        "?_count=100&_sort=_id&_offset=0",
        "?_count=100&_sort=_id&_offset=-1",
        "?_count=100&_sort=_id&_offset=1.5",
        "?_count=100&_sort=_id&_offset=١٠٠",
        "?_count=100&_sort=_id&_offset=100&_offset=200",
        "?_count=100&_sort=_id&_offset=100&access_token=synthetic",
        "?_count=100&_sort=_id&_offset=100&name=example",
        "?_count=100&_sort=_id&_offset=100#",
        "?_count=100&_sort=_id&_offset=100#fragment",
        "?_count=100&_sort=_id&_offset=100\n",
    ],
)
def test_malformed_or_changed_query_is_rejected(suffix):
    with pytest.raises(ValueError, match="untrusted_anthem_pagination_link"):
        _resolve(COLLECTION + suffix)


@pytest.mark.parametrize(
    "next_url",
    [
        "http://" + COLLECTION.removeprefix("https://") + "?_count=100&_sort=_id&_offset=100",
        "https://other.example.test/Organization?_count=100&_sort=_id&_offset=100",
        COLLECTION.replace("https://", "https://user@") + "?_count=100&_sort=_id&_offset=100",
        COLLECTION.replace(".com/", ".com:444/") + "?_count=100&_sort=_id&_offset=100",
        BASE + "/Location?_count=100&_sort=_id&_offset=100",
        COLLECTION + "/?_count=100&_sort=_id&_offset=100",
        COLLECTION + "/../Organization?_count=100&_sort=_id&_offset=100",
        "Organization?_count=100&_sort=_id&_offset=100",
    ],
)
def test_continuation_cannot_escape_exact_https_collection(next_url):
    with pytest.raises(ValueError, match="untrusted_anthem_pagination_link"):
        _resolve(next_url)


def test_continuation_must_advance_and_current_sort_must_be_qualified():
    issued = COLLECTION + "?_count=100&_sort=_id&_offset=100"
    for current in (issued, issued.replace("_offset=100", "_offset=200"), START.replace("&_sort=_id", "")):
        with pytest.raises(ValueError, match="untrusted_anthem_pagination_link"):
            _resolve(issued, current=current)
    role = BASE + "/PractitionerRole?_count=501&_sort=_id"
    with pytest.raises(ValueError, match="untrusted_anthem_pagination_link"):
        importer._resolved_fhir_next_url(
            _source(), role, role + "&_offset=501", resource_type="PractitionerRole", page_entry_count=501
        )


@pytest.mark.parametrize("links", [[{"relation": "next"}], [{"relation": "next", "url": ""}], "invalid"])
def test_malformed_link_is_not_end_of_data(links):
    with pytest.raises(ValueError, match="untrusted_anthem_pagination_link"):
        importer.anthem.next_link({"link": links})
    with pytest.raises(ValueError, match="untrusted_anthem_pagination_link"):
        importer.anthem.next_link({"link": [{"relation": "next", "url": START}] * 2})


@pytest.mark.parametrize("ids", [("b", "a"), ("a", "a"), ("invalid/id",), ("",)])
def test_page_order_and_identity_are_validated_before_staging(ids):
    with pytest.raises(ValueError, match="anthem_sorted_id_page_invalid"):
        importer.anthem.page_boundary(_bundle(*ids), "Organization", None, 100)


@pytest.mark.parametrize(
    "changes",
    [
        {"resourceType": "OperationOutcome"},
        {"type": "collection"},
        {"entry": "invalid"},
        {"entry": [None]},
        {"entry": [{"resource": {"resourceType": "Location", "id": "a"}}]},
    ],
)
def test_malformed_search_page_is_not_empty_success(changes):
    with pytest.raises(ValueError, match="anthem_sorted_id_page_invalid"):
        importer.anthem.page_boundary({**_bundle(), **changes}, "Organization", None, 100)
    with pytest.raises(ValueError, match="anthem_sorted_id_page_invalid"):
        importer.anthem.page_boundary(_bundle("a", "b"), "Organization", None, 1)


def test_resume_requires_the_committed_sorted_boundary():
    assert importer.anthem.resume_boundary(0, {}) is None
    assert importer.anthem.resume_boundary(2, {importer.anthem.SORTED_LAST_ID_KEY: "b"}) == "b"
    for proof in ({}, {importer.anthem.SORTED_LAST_ID_KEY: 12}):
        with pytest.raises(ValueError, match="anthem_sorted_id_checkpoint_invalid"):
            importer.anthem.resume_boundary(2, proof)
    with pytest.raises(ValueError, match="anthem_sorted_id_page_invalid"):
        importer.anthem.page_boundary(_bundle("b", "c"), "Organization", "b", 100)


@pytest.mark.asyncio
@pytest.mark.parametrize("resumed", [False, True])
async def test_overlap_preserves_prior_rows_and_checkpoint_boundary(monkeypatch, resumed):
    monkeypatch.setitem(importer.PREFERRED_FULL_REFRESH_PAGE_COUNT_BY_BASE, BASE, 2)
    first = COLLECTION + "?_count=2&_sort=_id"
    second = first + "&_offset=2"
    bundle_by_url = {first: _bundle("a", "b", next_url=second), second: _bundle("b", "c")}
    fetch = AsyncMock(side_effect=lambda _source, url, **_kwargs: (200, bundle_by_url[url], None, 1))
    writer = AsyncMock(side_effect=lambda _model, rows: len(rows))
    save = AsyncMock()
    context = importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="sorted-source-scope",
        source_ids=("source-example",),
        owner_run_id="run-example",
        acquisition_root_run_id="root-example",
        endpoint_id="endpoint-example",
        dataset_id="dataset-example",
        lineage_verified=True,
    )
    state = _sorted_resume_state(second if resumed else first, is_resumed=resumed)
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", AsyncMock(return_value=state))
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save)
    monkeypatch.setattr(importer, "source_checkpoint_transaction", lambda *_args, **_kwargs: nullcontext())
    fetch_result = await importer._fetch_resource_rows(
        _source(),
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id="run-example",
        row_batch_handler=writer,
        retain_rows=False,
        pagination_checkpoint=context,
    )
    assert not fetch_result.complete and fetch_result.error == "anthem_sorted_id_page_invalid"
    assert fetch_result.rows_fetched == 2 and fetch_result.next_url_remaining
    if resumed:
        writer.assert_not_awaited()
        save.assert_not_awaited()
    else:
        assert [resource_row["resource_id"] for resource_row in writer.await_args.args[1]] == ["a", "b"]
        assert save.await_args.kwargs["next_url"] == second
        assert save.await_args.kwargs["rows_processed"] == 2
        assert save.await_args.kwargs["completeness"] == {importer.anthem.SORTED_LAST_ID_KEY: "b"}


@pytest.mark.asyncio
@pytest.mark.parametrize("complete,dropped_sort", [(False, False), (True, False), (False, True)])
async def test_invalid_resume_does_not_fetch_or_become_complete(monkeypatch, complete, dropped_sort):
    monkeypatch.setitem(importer.PREFERRED_FULL_REFRESH_PAGE_COUNT_BY_BASE, BASE, 2)
    context = importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="sorted-source-scope",
        source_ids=("source-example",),
        owner_run_id="run-example",
        acquisition_root_run_id="root-example",
    )
    state = importer.PaginationResumeState(
        next_url=None if complete else COLLECTION + "?_count=2&_offset=2",
        pages_processed=1,
        rows_processed=2,
        recent_url_hashes=(),
        complete=complete,
        resumed=True,
        completeness={importer.anthem.SORTED_LAST_ID_KEY: "b"} if dropped_sort else {},
    )
    fetch = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", AsyncMock(return_value=state))
    fetch_result = await importer._fetch_resource_rows(
        _source(),
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id="run-example",
        row_batch_handler=AsyncMock(),
        retain_rows=False,
        pagination_checkpoint=context,
    )
    assert not fetch_result.complete and fetch_result.next_url_remaining
    assert fetch_result.error == (
        "untrusted_anthem_pagination_link" if dropped_sort else "anthem_sorted_id_checkpoint_invalid"
    )
    fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 404, 410])
async def test_expired_resumed_cursor_never_resets_retained_rows_or_boundary(monkeypatch, status):
    monkeypatch.setitem(importer.PREFERRED_FULL_REFRESH_PAGE_COUNT_BY_BASE, BASE, 2)
    continuation = COLLECTION + "?_count=2&_sort=_id&_offset=2"
    boundary_by_field = {importer.anthem.SORTED_LAST_ID_KEY: "b"}
    context = importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="sorted-source-scope",
        source_ids=("source-example",),
        owner_run_id="retry-example",
        acquisition_root_run_id="root-example",
        endpoint_id="endpoint-example",
        dataset_id="dataset-example",
        lineage_verified=True,
        allow_retained_pagination_reset=True,
    )
    state = importer.PaginationResumeState(
        next_url=continuation,
        pages_processed=1,
        rows_processed=2,
        recent_url_hashes=(),
        resumed=True,
        completeness=dict(boundary_by_field),
    )
    fetch = AsyncMock(return_value=(status, None, None, 1))
    reset, save, writer = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", AsyncMock(return_value=state))
    monkeypatch.setattr(importer, "_reset_pagination_checkpoint", reset)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save)
    fetch_result = await importer._fetch_resource_rows(
        _source(),
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id="retry-example",
        row_batch_handler=writer,
        retain_rows=False,
        pagination_checkpoint=context,
    )
    assert not fetch_result.complete and fetch_result.error == f"http_{status}" and fetch_result.next_url_remaining
    assert fetch_result.rows_fetched == 2 and fetch_result.pages_fetched == 1
    assert state.next_url == continuation and state.completeness == boundary_by_field
    assert fetch.await_count == 1 and fetch.await_args.args[1] == continuation
    reset.assert_not_awaited()
    save.assert_not_awaited()
    writer.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline_seconds", [0, 10])
async def test_unrelated_fetch_timeout_keeps_its_existing_failure(monkeypatch, deadline_seconds):
    monkeypatch.setattr(importer, "_fetch_source_json", AsyncMock(side_effect=TimeoutError("upstream timeout")))
    with pytest.raises(TimeoutError, match="upstream timeout"):
        await importer._fetch_resource_rows(
            _source(),
            "Organization",
            per_resource_limit=0,
            page_limit=0,
            page_count=2,
            timeout=3,
            deadline_seconds=deadline_seconds,
            run_id="run-example",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("checkpointed", [False, True])
async def test_offset_gap_is_rejected_before_any_row_or_checkpoint_write(monkeypatch, checkpointed):
    monkeypatch.setitem(importer.PREFERRED_FULL_REFRESH_PAGE_COUNT_BY_BASE, BASE, 2)
    first = COLLECTION + "?_count=2&_sort=_id"
    issued = first + "&_offset=1000"
    fetch = AsyncMock(return_value=(200, _bundle("a", "b", next_url=issued), None, 1))
    writer, save = AsyncMock(), AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save)
    checkpoint = None
    if checkpointed:
        checkpoint = _checkpoint_context()
        monkeypatch.setattr(
            importer,
            "_load_or_initialize_pagination_checkpoint",
            AsyncMock(
                return_value=importer.PaginationResumeState(
                    next_url=first, pages_processed=0, rows_processed=0, recent_url_hashes=()
                )
            ),
        )
    fetch_result = await importer._fetch_resource_rows(
        _source(),
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=2,
        timeout=3,
        run_id="run-example",
        row_batch_handler=writer,
        row_batch_size=1,
        retain_rows=False,
        pagination_checkpoint=checkpoint,
    )
    assert not fetch_result.complete and fetch_result.next_url_remaining
    assert fetch_result.error == "untrusted_anthem_pagination_link" and fetch_result.rows_fetched == 0
    writer.assert_not_awaited()
    save.assert_not_awaited()
    assert fetch.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("resumed", [False, True])
async def test_inflight_fetch_deadline_keeps_the_committed_cursor(monkeypatch, resumed):
    """A deadline retains committed rows and the server cooldown for a later retry."""
    monkeypatch.setitem(importer.PREFERRED_FULL_REFRESH_PAGE_COUNT_BY_BASE, BASE, 2)
    first = COLLECTION + "?_count=2&_sort=_id"
    continuation = first + "&_offset=2"
    cancelled = asyncio.Event()
    minimum_retry_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=1199)

    async def fetch_page(_source, url, **_kwargs):
        if url == first:
            return 200, _bundle("a", "b", next_url=continuation), None, 1
        _source["_provider_directory_request_pacing_state"] = {
            "retry_after_monotonic": importer.time.monotonic() + 1200
        }
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    state = _sorted_resume_state(continuation if resumed else first, is_resumed=resumed)
    fetch = AsyncMock(side_effect=fetch_page)
    writer = AsyncMock(side_effect=lambda _model, rows: len(rows))
    save, reset = AsyncMock(), AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", AsyncMock(return_value=state))
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save)
    monkeypatch.setattr(importer, "_reset_pagination_checkpoint", reset)
    monkeypatch.setattr(importer, "source_checkpoint_transaction", lambda *_args, **_kwargs: nullcontext())
    fetch_result = await asyncio.wait_for(
        importer._fetch_resource_rows(
            _source(),
            "Organization",
            per_resource_limit=0,
            page_limit=0,
            page_count=2,
            timeout=3,
            deadline_seconds=1,
            run_id="run-example",
            row_batch_handler=writer,
            retain_rows=False,
            pagination_checkpoint=_checkpoint_context(),
        ),
        timeout=3,
    )
    assert cancelled.is_set() and fetch_result.deadline_reached
    assert not fetch_result.complete and fetch_result.next_url_remaining
    assert fetch_result.rows_fetched == 2 and fetch_result.pages_fetched == 1
    assert datetime.datetime.fromisoformat(fetch_result.retry_not_before) >= minimum_retry_at
    assert fetch.await_args.args[1] == continuation
    if resumed:
        writer.assert_not_awaited()
        save.assert_not_awaited()
        assert state.next_url == continuation and state.completeness == {importer.anthem.SORTED_LAST_ID_KEY: "b"}
    else:
        assert [resource_row["resource_id"] for resource_row in writer.await_args.args[1]] == ["a", "b"]
        assert save.await_count == 1 and save.await_args.kwargs["next_url"] == continuation
        assert save.await_args.kwargs["completeness"] == {importer.anthem.SORTED_LAST_ID_KEY: "b"}
    reset.assert_not_awaited()
