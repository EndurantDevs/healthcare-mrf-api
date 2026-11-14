# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained page counts preserve issued cursors and historical transition evidence."""

import copy
import hashlib
import json
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from tests.test_process_provider_directory_fhir_unit import importer
from tests.test_provider_directory_anthem import _source

adapter = importer.anthem
RESOURCE = "PractitionerRole"
START = adapter.start_url(RESOURCE, 1000)
LEGACY_START = adapter.start_url(RESOURCE, 500)
SAVED = adapter.API_BASE + "/PractitionerRole?_offset=000500&_sort=%5Fid&_count=500"
BOUNDARY = "role-000499"


def _state():
    return importer.PaginationResumeState(
        next_url=SAVED,
        pages_processed=1,
        rows_processed=500,
        recent_url_hashes=("prior",),
        resumed=True,
        completeness={adapter.SORTED_LAST_ID_KEY: BOUNDARY},
    )


def _historical_completeness():
    """Represent a committed upgrade issued by an earlier worker."""
    return {
        adapter.SORTED_LAST_ID_KEY: "role-001499",
        adapter.PAGE_COUNT_TRANSITION_KEY: {
            "from": 500,
            "to": 1000,
            "issued_offset": 500,
            "issued_cursor_sha256": hashlib.sha256(SAVED.encode()).hexdigest(),
            "sorted_last_id": BOUNDARY,
        },
    }


def _context():
    return importer.PaginationCheckpointContext(
        canonical_api_base=adapter.API_BASE,
        source_scope_hash="scope-example",
        source_ids=("source-example",),
        owner_run_id="run-successor",
        acquisition_root_run_id="root-example",
        dataset_id="dataset-example",
        retry_of_run_id="run-predecessor",
        lineage_verified=True,
        allow_retained_pagination_reset=False,
    )


def _checkpoint():
    state = _state()
    return {
        "dataset_id": "dataset-example",
        "source_ids": ["source-example"],
        "acquisition_root_run_id": "root-example",
        "owner_run_id": "run-predecessor",
        "state": "active",
        "rows_processed": state.rows_processed,
        "pages_processed": state.pages_processed,
        "recent_cursor_hashes": list(state.recent_url_hashes),
        "start_url_hash": hashlib.sha256(LEGACY_START.encode()).hexdigest(),
        "next_url": SAVED,
        "completeness_json": json.dumps(state.completeness),
    }


@pytest.mark.parametrize("count_field", ["_count=500", "%5Fcount=%35%30%30"])
@pytest.mark.parametrize("cap", [500, 1000])
def test_retained_page_preserves_exact_issued_count_and_state(count_field, cap):
    state = replace(_state(), next_url=SAVED.replace("_count=500", count_field))
    original = copy.deepcopy(state)
    boundary, request, proof = adapter.prepare_acquisition(START, RESOURCE, cap, state)
    assert state == original and boundary == BOUNDARY
    assert request == state.next_url and proof == state.completeness
    assert adapter.PAGE_COUNT_TRANSITION_KEY not in proof


def test_fresh_starts_and_other_resource_hashes_keep_their_existing_count():
    source = _source()
    preferred = importer._source_full_refresh_page_count(source, 100, 0, 0)
    assert preferred == 1000
    for resource in adapter.RESOURCES:
        expected = f"{adapter.API_BASE}/{resource}?_count=1000&_sort=_id"
        start = importer._resource_start_url(source, resource, page_count=preferred)
        assert start == expected
        assert adapter.checkpoint_start_hash(resource, start, {}, {}) == hashlib.sha256(expected.encode()).hexdigest()


def test_native_larger_page_checkpoint_preserves_cursor_without_a_transition():
    state = importer.PaginationResumeState(
        next_url=SAVED.replace("_count=500", "_count=1000").replace("000500", "002000"),
        pages_processed=2,
        rows_processed=2000,
        recent_url_hashes=("prior",),
        resumed=True,
        completeness={adapter.SORTED_LAST_ID_KEY: "role-001999"},
    )
    original = copy.deepcopy(state)
    boundary, request, proof = adapter.prepare_acquisition(START, RESOURCE, 1000, state)
    assert boundary == state.completeness[adapter.SORTED_LAST_ID_KEY]
    assert request == state.next_url and proof == state.completeness
    assert adapter.PAGE_COUNT_TRANSITION_KEY not in proof
    assert state == original


@pytest.mark.parametrize("changed", ["_offset=000501", "_sort=name", "_count=2000", "network=example"])
def test_retained_cursor_refuses_changed_position_or_scope(changed):
    replacement_by_value = {
        "_offset=000501": "_offset=000500",
        "_sort=name": "_sort=%5Fid",
        "_count=2000": "_count=500",
    }
    saved = (
        SAVED.replace(replacement_by_value[changed], changed)
        if changed in replacement_by_value
        else SAVED + "&" + changed
    )
    with pytest.raises(ValueError):
        adapter.prepare_resume(START, saved, RESOURCE, 1000, 500, _state().completeness)


def test_retained_cursor_refuses_missing_boundary_wrong_host_and_wrong_resource():
    for saved, resource, proof in (
        (SAVED, RESOURCE, {}),
        (SAVED.replace("https://", "https://other.example/"), RESOURCE, _state().completeness),
        (SAVED, "Location", _state().completeness),
    ):
        with pytest.raises(ValueError):
            adapter.prepare_resume(START, saved, resource, 1000, 500, proof)


def test_transition_survives_resume_and_refuses_tampering():
    following = START + "&_offset=1500"
    committed_by_field = _historical_completeness()
    assert adapter.prepare_resume(START, following, RESOURCE, 1000, 1500, committed_by_field) == (
        following,
        committed_by_field,
    )
    for field, value in (("from", 100), ("to", 500), ("issued_offset", 1500), ("issued_cursor_sha256", "invalid")):
        changed = copy.deepcopy(committed_by_field)
        changed[adapter.PAGE_COUNT_TRANSITION_KEY][field] = value
        with pytest.raises(ValueError, match="anthem_page_count_transition_invalid"):
            adapter.prepare_resume(START, following, RESOURCE, 1000, 1500, changed)
    with pytest.raises(ValueError):
        adapter.prepare_resume(LEGACY_START, following, RESOURCE, 500, 1500, committed_by_field)


@pytest.mark.asyncio
async def test_legacy_checkpoint_is_adopted_without_replacing_identity_or_cursor(monkeypatch):
    checkpoint = _checkpoint()
    original = copy.deepcopy(checkpoint)
    adopt, reset = AsyncMock(return_value=_state()), AsyncMock()
    monkeypatch.setattr(importer, "source_checkpoint_transaction", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(importer, "_fetch_pagination_checkpoint", AsyncMock(return_value=checkpoint))
    monkeypatch.setattr(importer, "_adopt_pagination_checkpoint_owner", adopt)
    monkeypatch.setattr(importer, "_reset_pagination_checkpoint", reset)
    state = await importer._load_or_initialize_pagination_checkpoint(_context(), RESOURCE, START)
    assert state == _state() and checkpoint == original
    assert adopt.await_args.args[-1] == checkpoint["start_url_hash"]
    reset.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("upgraded", [False, True])
@pytest.mark.parametrize("adopt_owner", [False, True])
async def test_completed_legacy_checkpoint_reuses_actual_loader_without_download(monkeypatch, upgraded, adopt_owner):
    checkpoint_by_field = _checkpoint()
    completeness_by_field = _state().completeness
    if upgraded:
        completeness_by_field = _historical_completeness()
        checkpoint_by_field.update(rows_processed=1500, pages_processed=2)
    checkpoint_by_field.update(state="complete", next_url=None, completeness_json=json.dumps(completeness_by_field))
    if not adopt_owner:
        checkpoint_by_field["owner_run_id"] = _context().owner_run_id
    original = copy.deepcopy(checkpoint_by_field)
    fetch, reset, writer = AsyncMock(), AsyncMock(), AsyncMock()
    status = AsyncMock(return_value="UPDATE 1")
    monkeypatch.setattr(importer, "source_checkpoint_transaction", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(importer, "_fetch_pagination_checkpoint", AsyncMock(return_value=checkpoint_by_field))
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "_reset_pagination_checkpoint", reset)
    fetch_result = await _read(writer)
    assert fetch_result.complete and fetch_result.fetch_mode == "checkpoint_complete"
    assert fetch_result.rows_fetched == original["rows_processed"]
    assert fetch_result.pages_fetched == original["pages_processed"]
    assert fetch_result.rows_written == 0 and not fetch_result.next_url_remaining
    assert checkpoint_by_field == original
    fetch.assert_not_awaited()
    writer.assert_not_awaited()
    reset.assert_not_awaited()
    if adopt_owner:
        status.assert_awaited_once()
        observed_by_field = status.await_args.kwargs
        assert observed_by_field["observed_start_url_hash"] == original["start_url_hash"]
        assert observed_by_field["observed_next_url"] is None
        assert observed_by_field["observed_rows_processed"] == original["rows_processed"]
        assert observed_by_field["observed_pages_processed"] == original["pages_processed"]
        assert json.loads(observed_by_field["observed_completeness"]) == completeness_by_field
        assert observed_by_field["source_scope_hash"] == _context().source_scope_hash
        assert observed_by_field["acquisition_root_run_id"] == original["acquisition_root_run_id"]
        assert observed_by_field["dataset_id"] == original["dataset_id"]
    else:
        status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["resource", "scope", "count", "missing_proof"])
async def test_legacy_hash_exception_does_not_relax_other_checkpoints(monkeypatch, change):
    checkpoint, resource, start = _checkpoint(), RESOURCE, START
    if change == "resource":
        resource, start = "Location", adapter.start_url("Location", 1000)
    elif change == "scope":
        checkpoint["source_ids"] = ["other-source"]
    elif change == "count":
        start = adapter.start_url(RESOURCE, 750)
    else:
        checkpoint["next_url"] = START + "&_offset=1500"
    reset, adopt = AsyncMock(), AsyncMock()
    monkeypatch.setattr(importer, "source_checkpoint_transaction", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(importer, "_fetch_pagination_checkpoint", AsyncMock(return_value=checkpoint))
    monkeypatch.setattr(importer, "_adopt_pagination_checkpoint_owner", adopt)
    monkeypatch.setattr(importer, "_reset_pagination_checkpoint", reset)
    with pytest.raises((RuntimeError, ValueError)):
        await importer._load_or_initialize_pagination_checkpoint(_context(), resource, start)
    reset.assert_not_awaited()
    adopt.assert_not_awaited()


def _page(*, terminal=False, invalid=False, start=500, count=500):
    ids = [f"role-{number:06}" for number in range(start, start + count)]
    if invalid:
        ids[0] = BOUNDARY
    payload = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{"resource": {"resourceType": RESOURCE, "id": resource_id}} for resource_id in ids],
    }
    if not terminal:
        following = adapter.start_url(RESOURCE, count) + f"&_offset={start + count}"
        payload["link"] = [{"relation": "next", "url": following}]
    return payload


def _patch_fetch(monkeypatch, responses, *, resume=None):
    fetch = AsyncMock(side_effect=responses)
    save, reset = AsyncMock(), AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(return_value=_state() if resume is None else resume),
    )
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save)
    monkeypatch.setattr(importer, "_reset_pagination_checkpoint", reset)
    monkeypatch.setattr(importer, "source_checkpoint_transaction", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(importer, "REST_PAGINATION_COOLDOWN_RETRY_CAP", 0)
    return fetch, save, reset


async def _read(writer, *, batch_size=5000, source=None):
    return await importer._fetch_resource_rows(
        _source() if source is None else source,
        RESOURCE,
        per_resource_limit=0,
        page_limit=0,
        page_count=1000,
        timeout=3,
        run_id="run-successor",
        row_batch_handler=writer,
        row_batch_size=batch_size,
        retain_rows=False,
        pagination_checkpoint=_context(),
    )


@pytest.mark.asyncio
async def test_retained_metadata_cap_keeps_the_issued_page_count(monkeypatch):
    source = _source()
    source["metadata_json"]["provider_directory_resource_page_count_caps"][RESOURCE] = 500
    original = copy.deepcopy(source)
    preferred = importer._source_full_refresh_page_count(source, 100, 0, 0)
    assert importer._resource_start_url(source, RESOURCE, page_count=preferred) == START
    fetch, save, reset = _patch_fetch(monkeypatch, [(200, _page(terminal=True), None, 1)])
    writer = AsyncMock(side_effect=lambda _model, rows: len(rows))
    result = await _read(writer, source=source)
    assert result.complete and result.rows_fetched == 1000 and result.rows_written == 500
    assert fetch.await_args.args[1] == SAVED
    assert save.await_args.kwargs["completeness"] == {adapter.SORTED_LAST_ID_KEY: "role-000999"}
    assert source == original
    reset.assert_not_awaited()


@pytest.mark.parametrize("page_count,metadata_cap,expected", [(100, 500, 100), (500, 500, 500), (1000, 250, 250)])
def test_legacy_cap_upgrade_preserves_explicit_lower_limits_and_other_sources(page_count, metadata_cap, expected):
    source = _source()
    caps = source["metadata_json"]["provider_directory_resource_page_count_caps"]
    caps[RESOURCE] = metadata_cap
    assert importer._source_resource_page_count(source, RESOURCE, page_count) == expected
    source["api_base"] = source["canonical_api_base"] = "https://directory.example.test/fhir"
    assert importer._source_resource_page_count(source, RESOURCE, 1000) == metadata_cap
    caps["Location"] = 500
    assert importer._source_resource_page_count(source, "Location", 1000) == 500


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", [False, True])
async def test_retained_page_commits_issued_cursor_and_boundary_with_rows(monkeypatch, terminal):
    fetch, save, reset = _patch_fetch(monkeypatch, [(200, _page(terminal=terminal), None, 1), (401, None, None, 1)])
    writer = AsyncMock(side_effect=lambda _model, rows: len(rows))
    result = await _read(writer)
    assert result.rows_fetched == 1000 and result.pages_fetched == 2 and result.rows_written == 500
    assert result.complete is terminal
    assert fetch.await_args_list[0].args[1] == SAVED
    assert save.await_count == 1 and writer.await_count == 1
    saved = save.await_args.kwargs
    assert saved["next_url"] == (None if terminal else LEGACY_START + "&_offset=1000")
    assert saved["completeness"] == {adapter.SORTED_LAST_ID_KEY: "role-000999"}
    reset.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("historical_transition", [False, True])
async def test_larger_count_checkpoint_loads_and_resumes_without_downgrade(monkeypatch, historical_transition):
    rows_processed = 1500 if historical_transition else 2000
    completeness = _historical_completeness() if historical_transition else {adapter.SORTED_LAST_ID_KEY: "role-001999"}
    state = replace(
        _state(),
        next_url=START + f"&_offset={rows_processed}",
        rows_processed=rows_processed,
        pages_processed=2,
        completeness=completeness,
    )
    checkpoint = _checkpoint()
    checkpoint.update(
        rows_processed=rows_processed,
        pages_processed=2,
        next_url=state.next_url,
        completeness_json=json.dumps(completeness),
        start_url_hash=hashlib.sha256((LEGACY_START if historical_transition else START).encode()).hexdigest(),
    )
    original = copy.deepcopy(checkpoint)
    load = importer._load_or_initialize_pagination_checkpoint
    fetch, save, reset = _patch_fetch(
        monkeypatch, [(200, _page(start=rows_processed, count=1000, terminal=True), None, 1)]
    )
    adopt = AsyncMock(return_value=state)
    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", load)
    monkeypatch.setattr(importer, "_fetch_pagination_checkpoint", AsyncMock(return_value=checkpoint))
    monkeypatch.setattr(importer, "_adopt_pagination_checkpoint_owner", adopt)
    writer = AsyncMock(side_effect=lambda _model, batch: len(batch))
    fetch_result = await _read(writer)
    assert fetch_result.complete and fetch_result.rows_fetched == rows_processed + 1000
    assert fetch_result.rows_written == 1000
    assert fetch.await_args.args[1] == state.next_url
    assert checkpoint == original and adopt.await_args.args[-1] == original["start_url_hash"]
    saved = save.await_args.kwargs["completeness"]
    assert saved[adapter.SORTED_LAST_ID_KEY] == f"role-{rows_processed + 999:06}"
    assert saved.get(adapter.PAGE_COUNT_TRANSITION_KEY) == completeness.get(adapter.PAGE_COUNT_TRANSITION_KEY)
    reset.assert_not_awaited()


@pytest.mark.asyncio
async def test_retained_500_count_survives_a_second_retry_without_restarting(monkeypatch):
    fetch, save, reset = _patch_fetch(monkeypatch, [(200, _page(), None, 1), (401, None, None, 1)])
    writer = AsyncMock(side_effect=lambda _model, batch: len(batch))
    first = await _read(writer)
    assert not first.complete and first.rows_fetched == 1000 and first.rows_written == 500
    saved = save.await_args.kwargs
    assert fetch.await_args_list[0].args[1] == SAVED
    assert saved["next_url"] == LEGACY_START + "&_offset=1000"
    assert saved["completeness"] == {adapter.SORTED_LAST_ID_KEY: "role-000999"}
    reset.assert_not_awaited()
    resumed = replace(
        _state(),
        next_url=saved["next_url"],
        rows_processed=saved["rows_processed"],
        pages_processed=saved["pages_processed"],
        completeness=saved["completeness"],
    )
    fetch, save, reset = _patch_fetch(monkeypatch, [(200, _page(start=1000, terminal=True), None, 1)], resume=resumed)
    second = await _read(writer)
    assert second.complete and second.rows_fetched == 1500 and second.rows_written == 500
    assert fetch.await_args.args[1] == resumed.next_url
    assert save.await_args.kwargs["completeness"] == {adapter.SORTED_LAST_ID_KEY: "role-001499"}
    assert save.await_args.args[0] == _context()
    reset.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_size", [250, 500, 5000])
async def test_retained_page_writes_only_inside_its_checkpoint_transaction(monkeypatch, batch_size):
    _, save, reset = _patch_fetch(monkeypatch, [(200, _page(terminal=True), None, 1)])
    active_transactions = []

    @asynccontextmanager
    async def transaction(*_args, **_kwargs):
        active_transactions.append(True)
        try:
            yield
        finally:
            active_transactions.pop()

    async def writer(_model, rows):
        assert active_transactions, "page rows must share the checkpoint transaction"
        return len(rows)

    async def fail_checkpoint(*_args, **_kwargs):
        assert active_transactions
        raise RuntimeError("checkpoint refused")

    save.side_effect = fail_checkpoint
    monkeypatch.setattr(importer, "source_checkpoint_transaction", transaction)
    with pytest.raises(RuntimeError, match="checkpoint refused"):
        await _read(writer, batch_size=batch_size)
    assert not active_transactions
    save.assert_awaited_once()
    reset.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["http", "invalid_ids", "writer", "checkpoint"])
async def test_failed_retained_page_never_resets_the_checkpoint(monkeypatch, failure):
    response = (500, None, None, 1) if failure == "http" else (200, _page(invalid=failure == "invalid_ids"), None, 1)
    _, save, reset = _patch_fetch(monkeypatch, [response])
    writer = AsyncMock(
        side_effect=RuntimeError("write refused") if failure == "writer" else lambda _model, rows: len(rows)
    )
    if failure == "checkpoint":
        save.side_effect = RuntimeError("checkpoint ownership lost")
    if failure in {"writer", "checkpoint"}:
        with pytest.raises(RuntimeError):
            await _read(writer)
    else:
        result = await _read(writer)
        assert not result.complete and result.next_url_remaining and result.rows_fetched == 500
        writer.assert_not_awaited()
        save.assert_not_awaited()
    reset.assert_not_awaited()
