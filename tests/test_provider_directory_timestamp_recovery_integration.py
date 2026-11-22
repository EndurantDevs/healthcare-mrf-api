# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The ordinary reader must commit recovered pages and their proof together."""

import datetime
import hashlib
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_adapters as adapters
from process import provider_directory_timestamp_recovery as recovery
from tests.test_provider_directory_resource_retry import BASE, _source, importer


def _stamp(second):
    return f"2026-01-01T00:00:{second:02d}.123456Z"


def _resource(identifier, second):
    return {
        "resourceType": "Organization",
        "id": identifier,
        "name": identifier,
        "meta": {"lastUpdated": _stamp(second)},
    }


def _bundle(*resources):
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "link": [],
        "entry": [{"resource": resource} for resource in resources],
    }


def _reader(monkeypatch, *, body=None, fail_save=False):
    """Bind an isolated ordinary reader with a verified retained prefix."""
    source_record = _source()
    context = replace(
        source_record["_pagination_checkpoint_context"],
        retry_of_run_id="prior-run",
        source_scope_hash=hashlib.sha256(b"scope").hexdigest(),
    )
    source_record.update(
        _pagination_checkpoint_context=context,
        _resource_hash_contract=importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )
    old_cursor = BASE + "/Organization?_page_token=old%2B%2F"
    checkpoint_by_field = {
        "next_url": old_cursor,
        "pages_processed": 4,
        "rows_processed": 80,
        "recent_url_hashes": (),
        "complete": False,
        "resumed": True,
        "completeness": {},
    }
    state = SimpleNamespace(
        source=source_record, context=context, checkpoint=checkpoint_by_field, writes=[], events=[], depth=0
    )
    parsed = importer.parse_fhir_resource(source_record["source_id"], _resource("boundary", 7))
    payload_by_field = importer._resource_payload_for_contract(*parsed, source_record["_resource_hash_contract"])
    summary_by_field = {
        "observations": 80,
        "unique_ids": 80,
        "pages": 4,
        "invalid_timestamps": 0,
        "bad_batches": 0,
        "inversions": 0,
        "lowest": datetime.datetime.fromisoformat(_stamp(7)),
        "second_lowest": datetime.datetime.fromisoformat(_stamp(8)),
    }
    summary_read = AsyncMock(
        return_value=(summary_by_field, [{"resource_id": "boundary", "payload_json": payload_by_field}])
    )

    state.summary_read = summary_read

    async def summary_in_transaction(*_args):
        assert state.depth > 0
        return state.summary_read.return_value

    state.summary_read.side_effect = summary_in_transaction
    return _bind_reader_hooks(monkeypatch, state, body, fail_save)


def _bind_reader_hooks(monkeypatch, state, body, fail_save):
    """Observe transaction boundaries without replacing the production parsing or recovery engine."""
    checkpoint_by_field = state.checkpoint

    @asynccontextmanager
    async def transaction(*_args, **_options):
        state.depth += 1
        try:
            yield
        except BaseException:
            state.events.append("rollback")
            raise
        finally:
            state.depth -= 1

    async def save(_context, _resource_type, **values):
        assert state.depth > 0
        if values["pages_processed"] > checkpoint_by_field["pages_processed"]:
            assert state.depth > 0 and state.events[-1] == "copy"
            if fail_save:
                raise RuntimeError("checkpoint-write-failed")
        checkpoint_by_field.update(
            next_url=values["next_url"],
            pages_processed=values["pages_processed"],
            rows_processed=values["rows_processed"],
            recent_url_hashes=tuple(values["recent_url_hashes"]),
            complete=values["next_url"] is None,
            completeness=deepcopy(
                values["completeness"] if values["completeness"] is not None else checkpoint_by_field["completeness"]
            ),
        )
        state.events.append("checkpoint")

    async def persist(_model, rows):
        assert state.depth > 0
        state.events.append("copy")
        state.writes.extend(row["resource_id"] for row in rows)
        return len(rows)

    load = AsyncMock(
        side_effect=lambda *_args, **_options: importer.PaginationResumeState(**deepcopy(checkpoint_by_field))
    )
    fetch = AsyncMock(
        side_effect=[
            (403, None, None, 1),
            (200, body or _bundle(_resource("boundary", 7), _resource("new", 6)), None, 1),
        ]
    )
    monkeypatch.setattr(importer, "INDEPENDENCE_PROVIDER_DIRECTORY_BASE", BASE)
    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", load)
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save)
    monkeypatch.setattr(importer, "source_checkpoint_transaction", transaction)
    monkeypatch.setattr(adapters, "source_transaction", transaction)
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(adapters, "candidate_timestamp_recovery_summary", state.summary_read)
    monkeypatch.setattr(adapters, "is_source_storage_required", lambda: True)

    state.fetch, state.persist = fetch, persist
    return state


async def _fetch(state, *, batch_size=1):
    return await importer._fetch_resource_rows(
        state.source,
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=20,
        timeout=2,
        run_id=state.context.owner_run_id,
        row_batch_handler=state.persist,
        row_batch_size=batch_size,
        retain_rows=False,
        pagination_checkpoint=state.context,
        deferred_materialization=True,
    )


@pytest.mark.asyncio
async def test_recovery_uses_fresh_search_commits_overlap_and_reuses_completed_proof(monkeypatch):
    state = _reader(monkeypatch)
    result = await _fetch(state)
    assert result.complete and result.error is None
    assert result.rows_fetched == 82 and result.rows_written == 2 and result.pages_fetched == 5
    assert state.writes == ["boundary", "new"] and state.events == ["checkpoint", "copy", "checkpoint"]
    requested = state.fetch.await_args_list[-1].args[1]
    assert "_lastUpdated=lt" in requested and "_page_token" not in requested
    assert state.checkpoint["completeness"][recovery.KEY]["original"]["url"].endswith("old%2B%2F")
    diagnostic = importer._resource_fetch_diagnostic(result, rows_written=2)
    assert recovery.diagnostic_is_complete(diagnostic)
    assert "old%2B%2F" not in repr(diagnostic) and diagnostic["source_fetch"] is None
    stats_by_type = {}
    importer._record_resource_fetch_stats(
        stats_by_type, "Organization", result, bulk_export_selection=importer.BULK_EXPORT_SELECTION_NOT_REQUESTED
    )
    assert stats_by_type["Organization"]["timestamp_recovery_traversal_completed_sources"] == 1
    assert stats_by_type["Organization"]["collection_complete_sources"] == 0
    repeated = await _fetch(state)
    assert repeated.complete and repeated.rows_written == 0 and repeated.fetch_mode == recovery.FETCH_MODE
    assert state.fetch.await_count == 2 and state.summary_read.await_count == 1
    assert recovery.diagnostic_is_complete(importer._resource_fetch_diagnostic(repeated, rows_written=0))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        _bundle(_resource("new", 6)),
        _bundle(_resource("boundary", 7), _resource("new", 8)),
        {"resourceType": "Bundle", "type": "collection", "entry": []},
    ],
)
async def test_recovery_rejects_unproven_page_before_copy_or_terminal_checkpoint(monkeypatch, body):
    state = _reader(monkeypatch, body=body)
    result = await _fetch(state)
    assert not result.complete and result.error.startswith("provider_directory_timestamp_recovery_blocked:")
    assert state.writes == [] and state.events == ["checkpoint"]
    assert not state.checkpoint["complete"] and state.checkpoint["rows_processed"] == 80
    assert state.checkpoint["completeness"][recovery.KEY]["observations"] == 0


@pytest.mark.asyncio
async def test_failed_checkpoint_save_propagates_inside_copy_transaction(monkeypatch):
    state = _reader(monkeypatch, fail_save=True)
    with pytest.raises(RuntimeError, match="checkpoint-write-failed"):
        await _fetch(state)
    assert state.events == ["checkpoint", "copy", "rollback"]
    assert not state.checkpoint["complete"] and state.checkpoint["rows_processed"] == 80


@pytest.mark.asyncio
async def test_ordinary_completed_resource_is_reused_without_timestamp_scan(monkeypatch):
    state = _reader(monkeypatch)
    state.checkpoint.update(next_url=None, complete=True)
    result = await _fetch(state)
    assert result.complete and result.fetch_mode == "checkpoint_complete"
    state.summary_read.assert_not_awaited()
    state.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"invalid_timestamps": 1},
        {"inversions": 1},
        {"bad_batches": 1},
        {"unique_ids": 79},
        {"observations": 81},
        {"pages": 3},
        {"second_lowest": None},
    ],
)
async def test_invalid_retained_prefix_refuses_recovery_before_new_search_or_checkpoint_change(monkeypatch, change):
    state = _reader(monkeypatch)
    summary, seam = state.summary_read.return_value
    state.summary_read.return_value = (summary | change, seam)
    result = await _fetch(state)
    assert not result.complete and result.error == "http_403"
    assert state.events == ["rollback"] and not state.writes
    assert state.fetch.await_count == 1
    assert "_page_token=old" in state.fetch.await_args.args[1]


@pytest.mark.asyncio
async def test_valid_ordinary_retained_cursor_never_starts_timestamp_recovery(monkeypatch):
    state = _reader(monkeypatch)
    state.fetch.side_effect = None
    state.fetch.return_value = (200, _bundle(_resource("boundary", 7), _resource("new", 6)), None, 1)
    result = await _fetch(state, batch_size=5000)
    assert result.complete and result.fetch_mode == "checkpointed_paged"
    assert state.fetch.await_count == 1 and "_page_token=old" in state.fetch.await_args.args[1]
    state.summary_read.assert_not_awaited()
    assert not state.checkpoint["completeness"]


@pytest.mark.asyncio
@pytest.mark.parametrize("complete", [False, True])
async def test_recovery_binds_outer_cursor_before_reusing_it(monkeypatch, complete):
    state = _reader(monkeypatch, body=None if complete else _bundle(_resource("new", 6)))
    await _fetch(state)
    checkpoint = state.checkpoint
    assert checkpoint["complete"] is complete
    start_hash = hashlib.sha256((BASE + "/Organization?_count=20").encode()).hexdigest()
    stored_checkpoint_by_field = {
        **checkpoint,
        "state": "complete" if complete else "active",
        "source_ids": list(state.context.source_ids),
        "dataset_id": state.context.dataset_id,
        "acquisition_root_run_id": state.context.acquisition_root_run_id,
        "owner_run_id": state.context.owner_run_id,
        "resource_type": "Organization",
        "start_url_hash": start_hash,
        "recent_cursor_hashes": list(checkpoint["recent_url_hashes"]),
        "completeness_json": checkpoint["completeness"],
    }
    resumed = importer._compatible_pagination_resume_state(stored_checkpoint_by_field, state.context, start_hash)
    assert resumed.complete is complete and resumed.next_url == checkpoint["next_url"]
    if complete:
        for alteration, error in (
            ({"start_url_hash": "0" * 64}, "start_url_changed"),
            ({"recent_cursor_hashes": ["0" * 64]}, "terminal_request_changed"),
        ):
            with pytest.raises(ValueError, match=error):
                importer._compatible_pagination_resume_state(
                    stored_checkpoint_by_field | alteration, state.context, start_hash
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("complete", [False, True])
async def test_unmarked_ordinary_context_without_named_dataset_is_unchanged(monkeypatch, complete):
    state = _reader(monkeypatch)
    context = replace(state.context, dataset_id=None, lineage_verified=False, allow_retained_pagination_reset=True)
    resume = importer.PaginationResumeState(
        next_url=None if complete else state.checkpoint["next_url"],
        pages_processed=4,
        rows_processed=80,
        recent_url_hashes=(),
        resumed=True,
        complete=complete,
    )
    assert (
        await adapters.prepare_timestamp_recovery(
            importer, context, "Organization", BASE + "/Organization?_count=20", resume
        )
        is resume
    )
    state.summary_read.assert_not_awaited()
    assert not state.events


@pytest.mark.asyncio
async def test_marked_active_recovery_cannot_enable_generic_cursor_reset(monkeypatch):
    state = _reader(monkeypatch, body=_bundle(_resource("missing-seam", 6)))
    await _fetch(state)
    saved = deepcopy(state.checkpoint)
    context = replace(state.context, allow_retained_pagination_reset=True)
    source = state.source | {"_pagination_checkpoint_context": context}
    state.source, state.context = source, context
    state.fetch.reset_mock()
    result = await _fetch(state)
    assert not result.complete and "retained_lineage_required" in result.error
    state.fetch.assert_not_awaited()
    assert state.checkpoint == saved


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 403])
async def test_cooldown_response_keeps_ordinary_success_or_starts_recovery(monkeypatch, status):
    state = _reader(monkeypatch)
    body = _bundle(_resource("boundary", 7), _resource("new", 6))
    state.fetch.side_effect = [(429, None, None, 1), (200, body, None, 1)]
    cooldown = SimpleNamespace(
        fetch_result=(status, body if status == 200 else None, None, 1),
        retries=1,
        wait_seconds=0,
        recovered=status == 200,
        exhausted=False,
        deadline_blocked=False,
    )
    monkeypatch.setattr(importer, "_is_rest_pagination_cooldown_failure", lambda code, *_a, **_k: code == 429)
    monkeypatch.setattr(importer, "_retry_rest_pagination_after_cooldown", AsyncMock(return_value=cooldown))
    result = await _fetch(state, batch_size=5000)
    assert result.complete
    assert result.fetch_mode == (recovery.FETCH_MODE if status == 403 else "checkpointed_paged")
    assert state.summary_read.await_count == (1 if status == 403 else 0)


@pytest.mark.asyncio
async def test_unqualified_prefix_preserves_original_retryable_transport(monkeypatch):
    state = _reader(monkeypatch)
    summary, seam = state.summary_read.return_value
    state.summary_read.return_value = (summary | {"invalid_timestamps": 1}, seam)
    state.fetch.side_effect = [(503, None, None, 1)]
    monkeypatch.setattr(importer, "_is_rest_pagination_cooldown_failure", lambda *_a, **_k: False)
    transport_by_field = {"status": 503, "response_class": "transient_server_error"}
    monkeypatch.setattr(importer, "_terminal_source_fetch_diagnostic", lambda *_a: transport_by_field)
    result = await _fetch(state)
    assert not result.complete and result.error == "http_503"
    assert result.fetch_mode == "checkpointed_paged" and result.fetch_diagnostic == transport_by_field
    assert state.events == ["rollback"] and not state.writes


@pytest.mark.asyncio
async def test_partial_seam_preserves_healthy_issued_cursor_and_transport_retry(monkeypatch):
    state = _reader(monkeypatch)
    summary, seam = state.summary_read.return_value
    model, resource_row = importer.parse_fhir_resource(state.source["source_id"], _resource("other-boundary", 7))
    payload = importer._resource_payload_for_contract(model, resource_row, state.source["_resource_hash_contract"])
    state.summary_read.return_value = (summary, seam + [{"resource_id": "other-boundary", "payload_json": payload}])
    issued = BASE + "/Organization?_page_token=healthy"
    body = _bundle(_resource("boundary", 7))
    body["link"] = [{"relation": "next", "url": issued}]
    state.fetch.side_effect = [(403, None, None, 1), (200, body, None, 1), (503, None, None, 1)]
    monkeypatch.setattr(importer, "_is_rest_pagination_cooldown_failure", lambda *_a, **_k: False)
    transport_by_field = {"status": 503, "response_class": "transient_server_error"}
    monkeypatch.setattr(importer, "_terminal_source_fetch_diagnostic", lambda *_a: transport_by_field)
    result = await _fetch(state)
    assert result.error == "http_503" and result.fetch_diagnostic["transport"] == transport_by_field
    assert state.checkpoint["next_url"] == issued
    saved = deepcopy(state.checkpoint)
    state.fetch.reset_mock()
    state.fetch.side_effect = [(200, _bundle(_resource("other-boundary", 7), _resource("new", 6)), None, 1)]
    result = await _fetch(state)
    assert result.complete and state.fetch.await_args.args[1] == issued
    assert state.summary_read.await_count == 1 and saved["rows_processed"] == 81


@pytest.mark.asyncio
async def test_active_failed_cursor_reanchors_only_from_its_closed_boundary(monkeypatch):
    state = _reader(monkeypatch)
    body = _bundle(_resource("boundary", 7), _resource("new", 6))
    body["link"] = [{"relation": "next", "url": BASE + "/Organization?_page_token=failed"}]
    state.fetch.side_effect = [
        (403, None, None, 1),
        (200, body, None, 1),
        (403, None, None, 1),
        (200, _bundle(_resource("new", 6), _resource("older", 5)), None, 1),
    ]
    result = await _fetch(state)
    assert result.complete and result.pages_fetched == 6 and result.rows_fetched == 84
    assert state.writes == ["boundary", "new", "new", "older"]
    assert "_lastUpdated=lt" in state.fetch.await_args.args[1]
    assert recovery.diagnostic_is_complete(importer._resource_fetch_diagnostic(result, rows_written=4))


@pytest.mark.asyncio
@pytest.mark.parametrize("resource", ["Organization", "HealthcareService", "PractitionerRole"])
@pytest.mark.parametrize("status", [400, 413])
async def test_filtered_recovery_request_never_substitutes_another_page_size(monkeypatch, resource, status):
    monkeypatch.setattr(importer, "INDEPENDENCE_PROVIDER_DIRECTORY_BASE", BASE)
    plain_url = f"{BASE}/{resource}?_count=20"
    url = plain_url + "&_lastUpdated=lt2026-01-01T00%3A00%3A08.123456Z"
    candidate = AsyncMock(side_effect=[((status, None, None, 1), True, 0), ((200, _bundle(), None, 1), False, 0)])
    monkeypatch.setattr(importer, "_fetch_source_json_candidate", candidate)
    result = await importer._fetch_source_json(_source(), url, timeout=2)
    assert result[0] == status and candidate.await_count == 1
    assert candidate.await_args.args[1] == url
    assert len(importer._source_fetch_candidate_urls(plain_url)) == 3
