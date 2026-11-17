# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cursor failures drain speculative work and retain the original acquisition state."""

import asyncio
import importlib
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_policy as policy
from process import provider_directory_generic_source_storage as storage
from tests.test_process_provider_directory_fhir_unit import (
    _assert_no_rest_page_prefetch_task,
    _cigna_checkpoint_context,
    _molina_checkpoint_context,
    _molina_checkpoint_source,
    _RestPageHarness,
    _source_http_test_session,
)
from tests.test_process_provider_directory_fhir_unit import unit_checkpoint_catalog as unit_checkpoint_catalog
from tests.test_process_provider_directory_fhir_unit import unit_source_factory as unit_source_factory

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_prefetched_transport_exception_preserves_original_cursor(
    monkeypatch, unit_source_factory, unit_checkpoint_catalog
):
    scan = _RestPageHarness(monkeypatch)
    release_fetch = asyncio.Event()
    original = OSError("synthetic speculative transport failure")

    async def fail_second_page(index):
        if index == 1:
            await release_fetch.wait()
            raise original

    async def finish_first_page(resource_ids):
        assert resource_ids == ["prac-1"]
        await asyncio.wait_for(scan.fetch_started[1].wait(), timeout=1)
        release_fetch.set()

    scan.fetch_hook, scan.write_hook = fail_second_page, finish_first_page
    with pytest.raises(OSError) as raised:
        await scan.run()
    assert raised.value is original and scan.active_fetches == 0
    assert scan.requests == scan.urls and scan.write_attempts == [["prac-1"]]
    assert scan.checkpoint_urls == [scan.urls[1]]
    assert scan.checkpoints[0]["pages_processed"] == scan.checkpoints[0]["rows_processed"] == 1
    _assert_no_rest_page_prefetch_task()


async def _run_cancelable_scan(scan):
    with _source_http_test_session(scan.source_session):
        return await fhir._fetch_resource_rows(
            scan.source,
            "Practitioner",
            per_resource_limit=0,
            page_limit=0,
            page_count=scan.page_count,
            timeout=3,
            run_id="run_1",
            row_batch_handler=scan._write_rows,
            row_batch_size=1000,
            retain_rows=False,
            pagination_checkpoint=scan.checkpoint_context,
            cancel_ctx={"run_id": "run_1"},
        )


@pytest.mark.asyncio
async def test_cancel_check_drains_started_prefetch_before_return(
    monkeypatch, unit_source_factory, unit_checkpoint_catalog
):
    scan = _RestPageHarness(monkeypatch)
    hold_fetch = asyncio.Event()
    original = asyncio.CancelledError("synthetic operator stop")

    async def hold_second_page(index):
        if index == 1:
            await hold_fetch.wait()

    async def finish_first_page(resource_ids):
        assert resource_ids == ["prac-1"]
        await asyncio.wait_for(scan.fetch_started[1].wait(), timeout=1)

    async def check_cancel(*_args):
        if scan.fetch_started[1].is_set():
            raise original

    scan.fetch_hook, scan.write_hook = hold_second_page, finish_first_page
    monkeypatch.setattr(fhir, "_raise_if_resource_import_cancelled", check_cancel)
    with pytest.raises(asyncio.CancelledError) as raised:
        await _run_cancelable_scan(scan)
    assert raised.value is original and scan.fetch_cancelled[1].is_set()
    assert scan.active_fetches == 0 and scan.write_attempts == [["prac-1"]]
    assert scan.checkpoint_urls == [scan.urls[1]]
    _assert_no_rest_page_prefetch_task()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_cooldown_active", [False, True])
async def test_page_deadline_keeps_issued_cursor_and_only_active_cooldown(
    monkeypatch, unit_source_factory, unit_checkpoint_catalog, is_cooldown_active
):
    """A cancelled page leaves the previous confirmed page and its cursor resumable."""
    scan = _RestPageHarness(monkeypatch, prefetch=False)
    unfinished_fetch = asyncio.Event()

    async def hold_second_page(page_index):
        if page_index == 1:
            scan.source["_provider_directory_request_pacing_state"] = {
                "retry_after_monotonic": time.monotonic() + (60 if is_cooldown_active else -60)
            }
            await unfinished_fetch.wait()

    scan.fetch_hook = hold_second_page
    attempt = await scan.run(deadline_seconds=0.5)

    assert attempt.deadline_reached and attempt.next_url_remaining and not attempt.complete
    assert (attempt.retry_not_before is not None) is is_cooldown_active
    assert scan.requests == scan.urls and scan.fetch_cancelled[1].is_set()
    assert scan.write_attempts == [["prac-1"]] and scan.checkpoint_urls == [scan.urls[1]]
    assert scan.active_fetches == 0
    _assert_no_rest_page_prefetch_task()


@pytest.mark.asyncio
async def test_empty_molina_self_link_saves_one_terminal_page(
    monkeypatch, unit_source_factory, unit_checkpoint_catalog
):
    scan = _RestPageHarness(monkeypatch, prefetch=False)
    previous_base = scan.source["api_base"]
    scan.source, scan.checkpoint_context = _molina_checkpoint_source(), _molina_checkpoint_context()
    scan.urls = [url.replace(previous_base, scan.source["api_base"]) for url in scan.urls]
    scan.responses = {
        scan.urls[0]: (
            200,
            {"resourceType": "Bundle", "entry": [], "link": [{"relation": "next", "url": scan.urls[0]}]},
            None,
            5,
        )
    }
    result = await scan.run()
    assert result.complete and result.pages_fetched == 1 and result.rows_fetched == 0
    assert scan.requests == [scan.urls[0]] and scan.checkpoint_urls == [None]
    assert not scan.write_attempts and scan.active_fetches == 0
    _assert_no_rest_page_prefetch_task()


@pytest.mark.asyncio
async def test_changed_acquisition_root_refuses_reset_and_proof_deletion(monkeypatch):
    context = _cigna_checkpoint_context("run_retry", "run_original", dataset_id="dataset_example")

    @asynccontextmanager
    async def transaction(*_args):
        yield

    monkeypatch.setattr(policy, "source_transaction", transaction)
    candidate = SimpleNamespace(identity={"acquisition_root_run_id": "another_root"})
    monkeypatch.setattr(storage, "resolve_existing_source_candidate", AsyncMock(return_value=candidate))
    reset, delete = AsyncMock(), AsyncMock()
    monkeypatch.setattr(storage, "reset_generic_source_family", reset)
    monkeypatch.setattr(fhir, "delete_dataset_resource_proof_shards", delete)
    with pytest.raises(RuntimeError, match="cleanup_parent_changed"):
        await fhir._clear_checkpoint_dataset_resource_type(context, "Practitioner")
    reset.assert_not_awaited()
    delete.assert_not_awaited()
    assert context.acquisition_root_run_id == "run_original"


@pytest.mark.asyncio
async def test_missing_dataset_scope_refuses_before_resource_copy(monkeypatch):
    persist = AsyncMock()
    monkeypatch.setattr(fhir, "_persist_endpoint_dataset_rows", persist)
    with pytest.raises(RuntimeError, match="dataset_scope_required"):
        await fhir._persist_compatible_resource_rows(
            fhir.ProviderDirectoryPractitioner,
            [{"resource_id": "practitioner_example"}],
            fhir.ProviderDirectoryResourceWriteOptions(run_id="run_example", track_seen=False),
            fhir._ResourceWriteIdentity(None, fhir.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT, None),
        )
    persist.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_diagnostic_write_still_records_original_terminal_failure(monkeypatch):
    original = fhir.ProviderDirectoryPaginationTerminalFailure(
        "synthetic terminal traversal failure", {"Practitioner": {"complete": False, "error": "cursor_expired"}}
    )
    candidate = SimpleNamespace(dataset_id="dataset_example")
    diagnostics = AsyncMock(side_effect=OSError("synthetic diagnostic write failure"))
    marker = AsyncMock()
    monkeypatch.setattr(fhir, "_update_source_resource_import_metadata", diagnostics)
    monkeypatch.setattr(fhir, "_mark_failed_endpoint_dataset_without_masking", marker)
    await fhir._retain_terminal_import_failure(
        candidate,
        [{"source_id": "source_b"}, {"source_id": "source_a"}, {"source_id": "source_b"}],
        "run_example",
        original,
    )
    assert diagnostics.await_args.args[0] == ["source_a", "source_b"]
    marker.assert_awaited_once_with(candidate, original.diagnostics_by_resource, original)
