# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import contextlib
import datetime
import hashlib
import importlib
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryPractitioner


importer = importlib.import_module("process.provider_directory_fhir")


def _identity() -> importer.BulkExportCheckpointIdentity:
    return importer.BulkExportCheckpointIdentity(
        checkpoint_id="checkpoint-atomic",
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        resource_type="Practitioner",
        source_scope_hash="scope-atomic",
        strategy_version=importer.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION,
        acquisition_root_run_id="root-atomic",
        owner_run_id="run-atomic",
        retry_of_run_id=None,
        endpoint_id="endpoint-atomic",
        dataset_id="dataset-atomic",
        start_url="https://providerdirectory.api.aetna.com/fhir/$export",
        start_url_hash="a" * 64,
    )


def _source() -> dict:
    return importer._source_row_from_seed(
        importer._aetna_provider_directory_data_seed_rows(source_query="Aetna")[0]
    )


def _manifest(output_count: int = 1) -> importer.BulkExportManifest:
    return importer._bulk_export_manifest_from_payload(
        {
            "transactionTime": "2026-07-20T00:00:00Z",
            "request": _identity().start_url,
            "requiresAccessToken": False,
            "output": [
                {
                    "type": "Practitioner",
                    "url": (
                        "https://storage.googleapis.com/aetna/"
                        f"part-{output_index}.ndjson?sig=x"
                    ),
                }
                for output_index in range(output_count)
            ],
        },
        "Practitioner",
        expected_request_url=_identity().start_url,
    )


def _output_checkpoint() -> dict:
    manifest_output = _manifest().outputs[0]
    return {
        "output_id": importer._bulk_manifest_output_id(
            _identity().checkpoint_id,
            manifest_output,
        ),
        "state": importer.BULK_EXPORT_OUTPUT_PENDING,
        "rows_written": 0,
        "committed_bytes": 0,
        "content_length_bytes": None,
        "etag_ciphertext": None,
        "etag_hash": None,
        "validator_checked_at": None,
    }


def _stream_options(concurrency: int = 1) -> importer.BulkExportStreamOptions:
    return importer.BulkExportStreamOptions(
        model=ProviderDirectoryPractitioner,
        timeout=3,
        run_id="run-atomic",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1,
        retain_rows=False,
        polls=0,
        ownership_probe=AsyncMock(),
        range_resume_enabled=True,
        output_concurrency=concurrency,
    )


@pytest.mark.asyncio
async def test_terminal_transition_scrubs_capabilities_in_one_statement(monkeypatch):
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)

    await importer._record_bulk_export_checkpoint_error(
        _identity(),
        "bulk_export_manifest_mismatch",
        terminal=True,
    )

    status.assert_awaited_once()
    statement = status.await_args.args[0]
    assert "cleared_outputs AS" in statement
    assert "output_url_ciphertext = NULL" in statement
    assert "status_url_ciphertext = CASE" in statement
    assert status.await_args.kwargs["terminal"] is True


@pytest.mark.asyncio
async def test_completion_scrubs_capabilities_in_transition_statement(monkeypatch):
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value={"state": importer.BULK_EXPORT_CHECKPOINT_COMPLETE}),
    )

    await importer._complete_bulk_export_checkpoint(
        _identity(),
        require_validators=True,
    )

    status.assert_awaited_once()
    statement = status.await_args.args[0]
    assert "cleared_outputs AS" in statement
    assert "etag_ciphertext = NULL" in statement
    assert "manifest_ciphertext = NULL" in statement
    assert "sum(output.rows_written)" in statement


@pytest.mark.asyncio
async def test_output_completion_scrubs_and_refreshes_total_atomically(monkeypatch):
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)

    await importer._complete_bulk_export_output(
        _identity(),
        "output-atomic",
        7,
        101,
        require_validator=True,
    )

    status.assert_awaited_once()
    statement = status.await_args.args[0]
    assert "WITH completed_output AS" in statement
    assert "output_url_ciphertext = NULL" in statement
    assert "etag_ciphertext = NULL" in statement
    assert "sum(" in statement
    assert status.await_args.kwargs["rows_written"] == 7


@pytest.mark.asyncio
async def test_terminal_reload_repairs_capability_scrubbing(monkeypatch):
    clear_capabilities = AsyncMock()

    @contextlib.asynccontextmanager
    async def client_session():
        yield object()

    monkeypatch.setattr(importer, "_bulk_client_session", client_session)
    monkeypatch.setattr(
        importer,
        "_load_or_start_checkpointed_bulk_export",
        AsyncMock(
            return_value=(
                {
                    "state": importer.BULK_EXPORT_CHECKPOINT_FAILED,
                    "rows_written": 7,
                    "error": "bulk_export_manifest_mismatch",
                },
                None,
                None,
            )
        ),
    )
    monkeypatch.setattr(importer, "_clear_bulk_export_capabilities", clear_capabilities)
    fetch_options = importer.BulkExportFetchOptions(
        timeout=3,
        run_id="run-atomic",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1,
        retain_rows=False,
    )

    fetch_result = await importer._fetch_owned_checkpointed_bulk_resource_rows(
        {"source_id": "aetna-provider-directory-data"},
        _identity(),
        ProviderDirectoryPractitioner,
        fetch_options,
        AsyncMock(),
    )

    assert fetch_result is not None
    assert fetch_result.error == "bulk_export_manifest_mismatch"
    clear_capabilities.assert_awaited_once_with(_identity())


@pytest.mark.asyncio
async def test_prefailed_output_repairs_terminal_checkpoint(monkeypatch):
    output_checkpoint_by_name = {
        **_output_checkpoint(),
        "state": importer.BULK_EXPORT_OUTPUT_FAILED,
        "error": "bulk_export_manifest_mismatch",
    }
    record_error = AsyncMock()
    stream_output = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_checkpoint_error",
        record_error,
    )
    monkeypatch.setattr(
        importer,
        "_stream_one_checkpointed_bulk_output",
        stream_output,
    )

    _stream_state, error = await importer._resume_checkpointed_bulk_outputs(
        object(),
        _source(),
        _identity(),
        _manifest(),
        _stream_options(),
        [output_checkpoint_by_name],
    )

    assert error == "bulk_export_manifest_mismatch"
    record_error.assert_awaited_once_with(_identity(), error, terminal=True)
    stream_output.assert_not_awaited()


@pytest.mark.asyncio
async def test_parent_cancellation_drains_all_bulk_output_tasks(monkeypatch):
    manifest = _manifest(2)
    all_started = asyncio.Event()
    all_drained = asyncio.Event()
    counts_by_name = {"started": 0, "drained": 0}

    async def stream_one(*_args):
        counts_by_name["started"] += 1
        if counts_by_name["started"] == 2:
            all_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            counts_by_name["drained"] += 1
            if counts_by_name["drained"] == 2:
                all_drained.set()

    monkeypatch.setattr(importer, "_stream_one_checkpointed_bulk_output", stream_one)
    output_checkpoints = [
        {
            "output_id": importer._bulk_manifest_output_id(
                _identity().checkpoint_id,
                manifest_output,
            ),
            "state": importer.BULK_EXPORT_OUTPUT_PENDING,
        }
        for manifest_output in manifest.outputs
    ]
    resume_task = asyncio.create_task(
        importer._resume_checkpointed_bulk_outputs(
            object(),
            _source(),
            _identity(),
            manifest,
            _stream_options(2),
            output_checkpoints,
        )
    )
    await all_started.wait()
    resume_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await resume_task

    assert all_drained.is_set()


@pytest.mark.asyncio
async def test_completed_output_validator_skips_network_probe(monkeypatch):
    output_checkpoint_by_name = {
        **_output_checkpoint(),
        "state": importer.BULK_EXPORT_OUTPUT_COMPLETE,
        "content_length_bytes": 100,
        "committed_bytes": 100,
        "etag_hash": hashlib.sha256(b'"output-v1"').hexdigest(),
        "etag_ciphertext": "legacy-encrypted-capability",
        "output_url_ciphertext": "legacy-encrypted-capability",
        "validator_checked_at": datetime.datetime(2026, 7, 20, 12),
    }
    probe = AsyncMock()
    clear_capability = AsyncMock()
    refreshed_checkpoints = [{"state": importer.BULK_EXPORT_OUTPUT_COMPLETE}]
    monkeypatch.setattr(importer, "_bulk_http_probe_output", probe)
    monkeypatch.setattr(
        importer,
        "_clear_bulk_output_capability",
        clear_capability,
    )
    monkeypatch.setattr(
        importer,
        "_load_bulk_output_checkpoints",
        AsyncMock(return_value=refreshed_checkpoints),
    )

    checkpoints, validation_error = await importer._prepare_bulk_output_validators(
        object(),
        _source(),
        _identity(),
        _manifest(),
        [output_checkpoint_by_name],
        timeout=3,
    )

    assert checkpoints == refreshed_checkpoints
    assert validation_error is None
    probe.assert_not_awaited()
    clear_capability.assert_awaited_once_with(
        _identity(),
        output_checkpoint_by_name["output_id"],
    )


@pytest.mark.asyncio
async def test_bulk_stream_propagates_import_cancellation_before_get():
    cancel_probe = AsyncMock(
        side_effect=importer.ImportCancelledError("cancelled")
    )

    with pytest.raises(importer.ImportCancelledError, match="cancelled"):
        await importer._stream_bulk_export_output_rows(
            object(),
            _source(),
            _manifest().outputs[0].url,
            model=ProviderDirectoryPractitioner,
            resource_type="Practitioner",
            per_resource_limit=0,
            timeout=3,
            run_id="run-atomic",
            row_batch_handler=AsyncMock(return_value=0),
            row_batch_size=1,
            retain_rows=False,
            resume_options=importer.BulkOutputResumeOptions(
                row_progress_handler=None,
                resume_offset=0,
                expected_etag=None,
                expected_content_length=None,
                cancel_probe=cancel_probe,
            ),
            requires_access_token=False,
        )

    cancel_probe.assert_awaited_once()


@pytest.mark.asyncio
async def test_resource_fetch_forwards_bulk_cancel_and_deadline(monkeypatch):
    expected_fetch_result = object()
    bulk_fetch = AsyncMock(return_value=expected_fetch_result)
    monkeypatch.setattr(importer, "_fetch_bulk_export_resource_rows", bulk_fetch)
    cancel_context_by_name = {"run_id": "run-atomic"}
    cancel_task_by_name = {"cancel_requested": False}

    fetch_result = await importer._fetch_resource_rows(
        _source(),
        "Practitioner",
        per_resource_limit=1,
        page_limit=0,
        page_count=0,
        timeout=3,
        run_id="run-atomic",
        row_batch_handler=AsyncMock(return_value=0),
        cancel_ctx=cancel_context_by_name,
        cancel_task=cancel_task_by_name,
        bulk_export=True,
        deadline_seconds=518400,
    )

    assert fetch_result is expected_fetch_result
    runtime_options = bulk_fetch.await_args.kwargs["runtime_options"]
    assert runtime_options.cancel_ctx is cancel_context_by_name
    assert runtime_options.cancel_task is cancel_task_by_name
    assert runtime_options.deadline_seconds == 518400
