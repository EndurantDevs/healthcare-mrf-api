# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryPractitioner
from tests.test_provider_directory_fhir_bulk_atomicity import (
    _identity,
    _manifest,
    _output_checkpoint,
    _source,
    _stream_options,
    importer,
)

@pytest.mark.asyncio
async def test_bulk_start_rechecks_guard_after_http_before_checkpoint_write(
    monkeypatch,
):
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(
            return_value=(
                202,
                {"content-location": "https://apif1.aetna.com/status/one"},
                None,
                None,
            )
        ),
    )
    accept_start = AsyncMock()
    fail_unknown = AsyncMock()
    release_reservation = AsyncMock()
    monkeypatch.setattr(importer, "_accept_checkpointed_bulk_start", accept_start)
    monkeypatch.setattr(importer, "_fail_unknown_bulk_export_acceptance", fail_unknown)
    monkeypatch.setattr(importer, "_release_bulk_export_reservation", release_reservation)

    with pytest.raises(
        RuntimeError,
        match="bulk_export_checkpoint_worker_guard_lost",
    ):
        await importer._start_checkpointed_bulk_export(
            object(),
            _source(),
            _identity(),
            timeout=3,
            ownership_probe=AsyncMock(
                side_effect=RuntimeError(
                    "bulk_export_checkpoint_worker_guard_lost"
                )
            ),
        )

    accept_start.assert_not_awaited()
    fail_unknown.assert_not_awaited()
    release_reservation.assert_not_awaited()


@pytest.mark.asyncio
async def test_preclaim_and_owned_probes_share_one_deadline(monkeypatch):
    deadline = object()
    observed_deadlines: list[object] = []

    monkeypatch.setattr(importer, "_bulk_deadline_at", lambda _seconds: deadline)

    async def cancel_probe(_ctx, _task, deadline_at):
        observed_deadlines.append(deadline_at)

    async def runtime_probe(_ownership, _ctx, _task, deadline_at):
        observed_deadlines.append(deadline_at)

    monkeypatch.setattr(importer, "_bulk_cancel_probe", cancel_probe)
    monkeypatch.setattr(importer, "_bulk_checkpoint_runtime_probe", runtime_probe)
    preclaim_probe, owned_probe = importer._bulk_fetch_runtime_probes(
        AsyncMock(),
        importer.BulkExportFetchOptions(
            timeout=3,
            run_id="run-atomic",
            row_batch_handler=AsyncMock(return_value=0),
            row_batch_size=1,
            retain_rows=False,
            deadline_seconds=10,
        ),
    )

    await preclaim_probe()
    await owned_probe()

    assert observed_deadlines == [deadline, deadline]


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
        ownership_probe=AsyncMock(),
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
async def test_bulk_stream_propagates_fencing_loss_before_get():
    ownership_probe = AsyncMock(
        side_effect=RuntimeError("bulk_export_checkpoint_worker_guard_lost")
    )

    with pytest.raises(
        RuntimeError,
        match="bulk_export_checkpoint_worker_guard_lost",
    ):
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
                cancel_probe=ownership_probe,
            ),
            requires_access_token=False,
        )

    ownership_probe.assert_awaited_once()


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
