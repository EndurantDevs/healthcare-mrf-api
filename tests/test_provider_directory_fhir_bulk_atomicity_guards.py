# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import hashlib
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryPractitioner
from process import provider_directory_source_checkpoint_control as checkpoint_control
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
            ownership_probe=AsyncMock(side_effect=RuntimeError("bulk_export_checkpoint_worker_guard_lost")),
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
    cancel_probe = AsyncMock(side_effect=importer.ImportCancelledError("cancelled"))

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
    ownership_probe = AsyncMock(side_effect=RuntimeError("bulk_export_checkpoint_worker_guard_lost"))

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


def _atomic_stream_state(monkeypatch, ownership_probe, during_copy):
    """Model durable rollback while exercising the real batch/progress callers."""
    durable_by_name = {"rows": [], "offset": 0}
    progress_by_name = {"committed_bytes": 0}

    @contextlib.asynccontextmanager
    async def transaction(*_args, **_kwargs):
        before_by_name = {"rows": list(durable_by_name["rows"]), "offset": durable_by_name["offset"]}
        try:
            yield
        except BaseException:
            durable_by_name.update(before_by_name)
            raise

    async def write(_model, rows):
        durable_by_name["rows"].extend(rows)
        await during_copy()
        return len(rows)

    async def record(_identity, _output_id, _rows, offset):
        durable_by_name["offset"] = offset

    monkeypatch.setattr(importer, "source_checkpoint_transaction", transaction)
    monkeypatch.setattr(importer, "_record_bulk_export_output_progress", record)
    options = dataclasses.replace(_stream_options(), row_batch_handler=write, ownership_probe=ownership_probe)
    progress_handler = partial(
        importer._persist_checkpointed_bulk_output_progress,
        _identity(),
        "output-atomic",
        0,
        progress_by_name,
        ownership_probe,
    )
    state = importer._BulkOutputStreamState(_atomic_stream_context(options, progress_handler))
    state.pending_rows.append({"resource_id": "one"})
    return state, durable_by_name, progress_by_name


def _atomic_stream_context(options, progress_handler):
    return importer._BulkOutputStreamContext(
        session=object(),
        source_record=_source(),
        url="https://example.test/output",
        model=options.model,
        resource_type="Practitioner",
        per_resource_limit=0,
        timeout=3,
        run_id=options.run_id,
        row_batch_handler=partial(importer._write_owned_bulk_batch, options),
        row_batch_size=1,
        retain_rows=False,
        resume_options=importer.BulkOutputResumeOptions(
            row_progress_handler=progress_handler,
            resume_offset=0,
            expected_etag=None,
            expected_content_length=None,
        ),
    )


@pytest.mark.parametrize("failure_after_copy", [False, True])
@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("bulk_export_checkpoint_ownership_lost"),
        RuntimeError("bulk_export_checkpoint_worker_guard_lost"),
        RuntimeError(importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED),
        importer.ImportCancelledError("cancelled"),
    ],
)
@pytest.mark.asyncio
async def test_bulk_batch_guard_loss_never_commits_payload_or_offsets(monkeypatch, failure_after_copy, failure):
    probe = AsyncMock(side_effect=[None, failure] if failure_after_copy else failure)
    copied = AsyncMock()
    state, durable_by_name, progress_by_name = _atomic_stream_state(monkeypatch, probe, copied)

    with pytest.raises(type(failure), match=str(failure)):
        await state.flush_pending_rows(51)

    assert durable_by_name == {"rows": [], "offset": 0}
    assert progress_by_name == {"committed_bytes": 0}
    assert state.committed_bytes == state.rows_written == 0
    assert state.pending_rows == [{"resource_id": "one"}]
    assert copied.await_count == int(failure_after_copy)
    assert probe.await_count == 1 + int(failure_after_copy)


@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("bulk_export_checkpoint_worker_guard_lost"),
        RuntimeError(importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED),
        importer.ImportCancelledError("cancelled"),
    ],
)
@pytest.mark.asyncio
async def test_chunk_probe_failure_does_not_flush_staged_rows(monkeypatch, failure):
    copied = AsyncMock()
    state, durable_by_name, progress_by_name = _atomic_stream_state(monkeypatch, AsyncMock(), copied)
    state.pending_rows.clear()
    state.context = dataclasses.replace(
        state.context,
        row_batch_size=10,
        resume_options=dataclasses.replace(
            state.context.resume_options,
            cancel_probe=AsyncMock(side_effect=[None, failure]),
        ),
    )

    async def chunks(_size):
        yield b'{"resourceType":"Practitioner","id":"one"}\n'
        yield b'{"resourceType":"Practitioner","id":"two"}\n'

    response = SimpleNamespace(status=200, headers={}, content=SimpleNamespace(iter_chunked=chunks))
    with pytest.raises(type(failure), match=str(failure)):
        await importer._read_bulk_output_response(response, state)

    copied.assert_not_awaited()
    assert durable_by_name == {"rows": [], "offset": 0}
    assert progress_by_name == {"committed_bytes": 0}
    assert state.committed_bytes == state.rows_written == 0
    assert [row["resource_id"] for row in state.pending_rows] == ["one"]


@pytest.mark.asyncio
async def test_bulk_cancelled_blocked_batch_drains_before_offset_commit(monkeypatch):
    started, drained = asyncio.Event(), asyncio.Event()

    async def blocked_copy():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            drained.set()

    state, durable_by_name, progress_by_name = _atomic_stream_state(monkeypatch, AsyncMock(), blocked_copy)
    task = asyncio.create_task(state.flush_pending_rows(51))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert drained.is_set()
    assert durable_by_name == {"rows": [], "offset": 0}
    assert progress_by_name == {"committed_bytes": 0}
    assert state.committed_bytes == state.rows_written == 0
    assert state.pending_rows == [{"resource_id": "one"}]


@pytest.mark.asyncio
async def test_bulk_stream_probe_checks_lock_without_source_transaction(monkeypatch):
    reads = []

    @contextlib.asynccontextmanager
    async def source_read(_directory):
        reads.append("source")
        yield

    connection = SimpleNamespace(scalar=AsyncMock(return_value=True))
    connection.execution_options = lambda **_kwargs: connection

    @contextlib.asynccontextmanager
    async def connect():
        yield connection

    monkeypatch.setattr(importer.db, "engine", SimpleNamespace(connect=connect))
    monkeypatch.setattr(importer.profile_scope_copy, "source_read_transaction", source_read)
    owner = AsyncMock()
    monkeypatch.setattr(importer, "_assert_bulk_checkpoint_guard_owner", owner)
    async with importer._bulk_checkpoint_worker_guard(_identity()) as probes:
        await probes.stream_probe()
        assert reads == []
        owner.assert_not_awaited()
        statement, parameters = connection.scalar.await_args.args
        assert "pg_locks" in str(statement) and "pid=pg_backend_pid()" in str(statement)
        assert parameters == {"checkpoint_id": _identity().checkpoint_id}
        await probes()
        await probes()
        assert reads == ["source", "source"]
        assert owner.await_count == 2


@pytest.mark.parametrize("result", [False, None, 1, OSError("connection lost")])
@pytest.mark.asyncio
async def test_bulk_stream_probe_refuses_missing_original_lock(result):
    scalar = AsyncMock(side_effect=result) if isinstance(result, Exception) else AsyncMock(return_value=result)
    with pytest.raises(RuntimeError, match="bulk_export_checkpoint_worker_guard_lost"):
        await checkpoint_control.assert_bulk_checkpoint_guard_live(
            importer, SimpleNamespace(scalar=scalar), _identity()
        )


@pytest.mark.asyncio
async def test_stream_and_write_probes_share_cancellation_and_deadline(monkeypatch):
    owner, lock = AsyncMock(), AsyncMock()
    calls = []
    deadline = object()
    monkeypatch.setattr(importer, "_bulk_deadline_at", lambda _seconds: deadline)

    async def cancel(ctx, task, observed_deadline):
        calls.append((ctx, task, observed_deadline))

    monkeypatch.setattr(importer, "_bulk_cancel_probe", cancel)
    options = importer.BulkExportFetchOptions(
        timeout=3,
        run_id="run-atomic",
        row_batch_handler=AsyncMock(),
        row_batch_size=1,
        retain_rows=False,
        cancel_ctx={},
        cancel_task={},
        deadline_seconds=10,
    )
    preclaim, runtime = importer._bulk_fetch_runtime_probes(importer._BulkCheckpointProbes(owner, lock), options)
    await preclaim()
    await runtime.stream_probe()
    owner.assert_not_awaited()
    lock.assert_awaited_once()
    await runtime()
    owner.assert_awaited_once()
    assert calls == [(options.cancel_ctx, options.cancel_task, deadline)] * 3
