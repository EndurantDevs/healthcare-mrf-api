# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared bulk rows retain ordinary parsing, COPY bytes and atomic boundaries."""

import asyncio
import datetime
import importlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_bulk_parallel as parallel
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_cms_resource_batch import prepare_dataset_rows

fhir = importlib.import_module("process.provider_directory_fhir")


def _identity():
    return {
        "dataset_id": "dataset-example",
        "endpoint_id": "endpoint-example",
        "acquisition_root_run_id": "run-example",
        "source_ids": ["source-example"],
        "resource_hash_contract": fhir.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "semantic_projection_as_of": "2026-01-01",
    }


def _fields(resource_type="PractitionerRole", batch_size=5000):
    return {
        "source": {"source_id": "source-example", "_semantic_projection_as_of": "2026-01-01"},
        "url": "https://example.test/output",
        "resource_type": resource_type,
        "run_id": "run-example",
        "batch_size": batch_size,
    }


def _context(fields, *, source=None, progress=None, offset=0, length=None):
    return fhir._BulkOutputStreamContext(
        session=None,
        source_record=source or fields["source"],
        url=fields["url"],
        model=fhir.RESOURCE_MODELS_BY_TYPE[fields["resource_type"]],
        resource_type=fields["resource_type"],
        per_resource_limit=0,
        timeout=3,
        run_id=fields["run_id"],
        row_batch_handler=AsyncMock(),
        row_batch_size=fields["batch_size"],
        retain_rows=False,
        resume_options=fhir.BulkOutputResumeOptions(progress or AsyncMock(), offset, '"etag"', length),
    )


def _copy_batches(rows):
    return [
        batch
        for offset in range(0, len(rows), 4096)
        for batch in storage.copy_record_batches(
            storage._records(rows[offset : offset + 4096], storage.RESOURCE_COLUMNS, 1)
        )
    ]


def test_capability_identity_is_cached_from_the_original_url_once(monkeypatch):
    fields_by_name = {**_fields(), "url": "https://example.test/output?token=synthetic"}
    original, calls = fhir._bulk_capability_log_identity, []
    expected = original(fields_by_name["url"])
    assert expected != original(expected)

    def identity(url):
        calls.append(url)
        return original(url)

    monkeypatch.setattr(fhir, "_bulk_capability_log_identity", identity)
    state = fhir._BulkOutputStreamState(_context(fields_by_name))
    for index in range(2):
        assert state.handle_line(json.dumps({"resourceType": "PractitionerRole", "id": str(index)}).encode()) is None
    assert calls == [fields_by_name["url"]]
    assert all(row["resource_url"] == fhir._sanitized_fhir_url_identity(expected) for row in state.pending_rows)


@pytest.mark.parametrize("resource_type", fhir.DEFAULT_RESOURCES)
def test_worker_preserves_all_resource_parsing_canonical_copy_and_proof(monkeypatch, resource_type):
    monkeypatch.setattr(fhir, "_now", lambda: datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC))
    resource_by_field = {
        "resourceType": resource_type,
        "id": "unicode-é",
        "name": "Example é",
        "position": {"longitude": -0.0, "latitude": 1.25},
        "identifier": [{"system": "https://example.test/id", "value": 2**100}],
    }
    if resource_type == "Practitioner":
        resource_by_field["name"] = [{"family": "Example é"}]
    raw = json.dumps(resource_by_field, ensure_ascii=False).encode()
    lines = [b"\xef\xbb\xbf" + raw + b"\r", b"  ", raw, raw.replace(b"unicode", b"invalid\xff")]
    fields, identity = _fields(resource_type), _identity()
    original = fhir._BulkOutputStreamState(_context(fields))
    for line in lines:
        assert original.handle_line(line) is None
    original_rows = fhir._resource_rows_with_location_contacts(original.context.model, original.pending_rows)
    rows, proof = prepare_dataset_rows(fhir, original.context.model, original_rows, SimpleNamespace(**identity))
    prepared, fetched, error = parallel.prepare_lines(lines, fields, [], identity, True)
    assert error is None and fetched == original.rows_fetched == 3
    assert isinstance(prepared, parallel.PreparedRows)
    assert _copy_batches(prepared.dataset_rows) == _copy_batches(rows)
    assert prepared.proof == proof


@pytest.mark.parametrize(
    "line,error",
    [
        (b"{", "invalid_ndjson"),
        (b"[]", "invalid_ndjson"),
        (b'{"resourceType":"Location"}', "bulk_export_output_resource_type_mismatch"),
    ],
)
def test_worker_preserves_strict_stream_errors(line, error):
    rows, fetched, observed = parallel.prepare_lines([line], _fields(), [], _identity(), True)
    assert observed == error and rows == [] and fetched == 0


def test_whole_batch_duplicate_proof_spans_copy_split():
    lines = [json.dumps({"resourceType": "PractitionerRole", "id": f"role-{index}"}).encode() for index in range(5000)]
    lines[4096] = lines[0]
    prepared, fetched, error = parallel.prepare_lines(lines, _fields(), [], _identity(), True)
    assert error is None and fetched == 5000 and len(prepared.dataset_rows) == 4999
    assert len(_copy_batches(prepared.dataset_rows)) == 2
    assert prepared.proof[0]["resource_count"] == 4999


def _resume_batch_handlers(durable_by_field, failure, large_batch_fallback):
    """Model one atomic batch with the same COPY, checkpoint and cancellation faults."""

    @asynccontextmanager
    async def transaction(*_args, **_kwargs):
        before_by_field = {"rows": list(durable_by_field["rows"]), "offset": durable_by_field["offset"]}
        try:
            yield
        except BaseException:
            durable_by_field.update(before_by_field)
            raise

    async def write(_model, rows):
        assert isinstance(rows, parallel.PreparedRows) is not large_batch_fallback
        dataset_rows = (
            prepare_dataset_rows(fhir, _model, rows, SimpleNamespace(**_identity()))[0]
            if large_batch_fallback
            else rows.dataset_rows
        )
        durable_by_field["rows"].extend(dataset_rows)
        if failure == "copy":
            raise ValueError("copy_failed")
        return len(dataset_rows)

    async def progress(_rows, boundary):
        if failure == "checkpoint":
            raise ValueError("checkpoint_failed")
        if failure == "cancel":
            raise asyncio.CancelledError()
        durable_by_field["offset"] = boundary

    return transaction, write, progress


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "copy", "checkpoint", "cancel"])
@pytest.mark.parametrize("large_batch_fallback", [False, True])
async def test_prepared_resume_keeps_utf8_boundaries_and_rolls_back_failed_batch(
    monkeypatch, failure, large_batch_fallback
):
    """Commit complete UTF-8 rows and offsets together, or retain the exact preimage."""
    if large_batch_fallback:
        monkeypatch.setattr(parallel, "PREPARATION_MAX_BYTES", 1)
    fields, source_by_field = _fields(batch_size=2), _fields()["source"]
    source_by_field["_bulk_preparation_identity"] = _identity()
    durable_by_field = {"rows": [], "offset": 13}
    with ThreadPoolExecutor(max_workers=1) as executor:
        slots = asyncio.Semaphore(1)
        source_by_field["_provider_directory_bulk_parallel"] = {
            "preparation": {"executor": executor, "slots": slots, "large_batch": asyncio.Semaphore(1)}
        }
        transaction, write, progress = _resume_batch_handlers(durable_by_field, failure, large_batch_fallback)
        monkeypatch.setattr(fhir, "source_checkpoint_transaction", transaction)
        monkeypatch.setattr(parallel, "write_slot", lambda *_args: transaction())
        raw = (
            b'{"resourceType":"PractitionerRole","id":"one-\xc3\xa9"}\r\n\n'
            + b'{"resourceType":"PractitionerRole","id":"two"}\n'
        )
        state = fhir._BulkOutputStreamState(
            _context(fields, source=source_by_field, progress=progress, offset=13, length=13 + len(raw))
        )
        object.__setattr__(state.context, "row_batch_handler", write)
        split = raw.index(b"\xc3") + 1
        try:
            assert await fhir._consume_bulk_stream_chunk(state, raw[:split]) == (False, None)
            assert durable_by_field["offset"] == state.committed_bytes == 13
            if failure:
                expected = asyncio.CancelledError if failure == "cancel" else ValueError
                with pytest.raises(expected):
                    await fhir._consume_bulk_stream_chunk(state, raw[split:])
                assert durable_by_field == {"rows": [], "offset": 13}
                assert state.committed_bytes == 13 and state.rows_written == 0
            else:
                assert await fhir._consume_bulk_stream_chunk(state, raw[split:]) == (False, None)
                assert durable_by_field["offset"] == state.committed_bytes == 13 + len(raw)
                assert state.rows_written == 2 and len(durable_by_field["rows"]) == 2
        finally:
            state.release_preparation_slot()
        assert slots._value == 1
        assert source_by_field["_provider_directory_bulk_parallel"]["preparation"]["large_batch"]._value == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_waiter", [False, True])
async def test_four_held_batches_bound_a_fifth_and_release_on_cleanup(cancel_waiter):
    source_by_field = {**_fields()["source"], "_bulk_preparation_identity": _identity()}
    slots = asyncio.Semaphore(4)
    source_by_field["_provider_directory_bulk_parallel"] = {"preparation": {"executor": None, "slots": slots}}
    states = [fhir._BulkOutputStreamState(_context(_fields(batch_size=1), source=source_by_field)) for _ in range(5)]
    line = b'{"resourceType":"PractitionerRole","id":"one"}'
    for state in states[:4]:
        assert await parallel.stage_line(state, line) is None
    waiting = asyncio.create_task(parallel.stage_line(states[-1], line))
    try:
        await asyncio.sleep(0)
        assert not waiting.done() and slots._value == 0
        if cancel_waiter:
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
            assert states[-1].preparation_slot is None
        else:
            states[0].release_preparation_slot()
            assert await asyncio.wait_for(waiting, 1) is None
        assert all(state.committed_bytes == state.rows_written == 0 for state in states)
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        for state in states:
            state.release_preparation_slot()
    assert slots._value == 4


def _scan_options():
    return SimpleNamespace(
        deferred_materialization=True,
        stream_batch_size=5000,
        per_resource_limit=0,
        page_limit=0,
        linked_resource_limit=0,
        source_concurrency=1,
        checkpointing_enabled=True,
        stale_cleanup=False,
        scan_role_reverse_lookup_planned=False,
    )


def _checkpoint_context():
    return SimpleNamespace(
        canonical_api_base="https://example.test/fhir",
        source_scope_hash="scope-example",
        owner_run_id="run-example",
        acquisition_root_run_id="run-example",
        endpoint_id="endpoint-example",
        dataset_id="dataset-example",
    )


@pytest.mark.asyncio
async def test_scan_cancellation_drains_spawned_worker(monkeypatch):
    source_by_field = {
        **_fields()["source"],
        "_bulk_preparation_identity": _identity(),
        "_pagination_checkpoint_context": _checkpoint_context(),
    }
    options = _scan_options()
    monkeypatch.setattr(fhir, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(fhir, "_is_bulk_range_resume_enabled", lambda _source: True)
    monkeypatch.setattr(parallel, "is_bulk_scan", lambda *_args: True)
    monkeypatch.setattr(parallel.os, "process_cpu_count", lambda: 1)
    pid = None
    with pytest.raises(asyncio.CancelledError):
        async with parallel.preparation_scope(fhir, source_by_field, options):
            executor = source_by_field["_provider_directory_bulk_parallel"]["preparation"]["executor"]
            loop = asyncio.get_running_loop()
            pid = await loop.run_in_executor(executor, os.getpid)
            running = loop.run_in_executor(executor, time.sleep, 0.1)
            asyncio.current_task().cancel()
            await running
    assert "preparation" not in source_by_field["_provider_directory_bulk_parallel"]
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_worker_preparation_requires_the_durable_checkpoint_path(monkeypatch, durable):
    source_by_field = {**_fields()["source"], "_bulk_preparation_identity": _identity()}
    if durable:
        source_by_field["_pagination_checkpoint_context"] = _checkpoint_context()
    pool, drain = Mock(return_value=object()), AsyncMock()
    monkeypatch.setattr(fhir, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(fhir, "_is_bulk_range_resume_enabled", lambda _source: True)
    monkeypatch.setattr(parallel, "is_bulk_scan", lambda *_args: True)
    monkeypatch.setattr(parallel, "ProcessPoolExecutor", pool)
    monkeypatch.setattr(importlib.import_module("process.provider_directory_cms_npd"), "_drain_stage_executor", drain)
    async with parallel.preparation_scope(fhir, source_by_field, _scan_options()):
        assert ("preparation" in source_by_field.get("_provider_directory_bulk_parallel", {})) == durable
    assert pool.call_count == drain.await_count == int(durable)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_inline_large_batch_yields_before_completion_without_committing(monkeypatch, cancel):
    line = b'{"resourceType":"PractitionerRole","id":"one"}'
    count = 257
    monkeypatch.setattr(parallel, "PREPARATION_MAX_BYTES", len(line) * count)
    preparation_by_field = {"executor": None, "slots": asyncio.Semaphore(1), "large_batch": asyncio.Semaphore(1)}
    source_by_field = {
        **_fields()["source"],
        "_provider_directory_bulk_parallel": {"preparation": preparation_by_field},
    }
    state = fhir._BulkOutputStreamState(_context(_fields(batch_size=count + 1), source=source_by_field))
    for _ in range(count - 1):
        assert await parallel.stage_line(state, line) is None
    observed, task = [], asyncio.current_task()

    async def observe():
        observed.append(state.rows_fetched)
        if cancel:
            task.cancel()

    observer = asyncio.create_task(observe())
    try:
        if cancel:
            with pytest.raises(asyncio.CancelledError):
                await parallel.stage_line(state, line)
        else:
            assert await parallel.stage_line(state, line) is None
        await observer
        assert observed == [256]
        assert state.rows_fetched == (256 if cancel else count)
        assert state.committed_bytes == state.rows_written == 0
    finally:
        await asyncio.gather(observer, return_exceptions=True)
        state.release_preparation_slot()
    assert preparation_by_field["large_batch"]._value == 1


@pytest.mark.asyncio
async def test_inline_large_batch_checks_cooperative_stop_at_yield(monkeypatch):
    """A cooperative stop discards the rest of an uncommitted inline batch."""
    line = b'{"resourceType":"PractitionerRole","id":"one"}'
    resource_count = 520
    monkeypatch.setattr(parallel, "PREPARATION_MAX_BYTES", len(line) * resource_count)
    preparation_by_field = {"executor": None, "slots": asyncio.Semaphore(1), "large_batch": asyncio.Semaphore(1)}
    source_by_field = {
        **_fields()["source"],
        "_provider_directory_bulk_parallel": {"preparation": preparation_by_field},
    }
    stopped = asyncio.Event()

    async def probe():
        if stopped.is_set():
            raise fhir.ImportCancelledError("synthetic_stop")

    context = _context(_fields(batch_size=resource_count + 1), source=source_by_field)
    context = replace(context, resume_options=replace(context.resume_options, cancel_probe=probe))
    state = fhir._BulkOutputStreamState(context)
    for _ in range(resource_count - 1):
        assert await parallel.stage_line(state, line) is None

    async def stop():
        stopped.set()

    observer = asyncio.create_task(stop())
    try:
        with pytest.raises(fhir.ImportCancelledError, match="synthetic_stop"):
            await parallel.stage_line(state, line)
        await observer
        assert state.rows_fetched == 256
        assert state.committed_bytes == state.rows_written == 0
        context.row_batch_handler.assert_not_awaited()
        context.resume_options.row_progress_handler.assert_not_awaited()
    finally:
        await asyncio.gather(observer, return_exceptions=True)
        state.release_preparation_slot()
    assert preparation_by_field["large_batch"]._value == 1
