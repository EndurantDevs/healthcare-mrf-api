# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Slow downloads and receive backpressure do not reserve preparation capacity."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, suppress
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from tests.test_provider_directory_bulk_preparation import _context, _fields, _identity, fhir, parallel


@asynccontextmanager
async def _transaction(*_args, **_kwargs):
    yield


def _source(executor, slots=4):
    source_by_field = {
        **_fields()["source"],
        "_bulk_preparation_identity": _identity(),
        "metadata_json": {
            "provider_directory_bulk_export_range_resume": True,
            "provider_directory_bulk_export_output_concurrency": 32,
        },
    }
    parallel.state(fhir, source_by_field)["preparation"] = {
        "executor": executor,
        "slots": asyncio.Semaphore(slots),
        "large_batch": asyncio.Semaphore(1),
    }
    return source_by_field


def _slow_file_handler(release, started, large):
    """Hold four responses after their first row until the test permits completion."""

    def line(index, row):
        resource_by_field = {"resourceType": "PractitionerRole", "id": f"role-{index}-{row}"}
        if large and index < 4:
            resource_by_field["meta"] = {"tag": [{"code": "x" * 2000}]}
        return json.dumps(resource_by_field).encode() + b"\n"

    async def serve(request):
        index = int(request.match_info["index"])
        started.add(index)
        response = web.StreamResponse()
        await response.prepare(request)
        await response.write(line(index, 0))
        if index < 4:
            await release.wait()
        with suppress(ConnectionResetError):
            await response.write(line(index, 1))
        return response

    return serve


def _tracked_batch_writer(writers_by_field):
    """Count simultaneous writes while preserving the original scheduling yield."""

    async def write(_model, rows):
        writers_by_field["active"] += 1
        writers_by_field["maximum"] = max(writers_by_field["maximum"], writers_by_field["active"])
        try:
            await asyncio.sleep(0)
            return len(rows.dataset_rows) if isinstance(rows, parallel.PreparedRows) else len(rows)
        finally:
            writers_by_field["active"] -= 1

    return write


def _assert_slow_file_outcome(cancel, stream_results, durable_by_index, states_by_index):
    """Slow files either retain their initial offsets or commit both complete rows."""
    if cancel:
        assert all(isinstance(stream_result, asyncio.CancelledError) for stream_result in stream_results[:4])
        assert all(states_by_index[index].committed_bytes == 0 for index in range(4))
    else:
        assert set(durable_by_index) == set(range(31)) and all(
            row_count == 2 for row_count, _ in durable_by_index.values()
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("large", [False, True])
@pytest.mark.parametrize("cancel", [False, True])
async def test_four_slow_files_leave_all_twenty_seven_ready_files_progressing(monkeypatch, large, cancel):
    """Slow responses preserve all ready-file progress and release capacity on cancellation."""
    monkeypatch.setattr(fhir, "source_checkpoint_transaction", _transaction)
    monkeypatch.setattr(fhir, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(fhir, "_pace_source_request", AsyncMock())
    if large:
        monkeypatch.setattr(parallel, "PREPARATION_MAX_BYTES", 1024)
    release, ready = asyncio.Event(), asyncio.Event()
    started, durable_by_index, states_by_index = set(), {}, {}
    writers_by_field = {"active": 0, "maximum": 0}
    write = _tracked_batch_writer(writers_by_field)

    async def read(index):
        async def progress(rows, boundary):
            durable_by_index[index] = (rows, boundary)
            if all(item in durable_by_index for item in range(4, 31)):
                ready.set()

        state = fhir._BulkOutputStreamState(_context(_fields(batch_size=2), source=source_by_field, progress=progress))
        states_by_index[index] = state
        object.__setattr__(state.context, "row_batch_handler", write)
        try:
            async with parallel.request(
                fhir, source_by_field, session, server.make_url(f"/{index}"), timeout=aiohttp.ClientTimeout(sock_read=5)
            ) as response:
                stream_result = await fhir._read_bulk_output_response(response, state)
                assert stream_result[4] is None
                return stream_result
        finally:
            state.release_preparation_slot()

    app = web.Application()
    app.router.add_get("/{index}", _slow_file_handler(release, started, large))
    with ThreadPoolExecutor(max_workers=4) as executor:
        source_by_field = _source(executor)
        async with TestServer(app) as server, aiohttp.ClientSession() as session:
            tasks = [asyncio.create_task(read(index)) for index in range(31)]
            try:
                await asyncio.wait_for(ready.wait(), 4)
                assert started == set(range(31)) and set(durable_by_index) == set(range(4, 31))
                for task in tasks[:4] if cancel else ():
                    task.cancel()
                release.set()
                stream_results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=cancel), 4)
                _assert_slow_file_outcome(cancel, stream_results, durable_by_index, states_by_index)
            finally:
                release.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        shared = source_by_field["_provider_directory_bulk_parallel"]
        assert shared["preparation"]["slots"]._value == 4
        assert shared["preparation"]["large_batch"]._value == 1
        assert shared["requests"].active == 0 and writers_by_field["maximum"] == 1


@pytest.mark.asyncio
async def test_skip_heavy_partial_batches_release_cpu_slots_without_building_duplicate_payloads(monkeypatch):
    monkeypatch.setattr(fhir, "source_checkpoint_transaction", _transaction)
    monkeypatch.setattr(parallel, "write_slot", lambda *_args: _transaction())
    parse = fhir._BulkOutputStreamState._parse_resource
    monkeypatch.setattr(
        fhir._BulkOutputStreamState,
        "_parse_resource",
        lambda stream, resource: None if resource["id"] == "skip" else parse(stream, resource),
    )
    with ThreadPoolExecutor(max_workers=4) as executor:
        source_by_field = _source(executor)
        states = [
            fhir._BulkOutputStreamState(_context(_fields(batch_size=4), source=source_by_field)) for _ in range(31)
        ]
        for state in states:
            object.__setattr__(state.context, "row_batch_handler", AsyncMock(return_value=4))

        def lines(ids):
            return b"".join(
                json.dumps({"resourceType": "PractitionerRole", "id": item}).encode() + b"\n" for item in ids
            )

        await asyncio.gather(
            *(fhir._consume_bulk_stream_chunk(state, lines(["skip"] * 3 + ["one"])) for state in states)
        )
        assert source_by_field["_provider_directory_bulk_parallel"]["preparation"]["slots"]._value == 4
        assert all(
            type(state.pending_rows) is list and len(state.pending_rows) == 1 and not state.raw_lines
            for state in states
        )
        await asyncio.gather(
            *(fhir._consume_bulk_stream_chunk(state, lines(["two", "three", "four"])) for state in states)
        )
        assert all(
            state.rows_written == 4 and not state.pending_rows and state.preparation_slot is None for state in states
        )


async def _consume_after_receive_pause(response, source_by_field, release, body):
    """Keep the read timeout paused while capacity is held, then finish the same response."""
    state = fhir._BulkOutputStreamState(_context(_fields(batch_size=64), source=source_by_field))
    object.__setattr__(state.context, "row_batch_handler", AsyncMock(return_value=64))
    task = asyncio.create_task(fhir._read_bulk_output_response(response, state))
    try:
        async with asyncio.timeout(2):
            while not (len(state.raw_lines) == 64 and response._protocol._reading_paused):
                await asyncio.sleep(0)
        await asyncio.sleep(0.2)
        release.set()
        source_by_field["_provider_directory_bulk_parallel"]["preparation"]["slots"].release()
        stream_result = await asyncio.wait_for(task, 4)
        assert stream_result[2] == 1024 and stream_result[4] is None and state.committed_bytes == len(body)
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        state.release_preparation_slot()


@pytest.mark.asyncio
async def test_receive_pause_stops_sock_read_clock_while_preparation_waits(monkeypatch):
    monkeypatch.setattr(fhir, "source_checkpoint_transaction", _transaction)
    monkeypatch.setattr(parallel, "write_slot", lambda *_args: _transaction())
    release = asyncio.Event()
    body = b"".join(
        json.dumps(
            {"resourceType": "PractitionerRole", "id": str(index), "meta": {"tag": [{"code": "x" * 1000}]}}
        ).encode()
        + b"\n"
        for index in range(1024)
    )

    async def serve(request):
        response = web.StreamResponse()
        await response.prepare(request)
        await response.write(body)
        await release.wait()
        return response

    app = web.Application()
    app.router.add_get("/output", serve)
    with ThreadPoolExecutor(max_workers=1) as executor:
        source_by_field = _source(executor, slots=0)
        async with TestServer(app) as server, aiohttp.ClientSession(read_bufsize=1024) as session:
            async with session.get(
                server.make_url("/output"), timeout=aiohttp.ClientTimeout(sock_read=0.1)
            ) as response:
                await _consume_after_receive_pause(response, source_by_field, release, body)
