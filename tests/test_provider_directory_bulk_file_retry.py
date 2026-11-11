# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Broken bulk streams resume one durable file while healthy files keep running."""

import asyncio
import copy
import dataclasses
import datetime
import hashlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import urlsplit

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from process import provider_directory_bulk_parallel as parallel
from tests.provider_directory_checkpoint_unit_support import checkpoint_unit_scope as checkpoint_unit_scope
from tests.test_provider_directory_bulk_parallel import _source, directory
from tests.test_provider_directory_fhir_bulk_resume import _checkpoint_identity, _resume_stream_options


@pytest.fixture
def files(monkeypatch):
    """Use real NDJSON and validators with an explicit in-memory durable ledger."""
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "synthetic-bulk-retry-key")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_FETCH_ATTEMPTS", "3")
    monkeypatch.setattr(directory, "_source_fetch_retry_delay_seconds", lambda _attempt: 0.01)
    source_record = _source()
    source_record.update(canonical_api_base="https://files.example.org", api_base="https://files.example.org")
    identity = dataclasses.replace(
        _checkpoint_identity(),
        canonical_api_base=source_record["canonical_api_base"],
        start_url=source_record["canonical_api_base"] + "/$export?_type=Practitioner",
    )
    bodies = [
        [
            json.dumps({"resourceType": "Practitioner", "id": f"{output}-{resource_row}"}).encode() + b"\n"
            for resource_row in range(3)
        ]
        for output in range(4)
    ]
    manifest = directory._bulk_export_manifest_from_payload(
        {
            "transactionTime": "2026-07-20T00:00:00Z",
            "request": identity.start_url,
            "requiresAccessToken": False,
            "output": [{"type": "Practitioner", "url": f"https://files.example.org/{i}"} for i in range(4)],
        },
        "Practitioner",
    )
    checkpoint_by_index = {}
    for output in manifest.outputs:
        index = output.output_index
        etag = f'"file-{index}"'
        checkpoint_by_index[index] = {
            "output_id": directory._bulk_manifest_output_id(identity.checkpoint_id, output),
            "output_index": index,
            "resource_type": "Practitioner",
            "output_url_hash": output.url_hash,
            "state": directory.BULK_EXPORT_OUTPUT_PENDING,
            "rows_written": int(index == 0),
            "committed_bytes": len(bodies[0][0]) if index == 0 else 0,
            "content_length_bytes": sum(map(len, bodies[index])),
            "etag_ciphertext": directory._encrypt_bulk_capability(etag),
            "etag_hash": hashlib.sha256(etag.encode()).hexdigest(),
            "validator_checked_at": datetime.datetime.now(datetime.UTC),
        }
    case = SimpleNamespace(
        source_record=source_record,
        identity=identity,
        manifest=manifest,
        bodies=bodies,
        checkpoint_by_index=checkpoint_by_index,
        committed=asyncio.Event(),
        written=[],
        record_error=AsyncMock(),
    )
    _patch_file_ledger(monkeypatch, case)
    return case


def _patch_file_ledger(monkeypatch, case):
    """Keep unit storage separate from the native guard-session regression."""
    by_id = {checkpoint["output_id"]: checkpoint for checkpoint in case.checkpoint_by_index.values()}

    async def begin(_identity, output_id, *, preserve_progress):
        assert preserve_progress
        by_id[output_id]["state"] = directory.BULK_EXPORT_OUTPUT_STREAMING

    async def progress(_identity, output_id, rows, offset):
        checkpoint = by_id[output_id]
        checkpoint.update(rows_written=rows, committed_bytes=offset)
        if checkpoint["output_index"] == 0:
            case.committed.set()

    async def finish(_identity, output_id, rows, offset, **options):
        assert options.get("require_validator", True)
        assert by_id[output_id]["rows_written"] == rows and by_id[output_id]["committed_bytes"] == offset
        by_id[output_id]["state"] = directory.BULK_EXPORT_OUTPUT_COMPLETE

    async def failed(_identity, output_id, rows, offset, error, *, record_checkpoint):
        assert record_checkpoint is False
        assert by_id[output_id]["rows_written"] == rows and by_id[output_id]["committed_bytes"] == offset
        by_id[output_id]["state"] = (
            directory.BULK_EXPORT_OUTPUT_FAILED
            if directory._is_bulk_export_error_terminal(error)
            else directory.BULK_EXPORT_OUTPUT_PENDING
        )

    async def write(_model, rows):
        case.written.extend(resource_row["resource_id"] for resource_row in rows)
        return len(rows)

    for name, callback in (
        ("_begin_bulk_export_output", begin),
        ("_record_bulk_export_output_progress", progress),
        ("_complete_bulk_export_output", finish),
        ("_record_bulk_export_output_error", failed),
    ):
        monkeypatch.setattr(directory, name, callback)
    monkeypatch.setattr(
        directory,
        "_load_bulk_output_checkpoints",
        AsyncMock(side_effect=lambda _id: copy.deepcopy(list(case.checkpoint_by_index.values()))),
    )
    monkeypatch.setattr(directory, "_record_bulk_export_checkpoint_error", case.record_error)
    case.options = dataclasses.replace(_resume_stream_options(4), row_batch_handler=write)


async def _serve_file(case, request):
    index = int(request.match_info["index"])
    headers_by_name = dict(request.headers)
    case.requests[index].append(headers_by_name)
    attempt = len(case.requests[index])
    if index:
        await case.release_healthy.wait()
    elif attempt > 1:
        case.release_healthy.set()
    if index == 0 and case.damage == "rate-limit":
        return web.Response(status=429, headers={"Retry-After": "0"})
    body = b"".join(case.files.bodies[index])
    offset = int(headers_by_name.get("Range", "bytes=0-").split("=")[1][:-1])
    response_headers_by_name = {"ETag": f'"file-{index}"', "Content-Length": str(len(body) - offset)}
    if offset:
        response_headers_by_name["Content-Range"] = f"bytes {offset}-{len(body) - 1}/{len(body)}"
    if index == 0 and attempt > 1:
        if case.damage == "etag":
            response_headers_by_name["ETag"] = '"changed"'
        elif case.damage == "length":
            response_headers_by_name["Content-Length"] = str(len(body) - offset + 1)
        elif case.damage == "range":
            response_headers_by_name["Content-Range"] = f"bytes 0-{len(body) - 1}/{len(body)}"
    response = web.StreamResponse(status=206 if offset else 200, headers=response_headers_by_name)
    await response.prepare(request)
    if index == 0 and (attempt == 1 or case.damage == "exhausted"):
        if attempt == 1:
            await response.write(case.files.bodies[0][1])
            await case.files.committed.wait()
        await response.write(b'{"resourceType":')
        request.transport.close()
    else:
        await response.write(body[offset:])
        await response.write_eof()
    return response


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "etag", "length", "range", "exhausted", "rate-limit"])
async def test_real_truncated_response_resumes_only_its_committed_file(files, damage):
    case = SimpleNamespace(
        files=files, damage=damage, requests={i: [] for i in range(4)}, release_healthy=asyncio.Event()
    )
    app = web.Application()
    app.router.add_get("/{index}", lambda request: _serve_file(case, request))
    async with TestServer(app) as server, aiohttp.ClientSession() as session:

        class LocalSession:
            def get(self, url, **options):
                return session.get(server.make_url(urlsplit(url).path), **options)

        try:
            state, error = await asyncio.wait_for(
                directory._resume_checkpointed_bulk_outputs(
                    LocalSession(),
                    files.source_record,
                    files.identity,
                    files.manifest,
                    files.options,
                    copy.deepcopy(list(files.checkpoint_by_index.values())),
                ),
                3,
            )
        finally:
            case.release_healthy.set()
    assert len(case.requests[0]) == (3 if damage in ("exhausted", "rate-limit") else 2)
    assert case.requests[0][0]["Range"] == f"bytes={len(files.bodies[0][0])}-"
    committed_rows = 1 if damage == "rate-limit" else 2
    assert all(
        request["Range"] == f"bytes={sum(map(len, files.bodies[0][:committed_rows]))}-"
        for request in case.requests[0][1:]
    )
    assert all(request["If-Match"] == '"file-0"' for request in case.requests[0])
    assert len(files.written) == len(set(files.written))
    assert "0-0" not in files.written
    if damage is None:
        assert error is None and state.rows_written_this_run == 11
        assert set(files.written) == {f"{i}-{j}" for i in range(4) for j in range(3)} - {"0-0"}
        assert all(
            resource_row["state"] == directory.BULK_EXPORT_OUTPUT_COMPLETE
            for resource_row in files.checkpoint_by_index.values()
        )
        files.record_error.assert_not_awaited()
    elif damage in ("exhausted", "rate-limit"):
        assert error == (
            "bulk_export_output_http_429" if damage == "rate-limit" else "bulk_export_transport_clientpayloaderror"
        )
        assert files.checkpoint_by_index[0]["state"] == directory.BULK_EXPORT_OUTPUT_PENDING
        assert all(files.checkpoint_by_index[i]["state"] == directory.BULK_EXPORT_OUTPUT_COMPLETE for i in range(1, 4))
        files.record_error.assert_awaited_once_with(files.identity, error, terminal=False)
    else:
        assert error == (
            "bulk_export_output_etag_mismatch" if damage == "etag" else "bulk_export_output_content_range_invalid"
        )
        assert "0-2" not in files.written
        files.record_error.assert_awaited_once_with(files.identity, error, terminal=True)
    assert all(len(case.requests[i]) == 1 for i in range(1, 4))


@pytest.mark.parametrize(
    "error",
    ["bulk_export_output_http_401", "bulk_export_output_validator_mismatch"],
)
def test_file_isolation_keeps_authentication_and_validator_failures_fatal(error):
    assert not parallel.is_transient_output_error(directory, error)


@pytest.mark.asyncio
async def test_bulk_budget_reduction_rechecks_transfers_waiting_for_pacing(monkeypatch):
    source_record = _source()
    shared = parallel.state(directory, source_record)
    all_entered, release = asyncio.Event(), asyncio.Event()
    active_counts = []

    async def pace(_source):
        if pace_probe.await_count == 3:
            all_entered.set()
        await release.wait()

    @asynccontextmanager
    async def response(_url, **_options):
        active_counts.append(shared["requests"].active)
        yield SimpleNamespace(status=200, headers={})

    async def transfer():
        async with parallel.request(
            directory, source_record, SimpleNamespace(get=response), "https://files.example.org/part"
        ) as received:
            assert received.status == 200

    pace_probe = AsyncMock(side_effect=pace)
    monkeypatch.setattr(directory, "_pace_source_request", pace_probe)
    tasks = []
    try:
        tasks = [asyncio.create_task(transfer()) for _ in range(3)]
        await asyncio.wait_for(all_entered.wait(), 3)
        shared["requests"].reduce()
        shared["requests"].reduce()
        assert shared["requests"].limit == 2
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 3)
    finally:
        release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert pace_probe.await_count > 3
    assert len(active_counts) == 3 and max(active_counts) <= 2
    assert shared["requests"].active == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expiry_error",
    [
        "bulk_export_output_url_expired",
        "bulk_export_output_http_403",
        "bulk_export_output_http_404",
        "bulk_export_output_http_410",
    ],
)
@pytest.mark.parametrize("transient_order", [None, "before", "after"])
async def test_known_url_expiry_preserves_healthy_open_responses(files, monkeypatch, expiry_error, transient_order):
    """An expired capability waits for existing transfers without retrying it."""
    errors = (expiry_error,)
    if transient_order:
        errors = ("bulk_export_output_http_503", expiry_error)
        if transient_order == "after":
            errors = tuple(reversed(errors))
    collection_events = [asyncio.Event() for _ in errors]
    observed_errors = []
    completed_output_ids = []
    collect = directory._completed_bulk_output_error

    def observed_error(completed, active):
        error = collect(completed, active)
        if error:
            observed_errors.append(error)
            collection_events[len(observed_errors) - 1].set()
        return error

    async def stream(_session, _source, _identity, _manifest, output, *_args):
        if output.output_index < len(errors):
            if output.output_index:
                await collection_events[output.output_index - 1].wait()
            return errors[output.output_index]
        await collection_events[-1].wait()
        completed_output_ids.append(output.output_index)
        return None

    monkeypatch.setattr(directory, "_completed_bulk_output_error", observed_error)
    monkeypatch.setattr(directory, "_stream_one_checkpointed_bulk_output", stream)
    _state, error = await directory._resume_checkpointed_bulk_outputs(
        object(),
        files.source_record,
        files.identity,
        files.manifest,
        files.options,
        copy.deepcopy(list(files.checkpoint_by_index.values())),
    )
    assert error == expiry_error
    assert observed_errors == list(errors)
    assert sorted(completed_output_ids) == list(range(len(errors), 4))
    files.record_error.assert_awaited_once_with(files.identity, error, terminal=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("bulk_export_checkpoint_worker_guard_lost"),
        RuntimeError("bulk_export_checkpoint_ownership_lost"),
        directory.ImportCancelledError("canceled"),
        asyncio.CancelledError(),
    ],
)
async def test_retry_refuses_lost_owner_or_cancellation_before_another_request(files, monkeypatch, failure):
    stream = AsyncMock(return_value="bulk_export_transport_clientpayloaderror")
    monkeypatch.setattr(directory, "_stream_one_checkpointed_bulk_output", stream)
    options = dataclasses.replace(files.options, ownership_probe=AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)):
        await parallel.stream_output(
            directory,
            object(),
            _source(),
            files.identity,
            files.manifest,
            (files.manifest.outputs[0], files.checkpoint_by_index[0]),
            options,
            object(),
        )
    stream.assert_awaited_once()
    directory._load_bulk_output_checkpoints.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "errors,expected",
    [
        ((None, ""), None),
        (("bulk_export_output_url_expired", None), "bulk_export_output_url_expired"),
        (("bulk_export_transport_clientpayloaderror", "invalid_ndjson"), "invalid_ndjson"),
        (("bulk_export_output_http_503", "bulk_export_output_url_expired"), "bulk_export_output_url_expired"),
        (("bulk_export_output_url_expired", "bulk_export_output_http_401"), "bulk_export_output_http_401"),
        (("bulk_export_output_http_404", "invalid_ndjson"), "invalid_ndjson"),
        (("bulk_export_output_http_410", "invalid_ndjson"), "invalid_ndjson"),
        (("bulk_export_output_http_404", "bulk_export_output_http_401"), "bulk_export_output_http_401"),
        (("bulk_export_output_http_410", "bulk_export_output_http_401"), "bulk_export_output_http_401"),
        (("bulk_export_output_http_403", "invalid_ndjson"), "invalid_ndjson"),
        (("bulk_export_output_http_403", "bulk_export_output_http_401"), "bulk_export_output_http_401"),
        (
            ("bulk_export_output_http_403", "bulk_export_output_validator_mismatch"),
            "bulk_export_output_validator_mismatch",
        ),
        (
            ("bulk_export_output_url_expired", "bulk_export_output_validator_mismatch"),
            "bulk_export_output_validator_mismatch",
        ),
    ],
)
@pytest.mark.parametrize("reverse", [False, True])
async def test_simultaneous_output_errors_preserve_recovery_priority(errors, expected, reverse):
    if reverse:
        errors = tuple(reversed(errors))
    tasks = [asyncio.create_task(asyncio.sleep(0, result=error)) for error in errors]
    await asyncio.gather(*tasks)
    active = dict.fromkeys(tasks)
    assert directory._completed_bulk_output_error(tasks, active) == expected
    assert not active


@pytest.mark.asyncio
@pytest.mark.parametrize("range_resume,validator", [(False, True), (True, False)])
async def test_retry_requires_enabled_range_and_stored_strong_validator(files, monkeypatch, range_resume, validator):
    stream = AsyncMock(return_value="bulk_export_transport_clientpayloaderror")
    monkeypatch.setattr(directory, "_stream_one_checkpointed_bulk_output", stream)
    checkpoint_by_field = dict(files.checkpoint_by_index[0])
    if not validator:
        for name in ("content_length_bytes", "etag_ciphertext", "etag_hash", "validator_checked_at"):
            checkpoint_by_field[name] = None
    error = await parallel.stream_output(
        directory,
        object(),
        files.source_record,
        files.identity,
        files.manifest,
        (files.manifest.outputs[0], checkpoint_by_field),
        dataclasses.replace(files.options, range_resume_enabled=range_resume),
        object(),
    )
    assert error == "bulk_export_transport_clientpayloaderror"
    stream.assert_awaited_once()
    directory._load_bulk_output_checkpoints.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage,expected",
    [
        ("etag", "bulk_export_output_validator_mismatch"),
        ("offset", "bulk_export_output_validator_checkpoint_corrupt"),
        ("manifest", "bulk_export_manifest_output_checkpoint_mismatch"),
    ],
)
async def test_retry_revalidates_the_durable_file_identity_and_offset(files, monkeypatch, damage, expected):
    stream = AsyncMock(return_value="bulk_export_transport_clientpayloaderror")
    monkeypatch.setattr(directory, "_stream_one_checkpointed_bulk_output", stream)
    original_checkpoint_by_field = dict(files.checkpoint_by_index[0])
    if damage == "etag":
        etag = '"changed"'
        files.checkpoint_by_index[0].update(
            etag_ciphertext=directory._encrypt_bulk_capability(etag),
            etag_hash=hashlib.sha256(etag.encode()).hexdigest(),
        )
    elif damage == "offset":
        files.checkpoint_by_index[0]["committed_bytes"] = 0
    else:
        files.checkpoint_by_index[0]["output_url_hash"] = "f" * 64
    error = await parallel.stream_output(
        directory,
        object(),
        files.source_record,
        files.identity,
        files.manifest,
        (files.manifest.outputs[0], original_checkpoint_by_field),
        files.options,
        object(),
    )
    assert error == expected
    stream.assert_awaited_once()
