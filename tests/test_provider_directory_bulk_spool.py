# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opted-in bulk outputs are saved compressed first and parsed from the copy, independent of their links."""

import asyncio
import datetime
import importlib
import json
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from process import provider_directory_bulk_parallel as parallel
from process import provider_directory_bulk_spool as spool
from tests.test_provider_directory_bulk_parallel import _source

directory = importlib.import_module("process.provider_directory_fhir")
REAL_IS_COMPLETE = directory._is_complete_bulk_output_proof
REAL_STORED_VALIDATOR = directory._stored_bulk_output_validator
CHECKPOINT_ID = "c" * 64
IDENTITY = SimpleNamespace(checkpoint_id=CHECKPOINT_ID)
ETAG = '"output-etag"'
BODY = b"".join(
    json.dumps({"resourceType": "PractitionerRole", "id": f"r{index}", "active": True}).encode() + b"\n"
    for index in range(30000)
)


def _validator(content_length=len(BODY)):
    return SimpleNamespace(etag=ETAG, etag_hash="e" * 64, content_length_bytes=content_length)


@pytest.fixture
def spooled_source(monkeypatch, tmp_path):
    monkeypatch.setenv(spool.SPOOL_ROOT_ENV, str(tmp_path))
    monkeypatch.setenv(spool.SPOOL_SOURCES_ENV, "other, pd-source")
    monkeypatch.setattr(spool, "FRAME_BYTES", 64 * 1024)
    monkeypatch.setattr(spool, "READ_BYTES", 16 * 1024)
    monkeypatch.setattr(directory, "_source_fetch_retry_delay_seconds", lambda attempt: 0)
    monkeypatch.setattr(directory, "_refresh_bulk_export_rows_written", AsyncMock())
    monkeypatch.setattr(
        directory,
        "_bulk_output_stream_request",
        lambda source, url, *, requires_access_token, resume_options: directory.BulkOutputRequest(
            url,
            {"If-Match": resume_options.expected_etag}
            | ({"Range": f"bytes={resume_options.resume_offset}-"} if resume_options.resume_offset else {}),
        ),
    )
    return {**_source(), "source_id": "pd-source"}


def _output_server(requests, break_first_after=None, trickle=None):
    async def output(request):
        requests.append(dict(request.headers))
        start = int(request.headers.get("Range", "bytes=0-")[6:].split("-")[0])
        length = trickle if trickle is not None else len(BODY) - start
        headers_by_name = {"ETag": ETAG, "Accept-Ranges": "bytes", "Content-Length": str(length)}
        if start:
            headers_by_name["Content-Range"] = f"bytes {start}-{len(BODY) - 1}/{len(BODY)}"
        response = web.StreamResponse(status=206 if start else 200, headers=headers_by_name)
        await response.prepare(request)
        if break_first_after is not None and len(requests) == 1:
            await response.write(BODY[:break_first_after])
            await asyncio.sleep(0.5)  # let the client consume what arrived
            request.transport.close()
            return response
        if trickle is not None:
            for offset in range(0, trickle, trickle // 5):
                await response.write(BODY[offset : offset + trickle // 5])
                await asyncio.sleep(0.1)
            return response
        await response.write(BODY[start:])
        return response

    application = web.Application()
    application.router.add_get("/output", output)
    return TestServer(application)


def _options():
    return SimpleNamespace(ownership_probe=AsyncMock(), timeout=30, range_resume_enabled=True)


def _context(session, source_record, manifest):
    return parallel._CopyContext(directory, session, source_record, IDENTITY, manifest, _options())


def _two_output_export(monkeypatch, url, states=("complete", "streaming")):
    manifest = SimpleNamespace(
        requires_access_token=False,
        outputs=[SimpleNamespace(url=f"{url}?part={index}", output_index=index) for index in range(len(states))],
    )
    checkpoints = [
        {"output_id": f"o{index}", "output_index": index, "state": state} for index, state in enumerate(states)
    ]
    monkeypatch.setattr(directory, "_load_bulk_output_checkpoints", AsyncMock(return_value=checkpoints))
    monkeypatch.setattr(
        directory, "_bulk_output_checkpoint_for_manifest_output", lambda _, output, rows: rows[output.output_index]
    )
    monkeypatch.setattr(
        directory, "_is_complete_bulk_output_proof", lambda checkpoint: checkpoint["state"] == "complete"
    )
    monkeypatch.setattr(directory, "_stored_bulk_output_validator", lambda checkpoint: _validator())
    return manifest, checkpoints


@pytest.mark.asyncio
async def test_broken_download_resumes_from_last_frame_and_serves_exact_bytes(spooled_source):
    requests = []
    async with _output_server(requests, break_first_after=300 * 1024) as server, aiohttp.ClientSession() as session:
        url = str(server.make_url("/output"))
        output = SimpleNamespace(url=url, output_index=3)
        spooled = spool.spooled_output(CHECKPOINT_ID, 3, _validator())
        context = _context(session, spooled_source, SimpleNamespace(requires_access_token=False))

        assert await parallel._spool_output(context, (output, spooled)) is None

        assert spooled.path.exists()
        assert [request.get("Range") for request in requests] == [None, f"bytes={4 * 64 * 1024}-"]
        assert directory._refresh_bulk_export_rows_written.await_count == len(BODY) // (64 * 1024)
        async with parallel.request(directory, spooled_source, session, url, headers={"If-Match": ETAG}) as response:
            assert isinstance(response, spool.SpooledResponse)
            assert b"".join([chunk async for chunk in response.content.iter_chunked(7000)]) == BODY
        resume_by_name = {"If-Match": ETAG, "Range": "bytes=123457-"}
        async with parallel.request(directory, spooled_source, session, url, headers=resume_by_name) as response:
            headers_by_name = {name.lower(): header_value for name, header_value in response.headers.items()}
            assert (
                directory._bulk_output_response_error(
                    response.status,
                    headers_by_name,
                    resume_offset=123457,
                    expected_etag=ETAG,
                    expected_content_length=len(BODY),
                )
                is None
            )
            assert b"".join([chunk async for chunk in response.content.iter_chunked(65536)]) == BODY[123457:]
        # A validator probe or a different validator still goes to the source.
        for headers in ({"If-Match": ETAG, "Range": "bytes=0-0"}, {"If-Match": '"other"'}):
            async with parallel.request(directory, spooled_source, session, url, headers=headers) as response:
                assert response.status in {200, 206} and not isinstance(response, spool.SpooledResponse)
        assert len(requests) == 4


@pytest.mark.asyncio
async def test_every_output_downloads_before_any_parse_and_copies_go_after(spooled_source, monkeypatch):
    requests, parsed_bodies = [], []
    async with _output_server(requests) as server, aiohttp.ClientSession() as session:
        manifest, checkpoints = _two_output_export(monkeypatch, str(server.make_url("/output")), ("streaming",) * 2)

        async def parse(session, source_record, identity, manifest, output, checkpoint, options, stream_state):
            assert len(requests) == 2  # both outputs were saved before this parse began
            async with parallel.request(directory, source_record, session, output.url, headers={"If-Match": ETAG}) as (
                response
            ):
                parsed_bodies.append(b"".join([chunk async for chunk in response.content.iter_chunked(1 << 16)]))
            return None

        monkeypatch.setattr(directory, "_stream_one_checkpointed_bulk_output", parse)
        streams = [
            parallel.stream_output(directory, session, spooled_source, IDENTITY, manifest, pair, _options(), None)
            for pair in zip(manifest.outputs, checkpoints)
        ]

        assert await asyncio.gather(*streams) == [None, None]

        assert parsed_bodies == [BODY, BODY] and len(requests) == 2
        assert not any(spool.spooled_output(CHECKPOINT_ID, index, _validator()).path.exists() for index in (0, 1))


@pytest.mark.asyncio
async def test_export_saves_only_incomplete_outputs_and_reports_a_missing_validator(spooled_source, monkeypatch):
    requests = []
    async with _output_server(requests) as server, aiohttp.ClientSession() as session:
        manifest, _ = _two_output_export(monkeypatch, str(server.make_url("/output")))

        assert await parallel._spool_export(_context(session, spooled_source, manifest)) is None
        assert len(requests) == 1
        assert spool.spooled_output(CHECKPOINT_ID, 1, _validator()).path.exists()
        assert not spool.spooled_output(CHECKPOINT_ID, 0, _validator()).path.exists()
        # A copy that is already whole is used as it is.
        assert await parallel._spool_export(_context(session, spooled_source, manifest)) is None
        assert len(requests) == 1

        monkeypatch.setattr(directory, "_stored_bulk_output_validator", lambda checkpoint: None)
        assert await parallel._spool_export(_context(session, spooled_source, manifest)) == (
            "bulk_export_spool_validator_missing"
        )


@pytest.mark.asyncio
async def test_completed_outputs_keep_only_their_proof_and_are_skipped(spooled_source, monkeypatch):
    requests = []
    async with _output_server(requests) as server, aiohttp.ClientSession() as session:
        manifest, checkpoints = _two_output_export(monkeypatch, str(server.make_url("/output")))
        # The shape completion leaves: length, ETag hash and check time, but no ETag capability.
        checkpoints[0] |= {
            "content_length_bytes": len(BODY),
            "committed_bytes": len(BODY),
            "etag_hash": "e" * 64,
            "etag_ciphertext": None,
            "validator_checked_at": datetime.datetime(2026, 11, 14),
        }
        monkeypatch.setattr(directory, "_is_complete_bulk_output_proof", REAL_IS_COMPLETE)
        with pytest.raises(ValueError, match="validator_checkpoint_corrupt"):
            REAL_STORED_VALIDATOR(checkpoints[0])
        monkeypatch.setattr(
            directory,
            "_stored_bulk_output_validator",
            lambda checkpoint: REAL_STORED_VALIDATOR(checkpoint) if checkpoint["state"] == "complete" else _validator(),
        )

        assert await parallel._spool_export(_context(session, spooled_source, manifest)) is None
        assert len(requests) == 1 and spool.spooled_output(CHECKPOINT_ID, 1, _validator()).path.exists()


@pytest.mark.asyncio
async def test_retry_after_link_expiry_uses_copies_instead_of_probing(spooled_source, monkeypatch):
    expired = SimpleNamespace(
        **vars(_validator()), output_expires_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    )
    manifest, checkpoints = _two_output_export(monkeypatch, "https://files.example/output", ("streaming",) * 2)
    monkeypatch.setattr(directory, "_stored_bulk_output_validator", lambda checkpoint: expired)
    probe = AsyncMock()
    monkeypatch.setattr(directory, "_bulk_http_probe_output", probe)
    monkeypatch.setattr(directory, "_record_bulk_export_output_error", AsyncMock())
    arguments = (None, spooled_source, IDENTITY, manifest, checkpoints)

    _, error = await directory._prepare_bulk_output_validators(*arguments, timeout=5, ownership_probe=AsyncMock())
    assert error == "bulk_export_output_url_expired"
    for index in (0, 1):
        path = spool.spooled_output(CHECKPOINT_ID, index, expired).path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")

    _, error = await directory._prepare_bulk_output_validators(*arguments, timeout=5, ownership_probe=AsyncMock())
    assert error is None
    probe.assert_not_awaited()


def test_copies_need_both_the_source_and_copy_root(monkeypatch, tmp_path):
    monkeypatch.setenv(spool.SPOOL_SOURCES_ENV, "pd-source")
    monkeypatch.delenv(spool.SPOOL_ROOT_ENV, raising=False)
    assert not spool.is_enabled({"source_id": "pd-source"})
    monkeypatch.setenv(spool.SPOOL_ROOT_ENV, str(tmp_path))
    assert spool.is_enabled({"source_id": "pd-source"}) and not spool.is_enabled({"source_id": "other"})
    with pytest.raises(ValueError, match="checkpoint_invalid"):
        spool.spooled_output("../escape", 0, _validator())


def test_a_copy_of_the_wrong_length_is_discarded(spooled_source):
    spooled = spool.spooled_output(CHECKPOINT_ID, 5, _validator(content_length=10))
    writer = spool.SpoolWriter(spooled)
    writer.write(b"x" * 11)
    with pytest.raises(spool.SpoolLengthError):
        writer.finish()
    writer.discard()
    assert not any(spooled.path.parent.iterdir())


@pytest.mark.asyncio
async def test_slow_transfer_below_one_frame_is_still_probed(spooled_source, monkeypatch):
    monkeypatch.setattr(parallel, "_COPY_PROBE_SECONDS", 0.05)
    size = 40 * 1024  # below one 64 KiB test frame
    async with _output_server([], trickle=size) as server, aiohttp.ClientSession() as session:
        output = SimpleNamespace(url=str(server.make_url("/output")), output_index=7)
        spooled = spool.spooled_output(CHECKPOINT_ID, 7, _validator(content_length=size))
        context = _context(session, spooled_source, SimpleNamespace(requires_access_token=False))

        assert await parallel._download_copy(context, (output, spooled)) is None

        assert context.options.ownership_probe.await_count >= 3
        directory._refresh_bulk_export_rows_written.assert_not_awaited()
        assert spool.zstd.decompress(spooled.path.read_bytes()) == BODY[:size]


@pytest.mark.asyncio
async def test_cancelling_during_frame_publication_keeps_a_resumable_copy(spooled_source, monkeypatch):
    publishing, real_fsync = threading.Event(), spool.os.fsync

    def slow_fsync(descriptor):
        if not publishing.is_set():
            publishing.set()
            time.sleep(0.3)  # the frame is being made durable when the caller is cancelled
        real_fsync(descriptor)

    monkeypatch.setattr(spool.os, "fsync", slow_fsync)
    requests = []
    async with _output_server(requests) as server, aiohttp.ClientSession() as session:
        output = SimpleNamespace(url=str(server.make_url("/output")), output_index=8)
        spooled = spool.spooled_output(CHECKPOINT_ID, 8, _validator())
        context = _context(session, spooled_source, SimpleNamespace(requires_access_token=False))
        task = asyncio.create_task(parallel._download_copy(context, (output, spooled)))
        await asyncio.to_thread(publishing.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        resumed = spool.SpoolWriter(spooled)
        stored = resumed.part_path.read_bytes()
        resumed.close()
        assert resumed.durable_bytes > 0 and len(spool.zstd.decompress(stored)) == resumed.durable_bytes
        assert await parallel._spool_output(context, (output, spooled)) is None
        assert spool.zstd.decompress(spooled.path.read_bytes()) == BODY
        assert requests[-1].get("Range") == f"bytes={resumed.durable_bytes}-"


@pytest.mark.asyncio
async def test_cancelled_streams_wait_for_every_download_to_close_its_copy(spooled_source, monkeypatch):
    publishing, published, real_fsync = threading.Event(), threading.Event(), spool.os.fsync

    def slow_fsync(descriptor):
        if not publishing.is_set():
            publishing.set()
            time.sleep(0.5)  # one download is making a frame durable while the other is still reading
            published.set()
        real_fsync(descriptor)

    monkeypatch.setattr(spool.os, "fsync", slow_fsync)
    async with _output_server([], trickle=None) as fast, _output_server([], trickle=40 * 1024) as slow:
        async with aiohttp.ClientSession() as session:
            manifest, checkpoints = _two_output_export(monkeypatch, str(fast.make_url("/output")), ("streaming",) * 2)
            manifest.outputs[1].url = str(slow.make_url("/output"))
            streams = [
                asyncio.create_task(
                    parallel.stream_output(
                        directory, session, spooled_source, IDENTITY, manifest, pair, _options(), None
                    )
                )
                for pair in zip(manifest.outputs, checkpoints)
            ]
            await asyncio.to_thread(publishing.wait, 5)
            for stream in streams:  # the second cancel arrives while a download is already closing its copy
                stream.cancel()
                await asyncio.sleep(0.1)
            for stream in streams:
                with pytest.raises(asyncio.CancelledError):
                    await stream

            assert published.is_set()  # the frame in flight finished before cancellation was released
            for index in (0, 1):
                writer = spool.SpoolWriter(spool.spooled_output(CHECKPOINT_ID, index, _validator()))
                stored = writer.part_path.read_bytes() if writer.part_path.exists() else b""
                writer.close()
                assert (len(spool.zstd.decompress(stored)) if stored else 0) == writer.durable_bytes
