# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opted-in bulk outputs are saved compressed first and parsed from the copy, independent of their links."""

import asyncio
import datetime
import importlib
import json
import threading
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from process import provider_directory_bulk_grouped_load as grouped_load
from process import provider_directory_bulk_grouping as grouping
from process import provider_directory_bulk_parallel as parallel
from process import provider_directory_bulk_spool as spool
from process import provider_directory_fhir_response as fhir_response
from process import provider_directory_generic_source_adapters as adapters
from process import provider_directory_role_merge as role_merge
from process import provider_directory_rows_native as rows_native
from process.provider_directory_resource_hash import SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
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
        # "/other" serves the same roles under three other networks, at the same length.
        body = BODY if request.path == "/output" else BODY.replace(b"Organization/n", b"Organization/m")
        start = int(request.headers.get("Range", "bytes=0-")[6:].split("-")[0])
        length = trickle if trickle is not None else len(body) - start
        headers_by_name = {"ETag": ETAG, "Accept-Ranges": "bytes", "Content-Length": str(length)}
        if start:
            headers_by_name["Content-Range"] = f"bytes {start}-{len(body) - 1}/{len(body)}"
        response = web.StreamResponse(status=206 if start else 200, headers=headers_by_name)
        await response.prepare(request)
        if break_first_after is not None and len(requests) == 1:
            await response.write(body[:break_first_after])
            await asyncio.sleep(0.5)  # let the client consume what arrived
            request.transport.close()
            return response
        if trickle is not None:
            for offset in range(0, trickle, trickle // 5):
                await response.write(body[offset : offset + trickle // 5])
                await asyncio.sleep(0.1)
            return response
        await response.write(body[start:])
        return response

    application = web.Application()
    application.router.add_get("/output", output)
    application.router.add_get("/other", output)
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
async def test_all_outputs_download_at_once_beyond_the_output_concurrency(spooled_source, monkeypatch):
    count_by_kind = {"active": 0, "peak": 0}

    async def output(request):
        count_by_kind["active"] += 1
        count_by_kind["peak"] = max(count_by_kind["peak"], count_by_kind["active"])
        await asyncio.sleep(0.3)  # hold the response so concurrent downloads overlap
        count_by_kind["active"] -= 1
        return web.Response(body=BODY, headers={"ETag": ETAG, "Accept-Ranges": "bytes"})

    application = web.Application()
    application.router.add_get("/output", output)
    async with TestServer(application) as server, aiohttp.ClientSession() as session:
        manifest, _ = _two_output_export(monkeypatch, str(server.make_url("/output")), ("streaming",) * 12)
        assert parallel.output_concurrency(directory, spooled_source) == 8

        assert await parallel._spool_export(_context(session, spooled_source, manifest)) is None

        assert count_by_kind["peak"] == 12
        assert all(spool.spooled_output(CHECKPOINT_ID, index, _validator()).path.exists() for index in range(12))


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
    path = spool.spooled_output(CHECKPOINT_ID, 0, expired).path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    # A grouped output's copy is gone once its group files are durable; its marker holds the bytes instead.
    group_writer = grouping.GroupWriter(spool.group_directory(CHECKPOINT_ID), 1, partitions=4)
    group_writer.finish(0)

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


# 120 source roles of 7 practitioners, each seen in 3 networks: 7 merged roles per output.
ROLE_BODY = b"".join(
    json.dumps(
        {
            "resourceType": "PractitionerRole",
            "id": f"r{index}",
            "active": True,
            "practitioner": {"reference": f"Practitioner/p{index % 7}"},
            "network": [{"reference": f"Organization/n{index % 3}"}],
        }
    ).encode()
    + b"\n"
    for index in range(120)
)
CRASH_STAGES = ("mid_group", "before_load", "mid_load", "before_finish", "before_cleanup")


class _Ledger:
    """What a fake database committed: staged rows and output completions; a failed transaction keeps nothing."""

    def __init__(self):
        self.staged_rows, self.completions, self.pending = [], {}, None
        self.complete_calls = 0

    @asynccontextmanager
    async def transaction(self, database, **coordinates):
        if self.pending is not None:
            yield
            return
        self.pending = ([], {})
        try:
            yield
            self.staged_rows += self.pending[0]
            self.completions |= self.pending[1]
        finally:
            self.pending = None

    async def persist(self, directory, prepared_rows, **scope):
        self.pending[0].extend(prepared_rows)
        return prepared_rows

    async def count(self, directory, context, resource_type, *, distinct=True):
        assert distinct is False and resource_type == "PractitionerRole"
        return len(self.staged_rows)

    async def complete(self, identity, output_id, rows_written, committed_bytes, *, require_validator):
        assert require_validator and committed_bytes == len(ROLE_BODY)
        self.complete_calls += 1
        self.pending[1][output_id] = rows_written


@pytest.fixture
def grouped_source(spooled_source, monkeypatch):
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, "pd-source")
    monkeypatch.setenv(grouped_load.WORKERS_ENV, "0")
    monkeypatch.setattr(grouping, "PARTITIONS", 8)
    monkeypatch.setitem(globals(), "BODY", ROLE_BODY)  # what the fake output server serves
    return spooled_source | {
        "_endpoint_dataset_id": "dataset-grouped",
        "_resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "_semantic_projection_as_of": "2026-08-09",
    }


def _grouped_fakes(monkeypatch, ledger):
    monkeypatch.setattr(directory, "source_checkpoint_transaction", ledger.transaction)
    monkeypatch.setattr(grouped_load, "persist_prepared_rows", ledger.persist)
    monkeypatch.setattr(adapters, "candidate_resource_count", ledger.count)
    monkeypatch.setattr(directory, "_complete_bulk_export_output", ledger.complete)
    monkeypatch.setattr(directory, "_begin_bulk_export_output", AsyncMock())
    monkeypatch.setattr(directory, "_stored_bulk_output_validator", lambda checkpoint: _validator(len(ROLE_BODY)))
    monkeypatch.setattr(directory, "_record_bulk_export_output_progress", AsyncMock())
    monkeypatch.setattr(
        directory, "_is_complete_bulk_output_proof", lambda checkpoint: checkpoint["output_id"] in ledger.completions
    )


def _grouped_identity(checkpoint_id=CHECKPOINT_ID):
    return SimpleNamespace(
        checkpoint_id=checkpoint_id,
        resource_type="PractitionerRole",
        dataset_id="dataset-grouped",
        endpoint_id="endpoint-grouped",
        acquisition_root_run_id="root-grouped",
    )


def _grouped_options():
    return SimpleNamespace(
        ownership_probe=AsyncMock(),
        timeout=30,
        range_resume_enabled=True,
        retain_rows=False,
        row_batch_size=50,
        run_id="run-grouped",
    )


async def _run_grouped(session, source_record, identity, manifest, checkpoints, ledger):
    """One worker run: the scheduler streams only the outputs the ledger has not completed."""
    source_record.pop("_provider_directory_bulk_parallel", None)
    options, stream_state = _grouped_options(), SimpleNamespace(rows_written_this_run=0, outputs_processed=0)
    pairs = [pair for pair in zip(manifest.outputs, checkpoints) if pair[1]["output_id"] not in ledger.completions]
    streams = [
        parallel.stream_output(directory, session, source_record, identity, manifest, pair, options, stream_state)
        for pair in pairs
    ]
    return await asyncio.gather(*streams, return_exceptions=True)


def _crash_once(monkeypatch, owner, name, call_number, should_crash=lambda *arguments: True):
    original, calls = getattr(owner, name), []

    def crashing(*arguments, **keywords):
        if should_crash(*arguments):
            calls.append(arguments)
            if len(calls) == call_number:
                raise RuntimeError("injected crash")
        return original(*arguments, **keywords)

    monkeypatch.setattr(owner, name, crashing)


def _inject_crash(monkeypatch, stage):
    if stage == "mid_group":  # the second output's first batch: the first output is grouped, the second is not
        _crash_once(monkeypatch, grouped_load, "_prepare_batch", 4)
    elif stage == "before_load":
        _crash_once(monkeypatch, grouping, "reduce_partition", 1)
    elif stage == "mid_load":  # the second partition's rows are committed, its marker is not: it is replayed
        _crash_once(monkeypatch, grouping, "mark_loaded", 2)
    elif stage == "before_finish":
        monkeypatch.setattr(adapters, "candidate_resource_count", AsyncMock(side_effect=RuntimeError("injected crash")))
    else:  # the outputs are complete; only the export's local directory is still there
        _crash_once(monkeypatch, grouping, "discard", 1, lambda path: path.name == CHECKPOINT_ID)


def _merged_rows(ledger):
    return {(staged["resource_id"], staged["payload_hash"]) for staged in ledger.staged_rows}


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", CRASH_STAGES)
async def test_grouped_load_resumes_after_a_crash_at_each_stage(grouped_source, monkeypatch, stage):
    requests = []
    async with _output_server(requests) as server, aiohttp.ClientSession() as session:
        manifest, checkpoints = _two_output_export(monkeypatch, str(server.make_url("/output")), ("streaming",) * 2)
        manifest.outputs[1].url = str(server.make_url("/other"))  # neither output alone holds a role's networks
        clean = _Ledger()
        _grouped_fakes(monkeypatch, clean)
        clean_identity = _grouped_identity("d" * 64)
        assert await _run_grouped(session, dict(grouped_source), clean_identity, manifest, checkpoints, clean) == [
            None,
            None,
        ]
        requests.clear()

        ledger = _Ledger()
        _grouped_fakes(monkeypatch, ledger)
        with monkeypatch.context() as crash:
            _inject_crash(crash, stage)
            crashed_streams = await _run_grouped(
                session, grouped_source, _grouped_identity(), manifest, checkpoints, ledger
            )
        assert crashed_streams and all(isinstance(error, RuntimeError) for error in crashed_streams)
        assert len(requests) == 2  # every copy was saved before grouping began
        assert await _run_grouped(session, grouped_source, _grouped_identity(), manifest, checkpoints, ledger) in (
            [None, None],
            [],
        )

    assert len(requests) == 2  # the restart used the copies and markers, never the links
    assert ledger.complete_calls == 2 and set(ledger.completions) == {"o0", "o1"}
    assert sum(ledger.completions.values()) == len(ledger.staged_rows)
    assert _merged_rows(ledger) == _merged_rows(clean) and len(_merged_rows(clean)) == 7
    assert all(len(staged["payload_json"]["network_refs"]) == 6 for staged in clean.staged_rows)
    # committed_bytes moves only at finish: no output progress, and outputs begin only to be completed.
    directory._record_bulk_export_output_progress.assert_not_awaited()
    assert all(
        call.kwargs == {"preserve_progress": True} for call in directory._begin_bulk_export_output.await_args_list
    )
    assert not list(spool.group_directory(CHECKPOINT_ID).parent.rglob("*.zst"))  # copies and group files are gone


@pytest.mark.asyncio
async def test_grouping_renews_the_lease_while_workers_run(grouped_source, monkeypatch):
    clock = iter(range(0, 10**6, 61))  # every reading is a minute after the last
    monkeypatch.setattr(directory, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    monkeypatch.setattr(grouped_load, "_PROBE_SECONDS", 0.01)
    renewed, real_group_output = threading.Event(), grouped_load.group_output
    renewals_by_stage = {}

    async def renew(identity):
        if "download" in renewals_by_stage and directory._refresh_bulk_export_rows_written.await_count >= (
            renewals_by_stage["download"] + 2
        ):
            renewed.set()

    def slow_group_output(*arguments):
        assert renewed.wait(30)  # a long group step: it ends only after the lease was renewed twice
        return real_group_output(*arguments)

    directory._refresh_bulk_export_rows_written.side_effect = renew
    monkeypatch.setattr(grouped_load, "group_output", slow_group_output)
    requests = []
    async with _output_server(requests) as server, aiohttp.ClientSession() as session:
        manifest, checkpoints = _two_output_export(monkeypatch, str(server.make_url("/output")), ("streaming",) * 2)
        ledger = _Ledger()
        _grouped_fakes(monkeypatch, ledger)
        original_group = grouped_load._group

        async def observed_group(*arguments):
            renewals_by_stage["download"] = directory._refresh_bulk_export_rows_written.await_count
            return await original_group(*arguments)

        monkeypatch.setattr(grouped_load, "_group", observed_group)
        observations = []
        token = fhir_response.BULK_EXPORT_WAIT_CALLBACK.set(AsyncMock(side_effect=observations.append))
        try:
            assert await _run_grouped(session, grouped_source, _grouped_identity(), manifest, checkpoints, ledger) == [
                None,
                None,
            ]
        finally:
            fhir_response.BULK_EXPORT_WAIT_CALLBACK.reset(token)

    assert renewed.is_set()
    assert {"group", "load"} <= {observation["stage"] for observation in observations}  # progress stays live
    assert set(ledger.completions) == {"o0", "o1"}


async def _route(monkeypatch, source_record):
    """Which way stream_output sends one role output, with the download and both paths faked."""
    stream, grouped = AsyncMock(return_value=None), AsyncMock(return_value=None)
    monkeypatch.setattr(parallel, "_spooled_export", AsyncMock(return_value=None))
    monkeypatch.setattr(directory, "_stream_one_checkpointed_bulk_output", stream)
    monkeypatch.setattr(grouped_load, "grouped_export", grouped)
    monkeypatch.setattr(directory, "_stored_bulk_output_validator", lambda checkpoint: _validator())
    checkpoint_by_field = {"output_id": "o0", "output_index": 0, "state": "streaming"}
    output = SimpleNamespace(url="https://files.example/output", output_index=0)
    error = await parallel.stream_output(
        directory,
        None,
        source_record,
        _grouped_identity(),
        None,
        (output, checkpoint_by_field),
        _grouped_options(),
        None,
    )
    return error, stream.await_count, grouped.await_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {},
        {"_resource_hash_contract": "transport_neutral_v2"},
        {"canonical_api_base": directory.SCAN_PROVIDER_DIRECTORY_BASE},
    ],
    ids=["merged", "unmerged contract", "reverse lookup"],
)
async def test_only_semantic_sources_without_reverse_lookup_take_grouping(grouped_source, monkeypatch, changes):
    error, streams, groups = await _route(monkeypatch, grouped_source | changes)
    assert error is None and (streams, groups) == ((0, 1) if not changes else (1, 0))


@pytest.mark.asyncio
@pytest.mark.parametrize("setting", [role_merge.MERGED_ROLE_SOURCES_ENV, spool.SPOOL_SOURCES_ENV])
async def test_turning_grouping_off_mid_acquisition_ends_it(grouped_source, monkeypatch, setting):
    mode_path = spool.group_directory(CHECKPOINT_ID) / "mode"
    mode_path.parent.mkdir(parents=True)
    mode_path.write_bytes(b'{"partitions":8,"version":1}')
    monkeypatch.setenv(setting, "")  # merged roles off, or saved outputs off for this source

    assert await _route(monkeypatch, grouped_source) == ("bulk_export_group_mode_changed", 0, 0)
    assert directory._is_bulk_export_error_terminal("bulk_export_group_mode_changed")


@pytest.mark.asyncio
async def test_shared_task_is_redone_after_it_raises_or_is_cancelled(spooled_source):
    starts = []

    async def start():
        starts.append(None)
        if len(starts) == 1:
            raise RuntimeError("first attempt")
        if len(starts) == 2:
            await asyncio.sleep(3600)

    def run():
        return parallel.shared_task(directory, spooled_source, "kind", CHECKPOINT_ID, start)

    with pytest.raises(RuntimeError):
        await run()
    waiter = asyncio.create_task(run())
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    assert await run() is None
    assert await run() is None
    assert len(starts) == 3  # only the clean return stays cached


@pytest.mark.asyncio
async def test_spawned_worker_groups_loads_and_finishes(grouped_source, monkeypatch):
    monkeypatch.setenv(grouped_load.WORKERS_ENV, "1")  # a real spawn pool: tasks, results and errors are pickled
    requests = []
    async with _output_server(requests) as server, aiohttp.ClientSession() as session:
        manifest, checkpoints = _two_output_export(monkeypatch, str(server.make_url("/output")), ("streaming",) * 2)
        ledger = _Ledger()
        _grouped_fakes(monkeypatch, ledger)
        assert await _run_grouped(session, grouped_source, _grouped_identity(), manifest, checkpoints, ledger) == [
            None,
            None,
        ]

    assert set(ledger.completions) == {"o0", "o1"} and len(requests) == 2
    assert len(_merged_rows(ledger)) == 7 and sum(ledger.completions.values()) == len(ledger.staged_rows)


async def _grouped_run_with_markers(session, url, grouped_source, monkeypatch, identity):
    """One complete grouped run; returns the staged rows and each output's .done marker, read before cleanup."""
    markers = []
    real_fallback_counts = grouping.fallback_counts

    def recording_fallback_counts(group_root, output_indexes):
        markers.extend(json.loads((group_root / f"{index}.done").read_bytes()) for index in output_indexes)
        return real_fallback_counts(group_root, output_indexes)

    manifest, checkpoints = _two_output_export(monkeypatch, url, ("streaming",) * 2)
    ledger = _Ledger()
    _grouped_fakes(monkeypatch, ledger)
    with monkeypatch.context() as recording:
        recording.setattr(grouping, "fallback_counts", recording_fallback_counts)
        assert await _run_grouped(session, dict(grouped_source), identity, manifest, checkpoints, ledger) == [
            None,
            None,
        ]
    return sorted(ledger.staged_rows, key=lambda staged: staged["resource_id"]), markers


@pytest.mark.asyncio
async def test_spawned_native_worker_stages_what_python_stages(grouped_source, monkeypatch):
    if not rows_native.has_native_row_lines("PractitionerRole"):
        pytest.skip("compiled ptg2_address_canon with prepare_provider_directory_row_lines_v1 is not importable")
    async with _output_server([]) as server, aiohttp.ClientSession() as session:
        url = str(server.make_url("/output"))
        with monkeypatch.context() as python_only:  # in process (WORKERS_ENV is 0), so the patch reaches the worker
            python_only.setattr(rows_native, "_native_export", lambda name: None)
            expected_rows, python_markers = await _grouped_run_with_markers(
                session, url, grouped_source, monkeypatch, _grouped_identity("d" * 64)
            )
        monkeypatch.setenv(grouped_load.WORKERS_ENV, "1")  # a spawned worker imports the compiled module itself
        staged_rows, native_markers = await _grouped_run_with_markers(
            session, url, grouped_source, monkeypatch, _grouped_identity()
        )

    assert staged_rows == expected_rows and len(staged_rows) == 7
    # Either way one group line per source role: no rule depends on which lines share a batch.
    assert [marker["rows"] for marker in native_markers] == [120, 120]
    assert [marker["rows"] for marker in python_markers] == [120, 120]
    assert all(marker["fallbacks"] == {} for marker in native_markers)
    assert [marker["fallbacks"] for marker in python_markers] == [{"native_unavailable": 120}] * 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        ("first_batch", "invalid_ndjson"),
        ("last_batch", "invalid_ndjson"),
        ("length", "bulk_export_output_length_mismatch"),
        ("cancel", "bulk_export_group_cancelled"),
    ],
)
async def test_unfinished_group_preserves_the_saved_copy(grouped_source, monkeypatch, failure, expected):
    manifest, checkpoints = _two_output_export(monkeypatch, "https://files.example/output", ("streaming",))
    ledger = _Ledger()
    _grouped_fakes(monkeypatch, ledger)
    validator = _validator(len(ROLE_BODY))
    saved = spool.spooled_output(CHECKPOINT_ID, 0, validator).path
    body = ROLE_BODY
    if failure == "first_batch":
        body = b"bad\n" + body
    elif failure == "last_batch":
        body += b"bad\n"
    elif failure == "length":
        body += ROLE_BODY.splitlines(keepends=True)[0]
    else:
        prepare = grouped_load._prepare_batch

        def cancel_after_prepare(*args):
            result = prepare(*args)
            grouped_load._WORKER_STATE_BY_NAME["cancel"].set()
            return result

        monkeypatch.setattr(grouped_load, "_prepare_batch", cancel_after_prepare)

    saved.parent.mkdir(parents=True)
    saved.write_bytes(spool.zstd.compress(body))
    assert await _run_grouped(None, grouped_source, _grouped_identity(), manifest, checkpoints, ledger) == [expected]
    assert spool.zstd.decompress(saved.read_bytes()) == body
    assert not grouping.is_grouped(spool.group_directory(CHECKPOINT_ID), 0)
    assert ledger.staged_rows == [] and ledger.completions == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("remaining", [1, 2])
async def test_invalid_partition_stops_ordered_load_and_preserves_markers(grouped_source, monkeypatch, remaining):
    monkeypatch.setenv(grouped_load.WORKERS_ENV, "2")
    root = spool.group_directory(CHECKPOINT_ID)
    root.mkdir(parents=True)
    # The saved output is complete but was grouped with a different partition count.
    grouping.GroupWriter(root, 0, grouping.PARTITIONS + 1).finish(0)
    for partition in range(grouping.PARTITIONS - remaining):
        grouping.mark_loaded(root, partition)
    manifest, checkpoints = _two_output_export(monkeypatch, "https://files.example/output", ("streaming",))
    ledger = _Ledger()
    _grouped_fakes(monkeypatch, ledger)
    assert await _run_grouped(None, grouped_source, _grouped_identity(), manifest, checkpoints, ledger) == [
        "provider_directory_bulk_group_partitions_changed"
    ]
    assert [grouping.is_loaded(root, n) for n in range(grouping.PARTITIONS)] == [
        n < grouping.PARTITIONS - remaining for n in range(grouping.PARTITIONS)
    ]
    assert grouping.is_grouped(root, 0)
    assert ledger.staged_rows == [] and ledger.completions == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["previous_progress", "missing_validator"])
async def test_grouped_export_refuses_unusable_checkpoint_before_download(grouped_source, monkeypatch, failure):
    manifest, checkpoints = _two_output_export(monkeypatch, "https://files.example/output", ("streaming",))
    ledger = _Ledger()
    _grouped_fakes(monkeypatch, ledger)
    if failure == "previous_progress":
        checkpoints[0]["committed_bytes"] = 1
        expected = "bulk_export_group_mode_changed"
    else:
        monkeypatch.setattr(directory, "_stored_bulk_output_validator", lambda _: None)
        expected = "bulk_export_spool_validator_missing"
    assert await _run_grouped(None, grouped_source, _grouped_identity(), manifest, checkpoints, ledger) == [expected]
    root = spool.group_directory(CHECKPOINT_ID)
    assert (root / "mode").exists() == (failure == "missing_validator")
    assert not grouping.is_grouped(root, 0)
    assert ledger.staged_rows == [] and ledger.completions == {}


def test_group_workers_reserves_one_cpu_and_handles_an_unlimited_quota(monkeypatch, tmp_path):
    quota = tmp_path / "cpu.max"
    monkeypatch.delenv(grouped_load.WORKERS_ENV, raising=False)
    monkeypatch.setattr(grouped_load, "Path", lambda _: quota)
    quota.write_text("400000 100000")
    assert grouped_load.group_workers() == 3
    quota.write_text("max 100000")
    assert grouped_load.group_workers() == 1

    monkeypatch.setenv(grouped_load.WORKERS_ENV, "-1")
    with pytest.raises(ValueError, match="bulk_export_group_workers_invalid"):
        grouped_load.group_workers()
