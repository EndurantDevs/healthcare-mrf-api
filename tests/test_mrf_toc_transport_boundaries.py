# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Incomplete transports never become successful catalog writes."""

import asyncio
import io
import json
import struct
import threading
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from process import mrf_discovery_toc_crawl as crawl
from process import mrf_source_discovery as discovery
from process.mrf_toc_spool import IncompleteToc
from tests.mrf_toc_stream_support import TocResponse, toc_session, zip_members

URL = "https://catalog.example.test/index.json"
SOURCE = {"source_id": "synthetic-source", "payer_id": "synthetic-payer", "metadata_json": {}}
DOCUMENT = {
    "reporting_structure": [
        {
            "reporting_plans": [{"plan_id": "a", "plan_name": "Example Plan"}],
            "in_network_files": [{"location": "https://files.example.test/rates.json"}],
        }
    ]
}


def _assert_only_shared_parents(root):
    assert all(path.is_dir() and len(path.relative_to(root).parts) <= 3 for path in root.rglob("*"))


class ClosedResponse(TocResponse):
    def __init__(self, body, url, status=200):
        super().__init__(body, url, status)
        self.closed = False

    async def __aexit__(self, *_args):
        self.closed = True
        return False


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["http", "content_type", "body", "disconnect"])
async def test_browser_retry_preserves_bounded_json_processing(monkeypatch, tmp_path, cause):
    initial = toc_session(monkeypatch, tmp_path, b"<html>challenge</html>", status=403 if cause == "http" else 200)
    original_get = initial.get

    def get(url, **options):
        if cause == "disconnect":
            raise aiohttp.ServerDisconnectedError()
        response = original_get(url, **options)
        if cause == "content_type":
            response.headers["Content-Type"] = "text/html"
        return response

    initial.get = get
    retry = toc_session(monkeypatch, tmp_path, DOCUMENT)

    @asynccontextmanager
    async def browser_session(**options):
        assert options["trust_env"] is False
        yield retry

    monkeypatch.setattr(crawl.aiohttp, "ClientSession", browser_session)
    monkeypatch.setattr(discovery, "_tcp_connector", Mock(return_value=object()))
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    target = discovery.CrawlTarget(source=SOURCE, url=URL)
    plans, files = await crawl.stream_toc_target(target, initial, max_bytes=10_000, run_id="synthetic")
    assert (plans, files) == (1, 1)
    assert len(retry.requests) == 1 and writes.await_count == 1
    _assert_only_shared_parents(tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["prefix", "length", "redirect"])
async def test_incomplete_or_disallowed_response_cannot_reach_writer(monkeypatch, tmp_path, failure):
    session = toc_session(monkeypatch, tmp_path, DOCUMENT)
    response = TocResponse(b"{}", URL)
    if failure == "prefix":
        response.read = AsyncMock(side_effect=aiohttp.ClientPayloadError("synthetic truncated stream"))
    elif failure == "length":
        response.headers["Content-Length"] = "20000"
    else:
        response.url = "http://127.0.0.1/private"
        monkeypatch.setattr(
            discovery, "_assert_fetch_url_allowed", AsyncMock(side_effect=[None, ValueError("redirect refused")])
        )
    session.get = Mock(return_value=response)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    target = discovery.CrawlTarget(source=SOURCE, url=URL)
    with pytest.raises(ValueError if failure == "redirect" else IncompleteToc):
        await crawl.stream_toc_target(target, session, max_bytes=1000, run_id="synthetic")
    writes.assert_not_awaited()
    _assert_only_shared_parents(tmp_path)


@pytest.mark.parametrize("corruption", ["multidisk", "outside", "signature", "short"])
def test_zip64_directory_corruption_is_rejected_before_index_allocation(monkeypatch, tmp_path, corruption):
    monkeypatch.setattr(zipfile, "ZIP64_LIMIT", 0)
    payload = bytearray(zip_members([("index.json", DOCUMENT)]))
    locator = payload.rfind(b"PK\x06\x07")
    assert locator >= 0
    record = struct.unpack_from("<4sIQI", payload, locator)[2]
    if corruption == "multidisk":
        struct.pack_into("<I", payload, locator + 4, 1)
    elif corruption == "outside":
        struct.pack_into("<Q", payload, locator + 8, len(payload))
    elif corruption == "signature":
        payload[record : record + 4] = b"bad!"
    else:
        payload[record:locator] = b""
    path = tmp_path / "synthetic.zip"
    path.write_bytes(payload)
    with pytest.raises(IncompleteToc):
        crawl._validate_zip_directory(path, 1024 * 1024)


@pytest.mark.parametrize("payload", [b"", b"PK\x05\x06" + b"\0" * 3])
def test_missing_zip_directory_is_incomplete(tmp_path, payload):
    path = tmp_path / "synthetic.zip"
    path.write_bytes(payload)
    with pytest.raises(IncompleteToc, match="missing or truncated"):
        crawl._validate_zip_directory(path, 1000)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["no_json", "decoded_member", "directory_budget", "bad_crc"])
async def test_zip_container_failure_cannot_emit_partial_rows(monkeypatch, tmp_path, failure):
    payload = (
        zip_members([("notes.txt", DOCUMENT)]) if failure == "no_json" else zip_members([("index.json", DOCUMENT)])
    )
    max_bytes = 10_000
    if failure == "decoded_member":
        payload = zip_members([("index.json", {"padding": "x" * 20_000})])
        max_bytes = 1000
    elif failure == "directory_budget":
        monkeypatch.setattr(discovery, "WRITE_BATCH_BYTES", 100)
    elif failure == "bad_crc":
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
            archive.writestr("index.json", b"{}")
        payload = bytearray(buffer.getvalue())
        payload[30 + len("index.json")] ^= 1
        payload = bytes(payload)
    session = toc_session(monkeypatch, tmp_path, payload)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    target = discovery.CrawlTarget(source=SOURCE, url=URL, metadata={"container_format": "zip"})
    with pytest.raises(IncompleteToc):
        await crawl.stream_toc_target(target, session, max_bytes=max_bytes, run_id="synthetic")
    writes.assert_not_awaited()
    _assert_only_shared_parents(tmp_path)


@pytest.mark.parametrize(
    "status,content_type,prefix",
    [(403, "application/json", b"{}"), (200, "text/html", b"{}"), (200, "application/json", b"<html>challenge</html>")],
)
@pytest.mark.asyncio
async def test_browser_response_does_not_retry_itself_forever(monkeypatch, tmp_path, status, content_type, prefix):
    session = toc_session(monkeypatch, tmp_path, prefix, status=status)
    original_get = session.get

    def get(url, **options):
        response = original_get(url, **options)
        response.headers["Content-Type"] = content_type
        return response

    session.get = get
    with pytest.raises(ValueError):
        await crawl._process_response(
            discovery.CrawlTarget(source=SOURCE, url=URL), session, 1000, "synthetic", allow_fallback=False
        )
    assert len(session.requests) == 1


@pytest.mark.asyncio
async def test_preexisting_worker_state_survives_complete_toc_cleanup(monkeypatch, tmp_path):
    from process import initial

    directory = tmp_path / "run" / "toc" / "body"
    directory.mkdir(parents=True)
    retained = directory / "retained.data"
    retained.write_bytes(b"unrelated state")
    monkeypatch.setattr(initial, "_mrf_chunk_dir", lambda *_args: directory)
    session = toc_session(monkeypatch, tmp_path, DOCUMENT)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    target = discovery.CrawlTarget(source=SOURCE, url=URL)
    assert await crawl.stream_toc_target(target, session, max_bytes=10_000, run_id="synthetic") == (1, 1)
    writes.assert_awaited_once()
    assert list(directory.iterdir()) == [retained]
    assert retained.read_bytes() == b"unrelated state"


@pytest.mark.asyncio
async def test_worker_state_file_collision_cannot_destroy_or_publish_data(monkeypatch, tmp_path):
    from process import initial

    location = tmp_path / "run" / "toc" / "body"
    location.parent.mkdir(parents=True)
    location.write_bytes(b"retained file")
    monkeypatch.setattr(initial, "_mrf_chunk_dir", lambda *_args: location)
    session = toc_session(monkeypatch, tmp_path, DOCUMENT)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    with pytest.raises(IncompleteToc, match="not a directory"):
        await crawl.stream_toc_target(
            discovery.CrawlTarget(source=SOURCE, url=URL), session, max_bytes=10_000, run_id="synthetic"
        )
    writes.assert_not_awaited()
    assert location.read_bytes() == b"retained file"


@pytest.mark.asyncio
async def test_worker_state_creation_failure_preserves_shared_parents(monkeypatch, tmp_path):
    from process import initial

    location = tmp_path / "run" / "toc" / "body"
    monkeypatch.setattr(initial, "_mrf_chunk_dir", lambda *_args: location)
    original_mkdir = Path.mkdir

    def mkdir(path, *args, **kwargs):
        if path == location:
            raise PermissionError("synthetic unavailable storage")
        return original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", mkdir)
    session = toc_session(monkeypatch, tmp_path, DOCUMENT)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    with pytest.raises(IncompleteToc, match="worker-state directory is unavailable"):
        await crawl.stream_toc_target(
            discovery.CrawlTarget(source=SOURCE, url=URL), session, max_bytes=10_000, run_id="synthetic"
        )
    writes.assert_not_awaited()
    assert not location.exists()
    _assert_only_shared_parents(tmp_path)


@pytest.mark.asyncio
async def test_healthsparq_plan_metadata_without_a_file_location_emits_no_rows(monkeypatch, tmp_path):
    session = toc_session(monkeypatch, tmp_path, {"files": [{"reportingPlans": [{"planId": "a"}]}]})
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    assert await crawl.stream_toc_target(
        discovery.CrawlTarget(source=SOURCE, url=URL), session, max_bytes=10_000, run_id="synthetic"
    ) == (0, 0)
    writes.assert_not_awaited()
    _assert_only_shared_parents(tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload,limit,message",
    [
        (b'{"padding":"' + b"x" * 2000 + b'"}', 1000, "response exceeds"),
        (
            b'{"reporting_structure":[{"reporting_plans":[{"plan_id":"a"}{"plan_id":"b"}],'
            b'"in_network_files":[{"location":"https://files.example.test/rates.json"}]}],"broken":}',
            10_000,
            "invalid or incomplete",
        ),
    ],
)
async def test_unknown_length_and_unrepairable_json_cannot_publish_partial_rows(
    monkeypatch, tmp_path, payload, limit, message
):
    session = toc_session(monkeypatch, tmp_path, payload)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    with pytest.raises(IncompleteToc, match=message):
        await crawl.stream_toc_target(
            discovery.CrawlTarget(source=SOURCE, url=URL), session, max_bytes=limit, run_id="synthetic"
        )
    writes.assert_not_awaited()
    _assert_only_shared_parents(tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize("resolver", ["nested", "bcbs", "healthsparq", "query_probe"])
async def test_resource_failure_cannot_be_downgraded_to_empty_or_fallback_targets(monkeypatch, resolver):
    failure = IncompleteToc("synthetic complete-input budget exhausted")
    fetch = AsyncMock(side_effect=failure)
    session = object()
    with pytest.raises(IncompleteToc) as caught:
        if resolver == "nested":
            monkeypatch.setattr(discovery, "classify_hosting_platform", lambda _url: "synthetic")
            monkeypatch.setattr(discovery, "_crawl_targets_for_source", fetch)
            await discovery._healthcarebluebook_nested_targets(
                SOURCE, URL, (URL, "Example Plan", "", "table-of-contents"), "synthetic", None, session
            )
        elif resolver == "bcbs":
            monkeypatch.setattr(discovery, "_fetch_json_value", fetch)
            await discovery._bcbs_global_solutions_toc_target(
                SOURCE,
                {"url": URL},
                resolved_from_url=URL,
                resolver_type="synthetic",
                toc_max_bytes=1000,
                session=session,
            )
        elif resolver == "healthsparq":
            monkeypatch.setattr(discovery, "_fetch_json", fetch)
            await discovery._healthsparq_metadata_targets_or_fallback(
                SOURCE, URL, fallback_url=URL, params={}, resolver={}, session=session, verify_metadata_url=False
            )
        else:
            monkeypatch.setattr(discovery, "_sapphire_query_probe_targets", fetch)
            await discovery._query_probe_targets(SOURCE, URL, "Example Plan", SimpleNamespace(session=session))
    assert caught.value is failure
    fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_retained_text_adapter_preserves_complete_plan_and_direct_file_rows(monkeypatch):
    content = "File Scope: In Network | Plan Name: Example | https://files.example.test/in-network-rates.zip"
    fetch = AsyncMock(return_value=content)
    monkeypatch.setattr(discovery, "_fetch_text", fetch)
    target = discovery.CrawlTarget(source=SOURCE, url=URL, metadata={"target_kind": "metadata_text"})
    plans, files, count = await discovery._crawl_target_rows(target, object(), target_max_bytes=10_000)
    assert len(plans) == count == len(files) == 1
    assert plans[0]["plan_name"] == "Example"
    assert files[0]["url"] == "https://files.example.test/in-network-rates.zip"
    assert files[0]["metadata_json"]["container_format"] == "zip"


@pytest.mark.asyncio
async def test_private_redirect_is_refused_before_request_and_closes_response(monkeypatch, tmp_path):
    guard = discovery._assert_fetch_url_allowed
    session = toc_session(monkeypatch, tmp_path, DOCUMENT)
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", guard)
    monkeypatch.setattr(discovery.socket, "getaddrinfo", lambda *_args, **_kwargs: [(0, 0, 0, "", ("8.8.8.8", 443))])
    response = ClosedResponse(b"", URL, 302)
    response.headers["Location"] = "http://127.0.0.1/private"
    session.get = Mock(return_value=response)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    with pytest.raises(ValueError, match="non-public IP"):
        await crawl.stream_toc_target(
            discovery.CrawlTarget(source=SOURCE, url=URL), session, max_bytes=10_000, run_id="synthetic"
        )
    assert [call.args[0] for call in session.get.call_args_list] == [URL]
    assert session.get.call_args.kwargs["allow_redirects"] is False
    assert response.closed
    writes.assert_not_awaited()
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("location", ["../resolved.json", "https://files.example.test/resolved.json"])
async def test_redirects_guard_resolved_urls_and_recompute_ssl(monkeypatch, tmp_path, status, location):
    session = toc_session(monkeypatch, tmp_path, DOCUMENT)
    monkeypatch.setattr(discovery, "_incomplete_tls_chain_hosts", lambda: {"catalog.example.test"})
    resolved = "https://catalog.example.test/resolved.json" if location.startswith("..") else location
    redirect = ClosedResponse(b"unread redirect body", URL, status)
    redirect.headers["Location"] = location
    redirect.read = AsyncMock(side_effect=AssertionError("redirect body must not be consumed"))
    final = ClosedResponse(json.dumps(DOCUMENT).encode(), resolved)
    responses = [redirect, final]
    requests = []

    def get(url, **options):
        assert discovery._assert_fetch_url_allowed.await_args.args == (url,)
        if requests:
            assert redirect.closed
        requests.append((url, options))
        return responses[len(requests) - 1]

    session.get = get
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    assert await crawl.stream_toc_target(
        discovery.CrawlTarget(source=SOURCE, url=URL), session, max_bytes=10_000, run_id="synthetic"
    ) == (1, 1)
    assert [url for url, _options in requests] == [URL, resolved]
    assert all(options["allow_redirects"] is False and options["auto_decompress"] is False for _, options in requests)
    assert requests[0][1]["ssl"] is False
    assert requests[1][1].get("ssl") is (False if location.startswith("..") else None)
    assert redirect.closed and final.closed
    redirect.read.assert_not_awaited()
    writes.assert_awaited_once()
    _assert_only_shared_parents(tmp_path)


@pytest.mark.asyncio
async def test_redirect_limit_closes_every_response_without_writing(monkeypatch, tmp_path):
    session = toc_session(monkeypatch, tmp_path, DOCUMENT)
    responses = []
    for index in range(11):
        url = URL if index == 0 else f"https://catalog.example.test/hop-{index}"
        response = ClosedResponse(b"", url, 302)
        response.headers["Location"] = f"/hop-{index + 1}"
        responses.append(response)
    session.get = Mock(side_effect=responses)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    with pytest.raises(ValueError, match="redirect hop limit"):
        await crawl.stream_toc_target(
            discovery.CrawlTarget(source=SOURCE, url=URL), session, max_bytes=10_000, run_id="synthetic"
        )
    assert session.get.call_count == 11 and all(response.closed for response in responses)
    assert all(not call.kwargs["allow_redirects"] for call in session.get.call_args_list)
    writes.assert_not_awaited()
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_failed_final_response_closes_both_redirect_hops(monkeypatch, tmp_path):
    session = toc_session(monkeypatch, tmp_path, DOCUMENT)
    redirect = ClosedResponse(b"", URL, 302)
    redirect.headers["Location"] = "/missing.json"
    final = ClosedResponse(b"", "https://catalog.example.test/missing.json", 404)
    session.get = Mock(side_effect=[redirect, final])
    with pytest.raises(ValueError, match="HTTP 404"):
        await crawl._process_response(
            discovery.CrawlTarget(source=SOURCE, url=URL), session, 1000, "synthetic", allow_fallback=False
        )
    assert redirect.closed and final.closed
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_peer_target_cannot_lose_shared_parent_during_leaf_creation(monkeypatch, tmp_path):
    from process import initial

    first = tmp_path / "run" / "toc" / "first"
    second = first.with_name("second")
    monkeypatch.setattr(initial, "_mrf_chunk_dir", lambda _ctx, _kind, url: first if url == URL else second)
    ready, proceed = threading.Event(), threading.Event()
    original_mkdir = Path.mkdir

    def mkdir(path, *args, **kwargs):
        if path == second:
            ready.set()
            assert proceed.wait(5), "peer target was not released"
        return original_mkdir(path, *args, **kwargs)

    async def open_peer():
        async with crawl._target_directory(SimpleNamespace(url=URL + "?peer"), "run") as directory:
            assert directory.is_dir()

    monkeypatch.setattr(Path, "mkdir", mkdir)
    peer = None
    try:
        async with crawl._target_directory(SimpleNamespace(url=URL), "run"):
            peer = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(open_peer())))
            assert await asyncio.to_thread(ready.wait, 5), "peer did not observe the shared parents"
    finally:
        proceed.set()
        if peer is not None:
            await peer
    assert first.is_dir() and second.is_dir()
    _assert_only_shared_parents(tmp_path)


async def _peer_target_scratch(target, entered, proceed, opened, finish):
    try:
        async with crawl._target_directory(target, "run") as directory:
            entered.set()
            await proceed.wait()
            async with crawl.open_toc_store(
                URL, row_bytes=10_000, spool_bytes=1024**2, temporary_parent=directory
            ) as spool:
                await spool.read_document(TocResponse(json.dumps(DOCUMENT).encode(), URL), max_bytes=10_000)
                opened.put_nowait(spool.directory)
                await finish.wait()
    except BaseException as exc:
        opened.put_nowait(exc)
        raise


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_peer", [False, True])
async def test_same_url_sources_keep_shared_leaf_until_peer_scratch_closes(monkeypatch, tmp_path, cancel_peer):
    monkeypatch.setenv("HLTHPRT_MRF_CHUNK_WORKDIR", str(tmp_path))
    creator_target = discovery.CrawlTarget(source=SOURCE, url=URL)
    peer_target = discovery.CrawlTarget(source={**SOURCE, "source_id": "synthetic-peer"}, url=URL)
    entered, proceed, finish = asyncio.Event(), asyncio.Event(), asyncio.Event()
    opened = asyncio.Queue()
    peer = None
    try:
        async with crawl._target_directory(creator_target, "run") as directory:
            async with crawl.open_toc_store(
                URL, row_bytes=10_000, spool_bytes=1024**2, temporary_parent=directory
            ) as spool:
                first_scratch = spool.directory
                await spool.read_document(TocResponse(json.dumps(DOCUMENT).encode(), URL), max_bytes=10_000)
                peer = asyncio.create_task(_peer_target_scratch(peer_target, entered, proceed, opened, finish))
                await asyncio.wait_for(entered.wait(), 5)
            assert not first_scratch.exists()
        assert directory.is_dir()
        proceed.set()
        peer_scratch = await asyncio.wait_for(opened.get(), 5)
        if isinstance(peer_scratch, BaseException):
            raise peer_scratch
        assert peer_scratch != first_scratch and peer_scratch.parent == directory
        assert list(directory.iterdir()) == [peer_scratch]
        if cancel_peer:
            peer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await peer
        else:
            finish.set()
            await peer
    finally:
        if peer is not None:
            peer.cancel()
            await asyncio.gather(peer, return_exceptions=True)
    assert directory.is_dir() and not list(directory.iterdir())
    assert not peer_scratch.exists()
    _assert_only_shared_parents(tmp_path)
