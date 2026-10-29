# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""End-to-end synthetic TOC admission, writing and checkpoint failure contracts."""

from __future__ import annotations

import asyncio
import gzip
import io
import json
import zipfile
from unittest.mock import AsyncMock, Mock

import pytest

from process import mrf_discovery_toc_crawl as crawl
from process import mrf_source_discovery as discovery
from process import mrf_toc_spool
from process.mrf_discovery_checkpoints import (
    DiscoverySourceBatchIncomplete,
    SourceProcessResult,
    execute_checkpointed_source_batch,
)
from process.mrf_discovery_toc_crawl import toc_spool_bytes
from tests.mrf_toc_stream_support import toc_session, zip_members
from tests.test_mrf_discovery_source_checkpoints import InMemoryDiscoveryCheckpointStore

URL = "https://catalog.example.test/index.json"
SOURCE = {"source_id": "source_synthetic", "payer_id": "payer_synthetic", "metadata_json": {}}


def _document():
    return {
        "version": "version-" + "x" * 50,
        "reporting_entity_name": "Synthetic Payer",
        "reporting_structure": [
            {
                "reporting_plans": [{"plan_id": "a", "plan_name": "Alpha"}],
                "in_network_files": [{"location": "https://files.example.test/shared.json", "description": "old"}],
            },
            {
                "reporting_plans": [{"plan_id": "b", "plan_name": "Beta"}],
                "in_network_files": [{"location": "https://files.example.test/shared.json", "description": "new"}],
            },
        ],
    }


def _without_times(rows):
    return [{key: value for key, value in row.items() if key not in {"first_seen_at", "last_seen_at"}} for row in rows]


def _encoded_zip_members(members):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, body in members:
            archive.writestr(name, body)
    return stream.getvalue()


@pytest.mark.asyncio
@pytest.mark.parametrize("compressed", [(False, False), (False, True), (True, False), (True, True)])
async def test_zip_stream_shares_decoded_budget_before_any_sink(monkeypatch, tmp_path, compressed):
    document = json.dumps(_document() | {"padding": "x" * 4096}).encode()
    members = [
        (f"{index}.json" + (".gz" if zipped else ""), gzip.compress(document) if zipped else document)
        for index, zipped in enumerate(compressed)
    ]
    body = _encoded_zip_members(members)
    limit = 2 * len(document) - 1
    assert len(body) < limit and all(len(member) < limit for _name, member in members)
    session = toc_session(monkeypatch, tmp_path, body)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    target = discovery.CrawlTarget(source=SOURCE, url=URL, metadata={"container_format": "zip"})
    with pytest.raises(discovery.IncompleteToc, match="combined discovery byte limit"):
        await crawl.stream_toc_target(target, session, max_bytes=limit, run_id="synthetic")
    writes.assert_not_awaited()
    assert all(path.is_dir() and len(path.relative_to(tmp_path).parts) <= 3 for path in tmp_path.rglob("*"))


@pytest.mark.asyncio
async def test_zip_decoded_boundary_does_not_charge_container_or_gzip_headers(monkeypatch, tmp_path):
    document = _document() | {"padding": "x" * 1024}
    first_body = json.dumps(document).encode()
    last_body = gzip.compress(b"{}")
    assert len(last_body) > 2
    limit = len(first_body) + 2
    body = _encoded_zip_members([("a-index.json", first_body), ("z-empty.json.gz", last_body)])
    assert len(body) < limit
    session = toc_session(monkeypatch, tmp_path, body)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    target = discovery.CrawlTarget(source=SOURCE, url=URL, metadata={"container_format": "zip"})
    assert await crawl.stream_toc_target(target, session, max_bytes=limit, run_id="synthetic") == (2, 1)
    plans = [row for call in writes.await_args_list for row in call.args[0]]
    files = [row for call in writes.await_args_list for row in call.args[1]]
    expected_plans, expected_files = discovery._toc_rows_from_content(SOURCE, URL, document)
    expected_files = discovery._apply_crawl_target_context_to_file_rows(expected_files, target)
    assert _without_times(plans) == _without_times(expected_plans)
    assert _without_times(files) == _without_times(expected_files)
    assert all(path.is_dir() and len(path.relative_to(tmp_path).parts) <= 3 for path in tmp_path.rglob("*"))


@pytest.mark.asyncio
@pytest.mark.parametrize("container", ["plain", "gzip", "zip", "zip64"])
async def test_complete_stream_matches_existing_rows_and_returns_only_unique_counts(monkeypatch, tmp_path, container):
    document_dict = _document()
    encoded = json.dumps(document_dict).encode()
    if container == "gzip":
        encoded = gzip.compress(encoded)
    elif container in {"zip", "zip64"}:
        if container == "zip64":
            monkeypatch.setattr(zipfile, "ZIP64_LIMIT", 0)
        encoded = zip_members([("index.json", document_dict), ("second.json", document_dict)])
    session = toc_session(monkeypatch, tmp_path, encoded)
    crawl_target = discovery.CrawlTarget(
        source=SOURCE,
        url=URL,
        label="Synthetic",
        metadata={"container_format": "zip" if container == "zip64" else container},
    )
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    crawl_result = await discovery._crawl_one_toc_target(
        crawl_target, session, max_toc_bytes=100_000, run_id="synthetic_run"
    )
    assert isinstance(crawl_result, discovery.PersistedTocResult)
    assert (crawl_result.plan_count, crawl_result.file_count) == (2, 1)
    plans = [plan_row for call in writes.await_args_list for plan_row in call.args[0]]
    files = [file_row for call in writes.await_args_list for file_row in call.args[1]]
    expected_plans, expected_files = discovery._toc_rows_from_content(SOURCE, URL, document_dict)
    expected_files = discovery._apply_crawl_target_context_to_file_rows(expected_files, crawl_target)
    assert _without_times(plans) == _without_times(expected_plans)
    assert _without_times(files) == _without_times(expected_files)
    assert files[0]["schema_version"] == document_dict["version"][:32]
    assert session.requests[0][1]["auto_decompress"] is False
    assert session.requests[0][1]["headers"] == {"Accept-Encoding": "identity"}
    assert all(path.is_dir() and len(path.relative_to(tmp_path).parts) <= 3 for path in tmp_path.rglob("*"))


@pytest.mark.asyncio
async def test_root_files_adapter_matches_existing_row_builders(monkeypatch, tmp_path):
    document_dict = {
        "files": [
            {
                "filePath": "rates.json",
                "fileSchema": "in_network_rates",
                "reportingPlans": [{"planId": "a"}, {"planId": "b"}],
            }
        ]
    }
    session = toc_session(monkeypatch, tmp_path, document_dict)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    target = discovery.CrawlTarget(source=SOURCE, url=URL)
    result = await discovery._crawl_one_toc_target(target, session, max_toc_bytes=10_000, run_id="run")
    plans, files = discovery._toc_rows_from_content(SOURCE, URL, document_dict)
    assert result.plan_count == len(plans)
    assert result.file_count == len(files)
    assert _without_times(writes.await_args.args[0]) == _without_times(plans)
    assert _without_times(writes.await_args.args[1]) == _without_times(files)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["truncated", "decoded", "disk", "scratch"])
async def test_resource_failure_persists_diagnostic_and_fails_source_checkpoint(monkeypatch, tmp_path, failure):
    body = b'{"reporting_structure":[' if failure == "truncated" else json.dumps(_document()).encode()
    if failure == "decoded":
        body = gzip.compress(json.dumps({"padding": "x" * 100_000}).encode())
    session = toc_session(monkeypatch, tmp_path, body)
    if failure == "disk":
        monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_SPOOL_BYTES", "1")
    if failure == "scratch":
        monkeypatch.setattr(
            mrf_toc_spool.tempfile, "TemporaryDirectory", Mock(side_effect=OSError("storage unavailable"))
        )
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    store = InMemoryDiscoveryCheckpointStore()

    async def process_source(source_dict):
        target = discovery.CrawlTarget(source=source_dict, url=URL)
        await discovery._crawl_one_toc_target(target, session, max_toc_bytes=10_000, run_id="run")
        pytest.fail("incomplete target returned a successful source")

    with pytest.raises(DiscoverySourceBatchIncomplete) as caught:
        await execute_checkpointed_source_batch(
            root_run_id="run",
            owner_run_id="run",
            source_records=[SOURCE],
            concurrency=1,
            process_source=process_source,
            checkpoint_store=store,
        )
    assert caught.value.summary.failed_source_count == 1
    assert caught.value.summary.completed_source_count == 0
    assert store.checkpoints[SOURCE["source_id"]]["status"] == "failed"
    observations = [observation_row for call in writes.await_args_list for observation_row in call.args[2]]
    assert len(observations) == 1 and observations[0]["status"] == "crawl_failed"
    assert all(path.is_dir() and len(path.relative_to(tmp_path).parts) <= 3 for path in tmp_path.rglob("*"))


@pytest.mark.asyncio
async def test_ordinary_http_failure_remains_a_failed_url_observation(monkeypatch, tmp_path):
    session = toc_session(monkeypatch, tmp_path, b"{}", status=404)
    target = discovery.CrawlTarget(source=SOURCE, url=URL)
    plans, files, observations, _ = await discovery._crawl_one_toc_target(
        target, session, max_toc_bytes=1000, run_id="run"
    )
    assert plans == files == []
    assert observations[0]["status"] == "crawl_failed"
    assert "HTTP 404" in observations[0]["error"]


@pytest.mark.parametrize("configured", ["0", "-1", "invalid", ""])
def test_spool_configuration_must_be_positive(monkeypatch, configured):
    monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_SPOOL_BYTES", configured)
    with pytest.raises(ValueError, match="must be positive"):
        toc_spool_bytes()


def test_spool_default_and_explicit_worker_value(monkeypatch):
    monkeypatch.delenv("HLTHPRT_MRF_DISCOVERY_SPOOL_BYTES", raising=False)
    assert toc_spool_bytes() == 1024**3
    monkeypatch.setenv("HLTHPRT_MRF_DISCOVERY_SPOOL_BYTES", "512 MiB")
    assert toc_spool_bytes() == 512 * 1024**2


@pytest.mark.parametrize("body", [gzip.compress(b"x" * 100_000), gzip.compress(b"{}")[:-4]])
def test_raw_gzip_decode_never_bypasses_decoded_limit(body):
    with pytest.raises(discovery.IncompleteToc):
        discovery._decode_response_body(body, max_bytes=1000)


@pytest.mark.asyncio
async def test_resolver_resource_failure_drains_other_children(monkeypatch):
    started, stopped = asyncio.Event(), asyncio.Event()
    sources = [SOURCE | {"index_url": URL}, SOURCE | {"source_id": "source_other", "index_url": URL + "?other"}]

    async def resolve(source_dict, *_args, **_kwargs):
        if source_dict["source_id"] == SOURCE["source_id"]:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        await started.wait()
        raise discovery.IncompleteToc("decoded response exceeds the discovery byte limit")

    monkeypatch.setattr(discovery, "_crawl_targets_for_source", resolve)
    writes = AsyncMock()
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", writes)
    with pytest.raises(discovery.IncompleteToc, match="decoded response"):
        await discovery._resolve_crawl_targets(sources, session=object(), run_id="run", concurrency=2)
    assert stopped.is_set()
    assert writes.await_args.args[2][0]["status"] == "crawl_failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("resource", [False, True])
async def test_resolver_retains_http_fallback_but_propagates_resource_failure(monkeypatch, resource):
    failure = discovery.IncompleteToc("decoded limit") if resource else ValueError("HTTP 404")
    monkeypatch.setattr(discovery, "_fetch_json", AsyncMock(side_effect=failure))
    request = discovery._resolve_healthsparq_direct_metadata(
        SOURCE,
        "https://catalog.example.test/prd/mrf/example/brand/index.json",
        {},
        object(),
    )
    if resource:
        with pytest.raises(discovery.IncompleteToc):
            await request
    else:
        assert len(await request) == 1


def test_retained_zip_resolver_bounds_all_decoded_members_together():
    archive = zip_members([(name, {"value": "x" * 25}) for name in ("first.json", "second.json")])
    with pytest.raises(discovery.IncompleteToc, match="combined discovery byte limit"):
        discovery._json_values_from_zip_bytes(archive, max_bytes=50)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failed_fetch,direct_fallback", [("name_index", False), ("name_index", True), ("employer", False)]
)
async def test_anthem_resource_failure_reaches_failed_checkpoint(monkeypatch, failed_fetch, direct_fallback):
    from tests.test_mrf_source_discovery import (
        _example_anthem_employer_result,
        _example_anthem_employer_source_row,
        _example_anthem_lookup_context,
    )

    source_record = _example_anthem_employer_source_row()
    source_record["index_url"] = URL
    failure = discovery.IncompleteToc("decoded response exceeds the discovery byte limit")
    failures = []

    async def fetch(url, **kwargs):
        if "/namesearch/" in url:
            if failed_fetch == "name_index":
                raise failure
            return {"namesearch": [{"ein": "12-3456789", "name": "example employer"}]}
        if failed_fetch == "employer":
            raise failure
        return _example_anthem_employer_result()

    async def resolve(source_row, *args, **kwargs):
        return await discovery._resolve_anthem_s3_context(
            source_row,
            employer_ein="12-3456789" if direct_fallback else "",
            employer_names=("Example Employer",),
            lookup_context=_example_anthem_lookup_context(),
        )

    async def process_source(source_row):
        try:
            await discovery._resolve_crawl_targets([source_row], session=object(), run_id="run", concurrency=1)
        except discovery.IncompleteToc as error:
            failures.append(error)
            raise
        return SourceProcessResult()

    monkeypatch.setattr(discovery, "_fetch_json", fetch)
    monkeypatch.setattr(discovery, "_crawl_targets_for_source", resolve)
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", AsyncMock())
    store = InMemoryDiscoveryCheckpointStore()
    with pytest.raises(DiscoverySourceBatchIncomplete) as caught:
        await execute_checkpointed_source_batch(
            root_run_id="run",
            owner_run_id="run",
            source_records=[source_record],
            concurrency=1,
            process_source=process_source,
            checkpoint_store=store,
        )
    assert failures == [failure]
    assert caught.value.summary.failed_source_count == 1 and caught.value.summary.completed_source_count == 0
    assert store.checkpoints[source_record["source_id"]]["status"] == "failed"


@pytest.mark.asyncio
async def test_shared_plan_group_preserves_complete_legacy_rows(monkeypatch, tmp_path):
    from process.mrf_discovery_toc_crawl import _append_document_rows

    plans = [{"plan_id": str(index), "plan_name": "Synthetic " + str(index)} for index in range(100)]
    document_dict = {
        "reporting_structure": [
            {
                "reporting_plans": plans,
                "in_network_files": [{"location": f"https://files.example.test/{index}.json"} for index in range(40)],
            }
        ]
    }
    body = json.dumps(document_dict).encode()
    crawl_target = discovery.CrawlTarget(source=SOURCE, url=URL)
    real_plan_row = discovery._toc_plan_row
    plan_calls = Mock(wraps=real_plan_row)
    monkeypatch.setattr(discovery, "_toc_plan_row", plan_calls)
    batches = []

    async def sink(plan_rows, file_rows):
        batches.append((plan_rows, file_rows))

    async with mrf_toc_spool.open_toc_store(
        URL,
        row_bytes=128_000,
        spool_bytes=32 * 1024**2,
        temporary_parent=tmp_path,
    ) as spool:
        await spool.read_document(toc_session_reader(body), max_bytes=128_000)
        await _append_document_rows(spool, crawl_target, False)
        assert plan_calls.call_count == 200
        assert spool.connection.execute("SELECT count(*) FROM group_plans").fetchone()[0] == 100
        assert spool.connection.execute("SELECT count(*) FROM entry_groups").fetchone()[0] == 40
        assert spool.connection.execute("SELECT count(*) FROM plan_sets").fetchone()[0] == 1
        assert spool.connection.execute("SELECT count(*) FROM plans").fetchone()[0] == 0
        assert spool.connection.execute("SELECT max(length(payload)) FROM rows WHERE kind='file'").fetchone()[0] < 2000
        counts = await spool.drain_rows(sink, batch_size=1000, batch_bytes=128_000)
    actual_plans = [row_record for batch_rows, _files in batches for row_record in batch_rows]
    actual_files = [row_record for _plans, batch_rows in batches for row_record in batch_rows]
    expected_plans, expected_files = discovery._toc_rows_from_content(SOURCE, URL, document_dict)
    expected_files = discovery._apply_crawl_target_context_to_file_rows(expected_files, crawl_target)
    assert counts == (100, 40)
    assert _without_times(actual_plans) == _without_times(expected_plans)
    assert _without_times(actual_files) == _without_times(expected_files)
    assert not list(tmp_path.iterdir())


def toc_session_reader(body):
    class Reader:
        def __init__(self):
            self.offset = 0

        async def read(self, size):
            chunk = body[self.offset : self.offset + size]
            self.offset += len(chunk)
            return chunk

    return Reader()


@pytest.mark.asyncio
async def test_interleaved_plan_unions_keep_first_order_and_last_fields(monkeypatch, tmp_path):
    from process.mrf_discovery_toc_crawl import _append_document_rows

    def file(name):
        return {"location": f"https://files.example.test/{name}.json"}

    document_dict = {
        "reporting_structure": [
            {"reporting_plans": [{"plan_id": "base"}], "in_network_files": [file("a"), file("b"), file("c")]},
            {"reporting_plans": [{"plan_id": "one"}], "in_network_files": [file("a"), file("c")]},
            {"reporting_plans": [{"plan_id": "two"}], "in_network_files": [file("b")]},
        ]
    }
    original_plan_row = discovery._toc_plan_row

    def plan_row(source_record, entry, plan):
        identity, plan_record = original_plan_row(source_record, entry, plan)
        return identity, {**plan_record, "reporting_entity_name": entry.canonical_url}

    monkeypatch.setattr(discovery, "_toc_plan_row", plan_row)
    crawl_target = discovery.CrawlTarget(source=SOURCE, url=URL)
    batches = []

    async def sink(plans, files):
        batches.append((plans, files))

    async with mrf_toc_spool.open_toc_store(
        URL,
        row_bytes=128_000,
        spool_bytes=32 * 1024**2,
        temporary_parent=tmp_path,
    ) as spool:
        await spool.read_document(toc_session_reader(json.dumps(document_dict).encode()), max_bytes=128_000)
        await _append_document_rows(spool, crawl_target, False)
        assert spool.connection.execute("SELECT count(*) FROM plan_sets").fetchone()[0] == 2
        await spool.drain_rows(sink, batch_size=1000, batch_bytes=128_000)
    actual_plans = [plan_record for plan_batch, _files in batches for plan_record in plan_batch]
    actual_files = [plan_record for _plans, file_batch in batches for plan_record in file_batch]
    expected_plans, expected_files = discovery._toc_rows_from_content(SOURCE, URL, document_dict)
    expected_files = discovery._apply_crawl_target_context_to_file_rows(expected_files, crawl_target)
    assert [plan_record["plan_id"] for plan_record in actual_plans] == ["base", "one", "two"]
    assert _without_times(actual_plans) == _without_times(expected_plans)
    assert _without_times(actual_files) == _without_times(expected_files)
    assert actual_plans[0]["reporting_entity_name"].endswith("/c.json")
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_transport_deadline_ends_at_eof_before_slow_row_processing(monkeypatch, tmp_path):
    from process import initial
    from process import mrf_discovery_toc_crawl as crawl

    body = json.dumps(_document()).encode()

    async def serve(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: "
                + str(len(body)).encode()
                + b"\r\nConnection: close\r\n\r\n"
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    address = server.sockets[0].getsockname()
    monkeypatch.setattr(initial, "_mrf_chunk_dir", lambda *_args: tmp_path / "run" / "toc" / "body")
    monkeypatch.setattr(discovery, "_push_crawl_row_batches", AsyncMock())
    original_append = crawl._append_document_rows
    try:
        async with server, discovery.aiohttp.ClientSession(timeout=discovery.aiohttp.ClientTimeout(total=1)) as session:
            async with session.get(f"http://127.0.0.1:{address[1]}/index.json") as response:

                async def slow_append(*args):
                    assert response.content.at_eof() and response.closed
                    await asyncio.sleep(1.1)
                    return await original_append(*args)

                monkeypatch.setattr(crawl, "_append_document_rows", slow_append)
                crawl_target = discovery.CrawlTarget(source=SOURCE, url=URL)
                assert await crawl._persist_target_response(
                    crawl_target,
                    response.content,
                    "utf-8",
                    100_000,
                    "run",
                    is_zip=False,
                ) == (2, 1)
    finally:
        server.close()
        await server.wait_closed()
    assert all(path.is_dir() and len(path.relative_to(tmp_path).parts) <= 3 for path in tmp_path.rglob("*"))
