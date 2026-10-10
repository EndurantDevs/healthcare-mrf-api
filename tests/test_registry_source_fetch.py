# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Controlled native HTTP proves artifact accounting, never publisher coverage."""

import asyncio
import hashlib
import io
import threading
import zipfile
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from aiohttp import web

from process import registry_source_fetch as source_fetch
from process.registry_source_admission import RegistrySourceEdition, admit_cms_mlr_edition
from process.registry_source_fetch import (
    MAX_ARTIFACT_BYTES,
    PLAN_FINDER_MAX_ARTIFACT_BYTES,
    RegistrySourceFetchError,
    RegistrySourceFetchSpec,
    RegistrySourceLoopbackTransport,
    fetch_registry_source,
)
from tests.test_network_serving_schema_postgres import serving_schema


def _spec(**changes):
    spec = RegistrySourceFetchSpec(
        "cms",
        "commercial-mlr",
        "synthetic-edition",
        "https://www.cms.gov/files/zip/synthetic.zip",
        "cms-header-v1",
        2024,
        datetime(2025, 9, 12, tzinfo=timezone.utc),
        4 * 1024 * 1024,
    )
    return replace(spec, **changes)


@asynccontextmanager
async def _source_http(body=b"synthetic artifact", *, chunk_bytes=8192):
    state = SimpleNamespace(
        body=body,
        status=200,
        headers={},
        conditional=False,
        requests=[],
        abort=False,
        delay=0,
        callback=None,
        abort_before_headers=False,
    )

    async def serve(request):
        state.requests.append(dict(request.headers))
        if state.delay:
            await asyncio.sleep(state.delay)
        if state.callback is not None:
            state.callback()
        if state.abort_before_headers:
            request.transport.abort()
            return web.Response()
        if state.conditional and request.headers.get("If-None-Match") == state.headers.get("ETag"):
            return web.Response(status=304, headers=state.headers)
        response = web.StreamResponse(status=state.status, headers=state.headers)
        await response.prepare(request)
        for offset in range(0, len(state.body), chunk_bytes):
            await response.write(state.body[offset : offset + chunk_bytes])
            if state.abort:
                request.transport.abort()
                return response
        await response.write_eof()
        return response

    application = web.Application()
    application.router.add_get("/artifact", serve)
    runner = web.AppRunner(application, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    try:
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        yield state, RegistrySourceLoopbackTransport(f"http://127.0.0.1:{port}/artifact")
    finally:
        site._server.close_clients()
        await runner.cleanup()
        await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("chunk_bytes", [1, 4093, 65536, 131071])
async def test_streamed_bytes_have_identical_digest_and_single_artifact(tmp_path, chunk_bytes):
    payload = bytes(range(256)) * (4096 if chunk_bytes > 1 else 16)
    async with _source_http(payload, chunk_bytes=chunk_bytes) as (state, transport):
        result = await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        replay = await fetch_registry_source(_spec(), tmp_path, previous=result.receipt, _test_transport=transport)
    assert result.receipt.artifact_sha256 == hashlib.sha256(payload).hexdigest()
    assert result.receipt.artifact_bytes == len(payload)
    assert result.artifact_path.read_bytes() == payload
    assert result.unchanged is False and replay.unchanged is True
    assert replay.artifact_path == result.artifact_path and len(list(tmp_path.iterdir())) == 1
    assert len(state.requests) == 2 and state.requests[0]["Accept-Encoding"] == "identity"
    assert result.receipt.spec.reporting_year == 2024
    assert result.receipt.spec.published_at.year == 2025
    assert result.receipt.retrieved_at.utcoffset().total_seconds() == 0


@pytest.mark.asyncio
async def test_conditional_receipt_requires_previously_verified_local_bytes(tmp_path):
    async with _source_http() as (state, transport):
        state.headers = {"ETag": '"edition-a"'}
        state.conditional = True
        first = await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        second = await fetch_registry_source(_spec(), tmp_path, previous=first.receipt, _test_transport=transport)
        assert second.unchanged and second.receipt.artifact_sha256 == first.receipt.artifact_sha256
        assert state.requests[-1]["If-None-Match"] == '"edition-a"'
        first.artifact_path.write_bytes(b"corrupted")
        repaired = await fetch_registry_source(_spec(), tmp_path, previous=first.receipt, _test_transport=transport)
        assert "If-None-Match" not in state.requests[-1]
        assert repaired.unchanged is False and repaired.artifact_path.read_bytes() == state.body


@pytest.mark.asyncio
async def test_last_modified_and_changed_content_preserve_old_artifact(tmp_path):
    async with _source_http(b"earlier") as (state, transport):
        state.headers = {"Last-Modified": "Fri, 12 Sep 2025 00:00:00 GMT"}
        first = await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        state.body = b"changed"
        second = await fetch_registry_source(_spec(), tmp_path, previous=first.receipt, _test_transport=transport)
        assert state.requests[-1]["If-Modified-Since"] == state.headers["Last-Modified"]
    assert second.unchanged is False and second.receipt.artifact_sha256 != first.receipt.artifact_sha256
    assert first.artifact_path.read_bytes() == b"earlier" and second.artifact_path.read_bytes() == b"changed"


@pytest.mark.asyncio
async def test_oversized_cache_is_not_read_before_atomic_repair(tmp_path, monkeypatch):
    async with _source_http(b"good") as (state, transport):
        first = await fetch_registry_source(_spec(max_bytes=8), tmp_path, _test_transport=transport)
        first.artifact_path.write_bytes(b"oversized corrupt cache")

        def reject_cache_read(path):
            raise AssertionError("Oversized cache must not be hashed")

        monkeypatch.setattr(source_fetch, "sha256_file", reject_cache_read)
        repaired = await fetch_registry_source(_spec(max_bytes=8), tmp_path, _test_transport=transport)
    assert repaired.artifact_path == first.artifact_path and repaired.artifact_path.read_bytes() == state.body
    assert repaired.unchanged is False and len(list(tmp_path.iterdir())) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes", [{"edition_id": "later"}, {"parser_version": "v2"}, {"reporting_year": 2025}, {"published_at": None}]
)
async def test_same_bytes_do_not_skip_distinct_edition_or_parser(tmp_path, changes):
    async with _source_http() as (state, transport):
        state.headers = {"ETag": '"same"'}
        first = await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        later = await fetch_registry_source(
            _spec(**changes), tmp_path, previous=first.receipt, _test_transport=transport
        )
    assert later.unchanged is False and later.artifact_path == first.artifact_path
    assert "If-None-Match" not in state.requests[-1] and len(list(tmp_path.iterdir())) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [301, 302, 307, 308, 403, 404, 429, 500, 503])
async def test_unavailable_and_redirects_are_single_attempt_sanitized(tmp_path, status):
    async with _source_http() as (state, transport):
        first = await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        state.status = status
        state.headers = {"Location": "http://127.0.0.1/private"}
        with pytest.raises(RegistrySourceFetchError) as caught:
            await fetch_registry_source(_spec(), tmp_path, previous=first.receipt, _test_transport=transport)
        assert len(state.requests) == 2
    assert str(caught.value) == "Registry source artifact is unavailable or invalid"
    assert "cms.gov" not in str(caught.value) and "127.0.0.1" not in str(caught.value)
    assert first.artifact_path.read_bytes() == b"synthetic artifact" and len(list(tmp_path.iterdir())) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["overflow", "length", "truncated", "digest", "encoding", "validator", "empty"])
async def test_transfer_failure_cleans_stage_and_preserves_previous(tmp_path, mode):
    async with _source_http(b"good", chunk_bytes=2) as (state, transport):
        first = await fetch_registry_source(_spec(max_bytes=8), tmp_path, _test_transport=transport)
        configuration_by_mode = {
            "overflow": (b"bad-content", {}, {}, False),
            "length": (b"bad-content", {"Content-Length": "11"}, {}, False),
            "truncated": (b"bad-content", {}, {}, True),
            "digest": (b"bad-content", {}, {"max_bytes": 32, "expected_sha256": "a" * 64}, False),
            "encoding": (b"bad-content", {"Content-Encoding": "gzip"}, {}, False),
            "validator": (b"new", {"Last-Modified": "not a date"}, {}, False),
            "empty": (b"", {}, {}, False),
        }
        state.body, state.headers, changes, state.abort = configuration_by_mode[mode]
        spec = replace(_spec(max_bytes=8), **changes)
        with pytest.raises(RegistrySourceFetchError):
            await fetch_registry_source(spec, tmp_path, previous=first.receipt, _test_transport=transport)
    assert first.artifact_path.read_bytes() == b"good" and len(list(tmp_path.iterdir())) == 1


@pytest.mark.asyncio
async def test_unearned_304_does_not_create_content_hash(tmp_path):
    async with _source_http() as (state, transport):
        state.status = 304
        with pytest.raises(RegistrySourceFetchError):
            await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_disconnect_before_headers_has_no_hidden_retry(tmp_path):
    async with _source_http() as (state, transport):
        state.abort_before_headers = True
        with pytest.raises(RegistrySourceFetchError):
            await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        assert len(state.requests) == 1
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["etag", "deleted", "changed"])
async def test_conditional_response_cannot_reuse_stale_proof(tmp_path, mode):
    async with _source_http() as (state, transport):
        state.headers = {"ETag": '"stable"'}
        first = await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        state.status = 304
        if mode == "etag":
            state.headers["ETag"] = '"different"'
        if mode == "deleted":
            state.callback = first.artifact_path.unlink
        if mode == "changed":
            state.callback = lambda: first.artifact_path.write_bytes(b"tampered")
        with pytest.raises(RegistrySourceFetchError):
            await fetch_registry_source(_spec(), tmp_path, previous=first.receipt, _test_transport=transport)
    assert len(state.requests) == 2


def test_download_hosts_and_unknown_publication_remain_explicit():
    assert _spec(source_url="https://download.cms.gov/marketplace-puf/synthetic.zip").source_system == "cms"
    assert _spec(source_url="https://downloads.cms.gov/files/synthetic.zip").source_system == "cms"
    naic = _spec(
        source_system="naic",
        source_id="company-list",
        source_url="https://content.naic.org/sites/default/files/synthetic.zip",
        reporting_year=None,
        published_at=None,
    )
    assert naic.reporting_year is None and naic.published_at is None
    with pytest.raises(RegistrySourceFetchError):
        replace(naic, source_url="https://www.cms.gov/a.zip")
    with pytest.raises(RegistrySourceFetchError):
        replace(naic, source_url="https://downloads.cms.gov/files/synthetic.zip")


@pytest.mark.parametrize(
    "system,source_id,url,maximum",
    [
        ("cms", "plan-finder", "https://downloads.cms.gov/files/synthetic.zip", PLAN_FINDER_MAX_ARTIFACT_BYTES),
        ("cms", "commercial-mlr", "https://downloads.cms.gov/files/synthetic.zip", MAX_ARTIFACT_BYTES),
        ("cms", "plan-finder-extra", "https://www.cms.gov/files/synthetic.zip", MAX_ARTIFACT_BYTES),
        ("cms", "Plan-Finder", "https://www.cms.gov/files/synthetic.zip", MAX_ARTIFACT_BYTES),
        ("naic", "plan-finder", "https://content.naic.org/files/synthetic.zip", MAX_ARTIFACT_BYTES),
        ("naic", "company-directory", "https://content.naic.org/files/synthetic.zip", MAX_ARTIFACT_BYTES),
    ],
)
def test_artifact_cap_requires_exact_source_coordinates(system, source_id, url, maximum):
    spec = _spec(source_system=system, source_id=source_id, source_url=url, max_bytes=maximum)
    assert spec.max_bytes == maximum
    with pytest.raises(RegistrySourceFetchError):
        replace(spec, max_bytes=maximum + 1)
    if (system, source_id) != ("cms", "plan-finder"):
        with pytest.raises(RegistrySourceFetchError):
            replace(spec, max_bytes=MAX_ARTIFACT_BYTES + 1)


def test_plan_finder_large_artifact_preserves_supplied_edition_provenance():
    spec = _spec(
        source_id="plan-finder",
        source_url="https://downloads.cms.gov/files/synthetic.zip",
        max_bytes=419129170,
    )
    assert spec.max_bytes > MAX_ARTIFACT_BYTES
    assert PLAN_FINDER_MAX_ARTIFACT_BYTES == 512 * 1024 * 1024
    assert spec.edition_id == "synthetic-edition" and spec.parser_version == "cms-header-v1"
    assert spec.reporting_year == 2024 and spec.published_at == datetime(2025, 9, 12, tzinfo=timezone.utc)


@pytest.mark.parametrize("maximum", [True, False, 0, -1, 1.0, "536870912", None, PLAN_FINDER_MAX_ARTIFACT_BYTES + 1])
def test_plan_finder_artifact_cap_rejects_noninteger_and_out_of_range_values(maximum):
    with pytest.raises(RegistrySourceFetchError):
        _spec(source_id="plan-finder", max_bytes=maximum)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "system,source_id,url,deadline",
    [
        ("cms", "plan-finder", "https://downloads.cms.gov/files/synthetic.zip", 120),
        ("cms", "commercial-mlr", "https://downloads.cms.gov/files/synthetic.zip", 30),
        ("cms", "plan-finder-extra", "https://www.cms.gov/files/synthetic.zip", 30),
        ("naic", "plan-finder", "https://content.naic.org/files/synthetic.zip", 30),
    ],
)
async def test_actual_fetch_applies_source_deadline_to_http_and_whole_operation(
    tmp_path, monkeypatch, system, source_id, url, deadline
):
    deadlines = []
    http_timeouts = []
    original_timeout = asyncio.timeout
    original_http_timeout = source_fetch.aiohttp.ClientTimeout

    def operation_timeout(seconds):
        deadlines.append(seconds)
        return original_timeout(seconds)

    def http_timeout(**fields):
        http_timeouts.append(fields)
        return original_http_timeout(**fields)

    monkeypatch.setattr(source_fetch.asyncio, "timeout", operation_timeout)
    monkeypatch.setattr(source_fetch.aiohttp, "ClientTimeout", http_timeout)
    spec = _spec(source_system=system, source_id=source_id, source_url=url)
    async with _source_http() as (state, transport):
        result = await fetch_registry_source(spec, tmp_path, _test_transport=transport)
    assert result.receipt.spec == spec and result.artifact_path.read_bytes() == state.body
    assert [seconds for seconds in deadlines if seconds is not None] == [deadline]
    assert http_timeouts == [{"total": deadline, "connect": 5, "sock_read": 10}]


@pytest.mark.asyncio
async def test_plan_finder_has_independent_bounded_deadline_and_preserves_old_artifact(tmp_path, monkeypatch):
    monkeypatch.setattr(source_fetch, "FETCH_DEADLINE_SECONDS", 0.01)
    monkeypatch.setattr(source_fetch, "PLAN_FINDER_FETCH_DEADLINE_SECONDS", 0.3)
    spec = _spec(source_id="plan-finder")
    async with _source_http() as (state, transport):
        state.delay = 0.04
        first = await fetch_registry_source(spec, tmp_path, _test_transport=transport)
        with pytest.raises(RegistrySourceFetchError):
            await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        monkeypatch.setattr(source_fetch, "PLAN_FINDER_FETCH_DEADLINE_SECONDS", 0.01)
        with pytest.raises(RegistrySourceFetchError):
            await fetch_registry_source(spec, tmp_path, previous=first.receipt, _test_transport=transport)
    assert first.artifact_path.read_bytes() == state.body and len(list(tmp_path.iterdir())) == 1


@pytest.mark.asyncio
async def test_deadline_cancel_and_proxy_environment_leave_no_partial(tmp_path, monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    async with _source_http() as (state, transport):
        first = await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        state.delay = 0.1
        monkeypatch.setattr(source_fetch, "FETCH_DEADLINE_SECONDS", 0.01)
        with pytest.raises(RegistrySourceFetchError):
            await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        monkeypatch.setattr(source_fetch, "FETCH_DEADLINE_SECONDS", 1)
        request_count = len(state.requests)
        task = asyncio.create_task(fetch_registry_source(_spec(), tmp_path, _test_transport=transport))
        async with asyncio.timeout(1):
            while len(state.requests) == request_count:
                await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert first.artifact_path.read_bytes() == b"synthetic artifact" and len(list(tmp_path.iterdir())) == 1


@pytest.mark.parametrize(
    "url",
    [
        "http://www.cms.gov/a.zip",
        "https://www.cms.gov.evil.test/a.zip",
        "https://cms.gov/a.zip",
        "https://user@www.cms.gov/a.zip",
        "https://www.cms.gov:443/a.zip",
        "https://www.cms.gov/a?token=x",
        "https://www.cms.gov/a#fragment",
        "https://127.0.0.1/a.zip",
        "https://www.cms.gov/../a.zip",
        "http://downloads.cms.gov/a.zip",
        "https://downloads.cms.gov.evil.test/a.zip",
        "https://downloads.cms.gov:443/a.zip",
        "https://user@downloads.cms.gov/a.zip",
    ],
)
def test_only_canonical_verified_official_origins_are_allowed(url):
    with pytest.raises(RegistrySourceFetchError):
        _spec(source_url=url)


@pytest.mark.parametrize(
    "changes",
    [
        {"source_system": "unknown"},
        {"source_id": ""},
        {"edition_id": " padded "},
        {"parser_version": "bad\nparser"},
        {"max_bytes": True},
        {"max_bytes": 0},
        {"max_bytes": MAX_ARTIFACT_BYTES + 1},
        {"reporting_year": True},
        {"published_at": datetime(2025, 1, 1)},
        {"expected_sha256": "A" * 64},
    ],
)
def test_bounds_and_explicit_provenance_are_validated(changes):
    with pytest.raises(RegistrySourceFetchError):
        _spec(**changes)


@pytest.mark.asyncio
async def test_strict_dns_ignores_global_local_override(monkeypatch):
    monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "true")

    async def resolve(resolver, host, port=0, family=0):
        return [{"host": "127.0.0.1"}]

    monkeypatch.setattr(source_fetch._PublicResolver, "resolve", resolve)
    resolver = source_fetch._StrictResolver()
    try:
        with pytest.raises(ValueError):
            await resolver.resolve("www.cms.gov", 443)
    finally:
        await resolver.close()


def _zip(input_bytes):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("MR_Submission_Template_Header.csv", input_bytes)
    return output.getvalue()


def _admission_edition(fetch_result, input_bytes):
    receipt = fetch_result.receipt
    spec = receipt.spec
    return RegistrySourceEdition(
        uuid4(),
        spec.source_system,
        spec.source_id,
        spec.edition_id,
        spec.source_url,
        receipt.artifact_sha256,
        hashlib.sha256(input_bytes).hexdigest(),
        spec.parser_version,
        spec.reporting_year,
        spec.published_at,
    )


@pytest.mark.asyncio
async def test_native_fetch_zip_admission_skip_and_header_failure_preserve_accepted(serving_schema, tmp_path):
    from tests.test_registry_source_admission_postgres import _input

    connection, schema, _ = serving_schema
    input_bytes = _input()
    async with _source_http(_zip(input_bytes)) as (state, transport):
        first = await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        with zipfile.ZipFile(first.artifact_path) as archive:
            extracted = archive.read("MR_Submission_Template_Header.csv")
        edition = _admission_edition(first, extracted)
        async with connection.transaction():
            admitted = await admit_cms_mlr_edition(connection, extracted, edition, control_schema=schema)
        replay = await fetch_registry_source(_spec(), tmp_path, previous=first.receipt, _test_transport=transport)
        assert replay.unchanged and admitted["observations"] == 2
        assert edition.artifact_sha256 != edition.input_sha256
        state.body = _zip(input_bytes.replace(b"mr_submission_template_id", b"unknown_header"))
        drift = await fetch_registry_source(_spec(), tmp_path, previous=first.receipt, _test_transport=transport)
        with zipfile.ZipFile(drift.artifact_path) as archive:
            drift_input = archive.read("MR_Submission_Template_Header.csv")
        with pytest.raises(ValueError):
            async with connection.transaction():
                await admit_cms_mlr_edition(
                    connection, drift_input, _admission_edition(drift, drift_input), control_schema=schema
                )
    assert first.artifact_path.read_bytes() != drift.artifact_path.read_bytes()
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_observation') == 2
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (0, 0)


def _block_artifact_digest(monkeypatch, target_call, *, fail_after_release=False):
    """Hold a real file hash in its worker while the owning event loop remains runnable."""
    loop = asyncio.get_running_loop()
    started, finished = asyncio.Event(), asyncio.Event()
    release = threading.Event()
    original_digest = source_fetch.sha256_file
    calls = []

    def blocked_digest(path):
        """Preserve actual file validation except for one bounded synchronization gate."""
        calls.append(threading.get_ident())
        if len(calls) != target_call:
            return original_digest(path)
        loop.call_soon_threadsafe(started.set)
        try:
            if not release.wait(2):
                raise AssertionError("file digest worker was not released")
            if fail_after_release:
                raise OSError("synthetic digest failure")
            return original_digest(path)
        finally:
            loop.call_soon_threadsafe(finished.set)

    monkeypatch.setattr(source_fetch, "sha256_file", blocked_digest)
    return started, finished, release, calls


async def _start_blocked_fetch(monkeypatch, tmp_path, state, transport, first, mode, fail_after_release=False):
    """Select cache,304 or duplicate-target verification without replacing HTTP/file validation."""
    state.conditional = mode == "not_modified"
    started, finished, release, calls = _block_artifact_digest(
        monkeypatch, 2 if state.conditional else 1, fail_after_release=fail_after_release
    )
    previous = None if mode in {"existing_artifact", "repair"} else first.receipt
    task = asyncio.create_task(fetch_registry_source(_spec(), tmp_path, previous=previous, _test_transport=transport))
    try:
        async with asyncio.timeout(1):
            await started.wait()
        assert not task.done() and calls[-1] != threading.get_ident()
        return task, finished, release
    except BaseException:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        raise


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["cache", "not_modified", "existing_artifact"])
async def test_complete_file_checks_leave_event_loop_responsive(tmp_path, monkeypatch, mode):
    """All three complete-file checks run on workers while actual local HTTP remains live."""
    async with _source_http() as (state, transport):
        state.headers = {"ETag": '"stable"'}
        first = await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        task, finished, release = await _start_blocked_fetch(monkeypatch, tmp_path, state, transport, first, mode)
        try:
            await asyncio.sleep(0)
            assert not finished.is_set() and not task.done()
            release.set()
            result = await task
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
    assert result.artifact_path == first.artifact_path and first.artifact_path.read_bytes() == state.body
    assert finished.is_set() and len(list(tmp_path.iterdir())) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["cache", "not_modified", "existing_artifact", "repair"])
@pytest.mark.parametrize("worker_failure", [False, True])
async def test_cancelled_file_checks_are_drained_without_publication(tmp_path, monkeypatch, mode, worker_failure):
    """Repeated cancellation drains successful/failing workers before returning or cleaning stages."""
    async with _source_http() as (state, transport):
        state.headers = {"ETag": '"stable"'}
        first = await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        if mode == "repair":
            first.artifact_path.write_bytes(b"x" * len(state.body))
        retained_bytes = first.artifact_path.read_bytes()
        task, finished, release = await _start_blocked_fetch(
            monkeypatch, tmp_path, state, transport, first, mode, worker_failure
        )
        try:
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done() and not finished.is_set()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
    assert finished.is_set() and first.artifact_path.read_bytes() == retained_bytes
    assert len(list(tmp_path.iterdir())) == 1
    assert len(state.requests) == (1 if mode == "cache" else 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["cache", "not_modified", "existing_artifact", "repair"])
async def test_digest_deadline_drains_worker_and_preserves_artifact(tmp_path, monkeypatch, mode):
    """The existing whole-fetch deadline fails closed without abandoning a read worker."""
    async with _source_http() as (state, transport):
        state.headers = {"ETag": '"stable"'}
        first = await fetch_registry_source(_spec(), tmp_path, _test_transport=transport)
        if mode == "repair":
            first.artifact_path.write_bytes(b"x" * len(state.body))
        retained_bytes = first.artifact_path.read_bytes()
        monkeypatch.setattr(source_fetch, "FETCH_DEADLINE_SECONDS", 0.05)
        task, finished, release = await _start_blocked_fetch(monkeypatch, tmp_path, state, transport, first, mode)
        try:
            await asyncio.sleep(0.08)
            assert not task.done() and not finished.is_set()
            release.set()
            with pytest.raises(RegistrySourceFetchError):
                await task
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
    assert finished.is_set() and first.artifact_path.read_bytes() == retained_bytes
    assert len(list(tmp_path.iterdir())) == 1
