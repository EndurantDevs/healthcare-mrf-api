# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed browser transport checks for exact hospital sources."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import traceback
from types import SimpleNamespace
from typing import Any

import aiohttp
import pytest
from curl_cffi import CurlHttpVersion, CurlOpt

from process import hospital_price_source_download
from process.ptg_parts import source_download
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.ptg_parts.domain import PTG2HeadMetadata
from process.url_security import UnsafeUrlError
from tests.hospital_price_control_support import (
    acquisition_module as _acquisition_module,
)
from tests.ptg2_source_download_security_support import (
    _Response as _AiohttpResponse,
)
from tests.ptg2_source_download_security_support import _Session as _AiohttpSession


class _Response:
    def __init__(self, chunks=(b"hospital",), **changes):
        self.status_code = 200
        self.url = "https://www.avera.org/cms-hpt.txt"
        self.redirect_count = 0
        self.primary_ip = "8.8.8.8"
        self.primary_port = 443
        self.http_version = CurlHttpVersion.V2_0
        self.headers = {"Content-Length": str(sum(map(len, chunks)))}
        self.chunks = chunks
        self.entered = asyncio.Event()
        self.exited = False
        self.quit_now = asyncio.Event()
        self.astream_task = None
        self.__dict__.update(changes)

    async def aiter_content(self, chunk_size=None):
        assert chunk_size is None
        self.entered.set()
        for chunk in self.chunks:
            yield chunk


class _Stream:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, *_args):
        self.response.exited = True


class _Session:
    instances = []

    def __init__(self, *, curl_options, response):
        self.curl_options = curl_options
        self.response = response
        self.request = None
        self.exited = False
        self.instances.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.exited = True

    def stream(self, method, url, **options):
        self.request = (method, url, options)
        return _Stream(self.response)


def _install_transport(monkeypatch, response):
    _Session.instances.clear()
    monkeypatch.setattr(
        source_download,
        "AsyncSession",
        lambda **options: _Session(response=response, **options),
    )

    async def resolve(_url):
        return "www.avera.org", 443, ("8.8.8.8", "2001:4860:4860::8888")

    monkeypatch.setattr(source_download, "resolve_safe_url", resolve)


async def _download(
    path,
    *,
    max_bytes=100,
    url="https://www.avera.org/cms-hpt.txt",
    browser_profile="chrome136",
    proxy_url=None,
    user_agent=None,
):
    return await source_download._download_raw_artifact_browser(
        url=url,
        path=path,
        head=PTG2HeadMetadata(url=url),
        max_bytes=max_bytes,
        started_at=0,
        browser_profile=browser_profile,
        proxy_url=proxy_url,
        user_agent=user_agent,
    )


@pytest.mark.parametrize(
    ("source_url", "browser_profile"),
    (
        ("https://www.avera.org/cms-hpt.txt", "chrome136"),
        (
            "https://www.avera.org/app/files/public/current/file.csv",
            "chrome136",
        ),
        ("http://www.avera.org/cms-hpt.txt", None),
        ("https://www.avera.org.evil/cms-hpt.txt", None),
        ("https://user@www.avera.org/cms-hpt.txt", None),
        ("https://www.avera.org:444/cms-hpt.txt", None),
        ("https://www.avera.org/other.csv", None),
    ),
)
@pytest.mark.asyncio
async def test_browser_transport_is_exactly_scoped(
    monkeypatch, source_url, browser_profile
):
    acquisition = _acquisition_module()
    attempt = acquisition.Attempt("attempt", "a", "Hospital A", source_url, 1)
    raw_artifact = SimpleNamespace(head=SimpleNamespace(url=source_url, status=200))
    requests: list[dict[str, Any]] = []

    async def download(_url, **kwargs):
        requests.append(dict(kwargs))
        return raw_artifact

    monkeypatch.setattr(acquisition, "download_raw_artifact", download)

    source_download_result = await acquisition.download_source(
        (source_url, (attempt,)), object(), 1024
    )
    assert source_download_result.raw is raw_artifact
    assert len(requests) == 1
    assert requests[0].get("browser_profile") == browser_profile
    assert ("user_agent" in requests[0]) is (browser_profile is None)


@pytest.mark.asyncio
async def test_avera_locator_uses_exact_browser_transport(tmp_path, monkeypatch):
    acquisition = _acquisition_module()
    locator_url = "https://www.avera.org/cms-hpt.txt"
    store = object()
    locator_path = tmp_path / "cms-hpt.txt"
    locator_payload = b"location-name: Hospital\nmrf-url: https://files.example/hospital.csv\n"
    locator_path.write_bytes(locator_payload)
    raw_artifact = SimpleNamespace(
        raw_path=str(locator_path), raw_sha256=hashlib.sha256(locator_payload).hexdigest(),
        byte_count=len(locator_payload),
        head=SimpleNamespace(url=locator_url, status=200),
    )
    requests: list[dict[str, Any]] = []

    async def download(_url, **kwargs):
        requests.append(dict(kwargs))
        return raw_artifact

    async def record(*_args, **_kwargs):
        return None

    monkeypatch.setattr(acquisition, "download_raw_artifact", download)
    monkeypatch.setattr(acquisition, "_record_locator_observation", record)

    locator_result = await acquisition.fetch_locator(
        (locator_url, ({"hospital_id": "a", "name": "Hospital"},)),
        store,
    )

    assert locator_result.records == (
        acquisition.HospitalHptLocatorRecord("Hospital", "https://files.example/hospital.csv"),
    )
    assert locator_result.error_code is None
    assert locator_result.error_detail is None
    assert locator_result.fetch_failed is False
    assert len(requests) == 1
    assert requests[0]["store"] is store
    assert requests[0]["browser_profile"] == "chrome136"
    assert requests[0]["exact_get_evidence"] is True
    assert "user_agent" not in requests[0]


@pytest.mark.asyncio
async def test_browser_download_retries_transient_transport_errors(
    tmp_path, monkeypatch
):
    attempts = []
    successful_download = object()

    async def download_once(**_options):
        attempts.append(1)
        if len(attempts) < 3:
            raise source_download.RequestException("connection reset")
        return successful_download

    monkeypatch.setattr(
        source_download,
        "_download_raw_artifact_browser_once",
        download_once,
    )
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 2)
    monkeypatch.setattr(source_download, "_download_retry_delay_seconds", lambda: 0)

    assert await _download(tmp_path / "artifact.part") is successful_download
    assert len(attempts) == 3


@pytest.mark.asyncio
async def test_browser_download_does_not_retry_curl_size_limit(
    tmp_path, monkeypatch
):
    attempts = []

    async def download_once(**_options):
        attempts.append(1)
        raise source_download.RequestException(
            "declared source is too large",
            code=source_download.CurlECode.FILESIZE_EXCEEDED,
        )

    monkeypatch.setattr(
        source_download,
        "_download_raw_artifact_browser_once",
        download_once,
    )
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 4)

    with pytest.raises(source_download._DownloadSizeLimitError):
        await _download(tmp_path / "artifact.part")
    assert len(attempts) == 1


@pytest.mark.asyncio
async def test_browser_download_is_pinned_streamed_and_exact(tmp_path, monkeypatch):
    response = _Response(chunks=(b"hospital", b" prices"))
    _install_transport(monkeypatch, response)
    path = tmp_path / "artifact.part"

    download_result = await _download(path)

    session = _Session.instances[0]
    assert path.read_bytes() == b"hospital prices"
    assert download_result[0].hexdigest() == hashlib.sha256(b"hospital prices").hexdigest()
    assert download_result[1:4] == (15, None, 15)
    assert download_result[5:7] == (response.url, 200)
    assert session.curl_options[CurlOpt.RESOLVE] == [
        "www.avera.org:443:8.8.8.8,[2001:4860:4860::8888]"
    ]
    assert session.curl_options[CurlOpt.NOPROXY] == "*"
    assert session.curl_options[CurlOpt.MAXFILESIZE_LARGE] == 100
    assert session.request == (
        "GET",
        response.url,
        {
            "allow_redirects": False,
            "verify": True,
            "impersonate": "chrome136",
            "quote": False,
            "accept_encoding": "identity",
            "http_version": CurlHttpVersion.V2_0,
            "timeout": (60, 600),
        },
    )
    assert response.exited and session.exited


@pytest.mark.asyncio
async def test_proxied_download_keeps_target_pin_without_direct_only_options(
    tmp_path, monkeypatch
):
    proxy_url = "http://hospital-test:test-token@10.42.0.1:39081"
    response = _Response(
        chunks=(b"PK\x03\x04",),
        primary_ip="10.42.0.1",
        primary_port=39081,
        http_version=CurlHttpVersion.V1_1,
    )
    _install_transport(monkeypatch, response)
    path = tmp_path / "artifact.part"

    await _download(
        path,
        browser_profile=None,
        proxy_url=proxy_url,
        user_agent="Hospital importer/1.0",
    )

    session = _Session.instances[0]
    assert path.read_bytes() == b"PK\x03\x04"
    assert session.curl_options[CurlOpt.RESOLVE] == [
        "www.avera.org:443:8.8.8.8,[2001:4860:4860::8888]"
    ]
    assert session.curl_options[CurlOpt.PROXY] == "http://10.42.0.1:39081"
    assert session.curl_options[CurlOpt.PROXYUSERNAME] == "hospital-test"
    assert session.curl_options[CurlOpt.PROXYPASSWORD] == "test-token"
    assert session.curl_options[CurlOpt.NOPROXY] == ""
    assert CurlOpt.CONNECT_TO not in session.curl_options
    assert "impersonate" not in session.request[2]
    assert "http_version" not in session.request[2]
    assert session.request[2]["headers"] == {
        "User-Agent": "Hospital importer/1.0"
    }


@pytest.mark.asyncio
async def test_proxied_download_preserves_the_validated_origin_hostname(
    tmp_path, monkeypatch
):
    requests = []

    async def proxy(reader, writer):
        requests.append((await reader.readline()).decode().rstrip())
        while await reader.readline() != b"\r\n":
            continue
        writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(proxy, "127.0.0.1", 0)
    proxy_port = server.sockets[0].getsockname()[1]

    async def resolve(_url):
        return "hospital.example", 443, ("8.8.8.8",)

    monkeypatch.setattr(source_download, "resolve_safe_url", resolve)
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 0)
    try:
        with pytest.raises(source_download.RequestException):
            await _download(
                tmp_path / "artifact.part",
                url="https://hospital.example/file.csv",
                browser_profile=None,
                proxy_url=(
                    f"http://hospital-test:test-token@127.0.0.1:{proxy_port}"
                ),
            )
    finally:
        server.close()
        await server.wait_closed()

    assert requests == ["CONNECT hospital.example:443 HTTP/1.1"]


@pytest.mark.asyncio
async def test_proxy_range_probe_is_bounded_and_authenticated(monkeypatch):
    url = "https://hospital.example/file.csv"
    response = _AiohttpResponse(
        status=206,
        url=url,
        headers={"Content-Range": "bytes 0-0/4", "ETag": '"stable"'},
        chunks=[b"a"],
    )
    session = _AiohttpSession(response)

    async def safe(_url):
        return None

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    monkeypatch.setenv(
        source_download.INCOMPLETE_TLS_CHAIN_HOSTS_ENV,
        "hospital.example",
    )
    monkeypatch.setattr(
        source_download,
        "_download_session_for_transport",
        lambda *_args: session,
    )

    assert await source_download._probe_http_range_support(
        url,
        user_agent="Hospital importer/1.0",
        proxy_url="http://hospital-test:test-token@127.0.0.1:39081",
    ) == (True, 4, '"stable"', url)
    assert len(session.calls) == 1
    _, _, options = session.calls[0]
    assert options["headers"] == {"Range": "bytes=0-0"}
    assert options["proxy"] == "http://127.0.0.1:39081"
    assert options["proxy_auth"].login == "hospital-test"
    assert options["proxy_auth"].password == "test-token"
    assert options["allow_redirects"] is False
    assert options["ssl"] is True
    assert response.released


@pytest.mark.parametrize(
    ("response_headers", "response_chunks"),
    [
        (
            {
                "Content-Range": "bytes 0-0/4",
                "Content-Encoding": "gzip",
            },
            [b"a"],
        ),
        ({"Content-Range": "bytes 0-0/4"}, [b"a", b"b"]),
    ],
)
@pytest.mark.asyncio
async def test_proxy_range_probe_rejects_encoded_or_oversized_body(
    monkeypatch, response_headers, response_chunks
):
    response = _AiohttpResponse(
        status=206,
        url="https://hospital.example/file.csv",
        headers=response_headers,
        chunks=response_chunks,
    )
    session = _AiohttpSession(response)

    async def safe(_url):
        return None

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    monkeypatch.setattr(
        source_download,
        "_download_session_for_transport",
        lambda *_args: session,
    )

    assert await source_download._probe_http_range_support(
        "https://hospital.example/file.csv",
        proxy_url="http://hospital-test:test-token@127.0.0.1:39081",
    ) == (False, None, None, None)
    assert response.released


@pytest.mark.asyncio
async def test_exported_proxy_range_helper_rejects_plain_http_before_network(
    tmp_path, monkeypatch
):
    def unexpected_session(*_args):
        raise AssertionError("plain HTTP must fail before proxy range I/O")

    monkeypatch.setattr(
        source_download,
        "_download_session_for_transport",
        unexpected_session,
    )
    ptg = importlib.import_module("process.ptg")

    with pytest.raises(UnsafeUrlError, match="requires HTTPS"):
        await source_download._probe_http_range_support(
            "http://hospital.example/file.csv",
            proxy_url="http://hospital-test:test-token@127.0.0.1:39081",
        )

    with pytest.raises(UnsafeUrlError, match="requires HTTPS"):
        await ptg._download_raw_artifact_ranges(
            url="http://hospital.example/file.csv",
            partial_path=tmp_path / "artifact.part",
            total_bytes=1,
            etag='"stable"',
            max_bytes=1,
            started_at=0,
            proxy_url="http://hospital-test:test-token@127.0.0.1:39081",
        )


@pytest.mark.asyncio
async def test_proxy_range_transport_preserves_origin_connect_hostname(monkeypatch):
    requests = []
    proxy_headers = []

    async def proxy(reader, writer):
        requests.append((await reader.readline()).decode().rstrip())
        while (line := await reader.readline()) != b"\r\n":
            proxy_headers.append(line.decode().rstrip())
        writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(proxy, "127.0.0.1", 0)
    proxy_port = server.sockets[0].getsockname()[1]

    async def safe(_url):
        return None

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    try:
        assert not (await source_download._probe_http_range_support(
            "https://hospital.example/file.csv",
            proxy_url=(
                f"http://hospital-test:test-token@127.0.0.1:{proxy_port}"
            ),
        ))[0]
    finally:
        server.close()
        await server.wait_closed()

    assert requests == ["CONNECT hospital.example:443 HTTP/1.1"]
    assert any(
        header.lower().startswith("proxy-authorization: basic ")
        for header in proxy_headers
    )


@pytest.mark.asyncio
async def test_proxy_ranges_resume_failed_chunks_with_stable_etag(
    tmp_path, monkeypatch
):
    url = "https://hospital.example/file.csv"
    artifact_bytes = b"abcd"
    attempts_by_range = {}

    def respond(_method, _url, options):
        byte_range = options["headers"]["Range"]
        start, end = map(int, byte_range.removeprefix("bytes=").split("-"))
        attempts_by_range[byte_range] = attempts_by_range.get(byte_range, 0) + 1
        chunk = artifact_bytes[start : end + 1]
        if byte_range == "bytes=2-3" and attempts_by_range[byte_range] == 1:
            chunk = chunk[:1]
        return _AiohttpResponse(
            status=206,
            url=url,
            headers={
                "Content-Range": f"bytes {start}-{end}/{len(artifact_bytes)}",
                "ETag": '"stable"',
            },
            chunks=[chunk],
        )

    session = _AiohttpSession(respond)

    async def safe(_url):
        return None

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    monkeypatch.setattr(
        source_download,
        "_download_session_for_transport",
        lambda *_args: session,
    )
    monkeypatch.setattr(source_download, "_range_download_chunk_bytes", lambda: 2)
    monkeypatch.setattr(source_download, "_range_download_tasks", lambda: 1)
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 1)
    monkeypatch.setattr(source_download, "_download_retry_delay_seconds", lambda: 0)
    path = tmp_path / "artifact.part"

    await source_download._download_raw_artifact_ranges(
        url=url,
        partial_path=path,
        total_bytes=len(artifact_bytes),
        etag='"stable"',
        max_bytes=len(artifact_bytes),
        started_at=0,
        proxy_url="http://hospital-test:test-token@127.0.0.1:39081",
    )

    assert path.read_bytes() == artifact_bytes
    assert attempts_by_range == {"bytes=0-1": 1, "bytes=2-3": 2}
    assert all(
        options["headers"]["If-Match"] == '"stable"'
        for _, _, options in session.calls
    )
    assert all(options["ssl"] is True for _, _, options in session.calls)
    assert not source_download._range_sidecar_path(path).exists()


@pytest.mark.parametrize(
    ("response_headers", "response_chunks", "expected_error"),
    [
        (
            {
                "Content-Range": "bytes 0-0/1",
                "Content-Encoding": "gzip",
                "ETag": '"stable"',
            },
            [b"a"],
            "returned encoded content",
        ),
        (
            {"Content-Range": "bytes 0-0/1", "ETag": '"stable"'},
            [b"ab"],
            "exceeded bytes 0-0",
        ),
    ],
)
@pytest.mark.asyncio
async def test_proxy_ranges_reject_encoded_or_oversized_chunks(
    tmp_path,
    monkeypatch,
    response_headers,
    response_chunks,
    expected_error,
):
    url = "https://hospital.example/file.csv"
    session = _AiohttpSession(
        _AiohttpResponse(
            status=206,
            url=url,
            headers=response_headers,
            chunks=response_chunks,
        )
    )

    async def safe(_url):
        return None

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    monkeypatch.setattr(
        source_download,
        "_download_session_for_transport",
        lambda *_args: session,
    )
    monkeypatch.setattr(source_download, "_range_download_chunk_bytes", lambda: 1)
    monkeypatch.setattr(source_download, "_range_download_tasks", lambda: 1)
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 0)

    with pytest.raises(
        source_download._UnsafeRangeResponseError,
        match=expected_error,
    ):
        await source_download._download_raw_artifact_ranges(
            url=url,
            partial_path=tmp_path / "artifact.part",
            total_bytes=1,
            etag='"stable"',
            max_bytes=1,
            started_at=0,
            proxy_url="http://hospital-test:test-token@127.0.0.1:39081",
        )


@pytest.mark.asyncio
async def test_proxy_range_timeout_after_body_is_preserved_over_direct_403(
    tmp_path, monkeypatch
):
    url = "https://hospital.example/file.csv"
    response = _AiohttpResponse(
        status=206,
        url=url,
        headers={
            "Content-Range": "bytes 0-1/2",
            "ETag": '"stable"',
        },
    )

    class PartialTimeoutContent:
        async def iter_chunked(self, _size):
            yield b"a"
            raise aiohttp.ServerTimeoutError("range timed out")

    response.content = PartialTimeoutContent()
    session = _AiohttpSession(response)

    async def safe(_url):
        return None

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    monkeypatch.setattr(
        source_download,
        "_download_session_for_transport",
        lambda *_args: session,
    )
    monkeypatch.setattr(source_download, "_range_download_chunk_bytes", lambda: 2)
    monkeypatch.setattr(source_download, "_range_download_tasks", lambda: 1)
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 0)

    with pytest.raises(aiohttp.ServerTimeoutError) as failure:
        await source_download._download_raw_artifact_ranges(
            url=url,
            partial_path=tmp_path / "artifact.part",
            total_bytes=2,
            etag='"stable"',
            max_bytes=2,
            started_at=0,
            proxy_url="http://hospital-test:test-token@127.0.0.1:39081",
        )

    assert failure.value._ptg2_response_body_started is True
    assert not source_download.is_prebody_connect_or_timeout(failure.value)


@pytest.mark.asyncio
async def test_hospital_proxy_keeps_postbody_range_failure_over_direct_403(
    monkeypatch,
):
    url = "https://hospital.example/file.csv"
    proxy_failure = aiohttp.ServerTimeoutError("range timed out")
    proxy_failure._ptg2_response_body_started = True

    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_PROXY",
        "http://hospital-test:test-token@127.0.0.1:39081",
    )
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_HOSTS",
        "hospital.example",
    )
    direct_error = RuntimeError("blocked outside the US")
    direct_error.status = 403
    direct_error._ptg2_response_body_started = False

    async def direct_download(*_args, **_options):
        raise direct_error

    async def proxy_download(*_args, **_options):
        raise proxy_failure

    monkeypatch.setattr(
        hospital_price_source_download,
        "download_raw_artifact_via_proxy",
        proxy_download,
    )

    with pytest.raises(aiohttp.ServerTimeoutError) as hospital_failure:
        await hospital_price_source_download.download_hospital_source(
            direct_download,
            url,
            object(),
            2,
            "Hospital importer/1.0",
        )
    assert hospital_failure.value is proxy_failure


@pytest.mark.asyncio
async def test_proxy_range_malformed_zip_preserves_body_marker(
    tmp_path, monkeypatch
):
    url = "https://hospital.example/file.zip"
    artifact_bytes = b"not a zip"

    def respond(_method, _url, options):
        start, end = map(
            int,
            options["headers"]["Range"].removeprefix("bytes=").split("-"),
        )
        return _AiohttpResponse(
            status=206,
            url=url,
            headers={
                "Content-Range": f"bytes {start}-{end}/{len(artifact_bytes)}",
                "ETag": '"stable"',
            },
            chunks=[artifact_bytes[start : end + 1]],
        )

    session = _AiohttpSession(respond)

    async def safe(_url):
        return None

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    monkeypatch.setattr(
        source_download,
        "_download_session_for_transport",
        lambda *_args: session,
    )
    monkeypatch.setattr(source_download, "_range_download_min_bytes", lambda: 1)
    monkeypatch.setattr(
        source_download,
        "_range_download_chunk_bytes",
        lambda: len(artifact_bytes),
    )

    with pytest.raises(
        source_download._UnexpectedArtifactContainerError,
        match="not a readable ZIP container",
    ) as failure:
        await source_download.download_raw_artifact_via_proxy(
            url,
            proxy_url="http://hospital-test:test-token@127.0.0.1:39081",
            store=PTG2ArtifactStore(tmp_path / "store"),
            max_bytes=len(artifact_bytes),
        )

    assert failure.value._ptg2_response_body_started is True


def test_proxy_ranges_require_the_observed_etag_on_every_chunk():
    response = SimpleNamespace(
        status=206,
        headers={"Content-Range": "bytes 0-0/1"},
    )
    with pytest.raises(source_download._UnsafeRangeResponseError, match="changed ETag"):
        source_download._validate_range_response(
            response,
            url="https://hospital.example/file.csv",
            expected_start=0,
            expected_end=0,
            expected_total=1,
            expected_etag='"stable"',
            require_response_etag=True,
        )


@pytest.mark.parametrize(
    "code",
    [
        source_download.CurlECode.COULDNT_CONNECT,
        source_download.CurlECode.OPERATION_TIMEDOUT,
    ],
)
@pytest.mark.asyncio
async def test_proxy_retries_prebody_connect_and_timeout(
    tmp_path, monkeypatch, code
):
    attempts = []
    successful_download = object()

    async def download_once(**_options):
        attempts.append(1)
        if len(attempts) == 1:
            error = source_download.RequestException("transient", code=code)
            error._ptg2_response_body_started = False
            raise error
        return successful_download

    monkeypatch.setattr(
        source_download,
        "_download_raw_artifact_browser_once",
        download_once,
    )
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 2)
    monkeypatch.setattr(source_download, "_download_retry_delay_seconds", lambda: 0)

    assert await _download(
        tmp_path / "artifact.part",
        browser_profile=None,
        proxy_url="http://hospital-test:test-token@10.42.0.1:39081",
    ) is successful_download
    assert len(attempts) == 2


@pytest.mark.parametrize(
    ("code", "body_started"),
    [
        (source_download.CurlECode.PEER_FAILED_VERIFICATION, False),
        (source_download.CurlECode.RECV_ERROR, True),
        (source_download.CurlECode.OPERATION_TIMEDOUT, True),
    ],
)
@pytest.mark.asyncio
async def test_proxy_does_not_retry_policy_or_midbody_failures(
    tmp_path, monkeypatch, code, body_started
):
    attempts = []

    async def download_once(**_options):
        attempts.append(1)
        error = source_download.RequestException("terminal", code=code)
        error._ptg2_response_body_started = body_started
        error._ptg2_downloaded_byte_count = int(body_started)
        raise error

    monkeypatch.setattr(
        source_download,
        "_download_raw_artifact_browser_once",
        download_once,
    )
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 2)

    with pytest.raises(source_download.RequestException):
        await _download(
            tmp_path / "artifact.part",
            browser_profile=None,
            proxy_url="http://hospital-test:test-token@10.42.0.1:39081",
        )
    assert len(attempts) == 1


def test_proxy_transport_formats_ipv6_and_rejects_invalid_port():
    options = source_download._curl_transport_option_map(
        "www.avera.org",
        443,
        ("8.8.8.8",),
        None,
        "http://hospital-test:test-token@[2001:db8::1]:39081",
    )
    assert options[CurlOpt.PROXY] == "http://[2001:db8::1]:39081"
    proxy_options = source_download._proxy_request_kwargs(
        "http://hospital-test:test-token@[2001:db8::1]:39081"
    )
    assert proxy_options["proxy"] == "http://[2001:db8::1]:39081"

    with pytest.raises(RuntimeError, match="proxy URL is invalid"):
        source_download.validated_http_proxy_url(
            "http://hospital-test:test-token@10.42.0.1:not-a-port"
        )

    credential_marker = "must-not-leak"
    with pytest.raises(RuntimeError, match="proxy URL is invalid") as failure:
        source_download.validated_http_proxy_url(
            f"http://hospital-test:{credential_marker}@ho／st:39081"
        )
    assert credential_marker not in str(failure.value)
    assert credential_marker not in "".join(
        traceback.format_exception(failure.value)
    )


@pytest.mark.asyncio
async def test_proxied_download_rejects_unsupported_http_version(
    tmp_path, monkeypatch
):
    response = _Response(http_version=CurlHttpVersion.V3)
    _install_transport(monkeypatch, response)

    with pytest.raises(RuntimeError, match="HTTP/1.1 or HTTP/2"):
        await _download(
            tmp_path / "artifact.part",
            browser_profile=None,
            proxy_url="http://hospital-test:test-token@10.42.0.1:39081",
        )


@pytest.mark.asyncio
async def test_curl_transport_rejects_plain_http(tmp_path):
    with pytest.raises(UnsafeUrlError, match="requires HTTPS"):
        await _download(
            tmp_path / "artifact.part",
            url="http://www.avera.org/cms-hpt.txt",
        )


@pytest.mark.asyncio
async def test_proxy_transport_skips_head_and_forces_uncached_download(monkeypatch):
    request_options = []

    async def request(_url, **options):
        request_options.append(options)
        return object()

    async def unexpected_head(*_args, **_options):
        raise AssertionError("proxied transport must not issue HEAD")

    monkeypatch.setattr(source_download, "_download_raw_request", request)
    monkeypatch.setattr(source_download, "fetch_head_metadata", unexpected_head)
    proxy_url = "http://hospital-test:test-token@10.42.0.1:39081"

    head = await source_download._download_head_metadata(
        "https://www.avera.org/cms-hpt.txt",
        source_download._DownloadTransport(proxy_url=proxy_url),
    )
    await source_download.download_raw_artifact_via_proxy(
        "https://www.avera.org/cms-hpt.txt",
        proxy_url=proxy_url,
        store=object(),
        max_bytes=100,
    )

    assert head.supports_head is False
    assert request_options[0]["reuse_raw_artifacts"] is False
    assert request_options[0]["keep_partial_artifacts"] is False
    assert request_options[0]["transport"].proxy_url == proxy_url


@pytest.mark.asyncio
async def test_proxy_nonrange_fallback_keeps_the_single_get_cap(
    tmp_path, monkeypatch
):
    browser_requests = []

    async def no_ranges(*_args, **_options):
        return None

    async def browser_download(**options):
        browser_requests.append(options)
        return object()

    monkeypatch.setattr(
        source_download,
        "_try_ranged_raw_artifact",
        no_ranges,
    )
    monkeypatch.setattr(
        source_download,
        "_download_raw_artifact_browser",
        browser_download,
    )

    downloaded_artifact = await source_download._download_raw_to_path(
        "https://hospital.example/file.csv",
        tmp_path / "artifact.part",
        head=PTG2HeadMetadata(url="https://hospital.example/file.csv"),
        max_bytes=1000,
        started_at=0,
        exact_get_evidence=False,
        transport=source_download._DownloadTransport(
            proxy_url="http://hospital-test:test-token@127.0.0.1:39081",
            proxy_single_get_max_bytes=100,
        ),
        failure_digest=hashlib.sha256(),
    )

    assert downloaded_artifact is not None
    assert browser_requests[0]["max_bytes"] == 100


@pytest.mark.asyncio
async def test_hospital_download_falls_back_once_to_configured_us_proxy(
    monkeypatch,
):
    proxy_url = "http://hospital-test:test-token@10.42.0.1:39081"
    monkeypatch.setenv("HLTHPRT_HOSPITAL_PRICE_US_EGRESS_PROXY", proxy_url)
    monkeypatch.setenv("HLTHPRT_HOSPITAL_PRICE_US_EGRESS_HOSTS", "cdn.hs.uab.edu")
    requests = []
    downloaded = object()
    max_bytes = 1024 * 1024**2

    async def download(_url, **options):
        requests.append(options)
        error = aiohttp.ServerTimeoutError("direct route timed out")
        error._ptg2_response_body_started = False
        raise error

    async def proxy_download(_url, **options):
        requests.append(options)
        return downloaded

    monkeypatch.setattr(
        hospital_price_source_download,
        "download_raw_artifact_via_proxy",
        proxy_download,
    )

    assert await hospital_price_source_download.download_hospital_source(
        download,
        "https://cdn.hs.uab.edu/static/hospital.zip",
        object(),
        max_bytes,
        "Mozilla/5.0",
    ) is downloaded
    assert len(requests) == 2
    assert "proxy_url" not in requests[0]
    assert requests[1]["proxy_url"] == proxy_url
    assert requests[1]["max_bytes"] == max_bytes
    assert requests[1]["single_get_max_bytes"] == 512 * 1024**2


@pytest.mark.parametrize(
    "direct_error",
    [
        UnsafeUrlError("unsafe source"),
        source_download._DownloadSizeLimitError("too large"),
        PermissionError("local write denied"),
        aiohttp.ClientConnectorSSLError(None, OSError("TLS failed")),
        source_download.RequestException(
            "certificate rejected",
            code=source_download.CurlECode.PEER_FAILED_VERIFICATION,
        ),
    ],
)
@pytest.mark.asyncio
async def test_hospital_proxy_rejects_prebody_nontransport_failures(
    monkeypatch, direct_error
):
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_PROXY",
        "http://hospital-test:test-token@10.42.0.1:39081",
    )
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_HOSTS",
        "hospital.example",
    )
    direct_error._ptg2_response_body_started = False
    proxy_calls = []

    async def direct_download(*_args, **_options):
        raise direct_error

    async def proxy_download(*_args, **_options):
        proxy_calls.append(1)

    monkeypatch.setattr(
        hospital_price_source_download,
        "download_raw_artifact_via_proxy",
        proxy_download,
    )

    with pytest.raises(type(direct_error)) as failure:
        await hospital_price_source_download.download_hospital_source(
            direct_download,
            "https://hospital.example/file.csv",
            object(),
            1024,
            "Mozilla/5.0",
        )
    assert failure.value is direct_error
    assert proxy_calls == []


@pytest.mark.parametrize(
    "direct_error",
    [
        aiohttp.ClientConnectorError(None, OSError("connect failed")),
        aiohttp.ServerTimeoutError("timed out"),
        source_download.RequestException(
            "connect failed", code=source_download.CurlECode.COULDNT_CONNECT
        ),
        source_download.RequestException(
            "timed out", code=source_download.CurlECode.OPERATION_TIMEDOUT
        ),
    ],
)
@pytest.mark.asyncio
async def test_hospital_proxy_accepts_prebody_connect_and_timeout(
    monkeypatch, direct_error
):
    proxy_url = "http://hospital-test:test-token@10.42.0.1:39081"
    monkeypatch.setenv("HLTHPRT_HOSPITAL_PRICE_US_EGRESS_PROXY", proxy_url)
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_HOSTS",
        "hospital.example",
    )
    direct_error._ptg2_response_body_started = False
    proxy_calls = []
    downloaded = object()

    async def direct_download(*_args, **_options):
        raise direct_error

    async def proxy_download(*_args, **_options):
        proxy_calls.append(1)
        return downloaded

    monkeypatch.setattr(
        hospital_price_source_download,
        "download_raw_artifact_via_proxy",
        proxy_download,
    )

    assert await hospital_price_source_download.download_hospital_source(
        direct_download,
        "https://hospital.example/file.csv",
        object(),
        1024,
        "Mozilla/5.0",
    ) is downloaded
    assert proxy_calls == [1]


@pytest.mark.asyncio
async def test_hospital_proxy_rejects_invalid_host_allowlist(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_PROXY",
        "http://hospital-test:test-token@10.42.0.1:39081",
    )
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_HOSTS",
        "cdn.hs.uab.edu/path",
    )

    with pytest.raises(RuntimeError, match="must contain exact hostnames"):
        await hospital_price_source_download.download_hospital_source(
            None,
            "https://cdn.hs.uab.edu/hospital.zip",
            object(),
            1024,
            "Mozilla/5.0",
        )


@pytest.mark.asyncio
async def test_hospital_proxy_propagates_cancellation(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_PROXY",
        "http://hospital-test:test-token@10.42.0.1:39081",
    )
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_HOSTS",
        "cdn.hs.uab.edu",
    )
    direct_error = aiohttp.ServerTimeoutError("direct route timed out")
    direct_error._ptg2_response_body_started = False

    async def direct_download(*_args, **_options):
        raise direct_error

    async def cancelled_proxy(*_args, **_options):
        raise asyncio.CancelledError

    monkeypatch.setattr(
        hospital_price_source_download,
        "download_raw_artifact_via_proxy",
        cancelled_proxy,
    )

    with pytest.raises(asyncio.CancelledError):
        await hospital_price_source_download.download_hospital_source(
            direct_download,
            "https://cdn.hs.uab.edu/hospital.zip",
            object(),
            1024,
            "Mozilla/5.0",
        )


@pytest.mark.asyncio
async def test_hospital_proxy_requires_the_terminal_direct_failure_to_be_prebody(
    monkeypatch,
):
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_PROXY",
        "http://hospital-test:test-token@10.42.0.1:39081",
    )
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_HOSTS",
        "hospital.example",
    )
    requests = []

    async def download(_url, **options):
        requests.append(options)
        error = RuntimeError("direct failure")
        error.status = 403 if len(requests) == 1 else 500
        error._ptg2_response_body_started = len(requests) > 1
        raise error

    with pytest.raises(RuntimeError, match="direct failure") as failure:
        await hospital_price_source_download.download_hospital_source(
            download,
            "https://hospital.example/cms-hpt.txt",
            object(),
            1024,
            "Mozilla/5.0",
        )
    assert len(requests) == 2
    assert failure.value.status == 403


@pytest.mark.parametrize(
    ("proxy_code", "proxy_body_started", "expected_message"),
    [
        (
            source_download.CurlECode.COULDNT_CONNECT,
            False,
            "blocked outside the US",
        ),
        (
            source_download.CurlECode.COULDNT_CONNECT,
            True,
            "proxy unavailable",
        ),
        (
            source_download.CurlECode.PEER_FAILED_VERIFICATION,
            False,
            "proxy unavailable",
        ),
    ],
)
@pytest.mark.asyncio
async def test_hospital_proxy_preserves_only_a_prebody_direct_403(
    monkeypatch, proxy_code, proxy_body_started, expected_message
):
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_PROXY",
        "http://hospital-test:test-token@10.42.0.1:39081",
    )
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_HOSTS",
        "hospital.example",
    )
    direct_error = RuntimeError("blocked outside the US")
    direct_error.status = 403
    direct_error._ptg2_response_body_started = False

    async def direct_download(*_args, **_options):
        raise direct_error

    async def proxy_download(*_args, **_options):
        error = source_download.RequestException(
            "proxy unavailable", code=proxy_code
        )
        error._ptg2_response_body_started = proxy_body_started
        raise error

    monkeypatch.setattr(
        hospital_price_source_download,
        "download_raw_artifact_via_proxy",
        proxy_download,
    )

    with pytest.raises(Exception, match=expected_message) as failure:
        await hospital_price_source_download.download_hospital_source(
            direct_download,
            "https://hospital.example/cms-hpt.txt",
            object(),
            1024,
            "Mozilla/5.0",
        )
    assert getattr(failure.value, "status", None) == (
        403
        if proxy_code == source_download.CurlECode.COULDNT_CONNECT
        and not proxy_body_started
        else None
    )


@pytest.mark.asyncio
async def test_hospital_proxy_does_not_route_an_unapproved_host(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_PROXY",
        "http://hospital-test:test-token@10.42.0.1:39081",
    )
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_HOSTS",
        "cdn.hs.uab.edu",
    )
    direct_error = TimeoutError("direct route timed out")
    direct_error._ptg2_response_body_started = False
    proxy_calls = []

    async def direct_download(*_args, **_options):
        raise direct_error

    async def proxy_download(*_args, **_options):
        proxy_calls.append(1)

    monkeypatch.setattr(
        hospital_price_source_download,
        "download_raw_artifact_via_proxy",
        proxy_download,
    )

    with pytest.raises(TimeoutError, match="direct route timed out"):
        await hospital_price_source_download.download_hospital_source(
            direct_download,
            "https://unapproved.example/file.zip",
            object(),
            1024,
            "Mozilla/5.0",
        )
    assert proxy_calls == []


@pytest.mark.parametrize(
    "proxy_url",
    [
        "socks5://hospital-test:test-token@10.42.0.1:39081",
        "http://10.42.0.1:39081",
        "http://user@10.42.0.1:39081",
        "http://user:bad%2Ftoken@10.42.0.1:39081",
        "http://user:token@10.42.0.1:39081/path",
    ],
)
@pytest.mark.asyncio
async def test_hospital_download_rejects_unsafe_proxy_configuration(
    monkeypatch, proxy_url
):
    monkeypatch.setenv("HLTHPRT_HOSPITAL_PRICE_US_EGRESS_PROXY", proxy_url)

    with pytest.raises(RuntimeError, match="must be an authenticated HTTP URL"):
        await hospital_price_source_download.download_hospital_source(
            None,
            "https://hospital.example/cms-hpt.txt",
            object(),
            1024,
            "Mozilla/5.0",
        )


@pytest.mark.asyncio
async def test_browser_download_accepts_equivalent_root_url_and_validated_ipv6(
    tmp_path, monkeypatch
):
    response = _Response(
        url="https://www.avera.org/",
        primary_ip="2001:4860:4860::8888",
    )
    _install_transport(monkeypatch, response)

    await _download(
        tmp_path / "artifact.part",
        url="https://www.avera.org",
    )

    assert response.exited and _Session.instances[0].exited


@pytest.mark.parametrize(
    ("changes", "error"),
    (
        ({"status_code": 403}, source_download._BrowserDownloadStatusError),
        ({"redirect_count": 1}, UnsafeUrlError),
        ({"url": "https://www.avera.org/other"}, UnsafeUrlError),
        ({"primary_ip": "1.1.1.1"}, UnsafeUrlError),
        ({"primary_port": 444}, UnsafeUrlError),
        ({"http_version": CurlHttpVersion.V1_1}, RuntimeError),
    ),
)
@pytest.mark.asyncio
async def test_browser_download_rejects_unproven_response(
    tmp_path, monkeypatch, changes, error
):
    async def stalled_transfer():
        await asyncio.Event().wait()

    stream_task = asyncio.create_task(stalled_transfer())
    response = _Response(astream_task=stream_task, **changes)
    _install_transport(monkeypatch, response)
    path = tmp_path / "artifact.part"

    with pytest.raises(error):
        await _download(path)

    assert not path.exists()
    assert response.quit_now.is_set()
    assert stream_task.cancelled()
    assert response.astream_task is None
    assert len(_Session.instances) == 1
    assert response.exited and _Session.instances[0].exited


@pytest.mark.asyncio
async def test_browser_download_removes_overflow_and_cancelled_stage(
    tmp_path, monkeypatch
):
    overflow = _Response(chunks=(b"too-large",), headers={})
    _install_transport(monkeypatch, overflow)
    overflow_path = tmp_path / "overflow.part"
    with pytest.raises(source_download._DownloadSizeLimitError):
        await _download(overflow_path, max_bytes=3)
    assert not overflow_path.exists()
    assert overflow.quit_now.is_set()

    release = asyncio.Event()

    async def blocked_chunks(_chunk_size=None):
        cancelled.entered.set()
        await release.wait()
        yield b"late"

    cancelled = _Response(chunks=())
    cancelled.aiter_content = blocked_chunks
    _install_transport(monkeypatch, cancelled)
    cancelled_path = tmp_path / "cancelled.part"
    task = asyncio.create_task(_download(cancelled_path))
    await asyncio.wait_for(cancelled.entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not cancelled_path.exists()
    assert cancelled.quit_now.is_set()
    assert cancelled.exited and _Session.instances[0].exited
