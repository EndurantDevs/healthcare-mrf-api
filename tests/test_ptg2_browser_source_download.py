# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed browser transport checks for exact hospital sources."""

from __future__ import annotations

import asyncio
import hashlib
from types import SimpleNamespace
from typing import Any

import pytest
from curl_cffi import CurlHttpVersion, CurlOpt

from process.ptg_parts import source_download
from process.ptg_parts.domain import PTG2HeadMetadata
from process.url_security import UnsafeUrlError
from tests.hospital_price_control_support import (
    acquisition_module as _acquisition_module,
)


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


async def _download(path, *, max_bytes=100):
    return await source_download._download_raw_artifact_browser(
        url="https://www.avera.org/cms-hpt.txt",
        path=path,
        head=PTG2HeadMetadata(url="https://www.avera.org/cms-hpt.txt"),
        max_bytes=max_bytes,
        started_at=0,
        browser_profile="chrome136",
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
    locator_path.write_text("Hospital|https://www.avera.org/file.csv\n")
    raw_artifact = SimpleNamespace(
        raw_path=str(locator_path), raw_sha256="a" * 64, byte_count=48,
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
    monkeypatch.setattr(
        acquisition,
        "parse_hospital_hpt_locator",
        lambda _payload: (object(),),
    )

    locator_result = await acquisition.fetch_locator(
        (locator_url, ({"hospital_id": "a", "name": "Hospital"},)),
        store,
    )

    assert locator_result.records is not None
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
        "www.avera.org:443:8.8.8.8"
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
