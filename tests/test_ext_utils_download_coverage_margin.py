# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pytest
from arq import Retry

from process.ext import utils
from tests.ext_utils_import_coverage_support import _Client, _Request, _Response

@pytest.mark.parametrize(
    ("raw_value", "default_bytes", "expected"),
    [
        ("", 17, 17),
        ("7", 17, 7 * 1024 * 1024),
        ("1024", 17, 1024),
        ("2kb", 17, 2 * 1024),
        ("3 m", 17, 3 * 1024 * 1024),
        ("4GB", 17, 4 * 1024 * 1024 * 1024),
        ("5t", 17, 5 * 1024 * 1024 * 1024 * 1024),
        ("invalid", 17, 17),
    ],
)
def test_download_size_and_progress_contracts(raw_value, default_bytes, expected):
    assert utils._parse_size_bytes(raw_value, default_bytes) == expected

    bounded = utils._render_progress_line(5, 10, 2.0)
    unbounded = utils._render_progress_line(5, None, 2.0)

    assert "50.0%" in bounded
    assert " / " in bounded
    assert "50.0%" not in unbounded


def test_timeout_estimates_are_bounded(monkeypatch):
    assert utils._estimate_timeout_seconds(None, None) is None
    assert utils._estimate_timeout_seconds(0, None) is None

    monkeypatch.setattr(utils, "MIN_STREAM_TIMEOUT", 10.0)
    monkeypatch.setattr(utils, "MAX_STREAM_TIMEOUT", 30.0)
    monkeypatch.setattr(utils, "SECONDS_PER_MEGABYTE", 1.0)
    monkeypatch.setattr(utils, "DOWNLOAD_TIMEOUT_MULTIPLIER", 1.0)

    assert utils._estimate_timeout_seconds(1024 * 1024, None) == 10.0
    assert utils._estimate_timeout_seconds(100 * 1024 * 1024, 1024) == 30.0


@pytest.mark.asyncio
async def test_head_probe_and_request_timeout_fallbacks(monkeypatch):
    ranged = _Response(
        status=206,
        headers={"Content-Range": "bytes 0-0/4096"},
        body=b"x",
    )
    client = _Client(
        head_requests=[
            _Request(
                _Response(
                    headers={
                        "Content-Length": "1",
                        "Accept-Ranges": "bytes",
                    }
                )
            ),
            _Request(error=aiohttp.ClientError("head failed")),
        ],
        get_requests=[
            _Request(ranged),
            _Request(
                _Response(
                    status=206,
                    headers={"Content-Range": "bytes 0-0/*"},
                    body=b"x",
                )
            ),
        ],
    )

    assert await utils._head_download_info(client, "https://example.test/a") == (
        4096,
        True,
    )
    assert await utils._head_download_info(client, "https://example.test/b") == (
        None,
        False,
    )

    monkeypatch.setattr(
        utils,
        "_head_download_info",
        AsyncMock(side_effect=[(4096, True), (None, False)]),
    )
    monkeypatch.setattr(utils, "_estimate_timeout_seconds", lambda *_args: 321.0)
    sized = await utils._determine_request_timeout(
        client,
        "https://example.test/sized",
        1024,
    )
    fallback = await utils._determine_request_timeout(
        client,
        "https://example.test/unknown",
        None,
    )

    assert sized.total == 321.0
    assert sized.sock_read == 321.0
    assert fallback.total >= utils.MIN_STREAM_TIMEOUT


@pytest.mark.asyncio
async def test_head_probe_disabled_and_zero_estimate_fallback(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PARALLEL_DOWNLOAD_DISABLED_HOSTS",
        "blocked.test",
    )
    disabled_client = _Client(
        head_requests=[
            _Request(
                _Response(
                    headers={
                        "Content-Length": "invalid",
                        "Accept-Ranges": "bytes",
                    }
                )
            )
        ]
    )
    assert await utils._head_download_info(
        disabled_client,
        "https://blocked.test/file",
    ) == (None, False)

    monkeypatch.setattr(
        utils,
        "_head_download_info",
        AsyncMock(return_value=(1, False)),
    )
    monkeypatch.setattr(utils, "_estimate_timeout_seconds", lambda *_args: None)
    fallback = await utils._determine_request_timeout(
        disabled_client,
        "https://blocked.test/file",
        None,
    )
    assert fallback.total >= utils.MIN_STREAM_TIMEOUT


def test_parallel_download_host_matching(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PARALLEL_DOWNLOAD_DISABLED_HOSTS",
        "blocked.test,.example.test",
    )
    assert utils._is_parallel_download_disabled_for_url(
        "https://blocked.test/file"
    )
    assert utils._is_parallel_download_disabled_for_url(
        "https://child.example.test/file"
    )
    assert not utils._is_parallel_download_disabled_for_url(
        "https://other.test/file"
    )
    monkeypatch.setenv("HLTHPRT_PARALLEL_DOWNLOAD_DISABLED_HOSTS", "")
    assert not utils._is_parallel_download_disabled_for_url(
        "https://blocked.test/file"
    )


@pytest.mark.asyncio
async def test_parallel_range_download_success_and_refusal(tmp_path, monkeypatch):
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_RANGE_SIZE", 2)
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_WORKERS", 2)
    monkeypatch.setattr(utils, "PARALLEL_CHUNK_RETRIES", 1)
    monkeypatch.setattr(utils, "PROGRESS_INTERVAL_SECONDS", 0.0)

    destination_path = tmp_path / "ranged.bin"
    client = _Client(
        get_requests=[
            _Request(_Response(status=206, body=b"ab")),
            _Request(_Response(status=206, body=b"cd")),
        ]
    )
    await utils._download_parallel_by_ranges(
        client,
        "https://example.test/ranged",
        str(destination_path),
        4,
        aiohttp.ClientTimeout(total=10, sock_read=10),
    )
    assert destination_path.read_bytes() == b"abcd"

    refused = _Client(
        get_requests=[_Request(_Response(status=200, body=b"ab"))]
    )
    with pytest.raises(RuntimeError, match="Failed to download range"):
        await utils._download_parallel_by_ranges(
            refused,
            "https://example.test/refused",
            str(tmp_path / "refused.bin"),
            2,
            aiohttp.ClientTimeout(total=10, sock_read=10),
        )

    mismatched = _Client(
        get_requests=[_Request(_Response(status=206, body=b"a"))]
    )
    with pytest.raises(RuntimeError, match="payload mismatch"):
        await utils._download_parallel_by_ranges(
            mismatched,
            "https://example.test/mismatched",
            str(tmp_path / "mismatched.bin"),
            2,
            aiohttp.ClientTimeout(total=10, sock_read=10),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("size_bytes", [513, 1025])
async def test_parallel_range_download_large_range_tuning(
    size_bytes,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_RANGE_SIZE", 1)
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_WORKERS", 1)
    monkeypatch.setattr(utils, "PARALLEL_CHUNK_RETRIES", 1)
    monkeypatch.setattr(utils, "PROGRESS_INTERVAL_SECONDS", 0.0)
    payload = b"x" * size_bytes
    client = _Client(
        get_requests=[_Request(_Response(status=206, body=payload))]
    )

    destination_path = tmp_path / f"tuned-{size_bytes}.bin"
    await utils._download_parallel_by_ranges(
        client,
        "https://example.test/tuned",
        str(destination_path),
        size_bytes,
        aiohttp.ClientTimeout(total=10, sock_read=10),
    )

    assert destination_path.read_bytes() == payload
    assert client.calls[0][2]["headers"]["Range"] == f"bytes=0-{size_bytes - 1}"


@pytest.mark.asyncio
async def test_parallel_range_download_retries_one_chunk(tmp_path, monkeypatch):
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_RANGE_SIZE", 2)
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_WORKERS", 1)
    monkeypatch.setattr(utils, "PARALLEL_CHUNK_RETRIES", 2)
    monkeypatch.setattr(utils, "PARALLEL_CHUNK_BACKOFF_SECONDS", 0.0)
    monkeypatch.setattr(utils, "PROGRESS_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(utils.asyncio, "sleep", AsyncMock())
    client = _Client(
        get_requests=[
            _Request(error=aiohttp.ClientError("temporary reset")),
            _Request(_Response(status=206, body=b"ok")),
        ]
    )

    destination_path = tmp_path / "retried.bin"
    await utils._download_parallel_by_ranges(
        client,
        "https://example.test/retried",
        str(destination_path),
        2,
        aiohttp.ClientTimeout(total=10, sock_read=10),
    )

    assert destination_path.read_bytes() == b"ok"
    utils.asyncio.sleep.assert_awaited_once_with(0.0)


@pytest.mark.asyncio
async def test_http_client_proxy_variants(monkeypatch):
    created_sessions = []

    def client_session(**kwargs):
        created_sessions.append(kwargs)
        return SimpleNamespace()

    connector = object()
    monkeypatch.setattr(utils.aiohttp, "ClientSession", client_session)
    monkeypatch.setattr(
        utils.ProxyConnector,
        "from_url",
        lambda proxy_url: (proxy_url, connector),
    )
    monkeypatch.setattr(utils, "choice", lambda values: values[0])

    monkeypatch.setenv("HLTHPRT_SOCKS_PROXY", '["socks5://proxy.test:1080"]')
    await utils.get_http_client()
    assert created_sessions[-1]["connector"] == (
        "socks5://proxy.test:1080",
        connector,
    )

    monkeypatch.setenv("HLTHPRT_SOCKS_PROXY", '["http://proxy.test:8080"]')
    await utils.get_http_client()
    assert created_sessions[-1]["proxy"] == "http://proxy.test:8080"

    monkeypatch.setenv("HLTHPRT_SOCKS_PROXY", "not-json")
    await utils.get_http_client()
    assert "proxy" not in created_sessions[-1]

    monkeypatch.setenv("HLTHPRT_SOCKS_PROXY", '["http://ignored.test"]')
    await utils.get_http_client(use_proxy=False)
    assert "proxy" not in created_sessions[-1]


@pytest.mark.asyncio
async def test_text_and_nostream_downloads(tmp_path, monkeypatch):
    text_client = _Client(
        get_requests=[
            _Request(_Response(body=b"plain")),
            _Request(_Response(body=b"timed")),
        ]
    )
    monkeypatch.setattr(utils, "get_http_client", AsyncMock(return_value=text_client))
    assert await utils.download_it("https://example.test/plain") == "plain"
    assert await utils.download_it(
        "https://example.test/timed",
        local_timeout=3,
    ) == "timed"

    destination_path = tmp_path / "nostream.bin"
    binary_client = _Client(
        get_requests=[_Request(_Response(status=200, body=b"payload"))]
    )
    monkeypatch.setattr(
        utils,
        "get_http_client",
        AsyncMock(return_value=binary_client),
    )
    await utils.download_it_and_save_nostream(
        "https://example.test/binary",
        str(destination_path),
    )
    assert destination_path.read_bytes() == b"payload"

    refused_client = _Client(
        get_requests=[_Request(_Response(status=503, body=b"retry"))]
    )
    monkeypatch.setattr(
        utils,
        "get_http_client",
        AsyncMock(return_value=refused_client),
    )
    with pytest.raises(Retry):
        await utils.download_it_and_save_nostream(
            "https://example.test/refused",
            str(tmp_path / "refused.bin"),
        )
