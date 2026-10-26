# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import aiohttp
import pytest

from process.ext import utils
from tests.ext_utils_import_coverage_support import _Client, _fixed_download_timeout, _Request, _Response


@pytest.mark.asyncio
async def test_head_download_info_requests_identity_representation():
    client = _Client(head_requests=[_Request(_Response(headers={"Content-Length": "4096", "Accept-Ranges": "bytes"}))])

    assert await utils._head_download_info(client, "https://example.test/data") == (4096, True)

    assert client.calls[0][2]["headers"]["Accept-Encoding"] == "identity"


@pytest.mark.asyncio
async def test_stale_head_size_restarts_with_complete_stream(tmp_path, monkeypatch):
    destination = tmp_path / "providers.json"
    document_bytes = b'[{"id":1,"name":"' + b"x" * 1024 + b'"}]'
    stale_size = len(document_bytes) - 2
    client = _Client(
        head_requests=[_Request(_Response(headers={"Content-Length": str(stale_size), "Accept-Ranges": "bytes"}))],
        get_requests=[
            _Request(
                _Response(
                    status=206,
                    headers={"Content-Range": f"bytes 0-{stale_size - 1}/{len(document_bytes)}"},
                    body=document_bytes[:stale_size],
                )
            ),
            _Request(_Response(body=document_bytes)),
        ],
    )
    monkeypatch.setattr(utils, "get_http_client", AsyncMock(return_value=client))
    monkeypatch.setattr(utils, "_determine_request_timeout", _fixed_download_timeout)
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_THRESHOLD_BYTES", 1)
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_RANGE_SIZE", len(document_bytes))
    monkeypatch.setattr(utils, "PARALLEL_CHUNK_RETRIES", 1)
    monkeypatch.setattr(utils, "PREFER_COMPRESSED_STREAM", False)

    await utils.download_it_and_save("https://example.test/providers.json", str(destination))

    assert destination.read_bytes() == document_bytes
    get_calls = [call for call in client.calls if call[0] == "get"]
    assert len(get_calls) == 2
    assert "Range" not in get_calls[-1][2].get("headers", {})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Content-Range": "invalid"},
        {"Content-Range": "bytes 0-1/*"},
        {"Content-Range": "bytes 1-2/2"},
        {"Content-Range": "bytes 0-0/2"},
        {"Content-Range": "bytes 0-1/3"},
        {"Content-Range": "bytes 0-1/2", "Content-Encoding": "gzip"},
    ],
)
async def test_invalid_range_metadata_is_rejected_before_read(tmp_path, monkeypatch, headers):
    destination = tmp_path / "invalid.bin"
    response = _Response(status=206, headers=headers, body=b"ab")
    response.read = AsyncMock(return_value=b"ab")
    client = _Client(get_requests=[_Request(response)])
    monkeypatch.setattr(utils, "PARALLEL_CHUNK_RETRIES", 1)

    with pytest.raises(RuntimeError, match="Range (metadata|encoding) mismatch"):
        await utils._download_parallel_by_ranges(
            client,
            "https://example.test/data",
            str(destination),
            2,
            aiohttp.ClientTimeout(total=10, sock_read=10),
        )

    response.read.assert_not_awaited()
    assert not destination.exists()
